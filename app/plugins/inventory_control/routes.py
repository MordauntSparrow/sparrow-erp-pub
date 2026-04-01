# Inventory Control plugin routes: admin UI, JSON APIs, mobile API, audit, Socket.IO
import hashlib
import json
import logging
import os
import secrets
import uuid
from datetime import datetime, date
import tempfile
from typing import Any, Dict, Optional
from functools import wraps
from io import StringIO
import csv as csv_module

from flask import (
    Blueprint,
    request,
    jsonify,
    render_template,
    current_app,
    g,
    redirect,
    url_for,
    flash,
    session,
)
from flask_login import login_required, current_user

from app.objects import get_db_connection, has_permission
try:
    from app import socketio
except ImportError:
    socketio = None  # optional: app may not expose Socket.IO

# JWT session auth: required for tablet/mobile Bearer token login. If app.auth_jwt is missing,
# decode_session_token is a stub (returns None) and /api/* routes return 401 for Bearer requests.
_jwt_decode = None
try:
    from app.auth_jwt import decode_session_token as _jwt_decode
except ImportError:
    pass
JWT_AUTH_AVAILABLE = _jwt_decode is not None

def decode_session_token(token):
    """Decode session JWT; returns None if module unavailable or token invalid."""
    if _jwt_decode is None:
        return None
    return _jwt_decode(token)

from app.openapi_utils import register_path

from .objects import add_calendar_months, get_inventory_service
from .ocr import InventoryInvoiceService, TesseractOCRProvider, AmazonInvoiceParser


def _get_asset_service():
    from .asset_service import get_asset_service

    return get_asset_service()

BLUEPRINT_NAME = "inventory_control_internal"
BASE_PATH = "/plugin/inventory_control"

logger = logging.getLogger("inventory_control")
logger.setLevel(logging.INFO)

# Admin/internal blueprint (all admin + API + mobile under one blueprint)
internal = Blueprint(
    "inventory_control_internal",
    __name__,
    url_prefix="/plugin/inventory_control",
    template_folder="templates",
)

from .equipment_asset_routes import register_equipment_asset_routes

register_equipment_asset_routes(internal)

from .equipment_display import (
    equipment_asset_status_label,
    equipment_issue_severity_label,
    equipment_issue_status_label,
    equipment_holder_type_label,
    inventory_location_type_label,
    inventory_transaction_type_label,
    contractor_kit_request_status_label,
)


@internal.app_template_filter("equipment_status_label")
def _filter_equipment_status_label(code):
    return equipment_asset_status_label(code)


@internal.app_template_filter("equipment_issue_status_label")
def _filter_equipment_issue_status_label(code):
    return equipment_issue_status_label(code)


@internal.app_template_filter("equipment_issue_severity_label")
def _filter_equipment_issue_severity_label(code):
    return equipment_issue_severity_label(code)


@internal.app_template_filter("equipment_holder_type_label")
def _filter_equipment_holder_type_label(code):
    return equipment_holder_type_label(code)


@internal.app_template_filter("inventory_tx_type_label")
def _filter_inventory_tx_type_label(code):
    return inventory_transaction_type_label(code)


@internal.app_template_filter("inventory_location_type_label")
def _filter_inventory_location_type_label(code):
    return inventory_location_type_label(code)


@internal.app_template_filter("contractor_kit_status_label")
def _filter_contractor_kit_status_label(code):
    return contractor_kit_request_status_label(code)


@internal.context_processor
def _inventory_serial_equipment_template_ctx():
    """Cross-module nav flags for serial-equipment pages (merged asset UI)."""
    try:
        b = current_app.blueprints
        return {
            "inventory_bp_available": True,
            "asset_links_available": True,
            "ventus_bp_available": "medical_response_internal" in b,
            "cura_bp_available": "medical_records_internal" in b,
        }
    except Exception:
        return {
            "inventory_bp_available": True,
            "asset_links_available": True,
            "ventus_bp_available": False,
            "cura_bp_available": False,
        }


@internal.before_request
def _inventory_token_auth():
    """Set g.inventory_api_user when Authorization: Bearer <token> is valid."""
    g.inventory_api_user = None
    auth = request.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        return
    token = auth[7:].strip()
    if not token:
        return
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    conn = get_db_connection()
    cur = None
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, role, supplier_id, customer_id, scopes FROM inventory_api_tokens WHERE token_hash = %s",
            (token_hash,),
        )
        row = cur.fetchone()
        if row:
            g.inventory_api_user = {
                "id": row["id"],
                "role": row["role"],
                "supplier_id": row.get("supplier_id"),
                "customer_id": row.get("customer_id"),
                "scopes": json.loads(row["scopes"]) if isinstance(row.get("scopes"), str) else (row.get("scopes") or []),
            }
            cur.execute("UPDATE inventory_api_tokens SET last_used_at = NOW() WHERE id = %s", (row["id"],))
            conn.commit()
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass


@internal.before_request
def _session_token_auth():
    """Set g.token_user when Authorization: Bearer <session JWT> is valid (any role)."""
    g.token_user = None
    auth = request.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        return
    token = auth[7:].strip()
    if not token:
        return
    if getattr(g, "inventory_api_user", None):
        return
    payload = decode_session_token(token)
    if payload:
        g.token_user = {
            "id": payload["sub"],
            "username": payload["username"],
            "role": payload.get("role") or "",
        }


def _json_compatible(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_compatible(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_compatible(v) for v in value]
    if isinstance(value, tuple):
        return [_json_compatible(v) for v in value]
    if hasattr(value, "__float__") and hasattr(value, "as_integer_ratio"):  # Decimal
        return float(value)
    return value


def _jsonify_safe(payload, status=200):
    return jsonify(_json_compatible(payload)), status


def _emit_inventory_event(event_type: str, payload: dict):
    if socketio is None:
        return
    try:
        socketio.emit("inventory_event", {"type": event_type, **payload}, broadcast=True)
    except Exception as e:
        logger.warning("Socket.IO emit failed: %s", e)


def _coerce_int_user_id(value):
    """
    inventory_audit.user_id is INT in current schema. Core users often have UUID ids.
    Coerce only numeric ids; otherwise return None (UUID can be stored in details).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    s = str(value).strip()
    if s.isdigit():
        try:
            return int(s)
        except Exception:
            return None
    return None


def log_audit(
    user,
    action: str,
    item_id=None,
    location_id=None,
    batch_id=None,
    transaction_id=None,
    details=None,
):
    try:
        extra = {"user": str(user), "action": action, "module": "inventory_control"}
        if item_id is not None:
            extra["item_id"] = item_id
        if location_id is not None:
            extra["location_id"] = location_id
        if batch_id is not None:
            extra["batch_id"] = batch_id
        if transaction_id is not None:
            extra["transaction_id"] = transaction_id
        if details is not None:
            extra["details"] = details
        audit_logger = getattr(current_app, "audit_logger", None)
        if audit_logger:
            audit_logger.info(action, extra={"extra": extra})
        else:
            logger.info("AUDIT: %s", json.dumps(extra, default=str))

        # Optional DB-level audit trail. Safe with UUID users: store numeric user_id when possible
        # and always include the raw id string in details for traceability.
        if os.environ.get("INVENTORY_AUDIT_TO_DB", "false").lower() == "true":
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                try:
                    details_payload = extra.get("details")
                    if isinstance(details_payload, dict):
                        details_payload = {**details_payload, "user": str(user)}
                    else:
                        details_payload = {"details": details_payload, "user": str(user)}
                    cur.execute(
                        """
                        INSERT INTO inventory_audit (user_id, action, item_id, location_id, batch_id, transaction_id, details)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            _coerce_int_user_id(user),
                            action,
                            item_id,
                            location_id,
                            batch_id,
                            transaction_id,
                            json.dumps(details_payload, default=str),
                        ),
                    )
                    conn.commit()
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass
                    try:
                        conn.close()
                    except Exception:
                        pass
            except Exception:
                # Never break the main request due to audit insert failure
                logger.exception("DB audit insert failed")
    except Exception:
        logger.exception("Audit logging failed")


PERM_INV_ACCESS = "inventory_control.access"
PERM_INV_EDIT = "inventory_control.edit"
PERM_INV_TRANSACT = "inventory_control.transactions"


def _is_admin():
    """True if current user or Bearer token user has admin role (session or g.token_user)."""
    token_user = getattr(g, "token_user", None)
    if token_user and token_user.get("role") in ("admin", "superuser", "support_break_glass"):
        return True
    return getattr(current_user, "role", None) in ("admin", "superuser", "support_break_glass")


def _session_has_any_inventory_perm() -> bool:
    if not getattr(current_user, "is_authenticated", False):
        return False
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return (
        has_permission(PERM_INV_ACCESS)
        or has_permission(PERM_INV_TRANSACT)
        or has_permission(PERM_INV_EDIT)
    )


def _session_inventory_transact() -> bool:
    if not getattr(current_user, "is_authenticated", False):
        return False
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return has_permission(PERM_INV_TRANSACT) or has_permission(PERM_INV_EDIT)


def _session_inventory_edit() -> bool:
    if not getattr(current_user, "is_authenticated", False):
        return False
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return has_permission(PERM_INV_EDIT)


def _csv_import_safe_path(path: Optional[str]) -> bool:
    if not path or not os.path.isfile(path):
        return False
    try:
        base = os.path.normcase(os.path.realpath(tempfile.gettempdir()))
        real = os.path.normcase(os.path.realpath(path))
        return (
            (real.startswith(base + os.sep) or real == base)
            and os.path.basename(real).lower().startswith("invimp_")
        )
    except OSError:
        return False


def _permission_inventory():
    """Session user may open inventory UI (browse) with access, transactions, or edit."""
    return _session_has_any_inventory_perm()


def _bearer_jwt_admin_inventory() -> bool:
    token_user = getattr(g, "token_user", None)
    return bool(token_user and token_user.get("role") in ("admin", "superuser", "support_break_glass"))


def _api_allow_inventory(min_level: str) -> bool:
    """
    API auth: admin/superuser session JWT unchanged; session cookie uses plugin permissions.
    Long-lived inventory_api_user Bearer tokens are not granted general inventory JSON API access here.
    """
    if _bearer_jwt_admin_inventory():
        return True
    if getattr(g, "inventory_api_user", None):
        return False
    if min_level == "access":
        return _session_has_any_inventory_perm()
    if min_level == "transact":
        return _session_inventory_transact()
    if min_level == "edit":
        return _session_inventory_edit()
    return False


def _allow_token_role(role: str) -> bool:
    """True if request is authenticated via session (admin) or Bearer token with given role."""
    if getattr(current_user, "is_authenticated", False) and _is_admin():
        return True
    api_user = getattr(g, "inventory_api_user", None)
    return api_user and api_user.get("role") == role


def admin_required(f):
    """Decorator: inventory module UI (any of access / transactions / edit, or admin role)."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not _session_has_any_inventory_perm():
            flash("Access denied: You do not have permission to access Inventory Control.", "danger")
            return redirect(url_for("routes.dashboard"))
        return f(*args, **kwargs)
    return wrapper


def _api_401_payload():
    """Return 401 JSON payload; add hint when Bearer was sent but JWT auth is unavailable (stub)."""
    payload = {"error": "Authentication required"}
    auth = request.headers.get("Authorization") if request else None
    if auth and auth.startswith("Bearer ") and not JWT_AUTH_AVAILABLE:
        payload["hint"] = (
            "Bearer token auth is not available on this server (app.auth_jwt missing). "
            "Update the deployment to include auth_jwt.py, or use session cookies."
        )
    return payload


def api_authenticated_required(f):
    """Decorator for /api/* routes: allow any valid Bearer token or session. Return 401 JSON when unauthenticated."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if getattr(g, "token_user", None):
            return f(*args, **kwargs)
        if getattr(current_user, "is_authenticated", False):
            return f(*args, **kwargs)
        return _jsonify_safe(_api_401_payload(), 401)
    return wrapper


def api_inventory_access_required(f):
    """Read APIs: session with inventory access (or admin JWT)."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if _api_allow_inventory("access"):
            return f(*args, **kwargs)
        return _jsonify_safe(_api_401_payload(), 401)
    return wrapper


def api_inventory_transact_required(f):
    """Stock movements: session with transactions or edit (or admin JWT)."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if _api_allow_inventory("transact"):
            return f(*args, **kwargs)
        return _jsonify_safe(_api_401_payload(), 401)
    return wrapper


def api_inventory_edit_required(f):
    """Master data / invoices / equipment create: session with edit (or admin JWT)."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if _api_allow_inventory("edit"):
            return f(*args, **kwargs)
        return _jsonify_safe(_api_401_payload(), 401)
    return wrapper


def _effective_user_for_audit():
    """Username or id for audit log: session user or token user."""
    if getattr(g, "token_user", None):
        return g.token_user.get("username") or f"token:{g.token_user.get('id')}"
    if getattr(current_user, "is_authenticated", False):
        return getattr(current_user, "username", None) or str(getattr(current_user, "id", ""))
    return "anonymous"


def _effective_user_id():
    """User id for audit/DB when using session or Bearer token."""
    if getattr(g, "token_user", None):
        return g.token_user.get("id")
    if getattr(current_user, "is_authenticated", False):
        return getattr(current_user, "id", None)
    return None


# ---------------------------------------------------------------------------
# Dashboard & UI pages
# ---------------------------------------------------------------------------

def _app_blueprint_registered(name: str) -> bool:
    try:
        return name in current_app.blueprints
    except Exception:
        return False


def _user_asset_can_assign() -> bool:
    """Matches asset_management._can_assign for cross-module UI (sign back in, etc.)."""
    if not getattr(current_user, "is_authenticated", False):
        return False
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return has_permission("asset_management.edit") or has_permission("asset_management.assign")


def _user_asset_can_edit() -> bool:
    if not getattr(current_user, "is_authenticated", False):
        return False
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return has_permission("asset_management.edit")


@internal.route("/")
@internal.route("/index")
@login_required
def dashboard():
    """Plugin index/landing: admin role required (aligned with rest of system)."""
    if not _permission_inventory():
        flash("Access denied: You do not have permission to access Inventory Control.", "danger")
        return redirect(url_for("routes.dashboard"))
    svc = get_inventory_service()
    health = svc.health_check()
    metrics = svc.get_dashboard_metrics()
    command_center = svc.get_dashboard_command_center()
    return render_template(
        "admin/inventory_dashboard.html",
        health=health,
        metrics=metrics,
        command_center=command_center,
        fleet_links_available=_app_blueprint_registered("fleet_management"),
        ventus_links_available=_app_blueprint_registered("medical_response_internal"),
        medical_links_available=_app_blueprint_registered("medical_records_internal"),
    )


@internal.route("/items")
@login_required
@admin_required
def items_page():
    """Consumables catalog (bulk / small stock). Serial equipment uses equipment routes."""
    return render_template("admin/items.html")


@internal.route("/items/<int:item_id>")
@login_required
@admin_required
def item_overview_page(item_id: int):
    svc = get_inventory_service()
    item = svc.get_item(item_id)
    if not item:
        flash("Item not found.", "danger")
        return redirect(url_for("inventory_control_internal.items_page"))
    if _inventory_row_is_equipment(item):
        flash("Serial equipment product lines are managed under Equipment, not Consumables.", "info")
        q = (item.get("sku") or item.get("name") or "").strip()
        return redirect(
            url_for("inventory_control_internal.equipment_desk_page", **({"q": q} if q else {}))
        )
    return render_template("admin/item_overview.html", item=item)


@internal.route("/categories")
@login_required
@admin_required
def categories_page():
    return render_template("admin/categories.html")


@internal.route("/locations")
@login_required
@admin_required
def locations_page():
    return render_template("admin/locations.html")


@internal.route("/batches")
@login_required
@admin_required
def batches_page():
    return render_template("admin/batches.html")


@internal.route("/repack")
@login_required
@admin_required
def repack_page():
    return render_template("admin/repack.html")


@internal.route("/transactions")
@login_required
@admin_required
def transactions_page():
    return render_template("admin/transactions.html")


@internal.route("/invoices")
@login_required
@admin_required
def invoices_page():
    return render_template("admin/invoices.html")


@internal.route("/analytics")
@login_required
@admin_required
def analytics_page():
    return render_template("admin/analytics.html")


@internal.route("/equipment")
@login_required
@admin_required
def equipment_desk_page():
    """Serialised equipment across all items: assignments, servicing, issues."""
    svc = _get_asset_service()
    q = (request.args.get("q") or "").strip() or None
    st = (request.args.get("status") or "").strip() or None
    assigned_only = (request.args.get("assigned") or "").strip() == "1"
    rows = svc.list_equipment_desk_rows(
        search=q,
        status=st,
        assigned_only=assigned_only,
        limit=500,
    )
    locations = []
    asset_can_assign = _user_asset_can_assign()
    asset_can_edit = _user_asset_can_edit()
    if asset_can_assign:
        try:
            locations = get_inventory_service().list_locations() or []
        except Exception:
            logger.exception("equipment_desk_page list_locations")
    return render_template(
        "admin/equipment_desk.html",
        rows=rows,
        q=q or "",
        status=st or "",
        assigned_only=assigned_only,
        asset_can_assign=asset_can_assign,
        asset_can_edit=asset_can_edit,
        asset_return_locations=locations,
    )


@internal.route("/equipment/at-assignee-location/<int:location_id>")
@login_required
@admin_required
def equipment_at_assignee_location_page(location_id: int):
    """Serial kit whose latest out transaction assigns to this location (e.g. Cura OP-EVT-* pool)."""
    inv = get_inventory_service()
    loc = inv.get_location(int(location_id))
    if not loc:
        flash("Location not found.", "danger")
        return redirect(url_for("inventory_control_internal.locations_page"))
    try:
        rows = inv.list_equipment_held_at_assignee_location(int(location_id))
    except Exception:
        logger.exception("equipment_at_assignee_location_page")
        rows = []
    return render_template(
        "admin/equipment_at_assignee_location.html",
        location=loc,
        rows=rows or [],
    )


@internal.route("/data-import", methods=["GET", "POST"])
@login_required
@admin_required
def data_import_wizard():
    """CSV import: upload → map columns → preview → run (inventory edit only)."""
    if not _session_inventory_edit():
        flash("CSV import needs inventory edit permission.", "danger")
        return redirect(url_for("inventory_control_internal.dashboard"))

    from .csv_import_wizard import (
        SESSION_KEY,
        IMPORT_FIELD_SPECS,
        allowed_entities,
        sniff_read_headers_and_sample,
        run_import,
        clear_session,
        validate_mapping,
        row_from_mapping,
    )

    if request.args.get("reset"):
        st = session.get(SESSION_KEY) or {}
        p = st.get("path")
        if p and _csv_import_safe_path(p):
            try:
                os.unlink(p)
            except OSError:
                pass
        clear_session(session)
        flash("Import cancelled.", "info")
        return redirect(url_for("inventory_control_internal.data_import_wizard"))

    if request.method == "POST":
        act = (request.form.get("action") or "").strip().lower()
        if act == "upload":
            f = request.files.get("csv_file")
            ent = (request.form.get("entity") or "").strip()
            if not f or not f.filename:
                flash("Choose a CSV file.", "danger")
                return redirect(request.url)
            if ent not in allowed_entities():
                flash("Choose what you are importing.", "danger")
                return redirect(request.url)
            if request.content_length and request.content_length > 6 * 1024 * 1024:
                flash("File too large (max 6 MB).", "danger")
                return redirect(request.url)
            try:
                raw = f.read()
                headers, sample, _enc = sniff_read_headers_and_sample(raw)
                if not headers:
                    flash("Could not read CSV headers.", "danger")
                    return redirect(request.url)
                fd, path = tempfile.mkstemp(prefix="invimp_", suffix=".csv")
                os.write(fd, raw)
                os.close(fd)
            except Exception as ex:
                logger.exception("data_import upload")
                flash(str(ex), "danger")
                return redirect(request.url)
            session[SESSION_KEY] = {
                "path": path,
                "entity": ent,
                "headers": headers,
                "sample": sample,
                "mapping": None,
            }
            return redirect(url_for("inventory_control_internal.data_import_wizard"))

        if act == "save_map":
            st = session.get(SESSION_KEY) or {}
            path = st.get("path")
            entity = st.get("entity")
            if not _csv_import_safe_path(path) or entity not in allowed_entities():
                clear_session(session)
                flash("Session expired. Upload again.", "warning")
                return redirect(url_for("inventory_control_internal.data_import_wizard"))
            mapping: Dict[str, str] = {}
            for spec in IMPORT_FIELD_SPECS.get(entity, []):
                key = spec["key"]
                col = (request.form.get(f"map_{key}") or "").strip()
                mapping[key] = col
            errs = validate_mapping(entity, mapping)
            if errs:
                for e in errs[:5]:
                    flash(e, "danger")
                session[SESSION_KEY] = {**st, "mapping": None}
                return redirect(url_for("inventory_control_internal.data_import_wizard"))
            session[SESSION_KEY] = {**st, "mapping": mapping}
            return redirect(url_for("inventory_control_internal.data_import_wizard", step="preview"))

        if act == "run":
            st = session.get(SESSION_KEY) or {}
            path = st.get("path")
            entity = st.get("entity")
            mapping = st.get("mapping") or {}
            if not _csv_import_safe_path(path) or entity not in allowed_entities():
                clear_session(session)
                flash("Session expired. Upload again.", "warning")
                return redirect(url_for("inventory_control_internal.data_import_wizard"))
            errs = validate_mapping(entity, mapping)
            if errs:
                for e in errs[:5]:
                    flash(e, "danger")
                return redirect(url_for("inventory_control_internal.data_import_wizard"))
            inv = get_inventory_service()
            try:
                result = run_import(inv, entity=entity, file_path=path, mapping=mapping)
            except Exception as ex:
                logger.exception("data_import run")
                flash(str(ex), "danger")
                return redirect(url_for("inventory_control_internal.data_import_wizard"))
            try:
                os.unlink(path)
            except OSError:
                pass
            clear_session(session)
            flash(
                f"Import finished: {result.get('created', 0)} created, "
                f"{result.get('updated', 0)} updated, {result.get('skipped', 0)} skipped.",
                "success",
            )
            for err in (result.get("errors") or [])[:25]:
                flash(err, "warning")
            if len(result.get("errors") or []) > 25:
                flash("More errors were logged; check the first messages above.", "warning")
            return redirect(url_for("inventory_control_internal.data_import_wizard"))

    st = session.get(SESSION_KEY) or {}
    step = (request.args.get("step") or "").strip()
    path = st.get("path")
    entity = st.get("entity")
    headers = st.get("headers") or []
    sample = st.get("sample") or []
    mapping = st.get("mapping") or {}
    fields = IMPORT_FIELD_SPECS.get(entity or "", [])

    preview_rows = []
    if step == "preview" and mapping and entity and _csv_import_safe_path(path):
        try:
            raw = open(path, "rb").read()
            _, rows, _ = sniff_read_headers_and_sample(raw, max_sample_rows=5)
            for r in rows:
                preview_rows.append(row_from_mapping(r, mapping, entity))
        except Exception:
            preview_rows = []

    return render_template(
        "admin/data_import_wizard.html",
        session_state=st,
        entity=entity,
        headers=headers,
        sample=sample,
        fields=fields,
        mapping=mapping or {},
        step_preview=step == "preview" and bool(mapping),
        preview_rows=preview_rows,
        entities_meta=[
            ("items", "Items (SKUs, equipment models, consumables)"),
            ("equipment_assets", "Serial equipment (needs model SKU to exist)"),
            ("locations", "Warehouse / store / bin / holding locations"),
        ],
    )


@internal.route("/operations/fit-part-to-vehicle", methods=["GET", "POST"])
@login_required
@admin_required
def fit_part_to_vehicle_page():
    """Wizard: pick storeroom stock + vehicle, log fleet installed part (optional inventory deduction)."""
    if not _app_blueprint_registered("fleet_management"):
        flash("Enable the Fleet module to record parts fitted to vehicles.", "warning")
        return redirect(url_for("inventory_control_internal.dashboard"))
    from app.plugins.fleet_management.objects import get_fleet_service

    inv = get_inventory_service()
    fleet = get_fleet_service()
    locations = inv.list_locations() or []
    vehicles = fleet.list_vehicles(limit=500) or []

    if request.method == "POST":
        try:
            vehicle_id = int(request.form.get("vehicle_id") or 0)
            if not vehicle_id:
                flash("Choose a vehicle.", "danger")
                return redirect(request.url)
            v = fleet.get_vehicle(vehicle_id)
            if not v:
                flash("Vehicle not found.", "danger")
                return redirect(request.url)

            installed_date = (request.form.get("installed_date") or "").strip()
            if not installed_date:
                flash("Installed date is required.", "danger")
                return redirect(request.url)

            item_raw = (request.form.get("inventory_item_id") or "").strip()
            item_id = int(item_raw) if item_raw.isdigit() else None
            if not item_id:
                flash("Choose an inventory item (from stock at the selected location).", "danger")
                return redirect(request.url)

            qty = float(request.form.get("quantity") or 1) or 1.0
            deduct = request.form.get("deduct_stock") == "1"
            loc_id = int(request.form.get("stock_location_id") or 0) if deduct else None
            if deduct and not loc_id:
                flash("Choose the storeroom to deduct stock from.", "danger")
                return redirect(request.url)

            odo_raw = (request.form.get("odometer_at_install") or "").strip()
            try:
                odometer_at_install = int(odo_raw) if odo_raw else None
            except ValueError:
                odometer_at_install = None

            warranty_end = (request.form.get("warranty_expires_date") or "").strip() or None
            w_start = (request.form.get("warranty_start_date") or "").strip() or None
            w_months_s = (request.form.get("warranty_months") or "").strip()
            w_months = int(w_months_s) if w_months_s.isdigit() else None
            if request.form.get("apply_part_warranty_months") == "1" and w_start and w_months and w_months > 0:
                warranty_end = add_calendar_months(
                    date.fromisoformat(w_start[:10]), w_months
                ).isoformat()

            w_terms_parts = []
            if (request.form.get("warranty_start_basis") or "").strip():
                w_terms_parts.append(
                    "Warranty start: "
                    + (request.form.get("warranty_start_basis") or "").strip()
                )
            if w_start:
                w_terms_parts.append("Start date: " + w_start[:10])
            if w_months:
                w_terms_parts.append(f"Duration: {w_months} mo")
            user_terms = (request.form.get("warranty_terms") or "").strip()
            if user_terms:
                w_terms_parts.append(user_terms)
            warranty_terms = " · ".join(w_terms_parts) if w_terms_parts else None

            uid = _effective_user_id()
            fleet.install_part_from_workshop(
                vehicle_id,
                installed_date=installed_date,
                inventory_item_id=item_id,
                quantity=qty,
                deduct_stock=deduct,
                stock_location_id=loc_id,
                part_number=(request.form.get("part_number") or "").strip() or None,
                part_description=(request.form.get("part_description") or "").strip() or None,
                odometer_at_install=odometer_at_install,
                warranty_expires_date=warranty_end,
                warranty_terms=warranty_terms,
                invoice_reference=(request.form.get("invoice_reference") or "").strip() or None,
                notes=(request.form.get("notes") or "").strip() or None,
                created_by=str(uid) if uid is not None else None,
                performed_by_user_id=uid,
            )
            flash(
                "Part recorded on the vehicle. Open the vehicle in Fleet to review or adjust.",
                "success",
            )
            return redirect(
                url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id)
            )
        except Exception as e:
            logger.exception("fit_part_to_vehicle_page")
            flash(str(e), "danger")
            return redirect(request.url)

    today_iso = date.today().isoformat()
    return render_template(
        "admin/fit_part_to_vehicle.html",
        locations=locations,
        vehicles=vehicles,
        today_iso=today_iso,
    )


# ---------------------------------------------------------------------------
# API: health & dashboard
# ---------------------------------------------------------------------------

@internal.route("/api/health")
@api_authenticated_required
def api_health():
    svc = get_inventory_service()
    out = svc.health_check()
    if not isinstance(out, dict):
        out = {"status": "ok"} if out else {"status": "error"}
    token_user = getattr(g, "token_user", None)
    if token_user:
        out["user"] = {"id": token_user.get("id"), "username": token_user.get("username"), "role": token_user.get("role")}
    elif getattr(current_user, "is_authenticated", False):
        out["user"] = {"id": getattr(current_user, "id", None), "username": getattr(current_user, "username", ""), "role": getattr(current_user, "role", "")}
    return _jsonify_safe(out)


@internal.route("/api/dashboard")
@api_inventory_access_required
def api_dashboard():
    svc = get_inventory_service()
    metrics = svc.get_dashboard_metrics()
    metrics["movement_summary"] = svc.get_movement_summary()
    return _jsonify_safe(metrics)


# ---------------------------------------------------------------------------
# API: categories CRUD
# ---------------------------------------------------------------------------

@internal.route("/api/categories", methods=["GET"])
@api_inventory_access_required
def api_categories_list():
    svc = get_inventory_service()
    parent_id = request.args.get("parent_id", type=int)
    categories = svc.list_categories(parent_id=parent_id)
    return _jsonify_safe({"categories": categories})


@internal.route("/api/categories", methods=["POST"])
@api_inventory_edit_required
def api_categories_create():
    data = request.get_json() or {}
    if not data.get("name"):
        return _jsonify_safe({"error": "name required"}, 400)
    svc = get_inventory_service()
    try:
        cat_id = svc.create_category(data)
        return _jsonify_safe({"id": cat_id}, 201)
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


@internal.route("/api/categories/<int:category_id>", methods=["GET"])
@api_inventory_access_required
def api_categories_get(category_id):
    svc = get_inventory_service()
    cat = svc.get_category(category_id)
    if not cat:
        return _jsonify_safe({"error": "Not found"}, 404)
    return _jsonify_safe(cat)


@internal.route("/api/categories/<int:category_id>", methods=["PUT", "PATCH"])
@api_inventory_edit_required
def api_categories_update(category_id):
    data = request.get_json() or {}
    svc = get_inventory_service()
    if not svc.get_category(category_id):
        return _jsonify_safe({"error": "Not found"}, 404)
    try:
        svc.update_category(category_id, data)
        return _jsonify_safe({"ok": True})
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


@internal.route("/api/categories/<int:category_id>", methods=["DELETE"])
@api_inventory_edit_required
def api_categories_delete(category_id):
    svc = get_inventory_service()
    if not svc.get_category(category_id):
        return _jsonify_safe({"error": "Not found"}, 404)
    svc.delete_category(category_id)
    return _jsonify_safe({"ok": True})


# ---------------------------------------------------------------------------
# API: items CRUD
# ---------------------------------------------------------------------------

_MSG_EQUIPMENT_NOT_VIA_CONSUMABLES_API = (
    "Equipment product lines are not managed through the consumables API. "
    "Use Inventory → Equipment (Add equipment / desk)."
)


def _inventory_row_is_equipment(row: Optional[dict]) -> bool:
    if not row:
        return False
    try:
        return bool(int(row.get("is_equipment") or 0))
    except (TypeError, ValueError):
        return False


def _json_declares_equipment_product_line(data: dict) -> bool:
    def _truthy(val) -> bool:
        if val is True or val == 1:
            return True
        if isinstance(val, str) and val.strip().lower() in ("1", "true", "yes", "on"):
            return True
        return False

    return _truthy(data.get("is_equipment")) or _truthy(data.get("requires_serial"))


@internal.route("/api/items", methods=["GET"])
@api_inventory_access_required
def api_items_list():
    svc = get_inventory_service()
    skip = request.args.get("skip", type=int) or 0
    limit = min(request.args.get("limit", type=int) or 50, 200)
    search = request.args.get("search", "").strip() or None
    category = request.args.get("category", "").strip() or None
    category_id = request.args.get("category_id", type=int)
    is_active = request.args.get("is_active")
    if is_active is not None:
        is_active = str(is_active).lower() in ("1", "true", "yes")
    ex = (request.args.get("exclude_equipment") or "").strip().lower()
    eq_only = (request.args.get("equipment_only") or "").strip().lower()
    is_equipment_filter: Optional[bool] = None
    if ex in ("1", "true", "yes"):
        is_equipment_filter = False
    elif eq_only in ("1", "true", "yes"):
        is_equipment_filter = True
    items = svc.list_items(
        skip=skip,
        limit=limit,
        search=search,
        category=category,
        category_id=category_id,
        is_active=is_active,
        is_equipment=is_equipment_filter,
    )
    return _jsonify_safe({"items": items})


@internal.route("/api/items", methods=["POST"])
@api_inventory_edit_required
def api_items_create():
    data = dict(request.get_json() or {})
    if not data.get("sku") or not data.get("name"):
        return _jsonify_safe({"error": "sku and name required"}, 400)
    if _json_declares_equipment_product_line(data):
        return _jsonify_safe({"error": _MSG_EQUIPMENT_NOT_VIA_CONSUMABLES_API}, 400)
    data["is_equipment"] = False
    data["requires_serial"] = False
    svc = get_inventory_service()
    try:
        item_id = svc.create_item(data)
        log_audit(_effective_user_id(), "inventory_item_create", item_id=item_id, details=data)
        return _jsonify_safe({"id": item_id}, 201)
    except Exception as e:
        logger.exception("create item")
        return _jsonify_safe({"error": str(e)}, 400)


@internal.route("/api/items/<int:item_id>", methods=["GET"])
@api_inventory_access_required
def api_items_get(item_id):
    svc = get_inventory_service()
    item = svc.get_item(item_id)
    if not item:
        return _jsonify_safe({"error": "Not found"}, 404)
    return _jsonify_safe(item)


@internal.route("/api/items/<int:item_id>/overview", methods=["GET"])
@api_inventory_access_required
def api_item_overview(item_id: int):
    svc = get_inventory_service()
    item = svc.get_item(item_id)
    if not item:
        return _jsonify_safe({"error": "Not found"}, 404)
    if _inventory_row_is_equipment(item):
        return _jsonify_safe({"error": _MSG_EQUIPMENT_NOT_VIA_CONSUMABLES_API}, 404)

    range_days = request.args.get("range_days", type=int) or 30
    bucket = (request.args.get("bucket") or "auto").strip().lower()
    data = svc.get_item_stock_series(item_id=item_id, range_days=range_days, bucket=bucket)

    reorder_point = float(item.get("reorder_point") or 0)
    lead_time_days = int(item.get("lead_time_days") or 0)
    lead_time_threshold = reorder_point + (float(data.get("avg_daily_out") or 0) * float(max(lead_time_days, 0)))
    current_qoh = float(data.get("current_qoh") or 0)

    days_of_cover = None
    avg_daily_out = float(data.get("avg_daily_out") or 0)
    if avg_daily_out > 0:
        days_of_cover = current_qoh / avg_daily_out

    return _jsonify_safe(
        {
            "item": {
                "id": item.get("id"),
                "sku": item.get("sku"),
                "name": item.get("name"),
                "unit": item.get("unit"),
                "reorder_point": reorder_point,
                "reorder_quantity": float(item.get("reorder_quantity") or 0),
                "lead_time_days": lead_time_days,
            },
            "range_days": data.get("range_days"),
            "bucket_used": data.get("bucket_used"),
            "tx_count": data.get("tx_count"),
            "current_qoh": current_qoh,
            "avg_daily_out": avg_daily_out,
            "days_of_cover": days_of_cover,
            "reorder_point": reorder_point,
            "lead_time_days": lead_time_days,
            "lead_time_threshold": lead_time_threshold,
            "reorder_now": bool(current_qoh <= lead_time_threshold),
            "series": data.get("series") or [],
        }
    )


@internal.route("/api/items/<int:item_id>/usage_by_person", methods=["GET"])
@api_inventory_access_required
def api_item_usage_by_person(item_id: int):
    """
    Consumables usage trend by assignee. Excludes loaned transactions.
    Returns per-person totals and average monthly usage over the window.
    """
    months = min(request.args.get("months", type=int) or 6, 36)
    svc = get_inventory_service()
    item = svc.get_item(int(item_id))
    if not item:
        return _jsonify_safe({"error": "Not found"}, 404)
    if _inventory_row_is_equipment(item):
        return _jsonify_safe({"error": _MSG_EQUIPMENT_NOT_VIA_CONSUMABLES_API}, 404)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
              assignee_type,
              assignee_id,
              COALESCE(assignee_label, '') AS assignee_label,
              COUNT(*) AS tx_count,
              SUM(CASE WHEN quantity < 0 AND COALESCE(is_loan, 0) = 0 THEN -quantity ELSE 0 END) AS total_out,
              MAX(performed_at) AS last_used_at
            FROM inventory_transactions
            WHERE item_id = %s
              AND performed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MONTH)
              AND assignee_id IS NOT NULL
            GROUP BY assignee_type, assignee_id, assignee_label
            ORDER BY total_out DESC
            LIMIT 50
            """,
            (int(item_id), int(months)),
        )
        rows = cur.fetchall() or []
        for r in rows:
            total_out = float(r.get("total_out") or 0)
            r["avg_per_month"] = (total_out / float(months)) if months > 0 else 0.0
        return _jsonify_safe({"months": months, "usage": rows})
    except Exception:
        return _jsonify_safe({"months": months, "usage": []})
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


@internal.route("/api/items/<int:item_id>/usage_by_person_monthly", methods=["GET"])
@api_inventory_access_required
def api_item_usage_by_person_monthly(item_id: int):
    """
    Monthly usage per assignee for charting. Excludes loaned.
    Returns months[] and series[] with label and data (qty per month).
    """
    months = min(request.args.get("months", type=int) or 12, 36)
    svc = get_inventory_service()
    item = svc.get_item(int(item_id))
    if not item:
        return _jsonify_safe({"error": "Not found"}, 404)
    if _inventory_row_is_equipment(item):
        return _jsonify_safe({"error": _MSG_EQUIPMENT_NOT_VIA_CONSUMABLES_API}, 404)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # Month labels: last N months (YYYY-MM)
        cur.execute(
            """
            SELECT DATE_FORMAT(DATE_SUB(UTC_TIMESTAMP(), INTERVAL n MONTH), '%%Y-%%m') AS ym
            FROM (SELECT 0 AS n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
                  UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11
                  UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17
                  UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23
                  UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29
                  UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35) t
            WHERE n < %s
            ORDER BY ym
            """,
            (int(months),),
        )
        month_list = [r["ym"] for r in (cur.fetchall() or [])]

        cur.execute(
            """
            SELECT
              COALESCE(assignee_label, CONCAT(COALESCE(assignee_type,''), '#', COALESCE(assignee_id,''))) AS label,
              assignee_type,
              assignee_id,
              DATE_FORMAT(performed_at, '%%Y-%%m') AS ym,
              SUM(CASE WHEN quantity < 0 AND COALESCE(is_loan, 0) = 0 THEN -quantity ELSE 0 END) AS qty
            FROM inventory_transactions
            WHERE item_id = %s
              AND performed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MONTH)
              AND assignee_id IS NOT NULL
            GROUP BY assignee_type, assignee_id, assignee_label, DATE_FORMAT(performed_at, '%%Y-%%m')
            """,
            (int(item_id), int(months)),
        )
        rows = cur.fetchall() or []
        # Map (label) -> list of (ym, qty); then build series with data aligned to month_list
        by_person: dict = {}
        for r in rows:
            label = r.get("label") or "Unknown"
            if label not in by_person:
                by_person[label] = {}
            by_person[label][r["ym"]] = float(r.get("qty") or 0)
        series = []
        for label, ym_qty in by_person.items():
            data = [round(ym_qty.get(ym, 0.0), 4) for ym in month_list]
            series.append({"label": label, "data": data})
        series.sort(key=lambda s: -sum(s["data"]))
        return _jsonify_safe({"months": month_list, "series": series[:20]})
    except Exception:
        return _jsonify_safe({"months": [], "series": []})
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


@internal.route("/api/items/<int:item_id>", methods=["PUT", "PATCH"])
@api_inventory_edit_required
def api_items_update(item_id):
    data = dict(request.get_json() or {})
    svc = get_inventory_service()
    existing = svc.get_item(item_id)
    if not existing:
        return _jsonify_safe({"error": "Not found"}, 404)
    if _inventory_row_is_equipment(existing):
        return _jsonify_safe({"error": _MSG_EQUIPMENT_NOT_VIA_CONSUMABLES_API}, 400)
    if _json_declares_equipment_product_line(data):
        return _jsonify_safe({"error": _MSG_EQUIPMENT_NOT_VIA_CONSUMABLES_API}, 400)
    data.pop("is_equipment", None)
    data.pop("requires_serial", None)
    try:
        svc.update_item(item_id, data)
        log_audit(_effective_user_id(), "inventory_item_update", item_id=item_id, details=data)
        return _jsonify_safe({"ok": True})
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


@internal.route("/api/items/<int:item_id>", methods=["DELETE"])
@api_inventory_edit_required
def api_items_archive(item_id):
    svc = get_inventory_service()
    existing = svc.get_item(item_id)
    if not existing:
        return _jsonify_safe({"error": "Not found"}, 404)
    if _inventory_row_is_equipment(existing):
        return _jsonify_safe({"error": _MSG_EQUIPMENT_NOT_VIA_CONSUMABLES_API}, 400)
    svc.archive_item(item_id)
    log_audit(_effective_user_id(), "inventory_item_archive", item_id=item_id)
    return _jsonify_safe({"ok": True})


# ---------------------------------------------------------------------------
# API: locations CRUD
# ---------------------------------------------------------------------------

@internal.route("/api/locations", methods=["GET"])
@api_inventory_access_required
def api_locations_list():
    svc = get_inventory_service()
    parent_id = request.args.get("parent_id", type=int)
    locations = svc.list_locations(parent_id=parent_id)
    return _jsonify_safe({"locations": locations})


@internal.route("/api/locations", methods=["POST"])
@api_inventory_edit_required
def api_locations_create():
    data = request.get_json() or {}
    if not data.get("name") or not data.get("code"):
        return _jsonify_safe({"error": "name and code required"}, 400)
    svc = get_inventory_service()
    try:
        loc_id = svc.create_location(data)
        log_audit(_effective_user_id(), "inventory_location_create", location_id=loc_id, details=data)
        return _jsonify_safe({"id": loc_id}, 201)
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


@internal.route("/api/locations/<int:location_id>", methods=["GET"])
@api_inventory_access_required
def api_locations_get(location_id):
    svc = get_inventory_service()
    loc = svc.get_location(location_id)
    if not loc:
        return _jsonify_safe({"error": "Not found"}, 404)
    return _jsonify_safe(loc)


@internal.route("/api/locations/<int:location_id>", methods=["PUT", "PATCH"])
@api_inventory_edit_required
def api_locations_update(location_id):
    data = request.get_json() or {}
    svc = get_inventory_service()
    if not svc.get_location(location_id):
        return _jsonify_safe({"error": "Not found"}, 404)
    svc.update_location(location_id, data)
    log_audit(_effective_user_id(), "inventory_location_update", location_id=location_id, details=data)
    return _jsonify_safe({"ok": True})


# ---------------------------------------------------------------------------
# API: batches CRUD
# ---------------------------------------------------------------------------

@internal.route("/api/batches", methods=["GET"])
@api_inventory_access_required
def api_batches_list():
    svc = get_inventory_service()
    item_id = request.args.get("item_id", type=int)
    limit = request.args.get("limit", type=int) or 100
    batches = svc.list_batches(item_id=item_id, limit=limit)
    return _jsonify_safe({"batches": batches})


@internal.route("/api/batches", methods=["POST"])
@api_inventory_edit_required
def api_batches_create():
    data = request.get_json() or {}
    if not data.get("item_id"):
        return _jsonify_safe({"error": "item_id required"}, 400)
    svc = get_inventory_service()
    try:
        batch_id = svc.create_batch(data)
        log_audit(_effective_user_id(), "inventory_batch_create", batch_id=batch_id, details=data)
        return _jsonify_safe({"id": batch_id}, 201)
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


@internal.route("/api/batches/<int:batch_id>", methods=["GET"])
@api_inventory_access_required
def api_batches_get(batch_id):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM inventory_batches WHERE id = %s", (batch_id,))
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if not row:
        return _jsonify_safe({"error": "Not found"}, 404)
    return _jsonify_safe(row)


@internal.route("/api/batches/<int:batch_id>", methods=["PUT", "PATCH"])
@api_inventory_edit_required
def api_batches_update(batch_id):
    data = request.get_json() or {}
    svc = get_inventory_service()
    try:
        svc.update_batch(batch_id, data)
        log_audit(_effective_user_id(), "inventory_batch_update", batch_id=batch_id, details=data)
        return _jsonify_safe({"ok": True})
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


# ---------------------------------------------------------------------------
# API: transactions
# ---------------------------------------------------------------------------

@internal.route("/api/transactions", methods=["GET"])
@api_inventory_access_required
def api_transactions_list():
    svc = get_inventory_service()
    item_id = request.args.get("item_id", type=int)
    location_id = request.args.get("location_id", type=int)
    transaction_type = request.args.get("transaction_type", "").strip() or None
    from_date = request.args.get("from_date", "").strip() or None
    to_date = request.args.get("to_date", "").strip() or None
    skip = request.args.get("skip", type=int) or 0
    limit = min(request.args.get("limit", type=int) or 100, 500)
    rows = svc.list_transactions(
        item_id=item_id,
        location_id=location_id,
        transaction_type=transaction_type,
        from_date=from_date,
        to_date=to_date,
        skip=skip,
        limit=limit,
    )
    return _jsonify_safe({"transactions": rows})


@internal.route("/api/transactions", methods=["POST"])
@api_inventory_transact_required
def api_transactions_create():
    data = request.get_json() or {}
    item_id = data.get("item_id")
    location_id = data.get("location_id")
    quantity = data.get("quantity")
    transaction_type = data.get("transaction_type", "in")
    if item_id is None or location_id is None or quantity is None:
        return _jsonify_safe({"error": "item_id, location_id, quantity required"}, 400)
    try:
        quantity = float(quantity)
    except (TypeError, ValueError):
        return _jsonify_safe({"error": "quantity must be numeric"}, 400)
    svc = get_inventory_service()
    try:
        # Optional sign-out/loan fields (for consumables + equipment)
        assignee_type = (data.get("assignee_type") or "").strip() or None
        assignee_id = (data.get("assignee_id") or "").strip() or None
        assignee_label = (data.get("assignee_label") or "").strip() or None
        is_loan = bool(data.get("is_loan")) if data.get("is_loan") is not None else False
        due_back_date = (data.get("due_back_date") or "").strip() or None
        equipment_asset_id = data.get("equipment_asset_id")
        equipment_serial = (data.get("equipment_serial") or "").strip() or None

        if equipment_serial and not equipment_asset_id:
            asset = svc.get_equipment_asset_by_serial(equipment_serial)
            if not asset:
                return _jsonify_safe({"error": "Equipment serial not found"}, 400)
            equipment_asset_id = asset.get("id")
        if equipment_asset_id:
            asset = svc.get_equipment_asset(int(equipment_asset_id))
            if not asset:
                return _jsonify_safe({"error": "Equipment asset not found"}, 400)
            if int(asset.get("item_id")) != int(item_id):
                return _jsonify_safe({"error": "Equipment asset does not belong to this item"}, 400)
            # Basic status guard
            if transaction_type == "out" and str(asset.get("status")) not in ("in_stock",):
                return _jsonify_safe({"error": f"Asset not available (status={asset.get('status')})"}, 400)

        result = svc.record_transaction(
            item_id=int(item_id),
            location_id=int(location_id),
            quantity=quantity,
            transaction_type=transaction_type,
            batch_id=data.get("batch_id"),
            unit_cost=data.get("unit_cost"),
            reference_type=data.get("reference_type"),
            reference_id=data.get("reference_id"),
            performed_by_user_id=_effective_user_id(),
            assignee_type=assignee_type,
            assignee_id=assignee_id,
            assignee_label=assignee_label,
            is_loan=is_loan,
            due_back_date=due_back_date,
            equipment_asset_id=int(equipment_asset_id) if equipment_asset_id is not None else None,
            metadata=data.get("metadata"),
            client_action_id=data.get("client_action_id"),
            weight=data.get("weight"),
            weight_uom=data.get("weight_uom"),
            uom=data.get("uom"),
        )

        # Keep equipment asset status in sync (best-effort)
        try:
            if equipment_asset_id:
                if transaction_type == "out":
                    svc.set_equipment_asset_status(int(equipment_asset_id), "loaned" if is_loan else "assigned")
                elif transaction_type in ("return", "in"):
                    svc.set_equipment_asset_status(int(equipment_asset_id), "in_stock")
        except Exception:
            logger.exception("equipment asset status update failed")

        log_audit(
            _effective_user_id(),
            "inventory_transaction",
            item_id=item_id,
            location_id=location_id,
            transaction_id=result.get("transaction_id"),
            details=result,
        )
        _emit_inventory_event(
            "stock_changed",
            {
                "item_id": item_id,
                "location_id": location_id,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "stock": result.get("stock"),
            },
        )
        return _jsonify_safe(result, 201)
    except ValueError as e:
        return _jsonify_safe({"error": str(e)}, 400)
    except Exception as e:
        logger.exception("record_transaction")
        return _jsonify_safe({"error": str(e)}, 500)


@internal.route("/api/repack", methods=["POST"])
@api_inventory_transact_required
def api_repack():
    data = request.get_json() or {}
    source_batch_id = data.get("source_batch_id")
    location_id = data.get("location_id")
    outputs = data.get("outputs") or []
    if not source_batch_id or not location_id or not outputs:
        return _jsonify_safe({"error": "source_batch_id, location_id, outputs required"}, 400)
    svc = get_inventory_service()
    try:
        result = svc.repack(
            source_batch_id=int(source_batch_id),
            location_id=int(location_id),
            outputs=outputs,
            performed_by_user_id=_effective_user_id(),
        )
        log_audit(
            _effective_user_id(),
            "inventory_repack",
            batch_id=source_batch_id,
            location_id=location_id,
            details=result,
        )
        _emit_inventory_event("stock_changed", {"repack_id": result.get("repack_id")})
        return _jsonify_safe(result, 201)
    except ValueError as e:
        return _jsonify_safe({"error": str(e)}, 400)
    except Exception as e:
        logger.exception("repack")
        return _jsonify_safe({"error": str(e)}, 500)


@internal.route("/api/picking/suggest")
@api_inventory_transact_required
def api_picking_suggest():
    """FEFO: suggest batches to pick for outbound, ordered by expiry soonest first."""
    item_id = request.args.get("item_id", type=int)
    location_id = request.args.get("location_id", type=int)
    quantity = request.args.get("quantity", type=float)
    if not item_id or not location_id or quantity is None or quantity <= 0:
        return _jsonify_safe({"error": "item_id, location_id, quantity (positive) required"}, 400)
    svc = get_inventory_service()
    suggestions = svc.get_picking_suggestions_fefo(item_id=item_id, location_id=location_id, quantity=quantity)
    return _jsonify_safe({"suggestions": suggestions})


@internal.route("/api/transactions/<int:tx_id>/rollback", methods=["POST"])
@api_inventory_transact_required
def api_transactions_rollback(tx_id):
    svc = get_inventory_service()
    try:
        svc.rollback_transaction(tx_id)
        log_audit(_effective_user_id(), "inventory_transaction_rollback", transaction_id=tx_id)
        _emit_inventory_event("stock_changed", {"transaction_id": tx_id, "rollback": True})
        return _jsonify_safe({"ok": True})
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


# ---------------------------------------------------------------------------
# API: analytics (reports + activity)
# ---------------------------------------------------------------------------

@internal.route("/api/analytics/stock_levels")
@api_inventory_access_required
def api_analytics_stock_levels():
    """Stock levels report with item/location names. Optional item_id, location_id, limit."""
    item_id = request.args.get("item_id", type=int)
    location_id = request.args.get("location_id", type=int)
    limit = min(request.args.get("limit", type=int) or 500, 5000)
    skip = request.args.get("skip", type=int) or 0
    svc = get_inventory_service()
    report = svc.list_stock_levels_report(item_id=item_id, location_id=location_id, limit=limit, skip=skip)
    return _jsonify_safe({"report": _json_compatible(report), "count": len(report)})


@internal.route("/api/analytics/movers")
@api_inventory_access_required
def api_analytics_movers():
    """Fast and slow movers by transaction count and quantity in the last N days."""
    days = min(request.args.get("days", type=int) or 30, 365)
    top_n = min(request.args.get("top", type=int) or 20, 100)
    svc = get_inventory_service()
    data = svc.get_analytics_movers(days=days, top_n=top_n)
    return _jsonify_safe(data)


@internal.route("/api/analytics/activity")
@api_inventory_access_required
def api_analytics_activity():
    """Recent transaction activity with item and location names."""
    days = min(request.args.get("days", type=int) or 7, 90)
    limit = min(request.args.get("limit", type=int) or 200, 1000)
    svc = get_inventory_service()
    rows = svc.get_analytics_activity(days=days, limit=limit)
    return _jsonify_safe({"activity": rows})


@internal.route("/api/analytics/suppliers")
@api_inventory_access_required
def api_analytics_suppliers():
    """Supplier list for reporting (optional performance metrics later)."""
    svc = get_inventory_service()
    suppliers = svc.list_suppliers(limit=500)
    return _jsonify_safe({"suppliers": _json_compatible(suppliers)})


# ---------------------------------------------------------------------------
# API: invoices (upload, get, apply)
# ---------------------------------------------------------------------------

def _invoice_upload_dir():
    base = os.path.dirname(os.path.abspath(__file__))
    d = os.path.join(base, "data", "invoices")
    os.makedirs(d, exist_ok=True)
    return d


@internal.route("/api/invoices/upload", methods=["POST"])
@api_inventory_edit_required
def api_invoices_upload():
    f = request.files.get("file")
    if not f:
        return _jsonify_safe({"error": "file required"}, 400)
    ext = (os.path.splitext(f.filename or "")[1] or "").lower()
    allowed = (".png", ".jpg", ".jpeg", ".gif", ".tiff", ".bmp", ".pdf", ".docx")
    if ext not in allowed:
        return _jsonify_safe({"error": "File type not allowed. Use image (png, jpg, etc.), PDF, or DOCX."}, 400)
    path = os.path.join(_invoice_upload_dir(), f"{uuid.uuid4().hex}{ext}")
    f.save(path)
    source = (request.form.get("source") or "amazon").strip() or "amazon"
    supplier_id = request.form.get("supplier_id", type=int)
    try:
        invoice_svc = InventoryInvoiceService(
            ocr_provider=TesseractOCRProvider(),
            parser=AmazonInvoiceParser(),
        )
        parsed = invoice_svc.parse_file(path, source=source)
    except Exception as e:
        logger.exception("invoice parse")
        return _jsonify_safe({"error": f"OCR/parse failed: {e}"}, 400)
    svc = get_inventory_service()
    invoice_id = svc.create_invoice_record(
        supplier_id=supplier_id or None,
        external_source=source,
        invoice_number=(parsed.invoice_number or "").strip() or None,
        invoice_date=parsed.invoice_date,
        currency=parsed.currency,
        status="parsed",
        raw_file_path=path,
        parsed_payload={
            "invoice_number": parsed.invoice_number,
            "invoice_date": parsed.invoice_date,
            "supplier_name": parsed.supplier_name,
            "external_source": source,
            "lines_count": len(parsed.lines),
        },
    )
    for line in parsed.lines:
        svc.add_invoice_line(
            invoice_id=invoice_id,
            sku=line.sku,
            description=line.description,
            quantity=line.quantity,
            unit_price=line.unit_price,
            line_total=line.line_total,
            external_item_ref=line.external_item_ref,
            match_status="unmapped",
        )
    log_audit(_effective_user_id(), "inventory_invoice_upload", details={"invoice_id": invoice_id})
    _emit_inventory_event("invoice_parsed", {"invoice_id": invoice_id})
    return _jsonify_safe({"invoice_id": invoice_id, "lines": len(parsed.lines)}, 201)


@internal.route("/api/invoices", methods=["GET"])
@api_inventory_access_required
def api_invoices_list():
    """List invoices for admin UI."""
    svc = get_inventory_service()
    limit = min(int(request.args.get("limit", 100)), 500)
    skip = int(request.args.get("skip", 0))
    status = request.args.get("status")
    invoices = svc.list_invoices(limit=limit, skip=skip, status=status)
    return _jsonify_safe({"invoices": _json_compatible(invoices)})


@internal.route("/api/invoices/<int:invoice_id>", methods=["GET"])
@api_inventory_access_required
def api_invoices_get(invoice_id):
    svc = get_inventory_service()
    inv = svc.get_invoice(invoice_id)
    if not inv:
        return _jsonify_safe({"error": "Not found"}, 404)
    lines = svc.get_invoice_lines(invoice_id)
    inv["lines"] = _json_compatible(lines)
    if isinstance(inv.get("parsed_payload"), str):
        try:
            inv["parsed_payload"] = json.loads(inv["parsed_payload"])
        except Exception:
            pass
    return _jsonify_safe(inv)


@internal.route("/api/invoices/<int:invoice_id>", methods=["PUT", "PATCH"])
@api_inventory_edit_required
def api_invoices_update(invoice_id):
    data = request.get_json() or {}
    svc = get_inventory_service()
    if not svc.get_invoice(invoice_id):
        return _jsonify_safe({"error": "Not found"}, 404)
    updates = {}
    for key in ("supplier_id", "external_source", "invoice_number", "invoice_date", "total_amount", "currency"):
        if key in data:
            updates[key] = data[key]
    if updates:
        svc.update_invoice(invoice_id, **updates)
    return _jsonify_safe({"ok": True})


@internal.route("/api/invoices/<int:invoice_id>/lines/<int:line_id>", methods=["PUT", "PATCH"])
@api_inventory_edit_required
def api_invoices_line_update(invoice_id, line_id):
    data = request.get_json() or {}
    svc = get_inventory_service()
    lines = svc.get_invoice_lines(invoice_id)
    if not any(l.get("id") == line_id for l in lines):
        return _jsonify_safe({"error": "Line not found"}, 404)
    updates = {}
    for key in ("sku", "description", "quantity", "unit_price", "line_total"):
        if key in data:
            updates[key] = data[key]
    if updates:
        svc.update_invoice_line(line_id, **updates)
    return _jsonify_safe({"ok": True})


@internal.route("/api/invoices/<int:invoice_id>/lines/<int:line_id>/match", methods=["PUT", "PATCH"])
@api_inventory_edit_required
def api_invoices_line_match(invoice_id, line_id):
    data = request.get_json() or {}
    item_id = data.get("item_id")
    svc = get_inventory_service()
    svc.update_invoice_line_item(line_id, item_id)
    return _jsonify_safe({"ok": True})


@internal.route("/api/invoices/<int:invoice_id>/apply", methods=["POST"])
@api_inventory_transact_required
def api_invoices_apply(invoice_id):
    data = request.get_json() or {}
    location_id = data.get("location_id")
    if not location_id:
        return _jsonify_safe({"error": "location_id required"}, 400)
    svc = get_inventory_service()
    if not svc.get_invoice(invoice_id):
        return _jsonify_safe({"error": "Not found"}, 404)
    result = svc.apply_invoice_to_stock(
        invoice_id,
        int(location_id),
        performed_by_user_id=_effective_user_id(),
    )
    log_audit(_effective_user_id(), "inventory_invoice_apply", details={"invoice_id": invoice_id, **result})
    _emit_inventory_event("stock_changed", {"invoice_id": invoice_id})
    return _jsonify_safe(result)


# ---------------------------------------------------------------------------
# Mobile API: scan in/out/adjust, search, stock
# ---------------------------------------------------------------------------

@internal.route("/api/mobile/scan/in", methods=["POST"])
@api_inventory_transact_required
def api_mobile_scan_in():
    data = request.get_json() or {}
    barcode_or_sku = data.get("barcode") or data.get("sku")
    location_id = data.get("location_id")
    quantity = data.get("quantity", 1)
    if not barcode_or_sku or not location_id:
        return _jsonify_safe({"error": "barcode/sku and location_id required"}, 400)
    svc = get_inventory_service()
    item = svc.find_item_by_sku_or_barcode(str(barcode_or_sku))
    if not item:
        return _jsonify_safe({"error": "item not found"}, 404)
    try:
        result = svc.record_transaction(
            item_id=item["id"],
            location_id=int(location_id),
            quantity=float(quantity),
            transaction_type="in",
            performed_by_user_id=_effective_user_id(),
            client_action_id=data.get("client_action_id"),
            weight=data.get("weight"),
            weight_uom=data.get("weight_uom"),
            uom=data.get("uom"),
        )
        _emit_inventory_event("stock_changed", {"item_id": item["id"], "location_id": location_id, "quantity": quantity})
        return _jsonify_safe(result, 201)
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


@internal.route("/api/mobile/scan/out", methods=["POST"])
@api_inventory_transact_required
def api_mobile_scan_out():
    data = request.get_json() or {}
    barcode_or_sku = data.get("barcode") or data.get("sku")
    location_id = data.get("location_id")
    quantity = data.get("quantity", 1)
    if not barcode_or_sku or not location_id:
        return _jsonify_safe({"error": "barcode/sku and location_id required"}, 400)
    svc = get_inventory_service()
    item = svc.find_item_by_sku_or_barcode(str(barcode_or_sku))
    if not item:
        return _jsonify_safe({"error": "item not found"}, 404)
    try:
        result = svc.record_transaction(
            item_id=item["id"],
            location_id=int(location_id),
            quantity=float(quantity),
            transaction_type="out",
            performed_by_user_id=_effective_user_id(),
            client_action_id=data.get("client_action_id"),
            weight=data.get("weight"),
            weight_uom=data.get("weight_uom"),
            uom=data.get("uom"),
        )
        _emit_inventory_event("stock_changed", {"item_id": item["id"], "location_id": location_id, "quantity": -float(quantity)})
        return _jsonify_safe(result, 201)
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


@internal.route("/api/mobile/scan/adjust", methods=["POST"])
@api_inventory_transact_required
def api_mobile_scan_adjust():
    data = request.get_json() or {}
    barcode_or_sku = data.get("barcode") or data.get("sku")
    location_id = data.get("location_id")
    quantity = data.get("quantity")
    if not barcode_or_sku or not location_id or quantity is None:
        return _jsonify_safe({"error": "barcode/sku, location_id, quantity required"}, 400)
    svc = get_inventory_service()
    item = svc.find_item_by_sku_or_barcode(str(barcode_or_sku))
    if not item:
        return _jsonify_safe({"error": "item not found"}, 404)
    try:
        result = svc.record_transaction(
            item_id=item["id"],
            location_id=int(location_id),
            quantity=float(quantity),
            transaction_type="adjustment",
            performed_by_user_id=_effective_user_id(),
            client_action_id=data.get("client_action_id"),
            weight=data.get("weight"),
            weight_uom=data.get("weight_uom"),
            uom=data.get("uom"),
        )
        _emit_inventory_event("stock_changed", {"item_id": item["id"], "location_id": location_id})
        return _jsonify_safe(result, 201)
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


@internal.route("/api/mobile/items/search")
@api_inventory_access_required
def api_mobile_items_search():
    q = request.args.get("q", "").strip()
    if not q:
        return _jsonify_safe({"items": []})
    svc = get_inventory_service()
    items = svc.list_items(limit=20, search=q, is_active=True)
    return _jsonify_safe({"items": items})


@internal.route("/api/mobile/items/<int:item_id>/stock")
@api_inventory_access_required
def api_mobile_items_stock(item_id):
    svc = get_inventory_service()
    levels = svc.list_stock_levels(item_id)
    return _jsonify_safe({"item_id": item_id, "stock_levels": levels})


@internal.route("/api/mobile/bulk/actions", methods=["POST"])
@api_inventory_transact_required
def api_mobile_bulk_actions():
    data = request.get_json() or {}
    actions = data.get("actions") or []
    results = []
    svc = get_inventory_service()
    for a in actions:
        client_action_id = a.get("client_action_id")
        action_type = a.get("type")
        try:
            if action_type == "in":
                item = svc.find_item_by_sku_or_barcode(a.get("barcode") or a.get("sku") or "")
                if not item:
                    results.append({"client_action_id": client_action_id, "error": "item not found"})
                    continue
                r = svc.record_transaction(
                    item_id=item["id"],
                    location_id=int(a["location_id"]),
                    quantity=float(a.get("quantity", 1)),
                    transaction_type="in",
                    performed_by_user_id=_effective_user_id(),
                    client_action_id=client_action_id,
                    weight=a.get("weight"),
                    weight_uom=a.get("weight_uom"),
                    uom=a.get("uom"),
                )
                results.append({"client_action_id": client_action_id, "transaction_id": r.get("transaction_id")})
            elif action_type == "out":
                item = svc.find_item_by_sku_or_barcode(a.get("barcode") or a.get("sku") or "")
                if not item:
                    results.append({"client_action_id": client_action_id, "error": "item not found"})
                    continue
                r = svc.record_transaction(
                    item_id=item["id"],
                    location_id=int(a["location_id"]),
                    quantity=float(a.get("quantity", 1)),
                    transaction_type="out",
                    performed_by_user_id=_effective_user_id(),
                    client_action_id=client_action_id,
                    weight=a.get("weight"),
                    weight_uom=a.get("weight_uom"),
                    uom=a.get("uom"),
                )
                results.append({"client_action_id": client_action_id, "transaction_id": r.get("transaction_id")})
            else:
                results.append({"client_action_id": client_action_id, "error": f"unknown type {action_type}"})
        except Exception as e:
            results.append({"client_action_id": client_action_id, "error": str(e)})
    return _jsonify_safe({"results": results})


# ---------------------------------------------------------------------------
# API: Token-based auth (create/list/revoke) — admin only
# ---------------------------------------------------------------------------

@internal.route("/api/tokens", methods=["POST"])
@api_inventory_edit_required
def api_tokens_create():
    """Create an API token for external supplier access. Raw token returned only once."""
    data = request.get_json() or {}
    name = (data.get("name") or "").strip() or None
    role = (data.get("role") or "supplier").strip().lower()
    if role != "supplier":
        return _jsonify_safe({"error": "role must be 'supplier' (customer-facing features are in Sales module)"}, 400)
    supplier_id = data.get("supplier_id")
    customer_id = None  # reserved for future Sales module; not used in Inventory
    scopes = data.get("scopes")
    if isinstance(scopes, list):
        scopes = json.dumps(scopes)
    raw_token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO inventory_api_tokens (token_hash, name, role, supplier_id, customer_id, scopes)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (token_hash, name, role, supplier_id, customer_id, scopes),
        )
        conn.commit()
        token_id = cur.lastrowid
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()
    log_audit(_effective_user_id(), "inventory_token_create", details={"token_id": token_id, "role": role})
    return _jsonify_safe({
        "id": token_id,
        "name": name,
        "role": role,
        "supplier_id": supplier_id,
        "customer_id": customer_id,
        "token": raw_token,
        "message": "Store the token securely; it will not be shown again.",
    }, 201)


@internal.route("/api/tokens", methods=["GET"])
@api_inventory_edit_required
def api_tokens_list():
    """List API tokens (no secret value)."""
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, name, role, supplier_id, customer_id, scopes, created_at, last_used_at FROM inventory_api_tokens ORDER BY id DESC"
        )
        rows = cur.fetchall() or []
        for r in rows:
            if isinstance(r.get("scopes"), str):
                try:
                    r["scopes"] = json.loads(r["scopes"]) if r["scopes"] else []
                except Exception:
                    r["scopes"] = []
        return _jsonify_safe({"tokens": rows})
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


@internal.route("/api/tokens/<int:token_id>", methods=["DELETE"])
@api_inventory_edit_required
def api_tokens_revoke(token_id):
    """Revoke an API token."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM inventory_api_tokens WHERE id = %s", (token_id,))
        conn.commit()
        if cur.rowcount == 0:
            return _jsonify_safe({"error": "Not found"}, 404)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()
    log_audit(_effective_user_id(), "inventory_token_revoke", details={"token_id": token_id})
    return _jsonify_safe({"ok": True})


# ---------------------------------------------------------------------------
# API: Supplier-scoped (Bearer token role=supplier or session with inventory)
# ---------------------------------------------------------------------------

@internal.route("/api/supplier/items", methods=["GET"])
def api_supplier_items():
    """Limited item list for suppliers (id, sku, name, uom)."""
    if not _allow_token_role("supplier"):
        return _jsonify_safe({"error": "Forbidden"}, 403)
    svc = get_inventory_service()
    limit = min(int(request.args.get("limit", 50)), 200)
    skip = int(request.args.get("skip", 0))
    items = svc.list_items(skip=skip, limit=limit, is_active=True)
    out = []
    for row in items:
        out.append({
            "id": row["id"],
            "sku": row.get("sku"),
            "name": row.get("name"),
            "uom": row.get("uom"),
        })
    return _jsonify_safe({"items": out})


@internal.route("/api/supplier/invoices", methods=["GET"])
def api_supplier_invoices():
    """List invoices for the supplier (when token has supplier_id)."""
    if not _allow_token_role("supplier"):
        return _jsonify_safe({"error": "Forbidden"}, 403)
    api_user = getattr(g, "inventory_api_user", None)
    supplier_id = api_user.get("supplier_id") if api_user else None
    if not supplier_id and getattr(current_user, "is_authenticated", False):
        supplier_id = request.args.get("supplier_id", type=int)
    svc = get_inventory_service()
    limit = min(int(request.args.get("limit", 50)), 100)
    skip = int(request.args.get("skip", 0))
    invoices = svc.list_invoices(supplier_id=supplier_id, limit=limit, skip=skip)
    return _jsonify_safe({"invoices": _json_compatible(invoices)})


@internal.route("/api/supplier/receipts/confirm", methods=["POST"])
def api_supplier_receipts_confirm():
    """Confirm receipt: apply invoice to stock (supplier confirms)."""
    if not _allow_token_role("supplier"):
        return _jsonify_safe({"error": "Forbidden"}, 403)
    data = request.get_json() or {}
    invoice_id = data.get("invoice_id")
    location_id = data.get("location_id")
    if not invoice_id or not location_id:
        return _jsonify_safe({"error": "invoice_id and location_id required"}, 400)
    svc = get_inventory_service()
    inv = svc.get_invoice(int(invoice_id))
    if not inv:
        return _jsonify_safe({"error": "Invoice not found"}, 404)
    api_user = getattr(g, "inventory_api_user", None)
    if api_user and api_user.get("supplier_id") is not None and inv.get("supplier_id") != api_user.get("supplier_id"):
        return _jsonify_safe({"error": "Forbidden: invoice not for this supplier"}, 403)
    performed_by = _effective_user_id()
    try:
        result = svc.apply_invoice_to_stock(
            int(invoice_id), int(location_id), performed_by_user_id=performed_by
        )
        return _jsonify_safe(result)
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


# ---------------------------------------------------------------------------
# API: people search (core users + contractors)
# ---------------------------------------------------------------------------

@internal.route("/api/people/search", methods=["GET"])
@api_inventory_access_required
def api_people_search():
    q = (request.args.get("q") or "").strip()
    limit = min(request.args.get("limit", type=int) or 20, 50)
    if not q:
        return _jsonify_safe({"people": []})
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    people = []
    try:
        like = f"%{q}%"
        # Core users (UUID ids)
        try:
            cur.execute(
                """
                SELECT id, username, email, first_name, last_name
                FROM users
                WHERE username LIKE %s OR email LIKE %s OR first_name LIKE %s OR last_name LIKE %s
                ORDER BY username
                LIMIT %s
                """,
                (like, like, like, like, limit),
            )
            for r in (cur.fetchall() or []):
                label = (" ".join([str(r.get("first_name") or "").strip(), str(r.get("last_name") or "").strip()]).strip()
                         or str(r.get("username") or "").strip()
                         or str(r.get("email") or "").strip())
                people.append(
                    {"type": "user", "id": str(r.get("id")), "label": label, "username": r.get("username"), "email": r.get("email")}
                )
        except Exception:
            pass

        # Contractors (time billing module)
        try:
            cur.execute(
                """
                SELECT id, email, name
                FROM tb_contractors
                WHERE name LIKE %s OR email LIKE %s
                ORDER BY name
                LIMIT %s
                """,
                (like, like, limit),
            )
            for r in (cur.fetchall() or []):
                label = str(r.get("name") or r.get("email") or "").strip()
                people.append(
                    {"type": "contractor", "id": str(r.get("id")), "label": label, "email": r.get("email")}
                )
        except Exception:
            pass
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    # Basic dedupe (type+id)
    seen = set()
    out = []
    for p in people:
        key = (p.get("type"), p.get("id"))
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return _jsonify_safe({"people": out[:limit]})


# ---------------------------------------------------------------------------
# API: equipment assets (serialised inventory)
# ---------------------------------------------------------------------------

@internal.route("/api/equipment/assets", methods=["GET"])
@api_inventory_access_required
def api_equipment_assets_list():
    svc = get_inventory_service()
    item_id = request.args.get("item_id", type=int)
    status = (request.args.get("status") or "").strip() or None
    search = (request.args.get("search") or "").strip() or None
    assets = svc.list_equipment_assets(item_id=item_id, status=status, search=search, limit=200)
    return _jsonify_safe({"assets": assets})


@internal.route("/api/equipment/desk", methods=["GET"])
@api_inventory_access_required
def api_equipment_desk():
    """Equipment rows with holder, issues count, last service (for item overview / desk)."""
    item_id = request.args.get("item_id", type=int)
    status = (request.args.get("status") or "").strip() or None
    search = (request.args.get("search") or "").strip() or None
    assigned_only = request.args.get("assigned_only", type=int) == 1
    limit = min(request.args.get("limit", type=int) or 200, 500)
    rows = _get_asset_service().list_equipment_desk_rows(
        item_id=item_id,
        status=status,
        search=search,
        assigned_only=assigned_only,
        limit=limit,
    )
    return _jsonify_safe({"assets": rows})


@internal.route("/api/equipment/assets", methods=["POST"])
@api_inventory_edit_required
def api_equipment_assets_create():
    svc = get_inventory_service()
    data = request.get_json() or {}
    item_id = data.get("item_id")
    serial_number = (data.get("serial_number") or "").strip()
    if not item_id or not serial_number:
        return _jsonify_safe({"error": "item_id and serial_number required"}, 400)
    # Ensure item exists and is marked as equipment (best-effort)
    item = svc.get_item(int(item_id))
    if not item:
        return _jsonify_safe({"error": "Item not found"}, 404)
    try:
        asset_id = svc.create_equipment_asset(
            item_id=int(item_id),
            serial_number=serial_number,
            make=data.get("make"),
            model=data.get("model"),
            purchase_date=data.get("purchase_date"),
            warranty_expiry=data.get("warranty_expiry"),
            warranty_start_basis=data.get("warranty_start_basis"),
            warranty_start_date=data.get("warranty_start_date"),
            warranty_months=data.get("warranty_months"),
            service_interval_days=data.get("service_interval_days"),
            condition=data.get("condition"),
            metadata=data.get("metadata") or {},
        )
        log_audit(_effective_user_id(), "inventory_equipment_asset_create", item_id=item_id, details={"asset_id": asset_id, "serial_number": serial_number})
        return _jsonify_safe({"id": asset_id}, 201)
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


@internal.route("/api/equipment/assets/<int:asset_id>", methods=["PATCH"])
@api_inventory_edit_required
def api_equipment_asset_update(asset_id: int):
    svc = get_inventory_service()
    data = request.get_json() or {}
    try:
        patch: Dict[str, Any] = {}
        for key in (
            "make",
            "model",
            "purchase_date",
            "warranty_expiry",
            "warranty_start_basis",
            "warranty_start_date",
            "warranty_months",
            "service_interval_days",
            "condition",
            "status",
        ):
            if key in data:
                patch[key] = data.get(key)
        if "next_service_due_date" in data:
            patch["next_service_due_date"] = (data.get("next_service_due_date") or "") or None
        if "operational_state" in data:
            patch["operational_state"] = data.get("operational_state")
        if "metadata" in data:
            patch["metadata"] = data.get("metadata")
        svc.update_equipment_asset(asset_id, **patch)
        return _jsonify_safe({"ok": True})
    except ValueError as e:
        return _jsonify_safe({"error": str(e)}, 404)
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


@internal.route("/api/supplier/batches", methods=["GET"])
def api_supplier_batches():
    """List batches/lots supplied by this supplier (token must have supplier_id)."""
    if not _allow_token_role("supplier"):
        return _jsonify_safe({"error": "Forbidden"}, 403)
    api_user = getattr(g, "inventory_api_user", None)
    supplier_id = api_user.get("supplier_id") if api_user else None
    if not supplier_id and getattr(current_user, "is_authenticated", False):
        supplier_id = request.args.get("supplier_id", type=int)
    if not supplier_id:
        return _jsonify_safe({"error": "supplier_id required (set on token or query for session users)"}, 400)
    svc = get_inventory_service()
    limit = min(int(request.args.get("limit", 100)), 500)
    skip = int(request.args.get("skip", 0))
    batches = svc.list_batches(supplier_id=supplier_id, limit=limit, skip=skip)
    return _jsonify_safe({"batches": _json_compatible(batches)})


@internal.route("/api/supplier/pos", methods=["GET"])
def api_supplier_pos():
    """View PO status for this supplier (token must have supplier_id)."""
    if not _allow_token_role("supplier"):
        return _jsonify_safe({"error": "Forbidden"}, 403)
    api_user = getattr(g, "inventory_api_user", None)
    supplier_id = api_user.get("supplier_id") if api_user else None
    if not supplier_id and getattr(current_user, "is_authenticated", False):
        supplier_id = request.args.get("supplier_id", type=int)
    if not supplier_id:
        return _jsonify_safe({"error": "supplier_id required (set on token or query for session users)"}, 400)
    svc = get_inventory_service()
    status = request.args.get("status")
    limit = min(int(request.args.get("limit", 100)), 500)
    skip = int(request.args.get("skip", 0))
    pos = svc.list_purchase_orders(supplier_id=supplier_id, status=status, limit=limit, skip=skip)
    return _jsonify_safe({"purchase_orders": _json_compatible(pos)})


def _supplier_docs_upload_dir():
    base = os.path.dirname(os.path.abspath(__file__))
    d = os.path.join(base, "data", "supplier_documents")
    os.makedirs(d, exist_ok=True)
    return d


@internal.route("/api/supplier/compliance", methods=["GET"])
def api_supplier_compliance_list():
    """List compliance (and other) documents for this supplier."""
    if not _allow_token_role("supplier"):
        return _jsonify_safe({"error": "Forbidden"}, 403)
    api_user = getattr(g, "inventory_api_user", None)
    supplier_id = api_user.get("supplier_id") if api_user else None
    if not supplier_id and getattr(current_user, "is_authenticated", False):
        supplier_id = request.args.get("supplier_id", type=int)
    if not supplier_id:
        return _jsonify_safe({"error": "supplier_id required (set on token or query for session users)"}, 400)
    svc = get_inventory_service()
    document_type = request.args.get("document_type")
    limit = min(int(request.args.get("limit", 100)), 500)
    skip = int(request.args.get("skip", 0))
    docs = svc.list_supplier_documents(supplier_id=supplier_id, document_type=document_type, limit=limit, skip=skip)
    return _jsonify_safe({"documents": _json_compatible(docs)})


@internal.route("/api/supplier/compliance/upload", methods=["POST"])
def api_supplier_compliance_upload():
    """Upload a compliance (or other) document for this supplier."""
    if not _allow_token_role("supplier"):
        return _jsonify_safe({"error": "Forbidden"}, 403)
    api_user = getattr(g, "inventory_api_user", None)
    supplier_id = api_user.get("supplier_id") if api_user else None
    if not supplier_id and getattr(current_user, "is_authenticated", False):
        supplier_id = request.form.get("supplier_id", type=int)
    if not supplier_id:
        return _jsonify_safe({"error": "supplier_id required (set on token or form for session users)"}, 400)
    f = request.files.get("file")
    if not f or not f.filename:
        return _jsonify_safe({"error": "file required"}, 400)
    name = request.form.get("name") or f.filename
    document_type = (request.form.get("document_type") or "compliance").strip() or "compliance"
    ext = os.path.splitext(f.filename)[1] or ".bin"
    upload_dir = _supplier_docs_upload_dir()
    subdir = os.path.join(upload_dir, str(supplier_id))
    os.makedirs(subdir, exist_ok=True)
    path = os.path.join(subdir, f"{uuid.uuid4().hex}{ext}")
    f.save(path)
    token_id = api_user.get("id") if api_user else None
    svc = get_inventory_service()
    try:
        doc_id = svc.create_supplier_document(
            supplier_id=supplier_id,
            name=name,
            file_path=path,
            document_type=document_type,
            uploaded_by_token_id=token_id,
        )
        return _jsonify_safe({"id": doc_id, "name": name, "document_type": document_type}, 201)
    except Exception as e:
        return _jsonify_safe({"error": str(e)}, 400)


# ---------------------------------------------------------------------------
# API: CSV export (items, transactions, stock levels, batches)
# ---------------------------------------------------------------------------

def _csv_cell(value):
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "__float__") and hasattr(value, "as_integer_ratio"):
        return str(float(value))
    return str(value)


def _csv_response(rows, filename: str, fieldnames: list):
    if not fieldnames and rows:
        fieldnames = list(rows[0].keys()) if rows else []
    buf = StringIO()
    w = csv_module.writer(buf)
    w.writerow(fieldnames)
    for r in rows:
        row = [_csv_cell(r.get(f)) for f in fieldnames]
        w.writerow(row)
    from flask import Response
    resp = Response(buf.getvalue(), mimetype="text/csv")
    resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp


@internal.route("/api/export/items", methods=["GET"])
@login_required
def api_export_items():
    if not _permission_inventory():
        return _jsonify_safe({"error": "Forbidden"}, 403)
    fmt = (request.args.get("format") or "json").strip().lower()
    svc = get_inventory_service()
    limit = min(int(request.args.get("limit", 10000)), 50000)
    items = svc.list_items(limit=limit, skip=0)
    if fmt == "csv":
        fieldnames = ["id", "sku", "name", "barcode", "uom", "category", "standard_cost", "reorder_point", "is_active"]
        return _csv_response(items, "inventory_items.csv", fieldnames)
    return _jsonify_safe({"items": _json_compatible(items)})


@internal.route("/api/export/transactions", methods=["GET"])
@login_required
def api_export_transactions():
    if not _permission_inventory():
        return _jsonify_safe({"error": "Forbidden"}, 403)
    fmt = (request.args.get("format") or "json").strip().lower()
    svc = get_inventory_service()
    item_id = request.args.get("item_id", type=int)
    location_id = request.args.get("location_id", type=int)
    from_date = request.args.get("from_date")
    to_date = request.args.get("to_date")
    limit = min(int(request.args.get("limit", 10000)), 50000)
    tx = svc.list_transactions(item_id=item_id, location_id=location_id, from_date=from_date, to_date=to_date, limit=limit, skip=0)
    if fmt == "csv":
        fieldnames = ["id", "item_id", "location_id", "batch_id", "quantity", "transaction_type", "unit_cost", "weight", "weight_uom", "performed_at", "reference_type", "reference_id"]
        return _csv_response(tx, "inventory_transactions.csv", fieldnames)
    return _jsonify_safe({"transactions": _json_compatible(tx)})


@internal.route("/api/export/stock_levels", methods=["GET"])
@login_required
def api_export_stock_levels():
    if not _permission_inventory():
        return _jsonify_safe({"error": "Forbidden"}, 403)
    fmt = (request.args.get("format") or "json").strip().lower()
    svc = get_inventory_service()
    item_id = request.args.get("item_id", type=int)
    location_id = request.args.get("location_id", type=int)
    limit = min(int(request.args.get("limit", 10000)), 50000)
    levels = svc.list_stock_levels_all(item_id=item_id, location_id=location_id, limit=limit, skip=0)
    if fmt == "csv":
        fieldnames = ["id", "item_id", "location_id", "batch_id", "quantity_on_hand", "quantity_reserved", "quantity_available"]
        return _csv_response(levels, "inventory_stock_levels.csv", fieldnames)
    return _jsonify_safe({"stock_levels": _json_compatible(levels)})


@internal.route("/api/export/batches", methods=["GET"])
@login_required
def api_export_batches():
    if not _permission_inventory():
        return _jsonify_safe({"error": "Forbidden"}, 403)
    fmt = (request.args.get("format") or "json").strip().lower()
    svc = get_inventory_service()
    item_id = request.args.get("item_id", type=int)
    limit = min(int(request.args.get("limit", 10000)), 50000)
    batches = svc.list_batches(item_id=item_id, limit=limit)
    if fmt == "csv":
        fieldnames = ["id", "item_id", "batch_number", "lot_number", "quantity", "weight", "weight_uom", "unit_weight", "unit_weight_uom", "expiry_date", "created_at"]
        return _csv_response(batches, "inventory_batches.csv", fieldnames)
    return _jsonify_safe({"batches": _json_compatible(batches)})


@internal.route("/openapi.json")
@login_required
@admin_required
def openapi_spec():
    """Return OpenAPI 3 spec for this plugin's APIs."""
    from app.openapi_utils import generate_openapi_spec
    server_url = request.url_root.rstrip("/") + BASE_PATH
    spec = generate_openapi_spec(
        blueprint_name=BLUEPRINT_NAME,
        server_url=server_url,
        title="Inventory Control API",
        version="0.0.1",
    )
    return jsonify(spec)


def get_blueprints():
    from .legacy_asset_management_redirects import legacy_asset_management_bp

    return [internal, legacy_asset_management_bp]


def get_public_blueprint():
    from .contractor_portal import public_inventory_bp

    return public_inventory_bp


@internal.get("/contractor-kit-requests")
@login_required
def inventory_contractor_kit_requests_admin():
    if not has_permission(PERM_INV_ACCESS):
        flash("You do not have access to inventory.", "danger")
        return redirect(url_for("routes.dashboard"))
    from .contractor_portal import (
        KIT_STATUS_DECLINED,
        KIT_STATUS_FULFILLED,
        KIT_STATUS_PENDING,
    )

    rows = []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT k.id, k.contractor_id, k.need_from, k.need_until, k.request_text,
                   k.status, k.office_notes, k.created_at, k.resolved_at, k.resolved_by,
                   c.name AS contractor_name, c.email AS contractor_email
            FROM inventory_contractor_kit_requests k
            INNER JOIN tb_contractors c ON c.id = k.contractor_id
            ORDER BY (k.status = %s) DESC, k.created_at DESC
            LIMIT 250
            """,
            (KIT_STATUS_PENDING,),
        )
        rows = cur.fetchall() or []
    except Exception as e:
        logger.exception("inventory_contractor_kit_requests_admin: %s", e)
        flash(
            "Could not load requests. Run the inventory install/upgrade if the table is missing.",
            "danger",
        )
    finally:
        cur.close()
        conn.close()
    return render_template(
        "admin/contractor_kit_requests.html",
        rows=rows,
        status_pending=KIT_STATUS_PENDING,
        status_fulfilled=KIT_STATUS_FULFILLED,
        status_declined=KIT_STATUS_DECLINED,
    )


@internal.post("/contractor-kit-requests/<int:rid>/resolve")
@login_required
def inventory_contractor_kit_request_resolve(rid):
    if not has_permission(PERM_INV_ACCESS):
        flash("You do not have access to inventory.", "danger")
        return redirect(url_for("routes.dashboard"))
    from .contractor_portal import (
        KIT_STATUS_DECLINED,
        KIT_STATUS_FULFILLED,
        KIT_STATUS_PENDING,
    )

    status = (request.form.get("status") or "").strip()
    if status not in (KIT_STATUS_FULFILLED, KIT_STATUS_DECLINED):
        flash("Invalid status.", "danger")
        return redirect(url_for("inventory_control_internal.inventory_contractor_kit_requests_admin"))
    notes = (request.form.get("office_notes") or "").strip()[:8000] or None
    actor = getattr(current_user, "username", "") or ""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT status FROM inventory_contractor_kit_requests WHERE id = %s",
            (int(rid),),
        )
        row = cur.fetchone()
        if not row:
            flash("Request not found.", "warning")
            return redirect(url_for("inventory_control_internal.inventory_contractor_kit_requests_admin"))
        if (row[0] or "") != KIT_STATUS_PENDING:
            flash("That request is already closed.", "warning")
            return redirect(url_for("inventory_control_internal.inventory_contractor_kit_requests_admin"))
        cur.execute(
            """
            UPDATE inventory_contractor_kit_requests
            SET status = %s, office_notes = %s, resolved_at = NOW(), resolved_by = %s
            WHERE id = %s AND status = %s
            """,
            (status, notes, actor or None, int(rid), KIT_STATUS_PENDING),
        )
        conn.commit()
        if cur.rowcount:
            flash("Request updated.", "success")
        else:
            flash("Could not update request.", "warning")
    except Exception as e:
        conn.rollback()
        logger.exception("inventory_contractor_kit_request_resolve: %s", e)
        flash("Database error.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("inventory_control_internal.inventory_contractor_kit_requests_admin"))


# --- OpenAPI registration (for AI/docs integration) ---
def _register_openapi():
    for path, method, op_id, summary in [
        (BASE_PATH + "/api/health", "GET", "inventory_health", "Health check"),
        (BASE_PATH + "/api/dashboard", "GET", "inventory_dashboard", "Dashboard metrics"),
        (BASE_PATH + "/api/analytics/stock_levels", "GET", "inventory_analytics_stock_levels", "Analytics: stock levels report"),
        (BASE_PATH + "/api/analytics/movers", "GET", "inventory_analytics_movers", "Analytics: fast/slow movers"),
        (BASE_PATH + "/api/analytics/activity", "GET", "inventory_analytics_activity", "Analytics: transaction activity"),
        (BASE_PATH + "/api/analytics/suppliers", "GET", "inventory_analytics_suppliers", "Analytics: suppliers list"),
        (BASE_PATH + "/api/categories", "GET", "inventory_categories_list", "List categories"),
        (BASE_PATH + "/api/categories", "POST", "inventory_categories_create", "Create category"),
        (BASE_PATH + "/api/categories/{category_id}", "GET", "inventory_categories_get", "Get category"),
        (BASE_PATH + "/api/categories/{category_id}", "PUT", "inventory_categories_update", "Update category"),
        (BASE_PATH + "/api/categories/{category_id}", "DELETE", "inventory_categories_delete", "Delete category"),
        (BASE_PATH + "/api/items", "GET", "inventory_items_list", "List items"),
        (BASE_PATH + "/api/items", "POST", "inventory_items_create", "Create item"),
        (BASE_PATH + "/api/items/{item_id}", "GET", "inventory_items_get", "Get item"),
        (BASE_PATH + "/api/items/{item_id}", "PUT", "inventory_items_update", "Update item"),
        (BASE_PATH + "/api/items/{item_id}", "DELETE", "inventory_items_archive", "Archive item"),
        (BASE_PATH + "/api/locations", "GET", "inventory_locations_list", "List locations"),
        (BASE_PATH + "/api/locations", "POST", "inventory_locations_create", "Create location"),
        (BASE_PATH + "/api/locations/{location_id}", "GET", "inventory_locations_get", "Get location"),
        (BASE_PATH + "/api/batches", "GET", "inventory_batches_list", "List batches"),
        (BASE_PATH + "/api/batches", "POST", "inventory_batches_create", "Create batch"),
        (BASE_PATH + "/api/transactions", "GET", "inventory_transactions_list", "List transactions"),
        (BASE_PATH + "/api/transactions", "POST", "inventory_transactions_create", "Record transaction"),
        (BASE_PATH + "/api/transactions/{tx_id}/rollback", "POST", "inventory_transactions_rollback", "Rollback transaction"),
        (BASE_PATH + "/api/invoices/upload", "POST", "inventory_invoices_upload", "Upload invoice"),
        (BASE_PATH + "/api/invoices/{invoice_id}", "GET", "inventory_invoices_get", "Get invoice"),
        (BASE_PATH + "/api/invoices/{invoice_id}/apply", "POST", "inventory_invoices_apply", "Apply invoice to stock"),
        (BASE_PATH + "/api/mobile/scan/in", "POST", "inventory_mobile_scan_in", "Mobile scan in"),
        (BASE_PATH + "/api/mobile/scan/out", "POST", "inventory_mobile_scan_out", "Mobile scan out"),
        (BASE_PATH + "/api/mobile/scan/adjust", "POST", "inventory_mobile_scan_adjust", "Mobile adjust"),
        (BASE_PATH + "/api/mobile/items/search", "GET", "inventory_mobile_items_search", "Mobile item search"),
        (BASE_PATH + "/api/mobile/items/{item_id}/stock", "GET", "inventory_mobile_items_stock", "Mobile item stock"),
        (BASE_PATH + "/api/mobile/bulk/actions", "POST", "inventory_mobile_bulk_actions", "Mobile bulk actions"),
        (BASE_PATH + "/api/repack", "POST", "inventory_repack", "Repack/split batch"),
        (BASE_PATH + "/api/picking/suggest", "GET", "inventory_picking_suggest", "FEFO picking suggestions"),
        (BASE_PATH + "/api/tokens", "POST", "inventory_tokens_create", "Create API token"),
        (BASE_PATH + "/api/tokens", "GET", "inventory_tokens_list", "List API tokens"),
        (BASE_PATH + "/api/tokens/{token_id}", "DELETE", "inventory_tokens_revoke", "Revoke API token"),
        (BASE_PATH + "/api/supplier/items", "GET", "inventory_supplier_items", "Supplier: list items"),
        (BASE_PATH + "/api/supplier/invoices", "GET", "inventory_supplier_invoices", "Supplier: list invoices"),
        (BASE_PATH + "/api/supplier/receipts/confirm", "POST", "inventory_supplier_confirm_receipt", "Supplier: confirm receipt"),
        (BASE_PATH + "/api/supplier/batches", "GET", "inventory_supplier_batches", "Supplier: view supplied batches/lots"),
        (BASE_PATH + "/api/supplier/pos", "GET", "inventory_supplier_pos", "Supplier: view PO status"),
        (BASE_PATH + "/api/supplier/compliance", "GET", "inventory_supplier_compliance_list", "Supplier: list compliance docs"),
        (BASE_PATH + "/api/supplier/compliance/upload", "POST", "inventory_supplier_compliance_upload", "Supplier: upload compliance doc"),
        (BASE_PATH + "/api/export/items", "GET", "inventory_export_items", "Export items CSV/JSON"),
        (BASE_PATH + "/api/export/transactions", "GET", "inventory_export_transactions", "Export transactions CSV/JSON"),
        (BASE_PATH + "/api/export/stock_levels", "GET", "inventory_export_stock_levels", "Export stock levels CSV/JSON"),
        (BASE_PATH + "/api/export/batches", "GET", "inventory_export_batches", "Export batches CSV/JSON"),
    ]:
        register_path(BLUEPRINT_NAME, path, method, op_id, summary, tags=["inventory_control"])


_register_openapi()
