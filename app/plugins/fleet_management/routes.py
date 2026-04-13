# Fleet Management — admin UI and JSON API
from __future__ import annotations

import logging
import os
import uuid
from datetime import date

from flask import (
    Blueprint,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.utils import secure_filename
from flask_login import current_user, login_required

from app.plugins.inventory_control.objects import get_inventory_service

from .fleet_common import (
    PERM_ACCESS,
    PERM_EDIT,
    PERM_DRIVER,
    fleet_access_required,
    fleet_driver_required,
    fleet_edit_required,
    fleet_transact_required,
    fleet_safety_record_required,
    uid as _uid,
    can_access as _can_access,
    can_driver as _can_driver,
    can_edit as _can_edit,
    can_fleet_transactions as _can_fleet_transactions,
    can_record_safety_check as _can_record_safety_check,
)
from .fleet_csv_import import FLEET_IMPORT_FIELDS, import_fleet_rows, parse_csv_file
from .deployment_readiness import build_vehicle_deployment_readiness
from .objects import (
    FLEET_ISSUE_STAGES,
    fleet_validate_manual_mileage_trip,
    get_fleet_service,
    list_time_billing_role_names_for_picklist,
)
from . import uk_api_defaults as _uk_api_defaults
from .uk_vehicle_lookup import (
    fetch_mot_test_history_for_vehicle,
    format_mot_api_error_for_display,
    merge_uk_lookup,
    mot_error_should_open_circuit,
    mot_history_ui_suppressed_by_circuit,
    mot_oauth_configured,
    record_mot_infra_failure,
    record_mot_infra_success,
)

logger = logging.getLogger("fleet_management")

BASE_PATH = "/plugin/fleet_management"
fleet_bp = Blueprint(
    "fleet_management",
    __name__,
    url_prefix=BASE_PATH,
    template_folder="templates",
)


def _json_safe(obj):
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return obj


# ---------------------------------------------------------------------------
# HTML pages
# ---------------------------------------------------------------------------


@fleet_bp.route("/")
@fleet_bp.route("/index")
@login_required
@fleet_access_required
def dashboard():
    svc = get_fleet_service()
    compliance_sync = None
    notifications = []
    if _can_edit():
        try:
            compliance_sync = svc.sync_compliance_vehicle_status()
        except Exception:
            logger.exception("fleet compliance sync")
        try:
            notifications = svc.list_open_notifications()
        except Exception:
            notifications = []
    metrics = svc.dashboard_metrics()
    vehicles = svc.list_vehicles(limit=50)
    due = svc.compliance_due_list(days=30)
    return render_template(
        "admin/fleet_dashboard.html",
        metrics=metrics,
        vehicles=vehicles,
        compliance_due=due,
        compliance_sync=compliance_sync,
        fleet_notifications=notifications,
        can_edit=_can_edit(),
    )


@fleet_bp.route("/vehicles")
@login_required
@fleet_access_required
def vehicles_list():
    svc = get_fleet_service()
    q = (request.args.get("q") or "").strip() or None
    st = (request.args.get("status") or "").strip() or None
    rows = svc.list_vehicles(search=q, status=st, limit=300)
    return render_template("admin/fleet_vehicles.html", vehicles=rows, q=q or "", status=st or "")


def _fleet_import_dir() -> str:
    d = os.path.join(current_app.root_path, "static", "uploads", "fleet_import")
    os.makedirs(d, exist_ok=True)
    return d


def _first_nonempty_str(*candidates) -> str | None:
    for c in candidates:
        if c is None:
            continue
        s = str(c).strip()
        if s:
            return s
    return None


def _uk_lookup_config() -> dict:
    """MOT/DVLA settings: built-in defaults first, then app.config, then env."""
    c = current_app.config
    mot_client_id = _first_nonempty_str(
        getattr(_uk_api_defaults, "MOT_CLIENT_ID", None),
        c.get("FLEET_UK_MOT_CLIENT_ID"),
        os.environ.get("FLEET_UK_MOT_CLIENT_ID"),
    )
    mot_client_secret = _first_nonempty_str(
        getattr(_uk_api_defaults, "MOT_CLIENT_SECRET", None),
        c.get("FLEET_UK_MOT_CLIENT_SECRET"),
        os.environ.get("FLEET_UK_MOT_CLIENT_SECRET"),
    )
    mot_token_url = _first_nonempty_str(
        getattr(_uk_api_defaults, "MOT_TOKEN_URL", None),
        c.get("FLEET_UK_MOT_TOKEN_URL"),
        os.environ.get("FLEET_UK_MOT_TOKEN_URL"),
    )
    mot_scope = _first_nonempty_str(
        getattr(_uk_api_defaults, "MOT_SCOPE", None),
        c.get("FLEET_UK_MOT_SCOPE"),
        os.environ.get("FLEET_UK_MOT_SCOPE"),
    )
    mot_oauth_on = mot_oauth_configured(
        mot_client_id, mot_client_secret, mot_token_url, mot_scope
    )
    mot_base = _first_nonempty_str(
        getattr(_uk_api_defaults, "MOT_API_BASE_URL", None),
        c.get("FLEET_UK_MOT_API_BASE"),
        os.environ.get("FLEET_UK_MOT_API_BASE"),
    )
    if mot_base:
        mot_base_resolved = mot_base
    elif mot_oauth_on:
        mot_base_resolved = "https://tapi.dvsa.gov.uk"
    else:
        mot_base_resolved = "https://beta.check-mot.service.gov.uk"
    dvla_url = _first_nonempty_str(
        getattr(_uk_api_defaults, "DVLA_VEHICLE_ENQUIRY_URL", None),
        c.get("FLEET_UK_DVLA_API_URL"),
        os.environ.get("FLEET_UK_DVLA_API_URL"),
    ) or "https://driver-vehicle-licensing.api.gov.uk/vehicle-enquiry/v1/vehicles"
    return {
        "mot_api_key": _first_nonempty_str(
            getattr(_uk_api_defaults, "MOT_API_KEY", None),
            c.get("FLEET_UK_MOT_API_KEY"),
            os.environ.get("FLEET_UK_MOT_API_KEY"),
        ),
        "mot_base_url": mot_base_resolved,
        "mot_client_id": mot_client_id,
        "mot_client_secret": mot_client_secret,
        "mot_token_url": mot_token_url,
        "mot_scope": mot_scope,
        "dvla_api_key": _first_nonempty_str(
            getattr(_uk_api_defaults, "DVLA_API_KEY", None),
            c.get("FLEET_UK_DVLA_API_KEY"),
            os.environ.get("FLEET_UK_DVLA_API_KEY"),
        ),
        "dvla_api_url": dvla_url,
    }


def _uk_gov_vehicle_lookup_available() -> bool:
    """True if at least one of MOT / DVLA is configured (defaults, app.config, or env)."""
    cfg = _uk_lookup_config()
    if cfg.get("dvla_api_key"):
        return True
    mk = cfg.get("mot_api_key")
    if not mk:
        return False
    if mot_oauth_configured(
        cfg.get("mot_client_id"),
        cfg.get("mot_client_secret"),
        cfg.get("mot_token_url"),
        cfg.get("mot_scope"),
    ):
        return True
    if not (cfg.get("mot_client_id") or cfg.get("mot_client_secret")):
        return True
    return False


def _mot_api_configured(cfg: dict) -> bool:
    """True when DVSA MOT History API can be called (same rules as lookup MOT half)."""
    if not cfg.get("mot_api_key"):
        return False
    if mot_oauth_configured(
        cfg.get("mot_client_id"),
        cfg.get("mot_client_secret"),
        cfg.get("mot_token_url"),
        cfg.get("mot_scope"),
    ):
        return True
    if not (cfg.get("mot_client_id") or cfg.get("mot_client_secret")):
        return True
    return False


def _mot_official_history_tab_enabled(cfg: dict) -> bool:
    """
    Vehicle detail “Official MOT (DVSA)” tab: only when MOT is configured and not suppressed.

    Set FLEET_UK_MOT_HISTORY_UI=0 (or false/off) in env or app.config to hide the tab when
    the server cannot reach DVSA (DNS/firewall) so users are not shown repeated errors.
    After repeated DVSA infrastructure failures, the tab is also hidden automatically for a
    cooldown (see uk_vehicle_lookup MOT circuit); set FLEET_UK_MOT_CIRCUIT=0 to disable that.
    UK registration form lookup still uses merge_uk_lookup separately if configured.
    """
    if not _mot_api_configured(cfg):
        return False
    if mot_history_ui_suppressed_by_circuit():
        return False
    c = current_app.config
    raw = _first_nonempty_str(
        c.get("FLEET_UK_MOT_HISTORY_UI"),
        os.environ.get("FLEET_UK_MOT_HISTORY_UI"),
    )
    if raw is None:
        return True
    return str(raw).strip().lower() not in ("0", "false", "no", "off", "disabled", "hide")


@fleet_bp.route("/vehicles/import", methods=["GET"])
@login_required
@fleet_access_required
@fleet_edit_required
def fleet_import_csv():
    """Step 1: upload CSV. Step 2: column mapping (after POST upload)."""
    step = 1
    headers: list = []
    preview: list = []
    job = session.get("fleet_csv_import")
    if job and request.args.get("step") == "map":
        path = os.path.join(_fleet_import_dir(), job.get("file") or "")
        if os.path.isfile(path):
            with open(path, "rb") as f:
                raw = f.read()
            try:
                headers, rows = parse_csv_file(raw)
                preview = rows[:8]
                step = 2
            except Exception as e:
                logger.exception("fleet import parse")
                flash(f"Could not read CSV: {e}", "danger")
                session.pop("fleet_csv_import", None)
        else:
            session.pop("fleet_csv_import", None)
            flash("Upload session expired. Upload the file again.", "warning")

    return render_template(
        "admin/fleet_import_csv.html",
        step=step,
        fields=FLEET_IMPORT_FIELDS,
        headers=headers,
        preview_rows=preview,
        original_name=(job or {}).get("name", ""),
    )


@fleet_bp.route("/vehicles/import/upload", methods=["POST"])
@login_required
@fleet_access_required
@fleet_edit_required
def fleet_import_csv_upload():
    f = request.files.get("file")
    if not f or not f.filename:
        flash("Choose a CSV file.", "danger")
        return redirect(url_for("fleet_management.fleet_import_csv"))
    if not f.filename.lower().endswith(".csv"):
        flash("File must be a .csv export.", "danger")
        return redirect(url_for("fleet_management.fleet_import_csv"))
    raw = f.read()
    if len(raw) > 6 * 1024 * 1024:
        flash("File too large (max 6 MB).", "danger")
        return redirect(url_for("fleet_management.fleet_import_csv"))
    try:
        headers, rows = parse_csv_file(raw)
    except Exception as e:
        flash(f"Invalid CSV: {e}", "danger")
        return redirect(url_for("fleet_management.fleet_import_csv"))
    if not headers:
        flash("No column headers found.", "danger")
        return redirect(url_for("fleet_management.fleet_import_csv"))
    token = uuid.uuid4().hex + ".csv"
    path = os.path.join(_fleet_import_dir(), token)
    with open(path, "wb") as out:
        out.write(raw)
    session["fleet_csv_import"] = {"file": token, "name": f.filename}
    flash(
        f"Loaded {len(rows)} row(s). Map your columns to Sparrow fields, then run import.",
        "success",
    )
    return redirect(url_for("fleet_management.fleet_import_csv", step="map"))


@fleet_bp.route("/vehicles/import/commit", methods=["POST"])
@login_required
@fleet_access_required
@fleet_edit_required
def fleet_import_csv_commit():
    job = session.get("fleet_csv_import")
    if not job:
        flash("Nothing to import. Start from upload.", "danger")
        return redirect(url_for("fleet_management.fleet_import_csv"))
    path = os.path.join(_fleet_import_dir(), job.get("file") or "")
    if not os.path.isfile(path):
        session.pop("fleet_csv_import", None)
        flash("Import file missing. Upload again.", "danger")
        return redirect(url_for("fleet_management.fleet_import_csv"))
    with open(path, "rb") as f:
        raw = f.read()
    try:
        headers, rows = parse_csv_file(raw)
    except Exception as e:
        flash(f"Could not re-read CSV: {e}", "danger")
        return redirect(url_for("fleet_management.fleet_import_csv", step="map"))

    header_to_field: dict = {}
    for i, h in enumerate(headers):
        field = (request.form.get(f"map_{i}") or "").strip()
        if field:
            header_to_field[h] = field

    mapped_values = set(header_to_field.values())
    if "registration" not in mapped_values and "internal_code" not in mapped_values:
        flash(
            "Map at least one column to Registration and/or Unit name (internal code).",
            "danger",
        )
        return redirect(url_for("fleet_management.fleet_import_csv", step="map"))

    update_existing = request.form.get("update_existing") == "1"
    svc = get_fleet_service()
    summary = import_fleet_rows(
        rows,
        header_to_field,
        update_existing=update_existing,
        user_id=_uid(),
        create_vehicle=svc.create_vehicle,
        update_vehicle=svc.update_vehicle,
        get_vehicle_by_registration=svc.get_vehicle_by_registration,
        get_vehicle_by_internal_code=svc.get_vehicle_by_internal_code,
    )
    session.pop("fleet_csv_import", None)
    try:
        os.remove(path)
    except OSError:
        pass

    flash(
        f"Import finished: {summary['created']} created, {summary['updated']} updated, "
        f"{summary['skipped']} skipped.",
        "success" if not summary["errors"] else "warning",
    )
    for err in summary["errors"][:12]:
        flash(err, "warning")
    return redirect(url_for("fleet_management.vehicles_list"))


@fleet_bp.route("/api/uk-vehicle-lookup", methods=["POST"])
@login_required
def api_uk_vehicle_lookup():
    if not _can_access():
        return jsonify({"error": "Forbidden"}), 403
    if not _uk_gov_vehicle_lookup_available():
        return jsonify({"error": "UK GOV lookup is not configured"}), 503
    data = request.get_json(silent=True) or {}
    reg = (data.get("registration") or request.form.get("registration") or "").strip()
    if not reg:
        return jsonify({"error": "registration required"}), 400
    cfg = _uk_lookup_config()
    out = merge_uk_lookup(
        reg,
        mot_api_key=cfg["mot_api_key"],
        mot_base_url=cfg["mot_base_url"],
        mot_client_id=cfg.get("mot_client_id"),
        mot_client_secret=cfg.get("mot_client_secret"),
        mot_token_url=cfg.get("mot_token_url"),
        mot_scope=cfg.get("mot_scope"),
        dvla_api_key=cfg["dvla_api_key"],
        dvla_api_url=cfg["dvla_api_url"],
    )
    return jsonify(_json_safe(out))


@fleet_bp.route("/vehicles/new", methods=["GET", "POST"])
@login_required
@fleet_access_required
@fleet_edit_required
def vehicle_new():
    svc = get_fleet_service()
    vtypes = svc.list_vehicle_types(include_inactive=False)
    if request.method == "POST":
        try:
            vtid = (request.form.get("vehicle_type_id") or "").strip()
            vid = svc.create_vehicle(
                registration=(request.form.get("registration") or "").strip() or None,
                internal_code=(request.form.get("internal_code") or "").strip() or None,
                make=request.form.get("make"),
                model=request.form.get("model"),
                year=int(request.form.get("year")) if request.form.get("year") else None,
                fuel_type=request.form.get("fuel_type"),
                status=request.form.get("status") or "active",
                notes=request.form.get("notes"),
                mot_expiry=request.form.get("mot_expiry") or None,
                tax_expiry=request.form.get("tax_expiry") or None,
                insurance_expiry=request.form.get("insurance_expiry") or None,
                vin=request.form.get("vin"),
                vehicle_type_id=int(vtid) if vtid else None,
                last_service_date=request.form.get("last_service_date") or None,
                last_service_mileage=int(request.form.get("last_service_mileage"))
                if request.form.get("last_service_mileage")
                else None,
                next_service_due_date=request.form.get("next_service_due_date") or None,
                next_service_due_mileage=int(request.form.get("next_service_due_mileage"))
                if request.form.get("next_service_due_mileage")
                else None,
                servicing_notes=request.form.get("servicing_notes"),
                user_id=_uid(),
            )
            flash("Vehicle created.", "success")
            return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vid))
        except Exception as e:
            logger.exception("vehicle_new")
            flash(str(e), "danger")
    return render_template(
        "admin/fleet_vehicle_form.html",
        vehicle=None,
        mode="new",
        vehicle_types=vtypes,
        uk_gov_lookup_available=_uk_gov_vehicle_lookup_available(),
    )


@fleet_bp.route("/vehicles/<int:vehicle_id>", methods=["GET", "POST"])
@login_required
@fleet_access_required
def vehicle_detail(vehicle_id: int):
    svc = get_fleet_service()
    v = svc.get_vehicle(vehicle_id)
    if not v:
        flash("Vehicle not found.", "danger")
        return redirect(url_for("fleet_management.vehicles_list"))

    if request.method == "POST" and _can_edit():
        try:
            new_id_photo = None
            fimg = request.files.get("id_photo")
            if fimg and fimg.filename:
                ext = fimg.filename.rsplit(".", 1)[-1].lower() if "." in fimg.filename else ""
                if ext in {"png", "jpg", "jpeg", "webp"}:
                    _app_pkg = os.path.dirname(
                        os.path.dirname(os.path.abspath(__file__))
                    )
                    dest_dir = os.path.join(
                        _app_pkg, "static", "uploads", "fleet", "vehicles", str(vehicle_id)
                    )
                    os.makedirs(dest_dir, exist_ok=True)
                    fn = secure_filename(f"id_photo.{ext}")
                    fimg.save(os.path.join(dest_dir, fn))
                    new_id_photo = f"fleet/vehicles/{vehicle_id}/{fn}"

            ukwargs = dict(
                internal_code=request.form.get("internal_code"),
                registration=request.form.get("registration"),
                make=request.form.get("make"),
                model=request.form.get("model"),
                year=int(request.form.get("year")) if request.form.get("year") else None,
                fuel_type=request.form.get("fuel_type"),
                status=request.form.get("status"),
                notes=request.form.get("notes"),
                mot_expiry=request.form.get("mot_expiry") or None,
                tax_expiry=request.form.get("tax_expiry") or None,
                insurance_expiry=request.form.get("insurance_expiry") or None,
                vin=request.form.get("vin"),
                vehicle_type_id=request.form.get("vehicle_type_id"),
                last_service_date=request.form.get("last_service_date"),
                last_service_mileage=request.form.get("last_service_mileage"),
                next_service_due_date=request.form.get("next_service_due_date"),
                next_service_due_mileage=request.form.get("next_service_due_mileage"),
                servicing_notes=request.form.get("servicing_notes"),
                user_id=_uid(),
            )
            if new_id_photo is not None:
                ukwargs["id_photo_path"] = new_id_photo
            svc.update_vehicle(vehicle_id, **ukwargs)
            flash("Saved.", "success")
            return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))
        except Exception as e:
            logger.exception("vehicle_detail")
            flash(str(e), "danger")

    mileage = svc.list_mileage(vehicle_id)
    maintenance = svc.list_maintenance(vehicle_id)
    drivers = svc.list_driver_assignments(vehicle_id)
    equipment = svc.list_equipment_on_vehicle(vehicle_id)
    vdi_logs = svc.list_vdi_submissions(vehicle_id, limit=50)
    fleet_issues = svc.list_issues(vehicle_id=vehicle_id, open_only=False, limit=80)
    history = svc.list_vehicle_history_timeline(vehicle_id, limit=60)
    mileage_last_known_mi = svc.get_last_known_odometer_mi(vehicle_id)
    vtypes = svc.list_vehicle_types(include_inactive=True)
    safety_logs = svc.list_safety_checks(vehicle_id, limit=50)
    safety_schema = svc.get_safety_schema_for_vehicle(vehicle_id)
    interval = int(v.get("type_safety_interval_days") or 42)
    equipment_activity = svc.list_vehicle_equipment_activity(vehicle_id, limit=80)
    equipment_consumable_alerts: dict = {}
    try:
        aids = [int(a["id"]) for a in equipment]
        if aids:
            equipment_consumable_alerts = get_inventory_service().consumable_alert_summary_for_assets(
                aids, near_days=30
            )
    except Exception:
        logger.exception("vehicle_detail consumable alerts")
    inventory_locations = []
    inventory_items_select = []
    if _can_edit() or _can_fleet_transactions():
        try:
            inventory_locations = get_inventory_service().list_locations()
        except Exception:
            logger.exception("fleet vehicle_detail list_locations")
        try:
            inventory_items_select = get_inventory_service().list_items(
                limit=500, is_active=True, is_equipment=False
            )
        except Exception:
            logger.exception("fleet vehicle_detail list_items")
    installed_parts = svc.list_installed_parts(vehicle_id)
    deployment_readiness = build_vehicle_deployment_readiness(
        vehicle=v,
        equipment_rows=equipment,
        equipment_consumable_alerts=equipment_consumable_alerts,
        mileage_last_known_mi=mileage_last_known_mi,
        safety_logs=safety_logs,
        safety_interval_days=interval,
        vehicle_type_safety_schema=safety_schema,
    )
    uk_cfg = _uk_lookup_config()
    mot_history_tests: list = []
    mot_history_error: str | None = None
    mot_history_error_display: str | None = None
    mot_history_dvsa: dict | None = None
    mot_history_enabled = _mot_official_history_tab_enabled(uk_cfg)
    mot_history_no_registration = not (v.get("registration") or "").strip()
    mot_history_gov_uk_fallback = False
    if mot_history_enabled and not mot_history_no_registration:
        try:
            mh = fetch_mot_test_history_for_vehicle(
                v["registration"],
                api_key=uk_cfg["mot_api_key"],
                base_url=uk_cfg["mot_base_url"],
                mot_client_id=uk_cfg.get("mot_client_id"),
                mot_client_secret=uk_cfg.get("mot_client_secret"),
                mot_token_url=uk_cfg.get("mot_token_url"),
                mot_scope=uk_cfg.get("mot_scope"),
            )
            if mh.get("error"):
                if mot_error_should_open_circuit(mh):
                    record_mot_infra_failure()
                    mot_history_error = None
                    mot_history_error_display = None
                    mot_history_enabled = False
                    mot_history_gov_uk_fallback = True
                    logger.warning(
                        "vehicle_detail MOT history suppressed (infra error): %s",
                        str(mh.get("error"))[:200],
                    )
                else:
                    mot_history_error = str(mh.get("error"))
                    if mh.get("detail"):
                        mot_history_error = f"{mot_history_error}: {mh['detail']}"
                    logger.warning(
                        "vehicle_detail MOT history: %s",
                        mot_history_error[:500],
                    )
                    mot_history_error_display = format_mot_api_error_for_display(
                        mot_history_error
                    )
            else:
                record_mot_infra_success()
                mot_history_tests = mh.get("tests") or []
                mot_history_dvsa = {
                    "make": mh.get("dvsa_make"),
                    "model": mh.get("dvsa_model"),
                }
        except Exception:
            logger.exception("vehicle_detail mot history")
            record_mot_infra_failure()
            mot_history_enabled = False
            mot_history_error = None
            mot_history_error_display = None
            mot_history_gov_uk_fallback = True
    elif (
        not mot_history_enabled
        and (v.get("registration") or "").strip()
        and _mot_api_configured(uk_cfg)
    ):
        mot_history_gov_uk_fallback = True
    return render_template(
        "admin/fleet_vehicle_detail.html",
        vehicle=v,
        mileage_logs=mileage,
        mileage_last_known_mi=mileage_last_known_mi,
        maintenance_logs=maintenance,
        driver_assignments=drivers,
        equipment_on_board=equipment,
        equipment_consumable_alerts=equipment_consumable_alerts,
        equipment_activity=equipment_activity,
        inventory_locations=inventory_locations,
        inventory_items_select=inventory_items_select,
        installed_parts=installed_parts,
        vdi_logs=vdi_logs,
        fleet_issues=fleet_issues,
        vehicle_history=history,
        vehicle_types=vtypes,
        safety_logs=safety_logs,
        safety_schema=safety_schema,
        safety_form_choices=svc.list_safety_form_choices_for_vehicle(vehicle_id),
        deployment_readiness=deployment_readiness,
        can_edit=_can_edit(),
        can_fleet_transactions=_can_fleet_transactions(),
        can_driver=_can_driver(),
        can_record_safety_check=_can_record_safety_check(),
        now_date=date.today().isoformat()[:10],
        uk_gov_lookup_available=_uk_gov_vehicle_lookup_available(),
        mot_history_enabled=mot_history_enabled,
        mot_history_no_registration=mot_history_no_registration,
        mot_history_tests=mot_history_tests,
        mot_history_error=mot_history_error,
        mot_history_error_display=mot_history_error_display or mot_history_error,
        mot_history_dvsa=mot_history_dvsa,
        mot_history_gov_uk_fallback=mot_history_gov_uk_fallback,
    )


@fleet_bp.route(
    "/vehicles/<int:vehicle_id>/equipment/<int:asset_id>/return",
    methods=["POST"],
)
@login_required
@fleet_access_required
@fleet_transact_required
def vehicle_return_equipment(vehicle_id: int, asset_id: int):
    """Return a serialised asset from this vehicle into an inventory location."""
    try:
        location_id = int(request.form.get("location_id") or 0)
        if not location_id:
            flash("Choose a storeroom / return location.", "danger")
            return redirect(
                url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id)
            )
        inv = get_inventory_service()
        asset = inv.get_equipment_asset(int(asset_id))
        if not asset:
            flash("Equipment asset not found.", "danger")
            return redirect(
                url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id)
            )
        on_board = {int(r["id"]) for r in get_fleet_service().list_equipment_on_vehicle(vehicle_id)}
        if int(asset_id) not in on_board:
            flash("That asset is not currently on this vehicle.", "danger")
            return redirect(
                url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id)
            )
        item_id = int(asset["item_id"])
        inv.record_transaction(
            item_id=item_id,
            location_id=location_id,
            quantity=1.0,
            transaction_type="return",
            performed_by_user_id=_uid(),
            equipment_asset_id=int(asset_id),
            metadata={"from_vehicle_id": str(int(vehicle_id))},
        )
        inv.set_equipment_asset_status(int(asset_id), "in_stock")
        flash("Equipment returned to storeroom.", "success")
    except Exception as e:
        logger.exception("vehicle_return_equipment")
        flash(str(e), "danger")
    return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))


@fleet_bp.route("/vehicles/<int:vehicle_id>/parts", methods=["POST"])
@login_required
@fleet_access_required
@fleet_transact_required
def vehicle_add_installed_part(vehicle_id: int):
    """Log a part fitted to the vehicle; optionally deduct stock from inventory."""
    svc = get_fleet_service()
    v = svc.get_vehicle(vehicle_id)
    if not v:
        flash("Vehicle not found.", "danger")
        return redirect(url_for("fleet_management.vehicles_list"))
    try:
        installed_date = (request.form.get("installed_date") or "").strip()
        if not installed_date:
            flash("Installed date is required.", "danger")
            return redirect(
                url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id)
            )
        item_id_raw = (request.form.get("inventory_item_id") or "").strip()
        item_id = int(item_id_raw) if item_id_raw else None
        qty = float(request.form.get("quantity") or 1) or 1.0
        part_number = (request.form.get("part_number") or "").strip() or None
        part_description = (request.form.get("part_description") or "").strip() or None
        odo_raw = (request.form.get("odometer_at_install") or "").strip()
        try:
            odometer_at_install = int(odo_raw) if odo_raw else None
        except ValueError:
            odometer_at_install = None
        warranty_end = (request.form.get("warranty_expires_date") or "").strip() or None
        warranty_terms = (request.form.get("warranty_terms") or "").strip() or None
        invoice_ref = (request.form.get("invoice_reference") or "").strip() or None
        notes = (request.form.get("notes") or "").strip() or None

        deduct = request.form.get("deduct_stock") == "1" and item_id
        loc_id = int(request.form.get("location_id") or 0) if deduct else None
        if deduct and not loc_id:
            flash("Choose a storeroom to deduct stock from.", "danger")
            return redirect(
                url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id)
            )

        svc.install_part_from_workshop(
            vehicle_id,
            installed_date=installed_date,
            inventory_item_id=item_id,
            quantity=qty,
            deduct_stock=bool(deduct),
            stock_location_id=loc_id,
            part_number=part_number,
            part_description=part_description,
            odometer_at_install=odometer_at_install,
            warranty_expires_date=warranty_end,
            warranty_terms=warranty_terms,
            invoice_reference=invoice_ref,
            notes=notes,
            created_by=_uid(),
            performed_by_user_id=_uid(),
        )
        flash("Part / consumable recorded.", "success")
    except Exception as e:
        logger.exception("vehicle_add_installed_part")
        flash(str(e), "danger")
    return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))


@fleet_bp.route(
    "/vehicles/<int:vehicle_id>/parts/<int:part_id>/delete",
    methods=["POST"],
)
@login_required
@fleet_access_required
@fleet_transact_required
def vehicle_delete_installed_part(vehicle_id: int, part_id: int):
    svc = get_fleet_service()
    if svc.delete_installed_part(part_id, vehicle_id):
        flash("Install record removed.", "success")
    else:
        flash("Record not found.", "warning")
    return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))


@fleet_bp.route("/vehicles/<int:vehicle_id>/mileage", methods=["POST"])
@login_required
@fleet_access_required
@fleet_driver_required
def vehicle_add_mileage(vehicle_id: int):
    svc = get_fleet_service()
    try:
        start_mi = int(request.form.get("start_mileage") or 0)
        end_mi = int(request.form.get("end_mileage") or 0)
        ref = svc.get_last_known_odometer_mi(vehicle_id)
        err = fleet_validate_manual_mileage_trip(start_mi, end_mi, ref)
        override = (
            _can_edit()
            and (request.form.get("mileage_sanity_override") or "").strip() == "1"
        )
        note = (request.form.get("mileage_override_note") or "").strip() or None
        if err and not override:
            flash(err, "danger")
            return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))
        if err and override and not note:
            flash(
                "Please add a short reason in the override note when saving an unusual mileage entry.",
                "danger",
            )
            return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))
        svc.add_mileage_log(
            vehicle_id=vehicle_id,
            driver_user_id=_uid(),
            start_mileage=start_mi,
            end_mileage=end_mi,
            purpose=request.form.get("purpose"),
            created_by=_uid(),
            sanity_check_override=bool(override and err),
            sanity_override_note=note if override else None,
        )
        flash("Mileage log added.", "success")
    except Exception as e:
        flash(str(e), "danger")
    return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))


@fleet_bp.route(
    "/vehicles/<int:vehicle_id>/mileage/<int:log_id>/delete", methods=["POST"]
)
@login_required
@fleet_access_required
@fleet_edit_required
def vehicle_delete_mileage(vehicle_id: int, log_id: int):
    svc = get_fleet_service()
    try:
        if svc.delete_mileage_log(
            log_id=log_id, vehicle_id=vehicle_id, user_id=_uid()
        ):
            flash("Mileage log removed.", "success")
        else:
            flash("Log not found.", "warning")
    except Exception as e:
        flash(str(e), "danger")
    return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))


@fleet_bp.route("/vehicles/<int:vehicle_id>/maintenance", methods=["POST"])
@login_required
@fleet_access_required
@fleet_edit_required
def vehicle_add_maintenance(vehicle_id: int):
    svc = get_fleet_service()
    try:
        cost_raw = request.form.get("cost")
        cost = float(cost_raw) if cost_raw not in (None, "") else None
        svc.add_maintenance(
            vehicle_id=vehicle_id,
            service_date=request.form.get("service_date") or "",
            service_type=request.form.get("service_type") or "Service",
            provider=request.form.get("provider"),
            cost=cost,
            odometer_at_service=int(request.form.get("odometer_at_service"))
            if request.form.get("odometer_at_service")
            else None,
            notes=request.form.get("notes"),
            created_by=_uid(),
        )
        flash("Maintenance recorded.", "success")
    except Exception as e:
        flash(str(e), "danger")
    return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))


@fleet_bp.route("/vehicles/<int:vehicle_id>/drivers", methods=["POST"])
@login_required
@fleet_access_required
@fleet_edit_required
def vehicle_add_driver(vehicle_id: int):
    svc = get_fleet_service()
    try:
        svc.add_driver_assignment(
            vehicle_id=vehicle_id,
            user_id=(request.form.get("user_id") or "").strip(),
            assignment_role=request.form.get("assignment_role") or "primary",
            effective_from=request.form.get("effective_from") or None,
            effective_to=request.form.get("effective_to") or None,
            notes=request.form.get("notes"),
            created_by=_uid(),
        )
        flash("Driver assignment added.", "success")
    except Exception as e:
        flash(str(e), "danger")
    return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))


@fleet_bp.route("/driver")
@login_required
@fleet_access_required
def driver_portal():
    return redirect(url_for("fleet_vdi.vdi_home"))


@fleet_bp.route("/vehicle-types")
@login_required
@fleet_access_required
@fleet_edit_required
def vehicle_types_list():
    svc = get_fleet_service()
    rows = svc.list_vehicle_types(include_inactive=True)
    return render_template("admin/fleet_vehicle_types.html", types=rows)


@fleet_bp.route("/vehicle-types/new", methods=["GET", "POST"])
@login_required
@fleet_access_required
@fleet_edit_required
def vehicle_type_new():
    svc = get_fleet_service()
    if request.method == "POST":
        try:
            tid = svc.create_vehicle_type(
                name=request.form.get("name") or "New type",
                service_interval_days=int(request.form.get("service_interval_days"))
                if request.form.get("service_interval_days")
                else None,
                service_interval_miles=int(request.form.get("service_interval_miles"))
                if request.form.get("service_interval_miles")
                else None,
                safety_check_interval_days=int(
                    request.form.get("safety_check_interval_days") or 42
                ),
                sort_order=int(request.form.get("sort_order") or 0),
                active=request.form.get("active") == "1",
                user_id=_uid(),
            )
            flash("Vehicle type created.", "success")
            return redirect(
                url_for("fleet_management.vehicle_type_edit", type_id=tid)
            )
        except Exception as e:
            logger.exception("vehicle_type_new")
            flash(str(e), "danger")
    return render_template("admin/fleet_vehicle_type_form.html", vt=None, mode="new")


@fleet_bp.route("/vehicle-types/<int:type_id>/edit", methods=["GET", "POST"])
@login_required
@fleet_access_required
@fleet_edit_required
def vehicle_type_edit(type_id: int):
    svc = get_fleet_service()
    vt = svc.get_vehicle_type(type_id)
    if not vt:
        flash("Type not found.", "danger")
        return redirect(url_for("fleet_management.vehicle_types_list"))
    if request.method == "POST":
        try:
            svc.update_vehicle_type(
                type_id,
                name=request.form.get("name"),
                service_interval_days=request.form.get("service_interval_days"),
                service_interval_miles=request.form.get("service_interval_miles"),
                safety_check_interval_days=request.form.get(
                    "safety_check_interval_days"
                ),
                sort_order=request.form.get("sort_order"),
                active=request.form.get("active") == "1",
                user_id=_uid(),
            )
            flash("Saved.", "success")
            return redirect(
                url_for("fleet_management.vehicle_type_edit", type_id=type_id)
            )
        except Exception as e:
            logger.exception("vehicle_type_edit")
            flash(str(e), "danger")
    return render_template(
        "admin/fleet_vehicle_type_form.html", vt=vt, mode="edit"
    )


@fleet_bp.route("/vehicle-types/<int:type_id>/safety-schema", methods=["GET", "POST"])
@login_required
@fleet_access_required
@fleet_edit_required
def vehicle_type_safety_schema(type_id: int):
    svc = get_fleet_service()
    vt = svc.get_vehicle_type(type_id)
    if not vt:
        flash("Type not found.", "danger")
        return redirect(url_for("fleet_management.vehicle_types_list"))
    from app.plugins.fleet_management.safety_schema_default import (
        DEFAULT_SAFETY_SCHEMA,
    )
    from app.plugins.fleet_management.safety_schema_build import (
        merge_vehicle_type_safety_schema_from_post,
    )

    if request.method == "POST":
        action = (request.form.get("builder_action") or "save").strip().lower()
        if action == "restore_default":
            try:
                svc.update_vehicle_type(
                    type_id,
                    safety_schema=dict(DEFAULT_SAFETY_SCHEMA),
                    user_id=_uid(),
                )
                flash("Safety check form restored to default template.", "success")
            except Exception as e:
                flash(str(e), "danger")
            return redirect(
                url_for("fleet_management.vehicle_type_safety_schema", type_id=type_id)
            )
        try:
            schema = merge_vehicle_type_safety_schema_from_post(request.form)
            prev = vt.get("safety_schema") or {}
            for _k in ("required_equipment_skus",):
                if _k in prev and prev[_k] is not None:
                    schema[_k] = prev[_k]
            svc.update_vehicle_type(
                type_id, safety_schema=schema, user_id=_uid()
            )
            flash("Workshop safety form saved.", "success")
        except ValueError as e:
            flash(str(e), "danger")
        except Exception as e:
            flash(str(e), "danger")
        return redirect(
            url_for("fleet_management.vehicle_type_safety_schema", type_id=type_id)
        )

    schema = vt.get("safety_schema") or dict(DEFAULT_SAFETY_SCHEMA)
    return render_template(
        "admin/fleet_safety_schema.html",
        schema=schema,
        type_id=type_id,
        type_name=vt.get("name") or "",
        tb_role_names=list_time_billing_role_names_for_picklist(),
    )


@fleet_bp.route("/workshop-due")
@login_required
@fleet_access_required
def workshop_due():
    svc = get_fleet_service()
    days = int(request.args.get("days") or 30)
    servicing = svc.servicing_due_rows(days=days)
    safety = svc.safety_due_rows(days=days)
    return render_template(
        "admin/fleet_workshop_due.html",
        servicing_due=servicing,
        safety_due=safety,
        days=days,
        can_edit=_can_edit(),
        can_record_safety_check=_can_record_safety_check(),
    )


@fleet_bp.route(
    "/vehicles/<int:vehicle_id>/safety-check",
    methods=["GET", "POST"],
)
@login_required
@fleet_access_required
@fleet_safety_record_required
def vehicle_safety_check(vehicle_id: int):
    svc = get_fleet_service()
    v = svc.get_vehicle(vehicle_id)
    if not v:
        flash("Vehicle not found.", "danger")
        return redirect(url_for("fleet_management.vehicles_list"))
    from app.plugins.fleet_management.safety_schema_build import PRIMARY_FORM_KEY

    form_key = (request.args.get("form") or request.form.get("check_form_key") or PRIMARY_FORM_KEY).strip()
    schema = svc.get_safety_check_form_for_vehicle(vehicle_id, form_key)
    if not schema:
        flash("That checklist is not configured for this vehicle’s type.", "danger")
        return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))
    from app.plugins.fleet_management.vdi_submit_logic import (
        parse_vdi_form,
        save_fleet_safety_photos_after_submit,
    )

    if request.method == "POST":
        responses, mileage_val, err = parse_vdi_form(request, schema)
        if err:
            flash(err, "danger")
        else:
            try:
                perf = (request.form.get("performed_at") or "").strip() or date.today().isoformat()
                extra_mi = (request.form.get("mileage_at_check") or "").strip()
                mile_store = int(extra_mi) if extra_mi else mileage_val
                cid = svc.add_safety_check(
                    vehicle_id=vehicle_id,
                    performed_by_user_id=_uid(),
                    performed_at=perf[:10],
                    mileage_at_check=mile_store,
                    responses=responses,
                    photo_paths=[],
                    summary_notes=request.form.get("summary_notes"),
                    user_id=_uid(),
                    check_form_key=form_key,
                )
                paths = save_fleet_safety_photos_after_submit(
                    request, schema, cid, vehicle_id
                )
                if paths:
                    svc.update_safety_check_photos(cid, paths)
                flash("Safety check recorded.", "success")
                return redirect(
                    url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id)
                )
            except Exception as e:
                logger.exception("vehicle_safety_check")
                flash(str(e), "danger")
    return render_template(
        "admin/fleet_safety_check_record.html",
        vehicle=v,
        schema=schema,
        safety_form_key=form_key,
        now_date=date.today().isoformat()[:10],
    )


@fleet_bp.route("/forms", methods=["GET", "POST"])
@fleet_bp.route("/vdi-schema", methods=["GET", "POST"])
@login_required
@fleet_access_required
@fleet_edit_required
def vdi_schema_editor():
    svc = get_fleet_service()
    from app.plugins.fleet_management.vdi_schema_build import merge_vdi_schema_from_post
    from app.plugins.fleet_management.vdi_schema_default import DEFAULT_VDI_SCHEMA

    if request.method == "POST":
        action = (request.form.get("builder_action") or "save").strip().lower()
        if action == "restore_default":
            try:
                svc.save_vdi_schema(dict(DEFAULT_VDI_SCHEMA), user_id=_uid())
                flash("Forms restored to the default daily inspection template.", "success")
            except Exception as e:
                flash(str(e), "danger")
            return redirect(url_for("fleet_management.vdi_schema_editor"))
        try:
            schema = merge_vdi_schema_from_post(request.form)
            svc.save_vdi_schema(schema, user_id=_uid())
            flash("VDI and contractor portal forms saved.", "success")
        except ValueError as e:
            flash(str(e), "danger")
        except Exception as e:
            flash(str(e), "danger")
        return redirect(url_for("fleet_management.vdi_schema_editor"))

    schema = svc.get_vdi_schema()
    vehicle_types = svc.list_vehicle_types(include_inactive=True)
    return render_template(
        "admin/fleet_vdi_schema.html",
        schema=schema,
        tb_role_names=list_time_billing_role_names_for_picklist(),
        vehicle_types=vehicle_types,
    )


@fleet_bp.route("/issues/board", methods=["GET", "POST"])
@login_required
@fleet_access_required
@fleet_edit_required
def issues_board():
    svc = get_fleet_service()
    if request.method == "POST":
        iid = int(request.form.get("issue_id") or 0)
        action = (request.form.get("action") or "").strip().lower()
        try:
            if action == "move":
                stage = (request.form.get("kanban_stage") or "").strip()
                if stage in FLEET_ISSUE_STAGES:
                    if stage == "done":
                        svc.update_fleet_issue(
                            iid,
                            kanban_stage="done",
                            complete=True,
                            resolution_summary=request.form.get("resolution_summary")
                            or "Closed from board",
                            user_id=_uid(),
                        )
                    else:
                        svc.update_fleet_issue(
                            iid,
                            kanban_stage=stage,
                            user_id=_uid(),
                        )
            elif action == "notes":
                svc.update_fleet_issue(
                    iid,
                    manager_notes=request.form.get("manager_notes"),
                    scheduled_service_date=request.form.get("scheduled_service_date") or None,
                    user_id=_uid(),
                )
            elif action == "vor":
                svc.update_fleet_issue(
                    iid,
                    vehicle_marked_vor=True,
                    user_id=_uid(),
                )
            elif action == "workshop":
                svc.update_fleet_issue(
                    iid,
                    mark_vehicle_maintenance=True,
                    user_id=_uid(),
                )
            elif action == "pending_rt":
                svc.update_fleet_issue(
                    iid,
                    kanban_stage="pending_road_test",
                    mark_vehicle_pending_road_test=True,
                    user_id=_uid(),
                )
            elif action == "active":
                svc.update_fleet_issue(
                    iid,
                    mark_vehicle_active=True,
                    user_id=_uid(),
                )
            elif action == "complete":
                svc.update_fleet_issue(
                    iid,
                    kanban_stage="done",
                    complete=True,
                    resolution_summary=request.form.get("resolution_summary") or "",
                    user_id=_uid(),
                )
            flash("Updated.", "success")
        except Exception as e:
            logger.exception("issues_board")
            flash(str(e), "danger")
        return redirect(url_for("fleet_management.issues_board"))

    issues = svc.list_issues(open_only=False, limit=400)
    vehicle_regs: dict = {}
    for it in issues:
        vid = int(it.get("vehicle_id") or 0)
        if vid and vid not in vehicle_regs:
            vv = svc.get_vehicle(vid)
            vehicle_regs[vid] = (
                (vv or {}).get("registration")
                or (vv or {}).get("internal_code")
                or f"#{vid}"
            )
    by_stage = {s: [] for s in FLEET_ISSUE_STAGES}
    for it in issues:
        if it.get("completed_at"):
            by_stage["done"].append(it)
            continue
        st = (it.get("kanban_stage") or "reported").strip().lower()
        if st not in by_stage:
            st = "reported"
        by_stage[st].append(it)
    return render_template(
        "admin/fleet_issues_board.html",
        stages=FLEET_ISSUE_STAGES,
        issues_by_stage=by_stage,
        vehicle_regs=vehicle_regs,
    )


@fleet_bp.route("/crew-issue", methods=["POST"])
@login_required
@fleet_access_required
def crew_report_issue():
    svc = get_fleet_service()
    uid_s = _uid() or ""
    try:
        vid = int(request.form.get("vehicle_id") or 0)
        if not vid:
            raise ValueError("Select a vehicle")
        svc.add_fleet_issue(
            vehicle_id=vid,
            actor_type="user",
            actor_id=uid_s,
            title=(request.form.get("title") or "Issue reported").strip(),
            description=request.form.get("description"),
            user_id=uid_s,
        )
        flash("Issue reported to fleet.", "success")
    except Exception as e:
        flash(str(e), "danger")
    return redirect(request.referrer or url_for("fleet_vdi.vdi_home"))


@fleet_bp.route("/notifications/<int:notif_id>/dismiss", methods=["POST"])
@login_required
@fleet_access_required
@fleet_edit_required
def dismiss_fleet_notification(notif_id: int):
    svc = get_fleet_service()
    try:
        svc.dismiss_fleet_notification(notif_id)
        flash("Dismissed.", "info")
    except Exception as e:
        flash(str(e), "danger")
    return redirect(request.referrer or url_for("fleet_management.dashboard"))


@fleet_bp.route("/vehicles/<int:vehicle_id>/fleet-issue", methods=["POST"])
@login_required
@fleet_access_required
def vehicle_add_fleet_issue(vehicle_id: int):
    if not (_can_edit() or _can_driver()):
        flash("You need fleet driver or editor permission to log issues.", "danger")
        return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))
    svc = get_fleet_service()
    uid_s = _uid() or ""
    try:
        iid = svc.add_fleet_issue(
            vehicle_id=vehicle_id,
            actor_type="user",
            actor_id=uid_s,
            title=request.form.get("title") or "Issue",
            description=request.form.get("description"),
            user_id=uid_s,
        )
        if _can_edit() and request.form.get("quick_close") == "1":
            summary = (request.form.get("resolution_summary") or "").strip()
            svc.update_fleet_issue(
                int(iid),
                kanban_stage="done",
                complete=True,
                resolution_summary=summary or "Closed when logged (no further action).",
                user_id=uid_s,
            )
            flash("Issue logged and marked done.", "success")
        else:
            flash("Issue logged.", "success")
    except Exception as e:
        flash(str(e), "danger")
    return redirect(url_for("fleet_management.vehicle_detail", vehicle_id=vehicle_id))


# ---------------------------------------------------------------------------
# JSON API
# ---------------------------------------------------------------------------


@fleet_bp.route("/api/vehicles", methods=["GET"])
@login_required
def api_vehicles_list():
    if not _can_access():
        return jsonify({"error": "Forbidden"}), 403
    svc = get_fleet_service()
    q = (request.args.get("q") or "").strip() or None
    st = (request.args.get("status") or "").strip() or None
    rows = svc.list_vehicles(search=q, status=st, limit=500)
    return jsonify({"vehicles": _json_safe(rows)})


@fleet_bp.route("/api/vehicles/<int:vehicle_id>", methods=["GET"])
@login_required
def api_vehicle_get(vehicle_id: int):
    if not _can_access():
        return jsonify({"error": "Forbidden"}), 403
    svc = get_fleet_service()
    v = svc.get_vehicle(vehicle_id)
    if not v:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"vehicle": _json_safe(v)})


@fleet_bp.route("/api/vehicles", methods=["POST"])
@login_required
def api_vehicle_create():
    if not _can_edit():
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json() or {}
    svc = get_fleet_service()
    try:
        vid = svc.create_vehicle(
            registration=(data.get("registration") or "").strip() or None,
            make=data.get("make"),
            model=data.get("model"),
            year=data.get("year"),
            fuel_type=data.get("fuel_type"),
            status=data.get("status") or "active",
            notes=data.get("notes"),
            internal_code=(data.get("internal_code") or "").strip() or None,
            mot_expiry=data.get("mot_expiry"),
            tax_expiry=data.get("tax_expiry"),
            insurance_expiry=data.get("insurance_expiry"),
            vin=data.get("vin"),
            user_id=_uid(),
        )
        return jsonify({"id": vid, "vehicle": _json_safe(svc.get_vehicle(vid))})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@fleet_bp.route("/api/dashboard", methods=["GET"])
@login_required
def api_dashboard():
    if not _can_access():
        return jsonify({"error": "Forbidden"}), 403
    svc = get_fleet_service()
    return jsonify(
        {
            "metrics": _json_safe(svc.dashboard_metrics()),
            "compliance_due": _json_safe(svc.compliance_due_list(days=30)),
            "servicing_due": _json_safe(svc.servicing_due_rows(days=30)),
            "safety_due": _json_safe(svc.safety_due_rows(days=30)),
        }
    )


@fleet_bp.route("/api/vehicles/<int:vehicle_id>/equipment", methods=["GET"])
@login_required
def api_vehicle_equipment(vehicle_id: int):
    if not _can_access():
        return jsonify({"error": "Forbidden"}), 403
    svc = get_fleet_service()
    return jsonify({"equipment": _json_safe(svc.list_equipment_on_vehicle(vehicle_id))})


def get_blueprints():
    from .vdi_routes import fleet_vdi_bp

    return [fleet_bp, fleet_vdi_bp]


def get_public_blueprint():
    from .fleet_public import public_fleet_bp

    return public_fleet_bp
