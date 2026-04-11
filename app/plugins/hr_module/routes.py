import csv
import os
import uuid
from datetime import datetime
from functools import wraps
from io import StringIO
from itertools import zip_longest
from typing import Optional

from flask import Blueprint, abort, current_app, flash, make_response, redirect, render_template, request, send_file, session, url_for
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename
from app.objects import PluginManager
from app.portal_session import contractor_id_from_tb_user
from . import services as hr_services
from .dbs_update_client import (
    get_dbs_mass_check_interval_days_from_manifest,
    is_dbs_update_service_enabled,
    run_dbs_status_check,
    scheduled_check_interval_label,
    set_dbs_mass_check_interval_days_in_manifest,
)
from .appraisals_reminders import get_reminder_days_from_manifest
from .appraisals_service import (
    appraisal_attention_counts,
    appraisal_event_log_ready,
    appraisal_tables_ready,
    create_appraisal,
    delete_appraisal,
    get_appraisal,
    human_appraisal_status,
    list_all_appraisal_events,
    list_appraisal_events,
    list_appraisals,
    list_appraisals_needing_attention,
    set_appraisal_attachment,
    update_appraisal,
)
from .hcpc_register_check import (
    get_hcpc_mass_check_interval_days_from_manifest,
    hcpc_scheduled_check_interval_label,
    is_hcpc_register_api_enabled,
    list_hcpc_register_check_logs,
    run_hcpc_register_status_check,
    run_mass_hcpc_register_checks,
    set_hcpc_mass_check_interval_days_in_manifest,
)
from .employee_csv_import import (
    HR_EMPLOYEE_IMPORT_FIELDS,
    import_hr_employee_rows,
    parse_csv_file as hr_parse_employee_csv,
)

_plugin_manager = PluginManager(os.path.abspath("app/plugins"))
_core_manifest = _plugin_manager.get_core_manifest() or {}


def _get_website_settings():
    """Return website_settings for templates (website_public_base.html). From website_module or safe default."""
    try:
        from app.plugins.website_module.routes import get_website_settings
        return get_website_settings()
    except Exception:
        pass
    _keys = (
        "favicon_path", "default_og_image", "schema_json", "cookie_bar_colors", "cookie_bar_text",
        "cookie_bar_accept_text", "cookie_bar_decline_text", "cookie_policy", "analytics_code",
        "facebook_url", "instagram_url", "linkedin_url", "twitter_url", "youtube_url", "tiktok_url",
        "pinterest_url", "whatsapp_url", "threads_url", "reddit_url", "snapchat_url", "telegram_url",
        "discord_url", "tumblr_url", "github_url", "medium_url", "vimeo_url", "dribbble_url",
        "behance_url", "soundcloud_url", "slack_url", "mastodon_url",
    )
    return {k: None for k in _keys}


def _staff_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("tb_user"):
            return redirect("/employee-portal/login?next=" + request.path)
        return view(*args, **kwargs)
    return wrapped


def _contractor_id():
    return contractor_id_from_tb_user(session.get("tb_user"))


def _ventus_plugin_available() -> bool:
    """True when Ventus CAD has manifest.json and is enabled (blueprints registered — avoids HR links to 404)."""
    try:
        vd = os.path.join(_plugin_manager.plugins_dir,
                          "ventus_response_module")
        if not os.path.isdir(vd) or not os.path.exists(os.path.join(vd, "manifest.json")):
            return False
        pl = _plugin_manager.load_plugins() or {}
        m = pl.get("ventus_response_module") or {}
        return bool(m.get("enabled", False))
    except Exception:
        return False


_template = os.path.join(os.path.dirname(__file__), "templates")
internal_bp = Blueprint("internal_hr", __name__,
                        url_prefix="/plugin/hr_module", template_folder=_template)
public_bp = Blueprint("public_hr", __name__,
                      url_prefix="/hr", template_folder=_template)


def _register_hr_jinja_filters(state):
    """Human-readable labels for HR templates (snake_case -> UI text)."""
    from . import services as _svc

    env = state.app.jinja_env
    env.filters.setdefault("hr_request_type", _svc.human_request_type)
    env.filters.setdefault("hr_doc_request_status",
                           _svc.human_document_request_status)
    env.filters.setdefault("hr_upload_doc_type",
                           _svc.human_upload_document_type)
    env.filters.setdefault("hr_employee_doc_category",
                           _svc.human_employee_document_category)
    env.filters.setdefault("hr_expiry_doc_type", _svc.human_expiry_doc_type)
    env.filters.setdefault("hr_contractor_status",
                           _svc.human_contractor_status)
    env.filters.setdefault("hr_profile_field", _svc.human_profile_field)
    env.filters.setdefault("hr_safe_profile_picture",
                           _svc.hr_safe_profile_picture_path)
    env.filters.setdefault("hr_hpac_register_status",
                           _svc.human_hpac_register_status)
    env.filters.setdefault("hr_hcpc_register_status",
                           _svc.human_hcpc_register_status)
    env.filters.setdefault("hr_generic_register_status",
                           _svc.human_generic_register_status)
    from .appraisals_service import human_appraisal_status as _appr_st
    from .appraisals_service import human_appraisal_event_type as _appr_ev

    env.filters.setdefault("hr_appraisal_status", _appr_st)
    env.filters.setdefault("hr_appraisal_event_type", _appr_ev)


internal_bp.record(_register_hr_jinja_filters)
public_bp.record(_register_hr_jinja_filters)


def _app_static_dir() -> str:
    """
    …/app/static — resolved from this plugin path (always matches Flask app package layout).
    Do not use current_app.root_path alone: it can disagree with where we save uploads.
    """
    app_pkg_dir = os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", ".."))
    return os.path.join(app_pkg_dir, "static")


def _project_root_dir() -> str:
    """Repo root (parent of app/). Legacy uploads may exist in <root>/static/..."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _website_module_plugin_static_dir() -> str:
    """Some deployments stored HR uploads under website_module/static (legacy)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "website_module", "static"))


def _hr_upload_static_roots():
    """Roots where HR uploads may exist (app static, legacy repo static, website_module)."""
    return [
        os.path.abspath(_app_static_dir()),
        os.path.abspath(os.path.join(_project_root_dir(), "static")),
        os.path.abspath(_website_module_plugin_static_dir()),
    ]


def _normalize_hr_relative_upload_path(rel: str):
    """Normalize DB/static-relative path; reject traversal. Returns None if invalid."""
    rel = (rel or "").replace("\\", "/").strip().lstrip("/")
    if not rel or ".." in rel.split("/"):
        return None
    if rel.startswith("static/"):
        rel = rel[7:].lstrip("/")
    return rel


def _resolve_file_under_hr_roots(rel: str) -> Optional[str]:
    """Resolve rel (forward slashes, no leading slash) to first existing file under HR roots."""
    segs = [s for s in rel.split("/") if s]
    if not segs:
        return None
    for root in _hr_upload_static_roots():
        candidate = os.path.abspath(os.path.join(root, *segs))
        try:
            if os.path.commonpath([candidate, root]) != root:
                continue
        except ValueError:
            continue
        if os.path.isfile(candidate):
            return candidate
    return None


def _resolve_hr_request_upload_path(rel: str):
    """
    Resolve DB file_path (e.g. uploads/hr_documents/name.pdf) to an existing file.
    Tries: app/static, repo-root/static, website_module/static (legacy).
    """
    rel = _normalize_hr_relative_upload_path(rel)
    if not rel or not rel.startswith("uploads/hr_documents/"):
        return None
    return _resolve_file_under_hr_roots(rel)


def _resolve_hr_staff_profile_linked_path(rel: str, contractor_id: int) -> Optional[str]:
    """
    Paths stored on hr_staff_details / profile_picture_path: hr_documents (any contractor)
    or hr_employee/<this contractor id>/ only.
    """
    rel = _normalize_hr_relative_upload_path(rel)
    if not rel:
        return None
    cid = str(int(contractor_id))
    if rel.startswith("uploads/hr_documents/"):
        return _resolve_file_under_hr_roots(rel)
    if rel.startswith("uploads/hr_employee/"):
        parts = rel.split("/")
        if len(parts) < 4 or parts[0] != "uploads" or parts[1] != "hr_employee" or parts[2] != cid:
            return None
        return _resolve_file_under_hr_roots(rel)
    return None


# Whitelist: query param -> key on admin_get_staff_profile row
_HR_PROFILE_LINKED_FILE_FIELDS = {
    "driving_licence": "driving_licence_document_path",
    "right_to_work": "right_to_work_document_path",
    "dbs": "dbs_document_path",
    "contract": "contract_document_path",
    "profile_picture": "profile_picture_path",
}


# Granular HR permissions (manifest declared_permissions + alternate_access_permissions).
HR_ACCESS = "hr_module.access"
HR_READ = "hr_module.read"
HR_EDIT = "hr_module.edit_employees"
HR_REQ = "hr_module.document_requests"
HR_LIB = "hr_module.library"
HR_AUDIT_LOG = "hr_module.view_staff_activity_log"
_HR_ANY_VIEW = frozenset({HR_READ, HR_EDIT, HR_REQ, HR_LIB})


def _hr_perm_set(user):
    role = (getattr(user, "role", None) or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return None
    raw = getattr(user, "permissions", None) or []
    return {str(x) for x in raw if x}


def _hr_may_view(user) -> bool:
    ps = _hr_perm_set(user)
    if ps is None:
        return True
    if HR_ACCESS in ps:
        return True
    return bool(ps & _HR_ANY_VIEW)


def _hr_may(user, *needed: str) -> bool:
    ps = _hr_perm_set(user)
    if ps is None:
        return True
    if HR_ACCESS in ps:
        return True
    return any(n in ps for n in needed)


def _hr_require_view(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for("routes.login"))
        if not _hr_may_view(current_user):
            flash("You do not have access to HR.", "danger")
            return redirect(url_for("routes.dashboard"))
        return view(*args, **kwargs)

    return wrapped


def _hr_require(*needed: str):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for("routes.login"))
            if not _hr_may(current_user, *needed):
                flash("You do not have permission for this action.", "danger")
                return redirect(url_for("routes.dashboard"))
            return view(*args, **kwargs)

        return wrapped

    return decorator


@internal_bp.context_processor
def _hr_admin_perm_context():
    u = current_user
    if not getattr(u, "is_authenticated", False):
        return {
            "hr_can_view": False,
            "hr_can_edit_employees": False,
            "hr_can_document_requests": False,
            "hr_can_library": False,
            "hr_can_configure_onboarding_packs": False,
            "hr_can_view_staff_activity_log": False,
        }
    return {
        "hr_can_view": _hr_may_view(u),
        "hr_can_edit_employees": _hr_may(u, HR_EDIT),
        "hr_can_document_requests": _hr_may(u, HR_REQ),
        "hr_can_library": _hr_may(u, HR_LIB),
        "hr_can_configure_onboarding_packs": _hr_may(u, HR_EDIT, HR_REQ),
        "hr_can_view_staff_activity_log": _hr_may(u, HR_AUDIT_LOG),
    }


def _core_user_id_for_db():
    """Core `users.id` is CHAR(36) UUID — store as string, never int()."""
    uid = getattr(current_user, "id", None)
    if uid is None:
        return None
    return str(uid)


@internal_bp.get("/")
@login_required
@_hr_require_view
def admin_index():
    hr_services.ensure_hr_shell_rows_for_all_contractors()
    overview = hr_services.hr_compliance_overview()
    upcoming_birthdays = hr_services.admin_upcoming_birthdays(60)
    try:
        expiring = hr_services.get_expiring_documents(30)
    except Exception:
        expiring = []
    expired_compliance_count = int(overview.get(
        "contractors_with_expired_compliance") or 0)
    return render_template(
        "hr_module/admin/index.html",
        module_name="HR",
        module_description="Employee store: profiles, compliance fields, scanned documents, and requests.",
        plugin_system_name="hr_module",
        overview=overview,
        upcoming_birthdays=upcoming_birthdays,
        expiring_count=len(expiring),
        expired_compliance_count=expired_compliance_count,
        ventus_plugin_available=_ventus_plugin_available(),
        dbs_update_service_enabled=is_dbs_update_service_enabled(),
        dbs_mass_interval_setting=get_dbs_mass_check_interval_days_from_manifest(),
        hcpc_register_api_enabled=is_hcpc_register_api_enabled(),
        hcpc_mass_interval_setting=get_hcpc_mass_check_interval_days_from_manifest(),
        appraisal_attention=(
            appraisal_attention_counts() if appraisal_tables_ready() else None
        ),
        config=_core_manifest,
    )


@internal_bp.post("/admin/dbs-mass-interval")
@login_required
@_hr_require(HR_EDIT)
def admin_hr_dbs_mass_interval_save():
    """Persist mass CRSC cadence to hr_module manifest (same field as plugin settings)."""
    raw = (request.form.get("interval_days") or "0").strip()
    try:
        days = int(raw)
    except ValueError:
        flash("Choose a valid interval.", "error")
        return redirect(url_for("internal_hr.admin_index"))
    days = max(0, min(days, 90))
    ok, err = set_dbs_mass_check_interval_days_in_manifest(days)
    if ok:
        flash("DBS mass check interval saved.", "success")
    else:
        flash(err or "Could not save interval.", "error")
    return redirect(url_for("internal_hr.admin_index"))


@internal_bp.post("/admin/hcpc-mass-interval")
@login_required
@_hr_require(HR_EDIT)
def admin_hr_hcpc_mass_interval_save():
    raw = (request.form.get("interval_days") or "0").strip()
    try:
        days = int(raw)
    except ValueError:
        flash("Choose a valid interval.", "error")
        return redirect(url_for("internal_hr.admin_index"))
    days = max(0, min(days, 90))
    ok, err = set_hcpc_mass_check_interval_days_in_manifest(days)
    if ok:
        flash("HCPC mass check interval saved.", "success")
    else:
        flash(err or "Could not save interval.", "error")
    return redirect(url_for("internal_hr.admin_index"))


@internal_bp.post("/admin/hcpc-mass-check")
@login_required
@_hr_require(HR_EDIT)
def admin_hr_hcpc_mass_check_now():
    if not is_hcpc_register_api_enabled():
        flash(
            "HCPC HTTP checks are turned off on this server (HCPC_REGISTER_API_ENABLED=0/false/no/off).",
            "warning",
        )
        return redirect(url_for("internal_hr.admin_index"))
    label = (getattr(current_user, "username", None) or "").strip() or "HR admin"
    summary = run_mass_hcpc_register_checks(
        channel="manual_mass",
        checker_user_id=_core_user_id_for_db(),
        checker_label=label,
    )
    flash(
        "HCPC mass check complete. "
        f"Processed {summary.get('processed', 0)} of {summary.get('total', 0)} "
        f"(verified {summary.get('verified_on_register', 0)}, "
        f"not found {summary.get('not_found', 0)}, "
        f"no longer listed {summary.get('no_longer_listed', 0)}, "
        f"uncertain {summary.get('uncertain', 0)}, "
        f"failed {summary.get('failed', 0)}).",
        "success",
    )
    return redirect(url_for("internal_hr.admin_index"))


# -----------------------------------------------------------------------------
# Admin: Employee directory
# -----------------------------------------------------------------------------


@internal_bp.get("/employees")
@login_required
@_hr_require_view
def admin_employees():
    hr_services.ensure_hr_shell_rows_for_all_contractors()
    q = (request.args.get("q") or "").strip() or None
    status = (request.args.get("status") or "active").strip() or "active"
    view_mode = (request.args.get("view") or "kanban").strip().lower()
    if view_mode not in ("kanban", "list"):
        view_mode = "kanban"
    if status.lower() == "all":
        status = None
    employees = hr_services.admin_list_employees(
        q=q, status=status, limit=400, offset=0)
    compliance = (request.args.get("compliance") or "all").strip().lower()
    if compliance not in ("all", "expired"):
        compliance = "all"
    eids = [int(e["id"]) for e in employees if e.get("id") is not None]
    expired_ids = hr_services.contractor_ids_with_expired_hr_dates(eids)
    for e in employees:
        e["hr_has_expired_compliance"] = int(e["id"]) in expired_ids
    if compliance == "expired":
        employees = [e for e in employees if e.get(
            "hr_has_expired_compliance")]
    return render_template(
        "hr_module/admin/employees.html",
        employees=employees,
        q=q or "",
        status_filter=(request.args.get("status") or "active"),
        view_mode=view_mode,
        compliance_filter=compliance,
        config=_core_manifest,
    )


@internal_bp.route("/employees/new", methods=["GET", "POST"])
@login_required
@_hr_require(HR_EDIT)
def admin_employee_new():
    """Add someone who already works for you but has no Sparrow contractor record yet."""
    roles = hr_services.admin_list_time_billing_roles_for_select()
    if request.method == "POST":
        rid_raw = (request.form.get("tb_role_id") or "").strip()
        role_id = int(rid_raw) if rid_raw.isdigit() else None
        if not role_id:
            flash(
                "Select a staff role. Add roles under Time Billing → Staff roles (or HR) if the list is empty.",
                "error",
            )
            return render_template(
                "hr_module/admin/employee_new.html",
                roles=roles,
                config=_core_manifest,
            )
        ok, msg, cid = hr_services.admin_create_contractor_employee(
            request.form.get("name") or "",
            request.form.get("email") or "",
            role_id=role_id,
            role_name=None,
            password=(request.form.get("password") or "").strip(),
            phone=request.form.get("phone"),
            status=request.form.get("status") or "active",
            employment_type=request.form.get("employment_type"),
        )
        if ok and cid:
            prof = hr_services.admin_get_staff_profile(int(cid))
            un = (prof or {}).get("username") or "—"
            flash(
                f"Employee created — portal login username: {un}. Complete their HR profile or send document requests as needed.",
                "success",
            )
            return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))
        flash(msg or "Could not create employee.", "error")
    return render_template(
        "hr_module/admin/employee_new.html",
        roles=roles,
        config=_core_manifest,
    )


@internal_bp.route("/admin/pay-roles", methods=["GET", "POST"])
@login_required
@_hr_require(HR_EDIT)
def admin_pay_roles():
    """Create, rename, or remove staff roles (shared ``roles`` table; same as Time Billing → Staff roles)."""
    if request.method == "POST":
        if request.form.get("_csrf_token") and request.form.get("_csrf_token") != session.get("_csrf"):
            pass
        action = (request.form.get("action") or "").strip().lower()
        if action == "create":
            nm = (request.form.get("name") or "").strip()
            ok, msg = hr_services.admin_tb_pay_role_create(nm)
            flash("Staff role added." if ok else (msg or "Could not add role."), "success" if ok else "error")
        elif action == "rename":
            try:
                rid = int((request.form.get("role_id") or "").strip())
            except ValueError:
                flash("Invalid role.", "error")
            else:
                nm = (request.form.get("name") or "").strip()
                ok, msg = hr_services.admin_tb_pay_role_rename(rid, nm)
                flash("Role updated." if ok else (msg or "Could not update role."), "success" if ok else "error")
        elif action == "delete":
            try:
                rid = int((request.form.get("role_id") or "").strip())
            except ValueError:
                flash("Invalid role.", "error")
            else:
                ok, msg = hr_services.admin_tb_pay_role_delete(rid)
                flash("Role deleted." if ok else (msg or "Could not delete role."), "success" if ok else "error")
        else:
            flash("Unknown action.", "error")
        return redirect(url_for("internal_hr.admin_pay_roles"))

    roles = hr_services.admin_tb_pay_roles_with_usage()
    return render_template(
        "hr_module/admin/pay_roles.html",
        roles=roles,
        config=_core_manifest,
    )


def _hr_employee_import_dir() -> str:
    d = os.path.join(current_app.root_path, "static",
                     "uploads", "hr_employee_import")
    os.makedirs(d, exist_ok=True)
    return d


@internal_bp.get("/employees/import")
@login_required
@_hr_require(HR_EDIT)
def admin_employee_import_csv():
    """Upload CSV, then map columns to Sparrow employee fields (same pattern as fleet import)."""
    step = 1
    if (request.args.get("step") or "").strip().lower() == "map":
        step = 2
    job = session.get("hr_employee_csv_import")
    headers: list = []
    preview: list = []
    if step == 2 and job:
        path = os.path.join(_hr_employee_import_dir(), job.get("file") or "")
        if os.path.isfile(path):
            try:
                with open(path, "rb") as f:
                    raw = f.read()
                headers, rows = hr_parse_employee_csv(raw)
                preview = rows[:8]
            except Exception:
                session.pop("hr_employee_csv_import", None)
                flash("Could not read the uploaded CSV. Upload again.", "danger")
                return redirect(url_for("internal_hr.admin_employee_import_csv"))
        else:
            session.pop("hr_employee_csv_import", None)
            flash("Import file missing. Start again.", "danger")
            return redirect(url_for("internal_hr.admin_employee_import_csv"))
    return render_template(
        "hr_module/admin/employee_import_csv.html",
        step=step,
        headers=headers,
        fields=HR_EMPLOYEE_IMPORT_FIELDS,
        preview_rows=preview,
        original_name=(job or {}).get("name", ""),
        config=_core_manifest,
    )


@internal_bp.post("/employees/import/upload")
@login_required
@_hr_require(HR_EDIT)
def admin_employee_import_csv_upload():
    f = request.files.get("file")
    if not f or not f.filename:
        flash("Choose a CSV file.", "danger")
        return redirect(url_for("internal_hr.admin_employee_import_csv"))
    if not f.filename.lower().endswith(".csv"):
        flash("File must be a .csv export.", "danger")
        return redirect(url_for("internal_hr.admin_employee_import_csv"))
    raw = f.read()
    if len(raw) > 6 * 1024 * 1024:
        flash("File too large (max 6 MB).", "danger")
        return redirect(url_for("internal_hr.admin_employee_import_csv"))
    try:
        headers, rows = hr_parse_employee_csv(raw)
    except Exception as e:
        flash(f"Invalid CSV: {e}", "danger")
        return redirect(url_for("internal_hr.admin_employee_import_csv"))
    if not headers:
        flash("No column headers found.", "danger")
        return redirect(url_for("internal_hr.admin_employee_import_csv"))
    token = uuid.uuid4().hex + ".csv"
    path = os.path.join(_hr_employee_import_dir(), token)
    with open(path, "wb") as out:
        out.write(raw)
    session["hr_employee_csv_import"] = {"file": token, "name": f.filename}
    flash(
        f"Loaded {len(rows)} row(s). Map columns to Sparrow fields, then run import.",
        "success",
    )
    return redirect(url_for("internal_hr.admin_employee_import_csv", step="map"))


@internal_bp.post("/employees/import/commit")
@login_required
@_hr_require(HR_EDIT)
def admin_employee_import_csv_commit():
    job = session.get("hr_employee_csv_import")
    if not job:
        flash("Nothing to import. Upload a CSV first.", "danger")
        return redirect(url_for("internal_hr.admin_employee_import_csv"))
    path = os.path.join(_hr_employee_import_dir(), job.get("file") or "")
    if not os.path.isfile(path):
        session.pop("hr_employee_csv_import", None)
        flash("Import file missing. Upload again.", "danger")
        return redirect(url_for("internal_hr.admin_employee_import_csv"))
    with open(path, "rb") as f:
        raw = f.read()
    try:
        headers, rows = hr_parse_employee_csv(raw)
    except Exception as e:
        flash(f"Could not re-read CSV: {e}", "danger")
        return redirect(url_for("internal_hr.admin_employee_import_csv", step="map"))

    header_to_field: dict = {}
    for i, h in enumerate(headers):
        field = (request.form.get(f"map_{i}") or "").strip()
        if field:
            header_to_field[h] = field

    mapped = set(header_to_field.values())
    if "email" not in mapped:
        flash("You must map one column to Email (required).", "danger")
        return redirect(url_for("internal_hr.admin_employee_import_csv", step="map"))

    update_existing = request.form.get("update_existing") == "1"
    summary = import_hr_employee_rows(
        rows,
        header_to_field,
        update_existing=update_existing,
    )
    session.pop("hr_employee_csv_import", None)
    try:
        os.remove(path)
    except OSError:
        pass

    emailed = int(summary.get("credentials_emailed") or 0)
    flash(
        f"Import finished: {summary['created']} created, {summary['updated']} updated, "
        f"{summary['skipped']} skipped. Welcome emails with temporary passwords sent: {emailed}.",
        "success" if not summary["errors"] and not summary.get(
            "email_warnings") else "warning",
    )
    for err in summary["errors"][:15]:
        flash(err, "warning")
    for warn in (summary.get("email_warnings") or [])[:15]:
        flash(warn, "warning")
    return redirect(url_for("internal_hr.admin_employees"))


# -----------------------------------------------------------------------------
# Admin: Contractor search and profile
# -----------------------------------------------------------------------------


@internal_bp.get("/contractors")
@login_required
@_hr_require_view
def admin_contractors():
    q = (request.args.get("q") or "").strip()
    contractors = hr_services.admin_search_contractors(
        q, limit=50) if q else []
    return render_template(
        "hr_module/admin/contractors.html",
        q=q,
        contractors=contractors,
        config=_core_manifest,
    )


@internal_bp.get("/contractors/<int:cid>")
@login_required
@_hr_require_view
def admin_contractor_profile(cid):
    hr_services.ensure_hr_shell_rows_for_all_contractors()
    hr_services.reconcile_staff_details_from_approved_requests(cid)
    profile = hr_services.admin_get_staff_profile(cid)
    if not profile:
        flash("Contractor not found.", "error")
        return redirect(url_for("internal_hr.admin_contractors"))
    requests_list, _ = hr_services.admin_list_document_requests(
        contractor_id=cid, limit=100, offset=0)
    employee_documents = hr_services.list_employee_documents(cid)
    compliance_gaps = hr_services.admin_profile_compliance_gaps(
        profile, employee_documents)
    compliance_expired_items = hr_services.profile_expired_compliance_items(
        profile)
    expired_compliance_doc_types = {x["doc_type"]
                                    for x in compliance_expired_items}
    open_doc_request_button_state = hr_services.contractor_open_request_button_state_by_type(
        cid)
    focus_cat = (request.args.get("focus_cat") or "").strip().lower() or None
    if focus_cat and focus_cat not in hr_services.EMPLOYEE_DOCUMENT_CATEGORIES:
        focus_cat = None
    direct_reports = hr_services.admin_list_direct_reports(cid)
    onboarding_packs = hr_services.hr_onboarding_pack_choices()
    staff_admin_audit_logs = []
    staff_audit_retention_days = 90
    if _hr_may(current_user, HR_AUDIT_LOG):
        try:
            from app.admin_staff_audit import fetch_logs_for_contractor, retention_days

            staff_audit_retention_days = retention_days(current_app)
            staff_admin_audit_logs = fetch_logs_for_contractor(cid)
        except Exception:
            pass
    training_person_competencies = []
    training_competencies_table_ready = False
    training_competency_today_iso = None
    training_course_assignments = []
    training_courses_ready = False
    try:
        from datetime import date as _date

        from app.plugins.training_module.services import TrainingService as _TrnSvc

        if _TrnSvc.person_competencies_table_exists():
            training_competencies_table_ready = True
            training_person_competencies = _TrnSvc.list_person_competencies(cid)
            training_competency_today_iso = _date.today().isoformat()
        if _TrnSvc.training_assignments_table_ready():
            training_courses_ready = True
            training_course_assignments = _TrnSvc.list_assignments(
                cid, include_completed=True
            )
    except Exception:
        pass
    ventus_crew_profile = hr_services.admin_ventus_crew_profile_for_contractor(cid)
    training_admin_assignments_url = None
    try:
        training_admin_assignments_url = url_for(
            "internal_training.admin_assignments", contractor_id=cid
        )
    except Exception:
        pass
    dbs_logs = hr_services.list_dbs_status_check_logs(cid, limit=40)
    hcpc_logs = list_hcpc_register_check_logs(cid, limit=40)
    contractor_appraisals = (
        list_appraisals(contractor_id=cid, limit=25)
        if appraisal_tables_ready()
        else []
    )
    return render_template(
        "hr_module/admin/contractor_profile.html",
        profile=profile,
        requests_list=requests_list,
        employee_documents=employee_documents,
        doc_categories=hr_services.EMPLOYEE_DOCUMENT_CATEGORIES,
        compliance_gaps=compliance_gaps,
        compliance_expired_items=compliance_expired_items,
        expired_compliance_doc_types=expired_compliance_doc_types,
        open_doc_request_button_state=open_doc_request_button_state,
        focus_cat=focus_cat,
        direct_reports=direct_reports,
        onboarding_packs=onboarding_packs,
        staff_admin_audit_logs=staff_admin_audit_logs,
        staff_audit_retention_days=staff_audit_retention_days,
        training_person_competencies=training_person_competencies,
        training_competencies_table_ready=training_competencies_table_ready,
        training_competency_today_iso=training_competency_today_iso,
        training_course_assignments=training_course_assignments,
        training_courses_ready=training_courses_ready,
        ventus_crew_profile=ventus_crew_profile,
        ventus_plugin_available=_ventus_plugin_available(),
        training_admin_assignments_url=training_admin_assignments_url,
        dbs_update_service_enabled=is_dbs_update_service_enabled(),
        dbs_check_logs=dbs_logs,
        dbs_schedule_interval=scheduled_check_interval_label(),
        hcpc_register_api_enabled=is_hcpc_register_api_enabled(),
        hcpc_check_logs=hcpc_logs,
        hcpc_schedule_interval=hcpc_scheduled_check_interval_label(),
        appraisal_tables_ready=appraisal_tables_ready(),
        contractor_appraisals=contractor_appraisals,
        config=_core_manifest,
    )


@internal_bp.post("/contractors/<int:cid>/portal-password")
@login_required
@_hr_require(HR_EDIT)
def admin_contractor_portal_password(cid):
    if not hr_services.admin_get_staff_profile(cid):
        flash("Employee not found.", "error")
        return redirect(url_for("internal_hr.admin_employees"))
    pw = (request.form.get("new_password") or "").strip()
    ok, msg = hr_services.admin_set_contractor_portal_password(cid, pw)
    flash("Portal password updated." if ok else (
        msg or "Could not update password."), "success" if ok else "error")
    return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))


@internal_bp.post("/contractors/<int:cid>/delete")
@login_required
@_hr_require(HR_EDIT)
def admin_contractor_delete(cid):
    if not hr_services.admin_get_staff_profile(cid):
        flash("Employee not found.", "error")
        return redirect(url_for("internal_hr.admin_employees"))
    confirm = (request.form.get("confirm_email") or "").strip()
    ok, msg = hr_services.admin_delete_contractor_employee(
        cid, confirm_email=confirm)
    if ok:
        flash("Employee removed from Sparrow (contractor record deleted).", "success")
        return redirect(url_for("internal_hr.admin_employees"))
    flash(msg or "Could not delete employee.", "error")
    return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))


@internal_bp.post("/contractors/<int:cid>/onboarding-pack")
@login_required
@_hr_require(HR_REQ)
def admin_contractor_onboarding_pack(cid):
    if not hr_services.admin_get_staff_profile(cid):
        flash("Employee not found.", "error")
        return redirect(url_for("internal_hr.admin_employees"))
    pack = (request.form.get("pack_key") or "").strip().lower()
    created, skipped, msg = hr_services.admin_apply_onboarding_pack(cid, pack)
    flash(msg, "success" if created else "info")
    return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))


@internal_bp.post("/contractors/<int:cid>/request-document")
@login_required
@_hr_require(HR_REQ)
def admin_contractor_quick_request(cid):
    """Create a single document request for this employee in one click (profile onboarding)."""
    if not hr_services.admin_get_staff_profile(cid):
        flash("Employee not found.", "error")
        return redirect(url_for("internal_hr.admin_employees"))
    request_type = (request.form.get("request_type")
                    or "other").strip().lower()
    if request_type not in hr_services.REQUEST_TYPES:
        request_type = "other"
    title = (request.form.get("title") or "").strip()
    if not title:
        title = f"Please upload: {hr_services.human_request_type(request_type)}"
    title = title[:255]
    existing = hr_services.admin_active_request_id_for_type(cid, request_type)
    if existing:
        flash(
            "There is already an open request of this type for this person — see Document requests below.",
            "info",
        )
        return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))
    count = hr_services.admin_create_document_request(
        [int(cid)], title, description=None, required_by_date=None, request_type=request_type
    )
    if count:
        flash(f"Request sent: {title}", "success")
    else:
        flash("Could not create the request.", "error")
    return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))


@internal_bp.get("/contractors/<int:cid>/edit")
@login_required
@_hr_require(HR_EDIT)
def admin_contractor_edit_form(cid):
    profile = hr_services.admin_get_staff_profile(cid)
    if not profile:
        flash("Contractor not found.", "error")
        return redirect(url_for("internal_hr.admin_contractors"))
    manager_choices = [c for c in hr_services.admin_list_contractors_for_select(
        limit=800) if int(c["id"]) != int(cid)]
    tb_roles = hr_services.admin_list_time_billing_roles_for_select()
    wage_cards = hr_services.admin_list_wage_rate_cards_for_select()
    return render_template(
        "hr_module/admin/contractor_edit.html",
        profile=profile,
        manager_choices=manager_choices,
        tb_roles=tb_roles,
        wage_cards=wage_cards,
        request_types=hr_services.REQUEST_TYPES,
        dbs_update_service_enabled=is_dbs_update_service_enabled(),
        hcpc_register_api_enabled=is_hcpc_register_api_enabled(),
        config=_core_manifest,
    )


@internal_bp.post("/contractors/<int:cid>/edit")
@login_required
@_hr_require(HR_EDIT)
def admin_contractor_edit_save(cid):
    profile = hr_services.admin_get_staff_profile(cid)
    if not profile:
        flash("Contractor not found.", "error")
        return redirect(url_for("internal_hr.admin_contractors"))

    if not (request.form.get("tb_role_id") or "").strip():
        flash("Staff role is required.", "error")
        return redirect(url_for("internal_hr.admin_contractor_edit_form", cid=cid))

    wage_raw = request.form.get("tb_wage_rate_card_id")
    ok_core, core_msg = hr_services.admin_hr_update_contractor_core(
        cid,
        name=request.form.get("name") or "",
        email=request.form.get("email") or "",
        status=request.form.get("status") or "active",
        employment_type=request.form.get("employment_type"),
        role_id_raw=request.form.get("tb_role_id"),
        wage_rate_card_id_raw=wage_raw,
    )
    if not ok_core:
        flash(core_msg or "Could not save name, email, or Time Billing settings.", "error")
        return redirect(url_for("internal_hr.admin_contractor_edit_form", cid=cid))

    dbs_sub = request.form.get("dbs_update_service_subscribed") == "1"
    dbs_consent_chk = request.form.get("dbs_update_consent_confirm") == "1"
    consent_at = None
    if dbs_sub and dbs_consent_chk:
        existing_consent = (profile.get("dbs_update_consent_at") if profile else None)
        consent_at = existing_consent or datetime.utcnow()
    dbs_cert_ref_in = (request.form.get("dbs_certificate_ref") or "").strip()
    data = {
        "phone": request.form.get("phone"),
        "address_line1": request.form.get("address_line1"),
        "address_line2": request.form.get("address_line2"),
        "postcode": request.form.get("postcode"),
        "emergency_contact_name": request.form.get("emergency_contact_name"),
        "emergency_contact_phone": request.form.get("emergency_contact_phone"),
        "driving_licence_number": request.form.get("driving_licence_number"),
        "driving_licence_expiry": request.form.get("driving_licence_expiry"),
        "driving_licence_document_path": request.form.get("driving_licence_document_path"),
        "right_to_work_type": request.form.get("right_to_work_type"),
        "right_to_work_expiry": request.form.get("right_to_work_expiry"),
        "right_to_work_document_path": request.form.get("right_to_work_document_path"),
        "dbs_level": request.form.get("dbs_level"),
        "dbs_number": request.form.get("dbs_number"),
        "dbs_expiry": request.form.get("dbs_expiry"),
        "dbs_document_path": request.form.get("dbs_document_path"),
        "dbs_certificate_ref": dbs_cert_ref_in,
        "dbs_update_service_subscribed": dbs_sub,
        "dbs_update_consent_at": consent_at,
        "contract_type": request.form.get("contract_type"),
        "contract_start": request.form.get("contract_start"),
        "contract_end": request.form.get("contract_end"),
        "contract_document_path": request.form.get("contract_document_path"),
        "date_of_birth": request.form.get("date_of_birth"),
        "job_title": request.form.get("job_title"),
        "department": request.form.get("department"),
        "manager_contractor_id": request.form.get("manager_contractor_id"),
        "hpac_registration_number": request.form.get("hpac_registration_number"),
        "hpac_registered_on": request.form.get("hpac_registered_on"),
        "hpac_register_grade": request.form.get("hpac_register_grade"),
        "hpac_register_status": request.form.get("hpac_register_status"),
        "hpac_register_check_notes": request.form.get("hpac_register_check_notes"),
        "hcpc_registration_number": request.form.get("hcpc_registration_number"),
        "hcpc_registered_on": request.form.get("hcpc_registered_on"),
        "hcpc_register_profession": request.form.get("hcpc_register_profession"),
        "hcpc_register_status": request.form.get("hcpc_register_status"),
        "hcpc_register_check_notes": request.form.get("hcpc_register_check_notes"),
        "gmc_number": request.form.get("gmc_number"),
        "gmc_registered_on": request.form.get("gmc_registered_on"),
        "gmc_register_status": request.form.get("gmc_register_status"),
        "gmc_register_check_notes": request.form.get("gmc_register_check_notes"),
        "nmc_pin": request.form.get("nmc_pin"),
        "nmc_registered_on": request.form.get("nmc_registered_on"),
        "nmc_register_status": request.form.get("nmc_register_status"),
        "nmc_register_check_notes": request.form.get("nmc_register_check_notes"),
        "gphc_registration_number": request.form.get("gphc_registration_number"),
        "gphc_registered_on": request.form.get("gphc_registered_on"),
        "gphc_register_status": request.form.get("gphc_register_status"),
        "gphc_register_check_notes": request.form.get("gphc_register_check_notes"),
    }
    cf_json, cf_err = hr_services.build_staff_custom_fields_json_from_form(
        request.form.getlist("custom_field_label"),
        request.form.getlist("custom_field_value"),
        request.form.getlist("custom_field_review_date"),
    )
    if cf_err:
        flash(cf_err, "error")
        return redirect(url_for("internal_hr.admin_contractor_edit_form", cid=cid))
    data["custom_fields_json"] = cf_json
    if request.form.get("hpac_record_verification") == "1":
        data["hpac_register_last_checked_at"] = datetime.utcnow()
    else:
        data["hpac_register_last_checked_at"] = profile.get(
            "hpac_register_last_checked_at")
    if request.form.get("hcpc_record_verification") == "1":
        data["hcpc_register_last_checked_at"] = datetime.utcnow()
    else:
        data["hcpc_register_last_checked_at"] = profile.get(
            "hcpc_register_last_checked_at")
    if request.form.get("gmc_record_verification") == "1":
        data["gmc_register_last_checked_at"] = datetime.utcnow()
    else:
        data["gmc_register_last_checked_at"] = profile.get(
            "gmc_register_last_checked_at")
    if request.form.get("nmc_record_verification") == "1":
        data["nmc_register_last_checked_at"] = datetime.utcnow()
    else:
        data["nmc_register_last_checked_at"] = profile.get(
            "nmc_register_last_checked_at")
    if request.form.get("gphc_record_verification") == "1":
        data["gphc_register_last_checked_at"] = datetime.utcnow()
    else:
        data["gphc_register_last_checked_at"] = profile.get(
            "gphc_register_last_checked_at")
    if not dbs_sub:
        data["dbs_update_consent_at"] = None
        data["dbs_update_service_subscribed"] = False
    if hr_services.admin_update_staff_profile(cid, data):
        flash("Profile saved.", "success")
    else:
        flash("Failed to save HR detail fields.", "error")

    pic = request.files.get("profile_picture")
    if pic and pic.filename and pic.filename.strip():
        ext = os.path.splitext(pic.filename)[1].lower() or ""
        allowed_img = (".jpg", ".jpeg", ".png", ".webp", ".gif")
        if ext not in allowed_img:
            flash(
                "Profile photo was not changed — use a JPG, PNG, WebP, or GIF image.", "warning")
        else:
            orig = secure_filename(pic.filename) or "photo"
            safe = f"{uuid.uuid4().hex[:12]}{ext}"
            rel_path = f"uploads/hr_employee/{cid}/{safe}"
            upload_dir = os.path.join(
                _app_static_dir(), "uploads", "hr_employee", str(cid))
            os.makedirs(upload_dir, exist_ok=True)
            full_path = os.path.join(
                _app_static_dir(), rel_path.replace("/", os.sep))
            try:
                pic.save(full_path)
            except OSError:
                flash("Profile photo could not be saved to disk.", "error")
            else:
                doc_id = hr_services.add_employee_document(
                    cid,
                    "profile_picture",
                    "Profile photo (admin edit)",
                    rel_path,
                    file_name=orig,
                    notes="Uploaded from HR admin edit profile",
                    uploaded_by_user_id=_core_user_id_for_db(),
                )
                if doc_id and hr_services.admin_update_contractor_profile_picture(cid, rel_path):
                    flash("Profile photo updated.", "success")
                else:
                    try:
                        if os.path.isfile(full_path):
                            os.remove(full_path)
                    except OSError:
                        pass
                    if not doc_id:
                        flash(
                            "Profile photo was not saved — document library table may be missing (run HR install).",
                            "error",
                        )
                    else:
                        flash(
                            "Profile photo could not be saved (missing profile_picture_path column).", "error")

    return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))


@internal_bp.post("/contractors/<int:cid>/dbs-update-check")
@login_required
@_hr_require(HR_EDIT)
def admin_contractor_dbs_check_now(cid):
    """On-demand DBS Update Service (CRSC) status check for one employee."""
    if not hr_services.admin_get_staff_profile(cid):
        flash("Employee not found.", "error")
        return redirect(url_for("internal_hr.admin_employees"))
    if not is_dbs_update_service_enabled():
        flash(
            "CRSC checks are turned off on this server (DBS_UPDATE_SERVICE_ENABLED=0/false/no/off).",
            "warning",
        )
        return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))
    label = (getattr(current_user, "username", None) or "").strip() or "HR admin"
    out = run_dbs_status_check(
        int(cid),
        channel="manual",
        checker_user_id=_core_user_id_for_db(),
        checker_label=label,
    )
    if out.get("ok"):
        flash(f"DBS Update Service status: {out.get('result_type')}", "success")
        if out.get("new_info_alert"):
            flash(
                "This certificate returned NEW_INFO. Treat as high priority per your "
                "umbrella body / safeguarding process.",
                "danger",
            )
    else:
        flash(out.get("error") or "Status check failed.", "error")
    return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))


@internal_bp.post("/contractors/<int:cid>/hcpc-check")
@login_required
@_hr_require(HR_EDIT)
def admin_contractor_hcpc_check_now(cid):
    """On-demand HCPC register status check for one employee."""
    profile = hr_services.admin_get_staff_profile(cid)
    if not profile:
        flash("Employee not found.", "error")
        return redirect(url_for("internal_hr.admin_employees"))
    if not is_hcpc_register_api_enabled():
        flash(
            "HCPC HTTP checks are turned off on this server (HCPC_REGISTER_API_ENABLED=0/false/no/off).",
            "warning",
        )
        return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))
    label = (getattr(current_user, "username", None) or "").strip() or "HR admin"
    out = run_hcpc_register_status_check(
        int(cid),
        channel="manual",
        checker_user_id=_core_user_id_for_db(),
        checker_label=label,
    )
    if out.get("ok"):
        flash(
            f"HCPC register status: {out.get('result_type') or 'updated'}"
            + (f" ({out.get('message')})" if out.get("message") else ""),
            "success",
        )
    else:
        flash(out.get("message") or "HCPC register check failed.", "error")
    return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))


@internal_bp.post("/contractors/<int:cid>/documents")
@login_required
@_hr_require(HR_LIB)
def admin_employee_document_upload(cid):
    if not hr_services.admin_get_staff_profile(cid):
        flash("Contractor not found.", "error")
        return redirect(url_for("internal_hr.admin_employees"))
    file = request.files.get("file")
    if not file or not file.filename:
        flash("Choose a file to upload.", "error")
        return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))
    category = request.form.get("category") or "general"
    title = (request.form.get("title") or "").strip() or (
        secure_filename(file.filename) or "Document")
    notes = request.form.get("notes")
    orig = secure_filename(file.filename) or "upload"
    ext = os.path.splitext(orig)[1] or ".bin"
    safe = f"{uuid.uuid4().hex[:12]}{ext}"
    rel_path = f"uploads/hr_employee/{cid}/{safe}"
    upload_dir = os.path.join(
        _app_static_dir(), "uploads", "hr_employee", str(cid))
    os.makedirs(upload_dir, exist_ok=True)
    full_path = os.path.join(_app_static_dir(), rel_path.replace("/", os.sep))
    file.save(full_path)
    doc_id = hr_services.add_employee_document(
        cid, category, title, rel_path, file_name=orig, notes=notes, uploaded_by_user_id=_core_user_id_for_db()
    )
    if doc_id:
        flash("Document uploaded.", "success")
    else:
        flash("Could not save document record (run HR install/upgrade).", "error")
    return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))


@internal_bp.post("/contractors/<int:cid>/documents/<int:doc_id>/delete")
@login_required
@_hr_require(HR_LIB)
def admin_employee_document_delete(cid, doc_id):
    doc = hr_services.get_employee_document(doc_id)
    if not doc or int(doc["contractor_id"]) != int(cid):
        flash("Document not found.", "error")
        return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))
    if hr_services.delete_employee_document_and_file(doc_id, _app_static_dir()):
        flash("Document removed.", "success")
    else:
        flash("Could not remove document.", "error")
    return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))


@internal_bp.get("/documents/<int:doc_id>/file")
@login_required
@_hr_require_view
def admin_employee_document_file(doc_id):
    doc = hr_services.get_employee_document(doc_id)
    if not doc:
        abort(404)
    rel = (doc.get("file_path") or "").replace("\\", "/").lstrip("/")
    if not rel.startswith("uploads/hr_employee/"):
        abort(404)
    full = os.path.normpath(os.path.join(_app_static_dir(), rel))
    static_root = os.path.normpath(_app_static_dir())
    if not full.startswith(static_root) or not os.path.isfile(full):
        abort(404)
    dl = doc.get("file_name") or os.path.basename(full) or "document"
    return send_file(full, as_attachment=True, download_name=dl)


@internal_bp.get("/contractors/<int:cid>/profile-linked-file")
@login_required
@_hr_require_view
def admin_contractor_profile_linked_file(cid):
    """
    Serve a file linked on the employee HR profile (compliance cards / avatar).
    Uses the same multi-root resolution as document-request uploads so links work when
    paths use backslashes or files live under legacy static folders (not only url_for('static', ...)).
    """
    field = (request.args.get("field") or "").strip().lower()
    profile_key = _HR_PROFILE_LINKED_FILE_FIELDS.get(field)
    if not profile_key:
        abort(404)
    profile = hr_services.admin_get_staff_profile(cid)
    if not profile:
        abort(404)
    raw_path = profile.get(profile_key) or ""
    if field == "profile_picture":
        normalized = str(raw_path).replace("\\", "/").strip()
        safe = hr_services.hr_safe_profile_picture_path(normalized)
        if not safe:
            abort(404)
        rel_for_resolve = safe
    else:
        rel_for_resolve = str(raw_path).strip()
        if not rel_for_resolve:
            abort(404)
    full = _resolve_hr_staff_profile_linked_path(rel_for_resolve, cid)
    if not full:
        abort(404)
    dl = os.path.basename(full) or "file"
    inline = (request.args.get("disposition") or "").lower() == "inline"
    if field == "profile_picture":
        inline = True
    return send_file(full, as_attachment=not inline, download_name=dl)


def _appraisal_goals_from_form():
    titles = request.form.getlist("goal_title")
    descs = request.form.getlist("goal_description")
    enotes = request.form.getlist("goal_employee_notes")
    mnotes = request.form.getlist("goal_manager_notes")
    goals = []
    for t, d, e, m in zip_longest(titles, descs, enotes, mnotes, fillvalue=""):
        tt = (t or "").strip()
        if not tt:
            continue
        goals.append(
            {
                "title": tt,
                "description": (d or "").strip(),
                "employee_notes": (e or "").strip(),
                "manager_notes": (m or "").strip(),
            }
        )
    return goals


# -----------------------------------------------------------------------------
# Admin: Staff appraisals (HR-APPR-001)
# -----------------------------------------------------------------------------


@internal_bp.get("/appraisals")
@login_required
@_hr_require_view
def admin_appraisals_list():
    if not appraisal_tables_ready():
        flash(
            "Appraisal tables are not installed yet. Run: python app/plugins/hr_module/install.py upgrade",
            "warning",
        )
        return redirect(url_for("internal_hr.admin_index"))
    cid = request.args.get("cid", type=int)
    attention = request.args.get("attention", type=int)
    rows = list_appraisals(contractor_id=cid, limit=400)
    employee_label = None
    if cid:
        prof = hr_services.admin_get_staff_profile(cid)
        if prof:
            employee_label = prof.get("name") or prof.get("email")
    attention_items = None
    if attention == 1:
        attention_items = list_appraisals_needing_attention(
            get_reminder_days_from_manifest()
        )
        order = [x["appraisal_id"] for x in attention_items]
        id_set = set(order)
        rows = [r for r in rows if int(r["id"]) in id_set]
        rows.sort(
            key=lambda r: order.index(int(r["id"]))
            if int(r["id"]) in id_set
            else 9999
        )
    return render_template(
        "hr_module/admin/appraisals_list.html",
        appraisals=rows,
        filter_cid=cid,
        filter_employee_label=employee_label,
        attention_only=bool(attention == 1),
        attention_items=attention_items,
        config=_core_manifest,
    )


@internal_bp.get("/appraisals/events")
@login_required
@_hr_require_view
def admin_appraisal_events_log():
    if not appraisal_event_log_ready():
        flash(
            "Appraisal event log is not installed. Run: python app/plugins/hr_module/install.py upgrade",
            "warning",
        )
        return redirect(url_for("internal_hr.admin_index"))
    aid = request.args.get("appraisal_id", type=int)
    events = list_all_appraisal_events(limit=250, appraisal_id=aid)
    return render_template(
        "hr_module/admin/appraisal_events_log.html",
        events=events,
        filter_appraisal_id=aid,
        config=_core_manifest,
    )


@internal_bp.post("/appraisals/run-reminder-scan")
@login_required
@_hr_require(HR_EDIT)
def admin_appraisal_run_reminder_scan():
    from .appraisals_reminders import run_appraisal_daily_reminder_scan

    force = request.form.get("force") == "1"
    out = run_appraisal_daily_reminder_scan(send_email=True, force=force)
    if out.get("skip") == "disabled_env":
        flash("Appraisal reminders are disabled (HR_APPRAISAL_REMINDERS_DISABLED).", "warning")
    elif out.get("skip") == "tables":
        flash("Appraisal tables missing — run HR install/upgrade.", "error")
    elif out.get("skip") == "already_ran_today" and not force:
        flash(
            "Today's scan already ran. Tick “force” to run again or use flask hr-appraisal-reminders-now.",
            "info",
        )
    else:
        flash(
            "Reminder scan complete: %s item(s). Email sent: %s."
            % (out.get("items", 0), "yes" if out.get("email_sent") else "no"),
            "success",
        )
    return redirect(url_for("internal_hr.admin_appraisal_events_log"))


@internal_bp.get("/appraisals/new")
@login_required
@_hr_require(HR_EDIT)
def admin_appraisal_new_form():
    if not appraisal_tables_ready():
        flash("Run HR install/upgrade to enable appraisals.", "warning")
        return redirect(url_for("internal_hr.admin_index"))
    cid = request.args.get("cid", type=int)
    if not cid:
        flash("Choose an employee from their profile (New appraisal) or pick one below.", "info")
        return redirect(url_for("internal_hr.admin_appraisals_list"))
    profile = hr_services.admin_get_staff_profile(cid)
    if not profile:
        flash("Employee not found.", "error")
        return redirect(url_for("internal_hr.admin_employees"))
    managers = [
        c
        for c in hr_services.admin_list_contractors_for_select(limit=800)
        if int(c["id"]) != int(cid)
    ]
    default_mgr = profile.get("manager_contractor_id")
    return render_template(
        "hr_module/admin/appraisal_form.html",
        appraisal=None,
        goals=[{}],
        contractor_id=cid,
        employee_name=profile.get("name") or profile.get("email"),
        manager_choices=managers,
        default_manager_id=default_mgr,
        config=_core_manifest,
    )


@internal_bp.post("/appraisals/new")
@login_required
@_hr_require(HR_EDIT)
def admin_appraisal_new_save():
    if not appraisal_tables_ready():
        flash("Run HR install/upgrade to enable appraisals.", "warning")
        return redirect(url_for("internal_hr.admin_index"))
    cid = request.form.get("contractor_id", type=int)
    if not cid:
        flash("Missing employee.", "error")
        return redirect(url_for("internal_hr.admin_appraisals_list"))
    mgr_raw = request.form.get("manager_contractor_id")
    mgr_id = int(mgr_raw) if (mgr_raw or "").strip().isdigit() else None
    cycle = (request.form.get("cycle_label") or "").strip() or "Annual appraisal"
    goals = _appraisal_goals_from_form()
    aid, msg = create_appraisal(
        contractor_id=cid,
        manager_contractor_id=mgr_id,
        cycle_label=cycle,
        period_start=request.form.get("period_start"),
        period_end=request.form.get("period_end"),
        status=request.form.get("status") or "draft",
        employee_summary=request.form.get("employee_summary"),
        manager_summary=request.form.get("manager_summary"),
        goals=goals,
        created_by_user_id=_core_user_id_for_db(),
    )
    if aid:
        flash("Appraisal created.", "success")
        return redirect(url_for("internal_hr.admin_appraisal_detail", aid=aid))
    flash(msg or "Could not create appraisal.", "error")
    return redirect(url_for("internal_hr.admin_appraisal_new_form", cid=cid))


@internal_bp.get("/appraisals/<int:aid>")
@login_required
@_hr_require_view
def admin_appraisal_detail(aid):
    if not appraisal_tables_ready():
        flash("Run HR install/upgrade to enable appraisals.", "warning")
        return redirect(url_for("internal_hr.admin_index"))
    row = get_appraisal(aid)
    if not row:
        flash("Appraisal not found.", "error")
        return redirect(url_for("internal_hr.admin_appraisals_list"))
    cid = int(row["contractor_id"])
    managers = [
        c
        for c in hr_services.admin_list_contractors_for_select(limit=800)
        if int(c["id"]) != cid
    ]
    g = row.get("goals") or []
    if not g:
        g = [{}]
    ev = list_appraisal_events(aid, limit=80) if appraisal_event_log_ready() else []
    return render_template(
        "hr_module/admin/appraisal_form.html",
        appraisal=row,
        goals=g,
        contractor_id=cid,
        employee_name=row.get("employee_name") or row.get("employee_email"),
        manager_choices=managers,
        default_manager_id=row.get("manager_contractor_id"),
        appraisal_events=ev,
        config=_core_manifest,
    )


@internal_bp.post("/appraisals/<int:aid>")
@login_required
@_hr_require(HR_EDIT)
def admin_appraisal_save(aid):
    if not appraisal_tables_ready():
        flash("Run HR install/upgrade to enable appraisals.", "warning")
        return redirect(url_for("internal_hr.admin_index"))
    row = get_appraisal(aid)
    if not row:
        flash("Appraisal not found.", "error")
        return redirect(url_for("internal_hr.admin_appraisals_list"))
    mgr_raw = request.form.get("manager_contractor_id")
    mgr_id = int(mgr_raw) if (mgr_raw or "").strip().isdigit() else None
    cycle = (request.form.get("cycle_label") or "").strip() or "Annual appraisal"
    goals = _appraisal_goals_from_form()
    now = datetime.utcnow()
    es = None
    ms = None
    if request.form.get("sign_employee_now") == "1":
        es = now
    if request.form.get("sign_manager_now") == "1":
        ms = now
    ok, msg = update_appraisal(
        appraisal_id=aid,
        manager_contractor_id=mgr_id,
        cycle_label=cycle,
        period_start=request.form.get("period_start"),
        period_end=request.form.get("period_end"),
        status=request.form.get("status") or "draft",
        employee_summary=request.form.get("employee_summary"),
        manager_summary=request.form.get("manager_summary"),
        goals=goals,
        employee_signed_at=es,
        manager_signed_at=ms,
        clear_employee_sign=request.form.get("clear_employee_sign") == "1",
        clear_manager_sign=request.form.get("clear_manager_sign") == "1",
        actor_user_id=_core_user_id_for_db(),
    )
    if ok:
        flash("Appraisal saved.", "success")
    else:
        flash(msg or "Could not save appraisal.", "error")
    return redirect(url_for("internal_hr.admin_appraisal_detail", aid=aid))


@internal_bp.post("/appraisals/<int:aid>/delete")
@login_required
@_hr_require(HR_EDIT)
def admin_appraisal_delete(aid):
    if not appraisal_tables_ready():
        return redirect(url_for("internal_hr.admin_index"))
    cid = None
    row = get_appraisal(aid)
    if row:
        cid = int(row["contractor_id"])
    if delete_appraisal(
        aid, _app_static_dir(), actor_user_id=_core_user_id_for_db()
    ):
        flash("Appraisal deleted.", "success")
    else:
        flash("Could not delete appraisal.", "error")
    if cid:
        return redirect(
            url_for("internal_hr.admin_appraisals_list", cid=cid)
        )
    return redirect(url_for("internal_hr.admin_appraisals_list"))


@internal_bp.post("/appraisals/<int:aid>/attachment")
@login_required
@_hr_require(HR_EDIT)
def admin_appraisal_attachment_upload(aid):
    if not appraisal_tables_ready():
        return redirect(url_for("internal_hr.admin_index"))
    row = get_appraisal(aid)
    if not row:
        flash("Appraisal not found.", "error")
        return redirect(url_for("internal_hr.admin_appraisals_list"))
    if request.form.get("clear_attachment") == "1":
        old = (row.get("attachment_path") or "").replace("\\", "/")
        if old.startswith("uploads/hr_appraisal/"):
            full = os.path.normpath(
                os.path.join(_app_static_dir(), old.replace("/", os.sep))
            )
            root = os.path.normpath(_app_static_dir())
            if full.startswith(root) and os.path.isfile(full):
                try:
                    os.remove(full)
                except OSError:
                    pass
        set_appraisal_attachment(
            aid, None, actor_user_id=_core_user_id_for_db()
        )
        flash("Attachment removed.", "success")
        return redirect(url_for("internal_hr.admin_appraisal_detail", aid=aid))
    file = request.files.get("file")
    if not file or not file.filename:
        flash("Choose a PDF or document file.", "error")
        return redirect(url_for("internal_hr.admin_appraisal_detail", aid=aid))
    orig = secure_filename(file.filename) or "document"
    ext = os.path.splitext(orig)[1].lower() or ".pdf"
    if ext not in (".pdf", ".png", ".jpg", ".jpeg", ".webp"):
        flash("Allowed types: PDF or image (PNG, JPG, WebP).", "error")
        return redirect(url_for("internal_hr.admin_appraisal_detail", aid=aid))
    safe = f"{uuid.uuid4().hex[:12]}{ext}"
    rel_path = f"uploads/hr_appraisal/{aid}/{safe}"
    upload_dir = os.path.join(
        _app_static_dir(), "uploads", "hr_appraisal", str(aid)
    )
    os.makedirs(upload_dir, exist_ok=True)
    full_path = os.path.join(upload_dir, safe)
    file.save(full_path)
    set_appraisal_attachment(
        aid, rel_path, actor_user_id=_core_user_id_for_db()
    )
    flash("Attachment uploaded.", "success")
    return redirect(url_for("internal_hr.admin_appraisal_detail", aid=aid))


@internal_bp.get("/appraisals/<int:aid>/attachment")
@login_required
@_hr_require_view
def admin_appraisal_attachment_download(aid):
    row = get_appraisal(aid)
    if not row:
        abort(404)
    rel = (row.get("attachment_path") or "").replace("\\", "/").lstrip("/")
    if not rel.startswith("uploads/hr_appraisal/"):
        abort(404)
    full = os.path.normpath(os.path.join(_app_static_dir(), rel.replace("/", os.sep)))
    static_root = os.path.normpath(_app_static_dir())
    if not full.startswith(static_root) or not os.path.isfile(full):
        abort(404)
    dl = os.path.basename(full) or "appraisal-attachment"
    return send_file(full, as_attachment=True, download_name=dl)


# -----------------------------------------------------------------------------
# Admin: Document requests
# -----------------------------------------------------------------------------


@internal_bp.get("/requests")
@login_required
@_hr_require_view
def admin_requests():
    contractor_id = request.args.get("contractor_id", type=int)
    status = (request.args.get("status") or "").strip() or None
    date_from_s = request.args.get("date_from") or ""
    date_to_s = request.args.get("date_to") or ""
    date_from = None
    date_to = None
    if date_from_s:
        try:
            date_from = datetime.strptime(date_from_s, "%Y-%m-%d").date()
        except ValueError:
            pass
    if date_to_s:
        try:
            date_to = datetime.strptime(date_to_s, "%Y-%m-%d").date()
        except ValueError:
            pass
    page = max(1, request.args.get("page", type=int) or 1)
    per_page = 50
    rows, total = hr_services.admin_list_document_requests(
        contractor_id=contractor_id,
        status=status,
        date_from=date_from,
        date_to=date_to,
        limit=per_page,
        offset=(page - 1) * per_page,
    )
    total_pages = (total + per_page - 1) // per_page if total else 1
    return render_template(
        "hr_module/admin/requests.html",
        requests_list=rows,
        total=total,
        page=page,
        total_pages=total_pages,
        contractor_id=contractor_id,
        status=status,
        date_from=date_from_s,
        date_to=date_to_s,
        config=_core_manifest,
    )


@internal_bp.get("/requests/new")
@login_required
@_hr_require(HR_REQ)
def admin_request_new_form():
    contractors = hr_services.admin_list_contractors_for_select()
    pre_cid = request.args.get("contractor_id", type=int)
    pre_rt = (request.args.get("request_type") or "").strip().lower() or None
    if pre_rt and pre_rt not in hr_services.REQUEST_TYPES:
        pre_rt = None
    suggested_title = (request.args.get("title") or "").strip()[:255] or None
    return render_template(
        "hr_module/admin/request_new.html",
        contractors=contractors,
        request_types=hr_services.REQUEST_TYPES,
        preselect_contractor_id=pre_cid,
        preselect_request_type=pre_rt,
        suggested_title=suggested_title,
        config=_core_manifest,
    )


@internal_bp.post("/requests/new")
@login_required
@_hr_require(HR_REQ)
def admin_request_new():
    title = (request.form.get("title") or "").strip()
    description = (request.form.get("description") or "").strip() or None
    required_by_s = (request.form.get(
        "required_by_date") or "").strip() or None
    request_type = (request.form.get("request_type") or "other").strip()
    contractor_ids = []
    for part in request.form.getlist("contractor_ids"):
        if str(part).strip().isdigit():
            contractor_ids.append(int(part))
    if request.form.get("all_contractors") == "on":
        contractor_ids = [c["id"]
                          for c in hr_services.admin_list_contractors_for_select()]
    if not title:
        flash("Title is required.", "error")
        return redirect(url_for("internal_hr.admin_request_new_form"))
    if not contractor_ids:
        flash("Select at least one contractor (or All staff).", "error")
        return redirect(url_for("internal_hr.admin_request_new_form"))
    required_by = None
    if required_by_s:
        try:
            required_by = datetime.strptime(required_by_s, "%Y-%m-%d").date()
        except ValueError:
            pass
    count = hr_services.admin_create_document_request(
        contractor_ids, title, description=description, required_by_date=required_by, request_type=request_type
    )
    flash(f"Request created for {count} contractor(s).", "success")
    return redirect(url_for("internal_hr.admin_requests"))


@internal_bp.get("/requests/<int:rid>")
@login_required
@_hr_require_view
def admin_request_detail(rid):
    req = hr_services.admin_get_request(rid)
    if not req:
        flash("Request not found.", "error")
        return redirect(url_for("internal_hr.admin_requests"))
    return render_template(
        "hr_module/admin/request_detail.html",
        req=req,
        config=_core_manifest,
    )


@internal_bp.get("/requests/<int:rid>/uploads/<int:upload_id>/file")
@login_required
@_hr_require_view
def admin_request_upload_file(rid, upload_id):
    """Serve an uploaded document for this request (admin only; path locked to hr_documents)."""
    up = hr_services.admin_get_request_upload(rid, upload_id)
    if not up:
        abort(404)
    full = _resolve_hr_request_upload_path(up.get("file_path") or "")
    if not full:
        abort(404)
    dl = up.get("file_name") or os.path.basename(full) or "document"
    # inline=False lets browser open PDFs/images in-tab when user clicks "Open"
    inline = (request.args.get("disposition") or "").lower() == "inline"
    return send_file(full, as_attachment=not inline, download_name=dl)


@internal_bp.post("/requests/<int:rid>/approve")
@login_required
@_hr_require(HR_REQ)
def admin_request_approve(rid):
    if request.form.get("_csrf_token") and request.form.get("_csrf_token") != session.get("_csrf"):
        pass  # CSRF checked by app
    admin_notes = (request.form.get("admin_notes") or "").strip() or None
    if hr_services.admin_approve_request(rid, _core_user_id_for_db(), admin_notes=admin_notes):
        flash("Request approved.", "success")
    else:
        flash("Request not found.", "error")
    return redirect(url_for("internal_hr.admin_request_detail", rid=rid))


@internal_bp.post("/requests/<int:rid>/reject")
@login_required
@_hr_require(HR_REQ)
def admin_request_reject(rid):
    admin_notes = (request.form.get("admin_notes") or "").strip() or None
    if hr_services.admin_reject_request(rid, _core_user_id_for_db(), admin_notes=admin_notes):
        flash("Request rejected.", "success")
    else:
        flash("Request not found.", "error")
    return redirect(url_for("internal_hr.admin_request_detail", rid=rid))


# -----------------------------------------------------------------------------
# Admin: Expiry dashboard and reports
# -----------------------------------------------------------------------------


@internal_bp.get("/expiry")
@login_required
@_hr_require_view
def admin_expiry():
    days = min(365, max(7, request.args.get("days", type=int) or 90))
    scope = (request.args.get("scope") or "upcoming").strip().lower()
    if scope not in ("upcoming", "expired"):
        scope = "upcoming"
    if scope == "expired":
        rows = hr_services.get_expired_compliance_documents()
    else:
        rows = hr_services.get_expiring_documents(days=days)
    return render_template(
        "hr_module/admin/expiry.html",
        expiry_rows=rows,
        scope=scope,
        days=days,
        config=_core_manifest,
    )


@internal_bp.get("/reports")
@login_required
@_hr_require_view
def admin_reports():
    hr_services.ensure_hr_shell_rows_for_all_contractors()
    overview = hr_services.hr_compliance_overview()
    return render_template(
        "hr_module/admin/reports.html",
        overview=overview,
        config=_core_manifest,
    )


@internal_bp.get("/reports/export")
@login_required
@_hr_require_view
def admin_reports_export():
    """CSV export of staff + key dates and statuses."""
    hr_services.ensure_hr_shell_rows_for_all_contractors()
    contractors = hr_services.admin_list_contractors_for_select()
    profiles = []
    for c in contractors:
        p = hr_services.admin_get_staff_profile(c["id"])
        if p:
            profiles.append(p)
    out = StringIO()
    w = csv.writer(out)
    w.writerow([
        "id", "name", "email", "phone", "address_line1", "postcode",
        "driving_licence_expiry", "right_to_work_expiry", "dbs_expiry", "contract_end",
    ])
    for p in profiles:
        w.writerow([
            p.get("id"),
            p.get("name") or "",
            p.get("email") or "",
            p.get("phone") or "",
            p.get("address_line1") or "",
            p.get("postcode") or "",
            p.get("driving_licence_expiry") or "",
            p.get("right_to_work_expiry") or "",
            p.get("dbs_expiry") or "",
            p.get("contract_end") or "",
        ])
    resp = make_response(out.getvalue())
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = "attachment; filename=hr_export.csv"
    return resp


# -----------------------------------------------------------------------------
# Admin: Onboarding document packs (stored in DB; linked from plugin settings + profile)
# -----------------------------------------------------------------------------


def _onboarding_pack_item_pairs_from_form():
    rts = request.form.getlist("item_rt")
    titles = request.form.getlist("item_title")
    n = max(len(rts), len(titles))
    out = []
    for i in range(n):
        rt = (rts[i] if i < len(rts) else "") or ""
        tt = (titles[i] if i < len(titles) else "") or ""
        out.append((rt, tt))
    return out


def _onboarding_item_rows_from_form():
    rows = []
    for rt, tt in _onboarding_pack_item_pairs_from_form():
        rtype = (rt or "other").strip().lower()
        if rtype not in hr_services.REQUEST_TYPES:
            rtype = "other"
        rows.append({"request_type": rtype, "title": tt})
    return rows or [{"request_type": "other", "title": ""}]


@internal_bp.get("/settings/onboarding-packs")
@login_required
@_hr_require(HR_EDIT, HR_REQ)
def admin_onboarding_packs_list():
    rows = hr_services.admin_list_onboarding_packs_for_settings(
        include_inactive=True)
    return render_template(
        "hr_module/admin/onboarding_packs_list.html",
        packs=rows,
        request_types=hr_services.REQUEST_TYPES,
        config=_core_manifest,
    )


@internal_bp.route("/settings/onboarding-packs/new", methods=["GET", "POST"])
@login_required
@_hr_require(HR_EDIT, HR_REQ)
def admin_onboarding_pack_new():
    if request.method == "POST":
        ok, msg, pid = hr_services.admin_save_onboarding_pack(
            None,
            request.form.get("pack_key") or "",
            request.form.get("label") or "",
            int(request.form.get("sort_order") or 0),
            request.form.get("active_cb") == "1",
            _onboarding_pack_item_pairs_from_form(),
        )
        flash(msg, "success" if ok else "error")
        if ok and pid:
            return redirect(url_for("internal_hr.admin_onboarding_packs_list"))
        return render_template(
            "hr_module/admin/onboarding_pack_form.html",
            pack=None,
            items=_onboarding_item_rows_from_form(),
            is_new=True,
            request_types=hr_services.REQUEST_TYPES,
            config=_core_manifest,
        )
    empty_items = [{"request_type": "right_to_work", "title": ""}]
    return render_template(
        "hr_module/admin/onboarding_pack_form.html",
        pack=None,
        items=empty_items,
        is_new=True,
        request_types=hr_services.REQUEST_TYPES,
        config=_core_manifest,
    )


@internal_bp.route("/settings/onboarding-packs/<int:pack_id>/edit", methods=["GET", "POST"])
@login_required
@_hr_require(HR_EDIT, HR_REQ)
def admin_onboarding_pack_edit(pack_id):
    row = hr_services.admin_get_onboarding_pack_for_edit(pack_id)
    if not row:
        flash("Pack not found.", "error")
        return redirect(url_for("internal_hr.admin_onboarding_packs_list"))
    if request.method == "POST":
        ok, msg, _pid = hr_services.admin_save_onboarding_pack(
            pack_id,
            str(row.get("pack_key") or ""),
            request.form.get("label") or "",
            int(request.form.get("sort_order") or 0),
            request.form.get("active_cb") == "1",
            _onboarding_pack_item_pairs_from_form(),
        )
        flash(msg, "success" if ok else "error")
        if ok:
            return redirect(url_for("internal_hr.admin_onboarding_packs_list"))
        row = dict(row)
        row["label"] = request.form.get("label") or row.get("label")
        try:
            row["sort_order"] = int(request.form.get(
                "sort_order") or row.get("sort_order") or 0)
        except (TypeError, ValueError):
            row["sort_order"] = row.get("sort_order") or 0
        row["active"] = 1 if request.form.get("active_cb") == "1" else 0
        row["items"] = _onboarding_item_rows_from_form()
    items = row.get("items") or []
    if not items:
        items = [{"request_type": "other", "title": ""}]
    return render_template(
        "hr_module/admin/onboarding_pack_form.html",
        pack=row,
        items=items,
        is_new=False,
        request_types=hr_services.REQUEST_TYPES,
        config=_core_manifest,
    )


@internal_bp.post("/settings/onboarding-packs/<int:pack_id>/delete")
@login_required
@_hr_require(HR_EDIT, HR_REQ)
def admin_onboarding_pack_delete(pack_id):
    ok, msg = hr_services.admin_delete_onboarding_pack(pack_id)
    flash(msg, "success" if ok else "error")
    return redirect(url_for("internal_hr.admin_onboarding_packs_list"))


@public_bp.get("/")
@_staff_required
def public_index():
    cid = _contractor_id()
    if cid:
        hr_services.reconcile_staff_details_from_approved_requests(int(cid))
    requests_list = hr_services.list_document_requests(cid) if cid else []
    return render_template(
        "hr_module/public/index.html",
        module_name="HR",
        module_description="Your details and document requests.",
        requests_list=requests_list,
        config=_core_manifest,
        website_settings=_get_website_settings(),
    )


@public_bp.get("/profile")
@_staff_required
def profile():
    cid = _contractor_id()
    if not cid:
        return redirect(url_for("public_hr.public_index"))
    profile_data = hr_services.get_staff_profile(cid)
    compliance_expired_items = (
        hr_services.profile_expired_compliance_items(
            profile_data) if profile_data else []
    )
    return render_template(
        "hr_module/public/profile.html",
        profile=profile_data,
        compliance_expired_items=compliance_expired_items,
        expired_compliance_doc_types={x["doc_type"]
                                      for x in compliance_expired_items},
        config=_core_manifest,
        website_settings=_get_website_settings(),
    )


@public_bp.post("/profile")
@_staff_required
def profile_save():
    cid = _contractor_id()
    if not cid:
        return redirect(url_for("public_hr.public_index"))
    data = request.form
    hr_services.update_staff_details(cid, {
        "phone": data.get("phone"),
        "address_line1": data.get("address_line1"),
        "address_line2": data.get("address_line2"),
        "postcode": data.get("postcode"),
        "emergency_contact_name": data.get("emergency_contact_name"),
        "emergency_contact_phone": data.get("emergency_contact_phone"),
    })
    return redirect(url_for("public_hr.profile"))


@public_bp.get("/request/<int:req_id>/uploads/<int:upload_id>/file")
@_staff_required
def request_upload_file(req_id, upload_id):
    """Serve a document-request upload to the contractor (not via /static/ — files may live under legacy paths)."""
    cid = _contractor_id()
    if not cid:
        return redirect(url_for("public_hr.public_index"))
    up = hr_services.contractor_get_request_upload(int(cid), req_id, upload_id)
    if not up:
        abort(404)
    full = _resolve_hr_request_upload_path(up.get("file_path") or "")
    if not full:
        abort(404)
    dl = up.get("file_name") or os.path.basename(full) or "document"
    inline = (request.args.get("disposition") or "").lower() == "inline"
    return send_file(full, as_attachment=not inline, download_name=dl)


@public_bp.get("/request/<int:req_id>")
@_staff_required
def request_detail(req_id):
    from app.objects import get_db_connection
    cid = _contractor_id()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT r.*, (SELECT COUNT(*) FROM hr_document_uploads u WHERE u.request_id = r.id) AS upload_count
            FROM hr_document_requests r WHERE r.id = %s AND r.contractor_id = %s
        """, (req_id, cid))
        req = cur.fetchone()
        if not req:
            return redirect(url_for("public_hr.public_index"))
        cur.execute(
            "SELECT id, file_path, file_name, uploaded_at FROM hr_document_uploads WHERE request_id = %s ORDER BY uploaded_at", (req_id,))
        req["uploads"] = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return render_template(
        "hr_module/public/request_detail.html",
        req=req,
        config=_core_manifest,
        website_settings=_get_website_settings(),
    )


@public_bp.post("/request/<int:req_id>/upload")
@_staff_required
def request_upload(req_id):
    cid = _contractor_id()
    if not cid:
        return redirect(url_for("public_hr.public_index"))
    file = request.files.get("document")
    if not file or not file.filename:
        return redirect(url_for("public_hr.request_detail", req_id=req_id))
    req_rt = hr_services.contractor_get_document_request_type(
        int(cid), req_id) or ""
    ext = os.path.splitext(file.filename)[1].lower() or ""
    if req_rt == "profile_picture":
        if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
            flash(
                "Profile photo requests need an image file (JPG, PNG, WebP, or GIF).", "error")
            return redirect(url_for("public_hr.request_detail", req_id=req_id))
    static_root = _app_static_dir()
    upload_dir = os.path.join(static_root, "uploads", "hr_documents")
    os.makedirs(upload_dir, exist_ok=True)
    ext = os.path.splitext(file.filename)[1] or ".pdf"
    safe_name = f"{req_id}_{cid}_{uuid.uuid4().hex[:12]}{ext}"
    # Always store POSIX-style path in DB so admin resolve logic matches on all OSes
    rel_path = f"uploads/hr_documents/{safe_name}"
    full_path = os.path.join(upload_dir, safe_name)
    file.save(full_path)
    document_type = request.form.get("document_type") or "primary"
    try:
        hr_services.add_upload(req_id, cid, rel_path,
                               file.filename, document_type=document_type)
    except ValueError:
        pass
    return redirect(url_for("public_hr.request_detail", req_id=req_id))


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_bp
