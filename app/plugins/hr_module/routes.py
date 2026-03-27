import csv
import os
import uuid
from datetime import datetime
from functools import wraps
from io import StringIO
from typing import Optional

from flask import Blueprint, abort, current_app, flash, make_response, redirect, render_template, request, send_file, session, url_for
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename
from app.objects import PluginManager
from . import services as hr_services

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
    u = session.get("tb_user")
    return int(u["id"]) if u and u.get("id") is not None else None


def _ventus_plugin_available() -> bool:
    """True when Ventus CAD has manifest.json and is enabled (blueprints registered — avoids HR links to 404)."""
    try:
        vd = os.path.join(_plugin_manager.plugins_dir, "ventus_response_module")
        if not os.path.isdir(vd) or not os.path.exists(os.path.join(vd, "manifest.json")):
            return False
        pl = _plugin_manager.load_plugins() or {}
        m = pl.get("ventus_response_module") or {}
        return bool(m.get("enabled", False))
    except Exception:
        return False


_template = os.path.join(os.path.dirname(__file__), "templates")
internal_bp = Blueprint("internal_hr", __name__, url_prefix="/plugin/hr_module", template_folder=_template)
public_bp = Blueprint("public_hr", __name__, url_prefix="/hr", template_folder=_template)


def _register_hr_jinja_filters(state):
    """Human-readable labels for HR templates (snake_case -> UI text)."""
    from . import services as _svc

    env = state.app.jinja_env
    env.filters.setdefault("hr_request_type", _svc.human_request_type)
    env.filters.setdefault("hr_doc_request_status", _svc.human_document_request_status)
    env.filters.setdefault("hr_upload_doc_type", _svc.human_upload_document_type)
    env.filters.setdefault("hr_employee_doc_category", _svc.human_employee_document_category)
    env.filters.setdefault("hr_expiry_doc_type", _svc.human_expiry_doc_type)
    env.filters.setdefault("hr_contractor_status", _svc.human_contractor_status)
    env.filters.setdefault("hr_profile_field", _svc.human_profile_field)
    env.filters.setdefault("hr_safe_profile_picture", _svc.hr_safe_profile_picture_path)


internal_bp.record(_register_hr_jinja_filters)
public_bp.record(_register_hr_jinja_filters)


def _app_static_dir() -> str:
    """
    …/app/static — resolved from this plugin path (always matches Flask app package layout).
    Do not use current_app.root_path alone: it can disagree with where we save uploads.
    """
    app_pkg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
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


def _admin_required_hr(view):
    """For admin app: require core user with role admin/superuser (Flask-Login)."""
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for("routes.login"))
        role = (getattr(current_user, "role", None) or "").lower()
        if role not in ("admin", "superuser"):
            flash("Admin access required.", "error")
            return redirect(url_for("routes.dashboard"))
        return view(*args, **kwargs)
    return wrapped


def _core_user_id_for_db():
    """Core `users.id` is CHAR(36) UUID — store as string, never int()."""
    uid = getattr(current_user, "id", None)
    if uid is None:
        return None
    return str(uid)


@internal_bp.get("/")
@login_required
@_admin_required_hr
def admin_index():
    hr_services.ensure_hr_shell_rows_for_all_contractors()
    overview = hr_services.hr_compliance_overview()
    upcoming_birthdays = hr_services.admin_upcoming_birthdays(60)
    try:
        expiring = hr_services.get_expiring_documents(30)
    except Exception:
        expiring = []
    return render_template(
        "hr_module/admin/index.html",
        module_name="HR",
        module_description="Employee store: profiles, compliance fields, scanned documents, and requests.",
        plugin_system_name="hr_module",
        overview=overview,
        upcoming_birthdays=upcoming_birthdays,
        expiring_count=len(expiring),
        ventus_plugin_available=_ventus_plugin_available(),
        config=_core_manifest,
    )


# -----------------------------------------------------------------------------
# Admin: Employee directory
# -----------------------------------------------------------------------------


@internal_bp.get("/employees")
@login_required
@_admin_required_hr
def admin_employees():
    hr_services.ensure_hr_shell_rows_for_all_contractors()
    q = (request.args.get("q") or "").strip() or None
    status = (request.args.get("status") or "active").strip() or "active"
    view_mode = (request.args.get("view") or "kanban").strip().lower()
    if view_mode not in ("kanban", "list"):
        view_mode = "kanban"
    if status.lower() == "all":
        status = None
    employees = hr_services.admin_list_employees(q=q, status=status, limit=400, offset=0)
    return render_template(
        "hr_module/admin/employees.html",
        employees=employees,
        q=q or "",
        status_filter=(request.args.get("status") or "active"),
        view_mode=view_mode,
        config=_core_manifest,
    )


# -----------------------------------------------------------------------------
# Admin: Contractor search and profile
# -----------------------------------------------------------------------------


@internal_bp.get("/contractors")
@login_required
@_admin_required_hr
def admin_contractors():
    q = (request.args.get("q") or "").strip()
    contractors = hr_services.admin_search_contractors(q, limit=50) if q else []
    return render_template(
        "hr_module/admin/contractors.html",
        q=q,
        contractors=contractors,
        config=_core_manifest,
    )


@internal_bp.get("/contractors/<int:cid>")
@login_required
@_admin_required_hr
def admin_contractor_profile(cid):
    hr_services.ensure_hr_shell_rows_for_all_contractors()
    hr_services.reconcile_staff_details_from_approved_requests(cid)
    profile = hr_services.admin_get_staff_profile(cid)
    if not profile:
        flash("Contractor not found.", "error")
        return redirect(url_for("internal_hr.admin_contractors"))
    requests_list, _ = hr_services.admin_list_document_requests(contractor_id=cid, limit=100, offset=0)
    employee_documents = hr_services.list_employee_documents(cid)
    compliance_gaps = hr_services.admin_profile_compliance_gaps(profile, employee_documents)
    open_doc_request_button_state = hr_services.contractor_open_request_button_state_by_type(cid)
    ventus_plugin_available = _ventus_plugin_available()
    ventus_crew_profiles = (
        hr_services.get_ventus_crew_profiles_for_contractor(cid)
        if ventus_plugin_available
        else []
    )
    focus_cat = (request.args.get("focus_cat") or "").strip().lower() or None
    if focus_cat and focus_cat not in hr_services.EMPLOYEE_DOCUMENT_CATEGORIES:
        focus_cat = None
    return render_template(
        "hr_module/admin/contractor_profile.html",
        profile=profile,
        requests_list=requests_list,
        employee_documents=employee_documents,
        doc_categories=hr_services.EMPLOYEE_DOCUMENT_CATEGORIES,
        compliance_gaps=compliance_gaps,
        open_doc_request_button_state=open_doc_request_button_state,
        ventus_crew_profiles=ventus_crew_profiles,
        ventus_plugin_available=ventus_plugin_available,
        focus_cat=focus_cat,
        config=_core_manifest,
    )


@internal_bp.post("/contractors/<int:cid>/request-document")
@login_required
@_admin_required_hr
def admin_contractor_quick_request(cid):
    """Create a single document request for this employee in one click (profile onboarding)."""
    if not hr_services.admin_get_staff_profile(cid):
        flash("Employee not found.", "error")
        return redirect(url_for("internal_hr.admin_employees"))
    request_type = (request.form.get("request_type") or "other").strip().lower()
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
@_admin_required_hr
def admin_contractor_edit_form(cid):
    profile = hr_services.admin_get_staff_profile(cid)
    if not profile:
        flash("Contractor not found.", "error")
        return redirect(url_for("internal_hr.admin_contractors"))
    manager_choices = [c for c in hr_services.admin_list_contractors_for_select(limit=800) if int(c["id"]) != int(cid)]
    return render_template(
        "hr_module/admin/contractor_edit.html",
        profile=profile,
        manager_choices=manager_choices,
        request_types=hr_services.REQUEST_TYPES,
        config=_core_manifest,
    )


@internal_bp.post("/contractors/<int:cid>/edit")
@login_required
@_admin_required_hr
def admin_contractor_edit_save(cid):
    profile = hr_services.admin_get_staff_profile(cid)
    if not profile:
        flash("Contractor not found.", "error")
        return redirect(url_for("internal_hr.admin_contractors"))
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
        "contract_type": request.form.get("contract_type"),
        "contract_start": request.form.get("contract_start"),
        "contract_end": request.form.get("contract_end"),
        "contract_document_path": request.form.get("contract_document_path"),
        "date_of_birth": request.form.get("date_of_birth"),
        "job_title": request.form.get("job_title"),
        "department": request.form.get("department"),
        "manager_contractor_id": request.form.get("manager_contractor_id"),
    }
    if hr_services.admin_update_staff_profile(cid, data):
        flash("Profile saved.", "success")
    else:
        flash("Failed to save profile.", "error")

    et = (request.form.get("employment_type") or "").strip().lower()
    if et in ("paye", "self_employed"):
        if hr_services.admin_update_contractor_employment_type(cid, et):
            flash("Employment type updated for Time Billing.", "success")

    pic = request.files.get("profile_picture")
    if pic and pic.filename and pic.filename.strip():
        ext = os.path.splitext(pic.filename)[1].lower() or ""
        allowed_img = (".jpg", ".jpeg", ".png", ".webp", ".gif")
        if ext not in allowed_img:
            flash("Profile photo was not changed — use a JPG, PNG, WebP, or GIF image.", "warning")
        else:
            orig = secure_filename(pic.filename) or "photo"
            safe = f"{uuid.uuid4().hex[:12]}{ext}"
            rel_path = f"uploads/hr_employee/{cid}/{safe}"
            upload_dir = os.path.join(_app_static_dir(), "uploads", "hr_employee", str(cid))
            os.makedirs(upload_dir, exist_ok=True)
            full_path = os.path.join(_app_static_dir(), rel_path.replace("/", os.sep))
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
                        flash("Profile photo could not be saved (missing profile_picture_path column).", "error")

    return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))


@internal_bp.post("/contractors/<int:cid>/documents")
@login_required
@_admin_required_hr
def admin_employee_document_upload(cid):
    if not hr_services.admin_get_staff_profile(cid):
        flash("Contractor not found.", "error")
        return redirect(url_for("internal_hr.admin_employees"))
    file = request.files.get("file")
    if not file or not file.filename:
        flash("Choose a file to upload.", "error")
        return redirect(url_for("internal_hr.admin_contractor_profile", cid=cid))
    category = request.form.get("category") or "general"
    title = (request.form.get("title") or "").strip() or (secure_filename(file.filename) or "Document")
    notes = request.form.get("notes")
    orig = secure_filename(file.filename) or "upload"
    ext = os.path.splitext(orig)[1] or ".bin"
    safe = f"{uuid.uuid4().hex[:12]}{ext}"
    rel_path = f"uploads/hr_employee/{cid}/{safe}"
    upload_dir = os.path.join(_app_static_dir(), "uploads", "hr_employee", str(cid))
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
@_admin_required_hr
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
@_admin_required_hr
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
@_admin_required_hr
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


# -----------------------------------------------------------------------------
# Admin: Document requests
# -----------------------------------------------------------------------------


@internal_bp.get("/requests")
@login_required
@_admin_required_hr
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
@_admin_required_hr
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
@_admin_required_hr
def admin_request_new():
    title = (request.form.get("title") or "").strip()
    description = (request.form.get("description") or "").strip() or None
    required_by_s = (request.form.get("required_by_date") or "").strip() or None
    request_type = (request.form.get("request_type") or "other").strip()
    contractor_ids = []
    for part in request.form.getlist("contractor_ids"):
        if str(part).strip().isdigit():
            contractor_ids.append(int(part))
    if request.form.get("all_contractors") == "on":
        contractor_ids = [c["id"] for c in hr_services.admin_list_contractors_for_select()]
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
@_admin_required_hr
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
@_admin_required_hr
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
@_admin_required_hr
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
@_admin_required_hr
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
@_admin_required_hr
def admin_expiry():
    days = min(365, max(7, request.args.get("days", type=int) or 90))
    expiring = hr_services.get_expiring_documents(days=days)
    return render_template(
        "hr_module/admin/expiry.html",
        expiring=expiring,
        days=days,
        config=_core_manifest,
    )


@internal_bp.get("/reports")
@login_required
@_admin_required_hr
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
@_admin_required_hr
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
    return render_template(
        "hr_module/public/profile.html",
        profile=profile_data,
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
        cur.execute("SELECT id, file_path, file_name, uploaded_at FROM hr_document_uploads WHERE request_id = %s ORDER BY uploaded_at", (req_id,))
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
    req_rt = hr_services.contractor_get_document_request_type(int(cid), req_id) or ""
    ext = os.path.splitext(file.filename)[1].lower() or ""
    if req_rt == "profile_picture":
        if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
            flash("Profile photo requests need an image file (JPG, PNG, WebP, or GIF).", "error")
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
        hr_services.add_upload(req_id, cid, rel_path, file.filename, document_type=document_type)
    except ValueError:
        pass
    return redirect(url_for("public_hr.request_detail", req_id=req_id))


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_bp
