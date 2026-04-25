import os
import uuid
from functools import wraps
from typing import Optional
from urllib.parse import quote

from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from flask_login import current_user, login_required
from app.objects import PluginManager
from app.organization_profile import normalize_organization_industries

from . import services as rec_svc

_plugin_manager = PluginManager(os.path.abspath("app/plugins"))
_core_manifest = _plugin_manager.get_core_manifest() or {}

_template = os.path.join(os.path.dirname(__file__), "templates")
internal_bp = Blueprint(
    "internal_recruitment",
    __name__,
    url_prefix="/plugin/recruitment_module",
    template_folder=_template,
)
public_site_bp = Blueprint(
    "public_recruitment_site",
    __name__,
    url_prefix="",
    template_folder=_template,
)


@public_site_bp.record_once
def _recruitment_public_register_asset_url(state):
    """Careers pages use the same asset resolver as the website module (logo, theme CSS)."""
    app = state.app
    if app.jinja_env.globals.get("website_public_asset_url"):
        return
    try:
        from app.plugins.website_module.routes import website_public_asset_url

        app.jinja_env.globals["website_public_asset_url"] = website_public_asset_url
    except Exception:
        pass


SESSION_APPLICANT = "rec_applicant"
# Public careers site only — keeps core/admin flashes off job & applicant pages
REC_PUB_FLASH_SUCCESS = "recruitment_success"
REC_PUB_FLASH_ERROR = "recruitment_error"
ALLOWED_CV_EXT = {".pdf", ".doc", ".docx"}
ALLOWED_TASK_FORM_FILE_EXT = {
    ".pdf",
    ".doc",
    ".docx",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
}


def _get_website_settings():
    try:
        from app.plugins.website_module.routes import get_website_settings

        return get_website_settings()
    except Exception:
        pass
    _keys = (
        "favicon_path",
        "theme_color",
        "default_og_image",
        "schema_json",
        "cookie_bar_colors",
        "cookie_bar_text",
        "cookie_bar_accept_text",
        "cookie_bar_decline_text",
        "cookie_policy",
        "analytics_code",
        "facebook_url",
        "instagram_url",
        "linkedin_url",
        "twitter_url",
        "youtube_url",
        "tiktok_url",
    )
    return {k: None for k in _keys}


REC_ACCESS = "recruitment_module.access"
REC_READ = "recruitment_module.read"
REC_SETUP = "recruitment_module.manage_setup"
REC_APPS = "recruitment_module.manage_applications"
REC_HIRE = "recruitment_module.hire"
_REC_ANY_VIEW = frozenset({REC_READ, REC_SETUP, REC_APPS, REC_HIRE})


def _rec_perm_set(user):
    role = (getattr(user, "role", None) or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return None
    raw = getattr(user, "permissions", None) or []
    return {str(x) for x in raw if x}


def _rec_may_view(user) -> bool:
    ps = _rec_perm_set(user)
    if ps is None:
        return True
    if REC_ACCESS in ps:
        return True
    return bool(ps & _REC_ANY_VIEW)


def _rec_may(user, *needed: str) -> bool:
    ps = _rec_perm_set(user)
    if ps is None:
        return True
    if REC_ACCESS in ps:
        return True
    return any(n in ps for n in needed)


def _rec_require_view(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for("routes.login"))
        if not _rec_may_view(current_user):
            flash("You do not have access to Recruitment.", "danger")
            return redirect(url_for("routes.dashboard"))
        return view(*args, **kwargs)

    return wrapped


def _rec_require(*needed: str):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for("routes.login"))
            if not _rec_may(current_user, *needed):
                flash("You do not have permission for this action.", "danger")
                return redirect(url_for("routes.dashboard"))
            return view(*args, **kwargs)

        return wrapped

    return decorator


@internal_bp.context_processor
def _rec_admin_perm_context():
    u = current_user
    if not getattr(u, "is_authenticated", False):
        return {
            "rec_can_view": False,
            "rec_can_manage_setup": False,
            "rec_can_manage_applications": False,
            "rec_can_hire": False,
        }
    return {
        "rec_can_view": _rec_may_view(u),
        "rec_can_manage_setup": _rec_may(u, REC_SETUP),
        "rec_can_manage_applications": _rec_may(u, REC_APPS),
        "rec_can_hire": _rec_may(u, REC_HIRE),
    }


def _applicant_user():
    return session.get(SESSION_APPLICANT)


def _applicant_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not _applicant_user():
            nxt = quote(request.path, safe="/")
            return redirect(url_for("public_recruitment_site.applicant_login", next=nxt))
        return view(*args, **kwargs)

    return wrapped


def _flash_public(message: str, *, error: bool = False) -> None:
    """Queue a flash visible only on public recruitment templates (not mixed with ERP admin)."""
    flash(message, REC_PUB_FLASH_ERROR if error else REC_PUB_FLASH_SUCCESS)


def _form_optional_positive_int(field: str) -> Optional[int]:
    v = request.form.get(field)
    if v is None or str(v).strip() == "":
        return None
    try:
        i = int(v)
        return i if i > 0 else None
    except (TypeError, ValueError):
        return None


def _app_static_dir() -> str:
    """
    …/app/static — same resolution as HR module (plugin-relative).
    Avoid relying on current_app.root_path alone: it can disagree with where uploads are saved.
    """
    app_pkg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return os.path.join(app_pkg_dir, "static")


def _project_root_dir() -> str:
    """Repo root (parent of app/). Legacy uploads may exist under <root>/static/…"""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _recruitment_cv_static_roots():
    """Roots where recruitment CV files may exist (app static, legacy repo static)."""
    return [
        os.path.abspath(_app_static_dir()),
        os.path.abspath(os.path.join(_project_root_dir(), "static")),
    ]


def _resolve_recruitment_cv_file(rel: str) -> Optional[str]:
    """Resolve uploads/recruitment_cv/… to first existing file under known static roots."""
    rel = (rel or "").replace("\\", "/").strip().lstrip("/")
    if not rel.startswith("uploads/recruitment_cv/") or ".." in rel.split("/"):
        return None
    segs = [s for s in rel.split("/") if s]
    if len(segs) < 3:
        return None
    for root in _recruitment_cv_static_roots():
        candidate = os.path.abspath(os.path.join(root, *segs))
        try:
            if os.path.commonpath([candidate, root]) != root:
                continue
        except ValueError:
            continue
        if os.path.isfile(candidate):
            return candidate
    return None


def _save_cv_upload(file_storage):
    if not file_storage or not file_storage.filename:
        return None
    ext = os.path.splitext(file_storage.filename)[1].lower()
    if ext not in ALLOWED_CV_EXT:
        return None
    upload_dir = os.path.join(_app_static_dir(), "uploads", "recruitment_cv")
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = f"{uuid.uuid4().hex}{ext}"
    rel = os.path.join("uploads", "recruitment_cv", safe_name).replace("\\", "/")
    file_storage.save(os.path.join(_app_static_dir(), rel.replace("/", os.sep)))
    return rel


def _save_prehire_file_upload(file_storage):
    if not file_storage or not file_storage.filename:
        return None
    ext = os.path.splitext(file_storage.filename)[1].lower()
    if ext not in ALLOWED_TASK_FORM_FILE_EXT:
        return None
    upload_dir = os.path.join(_app_static_dir(), "uploads", "recruitment_prehire")
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = f"{uuid.uuid4().hex}{ext}"
    rel = os.path.join("uploads", "recruitment_prehire", safe_name).replace("\\", "/")
    file_storage.save(os.path.join(_app_static_dir(), rel.replace("/", os.sep)))
    return rel


def _save_task_form_file_upload(file_storage):
    """Store a single file for a recruitment screening form field."""
    if not file_storage or not file_storage.filename:
        return None
    ext = os.path.splitext(file_storage.filename)[1].lower()
    if ext not in ALLOWED_TASK_FORM_FILE_EXT:
        return None
    upload_dir = os.path.join(_app_static_dir(), "uploads", "recruitment_task_files")
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = f"{uuid.uuid4().hex}{ext}"
    rel = os.path.join("uploads", "recruitment_task_files", safe_name).replace("\\", "/")
    file_storage.save(os.path.join(_app_static_dir(), rel.replace("/", os.sep)))
    return rel


# =============================================================================
# Public site: vacancies + applicant portal (url_prefix "")
# =============================================================================


@public_site_bp.get("/vacancies")
def vacancies_list():
    items = rec_svc.list_open_vacancies()
    rec_svc.annotate_vacancies_capacity(items)
    return render_template(
        "recruitment_module/public/vacancies.html",
        vacancies=items,
        title="Vacancies",
        config=_core_manifest,
        website_settings=_get_website_settings(),
        applicant=_applicant_user(),
    )


@public_site_bp.get("/vacancies/<slug>")
def vacancy_detail(slug):
    opening = rec_svc.get_opening_public_by_slug(slug)
    if not opening:
        _flash_public("Vacancy not found or no longer open.", error=True)
        return redirect(url_for("public_recruitment_site.vacancies_list"))
    u = _applicant_user()
    applicant_profile = rec_svc.get_applicant_by_id(int(u["id"])) if u else None
    cap_block, cap_msg = rec_svc.opening_blocks_new_applications(opening)
    return render_template(
        "recruitment_module/public/vacancy_detail.html",
        opening=opening,
        title=opening.get("title") or "Vacancy",
        config=_core_manifest,
        website_settings=_get_website_settings(),
        applicant=u,
        applicant_profile=applicant_profile,
        opening_capacity_blocked=cap_block,
        opening_capacity_message=cap_msg,
    )


@public_site_bp.post("/vacancies/<slug>/apply")
def vacancy_apply_post(slug):
    opening = rec_svc.get_opening_public_by_slug(slug)
    if not opening:
        _flash_public("Vacancy not found.", error=True)
        return redirect(url_for("public_recruitment_site.vacancies_list"))
    u = _applicant_user()
    if not u:
        _flash_public("Please sign in or create an applicant account to apply.", error=True)
        return redirect(
            url_for("public_recruitment_site.applicant_login")
            + "?next="
            + quote(url_for("public_recruitment_site.vacancy_detail", slug=slug), safe="/")
        )
    cover = (request.form.get("cover_note") or "").strip() or None
    cv_path = None
    file = request.files.get("cv")
    up = _save_cv_upload(file)
    if file and file.filename and not up:
        _flash_public("CV must be PDF, DOC, or DOCX.", error=True)
        return redirect(url_for("public_recruitment_site.vacancy_detail", slug=slug))
    if up:
        cv_path = up
    ok_prof, msg_prof, hr_profile = rec_svc.validate_application_hr_profile_from_request(
        request.form
    )
    if not ok_prof:
        _flash_public(msg_prof, error=True)
        return redirect(url_for("public_recruitment_site.vacancy_detail", slug=slug))
    ok, msg, app_id = rec_svc.create_application(
        int(opening["id"]),
        int(u["id"]),
        cover_note=cover,
        cv_path=cv_path,
        hr_profile=hr_profile,
    )
    if not ok:
        _flash_public(msg, error=True)
        return redirect(url_for("public_recruitment_site.vacancy_detail", slug=slug))
    if app_id:
        try:
            from . import notifications as rec_notifications

            rec_notifications.notify_new_application_submitted(int(app_id))
        except Exception:
            pass
    _flash_public("Application submitted. You can track progress in your applicant portal.")
    return redirect(
        url_for("public_recruitment_site.applicant_application", application_id=app_id)
    )


@public_site_bp.get("/recruitment/applicant/register")
def applicant_register():
    if _applicant_user():
        return redirect(url_for("public_recruitment_site.applicant_dashboard"))
    return render_template(
        "recruitment_module/public/applicant_register.html",
        title="Applicant register",
        config=_core_manifest,
        website_settings=_get_website_settings(),
        applicant=None,
    )


@public_site_bp.post("/recruitment/applicant/register")
def applicant_register_post():
    email = request.form.get("email") or ""
    name = request.form.get("name") or ""
    phone = request.form.get("phone") or ""
    password = request.form.get("password") or ""
    ok, msg, aid = rec_svc.register_applicant(email, name, phone, password)
    if not ok:
        _flash_public(msg, error=True)
        return redirect(url_for("public_recruitment_site.applicant_register"))
    try:
        from . import notifications as rec_notifications

        rec_notifications.notify_applicant_account_registered(email, name)
    except Exception:
        pass
    session[SESSION_APPLICANT] = {
        "id": aid,
        "email": (email or "").strip().lower(),
        "name": (name or "").strip(),
    }
    session.modified = True
    _flash_public("Account created. You can apply for open roles.")
    nxt = request.args.get("next") or url_for("public_recruitment_site.applicant_dashboard")
    return redirect(nxt)


@public_site_bp.get("/recruitment/applicant/login")
def applicant_login():
    if _applicant_user():
        return redirect(url_for("public_recruitment_site.applicant_dashboard"))
    return render_template(
        "recruitment_module/public/applicant_login.html",
        title="Applicant sign in",
        config=_core_manifest,
        website_settings=_get_website_settings(),
        next_url=request.args.get("next") or "",
        applicant=None,
    )


@public_site_bp.post("/recruitment/applicant/login")
def applicant_login_post():
    email = request.form.get("email") or ""
    password = request.form.get("password") or ""
    row = rec_svc.verify_applicant_login(email, password)
    if not row:
        _flash_public("Invalid email or password.", error=True)
        return redirect(url_for("public_recruitment_site.applicant_login"))
    session[SESSION_APPLICANT] = {
        "id": row["id"],
        "email": row["email"],
        "name": row.get("name") or "",
    }
    session.modified = True
    nxt = request.form.get("next") or request.args.get("next") or ""
    if nxt and nxt.startswith("/"):
        return redirect(nxt)
    return redirect(url_for("public_recruitment_site.applicant_dashboard"))


@public_site_bp.get("/recruitment/applicant/logout")
def applicant_logout():
    session.pop(SESSION_APPLICANT, None)
    session.modified = True
    return redirect(url_for("public_recruitment_site.vacancies_list"))


@public_site_bp.get("/recruitment/applicant/")
@_applicant_required
def applicant_dashboard():
    uid = int(_applicant_user()["id"])
    apps = rec_svc.list_applications_for_applicant(uid)
    return render_template(
        "recruitment_module/public/applicant_dashboard.html",
        applications=apps,
        title="My applications",
        config=_core_manifest,
        website_settings=_get_website_settings(),
        applicant=_applicant_user(),
    )


@public_site_bp.get("/recruitment/applicant/applications/<int:application_id>")
@_applicant_required
def applicant_application(application_id):
    uid = int(_applicant_user()["id"])
    app_row = rec_svc.get_application_for_applicant(application_id, uid)
    if not app_row:
        _flash_public("Application not found.", error=True)
        return redirect(url_for("public_recruitment_site.applicant_dashboard"))
    tasks = rec_svc.get_application_tasks_for_applicant(application_id, uid)
    progress = rec_svc.application_progress(app_row.get("stage") or "applied")
    prehire = rec_svc.list_prehire_requests_for_applicant(application_id, uid)
    return render_template(
        "recruitment_module/public/applicant_application.html",
        application=app_row,
        tasks=tasks,
        prehire_requests=prehire,
        prehire_type_labels=rec_svc.PREHIRE_REQUEST_TYPE_LABELS,
        retention_days=rec_svc.recruitment_applicant_retention_days(),
        progress=progress,
        stages=rec_svc.STAGES_ORDER,
        stage_labels=rec_svc.STAGE_LABELS,
        title=app_row.get("opening_title") or "Application",
        config=_core_manifest,
        website_settings=_get_website_settings(),
        applicant=_applicant_user(),
    )


@public_site_bp.get("/recruitment/applicant/applications/<int:application_id>/cv-file")
@_applicant_required
def applicant_application_cv_file(application_id):
    """Serve the applicant's own CV (same path rules as admin)."""
    uid = int(_applicant_user()["id"])
    app_row = rec_svc.get_application_for_applicant(application_id, uid)
    if not app_row or not app_row.get("cv_path"):
        abort(404)
    rel = str(app_row["cv_path"]).replace("\\", "/").strip().lstrip("/")
    full = _resolve_recruitment_cv_file(rel)
    if not full:
        abort(404)
    ext = os.path.splitext(full)[1].lower()
    dl = f"cv-application-{application_id}{ext}"
    inline = ext == ".pdf" and (request.args.get("download") or "").strip() != "1"
    mimetype = None
    if ext == ".pdf":
        mimetype = "application/pdf"
    elif ext == ".doc":
        mimetype = "application/msword"
    elif ext == ".docx":
        mimetype = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    return send_file(
        full,
        mimetype=mimetype,
        as_attachment=not inline,
        download_name=dl,
    )


@public_site_bp.post("/recruitment/applicant/applications/<int:application_id>/cv")
@_applicant_required
def applicant_upload_cv(application_id):
    uid = int(_applicant_user()["id"])
    app_row = rec_svc.get_application_for_applicant(application_id, uid)
    if not app_row:
        _flash_public("Application not found.", error=True)
        return redirect(url_for("public_recruitment_site.applicant_dashboard"))
    file = request.files.get("cv")
    up = _save_cv_upload(file)
    if not up:
        _flash_public("Upload a PDF, DOC, or DOCX file.", error=True)
        return redirect(
            url_for("public_recruitment_site.applicant_application", application_id=application_id)
        )
    rec_svc.update_application_cv(application_id, uid, up)
    _flash_public("CV updated.")
    return redirect(
        url_for("public_recruitment_site.applicant_application", application_id=application_id)
    )


@public_site_bp.post("/recruitment/applicant/applications/<int:application_id>/policies-ack")
@_applicant_required
def applicant_policies_ack(application_id):
    uid = int(_applicant_user()["id"])
    ok, msg = rec_svc.applicant_set_policies_acknowledged(application_id, uid)
    _flash_public("Policies acknowledged — thank you." if ok else msg, error=not ok)
    return redirect(
        url_for("public_recruitment_site.applicant_application", application_id=application_id)
    )


@public_site_bp.post(
    "/recruitment/applicant/applications/<int:application_id>/prehire/<int:request_id>/upload"
)
@_applicant_required
def applicant_prehire_upload(application_id, request_id):
    uid = int(_applicant_user()["id"])
    fs = request.files.get("file")
    rel = _save_prehire_file_upload(fs)
    if not rel:
        _flash_public("Upload a supported file (PDF, Word, or image).", error=True)
        return redirect(
            url_for("public_recruitment_site.applicant_application", application_id=application_id)
        )
    ok, msg = rec_svc.applicant_save_prehire_upload(
        application_id, uid, request_id, rel, fs.filename if fs else None
    )
    _flash_public("Document uploaded." if ok else msg, error=not ok)
    return redirect(
        url_for("public_recruitment_site.applicant_application", application_id=application_id)
    )


@public_site_bp.post(
    "/recruitment/applicant/applications/<int:application_id>/prehire/<int:request_id>/policy-ack"
)
@_applicant_required
def applicant_prehire_policy_ack(application_id, request_id):
    uid = int(_applicant_user()["id"])
    ok, msg = rec_svc.applicant_confirm_prehire_policy_ack(application_id, uid, request_id)
    _flash_public("Recorded — awaiting HR confirmation." if ok else msg, error=not ok)
    return redirect(
        url_for("public_recruitment_site.applicant_application", application_id=application_id)
    )


@public_site_bp.get("/recruitment/applicant/tasks/<int:task_id>")
@_applicant_required
def applicant_task(task_id):
    uid = int(_applicant_user()["id"])
    task = rec_svc.get_task_for_applicant(task_id, uid)
    if not task:
        _flash_public("Task not found.", error=True)
        return redirect(url_for("public_recruitment_site.applicant_dashboard"))
    schema_fields = task.get("schema_fields") or []
    has_file_field = any(
        (f.get("type") or "").strip().lower() == "file" for f in schema_fields
    )
    return render_template(
        "recruitment_module/public/applicant_task.html",
        task=task,
        application_id=task.get("application_id"),
        title=task.get("template_name") or "Form",
        has_file_field=has_file_field,
        config=_core_manifest,
        website_settings=_get_website_settings(),
        applicant=_applicant_user(),
    )


@public_site_bp.post("/recruitment/applicant/tasks/<int:task_id>")
@_applicant_required
def applicant_task_post(task_id):
    uid = int(_applicant_user()["id"])
    task = rec_svc.get_task_for_applicant(task_id, uid)
    if not task:
        _flash_public("Task not found.", error=True)
        return redirect(url_for("public_recruitment_site.applicant_dashboard"))
    fields = task.get("schema_fields") or []
    response = rec_svc.collect_task_response_from_form(fields, request.form)
    for f in fields:
        if (f.get("type") or "").strip().lower() != "file":
            continue
        name = f.get("name")
        if not name:
            continue
        fs = request.files.get(name)
        rel = _save_task_form_file_upload(fs) if fs and fs.filename else None
        if fs and fs.filename and not rel:
            _flash_public(
                "One or more files were not accepted. Use PDF, Word, or a common image type.",
                error=True,
            )
            return redirect(url_for("public_recruitment_site.applicant_task", task_id=task_id))
        if f.get("required") and not rel:
            _flash_public(f"Please add the required file: {f.get('label') or name}.", error=True)
            return redirect(url_for("public_recruitment_site.applicant_task", task_id=task_id))
        response[name] = rel or ""
    ok, msg = rec_svc.submit_task_response(task_id, uid, response)
    if not ok:
        _flash_public(msg, error=True)
        return redirect(url_for("public_recruitment_site.applicant_task", task_id=task_id))
    _flash_public("Thank you — your response was saved.")
    return redirect(
        url_for(
            "public_recruitment_site.applicant_application",
            application_id=task["application_id"],
        )
    )


# =============================================================================
# Admin
# =============================================================================


@internal_bp.get("/")
@login_required
@_rec_require_view
def admin_index():
    stats = rec_svc.admin_recruitment_dashboard_stats()
    board = rec_svc.admin_list_openings_dashboard(q=None, status_filter="all")
    apps = rec_svc.admin_list_applications(limit=50)
    return render_template(
        "recruitment_module/admin/index.html",
        module_name="Recruitment",
        module_description="Job roles, openings, applicants, screening forms, and hire-to-contractor.",
        plugin_system_name="recruitment_module",
        openings_count=stats.get("openings_open", 0),
        stats=stats,
        openings_preview=board[:12],
        applications_recent=apps,
        applicant_retention_days=rec_svc.recruitment_applicant_retention_days(),
        stages=rec_svc.STAGES_ORDER,
        stage_labels=rec_svc.STAGE_LABELS,
        config=_core_manifest,
    )


@internal_bp.get("/roles")
@login_required
@_rec_require_view
def admin_roles():
    roles = rec_svc.admin_list_roles()
    return render_template(
        "recruitment_module/admin/roles.html",
        roles=roles,
        config=_core_manifest,
    )


@internal_bp.post("/roles/sync-from-time-billing")
@login_required
@_rec_require(REC_SETUP)
def admin_roles_sync_from_time_billing():
    ok, msg, st = rec_svc.sync_rec_job_roles_from_time_billing_roles()
    if ok:
        flash(
            "Time Billing sync: "
            f"{st.get('inserted', 0)} added, {st.get('updated', 0)} linked to existing titles, "
            f"{st.get('already_linked', 0)} already matched.",
            "success",
        )
    else:
        flash(f"Sync failed: {msg}", "danger")
    return redirect(url_for("internal_recruitment.admin_roles"))


@internal_bp.route("/roles/new", methods=["GET", "POST"])
@login_required
@_rec_require(REC_SETUP)
def admin_role_new():
    if request.method == "POST":
        ok, msg, rid = rec_svc.admin_save_role(
            None,
            request.form.get("title"),
            request.form.get("description"),
            request.form.get("department"),
            request.form.get("active") == "1",
            request.form.get("slug") or None,
            time_billing_role_id=_form_optional_positive_int("time_billing_role_id"),
            default_wage_rate_card_id=_form_optional_positive_int(
                "default_wage_rate_card_id"
            ),
        )
        if ok:
            flash("Role saved.", "success")
            return redirect(url_for("internal_recruitment.admin_roles"))
        flash(msg, "error")
    return render_template(
        "recruitment_module/admin/role_form.html",
        role=None,
        tb_roles=rec_svc.time_billing_roles_for_select(),
        wage_cards=rec_svc.time_billing_wage_cards_for_select(),
        config=_core_manifest,
    )


@internal_bp.route("/roles/<int:role_id>/edit", methods=["GET", "POST"])
@login_required
@_rec_require(REC_SETUP)
def admin_role_edit(role_id):
    role = rec_svc.admin_get_role(role_id)
    if not role:
        flash("Role not found.", "error")
        return redirect(url_for("internal_recruitment.admin_roles"))
    if request.method == "POST":
        ok, msg, rid = rec_svc.admin_save_role(
            role_id,
            request.form.get("title"),
            request.form.get("description"),
            request.form.get("department"),
            request.form.get("active") == "1",
            request.form.get("slug") or None,
            time_billing_role_id=_form_optional_positive_int("time_billing_role_id"),
            default_wage_rate_card_id=_form_optional_positive_int(
                "default_wage_rate_card_id"
            ),
        )
        if ok:
            flash("Role updated.", "success")
            return redirect(url_for("internal_recruitment.admin_roles"))
        flash(msg, "error")
    return render_template(
        "recruitment_module/admin/role_form.html",
        role=role,
        tb_roles=rec_svc.time_billing_roles_for_select(),
        wage_cards=rec_svc.time_billing_wage_cards_for_select(),
        config=_core_manifest,
    )


@internal_bp.get("/openings")
@login_required
@_rec_require_view
def admin_openings():
    q = (request.args.get("q") or "").strip() or None
    status_filter = (request.args.get("status") or "all").strip().lower()
    if status_filter not in ("all", "open", "draft", "closed"):
        status_filter = "all"
    view = (request.args.get("view") or "board").strip().lower()
    if view not in ("board", "list"):
        view = "board"
    rows = rec_svc.admin_list_openings_dashboard(q=q, status_filter=status_filter)
    return render_template(
        "recruitment_module/admin/openings.html",
        openings=rows,
        q=q or "",
        status_filter=status_filter,
        view=view,
        config=_core_manifest,
    )


@internal_bp.route("/openings/new", methods=["GET", "POST"])
@login_required
@_rec_require(REC_SETUP)
def admin_opening_new():
    roles = rec_svc.admin_list_roles()
    if not roles:
        flash("Create a job role first.", "error")
        return redirect(url_for("internal_recruitment.admin_roles"))
    if request.method == "POST":
        jid = int(request.form.get("job_role_id") or 0)
        ok, msg, oid = rec_svc.admin_save_opening(
            None,
            jid,
            request.form.get("title"),
            request.form.get("slug") or None,
            request.form.get("summary"),
            request.form.get("description"),
            request.form.get("status") or "draft",
            request.form.get("published_at") or None,
            request.form.get("closes_at") or None,
            rec_svc.parse_optional_positive_int(request.form.get("max_applicants")),
            rec_svc.parse_optional_positive_int(request.form.get("positions_to_fill")),
            listing_pay_mode=request.form.get("listing_pay_mode"),
            listing_pay_wage_card_id=_form_optional_positive_int(
                "listing_pay_wage_card_id"
            ),
            listing_pay_custom_text=request.form.get("listing_pay_custom_text"),
            hire_employment_type=request.form.get("hire_employment_type"),
        )
        if ok:
            flash(
                "Job position created. Use Actions on the board to edit, set stages, or view applications.",
                "success",
            )
            return redirect(
                url_for(
                    "internal_recruitment.admin_openings",
                    view="board",
                    status="all",
                )
            )
        flash(msg, "error")
    return render_template(
        "recruitment_module/admin/opening_form.html",
        opening=None,
        roles=roles,
        wage_cards=rec_svc.time_billing_wage_cards_for_select(),
        config=_core_manifest,
    )


@internal_bp.route("/openings/<int:opening_id>/edit", methods=["GET", "POST"])
@login_required
@_rec_require(REC_SETUP)
def admin_opening_edit(opening_id):
    opening = rec_svc.admin_get_opening(opening_id)
    if not opening:
        flash("Opening not found.", "error")
        return redirect(url_for("internal_recruitment.admin_openings"))
    roles = rec_svc.admin_list_roles()
    if request.method == "POST":
        jid = int(request.form.get("job_role_id") or 0)
        ok, msg, oid = rec_svc.admin_save_opening(
            opening_id,
            jid,
            request.form.get("title"),
            request.form.get("slug") or None,
            request.form.get("summary"),
            request.form.get("description"),
            request.form.get("status") or "draft",
            request.form.get("published_at") or None,
            request.form.get("closes_at") or None,
            rec_svc.parse_optional_positive_int(request.form.get("max_applicants")),
            rec_svc.parse_optional_positive_int(request.form.get("positions_to_fill")),
            listing_pay_mode=request.form.get("listing_pay_mode"),
            listing_pay_wage_card_id=_form_optional_positive_int(
                "listing_pay_wage_card_id"
            ),
            listing_pay_custom_text=request.form.get("listing_pay_custom_text"),
            hire_employment_type=request.form.get("hire_employment_type"),
        )
        if ok:
            flash("Opening updated.", "success")
            return redirect(url_for("internal_recruitment.admin_opening_edit", opening_id=opening_id))
        flash(msg, "error")
    return render_template(
        "recruitment_module/admin/opening_form.html",
        opening=opening,
        roles=roles,
        wage_cards=rec_svc.time_billing_wage_cards_for_select(),
        config=_core_manifest,
    )


@internal_bp.route("/openings/<int:opening_id>/rules", methods=["GET", "POST"])
@login_required
@_rec_require(REC_SETUP)
def admin_opening_rules(opening_id):
    opening = rec_svc.admin_get_opening(opening_id)
    if not opening:
        flash("Opening not found.", "error")
        return redirect(url_for("internal_recruitment.admin_openings"))
    templates = rec_svc.admin_list_templates()
    rules = rec_svc.admin_list_rules(opening_id)
    if request.method == "POST":
        action = request.form.get("action")
        if action == "delete":
            rid = int(request.form.get("rule_id") or 0)
            if rec_svc.admin_delete_rule(rid, opening_id):
                flash("Rule removed.", "success")
            return redirect(
                url_for("internal_recruitment.admin_opening_rules", opening_id=opening_id)
            )
        tid = int(request.form.get("form_template_id") or 0)
        if tid <= 0:
            flash("Select a form template.", "error")
            return redirect(
                url_for("internal_recruitment.admin_opening_rules", opening_id=opening_id)
            )
        ok, msg = rec_svc.admin_save_rule(
            int(request.form.get("rule_id") or 0) or None,
            opening_id,
            request.form.get("trigger_stage") or "applied",
            tid,
            request.form.get("auto_assign") == "1",
            int(request.form.get("sort_order") or 0),
        )
        flash("Rule saved." if ok else msg, "success" if ok else "error")
        return redirect(
            url_for("internal_recruitment.admin_opening_rules", opening_id=opening_id)
        )
    return render_template(
        "recruitment_module/admin/opening_rules.html",
        opening=opening,
        rules=rules,
        templates=templates,
        stages=rec_svc.STAGES_ORDER,
        stage_labels=rec_svc.STAGE_LABELS,
        config=_core_manifest,
    )


@internal_bp.get("/form-templates")
@login_required
@_rec_require_view
def admin_templates():
    rows = rec_svc.admin_list_templates()
    return render_template(
        "recruitment_module/admin/templates_list.html", templates=rows, config=_core_manifest
    )


@internal_bp.route("/form-templates/new", methods=["GET", "POST"])
@login_required
@_rec_require(REC_SETUP)
def admin_template_new():
    if request.method == "POST":
        schema_json, err = rec_svc.build_schema_json_from_builder_form(request.form)
        if err:
            flash(err, "error")
            return render_template(
                "recruitment_module/admin/template_form.html",
                template=None,
                builder_fields=rec_svc.builder_rows_from_post(request.form),
                form_meta={
                    "name": (request.form.get("name") or "").strip(),
                    "purpose": request.form.get("purpose") or "survey",
                    "active": request.form.get("active") == "1",
                },
                type_labels=rec_svc.RECRUITMENT_BUILDER_TYPE_LABELS,
                type_order=rec_svc.RECRUITMENT_BUILDER_FIELD_TYPES,
                config=_core_manifest,
            )
        ok, msg, tid = rec_svc.admin_save_template(
            None,
            request.form.get("name"),
            request.form.get("purpose") or "survey",
            schema_json,
            request.form.get("active") == "1",
        )
        if ok:
            flash("Template saved.", "success")
            return redirect(url_for("internal_recruitment.admin_templates"))
        flash(msg, "error")
    return render_template(
        "recruitment_module/admin/template_form.html",
        template=None,
        builder_fields=rec_svc.default_template_builder_rows(),
        form_meta={"name": "", "purpose": "survey", "active": True},
        type_labels=rec_svc.RECRUITMENT_BUILDER_TYPE_LABELS,
        type_order=rec_svc.RECRUITMENT_BUILDER_FIELD_TYPES,
        config=_core_manifest,
    )


@internal_bp.route("/form-templates/<int:template_id>/edit", methods=["GET", "POST"])
@login_required
@_rec_require(REC_SETUP)
def admin_template_edit(template_id):
    row = rec_svc.admin_get_template(template_id)
    if not row:
        flash("Template not found.", "error")
        return redirect(url_for("internal_recruitment.admin_templates"))
    if request.method == "POST":
        schema_json, err = rec_svc.build_schema_json_from_builder_form(request.form)
        if err:
            flash(err, "error")
            return render_template(
                "recruitment_module/admin/template_form.html",
                template=row,
                builder_fields=rec_svc.builder_rows_from_post(request.form),
                form_meta={
                    "name": (request.form.get("name") or "").strip(),
                    "purpose": request.form.get("purpose") or "survey",
                    "active": request.form.get("active") == "1",
                },
                type_labels=rec_svc.RECRUITMENT_BUILDER_TYPE_LABELS,
                type_order=rec_svc.RECRUITMENT_BUILDER_FIELD_TYPES,
                config=_core_manifest,
            )
        ok, msg, tid = rec_svc.admin_save_template(
            template_id,
            request.form.get("name"),
            request.form.get("purpose") or "survey",
            schema_json,
            request.form.get("active") == "1",
        )
        if ok:
            flash("Template updated.", "success")
            return redirect(url_for("internal_recruitment.admin_templates"))
        flash(msg, "error")
        return render_template(
            "recruitment_module/admin/template_form.html",
            template=row,
            builder_fields=rec_svc.builder_rows_from_post(request.form),
            form_meta={
                "name": (request.form.get("name") or "").strip(),
                "purpose": request.form.get("purpose") or "survey",
                "active": request.form.get("active") == "1",
            },
            type_labels=rec_svc.RECRUITMENT_BUILDER_TYPE_LABELS,
            type_order=rec_svc.RECRUITMENT_BUILDER_FIELD_TYPES,
            config=_core_manifest,
        )
    return render_template(
        "recruitment_module/admin/template_form.html",
        template=row,
        builder_fields=rec_svc.prepare_template_builder_rows(row.get("schema_json")),
        form_meta={
            "name": row.get("name") or "",
            "purpose": row.get("purpose") or "survey",
            "active": bool(row.get("active", 1)),
        },
        type_labels=rec_svc.RECRUITMENT_BUILDER_TYPE_LABELS,
        type_order=rec_svc.RECRUITMENT_BUILDER_FIELD_TYPES,
        config=_core_manifest,
    )


@internal_bp.post("/maintenance/run-applicant-retention")
@login_required
@_rec_require(REC_SETUP)
def admin_run_applicant_retention():
    """GDPR-style purge of recruitment-only PII after retention period (scheduled or manual)."""
    stats = rec_svc.run_recruitment_data_retention_purge(_app_static_dir())
    err = stats.get("error")
    flash(
        (
            f"Retention run: {stats.get('applications_deleted', 0)} application(s) removed, "
            f"{stats.get('applicants_anonymized', 0)} applicant account(s) anonymised."
        ),
        "success" if not err else "error",
    )
    if err:
        flash(str(err), "error")
    return redirect(url_for("internal_recruitment.admin_index"))


@internal_bp.get("/applications")
@login_required
@_rec_require_view
def admin_applications():
    oid = request.args.get("opening_id")
    oid_i = int(oid) if oid and oid.isdigit() else None
    q = (request.args.get("q") or "").strip() or None
    rows = rec_svc.admin_list_applications(opening_id=oid_i, q=q)
    openings = rec_svc.admin_list_openings()
    dash = rec_svc.admin_recruitment_dashboard_stats()
    return render_template(
        "recruitment_module/admin/applications.html",
        applications=rows,
        openings=openings,
        filter_opening_id=oid_i,
        q=q or "",
        multi_pipeline_applicants=int(dash.get("multi_application_applicants") or 0),
        stages=rec_svc.STAGES_ORDER,
        stage_labels=rec_svc.STAGE_LABELS,
        config=_core_manifest,
    )


@internal_bp.post("/applications/bulk-stage")
@login_required
@_rec_require(REC_APPS)
def admin_applications_bulk_stage():
    raw_ids = request.form.getlist("app_ids")
    stage = (request.form.get("bulk_stage") or "").strip().lower()
    oid = (request.form.get("return_opening_id") or "").strip()
    q = (request.form.get("return_q") or "").strip()
    ids = []
    for x in raw_ids:
        if str(x).strip().isdigit():
            ids.append(int(x))
    if not ids:
        flash("Select at least one application.", "error")
    elif stage not in rec_svc.STAGES_ORDER:
        flash("Invalid stage.", "error")
    else:
        n_ok, errs = rec_svc.admin_bulk_set_application_stage(ids, stage)
        flash(
            f"Updated stage for {n_ok} application(s).",
            "success" if n_ok else "warning",
        )
        if errs:
            flash("Some rows failed: " + "; ".join(errs[:5]), "error")
    redir = url_for("internal_recruitment.admin_applications")
    params = []
    if oid.isdigit():
        params.append(f"opening_id={int(oid)}")
    if q:
        params.append("q=" + quote(q))
    if params:
        redir = redir + "?" + "&".join(params)
    return redirect(redir)


@internal_bp.route("/settings/interview-locations", methods=["GET", "POST"])
@login_required
@_rec_require(REC_SETUP)
def admin_interview_presets():
    """Saved office / meeting-place text for quick fill on application interview details."""
    if request.method == "POST":
        action = (request.form.get("action") or "add").strip().lower()
        if action == "delete":
            pid = int(request.form.get("preset_id") or 0)
            ok, msg = rec_svc.admin_delete_interview_location_preset(pid)
            flash("Preset removed." if ok else msg, "success" if ok else "error")
        else:
            ok, msg = rec_svc.admin_add_interview_location_preset(
                request.form.get("label"),
                request.form.get("location_text"),
            )
            flash("Preset added." if ok else msg, "success" if ok else "error")
        return redirect(url_for("internal_recruitment.admin_interview_presets"))
    presets = rec_svc.list_interview_location_presets(active_only=False)
    return render_template(
        "recruitment_module/admin/interview_presets.html",
        presets=presets,
        config=_core_manifest,
    )


@internal_bp.get("/applications/<int:application_id>/cv")
@login_required
@_rec_require_view
def admin_application_cv_file(application_id):
    """
    Serve the applicant CV for an application (admin only).
    Path must be under uploads/recruitment_cv/ — not a raw public static URL.
    """
    row = rec_svc.admin_get_application(application_id)
    if not row or not row.get("cv_path"):
        abort(404)
    rel = str(row["cv_path"]).replace("\\", "/").strip().lstrip("/")
    full = _resolve_recruitment_cv_file(rel)
    if not full:
        abort(404)
    ext = os.path.splitext(full)[1].lower()
    dl = f"application-{application_id}-cv{ext}"
    inline = ext == ".pdf" and (request.args.get("download") or "").strip() != "1"
    mimetype = None
    if ext == ".pdf":
        mimetype = "application/pdf"
    elif ext == ".doc":
        mimetype = "application/msword"
    elif ext == ".docx":
        mimetype = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    return send_file(
        full,
        mimetype=mimetype,
        as_attachment=not inline,
        download_name=dl,
    )


@internal_bp.route("/applications/<int:application_id>", methods=["GET", "POST"])
@login_required
@_rec_require_view
def admin_application_detail(application_id):
    row = rec_svc.admin_get_application(application_id)
    if not row:
        flash("Application not found.", "error")
        return redirect(url_for("internal_recruitment.admin_applications"))
    tasks = rec_svc.admin_list_application_tasks(application_id)
    templates = rec_svc.admin_list_templates()
    prehire = rec_svc.list_prehire_requests_admin(application_id)
    hire_ok, hire_msg = rec_svc.hire_precheck(application_id)
    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        hire_actions = frozenset({"authorize_hire", "hire"})
        if action in hire_actions:
            if not _rec_may(current_user, REC_HIRE):
                flash(
                    "You do not have permission to authorise hire or create employees.",
                    "danger",
                )
                return redirect(
                    url_for(
                        "internal_recruitment.admin_application_detail",
                        application_id=application_id,
                    )
                )
        elif action:
            if not _rec_may(current_user, REC_APPS):
                flash(
                    "You do not have permission to change applications or pipeline data.",
                    "danger",
                )
                return redirect(
                    url_for(
                        "internal_recruitment.admin_application_detail",
                        application_id=application_id,
                    )
                )
        if action == "stage":
            ok, msg = rec_svc.set_application_stage(
                application_id, request.form.get("stage") or ""
            )
            flash("Stage updated." if ok else msg, "success" if ok else "error")
        elif action == "assign":
            tid = int(request.form.get("form_template_id") or 0)
            if tid <= 0:
                flash("Select a form template.", "error")
            else:
                ok, msg = (
                    rec_svc.force_assign_task(application_id, tid)
                    if request.form.get("force") == "1"
                    else rec_svc.create_manual_task(application_id, tid)
                )
                flash("Form assigned." if ok else msg, "success" if ok else "error")
                if ok:
                    try:
                        from . import notifications as rec_notifications

                        rec_notifications.notify_applicant_new_task(application_id)
                    except Exception:
                        pass
        elif action == "notes":
            rec_svc.admin_set_application_notes(
                application_id, request.form.get("admin_notes")
            )
            flash("Notes saved.", "success")
        elif action == "interview_details":
            ok_iv, msg_iv = rec_svc.admin_set_application_interview_details(
                application_id,
                request.form.get("interview_format"),
                request.form.get("interview_meeting_url"),
                request.form.get("interview_location"),
            )
            flash(
                "Interview details saved." if ok_iv else msg_iv,
                "success" if ok_iv else "error",
            )
        elif action == "prehire_add":
            ok, msg, new_rid = rec_svc.admin_create_prehire_request(
                application_id,
                request.form.get("prehire_title") or "",
                request.form.get("prehire_description"),
                request.form.get("prehire_type") or "other",
                None,
            )
            flash("Pre-hire request added." if ok else msg, "success" if ok else "error")
            if ok and new_rid:
                try:
                    from . import notifications as rec_notifications

                    rec_notifications.notify_applicant_prehire_request_added(
                        application_id, int(new_rid)
                    )
                except Exception:
                    pass
        elif action == "prehire_approve":
            rid = int(request.form.get("request_id") or 0)
            ok, msg = rec_svc.admin_prehire_approve(
                rid, application_id, request.form.get("prehire_admin_notes")
            )
            flash("Marked approved." if ok else msg, "success" if ok else "error")
            if ok and rid:
                try:
                    from . import notifications as rec_notifications

                    tit = rec_notifications.fetch_prehire_request_title(
                        rid, application_id
                    )
                    rec_notifications.notify_prehire_outcome(
                        application_id,
                        approved=True,
                        request_title=tit,
                        admin_notes=request.form.get("prehire_admin_notes"),
                    )
                except Exception:
                    pass
        elif action == "prehire_reject":
            rid = int(request.form.get("request_id") or 0)
            ok, msg = rec_svc.admin_prehire_reject(
                rid, application_id, request.form.get("prehire_admin_notes")
            )
            flash("Marked rejected — applicant can re-submit." if ok else msg, "success" if ok else "error")
            if ok and rid:
                try:
                    from . import notifications as rec_notifications

                    tit = rec_notifications.fetch_prehire_request_title(
                        rid, application_id
                    )
                    rec_notifications.notify_prehire_outcome(
                        application_id,
                        approved=False,
                        request_title=tit,
                        admin_notes=request.form.get("prehire_admin_notes"),
                    )
                except Exception:
                    pass
        elif action == "authorize_hire":
            uid = str(getattr(current_user, "id", "") or "")
            ok, msg = rec_svc.admin_authorize_hr_conversion(application_id, uid)
            flash("Hire authorized — you may create the contractor when ready." if ok else msg, "success" if ok else "error")
        elif action == "hire":
            ok, msg, cid = rec_svc.hire_application_as_contractor(
                application_id,
                request.form.get("contractor_role") or "staff",
                (request.form.get("password") or "").strip() or None,
                _app_static_dir(),
                override_time_billing_role_id=_form_optional_positive_int("tb_role_id"),
                override_wage_rate_card_id=_form_optional_positive_int("wage_card_id"),
            )
            flash(
                f"Hired — contractor #{cid}. Documents copied to HR; applicant recruitment data will purge after {rec_svc.recruitment_applicant_retention_days()} days."
                if ok
                else msg,
                "success" if ok else "error",
            )
            if ok and cid:
                try:
                    from . import notifications as rec_notifications

                    rec_notifications.notify_applicant_hired(application_id, int(cid))
                    rec_notifications.send_post_hire_portal_welcome_message(
                        int(cid), (row.get("opening_title") or "").strip()
                    )
                except Exception:
                    pass
        return redirect(
            url_for("internal_recruitment.admin_application_detail", application_id=application_id)
        )
    progress = rec_svc.application_progress(row.get("stage") or "applied")
    hire_preview = None
    tb_roles_hire = []
    wage_cards_hire = []
    if not row.get("contractor_id"):
        hire_preview = rec_svc.admin_preview_hire_billing(application_id)
        tb_roles_hire = rec_svc.time_billing_roles_for_select()
        wage_cards_hire = rec_svc.time_billing_wage_cards_for_select()
    interview_presets = rec_svc.list_interview_location_presets(active_only=True)
    _ind_ids = normalize_organization_industries(
        current_app.config.get("organization_industries")
    )
    prehire_type_add_choices = rec_svc.prehire_type_add_choices(_ind_ids)
    return render_template(
        "recruitment_module/admin/application_detail.html",
        application=row,
        interview_presets=interview_presets,
        tasks=tasks,
        templates=templates,
        prehire_requests=prehire,
        prehire_type_labels=rec_svc.PREHIRE_REQUEST_TYPE_LABELS,
        prehire_type_add_choices=prehire_type_add_choices,
        hire_precheck_ok=hire_ok,
        hire_precheck_message=hire_msg,
        hire_preview=hire_preview,
        tb_roles_hire=tb_roles_hire,
        wage_cards_hire=wage_cards_hire,
        retention_days=rec_svc.recruitment_applicant_retention_days(),
        stages=rec_svc.STAGES_ORDER,
        stage_labels=rec_svc.STAGE_LABELS,
        progress=progress,
        config=_core_manifest,
    )


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_site_bp
