import os
import uuid
from functools import wraps
from typing import Optional

from flask import (
    Blueprint,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename

from app.objects import PluginManager

from . import services as comp_svc

_plugin_manager = PluginManager(os.path.abspath("app/plugins"))
_core_manifest = _plugin_manager.get_core_manifest() or {}

_template = os.path.join(os.path.dirname(__file__), "templates")

internal_bp = Blueprint(
    "internal_compliance",
    __name__,
    url_prefix="/plugin/compliance_module",
    template_folder=_template,
)
public_bp = Blueprint(
    "public_compliance",
    __name__,
    url_prefix="/compliance",
    template_folder=_template,
)

ALLOWED_POLICY_EXT = {".pdf", ".doc", ".docx"}


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


def _admin_required(view):
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


def _app_static_dir() -> str:
    app_pkg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return os.path.join(app_pkg_dir, "static")


def _save_policy_file(file_storage) -> Optional[str]:
    if not file_storage or not file_storage.filename:
        return None
    ext = os.path.splitext(file_storage.filename)[1].lower()
    if ext not in ALLOWED_POLICY_EXT:
        return None
    upload_dir = os.path.join(_app_static_dir(), "uploads", "compliance_policies")
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = f"{uuid.uuid4().hex}{ext}"
    rel = os.path.join("uploads", "compliance_policies", safe_name).replace("\\", "/")
    file_storage.save(os.path.join(_app_static_dir(), rel.replace("/", os.sep)))
    return rel


# =============================================================================
# Public (contractor / employee portal session)
# =============================================================================


@public_bp.get("/")
@_staff_required
def public_index():
    cid = _contractor_id()
    pending = comp_svc.list_pending_policies_for_contractor(cid) if cid else []
    library = (
        comp_svc.list_acknowledged_mandatory_for_contractor(cid) if cid else []
    )
    published_other = comp_svc.list_optional_published_policies()
    return render_template(
        "compliance_module/public/index.html",
        pending=pending,
        acknowledged_library=library,
        optional_policies=published_other,
        pending_count=len(pending),
        config=_core_manifest,
    )


@public_bp.get("/policy/<int:policy_id>")
@_staff_required
def public_policy_view(policy_id):
    cid = _contractor_id()
    row = comp_svc.get_published_policy(policy_id)
    if not row:
        flash("Policy not found.", "error")
        return redirect(url_for("public_compliance.public_index"))
    ver = int(row["version"])
    acked = comp_svc.contractor_has_acknowledged(cid, policy_id, ver) if cid else False
    ack_at = (
        comp_svc.contractor_acknowledgement_at(cid, policy_id, ver) if cid and acked else None
    )
    return render_template(
        "compliance_module/public/policy_view.html",
        policy=row,
        acknowledged=acked,
        acknowledged_at=ack_at,
        category_label=comp_svc.human_category(row.get("category")),
        config=_core_manifest,
    )


@public_bp.post("/policy/<int:policy_id>/acknowledge")
@_staff_required
def public_policy_acknowledge(policy_id):
    cid = _contractor_id()
    if not cid:
        return redirect(url_for("public_compliance.public_index"))
    if not request.form.get("confirm") == "1":
        flash("You must tick the box to confirm you have read and agree to abide by this policy.", "error")
        return redirect(url_for("public_compliance.public_policy_view", policy_id=policy_id))
    ok, msg = comp_svc.acknowledge_policy(
        cid,
        policy_id,
        remote_addr=request.remote_addr,
        user_agent=request.headers.get("User-Agent"),
    )
    flash("Your acknowledgement has been recorded. Thank you." if ok else msg, "success" if ok else "error")
    return redirect(url_for("public_compliance.public_index"))


@public_bp.get("/policy/<int:policy_id>/file")
@_staff_required
def public_policy_file(policy_id):
    row = comp_svc.get_published_policy(policy_id)
    if not row or not row.get("file_path"):
        abort(404)
    rel = str(row["file_path"]).replace("\\", "/").lstrip("/")
    if ".." in rel.split("/"):
        abort(404)
    full = os.path.abspath(os.path.join(_app_static_dir(), *rel.split("/")))
    root = os.path.abspath(_app_static_dir())
    if not full.startswith(root) or not os.path.isfile(full):
        abort(404)
    return send_file(full, as_attachment=False)


# =============================================================================
# Admin
# =============================================================================


@internal_bp.get("/")
@login_required
@_admin_required
def admin_index():
    rows = comp_svc.admin_list_policies()
    return render_template(
        "compliance_module/admin/index.html",
        policies=rows,
        categories=comp_svc.POLICY_CATEGORIES,
        category_labels=comp_svc.CATEGORY_LABELS,
        config=_core_manifest,
    )


@internal_bp.route("/policies/new", methods=["GET", "POST"])
@login_required
@_admin_required
def admin_policy_new():
    if request.method == "POST":
        ok, msg, pid = comp_svc.admin_save_policy(
            None,
            request.form.get("title"),
            request.form.get("category") or "other",
            request.form.get("summary"),
            request.form.get("body_text"),
            request.form.get("mandatory") == "1",
            request.form.get("slug") or None,
            request.form.get("next_review_date") or None,
            request.form.get("last_reviewed_date") or None,
        )
        if ok and pid:
            f = request.files.get("file")
            rel = _save_policy_file(f)
            if rel:
                comp_svc.admin_set_policy_file(pid, rel)
            flash("Policy saved. Use “Issue to all staff” when ready to publish.", "success")
            return redirect(url_for("internal_compliance.admin_policy_edit", policy_id=pid))
        flash(msg, "error")
    return render_template(
        "compliance_module/admin/policy_form.html",
        policy=None,
        categories=comp_svc.POLICY_CATEGORIES,
        category_labels=comp_svc.CATEGORY_LABELS,
        config=_core_manifest,
    )


@internal_bp.route("/policies/<int:policy_id>/edit", methods=["GET", "POST"])
@login_required
@_admin_required
def admin_policy_edit(policy_id):
    row = comp_svc.admin_get_policy(policy_id)
    if not row:
        flash("Policy not found.", "error")
        return redirect(url_for("internal_compliance.admin_index"))
    if request.method == "POST":
        action = request.form.get("action")
        if action == "issue":
            ok, msg, meta = comp_svc.admin_issue_policy_to_staff(policy_id)
            flash(msg, "success" if ok else "error")
            return redirect(url_for("internal_compliance.admin_policy_edit", policy_id=policy_id))
        if action == "unpublish":
            ok, msg = comp_svc.admin_unpublish_policy(policy_id)
            flash(msg, "success" if ok else "error")
            return redirect(url_for("internal_compliance.admin_policy_edit", policy_id=policy_id))
        ok, msg, _ = comp_svc.admin_save_policy(
            policy_id,
            request.form.get("title"),
            request.form.get("category") or "other",
            request.form.get("summary"),
            request.form.get("body_text"),
            request.form.get("mandatory") == "1",
            request.form.get("slug") or None,
            request.form.get("next_review_date") or None,
            request.form.get("last_reviewed_date") or None,
        )
        if ok:
            f = request.files.get("file")
            rel = _save_policy_file(f)
            if rel:
                comp_svc.admin_set_policy_file(policy_id, rel)
            flash("Saved.", "success")
        else:
            flash(msg, "error")
        return redirect(url_for("internal_compliance.admin_policy_edit", policy_id=policy_id))
    acks = comp_svc.admin_list_acknowledgements_for_policy(policy_id)
    return render_template(
        "compliance_module/admin/policy_form.html",
        policy=row,
        acknowledgements=acks,
        categories=comp_svc.POLICY_CATEGORIES,
        category_labels=comp_svc.CATEGORY_LABELS,
        config=_core_manifest,
    )


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_bp
