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
from app.portal_session import contractor_id_from_tb_user

from . import services as comp_svc
from .audit_bridge import (
    note_document_type_admin_action,
    note_policy_admin_action,
    note_policy_contractor_acknowledge,
)

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
    return contractor_id_from_tb_user(session.get("tb_user"))


def _admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for("routes.login"))
        role = (getattr(current_user, "role", None) or "").lower()
        if role not in ("admin", "superuser", "support_break_glass"):
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


def _compliance_actor_label() -> Optional[str]:
    try:
        if not current_user.is_authenticated:
            return None
    except Exception:
        return None
    for attr in ("email", "name", "username"):
        v = getattr(current_user, attr, None)
        if v:
            return str(v)[:255]
    uid = getattr(current_user, "id", None)
    return f"user_id:{uid}" if uid is not None else None


# =============================================================================
# Public (contractor / employee portal session)
# =============================================================================


def _int_or_none(val):
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


@public_bp.get("/")
@_staff_required
def public_index():
    cid = _contractor_id()
    doc_type_filter = _int_or_none(request.args.get("type"))
    pending = (
        comp_svc.list_pending_policies_for_contractor(cid, doc_type_filter)
        if cid
        else []
    )
    library = (
        comp_svc.list_acknowledged_mandatory_for_contractor(cid, doc_type_filter)
        if cid
        else []
    )
    published_other = comp_svc.list_optional_published_policies(doc_type_filter)
    doc_types = comp_svc.list_document_types(active_only=True)
    return render_template(
        "compliance_module/public/index.html",
        pending=pending,
        acknowledged_library=library,
        optional_policies=published_other,
        pending_count=len(pending),
        document_types=doc_types,
        document_type_filter=doc_type_filter,
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
        topic_display=comp_svc.policy_topic_display(row),
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
    ok, msg, ver_inserted = comp_svc.acknowledge_policy(
        cid,
        policy_id,
        remote_addr=request.remote_addr,
        user_agent=request.headers.get("User-Agent"),
    )
    if ok and ver_inserted is not None:
        prow = comp_svc.get_published_policy(policy_id)
        note_policy_contractor_acknowledge(
            policy_id=int(policy_id),
            policy_title=(prow.get("title") or "") if prow else "",
            contractor_id=int(cid),
            version=int(ver_inserted),
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
    metrics = comp_svc.admin_compliance_dashboard_metrics()
    doc_type_filter = _int_or_none(request.args.get("type"))
    review_filter = (request.args.get("review") or "").strip().lower() or None
    _allowed_review = {"overdue", "due30", "draft", "published", "unscheduled"}
    if review_filter and review_filter not in _allowed_review:
        review_filter = None
    rows = comp_svc.admin_list_policies(
        document_type_id=doc_type_filter,
        review_filter=review_filter,
    )
    doc_types = comp_svc.list_document_types(active_only=True)
    return render_template(
        "compliance_module/admin/index.html",
        policies=rows,
        metrics=metrics,
        document_types=doc_types,
        document_type_filter=doc_type_filter,
        review_filter=review_filter,
        config=_core_manifest,
    )


@internal_bp.route("/document-types", methods=["GET", "POST"])
@login_required
@_admin_required
def admin_document_types():
    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()
        if action == "add":
            ok, msg, new_tid = comp_svc.admin_create_document_type(
                request.form.get("label"),
                request.form.get("slug") or None,
            )
            flash(msg if ok else msg, "success" if ok else "error")
            if ok and new_tid:
                note_document_type_admin_action(
                    "create",
                    type_id=int(new_tid),
                    label=request.form.get("label") or "",
                )
        elif action == "save":
            tid = _int_or_none(request.form.get("type_id"))
            if tid:
                try:
                    sort_o = int(request.form.get("sort_order") or 0)
                except ValueError:
                    sort_o = 0
                ok, msg = comp_svc.admin_update_document_type(
                    tid,
                    request.form.get("label") or "",
                    sort_o,
                    request.form.get("active") == "1",
                )
                flash("Saved." if ok else msg, "success" if ok else "error")
                if ok:
                    note_document_type_admin_action(
                        "update",
                        type_id=int(tid),
                        label=request.form.get("label") or "",
                    )
            else:
                flash("Invalid type.", "error")
        return redirect(url_for("internal_compliance.admin_document_types"))
    rows = comp_svc.list_document_types(active_only=False)
    return render_template(
        "compliance_module/admin/document_types.html",
        types=rows,
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
            request.form.get("document_type_id"),
            request.form.get("topic"),
            request.form.get("summary"),
            request.form.get("body_text"),
            request.form.get("mandatory") == "1",
            request.form.get("slug") or None,
            request.form.get("next_review_date") or None,
            request.form.get("last_reviewed_date") or None,
            request.form.get("lifecycle_status"),
            lifecycle_change_reason=request.form.get("lifecycle_change_reason"),
            actor_label=_compliance_actor_label(),
        )
        if ok and pid:
            f = request.files.get("file")
            rel = _save_policy_file(f)
            if rel:
                comp_svc.admin_set_policy_file(pid, rel)
            pr = comp_svc.admin_get_policy(pid)
            note_policy_admin_action(
                "create",
                policy_id=int(pid),
                policy_title=(pr.get("title") if pr else None) or request.form.get("title") or "",
                lifecycle_to=comp_svc.normalize_lifecycle(request.form.get("lifecycle_status")),
                version=int((pr or {}).get("version") or 1),
            )
            flash(
                "Document saved. Open it again to set status (Draft / Active / Retired) or use “Save & publish to staff”.",
                "success",
            )
            return redirect(url_for("internal_compliance.admin_policy_edit", policy_id=pid))
        flash(msg, "error")
    type_rows = comp_svc.list_document_types_for_policy_form()
    default_dt = None
    for t in type_rows:
        if (t.get("slug") or "") == "policy":
            default_dt = t.get("id")
            break
    if default_dt is None and type_rows:
        default_dt = type_rows[0].get("id")
    return render_template(
        "compliance_module/admin/policy_form.html",
        policy=None,
        document_types=type_rows,
        default_document_type_id=default_dt,
        lifecycle_labels=comp_svc.LIFECYCLE_LABELS,
        lifecycle_audit=[],
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
        admin_action = (request.form.get("admin_action") or "save").strip().lower()

        def _save_from_form(lifecycle_override=None):
            return comp_svc.admin_save_policy(
                policy_id,
                request.form.get("title"),
                request.form.get("document_type_id"),
                request.form.get("topic"),
                request.form.get("summary"),
                request.form.get("body_text"),
                request.form.get("mandatory") == "1",
                request.form.get("slug") or None,
                request.form.get("next_review_date") or None,
                request.form.get("last_reviewed_date") or None,
                lifecycle_override
                if lifecycle_override is not None
                else request.form.get("lifecycle_status"),
                lifecycle_change_reason=request.form.get("lifecycle_change_reason"),
                actor_label=_compliance_actor_label(),
            )

        def _save_uploaded_file():
            f = request.files.get("file")
            rel = _save_policy_file(f)
            if rel:
                comp_svc.admin_set_policy_file(policy_id, rel)

        if admin_action == "unpublish":
            ok, msg, _ = _save_from_form(lifecycle_override=comp_svc.LIFECYCLE_DRAFT)
            if not ok:
                flash(msg, "error")
                return redirect(url_for("internal_compliance.admin_policy_edit", policy_id=policy_id))
            _save_uploaded_file()
            pr_un = comp_svc.admin_get_policy(policy_id)
            note_policy_admin_action(
                "unpublish",
                policy_id=int(policy_id),
                policy_title=(pr_un.get("title") if pr_un else None) or row.get("title") or "",
                lifecycle_from=comp_svc.policy_effective_lifecycle_state(row),
                lifecycle_to=comp_svc.LIFECYCLE_DRAFT,
                version=int((pr_un or row).get("version") or 1),
                admin_action="unpublish",
            )
            flash(
                "Unpublished — status is now Draft (staff cannot see this document until you publish again).",
                "success",
            )
            return redirect(url_for("internal_compliance.admin_policy_edit", policy_id=policy_id))

        if admin_action == "issue":
            ok, msg, _ = _save_from_form()
            if not ok:
                flash(msg, "error")
                return redirect(url_for("internal_compliance.admin_policy_edit", policy_id=policy_id))
            _save_uploaded_file()
            ok_i, msg_i, meta = comp_svc.admin_issue_policy_to_staff(
                policy_id, _compliance_actor_label()
            )
            if ok_i:
                pr_is = comp_svc.admin_get_policy(policy_id)
                note_policy_admin_action(
                    "issue_to_staff",
                    policy_id=int(policy_id),
                    policy_title=(pr_is.get("title") if pr_is else None) or row.get("title") or "",
                    lifecycle_from=comp_svc.policy_effective_lifecycle_state(row),
                    lifecycle_to=comp_svc.LIFECYCLE_ACTIVE,
                    version=int((meta or {}).get("version") or (pr_is or row).get("version") or 1),
                    admin_action="issue",
                )
            flash(msg_i, "success" if ok_i else "error")
            return redirect(url_for("internal_compliance.admin_policy_edit", policy_id=policy_id))

        ok, msg, _ = _save_from_form()
        if ok:
            _save_uploaded_file()
            pr = comp_svc.admin_get_policy(policy_id)
            new_ls = comp_svc.normalize_lifecycle(request.form.get("lifecycle_status"))
            note_policy_admin_action(
                "update",
                policy_id=int(policy_id),
                policy_title=(pr.get("title") if pr else None) or "",
                lifecycle_from=comp_svc.policy_effective_lifecycle_state(row),
                lifecycle_to=new_ls,
                version=int((pr or {}).get("version") or row.get("version") or 1),
                admin_action="save",
            )
            flash("Saved.", "success")
        else:
            flash(msg, "error")
        return redirect(url_for("internal_compliance.admin_policy_edit", policy_id=policy_id))
    acks = comp_svc.admin_list_acknowledgements_for_policy(policy_id)
    lifecycle_audit = comp_svc.admin_list_policy_lifecycle_audit(policy_id)
    type_rows = comp_svc.list_document_types_for_policy_form(row.get("document_type_id"))
    return render_template(
        "compliance_module/admin/policy_form.html",
        policy=row,
        acknowledgements=acks,
        lifecycle_audit=lifecycle_audit,
        document_types=type_rows,
        default_document_type_id=row.get("document_type_id"),
        lifecycle_labels=comp_svc.LIFECYCLE_LABELS,
        config=_core_manifest,
    )


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_bp
