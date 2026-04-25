import os
import re
import uuid
from datetime import date
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

from . import services as training_services
from .services import DELIVERY_TYPES, TrainingService

_plugin_manager = PluginManager(os.path.abspath("app/plugins"))
_core_manifest = _plugin_manager.get_core_manifest() or {}

_template = os.path.join(os.path.dirname(__file__), "templates")
internal_bp = Blueprint(
    "internal_training",
    __name__,
    url_prefix="/plugin/training_module",
    template_folder=_template,
)
public_bp = Blueprint(
    "public_training",
    __name__,
    url_prefix="/training",
    template_folder=_template,
)

ALLOWED_LESSON_EXT = {".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp"}
ALLOWED_COMPETENCY_EXT = ALLOWED_LESSON_EXT


def _staff_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("tb_user"):
            return redirect("/employee-portal/login?next=" + request.path)
        return view(*args, **kwargs)

    return wrapped


def _current_contractor_id():
    return contractor_id_from_tb_user(session.get("tb_user"))


def _admin_required_training(view):
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


def _slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-") or "item"


def _get_website_settings():
    try:
        from app.plugins.website_module.routes import get_website_settings

        return get_website_settings()
    except Exception:
        pass
    return {}


def _app_static_dir() -> str:
    app_pkg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return os.path.join(app_pkg_dir, "static")


def _save_training_file(file_storage) -> Optional[str]:
    if not file_storage or not file_storage.filename:
        return None
    ext = os.path.splitext(file_storage.filename)[1].lower()
    if ext not in ALLOWED_LESSON_EXT:
        return None
    upload_dir = os.path.join(_app_static_dir(), "uploads", "training_lessons")
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = f"{uuid.uuid4().hex}{ext}"
    rel = os.path.join("uploads", "training_lessons", safe_name).replace("\\", "/")
    file_storage.save(os.path.join(_app_static_dir(), rel.replace("/", os.sep)))
    return rel


def _save_person_competency_file(file_storage) -> Optional[str]:
    if not file_storage or not file_storage.filename:
        return None
    ext = os.path.splitext(file_storage.filename)[1].lower()
    if ext not in ALLOWED_COMPETENCY_EXT:
        return None
    upload_dir = os.path.join(_app_static_dir(), "uploads", "training_competencies")
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = f"{uuid.uuid4().hex}{ext}"
    rel = os.path.join("uploads", "training_competencies", safe_name).replace("\\", "/")
    file_storage.save(os.path.join(_app_static_dir(), rel.replace("/", os.sep)))
    return rel


def _training_created_by_user_id():
    uid = getattr(current_user, "id", None)
    if uid is None:
        return None
    try:
        return int(uid)
    except (TypeError, ValueError):
        return None


def _may_access_competency_file_download():
    if not current_user.is_authenticated:
        return False
    role = (getattr(current_user, "role", None) or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    try:
        from app.plugins.hr_module.routes import _hr_may_view

        return _hr_may_view(current_user)
    except Exception:
        return False


# ---------- Public (contractor) ----------


@public_bp.get("/")
@_staff_required
def public_index():
    cid = _current_contractor_id()
    if not cid:
        return redirect("/employee-portal/")
    assignments = TrainingService.list_assignments(contractor_id=cid, include_completed=True)
    return render_template(
        "training_module/public/index.html",
        module_name="Training",
        module_description="Complete assigned training, assessments, and evidence uploads.",
        assignments=assignments,
        config=_core_manifest,
        website_settings=_get_website_settings(),
    )


@public_bp.get("/item/<int:assignment_id>")
@_staff_required
def view_item(assignment_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_training.public_index"))
    assignment = TrainingService.get_assignment(assignment_id, contractor_id=cid)
    if not assignment:
        flash("Not found.", "error")
        return redirect(url_for("public_training.public_index"))
    if assignment.get("course_id") is not None:
        return render_template(
            "training_module/public/assignment.html",
            assignment=assignment,
            config=_core_manifest,
            website_settings=_get_website_settings(),
        )
    return render_template(
        "training_module/public/view.html",
        assignment=assignment,
        config=_core_manifest,
        website_settings=_get_website_settings(),
    )


@public_bp.post("/item/<int:assignment_id>/complete")
@_staff_required
def complete_item(assignment_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_training.public_index"))
    notes = (request.form.get("notes") or "").strip() or None
    if TrainingService.mark_complete(assignment_id, cid, notes=notes):
        flash("Training marked complete. Thank you.", "success")
    else:
        flash("Could not complete — use lessons/quiz or submit required evidence.", "warning")
    return redirect(url_for("public_training.public_index"))


@public_bp.post("/item/<int:assignment_id>/lesson/<int:lesson_id>/done")
@_staff_required
def lesson_done(assignment_id, lesson_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_training.public_index"))
    ok, msg = TrainingService.mark_lesson_complete(assignment_id, cid, lesson_id)
    flash(msg, "success" if ok else "error")
    return redirect(url_for("public_training.view_item", assignment_id=assignment_id))


@public_bp.post("/item/<int:assignment_id>/lesson/<int:lesson_id>/quiz")
@_staff_required
def lesson_quiz(assignment_id, lesson_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_training.public_index"))
    picks = []
    for k in request.form.keys():
        if k.startswith("q_"):
            try:
                picks.append(int(request.form.get(k) or 0))
            except ValueError:
                pass
    ok, msg = TrainingService.submit_quiz(assignment_id, cid, lesson_id, picks)
    flash(msg, "success" if ok else "warning")
    return redirect(url_for("public_training.view_item", assignment_id=assignment_id))


@public_bp.post("/item/<int:assignment_id>/certificate")
@_staff_required
def certificate_upload(assignment_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_training.public_index"))
    f = request.files.get("file")
    rel = _save_training_file(f)
    if not rel:
        flash("Upload a PDF or image file.", "error")
        return redirect(url_for("public_training.view_item", assignment_id=assignment_id))
    issued_s = request.form.get("issued_at") or None
    exp_s = request.form.get("expires_at") or None
    issued = date.fromisoformat(issued_s) if issued_s else None
    exp = date.fromisoformat(exp_s) if exp_s else None
    ok, msg = TrainingService.save_certificate(
        assignment_id,
        cid,
        request.form.get("provider"),
        request.form.get("certificate_number"),
        issued,
        exp,
        rel,
    )
    flash(msg, "success" if ok else "error")
    return redirect(url_for("public_training.view_item", assignment_id=assignment_id))


@public_bp.get("/lesson-file/<int:lesson_id>")
@_staff_required
def lesson_file(lesson_id):
    cid = _current_contractor_id()
    if not cid:
        abort(403)
    conn = None
    try:
        from app.objects import get_db_connection

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT l.file_path FROM trn_lessons l
            JOIN trn_modules m ON m.id = l.module_id
            JOIN trn_course_versions v ON v.id = m.course_version_id
            JOIN trn_assignments a ON a.course_version_id = v.id
            WHERE l.id = %s AND a.contractor_id = %s
            LIMIT 1
            """,
            (lesson_id, cid),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        if conn:
            conn.close()
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


# ---------- Admin ----------


@internal_bp.get("/")
@login_required
@_admin_required_training
def admin_index():
    overview = {}
    try:
        overview = TrainingService.admin_overview_metrics(limit_recent=6)
    except Exception:
        overview = {}
    return render_template(
        "training_module/admin/index.html",
        module_name="Training",
        module_description="Courses, assignments, completions, certificates, audit.",
        config=_core_manifest,
        trn_ready=TrainingService._trn_tables_exist(),
        overview=overview,
    )


@internal_bp.get("/courses")
@login_required
@_admin_required_training
def admin_courses():
    if not TrainingService._trn_tables_exist():
        flash("Run training module install/upgrade to enable the new course engine.", "warning")
        return redirect(url_for("internal_training.admin_items"))
    courses = TrainingService.list_courses(active_only=False)
    return render_template(
        "training_module/admin/courses.html",
        courses=courses,
        delivery_types=sorted(DELIVERY_TYPES),
        config=_core_manifest,
    )


@internal_bp.route("/courses/new", methods=["GET", "POST"])
@login_required
@_admin_required_training
def admin_course_new():
    if not TrainingService._trn_tables_exist():
        return redirect(url_for("internal_training.admin_items"))
    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        if not title:
            flash("Title required.", "error")
            return redirect(url_for("internal_training.admin_course_new"))
        cid = TrainingService.create_course(
            title=title,
            slug=request.form.get("slug") or None,
            summary=(request.form.get("summary") or "").strip() or None,
            delivery_type=request.form.get("delivery_type") or "internal",
            grace_days=int(request.form.get("grace_days") or 0),
            comp_policy_id=request.form.get("comp_policy_id", type=int),
            require_certificate_verification=request.form.get("require_verify") == "1",
        )
        flash("Course created.", "success")
        return redirect(url_for("internal_training.admin_course_edit", course_id=cid))
    return render_template(
        "training_module/admin/course_form.html",
        course=None,
        delivery_types=sorted(DELIVERY_TYPES),
        compliance_policies=TrainingService.list_comp_policies_for_course_select(),
        config=_core_manifest,
    )


@internal_bp.route("/courses/<int:course_id>/edit", methods=["GET", "POST"])
@login_required
@_admin_required_training
def admin_course_edit(course_id):
    if not TrainingService._trn_tables_exist():
        return redirect(url_for("internal_training.admin_items"))
    course = TrainingService.get_course(course_id)
    if not course:
        flash("Not found.", "error")
        return redirect(url_for("internal_training.admin_courses"))
    vid = TrainingService.ensure_course_version(course_id)
    modules = TrainingService.list_modules(vid)
    lessons_by_mod = {m["id"]: TrainingService.list_lessons(m["id"]) for m in modules}
    if request.method == "POST":
        action = request.form.get("action")
        if action == "save_course":
            TrainingService.update_course(
                course_id,
                title=(request.form.get("title") or "").strip() or None,
                slug=(request.form.get("slug") or "").strip() or None,
                summary=(request.form.get("summary") or "").strip() or None,
                delivery_type=request.form.get("delivery_type"),
                grace_days=int(request.form.get("grace_days") or 0),
                comp_policy_id=request.form.get("comp_policy_id", type=int),
                require_certificate_verification=request.form.get("require_verify") == "1",
                active=request.form.get("active") == "1",
            )
            flash("Course saved.", "success")
        elif action == "add_lesson":
            mid = request.form.get("module_id", type=int)
            TrainingService.add_lesson(
                mid,
                (request.form.get("lesson_title") or "Lesson").strip(),
                lesson_type=request.form.get("lesson_type") or "text",
                body_text=(request.form.get("body_text") or "").strip() or None,
                external_url=(request.form.get("external_url") or "").strip() or None,
            )
            flash("Lesson added.", "success")
        elif action == "add_question":
            lid = request.form.get("lesson_id", type=int)
            TrainingService.add_question(lid, (request.form.get("question_text") or "").strip())
            flash("Question added.", "success")
        elif action == "add_option":
            qid = request.form.get("question_id", type=int)
            TrainingService.add_option(
                qid,
                (request.form.get("option_text") or "").strip(),
                request.form.get("is_correct") == "1",
            )
            flash("Option added.", "success")
        elif action == "upload_lesson_file":
            lid = request.form.get("lesson_id", type=int)
            f = request.files.get("file")
            rel = _save_training_file(f)
            if rel and lid:
                from app.objects import get_db_connection

                conn = get_db_connection()
                cur = conn.cursor()
                try:
                    cur.execute("UPDATE trn_lessons SET file_path = %s WHERE id = %s", (rel, lid))
                    conn.commit()
                finally:
                    cur.close()
                    conn.close()
                flash("File attached to lesson.", "success")
            else:
                flash("Invalid file or lesson.", "error")

        elif action == "add_rule":
            role_id = request.form.get("role_id", type=int)
            due_offset = request.form.get("due_date_offset_days", default=None, type=int)
            mandatory = request.form.get("mandatory") == "1"
            ok = TrainingService.add_course_assignment_rule(
                course_id=course_id,
                role_id=role_id,
                due_date_offset_days=due_offset,
                mandatory=mandatory,
                active=True,
                created_by_user_id=getattr(current_user, "id", None),
            )
            flash("Assignment rule added." if ok else "Could not add rule.", "success" if ok else "error")
        elif action == "delete_rule":
            rule_id = request.form.get("rule_id", type=int)
            ok = TrainingService.delete_course_assignment_rule(rule_id) if rule_id else False
            flash("Rule deleted." if ok else "Could not delete rule.", "success" if ok else "warning")
        return redirect(url_for("internal_training.admin_course_edit", course_id=course_id))
    questions_by_lesson = {}
    for m in modules:
        for les in lessons_by_mod.get(m["id"], []):
            if (les.get("lesson_type") or "") == "quiz":
                from app.objects import get_db_connection

                conn = get_db_connection()
                cur = conn.cursor(dictionary=True)
                try:
                    cur.execute(
                        "SELECT * FROM trn_questions WHERE lesson_id = %s ORDER BY sort_order, id",
                        (les["id"],),
                    )
                    qs = cur.fetchall() or []
                    for q in qs:
                        cur.execute(
                            "SELECT * FROM trn_question_options WHERE question_id = %s ORDER BY sort_order, id",
                            (q["id"],),
                        )
                        q["options"] = cur.fetchall() or []
                    questions_by_lesson[les["id"]] = qs
                finally:
                    cur.close()
                    conn.close()
    return render_template(
        "training_module/admin/course_edit.html",
        course=course,
        version_id=vid,
        modules=modules,
        lessons_by_mod=lessons_by_mod,
        questions_by_lesson=questions_by_lesson,
        delivery_types=sorted(DELIVERY_TYPES),
        roles=TrainingService.list_roles(),
        assignment_rules=TrainingService.list_course_assignment_rules(course_id),
        compliance_policies=TrainingService.list_comp_policies_for_course_select(),
        config=_core_manifest,
    )


@internal_bp.route("/courses/<int:course_id>/assign", methods=["GET", "POST"])
@login_required
@_admin_required_training
def admin_course_assign(course_id):
    if not TrainingService._trn_tables_exist():
        return redirect(url_for("internal_training.admin_items"))
    course = TrainingService.get_course(course_id)
    if not course:
        flash("Not found.", "error")
        return redirect(url_for("internal_training.admin_courses"))
    contractors = TrainingService.list_contractors()
    roles = TrainingService.list_roles()
    if request.method == "POST":
        mode = request.form.get("mode")
        due_s = request.form.get("due_date") or None
        due = date.fromisoformat(due_s) if due_s else None
        mandatory = request.form.get("mandatory") == "1"
        uid = getattr(current_user, "id", None)
        if mode == "contractor":
            cid = request.form.get("contractor_id", type=int)
            if cid:
                TrainingService.assign_contractor(course_id, cid, due, mandatory, uid)
                flash("Assigned to contractor.", "success")
        elif mode == "role":
            rid = request.form.get("role_id", type=int)
            if rid:
                n = TrainingService.assign_role(course_id, rid, due, mandatory, uid)
                flash(f"Assigned to {n} staff in role.", "success")
        return redirect(url_for("internal_training.admin_assignments", item_id=course_id))
    return render_template(
        "training_module/admin/course_assign.html",
        course=course,
        contractors=contractors,
        roles=roles,
        config=_core_manifest,
    )


@internal_bp.post("/assignments/<int:assignment_id>/verify-cert")
@login_required
@_admin_required_training
def admin_verify_cert(assignment_id):
    if TrainingService.verify_certificate(assignment_id, getattr(current_user, "id", 0) or 0):
        flash("Certificate verified.", "success")
    else:
        flash("No certificate to verify.", "warning")
    return redirect(request.referrer or url_for("internal_training.admin_assignments"))


@internal_bp.post("/assignments/<int:assignment_id>/signoff")
@login_required
@_admin_required_training
def admin_signoff(assignment_id):
    TrainingService.add_signoff(
        assignment_id,
        getattr(current_user, "id", 0) or 0,
        (request.form.get("comments") or "").strip() or None,
    )
    flash("Competency sign-off recorded.", "success")
    return redirect(request.referrer or url_for("internal_training.admin_assignments"))


@internal_bp.route("/assignments/<int:assignment_id>/exempt", methods=["POST"])
@login_required
@_admin_required_training
def admin_exempt(assignment_id):
    from app.objects import get_db_connection

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT course_id, contractor_id FROM trn_assignments WHERE id = %s",
            (assignment_id,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if not row:
        flash("Not found.", "error")
        return redirect(url_for("internal_training.admin_assignments"))
    TrainingService.grant_exemption(
        int(row["course_id"]),
        int(row["contractor_id"]),
        (request.form.get("reason") or "Exempt").strip(),
        getattr(current_user, "id", None),
        None,
    )
    flash("Exemption granted.", "success")
    return redirect(request.referrer or url_for("internal_training.admin_assignments"))


@internal_bp.route("/person-competencies/<int:contractor_id>", methods=["GET", "POST"])
@login_required
@_admin_required_training
def admin_person_competencies(contractor_id):
    if not TrainingService.person_competencies_table_exists():
        flash("Run training module install/upgrade to enable the person competency register.", "warning")
        return redirect(url_for("internal_training.admin_index"))
    cid = int(contractor_id)
    contractors = TrainingService.list_contractors()
    person = next((c for c in contractors if int(c.get("id") or 0) == cid), None)
    if not person:
        flash("Contractor not found.", "error")
        return redirect(url_for("internal_training.admin_index"))
    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()
        if action == "delete":
            comp_id = request.form.get("competency_id", type=int)
            if comp_id and TrainingService.delete_person_competency(comp_id, cid):
                flash("Entry removed.", "success")
            else:
                flash("Could not remove entry.", "error")
        elif action == "add":
            kind = (request.form.get("competency_kind") or "").strip().lower()
            label = (request.form.get("label") or "").strip()
            use_hr = request.form.get("use_hr_job_title") == "1"
            notes = (request.form.get("notes") or "").strip() or None
            issued_s = (request.form.get("issued_on") or "").strip()
            exp_s = (request.form.get("expires_on") or "").strip()
            issued = date.fromisoformat(issued_s) if issued_s else None
            exp = date.fromisoformat(exp_s) if exp_s else None
            f = request.files.get("file")
            file_rel = _save_person_competency_file(f)
            new_id = TrainingService.add_person_competency(
                cid,
                kind,
                label,
                use_hr_job_title=use_hr,
                file_path=file_rel,
                issued_on=issued,
                expires_on=exp,
                notes=notes,
                created_by_user_id=_training_created_by_user_id(),
            )
            if new_id:
                flash("Competency recorded.", "success")
            else:
                flash("Could not save — check kind, label, and clinical grade options.", "error")
        return redirect(url_for("internal_training.admin_person_competencies", contractor_id=cid))
    rows = TrainingService.list_person_competencies(cid)
    return render_template(
        "training_module/admin/person_competencies.html",
        person=person,
        rows=rows,
        competency_kinds=sorted(TrainingService.COMPETENCY_KINDS),
        today_iso=date.today().isoformat(),
        config=_core_manifest,
    )


@internal_bp.get("/person-competencies/<int:contractor_id>/file/<int:competency_id>")
@login_required
def admin_person_competency_file(contractor_id, competency_id):
    if not _may_access_competency_file_download():
        abort(403)
    if not TrainingService.person_competencies_table_exists():
        abort(404)
    cid = int(contractor_id)
    comp_id = int(competency_id)
    rows = TrainingService.list_person_competencies(cid)
    row = next((r for r in rows if int(r.get("id") or 0) == comp_id), None)
    if not row or not row.get("file_path"):
        abort(404)
    rel = str(row["file_path"]).replace("\\", "/").lstrip("/")
    if ".." in rel.split("/"):
        abort(404)
    full = os.path.abspath(os.path.join(_app_static_dir(), *rel.split("/")))
    root = os.path.abspath(_app_static_dir())
    if not full.startswith(root) or not os.path.isfile(full):
        abort(404)
    disp = (request.args.get("disposition") or "").strip().lower()
    return send_file(full, as_attachment=(disp == "attachment"))


@internal_bp.get("/audit")
@login_required
@_admin_required_training
def admin_audit():
    if not TrainingService._trn_tables_exist():
        return redirect(url_for("internal_training.admin_index"))
    from app.objects import get_db_connection

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT * FROM trn_audit_log ORDER BY id DESC LIMIT 300",
        )
        rows = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return render_template("training_module/admin/audit.html", rows=rows, config=_core_manifest)


# --- Legacy admin paths (items) ---


@internal_bp.get("/items")
@login_required
@_admin_required_training
def admin_items():
    if TrainingService._trn_tables_exist():
        return redirect(url_for("internal_training.admin_courses"))
    items = training_services.TrainingService.list_items(active_only=False)
    return render_template(
        "training_module/admin/items.html",
        items=items,
        config=_core_manifest,
    )


@internal_bp.get("/items/new")
@login_required
@_admin_required_training
def admin_item_new():
    if TrainingService._trn_tables_exist():
        return redirect(url_for("internal_training.admin_course_new"))
    return render_template(
        "training_module/admin/item_form.html",
        item=None,
        config=_core_manifest,
    )


@internal_bp.post("/items/new")
@login_required
@_admin_required_training
def admin_item_create():
    if TrainingService._trn_tables_exist():
        return redirect(url_for("internal_training.admin_course_new"))
    title = (request.form.get("title") or "").strip()
    slug = (request.form.get("slug") or "").strip() or _slugify(title)
    summary = (request.form.get("summary") or "").strip() or None
    content = (request.form.get("content") or "").strip() or None
    item_type = request.form.get("item_type") or "document"
    external_url = (request.form.get("external_url") or "").strip() or None
    if not title:
        flash("Title required.", "error")
        return redirect(url_for("internal_training.admin_item_new"))
    try:
        training_services.TrainingService.create_item(
            title=title,
            slug=slug,
            summary=summary,
            content=content,
            item_type=item_type,
            external_url=external_url,
        )
        flash("Training item created.", "success")
        return redirect(url_for("internal_training.admin_items"))
    except Exception as e:
        flash(str(e), "error")
        return redirect(url_for("internal_training.admin_item_new"))


@internal_bp.get("/items/<int:item_id>/edit")
@login_required
@_admin_required_training
def admin_item_edit(item_id):
    if TrainingService._trn_tables_exist():
        return redirect(url_for("internal_training.admin_course_edit", course_id=item_id))
    item = training_services.TrainingService.get_item(item_id)
    if not item:
        flash("Not found.", "error")
        return redirect(url_for("internal_training.admin_items"))
    return render_template(
        "training_module/admin/item_form.html",
        item=item,
        config=_core_manifest,
    )


@internal_bp.post("/items/<int:item_id>/edit")
@login_required
@_admin_required_training
def admin_item_update(item_id):
    if TrainingService._trn_tables_exist():
        return redirect(url_for("internal_training.admin_course_edit", course_id=item_id))
    item = training_services.TrainingService.get_item(item_id)
    if not item:
        flash("Not found.", "error")
        return redirect(url_for("internal_training.admin_items"))
    title = (request.form.get("title") or "").strip()
    slug = (request.form.get("slug") or "").strip()
    summary = (request.form.get("summary") or "").strip() or None
    content = (request.form.get("content") or "").strip() or None
    item_type = request.form.get("item_type")
    external_url = (request.form.get("external_url") or "").strip() or None
    active = None
    if "active" in request.form:
        active = request.form.get("active") == "1"
    if title:
        training_services.TrainingService.update_item(
            item_id,
            title=title,
            slug=slug or None,
            summary=summary,
            content=content,
            item_type=item_type,
            external_url=external_url,
            active=active,
        )
        flash("Training item updated.", "success")
    return redirect(url_for("internal_training.admin_items"))


@internal_bp.get("/assignments")
@login_required
@_admin_required_training
def admin_assignments():
    contractor_id = request.args.get("contractor_id", type=int)
    item_id = request.args.get("item_id", type=int)
    if TrainingService._trn_tables_exist():
        assignments = TrainingService.admin_list_assignments(
            contractor_id=contractor_id,
            course_id=item_id,
            include_completed=True,
        )
    else:
        assignments = training_services.TrainingService.list_assignments(
            contractor_id=contractor_id,
            training_item_id=item_id,
            include_completed=True,
        )
    contractors = training_services.TrainingService.list_contractors()
    items = training_services.TrainingService.list_items(active_only=False)
    return render_template(
        "training_module/admin/assignments.html",
        assignments=assignments,
        contractors=contractors,
        items=items,
        contractor_id=contractor_id,
        item_id=item_id,
        trn=TrainingService._trn_tables_exist(),
        config=_core_manifest,
    )


@internal_bp.get("/assignments/new")
@login_required
@_admin_required_training
def admin_assignment_new():
    if TrainingService._trn_tables_exist():
        flash("Pick a course and use Assign.", "info")
        return redirect(url_for("internal_training.admin_courses"))
    contractors = training_services.TrainingService.list_contractors()
    items = training_services.TrainingService.list_items(active_only=True)
    return render_template(
        "training_module/admin/assignment_form.html",
        contractors=contractors,
        items=items,
        config=_core_manifest,
    )


@internal_bp.post("/assignments/new")
@login_required
@_admin_required_training
def admin_assignment_create():
    if TrainingService._trn_tables_exist():
        return redirect(url_for("internal_training.admin_courses"))
    item_id = request.form.get("training_item_id", type=int)
    contractor_id = request.form.get("contractor_id", type=int)
    due_s = request.form.get("due_date") or None
    mandatory = request.form.get("mandatory") == "1"
    if not item_id or not contractor_id:
        flash("Item and contractor required.", "error")
        return redirect(url_for("internal_training.admin_assignment_new"))
    due_date = date.fromisoformat(due_s) if due_s else None
    try:
        training_services.TrainingService.add_assignment(
            training_item_id=item_id,
            contractor_id=contractor_id,
            due_date=due_date,
            mandatory=mandatory,
            assigned_by_user_id=getattr(current_user, "id", None),
        )
        flash("Assignment created.", "success")
    except Exception as e:
        flash(str(e), "error")
    return redirect(url_for("internal_training.admin_assignments"))


@internal_bp.get("/completions")
@login_required
@_admin_required_training
def admin_completions():
    from datetime import timedelta

    item_id = request.args.get("item_id", type=int)
    contractor_id = request.args.get("contractor_id", type=int)
    date_from_s = request.args.get("date_from")
    date_to_s = request.args.get("date_to")
    date_from = date.fromisoformat(date_from_s) if date_from_s else (date.today() - timedelta(days=30))
    date_to = date.fromisoformat(date_to_s) if date_to_s else date.today()
    rows = training_services.TrainingService.list_completions(
        training_item_id=item_id,
        contractor_id=contractor_id,
        date_from=date_from,
        date_to=date_to,
    )
    items = training_services.TrainingService.list_items(active_only=False)
    contractors = training_services.TrainingService.list_contractors()
    return render_template(
        "training_module/admin/completions.html",
        rows=rows,
        items=items,
        contractors=contractors,
        item_id=item_id,
        contractor_id=contractor_id,
        date_from=date_from,
        date_to=date_to,
        config=_core_manifest,
    )


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_bp
