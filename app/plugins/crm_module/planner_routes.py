# Medical event planner (R3) + checklist question admin
from __future__ import annotations

import json
import os
from datetime import datetime

from flask import (
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)
from flask_login import login_required

from app.objects import get_db_connection

from .crm_common import can_edit, crm_access_required, crm_edit_required, uid
from .crm_cura_links import cura_ops_event_detail_url_for_plan
from .crm_event_plan_handoff import (
    handoff_blocked_reason,
    handoff_mode,
    process_event_plan_handoff,
)
from .crm_event_plan_links import fetch_plan_link_labels
from .crm_clinical_handover import parse_clinical_handover
from .crm_hospital_stub import hospital_suggest_payload
from .crm_static_paths import crm_resolve_event_plan_pdf_path
from .crm_planner_pdf import store_event_plan_pdf_file, write_event_plan_pdf


def register_crm_planner_routes(crm_bp):
    @crm_bp.route("/event-plans")
    @login_required
    @crm_access_required
    def event_plans_list():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT p.id, p.title, p.status, p.wizard_step, p.updated_at,
                       p.quote_id, p.opportunity_id,
                       p.handoff_external_ref, p.handoff_status,
                       p.cura_operational_event_id,
                       a.name AS account_name,
                       cq.title AS quote_title,
                       o.name AS opportunity_name
                FROM crm_event_plans p
                LEFT JOIN crm_accounts a ON a.id = p.account_id
                LEFT JOIN crm_quotes cq ON cq.id = p.quote_id
                LEFT JOIN crm_opportunities o ON o.id = p.opportunity_id
                ORDER BY p.updated_at DESC
                LIMIT 200
                """
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()
        for p in rows:
            p["cura_ops_event_url"] = cura_ops_event_detail_url_for_plan(p)
        return render_template(
            "admin/crm_event_plans_list.html",
            plans=rows,
            can_edit=can_edit(),
        )

    @crm_bp.route("/event-plans/new", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def event_plan_new():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT id, title, account_id, opportunity_id FROM crm_quotes ORDER BY id DESC LIMIT 80")
            quotes = cur.fetchall() or []
            cur.execute(
                """
                SELECT o.id, o.name, o.account_id, a.name AS account_name
                FROM crm_opportunities o
                JOIN crm_accounts a ON a.id = o.account_id
                ORDER BY o.updated_at DESC
                LIMIT 120
                """
            )
            opportunities = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        q_param = (request.args.get("quote_id") or "").strip()
        pre_quote = int(q_param) if q_param.isdigit() else None
        o_param = (request.args.get("opportunity_id") or "").strip()
        pre_opp = int(o_param) if o_param.isdigit() else None
        suggested_title = ""
        if pre_opp:
            for o in opportunities:
                if int(o["id"]) == pre_opp:
                    suggested_title = f"{o.get('account_name') or ''} — {o.get('name') or ''}".strip(" —")[:255]
                    break

        if request.method == "POST":
            title = (request.form.get("title") or "").strip() or "Event plan"
            qid = _int_or_none(request.form.get("quote_id"))
            oid = _int_or_none(request.form.get("opportunity_id"))
            account_id = None
            opp_id = None
            if qid:
                c2 = get_db_connection()
                qc = c2.cursor(dictionary=True)
                try:
                    qc.execute(
                        "SELECT account_id, opportunity_id FROM crm_quotes WHERE id=%s",
                        (qid,),
                    )
                    qr = qc.fetchone()
                finally:
                    qc.close()
                    c2.close()
                if qr:
                    account_id = qr.get("account_id")
                    opp_id = qr.get("opportunity_id")
            elif oid:
                c2 = get_db_connection()
                qc = c2.cursor(dictionary=True)
                try:
                    qc.execute(
                        "SELECT account_id FROM crm_opportunities WHERE id=%s",
                        (oid,),
                    )
                    orow = qc.fetchone()
                finally:
                    qc.close()
                    c2.close()
                if orow and orow.get("account_id"):
                    account_id = orow.get("account_id")
                    opp_id = oid
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """INSERT INTO crm_event_plans
                    (title, status, quote_id, opportunity_id, account_id, created_by, checklist_answers_json)
                    VALUES (%s,'draft',%s,%s,%s,%s,'{}')""",
                    (title, qid, opp_id, account_id, uid()),
                )
                pid = cur.lastrowid
                conn.commit()
                flash("Plan created.", "success")
                return redirect(url_for("crm_module.event_plan_edit", plan_id=pid))
            finally:
                cur.close()
                conn.close()

        return render_template(
            "admin/crm_event_plan_new.html",
            quotes=quotes,
            opportunities=opportunities,
            pre_quote_id=pre_quote,
            pre_opportunity_id=pre_opp,
            suggested_title=suggested_title,
        )

    @crm_bp.route("/event-plans/<int:plan_id>/edit", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def event_plan_edit(plan_id: int):
        if request.method == "POST" and not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM crm_event_plans WHERE id=%s", (plan_id,))
            plan = cur.fetchone()
            cur.execute(
                """SELECT id, group_name, question_text, answer_type, sort_order, help_url
                FROM crm_event_plan_questions WHERE is_active=1 ORDER BY sort_order, id"""
            )
            questions = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        if not plan:
            flash("Plan not found.", "danger")
            return redirect(url_for("crm_module.event_plans_list"))

        answers = _parse_answers_json(plan.get("checklist_answers_json"))

        if request.method == "POST" and can_edit():
            csave = get_db_connection()
            try:
                qcur = csave.cursor(dictionary=True)
                qcur.execute(
                    """SELECT id, group_name, question_text, answer_type, sort_order, help_url
                    FROM crm_event_plan_questions WHERE is_active=1 ORDER BY sort_order, id"""
                )
                qrows = qcur.fetchall() or []
                qcur.close()
                _save_event_plan_from_form(csave, plan_id, qrows)
            finally:
                csave.close()
            flash("Event plan saved.", "success")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM crm_event_plans WHERE id=%s", (plan_id,))
            plan = cur.fetchone()
            if not plan:
                flash("Plan not found.", "danger")
                return redirect(url_for("crm_module.event_plans_list"))
            cur.execute(
                """SELECT file_path, pdf_generated_at FROM crm_event_plan_pdfs
                WHERE plan_id=%s ORDER BY id DESC LIMIT 8""",
                (plan_id,),
            )
            pdf_history = cur.fetchall() or []
            plan_link_labels = fetch_plan_link_labels(cur, plan)
            handoff_log: list = []
            try:
                cur.execute(
                    """
                    SELECT id, trigger_key, status, created_at, external_ref, pdf_hash
                    FROM crm_event_plan_handoff_log
                    WHERE plan_id=%s ORDER BY id DESC LIMIT 12
                    """,
                    (plan_id,),
                )
                handoff_log = cur.fetchall() or []
            except Exception:
                handoff_log = []
        finally:
            cur.close()
            conn.close()
        answers = _parse_answers_json(plan.get("checklist_answers_json"))

        step_arg = request.args.get("step")
        if step_arg is not None and str(step_arg).strip().isdigit():
            display_step = _wizard_step_clamp(step_arg)
        else:
            display_step = _wizard_step_clamp(str(plan.get("wizard_step") or 1))

        cura_ops_event_url = cura_ops_event_detail_url_for_plan(plan)

        return render_template(
            "admin/crm_event_plan_edit.html",
            plan=plan,
            questions=questions,
            answers=answers,
            pdf_history=pdf_history,
            hospitals_list_lines=_hospitals_lines_for_display(plan.get("hospitals_json")),
            clinical_handover=parse_clinical_handover(plan.get("clinical_handover_json")),
            display_step=display_step,
            hospital_suggest_url=url_for("crm_module.event_plan_hospital_suggest"),
            plan_link_labels=plan_link_labels,
            handoff_log=handoff_log,
            crm_handoff_mode=handoff_mode(),
            cura_ops_event_url=cura_ops_event_url,
            handoff_block_message=handoff_blocked_reason(plan),
            can_edit=can_edit(),
        )

    @crm_bp.route("/event-plans/<int:plan_id>/pdf", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def event_plan_pdf(plan_id: int):
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM crm_event_plans WHERE id=%s", (plan_id,))
            plan = cur.fetchone()
            cur.execute(
                """SELECT id, group_name, question_text, answer_type, sort_order, help_url
                FROM crm_event_plan_questions WHERE is_active=1 ORDER BY sort_order, id"""
            )
            questions = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()
        if not plan:
            flash("Plan not found.", "danger")
            return redirect(url_for("crm_module.event_plans_list"))

        answers = _parse_answers_json(plan.get("checklist_answers_json"))
        app = current_app._get_current_object()
        try:
            pdf_bytes, pdf_hash = write_event_plan_pdf(app, plan, questions, answers)
        except Exception as e:
            flash(f"PDF failed: {e}", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

        rel = store_event_plan_pdf_file(app, plan_id, pdf_bytes, pdf_hash)
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO crm_event_plan_pdfs (plan_id, file_path, pdf_hash)
                VALUES (%s,%s,%s)""",
                (plan_id, rel, pdf_hash),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        flash("PDF generated.", "success")
        return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

    @crm_bp.route("/event-plans/<int:plan_id>/pdf-download")
    @login_required
    @crm_access_required
    def event_plan_pdf_download(plan_id: int):
        rel = (request.args.get("f") or "").strip().lstrip("/\\")
        if ".." in rel or not rel.startswith("uploads/crm_event_plans/"):
            flash("Invalid file.", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id FROM crm_event_plan_pdfs WHERE plan_id=%s AND file_path=%s ORDER BY id DESC LIMIT 1",
                (plan_id, rel),
            )
            row = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        if not row:
            flash("PDF not found.", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))
        app = current_app._get_current_object()
        abs_path = crm_resolve_event_plan_pdf_path(app, rel)
        if not abs_path:
            flash("PDF file not on disk.", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))
        directory = os.path.dirname(abs_path)
        fname = os.path.basename(abs_path)
        return send_from_directory(
            directory,
            fname,
            as_attachment=True,
            download_name=f"event_plan_{plan_id}.pdf",
        )

    @crm_bp.route("/event-plans/<int:plan_id>/handoff", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def event_plan_handoff(plan_id: int):
        result = process_event_plan_handoff(
            plan_id,
            trigger="manual_ui",
            user_key=uid(),
            flask_app=current_app._get_current_object(),
        )
        if result.get("ok"):
            flash(result.get("message") or result.get("status", "OK"), "success")
        else:
            flash(result.get("error") or "Handoff failed.", "danger")
        return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

    @crm_bp.route("/api/event-plans/hospital-suggest")
    @login_required
    @crm_access_required
    def event_plan_hospital_suggest():
        """GET ?q= or ?postcode= — UK geocode (postcodes.io) + distance-ranked catalogue (CRM-HOSP-001)."""
        q = (request.args.get("q") or request.args.get("postcode") or "").strip()
        return jsonify(hospital_suggest_payload(q))

    @crm_bp.route("/event-plans/questions")
    @login_required
    @crm_access_required
    def event_plan_questions_list():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """SELECT id, group_name, question_text, answer_type, sort_order, is_active, help_url
                FROM crm_event_plan_questions ORDER BY sort_order, id"""
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()
        return render_template(
            "admin/crm_event_plan_questions_list.html",
            questions=rows,
            can_edit=can_edit(),
        )

    @crm_bp.route("/event-plans/questions/<int:qid>/edit", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def event_plan_question_edit(qid: int):
        if request.method == "POST" and not can_edit():
            flash("You do not have permission to edit questions.", "danger")
            return redirect(url_for("crm_module.event_plan_question_edit", qid=qid))

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM crm_event_plan_questions WHERE id=%s", (qid,))
            row = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        if not row:
            flash("Question not found.", "danger")
            return redirect(url_for("crm_module.event_plan_questions_list"))

        if request.method == "POST" and can_edit():
            new_text = (request.form.get("question_text") or "").strip()
            if not new_text:
                flash("Question text required.", "danger")
            else:
                old_text = row.get("question_text") or ""
                gn = (request.form.get("group_name") or "").strip()[:128]
                hu = (request.form.get("help_url") or "").strip()[:512] or None
                active = 1 if request.form.get("is_active") == "1" else 0
                conn = get_db_connection()
                cur = conn.cursor()
                try:
                    if new_text != old_text:
                        cur.execute(
                            """INSERT INTO crm_event_plan_question_audit
                            (question_id, previous_text, new_text, changed_by)
                            VALUES (%s,%s,%s,%s)""",
                            (qid, old_text[:512], new_text[:512], uid()),
                        )
                    cur.execute(
                        """UPDATE crm_event_plan_questions SET question_text=%s,
                        group_name=%s, help_url=%s, is_active=%s WHERE id=%s""",
                        (new_text, gn, hu, active, qid),
                    )
                    conn.commit()
                    flash("Question updated.", "success")
                    return redirect(url_for("crm_module.event_plan_questions_list"))
                finally:
                    cur.close()
                    conn.close()

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """SELECT * FROM crm_event_plan_question_audit WHERE question_id=%s
                ORDER BY created_at DESC LIMIT 30""",
                (qid,),
            )
            audit = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        return render_template(
            "admin/crm_event_plan_question_edit.html",
            question=row,
            audit=audit,
            can_edit=can_edit(),
        )


def _plan_status(raw: str | None) -> str:
    s = (raw or "draft").strip().lower()
    if s in ("draft", "completed", "archived"):
        return s
    return "draft"


def _wizard_step_clamp(raw: str | None) -> int:
    try:
        n = int((raw or "1").strip())
    except ValueError:
        n = 1
    return max(1, min(8, n))


def _int_or_none(raw: str | None) -> int | None:
    s = (raw or "").strip()
    return int(s) if s.isdigit() else None


def _hospitals_json_from_textarea(raw: str | None) -> str | None:
    lines = (raw or "").splitlines()
    lst = [ln.strip() for ln in lines if ln.strip()][:80]
    if not lst:
        return None
    return json.dumps(lst, ensure_ascii=False)


def _hospitals_lines_for_display(raw) -> str:
    if raw is None:
        return ""
    if isinstance(raw, list):
        return "\n".join(str(x) for x in raw)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return ""
        try:
            v = json.loads(s)
            if isinstance(v, list):
                return "\n".join(str(x) for x in v)
        except json.JSONDecodeError:
            return ""
    return ""


def _parse_answers_json(raw) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(k): v for k, v in raw.items()}
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return {}
        try:
            v = json.loads(s)
            return {str(k): val for k, val in v.items()} if isinstance(v, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _clinical_handover_json_from_form() -> str | None:
    mc = (request.form.get("medical_control") or "").strip()
    keys = ("age", "sex", "history", "injuries", "current", "expected")
    ash = {k: (request.form.get(f"ashice_{k}") or "").strip() for k in keys}
    if not mc and not any(ash.values()):
        return None
    return json.dumps(
        {"medical_control": mc or None, "ashice": ash},
        ensure_ascii=False,
    )


def _save_event_plan_from_form(conn, plan_id: int, questions: list[dict]) -> None:
    answers: dict[str, str] = {}
    for q in questions:
        qid = str(q["id"])
        key = f"chk_{qid}"
        if q.get("answer_type") == "yes_no":
            answers[qid] = (request.form.get(key) or "").strip()
        else:
            answers[qid] = (request.form.get(key) or "").strip()

    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE crm_event_plans SET
            title=%s, event_type=%s, start_datetime=%s, end_datetime=%s,
            expected_attendance=%s, demographics_notes=%s, environment_notes=%s,
            address_line1=%s, postcode=%s, what3words=%s, location_notes=%s,
            hospitals_notes=%s, hospitals_json=%s, clinical_handover_json=%s,
            resources_medics=%s, resources_vehicles=%s,
            resources_comms=%s, escalation_notes=%s, risk_summary=%s,
            risk_score=%s, signoff_name=%s, signoff_role=%s, signoff_at=%s,
            checklist_answers_json=%s, status=%s, wizard_step=%s
            WHERE id=%s""",
            (
                (request.form.get("title") or "").strip()[:255] or "Event plan",
                (request.form.get("event_type") or "").strip()[:128] or None,
                _parse_dt(request.form.get("start_datetime")),
                _parse_dt(request.form.get("end_datetime")),
                _int_or_none(request.form.get("expected_attendance")),
                (request.form.get("demographics_notes") or "").strip() or None,
                (request.form.get("environment_notes") or "").strip() or None,
                (request.form.get("address_line1") or "").strip()[:255] or None,
                (request.form.get("postcode") or "").strip()[:32] or None,
                (request.form.get("what3words") or "").strip()[:64] or None,
                (request.form.get("location_notes") or "").strip() or None,
                (request.form.get("hospitals_notes") or "").strip() or None,
                _hospitals_json_from_textarea(request.form.get("hospitals_list_lines")),
                _clinical_handover_json_from_form(),
                (request.form.get("resources_medics") or "").strip()[:255] or None,
                (request.form.get("resources_vehicles") or "").strip() or None,
                (request.form.get("resources_comms") or "").strip() or None,
                (request.form.get("escalation_notes") or "").strip() or None,
                (request.form.get("risk_summary") or "").strip() or None,
                _int_or_none(request.form.get("risk_score")),
                (request.form.get("signoff_name") or "").strip()[:255] or None,
                (request.form.get("signoff_role") or "").strip()[:255] or None,
                _parse_dt(request.form.get("signoff_at")),
                json.dumps(answers, ensure_ascii=False),
                _plan_status(request.form.get("status")),
                _wizard_step_clamp(request.form.get("wizard_step")),
                plan_id,
            ),
        )
        conn.commit()
    finally:
        cur.close()


def _parse_dt(raw: str | None):
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None
