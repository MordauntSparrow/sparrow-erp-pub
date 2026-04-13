# Medical event planner (R3) + checklist question admin
from __future__ import annotations

import json
import os
import uuid
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

from .crm_common import (
    can_edit,
    crm_access_required,
    crm_edit_required,
    crm_medical_surface_available,
    crm_medical_surface_required,
    uid,
)
from .crm_cura_links import cura_ops_event_detail_url_for_plan
from .crm_event_plan_handoff import (
    handoff_blocked_reason,
    handoff_mode,
    process_event_plan_handoff,
)
from .crm_event_plan_links import fetch_plan_link_labels
from .crm_clinical_handover import parse_clinical_handover
from .crm_hospital_stub import hospital_suggest_payload
from .crm_event_plan_extended import pad_risk_register, risk_register_json_from_form
from .crm_event_plan_media import (
    add_diagram_from_template,
    add_equipment_asset_to_plan,
    delete_diagram,
    delete_template,
    list_diagram_rows_for_plan,
    list_diagram_templates,
    list_plan_equipment_safe,
    remove_equipment_asset_from_plan,
    resolve_equipment_asset_id,
    save_diagram_caption,
    store_template_diagram_image,
    store_uploaded_diagram,
)
from .crm_plan_prefill import apply_prefill_to_plan, resolve_account_and_opp_for_links
from .crm_plan_pdf_context import pad_staff_roster, staff_roster_list_from_plan
from .crm_static_paths import (
    crm_event_map_path_is_allowed,
    crm_event_plan_maps_relative_subpath,
    crm_event_plan_maps_write_dir,
    crm_resolve_event_plan_map_path,
    crm_resolve_event_plan_pdf_path,
)
from .crm_planner_pdf import store_event_plan_pdf_file, write_event_plan_pdf


def _event_plan_kit_portal_url(plan_id: int) -> str | None:
    try:
        return url_for(
            "public_employee_portal.event_plan_kit_list",
            plan_id=int(plan_id),
            _external=True,
        )
    except Exception:
        return None


def _rollback_event_plan_pdf_revision(plan_id: int) -> None:
    """Undo the pre-render revision bump when PDF generation or persistence fails."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE crm_event_plans SET plan_pdf_revision = GREATEST(IFNULL(plan_pdf_revision, 0) - 1, 0) WHERE id=%s",
            (int(plan_id),),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def register_crm_planner_routes(crm_bp):
    @crm_bp.route("/event-plans")
    @login_required
    @crm_access_required
    @crm_medical_surface_required
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
    @crm_medical_surface_required
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
                if qid or opp_id:
                    try:
                        apply_prefill_to_plan(
                            conn,
                            pid,
                            quote_id=qid,
                            opportunity_id=opp_id,
                            only_if_empty=True,
                        )
                    except Exception:
                        pass
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
    @crm_medical_surface_required
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
            cur.execute(
                "SELECT id, title, account_id, opportunity_id FROM crm_quotes ORDER BY id DESC LIMIT 80"
            )
            crm_quotes_dropdown = cur.fetchall() or []
            cur.execute(
                """
                SELECT o.id, o.name, o.account_id, a.name AS account_name
                FROM crm_opportunities o
                JOIN crm_accounts a ON a.id = o.account_id
                ORDER BY o.updated_at DESC
                LIMIT 120
                """
            )
            crm_opportunities_dropdown = cur.fetchall() or []
            equipment_rows = list_plan_equipment_safe(conn, plan_id)
            diagram_rows = []
            diagram_templates = []
            try:
                diagram_rows = list_diagram_rows_for_plan(conn, plan_id)
                diagram_templates = list_diagram_templates(conn)
            except Exception:
                diagram_rows = []
                diagram_templates = []
        finally:
            cur.close()
            conn.close()
        answers = _parse_answers_json(plan.get("checklist_answers_json"))
        kit_portal_url = _event_plan_kit_portal_url(plan_id)
        try:
            pdf_rev = int(plan.get("plan_pdf_revision") or 0)
        except (TypeError, ValueError):
            pdf_rev = 0
        plan_pdf_version_display = None
        if pdf_rev >= 1:
            plan_pdf_version_display = f"1.{pdf_rev - 1}"

        step_arg = request.args.get("step")
        if step_arg is not None and str(step_arg).strip().isdigit():
            display_step = _wizard_step_clamp(step_arg)
        else:
            display_step = _wizard_step_clamp(str(plan.get("wizard_step") or 1))

        cura_ops_event_url = cura_ops_event_detail_url_for_plan(plan)

        linked_quote = None
        if plan.get("quote_id"):
            qc = get_db_connection()
            qcur = qc.cursor(dictionary=True)
            try:
                qcur.execute(
                    "SELECT id, status, title FROM crm_quotes WHERE id=%s",
                    (int(plan["quote_id"]),),
                )
                linked_quote = qcur.fetchone()
            finally:
                qcur.close()
                qc.close()

        staff_rows = pad_staff_roster(staff_roster_list_from_plan(plan))
        risk_rows = pad_risk_register(plan)

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
            linked_quote=linked_quote,
            staff_roster_rows=staff_rows,
            risk_register_rows=risk_rows,
            can_edit=can_edit(),
            crm_quotes_dropdown=crm_quotes_dropdown,
            crm_opportunities_dropdown=crm_opportunities_dropdown,
            equipment_rows=equipment_rows,
            diagram_rows=diagram_rows,
            diagram_templates=diagram_templates,
            kit_portal_url=kit_portal_url,
            plan_pdf_version_display=plan_pdf_version_display,
        )

    @crm_bp.route("/event-plans/<int:plan_id>/pdf", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
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

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "UPDATE crm_event_plans SET plan_pdf_revision = IFNULL(plan_pdf_revision, 0) + 1 WHERE id=%s",
                (plan_id,),
            )
            conn.commit()
            cur.execute("SELECT * FROM crm_event_plans WHERE id=%s", (plan_id,))
            plan = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        if not plan:
            flash("Plan not found.", "danger")
            return redirect(url_for("crm_module.event_plans_list"))

        answers = _parse_answers_json(plan.get("checklist_answers_json"))
        diagram_rows_pdf: list = []
        conn_d = get_db_connection()
        try:
            diagram_rows_pdf = list_diagram_rows_for_plan(conn_d, plan_id)
        except Exception:
            diagram_rows_pdf = []
        finally:
            conn_d.close()
        kit_url = _event_plan_kit_portal_url(plan_id)
        app = current_app._get_current_object()
        try:
            pdf_bytes, pdf_hash = write_event_plan_pdf(
                app,
                plan,
                questions,
                answers,
                diagram_rows=diagram_rows_pdf,
                equipment_kit_url=kit_url,
            )
        except Exception as e:
            _rollback_event_plan_pdf_revision(plan_id)
            flash(f"PDF failed: {e}", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

        try:
            rel = store_event_plan_pdf_file(app, plan_id, pdf_bytes, pdf_hash)
        except Exception as e:
            _rollback_event_plan_pdf_revision(plan_id)
            flash(f"Could not save PDF file: {e}", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

        conn_ins = get_db_connection()
        cur_ins = conn_ins.cursor()
        try:
            cur_ins.execute(
                """INSERT INTO crm_event_plan_pdfs (plan_id, file_path, pdf_hash)
                VALUES (%s,%s,%s)""",
                (plan_id, rel, pdf_hash),
            )
            conn_ins.commit()
        except Exception as e:
            conn_ins.rollback()
            _rollback_event_plan_pdf_revision(plan_id)
            flash(f"Could not record PDF in database: {e}", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))
        finally:
            cur_ins.close()
            conn_ins.close()
        flash("PDF generated.", "success")
        return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

    @crm_bp.route("/event-plans/<int:plan_id>/pdf-download")
    @login_required
    @crm_access_required
    @crm_medical_surface_required
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
    @crm_medical_surface_required
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

    @crm_bp.route("/event-plans/<int:plan_id>/equipment/add", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_equipment_add(plan_id: int):
        if not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))
        raw = (request.form.get("equipment_asset_lookup") or "").strip()
        conn = get_db_connection()
        try:
            aid = resolve_equipment_asset_id(conn, raw)
            if not aid:
                flash("Equipment asset not found (try numeric ID or public asset code).", "warning")
            elif add_equipment_asset_to_plan(conn, plan_id, aid):
                flash("Kit / asset linked to this event plan.", "success")
            else:
                flash("That asset is already linked.", "info")
        except Exception as e:
            flash(f"Could not link asset: {e}", "danger")
        finally:
            conn.close()
        return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

    @crm_bp.route("/event-plans/<int:plan_id>/equipment/remove", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_equipment_remove(plan_id: int):
        if not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))
        aid = _int_or_none(request.form.get("equipment_asset_id"))
        if aid:
            conn = get_db_connection()
            try:
                remove_equipment_asset_from_plan(conn, plan_id, aid)
                flash("Asset unlinked from plan.", "success")
            finally:
                conn.close()
        return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

    @crm_bp.route("/event-plans/<int:plan_id>/diagrams/add-template", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_diagram_add_template(plan_id: int):
        if not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))
        tid = _int_or_none(request.form.get("diagram_template_id"))
        if tid:
            conn = get_db_connection()
            try:
                add_diagram_from_template(conn, plan_id, tid)
                flash("Diagram added from template (updates if template image changes).", "success")
            except Exception as e:
                flash(f"Could not add diagram: {e}", "danger")
            finally:
                conn.close()
        return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

    @crm_bp.route("/event-plans/<int:plan_id>/diagrams/upload", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_diagram_upload(plan_id: int):
        if not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))
        f = request.files.get("diagram_file")
        if not f or not f.filename:
            flash("Choose an image file.", "warning")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))
        app = current_app._get_current_object()
        conn = get_db_connection()
        try:
            store_uploaded_diagram(app, plan_id, f, conn)
            flash("Diagram uploaded.", "success")
        except Exception as e:
            flash(str(e), "danger")
        finally:
            conn.close()
        return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

    @crm_bp.route("/event-plans/<int:plan_id>/diagrams/<int:dg_id>/delete", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_diagram_delete(plan_id: int, dg_id: int):
        if not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))
        app = current_app._get_current_object()
        conn = get_db_connection()
        try:
            delete_diagram(conn, dg_id, plan_id, app)
            flash("Diagram removed.", "success")
        except Exception as e:
            flash(str(e), "danger")
        finally:
            conn.close()
        return redirect(url_for("crm_module.event_plan_edit", plan_id=plan_id))

    @crm_bp.route("/event-plans/diagram-templates", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_diagram_templates_admin():
        if not can_edit():
            flash("You do not have permission to edit diagram templates.", "danger")
            return redirect(url_for("crm_module.event_plans_list"))
        app = current_app._get_current_object()
        if request.method == "POST":
            act = (request.form.get("action") or "").strip().lower()
            if act == "delete":
                tid = _int_or_none(request.form.get("template_id"))
                if tid:
                    conn = get_db_connection()
                    try:
                        delete_template(conn, tid, app)
                        flash("Template removed.", "success")
                    except Exception as e:
                        flash(str(e), "danger")
                    finally:
                        conn.close()
            else:
                f = request.files.get("template_image")
                title = (request.form.get("template_title") or "").strip() or "Diagram"
                if not f or not f.filename:
                    flash("Choose an image file.", "warning")
                else:
                    try:
                        store_template_diagram_image(app, f, title)
                        flash("Template added.", "success")
                    except Exception as e:
                        flash(str(e), "danger")
            return redirect(url_for("crm_module.event_plan_diagram_templates_admin"))
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, title, image_path, sort_order FROM crm_event_plan_diagram_templates ORDER BY sort_order, id"
            )
            templates = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()
        return render_template(
            "admin/crm_event_plan_diagram_templates.html",
            templates=templates,
            can_edit=can_edit(),
        )

    @crm_bp.route("/api/event-plans/hospital-suggest")
    @login_required
    @crm_access_required
    def event_plan_hospital_suggest():
        """GET ?q= or ?postcode= — UK geocode (postcodes.io) + distance-ranked catalogue (CRM-HOSP-001)."""
        if not crm_medical_surface_available():
            return (
                jsonify(
                    {
                        "error": "Hospital suggestions require Medical industry in Core settings.",
                        "suggestions": [],
                        "stub": True,
                    }
                ),
                403,
            )
        q = (request.args.get("q") or request.args.get("postcode") or "").strip()
        return jsonify(hospital_suggest_payload(q))

    @crm_bp.route("/event-plans/questions")
    @login_required
    @crm_access_required
    @crm_medical_surface_required
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
    @crm_medical_surface_required
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
    return max(1, min(9, n))


def _int_or_none(raw: str | None) -> int | None:
    s = (raw or "").strip()
    return int(s) if s.isdigit() else None


def _date_or_none(raw: str | None):
    s = (raw or "").strip()[:10]
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _form_text(key: str) -> str | None:
    s = (request.form.get(key) or "").strip()
    return s or None


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


def _staff_roster_json_from_form() -> str | None:
    callsigns = request.form.getlist("sr_callsign")
    names = request.form.getlist("sr_name")
    roles = request.form.getlist("sr_role")
    posts = request.form.getlist("sr_post")
    shifts = request.form.getlist("sr_shift")
    phones = request.form.getlist("sr_phone")
    notes = request.form.getlist("sr_notes")
    n = max(
        len(callsigns),
        len(names),
        len(roles),
        len(posts),
        len(shifts),
        len(phones),
        len(notes),
    )
    rows: list[dict[str, str]] = []
    for i in range(n):
        cs = (callsigns[i] if i < len(callsigns) else "").strip()
        nm = (names[i] if i < len(names) else "").strip()
        rl = (roles[i] if i < len(roles) else "").strip()
        po = (posts[i] if i < len(posts) else "").strip()
        sh = (shifts[i] if i < len(shifts) else "").strip()
        ph = (phones[i] if i < len(phones) else "").strip()
        nt = (notes[i] if i < len(notes) else "").strip()
        if not (cs or nm or rl or po or sh or ph or nt):
            continue
        rows.append(
            {
                "callsign": cs[:64],
                "name": nm[:128],
                "role_grade": rl[:128],
                "post_assignment": po[:255],
                "shift": sh[:128],
                "phone": ph[:32],
                "notes": nt[:512],
            }
        )
    if not rows:
        return None
    return json.dumps(rows, ensure_ascii=False)


_MAP_UPLOAD_EXT = (".png", ".jpg", ".jpeg", ".webp", ".gif")


def _event_map_upload_extension(filename: str) -> str | None:
    lower = (filename or "").lower().strip()
    for ext in _MAP_UPLOAD_EXT:
        if lower.endswith(ext):
            return ext
    return None


def _delete_stored_event_map(app, rel: str | None) -> None:
    if not rel or not crm_event_map_path_is_allowed(rel):
        return
    abs_path = crm_resolve_event_plan_map_path(app, rel)
    if abs_path and os.path.isfile(abs_path):
        try:
            os.remove(abs_path)
        except OSError:
            pass


def _next_event_map_path_after_form(app, plan_id: int, old_rel: str | None) -> str | None:
    old_norm = ((old_rel or "").strip().replace("\\", "/")) or None
    fstor = request.files.get("event_map_file")
    has_upload = bool(fstor and fstor.filename and str(fstor.filename).strip())
    clear = (request.form.get("event_map_clear") or "").strip() == "1"
    if has_upload:
        ext = _event_map_upload_extension(fstor.filename)
        if not ext:
            flash(
                "Event map was not updated — use PNG, JPG, JPEG, WebP, or GIF.",
                "warning",
            )
            return old_norm
        dest_dir = crm_event_plan_maps_write_dir(app)
        os.makedirs(dest_dir, exist_ok=True)
        fname = f"plan_{plan_id}_{uuid.uuid4().hex[:12]}{ext}"
        full = os.path.join(dest_dir, fname)
        try:
            fstor.save(full)
        except OSError as e:
            flash(f"Could not save event map: {e}", "danger")
            return old_norm
        new_rel = f"{crm_event_plan_maps_relative_subpath()}/{fname}".replace("\\", "/")
        if old_norm and old_norm != new_rel and crm_event_map_path_is_allowed(old_norm):
            _delete_stored_event_map(app, old_norm)
        return new_rel
    if clear:
        if old_norm and crm_event_map_path_is_allowed(old_norm):
            _delete_stored_event_map(app, old_norm)
        return None
    return old_norm


def _save_event_plan_from_form(conn, plan_id: int, questions: list[dict]) -> None:
    answers: dict[str, str] = {}
    for q in questions:
        qid = str(q["id"])
        key = f"chk_{qid}"
        if q.get("answer_type") == "yes_no":
            answers[qid] = (request.form.get(key) or "").strip()
        else:
            answers[qid] = (request.form.get(key) or "").strip()

    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """SELECT event_map_path, quote_id, opportunity_id, account_id
            FROM crm_event_plans WHERE id=%s""",
            (plan_id,),
        )
        prev_row = cur.fetchone() or {}
        old_map_path = prev_row.get("event_map_path")
        if isinstance(old_map_path, bytes):
            old_map_path = old_map_path.decode("utf-8", errors="replace")
        new_map_path = _next_event_map_path_after_form(current_app, plan_id, old_map_path)
        map_caption = (request.form.get("event_map_caption") or "").strip()[:512] or None
        qid_l = prev_row.get("quote_id")
        oid_l = prev_row.get("opportunity_id")
        acc_l = prev_row.get("account_id")
        if "crm_link_quote_id" in request.form or "crm_link_opportunity_id" in request.form:
            link_q = _int_or_none(request.form.get("crm_link_quote_id"))
            link_o = _int_or_none(request.form.get("crm_link_opportunity_id"))
            acc_l, oid_l, qid_l = resolve_account_and_opp_for_links(
                conn, link_q, link_o
            )
    finally:
        cur.close()

    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE crm_event_plans SET
            title=%s, event_type=%s, start_datetime=%s, end_datetime=%s,
            expected_attendance=%s, demographics_notes=%s, environment_notes=%s,
            address_line1=%s, postcode=%s, what3words=%s, location_notes=%s,
            hospitals_notes=%s, hospitals_json=%s, clinical_handover_json=%s,
            staff_roster_json=%s, event_map_path=%s, event_map_caption=%s,
            plan_pdf_status=%s, plan_written_by=%s,
            plan_distribution=%s, plan_document_release_date=%s, plan_purpose=%s,
            event_organiser=%s, event_content_summary=%s, purple_guide_tier=%s,
            operational_timings=%s, access_egress_text=%s, rendezvous_text=%s,
            risk_register_json=%s, health_safety_work_text=%s,
            staff_transport_text=%s, command_control_text=%s, uniform_ppe_text=%s,
            privacy_ipc_text=%s, incident_reporting_text=%s, equipment_detail_text=%s,
            major_incident_text=%s, welfare_text=%s, safeguarding_text=%s,
            ambulance_trust_text=%s, other_event_specific_text=%s,
            attendance_by_day_text=%s,
            resources_medics=%s, resources_vehicles=%s,
            resources_comms=%s, escalation_notes=%s, risk_summary=%s,
            risk_score=%s, signoff_name=%s, signoff_role=%s, signoff_at=%s,
            checklist_answers_json=%s, status=%s, wizard_step=%s,
            quote_id=%s, opportunity_id=%s, account_id=%s
            WHERE id=%s""",
            (
                (request.form.get("title") or "").strip()[:255] or "Event plan",
                (request.form.get("event_type") or "").strip()[:128] or None,
                _parse_dt(request.form.get("start_datetime")),
                _parse_dt(request.form.get("end_datetime")),
                _int_or_none(request.form.get("expected_attendance")),
                _form_text("demographics_notes"),
                _form_text("environment_notes"),
                (request.form.get("address_line1") or "").strip()[:255] or None,
                (request.form.get("postcode") or "").strip()[:32] or None,
                (request.form.get("what3words") or "").strip()[:64] or None,
                _form_text("location_notes"),
                _form_text("hospitals_notes"),
                _hospitals_json_from_textarea(request.form.get("hospitals_list_lines")),
                _clinical_handover_json_from_form(),
                _staff_roster_json_from_form(),
                new_map_path,
                map_caption,
                (request.form.get("plan_pdf_status") or "").strip()[:128] or None,
                (request.form.get("plan_written_by") or "").strip()[:255] or None,
                _form_text("plan_distribution"),
                _date_or_none(request.form.get("plan_document_release_date")),
                _form_text("plan_purpose"),
                (request.form.get("event_organiser") or "").strip()[:255] or None,
                _form_text("event_content_summary"),
                (request.form.get("purple_guide_tier") or "").strip()[:64] or None,
                _form_text("operational_timings"),
                _form_text("access_egress_text"),
                _form_text("rendezvous_text"),
                risk_register_json_from_form(),
                _form_text("health_safety_work_text"),
                _form_text("staff_transport_text"),
                _form_text("command_control_text"),
                _form_text("uniform_ppe_text"),
                _form_text("privacy_ipc_text"),
                _form_text("incident_reporting_text"),
                _form_text("equipment_detail_text"),
                _form_text("major_incident_text"),
                _form_text("welfare_text"),
                _form_text("safeguarding_text"),
                _form_text("ambulance_trust_text"),
                _form_text("other_event_specific_text"),
                _form_text("attendance_by_day_text"),
                (request.form.get("resources_medics") or "").strip()[:255] or None,
                _form_text("resources_vehicles"),
                _form_text("resources_comms"),
                _form_text("escalation_notes"),
                _form_text("risk_summary"),
                _int_or_none(request.form.get("risk_score")),
                (request.form.get("signoff_name") or "").strip()[:255] or None,
                (request.form.get("signoff_role") or "").strip()[:255] or None,
                _parse_dt(request.form.get("signoff_at")),
                json.dumps(answers, ensure_ascii=False),
                _plan_status(request.form.get("status")),
                _wizard_step_clamp(request.form.get("wizard_step")),
                qid_l,
                oid_l,
                acc_l,
                plan_id,
            ),
        )
        conn.commit()
    finally:
        cur.close()

    for fk, fv in request.form.items():
        if fk.startswith("diagram_caption_"):
            tail = fk[len("diagram_caption_") :]
            if tail.isdigit():
                save_diagram_caption(conn, int(tail), plan_id, fv)

    if request.form.get("apply_crm_prefill") == "1" and (qid_l is not None or oid_l is not None):
        n = apply_prefill_to_plan(
            conn,
            plan_id,
            quote_id=qid_l,
            opportunity_id=oid_l,
            only_if_empty=True,
        )
        if n:
            flash(
                f"Prefilled {n} empty field(s) from the linked quote/opportunity.",
                "info",
            )


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
