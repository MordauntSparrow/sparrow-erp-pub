# Medical event planner (R3) + checklist question admin
from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime
from typing import Any

from flask import (
    Blueprint,
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
from .crm_pipeline_flow import event_plan_edit_url, flow_active, quote_new_url
from .crm_clinical_handover import (
    ashice_editor_rows,
    atmist_editor_rows,
    parse_clinical_handover,
)
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
from .crm_event_plan_policy_templates import (
    POLICY_CATEGORY_HEALTH_SAFETY,
    POLICY_CATEGORY_PRIVACY_IPC,
    delete_policy_template,
    insert_policy_template,
    list_policy_templates,
)
from .crm_event_plan_accommodation import (
    accommodation_enabled_from_form,
    accommodation_rooms_json_from_form,
    accommodation_venues_from_plan,
    accommodation_venues_json_from_form,
    accommodation_rooms_from_plan,
    pad_accommodation_room_rows_for_edit,
    pad_accommodation_venue_rows_for_edit,
)
from .crm_event_plan_major_incident import (
    coerce_major_incident_detail,
    major_incident_detail_json_from_form,
)
from .crm_event_plan_user_search import (
    search_event_plan_staff,
    written_by_me_display_for_staff,
)
from .crm_event_plan_pre_alert_templates import (
    delete_pre_alert_template,
    insert_pre_alert_template,
    list_pre_alert_templates,
)
from .crm_event_plan_purpose_templates import (
    delete_purpose_template,
    insert_purpose_template,
    list_purpose_templates,
)
from .crm_plan_prefill import apply_prefill_to_plan, resolve_account_and_opp_for_links
from .crm_plan_pdf_context import (
    management_support_list_from_plan,
    pad_management_support_rows,
    pad_staff_roster_for_edit,
    staff_roster_list_from_plan,
)
from .crm_roster_summary import format_staff_roster_summary_text
from .crm_static_paths import (
    crm_event_map_path_is_allowed,
    crm_event_plan_branding_logo_relative_subpath,
    crm_event_plan_branding_logo_write_dir,
    crm_event_plan_covers_relative_subpath,
    crm_event_plan_covers_write_dir,
    crm_event_plan_maps_relative_subpath,
    crm_event_plan_maps_write_dir,
    crm_event_plan_pdf_logo_path_is_allowed,
    crm_plan_cover_image_path_is_allowed,
    crm_resolve_event_plan_map_path,
    crm_resolve_event_plan_pdf_logo_path,
    crm_resolve_event_plan_pdf_path,
    crm_resolve_plan_cover_image_path,
)
from .crm_planner_pdf import (
    access_egress_json_has_rows,
    access_egress_list_for_editor,
    access_egress_text_from_structured_rows,
    attendance_by_day_json_has_rows,
    attendance_by_day_list_for_editor,
    attendance_text_from_structured_rows,
    event_days_list_for_editor,
    hospital_table_rows_from_plan,
    operational_timings_json_has_rows,
    operational_timings_list_for_editor,
    operational_timings_text_from_structured_rows,
    pad_access_egress_rows_for_edit,
    pad_attendance_by_day_rows_for_edit,
    pad_event_day_rows_for_edit,
    pad_hospital_rows_for_edit,
    summary_datetimes_from_padded_event_day_rows,
    pad_operational_timings_rows_for_edit,
    pad_rendezvous_rows_for_edit,
    pad_staff_transport_rows_for_edit,
    clinical_signoff_list_for_editor,
    pad_clinical_signoff_rows_for_edit,
    pad_plan_distribution_rows_for_edit,
    pad_uniform_ppe_rows_for_edit,
    rendezvous_json_has_rows,
    rendezvous_list_for_editor,
    rendezvous_text_from_structured_rows,
    staff_transport_json_has_rows,
    staff_transport_list_for_editor,
    staff_transport_text_from_structured_rows,
    store_event_plan_pdf_file,
    plan_distribution_json_has_rows,
    plan_distribution_list_for_editor,
    uniform_ppe_json_has_rows,
    uniform_ppe_list_for_editor,
    uniform_ppe_text_from_structured_rows,
    write_event_plan_pdf,
)

# Medical event planner UI — registered under medical_records (Cura event manager area), not CRM shell.
cura_event_planner_bp = Blueprint(
    "cura_event_planner",
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)


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


def register_cura_event_planner_on_internal(internal_bp):
    @cura_event_planner_bp.route("/")
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

    @cura_event_planner_bp.route("/new", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_new():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, title, account_id, opportunity_id FROM crm_quotes ORDER BY id DESC LIMIT 80")
            quotes = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        q_param = (request.args.get("quote_id") or "").strip()
        pre_quote = int(q_param) if q_param.isdigit() else None
        o_param = (request.args.get("opportunity_id") or "").strip()
        pre_opp = int(o_param) if o_param.isdigit() else None
        suggested_title = ""
        if pre_opp and not pre_quote:
            flash(
                "Event plans are created from a quote — create or open a quote for this opportunity first.",
                "info",
            )
            return redirect(
                quote_new_url(
                    opportunity_id=pre_opp,
                    flow=flow_active(request),
                )
            )
        if pre_quote:
            for q in quotes:
                if int(q["id"]) == pre_quote:
                    qt = (q.get("title") or "").strip()
                    if qt:
                        suggested_title = qt[:255]
                    break

        if request.method == "POST":
            title = (request.form.get("title") or "").strip() or "Event plan"
            qid = _int_or_none(request.form.get("quote_id"))
            account_id = None
            opp_id = None
            if not qid:
                flash("Select a quote — event plans are created from a quote.", "warning")
                return redirect(
                    url_for("medical_records_internal.cura_event_planner.event_plan_new")
                )
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
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """INSERT INTO crm_event_plans
                    (title, status, quote_id, opportunity_id, account_id, created_by, checklist_answers_json, wizard_step)
                    VALUES (%s,'draft',%s,%s,%s,%s,'{}',0)""",
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
                return redirect(event_plan_edit_url(plan_id=int(pid), flow=True))
            finally:
                cur.close()
                conn.close()

        return render_template(
            "admin/crm_event_plan_new.html",
            quotes=quotes,
            pre_quote_id=pre_quote,
            suggested_title=suggested_title,
            pipeline_flow_enabled=flow_active(request),
            pipeline_step="plan",
            pipeline_opp_id=pre_opp,
            pipeline_quote_id=pre_quote,
        )

    @cura_event_planner_bp.route("/<int:plan_id>/edit", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    def event_plan_edit(plan_id: int):
        if request.method == "POST" and not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM crm_event_plans WHERE id=%s", (plan_id,))
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
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plans_list"))

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
            ws_disp = _wizard_step_clamp(request.form.get("wizard_step"))
            return redirect(
                url_for(
                    "medical_records_internal.cura_event_planner.event_plan_edit",
                    plan_id=plan_id,
                )
                + f"?saved=1&step={ws_disp}"
            )

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM crm_event_plans WHERE id=%s", (plan_id,))
            plan = cur.fetchone()
            if not plan:
                flash("Plan not found.", "danger")
                return redirect(url_for("medical_records_internal.cura_event_planner.event_plans_list"))
            cur.execute(
                """SELECT file_path, pdf_generated_at FROM crm_event_plan_pdfs
                WHERE plan_id=%s ORDER BY id DESC LIMIT 8""",
                (plan_id,),
            )
            pdf_history = cur.fetchall() or []
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
                SELECT o.id, o.name, o.account_id, a.name AS account_name, o.lead_meta_json
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
            purpose_templates: list = []
            pre_alert_templates: list = []
            policy_templates_hsw: list = []
            policy_templates_privacy: list = []
            try:
                diagram_rows = list_diagram_rows_for_plan(conn, plan_id)
                diagram_templates = list_diagram_templates(conn)
            except Exception:
                diagram_rows = []
                diagram_templates = []
            try:
                purpose_templates = list_purpose_templates(conn)
            except Exception:
                purpose_templates = []
            try:
                pre_alert_templates = list_pre_alert_templates(conn)
            except Exception:
                pre_alert_templates = []
            try:
                policy_templates_hsw = list_policy_templates(
                    conn, POLICY_CATEGORY_HEALTH_SAFETY
                )
                policy_templates_privacy = list_policy_templates(
                    conn, POLICY_CATEGORY_PRIVACY_IPC
                )
            except Exception:
                policy_templates_hsw = []
                policy_templates_privacy = []
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
            ws = plan.get("wizard_step")
            display_step = _wizard_step_clamp(
                str(ws) if ws is not None else "0"
            )

        cura_ops_event_url = cura_ops_event_detail_url_for_plan(plan)

        po = plan.get("opportunity_id")
        pq = plan.get("quote_id")
        try:
            pipeline_opp_id = int(po) if po is not None else None
        except (TypeError, ValueError):
            pipeline_opp_id = None
        try:
            pipeline_quote_id = int(pq) if pq is not None else None
        except (TypeError, ValueError):
            pipeline_quote_id = None

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

        staff_rows = pad_staff_roster_for_edit(staff_roster_list_from_plan(plan))
        fleet_vehicle_options: list[dict] = []
        fleet_vehicle_option_ids: set[str] = set()
        try:
            fc = get_db_connection()
            try:
                from .crm_fleet_bridge import (
                    apply_fleet_vehicle_display_to_slots,
                    list_fleet_vehicles_for_event_plan_roster,
                )

                fleet_vehicle_options = list_fleet_vehicles_for_event_plan_roster(fc)
                fleet_vehicle_option_ids = {
                    str(x["id"]) for x in fleet_vehicle_options if x.get("id") is not None
                }
                apply_fleet_vehicle_display_to_slots(staff_rows, fc)
            finally:
                fc.close()
        except Exception:
            fleet_vehicle_options = []
            fleet_vehicle_option_ids = set()

        management_support_rows = pad_management_support_rows(
            management_support_list_from_plan(plan)
        )
        risk_rows = pad_risk_register(plan)

        try:
            from .crm_scheduling_bridge import (
                list_active_clients,
                list_job_types,
                scheduling_stack_available,
            )

            scheduling_available = scheduling_stack_available()
            schedule_clients = list_active_clients() if scheduling_available else []
            schedule_job_types = list_job_types() if scheduling_available else []
        except Exception:
            scheduling_available = False
            schedule_clients = []
            schedule_job_types = []

        plan_written_by_me = ""
        if can_edit():
            try:
                from flask_login import current_user

                if getattr(current_user, "is_authenticated", False):
                    wconn = get_db_connection()
                    try:
                        plan_written_by_me = written_by_me_display_for_staff(
                            wconn,
                            getattr(current_user, "id", None),
                            str(getattr(current_user, "username", "") or ""),
                        )
                    finally:
                        wconn.close()
            except Exception:
                plan_written_by_me = ""

        event_day_rows_ed = pad_event_day_rows_for_edit(
            event_days_list_for_editor(plan)
        )
        plan_summary_start, plan_summary_end = (
            summary_datetimes_from_padded_event_day_rows(event_day_rows_ed)
        )

        return render_template(
            "admin/crm_event_plan_edit.html",
            plan=plan,
            questions=questions,
            answers=answers,
            pdf_history=pdf_history,
            hospital_rows=pad_hospital_rows_for_edit(
                hospital_table_rows_from_plan(plan)
            ),
            event_day_rows=event_day_rows_ed,
            plan_summary_start=plan_summary_start,
            plan_summary_end=plan_summary_end,
            attendance_by_day_rows=pad_attendance_by_day_rows_for_edit(
                attendance_by_day_list_for_editor(plan)
            ),
            operational_timings_rows=pad_operational_timings_rows_for_edit(
                operational_timings_list_for_editor(plan)
            ),
            access_egress_rows=pad_access_egress_rows_for_edit(
                access_egress_list_for_editor(plan)
            ),
            rendezvous_rows=pad_rendezvous_rows_for_edit(
                rendezvous_list_for_editor(plan)
            ),
            staff_transport_rows=pad_staff_transport_rows_for_edit(
                staff_transport_list_for_editor(plan)
            ),
            uniform_ppe_rows=pad_uniform_ppe_rows_for_edit(
                uniform_ppe_list_for_editor(plan)
            ),
            plan_distribution_rows=pad_plan_distribution_rows_for_edit(
                plan_distribution_list_for_editor(plan)
            ),
            clinical_signoff_rows=pad_clinical_signoff_rows_for_edit(
                clinical_signoff_list_for_editor(plan)
            ),
            clinical_handover=parse_clinical_handover(
                plan.get("clinical_handover_json")),
            ashice_editor_rows=ashice_editor_rows(),
            atmist_editor_rows=atmist_editor_rows(),
            pre_alert_templates=pre_alert_templates,
            display_step=display_step,
            handoff_log=handoff_log,
            crm_handoff_mode=handoff_mode(),
            cura_ops_event_url=cura_ops_event_url,
            handoff_block_message=handoff_blocked_reason(plan),
            linked_quote=linked_quote,
            staff_roster_rows=staff_rows,
            management_support_rows=management_support_rows,
            risk_register_rows=risk_rows,
            can_edit=can_edit(),
            crm_quotes_dropdown=crm_quotes_dropdown,
            crm_opportunities_dropdown=crm_opportunities_dropdown,
            equipment_rows=equipment_rows,
            diagram_rows=diagram_rows,
            diagram_templates=diagram_templates,
            purpose_templates=purpose_templates,
            policy_templates_hsw=policy_templates_hsw,
            policy_templates_privacy=policy_templates_privacy,
            kit_portal_url=kit_portal_url,
            plan_pdf_version_display=plan_pdf_version_display,
            pipeline_flow_enabled=flow_active(request),
            pipeline_step="plan",
            pipeline_opp_id=pipeline_opp_id,
            pipeline_quote_id=pipeline_quote_id,
            scheduling_available=scheduling_available,
            schedule_clients=schedule_clients,
            schedule_job_types=schedule_job_types,
            event_plan_user_search_url=url_for(
                "medical_records_internal.cura_event_planner.event_plan_user_search"
            ),
            event_plan_purpose_template_url=url_for(
                "medical_records_internal.cura_event_planner.event_plan_purpose_template_create"
            ),
            fleet_vehicle_options=fleet_vehicle_options,
            fleet_vehicle_option_ids=fleet_vehicle_option_ids,
            plan_written_by_me=plan_written_by_me,
            major_incident_detail=coerce_major_incident_detail(
                plan.get("major_incident_detail_json")
            ),
            accommodation_venue_rows=pad_accommodation_venue_rows_for_edit(
                accommodation_venues_from_plan(plan)
            ),
            accommodation_room_rows=pad_accommodation_room_rows_for_edit(
                accommodation_rooms_from_plan(plan)
            ),
        )

    @cura_event_planner_bp.route("/<int:plan_id>/schedule-sync", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def event_plan_schedule_sync(plan_id: int):
        """Push event plan window to ``schedule_shifts`` (non-Cura); roster syncs back from scheduling."""
        from datetime import datetime as dt_mod

        from .crm_scheduling_bridge import (
            scheduling_stack_available,
            sync_event_plan_to_schedule_shift,
        )

        if not scheduling_stack_available():
            flash("Scheduling module is not available.", "warning")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

        conn_gate = get_db_connection()
        cur_gate = conn_gate.cursor(dictionary=True)
        try:
            cur_gate.execute(
                "SELECT IFNULL(cura_sync_enabled, 0) AS cura_sync_enabled FROM crm_event_plans WHERE id=%s",
                (int(plan_id),),
            )
            sync_row = cur_gate.fetchone() or {}
        finally:
            cur_gate.close()
            conn_gate.close()
        try:
            cura_on = int(sync_row.get("cura_sync_enabled") or 0) == 1
        except (TypeError, ValueError):
            cura_on = False
        if cura_on:
            flash(
                "Non-Cura scheduling is not available while push to Cura ops is allowed. Turn it off, save the plan, then use scheduling.",
                "warning",
            )
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

        cid_raw = (request.form.get("schedule_client_id") or "").strip()
        jt_raw = (request.form.get("schedule_job_type_id") or "").strip()
        wd_raw = (request.form.get("schedule_work_date") or "").strip()
        st_raw = (request.form.get("schedule_start") or "").strip()
        en_raw = (request.form.get("schedule_end") or "").strip()
        br_raw = (request.form.get("schedule_break_mins") or "0").strip()
        rc_raw = (request.form.get("schedule_required_count") or "1").strip()
        status = (request.form.get("schedule_status") or "published").strip()
        site_raw = (request.form.get("schedule_site_id") or "").strip()

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT start_datetime, end_datetime FROM crm_event_plans WHERE id=%s",
                (int(plan_id),),
            )
            prow = cur.fetchone() or {}
        finally:
            cur.close()
            conn.close()

        def _part(dv, want_date: bool):
            if dv is None:
                return None
            if want_date and hasattr(dv, "date"):
                return dv.date()
            if not want_date and hasattr(dv, "time"):
                return dv.time().replace(microsecond=0)
            return None

        if not wd_raw:
            d0 = _part(prow.get("start_datetime"), True)
            if d0:
                wd_raw = d0.isoformat()
        if not st_raw:
            t0 = _part(prow.get("start_datetime"), False)
            if t0:
                st_raw = t0.strftime("%H:%M")
        if not en_raw:
            t1 = _part(prow.get("end_datetime"), False)
            if t1:
                en_raw = t1.strftime("%H:%M")

        if not cid_raw.isdigit() or not jt_raw.isdigit():
            flash(
                "Client and job type are required for scheduling (pick from lists).",
                "danger",
            )
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        if not wd_raw or not st_raw or not en_raw:
            flash(
                "Work date and start/end times are required — set them on the plan or fill the form.",
                "danger",
            )
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        try:
            work_date = dt_mod.strptime(wd_raw, "%Y-%m-%d").date()
            scheduled_start = dt_mod.strptime(st_raw, "%H:%M").time()
            scheduled_end = dt_mod.strptime(en_raw, "%H:%M").time()
        except ValueError:
            flash("Work date must be YYYY-MM-DD and times HH:MM (24h).", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        site_id = int(site_raw) if site_raw.isdigit() else None
        try:
            break_mins = max(0, int(br_raw))
        except ValueError:
            break_mins = 0
        try:
            required_count = max(1, int(rc_raw))
        except ValueError:
            required_count = 1
        actor_uid = None
        actor_uname = None
        try:
            from flask_login import current_user

            if getattr(current_user, "is_authenticated", False):
                try:
                    actor_uid = int(
                        getattr(current_user, "id", 0) or 0) or None
                except (TypeError, ValueError):
                    actor_uid = None
                actor_uname = (
                    str(getattr(current_user, "username", "") or "").strip()
                    or str(getattr(current_user, "email", "") or "").strip()
                    or None
                )
        except Exception:
            pass

        res = sync_event_plan_to_schedule_shift(
            plan_id=int(plan_id),
            client_id=int(cid_raw),
            job_type_id=int(jt_raw),
            work_date=work_date,
            scheduled_start=scheduled_start,
            scheduled_end=scheduled_end,
            site_id=site_id,
            break_mins=break_mins,
            required_count=required_count,
            status=status or "published",
            actor_user_id=actor_uid,
            actor_username=actor_uname,
        )
        if res.get("ok"):
            flash(
                "Scheduling shift saved. Assign crew in Shifts — roster syncs back here for PDFs (no Cura).",
                "success",
            )
        else:
            flash(res.get("error") or "Could not save scheduling shift.", "danger")
        return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

    @cura_event_planner_bp.route("/<int:plan_id>/pdf", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_pdf(plan_id: int):
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM crm_event_plans WHERE id=%s", (plan_id,))
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
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plans_list"))

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "UPDATE crm_event_plans SET plan_pdf_revision = IFNULL(plan_pdf_revision, 0) + 1 WHERE id=%s",
                (plan_id,),
            )
            conn.commit()
            cur.execute(
                "SELECT * FROM crm_event_plans WHERE id=%s", (plan_id,))
            plan = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        if not plan:
            flash("Plan not found.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plans_list"))

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
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

        try:
            rel = store_event_plan_pdf_file(app, plan_id, pdf_bytes, pdf_hash)
        except Exception as e:
            _rollback_event_plan_pdf_revision(plan_id)
            flash(f"Could not save PDF file: {e}", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

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
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        finally:
            cur_ins.close()
            conn_ins.close()
        flash("PDF generated.", "success")
        return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

    @cura_event_planner_bp.route("/<int:plan_id>/pdf-download")
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    def event_plan_pdf_download(plan_id: int):
        rel = (request.args.get("f") or "").strip().lstrip("/\\")
        if ".." in rel or not rel.startswith("uploads/crm_event_plans/"):
            flash("Invalid file.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
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
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        app = current_app._get_current_object()
        abs_path = crm_resolve_event_plan_pdf_path(app, rel)
        if not abs_path:
            flash("PDF file not on disk.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        directory = os.path.dirname(abs_path)
        fname = os.path.basename(abs_path)
        return send_from_directory(
            directory,
            fname,
            as_attachment=True,
            download_name=f"event_plan_{plan_id}.pdf",
        )

    @cura_event_planner_bp.route("/<int:plan_id>/handoff", methods=["POST"])
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
            flash(result.get("message") or result.get(
                "status", "OK"), "success")
        else:
            flash(result.get("error") or "Handoff failed.", "danger")
        return redirect(
            url_for("medical_records_internal.cura_event_planner.event_plan_edit",
                    plan_id=plan_id) + "#crmCuraHandoffBanner"
        )

    @cura_event_planner_bp.route("/<int:plan_id>/equipment/add", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_equipment_add(plan_id: int):
        if not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        raw = (request.form.get("equipment_asset_lookup") or "").strip()
        conn = get_db_connection()
        try:
            aid = resolve_equipment_asset_id(conn, raw)
            if not aid:
                flash(
                    "Equipment asset not found (try numeric ID or public asset code).", "warning")
            elif add_equipment_asset_to_plan(conn, plan_id, aid):
                flash("Kit / asset linked to this event plan.", "success")
            else:
                flash("That asset is already linked.", "info")
        except Exception as e:
            flash(f"Could not link asset: {e}", "danger")
        finally:
            conn.close()
        return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

    @cura_event_planner_bp.route("/<int:plan_id>/equipment/remove", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_equipment_remove(plan_id: int):
        if not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        aid = _int_or_none(request.form.get("equipment_asset_id"))
        if aid:
            conn = get_db_connection()
            try:
                remove_equipment_asset_from_plan(conn, plan_id, aid)
                flash("Asset unlinked from plan.", "success")
            finally:
                conn.close()
        return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

    @cura_event_planner_bp.route("/<int:plan_id>/diagrams/add-template", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_diagram_add_template(plan_id: int):
        if not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        tid = _int_or_none(request.form.get("diagram_template_id"))
        if tid:
            conn = get_db_connection()
            try:
                add_diagram_from_template(conn, plan_id, tid)
                flash(
                    "Diagram added from template (updates if template image changes).", "success")
            except Exception as e:
                flash(f"Could not add diagram: {e}", "danger")
            finally:
                conn.close()
        return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

    @cura_event_planner_bp.route("/<int:plan_id>/diagrams/upload", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_diagram_upload(plan_id: int):
        if not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        f = request.files.get("diagram_file")
        if not f or not f.filename:
            flash("Choose an image file.", "warning")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        app = current_app._get_current_object()
        conn = get_db_connection()
        try:
            store_uploaded_diagram(app, plan_id, f, conn)
            flash("Diagram uploaded.", "success")
        except Exception as e:
            flash(str(e), "danger")
        finally:
            conn.close()
        return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

    @cura_event_planner_bp.route("/<int:plan_id>/diagrams/<int:dg_id>/delete", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_diagram_delete(plan_id: int, dg_id: int):
        if not can_edit():
            flash("You do not have permission to edit event plans.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))
        app = current_app._get_current_object()
        conn = get_db_connection()
        try:
            delete_diagram(conn, dg_id, plan_id, app)
            flash("Diagram removed.", "success")
        except Exception as e:
            flash(str(e), "danger")
        finally:
            conn.close()
        return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=plan_id))

    @cura_event_planner_bp.route("/diagram-templates", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_diagram_templates_admin():
        if not can_edit():
            flash("You do not have permission to edit diagram templates.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plans_list"))
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
                title = (request.form.get("template_title")
                         or "").strip() or "Diagram"
                if not f or not f.filename:
                    flash("Choose an image file.", "warning")
                else:
                    try:
                        store_template_diagram_image(app, f, title)
                        flash("Template added.", "success")
                    except Exception as e:
                        flash(str(e), "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_diagram_templates_admin"))
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

    @cura_event_planner_bp.route("/purpose-templates", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_purpose_templates_admin():
        if not can_edit():
            flash("You do not have permission to edit purpose templates.", "danger")
            return redirect(
                url_for("medical_records_internal.cura_event_planner.event_plans_list")
            )
        if request.method == "POST":
            act = (request.form.get("action") or "").strip().lower()
            if act == "delete":
                tid = _int_or_none(request.form.get("template_id"))
                if tid:
                    conn = get_db_connection()
                    try:
                        delete_purpose_template(conn, tid)
                        flash("Purpose template removed.", "success")
                    except Exception as e:
                        flash(str(e), "danger")
                    finally:
                        conn.close()
            else:
                title = (request.form.get("template_title") or "").strip()
                body = (request.form.get("template_body") or "").strip()
                if not body:
                    flash("Enter purpose text for the template.", "warning")
                else:
                    conn = get_db_connection()
                    try:
                        insert_purpose_template(conn, title, body)
                        flash("Purpose template added.", "success")
                    except Exception as e:
                        flash(str(e), "danger")
                    finally:
                        conn.close()
            return redirect(
                url_for(
                    "medical_records_internal.cura_event_planner.event_plan_purpose_templates_admin"
                )
            )
        conn = get_db_connection()
        try:
            templates = list_purpose_templates(conn)
        except Exception:
            templates = []
        finally:
            conn.close()
        return render_template(
            "admin/crm_event_plan_purpose_templates.html",
            templates=templates,
            can_edit=can_edit(),
        )

    @cura_event_planner_bp.route("/purpose-template", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_purpose_template_create():
        """Create a purpose template from the plan editor (JSON). SeaSurf adds ``X-CSRFToken`` on fetch."""
        if not can_edit():
            return jsonify({"ok": False, "error": "Forbidden"}), 403
        data = request.get_json(silent=True) or {}
        title = (data.get("title") or "").strip()[:255]
        body = (data.get("body") or "").strip()
        if not body:
            return jsonify({"ok": False, "error": "Purpose text is required."}), 400
        conn = get_db_connection()
        try:
            try:
                new_id = insert_purpose_template(
                    conn, title or "Purpose template", body[:65000]
                )
            except ValueError as e:
                return jsonify({"ok": False, "error": str(e)}), 400
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute(
                    """
                    SELECT id, title, body, sort_order
                    FROM crm_event_plan_purpose_templates
                    WHERE id = %s
                    """,
                    (int(new_id),),
                )
                row = cur.fetchone() or {}
            finally:
                cur.close()
            b = row.get("body")
            if isinstance(b, (bytes, bytearray)):
                row["body"] = b.decode("utf-8", errors="replace")
            return jsonify(
                {
                    "ok": True,
                    "template": {
                        "id": int(row.get("id") or new_id),
                        "title": str(row.get("title") or title or "Purpose template"),
                        "body": str(row.get("body") or body),
                        "sort_order": int(row.get("sort_order") or 0),
                    },
                }
            )
        except Exception as e:
            current_app.logger.exception("event_plan_purpose_template_create")
            return jsonify({"ok": False, "error": str(e) or "Save failed"}), 500
        finally:
            conn.close()

    @cura_event_planner_bp.route("/pre-alert-templates", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_pre_alert_templates_admin():
        if not can_edit():
            flash("You do not have permission to edit pre-alert templates.", "danger")
            return redirect(
                url_for("medical_records_internal.cura_event_planner.event_plans_list")
            )
        if request.method == "POST":
            act = (request.form.get("action") or "").strip().lower()
            if act == "delete":
                tid = _int_or_none(request.form.get("template_id"))
                if tid:
                    conn = get_db_connection()
                    try:
                        delete_pre_alert_template(conn, tid)
                        flash("Pre-alert template removed.", "success")
                    except Exception as e:
                        flash(str(e), "danger")
                    finally:
                        conn.close()
            return redirect(
                url_for(
                    "medical_records_internal.cura_event_planner.event_plan_pre_alert_templates_admin"
                )
            )
        conn = get_db_connection()
        try:
            templates = list_pre_alert_templates(conn)
        except Exception:
            templates = []
        finally:
            conn.close()
        return render_template(
            "admin/crm_event_plan_pre_alert_templates.html",
            templates=templates,
            can_edit=can_edit(),
        )

    @cura_event_planner_bp.route("/pre-alert-templates/save", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_pre_alert_template_save():
        if not can_edit():
            flash("You do not have permission to save pre-alert templates.", "danger")
            return redirect(
                url_for("medical_records_internal.cura_event_planner.event_plans_list")
            )
        title = (request.form.get("template_title") or "").strip()
        raw = (request.form.get("payload_json") or "").strip()
        plan_back = _int_or_none(request.form.get("return_plan_id"))
        try:
            body = json.loads(raw) if raw else {}
            policy = (body.get("policy") or body.get("pre_alert_policy") or "").strip().lower()
            conn = get_db_connection()
            try:
                insert_pre_alert_template(conn, title or "Pre-alert template", policy, body)
                flash("Pre-alert template saved. You can insert it on any plan from the Pre-Alert section.", "success")
            finally:
                conn.close()
        except Exception as e:
            flash(str(e), "danger")
        if plan_back:
            return redirect(
                url_for(
                    "medical_records_internal.cura_event_planner.event_plan_edit",
                    plan_id=plan_back,
                )
            )
        return redirect(
            url_for(
                "medical_records_internal.cura_event_planner.event_plan_pre_alert_templates_admin"
            )
        )

    @cura_event_planner_bp.route("/policy-templates", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    @crm_edit_required
    def event_plan_policy_templates_admin():
        if not can_edit():
            flash("You do not have permission to edit policy templates.", "danger")
            return redirect(
                url_for("medical_records_internal.cura_event_planner.event_plans_list")
            )
        if request.method == "POST":
            act = (request.form.get("action") or "").strip().lower()
            if act == "delete":
                tid = _int_or_none(request.form.get("template_id"))
                if tid:
                    conn = get_db_connection()
                    try:
                        delete_policy_template(conn, tid)
                        flash("Policy template removed.", "success")
                    except Exception as e:
                        flash(str(e), "danger")
                    finally:
                        conn.close()
            else:
                cat = (request.form.get("policy_category") or "").strip()
                title = (request.form.get("template_title") or "").strip()
                body = (request.form.get("template_body") or "").strip()
                if cat not in (
                    POLICY_CATEGORY_HEALTH_SAFETY,
                    POLICY_CATEGORY_PRIVACY_IPC,
                ):
                    flash("Invalid policy category.", "danger")
                elif not body:
                    flash("Enter policy text for the template.", "warning")
                else:
                    conn = get_db_connection()
                    try:
                        insert_policy_template(conn, cat, title, body)
                        flash("Policy template added.", "success")
                    except Exception as e:
                        flash(str(e), "danger")
                    finally:
                        conn.close()
            return redirect(
                url_for(
                    "medical_records_internal.cura_event_planner.event_plan_policy_templates_admin"
                )
                + ("#privacy" if request.form.get("policy_category") == POLICY_CATEGORY_PRIVACY_IPC else "#hsw")
            )
        conn = get_db_connection()
        try:
            templates_hsw = list_policy_templates(conn, POLICY_CATEGORY_HEALTH_SAFETY)
            templates_privacy = list_policy_templates(conn, POLICY_CATEGORY_PRIVACY_IPC)
        except Exception:
            templates_hsw = []
            templates_privacy = []
        finally:
            conn.close()
        return render_template(
            "admin/crm_event_plan_policy_templates.html",
            templates_hsw=templates_hsw,
            templates_privacy=templates_privacy,
            can_edit=can_edit(),
        )

    @cura_event_planner_bp.route("/api/plan-user-search")
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    def event_plan_user_search():
        """Typeahead for roster / management rows: HR ``tb_contractors`` + ``hr_staff_details``, merged with ``users``."""
        q = (request.args.get("q") or "").strip()
        if len(q) < 2:
            return jsonify({"results": []}), 200
        if len(q) > 64:
            return jsonify({"results": [], "error": "query too long"}), 400
        if not re.match(r"^[\w.@+\-\s]+$", q, re.I):
            return jsonify({"results": [], "error": "invalid query"}), 400
        pat = f"%{q}%"
        conn = get_db_connection()
        try:
            out = search_event_plan_staff(conn, pat)
        except Exception:
            current_app.logger.exception("event_plan_user_search")
            return jsonify({"results": [], "error": "search failed"}), 500
        finally:
            conn.close()
        return jsonify({"results": out}), 200

    @cura_event_planner_bp.route("/api/hospital-suggest")
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

    @cura_event_planner_bp.route("/questions")
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

    @cura_event_planner_bp.route("/questions/<int:qid>/edit", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_medical_surface_required
    def event_plan_question_edit(qid: int):
        if request.method == "POST" and not can_edit():
            flash("You do not have permission to edit questions.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_question_edit", qid=qid))

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM crm_event_plan_questions WHERE id=%s", (qid,))
            row = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        if not row:
            flash("Question not found.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_questions_list"))

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
                    return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_questions_list"))
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

    @cura_event_planner_bp.route("/settings/pdf-cover", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def event_plan_pdf_settings():
        """Event plan PDF cover text (moved from CRM settings with event planning)."""
        app = current_app._get_current_object()
        if request.method == "POST" and not can_edit():
            flash("You do not have permission to change these settings.", "danger")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_pdf_settings"))

        from .crm_manifest_settings import get_crm_module_settings, update_crm_module_settings

        if request.method == "POST" and can_edit():
            text_updates = {
                "event_plan_pdf_tagline": (
                    request.form.get("event_plan_pdf_tagline") or ""
                ).strip()[:500],
                "event_plan_pdf_about_us": (
                    request.form.get("event_plan_pdf_about_us") or ""
                ).strip()[:20000],
            }
            logo_updates = _event_plan_pdf_logo_updates_from_request(app)
            merged = dict(text_updates)
            if logo_updates is not None:
                merged.update(logo_updates)
            update_crm_module_settings(app, merged)
            flash("Event plan PDF cover settings saved to core manifest.", "success")
            return redirect(url_for("medical_records_internal.cura_event_planner.event_plan_pdf_settings"))

        settings = get_crm_module_settings(app)
        return render_template(
            "admin/crm_event_plan_pdf_settings.html",
            settings=settings,
            can_edit=can_edit(),
        )

    internal_bp.register_blueprint(
        cura_event_planner_bp,
        url_prefix="/clinical/cura-ops/event-plans",
    )


def register_crm_event_planner_legacy_redirects(crm_bp):
    """
    Historical URLs lived under ``/plugin/crm_module/event-plans/...``.
    Event planning now lives under Medical → Cura event manager.
    """
    from flask import redirect

    _base = "/plugin/medical_records_module/clinical/cura-ops/event-plans"

    @crm_bp.route("/event-plans", defaults={"sub": ""})
    @crm_bp.route("/event-plans/<path:sub>")
    @login_required
    def _legacy_crm_event_planner_paths(sub: str):
        tail = ("/" + sub.lstrip("/")) if (sub or "").strip() else "/"
        dest = _base + (tail if tail != "/" else "/")
        if request.query_string:
            dest = dest + ("?" + request.query_string.decode())
        return redirect(dest, code=301)

    @crm_bp.route("/api/event-plans/hospital-suggest")
    @login_required
    def _legacy_crm_event_plan_hospital_suggest():
        dest = _base + "/api/hospital-suggest"
        if request.query_string:
            dest = dest + ("?" + request.query_string.decode())
        return redirect(dest, code=301)

    @crm_bp.route("/settings/event-plan-pdf")
    @login_required
    def _legacy_crm_event_plan_pdf_settings():
        dest = _base + "/settings/pdf-cover"
        if request.query_string:
            dest = dest + ("?" + request.query_string.decode())
        return redirect(dest, code=301)


def _plan_status(raw: str | None) -> str:
    s = (raw or "draft").strip().lower()
    if s in ("draft", "completed", "archived"):
        return s
    return "draft"


def _wizard_step_clamp(raw: str | None) -> int:
    """Event plan editor step index including 0 = CRM links tab (0–11)."""
    try:
        n = int((raw or "0").strip())
    except ValueError:
        n = 0
    return max(0, min(11, n))


def _int_or_none(raw: str | None) -> int | None:
    s = (raw or "").strip()
    return int(s) if s.isdigit() else None


def _link_int_id(raw) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


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


def _attendance_amount_loose(raw: str | None) -> int | None:
    digits = "".join(c for c in (raw or "") if c.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _attendance_by_day_rows_from_form() -> list[dict[str, Any]]:
    labels = request.form.getlist("att_day_label")
    dates = request.form.getlist("att_day_date")
    amounts = request.form.getlist("att_day_amount")
    n = max(len(labels), len(dates), len(amounts))
    out: list[dict[str, Any]] = []
    for i in range(n):
        lbl = (labels[i] if i < len(labels) else "").strip()[:64]
        ds = (dates[i] if i < len(dates) else "").strip()[:10]
        amt = _attendance_amount_loose(
            amounts[i] if i < len(amounts) else None)
        if not ds and amt is None and not lbl:
            continue
        if not ds or amt is None:
            continue
        try:
            datetime.strptime(ds, "%Y-%m-%d")
        except ValueError:
            continue
        row: dict[str, Any] = {"date": ds, "amount": amt}
        if lbl:
            row["label"] = lbl
        out.append(row)
    return out


def _op_time_norm(raw: str | None) -> str | None:
    s = (raw or "").strip()
    if not s:
        return None
    if len(s) >= 8 and s[2] == ":" and s[5] == ":":
        return s[:5]
    if len(s) >= 5 and s[2] == ":":
        return s[:5]
    return s or None


def _operational_timings_rows_from_form() -> list[dict[str, Any]]:
    dates = request.form.getlist("op_tim_date")
    times = request.form.getlist("op_tim_time")
    descs = request.form.getlist("op_tim_desc")
    n = max(len(dates), len(times), len(descs))
    out: list[dict[str, Any]] = []
    for i in range(n):
        ds = (dates[i] if i < len(dates) else "").strip()[:10]
        tm = _op_time_norm(times[i] if i < len(times) else None)
        desc = (descs[i] if i < len(descs) else "").strip()[:4000]
        if not ds and not tm and not desc:
            continue
        if ds:
            try:
                datetime.strptime(ds, "%Y-%m-%d")
            except ValueError:
                continue
        row: dict[str, Any] = {}
        if ds:
            row["date"] = ds
        if tm:
            row["time"] = tm
        if desc:
            row["description"] = desc
        if row:
            out.append(row)
    return out


def _operational_timings_save_pair(prev: dict) -> tuple[str | None, str | None]:
    """Returns ``(operational_timings_json, operational_timings)`` for UPDATE."""
    rows = _operational_timings_rows_from_form()
    had_structured = operational_timings_json_has_rows(
        prev.get("operational_timings_json")
    )
    if rows:
        return (
            json.dumps(rows, ensure_ascii=False),
            operational_timings_text_from_structured_rows(rows),
        )
    if had_structured:
        return None, None
    prev_text = prev.get("operational_timings")
    if isinstance(prev_text, (bytes, bytearray)):
        prev_text = prev_text.decode("utf-8", errors="replace")
    s = (prev_text or "").strip()
    return None, (s or None)


def _access_egress_rows_from_form() -> list[dict[str, Any]]:
    names = request.form.getlist("ae_name")
    addresses = request.form.getlist("ae_address")
    w3ws = request.form.getlist("ae_w3w")
    n = max(len(names), len(addresses), len(w3ws))
    out: list[dict[str, Any]] = []
    for i in range(n):
        name = (names[i] if i < len(names) else "").strip()[:256]
        addr = (addresses[i] if i < len(addresses) else "").strip()[:512]
        w = (w3ws[i] if i < len(w3ws) else "").strip()[:128]
        if not name and not addr and not w:
            continue
        row: dict[str, Any] = {}
        if name:
            row["name"] = name
        if addr:
            row["address"] = addr
        if w:
            row["what3words"] = w
        if row:
            out.append(row)
    return out


def _access_egress_save_pair(prev: dict) -> tuple[str | None, str | None]:
    rows = _access_egress_rows_from_form()
    had_structured = access_egress_json_has_rows(
        prev.get("access_egress_json"))
    if rows:
        return (
            json.dumps(rows, ensure_ascii=False),
            access_egress_text_from_structured_rows(rows),
        )
    if had_structured:
        return None, None
    prev_text = prev.get("access_egress_text")
    if isinstance(prev_text, (bytes, bytearray)):
        prev_text = prev_text.decode("utf-8", errors="replace")
    s = (prev_text or "").strip()
    return None, (s or None)


def _rendezvous_rows_from_form() -> list[dict[str, Any]]:
    names = request.form.getlist("rvp_name")
    addresses = request.form.getlist("rvp_address")
    w3ws = request.form.getlist("rvp_w3w")
    n = max(len(names), len(addresses), len(w3ws))
    out: list[dict[str, Any]] = []
    for i in range(n):
        name = (names[i] if i < len(names) else "").strip()[:256]
        addr = (addresses[i] if i < len(addresses) else "").strip()[:512]
        w = (w3ws[i] if i < len(w3ws) else "").strip()[:128]
        if not name and not addr and not w:
            continue
        row: dict[str, Any] = {}
        if name:
            row["name"] = name
        if addr:
            row["address"] = addr
        if w:
            row["what3words"] = w
        if row:
            out.append(row)
    return out


def _rendezvous_save_pair(prev: dict) -> tuple[str | None, str | None]:
    rows = _rendezvous_rows_from_form()
    had_structured = rendezvous_json_has_rows(prev.get("rendezvous_json"))
    if rows:
        return (
            json.dumps(rows, ensure_ascii=False),
            rendezvous_text_from_structured_rows(rows),
        )
    if had_structured:
        return None, None
    prev_text = prev.get("rendezvous_text")
    if isinstance(prev_text, (bytes, bytearray)):
        prev_text = prev_text.decode("utf-8", errors="replace")
    s = (prev_text or "").strip()
    return None, (s or None)


def _staff_transport_rows_from_form() -> list[dict[str, Any]]:
    dates = request.form.getlist("st_tr_date")
    vehicles = request.form.getlist("st_tr_vehicle")
    staff = request.form.getlist("st_tr_staff")
    n = max(len(dates), len(vehicles), len(staff))
    out: list[dict[str, Any]] = []
    for i in range(n):
        ds = (dates[i] if i < len(dates) else "").strip()[:10]
        veh = (vehicles[i] if i < len(vehicles) else "").strip()[:512]
        stf = (staff[i] if i < len(staff) else "").strip()[:512]
        if not ds and not veh and not stf:
            continue
        if ds:
            try:
                datetime.strptime(ds, "%Y-%m-%d")
            except ValueError:
                continue
        row: dict[str, Any] = {}
        if ds:
            row["date"] = ds
        if veh:
            row["vehicle"] = veh
        if stf:
            row["staff"] = stf
        if row:
            out.append(row)
    return out


def _staff_transport_save_pair(prev: dict) -> tuple[str | None, str | None]:
    rows = _staff_transport_rows_from_form()
    had_structured = staff_transport_json_has_rows(
        prev.get("staff_transport_json"))
    if rows:
        return (
            json.dumps(rows, ensure_ascii=False),
            staff_transport_text_from_structured_rows(rows),
        )
    if had_structured:
        return None, None
    prev_text = prev.get("staff_transport_text")
    if isinstance(prev_text, (bytes, bytearray)):
        prev_text = prev_text.decode("utf-8", errors="replace")
    s = (prev_text or "").strip()
    return None, (s or None)


def _uniform_ppe_rows_from_form() -> list[dict[str, Any]]:
    types = request.form.getlist("upe_type")
    yns = request.form.getlist("upe_yes_no")
    notes = request.form.getlist("upe_notes")
    n = max(len(types), len(yns), len(notes))
    out: list[dict[str, Any]] = []
    for i in range(n):
        typ = (types[i] if i < len(types) else "").strip()[:256]
        yn = (yns[i] if i < len(yns) else "").strip().lower()
        if yn not in ("yes", "no", ""):
            yn = ""
        nt = (notes[i] if i < len(notes) else "").strip()[:512]
        if not typ and not yn and not nt:
            continue
        row: dict[str, Any] = {}
        if typ:
            row["ppe_type"] = typ
        if yn:
            row["yes_no"] = yn
        if nt:
            row["notes"] = nt
        if row:
            out.append(row)
    return out


def _uniform_ppe_save_pair(prev: dict) -> tuple[str | None, str | None]:
    rows = _uniform_ppe_rows_from_form()
    had_structured = uniform_ppe_json_has_rows(prev.get("uniform_ppe_json"))
    if rows:
        return (
            json.dumps(rows, ensure_ascii=False),
            uniform_ppe_text_from_structured_rows(rows),
        )
    if had_structured:
        return None, None
    prev_text = prev.get("uniform_ppe_text")
    if isinstance(prev_text, (bytes, bytearray)):
        prev_text = prev_text.decode("utf-8", errors="replace")
    s = (prev_text or "").strip()
    return None, (s or None)


def _plan_distribution_lines_from_form() -> list[str]:
    raw_lines = request.form.getlist("plan_dist_line")
    out: list[str] = []
    for raw in raw_lines:
        s = (raw or "").strip()[:512]
        if s:
            out.append(s)
    return out


def _plan_distribution_save_pair(prev: dict) -> tuple[str | None, str | None]:
    lines = _plan_distribution_lines_from_form()
    had_structured = plan_distribution_json_has_rows(
        prev.get("plan_distribution_json")
    )
    if lines:
        text = "\n".join(lines)
        return json.dumps(lines, ensure_ascii=False), (text or None)
    if had_structured:
        return None, None
    prev_text = prev.get("plan_distribution")
    if isinstance(prev_text, (bytes, bytearray)):
        prev_text = prev_text.decode("utf-8", errors="replace")
    s = (prev_text or "").strip()
    return None, (s or None)


def _clinical_signoff_rows_from_form() -> list[dict[str, Any]]:
    names = request.form.getlist("clinical_so_name")
    roles = request.form.getlist("clinical_so_role")
    ats = request.form.getlist("clinical_so_at")
    n = max(len(names), len(roles), len(ats))
    out: list[dict[str, Any]] = []
    for i in range(n):
        name = (names[i] if i < len(names) else "").strip()[:255]
        role = (roles[i] if i < len(roles) else "").strip()[:255]
        raw_at = (ats[i] if i < len(ats) else "") or ""
        at_dt = _parse_dt(raw_at.strip() or None)
        if not name and not role and not at_dt:
            continue
        row: dict[str, Any] = {}
        if name:
            row["name"] = name
        if role:
            row["role"] = role
        if at_dt:
            row["at"] = at_dt.isoformat(timespec="minutes")
        if row:
            out.append(row)
    return out


def _clinical_signoff_bundle_from_form() -> tuple[
    str | None, str | None, str | None, datetime | None
]:
    """JSON array for ``clinical_signoff_json`` plus first row mirrored to legacy columns."""
    rows = _clinical_signoff_rows_from_form()
    if not rows:
        return None, None, None, None
    j = json.dumps(rows, ensure_ascii=False)
    first = rows[0]
    fn = str(first.get("name") or "").strip()[:255] or None
    fr = str(first.get("role") or "").strip()[:255] or None
    raw_at = first.get("at")
    fa = _parse_dt(raw_at) if raw_at else None
    return j, fn, fr, fa


def _attendance_by_day_save_pair(prev: dict) -> tuple[str | None, str | None]:
    """Returns ``(attendance_by_day_json, attendance_by_day_text)`` for UPDATE.

    Clears both when structured rows are empty only if the plan already used JSON;
    otherwise preserves legacy free-text until structured rows are saved.
    """
    rows = _attendance_by_day_rows_from_form()
    had_structured = attendance_by_day_json_has_rows(
        prev.get("attendance_by_day_json"))
    if rows:
        return (
            json.dumps(rows, ensure_ascii=False),
            attendance_text_from_structured_rows(rows),
        )
    if had_structured:
        return None, None
    prev_text = prev.get("attendance_by_day_text")
    if isinstance(prev_text, (bytes, bytearray)):
        prev_text = prev_text.decode("utf-8", errors="replace")
    s = (prev_text or "").strip()
    return None, (s or None)


def _summary_datetimes_from_event_day_form():
    """Match UI: first day row start, last day row end (scan ends from bottom for first set time)."""
    starts = request.form.getlist("event_day_start")
    ends = request.form.getlist("event_day_end")
    start_dt = _parse_dt(starts[0]) if starts else None
    end_dt = None
    if ends:
        for i in range(len(ends) - 1, -1, -1):
            en = _parse_dt(ends[i] if i < len(ends) else None)
            if en:
                end_dt = en
                break
    return start_dt, end_dt


def _event_days_json_from_form() -> str | None:
    labels = request.form.getlist("event_day_label")
    starts = request.form.getlist("event_day_start")
    ends = request.form.getlist("event_day_end")
    n = max(len(labels), len(starts), len(ends))
    out: list[dict] = []
    for i in range(n):
        st = _parse_dt(starts[i] if i < len(starts) else None)
        en = _parse_dt(ends[i] if i < len(ends) else None)
        if not st and not en:
            continue
        label = (labels[i] if i < len(labels) else "").strip()[:64]
        if not label:
            label = f"Day {len(out) + 1}"
        row = {
            "label": label,
            "start": st.isoformat(timespec="minutes") if st else None,
            "end": en.isoformat(timespec="minutes") if en else None,
        }
        out.append(row)
    if not out:
        return None
    return json.dumps(out, ensure_ascii=False)


def _hospital_facility_lines_from_form_cell(raw: str | None) -> list[str]:
    s = (raw or "").strip()
    if not s:
        return []
    try:
        v = json.loads(s)
    except json.JSONDecodeError:
        return []
    if not isinstance(v, list):
        return []
    lines: list[str] = []
    for x in v:
        if isinstance(x, str):
            t = x.strip()[:255]
            if t:
                lines.append(t)
        elif isinstance(x, dict):
            t = str(x.get("line") or x.get("text") or "").strip()[:255]
            if t:
                lines.append(t)
    return lines


def _hospital_contact_lines_from_form_cell(raw: str | None) -> list[dict[str, str]]:
    s = (raw or "").strip()
    if not s:
        return []
    try:
        v = json.loads(s)
    except json.JSONDecodeError:
        return []
    if not isinstance(v, list):
        return []
    lines: list[dict[str, str]] = []
    for x in v:
        if not isinstance(x, dict):
            continue
        t = str(x.get("title") or "").strip()[:128]
        d = str(x.get("detail") or x.get("value") or "").strip()[:255]
        if t or d:
            lines.append({"title": t, "detail": d})
    return lines


def _hospitals_json_from_form() -> str | None:
    names = request.form.getlist("hosp_name")
    facilities_json_cells = request.form.getlist("hosp_facilities_json")
    addresses = request.form.getlist("hosp_address")
    contacts_json_cells = request.form.getlist("hosp_contacts_json")
    n = max(
        len(names),
        len(facilities_json_cells),
        len(addresses),
        len(contacts_json_cells),
    )
    out: list[dict[str, Any]] = []
    for i in range(n):
        contact_lines = _hospital_contact_lines_from_form_cell(
            contacts_json_cells[i] if i < len(contacts_json_cells) else None
        )
        facility_lines = _hospital_facility_lines_from_form_cell(
            facilities_json_cells[i] if i < len(facilities_json_cells) else None
        )
        row: dict[str, Any] = {
            "name": (names[i] if i < len(names) else "").strip()[:512],
            "address": (addresses[i] if i < len(addresses) else "").strip()[:512],
            "contact_lines": contact_lines,
        }
        if facility_lines:
            row["facility_lines"] = facility_lines
        if contact_lines:
            row["contact"] = "\n".join(
                (f"{c['title']} — {c['detail']}" if c["title"] else c["detail"])
                for c in contact_lines
            )
        if any((row["name"], row["address"])) or contact_lines or facility_lines:
            out.append(row)
    if not out:
        return None
    return json.dumps(out, ensure_ascii=False)


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
    ashice_keys = ("age", "sex", "history", "injuries", "current", "expected")
    atmist_keys = ("age", "time", "mechanism", "injuries", "signs", "treatment")
    policy = (request.form.get("pre_alert_policy") or "ashice").strip().lower()
    if policy not in ("ashice", "atmist", "custom"):
        policy = "ashice"
    ash = {k: (request.form.get(f"ashice_{k}") or "").strip() for k in ashice_keys}
    atm = {k: (request.form.get(f"atmist_{k}") or "").strip() for k in atmist_keys}
    letters = request.form.getlist("pre_alert_custom_letter")
    details = request.form.getlist("pre_alert_custom_detail")
    n = max(len(letters), len(details))
    custom: list[dict[str, str]] = []
    for i in range(n):
        letter = (letters[i] if i < len(letters) else "").strip()[:16]
        detail = (details[i] if i < len(details) else "").strip()[:2000]
        if letter or detail:
            custom.append({"letter": letter, "detail": detail})
    ash_save = ash if policy == "ashice" else {k: "" for k in ashice_keys}
    atm_save = atm if policy == "atmist" else {k: "" for k in atmist_keys}
    custom_save = custom if policy == "custom" else []
    obj: dict[str, Any] = {
        "medical_control": None,
        "pre_alert_policy": policy,
        "ashice": ash_save,
        "atmist": atm_save,
        "custom_pre_alert": custom_save,
    }
    has_ash = any(ash_save.values())
    has_atm = any(atm_save.values())
    has_custom = bool(custom_save)
    if policy == "ashice" and not has_ash and not has_atm and not has_custom:
        return None
    return json.dumps(obj, ensure_ascii=False)


def _staff_roster_json_from_form() -> str | None:
    callsigns = request.form.getlist("sr_callsign")
    names = request.form.getlist("sr_name")
    roles = request.form.getlist("sr_role")
    posts = request.form.getlist("sr_post")
    vehicles = request.form.getlist("sr_vehicle")
    shifts = request.form.getlist("sr_shift")
    phones = request.form.getlist("sr_phone")
    notes = request.form.getlist("sr_notes")
    n = max(
        len(callsigns),
        len(names),
        len(roles),
        len(posts),
        len(vehicles),
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
        raw_v = (vehicles[i] if i < len(vehicles) else "").strip()
        if raw_v.lower() == "onfoot":
            fv = "onfoot"
        elif raw_v.isdigit():
            fv = str(int(raw_v))
        else:
            fv = ""
        sh = (shifts[i] if i < len(shifts) else "").strip()
        ph = (phones[i] if i < len(phones) else "").strip()
        nt = (notes[i] if i < len(notes) else "").strip()
        if not (cs or nm or rl or po or fv or sh or ph or nt):
            continue
        row: dict[str, str] = {
            "callsign": cs[:64],
            "name": nm[:128],
            "role_grade": rl[:128],
            "post_assignment": po[:255],
            "shift": sh[:128],
            "phone": ph[:32],
            "notes": nt[:512],
        }
        if fv:
            row["fleet_vehicle"] = fv[:32]
        rows.append(row)
    if not rows:
        return None
    return json.dumps(rows, ensure_ascii=False)


def _management_support_json_from_form() -> str | None:
    roles = request.form.getlist("ms_role")
    names = request.form.getlist("ms_name")
    phones = request.form.getlist("ms_phone")
    notes = request.form.getlist("ms_notes")
    n = max(len(roles), len(names), len(phones), len(notes))
    rows: list[dict[str, str]] = []
    for i in range(n):
        rl = (roles[i] if i < len(roles) else "").strip()
        nm = (names[i] if i < len(names) else "").strip()
        ph = (phones[i] if i < len(phones) else "").strip()
        nt = (notes[i] if i < len(notes) else "").strip()
        if not (rl or nm or ph or nt):
            continue
        rows.append(
            {
                "role": rl[:128],
                "name": nm[:128],
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


def _delete_stored_plan_cover_image(app, rel: str | None) -> None:
    if not rel or not crm_plan_cover_image_path_is_allowed(rel):
        return
    abs_path = crm_resolve_plan_cover_image_path(app, rel)
    if abs_path and os.path.isfile(abs_path):
        try:
            os.remove(abs_path)
        except OSError:
            pass


def _next_plan_cover_image_path_after_form(
    app, plan_id: int, old_rel: str | None
) -> str | None:
    old_norm = ((old_rel or "").strip().replace("\\", "/")) or None
    fstor = request.files.get("plan_cover_image_file")
    has_upload = bool(fstor and fstor.filename and str(fstor.filename).strip())
    clear = (request.form.get("plan_cover_image_clear") or "").strip() == "1"
    if has_upload:
        ext = _event_map_upload_extension(fstor.filename)
        if not ext:
            flash(
                "Cover image was not updated — use PNG, JPG, JPEG, WebP, or GIF.",
                "warning",
            )
            return old_norm
        dest_dir = crm_event_plan_covers_write_dir(app)
        os.makedirs(dest_dir, exist_ok=True)
        fname = f"cover_{plan_id}_{uuid.uuid4().hex[:12]}{ext}"
        full = os.path.join(dest_dir, fname)
        try:
            fstor.save(full)
        except OSError as e:
            flash(f"Could not save cover image: {e}", "danger")
            return old_norm
        new_rel = f"{crm_event_plan_covers_relative_subpath()}/{fname}".replace(
            "\\", "/"
        )
        if old_norm and old_norm != new_rel and crm_plan_cover_image_path_is_allowed(
            old_norm
        ):
            _delete_stored_plan_cover_image(app, old_norm)
        return new_rel
    if clear:
        if old_norm and crm_plan_cover_image_path_is_allowed(old_norm):
            _delete_stored_plan_cover_image(app, old_norm)
        return None
    return old_norm


def _event_plan_pdf_logo_updates_from_request(app) -> dict[str, str] | None:
    """Merge dict for ``update_crm_module_settings``, or ``None`` if logo unchanged."""
    clear = (request.form.get("event_plan_pdf_logo_clear") or "").strip() == "1"
    fstor = request.files.get("event_plan_pdf_logo_file")
    has_upload = bool(fstor and fstor.filename and str(fstor.filename).strip())
    if not clear and not has_upload:
        return None
    from .crm_manifest_settings import get_crm_module_settings

    cur_settings = get_crm_module_settings(app)
    old_path = (
        (cur_settings.get("event_plan_pdf_logo_path") or "").strip().replace("\\", "/")
    )
    if clear:
        if old_path and crm_event_plan_pdf_logo_path_is_allowed(old_path):
            abs_p = crm_resolve_event_plan_pdf_logo_path(app, old_path)
            if abs_p and os.path.isfile(abs_p):
                try:
                    os.remove(abs_p)
                except OSError:
                    pass
        return {"event_plan_pdf_logo_path": ""}
    ext = _event_map_upload_extension(fstor.filename)
    if not ext:
        flash(
            "PDF logo not updated — use PNG, JPG, JPEG, WebP, or GIF.",
            "warning",
        )
        return None
    dest = crm_event_plan_branding_logo_write_dir(app)
    os.makedirs(dest, exist_ok=True)
    fname = f"tenant_{uuid.uuid4().hex[:12]}{ext}"
    full = os.path.join(dest, fname)
    try:
        fstor.save(full)
    except OSError as e:
        flash(f"Could not save PDF logo: {e}", "danger")
        return None
    new_rel = f"{crm_event_plan_branding_logo_relative_subpath()}/{fname}".replace(
        "\\", "/"
    )
    if (
        old_path
        and old_path != new_rel
        and crm_event_plan_pdf_logo_path_is_allowed(old_path)
    ):
        abs_p = crm_resolve_event_plan_pdf_logo_path(app, old_path)
        if abs_p and os.path.isfile(abs_p):
            try:
                os.remove(abs_p)
            except OSError:
                pass
    return {"event_plan_pdf_logo_path": new_rel}


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
            """SELECT event_map_path, plan_cover_image_path, quote_id, opportunity_id, account_id,
            attendance_by_day_json, attendance_by_day_text,
            operational_timings_json, operational_timings,
            access_egress_json, access_egress_text,
            rendezvous_json, rendezvous_text,
            staff_transport_json, staff_transport_text,
            uniform_ppe_json, uniform_ppe_text,
            plan_distribution_json, plan_distribution,
            resources_vehicles, resources_comms, escalation_notes,
            command_control_text, equipment_detail_text
            FROM crm_event_plans WHERE id=%s""",
            (plan_id,),
        )
        prev_row = cur.fetchone() or {}
        att_json, att_text = _attendance_by_day_save_pair(prev_row)
        op_json, op_text = _operational_timings_save_pair(prev_row)
        ae_json, ae_text = _access_egress_save_pair(prev_row)
        rvp_json, rvp_text = _rendezvous_save_pair(prev_row)
        st_json, st_text = _staff_transport_save_pair(prev_row)
        upe_json, upe_text = _uniform_ppe_save_pair(prev_row)
        dist_json, dist_text = _plan_distribution_save_pair(prev_row)
        old_map_path = prev_row.get("event_map_path")
        if isinstance(old_map_path, bytes):
            old_map_path = old_map_path.decode("utf-8", errors="replace")
        new_map_path = _next_event_map_path_after_form(
            current_app, plan_id, old_map_path)
        old_cover_path = prev_row.get("plan_cover_image_path")
        if isinstance(old_cover_path, bytes):
            old_cover_path = old_cover_path.decode("utf-8", errors="replace")
        new_cover_path = _next_plan_cover_image_path_after_form(
            current_app, plan_id, old_cover_path
        )
        map_caption = (request.form.get("event_map_caption")
                       or "").strip()[:512] or None
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

    staff_roster_json = _staff_roster_json_from_form()
    roster_rows_summary = staff_roster_list_from_plan(
        {"staff_roster_json": staff_roster_json} if staff_roster_json else {}
    )
    resources_medics_computed = format_staff_roster_summary_text(roster_rows_summary) or None

    def _prev_text(col: str) -> str | None:
        v = prev_row.get(col)
        if v is None:
            return None
        if isinstance(v, bytes):
            v = v.decode("utf-8", errors="replace")
        s = str(v).strip()
        return s or None

    keep_vehicles = _prev_text("resources_vehicles")
    keep_comms = _prev_text("resources_comms")
    keep_escalation = _prev_text("escalation_notes")
    keep_command_control = _prev_text("command_control_text")
    keep_equipment_detail = _prev_text("equipment_detail_text")
    so_json, so_fn, so_fr, so_fa = _clinical_signoff_bundle_from_form()
    summary_start_dt, summary_end_dt = _summary_datetimes_from_event_day_form()

    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE crm_event_plans SET
            title=%s, event_type=%s, start_datetime=%s, end_datetime=%s,
            expected_attendance=%s, demographics_notes=%s, environment_notes=%s,
            address_line1=%s, postcode=%s, what3words=%s, location_notes=%s,
            hospitals_notes=%s, hospitals_json=%s, clinical_handover_json=%s,
            staff_roster_json=%s, management_support_json=%s, event_map_path=%s, event_map_caption=%s, plan_cover_image_path=%s,
            plan_pdf_status=%s, plan_written_by=%s,
            plan_distribution_json=%s, plan_distribution=%s, plan_document_release_date=%s, plan_purpose=%s,
            event_organiser=%s, event_organiser_phone=%s, event_content_summary=%s, purple_guide_tier=%s,
            operational_timings_json=%s, operational_timings=%s, event_days_json=%s,
            access_egress_json=%s, access_egress_text=%s, rendezvous_json=%s, rendezvous_text=%s,
            risk_register_json=%s, health_safety_work_text=%s,
            staff_transport_json=%s, staff_transport_text=%s, command_control_text=%s,
            uniform_ppe_json=%s, uniform_ppe_text=%s,
            privacy_ipc_text=%s, incident_reporting_text=%s, equipment_detail_text=%s,
            major_incident_text=%s, major_incident_detail_json=%s,
            accommodation_enabled=%s, accommodation_venues_json=%s, accommodation_rooms_json=%s,
            welfare_text=%s, safeguarding_text=%s,
            ambulance_trust_text=%s, other_event_specific_text=%s,
            attendance_by_day_json=%s, attendance_by_day_text=%s,
            resources_medics=%s, resources_vehicles=%s,
            resources_comms=%s, escalation_notes=%s, risk_summary=%s,
            risk_score=%s, clinical_signoff_json=%s, signoff_name=%s, signoff_role=%s, signoff_at=%s,
            checklist_answers_json=%s, status=%s, cura_sync_enabled=%s, wizard_step=%s,
            quote_id=%s, opportunity_id=%s, account_id=%s
            WHERE id=%s""",
            (
                (request.form.get("title") or "").strip()[
                    :255] or "Event plan",
                (request.form.get("event_type") or "").strip()[:128] or None,
                summary_start_dt,
                summary_end_dt,
                _int_or_none(request.form.get("expected_attendance")),
                _form_text("demographics_notes"),
                _form_text("environment_notes"),
                (request.form.get("address_line1")
                 or "").strip()[:255] or None,
                (request.form.get("postcode") or "").strip()[:32] or None,
                (request.form.get("what3words") or "").strip()[:64] or None,
                _form_text("location_notes"),
                None,
                _hospitals_json_from_form(),
                _clinical_handover_json_from_form(),
                staff_roster_json,
                _management_support_json_from_form(),
                new_map_path,
                map_caption,
                new_cover_path,
                (request.form.get("plan_pdf_status")
                 or "").strip()[:128] or None,
                (request.form.get("plan_written_by")
                 or "").strip()[:255] or None,
                dist_json,
                dist_text,
                _date_or_none(request.form.get("plan_document_release_date")),
                _form_text("plan_purpose"),
                (request.form.get("event_organiser")
                 or "").strip()[:255] or None,
                (request.form.get("event_organiser_phone")
                 or "").strip()[:64] or None,
                _form_text("event_content_summary"),
                (request.form.get("purple_guide_tier")
                 or "").strip()[:64] or None,
                op_json,
                op_text,
                _event_days_json_from_form(),
                ae_json,
                ae_text,
                rvp_json,
                rvp_text,
                risk_register_json_from_form(),
                _form_text("health_safety_work_text"),
                st_json,
                st_text,
                keep_command_control,
                upe_json,
                upe_text,
                _form_text("privacy_ipc_text"),
                _form_text("incident_reporting_text"),
                keep_equipment_detail,
                _form_text("major_incident_text"),
                major_incident_detail_json_from_form(),
                accommodation_enabled_from_form(),
                accommodation_venues_json_from_form(),
                accommodation_rooms_json_from_form(),
                _form_text("welfare_text"),
                _form_text("safeguarding_text"),
                _form_text("ambulance_trust_text"),
                _form_text("other_event_specific_text"),
                att_json,
                att_text,
                resources_medics_computed,
                keep_vehicles,
                keep_comms,
                keep_escalation,
                _form_text("risk_summary"),
                _int_or_none(request.form.get("risk_score")),
                so_json,
                so_fn,
                so_fr,
                so_fa,
                json.dumps(answers, ensure_ascii=False),
                _plan_status(request.form.get("status")),
                1 if request.form.get("cura_sync_enabled") == "1" else 0,
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
            tail = fk[len("diagram_caption_"):]
            if tail.isdigit():
                save_diagram_caption(conn, int(tail), plan_id, fv)

    pq = _link_int_id(prev_row.get("quote_id"))
    po = _link_int_id(prev_row.get("opportunity_id"))
    nq = _link_int_id(qid_l)
    no = _link_int_id(oid_l)
    link_changed = (pq != nq) or (po != no)
    want_prefill = request.form.get("apply_crm_prefill") == "1"
    if (link_changed or want_prefill) and (nq is not None or no is not None):
        n_pf = apply_prefill_to_plan(
            conn,
            plan_id,
            quote_id=qid_l,
            opportunity_id=oid_l,
            only_if_empty=True,
        )
        if n_pf:
            if link_changed:
                flash(
                    f"Prefilled {n_pf} empty field(s) from CRM (quote/opportunity link changed).",
                    "info",
                )
            else:
                flash(
                    f"Prefilled {n_pf} empty field(s) from the linked quote/opportunity.",
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
