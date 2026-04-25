# CRM admin settings (guide ↔ Time & Billing wage card job types)
from __future__ import annotations

from datetime import date

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import login_required

from app.objects import get_db_connection

from .crm_common import can_edit, crm_access_required
from .crm_event_intake_layout import (
    load_event_intake_layout,
    load_private_transfer_intake_layout,
    parse_extra_fields_from_settings_post,
    parse_hidden_sections_from_settings_post,
    parse_pt_extra_fields_from_settings_post,
    parse_pt_hidden_sections_from_settings_post,
    save_event_intake_layout,
    save_private_transfer_intake_layout,
)
from .crm_event_risk import enrich_lead_meta_with_staffing_breakdown
from .crm_guide_staffing_costs import (
    _resolve_wage_card_id,
    estimate_guide_staffing_costs,
    list_all_job_types,
    list_job_types_on_wage_card,
    load_guide_wage_slot_map,
    save_guide_wage_slot_map,
)


def register_crm_settings_routes(crm_bp):
    @crm_bp.route("/settings/guide-wage-map", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def guide_wage_job_map():
        if request.method == "POST" and not can_edit():
            flash("You do not have permission to change CRM settings.", "danger")
            return redirect(url_for("crm_module.guide_wage_job_map"))

        on = date.today()
        conn = get_db_connection()
        try:
            card_id, card_name = _resolve_wage_card_id(conn, on)
            if request.method == "POST" and can_edit():
                def _int_or_none(key: str):
                    raw = (request.form.get(key) or "").strip()
                    return int(raw) if raw.isdigit() else None

                save_guide_wage_slot_map(
                    conn,
                    clinical=_int_or_none("clinical_job_type_id"),
                    driver=_int_or_none("driver_job_type_id"),
                    clinical_lead=_int_or_none("clinical_lead_job_type_id"),
                    doctor=_int_or_none("doctor_job_type_id"),
                    paramedic_advanced=_int_or_none("paramedic_advanced_job_type_id"),
                    paramedic=_int_or_none("paramedic_job_type_id"),
                    als=_int_or_none("als_job_type_id"),
                    emt=_int_or_none("emt_job_type_id"),
                    technician=_int_or_none("technician_job_type_id"),
                    first_aider=_int_or_none("first_aider_job_type_id"),
                    eca=_int_or_none("eca_job_type_id"),
                    solo_clinical=_int_or_none("solo_clinical_job_type_id"),
                )
                flash("Guide ↔ wage job mapping saved.", "success")
                return redirect(url_for("crm_module.guide_wage_job_map"))

            slot_map = load_guide_wage_slot_map(conn)
            on_card = (
                list_job_types_on_wage_card(conn, card_id, on) if card_id else []
            )
            all_types = list_all_job_types(conn)
            sample_lm = enrich_lead_meta_with_staffing_breakdown(
                {
                    "duration_hours": "8",
                    "expected_attendees": 2500,
                    "risk": {
                        "suggested_medics": 10,
                        "suggested_vehicles": 2,
                        "band": 4,
                        "purple_guide": {"tier": 5},
                        "inputs": {
                            "duration_hours": "8",
                            "expected_attendees": 2500,
                            "venue_outdoor": False,
                            "alcohol": True,
                            "late_finish": False,
                            "crowd_profile": "mixed",
                        },
                    },
                }
            )
            preview = estimate_guide_staffing_costs(sample_lm, on_date=on)
        finally:
            conn.close()

        return render_template(
            "admin/crm_guide_wage_map.html",
            card_id=card_id,
            card_name=card_name or None,
            job_types_on_card=on_card,
            all_job_types=all_types,
            slot_map=slot_map,
            preview_estimate=preview,
            can_edit=can_edit(),
        )

    @crm_bp.route("/settings/dispatch-bases", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def dispatch_bases_settings():
        """Tenant-wide ambulance / crew departure points for intake mileage hints."""
        if request.method == "POST" and not can_edit():
            flash("You do not have permission to change CRM settings.", "danger")
            return redirect(url_for("crm_module.dispatch_bases_settings"))

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            if request.method == "POST" and can_edit():
                action = (request.form.get("action") or "").strip()
                if action == "delete":
                    rid = (request.form.get("base_id") or "").strip()
                    if rid.isdigit():
                        c2 = conn.cursor()
                        try:
                            c2.execute(
                                "DELETE FROM crm_dispatch_bases WHERE id=%s",
                                (int(rid),),
                            )
                            conn.commit()
                            flash("Dispatch base removed.", "success")
                        finally:
                            c2.close()
                    return redirect(url_for("crm_module.dispatch_bases_settings"))
                label = (request.form.get("label") or "").strip()
                if label:
                    pc = (request.form.get("postcode") or "").strip() or None
                    w3w = (request.form.get("what3words") or "").strip() or None
                    lat_r = (request.form.get("lat") or "").strip()
                    lng_r = (request.form.get("lng") or "").strip()
                    lat = lng = None
                    if lat_r and lng_r:
                        try:
                            lat = float(lat_r)
                            lng = float(lng_r)
                            if not (-90 <= lat <= 90 and -180 <= lng <= 180):
                                lat = lng = None
                        except (TypeError, ValueError):
                            lat = lng = None
                    c2 = conn.cursor()
                    try:
                        c2.execute("SELECT IFNULL(MAX(sort_order),0) FROM crm_dispatch_bases")
                        mx_row = c2.fetchone()
                        nxt = int(mx_row[0]) + 1 if mx_row else 1
                        c2.execute(
                            """
                            INSERT INTO crm_dispatch_bases
                            (label, postcode, what3words, lat, lng, sort_order)
                            VALUES (%s,%s,%s,%s,%s,%s)
                            """,
                            (label[:128], pc, w3w, lat, lng, nxt),
                        )
                        conn.commit()
                        flash("Dispatch base added.", "success")
                    finally:
                        c2.close()
                    return redirect(url_for("crm_module.dispatch_bases_settings"))
                flash("Label is required.", "danger")
                return redirect(url_for("crm_module.dispatch_bases_settings"))

            cur.execute(
                "SELECT id, label, postcode, what3words, lat, lng, sort_order "
                "FROM crm_dispatch_bases ORDER BY sort_order ASC, id ASC"
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        return render_template(
            "admin/crm_dispatch_bases.html",
            bases=rows,
            can_edit=can_edit(),
        )

    def _desk_intake_layout_page():
        """Event + private transfer desk intake layout (manifest)."""
        app = current_app._get_current_object()
        redir_ep = "crm_module.desk_intake_layout_settings"
        if request.method == "POST" and not can_edit():
            flash("You do not have permission to change CRM settings.", "danger")
            return redirect(url_for(redir_ep))
        if request.method == "POST" and can_edit():
            target = (request.form.get("layout_save_target") or "event").strip().lower()
            if target == "private_transfer":
                save_private_transfer_intake_layout(
                    app,
                    hidden_sections=parse_pt_hidden_sections_from_settings_post(),
                    extra_fields=parse_pt_extra_fields_from_settings_post(),
                )
                flash("Private transfer desk layout saved.", "success")
            else:
                save_event_intake_layout(
                    app,
                    hidden_sections=parse_hidden_sections_from_settings_post(),
                    extra_fields=parse_extra_fields_from_settings_post(),
                )
                flash("Event desk intake layout saved.", "success")
            return redirect(url_for(redir_ep))

        event_hidden, event_extras = load_event_intake_layout(app)
        pt_hidden, pt_extras = load_private_transfer_intake_layout(app)
        event_section_choices = [
            (
                "desk_checklist",
                "Quote desk checklist",
                "The grey help box at the top of the intake form.",
            ),
            (
                "account_history",
                "Recent opportunities on account",
                "Panel that appears after linking a CRM account.",
            ),
            (
                "venue_location",
                "Event site block",
                "Venue address, postcode, what3words, coordinates, dispatch base, saved venues.",
            ),
            (
                "audience_scale",
                "Attendance & venue type row",
                "Expected attendance, hours on site, indoor/outdoor, crowd profile.",
            ),
            (
                "risk_calculator",
                "Risk & hospital questions",
                "All programme / hospital fields (same engine as the public calculator). Safe to hide for a minimal desk flow — defaults apply.",
            ),
            (
                "risk_more_detail",
                "Extended risk (More detail)",
                "Only applies when the risk block is shown: hides the collapsible posture / season / hazards section.",
            ),
            (
                "internal_notes",
                "Internal notes box",
                "The large message field before submit.",
            ),
        ]
        pt_section_choices = [
            (
                "pt_progress_header",
                "Step progress bar",
                "The 1–4 step labels and green progress bar (steps still work).",
            ),
            (
                "pt_patient_extras",
                "Patient extras",
                "Date of birth, weight, additional needs (names stay required).",
            ),
            (
                "pt_return_leg",
                "Return date / time",
                "Optional return leg fields on the journey step.",
            ),
            (
                "pt_infectious_meds",
                "Infectious flags & medications",
                "Tick-box precautions and current medications textarea.",
            ),
            (
                "pt_payee_contact_extras",
                "Payee extras",
                "Organisation (invoice) and payee phone on the applicant step.",
            ),
        ]
        return render_template(
            "admin/crm_desk_intake_layout_settings.html",
            event_hidden_sections=list(event_hidden),
            event_extra_fields=event_extras,
            event_section_choices=event_section_choices,
            pt_hidden_sections=list(pt_hidden),
            pt_extra_fields=pt_extras,
            pt_section_choices=pt_section_choices,
            can_edit=can_edit(),
        )

    @crm_bp.route("/settings/desk-intake-layout", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def desk_intake_layout_settings():
        return _desk_intake_layout_page()

    @crm_bp.route("/settings/event-intake-layout", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def event_intake_layout_settings():
        """Back-compat URL — same page as desk intake layout."""
        return _desk_intake_layout_page()
