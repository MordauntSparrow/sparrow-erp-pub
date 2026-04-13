# CRM admin settings (guide ↔ Time & Billing wage card job types)
from __future__ import annotations

from datetime import date

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import login_required

from app.objects import get_db_connection

from .crm_common import can_edit, crm_access_required
from .crm_manifest_settings import get_crm_module_settings, update_crm_module_settings
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
                    clinical_job_type_id=_int_or_none("clinical_job_type_id"),
                    driver_job_type_id=_int_or_none("driver_job_type_id"),
                )
                flash("Guide ↔ wage job mapping saved.", "success")
                return redirect(url_for("crm_module.guide_wage_job_map"))

            slot_map = load_guide_wage_slot_map(conn)
            on_card = (
                list_job_types_on_wage_card(conn, card_id, on) if card_id else []
            )
            all_types = list_all_job_types(conn)
            preview = estimate_guide_staffing_costs(
                {
                    "duration_hours": "8",
                    "risk": {
                        "suggested_medics": 2,
                        "suggested_vehicles": 1,
                        "inputs": {"duration_hours": "8"},
                    },
                },
                on_date=on,
            )
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

    @crm_bp.route("/settings/event-plan-pdf", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def event_plan_pdf_settings():
        app = current_app._get_current_object()
        if request.method == "POST" and not can_edit():
            flash("You do not have permission to change CRM settings.", "danger")
            return redirect(url_for("crm_module.event_plan_pdf_settings"))

        if request.method == "POST" and can_edit():
            update_crm_module_settings(
                app,
                {
                    "event_plan_pdf_tagline": (
                        request.form.get("event_plan_pdf_tagline") or ""
                    ).strip()[:500],
                    "event_plan_pdf_about_us": (
                        request.form.get("event_plan_pdf_about_us") or ""
                    ).strip()[:20000],
                },
            )
            flash("Event plan PDF cover text saved to core manifest.", "success")
            return redirect(url_for("crm_module.event_plan_pdf_settings"))

        settings = get_crm_module_settings(app)
        return render_template(
            "admin/crm_event_plan_pdf_settings.html",
            settings=settings,
            can_edit=can_edit(),
        )
