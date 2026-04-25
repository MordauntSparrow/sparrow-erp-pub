# Vehicle Daily Inspection — crew UI at /VDIs (Flask-Login users).
from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from .fleet_common import uid, user_matches_fleet_vdi_portal_rules, vdi_submit_required
from .objects import get_fleet_service
from .vdi_submit_logic import submit_vdi

fleet_vdi_bp = Blueprint(
    "fleet_vdi",
    __name__,
    url_prefix="/VDIs",
    template_folder="templates",
)


@fleet_vdi_bp.route("/")
@login_required
@vdi_submit_required
def vdi_home():
    if not user_matches_fleet_vdi_portal_rules(
        getattr(current_user, "role", None)
    ):
        flash(
            "Vehicle inspections are not enabled for your Sparrow role. "
            "Ask a fleet admin to adjust Fleet → Portal form access.",
            "danger",
        )
        return redirect(url_for("routes.dashboard"))
    svc = get_fleet_service()
    vehicles = svc.vehicles_for_crew_picklist()
    return render_template(
        "public/vdi_home.html",
        vehicles=vehicles,
    )


@fleet_vdi_bp.route("/vehicle/<int:vehicle_id>", methods=["GET", "POST"])
@login_required
@vdi_submit_required
def vdi_vehicle(vehicle_id: int):
    if not user_matches_fleet_vdi_portal_rules(
        getattr(current_user, "role", None)
    ):
        flash(
            "Vehicle inspections are not enabled for your Sparrow role.",
            "danger",
        )
        return redirect(url_for("routes.dashboard"))
    svc = get_fleet_service()
    v = svc.get_vehicle(vehicle_id)
    if not v or str(v.get("status")) == "decommissioned":
        flash("Vehicle not found.", "danger")
        return redirect(url_for("fleet_vdi.vdi_home"))

    schema = svc.get_vdi_schema()
    if request.method == "POST":
        actor = uid()
        if not actor:
            flash("Session error.", "danger")
            return redirect(url_for("routes.login"))
        err, sid = submit_vdi(
            svc,
            request,
            vehicle_id=vehicle_id,
            actor_type="user",
            actor_id=actor,
        )
        if err:
            flash(err, "danger")
            return render_template(
                "public/vdi_form.html",
                vehicle=v,
                schema=schema,
            )
        flash("Inspection submitted. Thank you.", "success")
        return redirect(url_for("fleet_vdi.vdi_submission", submission_id=sid))

    return render_template(
        "public/vdi_form.html",
        vehicle=v,
        schema=schema,
    )


@fleet_vdi_bp.route("/submission/<int:submission_id>")
@login_required
@vdi_submit_required
def vdi_submission(submission_id: int):
    if not user_matches_fleet_vdi_portal_rules(
        getattr(current_user, "role", None)
    ):
        flash(
            "Vehicle inspections are not enabled for your Sparrow role.",
            "danger",
        )
        return redirect(url_for("routes.dashboard"))
    svc = get_fleet_service()
    row = svc.get_vdi_submission(submission_id)
    if not row:
        flash("Not found.", "danger")
        return redirect(url_for("fleet_vdi.vdi_home"))
    v = svc.get_vehicle(int(row["vehicle_id"]))
    schema = svc.get_vdi_schema()
    return render_template(
        "public/vdi_submission_detail.html",
        submission=row,
        vehicle=v,
        schema=schema,
    )
