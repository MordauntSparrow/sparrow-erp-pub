"""Public organiser-facing tools (no login) — mounted at site root paths."""
from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for

from .crm_event_risk import compute_event_risk_assessment, parse_public_calculator_form
from .crm_lead_intake import intake_from_parsed_form

crm_public_bp = Blueprint(
    "crm_public",
    __name__,
    url_prefix="",
    template_folder="templates",
)


@crm_public_bp.route("/quoting", methods=["GET"])
def public_quoting_landing():
    """Friendly entry URL — forwards to the full calculator + guide."""
    return redirect(url_for("crm_public.public_event_risk_calculator"))


@crm_public_bp.route("/event-risk-calculator", methods=["GET", "POST"])
def public_event_risk_calculator():
    if request.method == "GET":
        return render_template("public/crm_event_risk_calculator.html")

    parsed = parse_public_calculator_form(request.form)
    try:
        opp_id, _cid, meta = intake_from_parsed_form(
            parsed, source="event_risk_calculator", stage="prospecting"
        )
    except ValueError:
        flash("Please complete organisation, contact name, email, and event name.", "warning")
        return render_template(
            "public/crm_event_risk_calculator.html",
            form_values=request.form,
            preview_risk=_preview_from_form(request.form),
        )
    except Exception as ex:
        flash("We could not save your enquiry. Please try again or call us.", "danger")
        return render_template(
            "public/crm_event_risk_calculator.html",
            form_values=request.form,
            preview_risk=_preview_from_form(request.form),
            error=str(ex)[:200],
        )

    return render_template(
        "public/crm_event_risk_thanks.html",
        opportunity_id=opp_id,
        lead_meta=meta,
    )


def _preview_from_form(form) -> dict | None:
    try:
        p = parse_public_calculator_form(form)
        if not p.get("expected_attendees"):
            return None
        return compute_event_risk_assessment(
            expected_attendees=int(p.get("expected_attendees") or 0),
            duration_hours=p.get("duration_hours"),
            venue_outdoor=bool(p.get("venue_outdoor")),
            alcohol=bool(p.get("alcohol")),
            late_finish=bool(p.get("late_finish")),
            crowd_profile=str(p.get("crowd_profile") or "mixed"),
        )
    except Exception:
        return None
