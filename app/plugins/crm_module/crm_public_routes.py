"""Public organiser-facing tools (no login) — mounted at site root paths."""
from __future__ import annotations

import logging

from flask import Blueprint, abort, flash, redirect, render_template, request, url_for

from .crm_event_risk import calculator_template_values, parse_public_calculator_form
from .crm_purple_guide import (
    TIER_MEDIC_BASE,
    TIER_VEHICLE_BASE,
    public_tier_resource_rows,
)
from .crm_intake_forms_data import get_intake_form_by_slug
from .crm_lead_intake import (
    LeadIntakeInvalidAccount,
    LeadIntakeMissingRequiredFields,
    intake_from_parsed_form,
)
from .crm_website_lead_bridge import create_lead_from_website_submission

crm_public_bp = Blueprint(
    "crm_public",
    __name__,
    url_prefix="",
    template_folder="templates",
)

_LOG = logging.getLogger(__name__)


def _calculator_ctx(**extra):
    return {
        "tier_resource_rows": public_tier_resource_rows(),
        "tier_medic_base": dict(TIER_MEDIC_BASE),
        "tier_vehicle_base": dict(TIER_VEHICLE_BASE),
        **extra,
    }


@crm_public_bp.route("/quoting", methods=["GET"])
def public_quoting_landing():
    """Friendly entry URL — forwards to the full calculator + guide."""
    return redirect(url_for("crm_public.public_event_risk_calculator"))


@crm_public_bp.route("/event-risk-calculator", methods=["GET", "POST"])
def public_event_risk_calculator():
    if request.method == "GET":
        return render_template(
            "public/crm_event_risk_calculator.html",
            **_calculator_ctx(),
        )

    parsed = parse_public_calculator_form(request.form)
    try:
        opp_id, _cid, meta = intake_from_parsed_form(
            parsed, source="event_risk_calculator", stage="prospecting"
        )
    except LeadIntakeMissingRequiredFields:
        flash("Please complete organisation, contact name, email, and event name.", "warning")
        return render_template(
            "public/crm_event_risk_calculator.html",
            **_calculator_ctx(
                form_values=calculator_template_values(request.form),
            ),
        )
    except LeadIntakeInvalidAccount:
        flash("Invalid account selected.", "danger")
        return render_template(
            "public/crm_event_risk_calculator.html",
            **_calculator_ctx(
                form_values=calculator_template_values(request.form),
            ),
        )
    except Exception:
        _LOG.exception("CRM public event risk calculator: intake failed")
        flash("We could not save your enquiry. Please try again or call us.", "danger")
        return render_template(
            "public/crm_event_risk_calculator.html",
            **_calculator_ctx(
                form_values=calculator_template_values(request.form),
            ),
        )

    return render_template(
        "public/crm_event_risk_thanks.html",
        opportunity_id=opp_id,
        lead_meta=meta,
    )


def _public_intake_submission_dict() -> dict:
    raw = request.form.to_dict(flat=False)
    out: dict = {}
    for k, v in raw.items():
        if k == "_csrf_token":
            continue
        if isinstance(v, list):
            out[k] = v[0] if len(v) == 1 else v
        else:
            out[k] = v
    return out


@crm_public_bp.route("/intake/<slug>", methods=["GET", "POST"])
def public_intake_form(slug: str):
    """Public lead capture when the form is marked active + public in CRM admin."""
    form = get_intake_form_by_slug(slug)
    if not form or not form.get("is_active") or not int(form.get("is_public") or 0):
        abort(404)

    if request.method == "POST":
        if (request.form.get("website") or "").strip():
            flash("Submission rejected.", "danger")
            return redirect(url_for("crm_public.public_intake_form", slug=slug))
        sub = _public_intake_submission_dict()
        sub["remote_ip"] = request.remote_addr or ""
        sub["page"] = request.path
        sub["referrer"] = request.referrer or ""
        sub["timestamp"] = ""
        sub["intake_form_title"] = str(form.get("title") or "")
        sub["intake_form_id"] = str(form.get("id") or "")
        for fld in form.get("fields") or []:
            if not isinstance(fld, dict) or not fld.get("required"):
                continue
            fn = str(fld.get("name") or "").strip()
            if fn and not str(sub.get(fn) or "").strip():
                flash(f"Please fill in: {fld.get('label') or fn}.", "warning")
                return render_template(
                    "public/crm_public_intake_form.html",
                    form=form,
                    values=sub,
                )
        res = create_lead_from_website_submission(
            sub,
            form_id=f"crm_intake:{form.get('slug')}",
            stage=str(form.get("default_stage") or "prospecting"),
            company_field=(form.get("company_field") or "company").strip() or None,
            linked_account_id=None,
        )
        if not res:
            flash(
                "We could not save your enquiry. Please include your name or email.",
                "danger",
            )
            return render_template(
                "public/crm_public_intake_form.html",
                form=form,
                values=sub,
            )
        oid, _ = res
        return render_template(
            "public/crm_public_intake_thanks.html",
            form=form,
            opportunity_id=oid,
        )

    return render_template("public/crm_public_intake_form.html", form=form, values={})
