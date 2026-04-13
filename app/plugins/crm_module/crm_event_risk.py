"""Event risk heuristics for the public calculator (organiser guide — not clinical advice)."""
from __future__ import annotations

import math
from decimal import Decimal
from typing import Any

from .crm_purple_guide import (
    CQC_COMPLIANCE_LINES,
    TIER_GUIDANCE,
    conversion_row_for_purple_score,
    compute_purple_score,
    infer_tier_from_signals,
)


def _i(raw: str | None, default: int = 0) -> int:
    t = (raw or "").strip()
    if not t.isdigit():
        return default
    return max(0, int(t))


def _dec(raw: str | None) -> Decimal | None:
    t = (raw or "").strip()
    if not t:
        return None
    try:
        return Decimal(t)
    except Exception:
        return None


def _default_activity_risk(crowd_profile: str, n: int) -> str:
    cp = (crowd_profile or "mixed").strip().lower()
    if cp in ("family", "corporate") and n < 800:
        return "low"
    return "moderate"


def _infer_duration_span(hours: float, explicit: str | None) -> str:
    e = (explicit or "").strip().lower()
    if e in ("few_hours", "single_day", "multi_day"):
        return e
    if hours <= 5.5:
        return "few_hours"
    if hours <= 24:
        return "single_day"
    return "multi_day"


def _default_hospital_referrals(n: int) -> str:
    if n >= 5000:
        return "likely"
    if n >= 2000:
        return "possible"
    return "unlikely"


def compute_event_risk_assessment(
    *,
    expected_attendees: int,
    duration_hours: Decimal | None,
    venue_outdoor: bool,
    alcohol: bool,
    late_finish: bool,
    crowd_profile: str,
    activity_risk: str | None = None,
    drug_risk: str | None = None,
    duration_span: str | None = None,
    hospital_referrals: str | None = None,
    alcohol_level: str | None = None,
) -> dict[str, Any]:
    """
    Returns score 1–5, Purple Guide–style purple_score (0–100+), indicative tier 1–5,
    conversion-table row, factor list, and resource hints for quoting (not an MNA).
    """
    n = max(0, int(expected_attendees))
    hours = float(duration_hours) if duration_hours is not None else 6.0
    hours = max(1.0, min(hours, 72.0))

    cp = (crowd_profile or "mixed").strip().lower()
    ar = (activity_risk or "").strip().lower() or _default_activity_risk(cp, n)
    if ar not in ("low", "moderate", "significant", "high"):
        ar = "moderate"
    dr = (drug_risk or "none").strip().lower()
    if dr not in ("none", "isolated", "likely", "expected"):
        dr = "none"
    dspan = _infer_duration_span(hours, duration_span)
    hr = (hospital_referrals or "").strip().lower() or _default_hospital_referrals(n)
    if hr not in ("unlikely", "possible", "likely"):
        hr = "unlikely"
    alvl_raw = (alcohol_level or "").strip().lower()
    if alvl_raw not in ("none", "social", "likely", "expected"):
        alvl = "social" if alcohol else "none"
    else:
        alvl = alvl_raw

    score = 1.0
    factors: list[str] = []

    if n >= 5000:
        score += 1.2
        factors.append("Very large expected attendance (5k+)")
    elif n >= 1500:
        score += 0.7
        factors.append("Large expected attendance (1.5k+)")
    elif n >= 500:
        score += 0.35
        factors.append("Medium attendance (500+)")

    if hours >= 10:
        score += 0.5
        factors.append("Long operational window (10h+)")
    elif hours >= 6:
        score += 0.25
        factors.append("Full-day style duration")

    if venue_outdoor:
        score += 0.45
        factors.append("Outdoor / exposed venue")

    if alcohol:
        score += 0.55
        factors.append("Alcohol service")

    if late_finish:
        score += 0.4
        factors.append("Late finish (after 23:00)")

    if cp in ("young_adult", "young adult", "nightlife"):
        score += 0.35
        factors.append("Higher-energy crowd profile")
    elif cp in ("family", "corporate"):
        score += 0.0
        factors.append("Family / corporate weighted profile (baseline)")

    if ar == "high":
        score += 0.38
        factors.append("Purple Guide–style factor: high activity risk")
    elif ar == "significant":
        score += 0.24
        factors.append("Purple Guide–style factor: significant activity risk")
    elif ar == "moderate":
        score += 0.12
        factors.append("Purple Guide–style factor: moderate activity risk")

    if dr == "expected":
        score += 0.38
        factors.append("Purple Guide–style factor: drug intoxication expected")
    elif dr == "likely":
        score += 0.24
        factors.append("Purple Guide–style factor: drug intoxication likely")
    elif dr == "isolated":
        score += 0.08
        factors.append("Purple Guide–style factor: isolated drug use possible")

    if dspan == "multi_day":
        score += 0.34
        factors.append("Multi-day / overnight programme (higher tier expectations)")
    elif dspan == "few_hours":
        score -= 0.04
        factors.append("Short programme (few hours)")

    if hr == "likely":
        score += 0.28
        factors.append("Hospital transfers likely — ambulance & governance planning")
    elif hr == "possible":
        score += 0.12
        factors.append("Some hospital transfers possible")

    score = max(1.0, min(5.0, score))
    band = int(round(score))
    labels = {
        1: "Low",
        2: "Low–moderate",
        3: "Moderate",
        4: "Elevated",
        5: "High",
    }
    label = labels.get(band, "Moderate")

    purple_score, purple_factors = compute_purple_score(
        expected_attendees=n,
        duration_hours=hours,
        venue_outdoor=venue_outdoor,
        alcohol=alcohol,
        late_finish=late_finish,
        crowd_profile=cp,
        activity_risk=ar,
        drug_risk=dr,
        duration_span=dspan,
        hospital_referrals=hr,
        alcohol_level=alvl,
    )
    tier = infer_tier_from_signals(
        purple_score=purple_score,
        expected_attendees=n,
        duration_span=dspan,
        activity_risk=ar,
        drug_risk=dr,
        alcohol_level=alvl,
        hospital_referrals=hr,
    )
    conversion = conversion_row_for_purple_score(purple_score)
    tier_info = TIER_GUIDANCE.get(tier, TIER_GUIDANCE[3])

    medic_floor = 2 if band >= 3 else 1
    medic_from_crowd = max(1, math.ceil(n / 2500)) if n else 1
    suggested_medics = max(medic_floor, medic_from_crowd, math.ceil(band * 1.5))
    suggested_vehicles = max(1, min(12, math.ceil(suggested_medics / 3)))

    fr = conversion.get("first_responder", 0)
    doc = conversion.get("doctor", 0)
    nur = conversion.get("nurse", 0)
    if isinstance(fr, int) and isinstance(doc, int) and isinstance(nur, int):
        table_floor = doc + nur + max(2, fr // 4)
        suggested_medics = max(suggested_medics, min(220, table_floor))
    amb = conversion.get("ambulances", 0)
    if isinstance(amb, int):
        suggested_vehicles = max(suggested_vehicles, min(24, max(1, amb)))

    inputs: dict[str, Any] = {
        "expected_attendees": n,
        "duration_hours": str(Decimal(str(hours)).quantize(Decimal("0.01"))),
        "venue_outdoor": venue_outdoor,
        "alcohol": alcohol,
        "late_finish": late_finish,
        "crowd_profile": cp or "mixed",
        "activity_risk": ar,
        "drug_risk": dr,
        "duration_span": dspan,
        "hospital_referrals": hr,
        "alcohol_level": alvl,
    }
    staffing_breakdown = build_staffing_breakdown(
        suggested_medics=suggested_medics,
        suggested_vehicles=suggested_vehicles,
        band=band,
        expected_attendees=n,
        duration_hours=hours,
        venue_outdoor=venue_outdoor,
        alcohol=alcohol,
        late_finish=late_finish,
    )
    sb_intro = staffing_breakdown.get("intro") or ""
    staffing_breakdown = {
        **staffing_breakdown,
        "intro": (
            (sb_intro + " " if sb_intro else "")
            + "Indicative Purple Guide tier "
            + str(tier)
            + " and conversion-table row are for planning only — confirm with your medical needs assessment."
        ).strip(),
    }

    merged_factors = list(dict.fromkeys([*factors, *purple_factors]))
    if not merged_factors:
        merged_factors = ["Baseline event medical planning considerations"]

    return {
        "score": round(score, 2),
        "band": band,
        "label": label,
        "factors": merged_factors,
        "suggested_medics": suggested_medics,
        "suggested_vehicles": suggested_vehicles,
        "staffing_breakdown": staffing_breakdown,
        "inputs": inputs,
        "purple_guide": {
            "purple_score": purple_score,
            "tier": tier,
            "tier_title": tier_info.get("title"),
            "tier_summary": tier_info.get("summary"),
            "tier_cover_points": tier_info.get("cover"),
            "conversion_row": conversion,
            "cqc_compliance": list(CQC_COMPLIANCE_LINES),
            "disclaimer": (
                "Indicative only — not a substitute for a formal medical needs assessment, "
                "your medical director, insurer, venue, or Safety Advisory Group (SAG) expectations."
            ),
        },
    }


def _staffing_totals_summary(on_foot_clinical: int, vehicles: int) -> dict[str, Any]:
    """
    Explicit totals so quotes do not confuse on-foot clinical posts with vehicle crew.
    combined_planning_headcount is clinical + typical crew (2 per vehicle) before rota deduplication.
    """
    on_foot = max(0, int(on_foot_clinical))
    v = max(0, int(vehicles))
    crew = v * 2
    return {
        "on_foot_clinical": on_foot,
        "response_vehicles": v,
        "vehicle_crew_positions_hint": crew,
        "combined_planning_headcount": on_foot + crew,
        "clarification": (
            "On-foot clinical posts do not include vehicle crew. Vehicles are separate resource lines; "
            "real rotas may overlap (same person on a vehicle and a static post) — avoid double-counting hours in the quote."
        ),
    }


def build_staffing_breakdown(
    *,
    suggested_medics: int,
    suggested_vehicles: int,
    band: int,
    expected_attendees: int,
    duration_hours: float,
    venue_outdoor: bool = False,
    alcohol: bool = False,
    late_finish: bool = False,
) -> dict[str, Any]:
    """
    Indicative skill mix for sales / resourcing (not a deployment order or clinical sign-off).
    Role counts sum to suggested_medics when that value is positive.
    """
    m = max(0, int(suggested_medics))
    v = max(0, int(suggested_vehicles))
    n = max(0, int(expected_attendees))
    hours = max(1.0, min(float(duration_hours), 36.0))

    clinical_roles: list[dict[str, Any]] = []
    if m == 1:
        clinical_roles.append(
            {
                "role": "Solo clinical cover",
                "count": 1,
                "grade_hint": "Typically one registered paramedic or experienced EMT / emergency technician — final grade per your medical governance.",
                "rationale": "Smallest footprint; confirm ALS capability vs EMT-only for the profile.",
            }
        )
    elif m >= 2:
        lead = 0
        if m >= 3 or band >= 4 or n >= 2500 or (band >= 3 and m >= 2):
            lead = 1
        remaining = m - lead
        if lead:
            clinical_roles.append(
                {
                    "role": "Event clinical lead",
                    "count": 1,
                    "grade_hint": "Registered paramedic (often described as FCP / P7+ or organisational equivalent).",
                    "rationale": "Coordinates on-site care, serious incidents, rota, and NHS handover.",
                }
            )
        if remaining > 0:
            als_share = 0.45 if band >= 4 else (0.38 if band >= 3 else 0.28)
            als_floor = 1 if band >= 3 and remaining >= 2 else 0
            als_n = max(als_floor, int(round(remaining * als_share)))
            als_n = min(remaining, als_n)
            emt_n = remaining - als_n
            if als_n:
                clinical_roles.append(
                    {
                        "role": "ALS / paramedic posts",
                        "count": als_n,
                        "grade_hint": "Registered paramedics or agreed ALS providers — may be roaming, static, or treatment-centre based.",
                        "rationale": "Higher-acuity assessment and interventions within scope of practice.",
                    }
                )
            if emt_n:
                clinical_roles.append(
                    {
                        "role": "EMT / technician / first-responder posts",
                        "count": emt_n,
                        "grade_hint": "EMT, emergency care technician, or qualified first-responder tier — map to your job types and wage card.",
                        "rationale": "Throughput, walking wounded, reassurance patrols, and support to ALS posts.",
                    }
                )

    operational_notes: list[str] = []
    if hours >= 10:
        operational_notes.append(
            "Long on-site window (10h+): plan staggered handovers and relief in the quote — this guide shows one shift-equivalent headcount."
        )
    if alcohol:
        operational_notes.append(
            "Alcohol on site: triage and intoxication demand often warrants ALS-heavy positioning near bars and arena exits."
        )
    if late_finish:
        operational_notes.append(
            "Late finish: fatigue and night-time presentation patterns may need extra ALS cover or supervisor presence vs daytime."
        )
    if venue_outdoor:
        operational_notes.append(
            "Outdoor / exposed site: consider evacuation distances, weather, and dispersed crowd lines when placing posts."
        )

    vehicle_block: dict[str, Any] | None = None
    if v:
        vehicle_block = {
            "vehicles": v,
            "crew_headcount_hint": v * 2,
            "grade_hint": "Usually driver (C1/D1 or fleet SOP) plus clinician or dual-trained crew per vehicle.",
            "notes": "Vehicle lines are separate from on-foot clinical headcount; rota overlap is normal in quoted hours.",
        }

    return {
        "intro": (
            "Indicative mix for early sales and wage hints only — not a deployment order. "
            "Final staffing, equipment, and vehicles follow medical needs assessment and the event medical plan "
            "(typical before a binding quote)."
        ),
        "totals": _staffing_totals_summary(m, v),
        "clinical_roles": clinical_roles,
        "vehicle_package": vehicle_block,
        "operational_notes": operational_notes,
    }


def enrich_lead_meta_with_staffing_breakdown(lead_meta: dict[str, Any]) -> dict[str, Any]:
    """Copy lead_meta and attach staffing_breakdown on risk when missing (legacy rows)."""
    out = dict(lead_meta)
    risk = out.get("risk")
    if not isinstance(risk, dict):
        return out
    r = dict(risk)
    if r.get("staffing_breakdown"):
        sb = r["staffing_breakdown"]
        if isinstance(sb, dict) and "totals" not in sb:
            sm = int(r.get("suggested_medics") or 0)
            sv = int(r.get("suggested_vehicles") or 0)
            r = dict(r)
            r["staffing_breakdown"] = {**sb, "totals": _staffing_totals_summary(sm, sv)}
        out["risk"] = r
        return out
    sm = int(r.get("suggested_medics") or 0)
    sv = int(r.get("suggested_vehicles") or 0)
    if sm <= 0 and sv <= 0:
        out["risk"] = r
        return out
    band = int(r.get("band") or 3)
    inp = r.get("inputs") or {}
    try:
        n = int(inp.get("expected_attendees") or 0)
    except (TypeError, ValueError):
        n = 0
    try:
        hours = float(inp.get("duration_hours") or 6)
    except (TypeError, ValueError):
        hours = 6.0
    hours = max(1.0, min(hours, 36.0))
    r["staffing_breakdown"] = build_staffing_breakdown(
        suggested_medics=sm,
        suggested_vehicles=sv,
        band=band,
        expected_attendees=n,
        duration_hours=hours,
        venue_outdoor=bool(inp.get("venue_outdoor")),
        alcohol=bool(inp.get("alcohol")),
        late_finish=bool(inp.get("late_finish")),
    )
    out["risk"] = r
    return out


def parse_public_calculator_form(form: Any) -> dict[str, Any]:
    """Normalise request.form-like mapping for lead creation."""
    org = (form.get("organisation_name") or form.get("company_name") or "").strip()
    contact = (form.get("contact_name") or "").strip()
    email = (form.get("email") or "").strip()[:255]
    phone = (form.get("phone") or "").strip()[:64]
    event_name = (form.get("event_name") or "").strip()
    message = (form.get("message") or "").strip() or None
    attendees = _i(form.get("expected_attendees"), 0)
    duration_hours = _dec(form.get("duration_hours")) or Decimal("6")
    venue_outdoor = (form.get("venue_type") or "").strip().lower() in (
        "outdoor",
        "both",
        "outside",
    )
    alcohol = (form.get("alcohol_served") or "").strip().lower() in (
        "yes",
        "y",
        "1",
        "true",
        "on",
    )
    late_finish = (form.get("late_finish") or "").strip().lower() in (
        "yes",
        "y",
        "1",
        "true",
        "on",
    )
    crowd_profile = (form.get("crowd_profile") or "mixed").strip()
    venue_type = (form.get("venue_type") or "indoor").strip().lower()
    if venue_type not in ("indoor", "outdoor", "both"):
        venue_type = "indoor"

    def _opt(name: str, allowed: frozenset[str]) -> str | None:
        v = (form.get(name) or "").strip().lower()
        return v if v in allowed else None

    activity_risk = _opt(
        "activity_risk", frozenset({"low", "moderate", "significant", "high"})
    )
    drug_risk = _opt(
        "drug_risk", frozenset({"none", "isolated", "likely", "expected"})
    )
    duration_span = _opt(
        "duration_span", frozenset({"few_hours", "single_day", "multi_day"})
    )
    hospital_referrals = _opt(
        "hospital_referrals", frozenset({"unlikely", "possible", "likely"})
    )
    alcohol_level = _opt(
        "alcohol_level", frozenset({"none", "social", "likely", "expected"})
    )
    return {
        "organisation_name": org,
        "contact_name": contact,
        "email": email,
        "phone": phone or None,
        "event_name": event_name,
        "message": message,
        "expected_attendees": attendees,
        "duration_hours": duration_hours,
        "venue_type": venue_type,
        "venue_outdoor": venue_outdoor,
        "alcohol": alcohol,
        "late_finish": late_finish,
        "crowd_profile": crowd_profile,
        "activity_risk": activity_risk,
        "drug_risk": drug_risk,
        "duration_span": duration_span,
        "hospital_referrals": hospital_referrals,
        "alcohol_level": alcohol_level,
    }


def split_contact_name(full: str) -> tuple[str, str]:
    parts = (full or "").strip().split(None, 1)
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]
