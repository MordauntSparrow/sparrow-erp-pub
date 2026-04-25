"""Event risk heuristics for the public calculator (organiser guide — not clinical advice)."""
from __future__ import annotations

import math
from collections.abc import Iterable
from decimal import Decimal
from typing import Any

# K/L-style multi-selects (common on provider calculators — e.g. carnival, suturing, X-ray).
ADDITIONAL_HAZARDS_ALLOWED: frozenset[str] = frozenset(
    {"carnival", "parachute_display", "helicopters", "street_theatre", "motor_sport"}
)
ON_SITE_FACILITIES_ALLOWED: frozenset[str] = frozenset(
    {"suturing", "plastering", "xray", "psych_gp", "minor_surgery"}
)


def _normalize_allowed_tokens(
    values: Iterable[Any] | None, allowed: frozenset[str]
) -> list[str]:
    if not values:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for x in values:
        s = str(x or "").strip().lower().replace("-", "_")
        if s in allowed and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _checkbox_values_from_form(form: Any, name: str, allowed: frozenset[str]) -> list[str]:
    raw: list[Any] = []
    if hasattr(form, "getlist"):
        raw = list(form.getlist(name))
    else:
        v = form.get(name)
        if v is None:
            raw = []
        elif isinstance(v, (list, tuple)):
            raw = list(v)
        else:
            raw = [v]
    return _normalize_allowed_tokens(raw, allowed)


from .crm_purple_guide import (
    CQC_COMPLIANCE_LINES,
    TIER_GUIDANCE,
    TIER_MEDIC_BASE,
    TIER_VEHICLE_BASE,
    conversion_row_for_tier,
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


def suggested_event_physician_count(
    *,
    suggested_medics: int,
    tier: int,
    expected_attendees: int,
    band: int,
) -> int:
    """
    Indicative event physician footprint (not part of suggested_medics on-foot count).
    Kept in sync with staffing_breakdown doctor lines for wage hints and organiser copy.
    """
    m = max(0, int(suggested_medics))
    n = max(0, int(expected_attendees))
    t = max(1, min(5, int(tier)))
    b = max(1, min(5, int(band)))
    if m <= 1:
        return 0
    if t >= 5:
        return 1
    if t == 4 and (n >= 2000 or b >= 5):
        return 1
    if t == 3 and n >= 5000:
        return 1
    return 0


def customer_facing_resource_matrix(
    *,
    suggested_medics: int,
    suggested_vehicles: int,
    band: int,
    tier: int,
    expected_attendees: int,
    duration_hours: float,
    venue_outdoor: bool = False,
    alcohol: bool = False,
    late_finish: bool = False,
) -> dict[str, Any]:
    """
    Plain-language counts for organisers (doctors, paramedic/ALS footprint, EMTs, ambulances).
    Aligns with :func:`build_staffing_breakdown` — not an MNA.
    """
    m = max(0, int(suggested_medics))
    v = max(0, int(suggested_vehicles))
    n = max(0, int(expected_attendees))
    t = max(1, min(5, int(tier)))
    b = max(1, min(5, int(band)))

    sb = build_staffing_breakdown(
        suggested_medics=m,
        suggested_vehicles=v,
        band=b,
        tier=t,
        expected_attendees=n,
        duration_hours=duration_hours,
        venue_outdoor=venue_outdoor,
        alcohol=alcohol,
        late_finish=late_finish,
    )

    doctors = suggested_event_physician_count(
        suggested_medics=m,
        tier=t,
        expected_attendees=n,
        band=b,
    )

    lead = als = emt = solo = 0
    for row in sb.get("clinical_roles") or []:
        c = int(row.get("count") or 0)
        ws = str(row.get("wage_slot") or "")
        if ws == "clinical_lead":
            lead = c
        elif ws in ("als", "paramedic_advanced", "paramedic"):
            als += c
        elif ws in ("emt", "technician", "first_aider", "eca"):
            emt += c
        elif ws == "solo_clinical":
            solo = c

    rows: list[dict[str, Any]] = []
    if doctors:
        rows.append(
            {
                "category": "Doctors (event physician / roving)",
                "count": doctors,
                "detail": (
                    "Typical on larger tiers; scope (on-site vs on-call) is agreed in your medical needs assessment."
                ),
            }
        )
    if solo:
        rows.append(
            {
                "category": "Solo clinical cover",
                "count": solo,
                "detail": "One paramedic or experienced EMT/technician grade for the profile — confirm ALS vs EMT in your MNA.",
            }
        )
    paramedic_total = lead + als
    if paramedic_total:
        rows.append(
            {
                "category": "Paramedics / ALS (includes clinical lead)",
                "count": paramedic_total,
                "detail": (
                    f"Clinical lead posts: {lead}; further ALS/paramedic posts: {als}. "
                    "Grades and rotas are finalised with your provider."
                ),
            }
        )
    if emt:
        rows.append(
            {
                "category": "EMTs / technicians / first responders",
                "count": emt,
                "detail": "Patrols, walking wounded, and support to ALS posts.",
            }
        )
    rows.append(
        {
            "category": "Crewed ambulances (hospital transfer)",
            "count": v,
            "detail": "For A&E runs; keep a fixed medical post so vehicles stay available for transfers.",
        }
    )
    rows.append(
        {
            "category": "Typical ambulance crew roles (driver + clinician)",
            "count": v * 2,
            "detail": (
                "Often two roles per vehicle — the driving post is commonly an ECA-grade "
                "(e.g. FREC4 responder with blue-light training) alongside a clinician; rotas may overlap with on-foot posts."
            ),
        }
    )

    return {
        "rows": rows,
        "footer": (
            "These numbers are indicative planning hints from your answers — not a licensed medical needs assessment. "
            "Final posts, vehicles, and grades are agreed with your medical provider, insurer, and SAG."
        ),
    }


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
    hospital_drive_band: str | None = None,
    audience_posture: str | None = None,
    disorder_risk: str | None = None,
    casualty_history: str | None = None,
    audience_dwell: str | None = None,
    season: str | None = None,
    definitive_care: str | None = None,
    additional_hazards: Iterable[str] | None = None,
    on_site_facilities: Iterable[str] | None = None,
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
    hdrv_raw = (hospital_drive_band or "").strip().lower()
    if hdrv_raw not in ("unknown", "under_15", "15_30", "over_30"):
        hdrv = "unknown"
    else:
        hdrv = hdrv_raw

    def _nz(
        raw: str | None,
        allowed: frozenset[str],
        default: str,
    ) -> str:
        v = (raw or default).strip().lower()
        return v if v in allowed else default

    posture = _nz(
        audience_posture,
        frozenset({"unknown", "seated", "mixed", "standing"}),
        "unknown",
    )
    disorder = _nz(
        disorder_risk,
        frozenset({"none", "low", "medium", "high", "opposing_factions"}),
        "none",
    )
    casualty = _nz(
        casualty_history,
        frozenset({"unknown", "low_rate", "medium_rate", "high_rate", "first_event"}),
        "unknown",
    )
    dwell = _nz(
        audience_dwell,
        frozenset({"unknown", "under_4h", "over_4h", "over_12h"}),
        "unknown",
    )
    seas = _nz(
        season,
        frozenset({"unknown", "spring", "summer", "autumn", "winter"}),
        "unknown",
    )
    defcare = _nz(
        definitive_care,
        frozenset({"unknown", "choice_ae", "large_ae", "small_ae"}),
        "unknown",
    )

    hz_list = _normalize_allowed_tokens(additional_hazards, ADDITIONAL_HAZARDS_ALLOWED)
    fc_list = _normalize_allowed_tokens(on_site_facilities, ON_SITE_FACILITIES_ALLOWED)

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

    if hdrv == "over_30":
        score += 0.2
        factors.append("Long drive to A&E — transfer timeline and ambulance numbers need explicit planning")
    elif hdrv == "15_30":
        score += 0.1
        factors.append("Moderate drive to A&E — confirm journey times vs ambulance availability")

    if posture == "standing":
        score += 0.12
        factors.append("Standing-dominant audience (planning factor)")
    elif posture == "mixed":
        score += 0.06
        factors.append("Mixed seated/standing audience")

    if disorder == "opposing_factions":
        score += 0.32
        factors.append("Opposing factions / high tension — tier floor may apply")
    elif disorder == "high":
        score += 0.22
        factors.append("High disorder or march-type risk")
    elif disorder == "medium":
        score += 0.12
        factors.append("Moderate disorder risk")
    elif disorder == "low":
        score += 0.04
        factors.append("Low public-order risk flagged")

    if casualty == "first_event":
        score += 0.1
        factors.append("No prior event casualty benchmark")
    elif casualty == "high_rate":
        score += 0.14
        factors.append("Historically high casualty rate at comparable events")
    elif casualty == "medium_rate":
        score += 0.06
        factors.append("Moderate historical casualty rate")

    if dwell == "over_12h":
        if hours < 11:
            score += 0.15
            factors.append("Very long audience dwell (12h+)")
        elif hours < 20:
            score += 0.06
            factors.append("Long dwell partly covered by posted hours")
    elif dwell == "over_4h":
        if hours < 8:
            score += 0.08
            factors.append("Extended audience on site (4h+) vs shorter cover window")
        elif hours < 12:
            score += 0.04
            factors.append("Moderate dwell with mid-length cover hours")

    if seas == "winter" and venue_outdoor:
        score += 0.12
        factors.append("Winter outdoor season factor")
    elif seas == "winter":
        score += 0.04
        factors.append("Winter timing")
    elif seas == "summer" and venue_outdoor:
        score += 0.1
        factors.append("Summer outdoor — heat exposure")
    elif seas == "summer":
        score += 0.03
        factors.append("Summer timing")

    if defcare == "small_ae":
        score += 0.1
        factors.append("Smaller definitive-care hospital profile")

    _hz_inner = {
        "carnival": 0.05,
        "parachute_display": 0.07,
        "helicopters": 0.07,
        "street_theatre": 0.04,
        "motor_sport": 0.06,
    }
    hz_inner_sum = 0.0
    for hx in hz_list:
        w = _hz_inner.get(hx, 0.0)
        if hx == "motor_sport" and ar in ("high", "significant"):
            w = min(w, 0.03)
        hz_inner_sum += w
    score += min(hz_inner_sum, 0.22)
    if hz_list:
        factors.append(
            "Additional hazards: " + ", ".join(hx.replace("_", " ") for hx in hz_list)
        )

    _fc_inner = {
        "suturing": 0.06,
        "plastering": 0.05,
        "xray": 0.11,
        "psych_gp": 0.07,
        "minor_surgery": 0.11,
    }
    fc_inner_sum = min(sum(_fc_inner.get(fx, 0.0) for fx in fc_list), 0.28)
    score += fc_inner_sum
    if fc_list:
        factors.append(
            "On-site capabilities: " + ", ".join(fx.replace("_", " ") for fx in fc_list)
        )

    score = max(1.0, min(5.0, score))
    inner_band = int(round(score))

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
        hospital_drive_band=hdrv,
        audience_posture=posture,
        disorder_risk=disorder,
        casualty_history=casualty,
        audience_dwell=dwell,
        season=seas,
        definitive_care=defcare,
        additional_hazards=hz_list,
        on_site_facilities=fc_list,
    )
    tier = infer_tier_from_signals(
        purple_score=purple_score,
        expected_attendees=n,
        duration_span=dspan,
        activity_risk=ar,
        drug_risk=dr,
        alcohol_level=alvl,
        hospital_referrals=hr,
        hospital_drive_band=hdrv,
        disorder_risk=disorder,
    )
    # Keep UI "band" aligned with Purple Guide tier: inner score capped crowd at 5k+ with one
    # increment, so huge crowds could sit at "Moderate" while tier hit 5. Floor band at tier.
    band_labels = {
        1: "Low",
        2: "Low / moderate",
        3: "Moderate",
        4: "Elevated",
        5: "High",
    }
    band = min(5, max(inner_band, tier))
    label = band_labels.get(band, "Moderate")

    conversion = conversion_row_for_tier(tier)
    tier_info = TIER_GUIDANCE.get(tier, TIER_GUIDANCE[3])

    tier_base = TIER_MEDIC_BASE.get(tier, 4)
    crowd_lift = math.ceil(n / 4000) if n else 0
    suggested_medics = max(tier_base, tier_base + crowd_lift - 1, math.ceil(band * 1.2))
    suggested_medics = min(suggested_medics, 56)
    suggested_vehicles = max(
        TIER_VEHICLE_BASE.get(tier, 1),
        min(10, 1 + math.ceil(n / 12000)),
    )

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
        "hospital_drive_band": hdrv,
        "audience_posture": posture,
        "disorder_risk": disorder,
        "casualty_history": casualty,
        "audience_dwell": dwell,
        "season": seas,
        "definitive_care": defcare,
        "additional_hazards": list(hz_list),
        "on_site_facilities": list(fc_list),
    }
    staffing_breakdown = build_staffing_breakdown(
        suggested_medics=suggested_medics,
        suggested_vehicles=suggested_vehicles,
        band=band,
        tier=tier,
        expected_attendees=n,
        duration_hours=hours,
        venue_outdoor=venue_outdoor,
        alcohol=alcohol,
        late_finish=late_finish,
        on_site_facilities=fc_list,
    )
    customer_matrix = customer_facing_resource_matrix(
        suggested_medics=suggested_medics,
        suggested_vehicles=suggested_vehicles,
        band=band,
        tier=tier,
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
            + "Purple Guide tier "
            + str(tier)
            + " resource themes — confirm posts and rotas in your MNA and with your SAG."
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
        "customer_resource_matrix": customer_matrix,
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
                "Tier and themes follow the Purple Guide framework; binding medical cover is set in your "
                "medical needs assessment and with your medical director, insurer, venue, and SAG."
            ),
        },
    }


def compute_event_risk_from_parsed(parsed: dict[str, Any]) -> dict[str, Any]:
    """
    Run compute_event_risk_assessment from parse_public_calculator_form output.
    Keeps public preview, staff intake preview, and opportunity creation aligned.
    """
    try:
        attendees = int(parsed.get("expected_attendees") or 0)
    except (TypeError, ValueError):
        attendees = 0
    return compute_event_risk_assessment(
        expected_attendees=attendees,
        duration_hours=parsed.get("duration_hours"),
        venue_outdoor=bool(parsed.get("venue_outdoor")),
        alcohol=bool(parsed.get("alcohol")),
        late_finish=bool(parsed.get("late_finish")),
        crowd_profile=str(parsed.get("crowd_profile") or "mixed"),
        activity_risk=parsed.get("activity_risk"),
        drug_risk=parsed.get("drug_risk"),
        duration_span=parsed.get("duration_span"),
        hospital_referrals=parsed.get("hospital_referrals"),
        alcohol_level=parsed.get("alcohol_level"),
        hospital_drive_band=parsed.get("hospital_drive_band"),
        audience_posture=parsed.get("audience_posture"),
        disorder_risk=parsed.get("disorder_risk"),
        casualty_history=parsed.get("casualty_history"),
        audience_dwell=parsed.get("audience_dwell"),
        season=parsed.get("season"),
        definitive_care=parsed.get("definitive_care"),
        additional_hazards=parsed.get("additional_hazards"),
        on_site_facilities=parsed.get("on_site_facilities"),
    )


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


def _support_grade_split(emt_n: int) -> tuple[int, int, int]:
    """Split support headcount into technician / first aider / ECA (sums to emt_n)."""
    n = max(0, int(emt_n))
    if not n:
        return (0, 0, 0)
    tech = (n * 40 + 50) // 100
    fa = (n * 35 + 50) // 100
    eca = n - tech - fa
    if eca < 0:
        while eca < 0 and tech > 0:
            tech -= 1
            eca += 1
        while eca < 0 and fa > 0:
            fa -= 1
            eca += 1
    return tech, fa, eca


def build_staffing_breakdown(
    *,
    suggested_medics: int,
    suggested_vehicles: int,
    band: int,
    expected_attendees: int,
    duration_hours: float,
    tier: int | None = None,
    venue_outdoor: bool = False,
    alcohol: bool = False,
    late_finish: bool = False,
    on_site_facilities: Iterable[str] | None = None,
) -> dict[str, Any]:
    """
    Indicative skill mix for sales / resourcing (not a deployment order or clinical sign-off).
    On-foot role counts sum to suggested_medics when that value is positive.
    Optional event physician line (wage_slot ``doctor``) is extra — not included in that sum.
    """
    m = max(0, int(suggested_medics))
    v = max(0, int(suggested_vehicles))
    n = max(0, int(expected_attendees))
    b = max(1, min(5, int(band)))
    t_eff = max(1, min(5, int(tier))) if tier is not None else b
    hours = max(1.0, min(float(duration_hours), 36.0))

    clinical_roles: list[dict[str, Any]] = []
    if m == 1:
        clinical_roles.append(
            {
                "role": "Solo clinical cover",
                "count": 1,
                "wage_slot": "solo_clinical",
                "grade_hint": "Typically one registered paramedic or experienced EMT / emergency technician — final grade per your medical governance.",
                "rationale": "Smallest footprint; confirm ALS capability vs EMT-only for the profile.",
            }
        )
    elif m >= 2:
        lead = 0
        if m >= 3 or b >= 4 or n >= 2500 or (b >= 3 and m >= 2):
            lead = 1
        remaining = m - lead
        if lead:
            clinical_roles.append(
                {
                    "role": "Event clinical lead",
                    "count": 1,
                    "wage_slot": "clinical_lead",
                    "grade_hint": "Registered paramedic (often described as FCP / P7+ or organisational equivalent).",
                    "rationale": "Coordinates on-site care, serious incidents, rota, and NHS handover.",
                }
            )
        if remaining > 0:
            als_share = 0.45 if b >= 4 else (0.38 if b >= 3 else 0.28)
            als_floor = 1 if b >= 3 and remaining >= 2 else 0
            als_n = max(als_floor, int(round(remaining * als_share)))
            als_n = min(remaining, als_n)
            emt_n = remaining - als_n
            adv_n = 0
            param_n = als_n
            if als_n >= 2 and (b >= 4 or t_eff >= 4):
                adv_n = 1
                param_n = als_n - 1
            if adv_n:
                clinical_roles.append(
                    {
                        "role": "Advanced paramedic / FCP-grade ALS posts",
                        "count": adv_n,
                        "wage_slot": "paramedic_advanced",
                        "grade_hint": "Senior ALS / advanced paramedic or equivalent — map to your advanced wage card line.",
                        "rationale": "Higher-complexity on-site care and clinical escalation alongside the event lead.",
                    }
                )
            if param_n:
                clinical_roles.append(
                    {
                        "role": "Registered paramedic (ALS) posts",
                        "count": param_n,
                        "wage_slot": "paramedic",
                        "grade_hint": "Registered paramedics or agreed ALS providers — roaming, static, or treatment-centre based.",
                        "rationale": "Higher-acuity assessment and interventions within scope of practice.",
                    }
                )
            tech_n, fa_n, eca_n = _support_grade_split(emt_n)
            if tech_n:
                clinical_roles.append(
                    {
                        "role": "Emergency medical technician / technician posts",
                        "count": tech_n,
                        "wage_slot": "technician",
                        "grade_hint": "EMT or emergency technician grade — map to your technician job type on the wage card.",
                        "rationale": "Clinical support, equipment, walking wounded, and relief to ALS posts.",
                    }
                )
            if fa_n:
                clinical_roles.append(
                    {
                        "role": "Qualified first aider / event first aid posts",
                        "count": fa_n,
                        "wage_slot": "first_aider",
                        "grade_hint": "FAW / EFAW or equivalent first-aid qualification for the venue profile.",
                        "rationale": "Triage support, reassurance patrols, and throughput for lower-acuity presentations.",
                    }
                )
            if eca_n:
                clinical_roles.append(
                    {
                        "role": "Emergency care assistant (ECA) posts",
                        "count": eca_n,
                        "wage_slot": "eca",
                        "grade_hint": "ECA or ambulance support worker tier — align with your ECA wage line if used.",
                        "rationale": "Ambulance co-response, driving support, and stretcher / logistics assistance.",
                    }
                )

        doc_n = suggested_event_physician_count(
            suggested_medics=m,
            tier=t_eff,
            expected_attendees=n,
            band=b,
        )
        if doc_n:
            clinical_roles.append(
                {
                    "role": "Event physician / doctor (on-site or on-call)",
                    "count": doc_n,
                    "wage_slot": "doctor",
                    "grade_hint": "GMC-registered medical practitioner — scope (roving, treatment centre, on-call) per MNA.",
                    "rationale": "Larger tiers and complex profiles often add a physician layer separate from paramedic headcount.",
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

    facs = _normalize_allowed_tokens(on_site_facilities, ON_SITE_FACILITIES_ALLOWED)
    _facility_notes: dict[str, str] = {
        "suturing": (
            "Organiser flagged wound-care / suturing at the medical post — confirm registered scope, "
            "equipment, and medical governance."
        ),
        "plastering": (
            "Minor injury / immobilisation support discussed — align treatment-centre staffing and consumables."
        ),
        "xray": (
            "On-site or event imaging discussed — needs appropriate clinical governance and pathways; "
            "not a substitute for definitive care planning in the MNA."
        ),
        "psych_gp": (
            "Mental health or GP-style assessment discussed — consider quiet space, safeguarding, "
            "and NHS mental health handover routes."
        ),
        "minor_surgery": (
            "Minor procedures discussed — typically senior clinician–led with full governance; "
            "significantly raises medical-post specification."
        ),
    }
    for fx in facs:
        note = _facility_notes.get(fx)
        if note:
            operational_notes.append(note)

    vehicle_block: dict[str, Any] | None = None
    if v:
        vehicle_block = {
            "vehicles": v,
            "crew_headcount_hint": v * 2,
            "grade_hint": (
                "Typical crew: clinician plus driver — often an ECA (FREC4 / blue-light trained) with C1/D1 or "
                "fleet SOP; dual-trained crews are common — map the driver slot to your ECA wage line if that matches payroll."
            ),
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
    raw_osf = inp.get("on_site_facilities")
    osf_norm = _normalize_allowed_tokens(
        raw_osf if isinstance(raw_osf, (list, tuple)) else None,
        ON_SITE_FACILITIES_ALLOWED,
    )
    pg = r.get("purple_guide") if isinstance(r.get("purple_guide"), dict) else {}
    raw_tier = pg.get("tier")
    tier_val: int | None = None
    try:
        if raw_tier is not None and str(raw_tier).strip() != "":
            tier_val = max(1, min(5, int(raw_tier)))
    except (TypeError, ValueError):
        tier_val = None
    r["staffing_breakdown"] = build_staffing_breakdown(
        suggested_medics=sm,
        suggested_vehicles=sv,
        band=band,
        tier=tier_val,
        expected_attendees=n,
        duration_hours=hours,
        venue_outdoor=bool(inp.get("venue_outdoor")),
        alcohol=bool(inp.get("alcohol")),
        late_finish=bool(inp.get("late_finish")),
        on_site_facilities=osf_norm,
    )
    out["risk"] = r
    return out


def _last_nonempty_stripped(form: Any, name: str) -> str:
    """
    Prefer the last non-empty submission for this field name.
    Some proxies, extensions, or duplicate markup can send multiple values; werkzeug's
    .get() returns only the first, which may be blank while the real value is later.
    """
    if hasattr(form, "getlist"):
        out = ""
        for v in form.getlist(name):
            s = (v or "").strip()
            if s:
                out = s
        return out
    return (form.get(name) or "").strip()


def parse_public_calculator_form(form: Any) -> dict[str, Any]:
    """Normalise request.form-like mapping for lead creation."""
    org = _last_nonempty_stripped(form, "organisation_name") or _last_nonempty_stripped(
        form, "company_name"
    )
    contact = _last_nonempty_stripped(form, "contact_name")
    email = _last_nonempty_stripped(form, "email")[:255]
    phone = _last_nonempty_stripped(form, "phone")[:64]
    event_name = _last_nonempty_stripped(form, "event_name")
    message = _last_nonempty_stripped(form, "message") or None
    attendees = _i(_last_nonempty_stripped(form, "expected_attendees"), 0)
    duration_raw = _last_nonempty_stripped(form, "duration_hours")
    duration_hours = _dec(duration_raw) if duration_raw else None
    if duration_hours is None:
        duration_hours = Decimal("6")
    venue_outdoor = (_last_nonempty_stripped(form, "venue_type")).lower() in (
        "outdoor",
        "both",
        "outside",
    )
    late_finish = _last_nonempty_stripped(form, "late_finish").lower() in (
        "yes",
        "y",
        "1",
        "true",
        "on",
    )
    crowd_profile = (_last_nonempty_stripped(form, "crowd_profile") or "mixed").strip()
    venue_type = (_last_nonempty_stripped(form, "venue_type") or "indoor").strip().lower()
    if venue_type not in ("indoor", "outdoor", "both"):
        venue_type = "indoor"

    def _opt(name: str, allowed: frozenset[str]) -> str | None:
        v = _last_nonempty_stripped(form, name).lower()
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
    hospital_drive_band = _opt(
        "hospital_drive_band",
        frozenset({"unknown", "under_15", "15_30", "over_30"}),
    )
    if hospital_drive_band is None:
        hospital_drive_band = "unknown"

    audience_posture = _opt(
        "audience_posture",
        frozenset({"unknown", "seated", "mixed", "standing"}),
    )
    if audience_posture is None:
        audience_posture = "unknown"

    disorder_risk = _opt(
        "disorder_risk",
        frozenset({"none", "low", "medium", "high", "opposing_factions"}),
    )
    if disorder_risk is None:
        disorder_risk = "none"

    casualty_history = _opt(
        "casualty_history",
        frozenset({"unknown", "low_rate", "medium_rate", "high_rate", "first_event"}),
    )
    if casualty_history is None:
        casualty_history = "unknown"

    audience_dwell = _opt(
        "audience_dwell",
        frozenset({"unknown", "under_4h", "over_4h", "over_12h"}),
    )
    if audience_dwell is None:
        audience_dwell = "unknown"

    season = _opt(
        "season",
        frozenset({"unknown", "spring", "summer", "autumn", "winter"}),
    )
    if season is None:
        season = "unknown"

    definitive_care = _opt(
        "definitive_care",
        frozenset({"unknown", "choice_ae", "large_ae", "small_ae"}),
    )
    if definitive_care is None:
        definitive_care = "unknown"

    alcohol_level = _opt(
        "alcohol_level", frozenset({"none", "social", "likely", "expected"})
    )
    legacy_alcohol_chk = _last_nonempty_stripped(form, "alcohol_served").lower() in (
        "yes",
        "y",
        "1",
        "true",
        "on",
    )
    if alcohol_level is None:
        alcohol_level = "social" if legacy_alcohol_chk else "none"
    elif alcohol_level == "none" and legacy_alcohol_chk:
        alcohol_level = "social"
    alcohol = alcohol_level != "none"
    additional_hazards = _checkbox_values_from_form(
        form, "additional_hazards", ADDITIONAL_HAZARDS_ALLOWED
    )
    on_site_facilities = _checkbox_values_from_form(
        form, "on_site_facilities", ON_SITE_FACILITIES_ALLOWED
    )
    venue_address = (_last_nonempty_stripped(form, "venue_address") or "")[:512] or None
    venue_postcode = (_last_nonempty_stripped(form, "venue_postcode") or "")[:32] or None
    venue_what3words = (_last_nonempty_stripped(form, "venue_what3words") or "")[:128] or None
    venue_lat_raw = _last_nonempty_stripped(form, "venue_lat")
    venue_lng_raw = _last_nonempty_stripped(form, "venue_lng")
    venue_lat: float | None = None
    venue_lng: float | None = None
    if venue_lat_raw and venue_lng_raw:
        try:
            la = float(venue_lat_raw)
            ln = float(venue_lng_raw)
            if -90 <= la <= 90 and -180 <= ln <= 180:
                venue_lat, venue_lng = la, ln
        except (TypeError, ValueError):
            pass
    db_raw = _last_nonempty_stripped(form, "dispatch_base_id")
    dispatch_base_id = int(db_raw) if db_raw.isdigit() else None
    sv_raw = _last_nonempty_stripped(form, "saved_venue_id")
    saved_venue_id = int(sv_raw) if sv_raw.isdigit() else None
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
        "hospital_drive_band": hospital_drive_band,
        "alcohol_level": alcohol_level,
        "audience_posture": audience_posture,
        "disorder_risk": disorder_risk,
        "casualty_history": casualty_history,
        "audience_dwell": audience_dwell,
        "season": season,
        "definitive_care": definitive_care,
        "additional_hazards": additional_hazards,
        "on_site_facilities": on_site_facilities,
        "venue_address": venue_address,
        "venue_postcode": venue_postcode,
        "venue_what3words": venue_what3words,
        "venue_lat": venue_lat,
        "venue_lng": venue_lng,
        "dispatch_base_id": dispatch_base_id,
        "saved_venue_id": saved_venue_id,
    }


def calculator_template_values(
    form: Any,
    *,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Repopulate calculator templates after validation errors.
    Unlike Werkzeug MultiDict, checkbox groups (additional_hazards, on_site_facilities)
    are full lists — MultiDict.get() would only return the first checked value.
    """
    out = dict(parse_public_calculator_form(form))
    if extra:
        for k, v in extra.items():
            if v is not None:
                out[k] = v
    dh = out.get("duration_hours")
    if isinstance(dh, Decimal):
        s = format(dh, "f").rstrip("0").rstrip(".")
        out["duration_hours"] = s or "0"
    return out


def split_contact_name(full: str) -> tuple[str, str]:
    parts = (full or "").strip().split(None, 1)
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]
