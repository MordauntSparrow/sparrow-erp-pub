"""
Purple Guide tier framework for event medical planning.

The Events Industry Forum’s *Purple Guide* (The Purple Guide to Health, Safety and Welfare at
Music and Other Events) describes medical cover through a **five-tier**, risk-led approach
(crowd, duration, activities, alcohol/drugs, environment, transfers) — not a single attendance
formula. Resource rows in this module summarise **typical themes for each tier** in the same
dimensions the industry uses when applying the Guide; **exact posts and rotas** are always set
in the organiser’s **medical needs assessment (MNA)** and with the **Safety Advisory Group (SAG)**.

This is not a licensed MNA and does not replace the current Purple Guide text, your medical
director, insurer, or regulator.
"""
from __future__ import annotations

from typing import Any

# Typical resource themes by tier — aligned with published descriptions of Tiers 1–5 (professional
# event medical roles, ambulance use for transfer, clinical leadership as scale increases).
# Wording is summary-level; organisers must follow the current Guide and their MNA/SAG.
TIER_RESOURCE_MATRIX: dict[int, dict[str, Any]] = {
    1: {
        "first_responder": (
            "Smallest / simplest profile: proportionate professional event medical cover "
            "(the Guide emphasises suitably trained event medical staff — e.g. ECA/UCA-style roles — "
            "rather than relying on workplace first-aid ratios alone)."
        ),
        "ambulances": "Usually none stationed on site",
        "ambulance_crew": "—",
        "doctor": "Not generally indicated for a straightforward Tier 1 profile",
        "nurse": "Not generally indicated for a straightforward Tier 1 profile",
        "nhs_ambulance_manager": "Agree access / call routes with local NHS ambulance service if needed",
        "support_unit": "Light, site-appropriate",
    },
    2: {
        "first_responder": (
            "Professional event medical team with an ECA/UCA-style core and a nominated on-site "
            "medical lead, as described for medium-sized lower-risk profiles in the Guide."
        ),
        "ambulances": "Consider a crewed ambulance if a hospital transfer is plausible",
        "ambulance_crew": "When an ambulance is deployed",
        "doctor": "Scale up if the risk profile is higher than a simple Tier 2",
        "nurse": "Paramedic / nurse cover often proportionate as presentations increase",
        "nhs_ambulance_manager": "Engage early if ambulance cover will be on site",
        "support_unit": "Match to footprint of the medical post",
    },
    3: {
        "first_responder": (
            "Broader mix of posts (technicians, ECAs/UCAs, responders) under a registered "
            "clinical lead with pre-hospital experience — the Guide’s Tier 3-style expectation."
        ),
        "ambulances": "Crewed ambulance capacity for timely transfers without stripping on-site cover",
        "ambulance_crew": "Sized to rota and concurrent incidents",
        "doctor": "Often part of the team as acuity and crowd size increase",
        "nurse": "Nursing / paramedic establishment scaled to duration and audience",
        "nhs_ambulance_manager": "Structured liaison with NHS ambulance service",
        "support_unit": "Medical facility / equipment scaled to expected presentations",
    },
    4: {
        "first_responder": (
            "Substantial multidisciplinary footprint (technicians through to advanced roles), "
            "coordinated under a senior clinical lead, per large complex event guidance."
        ),
        "ambulances": "Multiple crewed ambulances so transfers do not remove on-site resilience",
        "ambulance_crew": "Rota allows concurrent transfer and on-site cover",
        "doctor": "Doctors within the team as complexity requires",
        "nurse": "Major nursing / paramedic capacity alongside responders",
        "nhs_ambulance_manager": "Close coordination; command-and-control aligned with major-event practice",
        "support_unit": "Substantial medical posts; welfare / support as appropriate",
    },
    5: {
        "first_responder": (
            "Largest-scale temporary healthcare footprint: wide skill mix with dedicated "
            "coordination — consistent with mass-gathering / highest-complexity tier framing."
        ),
        "ambulances": "Fleet sized for mass-gathering transfer load while preserving on-site care",
        "ambulance_crew": "Major rota; resilience for concurrent major incidents",
        "doctor": "Senior clinical lead (often emergency / pre-hospital experience) as the Guide describes",
        "nurse": "Large nursing / paramedic establishment",
        "nhs_ambulance_manager": "Joint working with NHS ambulance, police, and local authority as standard",
        "support_unit": "Medical control / coordination hub; major-incident planning embedded",
    },
}

# Planning hints for headcount math (separate from the narrative matrix above).
TIER_MEDIC_BASE: dict[int, int] = {1: 2, 2: 4, 3: 8, 4: 14, 5: 22}
TIER_VEHICLE_BASE: dict[int, int] = {1: 1, 2: 1, 3: 2, 4: 3, 5: 4}

TIER_GUIDANCE: dict[int, dict[str, Any]] = {
    1: {
        "title": "Tier 1 — smallest / simplest",
        "summary": (
            "Often short duration, minimal injury risk, little or no alcohol or drugs, "
            "typically smaller crowds, hospital referrals very unlikely — the Guide’s lowest tier."
        ),
        "cover": [
            "Proportionate professional event medical cover (Guide: suitably trained event medical staff).",
            "Clear access, escalation routes, and AED awareness.",
        ],
    },
    2: {
        "title": "Tier 2 — local-licensing scale",
        "summary": (
            "Often up to a day, social drinking, isolated drug use, up to ~2k attendees, "
            "hospital referrals unlikely."
        ),
        "cover": [
            "Dedicated first aid resource; preferably healthcare professional–led where indicated.",
            "Nominated on-site lead for medical provision.",
            "Ambulance with qualified crew if hospital transfers are expected.",
        ],
    },
    3: {
        "title": "Tier 3 — larger / more illness & injury potential",
        "summary": (
            "Multi-day or moderate activity risk, alcohol or drug intoxication likely, "
            "up to ~5k attendees, hospital referrals foreseeable."
        ),
        "cover": [
            "Dedicated medical resource; clinical lead should be a registered healthcare professional "
            "with pre-hospital experience.",
            "Mix of healthcare professionals, first responders, and ambulances for transfers as needed.",
        ],
    },
    4: {
        "title": "Tier 4 — complex / high attendance",
        "summary": (
            "Multi-day, significant activity risk, intoxication expected, up to ~10k attendees, "
            "hospital referrals likely."
        ),
        "cover": [
            "Clinical lead (registered HCP, pre-hospital experience), doctors / paramedics / nurses, "
            "first responders, ambulances for transfer.",
            "Do not use ambulances as static treatment rooms — use a proper medical centre / post.",
        ],
    },
    5: {
        "title": "Tier 5 — mass gathering / highest risk",
        "summary": (
            "Largest or most complex events; high illness/injury risk; often 10k+ attendees; "
            "hospital referrals expected."
        ),
        "cover": [
            "Comprehensive medical resource; clinical lead often a senior doctor (e.g. emergency medicine).",
            "Sufficient cover that a hospital transfer does not strip the site of care.",
            "Formal medical needs assessment and detailed medical plan — typical SAG expectation.",
        ],
    },
}

CQC_COMPLIANCE_LINES = (
    "CQC registration: in England, vehicles used to transport patients to hospital are regulated; "
    "your provider should be appropriately registered where the law applies.",
    "HSE / industry guidance stresses ambulances should not be used as the primary treatment area — "
    "plan a dedicated medical post or centre so vehicles remain available for emergencies and transfers.",
)


def conversion_row_for_tier(tier: int) -> dict[str, Any]:
    """Narrative resource row for Purple Guide tier 1–5 (keys match legacy conversion_row shape)."""
    t = max(1, min(5, int(tier)))
    row = TIER_RESOURCE_MATRIX.get(t, TIER_RESOURCE_MATRIX[3])
    return {
        "score_range": f"Tier {t}",
        "tier": t,
        **dict(row),
    }


def public_tier_resource_rows() -> list[dict[str, Any]]:
    """Tiers 1–5 for the public calculator (tojson) — same structure as conversion_row_for_tier."""
    return [conversion_row_for_tier(t) for t in range(1, 6)]


def infer_tier_from_signals(
    *,
    purple_score: int,
    expected_attendees: int,
    duration_span: str,
    activity_risk: str,
    drug_risk: str,
    alcohol_level: str,
    hospital_referrals: str,
    hospital_drive_band: str | None = None,
    disorder_risk: str | None = None,
) -> int:
    """Derive indicative tier 1–5 from score and headline factors (Purple Guide style)."""
    n = max(0, int(expected_attendees))
    d = (duration_span or "single_day").strip().lower()
    ar = (activity_risk or "moderate").strip().lower()
    dr = (drug_risk or "none").strip().lower()
    al = (alcohol_level or "none").strip().lower()
    hr = (hospital_referrals or "unlikely").strip().lower()
    hdrv = (hospital_drive_band or "unknown").strip().lower()
    if hdrv not in ("unknown", "under_15", "15_30", "over_30"):
        hdrv = "unknown"
    dis = (disorder_risk or "none").strip().lower()
    if dis not in ("none", "low", "medium", "high", "opposing_factions"):
        dis = "none"

    if n > 10000 or purple_score >= 72:
        t = 5
    elif n > 5000 or purple_score >= 58:
        t = 4
    elif (
        n > 2000
        or purple_score >= 44
        or (d == "multi_day" and (al in ("likely", "expected") or dr in ("likely", "expected")))
        or hr == "likely"
        or (hdrv == "over_30" and hr in ("possible", "likely") and n >= 800)
    ):
        t = 3
    elif n > 500 or purple_score >= 28 or al in ("social", "likely", "expected") or ar in (
        "significant",
        "high",
    ):
        t = 2
    else:
        t = 1

    if dis == "opposing_factions" and n >= 100:
        t = max(t, 3)
    elif dis == "high" and n >= 250:
        t = max(t, 3)
    elif dis == "medium" and n >= 800:
        t = max(t, 2)

    return min(5, t)


def compute_purple_score(
    *,
    expected_attendees: int,
    duration_hours: float,
    venue_outdoor: bool,
    alcohol: bool,
    late_finish: bool,
    crowd_profile: str,
    activity_risk: str,
    drug_risk: str,
    duration_span: str,
    hospital_referrals: str,
    alcohol_level: str | None,
    hospital_drive_band: str | None = None,
    audience_posture: str | None = None,
    disorder_risk: str | None = None,
    casualty_history: str | None = None,
    audience_dwell: str | None = None,
    season: str | None = None,
    definitive_care: str | None = None,
    additional_hazards: list[str] | None = None,
    on_site_facilities: list[str] | None = None,
) -> tuple[int, list[str]]:
    """
    Return (purple_score 0–100+, factor strings for UI).
    """
    n = max(0, int(expected_attendees))
    factors: list[str] = []
    score = 0.0

    # Attendance (Purple Guide: risk-led, not attendance-only — but size still matters)
    if n >= 10000:
        score += 34
        factors.append("Very large crowd (10k+)")
    elif n >= 5000:
        score += 28
        factors.append("Large crowd (5k–10k)")
    elif n >= 2000:
        score += 20
        factors.append("Medium–large crowd (2k–5k)")
    elif n >= 500:
        score += 12
        factors.append("Crowd 500–2k")
    elif n > 0:
        score += 4
        factors.append("Smaller crowd (under 500)")

    dspan = (duration_span or "single_day").strip().lower()
    if dspan == "multi_day":
        score += 22
        factors.append("Multi-day / overnight programme (tiering: higher governance)")
    elif dspan == "few_hours":
        score += 2
        factors.append("Short programme (few hours)")
    else:
        score += 8
        factors.append("Single-day style duration")
    # Cross-check with hours field
    h = max(1.0, min(float(duration_hours), 72.0))
    if h > 24 and dspan != "multi_day":
        score += 6
        factors.append("Long on-site window")

    ar = (activity_risk or "moderate").strip().lower()
    if ar == "high":
        score += 24
        factors.append("High-risk activities (sports, motors, stages, water, etc.)")
    elif ar == "significant":
        score += 14
        factors.append("Significant injury / illness risk from activities")
    elif ar == "moderate":
        score += 6
        factors.append("Moderate activity risk")
    else:
        factors.append("Lower activity risk")

    alvl = (alcohol_level or ("social" if alcohol else "none")).strip().lower()
    if alvl == "expected":
        score += 18
        factors.append("Alcohol intoxication expected")
    elif alvl == "likely":
        score += 12
        factors.append("Alcohol intoxication likely")
    elif alvl == "social":
        score += 6
        factors.append("Social drinking")
    else:
        factors.append("Little or no alcohol")

    dr = (drug_risk or "none").strip().lower()
    if dr == "expected":
        score += 18
        factors.append("Recreational drug intoxication expected")
    elif dr == "likely":
        score += 12
        factors.append("Drug intoxication likely")
    elif dr == "isolated":
        score += 5
        factors.append("Isolated drug use possible")
    else:
        factors.append("No / minimal drug risk indicated")

    if venue_outdoor:
        score += 7
        factors.append("Outdoor / exposed site")

    if late_finish:
        score += 9
        factors.append("Late finish (after 23:00)")

    cp = (crowd_profile or "mixed").strip().lower()
    if cp in ("young_adult", "young adult", "nightlife"):
        score += 7
        factors.append("Young adult / evening-led crowd profile")

    hr = (hospital_referrals or "unlikely").strip().lower()
    if hr == "likely":
        score += 16
        factors.append("Hospital transfers likely — plan ambulance & CQC-registered providers")
    elif hr == "possible":
        score += 8
        factors.append("Some hospital transfers possible")
    else:
        factors.append("Hospital referrals assessed as unlikely at this stage")

    hdrv = (hospital_drive_band or "unknown").strip().lower()
    if hdrv not in ("unknown", "under_15", "15_30", "over_30"):
        hdrv = "unknown"
    if hdrv == "over_30":
        score += 10
        if hr in ("possible", "likely"):
            score += 8
        factors.append(
            "Long drive to A&E / hospital (~30+ min) — allow extra transfer time and ambulance resilience"
        )
    elif hdrv == "15_30":
        score += 6
        if hr == "likely":
            score += 4
        factors.append(
            "Moderate drive to A&E (~15–30 min) — factor journey time into transfer planning"
        )
    elif hdrv == "under_15":
        factors.append("Shorter drive to A&E (under ~15 min) — still plan concurrent on-site cover")
    else:
        factors.append(
            "Drive time to A&E not specified — your medical provider will confirm with local hospitals"
        )

    # Optional questionnaire-style factors (crowd posture, civil disorder, history, dwell, season, A&E profile)
    post = (audience_posture or "unknown").strip().lower()
    if post not in ("unknown", "seated", "mixed", "standing"):
        post = "unknown"
    if post == "standing":
        score += 8
        factors.append("Predominantly standing audience — higher density / fatigue / crush dynamics")
    elif post == "mixed":
        score += 4
        factors.append("Mixed seated and standing audience")

    dis = (disorder_risk or "none").strip().lower()
    if dis not in ("none", "low", "medium", "high", "opposing_factions"):
        dis = "none"
    if dis == "opposing_factions":
        score += 22
        factors.append("Opposing factions / high tension — multi-agency medical and police planning typical")
    elif dis == "high":
        score += 16
        factors.append("Elevated disorder or march risk — plan for surge presentations and egress")
    elif dis == "medium":
        score += 10
        factors.append("Moderate disorder or protest profile possible")
    elif dis == "low":
        score += 3
        factors.append("Low-level public-order risk noted")

    ch = (casualty_history or "unknown").strip().lower()
    if ch not in ("unknown", "low_rate", "medium_rate", "high_rate", "first_event"):
        ch = "unknown"
    if ch == "first_event":
        score += 8
        factors.append("First event / no prior casualty data — baseline demand uncertain; plan conservatively")
    elif ch == "high_rate":
        score += 12
        factors.append("Historically high on-site casualty rate — align resourcing with past learning")
    elif ch == "medium_rate":
        score += 5
        factors.append("Prior events showed moderate casualty rates")
    elif ch == "low_rate":
        factors.append("Prior events showed low casualty rates (still validate for this programme)")

    dwell = (audience_dwell or "unknown").strip().lower()
    if dwell not in ("unknown", "under_4h", "over_4h", "over_12h"):
        dwell = "unknown"
    # Avoid double-counting with "hours on site": dwell only adds when it signals more than the cover window.
    if dwell == "over_12h":
        if h < 11:
            score += 12
            factors.append(
                "Long audience dwell (12h+ typical stay) vs shorter posted cover — welfare / throughput load"
            )
        elif h < 20:
            score += 5
            factors.append(
                "Long dwell partly overlaps your cover hours — modest extra factor for fatigue / welfare density"
            )
    elif dwell == "over_4h":
        if h < 8:
            score += 6
            factors.append("Extended audience stay (4h+) vs shorter cover window — clarify rota vs gate times")
        elif h < 12:
            score += 3
            factors.append("Moderate dwell signal alongside mid-length cover hours")

    seas = (season or "unknown").strip().lower()
    if seas not in ("unknown", "spring", "summer", "autumn", "winter"):
        seas = "unknown"
    if seas == "winter" and venue_outdoor:
        score += 8
        factors.append("Winter outdoor — cold injury, slips, longer transfer exposure")
    elif seas == "winter":
        score += 3
        factors.append("Winter programme — seasonal illness / cold stress possible")
    elif seas == "summer" and venue_outdoor:
        score += 6
        factors.append("Summer outdoor — heat, sun, dehydration demand on medical footprint")
    elif seas == "summer":
        score += 2
        factors.append("Summer timing — heat load if crowds are dense or unshaded")

    dc = (definitive_care or "unknown").strip().lower()
    if dc not in ("unknown", "choice_ae", "large_ae", "small_ae"):
        dc = "unknown"
    if dc == "small_ae":
        score += 8
        factors.append("Smaller / rural A&E — longer offload or transfer to definitive care may apply")
    elif dc == "large_ae":
        factors.append("Large A&E department nearby — confirm ambulance handover routes")
    elif dc == "choice_ae":
        factors.append("Multiple A&E options — agree preferred receiving hospitals in the plan")

    # K/L-style extras (common on provider calculators — CTC, Beyond First Aid, etc.)
    _hz_allowed = frozenset(
        {"carnival", "parachute_display", "helicopters", "street_theatre", "motor_sport"}
    )
    _fc_allowed = frozenset(
        {"suturing", "plastering", "xray", "psych_gp", "minor_surgery"}
    )
    hz_list = [x for x in (additional_hazards or []) if x in _hz_allowed]
    fc_list = [x for x in (on_site_facilities or []) if x in _fc_allowed]
    hz_purple = {
        "carnival": 4,
        "parachute_display": 6,
        "helicopters": 6,
        "street_theatre": 3,
        "motor_sport": 5,
    }
    hz_cap = 16
    hz_sum = 0
    for hx in hz_list:
        w = hz_purple.get(hx, 0)
        if hx == "motor_sport" and ar in ("high", "significant"):
            w = min(w, 2)
        hz_sum += w
    hz_sum = min(hz_sum, hz_cap)
    if hz_sum:
        score += hz_sum
        factors.append(
            "Additional hazards (organiser): "
            + ", ".join(hx.replace("_", " ") for hx in hz_list)
        )
    fc_purple = {
        "suturing": 5,
        "plastering": 4,
        "xray": 9,
        "psych_gp": 6,
        "minor_surgery": 9,
    }
    fc_cap = 22
    fc_sum = min(sum(fc_purple.get(fx, 0) for fx in fc_list), fc_cap)
    if fc_sum:
        score += fc_sum
        factors.append(
            "On-site clinical capabilities flagged (organiser): "
            + ", ".join(fx.replace("_", " ") for fx in fc_list)
        )

    purple = int(round(score))
    purple = max(0, min(purple, 120))
    return purple, factors
