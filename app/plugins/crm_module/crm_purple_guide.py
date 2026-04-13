"""
Purple Guide–style tiering and indicative conversion table (educational / planning only).

Not a licensed medical needs assessment. Copy is aligned with common UK event-safety framing
(HSE Purple Guide themes, CQC registration for patient transport ambulances). Tables are
indicative — always follow your MNA, insurer, venue, and regulator.
"""
from __future__ import annotations

from typing import Any

# (lo, hi inclusive for purple_score), staffing hints from public industry calculator examples.
# NHS ambulance manager column may be numeric count or "VISIT".
_PURPLE_CONVERSION: list[tuple[tuple[int, int], dict[str, Any]]] = [
    ((0, 20), {
        "first_responder": 4,
        "ambulances": 0,
        "ambulance_crew": 0,
        "doctor": 0,
        "nurse": 0,
        "nhs_ambulance_manager": 0,
        "support_unit": 0,
    }),
    ((21, 25), {
        "first_responder": 6,
        "ambulances": 1,
        "ambulance_crew": 2,
        "doctor": 0,
        "nurse": 0,
        "nhs_ambulance_manager": "VISIT",
        "support_unit": 0,
    }),
    ((26, 30), {
        "first_responder": 8,
        "ambulances": 1,
        "ambulance_crew": 2,
        "doctor": 0,
        "nurse": 0,
        "nhs_ambulance_manager": "VISIT",
        "support_unit": 0,
    }),
    ((31, 35), {
        "first_responder": 12,
        "ambulances": 2,
        "ambulance_crew": 8,
        "doctor": 1,
        "nurse": 2,
        "nhs_ambulance_manager": 1,
        "support_unit": 0,
    }),
    ((36, 40), {
        "first_responder": 20,
        "ambulances": 3,
        "ambulance_crew": 10,
        "doctor": 2,
        "nurse": 4,
        "nhs_ambulance_manager": 1,
        "support_unit": 0,
    }),
    ((41, 50), {
        "first_responder": 40,
        "ambulances": 4,
        "ambulance_crew": 12,
        "doctor": 3,
        "nurse": 6,
        "nhs_ambulance_manager": 2,
        "support_unit": 1,
    }),
    ((51, 60), {
        "first_responder": 60,
        "ambulances": 4,
        "ambulance_crew": 12,
        "doctor": 4,
        "nurse": 8,
        "nhs_ambulance_manager": 2,
        "support_unit": 1,
    }),
    ((61, 65), {
        "first_responder": 80,
        "ambulances": 5,
        "ambulance_crew": 14,
        "doctor": 5,
        "nurse": 10,
        "nhs_ambulance_manager": 3,
        "support_unit": 1,
    }),
    ((66, 70), {
        "first_responder": 100,
        "ambulances": 6,
        "ambulance_crew": 16,
        "doctor": 6,
        "nurse": 12,
        "nhs_ambulance_manager": 4,
        "support_unit": 2,
    }),
    ((71, 75), {
        "first_responder": 150,
        "ambulances": 10,
        "ambulance_crew": 24,
        "doctor": 9,
        "nurse": 18,
        "nhs_ambulance_manager": 6,
        "support_unit": 3,
    }),
    ((76, 999), {
        "first_responder": "200+",
        "ambulances": "15+",
        "ambulance_crew": "35+",
        "doctor": "12+",
        "nurse": "24+",
        "nhs_ambulance_manager": "8+",
        "support_unit": 3,
    }),
]

TIER_GUIDANCE: dict[int, dict[str, Any]] = {
    1: {
        "title": "Tier 1 — smallest / simplest",
        "summary": (
            "Often short duration, minimal injury risk, little or no alcohol or drugs, "
            "typically under 500 attendees, hospital referrals very unlikely."
        ),
        "cover": [
            "May be appropriate with first aid kit, trained volunteers, and clear access to emergency help.",
            "Know nearest AED (e.g. defibfinder) and how to call emergency services.",
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


def conversion_row_for_purple_score(purple_score: int) -> dict[str, Any]:
    ps = max(0, int(purple_score))
    for (lo, hi), row in _PURPLE_CONVERSION:
        if lo <= ps <= hi:
            return {
                "score_range": f"{lo}–{hi}" if hi < 999 else f"{lo}+",
                **row,
            }
    return {**_PURPLE_CONVERSION[-1][1], "score_range": "76+"}


def infer_tier_from_signals(
    *,
    purple_score: int,
    expected_attendees: int,
    duration_span: str,
    activity_risk: str,
    drug_risk: str,
    alcohol_level: str,
    hospital_referrals: str,
) -> int:
    """Derive indicative tier 1–5 from score and headline factors (Purple Guide style)."""
    n = max(0, int(expected_attendees))
    d = (duration_span or "single_day").strip().lower()
    ar = (activity_risk or "moderate").strip().lower()
    dr = (drug_risk or "none").strip().lower()
    al = (alcohol_level or "none").strip().lower()
    hr = (hospital_referrals or "unlikely").strip().lower()

    # Strong upward drivers
    if n > 10000 or purple_score >= 72:
        return 5
    if n > 5000 or purple_score >= 58:
        return 4
    if (
        n > 2000
        or purple_score >= 44
        or (d == "multi_day" and (al in ("likely", "expected") or dr in ("likely", "expected")))
        or hr == "likely"
    ):
        return 3
    if n > 500 or purple_score >= 28 or al in ("social", "likely", "expected") or ar in (
        "significant",
        "high",
    ):
        return 2
    return 1


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

    purple = int(round(score))
    purple = max(0, min(purple, 120))
    return purple, factors
