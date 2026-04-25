"""
Map Sparrow user roles to Clinical Systems handover labels for SPA authorisation.

Heuristic v1: privileged roles get broad labels; crew is clinician-only.
Refine with explicit permissions when the users table supports them.
"""
from __future__ import annotations

# Sparrow roles allowed on EPCR/Cura JSON API (subset used by JWT).
SPARROW_ROLES_API = frozenset({"crew", "admin", "superuser", "clinical_lead"})


def handover_roles_for_sparrow_role(role: str | None) -> dict:
    """
    Returns { "labels": [...], "is_clinician": bool, ... } for GET /api/cura/auth/me.
    """
    r = (role or "").strip().lower()
    labels: list[str] = []
    if r in ("admin", "superuser", "support_break_glass"):
        labels = ["admin", "clinician", "event_lead", "safeguarding_reviewer"]
    elif r == "clinical_lead":
        labels = ["clinical_lead", "clinician", "event_lead", "safeguarding_reviewer"]
    elif r == "crew":
        labels = ["clinician"]
    else:
        labels = []
    return {
        "handover_roles": labels,
        "is_clinician": "clinician" in labels,
        "is_event_lead": "event_lead" in labels,
        "is_safeguarding_reviewer": "safeguarding_reviewer" in labels,
        "is_admin": "admin" in labels,
    }
