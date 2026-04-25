"""
Human-readable names for audit timeline / scheduled export domain keys (internal keys unchanged).
"""
from __future__ import annotations

from typing import Any

from .adapters import ALL_DOMAIN_KEYS

AUDIT_DOMAIN_LABELS: dict[str, str] = {
    "admin_audit": "Admin and staff audit",
    "cad": "CAD incident jobs",
    "cad_json": "CAD JSON signals (e.g. panic, priority)",
    "cad_comms": "CAD messages (crew and dispatch)",
    "intake": "Intake and triage",
    "epcr": "ePCR case summaries",
    "epcr_json": "ePCR record JSON changes",
    "epcr_clinical_audit": "ePCR and Cura access audit log",
    "epcr_access": "EPCR access requests and reviews",
    "mi": "Minor injury events",
    "mi_reports": "Minor injury reports",
    "inventory": "Inventory audit",
    "cura_event": "Cura operational events",
    "safeguarding": "Safeguarding referrals",
    "identity": "Sign-in and access",
    "training": "Training audit",
    "governance_exports": "Governance export history",
    # Event rows use internal domain key `governance` (export log); picker key is governance_exports.
    "governance": "Governance export history",
    "compliance_policies": "Policies & published documents (lifecycle / issue / retire)",
}


def audit_domain_options() -> list[dict[str, Any]]:
    return [
        {"key": k, "label": AUDIT_DOMAIN_LABELS.get(k, k)} for k in ALL_DOMAIN_KEYS
    ]
