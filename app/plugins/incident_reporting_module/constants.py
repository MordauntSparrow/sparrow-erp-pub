"""Status machine, categories, and transition rules (server-side)."""

from __future__ import annotations

from typing import Dict, FrozenSet, Tuple

# Sparrow ``users.role`` values assignable in Safety & Incidents → Settings → form visibility.
INCIDENT_FORM_VISIBILITY_ROLE_CHOICES: Tuple[Tuple[str, str], ...] = (
    ("staff", "Staff"),
    ("crew", "Crew"),
    ("user", "User"),
    ("logistics", "Logistics"),
    ("admin", "Administrator"),
    ("clinical_lead", "Clinical lead"),
    ("superuser", "Superuser"),
)

# Configurable display order; enforced transitions below.
STATUS_LABELS: Dict[str, str] = {
    "draft": "Draft",
    "pending_safeguarding": "Pending safeguarding",
    "submitted": "Submitted",
    "triaged": "Triaged",
    "investigation": "Investigation",
    "action_plan": "Action plan",
    "verification": "Verification",
    "closed": "Closed",
    "withdrawn": "Withdrawn",
    "merged": "Merged",
    "legal_hold": "Legal hold",
}

TERMINAL_STATUSES: FrozenSet[str] = frozenset(
    {"closed", "withdrawn", "merged"}
)

# Admin pipeline / kanban columns: (column_id, title, status keys in this column)
PIPELINE_STAGE_GROUPS: Tuple[Tuple[str, str, Tuple[str, ...]], ...] = (
    ("intake", "Draft & intake", ("draft", "pending_safeguarding")),
    ("submitted_lane", "Submitted", ("submitted",)),
    ("progress", "Investigation & plan", ("triaged", "investigation", "action_plan")),
    ("verification", "Verification", ("verification",)),
    ("legal", "Legal hold", ("legal_hold",)),
    ("terminal", "Closed / withdrawn", ("closed", "withdrawn", "merged")),
)

# from_status -> allowed to_status
ALLOWED_TRANSITIONS: Dict[str, FrozenSet[str]] = {
    "draft": frozenset(
        {"submitted", "withdrawn", "pending_safeguarding", "legal_hold"}
    ),
    "pending_safeguarding": frozenset({"draft", "submitted", "withdrawn"}),
    "submitted": frozenset({"triaged", "withdrawn", "legal_hold", "merged"}),
    "triaged": frozenset(
        {"investigation", "action_plan", "withdrawn", "legal_hold", "merged"}
    ),
    "investigation": frozenset(
        {"action_plan", "triaged", "verification", "legal_hold", "merged"}
    ),
    "action_plan": frozenset(
        {"verification", "investigation", "legal_hold", "merged"}
    ),
    "verification": frozenset(
        {"closed", "investigation", "legal_hold", "merged"}
    ),
    "closed": frozenset({"legal_hold"}),  # reopen to legal hold only
    "withdrawn": frozenset(set()),
    "merged": frozenset(set()),
    "legal_hold": frozenset(
        {"draft", "submitted", "triaged", "investigation", "action_plan", "verification"}
    ),
}

INCIDENT_MODES: Tuple[Tuple[str, str], ...] = (
    ("actual", "Actual event"),
    ("near_miss", "Near miss"),
    ("hazard", "Hazard"),
    ("good_catch", "Good catch"),
)

# Universal categories (always shown)
UNIVERSAL_CATEGORIES: Tuple[Tuple[str, str], ...] = (
    ("slip_trip_fall", "Slip, trip, or fall"),
    ("manual_handling", "Manual handling / ergonomics"),
    ("equipment_failure", "Equipment / machinery"),
    ("workplace_environment", "Workplace environment"),
    ("security_event", "Security-related"),
    ("fire_safety", "Fire safety"),
    ("road_transport", "Road / transport"),
    ("other_hs", "Other health & safety"),
)

# Medical overlay (when industry_visible('medical'))
MEDICAL_CATEGORIES: Tuple[Tuple[str, str], ...] = (
    ("patient_care", "Patient care"),
    ("medication", "Medication"),
    ("infection_control", "Infection prevention & control"),
    ("patient_fall", "Patient fall"),
    ("restraint", "Restraint / restrictive practice"),
    ("safeguarding_concern", "Safeguarding concern"),
    ("pressure_injury", "Tissue viability / pressure injury"),
    ("other_clinical", "Other clinical"),
)

ORG_SEVERITY_DEFAULTS: Tuple[Tuple[str, str], ...] = (
    ("low", "Low"),
    ("medium", "Medium"),
    ("high", "High"),
    ("critical", "Critical"),
)

# PSIRF-style labels (configurable in UI later; defaults here)
# Stored inside ``incidents.medication_json`` (MERP-style clinical capture).
MERP_FIELD_KEYS: Tuple[str, ...] = (
    "merp_outcome",
    "process_node",
    "error_type",
    "high_alert",
    "batch_lot",
)

MERP_FIELD_LABELS: Dict[str, str] = {
    "merp_outcome": "MERP outcome",
    "process_node": "Process node (e.g. prescribe → administer → monitor)",
    "error_type": "Error type",
    "high_alert": "High-alert medication",
    "batch_lot": "Batch / lot (optional)",
}

HARM_GRADE_DEFAULTS: Tuple[Tuple[str, str], ...] = (
    ("none", "No harm"),
    ("low_harm", "Low harm"),
    ("moderate_harm", "Moderate harm"),
    ("severe_harm", "Severe harm"),
    ("death", "Death"),
)

# Walkaround / inspection finding severity (risk-based scale; align captions in local H&S policy).
# Order: highest risk first (buttons left-to-right on walkaround completion form).
WALKAROUND_SEVERITY_CHOICES: Tuple[Tuple[str, str], ...] = (
    ("critical", "Critical / imminent risk"),
    ("high", "High"),
    ("medium", "Medium"),
    ("low", "Low"),
    ("trivial", "Trivial / observation"),
)

WALKAROUND_SEVERITY_GUIDANCE: Dict[str, str] = {
    "trivial": "Note for learning only; routine local follow-up if needed.",
    "low": "Limited foreseeable harm; agree a proportionate rectification date with the area manager.",
    "medium": "Could cause harm if unaddressed; prioritise corrective action and document verification.",
    "high": "Serious harm plausible without prompt action; escalate per local governance and consider interim controls.",
    "critical": "Immediate danger to life or health; stop work / isolate if required, notify competent person, and record emergency actions taken.",
}

# Bootstrap outline colour token per severity (traffic-light style in UI + report).
WALKAROUND_SEVERITY_BTN_OUTLINE: Dict[str, str] = {
    "trivial": "secondary",
    "low": "success",
    "medium": "warning",
    "high": "danger",
    "critical": "dark",
}

# Solid badge classes for print-friendly report (no internal slug shown to readers).
WALKAROUND_SEVERITY_BADGE_CLASS: Dict[str, str] = {
    "trivial": "badge rounded-pill bg-light text-dark border",
    "low": "badge rounded-pill bg-success",
    "medium": "badge rounded-pill bg-warning text-dark",
    "high": "badge rounded-pill bg-danger",
    "critical": "badge rounded-pill bg-dark",
}

WALKAROUND_SEVERITY_SHORT: Dict[str, str] = {
    "trivial": "T",
    "low": "L",
    "medium": "M",
    "high": "H",
    "critical": "C",
}

IR1_FIELD_LABELS: Dict[str, str] = {
    "incident_occurred_at": "Date & time incident occurred",
    "incident_discovered_at": "Date & time incident discovered / reported",
    "exact_location_detail": "Exact location (ward, room, area, asset)",
    "witnesses_text": "Witnesses (names/roles or anonymised references)",
    "equipment_involved": "Equipment / substances / vehicles involved",
    "riddor_notifiable": "Possible RIDDOR notifiable incident (seek competent H&S advice)",
    "reporter_job_title": "Reporter job title / band",
    "reporter_department": "Reporter department / directorate",
    "reporter_contact_phone": "Reporter contact phone",
    "people_affected_count": "People directly affected (number)",
    "ir1_supplementary_json": "Additional fields (key / value)",
}
