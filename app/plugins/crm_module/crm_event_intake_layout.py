"""Desk event intake layout: hideable sections + optional extra fields (manifest settings)."""
from __future__ import annotations

import json
import re
from typing import Any

from flask import Flask

from .crm_manifest_settings import get_crm_module_settings, update_crm_module_settings

# Sections safe to omit from POST — parser uses defaults (see parse_public_calculator_form).
EVENT_INTAKE_SECTION_IDS: frozenset[str] = frozenset(
    {
        "desk_checklist",
        "account_history",
        "venue_location",
        "audience_scale",
        "risk_calculator",
        "risk_more_detail",
        "internal_notes",
    }
)

_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,40}$")
_MAX_EXTRAS = 12

_EXTRA_BLOCKED_BASE: frozenset[str] = frozenset(
    {
        "organisation_name",
        "company_name",
        "contact_name",
        "email",
        "phone",
        "event_name",
        "message",
        "website",
    }
)

# Lowercase keys only — must not collide with configured extra field keys.
PRIVATE_TRANSFER_FORM_FIELD_KEYS: frozenset[str] = frozenset(
    {
        "patientfirst",
        "patientlast",
        "dob",
        "patientweight",
        "additionalneeds",
        "pickupdate",
        "pickuptime",
        "journeytype",
        "returndate",
        "returntime",
        "collectstreet1",
        "collectstreet2",
        "collecttown",
        "collectpostcode",
        "accessrequirementscollection",
        "destinationstreet1",
        "destinationstreet2",
        "destinationtown",
        "destinationpostcode",
        "accessrequirementsdestination",
        "medicalhistory",
        "currentmedications",
        "servicemobility",
        "crewvehicle",
        "crewvehicle_other_text",
        "travelmethod",
        "travel_other_text",
        "applicantsname",
        "applicantsphone",
        "applicantsmail",
        "payeecompany",
        "payeename",
        "payeemail",
        "payeephone",
        "pronouns",
        "gender",
        "escort",
        "account_id",
        "stage",
    }
)

PRIVATE_TRANSFER_INTAKE_SECTION_IDS: frozenset[str] = frozenset(
    {
        "pt_progress_header",
        "pt_patient_extras",
        "pt_return_leg",
        "pt_infectious_meds",
        "pt_payee_contact_extras",
    }
)


def _normalize_hidden_sections(raw: Any, allowed: frozenset[str]) -> frozenset[str]:
    if raw is None:
        return frozenset()
    if isinstance(raw, str) and raw.strip():
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return frozenset()
    if not isinstance(raw, (list, tuple, set, frozenset)):
        return frozenset()
    out: set[str] = set()
    for x in raw:
        s = str(x or "").strip()
        if s in allowed:
            out.add(s)
    return frozenset(out)


def normalize_hidden_sections(raw: Any) -> frozenset[str]:
    return _normalize_hidden_sections(raw, EVENT_INTAKE_SECTION_IDS)


def normalize_pt_hidden_sections(raw: Any) -> frozenset[str]:
    return _normalize_hidden_sections(raw, PRIVATE_TRANSFER_INTAKE_SECTION_IDS)


def normalize_extra_fields(
    raw: Any,
    *,
    blocked_keys: frozenset[str] | None = None,
) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, str) and raw.strip():
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []
    blocked = _EXTRA_BLOCKED_BASE | (blocked_keys or frozenset())
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in raw[:_MAX_EXTRAS]:
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "").strip().lower()
        if not key or key in seen or not _KEY_RE.match(key):
            continue
        if key in blocked:
            continue
        label = (str(row.get("label") or "").strip() or key.replace("_", " ").title())[:200]
        typ = str(row.get("type") or row.get("field_type") or "text").strip().lower()
        if typ not in ("text", "textarea"):
            typ = "text"
        req = bool(row.get("required"))
        seen.add(key)
        out.append({"key": key, "label": label, "type": typ, "required": req})
    return out


def load_event_intake_layout(app: Flask) -> tuple[frozenset[str], list[dict[str, Any]]]:
    s = get_crm_module_settings(app)
    hidden = normalize_hidden_sections(s.get("event_intake_hidden_sections"))
    extras = normalize_extra_fields(s.get("event_intake_extra_fields"))
    return hidden, extras


def load_private_transfer_intake_layout(
    app: Flask,
) -> tuple[frozenset[str], list[dict[str, Any]]]:
    s = get_crm_module_settings(app)
    hidden = normalize_pt_hidden_sections(
        s.get("private_transfer_intake_hidden_sections")
    )
    extras = normalize_extra_fields(
        s.get("private_transfer_intake_extra_fields"),
        blocked_keys=PRIVATE_TRANSFER_FORM_FIELD_KEYS,
    )
    return hidden, extras


def parse_hidden_sections_from_settings_post() -> frozenset[str]:
    from flask import request as rq

    return normalize_hidden_sections(rq.form.getlist("hidden[]"))


def parse_pt_hidden_sections_from_settings_post() -> frozenset[str]:
    from flask import request as rq

    return normalize_pt_hidden_sections(rq.form.getlist("pt_hidden[]"))


def save_event_intake_layout(
    app: Flask,
    *,
    hidden_sections: frozenset[str],
    extra_fields: list[dict[str, Any]],
) -> None:
    update_crm_module_settings(
        app,
        {
            "event_intake_hidden_sections": list(hidden_sections),
            "event_intake_extra_fields": extra_fields,
        },
    )


def save_private_transfer_intake_layout(
    app: Flask,
    *,
    hidden_sections: frozenset[str],
    extra_fields: list[dict[str, Any]],
) -> None:
    update_crm_module_settings(
        app,
        {
            "private_transfer_intake_hidden_sections": list(hidden_sections),
            "private_transfer_intake_extra_fields": extra_fields,
        },
    )


def collect_intake_extras_from_form(
    form: Any, defs: list[dict[str, Any]]
) -> list[dict[str, str]]:
    """Read intake_extra_<key> from form; returns list for lead_meta (key, label, value)."""
    rows: list[dict[str, str]] = []
    for fd in defs:
        key = str(fd.get("key") or "").strip()
        if not key:
            continue
        name = f"intake_extra_{key}"
        raw = form.get(name) if hasattr(form, "get") else None
        val = (raw if isinstance(raw, str) else str(raw or "")).strip()
        if len(val) > 8000:
            val = val[:8000]
        label = str(fd.get("label") or key)
        rows.append({"key": key, "label": label, "value": val})
    return rows


# Backwards-compatible name
def collect_event_intake_extras_from_form(
    form: Any, defs: list[dict[str, Any]]
) -> list[dict[str, str]]:
    return collect_intake_extras_from_form(form, defs)


def validate_required_extras(
    form: Any, defs: list[dict[str, Any]]
) -> list[str]:
    """Return human-readable error strings for missing required extras."""
    errs: list[str] = []
    for fd in defs:
        if not fd.get("required"):
            continue
        key = str(fd.get("key") or "").strip()
        if not key:
            continue
        name = f"intake_extra_{key}"
        v = (form.get(name) if hasattr(form, "get") else None) or ""
        if not str(v).strip():
            errs.append(str(fd.get("label") or key))
    return errs


def parse_extra_fields_from_settings_post() -> list[dict[str, Any]]:
    """POST from settings page: parallel extra_key[], extra_label[], extra_type[], extra_required[]."""
    from flask import request as rq

    keys = rq.form.getlist("extra_key[]")
    labels = rq.form.getlist("extra_label[]")
    types = rq.form.getlist("extra_type[]")
    req_flags = rq.form.getlist("extra_required[]")
    rows: list[dict[str, Any]] = []
    n = max(len(keys), len(labels), len(types))
    for i in range(n):
        key = (keys[i] if i < len(keys) else "").strip().lower()
        if not key:
            continue
        lb = (labels[i] if i < len(labels) else "").strip()
        typ = (types[i] if i < len(types) else "text").strip().lower() or "text"
        rf = (req_flags[i] if i < len(req_flags) else "0").strip().lower()
        req = rf in ("1", "true", "yes", "on")
        rows.append({"key": key, "label": lb, "type": typ, "required": req})
    return normalize_extra_fields(rows)


def parse_pt_extra_fields_from_settings_post() -> list[dict[str, Any]]:
    """POST: pt_extra_key[], pt_extra_label[], pt_extra_type[], pt_extra_required[]."""
    from flask import request as rq

    keys = rq.form.getlist("pt_extra_key[]")
    labels = rq.form.getlist("pt_extra_label[]")
    types = rq.form.getlist("pt_extra_type[]")
    req_flags = rq.form.getlist("pt_extra_required[]")
    rows: list[dict[str, Any]] = []
    n = max(len(keys), len(labels), len(types))
    for i in range(n):
        key = (keys[i] if i < len(keys) else "").strip().lower()
        if not key:
            continue
        lb = (labels[i] if i < len(labels) else "").strip()
        typ = (types[i] if i < len(types) else "text").strip().lower() or "text"
        rf = (req_flags[i] if i < len(req_flags) else "0").strip().lower()
        req = rf in ("1", "true", "yes", "on")
        rows.append({"key": key, "label": lb, "type": typ, "required": req})
    return normalize_extra_fields(
        rows, blocked_keys=PRIVATE_TRANSFER_FORM_FIELD_KEYS
    )
