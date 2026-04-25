"""Event plan METHANE / JESIP text for staff briefing on the issued PDF (planning reference, not CAD declaration)."""
from __future__ import annotations

import json
from typing import Any

from flask import request

# JSON keys align with Ventus CAD major-incident METHANE payload where applicable.
MI_JSON_KEYS: tuple[tuple[str, str], ...] = (
    ("major_incident_declared", "mi_major_incident_declared"),
    ("exact_location", "mi_exact_location"),
    ("type_of_incident", "mi_type_of_incident"),
    ("hazards", "mi_hazards"),
    ("access_routes", "mi_access_routes"),
    ("number_casualties", "mi_number_casualties"),
    ("emergency_services", "mi_emergency_services"),
    ("methane_notes", "mi_methane_notes"),
    ("jesip_notes", "mi_jesip_notes"),
    ("cordons_command_notes", "mi_cordons_command_notes"),
)

MI_FIELD_MAX = 4000

MI_METHANE_PDF_ROWS: tuple[tuple[str, str], ...] = (
    ("major_incident_declared", "M — Major incident"),
    ("exact_location", "E — Exact location"),
    ("type_of_incident", "T — Type of incident"),
    ("hazards", "H — Hazards"),
    ("access_routes", "A — Access / routes"),
    ("number_casualties", "N — Number / severity of casualties"),
    ("emergency_services", "E — Emergency services on scene / required"),
)


def coerce_major_incident_detail(raw: Any) -> dict[str, str]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        base = raw
    elif isinstance(raw, (bytes, str)):
        s = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
        s = s.strip()
        if not s:
            return {}
        try:
            parsed = json.loads(s)
        except (json.JSONDecodeError, TypeError):
            return {}
        base = parsed if isinstance(parsed, dict) else {}
    else:
        return {}
    out: dict[str, str] = {}
    for k, _ in MI_JSON_KEYS:
        v = base.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out[k] = s[:MI_FIELD_MAX]
    return out


def major_incident_detail_json_from_form() -> str | None:
    d: dict[str, str] = {}
    for json_key, form_name in MI_JSON_KEYS:
        v = (request.form.get(form_name) or "").strip()
        if v:
            d[json_key] = v[:MI_FIELD_MAX]
    if not d:
        return None
    return json.dumps(d, ensure_ascii=False)


def major_incident_structured_nonempty(detail: dict[str, str]) -> bool:
    return any((detail.get(k) or "").strip() for k, _ in MI_JSON_KEYS)


def major_incident_methane_pdf_rows(detail: dict[str, str]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for key, label in MI_METHANE_PDF_ROWS:
        val = (detail.get(key) or "").strip()
        if val:
            rows.append((label, val))
    return rows


def major_incident_extra_pdf_rows(detail: dict[str, str]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    n = (detail.get("methane_notes") or "").strip()
    if n:
        rows.append(("METHANE — narrative for staff", n))
    j = (detail.get("jesip_notes") or "").strip()
    if j:
        rows.append(("JESIP — briefing for staff", j))
    c = (detail.get("cordons_command_notes") or "").strip()
    if c:
        rows.append(("Cordons / command — briefing", c))
    return rows
