"""
Turn stored export scope_json into structured UI data (no raw JSON in templates).
"""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

_FILTER_LABELS = {
    "date_from": "From",
    "date_to": "To",
    "cad": "CAD",
    "case_id": "Case ID",
    "path_like": "Path contains",
    "q": "Search text",
    "domains": "Domains",
}

def _fmt_val(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.isoformat(sep=" ", timespec="seconds")
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def export_scope_for_ui(raw: str | None) -> dict[str, Any]:
    """
    Returns:
      one_line: short summary for <summary>
      groups: [{title, fields: [{label, value, badges}]}]
      error: optional user-facing message
    """
    empty: dict[str, Any] = {"one_line": "—", "groups": [], "error": None}
    if raw is None or not str(raw).strip():
        return empty
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {
            "one_line": "Scope unavailable",
            "groups": [],
            "error": "Stored scope could not be read.",
        }
    if not isinstance(data, dict):
        return {
            "one_line": "Scope unavailable",
            "groups": [],
            "error": "Unexpected scope format.",
        }

    groups: list[dict[str, Any]] = []
    one_bits: list[str] = []

    fmt = data.get("format")
    if fmt:
        one_bits.append(str(fmt).upper())
    rp = data.get("redaction_profile")
    if rp:
        one_bits.append(str(rp))
    mr = data.get("matter_reference")
    if mr:
        one_bits.append(f"matter: {mr}")
    mid = data.get("matter_id")
    if mid is not None and str(mid).strip():
        one_bits.append(f"matter #{mid}")

    filters = data.get("filters")
    if isinstance(filters, dict):
        fields: list[dict[str, Any]] = []
        for key in (
            "date_from",
            "date_to",
            "cad",
            "case_id",
            "path_like",
            "q",
            "actor_sub",
            "entity_type_sub",
            "action_sub",
        ):
            val = filters.get(key)
            if val is None or str(val).strip() == "":
                continue
            label = _FILTER_LABELS.get(key, key.replace("_", " ").replace("sub", "").strip().title())
            if key == "actor_sub":
                label = "Actor contains"
            elif key == "entity_type_sub":
                label = "Entity type contains"
            elif key == "action_sub":
                label = "Action contains"
            fields.append(
                {
                    "label": label,
                    "value": _fmt_val(val),
                    "badges": None,
                }
            )
        dom = filters.get("domains")
        if isinstance(dom, list) and dom:
            fields.append(
                {
                    "label": "Domains",
                    "value": "",
                    "badges": [str(x) for x in dom],
                }
            )
        elif dom is not None and str(dom).strip():
            fields.append(
                {
                    "label": "Domains",
                    "value": _fmt_val(dom),
                    "badges": None,
                }
            )
        if fields:
            groups.append({"title": "Filters", "fields": fields})

    job_fields: list[dict[str, Any]] = []
    _job_labels = {
        "scheduled_job_id": "Scheduled job ID",
        "label": "Job label",
        "lookback_days": "Lookback (days)",
        "trigger": "Trigger",
    }
    for key in ("scheduled_job_id", "label", "lookback_days", "trigger"):
        if key not in data:
            continue
        val = data[key]
        if val is None or str(val).strip() == "":
            continue
        job_fields.append(
            {
                "label": _job_labels.get(key, key.replace("_", " ").title()),
                "value": _fmt_val(val),
                "badges": None,
            }
        )
    top_dom = data.get("domains")
    if isinstance(top_dom, list) and top_dom:
        job_fields.append(
            {
                "label": "Domains",
                "value": "",
                "badges": [str(x) for x in top_dom],
            }
        )
    elif top_dom is not None and str(top_dom).strip():
        job_fields.append(
            {
                "label": "Domains",
                "value": _fmt_val(top_dom),
                "badges": None,
            }
        )
    if job_fields:
        groups.append({"title": "Scheduled job / export options", "fields": job_fields})

    ev_fields: list[dict[str, Any]] = []
    mid = data.get("matter_id")
    if mid is not None and str(mid).strip():
        ev_fields.append(
            {
                "label": "Saved matter ID",
                "value": str(mid),
                "badges": None,
            }
        )
    if mr:
        ev_fields.append(
            {
                "label": "Matter / reference",
                "value": str(mr),
                "badges": None,
            }
        )
    if ev_fields:
        groups.append({"title": "eDiscovery", "fields": ev_fields})

    dom_count = None
    if isinstance(filters, dict):
        fd = filters.get("domains")
        if isinstance(fd, list) and fd:
            dom_count = len(fd)
    if dom_count is None:
        td = data.get("domains")
        if isinstance(td, list) and td:
            dom_count = len(td)
    if dom_count is not None:
        one_bits.append(f"{dom_count} domains")

    lr = data.get("legacy_redact_checkbox")
    if lr is True or lr == 1 or str(lr).lower() in ("true", "1"):
        groups.append(
            {
                "title": "Options",
                "fields": [
                    {
                        "label": "Legacy redact",
                        "value": "Yes (minimum standard redaction if profile was minimal)",
                        "badges": None,
                    }
                ],
            }
        )

    one_line = " · ".join(one_bits) if one_bits else "Export"
    return {"one_line": one_line, "groups": groups, "error": None}
