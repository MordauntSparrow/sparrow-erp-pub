"""
CRM event planner → staffing snapshot for Cura config and Scheduling planning.

Staffing mix comes from the linked opportunity's ``lead_meta_json`` (event risk calculator),
via ``enrich_lead_meta_with_staffing_breakdown`` — same source as the planner / quote UX.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from .crm_event_risk import enrich_lead_meta_with_staffing_breakdown


def _parse_json_maybe(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        try:
            v = json.loads(s)
            return dict(v) if isinstance(v, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _row_value(row: Any, key: str, index: int = 0) -> Any:
    if row is None:
        return None
    if isinstance(row, Mapping):
        return row.get(key)
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes)):
        if 0 <= index < len(row):
            return row[index]
    return None


def staffing_snapshot_from_lead_meta(lead_meta: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    Build JSON-serialisable ``crm_recommended_staffing`` payload from opportunity lead_meta.
    Returns None if there is no usable risk / staffing data.
    """
    if not lead_meta or not isinstance(lead_meta, dict):
        return None
    enriched = enrich_lead_meta_with_staffing_breakdown(dict(lead_meta))
    risk = enriched.get("risk") or {}
    if not isinstance(risk, dict):
        return None
    sb = risk.get("staffing_breakdown")
    if not isinstance(sb, dict):
        return None
    sm = int(risk.get("suggested_medics") or 0)
    sv = int(risk.get("suggested_vehicles") or 0)
    roles = sb.get("clinical_roles") or []
    vp = sb.get("vehicle_package")
    has_roles = bool(isinstance(roles, list) and len(roles) > 0)
    try:
        veh_n = int((vp or {}).get("vehicles") or 0) if isinstance(vp, dict) else 0
    except (TypeError, ValueError):
        veh_n = 0
    has_veh = bool(isinstance(vp, dict) and veh_n > 0)
    if sm <= 0 and sv <= 0 and not has_roles and not has_veh:
        return None
    return {
        "version": 1,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "staffing_breakdown": sb,
        "risk_hints": {
            "suggested_medics": sm,
            "suggested_vehicles": sv,
        },
    }


def staffing_snapshot_for_plan_row(cur, plan_row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Load opportunity lead_meta for ``plan_row['opportunity_id']`` and build snapshot."""
    oid = plan_row.get("opportunity_id")
    if oid is None:
        return None
    try:
        oid_i = int(oid)
    except (TypeError, ValueError):
        return None
    cur.execute(
        "SELECT lead_meta_json FROM crm_opportunities WHERE id = %s LIMIT 1",
        (oid_i,),
    )
    row = cur.fetchone()
    raw = _row_value(row, "lead_meta_json", 0)
    lm = _parse_json_maybe(raw)
    return staffing_snapshot_from_lead_meta(lm)


def staffing_snapshot_for_crm_plan_id(cur, plan_id: int) -> dict[str, Any] | None:
    """Resolve plan → opportunity lead_meta in one query (for Cura events without a fresh handoff)."""
    cur.execute(
        """
        SELECT o.lead_meta_json
        FROM crm_event_plans p
        LEFT JOIN crm_opportunities o ON o.id = p.opportunity_id
        WHERE p.id = %s
        LIMIT 1
        """,
        (int(plan_id),),
    )
    row = cur.fetchone()
    raw = _row_value(row, "lead_meta_json", 0)
    lm = _parse_json_maybe(raw)
    return staffing_snapshot_from_lead_meta(lm)
