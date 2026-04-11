"""Safe cross-links between CRM event plans and Cura (medical_records) operational periods."""
from __future__ import annotations

from typing import Any

from flask import url_for


def parse_cura_operational_event_id(handoff_external_ref: str | None) -> int | None:
    """Parse ``handoff_external_ref`` from live handoff: ``cura_operational_event:{id}``."""
    if not handoff_external_ref:
        return None
    s = str(handoff_external_ref).strip()
    prefix = "cura_operational_event:"
    if not s.startswith(prefix):
        return None
    tail = s[len(prefix) :].strip()
    return int(tail) if tail.isdigit() else None


def safe_cura_ops_event_detail_url(operational_event_id: int) -> str | None:
    try:
        return url_for(
            "medical_records_internal.cura_ops_operational_event_detail",
            event_id=int(operational_event_id),
        )
    except Exception:
        return None


def safe_cura_ops_hub_url() -> str | None:
    try:
        return url_for("medical_records_internal.cura_ops_hub")
    except Exception:
        return None


def operational_event_id_for_cura_url(plan: dict[str, Any] | None) -> int | None:
    """Prefer persisted ``cura_operational_event_id``, then parse ``handoff_external_ref``."""
    if not plan:
        return None
    cid = plan.get("cura_operational_event_id")
    if cid is not None:
        try:
            i = int(cid)
            return i if i > 0 else None
        except (TypeError, ValueError):
            pass
    return parse_cura_operational_event_id(plan.get("handoff_external_ref"))


def cura_ops_event_detail_url_for_plan(plan: dict[str, Any] | None) -> str | None:
    oid = operational_event_id_for_cura_url(plan)
    if oid is None:
        return None
    return safe_cura_ops_event_detail_url(oid)
