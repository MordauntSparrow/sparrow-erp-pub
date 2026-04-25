"""Guided CRM pipeline (opportunity → quote → plan) for fewer taps on mobile."""
from __future__ import annotations

from urllib.parse import urlencode


def flow_active(request) -> bool:
    v = (request.args.get("flow") or request.form.get("crm_flow_active") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def quote_new_url(*, opportunity_id: int | None = None, flow: bool = False) -> str:
    from flask import url_for

    q: dict[str, str] = {}
    if opportunity_id is not None:
        q["opportunity_id"] = str(int(opportunity_id))
    if flow:
        q["flow"] = "1"
    base = url_for("crm_module.quote_new")
    return f"{base}?{urlencode(q)}" if q else base


def event_plan_new_url(
    *,
    opportunity_id: int | None = None,
    quote_id: int | None = None,
    flow: bool = False,
) -> str:
    from flask import url_for

    q: dict[str, str] = {}
    if quote_id is not None:
        q["quote_id"] = str(int(quote_id))
    if opportunity_id is not None:
        q["opportunity_id"] = str(int(opportunity_id))
    if flow:
        q["flow"] = "1"
    base = url_for("medical_records_internal.cura_event_planner.event_plan_new")
    return f"{base}?{urlencode(q)}" if q else base


def opportunity_edit_url(*, opp_id: int, flow: bool = False) -> str:
    from flask import url_for

    base = url_for("crm_module.opportunity_edit", opp_id=int(opp_id))
    if flow:
        return f"{base}?flow=1"
    return base


def quote_edit_url(*, quote_id: int, flow: bool = False) -> str:
    from flask import url_for

    base = url_for("crm_module.quote_edit", quote_id=int(quote_id))
    if flow:
        return f"{base}?flow=1"
    return base


def event_plan_edit_url(*, plan_id: int, flow: bool = False) -> str:
    from flask import url_for

    base = url_for("medical_records_internal.cura_event_planner.event_plan_edit", plan_id=int(plan_id))
    if flow:
        return f"{base}?flow=1"
    return base
