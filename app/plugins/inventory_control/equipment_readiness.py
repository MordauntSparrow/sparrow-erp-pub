"""Equipment asset deployment / issue readiness from existing fields (no new DB columns)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional


def _to_date(val: Any) -> Optional[date]:
    if val is None or val == "":
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    s = str(val)[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def build_equipment_asset_readiness(
    *,
    asset: Dict[str, Any],
    consumable_lines: List[Dict[str, Any]],
    today: Optional[date] = None,
) -> Dict[str, Any]:
    td = today or date.today()
    checks: List[Dict[str, Any]] = []
    blockers = 0
    cautions = 0

    def add(level: str, label: str, text: str) -> None:
        nonlocal blockers, cautions
        checks.append({"level": level, "label": label, "text": text})
        if level == "danger":
            blockers += 1
        elif level == "warning":
            cautions += 1

    op = str(asset.get("operational_state") or "operational")
    if op == "unserviceable":
        add("danger", "Operational", "Marked unserviceable.")
    elif op == "restricted":
        add("warning", "Operational", "Restricted use — confirm limits before issue.")
    else:
        add("success", "Operational", "Operational state OK.")

    wd = _to_date(asset.get("warranty_expiry"))
    if wd:
        if wd < td:
            add("warning", "Warranty", f"Manufacturer warranty end date passed ({wd.isoformat()}).")
        elif (wd - td).days <= 30:
            add("warning", "Warranty", f"Warranty ends {wd.isoformat()} ({(wd - td).days} days).")
        else:
            add("success", "Warranty", f"Warranty recorded until {wd.isoformat()}.")

    nd = _to_date(asset.get("next_service_due_date"))
    if nd:
        if nd < td:
            add("danger", "Maintenance", f"Next service due {nd.isoformat()} (overdue).")
        elif (nd - td).days <= 14:
            add("warning", "Maintenance", f"Next service due {nd.isoformat()} ({(nd - td).days} days).")
        else:
            add("success", "Maintenance", f"Next service due {nd.isoformat()}.")

    exp = near = disc = 0
    for c in consumable_lines or []:
        cls = str(c.get("consumable_badge_class") or "")
        if cls == "danger":
            exp += 1
        elif cls == "warning":
            near += 1
        if c.get("has_discrepancy"):
            disc += 1
    if exp:
        add("danger", "Consumables", f"{exp} expired consumable line(s) inside this unit.")
    elif near:
        add("warning", "Consumables", f"{near} consumable line(s) due within 30 days.")
    else:
        add("success", "Consumables", "No expired / due-soon consumable flags on recorded lines.")

    if disc:
        add("warning", "Field reports", f"{disc} line(s) with open field report.")

    if blockers > 0:
        verdict, verdict_class = "not_ready", "danger"
        summary = "Not ready to issue — resolve red items."
    elif cautions > 0:
        verdict, verdict_class = "caution", "warning"
        summary = "Review amber items before issue."
    else:
        verdict, verdict_class = "ready", "success"
        summary = "No blocking readiness issues from this record."

    return {
        "verdict": verdict,
        "verdict_class": verdict_class,
        "summary": summary,
        "blockers": blockers,
        "cautions": cautions,
        "checks": checks,
        "ready": blockers == 0,
    }
