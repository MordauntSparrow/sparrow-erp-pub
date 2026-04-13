"""Vehicle deployment readiness summary from existing fleet + inventory fields (no extra DB columns)."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
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


def _aggregate_kit_consumable_alerts(alerts_map: Dict[int, Any]) -> Dict[str, int]:
    expired = near = disc = 0
    for _aid, ca in (alerts_map or {}).items():
        if not isinstance(ca, dict):
            continue
        expired += int(ca.get("expired") or 0)
        near += int(ca.get("near") or 0)
        disc += int(ca.get("discrepancy") or 0)
    return {"expired": expired, "near": near, "discrepancy": disc}


def _required_skus_from_vehicle_type_schema(schema: Any) -> List[str]:
    if not isinstance(schema, dict):
        return []
    raw = schema.get("required_equipment_skus")
    if raw is None:
        return []
    if isinstance(raw, str) and raw.strip():
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    for x in raw:
        s = str(x).strip()
        if s:
            out.append(s)
    return out


def build_vehicle_deployment_readiness(
    *,
    vehicle: Dict[str, Any],
    equipment_rows: List[Dict[str, Any]],
    equipment_consumable_alerts: Dict[int, Any],
    mileage_last_known_mi: Optional[int],
    safety_logs: List[Dict[str, Any]],
    safety_interval_days: int,
    vehicle_type_safety_schema: Optional[Dict[str, Any]] = None,
    today: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Produce a single manager-facing readiness view.

    Optional vehicle type JSON (safety_schema root) key ``required_equipment_skus``:
    list of inventory item SKU strings that must appear on the vehicle's kit list.
    """
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

    status = str(vehicle.get("status") or "")
    off_reason = str(vehicle.get("off_road_reason") or "")

    if status == "decommissioned":
        add("secondary", "Fleet", "Vehicle is decommissioned — not a deployment target.")
    elif status == "off_road":
        add(
            "danger",
            "Fleet",
            f"Off road{f' ({off_reason})' if off_reason else ''}.",
        )
    elif status == "maintenance":
        add("warning", "Fleet", "In maintenance — confirm before operational use.")

    for title, key in (
        ("MOT", "mot_expiry"),
        ("Tax", "tax_expiry"),
        ("Insurance", "insurance_expiry"),
    ):
        d = _to_date(vehicle.get(key))
        if d is None:
            add("warning", title, f"{title} date not recorded — confirm paperwork.")
            continue
        if d < td:
            add("danger", title, f"{title} expired ({d.isoformat()}).")
        elif (d - td).days <= 30:
            add("warning", title, f"{title} due by {d.isoformat()} ({(d - td).days} days).")
        else:
            add("success", title, f"{title} OK until {d.isoformat()}.")

    interval = max(int(safety_interval_days or 42), 1)
    last_sc = None
    if safety_logs:
        last_sc = safety_logs[0].get("performed_at")
    anchor = last_sc or vehicle.get("created_at")
    ad: Optional[date] = None
    if anchor:
        ad = _to_date(anchor)
        if ad is None and hasattr(anchor, "date"):
            try:
                ad = anchor.date()  # type: ignore[union-attr]
            except Exception:
                ad = None
    if ad:
        due_by = ad + timedelta(days=interval)
        if due_by < td:
            add(
                "danger",
                "Safety check",
                f"Workshop safety check overdue (was due {due_by.isoformat()}).",
            )
        elif (due_by - td).days <= 14:
            add(
                "warning",
                "Safety check",
                f"Safety check due by {due_by.isoformat()} ({(due_by - td).days} days).",
            )
        else:
            add(
                "success",
                "Safety check",
                f"Next safety check by {due_by.isoformat()}.",
            )
    else:
        add("warning", "Safety check", "No safety check history — schedule first check.")

    nd = _to_date(vehicle.get("next_service_due_date"))
    if nd:
        if nd < td:
            add("danger", "Service", f"Planned service date was {nd.isoformat()} (overdue).")
        elif (nd - td).days <= 14:
            add(
                "warning",
                "Service",
                f"Planned service {nd.isoformat()} ({(nd - td).days} days).",
            )
        else:
            add("success", "Service", f"Planned service {nd.isoformat()}.")

    nm = vehicle.get("next_service_due_mileage")
    if nm is not None and mileage_last_known_mi is not None:
        try:
            rem = int(nm) - int(mileage_last_known_mi)
            if rem <= 0:
                add(
                    "danger",
                    "Service",
                    f"Service mileage target {nm} mi reached or passed "
                    f"(last known {mileage_last_known_mi} mi).",
                )
            elif rem <= 750:
                add(
                    "warning",
                    "Service",
                    f"Within {rem} mi of service target ({nm} mi).",
                )
        except (TypeError, ValueError):
            pass

    kit_agg = _aggregate_kit_consumable_alerts(equipment_consumable_alerts)
    if kit_agg["expired"] > 0:
        add(
            "danger",
            "Kit consumables",
            f"Expired dated items inside on-board kit ({kit_agg['expired']} line(s)). Open each asset for detail.",
        )
    elif kit_agg["near"] > 0:
        add(
            "warning",
            "Kit consumables",
            f"Consumables due within 30 days on kit ({kit_agg['near']} line(s)).",
        )
    else:
        add(
            "success",
            "Kit consumables",
            "No expired / due-soon consumable flags on serial kit (by batch/expiry records).",
        )

    if kit_agg["discrepancy"] > 0:
        add(
            "warning",
            "Field reports",
            f"Open field report(s) on kit consumables ({kit_agg['discrepancy']}).",
        )

    on_board_skus = {
        str(r.get("sku") or "").strip().upper()
        for r in (equipment_rows or [])
        if str(r.get("sku") or "").strip()
    }
    req = _required_skus_from_vehicle_type_schema(vehicle_type_safety_schema or {})
    missing_req: List[str] = []
    for sku in req:
        if sku.strip().upper() not in on_board_skus:
            missing_req.append(sku)
    if req:
        if missing_req:
            add(
                "danger",
                "Mandatory kit",
                "Missing required model SKU(s) on this vehicle: "
                + ", ".join(missing_req)
                + ". Configure under Fleet → Vehicle types → safety schema JSON key "
                "`required_equipment_skus`.",
            )
        else:
            add("success", "Mandatory kit", "Required model SKU(s) present on kit list.")

    if status in ("active", "pending_road_test") and not equipment_rows:
        add(
            "warning",
            "Serial kit",
            "No serialised kit assigned to this vehicle — confirm load-out before deployment.",
        )
    elif equipment_rows:
        add(
            "success",
            "Serial kit",
            f"{len(equipment_rows)} serial asset(s) on this vehicle.",
        )

    if blockers > 0:
        verdict, verdict_class = "not_ready", "danger"
        summary = "Not deployment-ready — resolve red items before release."
    elif cautions > 0:
        verdict, verdict_class = "caution", "warning"
        summary = "Deploy with caution — review amber items."
    else:
        verdict, verdict_class = "ready", "success"
        summary = "No blocking issues flagged from compliance, safety, service, and kit data."

    return {
        "verdict": verdict,
        "verdict_class": verdict_class,
        "summary": summary,
        "blockers": blockers,
        "cautions": cautions,
        "checks": checks,
        "ready": blockers == 0,
    }
