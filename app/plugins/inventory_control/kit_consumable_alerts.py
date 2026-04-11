"""
Email alerts for dated consumables recorded on serial equipment (kit bags, AEDs, etc.).

Best-practice defaults:
  - Opt out with INVENTORY_KIT_CONSUMABLE_ALERTS_DISABLED=1
  - Near window: INVENTORY_KIT_CONSUMABLE_ALERT_NEAR_DAYS (default 30)
  - Recipients: INVENTORY_KIT_CONSUMABLE_ALERT_EMAILS (comma-separated), else same as
    fleet reminders (FLEET_ASSET_REMINDER_EMAILS), else admin/superuser emails

Scheduled use:
  - ``flask inventory-kit-consumable-alerts`` (daily cron), or
  - Included automatically in ``flask fleet-asset-reminders`` unless
    FLEET_REMINDER_SKIP_KIT_CONSUMABLES=1
"""

from __future__ import annotations

import os
from typing import List, Optional

from app.objects import get_db_connection


def _truthy_env(name: str) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _alerts_disabled() -> bool:
    return _truthy_env("INVENTORY_KIT_CONSUMABLE_ALERTS_DISABLED")


def _near_days() -> int:
    try:
        return max(int(os.environ.get("INVENTORY_KIT_CONSUMABLE_ALERT_NEAR_DAYS", "30")), 1)
    except (TypeError, ValueError):
        return 30


def recipient_emails() -> List[str]:
    raw = (os.environ.get("INVENTORY_KIT_CONSUMABLE_ALERT_EMAILS") or "").strip()
    if raw:
        return [x.strip() for x in raw.split(",") if x.strip()]
    raw_fleet = (os.environ.get("FLEET_ASSET_REMINDER_EMAILS") or "").strip()
    if raw_fleet:
        return [x.strip() for x in raw_fleet.split(",") if x.strip()]
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT email FROM users WHERE role IN ('admin','superuser') AND email IS NOT NULL"
        )
        rows = cur.fetchall() or []
        return [r["email"] for r in rows if r.get("email")]
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def build_kit_consumable_alert_lines(
    near_days: Optional[int] = None,
    *,
    detail_limit: int = 200,
) -> List[str]:
    """Plain-text lines for batch emails (empty when disabled or nothing due)."""
    if _alerts_disabled():
        return []
    nd = int(near_days) if near_days is not None else _near_days()
    from app.plugins.inventory_control.objects import get_inventory_service

    inv = get_inventory_service()
    counts = inv.count_kit_consumable_expiry_alerts(nd)
    rows = inv.list_kit_consumable_expiry_alert_rows(near_days=nd, limit=detail_limit)
    if not counts.get("expired") and not counts.get("near"):
        return []

    lines: List[str] = []
    lines.append(
        f"[Kit consumables] {counts.get('expired', 0)} expired line(s), "
        f"{counts.get('near', 0)} due within {nd} day(s) (on serial assets)."
    )
    for r in rows:
        tag = "EXPIRED" if r.get("alert_tier") == "expired" else "DUE"
        asset = (r.get("public_asset_code") or r.get("serial_number") or "?").strip()
        model = (r.get("equipment_model_name") or "").strip()
        label = (r.get("label") or "").strip()
        exp = r.get("expiry_date") or ""
        dl = r.get("days_left")
        bits = [f"[{tag}]", asset]
        if model:
            bits.append(f"({model})")
        bits.append("—")
        bits.append(label or "item")
        bits.append(f"exp {exp}")
        if dl is not None:
            if dl < 0:
                bits.append(f"({-dl}d ago)")
            else:
                bits.append(f"({dl}d left)")
        lot = (r.get("lot_number") or "").strip()
        bat = (r.get("batch_number") or "").strip()
        if bat or lot:
            bits.append(f"batch/lot {bat or '—'}/{lot or '—'}")
        lines.append(" ".join(str(b) for b in bits if b != ""))
    return lines


def run_kit_consumable_alerts(send_email: bool = True) -> dict:
    """
    Build lines and optionally email. Returns summary for CLI / callers.
    """
    summary: dict = {
        "lines": 0,
        "recipients": [],
        "sent": False,
        "skipped": False,
    }
    if _alerts_disabled():
        summary["skipped"] = True
        summary["message"] = "Kit consumable alerts disabled (INVENTORY_KIT_CONSUMABLE_ALERTS_DISABLED)."
        return summary

    body_lines = build_kit_consumable_alert_lines()
    summary["lines"] = len(body_lines)
    if not body_lines:
        summary["message"] = "No kit consumable expiry alerts in the current window."
        return summary

    text = (
        "Sparrow ERP — Kit consumable expiry alerts\n"
        "(dated lines inside serial equipment — open each asset to update)\n\n"
        + "\n".join(body_lines)
    )
    summary["message"] = text[:2000]

    if not send_email:
        return summary

    recipients = recipient_emails()
    summary["recipients"] = recipients
    if not recipients:
        summary["message"] += (
            "\n(No recipients — set INVENTORY_KIT_CONSUMABLE_ALERT_EMAILS or FLEET_ASSET_"
            "REMINDER_EMAILS or add admin user emails.)"
        )
        return summary

    try:
        from app.objects import EmailManager

        em = EmailManager()
        em.send_email(
            "Sparrow — kit consumable expiry alerts",
            text,
            recipients,
        )
        summary["sent"] = True
    except Exception as e:
        summary["error"] = str(e)
    return summary
