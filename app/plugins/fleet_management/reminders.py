"""
Batch email reminders for fleet compliance and asset maintenance (30/14/7 day windows).

Configure SMTP (SMTP_HOST, etc.). Recipients: FLEET_ASSET_REMINDER_EMAILS (comma-separated)
or falls back to all admin user emails from the database.

Kit consumables (dated lines inside serial equipment) are appended to the same email by
default. Set FLEET_REMINDER_SKIP_KIT_CONSUMABLES=1 to omit them. Tune windows via
INVENTORY_KIT_CONSUMABLE_ALERT_NEAR_DAYS and opt out entirely with
INVENTORY_KIT_CONSUMABLE_ALERTS_DISABLED=1 (see inventory_control.kit_consumable_alerts).
"""

from __future__ import annotations

import os
from datetime import date
from typing import List

from app.objects import get_db_connection


def _recipient_emails() -> List[str]:
    raw = (os.environ.get("FLEET_ASSET_REMINDER_EMAILS") or "").strip()
    if raw:
        return [x.strip() for x in raw.split(",") if x.strip()]
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


def _parse_date(val):
    if not val:
        return None
    if hasattr(val, "date"):
        return val.date() if hasattr(val, "date") else val
    try:
        from datetime import datetime as _dt

        return _dt.strptime(str(val)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _lines_for_fleet_windows() -> List[str]:
    from .objects import get_fleet_service

    svc = get_fleet_service()
    today = date.today()
    lines: List[str] = []
    rows = svc.compliance_due_list(days=30)
    for r in rows:
        for label, col in (
            ("MOT", "mot_expiry"),
            ("Tax", "tax_expiry"),
            ("Insurance", "insurance_expiry"),
        ):
            d = _parse_date(r.get(col))
            if not d:
                continue
            days_left = (d - today).days
            vn = (r.get("registration") or r.get("internal_code") or f"#{r.get('id')}")
            if d < today:
                lines.append(
                    f"[Fleet OVERDUE] {vn} — {label} was {d.isoformat()}"
                )
            elif days_left <= 30:
                lines.append(
                    f"[Fleet due in {days_left}d] {vn} — {label} {d.isoformat()}"
                )
    return lines


def _lines_for_fleet_servicing_safety() -> List[str]:
    from .objects import get_fleet_service

    svc = get_fleet_service()
    lines: List[str] = []
    for r in svc.servicing_due_rows(days=30):
        reg = r.get("registration") or r.get("internal_code")
        note = (r.get("servicing_mileage_note") or "").strip()
        nd = r.get("next_service_due_date")
        nm = r.get("next_service_due_mileage")
        bits = []
        if nd:
            bits.append(f"date {nd}")
        if nm is not None:
            bits.append(f"mileage target {nm}")
        if note:
            bits.append(note)
        if bits:
            lines.append(f"[Fleet servicing] {reg} — " + "; ".join(bits))
    for r in svc.safety_due_rows(days=30):
        reg = r.get("registration") or r.get("internal_code")
        due = r.get("safety_due_by")
        dr = r.get("safety_days_remaining")
        if dr is not None and dr < 0:
            lines.append(
                f"[Fleet safety OVERDUE] {reg} — was due {due} ({-dr}d ago)"
            )
        elif dr is not None and dr <= 14:
            lines.append(f"[Fleet safety due in {dr}d] {reg} — by {due}")
    return lines


def _lines_for_asset_maintenance() -> List[str]:
    from app.plugins.inventory_control.asset_service import get_asset_service

    svc = get_asset_service()
    rows = svc.list_maintenance_due(within_days=30)
    lines = []
    for r in rows:
        tag = "OVERDUE" if r.get("overdue") else "Due"
        lines.append(
            f"[Asset {tag}] {r.get('public_asset_code') or r.get('serial_number')} "
            f"({r.get('item_name')}) next due {r.get('next_due_date')}"
        )
    return lines


def run_reminders(send_email: bool = True) -> dict:
    """
    Build reminder body; optionally send via EmailManager when send_email=True.
    Returns summary dict for CLI output.
    """
    compliance_sync = {}
    try:
        from .objects import get_fleet_service

        compliance_sync = get_fleet_service().sync_compliance_vehicle_status()
    except Exception:
        pass
    body_lines = (
        _lines_for_fleet_windows()
        + _lines_for_fleet_servicing_safety()
        + _lines_for_asset_maintenance()
    )
    if (os.environ.get("FLEET_REMINDER_SKIP_KIT_CONSUMABLES") or "").strip().lower() not in (
        "1",
        "true",
        "yes",
        "on",
    ):
        try:
            from app.plugins.inventory_control.kit_consumable_alerts import (
                build_kit_consumable_alert_lines,
            )

            body_lines.extend(build_kit_consumable_alert_lines())
        except Exception:
            pass
    summary = {
        "lines": len(body_lines),
        "recipients": [],
        "sent": False,
        "compliance_sync": compliance_sync,
    }
    if not body_lines:
        summary["message"] = "No reminders in current windows."
        return summary

    text = (
        "Sparrow ERP — Fleet, asset & kit consumable reminders\n\n"
        + "\n".join(body_lines)
    )
    summary["message"] = text[:2000]

    if not send_email:
        return summary

    recipients = _recipient_emails()
    summary["recipients"] = recipients
    if not recipients:
        summary["message"] += "\n(No recipients — set FLEET_ASSET_REMINDER_EMAILS or add admin users.)"
        return summary

    try:
        from app.objects import EmailManager

        em = EmailManager()
        em.send_email(
            "Fleet, asset & kit consumable reminders",
            text,
            recipients,
        )
        summary["sent"] = True
    except Exception as e:
        summary["error"] = str(e)
    return summary
