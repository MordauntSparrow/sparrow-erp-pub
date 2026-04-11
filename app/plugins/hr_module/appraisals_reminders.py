"""
Daily appraisal due / overdue scan, digest email, and scheduler integration.

Disable entirely: HR_APPRAISAL_REMINDERS_DISABLED=1 (or true/yes/on).
Recipients: HR_APPRAISAL_REMINDER_EMAILS (comma-separated), else admin/superuser emails.
Reminder windows: manifest ``appraisal_reminder_days`` (default 30,14,7,1,0).
Digest email: manifest ``appraisal_digest_email_enabled`` (default on); still needs SMTP env.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from app.objects import EmailManager, get_db_connection

from .appraisals_service import (
    appraisal_event_log_ready,
    appraisal_tables_ready,
    list_appraisals_needing_attention,
    log_appraisal_event,
)

_logger = logging.getLogger(__name__)
_HR_MANIFEST_PATH = Path(__file__).resolve().parent / "manifest.json"


def is_appraisal_reminders_disabled() -> bool:
    v = (os.environ.get("HR_APPRAISAL_REMINDERS_DISABLED") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def get_reminder_days_from_manifest() -> List[int]:
    default = [30, 14, 7, 1, 0]
    try:
        with open(_HR_MANIFEST_PATH, encoding="utf-8") as f:
            m = json.load(f)
        raw = (m.get("settings") or {}).get("appraisal_reminder_days") or {}
        s = str(raw.get("value") if isinstance(raw, dict) else raw or "").strip()
        if not s:
            return default
        out: List[int] = []
        for part in s.split(","):
            p = part.strip()
            if p.lstrip("-").isdigit():
                out.append(int(p))
        return sorted(set(out), reverse=True) or default
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return default


def is_appraisal_digest_email_enabled() -> bool:
    try:
        with open(_HR_MANIFEST_PATH, encoding="utf-8") as f:
            m = json.load(f)
        raw = (m.get("settings") or {}).get("appraisal_digest_email_enabled") or {}
        v = str(raw.get("value") if isinstance(raw, dict) else "1").strip().lower()
        return v not in ("0", "false", "no", "off")
    except (OSError, json.JSONDecodeError):
        return True


def _digest_recipient_emails() -> List[str]:
    raw = (os.environ.get("HR_APPRAISAL_REMINDER_EMAILS") or "").strip()
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


def _daily_scan_already_ran_today() -> bool:
    if not appraisal_event_log_ready():
        return True
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id FROM hr_appraisal_event_log
            WHERE event_type = 'daily_appraisal_scan' AND DATE(created_at) = CURDATE()
            LIMIT 1
            """
        )
        return bool(cur.fetchone())
    finally:
        cur.close()
        conn.close()


def run_appraisal_daily_reminder_scan(
    send_email: bool = True, force: bool = False
) -> Dict[str, Any]:
    """
    One run per calendar day (MySQL CURDATE) unless ``force=True``.
    Logs ``daily_appraisal_scan`` and per-item ``reminder_item`` rows; sends one digest
    email when enabled and SMTP works.
    """
    out: Dict[str, Any] = {
        "ok": True,
        "items": 0,
        "email_sent": False,
        "skip": None,
    }
    if not appraisal_tables_ready() or not appraisal_event_log_ready():
        out["ok"] = False
        out["skip"] = "tables"
        return out
    if is_appraisal_reminders_disabled():
        out["skip"] = "disabled_env"
        return out
    if not force and _daily_scan_already_ran_today():
        out["skip"] = "already_ran_today"
        return out

    days = get_reminder_days_from_manifest()
    items = list_appraisals_needing_attention(reminder_days=days)
    lines = [it["line"] for it in items]
    body_lines = lines if lines else ["No appraisal deadlines require attention today."]
    detail = "\n".join(body_lines)
    if len(detail) > 60000:
        detail = detail[:60000] + "\n…(truncated)"

    log_appraisal_event(
        "daily_appraisal_scan",
        "Daily appraisal reminder scan (%s item(s))" % len(items),
        appraisal_id=None,
        contractor_id=None,
        detail=detail,
        channel="scheduler",
    )
    out["items"] = len(items)

    for it in items:
        log_appraisal_event(
            "reminder_item",
            (it.get("line") or "")[:512],
            appraisal_id=int(it["appraisal_id"]),
            contractor_id=int(it["contractor_id"]),
            detail=it.get("bucket"),
            channel="scheduler",
        )

    if not send_email or not items:
        return out
    if not is_appraisal_digest_email_enabled():
        return out

    recipients = _digest_recipient_emails()
    if not recipients:
        log_appraisal_event(
            "digest_email_failed",
            "No recipients configured for appraisal digest",
            appraisal_id=None,
            contractor_id=None,
            channel="scheduler",
        )
        return out

    subject = "[Sparrow HR] Appraisal reminders — %s" % date.today().isoformat()
    text = (
        "The following appraisals need attention (open the HR module in Sparrow for detail):\n\n"
        + "\n".join(lines)
        + "\n\n— Sparrow HR (automated)\n"
    )
    try:
        EmailManager().send_email(subject, text, recipients)
        out["email_sent"] = True
        log_appraisal_event(
            "digest_email_sent",
            "Digest sent to %s recipient(s)" % len(recipients),
            appraisal_id=None,
            contractor_id=None,
            detail=",".join(recipients[:20]),
            channel="email",
        )
    except Exception as exc:
        _logger.warning("Appraisal digest email failed: %s", exc)
        log_appraisal_event(
            "digest_email_failed",
            str(exc)[:500],
            appraisal_id=None,
            contractor_id=None,
            channel="scheduler",
        )
        out["ok"] = False
    return out


def run_appraisal_scheduler_tick() -> None:
    """Called hourly from APScheduler; only one effective scan per day."""
    run_appraisal_daily_reminder_scan(send_email=True)
