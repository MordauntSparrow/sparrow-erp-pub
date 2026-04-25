"""
Time Billing outbound email when core SMTP is configured (EmailManager + branded HTML).

Uses the same SMTP_* environment variables as Core Settings → Email, and the same
branded layout as HR (`send_branded_email` from ``app.email_branding``).

Environment:
  TIME_BILLING_MODULE_EMAILS_DISABLED — set to 1/true to skip all TB-generated emails.
  TIME_BILLING_NOTIFY_EMAILS — comma-separated inboxes for **timesheet submitted**
      alerts to admins; if unset, uses ``users`` with role admin/superuser and a non-empty email.
  Public links also honour ``TIME_BILLING_PUBLIC_BASE_URL`` then ``SPARROW_PUBLIC_BASE_URL`` / ``PUBLIC_BASE_URL``.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from app.email_branding import send_branded_email
from app.objects import get_db_connection
from app.public_base import resolve_public_base_url

logger = logging.getLogger(__name__)

_TB_ADMIN_PATH = "/plugin/time_billing_module/timesheets"
_TB_PORTAL_PATH = "/time-billing/"


def _tb_module_emails_disabled() -> bool:
    v = (os.environ.get("TIME_BILLING_MODULE_EMAILS_DISABLED") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _get_email_manager():
    if _tb_module_emails_disabled():
        return None
    try:
        from app.objects import EmailManager

        return EmailManager()
    except Exception as e:
        logger.debug("Time Billing notifications: SMTP not configured: %s", e)
        return None


def _public_base() -> str:
    return resolve_public_base_url(
        extra_env_keys=("TIME_BILLING_PUBLIC_BASE_URL",),
    ).rstrip("/")


def _admin_timesheet_review_url(user_id: int, week_id: str) -> str:
    base = _public_base()
    path = f"{_TB_ADMIN_PATH}/{int(user_id)}/{week_id}"
    return f"{base}{path}" if base else path


def _portal_url() -> str:
    base = _public_base()
    return f"{base}{_TB_PORTAL_PATH}" if base else _TB_PORTAL_PATH


def _admin_recipient_emails() -> List[str]:
    raw = (os.environ.get("TIME_BILLING_NOTIFY_EMAILS") or "").strip()
    if raw:
        return [x.strip().lower() for x in raw.split(",") if x.strip()]
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT email FROM users
            WHERE role IN ('admin', 'superuser') AND email IS NOT NULL AND TRIM(email) <> ''
            """
        )
        rows = cur.fetchall() or []
        out = []
        for r in rows:
            em = (r.get("email") or "").strip().lower()
            if em and "@" in em and em not in out:
                out.append(em)
        return out
    finally:
        cur.close()
        conn.close()


def _contractor_email_name(contractor_id: int) -> Tuple[Optional[str], str]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT email, name FROM tb_contractors WHERE id = %s LIMIT 1",
            (int(contractor_id),),
        )
        row = cur.fetchone() or {}
        em = (row.get("email") or "").strip().lower()
        nm = (row.get("name") or "").strip() or "there"
        return (em if em and "@" in em else None), nm
    except Exception as e:
        logger.warning("Time Billing notify: contractor lookup %s: %s", contractor_id, e)
        return None, "there"
    finally:
        cur.close()
        conn.close()


def _format_date(d: Any) -> str:
    if d is None:
        return ""
    if isinstance(d, datetime):
        return d.strftime("%d/%m/%Y")
    if isinstance(d, date):
        return d.strftime("%d/%m/%Y")
    return str(d)


def _week_ending_display(wk: Dict[str, Any]) -> str:
    return _format_date(wk.get("week_ending"))


def notify_admins_timesheet_submitted(
    contractor_id: int, week_id: str, wk: Optional[Dict[str, Any]] = None
) -> None:
    """Alert admins that a contractor submitted a timesheet week (best-effort)."""
    recipients = _admin_recipient_emails()
    if not recipients:
        return
    em = _get_email_manager()
    if not em:
        return
    to_email, cname = _contractor_email_name(int(contractor_id))
    we = _week_ending_display(wk or {}) if wk else ""
    if not we and wk:
        we = str(wk.get("week_id") or week_id)
    if not we:
        we = str(week_id)
    subject = f"Timesheet submitted – {cname} – week ending {we}"
    review = _admin_timesheet_review_url(int(contractor_id), week_id)
    body = (
        f"A contractor has submitted a timesheet for review.\n\n"
        f"Name: {cname}\n"
        f"Week ending: {we}\n"
        f"Week code: {week_id}\n\n"
        f"Review in Time Billing:\n{review}\n\n"
        "This is an automated message from Time Billing."
    )
    preheader = f"{cname} submitted a timesheet for week ending {we}"
    try:
        send_branded_email(em, subject, body, recipients, preheader=preheader)
    except Exception as e:
        logger.warning("Time Billing timesheet submitted email failed: %s", e)


def notify_contractor_timesheet_approved(contractor_id: int, wk: Dict[str, Any]) -> None:
    """Branded approval notice (PDF remains in portal; same content as before)."""
    to_email, name = _contractor_email_name(int(contractor_id))
    if not to_email:
        return
    em = _get_email_manager()
    if not em:
        return
    we = _week_ending_display(wk)
    subject = f"Timesheet approved – week ending {we}"
    portal = _portal_url()
    body = (
        f"Hello {name},\n\n"
        f"Your timesheet for the week ending {we} has been approved.\n\n"
        f"If you have a current invoice for this week, sign in to the contractor portal ({portal}) "
        "and use Finalize invoice on the timesheet or My invoices when you are ready to mark it paid.\n\n"
        "You can download a PDF copy of this timesheet from the portal for your records.\n\n"
        "Regards,\nAccounts"
    )
    try:
        send_branded_email(em, subject, body, [to_email], preheader=subject)
    except Exception as e:
        logger.warning("Time Billing approval email failed (%s): %s", to_email, e)


def notify_contractor_timesheet_rejected(
    contractor_id: int, wk: Dict[str, Any], reason: str
) -> None:
    """Branded rejection notice with reason."""
    to_email, name = _contractor_email_name(int(contractor_id))
    if not to_email:
        return
    em = _get_email_manager()
    if not em:
        return
    we = _week_ending_display(wk)
    r = (reason or "").strip()
    subject = f"Timesheet needs corrections – week ending {we}"
    portal = _portal_url()
    body = (
        f"Hello {name},\n\n"
        f"We could not approve your timesheet for the week ending {we} "
        f"(and any invoice submitted with it).\n\n"
        f"What to change:\n{r}\n\n"
        f"Please sign in ({portal}), make the corrections, resubmit your timesheet, "
        "and create a new invoice for the week. We will approve or reject both together.\n\n"
        "If anything is unclear, reply to your usual contact.\n\n"
        "Thanks"
    )
    try:
        send_branded_email(em, subject, body, [to_email], preheader=subject)
    except Exception as e:
        logger.warning("Time Billing rejection email failed (%s): %s", to_email, e)


def _assignment_payroll_included(assignment: Dict[str, Any]) -> bool:
    v = assignment.get("payroll_included")
    if v is None:
        return True
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return int(v) != 0
    s = str(v).strip().lower()
    return s not in ("0", "false", "no")


def notify_runsheet_published_to_crew(rs: Dict[str, Any]) -> None:
    """
    Email crew (assignments with payroll included) that a runsheet was published
    and timesheet lines were updated.
    """
    em = _get_email_manager()
    if not em:
        return
    seen: Set[str] = set()
    recipients: List[str] = []
    greet_by_email: Dict[str, str] = {}

    def _add_contractor(cid: int) -> None:
        to_email, nm = _contractor_email_name(int(cid))
        if not to_email or to_email in seen:
            return
        seen.add(to_email)
        recipients.append(to_email)
        greet_by_email[to_email] = nm

    for a in rs.get("assignments") or []:
        if not isinstance(a, dict):
            continue
        uid = a.get("user_id")
        if uid is None or not _assignment_payroll_included(a):
            continue
        _add_contractor(int(uid))

    lead = rs.get("lead_user_id")
    if lead is not None:
        _add_contractor(int(lead))

    if not recipients:
        return

    client = (rs.get("client_name") or "").strip() or "Client"
    wd = _format_date(rs.get("work_date"))
    rs_id = int(rs.get("id") or 0)
    portal = _portal_url()
    subject = f"Runsheet published – {client} – {wd}"
    preheader = subject
    for to_email in recipients:
        name = greet_by_email.get(to_email) or "there"
        body = (
            f"Hello {name},\n\n"
            f"A runsheet for {client} on {wd} has been published. "
            "Your timesheet has been updated with the hours from this sheet.\n\n"
            f"Open the contractor portal to review: {portal}\n\n"
            f"Runsheet reference: #{rs_id}\n\n"
            "This is an automated message from Time Billing."
        )
        try:
            send_branded_email(em, subject, body, [to_email], preheader=preheader)
        except Exception as e:
            logger.warning("Time Billing runsheet published email failed (%s): %s", to_email, e)
