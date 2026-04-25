"""
HR outbound email when core SMTP is configured (EmailManager).

Uses the same SMTP_* environment variables as Core Settings → Email.

Environment:
  HR_MODULE_EMAILS_DISABLED — set to 1/true to skip HR-generated employee emails
    (document request notices, etc.). Manual/CSV welcome still honour
    HR_IMPORT_WELCOME_EMAIL_DISABLED / HR_PORTAL_WELCOME_EMAIL_DISABLED in services.
"""
from __future__ import annotations

import logging
import os
from typing import Optional, Tuple

from app.email_branding import send_branded_email
from app.objects import get_db_connection
from app.public_base import EMPLOYEE_PORTAL_PUBLIC_PATH, resolve_public_base_url
from app.user_notification_preferences import (
    lookup_user_id_for_contractor,
    user_wants_hr_contractor_email,
)

logger = logging.getLogger(__name__)


def _hr_module_emails_disabled() -> bool:
    v = (os.environ.get("HR_MODULE_EMAILS_DISABLED") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _get_email_manager():
    if _hr_module_emails_disabled():
        return None
    try:
        from app.objects import EmailManager

        return EmailManager()
    except Exception as e:
        logger.debug("HR notifications: SMTP not configured: %s", e)
        return None


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
        logger.warning("HR notify: contractor lookup %s: %s", contractor_id, e)
        return None, "there"
    finally:
        cur.close()
        conn.close()


def notify_contractor_document_request_created(
    contractor_id: int, request_id: int, title: str
) -> None:
    """Tell the employee a new HR document request was opened (best-effort)."""
    to_email, name = _contractor_email_name(contractor_id)
    if not to_email:
        return
    uid = lookup_user_id_for_contractor(int(contractor_id))
    if uid and not user_wants_hr_contractor_email(uid, "document_request"):
        return
    em = _get_email_manager()
    if not em:
        return
    base = resolve_public_base_url(extra_env_keys=("HR_PUBLIC_BASE_URL",))
    portal = (
        f"{base}{EMPLOYEE_PORTAL_PUBLIC_PATH}"
        if base
        else f"{EMPLOYEE_PORTAL_PUBLIC_PATH} on this site"
    )
    req_path = f"{base}/hr/request/{int(request_id)}" if base else f"/hr/request/{int(request_id)}"
    ttl = (title or "Document request").strip()[:500]
    subject = f"HR document request: {ttl}"
    body = (
        f"Hello {name},\n\n"
        f"HR has opened a new document request: \"{ttl}\".\n\n"
        f"Sign in to the employee portal ({portal}) and open this request directly:\n"
        f"{req_path}\n\n"
        "Upload any required files from there. If you were not expecting this, contact HR.\n"
    )
    try:
        send_branded_email(em, subject, body, [to_email], preheader=subject)
    except Exception as e:
        logger.warning("HR document request email failed (%s): %s", to_email, e)
