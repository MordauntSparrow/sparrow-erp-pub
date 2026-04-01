"""
Recruitment notifications: applicant email (core SMTP / EmailManager), internal staff email,
and optional Socket.IO panel events for staff who have recruitment module access.

Internal alerts go to:
  - Users whose `users.permissions` JSON list includes any recruitment permission declared for the
    module (module access, read, manage_setup, manage_applications, hire — see plugin manifest).
  - Role `superuser` is included by default (set RECRUITMENT_NOTIFY_INCLUDE_SUPERUSERS=0 to skip).
  - Role `admin` alone does NOT grant internal recruitment alerts (avoids e.g. dispatch admins).
  - Optional extra addresses: RECRUITMENT_NOTIFY_ADMIN_EMAILS (comma-separated).

Environment:
  RECRUITMENT_EMAILS_DISABLED   — set to 1/true to skip all recruitment emails
  RECRUITMENT_NOTIFY_ADMIN_EMAILS — optional extra internal recipients (always merged in)
  RECRUITMENT_NOTIFY_INCLUDE_SUPERUSERS — default true; set 0/false to exclude superuser role from internal alerts
  RECRUITMENT_PUBLIC_BASE_URL   — site origin for links in emails (e.g. https://hr.example.com)
  RAILWAY_PUBLIC_DOMAIN       — optional; Railway hostname (https:// added if no scheme); used if no explicit base URL
  Applicant-facing links use {origin}/vacancies; post-hire employee portal hint uses {origin}/employee-portal.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Set, Tuple

from app.objects import get_db_connection
from app.public_base import (
    EMPLOYEE_PORTAL_PUBLIC_PATH,
    RECRUITMENT_VACANCIES_PATH,
    resolve_public_base_url,
)

logger = logging.getLogger(__name__)

_STAGE_LABELS = {
    "applied": "Applied",
    "screening": "Screening",
    "interview": "Interview",
    "offer": "Offer",
    "hired": "Hired",
    "rejected": "Rejected",
}


def _emails_globally_disabled() -> bool:
    v = (os.environ.get("RECRUITMENT_EMAILS_DISABLED") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _public_base_url() -> str:
    return resolve_public_base_url(extra_env_keys=("RECRUITMENT_PUBLIC_BASE_URL",))


def _applicant_portal_hint() -> str:
    base = _public_base_url()
    if base:
        return f"{base}{RECRUITMENT_VACANCIES_PATH}"
    return "the careers / vacancies page on this site (sign in to your applicant account)."


def _plugins_base_dir() -> str:
    """…/app/plugins (parent of the recruitment_module package)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _recruitment_access_permission_id() -> str:
    """Same ID as plugin dashboard /plugin/recruitment_module access check."""
    try:
        from app.objects import PluginManager
        from app.permissions_registry import plugin_access_permission_id

        pm = PluginManager(_plugins_base_dir())
        manifest = (pm.load_plugins() or {}).get("recruitment_module") or {}
        return plugin_access_permission_id(manifest, "recruitment_module")
    except Exception as e:
        logger.debug("recruitment access permission id fallback: %s", e)
        return "recruitment_module.access"


def _recruitment_internal_notify_permission_ids() -> Set[str]:
    """Any of these on a user qualifies them for internal email + Socket.IO recruitment_event."""
    ids = {
        _recruitment_access_permission_id(),
        "recruitment_module.read",
        "recruitment_module.manage_setup",
        "recruitment_module.manage_applications",
        "recruitment_module.hire",
    }
    return {x for x in ids if x}


def _parse_user_permissions_json(raw: Any) -> List[str]:
    if raw is None or raw == "":
        return []
    try:
        if isinstance(raw, (list, tuple)):
            return [str(x).strip() for x in raw if str(x).strip()]
        data = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except (TypeError, ValueError, json.JSONDecodeError):
        pass
    return []


def _include_superusers_for_internal_notify() -> bool:
    v = (os.environ.get("RECRUITMENT_NOTIFY_INCLUDE_SUPERUSERS") or "true").strip().lower()
    return v not in ("0", "false", "no", "off")


def _env_extra_notify_emails() -> List[str]:
    raw = (os.environ.get("RECRUITMENT_NOTIFY_ADMIN_EMAILS") or "").strip()
    if not raw:
        return []
    return [x.strip().lower() for x in raw.split(",") if x.strip() and "@" in x.strip()]


def list_recruitment_internal_notify_targets() -> Tuple[List[str], List[str]]:
    """
    Returns (emails, user_ids) for staff who should receive internal recruitment alerts
    and Socket.IO recruitment_event fanout. Deduplicated emails (lowercase).
    """
    notify_perm_ids = _recruitment_internal_notify_permission_ids()
    emails: Set[str] = set()
    user_ids: List[str] = []
    seen_id: Set[str] = set()
    include_su = _include_superusers_for_internal_notify()

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, email, role, permissions
            FROM users
            """
        )
        rows = cur.fetchall() or []
    except Exception as e:
        logger.warning("recruitment notify user query failed: %s", e)
        rows = []
    finally:
        cur.close()
        conn.close()

    for row in rows:
        uid = row.get("id")
        sid = str(uid) if uid is not None else ""
        role = str(row.get("role") or "").strip().lower()
        perms = _parse_user_permissions_json(row.get("permissions"))
        pset = set(perms)
        match = bool(pset & notify_perm_ids)
        if include_su and role == "superuser":
            match = True
        if not match:
            continue
        if sid and sid not in seen_id:
            seen_id.add(sid)
            user_ids.append(sid)
        em = (row.get("email") or "").strip().lower()
        if em and "@" in em:
            emails.add(em)

    for extra in _env_extra_notify_emails():
        emails.add(extra)

    return sorted(emails), user_ids


def _get_email_manager():
    if _emails_globally_disabled():
        return None
    try:
        from app.objects import EmailManager

        return EmailManager()
    except Exception as e:
        logger.debug("Recruitment: SMTP not configured or EmailManager failed: %s", e)
        return None


def _fetch_application_row(application_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT a.*, o.title AS opening_title, o.slug AS opening_slug,
                   ap.email AS applicant_email, ap.name AS applicant_name
            FROM rec_applications a
            JOIN rec_openings o ON o.id = a.opening_id
            JOIN rec_applicants ap ON ap.id = a.applicant_id
            WHERE a.id = %s
            LIMIT 1
            """,
            (application_id,),
        )
        return cur.fetchone()
    except Exception as e:
        logger.warning("recruitment notify fetch app %s: %s", application_id, e)
        return None
    finally:
        cur.close()
        conn.close()


def _send_applicant_email(to_email: str, subject: str, body: str) -> None:
    if not to_email or "@" not in to_email:
        return
    em = _get_email_manager()
    if not em:
        return
    try:
        em.send_email(subject=subject, body=body, recipients=[to_email.strip().lower()])
    except Exception as e:
        logger.warning("Recruitment: applicant email failed (%s): %s", to_email, e)


def _send_internal_email(subject: str, body: str) -> None:
    recipients, _ = list_recruitment_internal_notify_targets()
    if not recipients:
        return
    em = _get_email_manager()
    if not em:
        return
    try:
        em.send_email(subject=subject, body=body, recipients=recipients)
    except Exception as e:
        logger.warning("Recruitment: internal email failed: %s", e)


def emit_recruitment_panel_event(event_type: str, payload: Dict[str, Any]) -> None:
    """Best-effort realtime hint (room panel_user_<users.id>). Same audience as internal email."""
    try:
        from app import socketio
    except Exception:
        return
    body = {"type": str(event_type), **(payload or {})}
    _, panel_ids = list_recruitment_internal_notify_targets()
    for uid in panel_ids:
        try:
            socketio.emit("recruitment_event", body, room=f"panel_user_{uid}")
        except Exception as e:
            logger.debug("recruitment socket emit skip: %s", e)


def notify_applicant_stage_change(application_id: int, new_stage: str) -> None:
    row = _fetch_application_row(application_id)
    if not row:
        return
    email = (row.get("applicant_email") or "").strip()
    name = (row.get("applicant_name") or "there").strip()
    opening = (row.get("opening_title") or "your application").strip()
    label = _STAGE_LABELS.get((new_stage or "").lower(), new_stage)
    subj = f"Application update: {opening}"
    body = (
        f"Hello {name},\n\n"
        f"Your application for \"{opening}\" has moved to stage: {label}.\n\n"
        f"Sign in via {_applicant_portal_hint()} to view details and any tasks.\n\n"
        f"(Application reference #{application_id})\n"
    )
    _send_applicant_email(email, subj, body)
    _send_internal_email(
        f"[Recruitment] Stage → {label}: {opening}",
        f"Application #{application_id} ({name}) is now in stage: {label}.\nOpening: {opening}\n",
    )
    emit_recruitment_panel_event(
        "stage_change",
        {"application_id": application_id, "stage": new_stage, "opening_title": opening},
    )


def notify_interview_details_updated(application_id: int) -> None:
    row = _fetch_application_row(application_id)
    if not row:
        return
    email = (row.get("applicant_email") or "").strip()
    name = (row.get("applicant_name") or "there").strip()
    opening = (row.get("opening_title") or "your application").strip()
    fmt = (row.get("interview_format") or "").strip().lower()
    subj = f"Interview details: {opening}"
    if fmt == "online":
        extra = "Your interview is scheduled as online — open your applicant portal for the meeting link."
    elif fmt == "onsite":
        extra = "Your interview is on-site — open your applicant portal for the location and instructions."
    else:
        extra = "Interview details were updated — please check your applicant portal."
    body = (
        f"Hello {name},\n\n"
        f"{extra}\n\n"
        f"Role: {opening}\n"
        f"Sign in via {_applicant_portal_hint()}\n\n"
        f"(Application #{application_id})\n"
    )
    _send_applicant_email(email, subj, body)
    _send_internal_email(
        f"[Recruitment] Interview details set: {opening}",
        f"Application #{application_id} ({name}) — interview arrangement updated.\n",
    )
    emit_recruitment_panel_event(
        "interview_details",
        {"application_id": application_id, "opening_title": opening},
    )


def notify_prehire_outcome(
    application_id: int,
    *,
    approved: bool,
    request_title: str,
    admin_notes: Optional[str] = None,
) -> None:
    row = _fetch_application_row(application_id)
    if not row:
        return
    email = (row.get("applicant_email") or "").strip()
    name = (row.get("applicant_name") or "there").strip()
    opening = (row.get("opening_title") or "your application").strip()
    title = (request_title or "Document").strip()
    if approved:
        subj = f"Document approved: {title}"
        body = (
            f"Hello {name},\n\n"
            f"HR has approved your submission for \"{title}\" ({opening}).\n\n"
            f"Sign in via {_applicant_portal_hint()} for next steps.\n"
        )
    else:
        subj = f"Document update required: {title}"
        body = (
            f"Hello {name},\n\n"
            f"HR needs you to re-submit or replace \"{title}\" for {opening}.\n"
            f"Please open your applicant portal for feedback and upload again.\n\n"
        )
        if (admin_notes or "").strip():
            body += f"Note from HR: {admin_notes.strip()}\n\n"
    body += f"(Application #{application_id})\n"
    _send_applicant_email(email, subj, body)
    _send_internal_email(
        f"[Recruitment] Pre-hire {'approved' if approved else 'rejected'}: {title}",
        f"Application #{application_id} — {title}\n",
    )
    emit_recruitment_panel_event(
        "prehire_" + ("approved" if approved else "rejected"),
        {"application_id": application_id, "title": title, "opening_title": opening},
    )


def notify_new_application_submitted(application_id: int) -> None:
    row = _fetch_application_row(application_id)
    if not row:
        return
    email = (row.get("applicant_email") or "").strip()
    name = (row.get("applicant_name") or "there").strip()
    opening = (row.get("opening_title") or "").strip()
    subj = f"Application received: {opening}"
    body = (
        f"Hello {name},\n\n"
        f"Thank you — we received your application for \"{opening}\".\n"
        f"You can track progress on {_applicant_portal_hint()}\n\n"
        f"(Reference #{application_id})\n"
    )
    _send_applicant_email(email, subj, body)
    _send_internal_email(
        f"[Recruitment] New application: {opening}",
        f"{name} <{email}> applied for \"{opening}\" (application #{application_id}).\n",
    )
    emit_recruitment_panel_event(
        "new_application",
        {"application_id": application_id, "opening_title": opening, "applicant_name": name},
    )


def notify_applicant_new_task(application_id: int) -> None:
    row = _fetch_application_row(application_id)
    if not row:
        return
    email = (row.get("applicant_email") or "").strip()
    name = (row.get("applicant_name") or "there").strip()
    opening = (row.get("opening_title") or "").strip()
    subj = f"Action needed: form for {opening}"
    body = (
        f"Hello {name},\n\n"
        f"There is a new form or task to complete for your application \"{opening}\".\n"
        f"Please sign in via {_applicant_portal_hint()} to open it.\n\n"
        f"(Application #{application_id})\n"
    )
    _send_applicant_email(email, subj, body)
    emit_recruitment_panel_event(
        "new_task",
        {"application_id": application_id, "opening_title": opening},
    )


def notify_applicant_hired(application_id: int, contractor_id: int) -> None:
    row = _fetch_application_row(application_id)
    if not row:
        return
    email = (row.get("applicant_email") or "").strip()
    name = (row.get("applicant_name") or "there").strip()
    opening = (row.get("opening_title") or "").strip()
    subj = f"Welcome — next steps for {opening}"
    base = _public_base_url()
    portal = (
        f"{base}{EMPLOYEE_PORTAL_PUBLIC_PATH}"
        if base
        else f"the employee portal ({EMPLOYEE_PORTAL_PUBLIC_PATH}) on this site"
    )
    body = (
        f"Hello {name},\n\n"
        f"Congratulations — your application for \"{opening}\" is complete and you have been added as an employee "
        f"(staff record #{contractor_id}).\n\n"
        f"Use {portal} with your work email when your organisation has enabled access.\n"
        f"HR may send further document requests separately.\n\n"
        f"(Application #{application_id})\n"
    )
    _send_applicant_email(email, subj, body)
    _send_internal_email(
        f"[Recruitment] Hired: {opening}",
        f"{name} hired as contractor #{contractor_id} (application #{application_id}).\n",
    )
    emit_recruitment_panel_event(
        "hired",
        {
            "application_id": application_id,
            "contractor_id": contractor_id,
            "opening_title": opening,
        },
    )


def send_post_hire_portal_welcome_message(contractor_id: int, opening_title: str) -> None:
    """In-app message on employee portal (best effort)."""
    try:
        from app.plugins.employee_portal_module.services import admin_send_message

        title = (opening_title or "your role").strip()
        admin_send_message(
            [int(contractor_id)],
            "Welcome — you’re on the team",
            f"Your recruitment application for \"{title}\" is complete. "
            f"Check HR for any open document requests and complete your profile as needed.",
            source_module="recruitment_module",
            sent_by_user_id=None,
        )
    except Exception as e:
        logger.debug("Recruitment: post-hire portal message skipped: %s", e)


def fetch_prehire_request_title(request_id: int, application_id: int) -> str:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT title FROM rec_prehire_document_requests WHERE id = %s AND application_id = %s LIMIT 1",
            (request_id, application_id),
        )
        row = cur.fetchone()
        if not row:
            return "Document request"
        return (row[0] if isinstance(row, (list, tuple)) else row.get("title")) or "Document request"
    except Exception:
        return "Document request"
    finally:
        cur.close()
        conn.close()
