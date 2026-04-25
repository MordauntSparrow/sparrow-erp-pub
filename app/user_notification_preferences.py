"""
Per-user email notification opt-out (stored on ``users.notification_preferences`` JSON).

Defaults: all channels on (missing key = True). Only applies where the code path checks prefs
(e.g. recruitment internal staff alerts, HR document request email when linked to ``users``).
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from app.objects import get_db_connection

logger = logging.getLogger(__name__)

# Recruitment internal digest kinds (must match recruitment_module.notifications._send_internal_email)
RECRUITMENT_INTERNAL_EMAIL_KINDS: Tuple[str, ...] = (
    "new_application",
    "stage_change",
    "interview",
    "prehire",
    "form_task",
    "hired",
)

HR_CONTRACTOR_EMAIL_KINDS: Tuple[str, ...] = ("document_request",)

RECRUITMENT_INTERNAL_KIND_LABELS: Dict[str, str] = {
    "new_application": "New applications submitted",
    "stage_change": "Pipeline stage changes",
    "interview": "Interview details updated",
    "prehire": "Pre-hire requests (added / approved / rejected)",
    "form_task": "Applicant form or task assigned",
    "hired": "Candidate hired",
}

HR_CONTRACTOR_KIND_LABELS: Dict[str, str] = {
    "document_request": "HR document requests (to my employee email)",
}

_HR_ANY_VIEW = frozenset(
    {
        "hr_module.read",
        "hr_module.edit_employees",
        "hr_module.document_requests",
        "hr_module.library",
    }
)


def user_qualifies_for_hr_staff_context(user) -> bool:
    """True if user has HR plugin access (for self-service HR email prefs)."""
    role = (getattr(user, "role", None) or "").strip().lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    raw = getattr(user, "permissions", None) or []
    ps = {str(x).strip() for x in raw if str(x).strip()}
    if "hr_module.access" in ps:
        return True
    return bool(ps & _HR_ANY_VIEW)


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """,
        (table, column),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def ensure_notification_preferences_schema(conn=None) -> None:
    """Add ``users.notification_preferences`` JSON if missing."""
    own = conn is None
    if own:
        conn = get_db_connection()
    try:
        cur = conn.cursor()
        try:
            if not _column_exists(cur, "users", "notification_preferences"):
                cur.execute(
                    "ALTER TABLE users ADD COLUMN notification_preferences JSON NULL DEFAULT NULL"
                )
            if own:
                conn.commit()
        finally:
            cur.close()
    finally:
        if own:
            conn.close()


def _default_prefs() -> Dict[str, Any]:
    return {
        "email": {
            "recruitment_internal": {
                k: True for k in RECRUITMENT_INTERNAL_EMAIL_KINDS
            },
            "hr_contractor": {k: True for k in HR_CONTRACTOR_EMAIL_KINDS},
        }
    }


def _deep_merge_defaults(stored: Any) -> Dict[str, Any]:
    out = _default_prefs()
    if not stored:
        return out
    if isinstance(stored, str):
        try:
            stored = json.loads(stored)
        except (TypeError, ValueError):
            return out
    if not isinstance(stored, dict):
        return out
    em = stored.get("email")
    if isinstance(em, dict):
        ri = em.get("recruitment_internal")
        if isinstance(ri, dict):
            for k in RECRUITMENT_INTERNAL_EMAIL_KINDS:
                if k in ri:
                    out["email"]["recruitment_internal"][k] = bool(ri[k])
        hc = em.get("hr_contractor")
        if isinstance(hc, dict):
            for k in HR_CONTRACTOR_EMAIL_KINDS:
                if k in hc:
                    out["email"]["hr_contractor"][k] = bool(hc[k])
    return out


def get_notification_preferences_for_user(user_id: str) -> Dict[str, Any]:
    uid = (user_id or "").strip()
    if not uid:
        return _default_prefs()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT notification_preferences FROM users WHERE id = %s LIMIT 1", (uid,)
        )
        row = cur.fetchone() or {}
        return _deep_merge_defaults(row.get("notification_preferences"))
    except Exception as e:
        logger.warning("notification_preferences load failed: %s", e)
        return _default_prefs()
    finally:
        cur.close()
        conn.close()


def save_notification_preferences_for_user(user_id: str, prefs: Dict[str, Any]) -> bool:
    uid = (user_id or "").strip()
    if not uid:
        return False
    blob = json.dumps(prefs, separators=(",", ":"))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE users SET notification_preferences = %s WHERE id = %s", (blob, uid)
        )
        conn.commit()
        return bool(cur.rowcount)
    except Exception as e:
        logger.warning("notification_preferences save failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        cur.close()
        conn.close()


def user_wants_recruitment_internal_email(user_id: Optional[str], kind: str) -> bool:
    """Env-only recipients (``user_id`` None) always receive mail."""
    if not user_id:
        return True
    if kind not in RECRUITMENT_INTERNAL_EMAIL_KINDS:
        return True
    p = get_notification_preferences_for_user(str(user_id))
    try:
        return bool(p["email"]["recruitment_internal"].get(kind, True))
    except (KeyError, TypeError):
        return True


def user_wants_hr_contractor_email(user_id: Optional[str], kind: str) -> bool:
    if not user_id:
        return True
    if kind not in HR_CONTRACTOR_EMAIL_KINDS:
        return True
    p = get_notification_preferences_for_user(str(user_id))
    try:
        return bool(p["email"]["hr_contractor"].get(kind, True))
    except (KeyError, TypeError):
        return True


def lookup_user_id_for_contractor(contractor_id: int) -> Optional[str]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM users WHERE contractor_id = %s LIMIT 1",
            (int(contractor_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        return str(row[0]) if row[0] is not None else None
    except Exception as e:
        logger.debug("lookup_user_id_for_contractor: %s", e)
        return None
    finally:
        cur.close()
        conn.close()
