"""
Time-limited vendor support ("shadow") login — customer-initiated from Core settings.

IG / security model (short):
- Customer admin explicitly generates credentials; password is shown once; DB stores hash only.
- Shadow row uses **role ``superuser``** in the database so all module and org-admin routes match a
  normal superuser. Browser login sets ``session[\"support_shadow\"]`` for audit correlation only.
- Shadow user is ``billable_exempt`` and uses a fixed placeholder email so seat limits ignore it.
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from app.objects import AuthManager, PluginManager, User, get_db_connection
from app.permissions_registry import collect_permission_catalog, default_permission_ids_for_role

# DB role for the shadow row: must be ``superuser`` so plugin routes that list superuser allow access.
SHADOW_EFFECTIVE_ROLE = "superuser"

logger = logging.getLogger(__name__)

# Placeholder email: must not collide with real staff; excluded from billable seat SQL via billable_exempt.
SHADOW_EMAIL = "support-access@invalid.local"

AUDIT_TABLE = "support_access_audit"

_DEFAULT_MINUTES = 30
_MIN_MINUTES = 15
_MAX_MINUTES = 120


def shadow_username() -> str:
    u = (os.environ.get("SPARROW_SUPPORT_SHADOW_USERNAME") or "sparrowsupport").strip()
    return u or "sparrowsupport"


def _plugins_folder() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "plugins")


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


def ensure_support_access_schema(conn=None) -> None:
    """Add users columns + audit table if missing. Commits when owning the connection."""
    own = conn is None
    if own:
        conn = get_db_connection()
    try:
        cur = conn.cursor()
        try:
            if not _column_exists(cur, "users", "billable_exempt"):
                cur.execute(
                    "ALTER TABLE users ADD COLUMN billable_exempt TINYINT(1) NOT NULL DEFAULT 0"
                )
            if not _column_exists(cur, "users", "support_access_expires_at"):
                cur.execute(
                    "ALTER TABLE users ADD COLUMN support_access_expires_at DATETIME NULL DEFAULT NULL"
                )
            if not _column_exists(cur, "users", "support_access_enabled"):
                cur.execute(
                    "ALTER TABLE users ADD COLUMN support_access_enabled TINYINT(1) NOT NULL DEFAULT 0"
                )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  action VARCHAR(32) NOT NULL,
                  actor_username VARCHAR(100) NOT NULL,
                  shadow_username VARCHAR(100) NOT NULL,
                  note TEXT,
                  expires_at DATETIME NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  INDEX idx_support_audit_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            # Existing shadow rows may still have legacy role support_break_glass — normalise to superuser.
            try:
                cur.execute(
                    """
                    UPDATE users SET role = %s
                    WHERE COALESCE(billable_exempt, 0) = 1
                    """,
                    (SHADOW_EFFECTIVE_ROLE,),
                )
            except Exception as _migrate_ex:
                logger.debug("support role migrate skipped: %s", _migrate_ex)
            conn.commit()
        finally:
            cur.close()
    except Exception as e:
        logger.warning("ensure_support_access_schema: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        if own:
            try:
                conn.close()
            except Exception:
                pass


def _utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _now_utc_naive() -> datetime:
    return _utc_naive(datetime.now(timezone.utc))


def support_preview_contractor_id() -> Optional[int]:
    """Active contractor id used when vendor support opens the employee / time-billing portal."""
    raw = (os.environ.get("SPARROW_SUPPORT_EMPLOYEE_PORTAL_CONTRACTOR_ID") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("SPARROW_SUPPORT_EMPLOYEE_PORTAL_CONTRACTOR_ID must be an integer")
        return None


def attempt_support_shadow_portal_login(password: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Validate the shadow user's password for contractor-facing portals. Caller must only invoke this when
    the submitted email is :data:`SHADOW_EMAIL`.

    Returns:
        ``(contractor_id, None)`` on success, or ``(None, error_message)`` on failure.
    """
    if not password:
        return None, "Invalid email or password."
    user_row = User.get_user_by_email(SHADOW_EMAIL)
    if not user_row or not user_row.get("password_hash"):
        return None, "Invalid email or password."
    if not AuthManager.verify_password(user_row["password_hash"], password):
        return None, "Invalid email or password."
    if not int(user_row.get("billable_exempt") or 0):
        return None, "Invalid email or password."
    blocked = support_login_blocked_reason(user_row)
    if blocked:
        return None, blocked
    cid = support_preview_contractor_id()
    if cid is None:
        return None, (
            "Vendor support cannot use the employee portal on this server until it is configured. "
            "Set environment variable SPARROW_SUPPORT_EMPLOYEE_PORTAL_CONTRACTOR_ID to an active "
            "contractor (employee) user id—often a dedicated preview account."
        )
    return cid, None


def support_login_blocked_reason(user_row: Optional[dict]) -> Optional[str]:
    """
    If this user row is the shadow account and access is not valid, return a login error message.
    """
    if not user_row:
        return None
    if not int(user_row.get("billable_exempt") or 0):
        return None
    if not int(user_row.get("support_access_enabled") or 0):
        return (
            "Vendor support access is not active. Ask your organisation administrator "
            "to generate new support credentials from Core settings."
        )
    exp = user_row.get("support_access_expires_at")
    if exp is None:
        return (
            "Vendor support access is not configured. Ask your organisation administrator "
            "to generate support credentials from Core settings."
        )
    if isinstance(exp, str):
        try:
            exp = datetime.fromisoformat(exp.replace("Z", "+00:00"))
        except Exception:
            return "Vendor support access has expired or is invalid."
    now = _now_utc_naive()
    exp_naive = _utc_naive(exp) if isinstance(exp, datetime) else None
    if exp_naive is not None and now > exp_naive:
        return (
            "Vendor support access has expired. Ask your organisation administrator "
            "for a new time-limited access window from Core settings."
        )
    return None


def _append_audit(
    cur,
    action: str,
    actor_username: str,
    shadow_uname: str,
    note: Optional[str],
    expires_at: Optional[datetime],
) -> None:
    cur.execute(
        f"""
        INSERT INTO {AUDIT_TABLE} (action, actor_username, shadow_username, note, expires_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            action[:32],
            (actor_username or "")[:100],
            (shadow_uname or "")[:100],
            (note or None) if note and str(note).strip() else None,
            _utc_naive(expires_at) if expires_at else None,
        ),
    )


def _cooldown_seconds() -> int:
    raw = (os.environ.get("SPARROW_SUPPORT_GENERATE_COOLDOWN_SEC") or "120").strip()
    try:
        return max(30, min(int(raw), 3600))
    except ValueError:
        return 120


def _last_generate_ts(cur) -> Optional[datetime]:
    cur.execute(
        f"""
        SELECT created_at FROM {AUDIT_TABLE}
        WHERE action = 'generate'
        ORDER BY id DESC LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return None
    ts = row[0]
    if isinstance(ts, datetime):
        return ts
    try:
        return datetime.fromisoformat(str(ts))
    except Exception:
        return None


def generate_support_access(
    duration_minutes: int,
    actor_username: str,
    note: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[datetime]]:
    """
    Set a new password for the shadow user and enable access until expires_at.
    Returns (plain_password, error_message, expires_at_utc_naive).
    """
    duration = max(_MIN_MINUTES, min(int(duration_minutes or _DEFAULT_MINUTES), _MAX_MINUTES))
    sun = shadow_username()
    pm = PluginManager(_plugins_folder())
    catalog = collect_permission_catalog(pm)
    perms = default_permission_ids_for_role(SHADOW_EFFECTIVE_ROLE, catalog)
    perms_json = json.dumps(perms)
    plain = secrets.token_urlsafe(18)
    phash = AuthManager.hash_password(plain)
    expires = _now_utc_naive() + timedelta(minutes=duration)
    uid = str(uuid.uuid4())

    conn = get_db_connection()
    try:
        ensure_support_access_schema(conn)
    except Exception as e:
        logger.warning("generate_support_access schema ensure: %s", e)
    cur = conn.cursor(dictionary=True)
    try:
        last = _last_generate_ts(cur)
        if last:
            elapsed = (_now_utc_naive() - _utc_naive(last)).total_seconds()
            if elapsed < _cooldown_seconds():
                wait = int(_cooldown_seconds() - elapsed)
                return None, f"Please wait {wait} seconds before generating again.", None

        cur.execute(
            "SELECT id FROM users WHERE username = %s LIMIT 1",
            (sun,),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE users SET
                  password_hash = %s,
                  role = %s,
                  permissions = %s,
                  billable_exempt = 1,
                  email = %s,
                  support_access_enabled = 1,
                  support_access_expires_at = %s,
                  first_name = %s,
                  last_name = %s
                WHERE username = %s
                """,
                (
                    phash,
                    SHADOW_EFFECTIVE_ROLE,
                    perms_json,
                    SHADOW_EMAIL,
                    expires,
                    "Vendor",
                    "Support",
                    sun,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO users (
                  id, username, email, password_hash, role, permissions,
                  billable_exempt, support_access_enabled, support_access_expires_at,
                  first_name, last_name
                ) VALUES (%s, %s, %s, %s, %s, %s, 1, 1, %s, %s, %s)
                """,
                (
                    uid,
                    sun,
                    SHADOW_EMAIL,
                    phash,
                    SHADOW_EFFECTIVE_ROLE,
                    perms_json,
                    expires,
                    "Vendor",
                    "Support",
                ),
            )
        _append_audit(cur, "generate", actor_username, sun, note, expires)
        conn.commit()
        return plain, None, expires
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("generate_support_access failed")
        return None, str(e), None
    finally:
        cur.close()
        conn.close()


def revoke_support_access(actor_username: str, note: Optional[str]) -> Tuple[bool, Optional[str]]:
    """Invalidate shadow password and disable access."""
    sun = shadow_username()
    dead_hash = AuthManager.hash_password(secrets.token_urlsafe(32))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        ensure_support_access_schema(conn)
        cur.execute(
            """
            UPDATE users SET
              password_hash = %s,
              support_access_enabled = 0,
              support_access_expires_at = NULL
            WHERE username = %s AND COALESCE(billable_exempt, 0) = 1
            """,
            (dead_hash, sun),
        )
        _append_audit(cur, "revoke", actor_username, sun, note, None)
        conn.commit()
        return True, None
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def get_support_access_status() -> Dict[str, Any]:
    """For Core settings UI (no secrets)."""
    sun = shadow_username()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        try:
            ensure_support_access_schema(conn)
        except Exception:
            pass
        cur.execute(
            """
            SELECT support_access_enabled, support_access_expires_at, billable_exempt
            FROM users WHERE username = %s LIMIT 1
            """,
            (sun,),
        )
        row = cur.fetchone() or {}
        enabled = bool(int(row.get("support_access_enabled") or 0))
        exp = row.get("support_access_expires_at")
        now = _now_utc_naive()
        exp_dt = None
        if exp:
            if isinstance(exp, datetime):
                exp_dt = _utc_naive(exp)
            else:
                try:
                    exp_dt = datetime.fromisoformat(str(exp).replace("Z", "+00:00"))
                    exp_dt = _utc_naive(exp_dt)
                except Exception:
                    exp_dt = None
        expired = bool(exp_dt and now > exp_dt)
        active = enabled and exp_dt and not expired
        if not row:
            state = "inactive"
        elif not enabled:
            state = "revoked"
        elif expired:
            state = "expired"
        elif active:
            state = "active"
        else:
            state = "inactive"
        cur.execute(
            f"""
            SELECT action, actor_username, note, expires_at, created_at
            FROM {AUDIT_TABLE}
            ORDER BY id DESC LIMIT 8
            """
        )
        history = cur.fetchall() or []
        return {
            "shadow_username": sun,
            "state": state,
            "active": active,
            "expires_at": exp_dt,
            "history": history,
            "cooldown_sec": _cooldown_seconds(),
            "duration_default": _DEFAULT_MINUTES,
            "duration_min": _MIN_MINUTES,
            "duration_max": _MAX_MINUTES,
        }
    finally:
        cur.close()
        conn.close()