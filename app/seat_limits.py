"""
Early-access billable seat limit: distinct people across core ``users`` and active ``tb_contractors``.

**Provisioning (operators only):** cap is stored in ``sparrow_seat_limit`` (row ``id = 1``) or overridden via
``set_max_billable_seats_admin`` / SQL. The Core settings UI shows a **read-only** “Plan” summary only.

**Fallback:** if the database row cannot be read, ``SPARROW_MAX_BILLABLE_SEATS`` / ``RAILWAY_MAX_BILLABLE_SEATS``
apply (see ``_max_billable_seats_from_env``).

**Database ``max_billable_seats``:** positive integer = cap; ``0`` = unlimited.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from app.objects import get_db_connection

logger = logging.getLogger(__name__)

SEAT_LIMIT_TABLE = "sparrow_seat_limit"
SEAT_LIMIT_ROW_ID = 1

SEAT_LIMIT_SUPPORT_MESSAGE = (
    "Your organisation has reached the early-access user limit for this plan. "
    "Please contact support if you need additional seats."
)


def _raw_seat_limit_from_env() -> str:
    for key in ("SPARROW_MAX_BILLABLE_SEATS", "RAILWAY_MAX_BILLABLE_SEATS"):
        val = os.environ.get(key)
        if val is not None and str(val).strip() != "":
            return str(val).strip()
    return "30"


def _max_billable_seats_from_env() -> Optional[int]:
    raw = _raw_seat_limit_from_env()
    if not raw or raw.lower() in ("0", "off", "none", "unlimited", "false", "no"):
        return None
    try:
        n = int(raw)
        if n <= 0:
            return None
        return n
    except ValueError:
        return 30


def ensure_sparrow_seat_limit_table(conn) -> None:
    """Create table and default row if missing. Commits on conn."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SEAT_LIMIT_TABLE} (
              id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
              max_billable_seats INT NOT NULL DEFAULT 30,
              updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        cur.execute(
            f"""
            INSERT IGNORE INTO {SEAT_LIMIT_TABLE} (id, max_billable_seats)
            VALUES (%s, 30)
            """,
            (SEAT_LIMIT_ROW_ID,),
        )
        conn.commit()
    finally:
        cur.close()


def max_billable_seats() -> Optional[int]:
    """
    Effective seat cap: database row first, then environment fallback.
    Returns None when enforcement is disabled (unlimited).
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            ensure_sparrow_seat_limit_table(conn)
            cur.execute(
                f"SELECT max_billable_seats FROM {SEAT_LIMIT_TABLE} WHERE id = %s LIMIT 1",
                (SEAT_LIMIT_ROW_ID,),
            )
            row = cur.fetchone()
            if row is not None and row[0] is not None:
                v = int(row[0])
                return None if v <= 0 else v
        finally:
            cur.close()
            conn.close()
            conn = None
    except Exception as e:
        logger.warning("sparrow_seat_limit: using env fallback (%s)", e)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return _max_billable_seats_from_env()


def set_max_billable_seats_admin(value: int) -> None:
    """
    Persist cap for row id=1. ``value <= 0`` means unlimited (store 0).
    For operator use only (SQL, migrations, managed console) — not the product settings UI.
    """
    if value < 0:
        value = 0
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        ensure_sparrow_seat_limit_table(conn)
        cur.execute(
            f"""
            UPDATE {SEAT_LIMIT_TABLE} SET max_billable_seats = %s WHERE id = %s
            """,
            (value, SEAT_LIMIT_ROW_ID),
        )
        if cur.rowcount == 0:
            cur.execute(
                f"""
                INSERT INTO {SEAT_LIMIT_TABLE} (id, max_billable_seats)
                VALUES (%s, %s)
                """,
                (SEAT_LIMIT_ROW_ID, value),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def get_seat_usage_snapshot() -> Dict[str, Any]:
    """For admin UI: current usage, effective limit, and raw DB value if readable."""
    used = 0
    db_raw: Optional[int] = None
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            ensure_sparrow_seat_limit_table(conn)
            cur.execute(
                f"SELECT max_billable_seats FROM {SEAT_LIMIT_TABLE} WHERE id = %s",
                (SEAT_LIMIT_ROW_ID,),
            )
            r = cur.fetchone()
            if r is not None and r[0] is not None:
                db_raw = int(r[0])
            used = count_billable_seats(cur)
        finally:
            cur.close()
            conn.close()
            conn = None
    except Exception as e:
        logger.warning("get_seat_usage_snapshot: %s", e)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    effective = max_billable_seats()
    return {
        "used": used,
        "limit": effective,
        "unlimited": effective is None,
        "db_max_billable_seats": db_raw,
        "env_fallback_limit": _max_billable_seats_from_env(),
    }


def _normalize_email(email: Optional[str]) -> str:
    e = (email or "").strip().lower()
    return e if e and "@" in e else ""


def _contractor_active_sql() -> str:
    return "(status IS NULL OR LOWER(TRIM(COALESCE(status,''))) IN ('active','1'))"


def _has_contractors_table(cur) -> bool:
    cur.execute("SHOW TABLES LIKE %s", ("tb_contractors",))
    return cur.fetchone() is not None


def count_billable_seats(cur) -> int:
    """Count billable seats: distinct valid emails across both tables, plus rows without a usable email."""
    cur.execute(
        """
        SELECT LOWER(TRIM(email)) AS e FROM users
        WHERE email IS NOT NULL AND TRIM(email) <> '' AND TRIM(email) LIKE %s
          AND COALESCE(billable_exempt, 0) = 0
        """,
        ("%@%",),
    )
    emails = {row[0] for row in (cur.fetchall() or []) if row and row[0]}
    c_bad = 0
    if _has_contractors_table(cur):
        cur.execute(
            f"""
            SELECT LOWER(TRIM(email)) AS e FROM tb_contractors
            WHERE {_contractor_active_sql()}
              AND email IS NOT NULL AND TRIM(email) <> '' AND TRIM(email) LIKE %s
            """,
            ("%@%",),
        )
        for row in cur.fetchall() or []:
            if row and row[0]:
                emails.add(row[0])
        cur.execute(
            f"""
            SELECT COUNT(*) FROM tb_contractors
            WHERE {_contractor_active_sql()}
              AND (email IS NULL OR TRIM(COALESCE(email,'')) = ''
                   OR TRIM(COALESCE(email,'')) NOT LIKE %s)
            """,
            ("%@%",),
        )
        c_bad = int((cur.fetchone() or (0,))[0])
    cur.execute(
        """
        SELECT COUNT(*) FROM users
        WHERE (email IS NULL OR TRIM(COALESCE(email,'')) = ''
           OR TRIM(COALESCE(email,'')) NOT LIKE %s)
          AND COALESCE(billable_exempt, 0) = 0
        """,
        ("%@%",),
    )
    u_bad = int((cur.fetchone() or (0,))[0])
    return len(emails) + u_bad + c_bad


def _email_already_in_either_table(cur, norm: str) -> bool:
    cur.execute(
        "SELECT 1 FROM users WHERE LOWER(TRIM(email)) = %s LIMIT 1",
        (norm,),
    )
    if cur.fetchone():
        return True
    if _has_contractors_table(cur):
        cur.execute(
            "SELECT 1 FROM tb_contractors WHERE LOWER(TRIM(email)) = %s LIMIT 1",
            (norm,),
        )
        return cur.fetchone() is not None
    return False


def seat_check_error_for_new_email(
    email: Optional[str],
    *,
    db_cursor=None,
) -> Optional[str]:
    """
    Before inserting a new user or contractor row, ensure the plan allows another billable identity.
    Same email in both tables counts once; adding the second row is allowed.
    Returns an error message to show the user, or None if OK / enforcement disabled.
    """
    limit = max_billable_seats()
    if limit is None:
        return None

    norm = _normalize_email(email)
    close_conn = False
    conn = None
    cur = db_cursor
    if cur is None:
        conn = get_db_connection()
        cur = conn.cursor()
        close_conn = True
    try:
        if norm:
            if _email_already_in_either_table(cur, norm):
                return None
        current = count_billable_seats(cur)
        if current >= limit:
            return SEAT_LIMIT_SUPPORT_MESSAGE
        return None
    finally:
        if close_conn:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
