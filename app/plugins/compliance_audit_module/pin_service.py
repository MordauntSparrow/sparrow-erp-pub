"""Compliance PIN: bcrypt hash, lockout, step-up session, audit attempts."""
from __future__ import annotations

import re
import time
from typing import Any

from app.objects import AuthManager, get_db_connection

_PIN_RE = re.compile(r"^\d{6,12}$")
_MAX_FAIL = 5


def validate_pin_format(pin: str | None) -> bool:
    return bool(pin and _PIN_RE.match(pin.strip()))


def get_pin_row(cur, user_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT user_id, pin_hash, failed_attempts, locked_until
        FROM user_compliance_pin_hash
        WHERE user_id = %s
        """,
        (str(user_id),),
    )
    return cur.fetchone()


def is_locked(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    lu = row.get("locked_until")
    if lu is None:
        return False
    from datetime import datetime

    if isinstance(lu, datetime):
        return lu > datetime.now()
    return False


def record_pin_attempt(
    cur,
    conn,
    *,
    user_id: str,
    success: bool,
    ip: str | None,
    detail: str | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO compliance_pin_audit (user_id, success, ip_address, detail)
        VALUES (%s, %s, %s, %s)
        """,
        (str(user_id), 1 if success else 0, ip, (detail or "")[:255] or None),
    )
    conn.commit()


def set_pin(conn, *, user_id: str, new_pin: str) -> tuple[bool, str]:
    if not validate_pin_format(new_pin):
        return False, "PIN must be 6–12 digits."
    cur = conn.cursor(dictionary=True)
    try:
        h = AuthManager.hash_password(new_pin.strip())
        cur.execute(
            """
            INSERT INTO user_compliance_pin_hash (user_id, pin_hash, failed_attempts, locked_until)
            VALUES (%s, %s, 0, NULL)
            ON DUPLICATE KEY UPDATE pin_hash = VALUES(pin_hash), failed_attempts = 0, locked_until = NULL
            """,
            (str(user_id), h),
        )
        conn.commit()
        return True, "Compliance PIN saved."
    finally:
        cur.close()


def verify_pin_and_update_lockout(
    conn,
    *,
    user_id: str,
    pin: str,
    ip: str | None,
    lockout_minutes: int = 15,
) -> tuple[bool, str]:
    cur = conn.cursor(dictionary=True)
    try:
        row = get_pin_row(cur, user_id)
        if not row or not row.get("pin_hash"):
            record_pin_attempt(cur, conn, user_id=user_id, success=False, ip=ip, detail="no_pin")
            return False, "Set a compliance PIN first (PIN enrolment)."
        if is_locked(row):
            record_pin_attempt(cur, conn, user_id=user_id, success=False, ip=ip, detail="locked")
            return False, "PIN is locked after failed attempts. Try again later."
        if not validate_pin_format(pin):
            record_pin_attempt(cur, conn, user_id=user_id, success=False, ip=ip, detail="bad_format")
            return False, "Invalid PIN format."
        ok = AuthManager.verify_password(row["pin_hash"], pin.strip())
        if ok:
            cur.execute(
                """
                UPDATE user_compliance_pin_hash
                SET failed_attempts = 0, locked_until = NULL
                WHERE user_id = %s
                """,
                (str(user_id),),
            )
            conn.commit()
            record_pin_attempt(cur, conn, user_id=user_id, success=True, ip=ip, detail="verify_ok")
            return True, "OK"
        fails = int(row.get("failed_attempts") or 0) + 1
        locked_until = None
        msg = "PIN incorrect."
        if fails >= _MAX_FAIL:
            from datetime import datetime, timedelta

            locked_until = datetime.now() + timedelta(minutes=max(1, int(lockout_minutes)))
            msg = f"Too many failures. Locked for {lockout_minutes} minutes."
        cur.execute(
            """
            UPDATE user_compliance_pin_hash
            SET failed_attempts = %s, locked_until = %s
            WHERE user_id = %s
            """,
            (fails, locked_until, str(user_id)),
        )
        conn.commit()
        record_pin_attempt(cur, conn, user_id=user_id, success=False, ip=ip, detail="bad_pin")
        return False, msg
    finally:
        cur.close()


def user_has_pin(user_id: str) -> bool:
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        try:
            row = get_pin_row(cur, user_id)
            return bool(row and row.get("pin_hash"))
        finally:
            cur.close()
    finally:
        conn.close()


# --- Step-up session (Flask session keys) ---

SESSION_KEY_UNTIL = "compliance_audit_step_up_until"


def step_up_deadline(session_dict: dict, ttl_seconds: int) -> float:
    return time.time() + max(60, int(ttl_seconds))


def set_step_up(session_dict: dict, ttl_seconds: int) -> None:
    session_dict[SESSION_KEY_UNTIL] = step_up_deadline(session_dict, ttl_seconds)


def step_up_valid(session_dict: dict) -> bool:
    until = session_dict.get(SESSION_KEY_UNTIL)
    try:
        return bool(until) and time.time() < float(until)
    except (TypeError, ValueError):
        return False


def clear_step_up(session_dict: dict) -> None:
    session_dict.pop(SESSION_KEY_UNTIL, None)
