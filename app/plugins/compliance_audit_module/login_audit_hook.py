"""
Append-only DB login audit for compliance timeline (optional; table created by install.py).
Safe to call from core routes — never raises.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _username_hint(username: Optional[str]) -> str:
    if not username or not str(username).strip():
        return ""
    d = hashlib.sha256(str(username).strip().lower().encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"sha256_16:{d}"


def record_compliance_login(
    *,
    success: bool,
    channel: str,
    user_id: Optional[str] = None,
    contractor_id: Optional[int] = None,
    username: Optional[str] = None,
    ip: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> None:
    try:
        from app.objects import get_db_connection

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT 1 FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'compliance_login_audit'
                """
            )
            if not cur.fetchone():
                return
            uh = _username_hint(username) if not success or not user_id else None
            cur.execute(
                """
                INSERT INTO compliance_login_audit
                (user_id, contractor_id, username_hash, success, channel, ip_address, user_agent)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    user_id,
                    contractor_id,
                    uh,
                    1 if success else 0,
                    (channel or "unknown")[:32],
                    (ip or "")[:45] or None,
                    (user_agent or "")[:512] or None,
                ),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
    except Exception:
        logger.debug("compliance_login_audit insert skipped", exc_info=True)
