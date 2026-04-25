"""
Create Cura safeguarding referrals from the employee portal using the same DB path as the
JSON facade (authoritative store: ``cura_safeguarding_referrals`` + audit events).

No Ventus dispatch — optional ``operational_event_id`` only when caller passes it and the
column exists (assignment guards are enforced inside medical_records when using their APIs;
here we only set the FK when provided and non-null).
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """,
        (table, column),
    )
    row = cur.fetchone()
    return bool(row and int(row[0] or 0) > 0)


def create_safeguarding_referral_for_contractor(
    *,
    contractor_username: str,
    payload: Dict[str, Any],
    subject_type: str = "incident_portal",
    operational_event_id: Optional[int] = None,
    idempotency_key: Optional[str] = None,
) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Returns (referral_id, public_id, error_message).
    """
    uname = (contractor_username or "").strip().lower()
    if not uname:
        return None, None, "Missing portal principal username"

    try:
        from app.plugins.medical_records_module.safeguarding_auth import (
            SafeguardingAuditError,
            insert_safeguarding_audit_event,
        )
    except ImportError as e:
        logger.warning("safeguarding_bridge import failed: %s", e)
        return None, None, "Safeguarding module is not available on this server"

    from app.objects import get_db_connection

    public_id = str(uuid.uuid4())
    payload_json = json.dumps(payload if isinstance(payload, dict) else {})
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cols = [
            "public_id",
            "idempotency_key",
            "status",
            "subject_type",
            "payload_json",
            "created_by",
            "updated_by",
        ]
        vals = [
            public_id,
            (idempotency_key or "").strip() or None,
            "draft",
            subject_type or "incident_portal",
            payload_json,
            uname,
            uname,
        ]
        if operational_event_id is not None and _column_exists(
            cur, "cura_safeguarding_referrals", "operational_event_id"
        ):
            cols.append("operational_event_id")
            vals.append(int(operational_event_id))
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO cura_safeguarding_referrals ({', '.join(cols)}) VALUES ({placeholders})"
        try:
            cur.execute(sql, tuple(vals))
        except Exception as exc:
            if idempotency_key and (
                "1062" in str(exc) or "Duplicate" in str(exc)
            ):
                conn.rollback()
                cur.execute(
                    "SELECT id, public_id FROM cura_safeguarding_referrals WHERE idempotency_key = %s LIMIT 1",
                    ((idempotency_key or "").strip(),),
                )
                ex = cur.fetchone()
                if ex:
                    return int(ex[0]), str(ex[1]), None
            raise
        rid = cur.lastrowid
        try:
            insert_safeguarding_audit_event(
                cur, int(rid), uname, "create", {"source": "incident_reporting_portal"}, required=True
            )
        except SafeguardingAuditError:
            conn.rollback()
            return None, None, "Safeguarding audit log unavailable; referral not saved"
        conn.commit()
        return int(rid), public_id, None
    except Exception as e:
        conn.rollback()
        logger.exception("create_safeguarding_referral_for_contractor: %s", e)
        if "doesn't exist" in str(e).lower() or "Unknown table" in str(e):
            return None, None, "Safeguarding database tables are not installed"
        return None, None, "Could not create safeguarding referral"
    finally:
        cur.close()
        conn.close()
