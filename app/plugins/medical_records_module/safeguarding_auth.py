"""
Authorisation helpers for ``cura_safeguarding_referrals`` (Cura / handover JSON APIs).

Product policy (server-enforced):
- **JSON APIs** (Cura app, ``/api/safeguarding/...`` facade, ``/api/cura/safeguarding/...``, and
  ``/plugin/safeguarding_module/api``): **creator-only read/list** for every authenticated principal.
  Privileged roles do **not** widen read access here — cross-referral oversight uses the browser
  safeguarding manager (separate session + PIN), not these endpoints.
- **Mutations**: ``assignment_guard_json_response`` still applies when setting ``operational_event_id``;
  ``principal_may_patch_safeguarding`` / delete rules may still allow privileged support actions where
  implemented.

There is **no** sharing via operational event, callsign, or co-assignment for **read** — those
dimensions do not widen read access on the JSON APIs.
"""
from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("medical_records_module.safeguarding_auth")


class SafeguardingAuditError(Exception):
    """Failed to persist a row to ``cura_safeguarding_audit_events`` on a compliance-critical path."""


def assignment_guard_json_response(cur, operational_event_id, username: str, privileged: bool, jsonify):
    """
    Same rules as Cura operational-event POST guard: non-privileged users need an existing event and,
    when ``enforce_assignments`` is on, a row in ``cura_operational_event_assignments``.
    Used for **mutations** that set ``operational_event_id``, not for widening **read** visibility.
    Returns a Flask ``(jsonify(...), status)`` tuple if the request must be blocked, else ``None``.
    """
    if privileged:
        return None
    if not operational_event_id:
        return None
    un = (username or "").strip()
    if not un:
        return (jsonify({"error": "Unauthenticated"}), 401)
    try:
        cur.execute(
            "SELECT enforce_assignments FROM cura_operational_events WHERE id = %s",
            (operational_event_id,),
        )
        r = cur.fetchone()
        if not r:
            return (jsonify({"error": "Operational period not found"}), 404)
        enf = r[0]
        try:
            enforce = bool(int(enf)) if enf is not None else False
        except (TypeError, ValueError):
            enforce = bool(enf)
        if not enforce:
            return None
        cur.execute(
            """
            SELECT 1 FROM cura_operational_event_assignments
            WHERE operational_event_id = %s AND principal_username = %s
            """,
            (operational_event_id, un),
        )
        if cur.fetchone():
            return None
        return (jsonify({"error": "You are not assigned to this operational period"}), 403)
    except Exception as ex:
        if "cura_operational_event_assignments" in str(ex) or "Unknown column" in str(ex):
            return (jsonify({"error": "Run database upgrade (migration 004)"}), 503)
        raise


def principal_may_read_referral(
    cur,
    *,
    operational_event_id: Any,
    created_by: Any,
    username: str,
    privileged: bool,
) -> bool:
    """When ``privileged`` is false, only the creator (``created_by``) may read. JSON APIs pass ``privileged=False``."""
    _ = cur
    if privileged:
        return True
    un = (username or "").strip()
    return (created_by or "").strip() == un


def principal_may_patch_safeguarding(
    *,
    privileged: bool,
    row_status: Any,
    created_by: Any,
    username: str,
) -> bool:
    """Draft edits: creator or privileged; submitted/closed/archived locked for non-privileged."""
    if privileged:
        return True
    st = (row_status or "").lower()
    if st in ("submitted", "closed", "archived"):
        return False
    return (created_by or "").strip() == (username or "").strip()


CREW_REFERRAL_VISIBILITY_SQL = "(cura_safeguarding_referrals.created_by = %s)"


def crew_referral_visibility_params(username: str) -> tuple[str]:
    """SQL bind value for :data:`CREW_REFERRAL_VISIBILITY_SQL`."""
    return ((username or "").strip(),)


def insert_safeguarding_audit_event(
    cur,
    referral_id: int,
    actor: str,
    action: str,
    detail: Any = None,
    *,
    required: bool = True,
) -> None:
    """
    Append to ``cura_safeguarding_audit_events``. If ``required`` and the insert fails, raises
    :class:`SafeguardingAuditError` (caller should roll back and return 503).
    ``detail`` must be JSON-serialisable and should not contain free-text PHI.
    """
    try:
        dj = None
        if detail is not None:
            dj = json.dumps(detail) if not isinstance(detail, str) else detail
        cur.execute(
            """
            INSERT INTO cura_safeguarding_audit_events (referral_id, actor_username, action, detail_json)
            VALUES (%s, %s, %s, %s)
            """,
            (referral_id, actor or None, action, dj),
        )
    except Exception as ex:
        logger.exception(
            "Safeguarding audit insert failed: referral_id=%s action=%s err=%s",
            referral_id,
            action,
            ex,
        )
        if required:
            raise SafeguardingAuditError(str(ex)) from ex
