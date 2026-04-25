"""
Opt-in: when rota assignments change, add missing Cura operational roster rows.

Requires ``SPARROW_SCHEDULING_CURA_ROSTER_SYNC=1`` (or ``true`` / ``yes`` / ``on``).
This path is **additive only** — it INSERTs ``cura_operational_event_assignments`` rows
for Sparrow usernames from ``tb_contractors`` that are not already on the linked event.
It does **not** remove assignments (manual clinical roster entries stay intact).

Resolves ``cura_operational_events.id`` from, in order (scheduling / CRM before run sheet):
  1. ``crm_event_plans.cura_operational_event_id`` where ``schedule_shift_id`` matches the shift
  2. ``crm_event_plans`` for ``crm_event_plan:{id}`` in ``schedule_shifts.external_id``
  3. ``runsheets.cura_operational_event_id`` for the shift's ``runsheet_id`` when no plan link applies
     (run sheets are an optional Time Billing layer for actuals; roster linkage prefers CRM/plan).
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Set

from app.objects import get_db_connection

from app.plugins.crm_module.scheduling_to_crm_sync import (
    _collect_assignees_for_shift,
    _contractor_name_email,
    _parse_external_crm,
    _shift_primary_contractor,
)

logger = logging.getLogger(__name__)

_ENV_KEYS = ("SPARROW_SCHEDULING_CURA_ROSTER_SYNC",)


def scheduling_cura_roster_sync_enabled() -> bool:
    raw = ""
    for k in _ENV_KEYS:
        v = os.environ.get(k)
        if v is not None and str(v).strip() != "":
            raw = str(v).strip().lower()
            break
    return raw in ("1", "true", "yes", "on")


def _table_exists(cur, name: str) -> bool:
    cur.execute("SHOW TABLES LIKE %s", (name,))
    return bool(cur.fetchone())


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s LIMIT 1",
        (table, column),
    )
    return bool(cur.fetchone())


def _resolve_cura_operational_event_id(cur, shift_row: Dict[str, Any]) -> Optional[int]:
    shift_id = int(shift_row["id"])
    runsheet_id = shift_row.get("runsheet_id")

    if _column_exists(cur, "crm_event_plans", "schedule_shift_id") and _column_exists(
        cur, "crm_event_plans", "cura_operational_event_id"
    ):
        cur.execute(
            "SELECT cura_operational_event_id FROM crm_event_plans "
            "WHERE schedule_shift_id = %s AND cura_operational_event_id IS NOT NULL LIMIT 1",
            (shift_id,),
        )
        pr = cur.fetchone() or {}
        eid = pr.get("cura_operational_event_id")
        if eid is not None:
            try:
                return int(eid)
            except (TypeError, ValueError):
                pass

    ext = (shift_row.get("external_id") or "").strip()
    parsed = _parse_external_crm(ext)
    if parsed and parsed[0] == "event_plan" and _column_exists(
        cur, "crm_event_plans", "cura_operational_event_id"
    ):
        pid = int(parsed[1])
        cur.execute(
            "SELECT cura_operational_event_id FROM crm_event_plans WHERE id = %s LIMIT 1",
            (pid,),
        )
        pr2 = cur.fetchone() or {}
        eid = pr2.get("cura_operational_event_id")
        if eid is not None:
            try:
                return int(eid)
            except (TypeError, ValueError):
                pass

    if runsheet_id is not None and _column_exists(cur, "runsheets", "cura_operational_event_id"):
        cur.execute(
            "SELECT cura_operational_event_id FROM runsheets WHERE id = %s LIMIT 1",
            (int(runsheet_id),),
        )
        rr = cur.fetchone() or {}
        eid = rr.get("cura_operational_event_id")
        if eid is not None:
            try:
                return int(eid)
            except (TypeError, ValueError):
                pass
    return None


def _contractor_username(cur, contractor_id: int) -> str:
    if not _column_exists(cur, "tb_contractors", "username"):
        return ""
    cur.execute(
        "SELECT COALESCE(TRIM(username), '') AS u FROM tb_contractors WHERE id = %s LIMIT 1",
        (int(contractor_id),),
    )
    row = cur.fetchone() or {}
    return str(row.get("u") or "").strip()


def _expected_callsign_from_name(name: str) -> Optional[str]:
    n = (name or "").strip()
    if not n:
        return None
    parts = n.split()
    if len(parts) >= 2:
        a, b = parts[0], parts[1]
        if a and b:
            return (a[0] + b[0]).upper()[:64]
    if parts and parts[0]:
        return parts[0][:3].upper()[:64]
    return None


def try_sync_schedule_shift_to_cura_roster(shift_id: int) -> Dict[str, Any]:
    """
    Best-effort additive roster sync for one ``schedule_shifts`` row.
    Returns a small dict for logging/tests (not an API contract).
    """
    sid = int(shift_id)
    if not scheduling_cura_roster_sync_enabled():
        return {"ok": True, "shift_id": sid, "skipped": "env_disabled"}

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    out: Dict[str, Any] = {"ok": True, "shift_id": sid}
    try:
        if not _table_exists(cur, "cura_operational_event_assignments"):
            out["skipped"] = "no_assignments_table"
            return out

        cur.execute(
            """
            SELECT id, external_id, contractor_id, runsheet_id
            FROM schedule_shifts WHERE id = %s LIMIT 1
            """,
            (sid,),
        )
        sh = cur.fetchone()
        if not sh:
            out["skipped"] = "shift_not_found"
            return out

        event_id = _resolve_cura_operational_event_id(cur, sh)
        if not event_id:
            out["skipped"] = "no_cura_event"
            return out
        out["operational_event_id"] = int(event_id)

        assignees = _collect_assignees_for_shift(cur, sid)
        primary = _shift_primary_contractor(cur, sid)
        if not assignees and primary is not None:
            nm, em = _contractor_name_email(cur, primary)
            assignees = [
                {"contractor_id": primary, "name": nm, "email": em},
            ]

        usernames: List[str] = []
        callsigns: Dict[str, Optional[str]] = {}
        for a in assignees:
            cid = int(a["contractor_id"])
            un = _contractor_username(cur, cid)
            if not un:
                continue
            key = un.lower()
            if key in {u.lower() for u in usernames}:
                continue
            usernames.append(un)
            if _column_exists(cur, "cura_operational_event_assignments", "expected_callsign"):
                cs = _expected_callsign_from_name(str(a.get("name") or ""))
                callsigns[un] = cs

        if not usernames:
            out["skipped"] = "no_usernames"
            return out

        cur.execute(
            "SELECT principal_username FROM cura_operational_event_assignments "
            "WHERE operational_event_id = %s",
            (int(event_id),),
        )
        existing: Set[str] = set()
        for r in cur.fetchall() or []:
            pu = (r.get("principal_username") or "").strip()
            if pu:
                existing.add(pu.lower())

        added = 0
        has_cs = _column_exists(cur, "cura_operational_event_assignments", "expected_callsign")
        for un in usernames:
            if un.lower() in existing:
                continue
            exp_cs = callsigns.get(un) if has_cs else None
            if has_cs:
                cur.execute(
                    """
                    INSERT INTO cura_operational_event_assignments
                      (operational_event_id, principal_username, expected_callsign, assigned_by)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (int(event_id), un, exp_cs, "scheduling_roster_sync"),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO cura_operational_event_assignments
                      (operational_event_id, principal_username, assigned_by)
                    VALUES (%s, %s, %s)
                    """,
                    (int(event_id), un, "scheduling_roster_sync"),
                )
            added += int(cur.rowcount or 0)

        conn.commit()
        out["added"] = added
        if added:
            logger.info(
                "Cura roster sync shift_id=%s event_id=%s added=%s users=%s",
                sid,
                event_id,
                added,
                usernames,
            )
        return out
    except Exception:
        logger.exception("try_sync_schedule_shift_to_cura_roster failed shift_id=%s", sid)
        try:
            conn.rollback()
        except Exception:
            pass
        return {"ok": False, "shift_id": sid, "error": "exception"}
    finally:
        cur.close()
        conn.close()
