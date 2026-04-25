"""
Push scheduling roster changes back into CRM (no Cura required).

When ``schedule_shifts.external_id`` is:
  - ``crm_opportunity:{id}`` → merges ``scheduling_sync`` into ``crm_opportunities.lead_meta_json``
    (private transfer / non-event path; medical jobs without Cura).
  - ``crm_event_plan:{id}`` → updates ``crm_event_plans.staff_roster_json`` with assignee rows
    suitable for event plan PDFs (hospitality / cleaning style jobs).

Also resolves plans linked via ``crm_event_plans.schedule_shift_id`` when that column exists.

**Cura roster (optional):** additive INSERTs into ``cura_operational_event_assignments`` are handled
by ``medical_records_module.scheduling_cura_roster_sync`` when
``SPARROW_SCHEDULING_CURA_ROSTER_SYNC`` is enabled; scheduling calls it after this CRM propagate step.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Set

from app.objects import get_db_connection

logger = logging.getLogger(__name__)


def _parse_external_crm(ext: Optional[str]) -> Optional[tuple[str, int]]:
    if not ext or not isinstance(ext, str):
        return None
    s = ext.strip()
    if s.startswith("crm_opportunity:"):
        try:
            return ("opportunity", int(s.split(":", 1)[1]))
        except (ValueError, IndexError):
            return None
    if s.startswith("crm_event_plan:"):
        try:
            return ("event_plan", int(s.split(":", 1)[1]))
        except (ValueError, IndexError):
            return None
    return None


def _coerce_json_dict(raw: Any) -> Dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return {}
        try:
            v = json.loads(s)
            return dict(v) if isinstance(v, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _row_get(row: Any, key: str, idx: int = 0) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes)):
        if 0 <= idx < len(row):
            return row[idx]
    return None


def _collect_assignees_for_shift(cur, shift_id: int) -> List[Dict[str, Any]]:
    cur.execute(
        "SHOW TABLES LIKE 'schedule_shift_assignments'"
    )
    if not cur.fetchone():
        return []
    cur.execute(
        """
        SELECT a.contractor_id, COALESCE(u.name, '') AS contractor_name,
               COALESCE(u.email, '') AS contractor_email
        FROM schedule_shift_assignments a
        INNER JOIN tb_contractors u ON u.id = a.contractor_id
        WHERE a.shift_id = %s
        ORDER BY a.contractor_id ASC
        """,
        (int(shift_id),),
    )
    rows = cur.fetchall() or []
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "contractor_id": int(r["contractor_id"]),
                "name": (r.get("contractor_name") or "").strip(),
                "email": (r.get("contractor_email") or "").strip(),
            }
        )
    return out


def _shift_primary_contractor(cur, shift_id: int) -> Optional[int]:
    cur.execute(
        "SELECT contractor_id FROM schedule_shifts WHERE id = %s LIMIT 1",
        (int(shift_id),),
    )
    row = cur.fetchone()
    if not row:
        return None
    cid = _row_get(row, "contractor_id", 0)
    try:
        return int(cid) if cid is not None else None
    except (TypeError, ValueError):
        return None


def _contractor_name_email(cur, contractor_id: int) -> tuple[str, str]:
    cur.execute(
        "SELECT COALESCE(name,'') AS n, COALESCE(email,'') AS e FROM tb_contractors WHERE id=%s LIMIT 1",
        (int(contractor_id),),
    )
    row = cur.fetchone() or {}
    return (str(row.get("n") or "").strip(), str(row.get("e") or "").strip())


def propagate_schedule_shift_to_crm(shift_id: int) -> None:
    """
    Best-effort: update CRM artefacts from ``schedule_shifts`` + assignments.
    Safe to call on every shift mutation; failures are logged only.
    """
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, external_id, contractor_id, work_date, scheduled_start, scheduled_end,
                   status, client_id
            FROM schedule_shifts WHERE id = %s LIMIT 1
            """,
            (int(shift_id),),
        )
        sh = cur.fetchone()
        if not sh:
            return
        ext = (sh.get("external_id") or "").strip()
        parsed = _parse_external_crm(ext)
        plan_ids: List[int] = []
        if parsed and parsed[0] == "event_plan":
            plan_ids.append(parsed[1])
        cur.execute("SHOW COLUMNS FROM crm_event_plans LIKE 'schedule_shift_id'")
        if cur.fetchone():
            cur.execute(
                "SELECT id FROM crm_event_plans WHERE schedule_shift_id = %s",
                (int(shift_id),),
            )
            for pr in cur.fetchall() or []:
                pid = int(pr["id"])
                if pid not in plan_ids:
                    plan_ids.append(pid)
        if not parsed and not plan_ids:
            return

        bundle_ids: List[int] = [int(shift_id)]
        if ext:
            cur.execute(
                "SELECT id FROM schedule_shifts WHERE external_id = %s ORDER BY id ASC",
                (ext,),
            )
            rows_ext = cur.fetchall() or []
            if rows_ext:
                bundle_ids = [int(r["id"]) for r in rows_ext]

        shifts_sync: List[Dict[str, Any]] = []
        roster_pdf_rows: List[Dict[str, Any]] = []
        seen_roster: Set[int] = set()

        for sid in bundle_ids:
            cur.execute(
                """
                SELECT id, external_id, contractor_id, work_date, scheduled_start, scheduled_end,
                       status, client_id
                FROM schedule_shifts WHERE id = %s LIMIT 1
                """,
                (int(sid),),
            )
            shr = cur.fetchone()
            if not shr:
                continue
            assignees = _collect_assignees_for_shift(cur, int(sid))
            primary = _shift_primary_contractor(cur, int(sid))
            if not assignees and primary is not None:
                nm, em = _contractor_name_email(cur, primary)
                assignees = [
                    {
                        "contractor_id": primary,
                        "name": nm,
                        "email": em,
                    }
                ]
            wd = shr.get("work_date")
            if hasattr(wd, "isoformat"):
                wd_s = wd.isoformat()[:10]
            else:
                wd_s = str(wd or "")[:10]
            st = shr.get("scheduled_start")
            en = shr.get("scheduled_end")
            st_s = st.strftime("%H:%M") if hasattr(st, "strftime") else str(st or "")
            en_s = en.strftime("%H:%M") if hasattr(en, "strftime") else str(en or "")
            shifts_sync.append(
                {
                    "shift_id": int(sid),
                    "work_date": wd_s,
                    "scheduled_start": st_s,
                    "scheduled_end": en_s,
                    "status": (shr.get("status") or "").strip(),
                    "assignees": assignees,
                }
            )
            for a in assignees:
                cid = int(a["contractor_id"])
                if cid in seen_roster:
                    continue
                seen_roster.add(cid)
                nm = (a.get("name") or "").strip()
                em = (a.get("email") or "").strip()
                roster_pdf_rows.append(
                    {
                        "name": nm or f"Staff #{cid}",
                        "role_grade": "Rota",
                        "phone": "",
                        "notes": (f"Email: {em}" if em else f"Contractor id {cid}")[:512],
                    }
                )

        if parsed and parsed[0] == "opportunity":
            opp_id = parsed[1]
            cur.execute(
                "SELECT lead_meta_json FROM crm_opportunities WHERE id = %s LIMIT 1",
                (opp_id,),
            )
            row = cur.fetchone()
            raw_lm = _row_get(row, "lead_meta_json", 0) if row else None
            lm = _coerce_json_dict(raw_lm)
            lm["scheduling_sync"] = {
                "version": 2,
                "bundle_external_id": ext,
                "updated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                "shifts": shifts_sync,
            }
            cur.execute(
                "UPDATE crm_opportunities SET lead_meta_json = %s WHERE id = %s",
                (json.dumps(lm), opp_id),
            )

        if plan_ids:
            payload = json.dumps(roster_pdf_rows)
            for pid in plan_ids:
                cur.execute(
                    "UPDATE crm_event_plans SET staff_roster_json = %s WHERE id = %s",
                    (payload, int(pid)),
                )
        conn.commit()
    except Exception:
        logger.exception("propagate_schedule_shift_to_crm failed shift_id=%s", shift_id)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()
