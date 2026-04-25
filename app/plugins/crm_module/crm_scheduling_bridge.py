"""CRM opportunities → scheduling (schedule_shifts) without Cura."""
from __future__ import annotations

import json
import logging
from datetime import date, time
from typing import Any

from app.objects import get_db_connection

logger = logging.getLogger(__name__)


def scheduling_stack_available() -> bool:
    try:
        from app.plugins.scheduling_module.services import ScheduleService  # noqa: F401

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shifts'")
            return bool(cur.fetchone())
        finally:
            cur.close()
            conn.close()
    except Exception:
        return False


def list_active_clients() -> list[dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, name FROM clients WHERE active = 1 ORDER BY name ASC LIMIT 500"
        )
        return cur.fetchall() or []
    except Exception:
        return []
    finally:
        cur.close()
        conn.close()


def list_job_types() -> list[dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, name FROM job_types ORDER BY name ASC LIMIT 500")
        return cur.fetchall() or []
    except Exception:
        return []
    finally:
        cur.close()
        conn.close()


def list_active_sites() -> list[dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, name, client_id FROM sites WHERE active = 1 ORDER BY name ASC LIMIT 500"
        )
        return cur.fetchall() or []
    except Exception:
        return []
    finally:
        cur.close()
        conn.close()


def _lead_meta_dict(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, str)):
        try:
            return json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return {}
    return {}


def _shift_notes_for_opportunity(
    opp_id: int,
    opp_name: str,
    lm: dict[str, Any],
) -> str:
    is_pt = lm.get("intake_kind") == "private_transfer" or (
        isinstance(lm.get("private_transfer"), dict) and lm.get("private_transfer")
    )
    tag = (
        "Private transfer / non-event — synced from CRM (not Cura)."
        if is_pt
        else "Synced from CRM to scheduling (not Cura)."
    )
    lines = [
        f"[CRM opportunity #{opp_id}] {opp_name}",
        tag,
    ]
    pt = lm.get("private_transfer") if isinstance(lm.get("private_transfer"), dict) else {}
    if pt:
        if pt.get("patient_first") or pt.get("patient_last"):
            lines.append(
                "Patient: "
                + " ".join(
                    x
                    for x in (pt.get("patient_first"), pt.get("patient_last"))
                    if x
                ).strip()
            )
        if pt.get("transfer_date"):
            lines.append(f"Transfer date: {pt.get('transfer_date')}")
        if pt.get("pickup_time"):
            lines.append(f"Pick-up time: {pt.get('pickup_time')}")
        if pt.get("journey_type"):
            lines.append(f"Journey: {pt.get('journey_type')}")
        if pt.get("pickup_address"):
            lines.append(f"From: {pt.get('pickup_address')}")
        if pt.get("destination_address"):
            lines.append(f"To: {pt.get('destination_address')}")
        if pt.get("clinical_notes"):
            lines.append(f"Notes: {pt.get('clinical_notes')}")
    return "\n".join(lines)[:60000]


def _assignment_count(cur, shift_id: int) -> int:
    cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
    if not cur.fetchone():
        return 0
    cur.execute(
        "SELECT COUNT(*) AS c FROM schedule_shift_assignments WHERE shift_id = %s",
        (int(shift_id),),
    )
    r = cur.fetchone() or {}
    return int(r.get("c") or 0)


def _bundle_shift_ids(cur, external_id: str, legacy_shift_id: int | None) -> list[int]:
    cur.execute(
        "SELECT id FROM schedule_shifts WHERE external_id = %s ORDER BY id ASC",
        (external_id,),
    )
    out = [int(r["id"]) for r in (cur.fetchall() or [])]
    if legacy_shift_id and int(legacy_shift_id) not in out:
        cur.execute(
            "SELECT id FROM schedule_shifts WHERE id = %s LIMIT 1",
            (int(legacy_shift_id),),
        )
        if cur.fetchone():
            out.append(int(legacy_shift_id))
            cur.execute(
                "UPDATE schedule_shifts SET external_id = %s, required_count = 1 WHERE id = %s",
                (external_id, int(legacy_shift_id)),
            )
    return sorted(set(out))


def sync_opportunity_to_schedule_shift(
    *,
    opportunity_id: int,
    client_id: int,
    job_type_id: int,
    work_date: date,
    scheduled_start: time,
    scheduled_end: time,
    site_id: int | None,
    break_mins: int,
    required_count: int,
    status: str,
    actor_user_id: int | None,
    actor_username: str | None,
) -> dict[str, Any]:
    """
    Create or update **N blank rota slots** (one ``schedule_shifts`` row per required staff)
    for this opportunity. All rows share ``external_id = crm_opportunity:{id}`` so **Week view →
    Job / event** groups them as one job. ``crm_opportunities.schedule_shift_id`` stores the
    first slot id for backwards compatibility.
    """
    from app.plugins.scheduling_module.services import ScheduleService

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, account_id, name, lead_meta_json, schedule_shift_id "
            "FROM crm_opportunities WHERE id=%s",
            (int(opportunity_id),),
        )
        row = cur.fetchone()
        if not row:
            return {"ok": False, "error": "Opportunity not found."}
        lm = _lead_meta_dict(row.get("lead_meta_json"))
        legacy_sid = row.get("schedule_shift_id")
        notes = _shift_notes_for_opportunity(
            int(row["id"]), str(row.get("name") or ""), lm
        )
        ext = f"crm_opportunity:{int(opportunity_id)}"
        n_slots = max(1, int(required_count or 1))
        slot_payload: dict[str, Any] = {
            "client_id": int(client_id),
            "job_type_id": int(job_type_id),
            "work_date": work_date,
            "scheduled_start": scheduled_start,
            "scheduled_end": scheduled_end,
            "break_mins": max(0, int(break_mins or 0)),
            "notes": notes,
            "status": (status or "draft").strip() or "draft",
            "source": "manual",
            "external_id": ext,
            "required_count": 1,
            "contractor_ids": [],
            "contractor_id": None,
        }
        if site_id is not None:
            slot_payload["site_id"] = int(site_id)

        update_fields = {
            k: v
            for k, v in slot_payload.items()
            if k
            in {
                "client_id",
                "site_id",
                "job_type_id",
                "work_date",
                "scheduled_start",
                "scheduled_end",
                "break_mins",
                "notes",
                "status",
            }
        }

        bundle_ids = _bundle_shift_ids(cur, ext, int(legacy_sid) if legacy_sid else None)
        conn.commit()

        for sid in bundle_ids[:n_slots]:
            try:
                ScheduleService.update_shift(
                    int(sid),
                    update_fields,
                    actor_user_id=actor_user_id,
                    actor_username=actor_username,
                )
            except Exception as ex:
                logger.exception("update_shift CRM opp %s sid=%s", opportunity_id, sid)
                return {"ok": False, "error": str(ex)[:2000]}

        while len(bundle_ids) < n_slots:
            try:
                new_id = ScheduleService.create_shift(
                    dict(slot_payload),
                    actor_user_id=actor_user_id,
                    actor_username=actor_username,
                )
                bundle_ids.append(int(new_id))
                bundle_ids.sort()
            except Exception as ex:
                logger.exception("create_shift CRM opp %s", opportunity_id)
                return {"ok": False, "error": str(ex)[:2000]}

        if len(bundle_ids) > n_slots:
            extras = bundle_ids[n_slots:]
            scored = sorted(
                extras,
                key=lambda sid: (_assignment_count(cur, int(sid)), -int(sid)),
            )
            to_remove = scored[: len(bundle_ids) - n_slots]
            for sid in to_remove:
                try:
                    ScheduleService.delete_shift(int(sid), "this")
                except Exception:
                    logger.exception("delete_shift CRM opp extra sid=%s", sid)
            bundle_ids = [x for x in bundle_ids if x not in to_remove]
            bundle_ids.sort()

        cur.execute(
            "SELECT id FROM schedule_shifts WHERE external_id = %s ORDER BY id ASC",
            (ext,),
        )
        final_ids = [int(r["id"]) for r in (cur.fetchall() or [])]
        first = final_ids[0] if final_ids else None
        if first is not None:
            cur.execute(
                "UPDATE crm_opportunities SET schedule_shift_id=%s WHERE id=%s",
                (int(first), int(opportunity_id)),
            )
            conn.commit()
        return {"ok": True, "shift_id": first, "shift_ids": final_ids}
    finally:
        cur.close()
        conn.close()


def sync_event_plan_to_schedule_shift(
    *,
    plan_id: int,
    client_id: int,
    job_type_id: int,
    work_date: date,
    scheduled_start: time,
    scheduled_end: time,
    site_id: int | None = None,
    break_mins: int = 0,
    required_count: int = 1,
    status: str = "published",
    actor_user_id: int | None = None,
    actor_username: str | None = None,
) -> dict[str, Any]:
    """
    Create or update **N blank rota slots** for a CRM event plan (non-Cura path).

    All rows share ``external_id = crm_event_plan:{plan_id}`` for **Job / event** week grouping.
    ``crm_event_plans.schedule_shift_id`` stores the first slot when that column exists.
    """
    from app.plugins.scheduling_module.services import ScheduleService

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW COLUMNS FROM crm_event_plans LIKE 'schedule_shift_id'")
        has_plan_shift_col = bool(cur.fetchone())
        sel = (
            "SELECT id, title, schedule_shift_id FROM crm_event_plans WHERE id=%s"
            if has_plan_shift_col
            else "SELECT id, title FROM crm_event_plans WHERE id=%s"
        )
        cur.execute(sel, (int(plan_id),))
        plan = cur.fetchone()
        if not plan:
            return {"ok": False, "error": "Event plan not found."}
        title = (plan.get("title") or "").strip() or f"Plan #{plan_id}"
        legacy_sid = plan.get("schedule_shift_id") if has_plan_shift_col else None

        notes = (
            f"[CRM event plan #{plan_id}] {title}\n"
            "Synced to scheduling. Assign each slot in Scheduling week view (Job / event or Staff)."
        )
        ext = f"crm_event_plan:{int(plan_id)}"
        n_slots = max(1, int(required_count or 1))
        slot_payload: dict[str, Any] = {
            "client_id": int(client_id),
            "job_type_id": int(job_type_id),
            "work_date": work_date,
            "scheduled_start": scheduled_start,
            "scheduled_end": scheduled_end,
            "break_mins": max(0, int(break_mins or 0)),
            "notes": notes[:60000],
            "status": (status or "draft").strip() or "draft",
            "source": "manual",
            "external_id": ext,
            "required_count": 1,
            "contractor_ids": [],
            "contractor_id": None,
        }
        if site_id is not None:
            slot_payload["site_id"] = int(site_id)

        update_fields = {
            k: v
            for k, v in slot_payload.items()
            if k
            in {
                "client_id",
                "site_id",
                "job_type_id",
                "work_date",
                "scheduled_start",
                "scheduled_end",
                "break_mins",
                "notes",
                "status",
            }
        }

        bundle_ids = _bundle_shift_ids(
            cur, ext, int(legacy_sid) if legacy_sid else None
        )
        conn.commit()

        for sid in bundle_ids[:n_slots]:
            try:
                ScheduleService.update_shift(
                    int(sid),
                    update_fields,
                    actor_user_id=actor_user_id,
                    actor_username=actor_username,
                )
            except Exception as ex:
                logger.exception("update_shift CRM plan %s sid=%s", plan_id, sid)
                return {"ok": False, "error": str(ex)[:2000]}

        while len(bundle_ids) < n_slots:
            try:
                new_id = ScheduleService.create_shift(
                    dict(slot_payload),
                    actor_user_id=actor_user_id,
                    actor_username=actor_username,
                )
                bundle_ids.append(int(new_id))
                bundle_ids.sort()
            except Exception as ex:
                logger.exception("create_shift CRM plan %s", plan_id)
                return {"ok": False, "error": str(ex)[:2000]}

        if len(bundle_ids) > n_slots:
            extras = bundle_ids[n_slots:]
            scored = sorted(
                extras,
                key=lambda sid: (_assignment_count(cur, int(sid)), -int(sid)),
            )
            to_remove = scored[: len(bundle_ids) - n_slots]
            for sid in to_remove:
                try:
                    ScheduleService.delete_shift(int(sid), "this")
                except Exception:
                    logger.exception("delete_shift CRM plan extra sid=%s", sid)
            bundle_ids = [x for x in bundle_ids if x not in to_remove]
            bundle_ids.sort()

        cur.execute(
            "SELECT id FROM schedule_shifts WHERE external_id = %s ORDER BY id ASC",
            (ext,),
        )
        final_ids = [int(r["id"]) for r in (cur.fetchall() or [])]
        first = final_ids[0] if final_ids else None
        if has_plan_shift_col and first is not None:
            cur.execute(
                "UPDATE crm_event_plans SET schedule_shift_id=%s WHERE id=%s",
                (int(first), int(plan_id)),
            )
            conn.commit()
        return {"ok": True, "shift_id": first, "shift_ids": final_ids}
    finally:
        cur.close()
        conn.close()
