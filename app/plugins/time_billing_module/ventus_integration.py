"""
Ventus sign-on/off integration: create/update runsheets and schedule shifts per contractor.
Call from Ventus after sign-on/sign-off. Uses contractor_ventus_mapping and ventus_integration_defaults.

**HR compliance:** This path does **not** load HR profile expiries (`hr_staff_details`) or document
library state. Eligibility based on DBS/right-to-work/etc. is a separate product decision; see
`hr_module/compliance_integration_contract.py`.

**Timesheets / payroll:** Sign-on creates **draft** runsheets; sign-off only updates `actual_end` on
assignments and `schedule_shifts`. To push (or re-push) those times into contractor timesheets and
recompute pay, run the same step as the admin UI: `RunsheetService.publish_runsheet(runsheet_id, None)`
for each affected runsheet (admin **Publish / resync** on Edit Runsheet or list). Safe to call publish
multiple times — it upserts `source='runsheet'` rows for assignments with `payroll_included = 1`.
"""
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

from app.objects import get_db_connection


def _get_defaults() -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT client_id, job_type_id, site_id FROM ventus_integration_defaults
            WHERE active = 1 LIMIT 1
        """)
        return cur.fetchone()
    except Exception:
        return None
    finally:
        cur.close()
        conn.close()


def _contractor_id_for_callsign(callsign: str) -> Optional[int]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT contractor_id FROM contractor_ventus_mapping
            WHERE ventus_callsign = %s AND active = 1 LIMIT 1
        """, (callsign.strip().upper(),))
        row = cur.fetchone()
        return int(row["contractor_id"]) if row else None
    except Exception:
        return None
    finally:
        cur.close()
        conn.close()


def _crew_to_contractor_ids(crew: List[str]) -> List[int]:
    ids = []
    for c in (crew or []):
        callsign = (c if isinstance(c, str) else str(c)).strip().upper()
        if not callsign:
            continue
        cid = _contractor_id_for_callsign(callsign)
        if cid and cid not in ids:
            ids.append(cid)
    return ids


def on_ventus_sign_on(
    callsign: str,
    crew: List[str],
    shift_start_at: Optional[datetime] = None,
    shift_end_at: Optional[datetime] = None,
    sign_on_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Called after Ventus sign-on. Creates runsheet + assignment + schedule_shift per contractor
    (from callsign mapping and crew list). Uses ventus_integration_defaults for client/job_type.
    """
    result = {"runsheets": [], "shifts": [], "errors": []}
    defaults = _get_defaults()
    if not defaults:
        result["errors"].append("ventus_integration_defaults not set")
        return result

    contractor_ids = set()
    cid = _contractor_id_for_callsign(callsign)
    if cid:
        contractor_ids.add(cid)
    for cid2 in _crew_to_contractor_ids(crew or []):
        contractor_ids.add(cid2)

    work_date = (shift_start_at or sign_on_time or datetime.utcnow()).date()
    scheduled_start = (shift_start_at or sign_on_time or datetime.utcnow()).time() if (shift_start_at or sign_on_time) else time(0, 0)
    scheduled_end = shift_end_at.time() if shift_end_at else time(23, 59)

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        for contractor_id in contractor_ids:
            try:
                cur.execute("""
                    INSERT INTO runsheets
                    (client_id, site_id, job_type_id, work_date, window_start, window_end, status, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s)
                """, (
                    defaults["client_id"],
                    defaults.get("site_id"),
                    defaults["job_type_id"],
                    work_date,
                    scheduled_start,
                    scheduled_end,
                    "Ventus sign-on " + callsign,
                ))
                rs_id = cur.lastrowid
                cur.execute("""
                    INSERT INTO runsheet_assignments
                    (runsheet_id, user_id, scheduled_start, scheduled_end, actual_start, actual_end, notes)
                    VALUES (%s, %s, %s, %s, %s, NULL, %s)
                """, (rs_id, contractor_id, scheduled_start, scheduled_end, scheduled_start, "Ventus"))
                ra_id = cur.lastrowid
                result["runsheets"].append({"runsheet_id": rs_id, "assignment_id": ra_id, "contractor_id": contractor_id})

                try:
                    cur.execute("""
                        INSERT INTO schedule_shifts
                        (contractor_id, client_id, site_id, job_type_id, work_date,
                         scheduled_start, scheduled_end, actual_start, status, source, external_id, runsheet_id, runsheet_assignment_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'published', 'ventus', %s, %s, %s)
                    """, (
                        contractor_id,
                        defaults["client_id"],
                        defaults.get("site_id"),
                        defaults["job_type_id"],
                        work_date,
                        scheduled_start,
                        scheduled_end,
                        scheduled_start,
                        callsign,
                        rs_id,
                        ra_id,
                    ))
                    result["shifts"].append(cur.lastrowid)
                except Exception as e:
                    result["errors"].append(f"schedule_shift: {e}")
            except Exception as e:
                result["errors"].append(f"contractor {contractor_id}: {e}")
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return result


def on_ventus_sign_off(callsign: str) -> Dict[str, Any]:
    """
    Called after Ventus sign-off. Sets actual_end on runsheet_assignments and schedule_shifts
    for today's runsheets/shifts linked to this callsign.

    Does **not** call Time Billing publish — operators should **Publish / resync** those runsheets
    (or invoke `RunsheetService.publish_runsheet`) so timesheets reflect the updated actual_end.
    """
    from datetime import datetime as dt
    result = {"updated_assignments": 0, "updated_shifts": 0, "errors": []}
    cid = _contractor_id_for_callsign(callsign)
    if not cid:
        return result

    now = dt.utcnow()
    work_date = now.date()
    end_time = now.time()

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE runsheet_assignments ra
            JOIN runsheets r ON r.id = ra.runsheet_id
            SET ra.actual_end = %s
            WHERE ra.user_id = %s AND r.work_date = %s AND ra.actual_end IS NULL
        """, (end_time, cid, work_date))
        result["updated_assignments"] = cur.rowcount
        cur.execute("""
            UPDATE schedule_shifts
            SET actual_end = %s, status = 'completed'
            WHERE contractor_id = %s AND work_date = %s AND source = 'ventus' AND actual_end IS NULL
        """, (end_time, cid, work_date))
        result["updated_shifts"] = cur.rowcount
        conn.commit()
    except Exception as e:
        result["errors"].append(str(e))
        conn.rollback()
    finally:
        cur.close()
        conn.close()

    return result
