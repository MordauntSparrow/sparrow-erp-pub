"""
Work module: my day from scheduling, record times/notes, sync to time billing.
Visit records (`work_visits`) give a stable id per stop for client/billing/evidence.
"""
import logging
from datetime import date, time
from typing import Any, Dict, List, Optional

from mysql.connector import errors as mysql_errors

from app.objects import get_db_connection

logger = logging.getLogger(__name__)

# Lazy import to avoid circular dependency; only needed when syncing to TB
def _get_schedule_service():
    from app.plugins.scheduling_module.services import ScheduleService
    return ScheduleService


def list_stops_admin(
    contractor_id: Optional[int] = None,
    client_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """List shifts (recorded stops) for admin with photo count."""
    shifts = _get_schedule_service().list_shifts(
        contractor_id=contractor_id,
        client_id=client_id,
        date_from=date_from,
        date_to=date_to,
        status=None,
    )
    if _work_visits_table_exists():
        for s in shifts:
            ensure_visit_for_shift(int(s["id"]))
    enrich_stops_with_visit_ids(shifts)
    for s in shifts:
        s["photo_count"] = _photo_count_for_shift(s["id"])
    return shifts


def _photo_count_for_shift(shift_id: int) -> int:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM work_photos WHERE shift_id = %s", (shift_id,))
        return cur.fetchone()[0] or 0
    finally:
        cur.close()
        conn.close()


def _work_visits_table_exists() -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'work_visits'")
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def _visit_map_for_shift_ids(shift_ids: List[int]) -> Dict[int, int]:
    if not shift_ids or not _work_visits_table_exists():
        return {}
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        ph = ",".join(["%s"] * len(shift_ids))
        cur.execute(
            f"SELECT schedule_shift_id AS sid, id AS visit_id FROM work_visits WHERE schedule_shift_id IN ({ph})",
            tuple(shift_ids),
        )
        rows = cur.fetchall() or []
        return {int(r["sid"]): int(r["visit_id"]) for r in rows}
    except Exception as e:
        logger.warning("work_visits map: %s", e)
        return {}
    finally:
        cur.close()
        conn.close()


def enrich_stops_with_visit_ids(stops: List[Dict[str, Any]]) -> None:
    """Mutates each stop dict with optional visit_id (stable client visit key)."""
    if not stops:
        return
    ids = [int(s["id"]) for s in stops]
    m = _visit_map_for_shift_ids(ids)
    for s in stops:
        s["visit_id"] = m.get(int(s["id"]))


def ensure_visit_for_shift(shift_id: int) -> Optional[int]:
    """
    Ensure a work_visits row exists for this schedule shift (1:1).
    Returns visit id or None if shift missing / table not installed.
    """
    if not shift_id or not _work_visits_table_exists():
        return None
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id FROM work_visits WHERE schedule_shift_id = %s", (shift_id,)
        )
        row = cur.fetchone()
        if row:
            return int(row["id"])
        cur.execute(
            """
            SELECT id, client_id, site_id, job_type_id, contractor_id, work_date,
                   runsheet_assignment_id, actual_end
            FROM schedule_shifts WHERE id = %s
            """,
            (shift_id,),
        )
        sh = cur.fetchone()
        if not sh:
            return None
        status = "completed" if sh.get("actual_end") else "open"
        try:
            cur.execute(
                """
                INSERT INTO work_visits (
                  schedule_shift_id, client_id, site_id, job_type_id, contractor_id, work_date,
                  runsheet_assignment_id, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    shift_id,
                    sh["client_id"],
                    sh.get("site_id"),
                    sh["job_type_id"],
                    sh["contractor_id"],
                    sh["work_date"],
                    sh.get("runsheet_assignment_id"),
                    status,
                ),
            )
            vid = cur.lastrowid
            conn.commit()
            return int(vid) if vid else None
        except mysql_errors.IntegrityError:
            conn.rollback()
            cur.execute(
                "SELECT id FROM work_visits WHERE schedule_shift_id = %s", (shift_id,)
            )
            row2 = cur.fetchone()
            return int(row2["id"]) if row2 else None
    except Exception as e:
        logger.exception("ensure_visit_for_shift: %s", e)
        conn.rollback()
        return None
    finally:
        cur.close()
        conn.close()


def sync_visit_from_shift(shift_id: int) -> None:
    """Refresh denormalised visit fields from schedule_shifts."""
    if not shift_id or not _work_visits_table_exists():
        return
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE work_visits v
            INNER JOIN schedule_shifts ss ON ss.id = v.schedule_shift_id
            SET v.client_id = ss.client_id,
                v.site_id = ss.site_id,
                v.job_type_id = ss.job_type_id,
                v.contractor_id = ss.contractor_id,
                v.work_date = ss.work_date,
                v.runsheet_assignment_id = ss.runsheet_assignment_id,
                v.status = CASE WHEN ss.actual_end IS NOT NULL THEN 'completed' ELSE 'open' END
            WHERE v.schedule_shift_id = %s
            """,
            (shift_id,),
        )
        conn.commit()
    except Exception as e:
        logger.warning("sync_visit_from_shift: %s", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def _touch_visit_after_shift_sync(shift_id: int) -> None:
    if not _work_visits_table_exists():
        return
    try:
        ensure_visit_for_shift(shift_id)
        sync_visit_from_shift(shift_id)
    except Exception as e:
        logger.warning("_touch_visit_after_shift_sync: %s", e)


def ensure_visits_for_shifts_in_range(
    contractor_id: Optional[int] = None,
    client_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> int:
    """
    Create missing work_visits rows for schedule_shifts in the given filters.
    Returns number of ensure calls made (not necessarily new rows).
    """
    if not _work_visits_table_exists() or not date_from or not date_to:
        return 0
    shifts = _get_schedule_service().list_shifts(
        contractor_id=contractor_id,
        client_id=client_id,
        date_from=date_from,
        date_to=date_to,
        status=None,
    )
    n = 0
    for s in shifts:
        ensure_visit_for_shift(int(s["id"]))
        n += 1
    return n


def list_visits_admin(
    contractor_id: Optional[int] = None,
    client_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """List visit records (client account anchor) with shift actuals and photo count."""
    if not _work_visits_table_exists():
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["1=1"]
        params: List[Any] = []
        if contractor_id is not None:
            where.append("v.contractor_id = %s")
            params.append(contractor_id)
        if client_id is not None:
            where.append("v.client_id = %s")
            params.append(client_id)
        if date_from is not None:
            where.append("v.work_date >= %s")
            params.append(date_from)
        if date_to is not None:
            where.append("v.work_date <= %s")
            params.append(date_to)
        cur.execute(
            f"""
            SELECT v.id AS visit_id, v.schedule_shift_id, v.client_id, v.site_id, v.job_type_id,
                   v.contractor_id, v.work_date, v.status, v.runsheet_assignment_id, v.created_at,
                   c.name AS client_name, s.name AS site_name, u.name AS contractor_name,
                   jt.name AS job_type_name,
                   ss.actual_start, ss.actual_end, ss.notes AS shift_notes,
                   (SELECT COUNT(*) FROM work_photos p WHERE p.shift_id = v.schedule_shift_id) AS photo_count
            FROM work_visits v
            JOIN schedule_shifts ss ON ss.id = v.schedule_shift_id
            JOIN clients c ON c.id = v.client_id
            LEFT JOIN sites s ON s.id = v.site_id
            JOIN tb_contractors u ON u.id = v.contractor_id
            LEFT JOIN job_types jt ON jt.id = v.job_type_id
            WHERE {" AND ".join(where)}
            ORDER BY v.work_date DESC, v.id DESC
            """,
            params,
        )
        return cur.fetchall() or []
    except Exception as e:
        logger.warning("list_visits_admin: %s", e)
        return []
    finally:
        cur.close()
        conn.close()


def list_gaps(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    contractor_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Shifts that are published but have no actual_start or no actual_end (missing clock)."""
    shifts = _get_schedule_service().list_shifts(
        contractor_id=contractor_id,
        date_from=date_from,
        date_to=date_to,
        status="published",
    )
    enrich_stops_with_visit_ids(shifts)
    gaps = []
    for s in shifts:
        if s.get("actual_start") is None or s.get("actual_end") is None:
            s["photo_count"] = _photo_count_for_shift(s["id"])
            gaps.append(s)
    return gaps


def get_shift_for_admin(shift_id: int) -> Optional[Dict[str, Any]]:
    """Get any shift by id (admin)."""
    return _get_schedule_service().get_shift(shift_id)


def update_shift_times_admin(
    shift_id: int,
    actual_start: Optional[time] = None,
    actual_end: Optional[time] = None,
    notes: Optional[str] = None,
) -> bool:
    """Admin override: set actual times/notes and re-sync to Time Billing."""
    shift = _get_schedule_service().get_shift(shift_id)
    if not shift:
        return False
    updates = {}
    if actual_start is not None:
        updates["actual_start"] = actual_start
    if actual_end is not None:
        updates["actual_end"] = actual_end
    if notes is not None:
        updates["notes"] = notes
    if not updates:
        return True
    updates["status"] = "completed" if (actual_end or shift.get("actual_end")) else "in_progress"
    _get_schedule_service().update_shift(shift_id, updates)
    sync_shift_to_time_billing(shift_id)
    return True


def list_photos_admin(
    contractor_id: Optional[int] = None,
    shift_id: Optional[int] = None,
    visit_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """List photos with optional filters; join shift for client/date."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["1=1"]
        params: List[Any] = []
        if contractor_id is not None:
            where.append("p.contractor_id = %s")
            params.append(contractor_id)
        if shift_id is not None:
            where.append("p.shift_id = %s")
            params.append(shift_id)
        if visit_id is not None and _work_visits_table_exists():
            where.append(
                "EXISTS (SELECT 1 FROM work_visits v WHERE v.schedule_shift_id = p.shift_id AND v.id = %s)"
            )
            params.append(visit_id)
        if date_from is not None:
            where.append("s.work_date >= %s")
            params.append(date_from)
        if date_to is not None:
            where.append("s.work_date <= %s")
            params.append(date_to)
        cur.execute(f"""
            SELECT p.id, p.shift_id, p.contractor_id, p.file_path, p.file_name, p.caption, p.created_at,
                   s.work_date, s.client_id, c.name AS client_name, u.name AS contractor_name,
                   (SELECT v.id FROM work_visits v WHERE v.schedule_shift_id = p.shift_id LIMIT 1) AS visit_id
            FROM work_photos p
            JOIN schedule_shifts s ON s.id = p.shift_id
            JOIN clients c ON c.id = s.client_id
            JOIN tb_contractors u ON u.id = p.contractor_id
            WHERE {" AND ".join(where)}
            ORDER BY p.created_at DESC
        """, params)
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def report_hours(
    date_from: date,
    date_to: date,
    contractor_id: Optional[int] = None,
    client_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Hours worked (from actual_start/actual_end) by shift; for CSV/reporting."""
    shifts = _get_schedule_service().list_shifts(
        contractor_id=contractor_id,
        client_id=client_id,
        date_from=date_from,
        date_to=date_to,
        status=None,
    )
    enrich_stops_with_visit_ids(shifts)
    rows = []
    for s in shifts:
        if s.get("actual_start") is None and s.get("actual_end") is None:
            continue
        start = s.get("actual_start")
        end = s.get("actual_end")
        hours = None
        if start and end and hasattr(start, "hour") and hasattr(end, "hour"):
            from datetime import datetime as dt
            d = date.today()
            hours = (dt.combine(d, end) - dt.combine(d, start)).total_seconds() / 3600.0
            if hours < 0:
                hours += 24
        start_str = start.strftime("%H:%M") if start and hasattr(start, "strftime") else (str(start)[:5] if start else "")
        end_str = end.strftime("%H:%M") if end and hasattr(end, "strftime") else (str(end)[:5] if end else "")
        sid = int(s["id"])
        rows.append({
            "shift_id": sid,
            "visit_id": s.get("visit_id"),
            "work_date": s.get("work_date"),
            "contractor_name": s.get("contractor_name"),
            "client_name": s.get("client_name"),
            "site_name": s.get("site_name"),
            "actual_start": start,
            "actual_end": end,
            "actual_start_str": start_str,
            "actual_end_str": end_str,
            "hours": round(hours, 2) if hours is not None else None,
            "notes": s.get("notes"),
        })
    return rows


def get_my_stops_for_today(contractor_id: int) -> List[Dict[str, Any]]:
    today = date.today()
    return _get_schedule_service().get_my_shifts_for_date(contractor_id, today)


def get_shift_for_stop(shift_id: int, contractor_id: int) -> Optional[Dict[str, Any]]:
    shift = _get_schedule_service().get_shift(shift_id)
    if not shift or shift["contractor_id"] != contractor_id:
        return None
    return shift


def record_stop(shift_id: int, contractor_id: int, actual_start: Optional[time] = None, actual_end: Optional[time] = None, notes: Optional[str] = None) -> bool:
    shift = get_shift_for_stop(shift_id, contractor_id)
    if not shift:
        return False
    _get_schedule_service().record_actual_times(shift_id, actual_start=actual_start, actual_end=actual_end, notes=notes)
    sync_shift_to_time_billing(shift_id)
    return True


def _sync_shift_to_time_billing_impl(shift_id: int) -> None:
    """
    If the shift is linked to a runsheet assignment, update that assignment and
    the corresponding timesheet entry. If the shift has no runsheet yet (scheduler-only),
    create a runsheet + assignment and publish so the timesheet is autofilled.
    """
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, contractor_id, client_id, site_id, job_type_id, work_date,
                   actual_start, actual_end, notes, runsheet_id, runsheet_assignment_id
            FROM schedule_shifts WHERE id = %s
        """, (shift_id,))
        shift = cur.fetchone()
        if not shift:
            return
        if not shift.get("runsheet_id"):
            cur.close()
            conn.close()
            from app.plugins.time_billing_module.services import RunsheetService
            RunsheetService.create_and_publish_runsheet_for_shift(shift_id)
            return
        if not shift.get("runsheet_assignment_id"):
            return
        ra_id = shift["runsheet_assignment_id"]
        rs_id = shift["runsheet_id"]
        user_id = shift["contractor_id"]
        work_date = shift["work_date"]
        actual_start = shift.get("actual_start")
        actual_end = shift.get("actual_end")
        notes = shift.get("notes")
        cur.execute("""
            UPDATE runsheet_assignments
            SET actual_start = %s, actual_end = %s, notes = %s
            WHERE id = %s
        """, (actual_start, actual_end, notes, ra_id))
        iso_year, iso_week, _ = work_date.isocalendar()
        week_id_str = f"{iso_year}{iso_week:02d}"
        cur.execute("SELECT id FROM tb_timesheet_weeks WHERE user_id = %s AND week_id = %s", (user_id, week_id_str))
        wk = cur.fetchone()
        if not wk:
            conn.commit()
            return
        week_pk = wk["id"]
        cur.execute("""
            UPDATE tb_timesheet_entries
            SET actual_start = COALESCE(%s, actual_start),
                actual_end = COALESCE(%s, actual_end),
                notes = COALESCE(%s, notes)
            WHERE week_id = %s AND user_id = %s AND work_date = %s AND source = 'runsheet' AND runsheet_id = %s
        """, (actual_start, actual_end, notes, week_pk, user_id, work_date, rs_id))
        from app.plugins.time_billing_module.services import TimesheetService
        TimesheetService.refresh_entries_actuals(cur, conn, week_pk, user_id, work_date, rs_id)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def sync_shift_to_time_billing(shift_id: int) -> None:
    try:
        _sync_shift_to_time_billing_impl(shift_id)
    finally:
        _touch_visit_after_shift_sync(shift_id)


def add_photo(shift_id: int, contractor_id: int, file_path: str, file_name: Optional[str] = None, mime_type: Optional[str] = None, caption: Optional[str] = None) -> int:
    visit_id = ensure_visit_for_shift(shift_id)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if visit_id and _column_exists_work_photos_visit_id():
            cur.execute(
                """
                INSERT INTO work_photos (shift_id, visit_id, contractor_id, file_path, file_name, mime_type, caption)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (shift_id, visit_id, contractor_id, file_path, file_name, mime_type, caption),
            )
        else:
            cur.execute(
                """
                INSERT INTO work_photos (shift_id, contractor_id, file_path, file_name, mime_type, caption)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (shift_id, contractor_id, file_path, file_name, mime_type, caption),
            )
        conn.commit()
        return cur.lastrowid
    finally:
        cur.close()
        conn.close()


def _column_exists_work_photos_visit_id() -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'work_photos' AND COLUMN_NAME = 'visit_id'
            """
        )
        row = cur.fetchone()
        return bool(row and row[0])
    finally:
        cur.close()
        conn.close()


def list_photos_for_shift(shift_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if _column_exists_work_photos_visit_id():
            cur.execute(
                """
                SELECT id, file_path, file_name, caption, created_at, visit_id
                FROM work_photos WHERE shift_id = %s ORDER BY created_at
                """,
                (shift_id,),
            )
        else:
            cur.execute(
                "SELECT id, file_path, file_name, caption, created_at FROM work_photos WHERE shift_id = %s ORDER BY created_at",
                (shift_id,),
            )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
