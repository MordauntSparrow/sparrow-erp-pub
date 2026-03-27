from datetime import datetime, date, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional, Tuple
import json
import os
import csv
import io
import re
from app.objects import get_db_connection, EmailManager

# ----------------- Helpers -----------------


def _dec(v, q='0.01') -> Decimal:
    """
    Convert a value to Decimal and round using ROUND_HALF_UP.

    Args:
        v: Value to convert (None treated as 0)
        q: Quantization precision (default '0.01')
    Returns:
        Decimal rounded to the given precision
    """
    return Decimal(str(v if v is not None else 0)).quantize(Decimal(q), rounding=ROUND_HALF_UP)


def _to_time(val: Any) -> time:
    """
    Convert a string or time object to a time object.

    Args:
        val: time object or string in "HH:MM" or "HH:MM:SS" format
    Returns:
        time object
    Raises:
        ValueError if input is invalid
    """
    if isinstance(val, time):
        return val
    if isinstance(val, str) and val:
        parts = [int(p) for p in val.split(':')]
        if len(parts) == 2:
            return time(parts[0], parts[1], 0)
        if len(parts) == 3:
            return time(parts[0], parts[1], parts[2])
    raise ValueError(f"Invalid time value: {val}")


def _hours_between(t1: time, t2: time, break_mins: int = 0) -> float:
    """
    Calculate hours between two times, accounting for breaks and cross-midnight shifts.

    Args:
        t1: start time
        t2: end time
        break_mins: break duration in minutes
    Returns:
        Hours as float
    """
    d0 = datetime.combine(date(2000, 1, 1), t1)
    d1 = datetime.combine(date(2000, 1, 1), t2)
    if d1 <= d0:
        d1 += timedelta(days=1)  # handle cross-midnight
    mins = (d1 - d0).total_seconds() / 60.0
    mins -= break_mins or 0
    return max(0.0, mins / 60.0)


def _now_utc_str() -> str:
    """Return current UTC datetime as a string."""
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


# ----------------- Rate Resolver -----------------

class MinimalRateResolver:
    @staticmethod
    def get_contractor_rate_card_id(contractor_id: int) -> Optional[int]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT wage_rate_card_id FROM tb_contractors WHERE id=%s", (contractor_id,))
            row = cur.fetchone()
            return row.get("wage_rate_card_id") if row else None
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def resolve_rate(contractor_id: int, job_type_id: int, on_date: date) -> Decimal:
        """
        Return the effective wage rate from wage_rate_rows.
        If no row matches, return Decimal('0.00').
        """
        rate_card_id = MinimalRateResolver.get_contractor_rate_card_id(
            contractor_id)
        if not rate_card_id:
            return _dec(0)

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT rate
                FROM wage_rate_rows
                WHERE rate_card_id=%s
                AND job_type_id=%s
                AND effective_from <= %s
                AND (effective_to IS NULL OR effective_to >= %s)
                ORDER BY effective_from DESC, id DESC
                LIMIT 1
            """, (rate_card_id, int(job_type_id), on_date, on_date))
            r = cur.fetchone()
            return _dec(r["rate"]) if r and r.get("rate") is not None else _dec(0)
        finally:
            cur.close()
            conn.close()


class RateResolver:
    """
    Resolves effective wage rates for timesheet entries.

    Resolution hierarchy:
    1. Contractor-client-job overrides
    2. Contractor override
    3. Contractor wage rate card (effective-dated per job type)
    4. Role default wage rate card

    Policies applied:
    - Weekend (WEEKEND)
    - Bank Holiday (BANK_HOLIDAY)
    - Night (NIGHT)
    - Overtime (OVERTIME_SHIFT)

    Combination rule:
    - Choose max-of(Base, Weekend, BH, Night)
    - OT applies on top if applicable
    """

    @staticmethod
    def _fetch_contractor(contractor_id: int) -> Dict[str, Any]:
        """Fetch contractor data from the database."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, role_id, wage_rate_card_id, wage_rate_override "
                "FROM tb_contractors WHERE id=%s", (contractor_id,)
            )
            return cur.fetchone() or {}
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _base_rate(contractor_id: int, job_type_id: int, work_date: date, client_name: Optional[int]) -> Decimal:
        """
        Determine the base rate using hierarchy of overrides and wage cards.
        """
        # 1) Contractor-client override (job-specific preferred)
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT wage_rate_override AS rate
                FROM contractor_client_overrides
                WHERE contractor_id=%s AND client_name=%s
                AND (job_type_id IS NULL OR job_type_id=%s)
                AND effective_from <= %s AND (effective_to IS NULL OR effective_to >= %s)
                ORDER BY (job_type_id IS NOT NULL) DESC, effective_from DESC
                LIMIT 1
            """, (contractor_id, client_name, job_type_id, work_date, work_date))
            r = cur.fetchone()
            if r and r.get('rate') is not None:
                return _dec(r['rate'])
        finally:
            cur.close()
            conn.close()

        # 2) Contractor override
        c = RateResolver._fetch_contractor(contractor_id)
        if c.get('wage_rate_override') is not None:
            return _dec(c['wage_rate_override'])

        # 3) Contractor card
        if c.get('wage_rate_card_id'):
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute("""
                    SELECT rate FROM wage_rate_rows
                    WHERE rate_card_id=%s AND job_type_id=%s
                    AND effective_from <= %s AND (effective_to IS NULL OR effective_to >= %s)
                    ORDER BY effective_from DESC LIMIT 1
                """, (c['wage_rate_card_id'], job_type_id, work_date, work_date))
                r = cur.fetchone()
                if r and r.get('rate') is not None:
                    return _dec(r['rate'])
            finally:
                cur.close()
                conn.close()

        # 4) Role default card
        if c.get('role_id'):
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute(
                    "SELECT id FROM wage_rate_cards WHERE role_id=%s AND active=1 ORDER BY id ASC LIMIT 1",
                    (c['role_id'],)
                )
                rc = cur.fetchone()
                if rc and rc.get('id'):
                    cur.execute("""
                        SELECT rate FROM wage_rate_rows
                        WHERE rate_card_id=%s AND job_type_id=%s
                        AND effective_from <= %s AND (effective_to IS NULL OR effective_to >= %s)
                        ORDER BY effective_from DESC LIMIT 1
                    """, (rc['id'], job_type_id, work_date, work_date))
                    r2 = cur.fetchone()
                    if r2 and r2.get('rate') is not None:
                        return _dec(r2['rate'])
            finally:
                cur.close()
                conn.close()

        return _dec(0)

    @staticmethod
    def _policies(policy_type: str, scope: Dict[str, Any], work_date: date) -> List[Dict[str, Any]]:
        """
        Retrieve applicable policies for a contractor/job/client scope on a given date.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT * FROM calendar_policies
                WHERE type=%s AND active=1
                AND effective_from <= %s AND (effective_to IS NULL OR effective_to >= %s)
                ORDER BY FIELD(scope,'CONTRACTOR_CLIENT','CLIENT','JOB_TYPE','ROLE','GLOBAL')
            """, (policy_type, work_date, work_date))
            allp = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        def match(p):
            s = p['scope']
            if s == 'GLOBAL':
                return True
            if s == 'ROLE':
                return p['role_id'] == scope.get('role_id')
            if s == 'JOB_TYPE':
                return p['job_type_id'] == scope.get('job_type_id')
            if s == 'CLIENT':
                return p['client_name'] == scope.get('client_name')
            if s == 'CONTRACTOR_CLIENT':
                return p['client_name'] == scope.get('client_name') and p['contractor_id'] == scope.get('contractor_id')
            return False

        return [p for p in allp if match(p)]

    @staticmethod
    def _is_bank_holiday(d: date) -> bool:
        """Return True if the date is a bank holiday."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT 1 FROM bank_holidays WHERE date=%s LIMIT 1", (d,))
            return cur.fetchone() is not None
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def resolve_rate_and_pay(
        contractor_id: int,
        role_id: Optional[int],
        job_type_id: int,
        client_name: Optional[int],
        work_date: date,
        actual_start: time,
        actual_end: time,
        break_mins: int,
        weekly_hours_before: float = 0.0
    ) -> Tuple[Decimal, Decimal, Dict[str, Any]]:
        """
        Resolve the effective rate and compute pay for a shift.

        Returns:
            chosen_rate: Decimal
            pay: Decimal
            policy_meta: Dict with applied policy details
        """
        base = RateResolver._base_rate(
            contractor_id, job_type_id, work_date, client_name)
        hrs = Decimal(
            str(_hours_between(actual_start, actual_end, break_mins)))
        scope = {
            "role_id": role_id,
            "job_type_id": job_type_id,
            "client_name": client_name,
            "contractor_id": contractor_id
        }

        # Candidate rates: base + policies
        candidates = [("BASE", base)]

        # Weekend policy
        if work_date.weekday() >= 5:
            pols = RateResolver._policies('WEEKEND', scope, work_date)
            if pols:
                p = pols[0]
                if p['mode'] == 'MULTIPLIER' and p.get('multiplier') is not None:
                    candidates.append(
                        ("WEEKEND", (base * _dec(p['multiplier'])).quantize(Decimal('0.01'))))
                elif p['mode'] == 'ABSOLUTE' and p.get('absolute_rate') is not None:
                    candidates.append(("WEEKEND", _dec(p['absolute_rate'])))

        # Bank Holiday policy
        if RateResolver._is_bank_holiday(work_date):
            pols = RateResolver._policies('BANK_HOLIDAY', scope, work_date)
            if pols:
                p = pols[0]
                if p['mode'] == 'MULTIPLIER' and p.get('multiplier') is not None:
                    candidates.append(
                        ("BANK_HOLIDAY", (base * _dec(p['multiplier'])).quantize(Decimal('0.01'))))
                elif p['mode'] == 'ABSOLUTE' and p.get('absolute_rate') is not None:
                    candidates.append(
                        ("BANK_HOLIDAY", _dec(p['absolute_rate'])))

        # Night policy
        pols = RateResolver._policies('NIGHT', scope, work_date)
        night_window = None
        if pols:
            p = pols[0]
            ws, we = p.get('window_start'), p.get('window_end')
            if ws and we:
                night_window = (ws, we)
            if p['mode'] == 'MULTIPLIER' and p.get('multiplier') is not None:
                candidates.append(
                    ("NIGHT", (base * _dec(p['multiplier'])).quantize(Decimal('0.01'))))
            elif p['mode'] == 'ABSOLUTE' and p.get('absolute_rate') is not None:
                candidates.append(("NIGHT", _dec(p['absolute_rate'])))

        # Choose max-of all candidate rates
        chosen_name, chosen_rate = max(candidates, key=lambda kv: kv[1])

        # Overtime (shift-based)
        ot_applied = None
        pols = RateResolver._policies('OVERTIME_SHIFT', scope, work_date)
        if pols:
            p = pols[0]
            th = p.get('ot_threshold_hours')
            t1 = p.get('ot_tier1_mult')
            t2 = p.get('ot_tier2_mult')
            th2 = p.get('ot_tier2_threshold_hours')
            if th and (t1 or t2):
                base_hours = min(hrs, Decimal(str(th)))
                ot_hours = max(Decimal('0'), hrs - Decimal(str(th)))
                # OT stacks on top
                if ot_hours > 0:
                    ot_mult = _dec(t2) if th2 and hrs > Decimal(
                        str(th2)) and t2 else _dec(t1 or 1)
                    ot_rate = (chosen_rate * ot_mult).quantize(Decimal('0.01'))
                    pay = (base_hours * chosen_rate + ot_hours *
                           ot_rate).quantize(Decimal('0.01'))
                    ot_applied = {"threshold": float(th), "ot_hours": float(
                        ot_hours), "ot_multiplier": float(ot_mult)}
                else:
                    pay = (hrs * chosen_rate).quantize(Decimal('0.01'))
            else:
                pay = (hrs * chosen_rate).quantize(Decimal('0.01'))
        else:
            pay = (hrs * chosen_rate).quantize(Decimal('0.01'))

        policy_meta = {
            "base_rate": float(base),
            "chosen_rate": float(chosen_rate),
            "chosen_reason": chosen_name,
            "hours": float(hrs),
            "night_window": str(night_window) if night_window else None,
            "overtime": ot_applied
        }

        return chosen_rate, pay, policy_meta


class TimesheetService:
    """
    Handles timesheet week and entry operations:
    - Week creation/loading
    - Entry validation and computation (hours, pay, variance)
    - Contractor lookups
    - Staff/admin API payloads
    """

    # ----------------- Module settings -----------------

    @staticmethod
    def _load_module_settings() -> dict:
        """
        Load module settings from the local plugin manifest.

        Note: settings are stored in the plugin's `manifest.json` (not DB),
        updated via the generic plugin settings page.
        """
        try:
            manifest_path = os.path.join(
                os.path.dirname(__file__), "manifest.json")
            if not os.path.exists(manifest_path):
                return {}
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f) or {}
            return manifest.get("settings") or {}
        except Exception:
            return {}

    @staticmethod
    def _get_setting_value(key: str, default: Any = None) -> Any:
        settings = TimesheetService._load_module_settings()
        item = settings.get(key) or {}
        return item.get("value", default)

    @staticmethod
    def _get_bool_setting(key: str, default: bool = False) -> bool:
        v = TimesheetService._get_setting_value(key, default)
        if isinstance(v, bool):
            return v
        if v is None:
            return default
        s = str(v).strip().lower()
        return s in ("1", "true", "yes", "y", "on")

    @staticmethod
    def _scheduler_week_prefill_enabled() -> bool:
        # When true, scheduler shifts (clock in/out optional) are used to
        # prefill weekly timesheet entries for staff.
        return TimesheetService._get_bool_setting(
            "scheduler_week_prefill_enabled", default=True
        )

    @staticmethod
    def _scheduler_source_scheduled_edit_allowed() -> bool:
        # When false, scheduled_start/end are treated as "from scheduler"
        # and cannot be changed by staff (even if submitted).
        return TimesheetService._get_bool_setting(
            "scheduler_source_scheduled_edit_allowed", default=False
        )

    @staticmethod
    def _prefill_from_schedule_shifts(user_id: int, wk_pk: int, week_ending: date) -> None:
        """
        Prefill `tb_timesheet_entries` for the contractor from `schedule_shifts`
        for the relevant ISO week.

        - Uses schedule `scheduled_start/end` as scheduled times.
        - Uses `actual_start/end` when clock-in/out exists; otherwise defaults
          actual to scheduled (so clocking is optional).
        - Stores the schedule shift id into `tb_timesheet_entries.runsheet_id`
          for `source='scheduler'` entries to keep mapping stable.
        - Respects staff deletions via `tb_scheduler_shift_removals`.
        - Does not overwrite entries that staff/admin already edited
          (`edited_by` is non-null).
        """
        # Ensure schedule module is installed / tables exist.
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shifts'")
            if not cur.fetchone():
                return

            date_from = week_ending - timedelta(days=6)
            date_to = week_ending

            # Optional existence check (new table / older DBs).
            cur.execute(
                "SHOW TABLES LIKE 'tb_scheduler_shift_removals'")
            removals_exists = bool(cur.fetchone())

            contractor = TimesheetService._get_contractor(user_id)
            if not contractor:
                return

            # Pull shifts; only those not yet linked to a runsheet.
            # (If runsheet_id is set, the normal runsheet->timesheet path
            # should be used instead.)
            cur.execute(
                """
                SELECT
                    ss.id,
                    ss.work_date,
                    ss.scheduled_start,
                    ss.scheduled_end,
                    ss.actual_start,
                    ss.actual_end,
                    ss.break_mins,
                    ss.notes,
                    ss.client_id,
                    ss.site_id,
                    ss.job_type_id,
                    c.name AS client_name,
                    s.name AS site_name
                FROM schedule_shifts ss
                LEFT JOIN clients c ON c.id = ss.client_id
                LEFT JOIN sites s   ON s.id = ss.site_id
                WHERE ss.contractor_id = %s
                  AND ss.work_date BETWEEN %s AND %s
                  AND (ss.status IS NULL OR LOWER(ss.status) <> 'cancelled')
                  AND ss.runsheet_id IS NULL
                ORDER BY ss.work_date ASC, ss.scheduled_start ASC
                """,
                (user_id, date_from, date_to),
            )
            shifts = cur.fetchall() or []
            if not shifts:
                return

            updated = 0
            created = 0

            for sh in shifts:
                schedule_shift_id = int(sh["id"])

                if removals_exists:
                    cur.execute(
                        """
                        SELECT 1
                        FROM tb_scheduler_shift_removals
                        WHERE user_id=%s AND schedule_shift_id=%s
                        LIMIT 1
                        """,
                        (user_id, schedule_shift_id),
                    )
                    if cur.fetchone():
                        continue

                actual_start = sh.get("actual_start") or sh.get(
                    "scheduled_start")
                actual_end = sh.get("actual_end") or sh.get(
                    "scheduled_end")

                # If no clock data was recorded, we still prefill actuals
                # with scheduled so the week is editable/submittable.
                clock_missing = (
                    sh.get("actual_start") is None or sh.get("actual_end") is None
                )
                auto_reason = "Clock not recorded; defaulted actual to scheduled" if clock_missing else None

                cur.execute(
                    """
                    SELECT
                        id,
                        edited_by,
                        client_name,
                        site_name,
                        job_type_id,
                        scheduled_start,
                        scheduled_end,
                        actual_start,
                        actual_end,
                        break_mins,
                        notes,
                        travel_parking,
                        edit_reason
                    FROM tb_timesheet_entries
                    WHERE user_id=%s
                      AND week_id=%s
                      AND source='scheduler'
                      AND runsheet_id=%s
                    LIMIT 1
                    """,
                    (user_id, wk_pk, schedule_shift_id),
                )
                row = cur.fetchone()

                payload = {
                    "client_name": sh.get("client_name"),
                    "site_name": sh.get("site_name"),
                    "job_type_id": int(sh["job_type_id"]),
                    "work_date": sh["work_date"],
                    "scheduled_start": sh.get("scheduled_start"),
                    "scheduled_end": sh.get("scheduled_end"),
                    "actual_start": actual_start,
                    "actual_end": actual_end,
                    "break_mins": int(sh.get("break_mins") or 0),
                    "travel_parking": 0.0,
                    "notes": sh.get("notes"),
                    "source": "scheduler",
                    "runsheet_id": schedule_shift_id,
                    "lock_job_client": 1,
                }

                computed = TimesheetService._compute_and_fill(
                    payload.copy(), contractor
                )

                if row:
                    # Do not overwrite if staff/admin already edited.
                    if row.get("edited_by") is not None:
                        continue

                    cur.execute(
                        """
                        UPDATE tb_timesheet_entries
                        SET
                            client_name=%s,
                            site_name=%s,
                            job_type_id=%s,
                            work_date=%s,
                            scheduled_start=%s,
                            scheduled_end=%s,
                            actual_start=%s,
                            actual_end=%s,
                            break_mins=%s,
                            notes=%s,
                            source='scheduler',
                            lock_job_client=1,
                            scheduled_hours=%s,
                            actual_hours=%s,
                            labour_hours=%s,
                            wage_rate_used=%s,
                            pay=%s,
                            lateness_mins=%s,
                            overrun_mins=%s,
                            variance_mins=%s,
                            policy_applied=%s,
                            policy_source=%s,
                            edited_by=NULL,
                            edited_at=NULL,
                            edit_reason=%s
                        WHERE id=%s
                        """,
                        (
                            payload["client_name"],
                            payload["site_name"],
                            payload["job_type_id"],
                            payload["work_date"],
                            payload["scheduled_start"],
                            payload["scheduled_end"],
                            payload["actual_start"],
                            payload["actual_end"],
                            payload["break_mins"],
                            payload["notes"],
                            computed["scheduled_hours"],
                            computed["actual_hours"],
                            computed["labour_hours"],
                            computed["wage_rate_used"],
                            computed["pay"],
                            computed["lateness_mins"],
                            computed["overrun_mins"],
                            computed["variance_mins"],
                            computed["policy_applied"],
                            computed["policy_source"],
                            auto_reason,
                            row["id"],
                        ),
                    )
                    updated += 1
                else:
                    cur.execute(
                        """
                        INSERT INTO tb_timesheet_entries (
                            week_id, user_id,
                            client_name, site_name, job_type_id,
                            work_date, scheduled_start, scheduled_end,
                            actual_start, actual_end, break_mins,
                            travel_parking, notes,
                            source, runsheet_id, lock_job_client,
                            scheduled_hours, actual_hours, labour_hours,
                            wage_rate_used, pay,
                            lateness_mins, overrun_mins, variance_mins,
                            policy_applied, policy_source,
                            rate_overridden,
                            edited_by, edited_at, edit_reason
                        ) VALUES (
                            %s,%s,
                            %s,%s,%s,
                            %s,%s,%s,
                            %s,%s,%s,
                            %s,%s,
                            'scheduler',%s,1,
                            %s,%s,%s,
                            %s,%s,
                            %s,%s,%s,
                            %s,%s,
                            0,
                            NULL,NULL,%s
                        )
                        """,
                        (
                            wk_pk,
                            user_id,
                            payload["client_name"],
                            payload["site_name"],
                            payload["job_type_id"],
                            payload["work_date"],
                            payload["scheduled_start"],
                            payload["scheduled_end"],
                            payload["actual_start"],
                            payload["actual_end"],
                            payload["break_mins"],
                            0.0,
                            payload["notes"],
                            payload["runsheet_id"],
                            computed["scheduled_hours"],
                            computed["actual_hours"],
                            computed["labour_hours"],
                            computed["wage_rate_used"],
                            computed["pay"],
                            computed["lateness_mins"],
                            computed["overrun_mins"],
                            computed["variance_mins"],
                            computed["policy_applied"],
                            computed["policy_source"],
                            auto_reason,
                        ),
                    )
                    created += 1

            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_scheduler_prefill_for_shift(
        schedule_shift_id: int, user_id: int, work_date
    ) -> None:
        """
        Remove the scheduler-prefilled timesheet row for this shift (if any).

        Prefill rows use source='scheduler' and runsheet_id=schedule_shift_id.
        When a shift is converted to a published runsheet, that row would duplicate
        the runsheet-backed timesheet entry — delete only if the contractor has not
        manually edited the row (edited_by IS NULL).
        """
        if not schedule_shift_id or not user_id or not work_date:
            return
        wd: date
        if isinstance(work_date, datetime):
            wd = work_date.date()
        elif isinstance(work_date, date):
            wd = work_date
        elif isinstance(work_date, str) and work_date:
            try:
                wd = datetime.strptime(work_date[:10], "%Y-%m-%d").date()
            except ValueError:
                return
        else:
            return
        iso_year, iso_week, _ = wd.isocalendar()
        week_id_str = f"{iso_year}{iso_week:02d}"
        wk = TimesheetService._ensure_week(user_id, week_id_str)
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                DELETE FROM tb_timesheet_entries
                WHERE user_id=%s AND week_id=%s AND work_date=%s
                  AND source='scheduler' AND runsheet_id=%s
                  AND edited_by IS NULL
                """,
                (user_id, wk["id"], wd, schedule_shift_id),
            )
            if cur.rowcount:
                TimesheetService._refresh_week_totals(cur, user_id, wk["id"])
            conn.commit()
        finally:
            cur.close()
            conn.close()

    # ---------- internal helpers ----------

    @staticmethod
    def _ensure_week(user_id: int, week_id: str) -> dict:
        """
        Ensure a timesheet week exists for a user; create if missing.

        Args:
            user_id: User ID
            week_id: ISO week string (YYYYWW)

        Returns:
            Week record as dict
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            # Check if week already exists
            cur.execute(
                "SELECT id, status, week_ending, updated_at "
                "FROM tb_timesheet_weeks WHERE user_id=%s AND week_id=%s",
                (user_id, week_id)
            )
            row = cur.fetchone()
            if row:
                return row

            # Derive week_ending (Sunday) from ISO week number
            year, wknum = int(week_id[:4]), int(week_id[4:])
            jan4 = date(year, 1, 4)
            monday = jan4 + \
                timedelta(days=(0 - jan4.weekday())) + \
                timedelta(weeks=wknum - 1)
            week_ending = monday + timedelta(days=6)

            # Insert new week record
            cur.execute(
                "INSERT INTO tb_timesheet_weeks (user_id, week_id, week_ending) VALUES (%s,%s,%s)",
                (user_id, week_id, week_ending)
            )
            conn.commit()

            # Return the newly created week
            cur.execute(
                "SELECT id, status, week_ending, updated_at "
                "FROM tb_timesheet_weeks WHERE user_id=%s AND week_id=%s",
                (user_id, week_id)
            )
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _validate_entry_payload(e: dict, allow_locked_fields=False) -> tuple:
        """
        Validate timesheet entry payload.

        Args:
            e: Entry dict
            allow_locked_fields: Currently unused; for future locked field enforcement

        Returns:
            (is_valid: bool, error_message: Optional[str])
        """
        required = ["job_type_id", "work_date", "scheduled_start",
                    "scheduled_end", "actual_start", "actual_end"]
        missing = [k for k in required if e.get(k) in (None, "")]
        if missing:
            return False, f"Missing fields: {', '.join(missing)}"

        # Type and format validation
        try:
            int(e["job_type_id"])
            # Parse dates and times
            _ = datetime.strptime(
                e["work_date"], "%Y-%m-%d").date() if isinstance(e["work_date"], str) else e["work_date"]
            _ = _to_time(e["scheduled_start"])
            _ = _to_time(e["scheduled_end"])
            _ = _to_time(e["actual_start"])
            _ = _to_time(e["actual_end"])
        except Exception as ex:
            return False, f"Invalid date/time or job_type_id: {ex}"

        # Numeric fields validation
        for fld in ("break_mins",):
            if fld in e and e[fld] not in (None, ""):
                try:
                    _ = int(e[fld])
                except Exception:
                    return False, f"{fld} must be integer minutes"

        for fld in ("travel_parking",):
            if fld in e and e[fld] not in (None, ""):
                try:
                    _ = _dec(e[fld])
                except Exception:
                    return False, f"{fld} must be a number"

        return True, None

    @staticmethod
    def _compute_and_fill(entry: dict, contractor: dict) -> dict:
        """
        Compute derived fields for a timesheet entry:
        - Scheduled/actual/labour hours
        - Lateness, overrun, variance
        - Rate and pay
        - Policy metadata

        Args:
            entry: Entry dict
            contractor: Contractor dict

        Returns:
            Updated entry dict with computed fields
        """
        work_date = entry["work_date"] if isinstance(
            entry["work_date"], date) else datetime.strptime(entry["work_date"], "%Y-%m-%d").date()
        ss = _to_time(entry["scheduled_start"])
        se = _to_time(entry["scheduled_end"])
        as_ = _to_time(entry["actual_start"])
        ae = _to_time(entry["actual_end"])
        break_mins = int(entry.get("break_mins") or 0)

        scheduled_hours = _hours_between(ss, se, 0)
        actual_hours = _hours_between(as_, ae, break_mins)
        labour_hours = actual_hours  # personal timesheets always 1 person

        # Lateness and overrun in minutes
        lateness = max(0, int((_hours_between(ss, as_, 0) * 60))
                       ) if as_ > ss else 0
        overrun = max(0, int((_hours_between(se, ae, 0) * 60))
                      ) if ae > se else 0

        scheduled_total_mins = int(scheduled_hours * 60)
        actual_total_mins = int(actual_hours * 60)
        variance = actual_total_mins - scheduled_total_mins

        base_rate = MinimalRateResolver.resolve_rate(
            contractor_id=contractor["id"],
            job_type_id=int(entry["job_type_id"]),
            on_date=work_date
        )
        hrs_dec = _dec(actual_hours, '0.0001')
        pay_dec = (hrs_dec * base_rate).quantize(Decimal('0.01'))

        entry.update({
            "scheduled_hours": float(_dec(scheduled_hours, '0.0001')),
            "actual_hours": float(hrs_dec),
            "labour_hours": float(_dec(labour_hours, '0.0001')),
            "lateness_mins": lateness,
            "overrun_mins": overrun,
            "variance_mins": variance,
            "wage_rate_used": float(base_rate),
            "pay": float(pay_dec),
            "policy_applied": None,
            "policy_source": "CARD_ONLY"
        })

        # # Resolve rate and pay using RateResolver
        # rate, pay, policy_meta = RateResolver.resolve_rate_and_pay(
        #     contractor_id=contractor["id"],
        #     role_id=contractor.get("role_id"),
        #     job_type_id=int(entry["job_type_id"]),
        #     client_name=entry.get("client_name"),
        #     work_date=work_date,
        #     actual_start=as_,
        #     actual_end=ae,
        #     break_mins=break_mins
        # )

        # entry.update({
        #     "scheduled_hours": float(_dec(scheduled_hours, '0.0001')),
        #     "actual_hours": float(_dec(actual_hours, '0.0001')),
        #     "labour_hours": float(_dec(labour_hours, '0.0001')),
        #     "lateness_mins": lateness,
        #     "overrun_mins": overrun,
        #     "variance_mins": variance,
        #     "wage_rate_used": float(rate),
        #     "pay": float(pay),
        #     "policy_applied": json.dumps(policy_meta),
        #     "policy_source": policy_meta.get("chosen_reason")
        # })
        return entry

    @staticmethod
    def _job_type_name_and_colour(job_type_id: int) -> Tuple[Optional[str], Optional[str]]:
        """Return (name, colour_hex) for badges/UI; colour_hex may be None."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT name, colour_hex FROM job_types WHERE id=%s",
                (int(job_type_id),),
            )
            r = cur.fetchone()
            if not r:
                return None, None
            return r.get("name"), r.get("colour_hex")
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _job_type_name(job_type_id: int) -> Optional[str]:
        n, _ = TimesheetService._job_type_name_and_colour(job_type_id)
        return n

    @staticmethod
    def _get_contractor(user_id: int) -> dict:
        """
        Retrieve contractor info for a given user ID.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, role_id, wage_rate_card_id, wage_rate_override FROM tb_contractors WHERE id=%s",
                (user_id,)
            )
            return cur.fetchone() or {}
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_week_payload(user_id: int, week_id: str, is_admin: bool) -> dict:
        """
        Get the full week payload for staff/admin:
        - Week info
        - Entries for the week
        - Totals (hours, pay, travel, lateness, overrun)

        MVP: uses free-text client_name/site_name (no joins on IDs).
        """
        wk = TimesheetService._ensure_week(user_id, week_id)

        # Pre-fill weekly entries from scheduler shifts (optional clocking).
        if TimesheetService._scheduler_week_prefill_enabled() and (wk.get("status") or "draft").lower() in ("draft", "rejected"):
            try:
                TimesheetService._prefill_from_schedule_shifts(
                    user_id=user_id,
                    wk_pk=wk["id"],
                    week_ending=wk["week_ending"],
                )
            except Exception:
                # Prefill is best-effort; don't break timesheet rendering.
                pass

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)

        try:
            # MVP: no joins to clients/sites (IDs not in use yet). Use free-text fields.
            cur.execute(
                """
                SELECT 
                    e.*,
                    jt.name AS job_type_name,
                    jt.colour_hex AS job_type_colour_hex,
                    e.client_name AS client_name,
                    e.site_name   AS site_name
                FROM tb_timesheet_entries e
                JOIN job_types jt ON jt.id = e.job_type_id
                WHERE e.week_id=%s AND e.user_id=%s
                ORDER BY e.work_date ASC, e.actual_start ASC
                """,
                (wk["id"], user_id),
            )
            rows = cur.fetchall() or []

            # ---------------------------
            # Helpers for safe JSON
            # ---------------------------
            def _to_float_hours(x):
                if x is None:
                    return 0.0
                if isinstance(x, timedelta):
                    return x.total_seconds() / 3600.0
                try:
                    return float(x)
                except Exception:
                    return 0.0

            def _to_int_mins(x):
                if x is None:
                    return 0
                if isinstance(x, timedelta):
                    return int(round(x.total_seconds() / 60.0))
                try:
                    return int(x)
                except Exception:
                    return 0

            def _to_money(x):
                try:
                    # uses your existing Decimal helper
                    return float(_dec(x or 0))
                except Exception:
                    return 0.0

            # ---------------------------
            # Normalize fields for JSON
            # ---------------------------
            for r in rows:
                # Date to "YYYY-MM-DD"
                if isinstance(r.get("work_date"), (date, datetime)):
                    r["work_date"] = r["work_date"].strftime("%Y-%m-%d")

                # Times to "HH:MM:SS" (string)
                for k in ("scheduled_start", "scheduled_end", "actual_start", "actual_end"):
                    v = r.get(k)
                    if isinstance(v, (time, datetime)):
                        r[k] = str(v)
                    elif v is None:
                        r[k] = None
                    else:
                        r[k] = str(v)

                # Hours as float
                for k in ("scheduled_hours", "actual_hours", "labour_hours"):
                    if k in r:
                        r[k] = _to_float_hours(r.get(k))

                # Minutes as int
                for k in ("lateness_mins", "overrun_mins", "variance_mins"):
                    if k in r:
                        r[k] = _to_int_mins(r.get(k))

                # Money as float
                for k in ("wage_rate_used", "pay", "travel_parking"):
                    if k in r:
                        r[k] = _to_money(r.get(k))

                # Ensure lock flag numeric/bool shape
                if "lock_job_client" in r and r["lock_job_client"] is not None:
                    try:
                        r["lock_job_client"] = int(r["lock_job_client"])
                    except Exception:
                        r["lock_job_client"] = 0

            # ---------------------------
            # Totals
            # ---------------------------
            totals = {
                "hours": float(sum(
                    (_to_float_hours(r.get("actual_hours"))
                     if r.get("actual_hours") is not None
                     else _to_float_hours(r.get("labour_hours")))
                    for r in rows
                )),
                "pay": _to_money(sum((r.get("pay") or 0) for r in rows)),
                "travel": _to_money(sum((r.get("travel_parking") or 0) for r in rows)),
                "lateness_mins": int(sum((r.get("lateness_mins") or 0) for r in rows)),
                "overrun_mins": int(sum((r.get("overrun_mins") or 0) for r in rows)),
            }

            # ---------------------------
            # Summaries by job type (for invoicing: wage * hours per type)
            # ---------------------------
            by_job: Dict[int, Dict[str, Any]] = {}
            for r in rows:
                jid = r.get("job_type_id")
                if jid is None:
                    continue
                if jid not in by_job:
                    by_job[jid] = {
                        "job_type_id": jid,
                        "job_type_name": r.get("job_type_name") or "",
                        "job_type_colour_hex": r.get("job_type_colour_hex"),
                        "total_hours": 0.0,
                        "total_pay": 0.0,
                        "entry_count": 0,
                    }
                hrs = r.get("actual_hours")
                if hrs is None:
                    hrs = r.get("labour_hours")
                by_job[jid]["total_hours"] += _to_float_hours(hrs)
                by_job[jid]["total_pay"] += _to_money(r.get("pay"))
                by_job[jid]["entry_count"] += 1
            summaries_by_job_type = list(by_job.values())

            # Self-employed: show "Create invoice" when submitted or approved (submit invoice with timesheet for approval)
            employment_type = "self_employed"
            invoice_info = {}
            try:
                employment_type = InvoiceService.get_contractor_employment_type(user_id)
                invoice_info = InvoiceService.get_week_invoice_info(wk["id"])
            except Exception:
                pass
            status_lower = (wk.get("status") or "draft").lower()
            current_inv = invoice_info.get("current_invoice")
            has_sent = current_inv and current_inv.get("status") == "sent"
            has_draft = current_inv and current_inv.get("status") == "draft"
            can_show_invoice = (
                status_lower in ("submitted", "approved")
                and employment_type == "self_employed"
                and not has_sent
                and not has_draft
            )
            can_finalize_draft_invoice = (
                status_lower == "approved"
                and employment_type == "self_employed"
                and has_draft
            )
            prompt_resend = (
                can_show_invoice
                and invoice_info.get("has_voided_invoice")
            )
            invoice_draft_pending = has_draft and status_lower == "submitted"

            uninvoiced_entries: List[Dict[str, Any]] = []
            if employment_type == "self_employed":
                try:
                    uninvoiced_entries = InvoiceService.get_uninvoiced_entries(wk["id"], user_id)
                except Exception:
                    uninvoiced_entries = []
            uni_n = len(uninvoiced_entries)
            invoice_banner = None
            if employment_type == "paye":
                invoice_banner = {
                    "level": "info",
                    "text": "You are set up as PAYE — invoicing is turned off. If you should invoice, ask an admin to change employment type to self-employed (Time Billing or HR).",
                }
            elif employment_type == "self_employed":
                if status_lower == "draft":
                    invoice_banner = {
                        "level": "info",
                        "text": "Self-employed: submit this week to attach a new invoice for approval (or open My invoices for past weeks).",
                    }
                elif status_lower == "rejected":
                    invoice_banner = {
                        "level": "warning",
                        "text": "This week was rejected — fix entries, resubmit, then create a new invoice if needed.",
                    }
                elif status_lower in ("submitted", "approved") and uni_n == 0 and not has_sent and not has_draft:
                    invoice_banner = {
                        "level": "info",
                        "text": "No uninvoiced shifts remain for this week. Use My invoices to view or download PDFs.",
                    }
                elif status_lower == "approved" and employment_type == "self_employed" and has_draft:
                    invoice_banner = {
                        "level": "success",
                        "text": "Timesheet approved — you have a draft invoice for this week. Use Finalize invoice to mark it sent, or open My invoices.",
                    }

            return {
                "week": {
                    "id": wk["id"],
                    "week_id": week_id,
                    "status": wk["status"],
                    "week_ending": wk["week_ending"].isoformat(),
                    "updated_at": (
                        wk["updated_at"].isoformat() if wk.get(
                            "updated_at") else None
                    ),
                },
                "entries": rows,
                "totals": totals,
                "summaries_by_job_type": summaries_by_job_type,
                "is_admin": bool(is_admin),
                "employment_type": employment_type,
                "can_show_invoice_prompt": can_show_invoice,
                "invoice_prompt_resend": prompt_resend,
                "invoice_draft_pending": invoice_draft_pending,
                "invoice_info": invoice_info,
                "invoice_banner": invoice_banner,
                "uninvoiced_entry_count": uni_n,
                "can_finalize_draft_invoice": can_finalize_draft_invoice,
                # UI policies
                "scheduler_week_prefill_enabled": TimesheetService._scheduler_week_prefill_enabled(),
                "scheduler_source_scheduled_edit_allowed": TimesheetService._scheduler_source_scheduled_edit_allowed(),
            }

        finally:
            cur.close()
            conn.close()

    @staticmethod
    def batch_upsert(
        user_id: int,
        week_id: str,
        entries: List[dict],
        client_updated_at: Optional[str] = None,
    ) -> dict:
        """
        Batch insert or update timesheet entries for a week.

        Args:
            user_id (int): Contractor/user ID
            week_id (str): ISO week string (YYYYWW)
            entries (List[dict]): List of entry dictionaries
            client_updated_at (Optional[str]): Timestamp for optimistic concurrency check

        Returns:
            dict: Success status, saved entries, totals, or conflicts
        """
        print(
            "UPSERT DEBUG:",
            {
                "user_id": user_id,
                "week_id": week_id,
                "entries": entries,
                "client_updated_at": client_updated_at,
            },
        )

        wk = TimesheetService._ensure_week(user_id, week_id)

        # ----- Optimistic concurrency check -----
        if client_updated_at:
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute(
                    "SELECT updated_at FROM tb_timesheet_weeks WHERE id=%s",
                    (wk["id"],),
                )
                row = cur.fetchone()
                if row and row.get("updated_at"):
                    server_ts = row["updated_at"].isoformat()
                    if server_ts > client_updated_at:
                        return {
                            "ok": False,
                            "conflicts": True,
                            "message": "Server has newer data. Please refresh.",
                        }
            finally:
                cur.close()
                conn.close()

        contractor = TimesheetService._get_contractor(user_id)
        if not contractor:
            return {"ok": False, "message": "Contractor not found."}

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        conflicts = []

        try:
            saved_entries = []
            jt_meta_cache: Dict[int, Tuple[Optional[str], Optional[str]]] = {}

            def _jt_meta(jtid: int) -> Tuple[Optional[str], Optional[str]]:
                jid = int(jtid)
                if jid not in jt_meta_cache:
                    jt_meta_cache[jid] = TimesheetService._job_type_name_and_colour(jid)
                return jt_meta_cache[jid]

            for e in entries or []:
                # Validate payload
                ok, err = TimesheetService._validate_entry_payload(e)
                if not ok:
                    conflicts.append(
                        {"temp_id": e.get("temp_id"), "reason": err})
                    continue

                entry_id = e.get("id")

                # ----- Existing entry checks -----
                edited_by_value = None
                edited_at_value = None
                edit_reason_value = None
                if entry_id:
                    cur.execute(
                        """
                        SELECT
                            user_id,
                            source,
                            runsheet_id,
                            lock_job_client,
                            client_name,
                            site_name,
                            job_type_id,
                            scheduled_start,
                            scheduled_end,
                            actual_start,
                            actual_end,
                            break_mins,
                            travel_parking,
                            notes,
                            edited_by,
                            edited_at,
                            edit_reason
                        FROM tb_timesheet_entries
                        WHERE id=%s
                        """,
                        (entry_id,),
                    )
                    existing = cur.fetchone()
                    if not existing or existing["user_id"] != user_id:
                        conflicts.append(
                            {"id": entry_id, "reason": "not_found_or_not_owner"}
                        )
                        continue

                    # Preserve source/run linkage on updates.
                    # The public staff UI doesn't send these fields back, but we
                    # need them stable for scheduler-prefilled entries.
                    e["source"] = existing.get("source")
                    e["runsheet_id"] = existing.get("runsheet_id")
                    e["lock_job_client"] = existing.get("lock_job_client")

                    # Lock handling: prevent changes on generated entries.
                    # (Override payload with existing values instead of deleting
                    # fields, so compute-and-fill still works.)
                    if existing["source"] in ("runsheet", "scheduler") and existing.get("lock_job_client"):
                        e["job_type_id"] = existing.get("job_type_id")
                        e["client_name"] = existing.get("client_name")
                        e["site_name"] = existing.get("site_name")

                    # Optional: scheduled time editing rules for scheduler-generated entries.
                    if existing.get("source") == "scheduler" and not TimesheetService._scheduler_source_scheduled_edit_allowed():
                        e["scheduled_start"] = existing.get("scheduled_start")
                        e["scheduled_end"] = existing.get("scheduled_end")

                    # Detect if staff/admin actually changed anything that should be tracked.
                    # We track edits by setting edited_by/edit_reason (so admin can show "adjusted").
                    incoming_actual_start = _to_time(e.get("actual_start"))
                    incoming_actual_end = _to_time(e.get("actual_end"))
                    incoming_break_mins = int(e.get("break_mins") or 0)
                    incoming_travel = float(_dec(e.get("travel_parking") or 0))
                    incoming_notes = e.get("notes")

                    existing_actual_start = existing.get("actual_start")
                    existing_actual_end = existing.get("actual_end")
                    existing_break_mins = int(existing.get("break_mins") or 0)
                    existing_travel = float(_dec(existing.get("travel_parking") or 0))
                    existing_notes = existing.get("notes")

                    did_adjust = (
                        existing_actual_start != incoming_actual_start
                        or existing_actual_end != incoming_actual_end
                        or existing_break_mins != incoming_break_mins
                        or existing_travel != incoming_travel
                        or (existing_notes or "") != (incoming_notes or "")
                    )

                    edited_by_value = existing.get("edited_by")
                    edited_at_value = existing.get("edited_at")
                    edit_reason_value = existing.get("edit_reason")

                    if did_adjust:
                        edited_by_value = user_id
                        edited_at_value = datetime.utcnow()
                        parts: List[str] = []
                        if existing_actual_start != incoming_actual_start or existing_actual_end != incoming_actual_end:
                            parts.append("Adjusted actual times (clock-in/out override)")
                        if existing_notes != incoming_notes:
                            parts.append("Updated notes")
                        if existing_break_mins != incoming_break_mins:
                            parts.append("Updated break time")
                        if existing_travel != incoming_travel:
                            parts.append("Updated travel/parking")
                        edit_reason_value = "; ".join(parts)[:255]

                # Compute derived fields
                computed = TimesheetService._compute_and_fill(e, contractor)

                # Columns and parameters (with client_name, site_name added)
                cols = [
                    "week_id",
                    "user_id",
                    "client_name",
                    "site_name",
                    "job_type_id",
                    "work_date",
                    "scheduled_start",
                    "scheduled_end",
                    "actual_start",
                    "actual_end",
                    "break_mins",
                    "travel_parking",
                    "notes",
                    "source",
                    "runsheet_id",
                    "lock_job_client",
                    "scheduled_hours",
                    "actual_hours",
                    "labour_hours",
                    "wage_rate_used",
                    "pay",
                    "lateness_mins",
                    "overrun_mins",
                    "variance_mins",
                    "policy_applied",
                    "policy_source",
                    "rate_overridden",
                    "edited_by",
                    "edited_at",
                    "edit_reason",
                ]

                params = {
                    "week_id": wk["id"],
                    "user_id": user_id,
                    "client_name": (e.get("client_name") or "").strip() or None,
                    "site_name": (e.get("site_name") or "").strip() or None,
                    "job_type_id": int(e["job_type_id"]),
                    "work_date": e["work_date"]
                    if isinstance(e["work_date"], date)
                    else datetime.strptime(e["work_date"], "%Y-%m-%d").date(),
                    "scheduled_start": _to_time(e["scheduled_start"]),
                    "scheduled_end": _to_time(e["scheduled_end"]),
                    "actual_start": _to_time(e["actual_start"]),
                    "actual_end": _to_time(e["actual_end"]),
                    "break_mins": int(e.get("break_mins") or 0),
                    "travel_parking": float(_dec(e.get("travel_parking") or 0)),
                    "notes": e.get("notes"),
                    "source": e.get("source") or "manual",
                    "runsheet_id": e.get("runsheet_id"),
                    "lock_job_client": int(e.get("lock_job_client") or 0),
                    "scheduled_hours": computed["scheduled_hours"],
                    "actual_hours": computed["actual_hours"],
                    "labour_hours": computed["labour_hours"],
                    "wage_rate_used": computed["wage_rate_used"],
                    "pay": computed["pay"],
                    "lateness_mins": computed["lateness_mins"],
                    "overrun_mins": computed["overrun_mins"],
                    "variance_mins": computed["variance_mins"],
                    "policy_applied": computed["policy_applied"],
                    "policy_source": computed["policy_source"],
                    "rate_overridden": 0,
                    "edited_by": edited_by_value,
                    "edited_at": edited_at_value,
                    "edit_reason": edit_reason_value,
                }

                if entry_id:
                    # Update existing entry
                    set_clause = ", ".join(
                        [f"{k}=%({k})s" for k in cols if k not in (
                            "week_id", "user_id")]
                    )
                    sql = f"""
                        UPDATE tb_timesheet_entries
                        SET {set_clause}
                        WHERE id=%(id)s AND user_id=%(user_id)s
                    """
                    params2 = params.copy()
                    params2["id"] = entry_id
                    cur.execute(sql, params2)
                else:
                    # Insert new entry
                    placeholders = ", ".join([f"%({k})s" for k in cols])
                    sql = f"""
                        INSERT INTO tb_timesheet_entries ({', '.join(cols)})
                        VALUES ({placeholders})
                    """
                    cur.execute(sql, params)
                    entry_id = cur.lastrowid

                if not entry_id:
                    entry_id = cur.lastrowid

                jn, jh = _jt_meta(params["job_type_id"])
                saved_entries.append(
                    {
                        "id": entry_id,
                        "work_date": params["work_date"].isoformat(),
                        "client_name": params["client_name"],
                        "site_name": params["site_name"],
                        "job_type_id": params["job_type_id"],
                        "job_type_name": jn,
                        "job_type_colour_hex": jh,
                        "scheduled_start": str(params["scheduled_start"]),
                        "scheduled_end": str(params["scheduled_end"]),
                        "actual_start": str(params["actual_start"]),
                        "actual_end": str(params["actual_end"]),
                        "break_mins": params["break_mins"],
                        "travel_parking": params["travel_parking"],
                        "notes": params["notes"],
                        "actual_hours": params["actual_hours"],
                        "wage_rate_used": params["wage_rate_used"],
                        "pay": params["pay"],
                    }
                )

            # Update week totals snapshot
            cur.execute(
                """
                UPDATE tb_timesheet_weeks w
                LEFT JOIN (
                    SELECT week_id,
                        SUM(actual_hours)     AS th,
                        SUM(pay)              AS tp,
                        SUM(travel_parking)   AS tt,
                        SUM(lateness_mins)    AS tl,
                        SUM(overrun_mins)     AS tovr
                    FROM tb_timesheet_entries
                    WHERE user_id=%s AND week_id=%s
                ) agg ON agg.week_id = w.id
                SET w.total_hours         = COALESCE(agg.th, 0),
                    w.total_pay           = COALESCE(agg.tp, 0),
                    w.total_travel        = COALESCE(agg.tt, 0),
                    w.total_lateness_mins = COALESCE(agg.tl, 0),
                    w.total_overrun_mins  = COALESCE(agg.tovr, 0)
                WHERE w.id=%s
                """,
                (user_id, wk["id"], wk["id"]),
            )

            conn.commit()

            if conflicts:
                return {"ok": False, "conflicts": conflicts}

            # Recompute totals for response
            conn2 = get_db_connection()
            cur2 = conn2.cursor(dictionary=True)
            try:
                cur2.execute(
                    """
                    SELECT
                        COALESCE(SUM(actual_hours), 0)   AS th,
                        COALESCE(SUM(pay), 0)            AS tp,
                        COALESCE(SUM(travel_parking), 0) AS tt
                    FROM tb_timesheet_entries
                    WHERE user_id=%s AND week_id=%s
                    """,
                    (user_id, wk["id"]),
                )
                agg = cur2.fetchone() or {}
            finally:
                cur2.close()
                conn2.close()

            return {
                "ok": True,
                "entries": saved_entries,
                "totals": {
                    "hours": float(agg.get("th", 0)),
                    "pay": float(_dec(agg.get("tp", 0))),
                    "travel": float(_dec(agg.get("tt", 0))),
                },
            }

        finally:
            cur.close()
            conn.close()

    @staticmethod
    def submit_week(user_id: int, week_id: str) -> None:
        """
        Mark a timesheet week as submitted.

        Args:
            user_id: ID of submitting user
            week_id: ISO week string
        """
        wk = TimesheetService._ensure_week(user_id, week_id)
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE tb_timesheet_weeks SET status='submitted', submitted_at=%s, submitted_by=%s WHERE id=%s",
                (_now_utc_str(), user_id, wk["id"])
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    # ---------- admin-only ----------

    @staticmethod
    def admin_patch_entry(entry_id: int, updates: dict, admin_id: Optional[int]) -> dict:
        """
        Allows an admin to patch a timesheet entry.

        Args:
            entry_id: ID of the timesheet entry
            updates: Dict of fields to update
            admin_id: Admin user ID performing the patch

        Returns:
            Dict indicating success or failure
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            # Fetch current entry with contractor role info for rate calculations
            cur.execute("""
                SELECT e.*, c.role_id
                FROM tb_timesheet_entries e
                JOIN tb_contractors c ON c.id = e.user_id
                WHERE e.id=%s
            """, (entry_id,))
            row = cur.fetchone()
            if not row:
                return {"ok": False, "message": "Entry not found."}

            # Only allow editing of core mutable fields
            mutable = {
                "client_name", "site_name", "job_type_id", "work_date", "scheduled_start",
                "scheduled_end", "actual_start", "actual_end", "break_mins", "travel_parking",
                "notes"
            }
            payload = {k: v for k, v in (
                updates or {}).items() if k in mutable}

            # Merge current row with updates
            merged = {**row, **payload}

            # Normalize types
            if isinstance(merged.get("work_date"), str):
                merged["work_date"] = datetime.strptime(
                    merged["work_date"], "%Y-%m-%d").date()
            for tkey in ("scheduled_start", "scheduled_end", "actual_start", "actual_end"):
                if tkey in merged and isinstance(merged[tkey], str):
                    merged[tkey] = _to_time(merged[tkey])

            # Recompute fields unless admin explicitly sets wage_rate_used/pay
            recompute = True
            manual_rate = updates.get("wage_rate_used")
            manual_pay = updates.get("pay")
            if manual_rate is not None or manual_pay is not None:
                recompute = False

            if recompute:
                computed = TimesheetService._compute_and_fill({
                    "job_type_id": merged.get("job_type_id"),
                    "work_date": merged.get("work_date"),
                    "scheduled_start": merged.get("scheduled_start"),
                    "scheduled_end": merged.get("scheduled_end"),
                    "actual_start": merged.get("actual_start"),
                    "actual_end": merged.get("actual_end"),
                    "break_mins": merged.get("break_mins"),
                    "client_name": merged.get("client_name"),
                    "site_name": merged.get("site_name"),
                    "source": merged.get("source"),
                    "runsheet_id": merged.get("runsheet_id"),
                    "lock_job_client": merged.get("lock_job_client"),
                    "notes": merged.get("notes")
                }, {"id": row["user_id"], "role_id": row.get("role_id")})

                wage_rate_used = computed["wage_rate_used"]
                pay = computed["pay"]
                policy_applied = computed["policy_applied"]
                policy_source = computed["policy_source"]
                rate_overridden = 0
                edit_reason = updates.get("edit_reason")
            else:
                wage_rate_used = float(
                    _dec(manual_rate if manual_rate is not None else row["wage_rate_used"]))
                if manual_pay is None:
                    pay = float(_dec(Decimal(str(wage_rate_used))
                                * Decimal(str(row["actual_hours"] or 0))))
                else:
                    pay = float(_dec(manual_pay))
                policy_applied = row.get("policy_applied")
                policy_source = row.get("policy_source")
                rate_overridden = 1
                edit_reason = updates.get(
                    "edit_reason") or "Admin manual rate/pay override"

            # Build SQL update parameters
            params = {
                "client_name": merged.get("client_name"),
                "site_name": merged.get("site_name"),
                "job_type_id": int(merged.get("job_type_id")),
                "work_date": merged.get("work_date"),
                "scheduled_start": merged.get("scheduled_start"),
                "scheduled_end": merged.get("scheduled_end"),
                "actual_start": merged.get("actual_start"),
                "actual_end": merged.get("actual_end"),
                "break_mins": int(merged.get("break_mins") or 0),
                "travel_parking": float(_dec(merged.get("travel_parking") or row.get("travel_parking") or 0)),
                "notes": merged.get("notes"),
                "scheduled_hours": float(_dec(_hours_between(merged.get("scheduled_start"), merged.get("scheduled_end"), 0), '0.0001')),
                "actual_hours": float(_dec(_hours_between(merged.get("actual_start"), merged.get("actual_end"), int(merged.get("break_mins") or 0)), '0.0001')),
                "labour_hours": float(_dec(_hours_between(merged.get("actual_start"), merged.get("actual_end"), int(merged.get("break_mins") or 0)), '0.0001')),
                "wage_rate_used": wage_rate_used,
                "pay": pay,
                "lateness_mins": max(0, int((_hours_between(merged.get("scheduled_start"), merged.get("actual_start"), 0) * 60))) if merged.get("actual_start") > merged.get("scheduled_start") else 0,
                "overrun_mins": max(0, int((_hours_between(merged.get("scheduled_end"), merged.get("actual_end"), 0) * 60))) if merged.get("actual_end") > merged.get("scheduled_end") else 0,
                "variance_mins": int((_hours_between(merged.get("actual_start"), merged.get("actual_end"), int(merged.get("break_mins") or 0)) - _hours_between(merged.get("scheduled_start"), merged.get("scheduled_end"), 0)) * 60),
                "policy_applied": policy_applied,
                "policy_source": policy_source,
                "rate_overridden": rate_overridden,
                "edited_by": admin_id,
                "edited_at": datetime.utcnow(),
                "edit_reason": edit_reason
            }

            set_clause = ", ".join([f"{k}=%({k})s" for k in params.keys()])
            sql = f"UPDATE tb_timesheet_entries SET {set_clause} WHERE id=%(id)s"
            params["id"] = entry_id
            cur.execute(sql, params)

            # Refresh week totals
            cur.execute(
                "SELECT week_id, user_id FROM tb_timesheet_entries WHERE id=%s", (entry_id,))
            key = cur.fetchone()
            if key:
                TimesheetService._refresh_week_totals(
                    cur, key["user_id"], key["week_id"])

            conn.commit()
            return {"ok": True}

        finally:
            cur.close()
            conn.close()

    @staticmethod
    def approve_week(admin_id: int, user_id: int, week_id: str) -> Tuple[bytes, dict]:
        """
        Approve a week for a contractor, generate PDF, and optionally email it.

        Returns:
            PDF bytes and dict with status/filename
        """
        wk = TimesheetService._ensure_week(user_id, week_id)
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            # Ensure week has entries
            cur.execute("SELECT COUNT(1) AS cnt FROM tb_timesheet_entries WHERE user_id=%s AND week_id=%s",
                        (user_id, wk["id"]))
            cnt = (cur.fetchone() or {}).get("cnt", 0)
            if cnt == 0:
                raise Exception("Cannot approve an empty week.")

            # Mark as approved
            cur.execute("""
                UPDATE tb_timesheet_weeks
                SET status='approved', approved_at=%s, approved_by=%s
                WHERE id=%s
            """, (datetime.utcnow(), admin_id, wk["id"]))
            conn.commit()

            # If there is a draft invoice for this week, mark it sent and set week to invoiced (paid)
            week_marked_invoiced = False
            cur.execute("""
                SELECT id FROM contractor_invoices
                WHERE timesheet_week_id=%s AND status='draft'
                ORDER BY id ASC
            """, (wk["id"],))
            draft_rows = cur.fetchall() or []
            if draft_rows:
                now_inv = datetime.utcnow()
                for dr in draft_rows:
                    inv_id = dr.get("id")
                    cur.execute("""
                        UPDATE contractor_invoices
                        SET status='sent', sent_at=%s
                        WHERE id=%s AND status='draft'
                    """, (now_inv, inv_id))
                cur.execute("""
                    UPDATE tb_timesheet_weeks
                    SET status='invoiced'
                    WHERE id=%s
                """, (wk["id"],))
                conn.commit()
                week_marked_invoiced = True

            # Generate PDF
            pdf_bytes, filename = ExportService.export_week_pdf(
                user_id=user_id, week_id=week_id)

            # Fetch contractor email
            cur.execute(
                "SELECT email FROM tb_contractors WHERE id=%s", (user_id,))
            c = cur.fetchone() or {}
            to_addr = [c.get("email")] if c.get("email") else []

            if not to_addr:
                return pdf_bytes, {"ok": True, "notice": "No recipient email on file; PDF not emailed."}

            if week_marked_invoiced:
                subject = f"Timesheet and invoice approved – Week Ending {wk['week_ending'].strftime('%d/%m/%Y')}"
                body = f"Hi,\n\nYour timesheet and invoice for week ending {wk['week_ending'].strftime('%d/%m/%Y')} have been approved and marked for payment.\nPlease find the PDF attached for your records.\n\nRegards,\nAccounts"
                html_body = f"<p>Your timesheet and invoice have been approved and marked for payment.</p><p>Please download your PDF from the portal.</p>"
            else:
                subject = f"Timesheet approved – Week Ending {wk['week_ending'].strftime('%d/%m/%Y')}"
                body = f"Hi,\n\nYour timesheet for week ending {wk['week_ending'].strftime('%d/%m/%Y')} has been approved.\nPlease find the PDF attached for your records.\n\nRegards,\nAccounts"
                html_body = f"<p>Your timesheet for week ending {wk['week_ending'].strftime('%d/%m/%Y')} has been approved.</p><p>Please download your PDF from the portal.</p>"

            try:
                EmailManager().send_email(subject=subject, body=body,
                                          recipients=to_addr, html_body=html_body)
            except Exception as e:
                print(f"[WARN] Failed to send approval email: {e}")

            return pdf_bytes, {"ok": True, "filename": filename}

        finally:
            cur.close()
            conn.close()

    @staticmethod
    def reject_week(admin_id: int, user_id: int, week_id: str, reason: str) -> None:
        """
        Reject a contractor's week with a reason and optionally email them.
        If the week had an invoice (sent), it is voided so the contractor can create a new one after re-approval.

        Args:
            reason: Mandatory rejection reason
        """
        if not reason or not reason.strip():
            raise Exception("Rejection reason is required.")

        wk = TimesheetService._ensure_week(user_id, week_id)
        # Void any invoice for this week so contractor can create a new one after corrections
        try:
            InvoiceService.void_invoice_for_week(wk["id"], "Timesheet rejected – create new invoice after re-approval.")
        except Exception as e:
            print(f"[WARN] Could not void invoice for week {wk['id']}: {e}")
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                UPDATE tb_timesheet_weeks
                SET status='rejected', rejected_at=%s, rejected_by=%s, rejection_reason=%s
                WHERE id=%s
            """, (datetime.utcnow(), admin_id, reason.strip(), wk["id"]))
            conn.commit()

            # Email user
            cur.execute(
                "SELECT email FROM tb_contractors WHERE id=%s", (user_id,))
            c = cur.fetchone() or {}
            to_addr = [c.get("email")] if c.get("email") else []

            if to_addr:
                subject = f"Action required – Timesheet corrections for Week Ending {wk['week_ending'].strftime('%d/%m/%Y')}"
                body = (
                    f"Hi,\n\nWe couldn’t approve your timesheet (and any invoice submitted with it). "
                    f"Please review and fix the following:\n\n- {reason.strip()}\n\n"
                    "Then resubmit your timesheet and create a new invoice for the week. We will approve or reject both together.\n\nThanks."
                )
                html_body = (
                    f"<p>We couldn’t approve your timesheet (and any invoice submitted with it).</p>"
                    f"<p><strong>What to change:</strong> {reason.strip()}</p>"
                    "<p>Please make the corrections, resubmit your timesheet, and create a new invoice for the week. We will approve or reject both together.</p>"
                )

                try:
                    EmailManager().send_email(subject=subject, body=body,
                                              recipients=to_addr, html_body=html_body)
                except Exception as e:
                    print(f"[WARN] Failed to send rejection email: {e}")

        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _refresh_week_totals(cur, user_id: int, week_pk: int) -> None:
        """
        Refreshes the aggregate totals for a given user/week.
        Args:
            cur: Open DB cursor
            user_id: Contractor ID
            week_pk: Week primary key
        """
        # cur is an open cursor inside a transaction
        cur.execute("""
            UPDATE tb_timesheet_weeks w
            LEFT JOIN (
                SELECT week_id,
                       SUM(actual_hours) AS th,
                       SUM(pay) AS tp,
                       SUM(travel_parking) AS tt,
                       SUM(lateness_mins) AS tl,
                       SUM(overrun_mins) AS tovr
                FROM tb_timesheet_entries
                WHERE user_id=%s AND week_id=%s
            ) agg ON agg.week_id = w.id
            SET w.total_hours = COALESCE(agg.th,0),
                w.total_pay = COALESCE(agg.tp,0),
                w.total_travel = COALESCE(agg.tt,0),
                w.total_lateness_mins = COALESCE(agg.tl,0),
                w.total_overrun_mins = COALESCE(agg.tovr,0)
            WHERE w.id=%s
        """, (user_id, week_pk, week_pk))

    @staticmethod
    def refresh_entries_actuals(
        cur, conn, week_pk: int, user_id: int, work_date: date, runsheet_id: int
    ) -> None:
        """
        After actual_start/actual_end are updated (e.g. from work app), recompute
        actual_hours, labour_hours, wage_rate_used, pay (and lateness/overrun/variance)
        for the affected timesheet entries and update the week totals.
        Call this with the same conn/cur used for the UPDATE so it runs in the same transaction.
        """
        cur.execute("""
            SELECT id, work_date, scheduled_start, scheduled_end, actual_start, actual_end,
                   break_mins, job_type_id, client_name, site_name, source, runsheet_id,
                   lock_job_client, notes
            FROM tb_timesheet_entries
            WHERE week_id=%s AND user_id=%s AND work_date=%s AND runsheet_id=%s
        """, (week_pk, user_id, work_date, runsheet_id))
        rows = cur.fetchall() or []
        if not rows:
            return
        cur.execute(
            "SELECT id, role_id FROM tb_contractors WHERE id=%s", (user_id,)
        )
        contractor_row = cur.fetchone()
        contractor = contractor_row or {"id": user_id, "role_id": None}
        for row in rows:
            if row.get("actual_start") is None or row.get("actual_end") is None:
                continue
            entry = {
                "work_date": row["work_date"],
                "scheduled_start": row.get("scheduled_start"),
                "scheduled_end": row.get("scheduled_end"),
                "actual_start": row.get("actual_start"),
                "actual_end": row.get("actual_end"),
                "break_mins": row.get("break_mins") or 0,
                "job_type_id": row["job_type_id"],
                "client_name": row.get("client_name"),
                "site_name": row.get("site_name"),
                "source": row.get("source"),
                "runsheet_id": row.get("runsheet_id"),
                "lock_job_client": row.get("lock_job_client"),
                "notes": row.get("notes"),
            }
            try:
                computed = TimesheetService._compute_and_fill(entry, contractor)
            except Exception:
                continue
            cur.execute("""
                UPDATE tb_timesheet_entries
                SET actual_hours=%s, labour_hours=%s, wage_rate_used=%s, pay=%s,
                    scheduled_hours=%s, lateness_mins=%s, overrun_mins=%s, variance_mins=%s
                WHERE id=%s
            """, (
                computed["actual_hours"], computed["labour_hours"],
                computed["wage_rate_used"], computed["pay"],
                computed["scheduled_hours"], computed["lateness_mins"],
                computed["overrun_mins"], computed["variance_mins"],
                row["id"],
            ))
        TimesheetService._refresh_week_totals(cur, user_id, week_pk)


# ---------- Contractor Invoice Service (self-employed) ----------
#
# Employment type (PAYE vs self-employed) is kept in Time Billing by default so
# you don't need to install HR (or any other module) just for invoicing. If HR
# module is installed and later exposes get_contractor_employment_type(contractor_id),
# we use that when present so HR can be the single place to edit.

class InvoiceService:
    """Invoicing for self-employed contractors: create invoice from approved week; void on reject."""

    @staticmethod
    def get_contractor_employment_type(contractor_id: int) -> str:
        """
        Return 'paye' or 'self_employed'.
        Optional: if HR module exposes get_contractor_employment_type(contractor_id) -> str | None,
        that value is used when present. Otherwise uses tb_contractors.employment_type.
        Keeps Time Billing usable without HR; HR can become source of truth when installed.
        """
        try:
            from app.plugins import hr_module
            getter = getattr(hr_module, "get_contractor_employment_type", None)
            if callable(getter):
                val = getter(contractor_id)
                if val in ("paye", "self_employed"):
                    return val
        except Exception:
            pass
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT employment_type FROM tb_contractors WHERE id=%s",
                (contractor_id,),
            )
            row = cur.fetchone()
            return (row.get("employment_type") or "self_employed") if row else "self_employed"
        except Exception:
            return "self_employed"
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_next_invoice_number(contractor_id: int) -> str:
        """Suggest next invoice number: max existing numeric + 1, or '1' if none."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT invoice_number FROM contractor_invoices
                WHERE contractor_id=%s AND status != 'void'
                ORDER BY id DESC LIMIT 1
                """,
                (contractor_id,),
            )
            row = cur.fetchone()
            if not row or not row.get("invoice_number"):
                return "1"
            try:
                num = int(str(row["invoice_number"]).strip())
                return str(num + 1)
            except (ValueError, TypeError):
                return "1"
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_week_invoice_info(timesheet_week_id: int) -> dict:
        """Return dict: has_voided_invoice (bool), current_invoice (id + status or None)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT id, status, invoice_number, total_amount, void_reason
                FROM contractor_invoices
                WHERE timesheet_week_id=%s
                ORDER BY id DESC
                """,
                (timesheet_week_id,),
            )
            rows = cur.fetchall() or []
            has_voided = any(r.get("status") == "void" for r in rows)
            current = None
            for r in rows:
                if r.get("status") in ("draft", "sent"):
                    current = r
                    break
            return {"has_voided_invoice": has_voided, "current_invoice": current}
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_uninvoiced_entries(timesheet_week_id: int, contractor_id: int) -> List[Dict[str, Any]]:
        """Entries for this week with no invoice_id (or invoice is void). Used to autofill invoice."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT e.id, e.work_date, e.client_name, e.site_name, jt.name AS job_type_name,
                       e.actual_hours, e.labour_hours, e.pay, e.travel_parking
                FROM tb_timesheet_entries e
                JOIN job_types jt ON jt.id = e.job_type_id
                WHERE e.week_id=%s AND e.user_id=%s
                  AND (e.invoice_id IS NULL OR EXISTS (
                    SELECT 1 FROM contractor_invoices i
                    WHERE i.id = e.invoice_id AND i.status = 'void'
                  ))
                ORDER BY e.work_date, e.id
                """,
                (timesheet_week_id, contractor_id),
            )
            rows = cur.fetchall() or []
            for r in rows:
                if isinstance(r.get("work_date"), (date, datetime)):
                    r["work_date"] = r["work_date"].strftime("%Y-%m-%d")
                for k in ("actual_hours", "labour_hours", "pay", "travel_parking"):
                    if k in r and r[k] is not None:
                        try:
                            r[k] = float(r[k])
                        except (TypeError, ValueError):
                            r[k] = 0.0
            return rows
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_invoice(
        contractor_id: int,
        timesheet_week_id: int,
        invoice_number: str,
        mark_sent: bool = True,
    ) -> dict:
        """
        Create invoice from uninvoiced entries for this week; link entries; set week status to invoiced.
        Returns dict with id, invoice_number, total_amount, status.
        """
        if not invoice_number or not str(invoice_number).strip():
            raise ValueError("Invoice number is required.")
        entries = InvoiceService.get_uninvoiced_entries(timesheet_week_id, contractor_id)
        if not entries:
            raise ValueError("No uninvoiced entries for this week.")
        total = sum((e.get("pay") or 0) + (e.get("travel_parking") or 0) for e in entries)
        entry_ids = [e["id"] for e in entries]
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                INSERT INTO contractor_invoices
                (contractor_id, timesheet_week_id, invoice_number, total_amount, status, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    contractor_id,
                    timesheet_week_id,
                    str(invoice_number).strip(),
                    total,
                    "sent" if mark_sent else "draft",
                    datetime.utcnow() if mark_sent else None,
                ),
            )
            conn.commit()
            inv_id = cur.lastrowid
            placeholders = ",".join(["%s"] * len(entry_ids))
            cur.execute(
                f"UPDATE tb_timesheet_entries SET invoice_id=%s WHERE id IN ({placeholders})",
                [inv_id] + entry_ids,
            )
            if mark_sent:
                cur.execute(
                    "UPDATE tb_timesheet_weeks SET status='invoiced' WHERE id=%s",
                    (timesheet_week_id,),
                )
            conn.commit()
            return {
                "id": inv_id,
                "invoice_number": str(invoice_number).strip(),
                "total_amount": round(total, 2),
                "status": "sent" if mark_sent else "draft",
            }
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def finalize_draft_invoice_for_week(contractor_id: int, timesheet_week_pk: int) -> Optional[Dict[str, Any]]:
        """
        Contractor self-service: timesheet must be **approved** and a **draft** invoice must exist
        for this week (created while week was still submitted). Promotes draft → sent and week → invoiced.
        Fixes the case where approval did not run the admin finalize path or data was migrated.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, status, user_id FROM tb_timesheet_weeks WHERE id=%s LIMIT 1",
                (int(timesheet_week_pk),),
            )
            wk = cur.fetchone()
            if not wk or int(wk["user_id"]) != int(contractor_id):
                return None
            st = (wk.get("status") or "").lower()
            if st != "approved":
                raise ValueError(
                    "Timesheet must be approved before you can finalize the invoice. "
                    "If it is still submitted, wait for admin approval."
                )
            cur.execute(
                """
                SELECT id, invoice_number, total_amount FROM contractor_invoices
                WHERE timesheet_week_id=%s AND contractor_id=%s AND status='draft'
                ORDER BY id DESC LIMIT 1
                """,
                (int(timesheet_week_pk), int(contractor_id)),
            )
            inv = cur.fetchone()
            if not inv:
                return None
            inv_id = int(inv["id"])
            cur.execute(
                """
                UPDATE contractor_invoices
                SET status='sent', sent_at=%s
                WHERE id=%s AND status='draft'
                """,
                (datetime.utcnow(), inv_id),
            )
            if cur.rowcount == 0:
                conn.rollback()
                return None
            cur.execute(
                "UPDATE tb_timesheet_weeks SET status='invoiced' WHERE id=%s",
                (int(timesheet_week_pk),),
            )
            conn.commit()
            return {
                "id": inv_id,
                "invoice_number": inv.get("invoice_number"),
                "total_amount": float(inv.get("total_amount") or 0),
                "status": "sent",
            }
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def void_invoice_for_week(timesheet_week_id: int, reason: str) -> None:
        """Void any non-void invoice for this week; clear invoice_id on entries; do not change week status (caller sets rejected)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id FROM contractor_invoices WHERE timesheet_week_id=%s AND status != 'void'",
                (timesheet_week_id,),
            )
            rows = cur.fetchall() or []
            if not rows:
                return
            now = datetime.utcnow()
            vreason = (reason or "")[:255]
            for row in rows:
                inv_id = row["id"]
                cur.execute(
                    """
                    UPDATE contractor_invoices
                    SET status='void', voided_at=%s, void_reason=%s
                    WHERE id=%s
                    """,
                    (now, vreason, inv_id),
                )
                cur.execute("UPDATE tb_timesheet_entries SET invoice_id=NULL WHERE invoice_id=%s", (inv_id,))
            # Allow a new invoice for the same week after void (e.g. rejection cycle or admin correction).
            cur.execute(
                """
                UPDATE tb_timesheet_weeks
                SET status='approved'
                WHERE id=%s AND status='invoiced'
                """,
                (timesheet_week_id,),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_invoices_for_contractor(contractor_id: int, limit: int = 200) -> List[Dict[str, Any]]:
        """All invoices for contractor with week id string and timesheet status (for portal list)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT i.id, i.invoice_number, i.total_amount, i.status, i.sent_at, i.voided_at, i.void_reason,
                       i.created_at, i.timesheet_week_id,
                       w.week_id AS timesheet_week_code, w.status AS timesheet_status,
                       w.week_ending, w.rejection_reason
                FROM contractor_invoices i
                JOIN tb_timesheet_weeks w ON w.id = i.timesheet_week_id
                WHERE i.contractor_id = %s
                ORDER BY i.id DESC
                LIMIT %s
                """,
                (int(contractor_id), int(limit)),
            )
            rows = cur.fetchall() or []
            for r in rows:
                we = r.get("week_ending")
                if hasattr(we, "strftime"):
                    r["week_ending"] = we.strftime("%Y-%m-%d")
            return rows
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_invoice_history_for_week(timesheet_week_pk: int) -> List[Dict[str, Any]]:
        """All invoice rows for a week (voided + current), newest first."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT id, invoice_number, total_amount, status, sent_at, voided_at, void_reason, created_at
                FROM contractor_invoices
                WHERE timesheet_week_id = %s
                ORDER BY id DESC
                """,
                (int(timesheet_week_pk),),
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_invoice_detail_for_contractor(invoice_id: int, contractor_id: int) -> Optional[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT i.*, w.week_id AS timesheet_week_code, w.status AS timesheet_status,
                       w.week_ending, w.rejection_reason, w.submitted_at, w.approved_at
                FROM contractor_invoices i
                JOIN tb_timesheet_weeks w ON w.id = i.timesheet_week_id
                WHERE i.id = %s AND i.contractor_id = %s
                LIMIT 1
                """,
                (int(invoice_id), int(contractor_id)),
            )
            inv = cur.fetchone()
            if not inv:
                return None
            we = inv.get("week_ending")
            if hasattr(we, "strftime"):
                inv["week_ending"] = we.strftime("%Y-%m-%d")
            cur.execute(
                """
                SELECT e.id, e.work_date, e.client_name, e.site_name, jt.name AS job_type_name,
                       e.actual_hours, e.pay, e.travel_parking
                FROM tb_timesheet_entries e
                JOIN job_types jt ON jt.id = e.job_type_id
                WHERE e.invoice_id = %s
                ORDER BY e.work_date, e.id
                """,
                (int(invoice_id),),
            )
            lines = cur.fetchall() or []
            for r in lines:
                wd = r.get("work_date")
                if hasattr(wd, "strftime"):
                    r["work_date"] = wd.strftime("%Y-%m-%d")
                for k in ("actual_hours", "pay", "travel_parking"):
                    if k in r and r[k] is not None:
                        try:
                            r[k] = float(r[k])
                        except (TypeError, ValueError):
                            r[k] = 0.0
            inv["lines"] = lines
            inv["history"] = InvoiceService.list_invoice_history_for_week(int(inv["timesheet_week_id"]))
            return inv
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_invoice_detail_admin(invoice_id: int) -> Optional[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT i.*, c.name AS contractor_name, c.email AS contractor_email,
                       w.week_id AS timesheet_week_code, w.status AS timesheet_status,
                       w.week_ending, w.rejection_reason, i.contractor_id AS contractor_id
                FROM contractor_invoices i
                JOIN tb_contractors c ON c.id = i.contractor_id
                JOIN tb_timesheet_weeks w ON w.id = i.timesheet_week_id
                WHERE i.id = %s
                LIMIT 1
                """,
                (int(invoice_id),),
            )
            inv = cur.fetchone()
            if not inv:
                return None
            we = inv.get("week_ending")
            if hasattr(we, "strftime"):
                inv["week_ending"] = we.strftime("%Y-%m-%d")
            cur.execute(
                """
                SELECT e.id, e.work_date, e.client_name, e.site_name, jt.name AS job_type_name,
                       e.actual_hours, e.pay, e.travel_parking
                FROM tb_timesheet_entries e
                JOIN job_types jt ON jt.id = e.job_type_id
                WHERE e.invoice_id = %s
                ORDER BY e.work_date, e.id
                """,
                (int(invoice_id),),
            )
            lines = cur.fetchall() or []
            for r in lines:
                wd = r.get("work_date")
                if hasattr(wd, "strftime"):
                    r["work_date"] = wd.strftime("%Y-%m-%d")
            inv["lines"] = lines
            inv["history"] = InvoiceService.list_invoice_history_for_week(int(inv["timesheet_week_id"]))
            return inv
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_invoices_admin(
        contractor_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 300,
    ) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = ["1=1"]
            params: List[Any] = []
            if contractor_id:
                where.append("i.contractor_id = %s")
                params.append(int(contractor_id))
            if status and str(status).lower() in ("draft", "sent", "void"):
                where.append("i.status = %s")
                params.append(str(status).lower())
            params.append(int(limit))
            cur.execute(
                f"""
                SELECT i.id, i.contractor_id, i.timesheet_week_id, i.invoice_number, i.total_amount,
                       i.status, i.sent_at, i.voided_at, i.created_at,
                       c.name AS contractor_name, c.email AS contractor_email,
                       w.week_id AS timesheet_week_code, w.status AS timesheet_status
                FROM contractor_invoices i
                JOIN tb_contractors c ON c.id = i.contractor_id
                JOIN tb_timesheet_weeks w ON w.id = i.timesheet_week_id
                WHERE {' AND '.join(where)}
                ORDER BY i.id DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cur.fetchall() or []
            for r in rows:
                pass
            return rows
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def admin_void_invoice(invoice_id: int, reason: str) -> bool:
        """Void a specific invoice by id; clear entry links; reopen week if it was invoiced."""
        reason = (reason or "").strip() or "Voided by administrator."
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, timesheet_week_id, status FROM contractor_invoices WHERE id = %s",
                (int(invoice_id),),
            )
            row = cur.fetchone()
            if not row or row.get("status") == "void":
                return False
            week_pk = int(row["timesheet_week_id"])
            cur.execute(
                """
                UPDATE contractor_invoices
                SET status='void', voided_at=%s, void_reason=%s
                WHERE id=%s
                """,
                (datetime.utcnow(), reason[:255], int(invoice_id)),
            )
            cur.execute("UPDATE tb_timesheet_entries SET invoice_id=NULL WHERE invoice_id=%s", (int(invoice_id),))
            cur.execute(
                """
                UPDATE tb_timesheet_weeks
                SET status='approved'
                WHERE id=%s AND status='invoiced'
                """,
                (week_pk,),
            )
            conn.commit()
            return True
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_contractor_billing_profile(contractor_id: int) -> Dict[str, Any]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            try:
                cur.execute(
                    """
                    SELECT id, name, email,
                           invoice_business_name, invoice_address_line1, invoice_address_line2,
                           invoice_city, invoice_postcode, invoice_country
                    FROM tb_contractors
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(contractor_id),),
                )
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.execute(
                    "SELECT id, name, email FROM tb_contractors WHERE id = %s LIMIT 1",
                    (int(contractor_id),),
                )
            row = cur.fetchone()
            return dict(row) if row else {}
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def save_contractor_billing_profile(contractor_id: int, data: Dict[str, Any]) -> bool:
        fields = {
            "invoice_business_name": (data.get("invoice_business_name") or "").strip() or None,
            "invoice_address_line1": (data.get("invoice_address_line1") or "").strip() or None,
            "invoice_address_line2": (data.get("invoice_address_line2") or "").strip() or None,
            "invoice_city": (data.get("invoice_city") or "").strip() or None,
            "invoice_postcode": (data.get("invoice_postcode") or "").strip() or None,
            "invoice_country": (data.get("invoice_country") or "").strip() or None,
        }
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE tb_contractors SET
                  invoice_business_name = %s,
                  invoice_address_line1 = %s,
                  invoice_address_line2 = %s,
                  invoice_city = %s,
                  invoice_postcode = %s,
                  invoice_country = %s
                WHERE id = %s
                """,
                (
                    fields["invoice_business_name"],
                    fields["invoice_address_line1"],
                    fields["invoice_address_line2"],
                    fields["invoice_city"],
                    fields["invoice_postcode"],
                    fields["invoice_country"],
                    int(contractor_id),
                ),
            )
            conn.commit()
            return cur.rowcount > 0
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _invoice_from_address_lines(contractor_id: int) -> List[str]:
        """Lines for 'From' on PDF: portal billing first, else HR staff address if available."""
        prof = InvoiceService.get_contractor_billing_profile(contractor_id)
        lines: List[str] = []
        name = (prof.get("invoice_business_name") or prof.get("name") or "").strip()
        if name:
            lines.append(name)
        a1 = (prof.get("invoice_address_line1") or "").strip()
        a2 = (prof.get("invoice_address_line2") or "").strip()
        city = (prof.get("invoice_city") or "").strip()
        pc = (prof.get("invoice_postcode") or "").strip()
        country = (prof.get("invoice_country") or "").strip()
        if a1:
            lines.append(a1)
        if a2:
            lines.append(a2)
        city_line = ", ".join(x for x in (city, pc) if x)
        if city_line:
            lines.append(city_line)
        if country:
            lines.append(country)
        if len(lines) <= 1:
            try:
                from app.plugins import hr_module

                getter = getattr(hr_module, "get_contractor_invoice_address_lines", None)
                if callable(getter):
                    alt = getter(int(contractor_id)) or []
                    if alt:
                        return list(alt)
            except Exception:
                pass
        return lines

    @staticmethod
    def generate_invoice_pdf(
        invoice_id: int,
        contractor_id: Optional[int] = None,
    ) -> Tuple[bytes, str]:
        """
        Contractor PDF when contractor_id is set (access check).
        Admin PDF when contractor_id is None (uses invoice id only).
        """
        from io import BytesIO
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

        if contractor_id is not None:
            inv = InvoiceService.get_invoice_detail_for_contractor(int(invoice_id), int(contractor_id))
        else:
            inv = InvoiceService.get_invoice_detail_admin(int(invoice_id))
        if not inv:
            return b"", "invoice.pdf"
        cid = int(inv.get("contractor_id") or contractor_id or 0)
        lines = inv.get("lines") or []
        buf = BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=A4,
            leftMargin=18 * mm,
            rightMargin=18 * mm,
            topMargin=16 * mm,
            bottomMargin=16 * mm,
            title=f"Invoice {inv.get('invoice_number')}",
        )
        styles = getSampleStyleSheet()
        story = []
        story.append(Paragraph("<b>Tax invoice</b>", styles["Title"]))
        story.append(Spacer(1, 6 * mm))
        from_lines = InvoiceService._invoice_from_address_lines(cid)
        if from_lines:
            story.append(Paragraph("<b>From</b>", styles["Heading4"]))
            for ln in from_lines:
                story.append(Paragraph(ln.replace("&", "&amp;"), styles["Normal"]))
            story.append(Spacer(1, 4 * mm))
        story.append(Paragraph(f"<b>Invoice #</b> {inv.get('invoice_number')}", styles["Normal"]))
        story.append(
            Paragraph(
                f"<b>Week</b> {inv.get('timesheet_week_code')} (ending {inv.get('week_ending')})",
                styles["Normal"],
            )
        )
        story.append(
            Paragraph(
                f"<b>Timesheet</b> {inv.get('timesheet_status') or ''} &nbsp; "
                f"<b>Invoice status</b> {inv.get('status') or ''}",
                styles["Normal"],
            )
        )
        if inv.get("void_reason"):
            story.append(Paragraph(f"<i>Void reason: {str(inv.get('void_reason')).replace('&', '&amp;')}</i>", styles["Small"]))
        story.append(Spacer(1, 6 * mm))
        tbl_data = [["Date", "Client", "Site", "Job", "Hrs", "Pay", "Travel"]]
        for r in lines:
            tbl_data.append(
                [
                    str(r.get("work_date") or ""),
                    str(r.get("client_name") or "")[:28],
                    str(r.get("site_name") or "")[:22],
                    str(r.get("job_type_name") or "")[:20],
                    f"{float(r.get('actual_hours') or 0):.2f}",
                    f"£{float(r.get('pay') or 0):.2f}",
                    f"£{float(r.get('travel_parking') or 0):.2f}",
                ]
            )
        tbl_data.append(
            [
                "",
                "",
                "",
                "Total",
                "",
                f"£{float(inv.get('total_amount') or 0):.2f}",
                "",
            ]
        )
        t = Table(tbl_data, repeatRows=1, colWidths=[22 * mm, 32 * mm, 28 * mm, 28 * mm, 18 * mm, 22 * mm, 22 * mm])
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eeeeee")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("ALIGN", (4, 1), (-1, -1), "RIGHT"),
                ]
            )
        )
        story.append(t)
        story.append(Spacer(1, 8 * mm))
        story.append(Paragraph("<i>Generated from Sparrow Time Billing.</i>", styles["Italic"]))
        doc.build(story)
        pdf_bytes = buf.getvalue()
        safe_num = "".join(c if c.isalnum() or c in "-_" else "_" for c in str(inv.get("invoice_number") or "invoice"))
        return pdf_bytes, f"invoice_{safe_num}.pdf"

    @staticmethod
    def admin_invoice_analytics(year: Optional[int] = None) -> Dict[str, Any]:
        """
        Admin: totals by period (month, ISO timesheet week) and per-contractor YTD-style rollups.
        Amounts use timesheet week_ending for the calendar year filter. Excludes void invoices from sums.
        """
        y = int(year) if year is not None else datetime.utcnow().year
        today = date.today()
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT COUNT(*) AS cnt, COALESCE(SUM(i.total_amount), 0) AS total
                FROM contractor_invoices i
                JOIN tb_timesheet_weeks w ON w.id = i.timesheet_week_id
                WHERE i.status = 'sent' AND YEAR(w.week_ending) = %s
                """,
                (y,),
            )
            ytd_sent = cur.fetchone() or {}

            cur.execute(
                """
                SELECT COUNT(*) AS cnt, COALESCE(SUM(i.total_amount), 0) AS total
                FROM contractor_invoices i
                JOIN tb_timesheet_weeks w ON w.id = i.timesheet_week_id
                WHERE i.status = 'draft' AND YEAR(w.week_ending) = %s
                """,
                (y,),
            )
            ytd_draft = cur.fetchone() or {}

            cur.execute(
                """
                SELECT COUNT(*) AS cnt, COALESCE(SUM(i.total_amount), 0) AS total
                FROM contractor_invoices i
                JOIN tb_timesheet_weeks w ON w.id = i.timesheet_week_id
                WHERE i.status = 'sent'
                  AND YEAR(w.week_ending) = YEAR(%s) AND MONTH(w.week_ending) = MONTH(%s)
                """,
                (today, today),
            )
            month_sent = cur.fetchone() or {}

            cur.execute(
                """
                SELECT w.week_id AS iso_week, w.week_ending,
                       COUNT(i.id) AS inv_count,
                       COALESCE(SUM(i.total_amount), 0) AS total_sent
                FROM contractor_invoices i
                JOIN tb_timesheet_weeks w ON w.id = i.timesheet_week_id
                WHERE i.status = 'sent' AND YEAR(w.week_ending) = %s
                GROUP BY w.week_id, w.week_ending
                ORDER BY w.week_ending DESC
                LIMIT 60
                """,
                (y,),
            )
            by_iso_week = cur.fetchall() or []
            for r in by_iso_week:
                we = r.get("week_ending")
                if hasattr(we, "strftime"):
                    r["week_ending"] = we.strftime("%Y-%m-%d")
                for k in ("total_sent",):
                    if k in r and r[k] is not None:
                        r[k] = float(r[k])

            cur.execute(
                """
                SELECT DATE_FORMAT(w.week_ending, '%%Y-%%m') AS ymonth,
                       COUNT(i.id) AS inv_count,
                       COALESCE(SUM(i.total_amount), 0) AS total_sent
                FROM contractor_invoices i
                JOIN tb_timesheet_weeks w ON w.id = i.timesheet_week_id
                WHERE i.status = 'sent' AND YEAR(w.week_ending) = %s
                GROUP BY ymonth
                ORDER BY ymonth DESC
                """,
                (y,),
            )
            by_month = cur.fetchall() or []
            for r in by_month:
                if r.get("total_sent") is not None:
                    r["total_sent"] = float(r["total_sent"])

            cur.execute(
                """
                SELECT i.contractor_id, c.name AS contractor_name, c.email AS contractor_email,
                       COUNT(i.id) AS invoice_count,
                       COALESCE(SUM(CASE WHEN i.status = 'sent' THEN i.total_amount ELSE 0 END), 0) AS total_sent,
                       COALESCE(SUM(CASE WHEN i.status = 'draft' THEN i.total_amount ELSE 0 END), 0) AS total_draft
                FROM contractor_invoices i
                JOIN tb_contractors c ON c.id = i.contractor_id
                JOIN tb_timesheet_weeks w ON w.id = i.timesheet_week_id
                WHERE i.status != 'void' AND YEAR(w.week_ending) = %s
                GROUP BY i.contractor_id, c.name, c.email
                ORDER BY total_sent DESC, contractor_name ASC
                """,
                (y,),
            )
            by_contractor = cur.fetchall() or []
            for r in by_contractor:
                for k in ("total_sent", "total_draft"):
                    if k in r and r[k] is not None:
                        r[k] = float(r[k])

            return {
                "year": y,
                "ytd_sent": {
                    "count": int(ytd_sent.get("cnt") or 0),
                    "total": float(ytd_sent.get("total") or 0),
                },
                "ytd_draft": {
                    "count": int(ytd_draft.get("cnt") or 0),
                    "total": float(ytd_draft.get("total") or 0),
                },
                "current_month_sent": {
                    "count": int(month_sent.get("cnt") or 0),
                    "total": float(month_sent.get("total") or 0),
                },
                "by_iso_week": by_iso_week,
                "by_month": by_month,
                "by_contractor": by_contractor,
            }
        finally:
            cur.close()
            conn.close()


# ---------- Runsheet Service ----------

class RunsheetService:
    @staticmethod
    def list_runsheets(client_name=None, job_type_id=None, week_id=None) -> List[Dict[str, Any]]:
        """
        List runsheets, optionally filtered by client, job type, or week.

        Accepts:
        - client_name: either a numeric client_id or a name fragment
        - job_type_id: either numeric id or name fragment
        - week_id: ISO 'YYYYWW' to filter by work_date range
        """
        from datetime import date, timedelta

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = ["1=1"]
            params = []

            # Client filter: accept id or name fragment
            cn = (client_name or "").strip()
            if cn:
                if cn.isdigit():
                    where.append("r.client_id = %s")
                    params.append(int(cn))
                else:
                    where.append(
                        "LOWER(COALESCE(c.name, r.client_free_text, '')) LIKE %s"
                    )
                    params.append(f"%{cn.lower()}%")

            # Job type filter: accept id or name fragment
            jt = (job_type_id or "").strip()
            if jt:
                if jt.isdigit():
                    where.append("r.job_type_id = %s")
                    params.append(int(jt))
                else:
                    where.append("LOWER(jt.name) LIKE %s")
                    params.append(f"%{jt.lower()}%")

            # Week filter: ISO YYYYWW → date range on work_date
            wk = (week_id or "").strip()
            if wk:
                year, wknum = int(wk[:4]), int(wk[4:])
                jan4 = date(year, 1, 4)
                monday = jan4 + \
                    timedelta(days=(0 - jan4.weekday())) + \
                    timedelta(weeks=wknum - 1)
                sunday = monday + timedelta(days=6)
                where.append("r.work_date BETWEEN %s AND %s")
                params += [monday, sunday]

            sql = f"""
                SELECT
                r.id,
                r.work_date,
                COALESCE(r.status, 'draft') AS status,
                r.client_id,
                COALESCE(c.name, r.client_free_text) AS client_name,
                r.site_id,
                COALESCE(s.name, r.site_free_text) AS site_name,
                r.job_type_id,
                jt.name AS job_type_name,
                jt.colour_hex AS job_type_colour_hex,
                (SELECT COUNT(1)
                    FROM runsheet_assignments ra
                    WHERE ra.runsheet_id = r.id) AS assignees
                FROM runsheets r
                LEFT JOIN clients c    ON c.id  = r.client_id
                LEFT JOIN sites s      ON s.id  = r.site_id
                LEFT JOIN job_types jt ON jt.id = r.job_type_id
                WHERE {" AND ".join(where)}
                ORDER BY r.work_date DESC, r.id DESC
            """

            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []

            # Normalize types/strings for UI
            for r in rows:
                # Ensure status lowercase for badge logic
                r["status"] = (r.get("status") or "draft").lower()
                # Make work_date ISO string if it’s a date
                wd = r.get("work_date")
                if hasattr(wd, "isoformat"):
                    r["work_date"] = wd.isoformat()

            return rows
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _template_allows_free_text_client_site(cur, template_id: Optional[Any]) -> bool:
        if template_id is None or template_id == "":
            return False
        try:
            tid = int(template_id)
        except (TypeError, ValueError):
            return False
        cur.execute(
            "SELECT allow_free_text_client_site FROM runsheet_templates WHERE id=%s",
            (tid,),
        )
        row = cur.fetchone()
        if not row:
            return False
        v = row[0] if not isinstance(row, dict) else row.get("allow_free_text_client_site")
        return v in (1, "1", True)

    @staticmethod
    def create_runsheet(data: Dict[str, Any]) -> int:
        """
        Create a new runsheet record. Uses client_id, site_id (FKs), or free-text
        client/site when the template allows it (adhoc / emergency).
        """
        # Accept client_id or legacy client_name (treated as id if numeric)
        client_id = data.get("client_id")
        if client_id is None and data.get("client_name") is not None:
            try:
                client_id = int(data["client_name"])
            except (TypeError, ValueError):
                client_id = None
        site_id = data.get("site_id")
        if site_id is None and data.get("site_name") is not None:
            try:
                site_id = int(data["site_name"])
            except (TypeError, ValueError):
                site_id = None

        client_free_text = (data.get("client_free_text") or "").strip() or None
        site_free_text = (data.get("site_free_text") or "").strip() or None

        required = ["job_type_id", "work_date"]
        missing = [k for k in required if not data.get(k)]

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            allow_ft = RunsheetService._template_allows_free_text_client_site(
                cur, data.get("template_id")
            )
            if client_id:
                client_free_text = None
            if site_id:
                site_free_text = None

            if not client_id:
                if allow_ft and client_free_text:
                    pass
                elif allow_ft:
                    missing.append("client_id or client_free_text")
                else:
                    missing.append("client_id")
            if missing:
                raise Exception(f"Missing fields: {', '.join(missing)}")

            cur.execute("""
                INSERT INTO runsheets (client_id, client_free_text, site_id, site_free_text,
                                       job_type_id, work_date,
                                       window_start, window_end, template_id, template_version,
                                       payload_json, mapping_json, lead_user_id, status, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'draft',%s)
            """, (
                client_id, client_free_text, site_id, site_free_text,
                data["job_type_id"], data["work_date"],
                data.get("window_start"), data.get("window_end"),
                data.get("template_id"), data.get("template_version"),
                json.dumps(data.get("payload") or {}),
                json.dumps(data.get("mapping") or {}),
                data.get("lead_user_id"),
                data.get("notes")
            ))
            rs_new_id = cur.lastrowid
            # Optional crew rows from contractor UI (same shape as update_runsheet)
            assigns = data.get("assignments")
            if isinstance(assigns, list) and assigns:
                seen = set()
                for a in assigns:
                    uid = a.get("user_id") or a.get("contractor_id")
                    if not uid or int(uid) in seen:
                        continue
                    seen.add(int(uid))
                    pi = a.get("payroll_included")
                    payroll_included = 1 if pi in (None, True, 1, "1", "true") else 0
                    cur.execute(
                        """
                        INSERT INTO runsheet_assignments
                        (runsheet_id, user_id, scheduled_start, scheduled_end, actual_start, actual_end,
                         break_mins, travel_parking, notes, payroll_included)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            rs_new_id,
                            int(uid),
                            a.get("scheduled_start"),
                            a.get("scheduled_end"),
                            a.get("actual_start"),
                            a.get("actual_end"),
                            int(a.get("break_mins") or 0),
                            float(a.get("travel_parking") or 0),
                            a.get("notes"),
                            payroll_included,
                        ),
                    )
            conn.commit()
            return rs_new_id
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def ensure_lead_assignment(runsheet_id: int, lead_user_id: int) -> None:
        """
        If the lead has no assignment row yet, add one so publish can create their timesheet line.
        """
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT 1 FROM runsheet_assignments WHERE runsheet_id=%s AND user_id=%s",
                (runsheet_id, lead_user_id),
            )
            if cur.fetchone():
                return
            cur.execute(
                """
                INSERT INTO runsheet_assignments
                (runsheet_id, user_id, scheduled_start, scheduled_end, actual_start, actual_end,
                 break_mins, travel_parking, notes)
                VALUES (%s,%s,NULL,NULL,NULL,NULL,0,0,NULL)
                """,
                (runsheet_id, lead_user_id),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_runsheet(rs_id: int) -> Dict[str, Any]:
        """
        Retrieve a runsheet and its assignments by ID.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT r.*,
                       COALESCE(c.name, r.client_free_text) AS client_name,
                       COALESCE(s.name, r.site_free_text) AS site_name,
                       jt.name AS job_type_name, jt.colour_hex AS job_type_colour_hex
                FROM runsheets r
                LEFT JOIN clients c ON c.id=r.client_id
                LEFT JOIN sites s ON s.id=r.site_id
                JOIN job_types jt ON jt.id=r.job_type_id
                WHERE r.id=%s
            """, (rs_id,))
            rs = cur.fetchone() or {}

            # Get assignments
            cur.execute("""
                SELECT ra.*, u.name AS user_name
                FROM runsheet_assignments ra
                JOIN tb_contractors u ON u.id = ra.user_id
                WHERE ra.runsheet_id=%s
                ORDER BY ra.id ASC
            """, (rs_id,))
            rs["assignments"] = cur.fetchall() or []
            # Expose contractor_id for edit form (same as user_id)
            for a in rs["assignments"]:
                a["contractor_id"] = a.get("user_id")
            return rs
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_runsheet(rs_id: int, data: Dict[str, Any]) -> None:
        """
        Update runsheet header and optionally its assignments.
        Header uses client_id, site_id (FKs). Assignments use user_id (or contractor_id).
        """
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Map client_name/site_name to client_id/site_id for UPDATE
            header_updates = {}
            for k in ("client_id", "site_id", "job_type_id", "work_date", "window_start",
                      "window_end", "template_id", "template_version", "lead_user_id", "status", "notes",
                      "client_free_text", "site_free_text"):
                if k in data:
                    header_updates[k] = data[k]
            if "client_name" in data and data["client_name"] is not None and "client_id" not in data:
                try:
                    header_updates["client_id"] = int(data["client_name"])
                except (TypeError, ValueError):
                    pass
            if "site_name" in data and data["site_name"] is not None and "site_id" not in data:
                try:
                    header_updates["site_id"] = int(data["site_name"])
                except (TypeError, ValueError):
                    pass
            # Directory IDs take precedence over free-text labels
            if header_updates.get("client_id"):
                header_updates["client_free_text"] = None
            if header_updates.get("site_id"):
                header_updates["site_free_text"] = None
            if header_updates:
                fields = [f"{k}=%s" for k in header_updates]
                params = list(header_updates.values()) + [rs_id]
                cur.execute(f"UPDATE runsheets SET {', '.join(fields)} WHERE id=%s", params)
            if "payload" in data:
                cur.execute("UPDATE runsheets SET payload_json=%s WHERE id=%s",
                            (json.dumps(data["payload"]), rs_id))
            if "mapping" in data:
                cur.execute("UPDATE runsheets SET mapping_json=%s WHERE id=%s",
                            (json.dumps(data["mapping"]), rs_id))

            # Handle assignments (delete and reinsert). Accept user_id or contractor_id.
            if "assignments" in data and isinstance(data["assignments"], list):
                cur.execute(
                    "DELETE FROM runsheet_assignments WHERE runsheet_id=%s", (rs_id,))
                for a in data["assignments"]:
                    user_id = a.get("user_id") or a.get("contractor_id")
                    if not user_id:
                        continue
                    pi = a.get("payroll_included")
                    if pi is None:
                        payroll_included = 1
                    else:
                        payroll_included = 1 if pi in (True, 1, "1", "true", "True") else 0
                    cur.execute("""
                        INSERT INTO runsheet_assignments
                        (runsheet_id, user_id, scheduled_start, scheduled_end, actual_start, actual_end,
                         break_mins, travel_parking, notes, payroll_included)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        rs_id, user_id,
                        a.get("scheduled_start"), a.get("scheduled_end"),
                        a.get("actual_start"), a.get("actual_end"),
                        int(a.get("break_mins") or 0),
                        float(a.get("travel_parking") or 0),
                        a.get("notes"),
                        payroll_included,
                    ))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _assignment_payroll_active(assignment: Dict[str, Any]) -> bool:
        """If payroll_included is 0/false, skip timesheet create/update for this assignment."""
        v = assignment.get("payroll_included")
        if v is None:
            return True
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return int(v) != 0
        s = str(v).strip().lower()
        return s not in ("0", "false", "no")

    @staticmethod
    def _purge_timesheet_row_for_runsheet_user(
        cur,
        user_id: int,
        week_pk: int,
        work_date: Any,
        job_type_id: int,
        rs_id: int,
    ) -> None:
        cur.execute(
            """DELETE FROM tb_timesheet_entries
               WHERE user_id=%s AND week_id=%s AND work_date=%s AND job_type_id=%s
                 AND source='runsheet' AND runsheet_id=%s""",
            (user_id, week_pk, work_date, job_type_id, rs_id),
        )
        TimesheetService._refresh_week_totals(cur, user_id, week_pk)

    @staticmethod
    def publish_runsheet(rs_id: int, published_by: Optional[int]) -> Dict[str, Any]:
        """
        Publish a runsheet by creating/updating personal timesheet entries for each assignment.
        Returns:
            Dict with status and counts.
        """
        rs = RunsheetService.get_runsheet(rs_id)
        if not rs:
            return {"ok": False, "message": "Runsheet not found."}

        # Validate against template if present
        if rs.get("template_id"):
            tpl = TemplateService.get_runsheet_template(rs["template_id"])
            try:
                TemplateService.validate_runsheet_payload(
                    tpl, json.loads(rs.get("payload_json") or "{}")
                )
            except Exception as e:
                return {"ok": False, "message": f"Template validation failed: {e}"}

        # Mapping (if you later map names, you can enrich here)
        mapping = {}
        if rs.get("mapping_json"):
            mapping = json.loads(rs["mapping_json"])
        elif rs.get("template_id"):
            mapping = TemplateService.get_mapping_for_template(
                rs["template_id"]) or {}

        lock_job_client = 1  # generated entries are locked

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        created = 0
        updated = 0
        try:
            for a in (rs.get("assignments") or []):
                user_id = a["user_id"]

                if not RunsheetService._assignment_payroll_active(a):
                    week_id = RunsheetService._week_id_for_date(rs["work_date"])
                    wk = TimesheetService._ensure_week(user_id, week_id)
                    RunsheetService._purge_timesheet_row_for_runsheet_user(
                        cur, user_id, wk["id"], rs["work_date"],
                        int(rs["job_type_id"]), rs_id,
                    )
                    continue

                # Ensure user week exists
                week_id = RunsheetService._week_id_for_date(rs["work_date"])
                wk = TimesheetService._ensure_week(user_id, week_id)

                # Compute times (fallback to assignment times or header window)
                scheduled_start = a.get(
                    "scheduled_start") or rs.get("window_start")
                scheduled_end = a.get("scheduled_end") or rs.get("window_end")
                actual_start = a.get("actual_start") or scheduled_start
                actual_end = a.get("actual_end") or scheduled_end
                break_mins = int(a.get("break_mins") or 0)
                travel = float(a.get("travel_parking") or 0)
                notes = a.get("notes")

                # Upsert by composite key
                cur.execute(
                    """SELECT id FROM tb_timesheet_entries
                    WHERE user_id=%s AND week_id=%s AND work_date=%s AND job_type_id=%s
                        AND source='runsheet' AND runsheet_id=%s
                    LIMIT 1""",
                    (user_id, wk["id"], rs["work_date"],
                     rs["job_type_id"], rs_id)
                )
                row = cur.fetchone()

                payload = {
                    "client_name": rs.get("client_name"),
                    "site_name": rs.get("site_name"),
                    "job_type_id": rs["job_type_id"],
                    "work_date": rs["work_date"],
                    "scheduled_start": scheduled_start,
                    "scheduled_end": scheduled_end,
                    "actual_start": actual_start,
                    "actual_end": actual_end,
                    "break_mins": break_mins,
                    "travel_parking": travel,
                    "notes": notes,
                    "source": "runsheet",
                    "runsheet_id": rs_id,
                    "lock_job_client": lock_job_client
                }

                contractor = TimesheetService._get_contractor(user_id)
                computed = TimesheetService._compute_and_fill(
                    payload.copy(), contractor)

                if row:
                    updated += 1
                    set_clause = ", ".join([
                        "client_name=%(client_name)s",
                        "site_name=%(site_name)s",
                        "job_type_id=%(job_type_id)s",
                        "work_date=%(work_date)s",
                        "scheduled_start=%(scheduled_start)s",
                        "scheduled_end=%(scheduled_end)s",
                        "actual_start=%(actual_start)s",
                        "actual_end=%(actual_end)s",
                        "break_mins=%(break_mins)s",
                        "travel_parking=%(travel_parking)s",
                        "notes=%(notes)s",
                        "scheduled_hours=%(scheduled_hours)s",
                        "actual_hours=%(actual_hours)s",
                        "labour_hours=%(labour_hours)s",
                        "wage_rate_used=%(wage_rate_used)s",
                        "pay=%(pay)s",
                        "lateness_mins=%(lateness_mins)s",
                        "overrun_mins=%(overrun_mins)s",
                        "variance_mins=%(variance_mins)s",
                        "policy_applied=%(policy_applied)s",
                        "policy_source=%(policy_source)s",
                        "lock_job_client=%(lock_job_client)s"
                    ])
                    sql = f"UPDATE tb_timesheet_entries SET {set_clause} WHERE id=%(id)s"
                    params = computed.copy()
                    params["id"] = row["id"]
                    cur.execute(sql, params)
                else:
                    cols = [
                        "week_id", "user_id",
                        "client_name", "site_name",
                        "job_type_id", "work_date",
                        "scheduled_start", "scheduled_end",
                        "actual_start", "actual_end",
                        "break_mins", "travel_parking",
                        "notes", "source", "runsheet_id", "lock_job_client",
                        "scheduled_hours", "actual_hours", "labour_hours", "wage_rate_used", "pay",
                        "lateness_mins", "overrun_mins", "variance_mins", "policy_applied", "policy_source"
                    ]
                    placeholders = ", ".join([f"%({k})s" for k in cols])
                    params = computed.copy()
                    params.update({"week_id": wk["id"], "user_id": user_id})
                    sql = f"INSERT INTO tb_timesheet_entries ({', '.join(cols)}) VALUES ({placeholders})"
                    cur.execute(sql, params)
                    created += 1

                # Refresh totals per user/week
                TimesheetService._refresh_week_totals(cur, user_id, wk["id"])

            # Mark runsheet as published
            cur.execute(
                "UPDATE runsheets SET status='published' WHERE id=%s", (rs_id,))
            conn.commit()

            return {"ok": True, "created": created, "updated": updated}

        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_runsheets_for_contractor(contractor_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        """Runsheets where the contractor is lead or has an assignment (draft/published)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT DISTINCT r.id, r.work_date, r.status, r.client_id, r.site_id, r.job_type_id,
                       r.template_id, r.lead_user_id, r.notes,
                       c.name AS client_name, jt.name AS job_type_name
                FROM runsheets r
                JOIN clients c ON c.id = r.client_id
                JOIN job_types jt ON jt.id = r.job_type_id
                LEFT JOIN runsheet_assignments ra
                    ON ra.runsheet_id = r.id AND ra.user_id = %s
                WHERE r.lead_user_id = %s OR ra.id IS NOT NULL
                ORDER BY r.work_date DESC, r.id DESC
                LIMIT %s
                """,
                (contractor_id, contractor_id, int(limit)),
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def contractor_can_access_runsheet(runsheet_id: int, contractor_id: int) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT 1 FROM runsheets r
                LEFT JOIN runsheet_assignments ra
                    ON ra.runsheet_id = r.id AND ra.user_id = %s
                WHERE r.id = %s AND (r.lead_user_id = %s OR ra.id IS NOT NULL)
                LIMIT 1
                """,
                (contractor_id, runsheet_id, contractor_id),
            )
            return cur.fetchone() is not None
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def contractor_may_edit_runsheet_header(runsheet_id: int, contractor_id: int) -> bool:
        """Lead may edit/publish; if lead_user_id is NULL, any participant may (legacy / Ventus)."""
        rs = RunsheetService.get_runsheet(runsheet_id)
        if not rs or not RunsheetService.contractor_can_access_runsheet(
            runsheet_id, contractor_id
        ):
            return False
        lead = rs.get("lead_user_id")
        if lead is None:
            return True
        return int(lead) == int(contractor_id)

    @staticmethod
    def withdraw_runsheet_assignment(
        runsheet_id: int, assignment_id: int, contractor_id: int
    ) -> Dict[str, Any]:
        """
        Contractor removes their assignment from payroll for this runsheet (others unchanged).
        Deletes matching tb_timesheet_entries row if present.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT ra.id, ra.user_id, r.work_date, r.job_type_id
                FROM runsheet_assignments ra
                JOIN runsheets r ON r.id = ra.runsheet_id
                WHERE ra.id = %s AND ra.runsheet_id = %s
                """,
                (assignment_id, runsheet_id),
            )
            row = cur.fetchone()
            if not row:
                return {"ok": False, "message": "Assignment not found."}
            if int(row["user_id"]) != int(contractor_id):
                return {"ok": False, "message": "You can only withdraw your own assignment."}
            cur.execute(
                """
                UPDATE runsheet_assignments
                SET payroll_included = 0,
                    withdrawn_at = NOW(),
                    withdrawn_by_user_id = %s
                WHERE id = %s
                """,
                (contractor_id, assignment_id),
            )
            week_id_str = RunsheetService._week_id_for_date(row["work_date"])
            wk = TimesheetService._ensure_week(contractor_id, week_id_str)
            RunsheetService._purge_timesheet_row_for_runsheet_user(
                cur,
                contractor_id,
                wk["id"],
                row["work_date"],
                int(row["job_type_id"]),
                runsheet_id,
            )
            conn.commit()
            return {"ok": True}
        except Exception as e:
            conn.rollback()
            return {"ok": False, "message": str(e)}
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _sync_timesheet_row_from_assignment(runsheet_id: int, user_id: int) -> None:
        """If runsheet is published, push assignment actuals into tb_timesheet_entries and recompute pay."""
        rs = RunsheetService.get_runsheet(runsheet_id)
        if not rs or str(rs.get("status") or "").lower() != "published":
            return
        a = next(
            (
                x
                for x in (rs.get("assignments") or [])
                if int(x["user_id"]) == int(user_id)
            ),
            None,
        )
        if not a:
            return
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            week_id_str = RunsheetService._week_id_for_date(rs["work_date"])
            wk = TimesheetService._ensure_week(user_id, week_id_str)
            cur.execute(
                """
                UPDATE tb_timesheet_entries
                SET actual_start = COALESCE(%s, actual_start),
                    actual_end = COALESCE(%s, actual_end),
                    notes = COALESCE(%s, notes)
                WHERE week_id = %s AND user_id = %s AND work_date = %s
                  AND source = 'runsheet' AND runsheet_id = %s
                """,
                (
                    a.get("actual_start"),
                    a.get("actual_end"),
                    a.get("notes"),
                    wk["id"],
                    user_id,
                    rs["work_date"],
                    runsheet_id,
                ),
            )
            TimesheetService.refresh_entries_actuals(
                cur, conn, wk["id"], user_id, rs["work_date"], runsheet_id
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_own_assignment_times(
        runsheet_id: int, user_id: int, data: Dict[str, Any]
    ) -> None:
        """Update actual_start, actual_end, notes for the user's assignment only."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        synced = False
        try:
            cur.execute(
                "SELECT id FROM runsheet_assignments WHERE runsheet_id=%s AND user_id=%s",
                (runsheet_id, user_id),
            )
            row = cur.fetchone()
            if not row:
                raise Exception("No assignment for you on this run sheet.")
            ra_id = int(row["id"])
            fields, vals = [], []
            for k in ("actual_start", "actual_end", "notes"):
                if k in data:
                    fields.append(f"{k}=%s")
                    vals.append(data[k])
            if not fields:
                return
            vals.append(ra_id)
            cur.execute(
                f"UPDATE runsheet_assignments SET {', '.join(fields)} WHERE id=%s",
                vals,
            )
            conn.commit()
            synced = True
        finally:
            cur.close()
            conn.close()
        if synced:
            RunsheetService._sync_timesheet_row_from_assignment(runsheet_id, user_id)

    @staticmethod
    def reactivate_runsheet_assignment(
        runsheet_id: int, assignment_id: int, contractor_id: int
    ) -> Dict[str, Any]:
        """Contractor opts back into payroll for this assignment; publish runsheet to refresh timesheet."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT id, user_id FROM runsheet_assignments
                WHERE id = %s AND runsheet_id = %s
                """,
                (assignment_id, runsheet_id),
            )
            row = cur.fetchone()
            if not row:
                return {"ok": False, "message": "Assignment not found."}
            if int(row["user_id"]) != int(contractor_id):
                return {"ok": False, "message": "You can only reactivate your own assignment."}
            cur.execute(
                """
                UPDATE runsheet_assignments
                SET payroll_included = 1,
                    withdrawn_at = NULL,
                    withdrawn_by_user_id = NULL,
                    reactivated_at = NOW()
                WHERE id = %s
                """,
                (assignment_id,),
            )
            conn.commit()
            return {
                "ok": True,
                "message": "Publish this runsheet (or ask an admin to resync) to update your timesheet.",
            }
        except Exception as e:
            conn.rollback()
            return {"ok": False, "message": str(e)}
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _week_id_for_date(d: date) -> str:
        """
        Converts a date into ISO week string: YYYYWW
        """
        iso_year, iso_week, _ = d.isocalendar()
        return f"{iso_year}{iso_week:02d}"

    @staticmethod
    def create_and_publish_runsheet_for_shift(shift_id: int) -> Optional[Dict[str, Any]]:
        """
        For a scheduler-only shift (no runsheet yet), create a runsheet + one assignment,
        publish it to create timesheet entry and pay, then link the shift to the runsheet.
        Returns {"runsheet_id": _, "runsheet_assignment_id": _} or None on failure.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT id, contractor_id, client_id, site_id, job_type_id, work_date,
                       scheduled_start, scheduled_end, actual_start, actual_end, notes
                FROM schedule_shifts WHERE id = %s
            """, (shift_id,))
            shift = cur.fetchone()
            if not shift or shift.get("runsheet_id"):
                return None
            cid = shift["client_id"]
            sid = shift.get("site_id")
            jid = shift["job_type_id"]
            wd = shift["work_date"]
            ss = shift.get("scheduled_start")
            se = shift.get("scheduled_end")
            uid = shift["contractor_id"]
            cur.execute("""
                INSERT INTO runsheets (client_id, site_id, job_type_id, work_date, window_start, window_end, status, notes)
                VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s)
            """, (cid, sid, jid, wd, ss, se, "From Work/Scheduling"))
            rs_id = cur.lastrowid
            cur.execute("""
                INSERT INTO runsheet_assignments (runsheet_id, user_id, scheduled_start, scheduled_end, actual_start, actual_end, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (rs_id, uid, ss, se, shift.get("actual_start"), shift.get("actual_end"), shift.get("notes")))
            ra_id = cur.lastrowid
            conn.commit()
        finally:
            cur.close()
            conn.close()

        result = RunsheetService.publish_runsheet(rs_id, None)
        if not result.get("ok"):
            return None
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE schedule_shifts SET runsheet_id = %s, runsheet_assignment_id = %s WHERE id = %s",
                (rs_id, ra_id, shift_id),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        # Drop duplicate scheduler prefill row (same shift keyed by runsheet_id=shift_id)
        TimesheetService.delete_scheduler_prefill_for_shift(shift_id, uid, wd)
        return {"runsheet_id": rs_id, "runsheet_assignment_id": ra_id}


class TemplateService:
    # ---- Job Types CRUD ----
    @staticmethod
    def list_job_types() -> list[dict]:
        """List all job types."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, name, code, active, colour_hex FROM job_types ORDER BY name")
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_job_type(data: dict) -> int:
        """Create a new job type."""
        if not data.get("name"):
            raise Exception("name required")
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO job_types (name, code, active, colour_hex) VALUES (%s,%s,%s,%s)",
                (data["name"], data.get("code"), int(data.get("active", 1)), data.get("colour_hex") or None)
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_job_type(data: dict) -> None:
        """Update an existing job type."""
        if not data.get("id"):
            raise Exception("id required")
        fields, params = [], {}
        for k in ("name", "code", "active", "colour_hex"):
            if k in data:
                fields.append(f"{k}=%({k})s")
                params[k] = data[k]
        if not fields:
            return
        params["id"] = data["id"]

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                f"UPDATE job_types SET {', '.join(fields)} WHERE id=%(id)s", params)
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_job_type(job_type_id: int) -> None:
        """
        Delete a job type. Removes all wage and bill rate rows for this job type (DB CASCADE).
        Contractor overrides, policies, and runsheet templates that reference it will have
        job_type_id set to NULL (DB SET NULL). Fails if the job type is used in any
        timesheet entries or runsheets (RESTRICT); use the returned error message to tell the user.
        """
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COUNT(*) AS c FROM tb_timesheet_entries WHERE job_type_id=%s",
                (job_type_id,),
            )
            row = cur.fetchone()
            entries_count = (row[0] if row else 0) or 0
            cur.execute(
                "SELECT COUNT(*) AS c FROM runsheets WHERE job_type_id=%s",
                (job_type_id,),
            )
            row2 = cur.fetchone()
            runsheets_count = (row2[0] if row2 else 0) or 0
            if entries_count or runsheets_count:
                parts = []
                if entries_count:
                    parts.append(f"{entries_count} timesheet entries")
                if runsheets_count:
                    parts.append(f"{runsheets_count} runsheets")
                raise Exception(
                    f"Cannot delete: this job type is used by {', '.join(parts)}. "
                    "Reassign or remove those first."
                )
            cur.execute("DELETE FROM job_types WHERE id=%s", (job_type_id,))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    # ---- Roles CRUD ----
    @staticmethod
    def list_roles() -> list[dict]:
        """List all roles."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, name, code, active FROM roles ORDER BY name")
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_role(data: dict) -> int:
        """Create a new role."""
        if not data.get("name"):
            raise Exception("name required")
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO roles (name, code, active) VALUES (%s,%s,%s)",
                (data["name"], data.get("code"), int(data.get("active", 1)))
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_role(data: dict) -> None:
        """Update an existing role."""
        if not data.get("id"):
            raise Exception("id required")
        fields, params = [], {}
        for k in ("name", "code", "active"):
            if k in data:
                fields.append(f"{k}=%({k})s")
                params[k] = data[k]
        if not fields:
            return
        params["id"] = data["id"]

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                f"UPDATE roles SET {', '.join(fields)} WHERE id=%(id)s", params)
            conn.commit()
        finally:
            cur.close()
            conn.close()

    # ---- Wage Rate Cards CRUD ----
    @staticmethod
    def list_wage_cards() -> list[dict]:
        """List all wage rate cards with role names."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT wrc.id, wrc.name, wrc.active, wrc.role_id, r.name AS role_name
                FROM wage_rate_cards wrc
                LEFT JOIN roles r ON r.id=wrc.role_id
                ORDER BY wrc.id
            """)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_wage_card(data: dict) -> int:
        """Create a new wage rate card."""
        if not data.get("name"):
            raise Exception("name required")
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO wage_rate_cards (name, role_id, active) VALUES (%s,%s,%s)",
                (data["name"], data.get("role_id"), int(data.get("active", 1)))
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    # ---- Wage Rate Rows ----
    @staticmethod
    def list_wage_rows(card_id: int) -> list[dict]:
        """List all wage rate rows for a given wage card."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT wrr.id, wrr.job_type_id, jt.name AS job_type_name,
                       wrr.rate, wrr.effective_from, wrr.effective_to
                FROM wage_rate_rows wrr
                JOIN job_types jt ON jt.id=wrr.job_type_id
                WHERE wrr.rate_card_id=%s
                ORDER BY jt.name, wrr.effective_from DESC
            """, (card_id,))
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_wage_row(card_id: int, data: dict) -> int:
        """Add a new wage row to a wage card."""
        required = ["job_type_id", "rate", "effective_from"]
        missing = [k for k in required if not data.get(k)]
        if missing:
            raise Exception(f"Missing: {', '.join(missing)}")

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO wage_rate_rows
                (rate_card_id, job_type_id, rate, effective_from, effective_to)
                VALUES (%s,%s,%s,%s,%s)
            """, (card_id, data["job_type_id"], data["rate"], data["effective_from"], data.get("effective_to")))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_wage_row(card_id: int, row_id: int, data: dict) -> None:
        """Update a wage rate row; must belong to card_id."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id FROM wage_rate_rows WHERE id=%s AND rate_card_id=%s",
                (row_id, card_id),
            )
            if not cur.fetchone():
                raise Exception("Row not found")
            fields, params = [], {}
            for k in ("job_type_id", "rate", "effective_from", "effective_to"):
                if k in data:
                    fields.append(f"{k}=%({k})s")
                    params[k] = data[k]
            if not fields:
                return
            params["id"] = row_id
            cur.execute(
                f"UPDATE wage_rate_rows SET {', '.join(fields)} WHERE id=%(id)s",
                params,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_wage_row(card_id: int, row_id: int) -> None:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM wage_rate_rows WHERE id=%s AND rate_card_id=%s",
                (row_id, card_id),
            )
            if cur.rowcount == 0:
                raise Exception("Row not found")
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_wage_card(card_id: int, data: dict) -> None:
        if not data:
            return
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM wage_rate_cards WHERE id=%s", (card_id,))
            if not cur.fetchone():
                raise Exception("Card not found")
            fields, params = [], {}
            for k in ("name", "role_id", "active"):
                if k in data:
                    fields.append(f"{k}=%({k})s")
                    v = data[k]
                    if k == "active" and isinstance(v, bool):
                        v = 1 if v else 0
                    params[k] = v
            if not fields:
                return
            params["id"] = card_id
            cur.execute(
                f"UPDATE wage_rate_cards SET {', '.join(fields)} WHERE id=%(id)s",
                params,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_wage_card(card_id: int) -> None:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM wage_rate_cards WHERE id=%s", (card_id,))
            if not cur.fetchone():
                raise Exception("Card not found")
            cur.execute(
                "UPDATE tb_contractors SET wage_rate_card_id=NULL WHERE wage_rate_card_id=%s",
                (card_id,),
            )
            cur.execute("DELETE FROM wage_rate_cards WHERE id=%s", (card_id,))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    # ---- Bill Rate Cards & Rows ----
    @staticmethod
    def list_bill_cards() -> list[dict]:
        """List all bill rate cards with associated client and site names."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT brc.id, brc.name, brc.active,
                       brc.client_name AS client_ref, brc.site_name AS site_ref,
                       c.name AS client_label, s.name AS site_label
                FROM bill_rate_cards brc
                LEFT JOIN clients c ON c.id=brc.client_name
                LEFT JOIN sites s ON s.id=brc.site_name
                ORDER BY brc.id
            """)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_bill_card(data: dict) -> int:
        """Create a new bill rate card."""
        if not data.get("name"):
            raise Exception("name required")

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO bill_rate_cards (name, client_name, site_name, active) VALUES (%s,%s,%s,%s)",
                (data["name"], data.get("client_name"), data.get(
                    "site_name"), int(data.get("active", 1)))
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_bill_rows(card_id: int) -> list[dict]:
        """List all bill rate rows for a given bill card."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT brr.id, brr.job_type_id, jt.name AS job_type_name,
                       brr.rate, brr.effective_from, brr.effective_to
                FROM bill_rate_rows brr
                JOIN job_types jt ON jt.id=brr.job_type_id
                WHERE brr.rate_card_id=%s
                ORDER BY jt.name, brr.effective_from DESC
            """, (card_id,))
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_bill_row(card_id: int, data: dict) -> int:
        """Add a new bill row to a bill card."""
        required = ["job_type_id", "rate", "effective_from"]
        missing = [k for k in required if not data.get(k)]
        if missing:
            raise Exception(f"Missing: {', '.join(missing)}")

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO bill_rate_rows
                (rate_card_id, job_type_id, rate, effective_from, effective_to)
                VALUES (%s,%s,%s,%s,%s)
            """, (card_id, data["job_type_id"], data["rate"], data["effective_from"], data.get("effective_to")))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_bill_row(card_id: int, row_id: int, data: dict) -> None:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id FROM bill_rate_rows WHERE id=%s AND rate_card_id=%s",
                (row_id, card_id),
            )
            if not cur.fetchone():
                raise Exception("Row not found")
            fields, params = [], {}
            for k in ("job_type_id", "rate", "effective_from", "effective_to"):
                if k in data:
                    fields.append(f"{k}=%({k})s")
                    params[k] = data[k]
            if not fields:
                return
            params["id"] = row_id
            cur.execute(
                f"UPDATE bill_rate_rows SET {', '.join(fields)} WHERE id=%(id)s",
                params,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_bill_row(card_id: int, row_id: int) -> None:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM bill_rate_rows WHERE id=%s AND rate_card_id=%s",
                (row_id, card_id),
            )
            if cur.rowcount == 0:
                raise Exception("Row not found")
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_bill_card(card_id: int, data: dict) -> None:
        if not data:
            return
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM bill_rate_cards WHERE id=%s", (card_id,))
            if not cur.fetchone():
                raise Exception("Card not found")
            fields, params = [], {}
            for k in ("name", "client_name", "site_name", "active"):
                if k in data:
                    fields.append(f"{k}=%({k})s")
                    v = data[k]
                    if k == "active" and isinstance(v, bool):
                        v = 1 if v else 0
                    params[k] = v
            if not fields:
                return
            params["id"] = card_id
            cur.execute(
                f"UPDATE bill_rate_cards SET {', '.join(fields)} WHERE id=%(id)s",
                params,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_bill_card(card_id: int) -> None:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM bill_rate_cards WHERE id=%s", (card_id,))
            if not cur.fetchone():
                raise Exception("Card not found")
            cur.execute("DELETE FROM bill_rate_cards WHERE id=%s", (card_id,))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    # ---- Policies ----
    @staticmethod
    def list_policies() -> list[dict]:
        """List all calendar policies."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT id, name, type, scope, role_id, job_type_id, client_name, contractor_id,
                       mode, multiplier, absolute_rate, window_start, window_end,
                       ot_threshold_hours, ot_tier2_threshold_hours, ot_tier1_mult, ot_tier2_mult,
                       applies_to, stacking, effective_from, effective_to, active
                FROM calendar_policies
                ORDER BY effective_from DESC, id DESC
            """)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_policy(data: dict) -> int:
        """Create a new calendar policy."""
        required = ["name", "type", "scope", "mode", "effective_from"]
        missing = [k for k in required if not data.get(k)]
        if missing:
            raise Exception(f"Missing: {', '.join(missing)}")

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO calendar_policies
                (name, type, scope, role_id, job_type_id, client_name, contractor_id,
                 mode, multiplier, absolute_rate, window_start, window_end,
                 ot_threshold_hours, ot_tier2_threshold_hours, ot_tier1_mult, ot_tier2_mult,
                 applies_to, stacking, effective_from, effective_to, active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                data["name"], data["type"], data["scope"], data.get(
                    "role_id"), data.get("job_type_id"),
                data.get("client_name"), data.get(
                    "contractor_id"), data["mode"], data.get("multiplier"),
                data.get("absolute_rate"), data.get(
                    "window_start"), data.get("window_end"),
                data.get("ot_threshold_hours"), data.get(
                    "ot_tier2_threshold_hours"),
                data.get("ot_tier1_mult"), data.get(
                    "ot_tier2_mult"), data.get("applies_to", "WAGE"),
                data.get("stacking", "OT_ON_TOP"), data["effective_from"], data.get(
                    "effective_to"),
                int(data.get("active", 1))
            ))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    # ---- Runsheet Templates ----
    @staticmethod
    def list_runsheet_templates() -> list[dict]:
        """List all runsheet templates with related job type, client, and site names."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT t.id, t.name, t.code, t.version, t.active,
                       t.allow_free_text_client_site,
                       t.job_type_id, jt.name AS job_type_name,
                       t.client_id, c.name AS client_label,
                       t.site_id, s.name AS site_label
                FROM runsheet_templates t
                LEFT JOIN job_types jt ON jt.id=t.job_type_id
                LEFT JOIN clients c ON c.id=t.client_id
                LEFT JOIN sites s ON s.id=t.site_id
                ORDER BY t.id DESC
            """)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_active_runsheet_templates() -> list[dict]:
        """Templates available to contractors for new emergency runsheets (active only)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT t.id, t.name, t.code, t.version, t.job_type_id,
                       jt.name AS job_type_name,
                       t.allow_free_text_client_site,
                       t.client_id, c.name AS client_label,
                       t.site_id, s.name AS site_label
                FROM runsheet_templates t
                LEFT JOIN job_types jt ON jt.id = t.job_type_id
                LEFT JOIN clients c ON c.id = t.client_id
                LEFT JOIN sites s ON s.id = t.site_id
                WHERE t.active IN (1, '1', TRUE)
                ORDER BY t.name ASC, t.id DESC
                """
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_runsheet_template(data: dict) -> int:
        """Create a new runsheet template."""
        if not data.get("name"):
            raise Exception("name required")

        cid = data.get("client_id")
        sid = data.get("site_id")
        if cid is None and data.get("client_name") is not None:
            try:
                cid = int(data["client_name"])
            except (TypeError, ValueError):
                cid = None
        if sid is None and data.get("site_name") is not None:
            try:
                sid = int(data["site_name"])
            except (TypeError, ValueError):
                sid = None
        jtid = data.get("job_type_id")
        if jtid is not None:
            try:
                jtid = int(jtid)
            except (TypeError, ValueError):
                jtid = None

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO runsheet_templates
                (name, code, job_type_id, client_id, site_id, active, version,
                 allow_free_text_client_site)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                data["name"], data.get("code"), jtid,
                cid, sid,
                int(data.get("active", 1)), int(data.get("version", 1)),
                int(data.get("allow_free_text_client_site", 0) or 0),
            ))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_runsheet_template(tpl_id: int) -> dict:
        """Get a runsheet template and its fields."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM runsheet_templates WHERE id=%s", (tpl_id,))
            tpl = cur.fetchone()
            if not tpl:
                raise Exception("template not found")

            cur.execute("""
                SELECT id, name, label, type, required, order_index,
                       placeholder, help_text, options_json, validation_json, visible_if_json
                FROM runsheet_template_fields
                WHERE template_id=%s
                ORDER BY order_index ASC, id ASC
            """, (tpl_id,))
            tpl["fields"] = cur.fetchall() or []
            return tpl
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_runsheet_template(tpl_id: int, data: dict) -> None:
        """Update a runsheet template."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            norm = dict(data)
            if "client_id" not in norm and "client_name" in norm:
                try:
                    norm["client_id"] = int(norm["client_name"]) if norm["client_name"] not in (None, "") else None
                except (TypeError, ValueError):
                    norm["client_id"] = None
            if "site_id" not in norm and "site_name" in norm:
                try:
                    norm["site_id"] = int(norm["site_name"]) if norm["site_name"] not in (None, "") else None
                except (TypeError, ValueError):
                    norm["site_id"] = None
            fields, params = [], {"id": tpl_id}
            for k in ("name", "code", "job_type_id", "client_id", "site_id", "active", "version",
                      "allow_free_text_client_site"):
                if k in norm:
                    fields.append(f"{k}=%({k})s")
                    if k == "allow_free_text_client_site":
                        v = norm[k]
                        params[k] = 1 if v in (True, 1, "1", "true", "True") else 0
                    else:
                        params[k] = norm[k]
            if fields:
                cur.execute(
                    f"UPDATE runsheet_templates SET {', '.join(fields)} WHERE id=%(id)s", params
                )
                conn.commit()
        finally:
            cur.close()
            conn.close()

    # ---- Template Fields ----
    @staticmethod
    def list_template_fields(tpl_id: int) -> list[dict]:
        """List all fields for a given runsheet template."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT id, name, label, type, required, order_index,
                       placeholder, help_text, options_json, validation_json, visible_if_json
                FROM runsheet_template_fields
                WHERE template_id=%s
                ORDER BY order_index ASC, id ASC
            """, (tpl_id,))
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_template_field(tpl_id: int, field: dict) -> int:
        """Add a field to a runsheet template."""
        field = dict(field or {})
        if not field.get("name"):
            import re
            base = (field.get("label") or "field").strip().lower()
            slug = re.sub(r"[^a-z0-9_]+", "_", base).strip("_")[:80]
            field["name"] = slug or f"field_{int(__import__('time').time())}"
        if not field.get("label"):
            field["label"] = field["name"].replace("_", " ").title()
        if not field.get("type"):
            field["type"] = "text"
        required = ["name", "label", "type"]
        missing = [k for k in required if not field.get(k)]
        if missing:
            raise Exception(f"Missing: {', '.join(missing)}")

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO runsheet_template_fields
                (template_id, name, label, type, required, order_index,
                 placeholder, help_text, options_json, validation_json, visible_if_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                tpl_id, field["name"], field["label"], field["type"],
                int(field.get("required", 0)),
                int(field.get("order_index", 0)),
                field.get("placeholder"), field.get("help_text"),
                json.dumps(field.get("options_json")) if isinstance(
                    field.get("options_json"), (dict, list)) else field.get("options_json"),
                json.dumps(field.get("validation_json")) if isinstance(
                    field.get("validation_json"), (dict, list)) else field.get("validation_json"),
                json.dumps(field.get("visible_if_json")) if isinstance(
                    field.get("visible_if_json"), (dict, list)) else field.get("visible_if_json")
            ))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_template_field(tpl_id: int, field_id: int, data: dict) -> None:
        """Update a specific field of a runsheet template."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            fields, params = [], {"id": field_id, "template_id": tpl_id}
            for k in ("name", "label", "type", "required", "order_index",
                      "placeholder", "help_text", "options_json", "validation_json", "visible_if_json"):
                if k in data:
                    val = data[k]
                    if k.endswith("_json") and isinstance(val, (dict, list)):
                        val = json.dumps(val)
                    fields.append(f"{k}=%({k})s")
                    params[k] = val
            if fields:
                cur.execute(f"""
                    UPDATE runsheet_template_fields
                    SET {', '.join(fields)}
                    WHERE id=%(id)s AND template_id=%(template_id)s
                """, params)
                conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_template_field(tpl_id: int, field_id: int) -> None:
        """Delete a field from a runsheet template."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM runsheet_template_fields WHERE id=%s AND template_id=%s",
                (field_id, tpl_id)
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    # ---- Render and Validation ----
    @staticmethod
    def _select_choice_values(options_raw: Any) -> set:
        """Valid stored values for select/multiselect (choices[].value or choices[].id)."""
        opts = options_raw
        if isinstance(opts, str):
            try:
                opts = json.loads(opts)
            except Exception:
                return set()
        out: set = set()
        if isinstance(opts, dict) and isinstance(opts.get("choices"), list):
            for o in opts["choices"]:
                if isinstance(o, dict):
                    if o.get("value") is not None and str(o.get("value")).strip() != "":
                        out.add(str(o["value"]))
                    if o.get("id") is not None and str(o.get("id")).strip() != "":
                        out.add(str(o["id"]))
                    if o.get("label") is not None and str(o["label"]).strip() != "":
                        if not any(
                            o.get(k) not in (None, "")
                            for k in ("value", "id")
                        ):
                            out.add(str(o["label"]))
                elif o is not None:
                    out.add(str(o))
        elif isinstance(opts, list):
            for o in opts:
                if isinstance(o, dict):
                    if o.get("value") is not None and str(o.get("value")).strip() != "":
                        out.add(str(o["value"]))
                    if o.get("id") is not None and str(o.get("id")).strip() != "":
                        out.add(str(o["id"]))
                elif o is not None:
                    out.add(str(o))
        return out

    @staticmethod
    def render_form_schema(tpl_id: int) -> dict:
        """Render runsheet template as a form schema."""
        tpl = TemplateService.get_runsheet_template(tpl_id)
        schema = {
            "template": {
                "id": tpl["id"],
                "name": tpl["name"],
                "code": tpl.get("code"),
                "version": tpl.get("version"),
                "allow_free_text_client_site": bool(
                    tpl.get("allow_free_text_client_site") in (1, "1", True)
                ),
            },
            "fields": []
        }
        for f in tpl.get("fields") or []:
            schema["fields"].append({
                "name": f["name"],
                "label": f["label"],
                "type": f["type"],
                "required": bool(f["required"]),
                "order_index": f.get("order_index", 0),
                "placeholder": f.get("placeholder"),
                "help_text": f.get("help_text"),
                "options": json.loads(f["options_json"]) if f.get("options_json") and isinstance(f.get("options_json"), str) else f.get("options_json"),
                "validation": json.loads(f["validation_json"]) if f.get("validation_json") and isinstance(f.get("validation_json"), str) else f.get("validation_json"),
                "visible_if": json.loads(f["visible_if_json"]) if f.get("visible_if_json") and isinstance(f.get("visible_if_json"), str) else f.get("visible_if_json"),
            })
        schema["fields"].sort(key=lambda x: (
            x.get("order_index") or 0, x["name"]))
        return schema

    @staticmethod
    def validate_runsheet_payload(template: Dict[str, Any], payload: Dict[str, Any]) -> None:
        """
        Validate a payload against a runsheet template.

        :param template: The runsheet template dictionary, including fields.
        :param payload: The submitted form payload to validate.
        :raises Exception: If any validation rule is violated.
        """
        fields = template.get("fields") or []

        # ---- Required fields ----
        for f in fields:
            if f.get("required"):
                if f["name"] not in payload or payload.get(f["name"]) in (None, "", []):
                    raise Exception(f"Field '{f['label']}' is required.")

        # ---- Select / multiselect (choices use id or value in options_json) ----
        for f in fields:
            name = f["name"]
            typ = (f.get("type") or "").lower()
            if name not in payload:
                continue
            val = payload.get(name)
            if typ in ("select", "dropdown"):
                if val in (None, ""):
                    continue
                valid_v = TemplateService._select_choice_values(f.get("options_json"))
                if valid_v and str(val) not in valid_v:
                    raise Exception(
                        f"Field '{f['label']}' has an invalid choice.")
            elif typ == "multiselect":
                if val in (None, "", []):
                    continue
                if not isinstance(val, list):
                    raise Exception(
                        f"Field '{f['label']}' must be a list of choices.")
                valid_v = TemplateService._select_choice_values(f.get("options_json"))
                if valid_v:
                    for item in val:
                        if str(item) not in valid_v:
                            raise Exception(
                                f"Field '{f['label']}' has an invalid choice.")

        # ---- Basic formats for date / time / datetime (always, not only when validation_json set) ----
        for f in fields:
            name = f["name"]
            typ = (f.get("type") or "").lower()
            if name not in payload:
                continue
            val = payload.get(name)
            if val in (None, "", []):
                continue
            if typ == "date":
                try:
                    datetime.strptime(str(val), "%Y-%m-%d")
                except Exception:
                    raise Exception(
                        f"Field '{f['label']}' must be a date in YYYY-MM-DD format.")
            elif typ == "time":
                try:
                    _ = _to_time(str(val))
                except Exception:
                    raise Exception(
                        f"Field '{f['label']}' must be a time in HH:MM or HH:MM:SS format.")
            elif typ == "datetime":
                s = str(val).strip()
                ok = False
                for parser in (
                    lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")),
                    lambda x: datetime.strptime(x[:19], "%Y-%m-%dT%H:%M:%S"),
                    lambda x: datetime.strptime(x[:19], "%Y-%m-%d %H:%M:%S"),
                ):
                    try:
                        parser(s)
                        ok = True
                        break
                    except Exception:
                        continue
                if not ok:
                    raise Exception(
                        f"Field '{f['label']}' must be a valid date-time.")

        # ---- Regex, min/max, type validations from validation_json ----
        for f in fields:
            rules = f.get("validation_json")
            if isinstance(rules, str):
                try:
                    rules = json.loads(rules)
                except Exception:
                    rules = None
            if not rules:
                continue

            name = f["name"]
            if name not in payload:
                continue
            val = payload.get(name)

            typ = (f.get("type") or "").lower()

            # --- Number/Integer validations ---
            if typ in ("number", "decimal", "float", "integer", "int"):
                try:
                    num = float(val)
                except Exception:
                    raise Exception(f"Field '{f['label']}' must be a number.")
                if typ in ("integer", "int") and not float(num).is_integer():
                    raise Exception(
                        f"Field '{f['label']}' must be an integer.")

                if "min" in rules and num < float(rules["min"]):
                    raise Exception(
                        f"Field '{f['label']}' must be >= {rules['min']}.")
                if "max" in rules and num > float(rules["max"]):
                    raise Exception(
                        f"Field '{f['label']}' must be <= {rules['max']}.")

                if "step" in rules:
                    step = float(rules["step"])
                    if step > 0:
                        base = float(rules.get("min", 0))
                        remainder = (num - base) % step
                        if remainder > 1e-9 and step - remainder > 1e-9:
                            raise Exception(
                                f"Field '{f['label']}' must be in steps of {rules['step']}.")

            # --- String/Text validations ---
            elif typ in ("text", "string"):
                s = "" if val is None else str(val)
                if "min_length" in rules and len(s) < int(rules["min_length"]):
                    raise Exception(
                        f"Field '{f['label']}' must be at least {rules['min_length']} characters.")
                if "max_length" in rules and len(s) > int(rules["max_length"]):
                    raise Exception(
                        f"Field '{f['label']}' must be at most {rules['max_length']} characters.")
                if "regex" in rules and rules["regex"]:
                    pattern = rules["regex"]
                    flags = 0
                    if rules.get("regex_flags"):
                        if "i" in rules["regex_flags"]:
                            flags |= re.IGNORECASE
                        if "m" in rules["regex_flags"]:
                            flags |= re.MULTILINE
                    if not re.fullmatch(pattern, s, flags):
                        raise Exception(
                            f"Field '{f['label']}' is not in the correct format.")

            # --- Boolean/Checkbox validation ---
            elif typ in ("checkbox", "boolean", "bool"):
                if not isinstance(val, (bool, int)):
                    s = str(val).lower()
                    if s not in ("true", "false", "1", "0", "yes", "no"):
                        raise Exception(
                            f"Field '{f['label']}' must be true/false.")

        # ---- Conditional visibility ----
        for f in fields:
            vis = f.get("visible_if_json")
            if isinstance(vis, str):
                try:
                    vis = json.loads(vis)
                except Exception:
                    vis = None
            if not vis:
                continue

            def eval_clause(cl):
                field = cl.get("field")
                op = (cl.get("op") or "=").lower()
                val = cl.get("value")
                cur = payload.get(field)
                try:
                    if op in ("=", "eq"):
                        return cur == val
                    if op in ("!=", "ne"):
                        return cur != val
                    if op in (">", "gt"):
                        return float(cur) > float(val)
                    if op in (">=", "ge"):
                        return float(cur) >= float(val)
                    if op in ("<", "lt"):
                        return float(cur) < float(val)
                    if op in ("<=", "le"):
                        return float(cur) <= float(val)
                    if op == "in":
                        return cur in (val if isinstance(val, list) else [val])
                    if op in ("not_in", "nin"):
                        return cur not in (val if isinstance(val, list) else [val])
                except Exception:
                    return False
                return False

            visible = True
            if isinstance(vis, dict):
                if "all" in vis and isinstance(vis["all"], list):
                    visible = all(eval_clause(c) for c in vis["all"])
                elif "any" in vis and isinstance(vis["any"], list):
                    visible = any(eval_clause(c) for c in vis["any"])

            # If not visible, skip required/type validations for leniency
            if not visible and f.get("required"):
                if f["name"] in payload and payload.get(f["name"]) in (None, "", []):
                    pass

    @staticmethod
    def get_mapping_for_template(tpl_id: int) -> Optional[Dict[str, Any]]:
        """
        Template-level field→timesheet mapping (optional).
        Core schema stores mapping_json on `runsheet` rows, not on `runsheet_templates`.
        If you add a mapping_json column to runsheet_templates later, read it here.
        """
        return None


class ExportService:

    @staticmethod
    def render_sheet_html(user_id: int, week_id: str) -> str:
        """
        Render a timesheet week as an HTML table.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            # Resolve week PK
            cur.execute(
                "SELECT id, week_ending, status FROM tb_timesheet_weeks WHERE user_id=%s AND week_id=%s",
                (user_id, week_id),
            )
            wk = cur.fetchone()
            if not wk:
                return "<p>No timesheet found.</p>"

            # Fetch entries with COALESCE for names
            cur.execute(
                """ 
                SELECT e.*,
                    jt.name AS job_type_name,
                    COALESCE(e.client_name, c.name) AS client_name,
                    COALESCE(e.site_name, s.name)   AS site_name
                FROM tb_timesheet_entries e 
                JOIN job_types jt ON jt.id = e.job_type_id 
                LEFT JOIN clients c ON c.id = e.client_name 
                LEFT JOIN sites s ON s.id = e.site_name 
                WHERE e.week_id=%s AND e.user_id=%s 
                ORDER BY e.work_date ASC, e.actual_start ASC 
                """,
                (wk["id"], user_id),
            )
            rows = cur.fetchall() or []

            # Compute totals
            totals = {
                "hours": sum((r.get("actual_hours") or 0) for r in rows),
                "pay": sum((r.get("pay") or 0) for r in rows),
                "travel": sum((r.get("travel_parking") or 0) for r in rows),
                "lateness_mins": sum((r.get("lateness_mins") or 0) for r in rows),
                "overrun_mins": sum((r.get("overrun_mins") or 0) for r in rows),
            }

            # Render HTML
            html = [
                "<html><head><meta charset='utf-8'><title>Timesheet</title>",
                "<style>"
                "body{font-family:Arial} "
                "table{width:100%;border-collapse:collapse} "
                "th,td{border:1px solid #ddd;padding:6px;font-size:12px} "
                "th{background:#f5f5f5;text-align:left}"
                "</style></head><body>",
                f"<h2>Timesheet – Week {week_id} (Status: {wk['status']})</h2>",
                f"<p>Week Ending: {wk['week_ending'].strftime('%d/%m/%Y')}</p>",
                "<table><thead><tr>"
                "<th>Date</th><th>Client</th><th>Site</th><th>Job Type</th>"
                "<th>Sched</th><th>Actual</th><th>Break</th><th>Hours</th>"
                "<th>Rate</th><th>Pay</th><th>Travel</th><th>Notes</th>"
                "</tr></thead><tbody>",
            ]

            for r in rows:
                html.append("<tr>")
                html.append(f"<td>{r['work_date'].strftime('%d/%m/%Y')}</td>")
                html.append(f"<td>{r.get('client_name') or ''}</td>")
                html.append(f"<td>{r.get('site_name') or ''}</td>")
                html.append(f"<td>{r.get('job_type_name') or ''}</td>")
                html.append(
                    f"<td>{str(r['scheduled_start'])[:5]}–{str(r['scheduled_end'])[:5]}</td>"
                )
                html.append(
                    f"<td>{str(r['actual_start'])[:5]}–{str(r['actual_end'])[:5]}</td>"
                )
                html.append(f"<td>{int(r.get('break_mins') or 0)}</td>")
                html.append(
                    f"<td>{float(r.get('actual_hours') or 0):.2f}</td>")
                html.append(
                    f"<td>£{float(r.get('wage_rate_used') or 0):.2f}</td>")
                html.append(f"<td>£{float(r.get('pay') or 0):.2f}</td>")
                html.append(
                    f"<td>£{float(r.get('travel_parking') or 0):.2f}</td>")
                html.append(f"<td>{r.get('notes') or ''}</td>")
                html.append("</tr>")

            html.append("</tbody></table>")
            html.append("<h4>Totals</h4><ul>")
            html.append(f"<li>Hours: {totals['hours']:.2f}</li>")
            html.append(f"<li>Pay: £{totals['pay']:.2f}</li>")
            html.append(f"<li>Travel: £{totals['travel']:.2f}</li>")
            html.append(
                f"<li>Lateness: {int(totals['lateness_mins'])} mins</li>")
            html.append(
                f"<li>Overrun: {int(totals['overrun_mins'])} mins</li>")
            html.append("</ul></body></html>")

            return "".join(html)

        finally:
            cur.close()
            conn.close()

    @staticmethod
    def export_week_csv(user_id: int, week_id: str) -> Tuple[bytes, str]:
        """
        Export timesheet week entries to CSV.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            # Resolve week PK
            cur.execute(
                "SELECT id, week_ending FROM tb_timesheet_weeks WHERE user_id=%s AND week_id=%s",
                (user_id, week_id),
            )
            wk = cur.fetchone()
            if not wk:
                return b"", f"timesheet_{user_id}_{week_id}.csv"

            # Fetch entries
            cur.execute(
                """ 
                SELECT e.*,
                    jt.name AS job_type_name,
                    COALESCE(e.client_name, c.name) AS client_name,
                    COALESCE(e.site_name, s.name)   AS site_name
                FROM tb_timesheet_entries e 
                JOIN job_types jt ON jt.id = e.job_type_id 
                LEFT JOIN clients c ON c.id = e.client_name 
                LEFT JOIN sites s ON s.id = e.site_name 
                WHERE e.week_id=%s AND e.user_id=%s 
                ORDER BY e.work_date ASC, e.actual_start ASC 
                """,
                (wk["id"], user_id),
            )
            rows = cur.fetchall() or []

            # Write CSV to buffer
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow([
                "Date", "Client", "Site", "Job Type",
                "Scheduled Start", "Scheduled End",
                "Actual Start", "Actual End",
                "Break (mins)", "Hours", "Rate", "Pay", "Travel", "Notes"
            ])

            for r in rows:
                writer.writerow([
                    r["work_date"].strftime("%Y-%m-%d"),
                    r.get("client_name") or "",
                    r.get("site_name") or "",
                    r.get("job_type_name") or "",
                    str(r["scheduled_start"])[:5],
                    str(r["scheduled_end"])[:5],
                    str(r["actual_start"])[:5],
                    str(r["actual_end"])[:5],
                    int(r.get("break_mins") or 0),
                    f"{float(r.get('actual_hours') or 0):.2f}",
                    f"{float(r.get('wage_rate_used') or 0):.2f}",
                    f"{float(r.get('pay') or 0):.2f}",
                    f"{float(r.get('travel_parking') or 0):.2f}",
                    r.get("notes") or ""
                ])

            # Encode as UTF-8 BOM for Excel compatibility
            csv_bytes = buf.getvalue().encode("utf-8-sig")
            filename = f"timesheet_{user_id}_{week_id}.csv"
            return csv_bytes, filename

        finally:
            cur.close()
            conn.close()

    from typing import Tuple

    @staticmethod
    def export_week_pdf(user_id: int, week_id: str) -> Tuple[bytes, str]:
        """
        Native PDF generation with ReportLab (no external binaries).
        Renders a branded, tabular timesheet in landscape orientation:
        header, entries, totals, footer.
        """
        from io import BytesIO
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

        # ------------------------------------------------------------------ #
        # Fetch week info + entries
        # ------------------------------------------------------------------ #
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT id, week_id, week_ending, status
                FROM tb_timesheet_weeks
                WHERE user_id=%s AND week_id=%s
                """,
                (user_id, week_id),
            )
            wk = cur.fetchone()
            if not wk:
                return b"", f"timesheet_{user_id}_{week_id}.pdf"

            cur.execute(
                """
                SELECT e.*,
                    jt.name AS job_type_name,
                    COALESCE(e.client_name, c.name) AS client_name,
                    COALESCE(e.site_name, s.name)   AS site_name
                FROM tb_timesheet_entries e
                JOIN job_types jt ON jt.id = e.job_type_id
                LEFT JOIN clients c ON c.id = e.client_name
                LEFT JOIN sites s   ON s.id = e.site_name
                WHERE e.week_id=%s AND e.user_id=%s
                ORDER BY e.work_date ASC, e.actual_start ASC
                """,
                (wk["id"], user_id),
            )
            entries = cur.fetchall() or []

        finally:
            cur.close()
            conn.close()

        totals = {
            "hours": sum((e.get("actual_hours") or 0) for e in entries),
            "pay": sum((e.get("pay") or 0) for e in entries),
            "travel": sum((e.get("travel_parking") or 0) for e in entries),
        }

        # ------------------------------------------------------------------ #
        # Build PDF
        # ------------------------------------------------------------------ #
        buf = BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=landscape(A4),
            leftMargin=15 * mm,
            rightMargin=15 * mm,
            topMargin=15 * mm,
            bottomMargin=15 * mm,
            title=f"Timesheet {user_id}-{week_id}",
        )
        styles = getSampleStyleSheet()
        story = []

        # Header
        try:
            company = (session.get("site_settings", {})
                       or {}).get("company_name")
        except Exception:
            company = None

        title = f"{company or 'Sparrow ERP'} – Timesheet"
        story.append(Paragraph(title, styles["Title"]))

        meta = (
            f"User: {user_id} • Week: {wk.get('week_id', week_id)} • "
            f"Status: {(wk.get('status') or 'draft').title()}"
        )
        if wk.get("week_ending"):
            try:
                meta += f" • Week ending: {wk['week_ending'].strftime('%Y-%m-%d')}"
            except Exception:
                meta += f" • Week ending: {wk['week_ending']}"
        story.append(Paragraph(meta, styles["Normal"]))
        story.append(Spacer(1, 8))

        # ------------------------------------------------------------------ #
        # Table data
        # ------------------------------------------------------------------ #
        header = [
            "Date", "Client", "Site", "Job Type",
            "Scheduled", "Actual", "Break", "Hours",
            "Rate", "Pay", "Travel", "Notes",
        ]
        data = [header]

        def tstr(t):
            if not t:
                return ""
            s = str(t)
            return s[:5] if len(s) >= 5 and s[2] == ":" else s

        def date_str(v):
            try:
                return v.strftime("%Y-%m-%d")
            except Exception:
                return str(v or "")

        for e in entries:
            row = [
                date_str(e.get("work_date")),
                e.get("client_name") or "",
                e.get("site_name") or "",
                e.get("job_type_name") or e.get("job_type_id") or "",
                f"{tstr(e.get('scheduled_start'))}–{tstr(e.get('scheduled_end'))}",
                f"{tstr(e.get('actual_start'))}–{tstr(e.get('actual_end'))}",
                str(int(e.get("break_mins") or 0)),
                f"{float(e.get('actual_hours') or 0):.2f}",
                f"£{float(e.get('wage_rate_used') or 0):.2f}",
                f"£{float(e.get('pay') or 0):.2f}",
                f"£{float(e.get('travel_parking') or 0):.2f}",
                e.get("notes") or "",
            ]
            data.append(row)

        # Totals row
        data.append([
            "", "", "", "", "", "Totals",
            "", f"{float(totals['hours']):.2f}",
            "", f"£{float(totals['pay']):.2f}",
            f"£{float(totals['travel']):.2f}",
            "",
        ])

        # ------------------------------------------------------------------ #
        # Column widths (sum exactly to page width)
        # ------------------------------------------------------------------ #
        page_width = landscape(A4)[0] - (15 * mm + 15 * mm)

        col_widths = [
            22 * mm,  # Date
            32 * mm,  # Client
            28 * mm,  # Site
            28 * mm,  # Job Type
            26 * mm,  # Scheduled
            26 * mm,  # Actual
            14 * mm,  # Break
            16 * mm,  # Hours
            16 * mm,  # Rate
            18 * mm,  # Pay
            18 * mm,  # Travel
            page_width - (22 + 32 + 28 + 28 + 26 + 26 + 14 +
                          16 + 16 + 18 + 18) * mm,  # Notes
        ]

        # ------------------------------------------------------------------ #
        # Table styling
        # ------------------------------------------------------------------ #
        table = Table(data, colWidths=col_widths,
                      repeatRows=1, hAlign="CENTER")
        table.setStyle(TableStyle([
            # Header row
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#444444")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 10),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            # Body rows
            ("FONT", (0, 1), (-1, -1), "Helvetica", 8),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cccccc")),
            ("BOX", (0, 0), (-1, -1), 0.25, colors.HexColor("#cccccc")),
            # Totals row
            ("FONT", (5, -1), (-1, -1), "Helvetica-Bold", 9),
            ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#eeeeee")),
        ]))

        story.append(table)
        story.append(Spacer(1, 8))

        # Rejection reason if present
        if wk.get("rejected_reason"):
            story.append(
                Paragraph(
                    f"Rejected reason: {wk['rejected_reason']}", styles["Italic"])
            )

        # Footer
        story.append(Spacer(1, 12))
        story.append(Paragraph("Generated by Sparrow ERP", styles["Normal"]))

        doc.build(story)
        pdf_bytes = buf.getvalue()
        buf.close()

        filename = f"timesheet_{user_id}_{week_id}.pdf"
        return pdf_bytes, filename
