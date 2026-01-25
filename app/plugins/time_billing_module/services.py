from datetime import datetime, date, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional, Tuple
import json
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
    def _job_type_name(job_type_id: int) -> Optional[str]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT name FROM job_types WHERE id=%s",
                        (job_type_id,))
            r = cur.fetchone()
            return r["name"] if r else None
        finally:
            cur.close()
            conn.close()

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
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)

        try:
            # MVP: no joins to clients/sites (IDs not in use yet). Use free-text fields.
            cur.execute(
                """
                SELECT 
                    e.*,
                    jt.name AS job_type_name,
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
                "hours": float(sum((r.get("actual_hours") or 0) for r in rows)),
                "pay": _to_money(sum((r.get("pay") or 0) for r in rows)),
                "travel": _to_money(sum((r.get("travel_parking") or 0) for r in rows)),
                "lateness_mins": int(sum((r.get("lateness_mins") or 0) for r in rows)),
                "overrun_mins": int(sum((r.get("overrun_mins") or 0) for r in rows)),
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
                "is_admin": bool(is_admin),
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
            for e in entries or []:
                # Validate payload
                ok, err = TimesheetService._validate_entry_payload(e)
                if not ok:
                    conflicts.append(
                        {"temp_id": e.get("temp_id"), "reason": err})
                    continue

                entry_id = e.get("id")

                # ----- Existing entry checks -----
                if entry_id:
                    cur.execute(
                        """
                        SELECT user_id, source, lock_job_client
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

                    # Lock handling: prevent changes on generated entries
                    if existing["source"] in ("runsheet", "scheduler") and existing.get(
                        "lock_job_client"
                    ):
                        for locked in (
                            "job_type_id",
                            "client_name",
                            "site_name",
                        ):
                            if locked in e:
                                del e[locked]

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
                    "edited_by": None,
                    "edited_at": None,
                    "edit_reason": None,
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

                saved_entries.append(
                    {
                        "id": entry_id,
                        "work_date": params["work_date"].isoformat(),
                        "client_name": params["client_name"],
                        "site_name": params["site_name"],
                        "job_type_id": params["job_type_id"],
                        "job_type_name": TimesheetService._job_type_name(
                            params["job_type_id"]
                        ),
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

        Args:
            reason: Mandatory rejection reason
        """
        if not reason or not reason.strip():
            raise Exception("Rejection reason is required.")

        wk = TimesheetService._ensure_week(user_id, week_id)
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
                body = f"Hi,\n\nWe couldn’t approve your timesheet. Please review and fix the following:\n\n- {reason.strip()}\n\nThen resubmit.\n\nThanks."
                html_body = f"<p>We couldn’t approve your timesheet.</p><p><strong>Reason:</strong> {reason.strip()}</p><p>Please log in to fix and resubmit.</p>"

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
                    where.append("LOWER(c.name) LIKE %s")
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
                c.name AS client_name,
                r.site_id,
                s.name AS site_name,
                r.job_type_id,
                jt.name AS job_type_name,
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
    def create_runsheet(data: Dict[str, Any]) -> int:
        """
        Create a new runsheet record.
        """
        required = ["client_name", "job_type_id", "work_date"]
        missing = [k for k in required if not data.get(k)]
        if missing:
            raise Exception(f"Missing fields: {', '.join(missing)}")

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO runsheets (client_name, site_name, job_type_id, work_date,
                                       window_start, window_end, template_id, template_version,
                                       payload_json, mapping_json, lead_user_id, status, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'draft',%s)
            """, (
                data["client_name"], data.get(
                    "site_name"), data["job_type_id"],
                data["work_date"],
                data.get("window_start"), data.get("window_end"),
                data.get("template_id"), data.get("template_version"),
                json.dumps(data.get("payload") or {}),
                json.dumps(data.get("mapping") or {}),
                data.get("lead_user_id"),
                data.get("notes")
            ))
            conn.commit()
            return cur.lastrowid
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
                SELECT r.*, c.name AS client_name, s.name AS site_name, jt.name AS job_type_name
                FROM runsheets r
                JOIN clients c ON c.id=r.client_name
                LEFT JOIN sites s ON s.id=r.site_name
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
            return rs
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_runsheet(rs_id: int, data: Dict[str, Any]) -> None:
        """
        Update runsheet header and optionally its assignments.
        """
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Update header
            fields = []
            params = {}
            for k in ("client_name", "site_name", "job_type_id", "work_date", "window_start",
                      "window_end", "template_id", "template_version", "lead_user_id", "status", "notes"):
                if k in data:
                    fields.append(f"{k}=%({k})s")
                    params[k] = data[k]
            if "payload" in data:
                fields.append("payload_json=%(payload_json)s")
                params["payload_json"] = json.dumps(data["payload"])
            if "mapping" in data:
                fields.append("mapping_json=%(mapping_json)s")
                params["mapping_json"] = json.dumps(data["mapping"])
            if fields:
                sql = f"UPDATE runsheets SET {', '.join(fields)} WHERE id=%(id)s"
                params["id"] = rs_id
                cur.execute(sql, params)

            # Handle assignments (delete and reinsert)
            if "assignments" in data and isinstance(data["assignments"], list):
                cur.execute(
                    "DELETE FROM runsheet_assignments WHERE runsheet_id=%s", (rs_id,))
                for a in data["assignments"]:
                    cur.execute("""
                        INSERT INTO runsheet_assignments
                        (runsheet_id, user_id, scheduled_start, scheduled_end, actual_start, actual_end,
                         break_mins, travel_parking, notes)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        rs_id, a["user_id"],
                        a.get("scheduled_start"), a.get("scheduled_end"),
                        a.get("actual_start"), a.get("actual_end"),
                        int(a.get("break_mins") or 0),
                        float(a.get("travel_parking") or 0),
                        a.get("notes")
                    ))
            conn.commit()
        finally:
            cur.close()
            conn.close()

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
                    "client_name": rs["client_name"],
                    "site_name": rs.get("site_name"),
                    "client_name": rs.get("client_name"),  # may be None
                    "site_name": rs.get("site_name"),      # may be None
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
    def _week_id_for_date(d: date) -> str:
        """
        Converts a date into ISO week string: YYYYWW
        """
        iso_year, iso_week, _ = d.isocalendar()
        return f"{iso_year}{iso_week:02d}"


class TemplateService:
    # ---- Job Types CRUD ----
    @staticmethod
    def list_job_types() -> list[dict]:
        """List all job types."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, name, code, active FROM job_types ORDER BY name")
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
                "INSERT INTO job_types (name, code, active) VALUES (%s,%s,%s)",
                (data["name"], data.get("code"), int(data.get("active", 1)))
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
                f"UPDATE job_types SET {', '.join(fields)} WHERE id=%(id)s", params)
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

    # ---- Bill Rate Cards & Rows ----
    @staticmethod
    def list_bill_cards() -> list[dict]:
        """List all bill rate cards with associated client and site names."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT brc.id, brc.name, brc.active, brc.client_name, c.name AS client_name,
                       brc.site_name, s.name AS site_name
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
                       t.job_type_id, jt.name AS job_type_name,
                       t.client_name, c.name AS client_name,
                       t.site_name, s.name AS site_name
                FROM runsheet_templates t
                LEFT JOIN job_types jt ON jt.id=t.job_type_id
                LEFT JOIN clients c ON c.id=t.client_name
                LEFT JOIN sites s ON s.id=t.site_name
                ORDER BY t.id DESC
            """)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    # ---- Runsheet Templates ----
    @staticmethod
    def create_runsheet_template(data: dict) -> int:
        """Create a new runsheet template."""
        if not data.get("name"):
            raise Exception("name required")

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO runsheet_templates
                (name, code, job_type_id, client_name, site_name, active, version)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                data["name"], data.get("code"), data.get("job_type_id"),
                data.get("client_name"), data.get("site_name"),
                int(data.get("active", 1)), int(data.get("version", 1))
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
            fields, params = [], {"id": tpl_id}
            for k in ("name", "code", "job_type_id", "client_name", "site_name", "active", "version"):
                if k in data:
                    fields.append(f"{k}=%({k})s")
                    params[k] = data[k]
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
    def render_form_schema(tpl_id: int) -> dict:
        """Render runsheet template as a form schema."""
        tpl = TemplateService.get_runsheet_template(tpl_id)
        schema = {
            "template": {
                "id": tpl["id"],
                "name": tpl["name"],
                "code": tpl.get("code"),
                "version": tpl.get("version")
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

            # --- Select/Dropdown validation ---
            elif typ in ("select", "dropdown"):
                opts = f.get("options_json")
                if isinstance(opts, str):
                    try:
                        opts = json.loads(opts)
                    except Exception:
                        opts = None
                valid_values = set()
                if isinstance(opts, list):
                    for o in opts:
                        if isinstance(o, dict) and "value" in o:
                            valid_values.add(str(o["value"]))
                        else:
                            valid_values.add(str(o))
                if valid_values and str(val) not in valid_values:
                    raise Exception(
                        f"Field '{f['label']}' has an invalid choice.")

            # --- Boolean/Checkbox validation ---
            elif typ in ("checkbox", "boolean", "bool"):
                if not isinstance(val, (bool, int)):
                    s = str(val).lower()
                    if s not in ("true", "false", "1", "0", "yes", "no"):
                        raise Exception(
                            f"Field '{f['label']}' must be true/false.")

            # --- Date validation ---
            elif typ == "date":
                try:
                    datetime.strptime(str(val), "%Y-%m-%d")
                except Exception:
                    raise Exception(
                        f"Field '{f['label']}' must be a date in YYYY-MM-DD format.")

            # --- Time validation ---
            elif typ == "time":
                try:
                    _ = _to_time(str(val))
                except Exception:
                    raise Exception(
                        f"Field '{f['label']}' must be a time in HH:MM or HH:MM:SS format.")

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
        Fetches the stored mapping JSON for a given runsheet template.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT mapping_json FROM runsheet_templates WHERE id=%s", (
                    tpl_id,)
            )
            row = cur.fetchone()
            if not row:
                return None
            m = row.get("mapping_json")
            if isinstance(m, str):
                try:
                    return json.loads(m)
                except Exception:
                    return None
            return m
        finally:
            cur.close()
            conn.close()


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
