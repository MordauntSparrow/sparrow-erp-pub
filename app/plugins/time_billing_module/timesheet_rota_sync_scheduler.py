"""
Background sync: create/update draft timesheet weeks from the scheduling rota.

Runs on an interval while the app is up. For each active contractor who has at least
one ``schedule_shifts`` row in the current or next two ISO weeks, ensures the
``tb_timesheet_weeks`` row exists and runs the same scheduler→timesheet merge as
opening the week in the UI (``TimesheetService.sync_scheduler_shifts_into_week``).

Respects ``scheduler_week_prefill_enabled`` in the Time Billing plugin manifest;
if disabled, this job is a no-op.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional, Set

from apscheduler.schedulers.background import BackgroundScheduler

_logger = logging.getLogger(__name__)

_scheduler: Optional[BackgroundScheduler] = None


def _iso_week_ids_covering_dates(center: date, *, weeks_ahead: int = 2) -> Set[str]:
    out: Set[str] = set()
    for offset in range(0, (weeks_ahead + 1) * 7, 7):
        d = center + timedelta(days=offset)
        y, w, _ = d.isocalendar()
        out.add(f"{y}{w:02d}")
    return out


def run_timesheet_rota_background_sync() -> None:
    from app.objects import get_db_connection
    from app.plugins.time_billing_module.services import TimesheetService

    if not TimesheetService._scheduler_week_prefill_enabled():
        return

    today = date.today()
    week_ids = sorted(_iso_week_ids_covering_dates(today, weeks_ahead=2))

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id FROM tb_contractors
            WHERE status IN ('active', '1', 1)
            ORDER BY id ASC
            """
        )
        uids = [int(r[0]) for r in (cur.fetchall() or [])]
    finally:
        cur.close()
        conn.close()

    for uid in uids:
        for wid in week_ids:
            try:
                we = TimesheetService.week_ending_date_from_week_id(wid)
                if not TimesheetService.contractor_has_schedule_shifts_in_week(uid, we):
                    continue
                TimesheetService.sync_scheduler_shifts_into_week(uid, wid)
            except Exception:
                _logger.exception(
                    "Timesheet rota sync failed user_id=%s week_id=%s", uid, wid
                )


def init_timesheet_rota_sync_scheduler(app) -> None:
    global _scheduler
    if app.config.get("_tb_rota_sync_scheduler_started"):
        return
    try:
        from app.plugins.time_billing_module.services import TimesheetService
    except Exception as e:
        _logger.warning("Timesheet rota sync: import failed: %s", e)
        return

    if not TimesheetService._scheduler_week_prefill_enabled():
        return
    if not TimesheetService._get_bool_setting(
        "scheduler_week_background_sync_enabled", default=True
    ):
        return

    try:
        _scheduler = BackgroundScheduler()

        def tick():
            try:
                with app.app_context():
                    run_timesheet_rota_background_sync()
            except Exception:
                _logger.exception("Timesheet rota scheduled tick failed")

        _scheduler.add_job(
            tick,
            "interval",
            hours=6,
            id="time_billing_rota_timesheet_sync",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            next_run_time=datetime.now() + timedelta(seconds=90),
        )
        _scheduler.start()
        app.config["_tb_rota_sync_scheduler_started"] = True
        _logger.info(
            "Time Billing rota→timesheet scheduler started (6h; current + next ISO weeks)."
        )
    except Exception as e:
        _logger.warning("Time Billing rota sync scheduler not started: %s", e)
