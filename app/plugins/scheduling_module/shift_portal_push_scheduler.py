"""
Background Web Push: remind contractors ~1 hour before a scheduled shift starts.

Uses ``schedule_shifts.portal_reminder_sent_at`` for idempotency. Respects employee portal
VAPID config (same as ``push_service.schedule_push_for_contractor``).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

_logger = logging.getLogger(__name__)
_scheduler: Optional[BackgroundScheduler] = None


def run_shift_start_reminder_pass() -> None:
    """Notify assignees once per shift when start is ~60 minutes away (server local time)."""
    from app.objects import get_db_connection
    from app.plugins.scheduling_module.services import (
        ScheduleService,
        _portal_shift_notify,
    )

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW COLUMNS FROM schedule_shifts LIKE 'portal_reminder_sent_at'")
        if not cur.fetchone():
            return
        cur.execute(
            """
            SELECT id FROM schedule_shifts s
            WHERE COALESCE(s.status,'') IN ('draft','published','in_progress')
              AND s.portal_reminder_sent_at IS NULL
              AND TIMESTAMPDIFF(MINUTE, NOW(), TIMESTAMP(s.work_date, s.scheduled_start)) BETWEEN 52 AND 74
            """
        )
        rows = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    for r in rows:
        sid = int(r["id"])
        recipients = ScheduleService._shift_portal_recipient_ids(sid)
        if recipients:
            _portal_shift_notify(recipients, "starting_soon", sid)
        conn_u = get_db_connection()
        cu = conn_u.cursor()
        try:
            cu.execute(
                "UPDATE schedule_shifts SET portal_reminder_sent_at = NOW() WHERE id = %s",
                (sid,),
            )
            conn_u.commit()
        finally:
            cu.close()
            conn_u.close()


def init_shift_portal_push_scheduler(app) -> None:
    global _scheduler
    if app.config.get("_scheduling_shift_reminder_scheduler_started"):
        return
    try:
        from app.plugins.employee_portal_module.push_service import is_push_configured
    except Exception as e:
        _logger.warning("Shift reminder scheduler: push import failed: %s", e)
        return
    if not is_push_configured():
        return
    try:
        _scheduler = BackgroundScheduler()

        def tick():
            try:
                with app.app_context():
                    run_shift_start_reminder_pass()
            except Exception:
                _logger.exception("Shift portal reminder tick failed")

        _scheduler.add_job(
            tick,
            "interval",
            minutes=12,
            id="scheduling_shift_portal_reminders",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            next_run_time=datetime.now() + timedelta(seconds=120),
        )
        _scheduler.start()
        app.config["_scheduling_shift_reminder_scheduler_started"] = True
        _logger.info(
            "Scheduling shift reminder Web Push scheduler started (12 min interval)."
        )
    except Exception as e:
        _logger.warning("Scheduling shift reminder scheduler not started: %s", e)
