"""
Hourly tick: at most one appraisal reminder scan + digest email per calendar day.
"""

from __future__ import annotations

import logging
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

_logger = logging.getLogger(__name__)

_scheduler: Optional[BackgroundScheduler] = None


def init_appraisal_reminder_scheduler(app) -> None:
    global _scheduler
    if app.config.get("_appraisal_reminder_scheduler_started"):
        return
    try:
        from .appraisals_reminders import run_appraisal_scheduler_tick
    except Exception as e:
        _logger.warning("Appraisal scheduler import failed: %s", e)
        return

    job_id = "hr_appraisal_reminder_hourly_tick"
    try:
        _scheduler = BackgroundScheduler()

        def tick():
            try:
                with app.app_context():
                    run_appraisal_scheduler_tick()
            except Exception:
                _logger.exception("Appraisal reminder scheduled tick failed")

        _scheduler.add_job(
            tick,
            "interval",
            hours=1,
            id=job_id,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        _scheduler.start()
        app.config["_appraisal_reminder_scheduler_started"] = True
        _logger.info("HR appraisal reminder scheduler started (hourly tick; one digest per day).")
    except Exception as e:
        _logger.warning("HR appraisal reminder scheduler not started: %s", e)
