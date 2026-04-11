"""
APScheduler hook for DBS Update Service mass CRSC checks.

Cadence comes from HR plugin settings (``dbs_crsc_mass_check_interval_days`` in
``manifest.json``), editable from the HR People dashboard or plugin settings.
If that is 0, legacy env ``DBS_STATUS_CHECK_INTERVAL`` (daily|weekly) is still honoured.

A lightweight hourly job reads the current setting and last scheduled run time;
when due, it checks every opted-in employee (same rules as manual checks).

Server-wide CRSC calls can be hard-disabled with DBS_UPDATE_SERVICE_ENABLED=0/false/no/off.
"""
from __future__ import annotations

import logging
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

_logger = logging.getLogger(__name__)

_scheduler: Optional[BackgroundScheduler] = None


def init_dbs_status_scheduler(app) -> None:
    """Start an hourly tick that runs mass checks when HR settings say it is due."""
    global _scheduler
    if app.config.get("_dbs_crsc_scheduler_started"):
        return
    try:
        from .dbs_update_client import (
            is_dbs_update_service_enabled,
            run_scheduled_dbs_status_checks_if_due,
        )
    except Exception as e:
        _logger.warning("DBS scheduler import failed: %s", e)
        return

    if not is_dbs_update_service_enabled():
        return

    job_id = "dbs_crsc_mass_check_hourly_tick"
    try:
        _scheduler = BackgroundScheduler()

        def tick():
            try:
                with app.app_context():
                    run_scheduled_dbs_status_checks_if_due()
            except Exception:
                _logger.exception("DBS CRSC scheduled tick failed")

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
        app.config["_dbs_crsc_scheduler_started"] = True
        _logger.info(
            "DBS CRSC scheduler started (hourly tick; interval from HR settings)."
        )
    except Exception as e:
        _logger.warning("DBS CRSC scheduler not started: %s", e)
