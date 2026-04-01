"""
APScheduler hook for DBS Update Service periodic status checks.
Configured with DBS_UPDATE_SERVICE_ENABLED and DBS_STATUS_CHECK_INTERVAL (daily|weekly|off).
"""
from __future__ import annotations

import logging
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

_logger = logging.getLogger(__name__)

_scheduler: Optional[BackgroundScheduler] = None


def init_dbs_status_scheduler(app) -> None:
    """Start background interval job once per process when env permits."""
    global _scheduler
    if app.config.get("_dbs_crsc_scheduler_started"):
        return
    try:
        from .dbs_update_client import (
            is_dbs_update_service_enabled,
            run_scheduled_dbs_status_checks,
            scheduled_check_interval_label,
        )
    except Exception as e:
        _logger.warning("DBS scheduler import failed: %s", e)
        return

    if not is_dbs_update_service_enabled():
        return
    if scheduled_check_interval_label() == "off":
        return

    job_id = "dbs_crsc_status_interval"
    try:
        _scheduler = BackgroundScheduler()
        if scheduled_check_interval_label() == "daily":
            _scheduler.add_job(
                run_scheduled_dbs_status_checks,
                "interval",
                days=1,
                id=job_id,
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
        else:
            _scheduler.add_job(
                run_scheduled_dbs_status_checks,
                "interval",
                weeks=1,
                id=job_id,
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
        _scheduler.start()
        app.config["_dbs_crsc_scheduler_started"] = True
        _logger.info(
            "DBS CRSC scheduled checks started (%s)",
            scheduled_check_interval_label(),
        )
    except Exception as e:
        _logger.warning("DBS CRSC scheduler not started: %s", e)
