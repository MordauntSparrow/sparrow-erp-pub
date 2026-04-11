"""
APScheduler hook for scheduled HCPC register checks.

Cadence comes from `hcpc_mass_check_interval_days` in `manifest.json`, editable
from the HR People dashboard or plugin settings. A lightweight hourly job checks
whether a scheduled mass run is due.
"""
from __future__ import annotations

import logging
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

_logger = logging.getLogger(__name__)

_scheduler: Optional[BackgroundScheduler] = None


def init_hcpc_status_scheduler(app) -> None:
    global _scheduler
    if app.config.get("_hcpc_scheduler_started"):
        return
    try:
        from .hcpc_register_check import (
            is_hcpc_register_api_enabled,
            run_scheduled_hcpc_register_checks_if_due,
        )
    except Exception as e:
        _logger.warning("HCPC scheduler import failed: %s", e)
        return

    if not is_hcpc_register_api_enabled():
        return

    try:
        _scheduler = BackgroundScheduler()

        def tick():
            try:
                with app.app_context():
                    run_scheduled_hcpc_register_checks_if_due()
            except Exception:
                _logger.exception("HCPC scheduled tick failed")

        _scheduler.add_job(
            tick,
            "interval",
            hours=1,
            id="hcpc_mass_check_hourly_tick",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        _scheduler.start()
        app.config["_hcpc_scheduler_started"] = True
        _logger.info("HCPC scheduler started (hourly tick; interval from HR settings).")
    except Exception as e:
        _logger.warning("HCPC scheduler not started: %s", e)
