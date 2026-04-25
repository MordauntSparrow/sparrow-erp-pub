"""Hourly tick: run scheduled compliance exports + file purge."""
from __future__ import annotations

import logging
import os
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

_logger = logging.getLogger(__name__)
_scheduler: Optional[BackgroundScheduler] = None


def init_compliance_audit_scheduler(app) -> None:
    global _scheduler
    if app.config.get("_compliance_audit_scheduler_started"):
        return
    enabled = (os.environ.get("COMPLIANCE_AUDIT_SCHEDULER") or "1").strip().lower()
    if enabled in ("0", "false", "no", "off"):
        return

    try:
        _scheduler = BackgroundScheduler()

        def tick():
            try:
                with app.app_context():
                    from .scheduled_export_runner import run_due_scheduled_exports

                    run_due_scheduled_exports()
            except Exception:
                _logger.exception("Compliance audit scheduled tick failed")

        _scheduler.add_job(
            tick,
            "interval",
            hours=1,
            id="compliance_audit_scheduled_exports",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        _scheduler.start()
        app.config["_compliance_audit_scheduler_started"] = True
        _logger.info("Compliance audit scheduler started (hourly).")
    except Exception as e:
        _logger.warning("Compliance audit scheduler not started: %s", e)
