"""
Detect MySQL schema drift (missing table/column) and recover before users see a raw 500.

When an organisation admin hits a ProgrammingError/OperationalError typical of an outdated
schema after a deploy, run the same upgrade subprocess chain as Settings → Version
(UpdateManager.run_upgrade_scripts), then redirect once so the request can succeed.

Kill-switches (env):
  SPARROW_AUTO_DB_UPGRADE=0|false|no     — never auto-run; still show friendly pages.
  SPARROW_AUTO_DB_UPGRADE_UNAUTH=1      — allow auto-upgrade with no logged-in user
                                         (dev/trusted deploys only; not for public internet).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlencode

if TYPE_CHECKING:
    from flask import Flask

logger = logging.getLogger(__name__)

# MySQL: ER_BAD_FIELD_ERROR, ER_NO_SUCH_TABLE
MYSQL_SCHEMA_ERRNOS = frozenset({1054, 1146})

_recovery_lock = threading.Lock()
_last_auto_upgrade_monotonic = 0.0
_AUTO_UPGRADE_DEBOUNCE_SEC = 12.0


def _run_upgrade_scripts_debounced():
    """
    Run UpdateManager.upgrade subprocess at most once per debounce window while holding a lock
    so parallel requests after a deploy do not start many concurrent upgrades.
    Returns (report dict, skipped: bool).
    """
    global _last_auto_upgrade_monotonic
    with _recovery_lock:
        now = time.monotonic()
        if now - _last_auto_upgrade_monotonic < _AUTO_UPGRADE_DEBOUNCE_SEC:
            logger.info(
                "Auto DB upgrade skipped (debounce %.0fs after last run)",
                _AUTO_UPGRADE_DEBOUNCE_SEC,
            )
            return {}, True
        _last_auto_upgrade_monotonic = now
        from app.objects import UpdateManager

        report = UpdateManager().run_upgrade_scripts()
        return report, False


def _iter_exception_chain(exc: BaseException):
    seen: set[int] = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        yield cur
        seen.add(id(cur))
        nxt = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
        cur = nxt if isinstance(nxt, BaseException) else None


def is_schema_mismatch_mysql_error(exc: BaseException) -> bool:
    """True when failure is likely fixable by running install.py upgrade scripts."""
    for e in _iter_exception_chain(exc):
        errno = getattr(e, "errno", None)
        if errno in MYSQL_SCHEMA_ERRNOS:
            return True
        msg = str(e).lower()
        if "unknown column" in msg and "field list" in msg:
            return True
        if "doesn't exist" in msg and "table" in msg:
            return True
        if "doesn't exist" in msg and "`" in msg:
            return True
    return False


def _env_auto_upgrade_enabled() -> bool:
    v = (os.environ.get("SPARROW_AUTO_DB_UPGRADE") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _env_unauth_upgrade_enabled() -> bool:
    v = (os.environ.get("SPARROW_AUTO_DB_UPGRADE_UNAUTH") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _can_org_admin_auto_upgrade() -> bool:
    try:
        from flask_login import current_user
    except Exception:
        return False
    if not getattr(current_user, "is_authenticated", False):
        return False
    r = (getattr(current_user, "role", None) or "").lower()
    return r in ("admin", "superuser", "support_break_glass")


def _wants_json_response(request) -> bool:
    path = request.path or ""
    if path.startswith("/api/") or "/api/" in path:
        return True
    accept = (request.headers.get("Accept") or "").lower()
    return "application/json" in accept and "text/html" not in accept


def _upgrade_report_failed(report: dict) -> bool:
    core = report.get("core") or {}
    if core.get("ran") and not core.get("ok"):
        return True
    if report.get("plugins_failed"):
        return True
    return False


def register_schema_upgrade_recovery(app: Flask) -> None:
    try:
        import mysql.connector as _mysqlc
    except ImportError:
        return

    mysql_error = _mysqlc.Error

    @app.errorhandler(mysql_error)
    def _mysql_schema_recovery(exc: mysql_error):  # type: ignore[name-defined]
        if not is_schema_mismatch_mysql_error(exc):
            raise exc

        from flask import (
            flash,
            jsonify,
            redirect,
            render_template,
            request,
            url_for,
        )

        wants_json = _wants_json_response(request)
        debug_detail = str(exc) if app.debug else None

        def _friendly_json(
            *,
            error: str,
            message: str,
            status: int = 503,
            extra: Optional[dict[str, Any]] = None,
        ):
            body: dict[str, Any] = {
                "error": error,
                "message": message,
            }
            if extra:
                body.update(extra)
            if debug_detail:
                body["detail"] = debug_detail
            return jsonify(body), status

        def _friendly_html(template: str, **ctx):
            return (
                render_template(
                    template,
                    debug_detail=debug_detail,
                    **ctx,
                ),
                503,
            )

        if not _env_auto_upgrade_enabled():
            if wants_json:
                return _friendly_json(
                    error="database_schema_outdated",
                    message=(
                        "The database schema is out of date. "
                        "An administrator must run database upgrades from Settings → Version."
                    ),
                )
            return _friendly_html(
                "database_schema_recovery.html",
                scenario="disabled",
                ran_upgrade=False,
            )

        # Avoid infinite upgrade loops (HTML uses ?_recover=1, JSON uses header).
        if request.args.get("_recover") == "1" or (
            wants_json and (request.headers.get("X-Sparrow-Schema-Recovery") or "").strip() == "1"
        ):
            if wants_json:
                return _friendly_json(
                    error="database_schema_still_outdated",
                    message=(
                        "Schema is still incompatible after an automatic upgrade attempt. "
                        "Check server logs and run upgrades manually if needed."
                    ),
                )
            return _friendly_html(
                "database_schema_recovery.html",
                scenario="still_failing",
                ran_upgrade=False,
            )

        allow_upgrade = _can_org_admin_auto_upgrade() or _env_unauth_upgrade_enabled()
        if not allow_upgrade:
            if wants_json:
                return _friendly_json(
                    error="database_schema_outdated",
                    message=(
                        "The application was updated but the database is not yet migrated. "
                        "Please ask an organisation administrator to sign in and load any page "
                        "(upgrades apply automatically) or run database upgrades from Settings → Version."
                    ),
                )
            return _friendly_html(
                "database_schema_recovery.html",
                scenario="need_admin",
                ran_upgrade=False,
            )

        report: dict = {}
        try:
            report, _skipped = _run_upgrade_scripts_debounced()
        except Exception as run_ex:
            logger.exception("Automatic database upgrade failed: %s", run_ex)
            if wants_json:
                return _friendly_json(
                    error="database_upgrade_failed",
                    message="Automatic schema upgrade failed. See server logs.",
                    status=503,
                )
            return _friendly_html(
                "database_schema_recovery.html",
                scenario="upgrade_exception",
                ran_upgrade=False,
                upgrade_error=str(run_ex) if app.debug else None,
            )

        had_failures = _upgrade_report_failed(report)
        if had_failures:
            logger.warning(
                "Automatic DB upgrade completed with failures: core=%s plugins_failed=%s",
                report.get("core"),
                report.get("plugins_failed"),
            )

        if wants_json:
            return _friendly_json(
                error="database_schema_updated",
                message="Upgrade scripts were run. Retry your request.",
                extra={"retry": True, "upgrade_partial_failure": had_failures},
                status=503,
            )

        if request.method == "GET":
            qargs = request.args.to_dict(flat=True)
            qargs["_recover"] = "1"
            qs = urlencode(sorted(qargs.items()))
            return redirect(request.path + "?" + qs)

        flash(
            "Database schema was updated automatically. Please submit the form again if needed.",
            "warning" if had_failures else "info",
        )
        try:
            dest = request.referrer or url_for("routes.dashboard")
        except Exception:
            dest = "/"
        return redirect(dest)
