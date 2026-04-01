"""
Unified staff / contractor activity audit (admin app + contractor portals in-process).

Records mutating HTTP requests where the actor is either a core admin user
(Flask-Login) or a contractor session (session[\"tb_user\"]) on configured path
prefixes. Rows tie to a subject contractor when the URL (or redirect) encodes one;
contractor self-service defaults subject = actor contractor.

Retention: ADMIN_STAFF_AUDIT_RETENTION_DAYS (default 90); purge at most hourly.

Table: admin_staff_action_logs (legacy name; holds all channels).
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

TABLE = "admin_staff_action_logs"
_last_purge_ts = 0.0
_PURGE_INTERVAL_SEC = 3600.0
_table_verified = False

MUTATING = frozenset({"POST", "PUT", "PATCH", "DELETE"})

CHANNEL_ADMIN = "admin_user"
CHANNEL_CONTRACTOR = "contractor_portal"

# Admin: plugin HTML/API and core management (session-authenticated admin user)
PLUGIN_AUDIT_PREFIXES: tuple[str, ...] = (
    "/plugin/hr_module/",
    "/plugin/time_billing_module/",
    "/plugin/inventory_control/",
    "/plugin/fleet_management/",
    "/plugin/asset_management/",
    "/plugin/recruitment_module/",
    "/plugin/scheduling_module/",
    "/plugin/ventus_response_module/",
    "/plugin/medical_records_module/",
    "/plugin/training_module/",
    "/plugin/compliance_module/",
    "/plugin/website_module/",
    "/plugin/event_manager_module/",
    "/plugin/employee_portal_module/",
    "/plugin/work_module/",
    "/plugin/news_blog_module/",
)

# Contractor-facing blueprints (same Flask process; session tb_user)
CONTRACTOR_AUDIT_PREFIXES: tuple[str, ...] = (
    "/time-billing/",
    "/employee-portal/",
    "/inventory/",
)

HR_CID_RE = re.compile(r"/plugin/hr_module/contractors/(\d+)")
TB_CID_RE = re.compile(r"/plugin/time_billing_module/api/contractors/(\d+)")
TB_TS_CONTRACTOR_RE = re.compile(r"/plugin/time_billing_module/api/timesheets/(\d+)/")
# Generic contractor id in path (other modules)
GENERIC_CONTRACTOR_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"/contractors/(\d+)(?:/|$)"),
    re.compile(r"/api/contractors/(\d+)(?:/|$)"),
    re.compile(r"/contractor/(\d+)(?:/|$)"),
)


def retention_days(app=None) -> int:
    raw = None
    if app is not None:
        raw = app.config.get("ADMIN_STAFF_AUDIT_RETENTION_DAYS")
    if raw is None:
        raw = os.environ.get("ADMIN_STAFF_AUDIT_RETENTION_DAYS", "90")
    try:
        n = int(raw)
        return max(1, min(n, 3650))
    except (TypeError, ValueError):
        return 90


def _column_exists(conn, table: str, column: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
            """,
            (table, column),
        )
        row = cur.fetchone()
        return bool(row and row[0])
    finally:
        cur.close()


def _ensure_extra_columns(conn) -> None:
    cur = conn.cursor()
    try:
        alters = [
            (
                "actor_contractor_id",
                "ADD COLUMN actor_contractor_id INT DEFAULT NULL AFTER actor_username",
            ),
            (
                "actor_channel",
                "ADD COLUMN actor_channel VARCHAR(32) NOT NULL DEFAULT 'admin_user' AFTER actor_contractor_id",
            ),
            (
                "inferred_permission",
                "ADD COLUMN inferred_permission VARCHAR(191) DEFAULT NULL AFTER actor_channel",
            ),
        ]
        for col, stmt in alters:
            if not _column_exists(conn, TABLE, col):
                cur.execute(f"ALTER TABLE {TABLE} {stmt}")
        conn.commit()
    finally:
        cur.close()

    cur = conn.cursor()
    try:
        cur.execute(
            f"SHOW INDEX FROM {TABLE} WHERE Key_name = 'idx_asa_actor_contractor'"
        )
        if not cur.fetchone():
            try:
                cur.execute(
                    f"CREATE INDEX idx_asa_actor_contractor ON {TABLE} (actor_contractor_id, created_at)"
                )
                conn.commit()
            except Exception:
                logger.debug("idx_asa_actor_contractor create skipped", exc_info=True)
    finally:
        cur.close()


def ensure_table(conn) -> None:
    global _table_verified
    if _table_verified:
        return
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
              id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              actor_user_id CHAR(36) DEFAULT NULL,
              actor_username VARCHAR(191) DEFAULT NULL,
              actor_contractor_id INT DEFAULT NULL,
              actor_channel VARCHAR(32) NOT NULL DEFAULT 'admin_user',
              inferred_permission VARCHAR(191) DEFAULT NULL,
              subject_contractor_id INT DEFAULT NULL,
              blueprint VARCHAR(128) DEFAULT NULL,
              endpoint VARCHAR(191) DEFAULT NULL,
              http_method VARCHAR(16) NOT NULL,
              path VARCHAR(512) NOT NULL,
              http_status INT NOT NULL,
              detail_json TEXT DEFAULT NULL,
              ip_address VARCHAR(45) DEFAULT NULL,
              KEY idx_asa_contractor_created (subject_contractor_id, created_at),
              KEY idx_asa_created (created_at),
              KEY idx_asa_actor_contractor (actor_contractor_id, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
    finally:
        cur.close()
    _ensure_extra_columns(conn)
    _table_verified = True


def _path_matches_prefix(path: str, prefixes: tuple[str, ...]) -> bool:
    return any(path.startswith(p) for p in prefixes)


def _matches_core_admin_audit(path: str, method: str) -> bool:
    if method not in MUTATING:
        return False
    if path in ("/core/settings", "/smtp-config", "/version/run-upgrades"):
        return True
    if path == "/users/add" or path.startswith("/users/edit/") or path.startswith("/users/delete/"):
        return True
    if path.startswith("/plugin/") and method == "POST":
        tail = path[len("/plugin/") :]
        if "/" not in tail:
            return False
        _sys_name, rest = tail.split("/", 1)
        if rest in ("install", "uninstall", "enable", "disable", "install-remote"):
            return True
        if rest == "settings" or rest.startswith("settings/"):
            return True
    return False


def _should_audit_admin_request(path: str, method: str) -> bool:
    if method not in MUTATING:
        return False
    if _path_matches_prefix(path, PLUGIN_AUDIT_PREFIXES):
        return True
    return _matches_core_admin_audit(path, method)


def _should_audit_contractor_request(path: str, method: str) -> bool:
    if method not in MUTATING:
        return False
    if not _path_matches_prefix(path, CONTRACTOR_AUDIT_PREFIXES):
        return False
    # Avoid logging password submission noise categorically? Still useful for audit.
    return True


def _extract_subject_from_path(path: str) -> int | None:
    for rx in (HR_CID_RE, TB_CID_RE, TB_TS_CONTRACTOR_RE):
        m = rx.search(path)
        if m:
            return int(m.group(1))
    for rx in GENERIC_CONTRACTOR_PATTERNS:
        m = rx.search(path)
        if m:
            return int(m.group(1))
    return None


def _subject_from_query() -> int | None:
    from flask import request

    for key in ("contractor_id", "user_id", "cid"):
        try:
            v = request.args.get(key, type=int)
            if v is not None:
                return int(v)
        except (TypeError, ValueError):
            continue
    return None


def _subject_from_request_and_response(path: str, response) -> int | None:
    cid = _extract_subject_from_path(path)
    if cid is not None:
        return cid
    q = _subject_from_query()
    if q is not None:
        return q
    loc = response.headers.get("Location") if response is not None else None
    if not loc:
        return None
    try:
        path_only = urlparse(loc).path or ""
    except Exception:
        path_only = str(loc).split("?")[0]
    m = HR_CID_RE.search(path_only)
    if m:
        return int(m.group(1))
    for rx in GENERIC_CONTRACTOR_PATTERNS:
        m = rx.search(path_only)
        if m:
            return int(m.group(1))
    return None


def _infer_permission(endpoint: str | None, path: str) -> str | None:
    if not endpoint:
        ep = ""
    else:
        ep = str(endpoint)
    low = ep.lower()
    path_l = path.lower()
    if low.startswith("internal_hr.") or "/plugin/hr_module/" in path_l:
        if "document" in low or "/documents" in path_l:
            return "hr_module.library"
        if "request" in low:
            return "hr_module.document_requests"
        if "employee" in low or "contractor" in low or "profile" in low or "/edit" in path_l:
            return "hr_module.edit_employees"
        return "hr_module.read"
    if low.startswith("internal_time_billing") or "/plugin/time_billing_module/" in path_l:
        return "time_billing_module.access"
    if "inventory" in low or "/inventory_control/" in path_l or path_l.startswith("/inventory/"):
        return "inventory_control.access"
    if "fleet" in low or "/fleet_management/" in path_l:
        return "fleet_management.access"
    if "asset_management" in low or "/asset_management/" in path_l:
        return "asset_management.access"
    if "recruitment" in low or "/recruitment_module/" in path_l:
        return "recruitment_module.access"
    if "scheduling" in low or "/scheduling_module/" in path_l:
        return "scheduling_module.access"
    if "ventus" in low or "medical_response" in low or "/ventus_response_module/" in path_l:
        return "ventus_response_module.access"
    if "medical_records" in low or "/medical_records_module/" in path_l:
        return "medical_records_module.access"
    if "training" in low or "/training_module/" in path_l:
        return "training_module.access"
    if "compliance" in low or "/compliance_module/" in path_l:
        return "compliance_module.access"
    if "website" in low or "/website_module/" in path_l:
        return "website_module.access"
    if "event_manager" in low or "/event_manager_module/" in path_l:
        return "event_manager_module.access"
    if "employee_portal" in low or "/employee_portal_module/" in path_l:
        return "employee_portal_module.access"
    if "work_module" in low or "/work_module/" in path_l:
        return "work_module.access"
    if "news_blog" in low or "/news_blog_module/" in path_l:
        return "news_blog_module.access"
    if low.startswith("public_employee_portal") or path_l.startswith("/employee-portal/"):
        return "employee_portal_module.access"
    if low.startswith("public_time_billing") or path_l.startswith("/time-billing/"):
        return "time_billing_module.access"
    if path_l.startswith("/users/"):
        return "core.manage_users"
    if path_l.startswith("/core/settings") or path_l.startswith("/smtp-config"):
        return "core.settings"
    return None


def _maybe_purge(conn, days: int) -> None:
    global _last_purge_ts
    now = time.monotonic()
    if now - _last_purge_ts < _PURGE_INTERVAL_SEC:
        return
    _last_purge_ts = now
    cur = conn.cursor()
    try:
        cur.execute(
            f"DELETE FROM {TABLE} WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
            (int(days),),
        )
        conn.commit()
    except Exception:
        logger.exception("admin_staff_audit: retention purge failed")
    finally:
        cur.close()


def _insert_row(
    conn,
    *,
    actor_user_id: str | None,
    actor_username: str | None,
    actor_contractor_id: int | None,
    actor_channel: str,
    inferred_permission: str | None,
    subject_contractor_id: int | None,
    blueprint: str | None,
    endpoint: str | None,
    http_method: str,
    path: str,
    http_status: int,
    detail: dict[str, Any] | None,
    ip: str | None,
) -> None:
    ensure_table(conn)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            INSERT INTO {TABLE}
            (actor_user_id, actor_username, actor_contractor_id, actor_channel, inferred_permission,
             subject_contractor_id, blueprint, endpoint, http_method, path, http_status, detail_json, ip_address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                actor_user_id,
                actor_username[:191] if actor_username else None,
                actor_contractor_id,
                actor_channel[:32],
                (inferred_permission[:191] if inferred_permission else None),
                subject_contractor_id,
                (blueprint[:128] if blueprint else None),
                (str(endpoint)[:191] if endpoint else None),
                http_method[:16],
                path[:512],
                int(http_status),
                json.dumps(detail, default=str)[:65000] if detail else None,
                ip,
            ),
        )
        conn.commit()
    finally:
        cur.close()


def fetch_logs_for_contractor(contractor_id: int, *, limit: int = 200) -> list[dict[str, Any]]:
    from app.objects import get_db_connection

    lim = max(1, min(int(limit), 500))
    conn = get_db_connection()
    try:
        ensure_table(conn)
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"""
                SELECT id, created_at, actor_user_id, actor_username, actor_contractor_id, actor_channel,
                       inferred_permission, subject_contractor_id, blueprint, endpoint, http_method, path,
                       http_status, detail_json, ip_address
                FROM {TABLE}
                WHERE subject_contractor_id = %s OR actor_contractor_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (int(contractor_id), int(contractor_id), lim),
            )
            return list(cur.fetchall() or [])
        finally:
            cur.close()
    finally:
        conn.close()


def _session_contractor_id() -> int | None:
    from flask import has_request_context, session

    if not has_request_context():
        return None
    from app.portal_session import contractor_id_from_tb_user

    return contractor_id_from_tb_user(session.get("tb_user"))


def after_request_record(response):
    from flask import has_request_context, request
    from flask_login import current_user

    if not has_request_context() or response is None:
        return response
    method = request.method or ""
    if method not in MUTATING:
        return response
    path = request.path or ""

    admin_ok = (
        getattr(current_user, "is_authenticated", False)
        and _should_audit_admin_request(path, method)
    )
    cid_actor = _session_contractor_id()
    contractor_ok = cid_actor is not None and _should_audit_contractor_request(path, method)

    if not admin_ok and not contractor_ok:
        return response
    if admin_ok and getattr(current_user, "is_authenticated", False):
        contractor_ok = False

    try:
        from flask import current_app

        days = retention_days(current_app)
        endpoint = getattr(request, "endpoint", None)
        blueprint = None
        if endpoint and "." in str(endpoint):
            blueprint = str(endpoint).rsplit(".", 1)[0]

        inferred = _infer_permission(endpoint, path)
        detail: dict[str, Any] = {}
        if endpoint:
            detail["endpoint"] = endpoint
        if inferred:
            detail["inferred_permission"] = inferred

        ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()
        if len(ip) > 45:
            ip = ip[:45]

        from app.objects import get_db_connection

        conn = get_db_connection()
        try:
            ensure_table(conn)
            _maybe_purge(conn, days)

            if admin_ok:
                actor_id = getattr(current_user, "id", None)
                actor_id_s = str(actor_id) if actor_id is not None else None
                if actor_id_s and len(actor_id_s) > 36:
                    actor_id_s = actor_id_s[:36]
                actor_username = getattr(current_user, "username", None) or ""
                subject_cid = _subject_from_request_and_response(path, response)
                _insert_row(
                    conn,
                    actor_user_id=actor_id_s,
                    actor_username=actor_username,
                    actor_contractor_id=None,
                    actor_channel=CHANNEL_ADMIN,
                    inferred_permission=inferred,
                    subject_contractor_id=subject_cid,
                    blueprint=blueprint,
                    endpoint=endpoint if endpoint else None,
                    http_method=method,
                    path=path,
                    http_status=int(response.status_code),
                    detail=detail or None,
                    ip=ip or None,
                )
            elif contractor_ok and cid_actor is not None:
                subject_cid = _subject_from_request_and_response(path, response)
                if subject_cid is None:
                    subject_cid = cid_actor
                from flask import session as _sess

                from app.portal_session import normalize_tb_user

                tu = normalize_tb_user(_sess.get("tb_user"))
                uname = ""
                actor_user_linked = None
                if isinstance(tu, dict):
                    uname = str(tu.get("email") or tu.get("name") or "").strip()
                    lid = tu.get("linked_user_id")
                    if lid:
                        actor_user_linked = str(lid)[:36]
                _insert_row(
                    conn,
                    actor_user_id=actor_user_linked,
                    actor_username=(uname[:191] if uname else f"contractor:{cid_actor}"),
                    actor_contractor_id=cid_actor,
                    actor_channel=CHANNEL_CONTRACTOR,
                    inferred_permission=inferred,
                    subject_contractor_id=subject_cid,
                    blueprint=blueprint,
                    endpoint=endpoint if endpoint else None,
                    http_method=method,
                    path=path,
                    http_status=int(response.status_code),
                    detail=detail or None,
                    ip=ip or None,
                )
        finally:
            conn.close()
    except Exception:
        logger.debug("admin_staff_audit: record failed", exc_info=True)

    return response
