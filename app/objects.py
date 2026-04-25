from pathlib import Path
from flask_login import current_user
import os
import json
import importlib
import requests
import shutil
import zipfile
import subprocess
import sys
from datetime import datetime, timedelta
from packaging.version import parse
from urllib.parse import urlparse, quote
from apscheduler.schedulers.background import BackgroundScheduler
from typing import List, Dict, Any, Optional, Set
import time
import hashlib
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import os
import mysql.connector
from mysql.connector import errors as mysql_errors
import bcrypt
from flask_login import UserMixin

from functools import wraps
from flask import flash, redirect, url_for

_logger = logging.getLogger(__name__)

# Official Sparrow registry — used when env vars are unset so deployments work without .env.
# Override with GITLAB_TOKEN / GITLAB_USERNAME / GITLAB_PASSWORD / CORE_PROJECT_ID (or GITLAB_PROJECT_ID).
_OFFICIAL_REGISTRY_CORE_PROJECT_ID = "65546585"
_OFFICIAL_REGISTRY_GITLAB_TOKEN = "gldt-QqB_xpu896k88KP84rzW"
_OFFICIAL_REGISTRY_GITLAB_USERNAME = "gitlab+deploy-token-9123788"
_OFFICIAL_REGISTRY_GITLAB_PASSWORD = "gldt-QqB_xpu896k88KP84rzW"


def effective_gitlab_read_token() -> str:
    """
    GitLab API token for official registry (manifests, update zips).
    Uses ``GITLAB_TOKEN`` first; if unset, ``SPARROW_OFFICIAL_REGISTRY_TOKEN``;
    if still unset, built-in official-registry read token for controlled deployments.
    """
    return (
        (os.environ.get("GITLAB_TOKEN") or "").strip()
        or (os.environ.get("SPARROW_OFFICIAL_REGISTRY_TOKEN") or "").strip()
        or _OFFICIAL_REGISTRY_GITLAB_TOKEN
    )


def effective_gitlab_basic_credentials() -> tuple:
    """(username, password) for GitLab HTTP basic auth; env first, then official-registry defaults."""
    user = (os.environ.get("GITLAB_USERNAME") or "").strip(
    ) or _OFFICIAL_REGISTRY_GITLAB_USERNAME
    password = (os.environ.get("GITLAB_PASSWORD")
                or "").strip() or _OFFICIAL_REGISTRY_GITLAB_PASSWORD
    return (user, password)


def effective_core_project_id() -> str:
    """GitLab project id for core/registry API; env first, then official default."""
    return (
        (os.environ.get("GITLAB_PROJECT_ID")
         or os.environ.get("CORE_PROJECT_ID") or "").strip()
        or _OFFICIAL_REGISTRY_CORE_PROJECT_ID
    )


class DatabaseTemporarilyUnavailable(Exception):
    """
    Raised when MySQL remains unreachable after connect retries (e.g. Railway
    cold start). Handled in Flask to show a friendly page instead of a traceback.
    """


def _mysql_connection_kwargs():
    """Keyword args shared by direct connect and connection pool."""
    kwargs = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', 'rootpassword'),
        'database': os.getenv('DB_NAME', 'sparrow_erp'),
    }
    timeout_raw = os.getenv('DB_CONNECTION_TIMEOUT_SEC')
    if timeout_raw:
        try:
            kwargs['connection_timeout'] = int(timeout_raw)
        except ValueError:
            pass
    return kwargs


def _is_transient_connect_error(exc):
    """
    True when the failure is likely a cold start / network race (e.g. Railway
    services still waking). MySQL client errno 2003 = can't reach server.
    """
    if isinstance(exc, mysql_errors.InterfaceError):
        return True
    errno = getattr(exc, 'errno', None)
    if errno == 2003:
        return True
    msg = str(exc).lower()
    if '2003' in msg or "can't connect" in msg or 'connection refused' in msg:
        return True
    return False


def _connect_mysql_with_retries(connect_callable):
    """
    Retry connect_callable() when the DB is temporarily unreachable.
    Tunable via DB_CONNECT_MAX_ATTEMPTS (default 8) and
    DB_CONNECT_RETRY_DELAY_SEC (default 0.75 base; scaled per attempt).
    Set DB_CONNECT_MAX_ATTEMPTS=1 to disable retries.
    """
    try:
        max_attempts = max(1, int(os.getenv('DB_CONNECT_MAX_ATTEMPTS', '8')))
    except ValueError:
        max_attempts = 8
    try:
        base_delay = float(os.getenv('DB_CONNECT_RETRY_DELAY_SEC', '0.75'))
    except ValueError:
        base_delay = 0.75

    for attempt in range(max_attempts):
        try:
            return connect_callable()
        except mysql.connector.Error as e:
            if not _is_transient_connect_error(e):
                raise
            if attempt >= max_attempts - 1:
                raise DatabaseTemporarilyUnavailable(
                    "The database could not be reached after several attempts. "
                    "It may still be starting; please try again in a moment."
                ) from e
            time.sleep(base_delay * (attempt + 1))


def get_db_connection():
    """
    Establish and return a new database connection using environment variables.
    """
    # Support optional connection pooling via mysql.connector.pooling.MySQLConnectionPool
    # Set environment variable DB_POOL_SIZE to enable pooling (e.g., 5)
    try:
        pool_size_raw = os.getenv('DB_POOL_SIZE')
        if pool_size_raw and int(pool_size_raw) > 0:
            # lazy-create a module-level pool
            if not hasattr(get_db_connection, '_pool') or get_db_connection._pool is None:
                try:
                    from mysql.connector import pooling
                    base_kw = _mysql_connection_kwargs()
                    pool_cfg = {
                        'pool_name': 'sparrow_pool',
                        'pool_size': int(pool_size_raw),
                        **base_kw,
                    }

                    def _make_pool():
                        return pooling.MySQLConnectionPool(**pool_cfg)

                    get_db_connection._pool = _connect_mysql_with_retries(
                        _make_pool)
                except DatabaseTemporarilyUnavailable:
                    raise
                except Exception:
                    # If pool creation fails, fallback to direct connections
                    get_db_connection._pool = None
            if getattr(get_db_connection, '_pool', None):
                return _connect_mysql_with_retries(
                    get_db_connection._pool.get_connection)
    except DatabaseTemporarilyUnavailable:
        raise
    except Exception:
        # Any other error using pooling should fall back to direct connect
        pass

    def _direct():
        return mysql.connector.connect(**_mysql_connection_kwargs())

    return _connect_mysql_with_retries(_direct)


def mysql_connect_with_retry(*, include_database: bool = True, **overrides):
    """
    ``mysql.connector.connect`` with the same ``DB_CONNECT_*`` retry policy as
    ``get_db_connection()``. Use ``include_database=False`` for a server-level
    connection (e.g. ``CREATE DATABASE``). Overrides are merged after env defaults.
    """
    kw = _mysql_connection_kwargs()
    if not include_database:
        kw.pop("database", None)
    kw.update(overrides)
    return _connect_mysql_with_retries(lambda: mysql.connector.connect(**kw))


def get_contractor_effective_role(contractor_id: int) -> str:
    """Resolve contractor role from tb_contractor_roles and role_id (same rules as employee portal)."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT LOWER(TRIM(r.name)) AS name FROM tb_contractor_roles cr
            JOIN roles r ON r.id = cr.role_id WHERE cr.contractor_id = %s
            """,
            (int(contractor_id),),
        )
        names = [row[0] for row in (cur.fetchall() or []) if row and row[0]]
        if "superuser" in names:
            return "superuser"
        if "admin" in names:
            return "admin"
        if names:
            return names[0] or "staff"
        cur.execute(
            """
            SELECT LOWER(TRIM(r.name)) FROM tb_contractors c
            LEFT JOIN roles r ON r.id = c.role_id WHERE c.id = %s
            """,
            (int(contractor_id),),
        )
        row = cur.fetchone()
        return (row[0] or "staff") if row else "staff"
    finally:
        cur.close()
        conn.close()


def find_tb_contractors_for_api_login(login_key: str):
    """
    Contractor API login: match ``tb_contractors.username`` (case-insensitive); if none,
    match ``email`` (case-insensitive) so MDT/Cura clients can use the same sign-in as the portal.

    Returns 0–2 dict rows (caller treats >1 as ambiguous). If ``username`` column is missing,
    falls back to email-only lookup.
    """
    if not login_key:
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        legacy_no_username_column = False
        rows = []
        try:
            cur.execute(
                """
                SELECT id, email, username, name, status, password_hash
                FROM tb_contractors
                WHERE username IS NOT NULL AND TRIM(username) <> ''
                  AND LOWER(TRIM(username)) = %s
                LIMIT 2
                """,
                (login_key,),
            )
            rows = cur.fetchall() or []
        except mysql.connector.Error as exc:
            code = getattr(exc, "errno", None)
            msg = str(exc).lower()
            if code == 1054 and "username" in msg:
                legacy_no_username_column = True
            elif code in (1146, 1054):
                return []
            else:
                raise

        if not legacy_no_username_column:
            if len(rows) >= 2:
                return rows[:2]
            if len(rows) == 1:
                return rows

        try:
            if legacy_no_username_column:
                cur.execute(
                    """
                    SELECT id, email, name, status, password_hash
                    FROM tb_contractors
                    WHERE email IS NOT NULL AND TRIM(email) <> ''
                      AND LOWER(TRIM(email)) = %s
                    LIMIT 2
                    """,
                    (login_key,),
                )
                legacy_rows = cur.fetchall() or []
                for r in legacy_rows:
                    r["username"] = None
                return legacy_rows

            cur.execute(
                """
                SELECT id, email, username, name, status, password_hash
                FROM tb_contractors
                WHERE email IS NOT NULL AND TRIM(email) <> ''
                  AND LOWER(TRIM(email)) = %s
                LIMIT 2
                """,
                (login_key,),
            )
            return cur.fetchall() or []
        except mysql.connector.Error as exc:
            if getattr(exc, "errno", None) in (1146, 1054):
                return []
            raise
    finally:
        cur.close()
        conn.close()


def linked_user_contractor_pair(user_data: dict | None, contractor_row: dict | None) -> bool:
    """
    True when a Sparrow ``users`` row and a ``tb_contractors`` row dict are the same person
    (mirrored login): ``users.contractor_id`` points at the contractor, or both have the same
    non-empty email. Used to avoid rejecting API login with 409 when both match the same login key.
    """
    if not user_data or not contractor_row:
        return False
    uid_c = user_data.get("contractor_id")
    if uid_c is not None and str(uid_c).strip() != "":
        try:
            if int(uid_c) == int(contractor_row.get("id")):
                return True
        except (TypeError, ValueError):
            pass
    ue = (user_data.get("email") or "").strip().lower()
    ce = (contractor_row.get("email") or "").strip().lower()
    return bool(ue and ce and ue == ce)


def find_tb_contractors_for_portal_login(login_key: str):
    """
    Portal login: match contractor ``username`` (if set) or full ``email`` (case-insensitive).
    Includes columns needed for session setup (initials, profile picture when present).
    """
    if not login_key:
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, email, username, name, initials, status, password_hash,
                   profile_picture_path
            FROM tb_contractors
            WHERE (
                (username IS NOT NULL AND TRIM(username) <> ''
                 AND LOWER(TRIM(username)) = %s)
                OR LOWER(TRIM(email)) = %s
            )
            LIMIT 2
            """,
            (login_key, login_key),
        )
        return cur.fetchall() or []
    except mysql.connector.Error as e:
        if getattr(e, "errno", None) in (1146, 1054):
            return []
        raise
    finally:
        cur.close()
        conn.close()


def find_sparrow_users_for_portal_login(login_key: str):
    """
    Match core ``users`` by case-insensitive username or email (portal parity with contractor login).
    Returns 0–3 rows; callers treat len != 1 as unusable for authentication.
    """
    if not login_key:
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, email, username, password_hash, billable_exempt, first_name, last_name,
                   contractor_id
            FROM users
            WHERE LOWER(TRIM(username)) = %s OR LOWER(TRIM(email)) = %s
            LIMIT 3
            """,
            (login_key, login_key),
        )
        return cur.fetchall() or []
    except mysql.connector.Error as e:
        if getattr(e, "errno", None) == 1054:
            cur.close()
            conn.close()
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            cur.execute(
                """
                SELECT id, email, username, password_hash, billable_exempt, first_name, last_name
                FROM users
                WHERE LOWER(TRIM(username)) = %s OR LOWER(TRIM(email)) = %s
                LIMIT 3
                """,
                (login_key, login_key),
            )
            rows = cur.fetchall() or []
            for r in rows:
                r["contractor_id"] = None
            return rows
        raise
    finally:
        cur.close()
        conn.close()


def get_tb_contractor_portal_row(contractor_id: int):
    """Single contractor row with columns needed for employee portal session (same shape as portal login)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, email, username, name, initials, status, password_hash,
                   profile_picture_path
            FROM tb_contractors
            WHERE id = %s
            LIMIT 1
            """,
            (int(contractor_id),),
        )
        return cur.fetchone()
    except mysql.connector.Error as e:
        if getattr(e, "errno", None) in (1146, 1054):
            return None
        raise
    finally:
        cur.close()
        conn.close()


def resolve_or_link_contractor_for_portal_user(user_row: dict):
    """
    PRD §6.3: from an authenticated core user row, return portal contractor dict or (None, error_message).
    Creates a minimal stub when no contractor exists for the user's email; fails closed on ambiguous email.
    """
    uid = user_row.get("id")
    email = (user_row.get("email") or "").strip()
    if not uid or not email or "@" not in email:
        return None, "Your account is missing a valid email. Contact an administrator."

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    def _active_ok(row) -> bool:
        if not row:
            return False
        return str(row.get("status", "")).lower() in (
            "active", "1", "true", "yes",
        )

    def _fetch_portal_row(cid: int):
        cur.execute(
            """
            SELECT id, email, username, name, initials, status, password_hash,
                   profile_picture_path
            FROM tb_contractors
            WHERE id = %s
            LIMIT 1
            """,
            (int(cid),),
        )
        return cur.fetchone()

    try:
        conn.autocommit = False
        cur.execute(
            """
            SELECT id, email, contractor_id, password_hash, username
            FROM users WHERE id = %s FOR UPDATE
            """,
            (uid,),
        )
        fresh = cur.fetchone()
        if not fresh:
            conn.rollback()
            return None, "Account not found."

        linked = fresh.get("contractor_id")
        user_hash = fresh.get("password_hash") or user_row.get("password_hash")

        if linked is not None:
            crow = _fetch_portal_row(int(linked))
            if not crow:
                cur.execute(
                    "UPDATE users SET contractor_id = NULL WHERE id = %s", (uid,)
                )
                conn.commit()
            elif not _active_ok(crow):
                conn.rollback()
                return None, (
                    "Your account is inactive. Please contact an administrator."
                )
            else:
                uname = (fresh.get("username") or user_row.get("username") or "").strip()
                _maybe_align_contractor_username_with_core_user(
                    cur, int(linked), uid, uname,
                )
                conn.commit()
                crow = _fetch_portal_row(int(linked))
                return crow, None

        norm = email.strip().lower()
        cur.execute(
            """
            SELECT id FROM tb_contractors
            WHERE LOWER(TRIM(email)) = %s
            ORDER BY id ASC
            LIMIT 3
            """,
            (norm,),
        )
        em_rows = cur.fetchall() or []
        c_ids = [r["id"] for r in em_rows if r and r.get("id") is not None]

        if len(c_ids) >= 2:
            conn.rollback()
            return None, (
                "Multiple employee records share your email. Ask an administrator "
                "to resolve this before using the portal."
            )

        if len(c_ids) == 1:
            cid = int(c_ids[0])
            crow = _fetch_portal_row(cid)
            if not crow or not _active_ok(crow):
                conn.rollback()
                return None, (
                    "Your account is inactive. Please contact an administrator."
                )
            uname = (fresh.get("username") or user_row.get("username") or "").strip()
            _maybe_align_contractor_username_with_core_user(cur, cid, uid, uname)
            cur.execute(
                "UPDATE users SET contractor_id = %s WHERE id = %s",
                (cid, uid),
            )
            conn.commit()
            crow = _fetch_portal_row(cid)
            return crow, None

        from app.plugins.time_billing_module.routes import (
            contractor_username_prefer_core_user,
        )

        fn = (user_row.get("first_name") or "").strip()
        ln = (user_row.get("last_name") or "").strip()
        display_name = (f"{fn} {ln}").strip() or norm

        username_un, ualloc_err = contractor_username_prefer_core_user(
            conn,
            core_user_id=uid,
            core_username=fresh.get("username") or user_row.get("username"),
            full_name=display_name,
            email=norm,
            exclude_contractor_id=None,
        )
        if ualloc_err or not username_un:
            conn.rollback()
            return None, ualloc_err or "Could not create portal employee record."

        cur.execute(
            "SELECT id FROM roles WHERE LOWER(TRIM(name)) = %s LIMIT 1",
            ("staff",),
        )
        role_row = cur.fetchone()
        if role_row and role_row.get("id") is not None:
            role_id = int(role_row["id"])
        else:
            cur.execute("INSERT INTO roles (name) VALUES (%s)", ("staff",))
            role_id = int(cur.lastrowid)

        pwd_store = user_hash or ""
        new_cid = None
        try:
            cur.execute(
                """
                INSERT INTO tb_contractors (
                    email, username, name, status, password_hash, role_id, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    norm,
                    username_un,
                    display_name[:255] if display_name else norm,
                    "active",
                    pwd_store,
                    role_id,
                ),
            )
            new_cid = cur.lastrowid
        except mysql.connector.Error as e:
            if getattr(e, "errno", None) == 1062:
                conn.rollback()
                cur.execute(
                    """
                    SELECT id FROM tb_contractors
                    WHERE LOWER(TRIM(email)) = %s
                    ORDER BY id ASC
                    LIMIT 2
                    """,
                    (norm,),
                )
                dup_rows = cur.fetchall() or []
                if len(dup_rows) == 1:
                    cid = int(dup_rows[0]["id"])
                    crow = _fetch_portal_row(cid)
                    if crow and _active_ok(crow):
                        uname = (
                            (fresh.get("username") or user_row.get("username") or "")
                            .strip()
                        )
                        _maybe_align_contractor_username_with_core_user(
                            cur, cid, uid, uname,
                        )
                        cur.execute(
                            "UPDATE users SET contractor_id = %s WHERE id = %s",
                            (cid, uid),
                        )
                        conn.commit()
                        crow = _fetch_portal_row(cid)
                        return crow, None
                conn.rollback()
                return None, (
                    "Could not create your portal employee record. "
                    "Please try again or contact an administrator."
                )
            raise

        try:
            cur.execute(
                """
                INSERT IGNORE INTO tb_contractor_roles (contractor_id, role_id)
                VALUES (%s, %s)
                """,
                (int(new_cid), int(role_id)),
            )
        except mysql.connector.Error:
            pass

        cur.execute(
            "UPDATE users SET contractor_id = %s WHERE id = %s",
            (int(new_cid), uid),
        )
        conn.commit()

        crow = _fetch_portal_row(int(new_cid))
        return crow, None

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.autocommit = True
        except Exception:
            pass
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _maybe_align_contractor_username_with_core_user(
    cur, contractor_id: int, core_user_id, core_username,
) -> None:
    """
    Set ``tb_contractors.username`` to the core user's login name when valid and not
    taken by another contractor (keeps portal sign-in aligned with admin username).
    """
    trial = (core_username or "").strip().lower()
    if not trial or len(trial) < 3:
        return
    from app.plugins.time_billing_module.routes import _CONTRACTOR_USERNAME_RE

    if not _CONTRACTOR_USERNAME_RE.match(trial):
        return
    umatch = User.get_user_by_username_ci(trial)
    if umatch is not None and str(umatch.get("id")) != str(core_user_id):
        return
    try:
        cur.execute(
            """
            SELECT 1 FROM tb_contractors
            WHERE LOWER(TRIM(username)) = %s AND id <> %s LIMIT 1
            """,
            (trial, int(contractor_id)),
        )
        if cur.fetchone():
            return
        cur.execute(
            "UPDATE tb_contractors SET username = %s WHERE id = %s",
            (trial, int(contractor_id)),
        )
    except mysql.connector.Error:
        pass


def backfill_users_contractor_link_from_contractor_email(
    contractor_id: int, contractor_email: str,
) -> None:
    """PRD §6.3: optional back-link when portal login is contractor-direct."""
    ce = (contractor_email or "").strip()
    if not ce or "@" not in ce:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, username FROM users
            WHERE contractor_id IS NULL
              AND LOWER(TRIM(email)) = LOWER(TRIM(%s))
            LIMIT 1
            """,
            (ce,),
        )
        urow = cur.fetchone()
        if not urow:
            return
        uid, uname = urow[0], urow[1]
        cur.execute(
            "UPDATE users SET contractor_id = %s WHERE id = %s",
            (int(contractor_id), uid),
        )
        _maybe_align_contractor_username_with_core_user(
            cur, int(contractor_id), uid, uname,
        )
        conn.commit()
    except mysql.connector.Error:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()


def sync_core_user_to_portal_contractor(user_id: str) -> None:
    """
    Ensure a core (non-vendor) user has a matching ``tb_contractors`` row and ``users.contractor_id``:
    mirrors email, username (portal rules), password hash, display name, and active status so they can
    use the employee portal with the same credentials as the admin app.

    Safe to call after any admin create/update of the user row. Logs warnings on conflicts; does not raise.
    """
    if not user_id:
        return
    conn = get_db_connection()
    dcur = conn.cursor(dictionary=True)
    xcur = conn.cursor()
    try:
        dcur.execute("SHOW TABLES LIKE %s", ("tb_contractors",))
        if not dcur.fetchone():
            return
        try:
            dcur.execute(
                """
                SELECT id, email, username, password_hash, first_name, last_name, contractor_id,
                       COALESCE(billable_exempt,0) AS billable_exempt
                FROM users WHERE id = %s LIMIT 1
                """,
                (user_id,),
            )
        except mysql.connector.Error as e:
            if getattr(e, "errno", None) == 1054:
                return
            raise
        u = dcur.fetchone()
        if not u:
            return
        if int(u.get("billable_exempt") or 0):
            return
        email = (u.get("email") or "").strip()
        if not email or "@" not in email:
            _logger.warning(
                "sync_core_user_to_portal_contractor: user %s has no usable email; skip",
                user_id,
            )
            return
        pwd = (u.get("password_hash") or "").strip()
        if not pwd:
            _logger.warning(
                "sync_core_user_to_portal_contractor: user %s has no password hash; skip",
                user_id,
            )
            return

        norm = email.lower()
        fn = (u.get("first_name") or "").strip()
        ln = (u.get("last_name") or "").strip()
        display = (f"{fn} {ln}").strip() or norm
        uname = (u.get("username") or "").strip()
        uid = u["id"]

        conn.autocommit = False
        cid = u.get("contractor_id")
        if cid is not None:
            dcur.execute(
                "SELECT id FROM tb_contractors WHERE id = %s LIMIT 1",
                (int(cid),),
            )
            if not dcur.fetchone():
                xcur.execute(
                    "UPDATE users SET contractor_id = NULL WHERE id = %s", (uid,)
                )
                conn.commit()
                cid = None

        if cid is not None:
            cid_int = int(cid)
            try:
                xcur.execute(
                    """
                    UPDATE tb_contractors
                    SET email = %s, name = %s, password_hash = %s, status = 'active'
                    WHERE id = %s
                    """,
                    (norm, display[:255], pwd, cid_int),
                )
            except mysql.connector.Error as e:
                if getattr(e, "errno", None) == 1062:
                    conn.rollback()
                    xcur.execute(
                        """
                        UPDATE tb_contractors
                        SET name = %s, password_hash = %s, status = 'active'
                        WHERE id = %s
                        """,
                        (display[:255], pwd, cid_int),
                    )
                else:
                    raise
            _maybe_align_contractor_username_with_core_user(
                xcur, cid_int, uid, uname,
            )
            conn.commit()
            return

        dcur.execute(
            """
            SELECT id FROM tb_contractors
            WHERE LOWER(TRIM(email)) = %s
            ORDER BY id ASC
            LIMIT 3
            """,
            (norm,),
        )
        em_rows = dcur.fetchall() or []
        c_ids = [r["id"] for r in em_rows if r and r.get("id") is not None]

        if len(c_ids) >= 2:
            _logger.warning(
                "sync_core_user_to_portal_contractor: ambiguous tb_contractors email=%s user=%s",
                norm,
                user_id,
            )
            return

        if len(c_ids) == 1:
            link_cid = int(c_ids[0])
            xcur.execute(
                "SELECT id FROM users WHERE contractor_id = %s AND id <> %s LIMIT 1",
                (link_cid, uid),
            )
            if xcur.fetchone():
                _logger.warning(
                    "sync_core_user_to_portal_contractor: contractor %s already "
                    "linked to another user; skip link for %s",
                    link_cid,
                    user_id,
                )
                return
            try:
                xcur.execute(
                    """
                    UPDATE tb_contractors
                    SET email = %s, name = %s, password_hash = %s, status = 'active'
                    WHERE id = %s
                    """,
                    (norm, display[:255], pwd, link_cid),
                )
            except mysql.connector.Error as e:
                if getattr(e, "errno", None) == 1062:
                    conn.rollback()
                    xcur.execute(
                        """
                        UPDATE tb_contractors
                        SET name = %s, password_hash = %s, status = 'active'
                        WHERE id = %s
                        """,
                        (display[:255], pwd, link_cid),
                    )
                else:
                    raise
            _maybe_align_contractor_username_with_core_user(
                xcur, link_cid, uid, uname,
            )
            try:
                xcur.execute(
                    "UPDATE users SET contractor_id = %s WHERE id = %s",
                    (link_cid, uid),
                )
                conn.commit()
            except mysql.connector.Error as le:
                conn.rollback()
                if getattr(le, "errno", None) == 1062:
                    _logger.warning(
                        "sync_core_user_to_portal_contractor: cannot assign contractor %s "
                        "to user %s (link in use)",
                        link_cid,
                        user_id,
                    )
                else:
                    raise
            return

        from app.plugins.time_billing_module.routes import (
            contractor_username_prefer_core_user,
        )

        username_un, ualloc_err = contractor_username_prefer_core_user(
            conn,
            core_user_id=uid,
            core_username=uname,
            full_name=display,
            email=norm,
            exclude_contractor_id=None,
        )
        if ualloc_err or not username_un:
            conn.rollback()
            _logger.warning(
                "sync_core_user_to_portal_contractor: could not allocate username for user %s: %s",
                user_id,
                ualloc_err,
            )
            return

        dcur.execute(
            "SELECT id FROM roles WHERE LOWER(TRIM(name)) = %s LIMIT 1",
            ("staff",),
        )
        role_row = dcur.fetchone()
        if role_row and role_row.get("id") is not None:
            role_id = int(role_row["id"])
        else:
            xcur.execute("INSERT INTO roles (name) VALUES (%s)", ("staff",))
            role_id = int(xcur.lastrowid)

        xcur.execute(
            """
            INSERT INTO tb_contractors (
                email, username, name, status, password_hash, role_id, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """,
            (norm, username_un, display[:255], "active", pwd, role_id),
        )
        new_cid = int(xcur.lastrowid)
        try:
            xcur.execute(
                """
                INSERT IGNORE INTO tb_contractor_roles (contractor_id, role_id)
                VALUES (%s, %s)
                """,
                (new_cid, role_id),
            )
        except mysql.connector.Error:
            pass
        try:
            xcur.execute(
                "UPDATE users SET contractor_id = %s WHERE id = %s",
                (new_cid, uid),
            )
            conn.commit()
        except mysql.connector.Error as le:
            conn.rollback()
            if getattr(le, "errno", None) == 1062:
                _logger.warning(
                    "sync_core_user_to_portal_contractor: cannot assign new contractor %s "
                    "to user %s",
                    new_cid,
                    user_id,
                )
            else:
                raise
    except mysql.connector.Error as e:
        _logger.warning("sync_core_user_to_portal_contractor: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    except Exception as e:
        _logger.warning("sync_core_user_to_portal_contractor: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.autocommit = True
        except Exception:
            pass
        try:
            dcur.close()
        except Exception:
            pass
        try:
            xcur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def sync_linked_tb_contractor_password_from_user(user_id, new_hash: str) -> None:
    """PRD §7.4: keep ``tb_contractors.password_hash`` in sync when a linked user password changes."""
    if not new_hash or not user_id:
        return
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT email, contractor_id FROM users WHERE id = %s LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return
        cid = row.get("contractor_id")
        email = (row.get("email") or "").strip()
        cur2 = conn.cursor()
        try:
            if cid:
                cur2.execute(
                    "UPDATE tb_contractors SET password_hash = %s WHERE id = %s",
                    (new_hash, int(cid)),
                )
            elif email:
                cur2.execute(
                    """
                    UPDATE tb_contractors
                    SET password_hash = %s
                    WHERE LOWER(TRIM(email)) = LOWER(TRIM(%s))
                    """,
                    (new_hash, email),
                )
            conn.commit()
        finally:
            cur2.close()
    finally:
        cur.close()
        conn.close()


def sync_linked_users_password_from_contractor(contractor_id: int, new_hash: str) -> None:
    """PRD §7.4: when portal/contractor password is set, update matching core ``users`` rows."""
    if not new_hash:
        return
    cid = int(contractor_id)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT email FROM tb_contractors WHERE id = %s LIMIT 1",
            (cid,),
        )
        cr = cur.fetchone()
        em = (cr.get("email") or "").strip() if cr else ""
        cur2 = conn.cursor()
        try:
            cur2.execute(
                "UPDATE users SET password_hash = %s WHERE contractor_id = %s",
                (new_hash, cid),
            )
            if em and "@" in em:
                cur2.execute(
                    """
                    UPDATE users SET password_hash = %s
                    WHERE LOWER(TRIM(email)) = LOWER(TRIM(%s))
                      AND (contractor_id IS NULL OR contractor_id = %s)
                    """,
                    (new_hash, em, cid),
                )
            conn.commit()
        finally:
            cur2.close()
    except mysql.connector.Error:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()


def run_module_script(module_path, action):
    """
    Calls install.py in the given module directory with the specified action.
    :param module_path: Path to the module directory (e.g., plugins/myplugin)
    :param action: 'install', 'upgrade', or 'uninstall'
    """
    import subprocess
    import sys
    import os
    script_path = os.path.join(module_path, "install.py")
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"install.py not found in {module_path}")
    try:
        result = subprocess.run(
            [sys.executable, script_path, action],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"[INFO] {action.capitalize()} completed for {module_path}:")
        print(result.stdout)
        if result.stderr:
            print("[STDERR]", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] {action.capitalize()} failed for {module_path}: {e}")
        if e.stdout:
            print("[STDOUT]", e.stdout)
        if e.stderr:
            print("[STDERR]", e.stderr)
        raise


class User(UserMixin):
    def __init__(self, id, username, email, role, permissions=None, personal_pin_hash=None):
        self.id = id
        self.username = username
        self.email = email
        self.role = role
        self.permissions = permissions or []
        self.personal_pin_hash = personal_pin_hash

    def get_id(self):
        return self.id

    @staticmethod
    def get_user_by_username_raw(username):
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()
        return user_data

    @staticmethod
    def get_user_by_username_ci(username_normalized: str):
        """Case-insensitive username match; ``username_normalized`` should be lowercased already."""
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM users WHERE LOWER(TRIM(username)) = %s LIMIT 1",
            (username_normalized,),
        )
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()
        return user_data

    @staticmethod
    def get_user_for_web_login(login_key: str):
        """
        Staff ``/login`` form: match a single ``users`` row by case-insensitive **username** or **email**.

        Used so HR-promoted portal staff (who are told to use their work email) can sign in without
        remembering the generated ``users.username``. Rejects ambiguous matches (two rows).
        """
        raw = (login_key or "").strip()
        if not raw:
            return None
        kl = raw.lower()
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                """
                SELECT * FROM users
                WHERE LOWER(TRIM(username)) = %s OR LOWER(TRIM(email)) = %s
                LIMIT 2
                """,
                (kl, kl),
            )
            rows = cursor.fetchall() or []
        finally:
            cursor.close()
            conn.close()
        if len(rows) != 1:
            return None
        return rows[0]

    @staticmethod
    def get_user_by_id(user_id):
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()
        if user_data:
            permissions = []
            if user_data.get('permissions'):
                try:
                    permissions = json.loads(user_data['permissions'])
                except Exception:
                    permissions = []
            # Ensure that if the personal_pin_hash is empty, we convert it to None.
            personal_pin_hash = user_data.get('personal_pin_hash')
            if personal_pin_hash is not None and personal_pin_hash.strip() == "":
                personal_pin_hash = None

            return User(
                user_data['id'],
                user_data['username'],
                user_data['email'],
                user_data['role'],
                permissions,
                personal_pin_hash=personal_pin_hash
            )
        return None

    @staticmethod
    def get_user_by_email(email):
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()
        return user_data

    @staticmethod
    def update_password(user_id, new_hash):
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET password_hash = %s WHERE id = %s", (new_hash, user_id))
        conn.commit()
        cursor.close()
        conn.close()
        sync_core_user_to_portal_contractor(user_id)

    @staticmethod
    def update_permissions(user_id, permissions_list):
        conn = get_db_connection()
        cursor = conn.cursor()
        json_permissions = json.dumps(permissions_list)
        cursor.execute(
            "UPDATE users SET permissions = %s WHERE id = %s", (json_permissions, user_id))
        conn.commit()
        cursor.close()
        conn.close()


class AuthManager:
    @staticmethod
    def hash_password(password):
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

    @staticmethod
    def verify_password(stored_password, provided_password):
        return bcrypt.checkpw(provided_password.encode('utf-8'), stored_password.encode('utf-8'))


def has_permission(permission):
    """
    Check if the current user has the given permission.
    Admin and superuser automatically have all permissions.
    """
    if not getattr(current_user, "is_authenticated", False):
        return False
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "clinical_lead", "support_break_glass"):
        return True
    return permission in (current_user.permissions or [])


def permission_required(permission):
    """
    Decorator to require a specific permission for a route.
    Admin and superuser bypass this check.
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not getattr(current_user, "is_authenticated", False):
                return redirect(url_for("routes.login"))
            if has_permission(permission):
                return f(*args, **kwargs)
            flash(
                "You do not have access to this area. Contact your administrator if you need permission.",
                "danger",
            )
            return redirect(url_for("routes.dashboard"))
        return wrapper
    return decorator


def ensure_core_data_folder():
    """Ensure that a 'data' folder exists in the core module directory (/app/data)."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(base_dir, "data")
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        print(f"Core data folder created at: {data_folder}")
    return data_folder


class EmailManager:
    """
    Handles system email sending using SMTP.
    SMTP configuration is loaded from environment variables.

    Required env:
      - SMTP_HOST
      - SMTP_PORT
      - SMTP_USERNAME
      - SMTP_PASSWORD

    Optional env:
      - SMTP_USE_TLS (default true) — STARTTLS after plain SMTP connect (typical for port 587)
      - SMTP_USE_SSL — force implicit TLS (SMTP_SSL). Default: true if SMTP_PORT is 465
      - SMTP_TIMEOUT — socket timeout seconds (default 30, min 5, max 300)
      - SMTP_FROM (default SMTP_USERNAME)
      - SMTP_FROM_NAME (default empty)

    Port 465 usually requires implicit SSL (SMTP_SSL), not STARTTLS on a plain connection.
    A common misconfiguration is port 465 + STARTTLS, which often hangs until timeout.
    """

    def __init__(self):
        host = (os.environ.get("SMTP_HOST") or "").strip()
        port_raw = (os.environ.get("SMTP_PORT") or "").strip()
        username = (os.environ.get("SMTP_USERNAME") or "").strip()
        password = (os.environ.get("SMTP_PASSWORD") or "").strip()
        use_tls = (os.environ.get("SMTP_USE_TLS", "true")
                   or "true").strip().lower() == "true"

        port = None
        if port_raw.isdigit():
            port = int(port_raw)

        timeout_raw = (os.environ.get("SMTP_TIMEOUT") or "30").strip()
        try:
            smtp_timeout = max(5, min(int(timeout_raw), 300))
        except ValueError:
            smtp_timeout = 30

        ssl_raw = (os.environ.get("SMTP_USE_SSL") or "").strip().lower()
        if ssl_raw in ("1", "true", "yes"):
            use_ssl = True
        elif ssl_raw in ("0", "false", "no"):
            use_ssl = False
        else:
            use_ssl = port == 465

        self.smtp_timeout = smtp_timeout
        self.smtp_config = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "use_tls": use_tls,
            "use_ssl": use_ssl,
            "from_email": (os.environ.get("SMTP_FROM") or username).strip(),
            "from_name": (os.environ.get("SMTP_FROM_NAME") or "").strip(),
        }

        required_keys = ["host", "port", "username", "password"]
        for key in required_keys:
            if not self.smtp_config.get(key):
                raise Exception(
                    f"Email configuration missing required key: {key}")

    def send_email(self, subject, body, recipients, sender=None, html_body=None):
        """
        Sends a multipart email:
          - Always includes text/plain (body)
          - Includes text/html if html_body provided
        """
        if not recipients or not isinstance(recipients, (list, tuple)):
            raise Exception("Recipients must be a non-empty list.")

        sender_email = (
            sender or self.smtp_config["from_email"] or self.smtp_config["username"]).strip()
        if not sender_email:
            raise Exception("Sender email is missing.")

        from_name = self.smtp_config.get("from_name", "").strip()
        msg_from = f"{from_name} <{sender_email}>" if from_name else sender_email

        msg = MIMEMultipart("alternative")
        msg["From"] = msg_from
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = Header(str(subject or "").strip(), "utf-8")

        msg.attach(MIMEText(str(body or ""), "plain", "utf-8"))
        if html_body:
            msg.attach(MIMEText(str(html_body), "html", "utf-8"))

        host = self.smtp_config["host"]
        port = self.smtp_config["port"]
        timeout = self.smtp_timeout
        use_ssl = bool(self.smtp_config.get("use_ssl"))
        use_tls = bool(self.smtp_config["use_tls"])
        user = self.smtp_config["username"]
        password = self.smtp_config["password"]

        server = None
        phase = "connect"
        try:
            if use_ssl:
                phase = "connect (SMTP_SSL)"
                server = smtplib.SMTP_SSL(host, port, timeout=timeout)
            else:
                phase = "connect (SMTP)"
                server = smtplib.SMTP(host, port, timeout=timeout)
                if use_tls:
                    phase = "STARTTLS"
                    server.starttls()
            phase = "AUTH"
            server.login(user, password)
            phase = "SEND"
            server.send_message(msg, from_addr=sender_email,
                                to_addrs=list(recipients))
            phase = "QUIT"
            server.quit()
            server = None
            print(f"Email sent successfully to {recipients}")
        except Exception as e:
            if server is not None:
                try:
                    server.close()
                except Exception:
                    pass
            et = type(e).__name__
            em = str(e).strip() or repr(e)
            hint = ""
            el = em.lower()
            if "timed out" in el or et in ("timeout", "TimeoutError", "socket.timeout"):
                hint = (
                    " For port 465 use implicit TLS (set SMTP_PORT=465 and leave SMTP_USE_SSL unset, "
                    "or set SMTP_USE_SSL=true). For port 587 use SMTP_USE_SSL=false and SMTP_USE_TLS=true."
                    " If host and TLS mode are correct, a timeout usually means outbound SMTP is blocked"
                    " (common on some cloud hosts) or filtered by the mail provider—try 465 if your host"
                    " allows SMTP egress, or use a transactional email HTTP API."
                )
            elif et == "SMTPAuthenticationError" or "authentication failed" in el:
                hint = " Check SMTP_USERNAME / SMTP_PASSWORD."
            elif "ssl" in el or "tls" in el or "certificate" in el:
                hint = " Check TLS mode vs port (465 SSL vs 587 STARTTLS)."
            msg = f"SMTP {phase} to {host}:{port} failed ({et}): {em}.{hint}"
            _logger.warning(msg)
            print(f"Failed to send email: {msg}")
            raise RuntimeError(msg) from e

    def send_email_html(self, subject, html_body, recipients, sender=None, text_body=None):
        """
        Convenience wrapper for HTML emails (still sends text/plain fallback).
        """
        if text_body is None:
            text_body = "This email contains HTML content. If you cannot view it, please contact support."
        return self.send_email(subject, text_body, recipients, sender=sender, html_body=html_body)


# Copied from factory_manifest.json into manifest.json on install / upgrade / enable
PLUGIN_DASHBOARD_MANIFEST_KEYS = (
    "dashboard_category",
    "dashboard_icon",
    "access_permission",
    "declared_permissions",
)


def _load_factory_manifest_dict(factory_path: str) -> dict:
    """Best-effort read of factory_manifest.json (empty dict on missing/error)."""
    if not factory_path or not os.path.exists(factory_path):
        return {}
    try:
        with open(factory_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError, TypeError):
        return {}


def merge_plugin_dashboard_fields_from_factory(
    local: dict, factory: dict | None
) -> None:
    """
    Copy dashboard / access metadata from factory into the local plugin manifest
    when the factory defines them. Safe to call with empty factory.
    """
    if not isinstance(local, dict) or not factory or not isinstance(factory, dict):
        return
    for key in PLUGIN_DASHBOARD_MANIFEST_KEYS:
        val = factory.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            s = val.strip()
            if not s:
                continue
            local[key] = s
        elif isinstance(val, list) and key == "declared_permissions":
            local[key] = val
        else:
            local[key] = val


def run_install_upgrade_scripts(app_root: str) -> dict:
    """
    Run idempotent ``install.py upgrade`` for core (if present) and every plugin
    under ``app_root/plugins`` that has install.py.

    No BackgroundScheduler or UpdateManager side effects — safe for Railway
    preDeploy / one-shot CLI (``python -m app.setup.init_db``).

    Tenant industry profile (Core manifest) may influence *which* optional seed rows
    are applied on a given run; it is not used as a signal to delete or truncate
    existing business data. Changing categories between deploys does not, by design,
    clear tables through this path.
    """
    import subprocess
    import sys

    app_root = os.path.abspath(app_root)
    plugins_dir = os.path.join(app_root, "plugins")

    report = {
        "core": {"ran": False, "ok": False, "message": ""},
        "plugins_ran": [],
        "plugins_skipped": [],
        "plugins_failed": [],
    }

    print("[UPGRADE] ---------- Upgrade scripts run starting ----------")
    print(f"[UPGRADE] APP_ROOT={app_root!r}, PLUGINS_DIR={plugins_dir!r}")

    core_script = os.path.join(app_root, "core", "install.py")
    if os.path.exists(core_script):
        print(f"[UPGRADE] Running core: {core_script}")
        try:
            result = subprocess.run(
                [sys.executable, core_script, "upgrade"],
                check=True,
                capture_output=True,
                text=True,
            )
            report["core"] = {
                "ran": True,
                "ok": True,
                "message": (result.stdout or "").strip(),
            }
            print(f"[UPGRADE] Core OK. stdout: {result.stdout or '(none)'}")
            if result.stderr:
                print(f"[UPGRADE] Core stderr: {result.stderr}")
        except subprocess.CalledProcessError as e:
            err = (e.stderr or e.stdout or str(e)).strip()
            report["core"] = {"ran": True, "ok": False, "message": err}
            print(f"[UPGRADE] Core FAILED: {err}")
    else:
        report["core"] = {
            "ran": False,
            "ok": True,
            "message": "No core install.py found.",
        }
        print(f"[UPGRADE] Core skipped (no install.py at {core_script})")

    plugin_names_with_install = []
    if os.path.isdir(plugins_dir):
        for name in sorted(os.listdir(plugins_dir), key=str.lower):
            if name.startswith("__"):
                continue
            path = os.path.join(plugins_dir, name)
            if not os.path.isdir(path):
                continue
            script_path = os.path.join(path, "install.py")
            if os.path.isfile(script_path):
                plugin_names_with_install.append(name)
    print(
        f"[UPGRADE] Plugins with install.py ({len(plugin_names_with_install)}): {plugin_names_with_install}"
    )

    for plugin_name in plugin_names_with_install:
        script_path = os.path.join(plugins_dir, plugin_name, "install.py")
        if not os.path.exists(script_path):
            report["plugins_skipped"].append(plugin_name)
            print(f"[UPGRADE] Skip {plugin_name}: install.py missing")
            continue

        print(f"[UPGRADE] Running plugin: {plugin_name} -> {script_path}")
        try:
            result = subprocess.run(
                [sys.executable, script_path, "upgrade"],
                check=True,
                capture_output=True,
                text=True,
            )
            report["plugins_ran"].append(plugin_name)
            print(
                f"[UPGRADE] Plugin {plugin_name} OK. stdout: {(result.stdout or '').strip() or '(none)'}"
            )
            if result.stderr:
                print(f"[UPGRADE] Plugin {plugin_name} stderr: {result.stderr}")
        except subprocess.CalledProcessError as e:
            err = (e.stderr or e.stdout or str(e)).strip()
            report["plugins_failed"].append({"plugin": plugin_name, "error": err})
            print(f"[UPGRADE] Plugin {plugin_name} FAILED: {err}")

    print("[UPGRADE] ---------- Upgrade scripts run complete ----------")
    print(
        f"[UPGRADE] Summary: core ran={report['core']['ran']} ok={report['core']['ok']}; "
        f"plugins ran={len(report['plugins_ran'])}, skipped={len(report['plugins_skipped'])}, failed={len(report['plugins_failed'])}"
    )
    return report


class UpdateManager:
    @staticmethod
    def _zip_relative_member_is_safe(rel: str) -> bool:
        """Reject zip-slip and absolute paths (member path uses forward slashes)."""
        rel = (rel or "").replace("\\", "/").lstrip("/")
        if not rel or rel.startswith("/"):
            return False
        parts = rel.split("/")
        if ".." in parts:
            return False
        if parts[-1] in (".", ".."):
            return False
        return True

    @staticmethod
    def _resolved_dest_within_root(root: str, dest: str) -> bool:
        """Ensure dest does not escape root after normalization (defense in depth)."""
        abs_root = os.path.abspath(root)
        abs_dest = os.path.abspath(dest)
        try:
            return os.path.commonpath([abs_root, abs_dest]) == abs_root
        except ValueError:
            return False

    UPDATE_DIR = "app/updates"
    BACKUP_DIR = "app/backups"
    LOG_PATH = "app/logs/update_history.json"
    CORE_PATH = "app/core"  # Not used directly for core updates; we apply to system root

    def __init__(self, plugins_dir='plugins'):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        self.ensure_directories()
        self.APP_ROOT = os.path.abspath(os.path.join(
            os.path.dirname(__file__), "..", "app"))
        self.PLUGINS_DIR = os.path.join(self.APP_ROOT, "plugins")
        self.BACKUP_DIR = os.path.join(self.APP_ROOT, "backups")
        self.UPDATE_DIR = os.path.join(self.APP_ROOT, "updates")
        for d in (self.PLUGINS_DIR, self.BACKUP_DIR, self.UPDATE_DIR):
            os.makedirs(d, exist_ok=True)
        print(f"[DEBUG] UpdateManager PLUGINS_DIR: {self.PLUGINS_DIR}")

        # PluginManager must use the same absolute path
        pm_dir = self.PLUGINS_DIR
        if not os.path.isabs(pm_dir):
            pm_dir = os.path.abspath(pm_dir)
        self.plugin_manager = PluginManager(pm_dir)
        print(f"[DEBUG] PluginManager dir (from UpdateManager): {pm_dir}")

        # Background jobs
        self.scheduler.add_job(
            self.check_for_forced_updates,
            'interval', hours=1, id='forced_update_check', name='Check for forced core updates'
        )
        self.scheduler.add_job(
            self.check_for_forced_plugin_updates,
            'interval', hours=1, id='forced_plugin_update_check', name='Check for forced plugin updates'
        )

        self.gitlab_project_id = effective_core_project_id()
        self.gitlab_token = effective_gitlab_read_token()
        self._gitlab_basic_user, self._gitlab_basic_pass = effective_gitlab_basic_credentials()
        self._gitlab_api_base = f"https://gitlab.com/api/v4/projects/{self.gitlab_project_id}"
        self._default_core_manifest_url = (
            f"{self._gitlab_api_base}/repository/files/manifest.json/raw?ref=main"
        )

    # ------------- Internal helpers -------------
    def _gitlab_session(self):
        s = requests.Session()
        if self._gitlab_basic_user and self._gitlab_basic_pass:
            # Basic auth for deploy token (env or official-registry defaults)
            s.auth = (self._gitlab_basic_user, self._gitlab_basic_pass)
        elif self.gitlab_token:
            s.headers.update({"PRIVATE-TOKEN": self.gitlab_token})
        s.headers.update({"Accept": "application/json"})
        return s

    def ensure_directories(self):
        os.makedirs(self.UPDATE_DIR, exist_ok=True)
        os.makedirs(self.BACKUP_DIR, exist_ok=True)
        os.makedirs(os.path.dirname(self.LOG_PATH), exist_ok=True)

    def _version_tuple(self, v):
        if not isinstance(v, str):
            return (0,)
        parts = []
        for p in v.split('.'):
            try:
                parts.append(int(p))
            except ValueError:
                num = ''.join(ch for ch in p if ch.isdigit())
                parts.append(int(num) if num else 0)
        return tuple(parts)

    def _remote_version_is_strictly_newer(
        self, local_version: Optional[str], remote_version: Optional[str]
    ) -> bool:
        """
        True only when remote is known and compares strictly greater than local.
        Used to block no-op redeploys, downgrades, and forced-update flags pointing at older feeds.
        """
        r = (remote_version or "").strip()
        if not r or r.lower() == "unknown":
            return False
        l = (local_version or "").strip()
        return self._version_tuple(r) > self._version_tuple(l)

    # ------------- Manifest Fetching -------------
    def get_core_manifest_remote(self):
        with self._gitlab_session() as s:
            r = s.get(
                os.environ.get("CORE_MANIFEST_REMOTE_URL")
                or self._default_core_manifest_url
            )
            if r.status_code == 200:
                try:
                    return json.loads(r.text)  # raw endpoint returns text
                except json.JSONDecodeError as e:
                    raise Exception(
                        f"Core manifest not valid JSON: {e}. Body: {r.text[:500]}")
            elif r.status_code == 401:
                raise Exception(
                    "Unauthorized fetching core manifest (401). Check GITLAB_USERNAME/GITLAB_PASSWORD.")
            else:
                raise Exception(
                    f"Failed to fetch core manifest: {r.status_code} - {r.text[:500]}")

    def get_plugin_manifest_remote(self, plugin_name):
        if not plugin_name:
            raise ValueError("Plugin name must be provided.")

        factory_manifest = self.plugin_manager.get_factory_manifest_by_name(
            plugin_name)

        if factory_manifest.get('repository') == 'official':
            encoded_name = quote(plugin_name, safe='')
            plugin_manifest_url = (
                f"{self._gitlab_api_base}/repository/files/plugins%2F{encoded_name}%2Fmanifest.json/raw?ref=main"
            )
        else:
            base_repo = (factory_manifest.get('repository') or '').rstrip('/')
            if not base_repo:
                raise Exception(
                    f"Repository URL for {plugin_name} is missing or invalid in the factory manifest.")
            plugin_manifest_url = f"{base_repo}/plugins/{plugin_name}/manifest.json"

        with self._gitlab_session() as s:
            r = s.get(plugin_manifest_url)
            if r.status_code == 200:
                try:
                    data = json.loads(r.text)
                except json.JSONDecodeError as e:
                    raise Exception(
                        f"Plugin manifest for {plugin_name} not valid JSON: {e}. Body: {r.text[:500]}")
                # Normalize: if bundled under "plugins", extract the single plugin dict
                if isinstance(data, dict) and 'plugins' in data and isinstance(data['plugins'], dict):
                    if plugin_name in data['plugins']:
                        return data['plugins'][plugin_name]
                    else:
                        raise Exception(
                            f"Plugin {plugin_name} not found in plugins bundle manifest.")
                return data  # already single plugin dict
            elif r.status_code == 401:
                raise Exception(
                    f"Unauthorized fetching plugin manifest for {plugin_name} (401).")
            elif r.status_code == 404:
                # Plugin not in remote marketplace (e.g. local-only); return empty so callers get "Unknown" version
                return {}
            else:
                raise Exception(
                    f"Failed to fetch plugin manifest for {plugin_name}: {r.status_code} - {r.text[:500]}")

    def get_plugin_factory_manifest(self, plugin_name):
        if not plugin_name:
            raise ValueError("Plugin name must be provided.")
        plugin_manifest = self.plugin_manager.get_factory_manifest(plugin_name)
        if plugin_manifest.get('repository') == 'official':
            url = f"{self._gitlab_api_base}/repository/files/manifest.json/raw?ref=main"
        else:
            repo = (plugin_manifest.get('repository') or '').rstrip('/')
            if not repo:
                raise Exception(
                    f"Repository URL missing for third-party plugin {plugin_name}.")
            url = f"{repo}/plugins/{plugin_name}/factory_manifest.json"

        with self._gitlab_session() as s:
            r = s.get(url)
            if r.status_code == 200:
                try:
                    return json.loads(r.text)
                except json.JSONDecodeError as e:
                    raise Exception(
                        f"Factory manifest JSON invalid for {plugin_name}: {e}. Body: {r.text[:500]}")
            elif r.status_code == 401:
                raise Exception(
                    f"Unauthorized fetching factory manifest for {plugin_name}.")
            else:
                raise Exception(
                    f"Failed to fetch factory manifest for {plugin_name}: {r.status_code} - {r.text[:500]}")

    from typing import List, Dict, Any, Optional

    @staticmethod
    def _validate_version(ver: Optional[str]) -> str:
        """
        Validate a version string.
        Accepts only dot-separated numeric groups (e.g. "1.0.3").
        Returns "Unknown" if invalid.
        """
        if not ver:
            return "Unknown"

        ver = str(ver).strip()
        if ver and all(part.isdigit() for part in ver.split(".")):
            return ver
        return "Unknown"

    def get_available_plugins(self) -> List[str]:
        """
        Returns a sorted list of plugin system_names that exist in the remote repo
        but are not installed locally (no manifest.json present).
        """
        try:
            # Remote plugin names (trimmed and normalized)
            remote = {(n or "").strip()
                      for n in (self.get_remote_plugin_list() or [])}

            # Installed plugin system_names
            installed = {
                (p.get("system_name", "") or "").strip()
                for p in (self.plugin_manager.get_all_plugins() or [])
                if p.get("installed")
            }

            # Drop blanks just in case
            remote.discard("")
            installed.discard("")

            # Return case-insensitively sorted difference
            return sorted(remote - installed, key=str.lower)

        except Exception as e:
            print(f"Failed to compute available plugins: {e}")
            return []

    def get_available_plugins_details(self) -> List[Dict[str, Any]]:
        """
        Returns detailed metadata for not-yet-installed plugins.
        Each entry includes:
            - system_name
            - name (fallback: Title Case from system_name)
            - current_version (validated; "Unknown" if invalid)
            - description
            - icon
            - download_url
            - repository (if resolvable)
            - changelog (short preview)
        """
        results: List[Dict[str, Any]] = []

        for plugin_name in self.get_available_plugins():
            if not plugin_name:
                continue

            meta: Dict[str, Any] = {"system_name": plugin_name}

            try:
                remote = self.get_plugin_manifest_remote(plugin_name) or {}

                # Fill metadata with safe defaults
                meta["name"] = remote.get(
                    "name", plugin_name.replace("_", " ").title()
                ).strip()
                meta["current_version"] = self._validate_version(
                    remote.get("current_version", "Unknown"))
                meta["description"] = str(
                    remote.get("description", "")).strip()
                meta["icon"] = remote.get(
                    "icon", "default-icon.png") or "default-icon.png"
                try:
                    plan = self._resolve_artifact_plan(
                        remote, section=None, require_complete_base=False
                    )
                    meta["download_url"] = plan.get("latest_download_url") or plan.get(
                        "initial_download_url"
                    ) or ""
                except Exception:
                    meta["download_url"] = self.convert_to_api_endpoint(
                        remote.get("download_url", "") or ""
                    )

                # Optional: repository badge (may fail silently)
                try:
                    meta["repository"] = self.plugin_manager.get_repository_for_plugin(
                        plugin_name
                    )
                except Exception:
                    pass

                # Changelog preview (first entry if structured)
                cl = remote.get("changelog", [])
                if isinstance(cl, list) and cl:
                    if isinstance(cl[0], dict):
                        meta["changelog"] = cl[0].get("changes", []) or []
                    else:
                        meta["changelog"] = cl
                else:
                    meta["changelog"] = []

            except Exception as e:
                print(f"Info fetch failed for {plugin_name}: {e}")
                meta.update({
                    "name": plugin_name.replace("_", " ").title(),
                    "current_version": "Unknown",
                    "description": "",
                    "icon": "default-icon.png",
                    "download_url": "",
                    "changelog": []
                })

            results.append(meta)

        return results

    # ------------- Version Checking -------------

    def get_current_version(self):
        core_manifest = self.plugin_manager.get_core_manifest()  # Local core manifest
        return core_manifest.get('version', 'Unknown')

    def get_latest_version(self):
        """
        Published core version from remote manifest (GitLab API).
        Returns None if the remote cannot be reached or auth fails (401/403),
        so local /version still loads without credentials.
        """
        try:
            core_manifest = self.get_core_manifest_remote()
            v = core_manifest.get("core", {}).get("current_version")
            return v if v else None
        except Exception as e:
            print(
                f"[WARN] get_latest_version: remote core manifest unavailable: {e}")
            return None

    def get_plugin_latest_version(self, plugin_name):
        try:
            plugin = self.get_plugin_manifest_remote(plugin_name) or {}
            return plugin.get('current_version', 'Unknown')
        except Exception:
            return 'Unknown'

    def get_plugins_versions(self):
        """
        Current version source of truth:
        - factory_manifest.json (shipped with plugin code; overwritten on updates)
        Fallback:
        - local manifest.json version (if present)
        """
        plugins_versions = {}

        plugins = self.plugin_manager.get_all_plugins() or []
        for plugin in plugins:
            plugin_name = (plugin.get("system_name") or "").strip()
            if not plugin_name:
                continue

            # 1) Factory manifest (FULL) version
            try:
                fm = self.plugin_manager.get_factory_manifest_full_by_name(plugin_name) or {
                }
                fv = fm.get("current_version") or fm.get("version")
                if fv:
                    plugins_versions[plugin_name] = fv
                    continue
            except Exception:
                pass

            # 2) Fallback: local manifest version (may not exist)
            local = self.plugin_manager.get_plugin(plugin_name)
            if isinstance(local, dict):
                plugins_versions[plugin_name] = local.get("version", "Unknown")
            else:
                plugins_versions[plugin_name] = "Unknown"

        return plugins_versions

    def get_update_status(self):
        current_version = self.get_current_version()
        latest_version = self.get_latest_version()
        if latest_version:
            core_update_available = self._version_tuple(
                current_version) < self._version_tuple(latest_version)
        else:
            core_update_available = False

        plugins_versions = self.get_plugins_versions()
        plugin_updates = []
        for plugin_name, plugin_version in plugins_versions.items():
            try:
                plugin_latest_version = self.get_plugin_latest_version(
                    plugin_name)
                plugin_update_available = self._version_tuple(
                    plugin_version) < self._version_tuple(plugin_latest_version)
            except Exception as e:
                plugin_latest_version = "Unknown"
                plugin_update_available = False
                print(
                    f"Error determining latest version for plugin {plugin_name}: {e}")
            # Human-readable title from local manifest (manifest.json "name")
            display_name = plugin_name.replace("_", " ").title()
            try:
                local = self.plugin_manager.get_plugin(plugin_name)
                if isinstance(local, dict):
                    dn = (local.get("name") or "").strip()
                    if dn:
                        display_name = dn
            except Exception:
                pass
            plugin_updates.append({
                "plugin_name": plugin_name,
                "display_name": display_name,
                "current_version": plugin_version,
                "latest_version": plugin_latest_version,
                "update_available": plugin_update_available
            })

        return {
            "core": {
                "current_version": current_version,
                "latest_version": latest_version,
                "update_available": core_update_available
            },
            "plugins": plugin_updates
        }

    # ------------- URL Conversion -------------
    def convert_to_api_endpoint(self, download_url):
        api_prefix = "https://gitlab.com/api/v4/"
        if download_url.startswith(api_prefix):
            return download_url

        parsed = urlparse(download_url)
        path_parts = parsed.path.split('/')
        # Expected web raw form: /<namespace>/<project>/-/raw/<branch>/<file path...>
        if len(path_parts) < 7 or path_parts[3] != '-' or path_parts[4] != 'raw':
            return download_url

        branch = path_parts[5]
        file_path = "/".join(path_parts[6:])
        encoded_file_path = quote(file_path, safe='')
        return f"{self._gitlab_api_base}/repository/files/{encoded_file_path}/raw?ref={branch}"

    # ------------- Backup and Restore -------------
    def backup(self, backup_name="whole_system_backup"):
        import zipfile
        system_root = os.path.dirname(self.BACKUP_DIR)  # typically "app"
        os.makedirs(self.BACKUP_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{backup_name}_{timestamp}.zip"
        backup_path = os.path.join(self.BACKUP_DIR, backup_filename)
        # Exclude heavy or generated directories
        exclude_dirs = {
            # app/backups
            os.path.normpath(self.BACKUP_DIR),
            # app/updates
            os.path.normpath(self.UPDATE_DIR),
            os.path.normpath(os.path.join(system_root, "logs")
                             ),        # app/logs
            os.path.normpath(os.path.join(system_root, "tmp")
                             ),         # app/tmp
            os.path.normpath(os.path.join(system_root, "node_modules")),
            os.path.normpath(os.path.join(system_root, "venv")),
            os.path.normpath(os.path.join(system_root, ".venv")),
            os.path.normpath(os.path.join(system_root, "__pycache__")),
            os.path.normpath(os.path.join(system_root, "media")),
            os.path.normpath(os.path.join(system_root, "uploads")),
        }

        def is_excluded(path):
            p = os.path.normpath(path)
            base = os.path.basename(p)
            # Never archive environment / secret files (backup confidentiality)
            if base == ".env" or (
                base.startswith(".env.") and not base.startswith("..")
            ):
                return True
            for ex in exclude_dirs:
                if p == ex or p.startswith(ex + os.sep):
                    return True
            return False

        # Optional: print some progress every N files
        file_counter = 0
        progress_step = 1000

        with zipfile.ZipFile(backup_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(system_root):
                # prune excluded dirs to speed up traversal
                dirs[:] = [d for d in dirs if not is_excluded(
                    os.path.join(root, d))]
                for fn in files:
                    fp = os.path.join(root, fn)
                    if is_excluded(fp):
                        continue
                    arcname = os.path.relpath(fp, system_root)
                    try:
                        zf.write(fp, arcname)
                    except FileNotFoundError:
                        # File changed/removed during walk — skip
                        continue
                    file_counter += 1
                    if file_counter % progress_step == 0:
                        print(
                            f"Backup progress: {file_counter} files zipped...")

        print(f"Backup completed: {backup_path} ({file_counter} files)")
        return backup_path

    def restore_backup(self, backup_name, restore_to):
        # basename only: backup_name must not be a path (prevents escape from BACKUP_DIR)
        stem = str(backup_name).strip()
        if not stem or os.path.basename(stem) != stem:
            raise Exception("Invalid backup name.")
        allowed = frozenset(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
        )
        if not all(c in allowed for c in stem):
            raise Exception("Invalid backup name.")
        backup_zip = os.path.join(self.BACKUP_DIR, f"{stem}.zip")
        if not os.path.exists(backup_zip):
            raise Exception(f"Backup {backup_zip} not found.")
        abs_restore = os.path.abspath(restore_to)
        os.makedirs(abs_restore, exist_ok=True)
        with zipfile.ZipFile(backup_zip, "r") as zip_ref:
            names = [
                n.replace("\\", "/")
                for n in zip_ref.namelist()
                if n and not n.startswith("__MACOSX")
            ]
            for member in names:
                if member.endswith("/"):
                    continue
                rel = member
                if not self._zip_relative_member_is_safe(rel):
                    raise Exception(
                        f"Unsafe path in backup archive: {member!r}")
                dest = os.path.join(abs_restore, rel)
                if not self._resolved_dest_within_root(abs_restore, dest):
                    raise Exception(f"Zip-slip blocked for member: {member!r}")
                parent = os.path.dirname(dest)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with zip_ref.open(member) as src, open(dest, "wb") as out:
                    shutil.copyfileobj(src, out)
                try:
                    info = zip_ref.getinfo(member)
                    mode = (info.external_attr >> 16) & 0o777
                    if mode:
                        os.chmod(dest, mode)
                except Exception:
                    pass

    # ------------- Update Installation -------------
    def download_update(self, url, save_path):
        with self._gitlab_session() as s:
            s.headers.update({"Accept": "*/*"})  # binary
            r = s.get(url, stream=True)
            if r.status_code == 200:
                with open(save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            elif r.status_code == 401:
                raise Exception(
                    f"Unauthorized downloading update from {url} (401).")
            else:
                snippet = ""
                try:
                    snippet = r.text[:500]
                except Exception:
                    pass
                raise Exception(
                    f"Failed to download update from {url}: {r.status_code} - {snippet}")

    @staticmethod
    def _should_skip_zip_extract_on_upgrade(
        zip_mode: Optional[str],
        strip_leading_app: bool,
        rel_path: str,
        dest_path: str,
    ) -> bool:
        """
        On *upgrade* only, avoid overwriting paths that hold deployment-specific data.

        - Plugin upgrades: preserve existing templates/public/, data/, site_data/,
          static/uploads/, pages.json, manifest.json (user site + module settings).
        - Core upgrades (strip_leading_app): preserve any existing file under config/
          (secrets, tokens); new config/* members from the zip still extract.

        Install / full extract: pass zip_mode='install' (or None).
        """
        mode = (zip_mode or "install").strip().lower()
        if mode != "upgrade":
            return False
        if not os.path.exists(dest_path):
            return False
        norm = (rel_path or "").replace("\\", "/").lstrip("/")
        if strip_leading_app:
            if norm.startswith("config/") or norm == "config":
                return True
            return False
        # Plugin package root
        if norm in ("pages.json", "manifest.json", "local_service_pages.json"):
            return True
        for prefix in (
            "data/",
            "site_data/",
            "templates/public/",
            "static/uploads/",
        ):
            if norm.startswith(prefix):
                return True
        return False

    def apply_zip(
        self,
        zip_path,
        target_path,
        strip_leading_app=False,
        zip_mode: Optional[str] = "install",
    ):
        """
        Extract a zip into target_path, handling single-root and flat zips, with basic safety.

        If strip_leading_app is True (e.g. for core update into app/), any relative path
        starting with "app/" is stripped so files land in target_path instead of target_path/app/.
        This ensures zips built as e.g. Core_v1.1.5/app/auth_jwt.py still add new files to app/.

        zip_mode:
          - 'install' (default): extract all members (first-time plugin install, dev, etc.).
          - 'upgrade': do not overwrite preserved user/deployment paths (see
            _should_skip_zip_extract_on_upgrade); new files from the zip are still added.
        """
        if not os.path.exists(zip_path):
            raise Exception(f"Update file not found: {zip_path}")

        os.makedirs(target_path, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            names = [n.replace('\\', '/') for n in zf.namelist()
                     if n and not n.startswith('__MACOSX')]
            if not names:
                return

            def is_safe(rel):
                return self._zip_relative_member_is_safe(rel)

            top = set(n.split('/')[0] for n in names)
            flatten = (len(top) == 1 and any('/' in n for n in names))
            root = next(iter(top)) if flatten else None

            for member in names:
                if member.endswith('/'):
                    continue
                rel = member[len(
                    root) + 1:] if (flatten and member.startswith(root + '/')) else member
                if strip_leading_app and rel.startswith('app/'):
                    rel = rel[4:].lstrip('/')
                if not rel:
                    continue
                if not is_safe(rel):
                    continue
                dest = os.path.join(target_path, rel)
                if not self._resolved_dest_within_root(target_path, dest):
                    continue
                if self._should_skip_zip_extract_on_upgrade(
                    zip_mode, strip_leading_app, rel, dest
                ):
                    continue
                parent = os.path.dirname(dest)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with zf.open(member) as src, open(dest, 'wb') as out:
                    shutil.copyfileobj(src, out)
                # optional: preserve Unix perms
                try:
                    info = zf.getinfo(member)
                    mode = (info.external_attr >> 16) & 0o777
                    if mode:
                        os.chmod(dest, mode)
                except Exception:
                    pass

    def run_update_instructions(self, script_path):
        try:
            print(f"Executing update instructions from {script_path}...")
            subprocess.run([sys.executable, script_path], check=True)
            print("Update instructions executed successfully.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Running update instructions failed: {e}")

    def run_upgrade_scripts(self):
        """
        Run idempotent upgrade scripts for core + all plugins that have install.py.
        Delegates to :func:`run_install_upgrade_scripts` (same behaviour as Railway preDeploy).
        """
        return run_install_upgrade_scripts(self.APP_ROOT)

    # ------------- Detailed Logging -------------
    def log_update(self, update_type, name, update_mode, old_version, new_version, status, details):
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "update_type": update_type,
            "name": name,
            "update_mode": update_mode,
            "old_version": old_version,
            "new_version": new_version,
            "status": status,
            "details": details
        }
        os.makedirs(os.path.dirname(self.LOG_PATH), exist_ok=True)
        with open(self.LOG_PATH, 'a') as log_file:
            json.dump(log_entry, log_file)
            log_file.write("\n")

    # ------------- Update Application -------------
    def _parse_version_from_zip_name(self, zip_path, plugin_name):
        """
        Attempt to extract a version string from a zip filename.

        Expected format: <plugin_name>_v<version>.zip
        Example: myplugin_v1.2.3.zip  -> returns "1.2.3"

        Args:
            zip_path (str | Path): Path to the zip file.
            plugin_name (str): Name of the plugin (prefix to match).

        Returns:
            str | None: Version string if found and valid, otherwise None.
        """
        # Ensure we’re working with a plain string filename
        base = os.path.basename(str(zip_path))

        # Construct the expected filename prefix
        prefix = f"{plugin_name}_v"

        # Check if filename matches the expected pattern
        # (prefix + ".zip" suffix; suffix comparison is case-insensitive)
        if base.startswith(prefix) and base.lower().endswith(".zip"):

            # Extract the version portion (strip off prefix and ".zip")
            ver = base[len(prefix):-4].strip()

            # Validate the version:
            # - Not empty
            # - Only dot-separated digit groups (e.g., "1.2.3")
            if ver and all(part.isdigit() for part in ver.split(".")):
                return ver

        # If checks fail, return None
        return None

    def _normalize_release_entries(self, releases_raw):
        """
        Normalize legacy `releases` metadata into a list of dict entries.
        Supports:
        - list[dict]
        - dict version->dict
        """
        normalized = []
        if isinstance(releases_raw, list):
            for item in releases_raw:
                if isinstance(item, dict):
                    normalized.append(dict(item))
            return normalized

        if isinstance(releases_raw, dict):
            for ver, item in releases_raw.items():
                if not isinstance(item, dict):
                    continue
                entry = dict(item)
                entry.setdefault("version", str(ver))
                normalized.append(entry)
        return normalized

    def _changelog_artifact_entries(self, block: dict) -> List[dict]:
        """
        Entries from `changelog` when it is a list of objects with `version`.
        Artifact fields on the same row as the release notes:
        - artifact_url or download_url
        - artifact_type: complete | partial (default complete when URL present)
        - base_version: for partial, which full release to layer on
        """
        cl = block.get("changelog")
        if not isinstance(cl, list):
            return []
        out: List[dict] = []
        for item in cl:
            if isinstance(item, dict) and (item.get("version") or "").strip():
                out.append(dict(item))
        return out

    def _resolve_artifact_plan(self, manifest: dict, section: str = None, require_complete_base: bool = False) -> dict:
        """
        Resolve update/install artifacts for core/plugins with backward compatibility.

        Preferred schema — same object as changelog rows (no separate releases dict):
            changelog: [{version, date, changes, artifact_url?, artifact_type?, base_version?}, ...]

        Legacy / optional:
            releases: [{version, artifact_url, ...}, ...]  (deprecated; changelog wins on same version)
            download_url, complete_download_url/full_download_url at block level
        """
        block = manifest.get(section, {}) if section else manifest
        if not isinstance(block, dict):
            block = {}

        current_version = (block.get("current_version") or "").strip()
        releases = self._normalize_release_entries(block.get("releases") or [])
        changelog_entries = self._changelog_artifact_entries(block)

        def _release_url(entry: dict) -> str:
            return (
                (entry.get("artifact_url") or "").strip()
                or (entry.get("download_url") or "").strip()
            )

        by_version: Dict[str, dict] = {}
        # Legacy `releases` first (changelog overrides when both define a URL for a version)
        for rel in releases:
            ver = (rel.get("version") or "").strip()
            if ver:
                by_version[ver] = rel
        for entry in changelog_entries:
            ver = (entry.get("version") or "").strip()
            if ver and _release_url(entry):
                by_version[ver] = entry

        selected = by_version.get(current_version) if current_version else None
        if not selected and by_version:
            # Fallback: highest version that declares an artifact URL
            def _ver_key(v: str):
                return parse(v) if v else parse("0")
            best_ver = max(by_version.keys(), key=_ver_key)
            selected = by_version[best_ver]

        legacy_latest = (block.get("download_url") or "").strip()
        legacy_complete = (
            (block.get("complete_download_url") or "").strip()
            or (block.get("full_download_url") or "").strip()
        )

        initial_url = ""
        latest_url = ""
        artifact_type = "legacy"

        if selected:
            artifact_type = ((selected.get("artifact_type")
                             or "complete").strip().lower() or "complete")
            selected_url = _release_url(selected)

            if artifact_type == "partial":
                base_url = (
                    (selected.get("complete_artifact_url") or "").strip()
                    or (selected.get("complete_download_url") or "").strip()
                    or (selected.get("full_download_url") or "").strip()
                )

                base_version = (selected.get("base_version") or "").strip()
                if not base_url and base_version:
                    base_rel = by_version.get(base_version)
                    if base_rel:
                        base_url = _release_url(base_rel)

                if not base_url:
                    base_url = legacy_complete

                if require_complete_base and not base_url:
                    raise Exception(
                        "Partial artifact requires a complete base artifact URL."
                    )

                initial_url = base_url or selected_url
                latest_url = selected_url
            else:
                initial_url = selected_url or legacy_complete or legacy_latest
                latest_url = selected_url or legacy_latest
        else:
            initial_url = legacy_complete or legacy_latest
            latest_url = legacy_latest or initial_url

        if not initial_url:
            raise Exception("No resolvable artifact URL in manifest.")

        initial_url = self.convert_to_api_endpoint(initial_url)
        latest_url = self.convert_to_api_endpoint(
            latest_url) if latest_url else None

        return {
            "initial_download_url": initial_url,
            "latest_download_url": latest_url,
            "current_version": current_version or "Unknown",
            "artifact_type": artifact_type,
        }

    def apply_update(self, update_type, plugin_name=None, update_mode="manual"):
        import os
        import sys
        import json
        import subprocess
        from urllib.parse import urlparse, unquote

        backup_archive = None
        system_root = os.path.dirname(self.BACKUP_DIR)  # typically .../app
        old_version = None

        def _load_json(path: str, default: dict) -> dict:
            if not os.path.exists(path):
                return dict(default)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return data if isinstance(data, dict) else dict(default)
            except Exception:
                return dict(default)

        def _write_json(path: str, data: dict) -> None:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass

        def _get_factory_version_local(name: str) -> str:
            try:
                if hasattr(self.plugin_manager, "get_factory_manifest_full_by_name"):
                    fm = self.plugin_manager.get_factory_manifest_full_by_name(name) or {
                    }
                else:
                    plugin_folder = os.path.join(self.PLUGINS_DIR, name)
                    fm = self.plugin_manager.get_factory_manifest(
                        plugin_folder) or {}
                v = (fm.get("current_version")
                     or fm.get("version") or "").strip()
                return v if v else "Unknown"
            except Exception:
                return "Unknown"

        try:
            # ----------------------------
            # Determine old version (safe)
            # ----------------------------
            if update_type == "core":
                old_version = self.get_current_version()
            elif update_type == "plugin" and plugin_name:
                old_version = _get_factory_version_local(plugin_name)
            else:
                raise Exception("Invalid update type.")

            # ----------------------------
            # Version gate (before backup / any download)
            # Ignore remote force_update flags — only strict semver upgrade.
            # ----------------------------
            if update_type == "core":
                remote_core = self.get_core_manifest_remote()
                remote_ver = (remote_core.get("core") or {}).get(
                    "current_version"
                )
                if not self._remote_version_is_strictly_newer(
                    old_version, remote_ver
                ):
                    raise Exception(
                        "Core update not applied: the repository version "
                        f"({remote_ver!r}) is not newer than this installation "
                        f"({old_version!r}). Forced-update flags are ignored."
                    )
            elif update_type == "plugin" and plugin_name:
                remote_pm = self.get_plugin_manifest_remote(plugin_name) or {}
                remote_ver = remote_pm.get("current_version")
                if not self._remote_version_is_strictly_newer(
                    old_version, remote_ver
                ):
                    raise Exception(
                        f"Plugin {plugin_name!r} update not applied: repository "
                        f"version ({remote_ver!r}) is not newer than local "
                        f"({old_version!r}). Forced-update flags are ignored."
                    )

            # ----------------------------
            # Backup
            # ----------------------------
            print("Creating whole system backup...")
            backup_archive = self.backup()
            print(f"Whole system backup created: {backup_archive}")

            # ============================================================
            # CORE UPDATE
            # ============================================================
            if update_type == "core":
                print("Fetching core manifest...")
                core_manifest = self.get_core_manifest_remote()
                plan = self._resolve_artifact_plan(
                    core_manifest, section="core", require_complete_base=False
                )
                initial_download_url = plan["initial_download_url"]
                latest_download_url = plan["latest_download_url"]

                print("Downloading core update (base artifact)...")
                url_name = unquote(
                    os.path.basename(urlparse(initial_download_url).path)
                )
                if not url_name.lower().endswith(".zip"):
                    url_name = "core_update_base.zip"
                zip_path_initial = os.path.join(self.UPDATE_DIR, url_name)
                self.download_update(initial_download_url, zip_path_initial)

                print("Applying core update (base)...")
                self.apply_zip(
                    zip_path_initial,
                    system_root,
                    strip_leading_app=True,
                    zip_mode="upgrade",
                )

                if (
                    latest_download_url
                    and latest_download_url != initial_download_url
                ):
                    print("Downloading core overlay (latest / delta)...")
                    overlay_name = unquote(
                        os.path.basename(urlparse(latest_download_url).path)
                    )
                    if not overlay_name.lower().endswith(".zip"):
                        overlay_name = "core_update_overlay.zip"
                    zip_path_overlay = os.path.join(
                        self.UPDATE_DIR, overlay_name
                    )
                    self.download_update(latest_download_url, zip_path_overlay)
                    print("Applying core overlay...")
                    self.apply_zip(
                        zip_path_overlay,
                        system_root,
                        strip_leading_app=True,
                        zip_mode="upgrade",
                    )

                # Update local core manifest version (app/config/manifest.json)
                manifest_path = os.path.join(
                    system_root, "config", "manifest.json")
                new_version = core_manifest.get("core", {}).get(
                    "current_version") or "Unknown"

                manifest_data = _load_json(manifest_path, {})
                manifest_data["version"] = new_version
                _write_json(manifest_path, manifest_data)

                print(f"Core manifest version updated to {new_version}.")

                # Run core install.py upgrade (if present)
                core_path = os.path.join(self.APP_ROOT, "core")
                script_path = os.path.join(core_path, "install.py")
                if os.path.exists(script_path):
                    print(f"[INFO] Running core upgrade script: {script_path}")
                    result = subprocess.run(
                        [sys.executable, script_path, "upgrade"],
                        check=True, capture_output=True, text=True
                    )
                    if result.stdout:
                        print(result.stdout)
                    if result.stderr:
                        print("[STDERR]", result.stderr)
                else:
                    print("[WARN] No install.py found in core directory.")

                print("Core update completed.")

                self.log_update(
                    "core", "core", update_mode, old_version, new_version,
                    "success", "Core update applied successfully."
                )

            # ============================================================
            # PLUGIN UPDATE
            # ============================================================
            elif update_type == "plugin" and plugin_name:
                plugin_name = (plugin_name or "").strip()
                if not plugin_name:
                    raise Exception(
                        "Plugin name must be provided for plugin updates.")

                print(f"Fetching plugin manifest for {plugin_name}...")
                plugin_manifest = self.get_plugin_manifest_remote(
                    plugin_name) or {}

                # Compute correct target path
                plugin_path = os.path.join(system_root, "plugins", plugin_name)
                os.makedirs(plugin_path, exist_ok=True)

                plan = self._resolve_artifact_plan(
                    plugin_manifest, section=None, require_complete_base=False
                )
                initial_download_url = plan["initial_download_url"]
                latest_download_url = plan["latest_download_url"]

                print(
                    f"Downloading plugin update for {plugin_name} (base artifact)...")
                url_name = unquote(os.path.basename(
                    urlparse(initial_download_url).path))
                if not url_name.lower().endswith(".zip"):
                    url_name = f"{plugin_name}_update_base.zip"
                zip_path_initial = os.path.join(self.UPDATE_DIR, url_name)
                self.download_update(initial_download_url, zip_path_initial)

                print(f"Applying plugin update for {plugin_name} (base)...")
                self.apply_zip(
                    zip_path_initial, plugin_path, zip_mode="upgrade")

                if latest_download_url and latest_download_url != initial_download_url:
                    print(
                        f"Downloading plugin overlay for {plugin_name} (latest / delta)...")
                    overlay_name = unquote(os.path.basename(
                        urlparse(latest_download_url).path))
                    if not overlay_name.lower().endswith(".zip"):
                        overlay_name = f"{plugin_name}_update_overlay.zip"
                    zip_path_overlay = os.path.join(
                        self.UPDATE_DIR, overlay_name)
                    self.download_update(latest_download_url, zip_path_overlay)
                    print(f"Applying plugin overlay for {plugin_name}...")
                    self.apply_zip(
                        zip_path_overlay, plugin_path, zip_mode="upgrade")

                # Determine new version from *local factory manifest after extraction*
                new_version = _get_factory_version_local(plugin_name)

                # Update local manifest.json (settings/state) BUT keep it up-to-date with version
                local_manifest_path = os.path.join(
                    plugin_path, "manifest.json")
                local = _load_json(local_manifest_path, {})

                # Preserve settings and custom keys; just normalize core fields
                local.setdefault("system_name", plugin_name)
                local["installed"] = True
                local.setdefault("enabled", False)
                local["update_available"] = False

                # Keep local manifest version in sync (as requested)
                if new_version and new_version != "Unknown":
                    local["version"] = new_version

                merge_plugin_dashboard_fields_from_factory(
                    local,
                    _load_factory_manifest_dict(
                        os.path.join(plugin_path, "factory_manifest.json")
                    ),
                )

                _write_json(local_manifest_path, local)

                # Run plugin install.py upgrade (if present)
                script_path = os.path.join(plugin_path, "install.py")
                if os.path.exists(script_path):
                    print(
                        f"[INFO] Running plugin upgrade script: {script_path}")
                    result = subprocess.run(
                        [sys.executable, script_path, "upgrade"],
                        check=True, capture_output=True, text=True
                    )
                    if result.stdout:
                        print(result.stdout)
                    if result.stderr:
                        print("[STDERR]", result.stderr)
                else:
                    print(
                        f"[WARN] No install.py found in plugin directory for {plugin_name}.")

                print(f"{plugin_name} update completed.")

                self.log_update(
                    "plugin", plugin_name, update_mode, old_version, new_version,
                    "success", f"Plugin {plugin_name} update applied successfully."
                )

            else:
                raise Exception("Invalid update type.")

            print("Update applied successfully.")

        except Exception as e:
            error_message = str(e)
            print(f"Update failed: {error_message}")

            # Log failure (best-effort)
            try:
                self.log_update(
                    update_type,
                    plugin_name if plugin_name else "core",
                    update_mode,
                    old_version,
                    "Unknown",
                    "error",
                    error_message
                )
            except Exception as le:
                print(f"Failed to write update log: {le}")

            # Rollback (best-effort)
            if backup_archive:
                try:
                    print("Rolling back to the previous whole system backup...")
                    backup_name = os.path.basename(
                        backup_archive).replace(".zip", "")
                    self.restore_backup(backup_name, system_root)
                    print("System restored from backup.")
                except Exception as re:
                    print(f"Rollback failed: {re}")

            raise

    def schedule_update(self, update_type, scheduled_time, plugin_name=None):
        run_date = datetime.strptime(scheduled_time, "%Y-%m-%d %H:%M:%S")
        if update_type == "core":
            self.scheduler.add_job(self.apply_update, 'date', run_date=run_date, args=[
                                   'core', None, "scheduled"])
        elif update_type == "plugin" and plugin_name:
            self.scheduler.add_job(self.apply_update, 'date', run_date=run_date, args=[
                                   'plugin', plugin_name, "scheduled"])
        else:
            raise Exception(f"Invalid update type: {update_type}")
        print(f"Update scheduled for {update_type} at {run_date}")

    def get_changelog_for_plugin(self, plugin_name):
        try:
            data = self.get_plugin_manifest_remote(plugin_name)
            return data.get('changelog', 'No changelog available.')
        except Exception as e:
            print(f"Failed to fetch changelog for plugin {plugin_name}: {e}")
            return 'No changelog available.'

    def get_changelog_for_core(self):
        try:
            core_manifest = self.get_core_manifest_remote()
            return core_manifest.get('core', {}).get('changelog', 'No changelog available.')
        except Exception as e:
            print(f"Failed to fetch changelog for core module: {e}")
            return 'No changelog available.'

    def get_remote_plugin_list(self):
        url = f"{self._gitlab_api_base}/repository/tree?path=plugins&ref=main&per_page=100"
        with self._gitlab_session() as s:
            r = s.get(url)
            if r.status_code == 200:
                try:
                    nodes = r.json()
                except Exception:
                    raise Exception(
                        f"Invalid JSON from plugin list. Body: {r.text[:500]}")
                # Return only folder names under plugins
                return [n['name'] for n in nodes if n.get('type') == 'tree']
            elif r.status_code == 401:
                raise Exception("Unauthorized fetching plugin list (401).")
            else:
                raise Exception(
                    f"Failed to fetch plugin list: {r.status_code} - {r.text[:500]}")

    import os
    import json
    from typing import List, Dict, Any, Optional, Set
    from urllib.parse import urlparse, unquote
    import subprocess

    def install_plugin(self, plugin_name: str) -> None:
        """
        Install a plugin from the remote repository into the local plugins folder.
        Handles dependency resolution and calls _install_single_plugin for each.
        """
        if not plugin_name or not isinstance(plugin_name, str):
            raise Exception("Invalid plugin name.")

        # 0) Resolve dependency order for this plugin (deps first)
        install_order = self._resolve_dependency_install_order(plugin_name)
        # install_order includes the target plugin at the end

        # Build quick lookup of current local plugin states
        def get_local_map():
            return {p.get("system_name"): p for p in (self.plugin_manager.get_all_plugins() or [])}

        local_map = get_local_map()

        # 1) Process dependencies first (all except the last, which is the target)
        for dep_name in install_order[:-1]:
            meta = local_map.get(dep_name)
            if not meta or not meta.get("installed"):
                # Install dependency
                print(
                    f"[INFO] Auto-installing dependency '{dep_name}' for '{plugin_name}'...")
                self._install_single_plugin(dep_name)
                # Refresh state
                local_map = get_local_map()
                meta = local_map.get(dep_name)
                if not meta or not meta.get("installed"):
                    raise Exception(
                        f"Failed to auto-install dependency '{dep_name}' required by '{plugin_name}'.")

            if not meta.get("enabled"):
                # Enable dependency
                print(
                    f"[INFO] Enabling dependency '{dep_name}' for '{plugin_name}'...")
                ok, msg = self.plugin_manager.enable_plugin(dep_name)
                if not ok:
                    raise Exception(
                        f"Failed to enable dependency '{dep_name}' required by '{plugin_name}': {msg}")
                local_map = get_local_map()

        # 2) Install the target plugin (do NOT auto-enable here)
        self._install_single_plugin(plugin_name)

        # 3) Refresh PluginManager cache one more time
        if hasattr(self.plugin_manager, "load_plugins"):
            try:
                self.plugin_manager.plugins = self.plugin_manager.load_plugins()
            except Exception as e:
                print(
                    f"[WARN] PluginManager refresh failed after installing '{plugin_name}': {e}")

        print(
            f"[INFO] Plugin '{plugin_name}' installed successfully with all dependencies satisfied.")

    def _install_single_plugin(self, plugin_name: str) -> None:
        """
        Install a single plugin by downloading and extracting its zip, and writing
        a minimal local manifest.json. Leaves enabled=False; enabling is a separate step.
        Runs install.py install if present (for FRESH installs only).
        Raises Exception on failure.
        """
        import os
        from urllib.parse import urlparse, unquote
        import json
        import subprocess

        # --- Verify remote presence (non-fatal if API listing fails) ---
        try:
            remote_list = set(self.get_remote_plugin_list() or [])
            if plugin_name not in remote_list:
                raise Exception(
                    f"Plugin '{plugin_name}' not found in remote repository (plugins/ folder).")
        except Exception as e:
            print(
                f"[WARN] Could not confirm remote listing for '{plugin_name}': {e}")

        # --- Fetch remote manifest ---
        try:
            remote_manifest = self.get_plugin_manifest_remote(
                plugin_name) or {}
        except Exception as e:
            raise Exception(
                f"Failed to fetch remote manifest for '{plugin_name}': {e}")

        # Fresh install requires a complete base artifact when latest is partial.
        plan = self._resolve_artifact_plan(
            remote_manifest, section=None, require_complete_base=True
        )
        initial_download_url = plan["initial_download_url"]
        latest_download_url = plan["latest_download_url"]

        api_root = f"{self._gitlab_api_base}/"
        if not initial_download_url.startswith(api_root):
            raise Exception(
                f"Initial download URL not from configured GitLab project for '{plugin_name}': {initial_download_url}")
        if latest_download_url and not latest_download_url.startswith(api_root):
            raise Exception(
                f"Latest download URL not from configured GitLab project for '{plugin_name}': {latest_download_url}")

        # --- Compute local zip path (initial complete artifact) ---
        url_name = unquote(os.path.basename(
            urlparse(initial_download_url).path))
        if not url_name.lower().endswith(".zip"):
            url_name = f"{plugin_name}_complete.zip"
        zip_path_initial = os.path.join(self.UPDATE_DIR, url_name)

        # --- Download initial zip ---
        try:
            self.download_update(initial_download_url, zip_path_initial)
            print(
                f"[DEBUG] Downloaded zip for '{plugin_name}' to: {zip_path_initial} (exists={os.path.exists(zip_path_initial)})")
        except Exception as e:
            raise Exception(
                f"Failed to download '{plugin_name}' initial zip: {e}")

        # --- Prepare target plugin path ---
        plugin_target = os.path.join(self.PLUGINS_DIR, plugin_name)
        os.makedirs(plugin_target, exist_ok=True)

        # --- Extract safely ---
        try:
            self.apply_zip(
                zip_path_initial, plugin_target, zip_mode="install")
            print(
                f"[DEBUG] Extracted '{plugin_name}' initial artifact into: {plugin_target}")
            try:
                print(
                    f"[DEBUG] Listing of {plugin_target}: {os.listdir(plugin_target)}")
            except Exception as le:
                print(f"[DEBUG] Could not list {plugin_target}: {le}")
        except Exception as e:
            raise Exception(
                f"Failed to extract '{plugin_name}' initial zip: {e}")

        # --- Overlay latest partial artifact (optional) ---
        zip_path_latest = None
        overlay_applied = False
        if latest_download_url and latest_download_url != initial_download_url:
            try:
                latest_name = unquote(os.path.basename(
                    urlparse(latest_download_url).path))
                if not latest_name.lower().endswith(".zip"):
                    latest_name = f"{plugin_name}_latest.zip"
                zip_path_latest = os.path.join(self.UPDATE_DIR, latest_name)
                self.download_update(latest_download_url, zip_path_latest)
                self.apply_zip(
                    zip_path_latest, plugin_target, zip_mode="install")
                overlay_applied = True
                print(
                    f"[DEBUG] Overlay applied for '{plugin_name}' from {zip_path_latest}")
            except Exception as e:
                raise Exception(
                    f"Failed to overlay latest partial artifact for '{plugin_name}': {e}")

        # --- Local manifest path ---
        local_manifest_path = os.path.join(plugin_target, "manifest.json")
        local_manifest = {}
        if os.path.exists(local_manifest_path):
            try:
                with open(local_manifest_path, "r", encoding="utf-8") as f:
                    local_manifest = json.load(f) or {}
            except json.JSONDecodeError:
                local_manifest = {}

        # --- Minimum viable manifest ---
        zip_for_version = zip_path_latest or zip_path_initial
        inferred_version = self._parse_version_from_zip_name(
            zip_for_version, plugin_name) or remote_manifest.get("current_version")
        local_manifest.setdefault("system_name", plugin_name)
        if inferred_version:
            local_manifest["version"] = inferred_version
        local_manifest["installed"] = True
        local_manifest.setdefault("enabled", False)
        local_manifest.setdefault("name", local_manifest.get(
            "name", plugin_name.replace("_", " ").title()))
        local_manifest.setdefault(
            "description", local_manifest.get("description", ""))

        # --- Persist dependencies for reverse lookup ---
        deps = remote_manifest.get(
            "dependencies") or remote_manifest.get("depends_on") or []
        if isinstance(deps, str):
            deps = [deps]
        deps = [d.strip() for d in deps if isinstance(d, str) and d.strip()]
        if deps:
            local_manifest["dependencies"] = deps

        # --- Resolve icon to local file ---
        icon_val = local_manifest.get(
            "icon") or remote_manifest.get("icon") or "icon.png"
        if isinstance(icon_val, str) and icon_val.lower().startswith("http"):
            icon_val = "icon.png"
        icon_abs = os.path.join(plugin_target, icon_val)
        if not os.path.exists(icon_abs):
            for cand in ("icon.png", "assets/icon.png", "images/icon.png", "static/icon.png", "logo.png"):
                p = os.path.join(plugin_target, cand)
                if os.path.exists(p):
                    icon_val = cand
                    break
        local_manifest["icon"] = str(icon_val).replace("\\", "/")

        merge_plugin_dashboard_fields_from_factory(
            local_manifest,
            _load_factory_manifest_dict(
                os.path.join(plugin_target, "factory_manifest.json")
            ),
        )

        # --- Persist manifest (flush + fsync) ---
        try:
            print(f"[DEBUG] Writing manifest at: {local_manifest_path}")
            with open(local_manifest_path, "w", encoding="utf-8") as f:
                json.dump(local_manifest, f, indent=4)
                f.flush()
                os.fsync(f.fileno())
            print(f"[DEBUG] Wrote manifest: {local_manifest_path}")
        except Exception as e:
            raise Exception(
                f"Failed to write local manifest for '{plugin_name}': {e}")

        # --- Run install.py install only for FRESH installs ---
        script_path = os.path.join(plugin_target, "install.py")
        if os.path.exists(script_path):
            try:
                result = subprocess.run(
                    [sys.executable, script_path, "install"],
                    check=True, capture_output=True, text=True
                )
                print(result.stdout)
                if result.stderr:
                    print("[STDERR]", result.stderr)
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Plugin install script failed: {e}")
                if e.stdout:
                    print("[STDOUT]", e.stdout)
                if e.stderr:
                    print("[STDERR]", e.stderr)
                raise

            # If we installed a complete base artifact and overlaid a newer
            # partial artifact, run the upgrade step too so DB migrations catch up.
            if overlay_applied:
                try:
                    result = subprocess.run(
                        [sys.executable, script_path, "upgrade"],
                        check=True, capture_output=True, text=True
                    )
                    print(result.stdout)
                    if result.stderr:
                        print("[STDERR]", result.stderr)
                except subprocess.CalledProcessError as e:
                    print(f"[ERROR] Plugin upgrade script failed: {e}")
                    if e.stdout:
                        print("[STDOUT]", e.stdout)
                    if e.stderr:
                        print("[STDERR]", e.stderr)
                    raise
        else:
            print(
                f"[WARN] No install.py found in plugin directory for {plugin_name}.")

        # --- Refresh PluginManager cache ---
        if hasattr(self.plugin_manager, "load_plugins"):
            try:
                self.plugin_manager.plugins = self.plugin_manager.load_plugins()
            except Exception as e:
                print(
                    f"[WARN] PluginManager refresh failed after installing '{plugin_name}': {e}")

    def _resolve_dependency_install_order(self, plugin_name: str) -> list:
        """
        Returns a list of plugin system_names in the order they should be installed:
        [dep1, dep2, ..., plugin_name]
        Performs DFS with cycle detection. Reads dependencies from remote manifests.
        """
        visited: Set[str] = set()
        temp_stack: Set[str] = set()
        order: list = []

        def dfs(name: str):
            n = (name or "").strip()
            if not n:
                raise Exception("Empty plugin name in dependency graph.")
            if n in temp_stack:
                raise Exception(
                    f"Cyclic dependency detected involving '{n}'.")
            if n in visited:
                return
            temp_stack.add(n)
            deps = self._get_remote_dependencies_safe(n)
            for d in deps:
                dfs(d)
            temp_stack.remove(n)
            visited.add(n)
            order.append(n)

        dfs(plugin_name)
        return order

    def _get_remote_dependencies_safe(self, plugin_name: str) -> list:
        """
        Fetches the remote manifest for plugin_name and returns a normalized list of dependencies.
        Accepts keys 'dependencies' or 'depends_on'. Returns [] on error.
        """
        try:
            man = self.get_plugin_manifest_remote(plugin_name) or {}
            deps = man.get("dependencies") or man.get("depends_on") or []
            if isinstance(deps, str):
                deps = [deps]
            deps = [d.strip()
                    for d in deps if isinstance(d, str) and d.strip()]
            # avoid self-dependency
            deps = [d for d in deps if d != plugin_name]
            return deps
        except Exception as e:
            print(
                f"[WARN] Could not load dependencies for '{plugin_name}': {e}")
            return []

    def check_and_download_new_plugins(self):
        print("Checking for new plugins...")
        remote_plugins = self.get_remote_plugin_list()
        installed_plugins = [plugin["system_name"]
                             for plugin in self.plugin_manager.get_all_plugins()]
        for plugin_name in remote_plugins:
            if plugin_name not in installed_plugins:
                print(
                    f"New plugin found: {plugin_name}. Downloading and installing...")
                try:
                    self.install_plugin(plugin_name)
                except Exception as e:
                    print(f"Failed to install plugin {plugin_name}: {e}")
            else:
                print(f"Plugin {plugin_name} is already installed.")

    # --- Legacy forced-update jobs (remote force_update flags are ignored) ---
    def check_for_forced_updates(self):
        """Remove any scheduled forced core job; we no longer honour manifest force_update."""
        try:
            if self.scheduler.get_job("forced_core_update"):
                self.scheduler.remove_job("forced_core_update")
        except Exception as e:
            print(f"[INFO] Forced core update job cleanup: {e}")

    def check_for_forced_plugin_updates(self):
        """Remove forced plugin jobs; manifest force_update is ignored."""
        try:
            for job in list(self.scheduler.get_jobs()):
                jid = getattr(job, "id", "") or ""
                if jid.startswith("forced_plugin_update_"):
                    self.scheduler.remove_job(jid)
        except Exception as e:
            print(f"[INFO] Forced plugin update job cleanup: {e}")


def run_plugin_database_install(system_name: str) -> tuple:
    """
    Import app.plugins.<system_name>.install and run install() (or upgrade() if no install).
    Idempotent for typical DDL. Skips cleanly when there is no install submodule.

    Returns:
        (True, message) on success or intentional skip
        (False, error_message) if install()/upgrade() raised
    """
    module_name = f"app.plugins.{system_name}.install"
    try:
        mod = importlib.import_module(module_name)
    except ImportError as e:
        return True, f"[plugins] No DB install for '{system_name}' ({e})"

    installer = getattr(mod, "install", None)
    if not callable(installer):
        installer = getattr(mod, "upgrade", None)
    if not callable(installer):
        return True, f"[plugins] No install()/upgrade() in '{system_name}.install'"

    try:
        installer()
        return True, f"[plugins] Database schema OK for '{system_name}'"
    except Exception as e:
        msg = str(e)
        print(f"[ERROR] Plugin '{system_name}' database install failed: {msg}")
        return False, msg


class Plugin:
    def __init__(self, system_name, plugins_dir='app/plugins'):
        self.system_name = system_name
        self.plugins_dir = plugins_dir
        self.plugin_path = os.path.join(self.plugins_dir, self.system_name)
        self.manifest_path = os.path.join(self.plugin_path, 'manifest.json')
        print(f"[DEBUG] Initialising Plugin object for '{self.system_name}'")
        self.manifest = self.get_manifest()
        if self.manifest:
            print(
                f"[DEBUG] Manifest loaded for '{self.system_name}': {self.manifest}")
        else:
            print(f"[DEBUG] No manifest found for '{self.system_name}'")

    def get_manifest(self):
        """Load the plugin's manifest file."""
        manifest_path = os.path.join(
            self.plugins_dir, self.system_name, 'manifest.json')
        if os.path.exists(manifest_path):
            try:
                with open(manifest_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data
            except json.JSONDecodeError as e:
                print(
                    f"[ERROR] Error decoding JSON for plugin '{self.system_name}': {e}")
            except Exception as e:
                print(
                    f"[ERROR] Failed to read manifest for plugin '{self.system_name}': {e}")
        else:
            print(
                f"[DEBUG] Manifest file does not exist for '{self.system_name}'")
        return None

    def save_manifest(self):
        """Save the current state of the plugin's manifest."""
        manifest_path = os.path.join(
            self.plugins_dir, self.system_name, 'manifest.json')
        print(
            f"[DEBUG] Saving manifest for '{self.system_name}' at '{manifest_path}'")
        try:
            os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(self.manifest, f, indent=4)
            print(
                f"[DEBUG] Manifest saved successfully for '{self.system_name}'")
        except Exception as e:
            print(
                f"[ERROR] Failed to save manifest for '{self.system_name}': {e}")

    def install(self):
        """Install the plugin using the factory manifest located in the plugin folder."""
        print(f"[DEBUG] Starting installation for plugin '{self.system_name}'")
        plugin_path = os.path.join(self.plugins_dir, self.system_name)

        # Ensure the plugin directory exists.
        if not os.path.exists(plugin_path):
            print(
                f"[DEBUG] Plugin directory '{plugin_path}' not found. Creating it.")
            os.makedirs(plugin_path, exist_ok=True)
        else:
            print(f"[DEBUG] Plugin directory '{plugin_path}' exists.")

        # If a manifest already exists, remove it to allow a fresh install.
        if os.path.exists(self.manifest_path):
            print(
                f"[DEBUG] Plugin manifest exists for '{self.system_name}'; re-installing by removing existing manifest.")
            try:
                os.remove(self.manifest_path)
                print(
                    f"[DEBUG] Existing manifest removed for '{self.system_name}'.")
            except Exception as e:
                error_msg = f"Failed to remove existing manifest: {e}"
                print(f"[ERROR] {error_msg}")
                return False, error_msg

        # Load the factory manifest from the plugin folder.
        factory_manifest_path = os.path.join(
            plugin_path, 'factory_manifest.json')
        print(
            f"[DEBUG] Looking for factory manifest at '{factory_manifest_path}'")
        if not os.path.exists(factory_manifest_path):
            error_msg = "Factory manifest not found."
            print(f"[ERROR] {error_msg}")
            return False, error_msg

        try:
            with open(factory_manifest_path, 'r', encoding='utf-8') as f:
                factory_manifest = json.load(f)
            print(
                f"[DEBUG] Factory manifest loaded for '{self.system_name}': {factory_manifest}")
        except json.JSONDecodeError as e:
            error_msg = f"Error reading factory manifest: {e}"
            print(f"[ERROR] {error_msg}")
            return False, error_msg
        except Exception as e:
            error_msg = f"Error reading factory manifest: {e}"
            print(f"[ERROR] {error_msg}")
            return False, error_msg

        # Initialize the plugin manifest from the factory manifest.
        self.manifest = factory_manifest

        # Ensure plugin is installed disabled + marked installed.
        self.manifest['enabled'] = False
        self.manifest['installed'] = True
        self.manifest.setdefault('update_available', False)

        print(
            f"[DEBUG] Setting 'enabled' to False for plugin '{self.system_name}'.")

        # Save the plugin manifest.
        self.save_manifest()

        ok, db_msg = run_plugin_database_install(self.system_name)
        print(f"[DEBUG] {db_msg}")
        if not ok:
            err = f"Database install failed for '{self.system_name}': {db_msg}"
            print(f"[ERROR] {err}")
            return False, err

        print(f"[DEBUG] Plugin '{self.system_name}' installed successfully.")
        return True, f"{self.system_name} installed successfully."

    def uninstall(self):
        """Uninstall the plugin by removing its manifest file."""
        print(f"[DEBUG] Uninstalling plugin '{self.system_name}'")
        if not os.path.exists(self.plugin_path):
            error_msg = "Plugin directory not found."
            print(f"[ERROR] {error_msg}")
            return False, error_msg
        try:
            if os.path.exists(self.manifest_path):
                os.remove(self.manifest_path)
                print(
                    f"[DEBUG] Removed manifest for plugin '{self.system_name}'")
            return True, f"{self.system_name} uninstalled successfully."
        except Exception as e:
            error_msg = f"Error uninstalling plugin: {e}"
            print(f"[ERROR] {error_msg}")
            return False, error_msg

    def enable(self):
        """Enable the plugin by setting 'enabled' to True and saving the manifest."""
        print(f"[DEBUG] Enabling plugin '{self.system_name}'")
        if not self.manifest:
            error_msg = "Manifest not found."
            print(f"[ERROR] {error_msg}")
            return False, error_msg
        self.manifest['enabled'] = True
        self.manifest['installed'] = True
        self.save_manifest()
        return True, f"{self.system_name} enabled successfully."

    def disable(self):
        """Disable the plugin by setting 'enabled' to False and saving the manifest."""
        print(f"[DEBUG] Disabling plugin '{self.system_name}'")
        if not self.manifest:
            error_msg = "Manifest not found."
            print(f"[ERROR] {error_msg}")
            return False, error_msg
        self.manifest['enabled'] = False
        self.save_manifest()
        return True, f"{self.system_name} disabled successfully."

    def get_settings(self):
        """Return the settings of the plugin, or an empty dictionary if not defined."""
        if not self.manifest or 'settings' not in self.manifest:
            print(
                f"[DEBUG] No settings found in manifest for '{self.system_name}'")
            return {}
        return self.manifest['settings']

    def save_settings(self, settings):
        """Save the provided settings to the plugin's manifest."""
        print(f"[DEBUG] Saving settings for plugin '{self.system_name}'")
        if not self.manifest:
            error_msg = "Manifest not found."
            print(f"[ERROR] {error_msg}")
            return False, error_msg
        self.manifest['settings'] = settings
        self.save_manifest()
        return True, "Settings saved successfully."

    def update_setting(self, setting_key, setting_value):
        """Update a setting if it's editable."""
        print(
            f"[DEBUG] Updating setting '{setting_key}' for plugin '{self.system_name}'")
        if not self.manifest or 'settings' not in self.manifest:
            error_msg = "Settings not found in the manifest."
            print(f"[ERROR] {error_msg}")
            return False, error_msg

        settings = self.manifest['settings']
        if setting_key not in settings:
            error_msg = f"Setting {setting_key} not found."
            print(f"[ERROR] {error_msg}")
            return False, error_msg

        setting = settings[setting_key]
        if not setting.get('editable', False):
            error_msg = f"Setting {setting_key} is not editable."
            print(f"[ERROR] {error_msg}")
            return False, error_msg

        setting['value'] = setting_value
        self.save_manifest()
        print(
            f"[DEBUG] Setting '{setting_key}' updated successfully for plugin '{self.system_name}'")
        return True, f"Setting {setting_key} updated successfully."

    @staticmethod
    def get_factory_manifest(plugin_path):
        """Load the factory manifest data from factory_manifest.json."""
        factory_manifest_path = os.path.join(
            plugin_path, "factory_manifest.json")
        print(
            f"[DEBUG] Loading factory manifest from '{factory_manifest_path}'")
        if os.path.exists(factory_manifest_path):
            try:
                with open(factory_manifest_path, "r", encoding="utf-8") as factory_manifest_file:
                    manifest_data = json.load(factory_manifest_file)
                    print(f"[DEBUG] Factory manifest data: {manifest_data}")
                    return manifest_data
            except (json.JSONDecodeError, IOError) as e:
                print(
                    f"[ERROR] Error reading factory_manifest.json for {plugin_path}: {e}")
        else:
            print(
                f"[DEBUG] Factory manifest not found at '{factory_manifest_path}'")
        return None


class PluginManager:
    """
    Discovers plugins under ``app/plugins/<folder>/`` and registers routes with the Flask app.

    **Module agents / JSON APIs:** URLs are ``/plugin/<system_name>/…``. Stateless JSON for Bearer
    clients should live under ``/plugin/<system_name>/api/…``; ``create_app`` lists those prefixes in
    ``_PLUGIN_ROUTE_LEVEL_JSON_API_PREFIXES`` so anonymous requests are not redirected to the login
    page. Use session JWTs from ``POST /api/login``; required claims are defined in ``app.auth_jwt``.
    """

    def __init__(self, plugins_dir='plugins'):
        """
        plugins_dir can be:
        - "plugins" (relative to app/objects.py directory)
        - an absolute path (recommended)
        """
        # Use the absolute path to avoid confusion.
        app_root = os.path.abspath(os.path.dirname(
            __file__))  # e.g. sparrow-erp/app

        # If caller passes an absolute path (like run.py does), respect it.
        if os.path.isabs(plugins_dir):
            self.plugins_dir = plugins_dir
        else:
            self.plugins_dir = os.path.join(app_root, plugins_dir)

        self.config_dir = os.path.join(app_root, 'config')
        print(
            f"[DEBUG] PluginManager initialized with plugins_dir: {self.plugins_dir}")

        # Loads manifest data for all plugins (initial cache; get_all_plugins() refreshes).
        self.plugins = self.load_plugins()

    def get_factory_manifest_by_name(self, plugin_name):
        """
        Return a minimal 'factory' descriptor for a plugin by name.
        Priority:
        1) Local manifest.json (repository field if present)
        2) factory_manifest.json inside the plugin folder
        3) Default to 'official'
        """
        plugin_folder = os.path.join(self.plugins_dir, plugin_name)
        # Try local manifest.json
        manifest_path = os.path.join(plugin_folder, 'manifest.json')
        if os.path.exists(manifest_path):
            try:
                with open(manifest_path, 'r') as f:
                    man = json.load(f)
                repo = man.get('repository')
                if repo:
                    return {"repository": repo}
            except json.JSONDecodeError:
                pass
        # Try local factory_manifest.json
        factory_manifest = self.get_factory_manifest(plugin_folder)
        if factory_manifest and isinstance(factory_manifest, dict):
            repo = factory_manifest.get('repository')
            if repo:
                return {"repository": repo}
        # Fallback
        return {"repository": "official"}

    def get_factory_manifest_full_by_name(self, plugin_name: str) -> dict:
        plugin_folder = os.path.join(self.plugins_dir, plugin_name)
        fm = self.get_factory_manifest(plugin_folder)
        return fm if isinstance(fm, dict) else {}

    def get_repository_for_plugin(self, plugin_name):
        """
        Convenience wrapper to return the repository string for a plugin:
        - 'official' or a base URL
        """
        data = self.get_factory_manifest_by_name(plugin_name)
        return (data or {}).get('repository', 'official')

    def load_plugin_modules(self):
        """
        Dynamically import and return a list of plugin modules.
        Each plugin is assumed to be a folder (with __init__.py) under self.plugins_dir.
        """
        plugin_modules = []
        if not os.path.exists(self.plugins_dir):
            print(f"[ERROR] Plugins folder does not exist: {self.plugins_dir}")
            return plugin_modules
        for plugin_folder in os.listdir(self.plugins_dir):
            folder_path = os.path.join(self.plugins_dir, plugin_folder)
            if not os.path.isdir(folder_path) or plugin_folder.startswith("__"):
                continue
            try:
                module = importlib.import_module(
                    f"app.plugins.{plugin_folder}")
                plugin_modules.append(module)
                print(f"[DEBUG] Imported plugin module: {plugin_folder}")
            except Exception as e:
                print(
                    f"[ERROR] Failed to import plugin module '{plugin_folder}': {e}")
        return plugin_modules

    def load_plugins(self):
        """
        Load plugin data from the plugins directory.
        For each plugin folder, if a manifest.json exists, load it.
        Otherwise, if a factory_manifest.json exists, load that and set 'enabled': False.
        """
        plugins = {}
        if not os.path.exists(self.plugins_dir):
            print(f"[ERROR] Plugins folder does not exist: {self.plugins_dir}")
            return plugins
        for plugin_folder in os.listdir(self.plugins_dir):
            folder_path = os.path.join(self.plugins_dir, plugin_folder)
            if not os.path.isdir(folder_path) or plugin_folder.startswith("__"):
                continue
            manifest_path = os.path.join(folder_path, 'manifest.json')
            if os.path.exists(manifest_path):
                try:
                    with open(manifest_path, 'r') as f:
                        manifest = json.load(f)
                except json.JSONDecodeError as e:
                    print(
                        f"[ERROR] Error decoding manifest for plugin '{plugin_folder}': {e}")
                    continue
            else:
                factory_manifest = self.get_factory_manifest(folder_path)
                if factory_manifest:
                    manifest = factory_manifest
                    manifest['enabled'] = False
                else:
                    print(
                        f"[DEBUG] No manifest found for plugin '{plugin_folder}'")
                    continue
            if 'allowed_roles' not in manifest:
                manifest['allowed_roles'] = []
            plugins[plugin_folder] = manifest
        return plugins

    def get_factory_manifest(self, plugin_path):
        """Retrieve the factory manifest for a plugin."""
        factory_manifest_path = os.path.join(
            plugin_path, 'factory_manifest.json')
        print(
            f"[DEBUG] Loading factory manifest from: {factory_manifest_path}")
        if os.path.exists(factory_manifest_path):
            try:
                with open(factory_manifest_path, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError as e:
                print(
                    f"[ERROR] Error decoding factory_manifest.json for {plugin_path}: {e}")
        return None

    def get_plugin_manifest(self, plugin_path):
        """Retrieve the manifest for a plugin."""
        manifest_path = os.path.join(plugin_path, 'manifest.json')
        print(f"[DEBUG] Loading plugin manifest from: {manifest_path}")
        if os.path.exists(manifest_path):
            try:
                return json.load(open(manifest_path, 'r'))
            except json.JSONDecodeError as e:
                print(
                    f"[ERROR] Error decoding manifest.json for {plugin_path}: {e}")
        return None

    def get_all_plugins(self):
        """
        Returns a list of all available plugins with their details.

        CHANGES:
        - Always reload manifests from disk (fixes stale UI + sitemap discovery).
        - Returns the FULL manifest dict (so public_sections/public_sitemaps survive),
          while still guaranteeing the legacy fields your UI expects.
        """
        plugin_list = []
        if not os.path.exists(self.plugins_dir):
            print(f"[ERROR] Plugins folder does not exist: {self.plugins_dir}")
            return plugin_list

        # Always reload from disk (no stale self.plugins)
        self.plugins = self.load_plugins() or {}

        print(f"[DEBUG] Scanning plugins in: {self.plugins_dir}")

        for plugin_folder, manifest in self.plugins.items():
            if not isinstance(manifest, dict):
                continue

            plugin_dir = os.path.join(self.plugins_dir, plugin_folder)
            manifest_path = os.path.join(plugin_dir, "manifest.json")
            installed = os.path.exists(manifest_path)

            # Start with FULL manifest so we keep custom keys (public_sitemaps, public_sections, etc.)
            data = dict(manifest)

            # Normalize system_name
            sys_name = (data.get("system_name")
                        or plugin_folder or "").strip() or plugin_folder
            data["system_name"] = sys_name

            # Back-compat guarantees for UI
            data["name"] = data.get("name") or plugin_folder
            data["description"] = data.get(
                "description") or "No description available."
            data["icon"] = data.get("icon") or "default-icon.png"
            data["version"] = data.get("version") or "Unknown"

            # Computed state fields
            data["installed"] = bool(installed)
            data["enabled"] = bool(data.get("enabled", False))
            data["update_available"] = bool(
                data.get("update_available", False))

            plugin_list.append(data)

        return plugin_list

    # Stable Bootstrap icon suffixes (no "bi-" prefix) for dashboard tiles when no image icon.
    _DASHBOARD_TILE_GLYPHS = (
        "puzzle-fill",
        "box-seam",
        "grid-3x3-gap",
        "layers-fill",
        "diagram-3-fill",
        "briefcase-fill",
        "heart-pulse",
        "cart3",
        "calendar3",
        "people-fill",
        "journal-richtext",
        "shield-check",
        "globe2",
        "camera-video-fill",
        "newspaper",
        "clock-history",
        "truck",
        "building",
        "clipboard2-pulse",
        "megaphone-fill",
        "graph-up-arrow",
        "folder2-open",
        "gear-wide-connected",
        "lightning-charge-fill",
    )

    def _dashboard_tile_style(self, system_name: str, manifest: dict) -> dict:
        """Deterministic accent colour + fallback icon for dashboard cards."""
        s = (manifest.get("dashboard_icon") or "").strip()
        while s:
            low = s.lower()
            if low.startswith("bi "):
                s = s[3:].strip()
            elif low.startswith("bi-"):
                s = s[3:].strip()
            else:
                break
        glyph = s
        h = int(
            hashlib.md5(
                (system_name or "").encode("utf-8"),
                usedforsecurity=False,
            ).hexdigest(),
            16,
        )
        if not glyph:
            glyph = self._DASHBOARD_TILE_GLYPHS[h % len(
                self._DASHBOARD_TILE_GLYPHS)]
        hue = h % 360
        return {"tile_glyph": glyph, "tile_hue": hue}

    def get_enabled_plugins(self):
        """
        Enabled plugins with dashboard metadata.

        Optional manifest keys:
        - dashboard_category: group on home (default "Modules")
        - dashboard_icon: Bootstrap icon, e.g. "bi-heart-pulse" or "heart-pulse"
        """
        plugins: list = []
        self.plugins = self.load_plugins() or {}

        for plugin_folder, manifest in self.plugins.items():
            if not isinstance(manifest, dict):
                continue
            if not manifest.get("enabled", False):
                continue
            sys_name = (
                manifest.get("system_name") or plugin_folder or ""
            ).strip() or plugin_folder
            extras = self._dashboard_tile_style(sys_name, manifest)
            category = (manifest.get("dashboard_category")
                        or "Modules").strip()
            if not category:
                category = "Modules"
            try:
                from app.permissions_registry import plugin_access_permission_id

                acc_perm = plugin_access_permission_id(manifest, sys_name)
            except Exception:
                acc_perm = (
                    (manifest.get("access_permission") or manifest.get(
                        "permission_required") or "").strip()
                    or f"{sys_name}.access"
                )
            plugins.append(
                {
                    "name": manifest.get("name", plugin_folder),
                    "system_name": sys_name,
                    "description": (manifest.get("description") or "").strip(),
                    "icon": manifest.get("icon") or "",
                    "dashboard_category": category,
                    "tile_glyph": extras["tile_glyph"],
                    "tile_hue": extras["tile_hue"],
                    "permission_required": manifest.get("permission_required"),
                    "access_permission": acc_perm,
                }
            )

        plugins.sort(
            key=lambda p: (p["dashboard_category"].lower(), p["name"].lower())
        )
        print(
            f"[DEBUG] Enabled plugins: {[p['system_name'] for p in plugins]}")
        return plugins

    def is_plugin_enabled(self, system_name):
        """
        Checks if the plugin is enabled based on its manifest.
        Returns True if enabled, False otherwise.
        """
        print(f"[DEBUG] Checking if plugin '{system_name}' is enabled.")
        plugins = self.load_plugins()
        if system_name not in plugins:
            raise ValueError(
                f"Plugin {system_name} not found. Available: {list(plugins.keys())}")
        enabled = plugins[system_name].get('enabled', False)
        print(f"[DEBUG] Plugin '{system_name}' enabled status: {enabled}")
        return enabled

    def install_plugin(self, plugin_name):
        """Install a plugin (using its factory manifest if not installed) and handle dependencies."""
        print(f"[DEBUG] Attempting to install plugin '{plugin_name}'")
        plugin_folder = os.path.join(self.plugins_dir, plugin_name)
        if not os.path.isdir(plugin_folder):
            raise ValueError(
                f"Plugin folder '{plugin_name}' not found in {self.plugins_dir}.")
        self.plugins = self.load_plugins()  # Update plugin list.
        manifest_path = os.path.join(plugin_folder, 'manifest.json')
        if os.path.exists(manifest_path):
            print(
                f"[DEBUG] Plugin '{plugin_name}' is already installed; running database install if present.")
            ok, db_msg = run_plugin_database_install(plugin_name)
            print(f"[DEBUG] {db_msg}")
            if not ok:
                raise Exception(
                    f"Plugin database install failed for '{plugin_name}': {db_msg}")
        else:
            from app.objects import Plugin  # Adjust the import path as needed.
            plugin = Plugin(plugin_name, plugins_dir=self.plugins_dir)
            install_status, install_message = plugin.install()
            if not install_status:
                raise Exception(
                    f"Plugin installation failed: {install_message}")
            print(f"[DEBUG] Plugin '{plugin_name}' installed successfully.")
        dependency_handler_path = os.path.join(os.path.abspath(
            os.path.dirname(__file__)), "dependency_handler.py")
        print(
            f"[DEBUG] Running dependency handler at: {dependency_handler_path}")
        if os.path.exists(dependency_handler_path):
            try:
                subprocess.check_call(
                    [sys.executable, dependency_handler_path])
                print(f"[DEBUG] Dependency handler executed successfully.")
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Dependency handler failed: {e}")
                raise
        else:
            print(
                f"[WARNING] Dependency handler not found at: {dependency_handler_path}")
        self.plugins = self.load_plugins()  # Reload plugins.
        print(f"[DEBUG] Plugin '{plugin_name}' installation complete.")

        return True, f"Plugin {plugin_name} installed successfully."

    def check_dependencies(self, system_name):
        """
        Checks whether the specified plugin has any missing dependencies.
        Returns a tuple (can_enable, missing_dependency).
        """
        print(f"[DEBUG] Checking dependencies for plugin '{system_name}'")
        plugins = self.load_plugins()
        if system_name not in plugins:
            raise ValueError(f"Plugin {system_name} not found.")
        dependencies = plugins[system_name].get('dependencies', [])
        if not dependencies:
            print(f"[DEBUG] Plugin '{system_name}' has no dependencies.")
            return True, None
        missing = []
        for dependency in dependencies:
            dep_manifest_path = os.path.join(
                self.plugins_dir, dependency, 'manifest.json')
            if not os.path.exists(dep_manifest_path):
                print(
                    f"[DEBUG] Dependency '{dependency}' is missing for plugin '{system_name}'.")
                missing.append(dependency)
            else:
                print(
                    f"[DEBUG] Dependency '{dependency}' is present for plugin '{system_name}'. Enabling it.")
                self.enable_plugin(dependency)
        if missing:
            return False, missing
        return True, None

    def get_dependents(self, plugin_name: str) -> list:
        """
        Find which installed plugins depend on a given plugin.
        Scans all currently loaded plugins for 'dependencies' or 'depends_on'.

        Returns:
            List of system_names of dependent plugins.
        """
        print(f"[DEBUG] Looking for dependents of plugin '{plugin_name}'")

        dependents = []

        # Load current plugins (fresh scan)
        plugins = self.load_plugins() or {}

        for sys_name, manifest in plugins.items():
            deps = manifest.get('dependencies') or manifest.get(
                'depends_on') or []

            # Normalize to list if string
            if isinstance(deps, str):
                deps = [deps]

            # Strip and filter empty entries
            deps = [d.strip()
                    for d in deps if isinstance(d, str) and d.strip()]

            if plugin_name in deps:
                dependents.append(sys_name)

        print(f"[DEBUG] Dependents for plugin '{plugin_name}': {dependents}")
        return dependents

    def uninstall_plugin(self, system_name: str) -> tuple:
        """
        Safely uninstall a plugin.

        Steps:
        - Skip if plugin is marked protected
        - Disable dependents that require this plugin (cascade)
        - Mark target plugin as disabled
        - Run install.py uninstall if present
        - Remove plugin directory
        - Refresh plugin cache

        Args:
            system_name: The system_name of the plugin to uninstall.

        Returns:
            Tuple[bool, str]: (success flag, summary message)
        """
        import os
        import json
        import shutil
        import subprocess
        import sys

        plugin_dir = os.path.join(self.plugins_dir, system_name)
        manifest_path = os.path.join(plugin_dir, "manifest.json")

        if not os.path.isdir(plugin_dir):
            return False, f"{system_name} is not installed."

        # Load manifest to check for 'protected' flag
        manifest = {}
        if os.path.exists(manifest_path):
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f) or {}
            except json.JSONDecodeError:
                manifest = {}

        if manifest.get("protected"):
            print(
                f"[WARN] Attempted uninstall of protected plugin '{system_name}' – operation blocked.")
            return False, f"{system_name} is protected and cannot be uninstalled."

        # 1) Disable dependents (cascade)
        disabled = []
        disable_errors = []
        for dep_name in self.get_dependents(system_name):
            ok, msg = self.disable_plugin(dep_name, cascade=True)
            if ok:
                print(
                    f"[DEBUG] Disabled dependent '{dep_name}' because it depends on '{system_name}'.")
                disabled.append(dep_name)
            else:
                print(
                    f"[WARN] Failed to disable dependent '{dep_name}': {msg}")
                disable_errors.append(f"{dep_name}: {msg}")

        # 2) Mark target disabled in manifest (if exists) before deletion
        if os.path.exists(manifest_path):
            try:
                m = manifest or {}
                m["enabled"] = False
                m["installed"] = False
                with open(manifest_path, "w", encoding="utf-8") as f:
                    json.dump(m, f, indent=4)
            except Exception as e:
                print(
                    f"[WARN] Failed to mark '{system_name}' disabled in manifest before deletion: {e}")

        # 3) Run install.py uninstall if present
        script_path = os.path.join(plugin_dir, "install.py")
        if os.path.exists(script_path):
            try:
                result = subprocess.run(
                    [sys.executable, script_path, "uninstall"],
                    check=True, capture_output=True, text=True
                )
                print(result.stdout)
                if result.stderr:
                    print("[STDERR]", result.stderr)
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Plugin uninstall script failed: {e}")
                if e.stdout:
                    print("[STDOUT]", e.stdout)
                if e.stderr:
                    print("[STDERR]", e.stderr)
        else:
            print(
                f"[WARN] No install.py found in plugin directory for {system_name}.")

        # 4) Remove the plugin directory
        try:
            print(f"[DEBUG] Removing plugin directory '{plugin_dir}'...")
            shutil.rmtree(plugin_dir, ignore_errors=False)
            print(
                f"[DEBUG] Plugin '{system_name}' directory removed successfully.")
        except Exception as e:
            return False, f"Failed to remove plugin files for '{system_name}': {e}"

        # 5) Refresh cache
        try:
            self.plugins = self.load_plugins()
        except Exception as e:
            print(
                f"[WARN] Failed to refresh plugin cache after uninstall: {e}")

        # 6) Build clear summary message
        msg_parts = [f"{system_name} has been uninstalled."]
        if disabled:
            msg_parts.append(f"Disabled dependents: {', '.join(disabled)}.")
        if disable_errors:
            msg_parts.append(
                f"Errors disabling dependents: {', '.join(disable_errors)}.")
        msg = " ".join(msg_parts)

        print(f"[DEBUG] Uninstall summary: {msg}")
        return True, msg

    def enable_plugin(self, system_name):
        """
        Enable a plugin. Creates manifest.json from factory manifest if missing.
        Enforces that all dependencies are installed AND enabled first.
        """
        import os
        import json
        import subprocess

        print(f"[DEBUG] Enabling plugin '{system_name}'")

        plugin_folder = os.path.join(self.plugins_dir, system_name)
        manifest_path = os.path.join(plugin_folder, 'manifest.json')
        factory_path = os.path.join(plugin_folder, 'factory_manifest.json')

        # 0) Seed manifest if missing
        manifest = {}
        if not os.path.exists(manifest_path):
            print(
                f"[DEBUG] No manifest.json for '{system_name}'. Seeding from factory manifest if present...")
            seed = {}
            if os.path.exists(factory_path):
                try:
                    with open(factory_path, 'r', encoding='utf-8') as f:
                        seed = json.load(f) or {}
                except json.JSONDecodeError:
                    print(
                        f"[WARN] factory_manifest.json for '{system_name}' is invalid JSON; proceeding with defaults.")
                    seed = {}

            manifest = {
                "system_name": system_name,
                "name": (seed.get("name") or system_name.replace("_", " ").title()).strip(),
                "description": str(seed.get("description", "")).strip(),
                "version": seed.get("current_version") or seed.get("version") or "0.0.0",
                "icon": "icon.png",
                "enabled": False,
                "installed": True,
                "update_available": False
            }

            # Prefer a local-looking icon if seed provided one
            seed_icon = seed.get("icon")
            if isinstance(seed_icon, str) and seed_icon and not seed_icon.lower().startswith("http"):
                manifest["icon"] = seed_icon.strip()

            # Resolve icon to existing file
            for cand in (manifest["icon"], "icon.png", "assets/icon.png", "images/icon.png", "static/icon.png", "logo.png"):
                p = os.path.join(plugin_folder, cand)
                if os.path.exists(p):
                    manifest["icon"] = cand.replace("\\", "/")
                    break

            merge_plugin_dashboard_fields_from_factory(manifest, seed)

            os.makedirs(plugin_folder, exist_ok=True)
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=4)
            print(f"[DEBUG] Seeded manifest.json for '{system_name}'.")
        # Load existing manifest if not freshly seeded
        if not manifest:
            try:
                with open(manifest_path, 'r', encoding='utf-8') as f:
                    manifest = json.load(f) or {}
            except (json.JSONDecodeError, FileNotFoundError):
                print(
                    f"[WARN] Corrupt or missing manifest.json for '{system_name}'. Re-seeding minimal manifest.")
                manifest = {
                    "system_name": system_name,
                    "name": system_name.replace("_", " ").title(),
                    "description": "",
                    "version": "0.0.0",
                    "icon": "icon.png",
                    "enabled": False,
                    "installed": True,
                    "update_available": False
                }

        factory_data = _load_factory_manifest_dict(factory_path)
        merge_plugin_dashboard_fields_from_factory(manifest, factory_data)

        already_enabled = bool(manifest.get("enabled", False))
        ok, db_msg = run_plugin_database_install(system_name)
        print(f"[DEBUG] {db_msg}")
        if not ok:
            if already_enabled:
                print(
                    f"[WARN] Database install failed for already-enabled plugin '{system_name}'; "
                    "fix the database and use Repair or re-enable after fixing."
                )
            else:
                return False, f"Database setup failed for {system_name}: {db_msg}"

        # Already enabled? Persist manifest so dashboard_category / dashboard_icon stay synced from factory.
        if manifest.get('enabled', False):
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=4)
            print(f"[DEBUG] Plugin '{system_name}' is already enabled.")
            return True, f"{system_name} is already enabled."

        # 1) Enforce dependencies
        deps = manifest.get("dependencies") or manifest.get("depends_on") or []
        missing = []
        disabled = []

        if deps:
            installed_plugins = {p.get("system_name"): p for p in (
                self.get_all_plugins() or [])}
            for dep in deps:
                meta = installed_plugins.get(dep)
                if not meta or not meta.get("installed"):
                    missing.append(dep)
                elif not meta.get("enabled"):
                    disabled.append(dep)

        if missing:
            return False, f"Cannot enable {system_name}: missing required plugin(s): {', '.join(missing)}."

        if disabled:
            return False, f"Cannot enable {system_name}: required plugin(s) disabled: {', '.join(disabled)}. Enable them first."

        # 2) Flip flags and persist
        manifest['enabled'] = True
        manifest['installed'] = True
        manifest['update_available'] = False

        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=4)

        # 3) Optionally run dependency handler or plugin-specific post-enable
        try:
            print(
                f"[DEBUG] Running dependency handler after enabling plugin '{system_name}'...")
            dependency_handler_path = os.path.join(os.path.abspath(
                os.path.dirname(__file__)), "dependency_handler.py")
            if os.path.exists(dependency_handler_path):
                subprocess.check_call(
                    [sys.executable, dependency_handler_path])
        except subprocess.CalledProcessError as e:
            print(
                f"[ERROR] Dependency handler failed after enabling plugin '{system_name}': {e}")

        print(f"[DEBUG] Plugin '{system_name}' enabled successfully.")
        return True, f"{system_name} enabled successfully."

    def disable_plugin(self, system_name: str, cascade: bool = True) -> tuple:
        """
        Disable a plugin and optionally cascade to its dependents.

        Args:
            system_name: The system_name of the plugin to disable.
            cascade: If True, recursively disables all dependent plugins.

        Returns:
            Tuple[bool, str]: (success flag, message)
        """
        print(f"[DEBUG] Disabling plugin '{system_name}'")

        plugin_folder = os.path.join(self.plugins_dir, system_name)
        manifest_path = os.path.join(plugin_folder, 'manifest.json')

        if not os.path.exists(manifest_path):
            return False, f"{system_name} is not installed."

        # Load manifest
        with open(manifest_path, 'r', encoding='utf-8') as f:
            try:
                manifest = json.load(f)
            except json.JSONDecodeError:
                manifest = {}

        if not manifest.get('enabled', False):
            print(f"[DEBUG] Plugin '{system_name}' already disabled.")

        # Disable plugin
        manifest['enabled'] = False
        manifest['update_available'] = True
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=4)
        print(f"[DEBUG] Plugin '{system_name}' disabled.")

        # Cascade disable dependents if requested
        if cascade:
            dependents = self.get_dependents(system_name)
            for dep in dependents:
                dep_folder = os.path.join(self.plugins_dir, dep)
                dep_manifest_path = os.path.join(dep_folder, 'manifest.json')
                if os.path.exists(dep_manifest_path):
                    with open(dep_manifest_path, 'r', encoding='utf-8') as f:
                        try:
                            dep_manifest = json.load(f)
                        except json.JSONDecodeError:
                            dep_manifest = {}
                    if dep_manifest.get('enabled', False):
                        # Recursively disable dependent plugin
                        self.disable_plugin(dep, cascade=True)
                        print(f"[DEBUG] Disabled dependent plugin: {dep}")

        return True, f"{system_name} and its dependents have been disabled."

    def update_plugin_manifest(self, plugin_name, update_flag):
        """Update the 'update_available' flag in the plugin's manifest."""
        plugin_folder = os.path.join(self.plugins_dir, plugin_name)
        manifest = self.get_plugin_manifest(plugin_folder)
        if manifest:
            manifest['update_available'] = update_flag
            self.save_plugin_manifest(plugin_name, manifest)

    def save_plugin_manifest(self, plugin_name, plugin_manifest):
        manifest_path = os.path.join(
            self.plugins_dir, plugin_name, 'manifest.json')
        print(
            f"[DEBUG] Saving updated manifest for plugin '{plugin_name}' at '{manifest_path}'")
        with open(manifest_path, 'w') as f:
            json.dump(plugin_manifest, f, indent=4)

    def get_core_manifest(self):
        """Load the core module's manifest file (config/manifest.json)."""
        core_manifest_path = os.path.join(self.config_dir, 'manifest.json')
        print(f"[DEBUG] Loading core manifest from '{core_manifest_path}'")
        if os.path.exists(core_manifest_path):
            with open(core_manifest_path, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    print(
                        f"[ERROR] Error reading core manifest at {core_manifest_path}.")
        else:
            print(f"[ERROR] Core manifest not found at {core_manifest_path}.")
        return None

    def get_core_manifest_path(self):
        """Return the core manifest file path."""
        core_manifest_path = os.path.join(self.config_dir, 'manifest.json')
        if os.path.exists(core_manifest_path):
            return core_manifest_path
        else:
            print(f"[ERROR] Core manifest not found at {core_manifest_path}.")
        return core_manifest_path

    def update_plugin_settings(self, plugin_system_name, form_data):
        """Update the plugin settings from the form data."""
        plugin_folder = os.path.join(self.plugins_dir, plugin_system_name)
        manifest_path = os.path.join(plugin_folder, 'manifest.json')
        print(f"[DEBUG] Updating settings for plugin '{plugin_system_name}'")
        if os.path.exists(manifest_path):
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
            for key, value in form_data.items():
                if key in manifest.get('settings', {}):
                    manifest['settings'][key]['value'] = value
                    print(f"[DEBUG] Updated setting '{key}' to '{value}'")
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=4)
        else:
            print(f"[ERROR] Manifest not found for {plugin_system_name}")

    def get_plugin(self, system_name):
        """Get a specific plugin by its system name (UID)."""
        from app.objects import Plugin  # Adjust as needed.
        plugin = Plugin(system_name)
        return plugin.get_manifest()

    def get_available_permissions(self):
        """Flat list of permission IDs (module access + declared features)."""
        try:
            from app.permissions_registry import (
                collect_permission_catalog,
                permission_ids_for_catalog,
            )

            return permission_ids_for_catalog(
                collect_permission_catalog(self)
            )
        except Exception as e:
            print(f"[WARN] get_available_permissions fallback: {e}")
            perms = set()
            for manifest in self.plugins.values():
                if not isinstance(manifest, dict):
                    continue
                perm = manifest.get("permission_required")
                if perm:
                    perms.add(perm)
            return sorted(perms)

    def register_admin_routes(self, app):
        """
        Dynamically register admin routes for all plugins.
        """
        # Refresh plugin cache before registering
        self.plugins = self.load_plugins() or {}

        for plugin_name, manifest in self.plugins.items():
            try:
                module = importlib.import_module(
                    f"app.plugins.{plugin_name}.routes")
                if hasattr(module, "get_blueprints"):
                    for blueprint in module.get_blueprints():
                        app.register_blueprint(blueprint)
                    print(
                        f"[DEBUG] Admin routes registered for plugin: {plugin_name} "
                        f"({len(module.get_blueprints())} blueprint(s))"
                    )
                elif hasattr(module, "get_blueprint"):
                    blueprint = module.get_blueprint()
                    app.register_blueprint(blueprint)
                    print(
                        f"[DEBUG] Admin routes registered for plugin: {plugin_name}")
                else:
                    print(
                        f"[DEBUG] Plugin {plugin_name} does not provide get_blueprint().")
            except Exception as e:
                print(f"[ERROR] Error registering plugin {plugin_name}: {e}")

    def register_public_routes(self, app):
        """
        Dynamically register public routes for all plugins.
        """
        # Refresh plugin cache before registering
        self.plugins = self.load_plugins() or {}

        for plugin_name, manifest in self.plugins.items():
            try:
                module = importlib.import_module(
                    f"app.plugins.{plugin_name}.routes")
                if hasattr(module, "get_public_blueprints"):
                    blueprints = module.get_public_blueprints()
                    for blueprint in blueprints:
                        app.register_blueprint(blueprint)
                    print(
                        f"[DEBUG] Public routes registered for plugin: {plugin_name} "
                        f"({len(blueprints)} blueprint(s))"
                    )
                elif hasattr(module, "get_public_blueprint"):
                    blueprint = module.get_public_blueprint()
                    app.register_blueprint(blueprint)
                    print(
                        f"[DEBUG] Public routes registered for plugin: {plugin_name}")
                else:
                    print(
                        f"[DEBUG] Plugin {plugin_name} does not provide get_public_blueprint().")
            except Exception as e:
                print(f"[ERROR] Error registering plugin {plugin_name}: {e}")

        # Contractor theme: session (set on login) + Jinja + POST /contractor-ui/set-theme
        try:
            from app.contractor_ui_theme import register_contractor_public_theme

            register_contractor_public_theme(app)
        except ImportError:
            pass
