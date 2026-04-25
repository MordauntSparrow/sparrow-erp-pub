"""
Compliance audit module — DB schema (exports, PIN, login audit, scheduled jobs).

Run: python app/plugins/compliance_audit_module/install.py upgrade
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PLUGINS_DIR = HERE.parent
APP_ROOT = PLUGINS_DIR.parent
PROJECT_ROOT = APP_ROOT.parent
for p in (str(PROJECT_ROOT), str(APP_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from app.objects import get_db_connection  # noqa: E402

MODULE_TABLES = [
    "compliance_export_log",
    "user_compliance_pin_hash",
    "compliance_pin_audit",
    "compliance_login_audit",
    "compliance_scheduled_export_jobs",
    "compliance_evidence_matters",
]


def _run_sql(conn, sql: str) -> None:
    for stmt in [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]:
        cur = conn.cursor()
        try:
            cur.execute(stmt)
        finally:
            cur.close()
    conn.commit()


def _column_exists(conn, table: str, column: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1 FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
            """,
            (table, column),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()


def _ensure_export_log_columns(conn) -> None:
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "compliance_export_log", "trigger_type"):
            cur.execute(
                "ALTER TABLE compliance_export_log ADD COLUMN trigger_type VARCHAR(16) NOT NULL DEFAULT 'manual'"
            )
            conn.commit()
        if not _column_exists(conn, "compliance_export_log", "stored_path"):
            cur.execute(
                "ALTER TABLE compliance_export_log ADD COLUMN stored_path VARCHAR(1024) NULL"
            )
            conn.commit()
        cur.execute(
            """
            SELECT IS_NULLABLE FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'compliance_export_log' AND COLUMN_NAME = 'user_id'
            """
        )
        row = cur.fetchone()
        if row and row[0] == "NO":
            cur.execute("ALTER TABLE compliance_export_log MODIFY user_id CHAR(36) NULL")
            conn.commit()
    except Exception as e:
        print(f"[compliance_audit_module] export_log alter: {e}")
    finally:
        cur.close()


SQL_EXPORT_LOG = """
CREATE TABLE IF NOT EXISTS compliance_export_log (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id CHAR(36) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  export_format VARCHAR(16) NOT NULL,
  scope_json LONGTEXT NOT NULL,
  row_count INT NOT NULL DEFAULT 0,
  ip_address VARCHAR(45) DEFAULT NULL,
  pin_step_up_ok TINYINT(1) NOT NULL DEFAULT 0,
  file_hash CHAR(64) DEFAULT NULL,
  generator_version VARCHAR(32) DEFAULT NULL,
  trigger_type VARCHAR(16) NOT NULL DEFAULT 'manual',
  stored_path VARCHAR(1024) NULL,
  KEY idx_cel_user_created (user_id, created_at),
  KEY idx_cel_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_PIN_HASH = """
CREATE TABLE IF NOT EXISTS user_compliance_pin_hash (
  user_id CHAR(36) NOT NULL PRIMARY KEY,
  pin_hash VARCHAR(255) NOT NULL,
  failed_attempts INT NOT NULL DEFAULT 0,
  locked_until DATETIME DEFAULT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_PIN_AUDIT = """
CREATE TABLE IF NOT EXISTS compliance_pin_audit (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id CHAR(36) NOT NULL,
  success TINYINT(1) NOT NULL DEFAULT 0,
  ip_address VARCHAR(45) DEFAULT NULL,
  detail VARCHAR(255) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_cpa_user_created (user_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_LOGIN_AUDIT = """
CREATE TABLE IF NOT EXISTS compliance_login_audit (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  user_id CHAR(36) NULL,
  contractor_id INT NULL,
  username_hash VARCHAR(64) NULL,
  success TINYINT(1) NOT NULL DEFAULT 0,
  channel VARCHAR(32) NOT NULL DEFAULT 'web',
  ip_address VARCHAR(45) NULL,
  user_agent VARCHAR(512) NULL,
  KEY idx_cla_time (occurred_at),
  KEY idx_cla_user (user_id, occurred_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_EVIDENCE_MATTERS = """
CREATE TABLE IF NOT EXISTS compliance_evidence_matters (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  reference_code VARCHAR(64) NOT NULL,
  title VARCHAR(255) NOT NULL,
  note TEXT NULL,
  filters_json LONGTEXT NOT NULL,
  created_by CHAR(36) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_cem_ref (reference_code),
  KEY idx_cem_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_SCHEDULED_JOBS = """
CREATE TABLE IF NOT EXISTS compliance_scheduled_export_jobs (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  label VARCHAR(128) NOT NULL,
  run_hour_utc TINYINT NOT NULL DEFAULT 6,
  lookback_days INT NOT NULL DEFAULT 1,
  row_cap INT NOT NULL DEFAULT 8000,
  domains_json TEXT NOT NULL,
  export_format VARCHAR(16) NOT NULL DEFAULT 'zip',
  redaction_profile VARCHAR(32) NOT NULL DEFAULT 'standard',
  last_run_on_date DATE NULL,
  last_run_at DATETIME NULL,
  last_status VARCHAR(64) NULL,
  last_error TEXT NULL,
  last_file_path VARCHAR(1024) NULL,
  last_file_hash CHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

CREATES = [
    SQL_EXPORT_LOG,
    SQL_PIN_HASH,
    SQL_PIN_AUDIT,
    SQL_LOGIN_AUDIT,
    SQL_SCHEDULED_JOBS,
    SQL_EVIDENCE_MATTERS,
]


def _ensure_scheduled_row_cap(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1 FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'compliance_scheduled_export_jobs'
            """
        )
        if not cur.fetchone():
            return
        if not _column_exists(conn, "compliance_scheduled_export_jobs", "row_cap"):
            cur.execute(
                "ALTER TABLE compliance_scheduled_export_jobs ADD COLUMN row_cap INT NOT NULL DEFAULT 8000 AFTER lookback_days"
            )
            conn.commit()
    except Exception as e:
        print(f"[compliance_audit_module] scheduled jobs alter: {e}")
    finally:
        cur.close()


def install() -> None:
    conn = get_db_connection()
    try:
        for sql in CREATES:
            _run_sql(conn, sql)
        _ensure_export_log_columns(conn)
        _ensure_scheduled_row_cap(conn)
        print("[compliance_audit_module] Schema OK:", ", ".join(MODULE_TABLES))
    finally:
        conn.close()


def upgrade() -> None:
    install()


def uninstall(drop_data: bool = False) -> None:
    if not drop_data:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        for t in reversed(MODULE_TABLES):
            cur.execute(f"DROP TABLE IF EXISTS `{t}`")
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Compliance audit module installer")
    p.add_argument("command", nargs="?", default="upgrade", choices=["install", "upgrade", "uninstall"])
    p.add_argument("--drop-data", action="store_true")
    a = p.parse_args()
    if a.command == "uninstall":
        uninstall(drop_data=a.drop_data)
    else:
        install()
