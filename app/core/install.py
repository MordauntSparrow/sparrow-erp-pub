"""
Core platform database DDL (single-tenant).

Run via ``python app/core/install.py install|upgrade`` or Railway preDeploy
(:func:`app.objects.run_install_upgrade_scripts`), same as other ``install.py`` scripts.

References:
- PRD: ``docs/prd/core-integrations-accounting-prd.md``
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
APP_ROOT = HERE.parent
PROJECT_ROOT = APP_ROOT.parent
for p in (str(PROJECT_ROOT), str(APP_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from app.objects import get_db_connection  # noqa: E402


SQL_CORE_INTEGRATION_CONNECTIONS = """
CREATE TABLE IF NOT EXISTS core_integration_connections (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  provider VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'disconnected',
  encrypted_tokens MEDIUMTEXT NULL,
  provider_org_id VARCHAR(255) NULL,
  provider_org_label VARCHAR(512) NULL,
  connected_by_user_id CHAR(36) NULL,
  connected_at TIMESTAMP NULL DEFAULT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  last_error TEXT NULL,
  last_api_success_at DATETIME NULL,
  refresh_token_expires_at DATETIME NULL,
  UNIQUE KEY uq_core_int_provider (provider),
  KEY idx_core_int_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CORE_INTEGRATION_SETTINGS = """
CREATE TABLE IF NOT EXISTS core_integration_settings (
  id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
  default_provider VARCHAR(32) NOT NULL DEFAULT 'none',
  auto_draft_invoice TINYINT(1) NOT NULL DEFAULT 0,
  auto_draft_trigger VARCHAR(64) NOT NULL DEFAULT 'crm_quote_accepted',
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CORE_INTEGRATION_OAUTH_CLIENTS = """
CREATE TABLE IF NOT EXISTS core_integration_oauth_clients (
  provider VARCHAR(32) NOT NULL PRIMARY KEY,
  client_id VARCHAR(512) NULL,
  encrypted_client_secret MEDIUMTEXT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CORE_INTEGRATION_EVENTS = """
CREATE TABLE IF NOT EXISTS core_integration_events (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  event_type VARCHAR(64) NOT NULL,
  provider VARCHAR(32) NULL,
  message VARCHAR(512) NULL,
  user_id CHAR(36) NULL,
  ip VARCHAR(45) NULL,
  user_agent VARCHAR(512) NULL,
  meta JSON NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_core_int_ev_created (created_at),
  KEY idx_core_int_ev_provider (provider)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def ensure_tables(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(SQL_CORE_INTEGRATION_CONNECTIONS)
        cur.execute(SQL_CORE_INTEGRATION_SETTINGS)
        cur.execute(SQL_CORE_INTEGRATION_OAUTH_CLIENTS)
        cur.execute(SQL_CORE_INTEGRATION_EVENTS)
        cur.execute(
            "INSERT IGNORE INTO core_integration_settings (id, default_provider, auto_draft_invoice, auto_draft_trigger) "
            "VALUES (1, 'none', 0, 'crm_quote_accepted')"
        )
        conn.commit()
        print("[core] Integration tables ensured (core_integration_*).")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def install() -> None:
    conn = get_db_connection()
    try:
        ensure_tables(conn)
    finally:
        conn.close()


def upgrade() -> None:
    install()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Core platform DB install/upgrade")
    p.add_argument("command", choices=("install", "upgrade"))
    args = p.parse_args()
    if args.command == "install":
        install()
    else:
        upgrade()
    print("[core] Done.")
