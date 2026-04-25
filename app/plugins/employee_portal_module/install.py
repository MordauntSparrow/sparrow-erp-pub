"""
Employee Portal install/upgrade: all table definitions and logic in this file.
Ensures ep_migrations, ep_messages, ep_todos, ep_push_subscriptions, ep_notification_prefs. No external db/*.sql.
Run from repo root: python app/plugins/employee_portal_module/install.py install
Or from plugin dir: python install.py install
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
PLUGIN_DIR = HERE.parent
PLUGINS_DIR = PLUGIN_DIR.parent
APP_ROOT = PLUGINS_DIR.parent
PROJECT_ROOT = APP_ROOT.parent
for p in (str(PROJECT_ROOT), str(APP_ROOT), str(PLUGIN_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)
from app.objects import get_db_connection  # noqa: E402

MIGRATIONS_TABLE = "ep_migrations"
SQL_CREATE_EP_SETTINGS = """
CREATE TABLE IF NOT EXISTS ep_settings (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  setting_key VARCHAR(128) NOT NULL UNIQUE,
  setting_value TEXT,
  updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_EP_PUSH_SUBSCRIPTIONS = """
CREATE TABLE IF NOT EXISTS ep_push_subscriptions (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  endpoint VARCHAR(768) NOT NULL,
  p256dh TEXT NOT NULL,
  auth_secret VARCHAR(255) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  user_agent VARCHAR(512) DEFAULT NULL,
  UNIQUE KEY uq_ep_push_endpoint (endpoint(512)),
  KEY idx_ep_push_contractor (contractor_id),
  CONSTRAINT fk_ep_push_contractor FOREIGN KEY (contractor_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_EP_NOTIFICATION_PREFS = """
CREATE TABLE IF NOT EXISTS ep_notification_prefs (
  contractor_id INT NOT NULL PRIMARY KEY,
  push_enabled TINYINT(1) NOT NULL DEFAULT 1,
  updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_ep_notif_prefs_contractor FOREIGN KEY (contractor_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# Optional allow-lists: intersection(role, contractor) ∩ known keys. No row = no restriction at that layer.
SQL_CREATE_EP_PORTAL_ROLE_MODULE_ACCESS = """
CREATE TABLE IF NOT EXISTS ep_portal_role_module_access (
  role_id INT NOT NULL PRIMARY KEY,
  allowed_modules_json JSON NOT NULL,
  updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_ep_prma_role FOREIGN KEY (role_id)
    REFERENCES roles(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_EP_PORTAL_CONTRACTOR_MODULE_ACCESS = """
CREATE TABLE IF NOT EXISTS ep_portal_contractor_module_access (
  contractor_id INT NOT NULL PRIMARY KEY,
  allowed_modules_json JSON NOT NULL,
  updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_ep_pcma_contractor FOREIGN KEY (contractor_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

MODULE_TABLES = [
    MIGRATIONS_TABLE,
    "ep_messages",
    "ep_todos",
    "ep_settings",
    "ep_push_subscriptions",
    "ep_notification_prefs",
    "ep_portal_contractor_module_access",
    "ep_portal_role_module_access",
]

# Full CREATE TABLE statements (all in this file)
SQL_CREATE_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS ep_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_EP_MESSAGES = """
CREATE TABLE IF NOT EXISTS ep_messages (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  source_module VARCHAR(64) NOT NULL,
  subject VARCHAR(255) NOT NULL,
  body TEXT,
  read_at DATETIME DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ep_messages_contractor (contractor_id),
  KEY idx_ep_messages_read (contractor_id, read_at),
  CONSTRAINT fk_ep_messages_contractor FOREIGN KEY (contractor_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_EP_TODOS = """
CREATE TABLE IF NOT EXISTS ep_todos (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  source_module VARCHAR(64) NOT NULL,
  title VARCHAR(255) NOT NULL,
  link_url VARCHAR(512) DEFAULT NULL,
  due_date DATE DEFAULT NULL,
  completed_at DATETIME DEFAULT NULL,
  reference_type VARCHAR(64) DEFAULT NULL,
  reference_id VARCHAR(128) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ep_todos_contractor (contractor_id),
  KEY idx_ep_todos_pending (contractor_id, completed_at),
  KEY idx_ep_todos_reference (source_module, reference_type, reference_id),
  CONSTRAINT fk_ep_todos_contractor FOREIGN KEY (contractor_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

CREATES = [
    SQL_CREATE_MIGRATIONS,
    SQL_CREATE_EP_MESSAGES,
    SQL_CREATE_EP_TODOS,
    SQL_CREATE_EP_SETTINGS,
    SQL_CREATE_EP_PUSH_SUBSCRIPTIONS,
    SQL_CREATE_EP_NOTIFICATION_PREFS,
    SQL_CREATE_EP_PORTAL_ROLE_MODULE_ACCESS,
    SQL_CREATE_EP_PORTAL_CONTRACTOR_MODULE_ACCESS,
]


def _run_sql(conn, sql):
    for stmt in [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]:
        cur = conn.cursor()
        try:
            cur.execute(stmt)
        finally:
            cur.close()
    conn.commit()


def _column_exists(conn, table, column):
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM `{}` LIKE %s".format(table), (column,))
        return bool(cur.fetchone())
    finally:
        cur.close()


def _ensure_audit_columns(conn):
    """Add sent_by_user_id, updated_at to ep_messages; created_by_user_id, updated_at to ep_todos."""
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "ep_messages", "sent_by_user_id"):
            cur.execute(
                "ALTER TABLE ep_messages ADD COLUMN sent_by_user_id INT NULL DEFAULT NULL AFTER read_at"
            )
        if not _column_exists(conn, "ep_messages", "updated_at"):
            cur.execute(
                "ALTER TABLE ep_messages ADD COLUMN updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP AFTER created_at"
            )
        if not _column_exists(conn, "ep_todos", "created_by_user_id"):
            cur.execute(
                "ALTER TABLE ep_todos ADD COLUMN created_by_user_id INT NULL DEFAULT NULL AFTER contractor_id"
            )
        if not _column_exists(conn, "ep_todos", "updated_at"):
            cur.execute(
                "ALTER TABLE ep_todos ADD COLUMN updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP AFTER created_at"
            )
        if not _column_exists(conn, "ep_messages", "deleted_at"):
            cur.execute(
                "ALTER TABLE ep_messages ADD COLUMN deleted_at DATETIME NULL DEFAULT NULL AFTER updated_at"
            )
        if not _column_exists(conn, "ep_todos", "reference_type"):
            cur.execute(
                "ALTER TABLE ep_todos ADD COLUMN reference_type VARCHAR(64) NULL DEFAULT NULL AFTER completed_at"
            )
        if not _column_exists(conn, "ep_todos", "reference_id"):
            cur.execute(
                "ALTER TABLE ep_todos ADD COLUMN reference_id VARCHAR(128) NULL DEFAULT NULL AFTER reference_type"
            )
        conn.commit()
    finally:
        cur.close()


def _ensure_users_contractor_id_column(conn):
    """PRD: nullable link from core users to tb_contractors for unified portal identity."""
    if not _column_exists(conn, "users", "contractor_id"):
        cur = conn.cursor()
        try:
            cur.execute(
                "ALTER TABLE users ADD COLUMN contractor_id INT NULL DEFAULT NULL AFTER support_access_enabled"
            )
            conn.commit()
        finally:
            cur.close()
    cur = conn.cursor()
    try:
        cur.execute(
            "SHOW INDEX FROM users WHERE Key_name = %s",
            ("uq_users_contractor_id",),
        )
        if not cur.fetchone():
            try:
                cur.execute(
                    "CREATE UNIQUE INDEX uq_users_contractor_id ON users (contractor_id)"
                )
                conn.commit()
            except Exception:
                conn.rollback()
    finally:
        cur.close()
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE %s", ("tb_contractors",))
        if not cur.fetchone():
            return
        cur.execute(
            """
            SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users'
              AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = 'fk_users_contractor_id'
            """
        )
        if cur.fetchone():
            return
        try:
            cur.execute(
                """
                ALTER TABLE users
                ADD CONSTRAINT fk_users_contractor_id
                FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id)
                ON DELETE SET NULL ON UPDATE CASCADE
                """
            )
            conn.commit()
        except Exception:
            conn.rollback()
    finally:
        cur.close()


def ensure_tables(conn):
    for sql in CREATES:
        _run_sql(conn, sql)
    _ensure_audit_columns(conn)
    _ensure_users_contractor_id_column(conn)


def install():
    """Ensure all MODULE_TABLES exist (idempotent)."""
    conn = get_db_connection()
    try:
        ensure_tables(conn)
    finally:
        conn.close()


def upgrade():
    """Ensure all MODULE_TABLES exist (same as install, idempotent)."""
    install()


def uninstall(drop_data: bool = False):
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
    import argparse
    parser = argparse.ArgumentParser(
        description="Employee Portal Module Installer")
    parser.add_argument("command", choices=["install", "upgrade", "uninstall"])
    parser.add_argument("--drop-data", action="store_true")
    args = parser.parse_args()
    if args.command == "install":
        install()
    elif args.command == "upgrade":
        upgrade()
    else:
        uninstall(drop_data=args.drop_data)
