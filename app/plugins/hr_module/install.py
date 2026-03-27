"""
HR module install/upgrade: all table definitions and logic in this file.
Ensures hr_migrations, hr_staff_details, hr_document_requests, hr_document_uploads. No external db/*.sql.
Run from repo root: python app/plugins/hr_module/install.py install
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

MIGRATIONS_TABLE = "hr_migrations"
MODULE_TABLES = [MIGRATIONS_TABLE, "hr_staff_details", "hr_employee_documents", "hr_document_requests", "hr_document_uploads"]

SQL_CREATE_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS hr_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_HR_STAFF_DETAILS = """
CREATE TABLE IF NOT EXISTS hr_staff_details (
  contractor_id INT NOT NULL PRIMARY KEY,
  phone VARCHAR(64) DEFAULT NULL,
  address_line1 VARCHAR(255) DEFAULT NULL,
  address_line2 VARCHAR(255) DEFAULT NULL,
  postcode VARCHAR(32) DEFAULT NULL,
  emergency_contact_name VARCHAR(255) DEFAULT NULL,
  emergency_contact_phone VARCHAR(64) DEFAULT NULL,
  date_of_birth DATE DEFAULT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_hrsd_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_HR_EMPLOYEE_DOCUMENTS = """
CREATE TABLE IF NOT EXISTS hr_employee_documents (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  category VARCHAR(64) NOT NULL DEFAULT 'general',
  title VARCHAR(255) NOT NULL,
  file_path VARCHAR(512) NOT NULL,
  file_name VARCHAR(255) DEFAULT NULL,
  notes TEXT,
  uploaded_by_user_id CHAR(36) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_hed_contractor (contractor_id),
  KEY idx_hed_category (contractor_id, category),
  CONSTRAINT fk_hed_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_HR_DOCUMENT_REQUESTS = """
CREATE TABLE IF NOT EXISTS hr_document_requests (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  title VARCHAR(255) NOT NULL,
  description TEXT,
  required_by_date DATE DEFAULT NULL,
  status ENUM('pending','uploaded','approved','overdue','rejected') NOT NULL DEFAULT 'pending',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_hrdr_contractor (contractor_id),
  KEY idx_hrdr_status (contractor_id, status),
  CONSTRAINT fk_hrdr_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_HR_DOCUMENT_UPLOADS = """
CREATE TABLE IF NOT EXISTS hr_document_uploads (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  request_id INT NOT NULL,
  file_path VARCHAR(512) NOT NULL,
  file_name VARCHAR(255) DEFAULT NULL,
  uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_hrdu_request (request_id),
  CONSTRAINT fk_hrdu_request FOREIGN KEY (request_id) REFERENCES hr_document_requests(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

CREATES = [
    SQL_CREATE_MIGRATIONS,
    SQL_CREATE_HR_STAFF_DETAILS,
    SQL_CREATE_HR_EMPLOYEE_DOCUMENTS,
    SQL_CREATE_HR_DOCUMENT_REQUESTS,
    SQL_CREATE_HR_DOCUMENT_UPLOADS,
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


def _ensure_hr_columns(conn):
    """Add PLAN columns: staff_details (licence, right to work, DBS, contract); requests (request_type, approve/reject); uploads (document_type)."""
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "hr_staff_details", "date_of_birth"):
            cur.execute(
                "ALTER TABLE hr_staff_details ADD COLUMN date_of_birth DATE DEFAULT NULL AFTER emergency_contact_phone"
            )
        # hr_staff_details: driving licence
        for col, defn in [
            ("driving_licence_number", "VARCHAR(64) DEFAULT NULL AFTER date_of_birth"),
            ("driving_licence_expiry", "DATE DEFAULT NULL AFTER driving_licence_number"),
            ("driving_licence_document_path", "VARCHAR(512) DEFAULT NULL AFTER driving_licence_expiry"),
        ]:
            if not _column_exists(conn, "hr_staff_details", col):
                cur.execute("ALTER TABLE hr_staff_details ADD COLUMN {} {}".format(col, defn))
        # right to work
        for col, defn in [
            ("right_to_work_type", "VARCHAR(64) DEFAULT NULL AFTER driving_licence_document_path"),
            ("right_to_work_expiry", "DATE DEFAULT NULL AFTER right_to_work_type"),
            ("right_to_work_document_path", "VARCHAR(512) DEFAULT NULL AFTER right_to_work_expiry"),
        ]:
            if not _column_exists(conn, "hr_staff_details", col):
                cur.execute("ALTER TABLE hr_staff_details ADD COLUMN {} {}".format(col, defn))
        # DBS
        for col, defn in [
            ("dbs_level", "VARCHAR(64) DEFAULT NULL AFTER right_to_work_document_path"),
            ("dbs_number", "VARCHAR(64) DEFAULT NULL AFTER dbs_level"),
            ("dbs_expiry", "DATE DEFAULT NULL AFTER dbs_number"),
            ("dbs_document_path", "VARCHAR(512) DEFAULT NULL AFTER dbs_expiry"),
        ]:
            if not _column_exists(conn, "hr_staff_details", col):
                cur.execute("ALTER TABLE hr_staff_details ADD COLUMN {} {}".format(col, defn))
        # contract
        for col, defn in [
            ("contract_type", "VARCHAR(64) DEFAULT NULL AFTER dbs_document_path"),
            ("contract_start", "DATE DEFAULT NULL AFTER contract_type"),
            ("contract_end", "DATE DEFAULT NULL AFTER contract_start"),
            ("contract_document_path", "VARCHAR(512) DEFAULT NULL AFTER contract_end"),
        ]:
            if not _column_exists(conn, "hr_staff_details", col):
                cur.execute("ALTER TABLE hr_staff_details ADD COLUMN {} {}".format(col, defn))
        # Role / organisation (admin-editable)
        for col, defn in [
            ("job_title", "VARCHAR(128) DEFAULT NULL AFTER contract_document_path"),
            ("department", "VARCHAR(128) DEFAULT NULL AFTER job_title"),
            ("manager_contractor_id", "INT DEFAULT NULL AFTER department"),
        ]:
            if not _column_exists(conn, "hr_staff_details", col):
                cur.execute("ALTER TABLE hr_staff_details ADD COLUMN {} {}".format(col, defn))
        # hr_document_requests: request_type, approve/reject
        for col, defn in [
            ("request_type", "VARCHAR(32) DEFAULT 'other' AFTER status"),
            ("approved_at", "DATETIME DEFAULT NULL AFTER request_type"),
            ("approved_by_user_id", "CHAR(36) DEFAULT NULL AFTER approved_at"),
            ("rejected_at", "DATETIME DEFAULT NULL AFTER approved_by_user_id"),
            ("rejected_by_user_id", "CHAR(36) DEFAULT NULL AFTER rejected_at"),
            ("admin_notes", "TEXT DEFAULT NULL AFTER rejected_by_user_id"),
        ]:
            if not _column_exists(conn, "hr_document_requests", col):
                cur.execute("ALTER TABLE hr_document_requests ADD COLUMN {} {}".format(col, defn))
        # hr_document_uploads: document_type
        if not _column_exists(conn, "hr_document_uploads", "document_type"):
            cur.execute("ALTER TABLE hr_document_uploads ADD COLUMN document_type VARCHAR(32) DEFAULT 'primary' AFTER file_name")
        # Ensure status ENUM includes 'rejected' (for existing DBs)
        try:
            cur.execute(
                "ALTER TABLE hr_document_requests MODIFY COLUMN status "
                "ENUM('pending','uploaded','approved','overdue','rejected') NOT NULL DEFAULT 'pending'"
            )
        except Exception:
            pass
        conn.commit()
    finally:
        cur.close()


def _ensure_hr_staff_manager_fk(conn):
    """FK: manager must reference an existing contractor; ON DELETE SET NULL."""
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "hr_staff_details", "manager_contractor_id"):
            return
        cur.execute(
            """
            SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'hr_staff_details'
              AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = 'fk_hrsd_manager'
            """
        )
        if cur.fetchone():
            return
        cur.execute(
            "ALTER TABLE hr_staff_details ADD CONSTRAINT fk_hrsd_manager "
            "FOREIGN KEY (manager_contractor_id) REFERENCES tb_contractors(id) ON DELETE SET NULL"
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()


def _migrate_user_id_columns_to_char36(conn):
    """
    Core users.id is CHAR(36) (UUID). Older HR installs used INT for *_user_id — fix type.
    """
    pairs = [
        ("hr_document_requests", "approved_by_user_id"),
        ("hr_document_requests", "rejected_by_user_id"),
        ("hr_employee_documents", "uploaded_by_user_id"),
    ]
    cur = conn.cursor()
    try:
        for table, col in pairs:
            if not _column_exists(conn, table, col):
                continue
            cur.execute(
                """
                SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
                """,
                (table, col),
            )
            row = cur.fetchone()
            if not row:
                continue
            dt = (row[0] or "").lower()
            if dt in ("int", "bigint", "smallint", "mediumint", "tinyint"):
                cur.execute(
                    "ALTER TABLE `{}` MODIFY COLUMN `{}` CHAR(36) DEFAULT NULL".format(table, col)
                )
        conn.commit()
    finally:
        cur.close()


def _backfill_hr_staff_shell_rows(conn):
    """Ensure each tb_contractors row has an hr_staff_details shell (same person, extended HR fields)."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO hr_staff_details (contractor_id)
            SELECT c.id FROM tb_contractors c
            LEFT JOIN hr_staff_details h ON h.contractor_id = c.id
            WHERE h.contractor_id IS NULL
            """
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()


def ensure_tables(conn):
    for sql in CREATES:
        _run_sql(conn, sql)
    _ensure_hr_columns(conn)
    _ensure_hr_staff_manager_fk(conn)
    _migrate_user_id_columns_to_char36(conn)
    _backfill_hr_staff_shell_rows(conn)


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
    parser = argparse.ArgumentParser(description="HR Module Installer")
    parser.add_argument("command", choices=["install", "upgrade", "uninstall"])
    parser.add_argument("--drop-data", action="store_true")
    args = parser.parse_args()
    if args.command == "install":
        install()
    elif args.command == "upgrade":
        upgrade()
    else:
        uninstall(drop_data=args.drop_data)
