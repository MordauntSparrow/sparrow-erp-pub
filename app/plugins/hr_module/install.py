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
MODULE_TABLES = [
    MIGRATIONS_TABLE,
    "hr_staff_details",
    "hr_employee_documents",
    "hr_document_requests",
    "hr_document_uploads",
    "hr_onboarding_pack_items",
    "hr_onboarding_packs",
    "dbs_status_check_log",
    "hcpc_register_check_log",
    "hr_appraisal",
    "hr_appraisal_goal",
    "hr_appraisal_event_log",
]

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

SQL_CREATE_HR_ONBOARDING_PACKS = """
CREATE TABLE IF NOT EXISTS hr_onboarding_packs (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  pack_key VARCHAR(64) NOT NULL,
  label VARCHAR(255) NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_hop_key (pack_key),
  KEY idx_hop_sort (sort_order, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_HR_ONBOARDING_PACK_ITEMS = """
CREATE TABLE IF NOT EXISTS hr_onboarding_pack_items (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  pack_id INT NOT NULL,
  request_type VARCHAR(32) NOT NULL DEFAULT 'other',
  title VARCHAR(255) NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  KEY idx_hopi_pack (pack_id, sort_order),
  CONSTRAINT fk_hopi_pack FOREIGN KEY (pack_id) REFERENCES hr_onboarding_packs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_DBS_STATUS_CHECK_LOG = """
CREATE TABLE IF NOT EXISTS dbs_status_check_log (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  checked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status_code VARCHAR(64) DEFAULT NULL,
  result_type VARCHAR(64) DEFAULT NULL,
  channel ENUM('manual','scheduled') NOT NULL DEFAULT 'manual',
  checker_user_id CHAR(36) DEFAULT NULL,
  checker_label VARCHAR(255) DEFAULT NULL,
  http_status SMALLINT DEFAULT NULL,
  error_message TEXT,
  KEY idx_dscl_contractor (contractor_id, checked_at),
  CONSTRAINT fk_dscl_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_HR_APPRAISAL = """
CREATE TABLE IF NOT EXISTS hr_appraisal (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  manager_contractor_id INT DEFAULT NULL,
  cycle_label VARCHAR(128) NOT NULL DEFAULT 'Annual appraisal',
  period_start DATE DEFAULT NULL,
  period_end DATE DEFAULT NULL,
  status ENUM('draft','open','complete') NOT NULL DEFAULT 'draft',
  employee_summary TEXT,
  manager_summary TEXT,
  employee_signed_at DATETIME DEFAULT NULL,
  manager_signed_at DATETIME DEFAULT NULL,
  attachment_path VARCHAR(512) DEFAULT NULL,
  created_by_user_id CHAR(36) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_hra_contractor (contractor_id, updated_at),
  KEY idx_hra_status (status, updated_at),
  CONSTRAINT fk_hra_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE,
  CONSTRAINT fk_hra_manager FOREIGN KEY (manager_contractor_id) REFERENCES tb_contractors(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_HR_APPRAISAL_GOAL = """
CREATE TABLE IF NOT EXISTS hr_appraisal_goal (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  appraisal_id INT NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  title VARCHAR(255) NOT NULL,
  description TEXT,
  employee_notes TEXT,
  manager_notes TEXT,
  KEY idx_hrag_appraisal (appraisal_id, sort_order),
  CONSTRAINT fk_hrag_appraisal FOREIGN KEY (appraisal_id) REFERENCES hr_appraisal(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_HR_APPRAISAL_EVENT_LOG = """
CREATE TABLE IF NOT EXISTS hr_appraisal_event_log (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  appraisal_id INT DEFAULT NULL,
  contractor_id INT DEFAULT NULL,
  event_type VARCHAR(64) NOT NULL,
  summary VARCHAR(512) NOT NULL,
  detail TEXT,
  channel VARCHAR(32) DEFAULT NULL,
  actor_user_id CHAR(36) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_hrael_appraisal (appraisal_id, created_at),
  KEY idx_hrael_contractor (contractor_id, created_at),
  KEY idx_hrael_type_date (event_type, created_at),
  CONSTRAINT fk_hrael_appraisal FOREIGN KEY (appraisal_id) REFERENCES hr_appraisal(id) ON DELETE SET NULL,
  CONSTRAINT fk_hrael_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_HCPC_REGISTER_CHECK_LOG = """
CREATE TABLE IF NOT EXISTS hcpc_register_check_log (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  checked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  result_type VARCHAR(64) DEFAULT NULL,
  channel VARCHAR(32) NOT NULL DEFAULT 'manual',
  checker_user_id CHAR(36) DEFAULT NULL,
  checker_label VARCHAR(255) DEFAULT NULL,
  http_status SMALLINT DEFAULT NULL,
  registration_number VARCHAR(16) DEFAULT NULL,
  profession_code VARCHAR(3) DEFAULT NULL,
  api_status_text VARCHAR(255) DEFAULT NULL,
  error_message TEXT,
  response_hash CHAR(64) DEFAULT NULL,
  KEY idx_hrhpc_contractor (contractor_id, checked_at),
  CONSTRAINT fk_hrhpc_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

CREATES = [
    SQL_CREATE_MIGRATIONS,
    SQL_CREATE_HR_STAFF_DETAILS,
    SQL_CREATE_HR_EMPLOYEE_DOCUMENTS,
    SQL_CREATE_HR_DOCUMENT_REQUESTS,
    SQL_CREATE_HR_DOCUMENT_UPLOADS,
    SQL_CREATE_HR_ONBOARDING_PACKS,
    SQL_CREATE_HR_ONBOARDING_PACK_ITEMS,
    SQL_CREATE_DBS_STATUS_CHECK_LOG,
    SQL_CREATE_HCPC_REGISTER_CHECK_LOG,
    SQL_CREATE_HR_APPRAISAL,
    SQL_CREATE_HR_APPRAISAL_GOAL,
    SQL_CREATE_HR_APPRAISAL_EVENT_LOG,
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
        # DBS Update Service (CRSC status check — read-only; see PRD)
        for col, defn in [
            ("dbs_update_service_subscribed", "TINYINT(1) NOT NULL DEFAULT 0 AFTER dbs_document_path"),
            ("dbs_certificate_ref", "VARCHAR(64) DEFAULT NULL AFTER dbs_update_service_subscribed"),
            ("dbs_update_consent_at", "DATETIME DEFAULT NULL AFTER dbs_certificate_ref"),
            ("dbs_last_check_at", "DATETIME DEFAULT NULL AFTER dbs_update_consent_at"),
            ("dbs_last_status_code", "VARCHAR(64) DEFAULT NULL AFTER dbs_last_check_at"),
            ("dbs_last_response_hash", "CHAR(64) DEFAULT NULL AFTER dbs_last_status_code"),
        ]:
            if not _column_exists(conn, "hr_staff_details", col):
                cur.execute("ALTER TABLE hr_staff_details ADD COLUMN {} {}".format(col, defn))
        # contract (after DBS document path / Update Service columns when present)
        contract_after = (
            "dbs_last_response_hash"
            if _column_exists(conn, "hr_staff_details", "dbs_last_response_hash")
            else "dbs_document_path"
        )
        for col, defn in [
            ("contract_type", "VARCHAR(64) DEFAULT NULL AFTER {}".format(contract_after)),
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
        # HPAC register (employer-recorded; see hr_module.hpac_register_check — no public API)
        for col, defn in [
            ("hpac_registration_number", "VARCHAR(64) DEFAULT NULL"),
            ("hpac_registered_on", "DATE DEFAULT NULL"),
            ("hpac_register_grade", "VARCHAR(64) DEFAULT NULL"),
            ("hpac_register_last_checked_at", "DATETIME DEFAULT NULL"),
            ("hpac_register_status", "VARCHAR(32) DEFAULT NULL"),
            ("hpac_register_check_notes", "TEXT DEFAULT NULL"),
        ]:
            if not _column_exists(conn, "hr_staff_details", col):
                cur.execute(
                    "ALTER TABLE hr_staff_details ADD COLUMN {} {}".format(col, defn)
                )
        # HCPC register (employer-recorded; Employer Check API via HCPC — see hcpc_register_check)
        for col, defn in [
            ("hcpc_registration_number", "VARCHAR(64) DEFAULT NULL"),
            ("hcpc_registered_on", "DATE DEFAULT NULL"),
            ("hcpc_register_profession", "VARCHAR(64) DEFAULT NULL"),
            ("hcpc_register_last_checked_at", "DATETIME DEFAULT NULL"),
            ("hcpc_register_status", "VARCHAR(32) DEFAULT NULL"),
            ("hcpc_register_check_notes", "TEXT DEFAULT NULL"),
        ]:
            if not _column_exists(conn, "hr_staff_details", col):
                cur.execute(
                    "ALTER TABLE hr_staff_details ADD COLUMN {} {}".format(col, defn)
                )
        # Arbitrary HR key/value pairs (JSON object), plus common UK medical regulators (manual / employer verify)
        for col, defn in [
            ("custom_fields_json", "LONGTEXT DEFAULT NULL"),
            ("gmc_number", "VARCHAR(32) DEFAULT NULL"),
            ("gmc_registered_on", "DATE DEFAULT NULL"),
            ("gmc_register_last_checked_at", "DATETIME DEFAULT NULL"),
            ("gmc_register_status", "VARCHAR(32) DEFAULT NULL"),
            ("gmc_register_check_notes", "TEXT DEFAULT NULL"),
            ("nmc_pin", "VARCHAR(32) DEFAULT NULL"),
            ("nmc_registered_on", "DATE DEFAULT NULL"),
            ("nmc_register_last_checked_at", "DATETIME DEFAULT NULL"),
            ("nmc_register_status", "VARCHAR(32) DEFAULT NULL"),
            ("nmc_register_check_notes", "TEXT DEFAULT NULL"),
            ("gphc_registration_number", "VARCHAR(32) DEFAULT NULL"),
            ("gphc_registered_on", "DATE DEFAULT NULL"),
            ("gphc_register_last_checked_at", "DATETIME DEFAULT NULL"),
            ("gphc_register_status", "VARCHAR(32) DEFAULT NULL"),
            ("gphc_register_check_notes", "TEXT DEFAULT NULL"),
        ]:
            if not _column_exists(conn, "hr_staff_details", col):
                cur.execute(
                    "ALTER TABLE hr_staff_details ADD COLUMN {} {}".format(col, defn)
                )
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


def _seed_hr_onboarding_packs_if_empty(conn, tenant_industries=None):
    """
    First-time seed of onboarding packs from built-in defaults (idempotent).

    Neutral packs apply to every tenant; industry-tagged packs are added when
    ``tenant_industries`` matches (from Core manifest via
    ``load_tenant_industries_for_install`` when None).
    """
    from app.organization_profile import tenant_matches_industry

    if tenant_industries is None:
        try:
            from app.plugins.hr_module.hr_industry_install import (
                load_tenant_industries_for_install,
            )

            tenant_industries = load_tenant_industries_for_install()
        except Exception:
            tenant_industries = ["medical"]

    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM hr_onboarding_packs")
        row = cur.fetchone()
        if row and int(row[0] or 0) > 0:
            return
        # Neutral baseline — all tenants
        builtin = {
            "office_standard": (
                "Office / standard",
                [
                    ("right_to_work", "Right to work evidence"),
                    ("dbs", "DBS certificate"),
                    ("profile_picture", "Profile photo for ID and systems"),
                ],
            ),
            "field_based": (
                "Field / mobile",
                [
                    ("driving_licence", "Driving licence"),
                    ("right_to_work", "Right to work evidence"),
                    ("dbs", "DBS certificate"),
                ],
            ),
            "minimal": (
                "Minimal (right to work + contract)",
                [
                    ("right_to_work", "Right to work evidence"),
                    ("contract", "Signed contract or written terms"),
                ],
            ),
        }
        if tenant_matches_industry(tenant_industries, "medical"):
            builtin["regulated_health_clinical"] = (
                "Clinical / regulated roles",
                [
                    ("right_to_work", "Right to work evidence"),
                    ("dbs", "DBS certificate"),
                    (
                        "other",
                        "Professional registration evidence (e.g. HCPC, GMC, NMC)",
                    ),
                ],
            )
        if tenant_matches_industry(tenant_industries, "security"):
            builtin["security_licence_hr"] = (
                "Security / guarding",
                [
                    ("right_to_work", "Right to work evidence"),
                    ("dbs", "DBS certificate (if required)"),
                    ("other", "Security licence evidence (e.g. SIA)"),
                ],
            )
        if tenant_matches_industry(tenant_industries, "hospitality"):
            builtin["hospitality_guest_facing"] = (
                "Hospitality / guest-facing",
                [
                    ("right_to_work", "Right to work evidence"),
                    ("profile_picture", "Uniform or ID photo"),
                ],
            )
        if tenant_matches_industry(tenant_industries, "cleaning"):
            builtin["cleaning_facilities"] = (
                "Cleaning / facilities",
                [
                    ("right_to_work", "Right to work evidence"),
                    ("dbs", "DBS certificate (if required)"),
                ],
            )
        sort_main = 0
        for pack_key, (label, items) in builtin.items():
            cur.execute(
                """
                INSERT INTO hr_onboarding_packs (pack_key, label, sort_order, active)
                VALUES (%s, %s, %s, 1)
                """,
                (pack_key, label[:255], sort_main),
            )
            pid = cur.lastrowid
            sort_main += 10
            for i, (rt, title) in enumerate(items):
                cur.execute(
                    """
                    INSERT INTO hr_onboarding_pack_items (pack_id, request_type, title, sort_order)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (int(pid), (rt or "other")[:32], (title or "")[:255], i * 10),
                )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
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
    _seed_hr_onboarding_packs_if_empty(conn)


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
