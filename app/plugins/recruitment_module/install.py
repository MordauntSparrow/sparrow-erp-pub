"""
Recruitment module: job roles, openings, applicants, applications, form templates, stage rules.
python app/plugins/recruitment_module/install.py install
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
for p in (str(HERE.parent.parent.parent), str(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)
from app.objects import get_db_connection  # noqa: E402

MIGRATIONS_TABLE = "rec_migrations"
MODULE_TABLES = [
    MIGRATIONS_TABLE,
    "rec_job_roles",
    "rec_openings",
    "rec_form_templates",
    "rec_opening_form_rules",
    "rec_applicants",
    "rec_applications",
    "rec_application_tasks",
    "rec_prehire_document_requests",
    "rec_prehire_document_uploads",
]

SQL_CREATE_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS rec_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_JOB_ROLES = """
CREATE TABLE IF NOT EXISTS rec_job_roles (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  slug VARCHAR(191) NOT NULL,
  description TEXT,
  department VARCHAR(128) DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_rec_role_slug (slug)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_OPENINGS = """
CREATE TABLE IF NOT EXISTS rec_openings (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  job_role_id INT NOT NULL,
  title VARCHAR(255) NOT NULL,
  slug VARCHAR(191) NOT NULL,
  summary TEXT,
  description TEXT,
  status ENUM('draft','open','closed') NOT NULL DEFAULT 'draft',
  published_at DATETIME DEFAULT NULL,
  closes_at DATE DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_rec_opening_slug (slug),
  KEY idx_rec_opening_status (status),
  KEY idx_rec_opening_role (job_role_id),
  CONSTRAINT fk_rec_opening_role FOREIGN KEY (job_role_id) REFERENCES rec_job_roles(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_FORM_TEMPLATES = """
CREATE TABLE IF NOT EXISTS rec_form_templates (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  purpose ENUM('survey','assessment') NOT NULL DEFAULT 'survey',
  schema_json JSON NOT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_OPENING_FORM_RULES = """
CREATE TABLE IF NOT EXISTS rec_opening_form_rules (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  opening_id INT NOT NULL,
  trigger_stage VARCHAR(64) NOT NULL,
  form_template_id INT NOT NULL,
  auto_assign TINYINT(1) NOT NULL DEFAULT 1,
  sort_order INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_rec_rule_opening (opening_id),
  KEY idx_rec_rule_stage (opening_id, trigger_stage),
  CONSTRAINT fk_rec_rule_opening FOREIGN KEY (opening_id) REFERENCES rec_openings(id) ON DELETE CASCADE,
  CONSTRAINT fk_rec_rule_template FOREIGN KEY (form_template_id) REFERENCES rec_form_templates(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_APPLICANTS = """
CREATE TABLE IF NOT EXISTS rec_applicants (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255) NOT NULL,
  name VARCHAR(255) NOT NULL,
  phone VARCHAR(64) DEFAULT NULL,
  password_hash VARCHAR(255) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_rec_applicant_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_APPLICATIONS = """
CREATE TABLE IF NOT EXISTS rec_applications (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  opening_id INT NOT NULL,
  applicant_id INT NOT NULL,
  stage VARCHAR(64) NOT NULL DEFAULT 'applied',
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  cv_path VARCHAR(512) DEFAULT NULL,
  cover_note TEXT,
  admin_notes TEXT,
  contractor_id INT DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_rec_app_opening (opening_id),
  KEY idx_rec_app_applicant (applicant_id),
  KEY idx_rec_app_stage (opening_id, stage),
  UNIQUE KEY uq_rec_app_opening_applicant (opening_id, applicant_id),
  CONSTRAINT fk_rec_app_opening FOREIGN KEY (opening_id) REFERENCES rec_openings(id) ON DELETE CASCADE,
  CONSTRAINT fk_rec_app_applicant FOREIGN KEY (applicant_id) REFERENCES rec_applicants(id) ON DELETE CASCADE,
  CONSTRAINT fk_rec_app_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_APPLICATION_TASKS = """
CREATE TABLE IF NOT EXISTS rec_application_tasks (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  application_id INT NOT NULL,
  form_template_id INT NOT NULL,
  status ENUM('pending','completed') NOT NULL DEFAULT 'pending',
  assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  completed_at DATETIME DEFAULT NULL,
  response_json JSON DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_rec_task_app (application_id),
  CONSTRAINT fk_rec_task_app FOREIGN KEY (application_id) REFERENCES rec_applications(id) ON DELETE CASCADE,
  CONSTRAINT fk_rec_task_template FOREIGN KEY (form_template_id) REFERENCES rec_form_templates(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_PREHIRE_DOCUMENT_REQUESTS = """
CREATE TABLE IF NOT EXISTS rec_prehire_document_requests (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  application_id INT NOT NULL,
  title VARCHAR(255) NOT NULL,
  description TEXT,
  request_type VARCHAR(32) NOT NULL DEFAULT 'other',
  required_by_date DATE DEFAULT NULL,
  status ENUM('pending','uploaded','approved','overdue','rejected') NOT NULL DEFAULT 'pending',
  admin_notes TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_rec_prehire_app (application_id),
  KEY idx_rec_prehire_app_status (application_id, status),
  CONSTRAINT fk_rec_prehire_app FOREIGN KEY (application_id) REFERENCES rec_applications(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_PREHIRE_DOCUMENT_UPLOADS = """
CREATE TABLE IF NOT EXISTS rec_prehire_document_uploads (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  request_id INT NOT NULL,
  file_path VARCHAR(512) NOT NULL,
  file_name VARCHAR(255) DEFAULT NULL,
  document_type VARCHAR(32) DEFAULT 'primary',
  uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_rec_prehire_upl_req (request_id),
  CONSTRAINT fk_rec_prehire_upl_req FOREIGN KEY (request_id) REFERENCES rec_prehire_document_requests(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

CREATES = [
    SQL_CREATE_MIGRATIONS,
    SQL_CREATE_JOB_ROLES,
    SQL_CREATE_OPENINGS,
    SQL_CREATE_FORM_TEMPLATES,
    SQL_CREATE_OPENING_FORM_RULES,
    SQL_CREATE_APPLICANTS,
    SQL_CREATE_APPLICATIONS,
    SQL_CREATE_APPLICATION_TASKS,
    SQL_CREATE_PREHIRE_DOCUMENT_REQUESTS,
    SQL_CREATE_PREHIRE_DOCUMENT_UPLOADS,
]


def _run_sql(conn, sql):
    for stmt in [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]:
        cur = conn.cursor()
        try:
            cur.execute(stmt)
        finally:
            cur.close()
    conn.commit()


def ensure_tables(conn):
    for sql in CREATES:
        _run_sql(conn, sql)


def _column_exists(conn, table, column):
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM `{}` LIKE %s".format(table), (column,))
        return bool(cur.fetchone())
    finally:
        cur.close()


def _ensure_recruitment_application_hr_columns(conn):
    """Pre-hire / HR conversion gate + GDPR-style applicant retention."""
    cur = conn.cursor()
    try:
        for col, defn in [
            (
                "hr_conversion_authorized",
                "TINYINT(1) NOT NULL DEFAULT 0 AFTER contractor_id",
            ),
            (
                "hr_conversion_authorized_at",
                "DATETIME DEFAULT NULL AFTER hr_conversion_authorized",
            ),
            (
                "hr_conversion_authorized_by_user_id",
                "CHAR(36) DEFAULT NULL AFTER hr_conversion_authorized_at",
            ),
            (
                "recruitment_data_purge_eligible_at",
                "DATE DEFAULT NULL AFTER hr_conversion_authorized_by_user_id",
            ),
            (
                "policies_acknowledged_at",
                "DATETIME DEFAULT NULL AFTER recruitment_data_purge_eligible_at",
            ),
        ]:
            if not _column_exists(conn, "rec_applications", col):
                cur.execute(
                    "ALTER TABLE rec_applications ADD COLUMN {} {}".format(col, defn)
                )
        conn.commit()
    finally:
        cur.close()


def _ensure_rec_job_roles_time_billing_columns(conn):
    """Link recruitment job roles to time billing roles + default wage card for hire."""
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "rec_job_roles", "time_billing_role_id"):
            cur.execute(
                "ALTER TABLE rec_job_roles ADD COLUMN time_billing_role_id INT DEFAULT NULL AFTER active"
            )
        if not _column_exists(conn, "rec_job_roles", "default_wage_rate_card_id"):
            cur.execute(
                "ALTER TABLE rec_job_roles ADD COLUMN default_wage_rate_card_id INT DEFAULT NULL AFTER time_billing_role_id"
            )
        conn.commit()
    finally:
        cur.close()


def install():
    conn = get_db_connection()
    try:
        ensure_tables(conn)
        _ensure_recruitment_application_hr_columns(conn)
        _ensure_rec_job_roles_time_billing_columns(conn)
    finally:
        conn.close()


def upgrade():
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
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["install", "upgrade", "uninstall"])
    p.add_argument("--drop-data", action="store_true")
    a = p.parse_args()
    if a.command == "install":
        install()
    elif a.command == "upgrade":
        upgrade()
    else:
        uninstall(drop_data=a.drop_data)
