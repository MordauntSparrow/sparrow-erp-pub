"""
Incident reporting module — schema and idempotent install.

Run from repo root:
  python app/plugins/incident_reporting_module/install.py install
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PLUGINS_DIR = HERE.parent
APP_ROOT = PLUGINS_DIR.parent
PROJECT_ROOT = APP_ROOT.parent
for p in (str(PROJECT_ROOT), str(APP_ROOT), str(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

from app.objects import get_db_connection  # noqa: E402

MIGRATIONS_TABLE = "incident_migrations"
# Drop order: children of ``incidents`` first, then ``incidents``, then independent tables.
MODULE_TABLES = [
    "incident_merge_suggestions",
    "incident_subscriptions",
    "incident_attachments",
    "incident_incident_factors",
    "incident_timeline_events",
    "incident_actions",
    "incident_comments",
    "incident_audit_log",
    "incident_status_history",
    "incidents",
    "hse_walkaround_templates",
    "hse_walkaround_records",
    "hse_walkaround_attachments",
    "incident_webhooks",
    "incident_routing_rules",
    "incident_picklist_versions",
    "incident_factor_definitions",
    "incident_form_role_visibility",
    MIGRATIONS_TABLE,
]

SQL_MIGRATIONS = f"""
CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_INCIDENTS = """
CREATE TABLE IF NOT EXISTS incidents (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  public_uuid CHAR(36) NOT NULL,
  reference_code VARCHAR(32) DEFAULT NULL,
  incident_mode VARCHAR(24) NOT NULL DEFAULT 'actual',
  category_slug VARCHAR(64) NOT NULL DEFAULT 'other_hs',
  status VARCHAR(36) NOT NULL DEFAULT 'draft',
  title VARCHAR(512) DEFAULT NULL,
  narrative MEDIUMTEXT,
  immediate_actions MEDIUMTEXT,
  environment_notes VARCHAR(512) DEFAULT NULL,
  org_severity_code VARCHAR(64) DEFAULT NULL,
  harm_grade_code VARCHAR(64) DEFAULT NULL,
  patient_involved TINYINT(1) NOT NULL DEFAULT 0,
  deidentified_narrative TINYINT(1) NOT NULL DEFAULT 1,
  medication_json JSON DEFAULT NULL,
  barrier_notes MEDIUMTEXT,
  five_whys MEDIUMTEXT,
  reporter_user_id CHAR(36) DEFAULT NULL,
  reporter_contractor_id INT DEFAULT NULL,
  site_label VARCHAR(255) DEFAULT NULL,
  division_label VARCHAR(255) DEFAULT NULL,
  shift_reference VARCHAR(128) DEFAULT NULL,
  vehicle_reference VARCHAR(128) DEFAULT NULL,
  crm_site_id INT DEFAULT NULL,
  hr_involved_contractor_ids JSON DEFAULT NULL,
  compliance_policy_id INT DEFAULT NULL,
  linked_safeguarding_referral_id INT DEFAULT NULL,
  linked_safeguarding_public_id VARCHAR(64) DEFAULT NULL,
  safeguarding_required TINYINT(1) NOT NULL DEFAULT 0,
  operational_event_id INT DEFAULT NULL,
  legal_hold TINYINT(1) NOT NULL DEFAULT 0,
  merged_into_id BIGINT DEFAULT NULL,
  merge_suggestion_score DECIMAL(6,2) DEFAULT NULL,
  sla_due_at DATETIME DEFAULT NULL,
  submitted_at DATETIME DEFAULT NULL,
  closed_at DATETIME DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_inc_pub (public_uuid),
  UNIQUE KEY uq_inc_ref (reference_code),
  KEY idx_inc_status (status),
  KEY idx_inc_cat (category_slug),
  KEY idx_inc_rep_c (reporter_contractor_id),
  KEY idx_inc_rep_u (reporter_user_id),
  KEY idx_inc_created (created_at),
  KEY idx_inc_sg (linked_safeguarding_referral_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_STATUS_HISTORY = """
CREATE TABLE IF NOT EXISTS incident_status_history (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  incident_id BIGINT NOT NULL,
  from_status VARCHAR(36) DEFAULT NULL,
  to_status VARCHAR(36) NOT NULL,
  actor_label VARCHAR(255) DEFAULT NULL,
  note VARCHAR(512) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ish_inc (incident_id),
  CONSTRAINT fk_ish_inc FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_AUDIT = """
CREATE TABLE IF NOT EXISTS incident_audit_log (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  incident_id BIGINT NOT NULL,
  action VARCHAR(64) NOT NULL,
  detail_json JSON DEFAULT NULL,
  actor_label VARCHAR(255) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ial_inc (incident_id),
  CONSTRAINT fk_ial_inc FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_COMMENTS = """
CREATE TABLE IF NOT EXISTS incident_comments (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  incident_id BIGINT NOT NULL,
  body MEDIUMTEXT NOT NULL,
  author_label VARCHAR(255) NOT NULL,
  author_user_id CHAR(36) DEFAULT NULL,
  author_contractor_id INT DEFAULT NULL,
  parent_id BIGINT DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_icm_inc (incident_id),
  CONSTRAINT fk_icm_inc FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_ACTIONS = """
CREATE TABLE IF NOT EXISTS incident_actions (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  incident_id BIGINT NOT NULL,
  title VARCHAR(512) NOT NULL,
  description MEDIUMTEXT,
  owner_user_id CHAR(36) DEFAULT NULL,
  owner_label VARCHAR(255) DEFAULT NULL,
  due_date DATE DEFAULT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'open',
  effectiveness_review MEDIUMTEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_iac_inc (incident_id),
  CONSTRAINT fk_iac_inc FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_TIMELINE = """
CREATE TABLE IF NOT EXISTS incident_timeline_events (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  incident_id BIGINT NOT NULL,
  event_time DATETIME NOT NULL,
  label VARCHAR(255) NOT NULL,
  body MEDIUMTEXT,
  actor_label VARCHAR(255) DEFAULT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ite_inc (incident_id),
  CONSTRAINT fk_ite_inc FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_FACTOR_DEFS = """
CREATE TABLE IF NOT EXISTS incident_factor_definitions (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  code VARCHAR(64) NOT NULL,
  label VARCHAR(255) NOT NULL,
  pack VARCHAR(24) NOT NULL DEFAULT 'universal',
  sort_order INT NOT NULL DEFAULT 0,
  active TINYINT(1) NOT NULL DEFAULT 1,
  UNIQUE KEY uq_ifd_code (code),
  KEY idx_ifd_pack (pack, sort_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_INCIDENT_FACTORS = """
CREATE TABLE IF NOT EXISTS incident_incident_factors (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  incident_id BIGINT NOT NULL,
  factor_code VARCHAR(64) NOT NULL,
  notes VARCHAR(512) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_iif (incident_id, factor_code),
  KEY idx_iif_code (factor_code),
  CONSTRAINT fk_iif_inc FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_ATTACHMENTS = """
CREATE TABLE IF NOT EXISTS incident_attachments (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  incident_id BIGINT NOT NULL,
  file_path VARCHAR(512) NOT NULL,
  file_name VARCHAR(255) DEFAULT NULL,
  uploaded_by_label VARCHAR(255) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_iat_inc (incident_id),
  CONSTRAINT fk_iat_inc FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_SUBSCRIPTIONS = """
CREATE TABLE IF NOT EXISTS incident_subscriptions (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  incident_id BIGINT NOT NULL,
  user_id CHAR(36) NOT NULL,
  events VARCHAR(255) NOT NULL DEFAULT 'all',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_isu (incident_id, user_id),
  CONSTRAINT fk_isu_inc FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_ROUTING = """
CREATE TABLE IF NOT EXISTS incident_routing_rules (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  category_slug VARCHAR(64) DEFAULT NULL,
  org_severity_codes VARCHAR(255) DEFAULT NULL,
  assignee_user_id CHAR(36) DEFAULT NULL,
  assignee_label VARCHAR(255) DEFAULT NULL,
  sla_hours INT DEFAULT NULL,
  priority_order INT NOT NULL DEFAULT 0,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_WEBHOOKS = """
CREATE TABLE IF NOT EXISTS incident_webhooks (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  target_url VARCHAR(1024) NOT NULL,
  secret VARCHAR(255) DEFAULT NULL,
  event_types VARCHAR(512) NOT NULL DEFAULT 'status_change,submitted,closed',
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_PICKLISTS = """
CREATE TABLE IF NOT EXISTS incident_picklist_versions (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  version_tag VARCHAR(32) NOT NULL,
  picklist_type VARCHAR(64) NOT NULL,
  items_json JSON NOT NULL,
  effective_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ipv_type (picklist_type, effective_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_HSE_WALKAROUND_TEMPLATES = """
CREATE TABLE IF NOT EXISTS hse_walkaround_templates (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description MEDIUMTEXT,
  site_label VARCHAR(255) DEFAULT NULL,
  checklist_json JSON NOT NULL,
  interval_days INT NOT NULL DEFAULT 7,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_hwt_active (active),
  KEY idx_hwt_site (site_label(64))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_HSE_WALKAROUND_RECORDS = """
CREATE TABLE IF NOT EXISTS hse_walkaround_records (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  template_id INT NOT NULL,
  due_at DATE NOT NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'pending',
  site_label VARCHAR(255) DEFAULT NULL,
  completed_at DATETIME DEFAULT NULL,
  completed_by_label VARCHAR(255) DEFAULT NULL,
  completed_by_user_id CHAR(36) DEFAULT NULL,
  answers_json JSON DEFAULT NULL,
  notes MEDIUMTEXT,
  linked_incident_id BIGINT DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_hwr_tpl_due (template_id, due_at),
  KEY idx_hwr_status_due (status, due_at),
  CONSTRAINT fk_hwr_tpl FOREIGN KEY (template_id) REFERENCES hse_walkaround_templates(id) ON DELETE CASCADE,
  CONSTRAINT fk_hwr_inc FOREIGN KEY (linked_incident_id) REFERENCES incidents(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_MERGE_SUGGESTIONS = """
CREATE TABLE IF NOT EXISTS incident_merge_suggestions (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  incident_id_a BIGINT NOT NULL,
  incident_id_b BIGINT NOT NULL,
  score DECIMAL(6,2) NOT NULL DEFAULT 0,
  dismissed TINYINT(1) NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_ims_pair (incident_id_a, incident_id_b),
  KEY idx_ims_a (incident_id_a),
  KEY idx_ims_b (incident_id_b)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def _run_sql(conn, sql: str) -> None:
    cur = conn.cursor()
    try:
        for stmt in sql.split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _migration_applied(conn, name: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT 1 FROM `{MIGRATIONS_TABLE}` WHERE filename = %s LIMIT 1", (name,)
        )
        return cur.fetchone() is not None
    finally:
        cur.close()


def _mark_migration(conn, name: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"INSERT IGNORE INTO `{MIGRATIONS_TABLE}` (filename) VALUES (%s)", (name,)
        )
        conn.commit()
    finally:
        cur.close()


def _seed_factor_definitions(conn) -> None:
    try:
        from app.plugins.incident_reporting_module.constants import (
            MEDICAL_CATEGORIES,
            UNIVERSAL_CATEGORIES,
        )
    except ImportError:
        from constants import MEDICAL_CATEGORIES, UNIVERSAL_CATEGORIES  # type: ignore

    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM incident_factor_definitions")
        n = int(cur.fetchone()[0] or 0)
        if n > 0:
            return
        universal_factors = [
            ("communication", "Communication", "universal", 10),
            ("training", "Training & competence", "universal", 20),
            ("equipment", "Equipment / resources", "universal", 30),
            ("environment", "Work environment / layout", "universal", 40),
            ("policy", "Policies / procedures", "universal", 50),
            ("fatigue", "Fatigue / workload", "universal", 60),
            ("supervision", "Supervision / oversight", "universal", 70),
        ]
        clinical_factors = [
            ("clinical_handover", "Handover", "clinical", 10),
            ("clinical_medication_process", "Medication process", "clinical", 20),
            ("clinical_assessment", "Assessment / diagnosis", "clinical", 30),
            ("clinical_teamwork", "Teamwork", "clinical", 40),
            ("clinical_equipment", "Clinical equipment / devices", "clinical", 50),
        ]
        for code, label, pack, so in universal_factors + clinical_factors:
            cur.execute(
                """
                INSERT INTO incident_factor_definitions (code, label, pack, sort_order)
                VALUES (%s, %s, %s, %s)
                """,
                (code, label, pack, so),
            )
        # Category hints as JSON in picklist (for admin configuration later)
        cur.execute(
            """
            INSERT INTO incident_picklist_versions (version_tag, picklist_type, items_json)
            VALUES (%s, %s, %s)
            """,
            (
                "v1",
                "categories_universal",
                json.dumps([{"slug": s, "label": l} for s, l in UNIVERSAL_CATEGORIES]),
            ),
        )
        cur.execute(
            """
            INSERT INTO incident_picklist_versions (version_tag, picklist_type, items_json)
            VALUES (%s, %s, %s)
            """,
            (
                "v1",
                "categories_medical",
                json.dumps([{"slug": s, "label": l} for s, l in MEDICAL_CATEGORIES]),
            ),
        )
        conn.commit()
    finally:
        cur.close()


def ensure_tables(conn) -> None:
    _run_sql(conn, SQL_MIGRATIONS)
    ordered = [
        ("001_core", SQL_INCIDENTS),
        ("002_status", SQL_STATUS_HISTORY),
        ("003_audit", SQL_AUDIT),
        ("004_comments", SQL_COMMENTS),
        ("005_actions", SQL_ACTIONS),
        ("006_timeline", SQL_TIMELINE),
        ("007_factor_defs", SQL_FACTOR_DEFS),
        ("008_incident_factors", SQL_INCIDENT_FACTORS),
        ("009_attachments", SQL_ATTACHMENTS),
        ("010_subscriptions", SQL_SUBSCRIPTIONS),
        ("011_routing", SQL_ROUTING),
        ("012_webhooks", SQL_WEBHOOKS),
        ("013_picklists", SQL_PICKLISTS),
        ("014_merge", SQL_MERGE_SUGGESTIONS),
        (
            "015_ir1_incident_columns",
            """
ALTER TABLE incidents
  ADD COLUMN incident_occurred_at DATETIME NULL,
  ADD COLUMN incident_discovered_at DATETIME NULL,
  ADD COLUMN exact_location_detail VARCHAR(512) NULL,
  ADD COLUMN witnesses_text MEDIUMTEXT NULL,
  ADD COLUMN equipment_involved VARCHAR(512) NULL,
  ADD COLUMN riddor_notifiable TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN reporter_job_title VARCHAR(255) NULL,
  ADD COLUMN reporter_department VARCHAR(255) NULL,
  ADD COLUMN reporter_contact_phone VARCHAR(64) NULL,
  ADD COLUMN people_affected_count INT NULL,
  ADD COLUMN ir1_supplementary_json JSON NULL
""",
        ),
        ("016_hse_walkaround_templates", SQL_HSE_WALKAROUND_TEMPLATES),
        ("017_hse_walkaround_records", SQL_HSE_WALKAROUND_RECORDS),
        (
            "018_walkaround_findings_attachments",
            """
ALTER TABLE hse_walkaround_records
  ADD COLUMN findings_json JSON NULL AFTER answers_json
""",
        ),
        (
            "019_hse_walkaround_attachments_table",
            """
CREATE TABLE IF NOT EXISTS hse_walkaround_attachments (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  record_id BIGINT NOT NULL,
  finding_sort INT DEFAULT NULL,
  file_path VARCHAR(512) NOT NULL,
  file_name VARCHAR(255) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_hwa_rec (record_id),
  KEY idx_hwa_find (record_id, finding_sort),
  CONSTRAINT fk_hwa_rec FOREIGN KEY (record_id) REFERENCES hse_walkaround_records(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""",
        ),
        ("020_drop_incident_webhooks", "DROP TABLE IF EXISTS incident_webhooks"),
        (
            "021_incident_form_role_visibility",
            """
CREATE TABLE IF NOT EXISTS incident_form_role_visibility (
  id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
  ir1_roles_json JSON NULL,
  hse_roles_json JSON NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO incident_form_role_visibility (id) VALUES (1)
""",
        ),
    ]
    for name, sql in ordered:
        if _migration_applied(conn, name):
            continue
        _run_sql(conn, sql)
        _mark_migration(conn, name)
    _seed_factor_definitions(conn)


def install() -> None:
    conn = get_db_connection()
    try:
        ensure_tables(conn)
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
    p = argparse.ArgumentParser(description="Incident reporting module installer")
    p.add_argument("command", choices=["install", "upgrade", "uninstall"])
    p.add_argument("--drop-data", action="store_true")
    args = p.parse_args()
    if args.command == "uninstall":
        uninstall(drop_data=args.drop_data)
    else:
        install()
