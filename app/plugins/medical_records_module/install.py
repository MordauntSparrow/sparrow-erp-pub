"""
Medical Records / Cura — database install & upgrade.

Creates extension tables for operational grouping, safeguarding, and patient-contact
reports; extends `cases` with optional dispatch linkage and versioning.

Retention / erasure: clinical rows (`cases`, `cura_*`, `cura_mi_*`) are subject to your
organisational retention policy — implement scheduled purge or anonymisation jobs outside
this module if required (see docs/compliance/DATA_RETENTION_POLICY.md).

Run from repo root:
  python app/plugins/medical_records_module/install.py upgrade
Or rely on core "run upgrades" which discovers install.py per plugin.
"""
from __future__ import annotations

import argparse
import logging
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

logger = logging.getLogger("medical_records_module.install")

MIGRATIONS_TABLE = "mr_cura_migrations"

SQL_CREATE_MIGRATIONS = f"""
CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  migration_id VARCHAR(64) NOT NULL UNIQUE,
  applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_OPERATIONAL_EVENTS = """
CREATE TABLE IF NOT EXISTS cura_operational_events (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  slug VARCHAR(96) NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  location_summary VARCHAR(512) NULL,
  starts_at DATETIME NULL,
  ends_at DATETIME NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'draft',
  config JSON NULL,
  enforce_assignments TINYINT(1) NOT NULL DEFAULT 0,
  created_by VARCHAR(128) NULL,
  updated_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_cura_events_status (status),
  KEY idx_cura_events_dates (starts_at, ends_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_SAFEGUARDING = """
CREATE TABLE IF NOT EXISTS cura_safeguarding_referrals (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  public_id CHAR(36) NOT NULL,
  client_local_id VARCHAR(128) NULL,
  idempotency_key VARCHAR(128) NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'draft',
  subject_type VARCHAR(32) NULL,
  payload_json LONGTEXT NOT NULL,
  sync_status VARCHAR(32) NULL,
  sync_error TEXT NULL,
  last_server_ack_at DATETIME NULL,
  record_version INT NOT NULL DEFAULT 1,
  created_by VARCHAR(128) NULL,
  updated_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_cura_sg_public (public_id),
  UNIQUE KEY uq_cura_sg_idem (idempotency_key),
  KEY idx_cura_sg_status (status),
  KEY idx_cura_sg_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_PATIENT_CONTACT_REPORTS = """
CREATE TABLE IF NOT EXISTS cura_patient_contact_reports (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  public_id CHAR(36) NOT NULL,
  operational_event_id BIGINT NULL,
  client_local_id VARCHAR(128) NULL,
  idempotency_key VARCHAR(128) NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'draft',
  payload_json LONGTEXT NOT NULL,
  sync_status VARCHAR(32) NULL,
  sync_error TEXT NULL,
  last_server_ack_at DATETIME NULL,
  submitted_by VARCHAR(128) NULL,
  record_version INT NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_cura_pcr_public (public_id),
  UNIQUE KEY uq_cura_pcr_idem (idempotency_key),
  KEY idx_cura_pcr_event (operational_event_id),
  KEY idx_cura_pcr_status (status),
  CONSTRAINT fk_cura_pcr_event FOREIGN KEY (operational_event_id)
    REFERENCES cura_operational_events(id) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_FILE_ATTACHMENTS = """
CREATE TABLE IF NOT EXISTS cura_file_attachments (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  entity_type VARCHAR(48) NOT NULL,
  entity_id BIGINT NOT NULL,
  storage_key VARCHAR(512) NOT NULL,
  original_filename VARCHAR(255) NULL,
  mime_type VARCHAR(128) NULL,
  byte_size BIGINT NULL,
  checksum_sha256 CHAR(64) NULL,
  created_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_cura_att_entity (entity_type, entity_id),
  KEY idx_cura_att_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_OPERATIONAL_EVENT_ASSIGNMENTS = """
CREATE TABLE IF NOT EXISTS cura_operational_event_assignments (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  operational_event_id BIGINT NOT NULL,
  principal_username VARCHAR(128) NOT NULL,
  assigned_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_cura_oea_evt_user (operational_event_id, principal_username),
  KEY idx_cura_oea_principal (principal_username),
  CONSTRAINT fk_cura_oea_event FOREIGN KEY (operational_event_id)
    REFERENCES cura_operational_events(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_CASES_FULL = """
CREATE TABLE IF NOT EXISTS cases (
  id BIGINT PRIMARY KEY,
  data JSON NOT NULL,
  status VARCHAR(50) NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  closed_at DATETIME NULL,
  dispatch_reference VARCHAR(64) NULL,
  primary_callsign VARCHAR(64) NULL,
  dispatch_synced_at DATETIME NULL,
  record_version INT NOT NULL DEFAULT 1,
  idempotency_key VARCHAR(128) NULL,
  close_idempotency_key VARCHAR(128) NULL,
  mpi_patient_id INT NULL,
  cura_location_id BIGINT NULL,
  patient_match_meta JSON NULL,
  UNIQUE KEY uq_cases_idempotency_key (idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _column_exists(cursor, table: str, column: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s",
        (table, column),
    )
    return cursor.fetchone() is not None


def _apply_migration(conn, migration_id: str, fn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT 1 FROM {MIGRATIONS_TABLE} WHERE migration_id = %s", (migration_id,))
        if cur.fetchone():
            return
        fn(conn, cur)
        cur.execute(
            f"INSERT INTO {MIGRATIONS_TABLE} (migration_id) VALUES (%s)",
            (migration_id,),
        )
        conn.commit()
        logger.info("Applied migration %s", migration_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _migration_003_file_attachments(conn, cursor) -> None:
    cursor.execute(SQL_CREATE_FILE_ATTACHMENTS)


def _migration_005_cases_idempotency(conn, cursor) -> None:
    if not _column_exists(cursor, "cases", "idempotency_key"):
        try:
            cursor.execute(
                "ALTER TABLE cases ADD COLUMN idempotency_key VARCHAR(128) NULL"
            )
        except Exception as e:
            logger.warning("Could not add cases.idempotency_key: %s", e)
    # Unique index (MySQL allows multiple NULLs)
    try:
        cursor.execute(
            "CREATE UNIQUE INDEX uq_cases_idempotency_key ON cases (idempotency_key)"
        )
    except Exception:
        pass  # already exists
    if not _column_exists(cursor, "cases", "close_idempotency_key"):
        try:
            cursor.execute(
                "ALTER TABLE cases ADD COLUMN close_idempotency_key VARCHAR(128) NULL"
            )
        except Exception as e:
            logger.warning("Could not add cases.close_idempotency_key: %s", e)


SQL_CREATE_SG_AUDIT = """
CREATE TABLE IF NOT EXISTS cura_safeguarding_audit_events (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  referral_id BIGINT NOT NULL,
  actor_username VARCHAR(128) NULL,
  action VARCHAR(64) NOT NULL,
  detail_json TEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_cura_sg_audit_ref (referral_id),
  CONSTRAINT fk_cura_sg_audit_ref FOREIGN KEY (referral_id)
    REFERENCES cura_safeguarding_referrals(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def _migration_006_safeguarding_audit(conn, cursor) -> None:
    cursor.execute(SQL_CREATE_SG_AUDIT)


SQL_CREATE_TENANT_DATASETS = """
CREATE TABLE IF NOT EXISTS cura_tenant_datasets (
  name VARCHAR(96) NOT NULL PRIMARY KEY,
  version INT NOT NULL DEFAULT 1,
  payload_json LONGTEXT NOT NULL,
  updated_by VARCHAR(128) NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_TENANT_SETTINGS = """
CREATE TABLE IF NOT EXISTS cura_tenant_settings (
  setting_key VARCHAR(128) NOT NULL PRIMARY KEY,
  value_json LONGTEXT NULL,
  updated_by VARCHAR(128) NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def _migration_007_cura_datasets_settings(conn, cursor) -> None:
    cursor.execute(SQL_CREATE_TENANT_DATASETS)
    cursor.execute(SQL_CREATE_TENANT_SETTINGS)


SQL_CREATE_MI_EVENTS = """
CREATE TABLE IF NOT EXISTS cura_mi_events (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  location_summary VARCHAR(512) NULL,
  starts_at DATETIME NULL,
  ends_at DATETIME NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'upcoming',
  config_json LONGTEXT NULL,
  created_by VARCHAR(128) NULL,
  updated_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_mi_ev_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_MI_ASSIGNMENTS = """
CREATE TABLE IF NOT EXISTS cura_mi_assignments (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  event_id BIGINT NOT NULL,
  principal_username VARCHAR(128) NOT NULL,
  assigned_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_mi_evt_user (event_id, principal_username),
  KEY idx_mi_assign_principal (principal_username),
  CONSTRAINT fk_mi_assign_event FOREIGN KEY (event_id)
    REFERENCES cura_mi_events(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_MI_NOTICES = """
CREATE TABLE IF NOT EXISTS cura_mi_notices (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  event_id BIGINT NOT NULL,
  message TEXT NOT NULL,
  severity VARCHAR(32) NOT NULL DEFAULT 'info',
  expires_at DATETIME NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_mi_notice_event (event_id),
  CONSTRAINT fk_mi_notice_event FOREIGN KEY (event_id)
    REFERENCES cura_mi_events(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_MI_DOCUMENTS = """
CREATE TABLE IF NOT EXISTS cura_mi_documents (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  event_id BIGINT NOT NULL,
  name VARCHAR(255) NOT NULL,
  doc_type VARCHAR(64) NOT NULL DEFAULT 'other',
  storage_key VARCHAR(512) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_mi_doc_event (event_id),
  CONSTRAINT fk_mi_doc_event FOREIGN KEY (event_id)
    REFERENCES cura_mi_events(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_MI_REPORTS = """
CREATE TABLE IF NOT EXISTS cura_mi_reports (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  event_id BIGINT NOT NULL,
  public_id CHAR(36) NOT NULL,
  idempotency_key VARCHAR(128) NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'draft',
  payload_json LONGTEXT NOT NULL,
  rejection_reason TEXT NULL,
  submitted_by VARCHAR(128) NULL,
  record_version INT NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_mi_report_public (public_id),
  UNIQUE KEY uq_mi_report_idem (idempotency_key),
  KEY idx_mi_report_event (event_id),
  KEY idx_mi_report_status (status),
  CONSTRAINT fk_mi_report_event FOREIGN KEY (event_id)
    REFERENCES cura_mi_events(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_MI_FORM_TEMPLATES = """
CREATE TABLE IF NOT EXISTS cura_mi_form_templates (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  slug VARCHAR(64) NOT NULL,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512) NULL,
  schema_json LONGTEXT NOT NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  created_by VARCHAR(128) NULL,
  updated_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_mi_form_slug (slug),
  KEY idx_mi_form_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_MI_EVENT_FORM_ASSIGNMENTS = """
CREATE TABLE IF NOT EXISTS cura_mi_event_form_assignments (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  event_id BIGINT NOT NULL,
  form_template_id BIGINT NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  created_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_mi_evt_form (event_id, form_template_id),
  KEY idx_mi_evt_form_event (event_id),
  KEY idx_mi_evt_form_tpl (form_template_id),
  CONSTRAINT fk_mi_evt_form_event FOREIGN KEY (event_id)
    REFERENCES cura_mi_events(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_mi_evt_form_tpl FOREIGN KEY (form_template_id)
    REFERENCES cura_mi_form_templates(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_MI_REFERENCE_CARD_TEMPLATES = """
CREATE TABLE IF NOT EXISTS cura_mi_reference_card_templates (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  slug VARCHAR(64) NOT NULL,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512) NULL,
  schema_json LONGTEXT NOT NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  created_by VARCHAR(128) NULL,
  updated_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_mi_refcard_slug (slug),
  KEY idx_mi_refcard_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_MI_EVENT_REFERENCE_CARDS = """
CREATE TABLE IF NOT EXISTS cura_mi_event_reference_cards (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  event_id BIGINT NOT NULL,
  template_id BIGINT NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  created_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_mi_evt_refcard (event_id, template_id),
  KEY idx_mi_evt_refcard_evt (event_id),
  KEY idx_mi_evt_refcard_tpl (template_id),
  CONSTRAINT fk_mi_evt_refcard_evt FOREIGN KEY (event_id)
    REFERENCES cura_mi_events(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_mi_evt_refcard_tpl FOREIGN KEY (template_id)
    REFERENCES cura_mi_reference_card_templates(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def _migration_008_minor_injury(conn, cursor) -> None:
    for stmt in (
        SQL_CREATE_MI_EVENTS,
        SQL_CREATE_MI_ASSIGNMENTS,
        SQL_CREATE_MI_NOTICES,
        SQL_CREATE_MI_DOCUMENTS,
        SQL_CREATE_MI_REPORTS,
    ):
        cursor.execute(stmt)


SQL_CREATE_CURA_LOCATIONS = """
CREATE TABLE IF NOT EXISTS cura_locations (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  postcode_norm VARCHAR(16) NULL,
  address_fingerprint CHAR(32) NOT NULL,
  address_line TEXT NULL,
  building_name VARCHAR(255) NULL,
  notes_internal VARCHAR(512) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_cura_loc_fp (address_fingerprint),
  KEY idx_cura_loc_pc (postcode_norm)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_CURA_MPI_RISK_FLAGS = """
CREATE TABLE IF NOT EXISTS cura_mpi_risk_flags (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  patient_row_id INT NULL,
  location_id BIGINT NULL,
  scope VARCHAR(16) NOT NULL DEFAULT 'patient',
  category VARCHAR(64) NOT NULL,
  severity VARCHAR(16) NOT NULL DEFAULT 'warning',
  summary VARCHAR(512) NOT NULL,
  detail TEXT NULL,
  source_type VARCHAR(64) NULL,
  source_reference VARCHAR(255) NULL,
  origin_case_id BIGINT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  review_at DATE NULL,
  created_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_mpi_flags_patient (patient_row_id),
  KEY idx_mpi_flags_loc (location_id),
  KEY idx_mpi_flags_active (active),
  CONSTRAINT fk_mpi_flags_loc FOREIGN KEY (location_id)
    REFERENCES cura_locations(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


SQL_CREATE_CALLSIGN_MDT_VALIDATION_LOG = """
CREATE TABLE IF NOT EXISTS cura_callsign_mdt_validation_log (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  operational_event_id BIGINT NULL,
  username VARCHAR(128) NOT NULL,
  callsign VARCHAR(64) NOT NULL,
  ok TINYINT(1) NOT NULL DEFAULT 0,
  reason_code VARCHAR(64) NOT NULL DEFAULT '',
  detail_json JSON NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_cura_cvl_event (operational_event_id),
  KEY idx_cura_cvl_ok_created (ok, created_at),
  KEY idx_cura_cvl_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_AUDIT_LOGS_COMPAT = """
CREATE TABLE IF NOT EXISTS audit_logs (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user VARCHAR(100) NOT NULL,
  action TEXT NOT NULL,
  patient_id BIGINT NULL,
  timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_audit_logs_timestamp (timestamp),
  KEY idx_audit_logs_patient (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_EPCR_ACCESS_REQUESTS = """
CREATE TABLE IF NOT EXISTS epcr_access_requests (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  request_id VARCHAR(64) NOT NULL UNIQUE,
  case_id BIGINT NOT NULL,
  requested_by VARCHAR(128) NOT NULL,
  requester_role VARCHAR(64) NULL,
  justification TEXT NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  reviewed_by VARCHAR(128) NULL,
  reviewed_at DATETIME NULL,
  review_note TEXT NULL,
  access_code VARCHAR(16) NULL,
  code_expires_at DATETIME NULL,
  used_by VARCHAR(128) NULL,
  used_at DATETIME NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_epcr_access_case (case_id),
  KEY idx_epcr_access_reqby (requested_by),
  KEY idx_epcr_access_status (status),
  KEY idx_epcr_access_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_CURA_OPERATIONAL_EVENT_ASSETS = """
CREATE TABLE IF NOT EXISTS cura_operational_event_assets (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  operational_event_id BIGINT NOT NULL,
  asset_kind VARCHAR(32) NOT NULL,
  title VARCHAR(255) NOT NULL,
  body_text LONGTEXT NULL,
  storage_path VARCHAR(512) NULL,
  original_filename VARCHAR(255) NULL,
  mime_type VARCHAR(128) NULL,
  sort_order INT NOT NULL DEFAULT 0,
  created_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_coe_assets_event (operational_event_id),
  KEY idx_coe_assets_kind (operational_event_id, asset_kind)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

SQL_CREATE_CURA_OPERATIONAL_EVENT_RESOURCES = """
CREATE TABLE IF NOT EXISTS cura_operational_event_resources (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  operational_event_id BIGINT NOT NULL,
  resource_kind VARCHAR(32) NOT NULL,
  resource_id BIGINT NOT NULL,
  role_label VARCHAR(255) NULL,
  notes VARCHAR(512) NULL,
  sort_order INT NOT NULL DEFAULT 0,
  assigned_by VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_cura_oer_evt_res (operational_event_id, resource_kind, resource_id),
  KEY idx_cura_oer_event (operational_event_id),
  KEY idx_cura_oer_res (resource_kind, resource_id),
  CONSTRAINT fk_cura_oer_event FOREIGN KEY (operational_event_id)
    REFERENCES cura_operational_events(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def _migration_010_event_ventus_integration(conn, cursor) -> None:
    """Optional expected_callsign on assignments; log MDT callsign validation for ops hub."""
    if not _column_exists(cursor, "cura_operational_event_assignments", "expected_callsign"):
        try:
            cursor.execute(
                "ALTER TABLE cura_operational_event_assignments "
                "ADD COLUMN expected_callsign VARCHAR(64) NULL"
            )
        except Exception as e:
            logger.warning("Could not add expected_callsign to cura_operational_event_assignments: %s", e)
    cursor.execute(SQL_CREATE_CALLSIGN_MDT_VALIDATION_LOG)


def _migration_011_legacy_audit_logs_compat(conn, cursor) -> None:
    """
    Compatibility bridge: some deployments can miss core `audit_logs`.
    Medical/Cura routes call log_audit() heavily; ensure the table exists.
    """
    cursor.execute(SQL_CREATE_AUDIT_LOGS_COMPAT)


def _migration_012_epcr_access_requests(conn, cursor) -> None:
    """
    Persistent EPCR admin access workflow:
    request -> clinical review -> case-scoped one-time code.
    """
    cursor.execute(SQL_CREATE_EPCR_ACCESS_REQUESTS)


def _migration_013_cura_operational_event_assets(conn, cursor) -> None:
    """Event plan PDFs + bulletins; files under app/static/uploads (Railway volume symlink)."""
    cursor.execute(SQL_CREATE_CURA_OPERATIONAL_EVENT_ASSETS)


def _migration_014_operational_event_resources(conn, cursor) -> None:
    """Fleet vehicles + serial inventory equipment (e.g. AEDs per treatment bay) linked to an ops period."""
    cursor.execute(SQL_CREATE_CURA_OPERATIONAL_EVENT_RESOURCES)


def _migration_015_cura_dataset_baseline_backfill(conn, cursor) -> None:
    """
    Persist bundled Cura defaults when rows are missing or hold empty JSON (`{}` / `[]`),
    so Sparrow admin tables and `/api/cura/datasets/...` stay aligned with the EPCR app.
    """
    import json

    from app.plugins.medical_records_module.cura_baseline_datasets import (
        get_cura_baseline_payload,
        is_cura_dataset_payload_unset,
    )

    if not _table_exists(cursor, "cura_tenant_datasets"):
        return
    slugs = (
        "drugs",
        "clinical_options",
        "clinical_indicators",
        "iv_fluids",
        "incident_response_options",
    )
    for slug in slugs:
        baseline = get_cura_baseline_payload(slug)
        if (isinstance(baseline, dict) and len(baseline) == 0) or (
            isinstance(baseline, list) and len(baseline) == 0
        ):
            continue
        cursor.execute(
            "SELECT version, payload_json FROM cura_tenant_datasets WHERE name = %s",
            (slug,),
        )
        row = cursor.fetchone()
        if not row:
            cursor.execute(
                "INSERT INTO cura_tenant_datasets (name, version, payload_json, updated_by) "
                "VALUES (%s,%s,%s,%s)",
                (slug, 1, json.dumps(baseline), "migration_015_cura_dataset_baseline"),
            )
            continue
        ver = int(row[0] or 1)
        try:
            pl = json.loads(row[1]) if row[1] else None
        except json.JSONDecodeError:
            pl = None
        if not is_cura_dataset_payload_unset(slug, pl):
            continue
        nv = ver + 1
        cursor.execute(
            "UPDATE cura_tenant_datasets SET payload_json=%s, version=%s, updated_by=%s WHERE name=%s",
            (json.dumps(baseline), nv, "migration_015_cura_dataset_baseline", slug),
        )


def _migration_016_patient_match_meta(conn, cursor) -> None:
    """Slim JSON for Cura patient-trace scans; avoids loading full case ``data`` when populated."""
    if not _column_exists(cursor, "cases", "patient_match_meta"):
        try:
            cursor.execute("ALTER TABLE cases ADD COLUMN patient_match_meta JSON NULL")
        except Exception as e:
            logger.warning("Could not add cases.patient_match_meta: %s", e)


def _migration_018_mi_custom_forms(conn, cursor) -> None:
    """Configurable MI add-on forms (Ventus-style JSON schema) and per-event assignment."""
    import json as _json

    cursor.execute(SQL_CREATE_MI_FORM_TEMPLATES)
    cursor.execute(SQL_CREATE_MI_EVENT_FORM_ASSIGNMENTS)
    seed_schema = _json.dumps(
        {
            "questions": [
                {
                    "key": "site_zone",
                    "label": "Site zone / stand",
                    "type": "text",
                    "required": False,
                },
                {
                    "key": "crowd_density",
                    "label": "Crowd density",
                    "type": "select",
                    "options": ["low", "medium", "high", "unknown"],
                    "required": False,
                },
                {
                    "key": "extra_notes",
                    "label": "Additional site notes",
                    "type": "textarea",
                    "required": False,
                },
            ]
        }
    )
    try:
        cursor.execute(
            """
            INSERT INTO cura_mi_form_templates
              (slug, name, description, schema_json, is_active, created_by, updated_by)
            VALUES (%s, %s, %s, %s, 1, %s, %s)
            ON DUPLICATE KEY UPDATE
              name = VALUES(name),
              description = VALUES(description),
              schema_json = VALUES(schema_json),
              is_active = VALUES(is_active),
              updated_by = VALUES(updated_by)
            """,
            (
                "event_site_checklist",
                "Event site checklist",
                "Example add-on form for minor injury (edit or assign per MI event in Cura ops).",
                seed_schema,
                "migration_018_mi_custom_forms",
                "migration_018_mi_custom_forms",
            ),
        )
    except Exception as e:
        logger.warning("migration_018 seed cura_mi_form_templates: %s", e)


def _migration_019_mi_reference_cards(conn, cursor) -> None:
    """Reusable MI quick-reference / advice cards; assign many templates per MI event."""
    import json as _json

    cursor.execute(SQL_CREATE_MI_REFERENCE_CARD_TEMPLATES)
    cursor.execute(SQL_CREATE_MI_EVENT_REFERENCE_CARDS)

    seeds: list[tuple[str, str, str, dict]] = [
        (
            "head_injury_red_flags",
            "Head Injury Red Flags",
            "Escalation prompts after head injury (reuse on contact-sport or high-risk events).",
            {
                "variant": "bullets",
                "title": "Head Injury Red Flags",
                "headerColor": "#f44336",
                "icon": "warning",
                "items": [
                    "Loss of consciousness (any duration)",
                    "Amnesia before or after event",
                    "Persistent headache",
                    "Vomiting (2+ episodes)",
                    "Seizure activity",
                    "Focal neurological deficit",
                    "Blood/fluid from ear or nose",
                    "High-risk mechanism (fall >1m)",
                ],
                "callout": {"severity": "error", "text": "Any red flag → Escalate to Medical Lead"},
            },
        ),
        (
            "safeguarding_triggers",
            "Safeguarding Triggers",
            "Safeguarding awareness card for event medical staff.",
            {
                "variant": "bullets",
                "title": "Safeguarding Triggers",
                "headerColor": "#1565c0",
                "icon": "security",
                "items": [
                    "Unaccompanied minor (<16)",
                    "Vulnerable adult (intoxicated/capacity)",
                    "Signs of abuse or neglect",
                    "Inconsistent injury explanation",
                    "Distressed patient refusing to leave",
                    "Concerning behaviour by carer/guardian",
                ],
                "callout": {"severity": "warning", "text": "If concerned → Complete Safeguarding Referral"},
            },
        ),
        (
            "when_to_escalate",
            "When to Escalate / Refer",
            "Clinical escalation prompts beyond minor injury scope.",
            {
                "variant": "bullets",
                "title": "When to Escalate / Refer",
                "headerColor": "#1565c0",
                "icon": "hospital",
                "items": [
                    "Abnormal observations (NEWS >4)",
                    "Wound requiring sutures/glue",
                    "Suspected fracture or dislocation",
                    "Unable to bear weight after injury",
                    "Chest pain or difficulty breathing",
                    "Allergic reaction (beyond local)",
                    "Significant mechanism of injury",
                ],
                "callout": {"severity": "info", "text": "When in doubt, escalate early"},
            },
        ),
        (
            "common_presentations",
            "Common Presentations",
            "Brief first-aid style prompts for typical minor presentations.",
            {
                "variant": "bullets",
                "title": "Common Presentations",
                "headerColor": "#1565c0",
                "icon": "pharmacy",
                "items": [
                    "Blisters: Clean, dress, footwear advice",
                    "Cuts/Grazes: Clean, steri-strip if needed",
                    "Sprains: RICE, check NV status, crutches PRN",
                    "Sunburn: Cool, moisturise, hydrate",
                    "Insect stings: Remove sting, antihistamine, monitor",
                    "Dehydration: Oral fluids, rest, shade",
                ],
            },
        ),
        (
            "welfare_mental_health",
            "Welfare & Mental Health",
            "Welfare and mental health encounter prompts.",
            {
                "variant": "bullets",
                "title": "Welfare & Mental Health",
                "headerColor": "#1565c0",
                "icon": "psychology",
                "items": [
                    "Listen without judgement",
                    "Assess capacity if intoxicated",
                    "Offer quiet space if overwhelmed",
                    "Ask directly about self-harm intent",
                    "Document exact statements made",
                    "Link with welfare team if available",
                ],
                "callout": {"severity": "warning", "text": "Active risk → Do not leave alone"},
            },
        ),
        (
            "event_risk_profile",
            "Event Risk Profile",
            "Shows event risk fields from MI event config when set by control.",
            {
                "variant": "event_risk_profile",
                "title": "Event Risk Profile",
                "headerColor": "#1565c0",
                "icon": "info",
                "emptyHint": "Risk information will be populated when the event is configured by control.",
            },
        ),
    ]

    for slug, name, desc, sch in seeds:
        try:
            cursor.execute(
                """
                INSERT INTO cura_mi_reference_card_templates
                  (slug, name, description, schema_json, is_active, created_by, updated_by)
                VALUES (%s, %s, %s, %s, 1, %s, %s)
                ON DUPLICATE KEY UPDATE
                  name = VALUES(name),
                  description = VALUES(description),
                  schema_json = VALUES(schema_json),
                  is_active = VALUES(is_active),
                  updated_by = VALUES(updated_by)
                """,
                (
                    slug,
                    name,
                    desc,
                    _json.dumps(sch),
                    "migration_019_mi_reference_cards",
                    "migration_019_mi_reference_cards",
                ),
            )
        except Exception as e:
            logger.warning("migration_019 seed template %s: %s", slug, e)

    slug_order = [s[0] for s in seeds]
    id_by_slug: dict[str, int] = {}
    for s in slug_order:
        cursor.execute(
            "SELECT id FROM cura_mi_reference_card_templates WHERE slug=%s AND is_active=1",
            (s,),
        )
        row = cursor.fetchone()
        if row:
            id_by_slug[s] = int(row[0])

    try:
        cursor.execute("SELECT id FROM cura_mi_events")
        for (eid,) in cursor.fetchall() or []:
            cursor.execute(
                "SELECT COUNT(*) FROM cura_mi_event_reference_cards WHERE event_id=%s",
                (eid,),
            )
            cnt_row = cursor.fetchone()
            if cnt_row and int(cnt_row[0] or 0) > 0:
                continue
            for order, slug in enumerate(slug_order):
                tid = id_by_slug.get(slug)
                if not tid:
                    continue
                try:
                    cursor.execute(
                        """
                        INSERT INTO cura_mi_event_reference_cards
                          (event_id, template_id, sort_order, created_by)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (eid, tid, order, "migration_019_mi_reference_cards"),
                    )
                except Exception:
                    pass
    except Exception as e:
        logger.warning("migration_019 backfill event reference cards: %s", e)


def _migration_017_cases_created_at_listing(conn, cursor) -> None:
    """
    EPCR GET /api/cases lists with ORDER BY created_at. Without an index, MySQL sorts wide rows
    (including large JSON ``data``) and can hit ER_OUT_OF_SORTMEMORY (1038) on modest DB tiers.
    """
    try:
        cursor.execute("CREATE INDEX idx_cases_created_at ON cases (created_at)")
    except Exception:
        pass


def _migration_009_mpi_locations_and_flags(conn, cursor) -> None:
    """Premises anchor + risk flags; link columns on cases. See cura_mpi.py."""
    cursor.execute(SQL_CREATE_CURA_LOCATIONS)
    cursor.execute(SQL_CREATE_CURA_MPI_RISK_FLAGS)

    for col, ddl in (
        ("mpi_patient_id", "ALTER TABLE cases ADD COLUMN mpi_patient_id INT NULL"),
        ("cura_location_id", "ALTER TABLE cases ADD COLUMN cura_location_id BIGINT NULL"),
    ):
        if not _column_exists(cursor, "cases", col):
            try:
                cursor.execute(ddl)
            except Exception as e:
                logger.warning("Could not add cases.%s: %s", col, e)

    # Helpful indexes for SAR-style listing (ignore failures on old MySQL)
    for idx_sql in (
        "CREATE INDEX idx_cases_mpi_patient ON cases (mpi_patient_id)",
        "CREATE INDEX idx_cases_cura_location ON cases (cura_location_id)",
    ):
        try:
            cursor.execute(idx_sql)
        except Exception:
            pass


def _table_exists(cursor, table: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        (table,),
    )
    return cursor.fetchone() is not None


def _migration_004_operational_assignments(conn, cursor) -> None:
    if not _column_exists(cursor, "cura_operational_events", "enforce_assignments"):
        try:
            cursor.execute(
                "ALTER TABLE cura_operational_events "
                "ADD COLUMN enforce_assignments TINYINT(1) NOT NULL DEFAULT 0"
            )
        except Exception as e:
            logger.warning("Could not add enforce_assignments to cura_operational_events: %s", e)
    cursor.execute(SQL_CREATE_OPERATIONAL_EVENT_ASSIGNMENTS)


def _migration_002_safeguarding_event_link(conn, cursor) -> None:
    """Optional grouping: safeguarding referrals may be tied to an operational event."""
    if _column_exists(cursor, "cura_safeguarding_referrals", "operational_event_id"):
        return
    try:
        cursor.execute(
            "ALTER TABLE cura_safeguarding_referrals ADD COLUMN operational_event_id BIGINT NULL"
        )
        cursor.execute(
            "ALTER TABLE cura_safeguarding_referrals ADD KEY idx_cura_sg_event (operational_event_id)"
        )
        cursor.execute(
            """
            ALTER TABLE cura_safeguarding_referrals
            ADD CONSTRAINT fk_cura_sg_event FOREIGN KEY (operational_event_id)
              REFERENCES cura_operational_events(id) ON DELETE SET NULL ON UPDATE CASCADE
            """
        )
    except Exception as e:
        logger.warning("Could not add operational_event_id to cura_safeguarding_referrals: %s", e)


def _migration_001_foundation(conn, cursor) -> None:
    for stmt in (
        SQL_CREATE_OPERATIONAL_EVENTS,
        SQL_CREATE_SAFEGUARDING,
        SQL_CREATE_PATIENT_CONTACT_REPORTS,
    ):
        cursor.execute(stmt)

    # Extend EPCR cases (optional dispatch link + optimistic version); idempotent adds.
    for col, ddl in (
        ("dispatch_reference", "ALTER TABLE cases ADD COLUMN dispatch_reference VARCHAR(64) NULL"),
        ("primary_callsign", "ALTER TABLE cases ADD COLUMN primary_callsign VARCHAR(64) NULL"),
        ("dispatch_synced_at", "ALTER TABLE cases ADD COLUMN dispatch_synced_at DATETIME NULL"),
        ("record_version", "ALTER TABLE cases ADD COLUMN record_version INT NOT NULL DEFAULT 1"),
    ):
        if not _column_exists(cursor, "cases", col):
            try:
                cursor.execute(ddl)
            except Exception as e:
                logger.warning("Could not add cases.%s: %s", col, e)

def _migration_000_ensure_cases_table(conn, cursor) -> None:
    """
    The EPCR sync flow expects a MySQL `cases` table to exist.
    Historically this module only added columns and assumed `cases` was created elsewhere,
    which results in "no cases table" and leaves the frontend cloud badge stuck on Pending.
    """
    if not _table_exists(cursor, "cases"):
        cursor.execute(SQL_CREATE_CASES_FULL)
        conn.commit()

    # If `cases` exists but older installs missed some columns, add them opportunistically.
    for col, ddl in (
        ("dispatch_reference", "ALTER TABLE cases ADD COLUMN dispatch_reference VARCHAR(64) NULL"),
        ("primary_callsign", "ALTER TABLE cases ADD COLUMN primary_callsign VARCHAR(64) NULL"),
        ("dispatch_synced_at", "ALTER TABLE cases ADD COLUMN dispatch_synced_at DATETIME NULL"),
        ("record_version", "ALTER TABLE cases ADD COLUMN record_version INT NOT NULL DEFAULT 1"),
        ("idempotency_key", "ALTER TABLE cases ADD COLUMN idempotency_key VARCHAR(128) NULL"),
        ("close_idempotency_key", "ALTER TABLE cases ADD COLUMN close_idempotency_key VARCHAR(128) NULL"),
        ("mpi_patient_id", "ALTER TABLE cases ADD COLUMN mpi_patient_id INT NULL"),
        ("cura_location_id", "ALTER TABLE cases ADD COLUMN cura_location_id BIGINT NULL"),
    ):
        if not _column_exists(cursor, "cases", col):
            try:
                cursor.execute(ddl)
            except Exception as e:
                logger.warning("Could not add cases.%s: %s", col, e)

    # Indexes for listing/search. Wrap so upgrades stay resilient.
    for idx_sql in (
        "CREATE INDEX idx_cases_mpi_patient ON cases (mpi_patient_id)",
        "CREATE INDEX idx_cases_cura_location ON cases (cura_location_id)",
    ):
        try:
            cursor.execute(idx_sql)
        except Exception:
            pass


def upgrade() -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(SQL_CREATE_MIGRATIONS)
        conn.commit()
    finally:
        cur.close()

    _apply_migration(conn, "000_ensure_cases_table", _migration_000_ensure_cases_table)
    _apply_migration(conn, "001_cura_foundation", _migration_001_foundation)
    _apply_migration(conn, "002_safeguarding_operational_event", _migration_002_safeguarding_event_link)
    _apply_migration(conn, "003_cura_file_attachments", _migration_003_file_attachments)
    _apply_migration(conn, "004_operational_event_assignments", _migration_004_operational_assignments)
    _apply_migration(conn, "005_cases_idempotency", _migration_005_cases_idempotency)
    _apply_migration(conn, "006_safeguarding_audit", _migration_006_safeguarding_audit)
    _apply_migration(conn, "007_cura_datasets_settings", _migration_007_cura_datasets_settings)
    _apply_migration(conn, "008_minor_injury", _migration_008_minor_injury)
    _apply_migration(conn, "009_mpi_locations_and_flags", _migration_009_mpi_locations_and_flags)
    _apply_migration(conn, "010_event_ventus_integration", _migration_010_event_ventus_integration)
    _apply_migration(conn, "011_legacy_audit_logs_compat", _migration_011_legacy_audit_logs_compat)
    _apply_migration(conn, "012_epcr_access_requests", _migration_012_epcr_access_requests)
    _apply_migration(conn, "013_cura_operational_event_assets", _migration_013_cura_operational_event_assets)
    _apply_migration(conn, "014_operational_event_resources", _migration_014_operational_event_resources)
    _apply_migration(conn, "015_cura_dataset_baseline_backfill", _migration_015_cura_dataset_baseline_backfill)
    _apply_migration(conn, "016_patient_match_meta", _migration_016_patient_match_meta)
    _apply_migration(conn, "017_cases_created_at_listing", _migration_017_cases_created_at_listing)
    _apply_migration(conn, "018_mi_custom_forms", _migration_018_mi_custom_forms)
    _apply_migration(conn, "019_mi_reference_cards", _migration_019_mi_reference_cards)
    conn.close()


def install() -> None:
    """Alias: same as upgrade (idempotent)."""
    upgrade()


def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Medical Records / Cura DB")
    parser.add_argument("command", choices=["install", "upgrade"])
    args = parser.parse_args()
    if args.command == "install":
        install()
    else:
        upgrade()
    print("medical_records_module: OK")


if __name__ == "__main__":
    main()
