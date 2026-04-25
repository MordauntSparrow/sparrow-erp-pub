"""
Scheduling module install/upgrade: all table definitions and logic in this file.

Tables ensured (in order): schedule_migrations, schedule_shifts, schedule_availability,
schedule_time_off, shift_swap_requests, schedule_open_shift_claims, schedule_settings,
schedule_templates, schedule_template_slots, schedule_job_type_requirements,
schedule_shift_audit, schedule_shift_tasks, schedule_clock_locations.

Depends on time_billing_module: tb_contractors, clients, sites, job_types must exist.
Run time_billing install (or upgrade) before scheduling install.

Install: creates all tables (CREATE TABLE IF NOT EXISTS); runs ALTERs for existing tables
(overtime_hours_per_week, weekly_budget on schedule_settings; time_off type/review columns;
schedule_shifts.contractor_id nullable for open shifts; time_off status 'cancelled').
Upgrade: same as install (idempotent). Uninstall: drops all MODULE_TABLES when --drop-data.

Run from repo root: python app/plugins/scheduling_module/install.py install
Or: python app/plugins/scheduling_module/install.py upgrade
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

MIGRATIONS_TABLE = "schedule_migrations"
MODULE_TABLES = [
    MIGRATIONS_TABLE,
    "schedule_shifts",
    "schedule_shift_assignments",
    "schedule_availability",
    "schedule_unavailability",
    "schedule_contractor_prefs",
    "schedule_time_off",
    "shift_swap_requests",
    "schedule_open_shift_claims",
    "schedule_settings",
    "sling_credentials",
    "sling_id_mappings",
    "sling_sync_runs",
    "sling_sync_run_steps",
    "schedule_templates",
    "schedule_template_slots",
    "schedule_job_type_requirements",
    "schedule_shift_audit",
    "schedule_shift_tasks",
    "schedule_clock_locations",
    "schedule_assignment_instructions",
]

SQL_CREATE_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS schedule_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_SCHEDULE_SHIFTS = """
CREATE TABLE IF NOT EXISTS schedule_shifts (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NULL,
  client_id INT NOT NULL,
  site_id INT DEFAULT NULL,
  job_type_id INT NOT NULL,
  work_date DATE NOT NULL,
  scheduled_start TIME NOT NULL,
  scheduled_end TIME NOT NULL,
  actual_start TIME DEFAULT NULL,
  actual_end TIME DEFAULT NULL,
  break_mins INT NOT NULL DEFAULT 0,
  notes TEXT,
  status ENUM('draft','published','in_progress','completed','cancelled','no_show') NOT NULL DEFAULT 'draft',
  source ENUM('manual','ventus','scheduler','work_module') NOT NULL DEFAULT 'manual',
  external_id VARCHAR(255) DEFAULT NULL,
  runsheet_id BIGINT DEFAULT NULL,
  runsheet_assignment_id BIGINT DEFAULT NULL,
  labour_cost DECIMAL(10,2) DEFAULT NULL,
  shared_labour_hours DECIMAL(6,2) DEFAULT NULL COMMENT 'Fixed person-hours, duration shrinks as crew or required headcount grows',
  recurrence_id BIGINT DEFAULT NULL,
  required_count INT NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_ss_contractor_date (contractor_id, work_date),
  KEY idx_ss_recurrence (recurrence_id),
  KEY idx_ss_date_status (work_date, status),
  KEY idx_ss_client_date (client_id, work_date),
  KEY idx_ss_source (source),
  KEY idx_ss_runsheet (runsheet_id),
  CONSTRAINT fk_ss_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE SET NULL,
  CONSTRAINT fk_ss_client FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE,
  CONSTRAINT fk_ss_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE SET NULL,
  CONSTRAINT fk_ss_jobtype FOREIGN KEY (job_type_id) REFERENCES job_types(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_SCHEDULE_SHIFT_ASSIGNMENTS = """
CREATE TABLE IF NOT EXISTS schedule_shift_assignments (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  shift_id BIGINT NOT NULL,
  contractor_id INT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_ssa_shift_contractor (shift_id, contractor_id),
  KEY idx_ssa_shift (shift_id),
  KEY idx_ssa_contractor (contractor_id),
  CONSTRAINT fk_ssa_shift FOREIGN KEY (shift_id) REFERENCES schedule_shifts(id) ON DELETE CASCADE,
  CONSTRAINT fk_ssa_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_SCHEDULE_AVAILABILITY = """
CREATE TABLE IF NOT EXISTS schedule_availability (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  day_of_week TINYINT NOT NULL,
  start_time TIME NOT NULL,
  end_time TIME NOT NULL,
  effective_from DATE NOT NULL,
  effective_to DATE DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_sa_contractor (contractor_id),
  KEY idx_sa_dow (contractor_id, day_of_week),
  CONSTRAINT fk_sa_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_SCHEDULE_UNAVAILABILITY = """
CREATE TABLE IF NOT EXISTS schedule_unavailability (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  day_of_week TINYINT NOT NULL,
  start_time TIME NOT NULL,
  end_time TIME NOT NULL,
  effective_from DATE NOT NULL,
  effective_to DATE DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_su_contractor (contractor_id),
  KEY idx_su_dow (contractor_id, day_of_week),
  CONSTRAINT fk_su_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_SCHEDULE_CONTRACTOR_PREFS = """
CREATE TABLE IF NOT EXISTS schedule_contractor_prefs (
  contractor_id INT NOT NULL,
  pref_key VARCHAR(64) NOT NULL,
  pref_value VARCHAR(255) DEFAULT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (contractor_id, pref_key),
  CONSTRAINT fk_scp_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_SCHEDULE_TIME_OFF = """
CREATE TABLE IF NOT EXISTS schedule_time_off (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  type ENUM('annual','sickness','other') NOT NULL DEFAULT 'annual',
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  start_time TIME DEFAULT NULL,
  end_time TIME DEFAULT NULL,
  reason VARCHAR(255) DEFAULT NULL,
  status ENUM('requested','approved','rejected','cancelled') NOT NULL DEFAULT 'requested',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_sto_contractor (contractor_id),
  KEY idx_sto_dates (start_date, end_date),
  CONSTRAINT fk_sto_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_SHIFT_SWAP_REQUESTS = """
CREATE TABLE IF NOT EXISTS shift_swap_requests (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  shift_id BIGINT NOT NULL,
  requester_contractor_id INT NOT NULL,
  requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status ENUM('open','claimed','approved','rejected','cancelled') NOT NULL DEFAULT 'open',
  claimer_contractor_id INT DEFAULT NULL,
  claimed_at DATETIME DEFAULT NULL,
  resolved_at DATETIME DEFAULT NULL,
  resolved_by INT DEFAULT NULL,
  notes TEXT,
  KEY idx_ssr_shift (shift_id),
  KEY idx_ssr_requester (requester_contractor_id),
  KEY idx_ssr_status (status),
  CONSTRAINT fk_ssr_shift FOREIGN KEY (shift_id) REFERENCES schedule_shifts(id) ON DELETE CASCADE,
  CONSTRAINT fk_ssr_requester FOREIGN KEY (requester_contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE,
  CONSTRAINT fk_ssr_claimer FOREIGN KEY (claimer_contractor_id) REFERENCES tb_contractors(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_SCHEDULE_TEMPLATES = """
CREATE TABLE IF NOT EXISTS schedule_templates (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(150) NOT NULL,
  client_id INT DEFAULT NULL,
  site_id INT DEFAULT NULL,
  job_type_id INT DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY fk_st_client (client_id),
  KEY fk_st_site (site_id),
  KEY fk_st_jobtype (job_type_id),
  CONSTRAINT fk_st_client FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE SET NULL,
  CONSTRAINT fk_st_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE SET NULL,
  CONSTRAINT fk_st_jobtype FOREIGN KEY (job_type_id) REFERENCES job_types(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_SCHEDULE_TEMPLATE_SLOTS = """
CREATE TABLE IF NOT EXISTS schedule_template_slots (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  template_id INT NOT NULL,
  day_of_week TINYINT NOT NULL,
  start_time TIME NOT NULL,
  end_time TIME NOT NULL,
  position_label VARCHAR(100) DEFAULT NULL,
  KEY fk_sts_template (template_id),
  CONSTRAINT fk_sts_template FOREIGN KEY (template_id) REFERENCES schedule_templates(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

CREATES = [
    SQL_CREATE_MIGRATIONS,
    SQL_CREATE_SCHEDULE_SHIFTS,
    SQL_CREATE_SCHEDULE_SHIFT_ASSIGNMENTS,
    SQL_CREATE_SCHEDULE_AVAILABILITY,
    SQL_CREATE_SCHEDULE_UNAVAILABILITY,
    SQL_CREATE_SCHEDULE_CONTRACTOR_PREFS,
    SQL_CREATE_SCHEDULE_TIME_OFF,
    SQL_CREATE_SHIFT_SWAP_REQUESTS,
    """
    CREATE TABLE IF NOT EXISTS sling_credentials (
      id TINYINT NOT NULL PRIMARY KEY,
      sling_email_enc TEXT NOT NULL,
      sling_password_enc TEXT NOT NULL,
      sling_base_url VARCHAR(255) NOT NULL DEFAULT 'https://api.getsling.com/v1',
      sling_org_id INT DEFAULT NULL,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS sling_id_mappings (
      id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      sling_position_id VARCHAR(255) DEFAULT NULL,
      job_type_id INT DEFAULT NULL,
      sling_location_id VARCHAR(255) DEFAULT NULL,
      site_id INT DEFAULT NULL,
      client_id INT DEFAULT NULL,
      sling_position_name VARCHAR(255) DEFAULT NULL,
      sling_location_name VARCHAR(255) DEFAULT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      KEY idx_sling_pos (sling_position_id),
      KEY idx_sling_loc (sling_location_id),
      KEY idx_mapped_job (job_type_id),
      KEY idx_mapped_site (site_id),
      KEY idx_mapped_client (client_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS sling_sync_runs (
      id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      finished_at TIMESTAMP NULL DEFAULT NULL,
      date_from DATE NOT NULL,
      date_to DATE NOT NULL,
      dry_run TINYINT(1) NOT NULL DEFAULT 0,
      actor_username VARCHAR(150) DEFAULT NULL,
      created_n INT NOT NULL DEFAULT 0,
      updated_n INT NOT NULL DEFAULT 0,
      cancelled_n INT NOT NULL DEFAULT 0,
      skipped_filter_n INT NOT NULL DEFAULT 0,
      unmapped_n INT NOT NULL DEFAULT 0,
      processed_n INT NOT NULL DEFAULT 0,
      errors_json TEXT,
      reverted_at TIMESTAMP NULL DEFAULT NULL,
      KEY idx_ssr_started (started_at),
      KEY idx_ssr_reverted (reverted_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS sling_sync_run_steps (
      id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      run_id BIGINT NOT NULL,
      schedule_shift_id BIGINT NOT NULL,
      external_id VARCHAR(255) DEFAULT NULL,
      action VARCHAR(24) NOT NULL,
      before_json LONGTEXT,
      KEY idx_ssrs_run (run_id),
      KEY idx_ssrs_shift (schedule_shift_id),
      CONSTRAINT fk_ssrs_run FOREIGN KEY (run_id) REFERENCES sling_sync_runs(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # open shifts claim approvals + module settings
    """
    CREATE TABLE IF NOT EXISTS schedule_open_shift_claims (
      id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      shift_id BIGINT NOT NULL,
      claimer_contractor_id INT NOT NULL,
      status ENUM('claimed','approved','rejected','cancelled') NOT NULL DEFAULT 'claimed',
      claimed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      resolved_at DATETIME DEFAULT NULL,
      resolved_by_user_id INT DEFAULT NULL,
      admin_notes TEXT,
      UNIQUE KEY uq_os_shift (shift_id),
      KEY idx_os_status (status, claimed_at),
      CONSTRAINT fk_os_shift FOREIGN KEY (shift_id) REFERENCES schedule_shifts(id) ON DELETE CASCADE,
      CONSTRAINT fk_os_claimer FOREIGN KEY (claimer_contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS schedule_settings (
      id TINYINT NOT NULL PRIMARY KEY,
      open_shift_claim_mode ENUM('auto','manager') NOT NULL DEFAULT 'auto',
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    SQL_CREATE_SCHEDULE_TEMPLATES,
    SQL_CREATE_SCHEDULE_TEMPLATE_SLOTS,
    """
    CREATE TABLE IF NOT EXISTS schedule_job_type_requirements (
      job_type_id INT NOT NULL PRIMARY KEY,
      required_skills_json JSON NULL,
      required_qualifications_json JSON NULL,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      CONSTRAINT fk_sjtr_jobtype FOREIGN KEY (job_type_id) REFERENCES job_types(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS schedule_shift_audit (
      id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      shift_id BIGINT NOT NULL,
      action VARCHAR(50) NOT NULL,
      actor_user_id INT DEFAULT NULL,
      actor_username VARCHAR(150) DEFAULT NULL,
      details_json JSON DEFAULT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      KEY idx_shift_audit_shift (shift_id),
      KEY idx_shift_audit_created (created_at),
      CONSTRAINT fk_shift_audit_shift FOREIGN KEY (shift_id) REFERENCES schedule_shifts(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS schedule_shift_tasks (
      id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      shift_id BIGINT NOT NULL,
      title VARCHAR(255) NOT NULL,
      sort_order INT NOT NULL DEFAULT 0,
      completed_at DATETIME DEFAULT NULL,
      completed_by_contractor_id INT DEFAULT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      KEY idx_sst_shift (shift_id),
      CONSTRAINT fk_sst_shift FOREIGN KEY (shift_id) REFERENCES schedule_shifts(id) ON DELETE CASCADE,
      CONSTRAINT fk_sst_contractor FOREIGN KEY (completed_by_contractor_id) REFERENCES tb_contractors(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS schedule_clock_locations (
      id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      site_id INT NOT NULL,
      latitude DECIMAL(10,7) NOT NULL,
      longitude DECIMAL(10,7) NOT NULL,
      radius_meters INT NOT NULL DEFAULT 100,
      name VARCHAR(100) DEFAULT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uq_scl_site (site_id),
      CONSTRAINT fk_scl_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS schedule_assignment_instructions (
      client_id INT NOT NULL PRIMARY KEY,
      instructions TEXT DEFAULT NULL,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      CONSTRAINT fk_sai_client FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


def _benign_ddl_error(exc):
    """MySQL codes where re-running upgrade/install should continue (already applied)."""
    code = getattr(exc, "errno", None)
    return code in (
        1050,  # table exists
        1060,  # duplicate column
        1061,  # duplicate key name
        1826,  # duplicate foreign key constraint name (MySQL 8+)
    )


def _run_sql(conn, sql):
    for stmt in [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]:
        cur = conn.cursor()
        try:
            try:
                cur.execute(stmt)
            except Exception as e:
                if _benign_ddl_error(e):
                    continue
                raise
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


def _table_exists(conn, table):
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE %s", (table,))
        return bool(cur.fetchone())
    finally:
        cur.close()


def _ensure_time_off_review_columns(conn):
    """Add reviewed_at, reviewed_by_user_id, admin_notes for approval workflow."""
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "schedule_time_off", "reviewed_at"):
            cur.execute("ALTER TABLE schedule_time_off ADD COLUMN reviewed_at DATETIME DEFAULT NULL AFTER status")
        if not _column_exists(conn, "schedule_time_off", "reviewed_by_user_id"):
            cur.execute("ALTER TABLE schedule_time_off ADD COLUMN reviewed_by_user_id INT DEFAULT NULL AFTER reviewed_at")
        if not _column_exists(conn, "schedule_time_off", "admin_notes"):
            cur.execute("ALTER TABLE schedule_time_off ADD COLUMN admin_notes TEXT DEFAULT NULL AFTER reviewed_by_user_id")
        conn.commit()
    finally:
        cur.close()


def _ensure_time_off_time_columns(conn):
    """Add start_time, end_time for partial-day time off (e.g. appointment vs whole-day holiday)."""
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "schedule_time_off", "start_time"):
            cur.execute("ALTER TABLE schedule_time_off ADD COLUMN start_time TIME DEFAULT NULL AFTER end_date")
        if not _column_exists(conn, "schedule_time_off", "end_time"):
            cur.execute("ALTER TABLE schedule_time_off ADD COLUMN end_time TIME DEFAULT NULL AFTER start_time")
        conn.commit()
    finally:
        cur.close()


def _ensure_open_shift_claims_columns(conn):
    """Add resolved_by_user_id and admin_notes to schedule_open_shift_claims for existing DBs."""
    if not _table_exists(conn, "schedule_open_shift_claims"):
        return
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "schedule_open_shift_claims", "resolved_by_user_id"):
            cur.execute(
                "ALTER TABLE schedule_open_shift_claims ADD COLUMN resolved_by_user_id INT DEFAULT NULL AFTER status"
            )
        if not _column_exists(conn, "schedule_open_shift_claims", "admin_notes"):
            cur.execute(
                "ALTER TABLE schedule_open_shift_claims ADD COLUMN admin_notes TEXT DEFAULT NULL AFTER resolved_by_user_id"
            )
        conn.commit()
    finally:
        cur.close()


def _ensure_required_count_and_assignments(conn):
    """Add required_count to schedule_shifts and migrate contractor_id into schedule_shift_assignments."""
    if not _table_exists(conn, "schedule_shifts"):
        return
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "schedule_shifts", "required_count"):
            cur.execute(
                "ALTER TABLE schedule_shifts ADD COLUMN required_count INT NOT NULL DEFAULT 1 AFTER recurrence_id"
            )
            conn.commit()
        cur.close()
    except Exception:
        try:
            cur.close()
        except Exception:
            pass
    if not _table_exists(conn, "schedule_shift_assignments"):
        return
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO schedule_shift_assignments (shift_id, contractor_id)
            SELECT s.id, s.contractor_id FROM schedule_shifts s
            WHERE s.contractor_id IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM schedule_shift_assignments a
                WHERE a.shift_id = s.id AND a.contractor_id = s.contractor_id
            )
        """)
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()


def _ensure_shared_labour_hours_column(conn):
    """Optional fixed person-hour budget for shared-crew visits (cleaning, etc.)."""
    if not _table_exists(conn, "schedule_shifts"):
        return
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "schedule_shifts", "shared_labour_hours"):
            cur.execute(
                "ALTER TABLE schedule_shifts ADD COLUMN shared_labour_hours DECIMAL(6,2) NULL "
                "DEFAULT NULL COMMENT 'Person-hours, wall-clock duration = this divided by crew size' AFTER labour_cost"
            )
            conn.commit()
    finally:
        cur.close()


def _ensure_portal_reminder_sent_at_column(conn):
    """Web push dedupe: one 'starting soon' per shift after send."""
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'schedule_shifts'")
        if not cur.fetchone():
            return
        if not _column_exists(conn, "schedule_shifts", "portal_reminder_sent_at"):
            cur.execute(
                "ALTER TABLE schedule_shifts ADD COLUMN portal_reminder_sent_at TIMESTAMP NULL DEFAULT NULL AFTER updated_at"
            )
            conn.commit()
    finally:
        cur.close()


def _ensure_recurrence_id_column(conn):
    """Add recurrence_id to schedule_shifts for series/delete-in-series support."""
    if not _table_exists(conn, "schedule_shifts"):
        return
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "schedule_shifts", "recurrence_id"):
            cur.execute(
                "ALTER TABLE schedule_shifts ADD COLUMN recurrence_id BIGINT DEFAULT NULL AFTER labour_cost"
            )
            conn.commit()
        cur.execute("SHOW INDEX FROM schedule_shifts WHERE Key_name = 'idx_ss_recurrence'")
        if not cur.fetchone():
            try:
                cur.execute("ALTER TABLE schedule_shifts ADD KEY idx_ss_recurrence (recurrence_id)")
                conn.commit()
            except Exception:
                pass
    finally:
        cur.close()


def ensure_tables(conn):
    for sql in CREATES:
        if "schedule_clock_locations" in sql:
            if not _table_exists(conn, "sites"):
                continue  # time_billing sites not present; skip FK to sites
        if "schedule_assignment_instructions" in sql:
            if not _table_exists(conn, "clients"):
                continue  # time_billing clients not present; skip FK to clients
        _run_sql(conn, sql)
    # If sites appeared later (e.g. time_billing installed after scheduling), ensure clock_locations table exists
    if _table_exists(conn, "sites") and not _table_exists(conn, "schedule_clock_locations"):
        for sql in CREATES:
            if "schedule_clock_locations" in sql:
                _run_sql(conn, sql)
                break
    if _table_exists(conn, "clients") and not _table_exists(conn, "schedule_assignment_instructions"):
        for sql in CREATES:
            if "schedule_assignment_instructions" in sql:
                _run_sql(conn, sql)
                break
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'schedule_time_off'")
        if cur.fetchone() and not _column_exists(conn, "schedule_time_off", "type"):
            cur.execute(
                "ALTER TABLE schedule_time_off ADD COLUMN type ENUM('annual','sickness','other') NOT NULL DEFAULT 'annual' AFTER contractor_id"
            )
            conn.commit()
    finally:
        cur.close()
    # Open shifts support: allow NULL contractor_id (unassigned)
    try:
        cur = conn.cursor()
        cur.execute("SHOW TABLES LIKE 'schedule_shifts'")
        if cur.fetchone():
            cur.execute("SHOW COLUMNS FROM schedule_shifts LIKE 'contractor_id'")
            col = cur.fetchone() or {}
            # When run with non-dict cursor, fallback check via string
            col_type = str(col.get("Type") or col.get("type") or "").lower() if isinstance(col, dict) else ""
            is_nullable = str(col.get("Null") or "").upper() == "YES" if isinstance(col, dict) else False
            if not is_nullable:
                # Drop and recreate FK to allow SET NULL, then change column nullability
                try:
                    cur.execute("ALTER TABLE schedule_shifts DROP FOREIGN KEY fk_ss_contractor")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE schedule_shifts MODIFY contractor_id INT NULL")
                except Exception:
                    pass
                try:
                    cur.execute(
                        "ALTER TABLE schedule_shifts ADD CONSTRAINT fk_ss_contractor "
                        "FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE SET NULL"
                    )
                except Exception:
                    pass
                conn.commit()
        cur.close()
    except Exception:
        try:
            cur.close()
        except Exception:
            pass
    _ensure_time_off_review_columns(conn)
    _ensure_time_off_time_columns(conn)
    _ensure_open_shift_claims_columns(conn)
    _ensure_recurrence_id_column(conn)
    _ensure_required_count_and_assignments(conn)
    _ensure_shared_labour_hours_column(conn)
    _ensure_portal_reminder_sent_at_column(conn)
    # Ensure status ENUM includes 'cancelled' (existing DBs)
    try:
        cur = conn.cursor()
        cur.execute(
            "ALTER TABLE schedule_time_off MODIFY COLUMN status "
            "ENUM('requested','approved','rejected','cancelled') NOT NULL DEFAULT 'requested'"
        )
        conn.commit()
        cur.close()
    except Exception:
        pass
    # Seed schedule_settings row and labor columns
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO schedule_settings (id, open_shift_claim_mode) VALUES (1, 'auto') "
            "ON DUPLICATE KEY UPDATE id = id"
        )
        conn.commit()
        cur.close()
    except Exception:
        pass
    if _column_exists(conn, "schedule_settings", "open_shift_claim_mode"):
        for col, sql in [
            ("overtime_hours_per_week", "ALTER TABLE schedule_settings ADD COLUMN overtime_hours_per_week INT NOT NULL DEFAULT 40 AFTER open_shift_claim_mode"),
            ("weekly_budget", "ALTER TABLE schedule_settings ADD COLUMN weekly_budget DECIMAL(10,2) DEFAULT NULL AFTER overtime_hours_per_week"),
            ("assignment_label", "ALTER TABLE schedule_settings ADD COLUMN assignment_label VARCHAR(64) NOT NULL DEFAULT 'Client' AFTER weekly_budget"),
            ("sling_default_job_type_id", "ALTER TABLE schedule_settings ADD COLUMN sling_default_job_type_id INT DEFAULT NULL AFTER assignment_label"),
            ("sling_default_client_id", "ALTER TABLE schedule_settings ADD COLUMN sling_default_client_id INT DEFAULT NULL AFTER sling_default_job_type_id"),
            ("sling_default_site_id", "ALTER TABLE schedule_settings ADD COLUMN sling_default_site_id INT DEFAULT NULL AFTER sling_default_client_id"),
            ("sling_cancel_missing", "ALTER TABLE schedule_settings ADD COLUMN sling_cancel_missing TINYINT(1) NOT NULL DEFAULT 1 AFTER sling_default_site_id"),
            (
                "sling_import_filter_mode",
                "ALTER TABLE schedule_settings ADD COLUMN sling_import_filter_mode VARCHAR(16) NOT NULL DEFAULT 'all' AFTER sling_cancel_missing",
            ),
            (
                "sling_import_filter_patterns",
                "ALTER TABLE schedule_settings ADD COLUMN sling_import_filter_patterns TEXT NULL AFTER sling_import_filter_mode",
            ),
        ]:
            try:
                if not _column_exists(conn, "schedule_settings", col):
                    cur = conn.cursor()
                    cur.execute(sql)
                    conn.commit()
                    cur.close()
            except Exception:
                try:
                    cur.close()
                except Exception:
                    pass

    if _table_exists(conn, "sling_credentials") and not _column_exists(conn, "sling_credentials", "sling_org_id"):
        try:
            cur = conn.cursor()
            cur.execute(
                "ALTER TABLE sling_credentials ADD COLUMN sling_org_id INT DEFAULT NULL AFTER sling_base_url"
            )
            conn.commit()
            cur.close()
        except Exception:
            try:
                cur.close()
            except Exception:
                pass


def install():
    """Ensure all MODULE_TABLES exist (idempotent). Creates missing tables and adds missing columns to existing ones."""
    conn = get_db_connection()
    try:
        ensure_tables(conn)
    finally:
        conn.close()


def upgrade():
    """Idempotent upgrade: runs same ensure_tables() as install. Use after code updates to add new tables/columns."""
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
    parser = argparse.ArgumentParser(description="Scheduling Module Installer")
    parser.add_argument("command", choices=["install", "upgrade", "uninstall"])
    parser.add_argument("--drop-data", action="store_true")
    args = parser.parse_args()
    if args.command == "install":
        install()
    elif args.command == "upgrade":
        upgrade()
    else:
        uninstall(drop_data=args.drop_data)
