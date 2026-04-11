"""
Work module install/upgrade: work_migrations, work_photos, work_visits.
Depends on schedule_shifts and tb_contractors.
Run from repo root: python -m app.plugins.work_module.install upgrade
Or: python app/plugins/work_module/install.py upgrade
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

MIGRATIONS_TABLE = "work_migrations"
# Drop order (reversed): work_photos -> work_visits -> work_migrations
MODULE_TABLES = [MIGRATIONS_TABLE, "work_visits", "work_photos"]

SQL_CREATE_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS work_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_WORK_PHOTOS = """
CREATE TABLE IF NOT EXISTS work_photos (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  shift_id BIGINT NOT NULL,
  contractor_id INT NOT NULL,
  file_path VARCHAR(512) NOT NULL,
  file_name VARCHAR(255) DEFAULT NULL,
  mime_type VARCHAR(128) DEFAULT NULL,
  caption VARCHAR(500) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_wp_shift (shift_id),
  KEY idx_wp_contractor (contractor_id),
  CONSTRAINT fk_wp_shift FOREIGN KEY (shift_id) REFERENCES schedule_shifts(id) ON DELETE CASCADE,
  CONSTRAINT fk_wp_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

CREATES = [SQL_CREATE_MIGRATIONS, SQL_CREATE_WORK_PHOTOS]


def _run_sql(conn, sql):
    for stmt in [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]:
        cur = conn.cursor()
        try:
            cur.execute(stmt)
        finally:
            cur.close()
    conn.commit()


def _table_exists(conn, table: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE %s", (table,))
        return cur.fetchone() is not None
    finally:
        cur.close()


def _column_exists(conn, table: str, column: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
            """,
            (table, column),
        )
        row = cur.fetchone()
        return bool(row and row[0])
    finally:
        cur.close()


def _create_work_visits(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            """
CREATE TABLE IF NOT EXISTS work_visits (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  schedule_shift_id BIGINT NOT NULL,
  client_id INT NOT NULL,
  site_id INT DEFAULT NULL,
  job_type_id INT NOT NULL,
  contractor_id INT NOT NULL,
  work_date DATE NOT NULL,
  runsheet_assignment_id BIGINT DEFAULT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'open',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_wv_shift (schedule_shift_id),
  KEY idx_wv_client_date (client_id, work_date),
  KEY idx_wv_contractor_date (contractor_id, work_date),
  CONSTRAINT fk_wv_shift FOREIGN KEY (schedule_shift_id) REFERENCES schedule_shifts(id) ON DELETE CASCADE,
  CONSTRAINT fk_wv_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
        )
        conn.commit()
    finally:
        cur.close()


def _backfill_visits_and_photo_visit_ids(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO work_visits (
              schedule_shift_id, client_id, site_id, job_type_id, contractor_id, work_date,
              runsheet_assignment_id, status
            )
            SELECT DISTINCT
              ss.id, ss.client_id, ss.site_id, ss.job_type_id, ss.contractor_id, ss.work_date,
              ss.runsheet_assignment_id,
              CASE WHEN ss.actual_end IS NOT NULL THEN 'completed' ELSE 'open' END
            FROM schedule_shifts ss
            INNER JOIN work_photos p ON p.shift_id = ss.id
            LEFT JOIN work_visits v ON v.schedule_shift_id = ss.id
            WHERE v.id IS NULL
            """
        )
        cur.execute(
            """
            UPDATE work_photos p
            INNER JOIN work_visits v ON v.schedule_shift_id = p.shift_id
            SET p.visit_id = v.id
            WHERE p.visit_id IS NULL
            """
        )
        conn.commit()
    finally:
        cur.close()


def _add_visit_id_to_photos(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            ALTER TABLE work_photos
              ADD COLUMN visit_id BIGINT DEFAULT NULL AFTER shift_id,
              ADD KEY idx_wp_visit (visit_id),
              ADD CONSTRAINT fk_wp_visit FOREIGN KEY (visit_id) REFERENCES work_visits(id) ON DELETE SET NULL
            """
        )
        conn.commit()
    finally:
        cur.close()


def ensure_work_visits_schema(conn):
    """Idempotent: work_visits table + work_photos.visit_id + one-time backfill."""
    if not _table_exists(conn, "schedule_shifts"):
        return
    _create_work_visits(conn)
    if _table_exists(conn, "work_photos") and not _column_exists(conn, "work_photos", "visit_id"):
        _add_visit_id_to_photos(conn)
        _backfill_visits_and_photo_visit_ids(conn)


def ensure_tables(conn):
    for sql in CREATES:
        _run_sql(conn, sql)
    ensure_work_visits_schema(conn)


def install():
    """Ensure all module tables and columns exist (idempotent)."""
    conn = get_db_connection()
    try:
        ensure_tables(conn)
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

    parser = argparse.ArgumentParser(description="Work Module Installer")
    parser.add_argument("command", choices=["install", "upgrade", "uninstall"])
    parser.add_argument("--drop-data", action="store_true")
    args = parser.parse_args()
    if args.command == "install":
        install()
    elif args.command == "upgrade":
        upgrade()
    else:
        uninstall(drop_data=args.drop_data)
