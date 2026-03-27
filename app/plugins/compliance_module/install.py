"""
Compliance module — policies and acknowledgements.
python app/plugins/compliance_module/install.py install
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
for p in (str(HERE.parent.parent.parent), str(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)
from app.objects import get_db_connection  # noqa: E402

MIGRATIONS_TABLE = "comp_migrations"
MODULE_TABLES = [MIGRATIONS_TABLE, "comp_policy_acknowledgements", "comp_policies"]

SQL_CREATE_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS comp_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_POLICIES = """
CREATE TABLE IF NOT EXISTS comp_policies (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  slug VARCHAR(191) NOT NULL,
  category VARCHAR(64) NOT NULL DEFAULT 'other',
  summary TEXT,
  body_text TEXT,
  file_path VARCHAR(512) DEFAULT NULL,
  version INT NOT NULL DEFAULT 1,
  published TINYINT(1) NOT NULL DEFAULT 0,
  mandatory TINYINT(1) NOT NULL DEFAULT 1,
  published_at DATETIME DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_comp_policy_slug (slug),
  KEY idx_comp_published (published, mandatory),
  KEY idx_comp_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_ACKS = """
CREATE TABLE IF NOT EXISTS comp_policy_acknowledgements (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  policy_id INT NOT NULL,
  contractor_id INT NOT NULL,
  version INT NOT NULL,
  acknowledged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ip_address VARCHAR(45) DEFAULT NULL,
  user_agent VARCHAR(512) DEFAULT NULL,
  UNIQUE KEY uq_comp_ack (policy_id, contractor_id, version),
  KEY idx_comp_ack_contractor (contractor_id),
  CONSTRAINT fk_comp_ack_policy FOREIGN KEY (policy_id) REFERENCES comp_policies(id) ON DELETE CASCADE,
  CONSTRAINT fk_comp_ack_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

CREATES = [SQL_CREATE_MIGRATIONS, SQL_CREATE_POLICIES, SQL_CREATE_ACKS]


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


def _column_exists(conn, table: str, column: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM `{}` LIKE %s".format(table), (column,))
        return bool(cur.fetchone())
    finally:
        cur.close()


def _ensure_comp_policy_review_columns(conn):
    """Admin-set review schedule + acknowledgement library metadata."""
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "comp_policies", "next_review_date"):
            cur.execute(
                "ALTER TABLE comp_policies ADD COLUMN next_review_date DATE DEFAULT NULL AFTER published_at"
            )
        if not _column_exists(conn, "comp_policies", "last_reviewed_date"):
            cur.execute(
                "ALTER TABLE comp_policies ADD COLUMN last_reviewed_date DATE DEFAULT NULL AFTER next_review_date"
            )
        conn.commit()
    finally:
        cur.close()


def install():
    conn = get_db_connection()
    try:
        ensure_tables(conn)
        _ensure_comp_policy_review_columns(conn)
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
