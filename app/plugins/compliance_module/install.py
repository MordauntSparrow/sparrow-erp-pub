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
# Drop order (reversed): acknowledgements → lifecycle audit → policies → document types → migrations
MODULE_TABLES = [
    MIGRATIONS_TABLE,
    "comp_document_types",
    "comp_policies",
    "comp_policy_lifecycle_audit",
    "comp_policy_acknowledgements",
]

SQL_CREATE_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS comp_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_DOCUMENT_TYPES = """
CREATE TABLE IF NOT EXISTS comp_document_types (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  slug VARCHAR(64) NOT NULL,
  label VARCHAR(128) NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_comp_doc_type_slug (slug),
  KEY idx_comp_doc_type_active (active, sort_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_POLICIES = """
CREATE TABLE IF NOT EXISTS comp_policies (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  slug VARCHAR(191) NOT NULL,
  category VARCHAR(255) NOT NULL DEFAULT '',
  document_type_id INT DEFAULT NULL,
  summary TEXT,
  body_text TEXT,
  file_path VARCHAR(512) DEFAULT NULL,
  version INT NOT NULL DEFAULT 1,
  published TINYINT(1) NOT NULL DEFAULT 0,
  lifecycle_status VARCHAR(16) NOT NULL DEFAULT 'draft',
  mandatory TINYINT(1) NOT NULL DEFAULT 1,
  expose_on_website TINYINT(1) NOT NULL DEFAULT 0,
  published_at DATETIME DEFAULT NULL,
  next_review_date DATE DEFAULT NULL,
  last_reviewed_date DATE DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_comp_policy_slug (slug),
  KEY idx_comp_published (published, mandatory),
  KEY idx_comp_category (category),
  KEY idx_comp_document_type (document_type_id),
  CONSTRAINT fk_comp_policy_document_type FOREIGN KEY (document_type_id) REFERENCES comp_document_types(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_POLICY_LIFECYCLE_AUDIT = """
CREATE TABLE IF NOT EXISTS comp_policy_lifecycle_audit (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  policy_id INT NOT NULL,
  from_status VARCHAR(16) NOT NULL,
  to_status VARCHAR(16) NOT NULL,
  reason TEXT NOT NULL,
  actor_label VARCHAR(255) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_comp_pla_policy_created (policy_id, created_at),
  CONSTRAINT fk_comp_pla_policy FOREIGN KEY (policy_id) REFERENCES comp_policies(id) ON DELETE CASCADE
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

CREATES = [
    SQL_CREATE_MIGRATIONS,
    SQL_CREATE_DOCUMENT_TYPES,
    SQL_CREATE_POLICIES,
    SQL_CREATE_POLICY_LIFECYCLE_AUDIT,
    SQL_CREATE_ACKS,
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


def _column_exists(conn, table: str, column: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM `{}` LIKE %s".format(table), (column,))
        return bool(cur.fetchone())
    finally:
        cur.close()


def _table_exists(conn, table: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s LIMIT 1",
            (table,),
        )
        return bool(cur.fetchone())
    finally:
        cur.close()


def _index_exists(conn, table: str, key_name: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SHOW INDEX FROM `{}` WHERE Key_name = %s".format(table), (key_name,))
        return bool(cur.fetchone())
    finally:
        cur.close()


def _fk_exists(conn, table: str, constraint_name: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_schema = DATABASE() AND table_name = %s AND constraint_name = %s LIMIT 1
            """,
            (table, constraint_name),
        )
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


def seed_document_types(conn) -> None:
    seeds = [
        ("policy", "Policy", 10),
        ("procedure", "Procedure", 20),
        ("sop", "SOP (Scope of practice)", 30),
        ("guideline", "Guideline", 40),
        ("protocol", "Protocol", 50),
        ("form", "Form / template", 60),
        ("other", "Other published document", 90),
    ]
    cur = conn.cursor()
    try:
        for slug, label, sort_order in seeds:
            cur.execute("SELECT id FROM comp_document_types WHERE slug = %s LIMIT 1", (slug,))
            if cur.fetchone():
                continue
            cur.execute(
                """
                INSERT INTO comp_document_types (slug, label, sort_order, active)
                VALUES (%s, %s, %s, 1)
                """,
                (slug, label, int(sort_order)),
            )
        conn.commit()
    finally:
        cur.close()


def _ensure_document_types_schema(conn):
    """Types table, policy.document_type_id FK, widen category → free-text topic."""
    cur = conn.cursor()
    try:
        if not _table_exists(conn, "comp_document_types"):
            _run_sql(conn, SQL_CREATE_DOCUMENT_TYPES)

        if _column_exists(conn, "comp_policies", "category"):
            cur.execute(
                """
                UPDATE comp_policies SET category = CASE category
                  WHEN 'health_safety' THEN 'Health & safety'
                  WHEN 'safeguarding' THEN 'Safeguarding'
                  WHEN 'privacy' THEN 'Privacy'
                  WHEN 'data_protection' THEN 'Data protection'
                  WHEN 'equal_opportunities' THEN 'Equal opportunities'
                  WHEN 'disciplinary' THEN 'Disciplinary / conduct'
                  WHEN 'other' THEN ''
                  ELSE category
                END
                WHERE category IN (
                  'health_safety','safeguarding','privacy','data_protection',
                  'equal_opportunities','disciplinary','other'
                )
                """
            )
            try:
                cur.execute("ALTER TABLE comp_policies MODIFY COLUMN category VARCHAR(255) NOT NULL DEFAULT ''")
            except Exception:
                pass

        if not _column_exists(conn, "comp_policies", "document_type_id"):
            cur.execute(
                "ALTER TABLE comp_policies ADD COLUMN document_type_id INT DEFAULT NULL AFTER category"
            )
        if not _index_exists(conn, "comp_policies", "idx_comp_document_type"):
            try:
                cur.execute("ALTER TABLE comp_policies ADD KEY idx_comp_document_type (document_type_id)")
            except Exception:
                pass
        if not _fk_exists(conn, "comp_policies", "fk_comp_policy_document_type"):
            try:
                cur.execute(
                    """
                    ALTER TABLE comp_policies
                    ADD CONSTRAINT fk_comp_policy_document_type
                    FOREIGN KEY (document_type_id) REFERENCES comp_document_types(id) ON DELETE SET NULL
                    """
                )
            except Exception:
                pass

        seed_document_types(conn)

        cur.execute("SELECT id FROM comp_document_types WHERE slug = 'policy' LIMIT 1")
        row = cur.fetchone()
        policy_type_id = int(row[0]) if row else None
        if policy_type_id:
            cur.execute(
                """
                UPDATE comp_policies SET document_type_id = %s
                WHERE document_type_id IS NULL
                """,
                (policy_type_id,),
            )
        conn.commit()
    finally:
        cur.close()


def _ensure_policy_lifecycle_status(conn):
    """draft | active | retired — kept in sync with published in application code."""
    cur = conn.cursor()
    try:
        added = False
        if not _column_exists(conn, "comp_policies", "lifecycle_status"):
            cur.execute(
                "ALTER TABLE comp_policies ADD COLUMN lifecycle_status VARCHAR(16) NOT NULL DEFAULT 'draft' AFTER published"
            )
            added = True
        if added:
            cur.execute(
                "UPDATE comp_policies SET lifecycle_status = IF(published = 1, 'active', 'draft')"
            )
        else:
            cur.execute(
                "UPDATE comp_policies SET published = 1 WHERE lifecycle_status = 'active' AND published = 0"
            )
            cur.execute(
                """
                UPDATE comp_policies SET published = 0
                WHERE lifecycle_status IN ('draft', 'retired') AND published = 1
                """
            )
        conn.commit()
    finally:
        cur.close()


def _ensure_policy_lifecycle_audit_table(conn):
    if not _table_exists(conn, "comp_policy_lifecycle_audit"):
        _run_sql(conn, SQL_CREATE_POLICY_LIFECYCLE_AUDIT)


def _ensure_expose_on_website_column(conn):
    """Opt-in: show Active documents on the public marketing site at /policies."""
    cur = conn.cursor()
    try:
        if not _column_exists(conn, "comp_policies", "expose_on_website"):
            cur.execute(
                "ALTER TABLE comp_policies ADD COLUMN expose_on_website TINYINT(1) NOT NULL DEFAULT 0 AFTER mandatory"
            )
        conn.commit()
    finally:
        cur.close()


def install():
    conn = get_db_connection()
    try:
        ensure_tables(conn)
        _ensure_comp_policy_review_columns(conn)
        _ensure_document_types_schema(conn)
        _ensure_policy_lifecycle_status(conn)
        _ensure_policy_lifecycle_audit_table(conn)
        _ensure_expose_on_website_column(conn)
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
