"""
Training module install/upgrade.

Legacy: training_items, training_assignments, training_completions.
New: trn_* (courses, versions, modules, lessons, questions, assignments, progress, etc.)

Run: python app/plugins/training_module/install.py install
"""
import sys
from datetime import date, datetime, timedelta
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

MIGRATIONS_TABLE = "training_migrations"
MODULE_TABLES = [
    MIGRATIONS_TABLE,
    "training_completions",
    "training_assignments",
    "training_items",
    "trn_audit_log",
    "trn_person_competencies",
    "trn_course_assignment_rules",
    "trn_question_options",
    "trn_questions",
    "trn_quiz_attempts",
    "trn_lesson_progress",
    "trn_competency_signoffs",
    "trn_certificates",
    "trn_exemptions",
    "trn_assignments",
    "trn_lessons",
    "trn_modules",
    "trn_course_versions",
    "trn_courses",
]

SQL_CREATE_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS training_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRAINING_ITEMS = """
CREATE TABLE IF NOT EXISTS training_items (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  slug VARCHAR(120) NOT NULL,
  summary TEXT,
  content LONGTEXT,
  item_type ENUM('document','link','acknowledgement') NOT NULL DEFAULT 'document',
  external_url VARCHAR(512) DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_slug (slug),
  KEY idx_active (active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRAINING_ASSIGNMENTS = """
CREATE TABLE IF NOT EXISTS training_assignments (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  training_item_id INT NOT NULL,
  contractor_id INT NOT NULL,
  assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  due_date DATE DEFAULT NULL,
  mandatory TINYINT(1) NOT NULL DEFAULT 0,
  assigned_by_user_id INT DEFAULT NULL,
  KEY idx_item (training_item_id),
  KEY idx_contractor (contractor_id),
  KEY idx_due (due_date),
  CONSTRAINT fk_ta_item FOREIGN KEY (training_item_id) REFERENCES training_items(id) ON DELETE CASCADE,
  CONSTRAINT fk_ta_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRAINING_COMPLETIONS = """
CREATE TABLE IF NOT EXISTS training_completions (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  assignment_id INT NOT NULL,
  completed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  notes VARCHAR(500) DEFAULT NULL,
  UNIQUE KEY uq_assignment (assignment_id),
  KEY idx_completed (completed_at),
  CONSTRAINT fk_tc_assignment FOREIGN KEY (assignment_id) REFERENCES training_assignments(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_COURSES = """
CREATE TABLE IF NOT EXISTS trn_courses (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  slug VARCHAR(120) NOT NULL,
  summary TEXT,
  delivery_type VARCHAR(32) NOT NULL DEFAULT 'internal',
  refresher_interval_months INT DEFAULT NULL,
  grace_days INT NOT NULL DEFAULT 0,
  comp_policy_id INT DEFAULT NULL,
  require_certificate_verification TINYINT(1) NOT NULL DEFAULT 1,
  active TINYINT(1) NOT NULL DEFAULT 1,
  current_version_id INT DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_trn_course_slug (slug),
  KEY idx_trn_course_active (active),
  KEY idx_trn_delivery (delivery_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_COURSE_VERSIONS = """
CREATE TABLE IF NOT EXISTS trn_course_versions (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  course_id INT NOT NULL,
  version INT NOT NULL DEFAULT 1,
  published TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_trn_cv (course_id, version),
  KEY idx_trn_cv_course (course_id),
  CONSTRAINT fk_trn_cv_course FOREIGN KEY (course_id) REFERENCES trn_courses(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_MODULES = """
CREATE TABLE IF NOT EXISTS trn_modules (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  course_version_id INT NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  title VARCHAR(255) NOT NULL DEFAULT 'Module',
  KEY idx_trn_mod_cv (course_version_id),
  CONSTRAINT fk_trn_mod_cv FOREIGN KEY (course_version_id) REFERENCES trn_course_versions(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_LESSONS = """
CREATE TABLE IF NOT EXISTS trn_lessons (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  module_id INT NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  lesson_type VARCHAR(32) NOT NULL DEFAULT 'text',
  title VARCHAR(255) NOT NULL,
  body_text LONGTEXT,
  file_path VARCHAR(512) DEFAULT NULL,
  external_url VARCHAR(512) DEFAULT NULL,
  comp_policy_id INT DEFAULT NULL,
  pass_mark_percent TINYINT UNSIGNED DEFAULT NULL,
  max_quiz_attempts INT NOT NULL DEFAULT 3,
  KEY idx_trn_lesson_mod (module_id),
  CONSTRAINT fk_trn_lesson_mod FOREIGN KEY (module_id) REFERENCES trn_modules(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_QUESTIONS = """
CREATE TABLE IF NOT EXISTS trn_questions (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  lesson_id INT NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  question_text TEXT NOT NULL,
  KEY idx_trn_q_lesson (lesson_id),
  CONSTRAINT fk_trn_q_lesson FOREIGN KEY (lesson_id) REFERENCES trn_lessons(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_QUESTION_OPTIONS = """
CREATE TABLE IF NOT EXISTS trn_question_options (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  question_id INT NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  option_text VARCHAR(500) NOT NULL,
  is_correct TINYINT(1) NOT NULL DEFAULT 0,
  KEY idx_trn_opt_q (question_id),
  CONSTRAINT fk_trn_opt_q FOREIGN KEY (question_id) REFERENCES trn_questions(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_ASSIGNMENTS = """
CREATE TABLE IF NOT EXISTS trn_assignments (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  course_id INT NOT NULL,
  course_version_id INT NOT NULL,
  contractor_id INT NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'assigned',
  assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  due_date DATE DEFAULT NULL,
  grace_ends_at DATE DEFAULT NULL,
  mandatory TINYINT(1) NOT NULL DEFAULT 0,
  assigned_by_user_id INT DEFAULT NULL,
  completed_at TIMESTAMP NULL DEFAULT NULL,
  notes VARCHAR(500) DEFAULT NULL,
  legacy_source_assignment_id INT DEFAULT NULL,
  UNIQUE KEY uq_trn_legacy_assign (legacy_source_assignment_id),
  KEY idx_trn_a_contractor (contractor_id),
  KEY idx_trn_a_course (course_id),
  KEY idx_trn_a_status (contractor_id, status),
  KEY idx_trn_a_due (due_date),
  CONSTRAINT fk_trn_a_course FOREIGN KEY (course_id) REFERENCES trn_courses(id) ON DELETE CASCADE,
  CONSTRAINT fk_trn_a_cv FOREIGN KEY (course_version_id) REFERENCES trn_course_versions(id) ON DELETE CASCADE,
  CONSTRAINT fk_trn_a_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_LESSON_PROGRESS = """
CREATE TABLE IF NOT EXISTS trn_lesson_progress (
  assignment_id INT NOT NULL,
  lesson_id INT NOT NULL,
  completed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (assignment_id, lesson_id),
  KEY idx_trn_lp_lesson (lesson_id),
  CONSTRAINT fk_trn_lp_a FOREIGN KEY (assignment_id) REFERENCES trn_assignments(id) ON DELETE CASCADE,
  CONSTRAINT fk_trn_lp_l FOREIGN KEY (lesson_id) REFERENCES trn_lessons(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_QUIZ_ATTEMPTS = """
CREATE TABLE IF NOT EXISTS trn_quiz_attempts (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  assignment_id INT NOT NULL,
  lesson_id INT NOT NULL,
  score INT NOT NULL DEFAULT 0,
  max_score INT NOT NULL DEFAULT 0,
  passed TINYINT(1) NOT NULL DEFAULT 0,
  attempt_number INT NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_trn_qa_a (assignment_id),
  KEY idx_trn_qa_lesson (lesson_id),
  CONSTRAINT fk_trn_qa_a FOREIGN KEY (assignment_id) REFERENCES trn_assignments(id) ON DELETE CASCADE,
  CONSTRAINT fk_trn_qa_l FOREIGN KEY (lesson_id) REFERENCES trn_lessons(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_SIGNOFFS = """
CREATE TABLE IF NOT EXISTS trn_competency_signoffs (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  assignment_id INT NOT NULL,
  supervisor_user_id INT NOT NULL,
  signed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  comments TEXT,
  UNIQUE KEY uq_trn_signoff_a (assignment_id),
  CONSTRAINT fk_trn_so_a FOREIGN KEY (assignment_id) REFERENCES trn_assignments(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_CERTIFICATES = """
CREATE TABLE IF NOT EXISTS trn_certificates (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  assignment_id INT NOT NULL,
  provider VARCHAR(255) DEFAULT NULL,
  certificate_number VARCHAR(128) DEFAULT NULL,
  issued_at DATE DEFAULT NULL,
  expires_at DATE DEFAULT NULL,
  file_path VARCHAR(512) DEFAULT NULL,
  verified_by_user_id INT DEFAULT NULL,
  verified_at TIMESTAMP NULL DEFAULT NULL,
  KEY idx_trn_cert_a (assignment_id),
  CONSTRAINT fk_trn_cert_a FOREIGN KEY (assignment_id) REFERENCES trn_assignments(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_EXEMPTIONS = """
CREATE TABLE IF NOT EXISTS trn_exemptions (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  course_id INT NOT NULL,
  contractor_id INT NOT NULL,
  reason TEXT,
  granted_by_user_id INT DEFAULT NULL,
  exempt_until DATE DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_trn_ex (course_id, contractor_id),
  CONSTRAINT fk_trn_ex_course FOREIGN KEY (course_id) REFERENCES trn_courses(id) ON DELETE CASCADE,
  CONSTRAINT fk_trn_ex_c FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_COURSE_ASSIGNMENT_RULES = """
CREATE TABLE IF NOT EXISTS trn_course_assignment_rules (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  course_id INT NOT NULL,
  role_id INT DEFAULT NULL,
  contractor_type VARCHAR(64) DEFAULT NULL,
  due_date_offset_days INT DEFAULT NULL,
  mandatory TINYINT(1) NOT NULL DEFAULT 0,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_trn_rule_course (course_id),
  KEY idx_trn_rule_role (role_id),
  CONSTRAINT fk_trn_rule_course FOREIGN KEY (course_id) REFERENCES trn_courses(id) ON DELETE CASCADE,
  CONSTRAINT fk_trn_rule_role FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SQL_CREATE_TRN_AUDIT = """
CREATE TABLE IF NOT EXISTS trn_audit_log (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  event_type VARCHAR(64) NOT NULL,
  entity_table VARCHAR(64) NOT NULL,
  entity_id INT NOT NULL,
  contractor_id INT DEFAULT NULL,
  actor_user_id INT DEFAULT NULL,
  payload_json TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_trn_audit_c (contractor_id),
  KEY idx_trn_audit_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# HR-linked register: skills, qualification evidence (file + expiry), clinical grade for CAD/dispatch.
SQL_CREATE_TRN_PERSON_COMPETENCIES = """
CREATE TABLE IF NOT EXISTS trn_person_competencies (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  competency_kind ENUM('skill','qualification','clinical_grade') NOT NULL,
  label VARCHAR(255) NOT NULL,
  use_hr_job_title TINYINT(1) NOT NULL DEFAULT 0,
  file_path VARCHAR(512) DEFAULT NULL,
  issued_on DATE DEFAULT NULL,
  expires_on DATE DEFAULT NULL,
  notes TEXT,
  created_by_user_id INT DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_trn_pc_contractor (contractor_id),
  KEY idx_trn_pc_expires (contractor_id, expires_on),
  CONSTRAINT fk_trn_pc_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

LEGACY_MIGRATION_FILENAME = "legacy_training_migrated_to_trn_v1"

CREATES = [
    SQL_CREATE_MIGRATIONS,
    SQL_CREATE_TRAINING_ITEMS,
    SQL_CREATE_TRAINING_ASSIGNMENTS,
    SQL_CREATE_TRAINING_COMPLETIONS,
    SQL_CREATE_TRN_COURSES,
    SQL_CREATE_TRN_COURSE_VERSIONS,
    SQL_CREATE_TRN_MODULES,
    SQL_CREATE_TRN_LESSONS,
    SQL_CREATE_TRN_QUESTIONS,
    SQL_CREATE_TRN_QUESTION_OPTIONS,
    SQL_CREATE_TRN_ASSIGNMENTS,
    SQL_CREATE_TRN_LESSON_PROGRESS,
    SQL_CREATE_TRN_QUIZ_ATTEMPTS,
    SQL_CREATE_TRN_SIGNOFFS,
    SQL_CREATE_TRN_CERTIFICATES,
    SQL_CREATE_TRN_EXEMPTIONS,
    SQL_CREATE_TRN_COURSE_ASSIGNMENT_RULES,
    SQL_CREATE_TRN_AUDIT,
    SQL_CREATE_TRN_PERSON_COMPETENCIES,
]


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
        return bool(cur.fetchone())
    finally:
        cur.close()


def _migration_applied(conn, filename: str) -> bool:
    if not _table_exists(conn, MIGRATIONS_TABLE):
        return False
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT 1 FROM `{MIGRATIONS_TABLE}` WHERE filename = %s LIMIT 1",
            (filename,),
        )
        return bool(cur.fetchone())
    finally:
        cur.close()


def _mark_migration(conn, filename: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f"INSERT IGNORE INTO `{MIGRATIONS_TABLE}` (filename) VALUES (%s)",
            (filename,),
        )
        conn.commit()
    finally:
        cur.close()


def _add_fk_current_version(conn):
    """Add FK from trn_courses.current_version_id after versions exist (idempotent)."""
    if not _table_exists(conn, "trn_courses"):
        return
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'trn_courses'
              AND CONSTRAINT_NAME = 'fk_trn_course_current_version'
            """
        )
        if cur.fetchone():
            return
        cur.execute(
            """
            ALTER TABLE trn_courses
            ADD CONSTRAINT fk_trn_course_current_version
            FOREIGN KEY (current_version_id) REFERENCES trn_course_versions(id) ON DELETE SET NULL
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


def _migrate_legacy_to_trn(conn):
    """One-time: training_items/assignments/completions -> trn_*."""
    if _migration_applied(conn, LEGACY_MIGRATION_FILENAME):
        return
    if not _table_exists(conn, "training_items") or not _table_exists(conn, "trn_courses"):
        _mark_migration(conn, LEGACY_MIGRATION_FILENAME)
        return
    cur = conn.cursor(dictionary=True)
    ins = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) AS n FROM trn_courses")
        if (cur.fetchone() or {}).get("n", 0) > 0:
            _mark_migration(conn, LEGACY_MIGRATION_FILENAME)
            return
        cur.execute("SELECT COUNT(*) AS n FROM training_items")
        if (cur.fetchone() or {}).get("n", 0) == 0:
            _mark_migration(conn, LEGACY_MIGRATION_FILENAME)
            return
        cur.execute("SELECT * FROM training_items ORDER BY id")
        items = cur.fetchall() or []
        item_id_to_course: dict = {}
        for it in items:
            delivery = "internal"
            base = (it.get("slug") or f"legacy-{it['id']}")[:100]
            slug = f"{base}-{it['id']}"[:120]
            ins.execute(
                """
                INSERT INTO trn_courses (title, slug, summary, delivery_type, active, comp_policy_id)
                VALUES (%s, %s, %s, %s, %s, NULL)
                """,
                (
                    it["title"],
                    slug,
                    it.get("summary"),
                    delivery,
                    1 if int(it.get("active") or 1) else 0,
                ),
            )
            cid = ins.lastrowid
            ins.execute(
                """
                INSERT INTO trn_course_versions (course_id, version, published)
                VALUES (%s, 1, 1)
                """,
                (cid,),
            )
            vid = ins.lastrowid
            ins.execute(
                """
                INSERT INTO trn_modules (course_version_id, sort_order, title)
                VALUES (%s, 0, 'Content')
                """,
                (vid,),
            )
            mid = ins.lastrowid
            ltype = "link" if (it.get("item_type") or "").lower() == "link" else "text"
            ins.execute(
                """
                INSERT INTO trn_lessons (module_id, sort_order, lesson_type, title, body_text, file_path, external_url)
                VALUES (%s, 0, %s, %s, %s, NULL, %s)
                """,
                (
                    mid,
                    ltype,
                    it["title"],
                    it.get("content"),
                    it.get("external_url") if ltype == "link" else None,
                ),
            )
            ins.execute("UPDATE trn_courses SET current_version_id = %s WHERE id = %s", (vid, cid))
            item_id_to_course[int(it["id"])] = (cid, vid)
        cur.execute("SELECT * FROM training_assignments ORDER BY id")
        assigns = cur.fetchall() or []
        for a in assigns:
            tid = int(a["training_item_id"])
            if tid not in item_id_to_course:
                continue
            cid, vid = item_id_to_course[tid]
            cur.execute(
                "SELECT id, completed_at, notes FROM training_completions WHERE assignment_id = %s LIMIT 1",
                (int(a["id"]),),
            )
            crow = cur.fetchone()
            has_comp = bool(crow)
            status = "passed" if has_comp else "assigned"
            due = a.get("due_date")
            if isinstance(due, datetime):
                due = due.date()
            cur.execute("SELECT grace_days FROM trn_courses WHERE id = %s", (cid,))
            grow = cur.fetchone()
            grace_days = int(grow["grace_days"] or 0) if grow else 0
            grace_end = None
            if due and grace_days:
                try:
                    grace_end = due + timedelta(days=int(grace_days))
                except TypeError:
                    grace_end = None
            comp_at = crow["completed_at"] if crow else None
            nnotes = (crow.get("notes") if crow else None) or None
            ins.execute(
                """
                INSERT INTO trn_assignments (
                  course_id, course_version_id, contractor_id, status, due_date, grace_ends_at,
                  mandatory, assigned_by_user_id, completed_at, notes, legacy_source_assignment_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    cid,
                    vid,
                    int(a["contractor_id"]),
                    status,
                    due,
                    grace_end,
                    int(a.get("mandatory") or 0),
                    a.get("assigned_by_user_id"),
                    comp_at,
                    nnotes,
                    int(a["id"]),
                ),
            )
            new_aid = ins.lastrowid
            if has_comp and crow:
                cur.execute(
                    """
                    SELECT l.id FROM trn_lessons l
                    JOIN trn_modules m ON m.id = l.module_id
                    WHERE m.course_version_id = %s ORDER BY l.sort_order LIMIT 1
                    """,
                    (vid,),
                )
                lrow = cur.fetchone()
                if lrow:
                    ins.execute(
                        """
                        INSERT IGNORE INTO trn_lesson_progress (assignment_id, lesson_id, completed_at)
                        VALUES (%s, %s, %s)
                        """,
                        (new_aid, int(lrow["id"]), comp_at),
                    )
        conn.commit()
        _mark_migration(conn, LEGACY_MIGRATION_FILENAME)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            ins.close()
        except Exception:
            pass
        try:
            cur.close()
        except Exception:
            pass


def ensure_tables(conn):
    for sql in CREATES:
        _run_sql(conn, sql)
    _add_fk_current_version(conn)


def install():
    conn = get_db_connection()
    try:
        ensure_tables(conn)
        _migrate_legacy_to_trn(conn)
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
        for t in MODULE_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS `{t}`")
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Training Module Installer")
    parser.add_argument("command", choices=["install", "upgrade", "uninstall"])
    parser.add_argument("--drop-data", action="store_true")
    args = parser.parse_args()
    if args.command == "install":
        install()
        print("Training module tables ready.")
    elif args.command == "upgrade":
        upgrade()
    else:
        uninstall(drop_data=args.drop_data)
