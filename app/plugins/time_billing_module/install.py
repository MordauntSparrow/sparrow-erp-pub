import os
import sys
from pathlib import Path

# =============================================================================
# Bootstrap import paths (project/app/plugin)
# =============================================================================
HERE = Path(__file__).resolve()
PLUGIN_DIR = HERE.parent              # .../app/plugins/time_billing_module
PLUGINS_DIR = PLUGIN_DIR.parent       # .../app/plugins
APP_ROOT = PLUGINS_DIR.parent         # .../app
PROJECT_ROOT = APP_ROOT.parent        # .../

for p in (str(PROJECT_ROOT), str(APP_ROOT), str(PLUGIN_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)
from app.objects import get_db_connection, AuthManager  # noqa: E402

# =============================================================================
# Migration Constants
# =============================================================================
SQL_DIR = os.path.join(os.path.dirname(__file__), "db")
MIGRATIONS_TABLE = "tb_time_billing_migrations"  # per-module ledger

# =============================================================================
# Internal Helpers
# =============================================================================


def _ensure_ledger(conn):
    """Ensure the migrations ledger table exists."""
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL UNIQUE,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
    finally:
        cur.close()


def _already_applied(conn, filename):
    """Check if a migration file has already been applied."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT 1 FROM {MIGRATIONS_TABLE} WHERE filename=%s", (filename,))
        row = cur.fetchone()
        return bool(row)
    finally:
        cur.close()


def _record_applied(conn, filename):
    """Record a migration file as applied in the ledger."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"INSERT INTO {MIGRATIONS_TABLE} (filename) VALUES (%s)", (filename,))
        conn.commit()
    finally:
        cur.close()


def _strip_sql_comments(text):
    """
    Remove SQL line comments (-- to EOL) so semicolons inside comments
    don't break statement splitting. Leaves block comments /* */ as-is
    (no semicolons usually inside). Safe for typical migration files.
    """
    lines = []
    for line in text.splitlines():
        # Remove trailing -- comment (ignore -- inside strings for simplicity)
        i = line.find("--")
        if i != -1:
            line = line[:i].rstrip()
        lines.append(line)
    return "\n".join(lines)


def _benign_ddl_error(exc):
    """Re-run safe: table/column/index/FK already present (ledger drift or partial past runs)."""
    code = getattr(exc, "errno", None)
    return code in (
        1050,  # ER_TABLE_EXISTS_ERROR
        1060,  # ER_DUP_FIELDNAME
        1061,  # ER_DUP_KEYNAME
        1826,  # duplicate foreign key constraint name (MySQL 8+)
    )


def _split_sql(statements_text):
    """
    Split into statements on ';' only when not inside parentheses or quotes.
    Naive ``split(';')`` breaks valid SQL when a semicolon appears inside a
    string (e.g. COMMENT '...; ...') or other nested context, producing fragments
    that MySQL reports as syntax errors near random tokens.
    """
    cleaned = _strip_sql_comments(statements_text)
    parts = []
    buf = []
    depth = 0
    i = 0
    n = len(cleaned)
    in_single = False
    in_double = False

    def push_char(c):
        buf.append(c)

    while i < n:
        c = cleaned[i]

        if in_single:
            push_char(c)
            if c == "'":
                if i + 1 < n and cleaned[i + 1] == "'":
                    push_char(cleaned[i + 1])
                    i += 2
                    continue
                in_single = False
            i += 1
            continue

        if in_double:
            push_char(c)
            if c == '"':
                if i + 1 < n and cleaned[i + 1] == '"':
                    push_char(cleaned[i + 1])
                    i += 2
                    continue
                in_double = False
            i += 1
            continue

        if c == "'":
            in_single = True
            push_char(c)
            i += 1
            continue
        if c == '"':
            in_double = True
            push_char(c)
            i += 1
            continue

        if c == "(":
            depth += 1
            push_char(c)
            i += 1
            continue
        if c == ")":
            depth = max(0, depth - 1)
            push_char(c)
            i += 1
            continue

        if c == ";" and depth == 0:
            stmt = "".join(buf).strip()
            if stmt:
                parts.append(stmt)
            buf = []
            i += 1
            continue

        push_char(c)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def _run_sql_file(conn, path):
    """Run all SQL statements from a file."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    cur = conn.cursor()
    fname = os.path.basename(path)
    try:
        stmts = _split_sql(sql)
        for idx, stmt in enumerate(stmts):
            if not stmt:
                continue
            try:
                cur.execute(stmt)
            except Exception as e:
                if _benign_ddl_error(e):
                    continue
                conn.rollback()
                preview = (stmt.replace("\n", " ").strip())[:240]
                raise RuntimeError(
                    f"{fname} statement {idx + 1}/{len(stmts)} failed: {e}\n"
                    f"--- preview ---\n{preview}"
                ) from e
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _list_sql_files():
    """List SQL files in lexical order to keep numbering sequence."""
    if not os.path.isdir(SQL_DIR):
        return []
    files = [f for f in os.listdir(SQL_DIR) if f.lower().endswith(".sql")]
    return sorted(files)


def _column_exists(conn, table: str, column: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            """,
            (table, column),
        )
        row = cur.fetchone()
        return bool(row and row[0])
    finally:
        cur.close()


def _ensure_job_types_colour_hex(conn):
    """
    Code expects job_types.colour_hex (runsheets, week payload, admin API).
    Migration 005 adds it; if the ledger was wrong or 005 was skipped, repair here.
    """
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'job_types'")
        if not cur.fetchone():
            return
        if _column_exists(conn, "job_types", "colour_hex"):
            return
        cur.execute(
            """
            ALTER TABLE job_types
              ADD COLUMN colour_hex VARCHAR(7) DEFAULT NULL
              COMMENT 'Hex colour e.g. #3366cc for badges/rows'
            """
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

# =============================================================================
# Public Migration Functions
# =============================================================================


def install(seed_demo: bool = False):
    """
    Fresh install: runs all SQL files in db/ in order, skipping those already applied.
    The canonical schema is in 001_schema.sql. Use seed_demo=True in dev to run optional demo seeds.
    """
    conn = get_db_connection()
    try:
        _ensure_ledger(conn)
        for fname in _list_sql_files():
            path = os.path.join(SQL_DIR, fname)
            if not _already_applied(conn, fname):
                _run_sql_file(conn, path)
                _record_applied(conn, fname)

        # Optional demo seed
        if seed_demo:
            demo_file = "008_seed_demo_contractors.sql"
            demo_path = os.path.join(SQL_DIR, demo_file)
            if os.path.exists(demo_path) and not _already_applied(conn, demo_file):
                _run_sql_file(conn, demo_path)
                _record_applied(conn, demo_file)

        _ensure_job_types_colour_hex(conn)
    finally:
        conn.close()


def upgrade():
    """
    Runs any new SQL files in db/ that aren’t recorded in the migrations ledger.
    Use this when you add 008_, 009_, etc.
    """
    conn = get_db_connection()
    try:
        _ensure_ledger(conn)
        for fname in _list_sql_files():
            if not _already_applied(conn, fname):
                _run_sql_file(conn, os.path.join(SQL_DIR, fname))
                _record_applied(conn, fname)

        _ensure_job_types_colour_hex(conn)
    finally:
        conn.close()


def uninstall(drop_data: bool = False):
    """
    Uninstalls the module.
    If drop_data=True, drops all module tables (destructive).
    """
    if not drop_data:
        return

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")

        # Drop in reverse dependency order
        tables = [
            "runsheet_template_pdf", "runsheet_template_fields", "runsheet_templates",
            "runsheet_assignments", "runsheets",
            "calendar_policies", "bank_holidays",
            "contractor_client_overrides",
            "bill_rate_rows", "bill_rate_cards",
            "wage_rate_rows", "wage_rate_cards",
            "tb_timesheet_entries", "tb_timesheet_weeks", "tb_contractors",
            "sites", "clients", "job_types", "roles",
            MIGRATIONS_TABLE,
        ]

        for t in tables:
            cur.execute(f"DROP TABLE IF EXISTS {t}")

        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()
    finally:
        cur.close()
        conn.close()


# =============================================================================
# Command-line Entrypoint
# =============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Time Billing Module Installer")
    parser.add_argument("command", choices=[
                        "install", "upgrade", "uninstall"], help="Action to perform")
    parser.add_argument("--seed-demo", action="store_true",
                        help="Seed demo data (install only)")
    parser.add_argument("--drop-data", action="store_true",
                        help="Drop all module tables (uninstall only)")

    args = parser.parse_args()

    if args.command == "install":
        print("[INSTALL] Running install...")
        install(seed_demo=args.seed_demo)
        print("[INSTALL] Complete.")
    elif args.command == "upgrade":
        print("[UPGRADE] Running upgrade...")
        upgrade()
        print("[UPGRADE] Complete.")
    elif args.command == "uninstall":
        print("[UNINSTALL] Running uninstall...")
        uninstall(drop_data=args.drop_data)
        print("[UNINSTALL] Complete.")
