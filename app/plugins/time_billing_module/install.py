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


def _split_sql(statements_text):
    """
    Simple splitter by semicolons.
    Assumes no procedural delimiter changes in files.
    Skips empty statements.
    """
    parts = [s.strip() for s in statements_text.split(";")]
    return [p for p in parts if p]


def _run_sql_file(conn, path):
    """Run all SQL statements from a file."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    cur = conn.cursor()
    try:
        for stmt in _split_sql(sql):
            if stmt:
                cur.execute(stmt)
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
