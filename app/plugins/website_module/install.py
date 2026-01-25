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

MIGRATIONS_TABLE = "website_module_migrations"

# =============================================================================
# Tables
# =============================================================================

TBL_PV_DAILY = "website_page_views_daily"
TBL_PV_HOURLY = "website_page_views_hourly"
TBL_PV_COUNTRY_DAILY = "website_page_views_country_daily"

PV_DAILY_COLUMNS = [
    ("day", "DATE NOT NULL"),
    ("path", "VARCHAR(512) NOT NULL"),
    ("views", "INT NOT NULL DEFAULT 0"),
    ("created_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("updated_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
    ("PRIMARY KEY", "(day, path)"),
]

PV_HOURLY_COLUMNS = [
    ("day", "DATE NOT NULL"),
    ("hour", "TINYINT UNSIGNED NOT NULL"),  # 0-23
    ("views", "INT NOT NULL DEFAULT 0"),
    ("created_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("updated_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
    ("PRIMARY KEY", "(day, hour)"),
]

PV_COUNTRY_DAILY_COLUMNS = [
    ("day", "DATE NOT NULL"),
    ("country", "VARCHAR(120) NOT NULL"),
    ("views", "INT NOT NULL DEFAULT 0"),
    ("created_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("updated_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
    ("PRIMARY KEY", "(day, country)"),
]

PV_DAILY_INDEXES = [
    ("idx_website_pv_daily_day", "INDEX", "(day)"),
    ("idx_website_pv_daily_path", "INDEX", "(path)"),
]

PV_HOURLY_INDEXES = [
    ("idx_website_pv_hourly_day", "INDEX", "(day)"),
    ("idx_website_pv_hourly_hour", "INDEX", "(hour)"),
]

PV_COUNTRY_DAILY_INDEXES = [
    ("idx_website_pv_country_day", "INDEX", "(day)"),
    ("idx_website_pv_country_country", "INDEX", "(country)"),
]

# =============================================================================
# Helpers
# =============================================================================


def table_exists(conn, table_name):
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE %s", (table_name,))
        row = cur.fetchone()
        try:
            cur.fetchall()
        except Exception:
            pass
        return bool(row)
    finally:
        try:
            cur.close()
        except Exception:
            pass


def column_exists(conn, table_name, column_name):
    cur = conn.cursor()
    try:
        cur.execute(
            f"SHOW COLUMNS FROM `{table_name}` LIKE %s", (column_name,))
        row = cur.fetchone()
        try:
            cur.fetchall()
        except Exception:
            pass
        return bool(row)
    finally:
        try:
            cur.close()
        except Exception:
            pass


def index_exists(conn, table_name, index_name):
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(f"SHOW INDEX FROM `{table_name}`")
        rows = cur.fetchall() or []
        for r in rows:
            key = r.get("Key_name") or r.get("key_name")
            if key == index_name:
                return True
        return False
    finally:
        try:
            cur.close()
        except Exception:
            pass


def create_migrations_table(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{MIGRATIONS_TABLE}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL UNIQUE,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
        print(f"Ensured table exists: {MIGRATIONS_TABLE}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def create_table(conn, table_name, columns, extra_sql=""):
    cur = conn.cursor()
    try:
        cols = ",\n    ".join([f"{col} {ctype}" for col, ctype in columns])
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                {cols}
                {extra_sql}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
        print(f"Ensured table exists: {table_name}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def add_missing_columns(conn, table_name, columns):
    cur = conn.cursor()
    try:
        for col, ctype in columns:
            if col.strip().upper() in ("PRIMARY KEY",):
                continue
            if not column_exists(conn, table_name, col):
                cur.execute(
                    f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` {ctype}")
                print(f"Added column '{col}' to {table_name}")
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass


def ensure_indexes(conn, table_name, indexes):
    cur = conn.cursor()
    try:
        for index_name, _kind, cols_sql in indexes:
            if index_exists(conn, table_name, index_name):
                continue
            cur.execute(
                f"CREATE INDEX `{index_name}` ON `{table_name}` {cols_sql}")
            print(f"Added INDEX '{index_name}' on {table_name} {cols_sql}")
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _ensure_table(conn, name, cols, idxs):
    if not table_exists(conn, name):
        create_table(conn, name, cols)
    add_missing_columns(conn, name, cols)
    ensure_indexes(conn, name, idxs)

# =============================================================================
# Install / Upgrade / Uninstall
# =============================================================================


def install(seed_demo: bool = False):
    conn = get_db_connection()
    try:
        create_migrations_table(conn)
        _ensure_table(conn, TBL_PV_DAILY, PV_DAILY_COLUMNS, PV_DAILY_INDEXES)
        _ensure_table(conn, TBL_PV_HOURLY,
                      PV_HOURLY_COLUMNS, PV_HOURLY_INDEXES)
        _ensure_table(conn, TBL_PV_COUNTRY_DAILY,
                      PV_COUNTRY_DAILY_COLUMNS, PV_COUNTRY_DAILY_INDEXES)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def upgrade():
    # same as install (idempotent)
    install(seed_demo=False)
    print("Website module upgrade complete. Analytics tables are ready.")


def uninstall(drop_data: bool = False):
    if not drop_data:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for t in (TBL_PV_COUNTRY_DAILY, TBL_PV_HOURLY, TBL_PV_DAILY, MIGRATIONS_TABLE):
            cur.execute(f"DROP TABLE IF EXISTS `{t}`")
        conn.commit()
        print("Website module tables dropped.")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Website Module Installer")
    parser.add_argument("action", choices=[
                        "install", "uninstall", "upgrade"], help="Action to perform")
    parser.add_argument("--seed-demo", action="store_true",
                        help="Seed demo data on install (unused)")
    parser.add_argument("--drop-data", action="store_true",
                        help="Drop tables on uninstall")
    args = parser.parse_args()

    if args.action == "install":
        install(seed_demo=args.seed_demo)
    elif args.action == "upgrade":
        upgrade()
    elif args.action == "uninstall":
        uninstall(drop_data=args.drop_data)

    print("Action completed.")
