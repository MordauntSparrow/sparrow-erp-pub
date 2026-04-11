import os
import sys
from pathlib import Path
import time

HERE = Path(__file__).resolve()
PLUGIN_DIR = HERE.parent
PLUGINS_DIR = PLUGIN_DIR.parent
APP_ROOT = PLUGINS_DIR.parent
PROJECT_ROOT = APP_ROOT.parent

for p in (str(PROJECT_ROOT), str(APP_ROOT), str(PLUGIN_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)
from app.objects import get_db_connection  # noqa: E402

MIGRATIONS_TABLE = "event_manager_migrations"

# Table and column definitions for upgrade checks
EVENTS_COLUMNS = [
    ("id", "INT AUTO_INCREMENT PRIMARY KEY"),
    ("event_name", "VARCHAR(255) NOT NULL"),
    ("category", "VARCHAR(64)"),
    ("entry_cost", "DECIMAL(10,2)"),
    ("start_date", "DATE NOT NULL"),
    ("end_date", "DATE"),
    ("start_time", "TIME"),
    ("end_time", "TIME"),
    ("food_menu_path", "VARCHAR(255)"),
    ("flyer_path", "VARCHAR(255)"),
    ("music_type", "ENUM('None','DJ','Live Band','Other') DEFAULT 'None'"),
    ("is_public", "TINYINT(1) DEFAULT 1"),
    # Public/Private details for band, DJ, other music, and event
    ("band_details_public", "TEXT"),
    ("band_details_private", "TEXT"),
    ("dj_details_public", "TEXT"),
    ("dj_details_private", "TEXT"),
    ("other_music_details_public", "TEXT"),
    ("other_music_details_private", "TEXT"),
    ("event_details_public", "TEXT"),
    ("event_details_private", "TEXT"),
    ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
]


def table_exists(conn, table_name):
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE %s", (table_name,))
        return bool(cur.fetchone())
    finally:
        cur.close()


def column_exists(conn, table_name, column_name):
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM {} LIKE %s".format(
            table_name), (column_name,))
        return bool(cur.fetchone())
    finally:
        cur.close()


def create_events_table(conn):
    cur = conn.cursor()
    try:
        cols = ",\n    ".join(
            [f"{col} {ctype}" for col, ctype in EVENTS_COLUMNS])
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS events (
                {cols}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
        print("Created table: events")
    finally:
        cur.close()


def add_missing_columns(conn, table_name, columns):
    cur = conn.cursor()
    try:
        for col, ctype in columns:
            if not column_exists(conn, table_name, col):
                cur.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN {col} {ctype}")
                print(f"Added column '{col}' to {table_name}")
        conn.commit()
    finally:
        cur.close()


def create_migrations_table(conn):
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
        print(f"Created table: {MIGRATIONS_TABLE}")
    finally:
        cur.close()


def install(seed_demo: bool = False):
    conn = get_db_connection()
    try:
        create_migrations_table(conn)
        create_events_table(conn)
        if seed_demo:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO events (
                    event_name, category, entry_cost, start_date, end_date, start_time, end_time,
                    food_menu_path, flyer_path, music_type, is_public,
                    band_details_public, band_details_private,
                    dj_details_public, dj_details_private,
                    other_music_details_public, other_music_details_private,
                    event_details_public, event_details_private, created_at
                ) VALUES (
                    'Demo Wedding', 'Wedding', 500.00, CURDATE(), CURDATE(), '17:00', '23:00',
                    NULL, NULL, 'Live Band', 1,
                    'The Example Band performing live.', 'Internal band notes.',
                    NULL, NULL,
                    NULL, NULL,
                    'A sample event for demo purposes.', 'Internal event notes.', NOW()
                )
            """)
            conn.commit()
            print("Demo event seeded.")
    finally:
        conn.close()


def uninstall(drop_data: bool = False):
    if not drop_data:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        for t in ["events", MIGRATIONS_TABLE]:
            print(f"Dropping table: {t}")
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()
        print("All tables dropped.")
    finally:
        cur.close()
        conn.close()


def upgrade():
    """
    Ensures all tables and columns exist, adding any missing columns (non-destructive).
    Safe to run repeatedly (nightly).
    """
    conn = get_db_connection()
    try:
        # Ensure migrations table exists
        create_migrations_table(conn)
        # Ensure events table and columns exist
        if not table_exists(conn, "events"):
            create_events_table(conn)
        else:
            add_missing_columns(conn, "events", EVENTS_COLUMNS)
        print("Upgrade complete. All tables/columns checked and up to date.")
    finally:
        conn.close()


# =============================================================================
# CLI Entrypoint
# =============================================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Event Manager Module Installer")
    parser.add_argument("action", choices=[
                        "install", "uninstall", "upgrade"], help="Action to perform")
    parser.add_argument("--seed-demo", action="store_true",
                        help="Seed demo data on install")
    parser.add_argument("--drop-data", action="store_true",
                        help="Drop all tables on uninstall")
    args = parser.parse_args()

    if args.action == "install":
        install(seed_demo=args.seed_demo)
    elif args.action == "uninstall":
        uninstall(drop_data=args.drop_data)
    elif args.action == "upgrade":
        upgrade()
    print("Action completed.")
