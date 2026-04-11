from app.objects import AuthManager, get_db_connection, mysql_connect_with_retry
import os
import uuid

# Only load dotenv for local/dev
if not os.environ.get("RAILWAY_ENVIRONMENT"):
    from dotenv import load_dotenv
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    dotenv_path = os.path.join(project_root, 'app', 'config', '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)


def create_database_and_tables():
    db_name = os.environ.get("DB_NAME", "sparrow_erp")

    conn = mysql_connect_with_retry(include_database=False)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print(f"Database '{db_name}' ensured.")
    cursor.close()
    conn.close()

    conn = mysql_connect_with_retry(database=db_name)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id char(36) NOT NULL,
            username varchar(50) NOT NULL,
            email varchar(100) NOT NULL,
            password_hash varchar(255) NOT NULL,
            role varchar(45) NOT NULL,
            created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            last_login timestamp NULL DEFAULT NULL,
            permissions json DEFAULT NULL,
            first_name varchar(45) DEFAULT NULL,
            last_name varchar(45) DEFAULT NULL,
            personal_pin_hash varchar(255) DEFAULT NULL,
            billable_exempt tinyint(1) NOT NULL DEFAULT 0,
            support_access_expires_at datetime DEFAULT NULL,
            support_access_enabled tinyint(1) NOT NULL DEFAULT 0,
            contractor_id INT NULL DEFAULT NULL,
            PRIMARY KEY (id),
            UNIQUE KEY username (username),
            UNIQUE KEY email (email),
            UNIQUE KEY uq_users_contractor_id (contractor_id)
        )
    """)
    print("Users table ensured.")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sparrow_seat_limit (
          id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
          max_billable_seats INT NOT NULL DEFAULT 30,
          updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    cursor.execute(
        """
        INSERT IGNORE INTO sparrow_seat_limit (id, max_billable_seats) VALUES (1, 30)
        """
    )
    print("sparrow_seat_limit table ensured.")
    conn.commit()
    cursor.close()
    conn.close()


def create_default_admin():
    db = get_db_connection()
    # Buffered cursor prevents "Unread result found" when closing cursor/connection.
    cursor = db.cursor(dictionary=True, buffered=True)
    cursor.execute("SELECT id FROM users WHERE role = 'admin' LIMIT 1")
    admin = cursor.fetchone()

    if admin:
        print("Admin user already exists.")
    else:
        default_username = "admin"
        default_email = "admin@example.com"
        default_password = "ChangeMe123!"
        password_hash = AuthManager.hash_password(default_password)
        admin_id = str(uuid.uuid4())
        cursor.execute(
            "INSERT INTO users (id, username, email, password_hash, role) VALUES (%s, %s, %s, %s, %s)",
            (admin_id, default_username, default_email, password_hash, "admin")
        )
        db.commit()
        print("Default admin user created. Username: admin, Password: ChangeMe123!")

    cursor.close()
    db.close()


def run_predeploy_install_upgrades():
    """
    Run core + plugin ``install.py upgrade`` (same as Settings → version / run upgrades).
    Railway ``preDeployCommand`` uses this module; failures exit non-zero so deploy does not proceed.

    Set SPARROW_SKIP_PREDEPLOY_UPGRADES=1 to skip (e.g. debugging).
    """
    flag = (os.environ.get("SPARROW_SKIP_PREDEPLOY_UPGRADES") or "").strip().lower()
    if flag in ("1", "true", "yes", "on"):
        print("[init_db] Skipping install upgrades (SPARROW_SKIP_PREDEPLOY_UPGRADES is set).")
        return
    from app.objects import run_install_upgrade_scripts

    app_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    report = run_install_upgrade_scripts(app_root)
    core = report.get("core") or {}
    if core.get("ran") and not core.get("ok"):
        print("[init_db] Core upgrade failed:", core.get("message"))
        raise SystemExit(1)
    if report.get("plugins_failed"):
        print("[init_db] Plugin upgrade failures:", report["plugins_failed"])
        raise SystemExit(1)
    print("[init_db] Install upgrades completed successfully.")


if __name__ == "__main__":
    create_database_and_tables()
    run_predeploy_install_upgrades()
    create_default_admin()
