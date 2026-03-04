from app.objects import get_db_connection, AuthManager
import os
import sys
import mysql.connector
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
    db_host = os.environ.get("DB_HOST", "localhost")
    db_user = os.environ.get("DB_USER", "root")
    db_password = os.environ.get("DB_PASSWORD", "rootpassword")
    db_name = os.environ.get("DB_NAME", "sparrow_erp")


    conn = mysql.connector.connect(
        host=db_host, user=db_user, password=db_password)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print(f"Database '{db_name}' ensured.")
    cursor.close()
    conn.close()

    conn = mysql.connector.connect(
        host=db_host, user=db_user, password=db_password, database=db_name)
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
            PRIMARY KEY (id),
            UNIQUE KEY username (username),
            UNIQUE KEY email (email)
        )
    """)
    print("Users table ensured.")
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


if __name__ == "__main__":
    create_database_and_tables()
    create_default_admin()
