import os
import sys
import mysql.connector
import uuid
from dotenv import load_dotenv

# Determine the current directory and project root
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))

# Add the project root to sys.path so that "app.objects" can be imported
sys.path.insert(0, project_root)

# Load environment variables from app/config/.env
dotenv_path = os.path.join(project_root, 'app', 'config', '.env')
load_dotenv(dotenv_path=dotenv_path)

# Now import the necessary functions and classes from app.objects
from app.objects import get_db_connection, AuthManager

def create_database_and_tables():
    """
    Create the database and the required tables if they don't exist.
    """
    # Retrieve database connection settings from environment variables
    db_host = os.environ.get("DB_HOST", "localhost")
    db_user = os.environ.get("DB_USER", "root")
    db_password = os.environ.get("DB_PASSWORD", "rootpassword")
    db_name = os.environ.get("DB_NAME", "sparrow_erp")

    # Connect to MySQL without specifying a database
    conn = mysql.connector.connect(host=db_host, user=db_user, password=db_password)
    cursor = conn.cursor()
    
    # Create the database if it doesn't exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print(f"Database '{db_name}' ensured.")
    cursor.close()
    conn.close()

    # Connect to the newly created (or existing) database
    conn = mysql.connector.connect(host=db_host, user=db_user, password=db_password, database=db_name)
    cursor = conn.cursor()

    # Create the 'users' table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id CHAR(36) PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            role ENUM('admin', 'user', 'client') NOT NULL DEFAULT 'user',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP NULL
        )
    """)
    print("Users table ensured.")

    conn.commit()
    cursor.close()
    conn.close()

def create_default_admin():
    """
    Insert a default admin user if no admin exists.
    """
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    
    # Check if an admin user already exists
    cursor.execute("SELECT * FROM users WHERE role = 'admin'")
    admin = cursor.fetchone()
    
    if admin:
        print("Admin user already exists.")
    else:
        default_username = "admin"
        default_email = "admin@example.com"
        default_password = "ChangeMe123!"  # Change this after first login
        password_hash = AuthManager.hash_password(default_password)
        admin_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO users (id, username, email, password_hash, role)
            VALUES (%s, %s, %s, %s, %s)
        """, (admin_id, default_username, default_email, password_hash, "admin"))
        db.commit()
        print("Default admin user created. Username: admin, Password: ChangeMe123!")
    
    cursor.close()
    db.close()

if __name__ == "__main__":
    create_database_and_tables()
    create_default_admin()
