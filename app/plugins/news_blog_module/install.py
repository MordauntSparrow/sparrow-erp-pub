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

MIGRATIONS_TABLE = "news_blog_migrations"

# =============================================================================
# Schema definitions (MySQL / MariaDB)
# =============================================================================

ARTICLES_COLUMNS = [
    ("id", "INT AUTO_INCREMENT PRIMARY KEY"),
    ("title", "VARCHAR(255) NOT NULL"),
    ("slug", "VARCHAR(255) NOT NULL"),
    ("summary", "TEXT"),
    ("content", "LONGTEXT NOT NULL"),
    ("status", "VARCHAR(20) NOT NULL DEFAULT 'draft'"),
    # Media
    ("cover_image_path", "VARCHAR(255) NULL"),
    ("cover_image_alt", "VARCHAR(160) NULL"),
    # Analytics
    ("view_count", "INT NOT NULL DEFAULT 0"),
    # Publishing
    ("published_at", "DATETIME NULL"),
    # Timestamps (DB-managed)
    ("created_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("updated_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
]

CATEGORIES_COLUMNS = [
    ("id", "INT AUTO_INCREMENT PRIMARY KEY"),
    ("name", "VARCHAR(120) NOT NULL"),
    ("slug", "VARCHAR(160) NOT NULL"),
    ("description", "TEXT NULL"),
    ("sort_order", "INT NOT NULL DEFAULT 0"),
    ("created_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("updated_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
]

TAGS_COLUMNS = [
    ("id", "INT AUTO_INCREMENT PRIMARY KEY"),
    ("name", "VARCHAR(120) NOT NULL"),
    ("slug", "VARCHAR(160) NOT NULL"),
    ("created_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("updated_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
]

ARTICLE_CATEGORIES_COLUMNS = [
    ("article_id", "INT NOT NULL"),
    ("category_id", "INT NOT NULL"),
    ("is_primary", "TINYINT(1) NOT NULL DEFAULT 0"),
    ("created_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("PRIMARY KEY", "(article_id, category_id)"),
]

ARTICLE_TAGS_COLUMNS = [
    ("article_id", "INT NOT NULL"),
    ("tag_id", "INT NOT NULL"),
    ("created_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("PRIMARY KEY", "(article_id, tag_id)"),
]

# Indexes tuned for:
# - public listing (status + published_at)
# - canonical lookup by slug
# - category/tag filters
# - related articles (category_id -> articles)
# - taxonomy fetch (article_id -> categories/tags)
ARTICLES_INDEXES = [
    ("uq_articles_slug", "UNIQUE", "(slug)"),
    ("idx_articles_status_published_at", "INDEX", "(status, published_at)"),
    ("idx_articles_published_at", "INDEX", "(published_at)"),
]

CATEGORIES_INDEXES = [
    ("uq_news_categories_slug", "UNIQUE", "(slug)"),
    ("idx_news_categories_sort", "INDEX", "(sort_order, name)"),
]

TAGS_INDEXES = [
    ("uq_news_tags_slug", "UNIQUE", "(slug)"),
    ("idx_news_tags_name", "INDEX", "(name)"),
]

# Join table indexes:
# - category filter: category_id
# - related articles: category_id + article_id
# - taxonomy fetch: article_id + is_primary
ARTICLE_CATEGORIES_INDEXES = [
    ("idx_article_categories_category", "INDEX", "(category_id)"),
    ("idx_article_categories_category_article",
     "INDEX", "(category_id, article_id)"),
    ("idx_article_categories_primary", "INDEX", "(article_id, is_primary)"),
]

# - tag filter: tag_id
# - taxonomy fetch: article_id
ARTICLE_TAGS_INDEXES = [
    ("idx_article_tags_tag", "INDEX", "(tag_id)"),
    ("idx_article_tags_article", "INDEX", "(article_id)"),
    ("idx_article_tags_tag_article", "INDEX", "(tag_id, article_id)"),
]

# =============================================================================
# Helpers
# =============================================================================


def table_exists(conn, table_name):
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE %s", (table_name,))
        row = cur.fetchone()
        # Consume any remaining results (safety)
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
        # Consume any remaining results (safety)
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
    """
    mysql-connector can throw 'Unread result found' if results aren't fully consumed.
    Use SHOW INDEX FROM table and filter in Python, always fetching all rows.
    """
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(f"SHOW INDEX FROM `{table_name}`")
        rows = cur.fetchall() or []  # IMPORTANT: consume all rows
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
        print(f"Created table: {table_name}")
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
        for index_name, kind, cols_sql in indexes:
            if index_exists(conn, table_name, index_name):
                continue

            if kind.upper() == "UNIQUE":
                cur.execute(
                    f"ALTER TABLE `{table_name}` ADD CONSTRAINT `{index_name}` UNIQUE {cols_sql}"
                )
                print(
                    f"Added UNIQUE index '{index_name}' on {table_name} {cols_sql}")
            else:
                cur.execute(
                    f"CREATE INDEX `{index_name}` ON `{table_name}` {cols_sql}")
                print(f"Added INDEX '{index_name}' on {table_name} {cols_sql}")

        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _consume_all(cur):
    try:
        cur.fetchall()
    except Exception:
        pass


def ensure_foreign_keys(conn):
    """
    Adds foreign keys if missing.
    We check by constraint name to keep it idempotent.
    """
    cur = conn.cursor()
    try:
        # article_categories -> articles/categories
        cur.execute(
            """
            SELECT CONSTRAINT_NAME
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'article_categories'
              AND CONSTRAINT_TYPE = 'FOREIGN KEY'
              AND CONSTRAINT_NAME = 'fk_article_categories_article'
            """
        )
        exists = cur.fetchone()
        _consume_all(cur)
        if not exists:
            cur.execute(
                """
                ALTER TABLE article_categories
                ADD CONSTRAINT fk_article_categories_article
                FOREIGN KEY (article_id) REFERENCES articles(id)
                ON DELETE CASCADE
                """
            )
            print("Added FK fk_article_categories_article")

        cur.execute(
            """
            SELECT CONSTRAINT_NAME
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'article_categories'
              AND CONSTRAINT_TYPE = 'FOREIGN KEY'
              AND CONSTRAINT_NAME = 'fk_article_categories_category'
            """
        )
        exists = cur.fetchone()
        _consume_all(cur)
        if not exists:
            cur.execute(
                """
                ALTER TABLE article_categories
                ADD CONSTRAINT fk_article_categories_category
                FOREIGN KEY (category_id) REFERENCES categories(id)
                ON DELETE CASCADE
                """
            )
            print("Added FK fk_article_categories_category")

        # article_tags -> articles/tags
        cur.execute(
            """
            SELECT CONSTRAINT_NAME
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'article_tags'
              AND CONSTRAINT_TYPE = 'FOREIGN KEY'
              AND CONSTRAINT_NAME = 'fk_article_tags_article'
            """
        )
        exists = cur.fetchone()
        _consume_all(cur)
        if not exists:
            cur.execute(
                """
                ALTER TABLE article_tags
                ADD CONSTRAINT fk_article_tags_article
                FOREIGN KEY (article_id) REFERENCES articles(id)
                ON DELETE CASCADE
                """
            )
            print("Added FK fk_article_tags_article")

        cur.execute(
            """
            SELECT CONSTRAINT_NAME
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'article_tags'
              AND CONSTRAINT_TYPE = 'FOREIGN KEY'
              AND CONSTRAINT_NAME = 'fk_article_tags_tag'
            """
        )
        exists = cur.fetchone()
        _consume_all(cur)
        if not exists:
            cur.execute(
                """
                ALTER TABLE article_tags
                ADD CONSTRAINT fk_article_tags_tag
                FOREIGN KEY (tag_id) REFERENCES tags(id)
                ON DELETE CASCADE
                """
            )
            print("Added FK fk_article_tags_tag")

        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass


# =============================================================================
# Install / Uninstall / Upgrade
# =============================================================================


def install(seed_demo: bool = False):
    conn = get_db_connection()
    try:
        create_migrations_table(conn)

        # Core tables
        create_table(conn, "articles", ARTICLES_COLUMNS)
        create_table(conn, "categories", CATEGORIES_COLUMNS)
        create_table(conn, "tags", TAGS_COLUMNS)

        # Join tables
        create_table(conn, "article_categories", ARTICLE_CATEGORIES_COLUMNS)
        create_table(conn, "article_tags", ARTICLE_TAGS_COLUMNS)

        # Non-destructive upgrades (columns + indexes)
        add_missing_columns(conn, "articles", ARTICLES_COLUMNS)
        add_missing_columns(conn, "categories", CATEGORIES_COLUMNS)
        add_missing_columns(conn, "tags", TAGS_COLUMNS)
        # V1 hardening: include join tables too
        add_missing_columns(conn, "article_categories",
                            ARTICLE_CATEGORIES_COLUMNS)
        add_missing_columns(conn, "article_tags", ARTICLE_TAGS_COLUMNS)

        ensure_indexes(conn, "articles", ARTICLES_INDEXES)
        ensure_indexes(conn, "categories", CATEGORIES_INDEXES)
        ensure_indexes(conn, "tags", TAGS_INDEXES)
        ensure_indexes(conn, "article_categories", ARTICLE_CATEGORIES_INDEXES)
        ensure_indexes(conn, "article_tags", ARTICLE_TAGS_INDEXES)

        ensure_foreign_keys(conn)

        if seed_demo:
            cur = conn.cursor()
            try:
                # Seed categories
                cur.execute(
                    """
                    INSERT IGNORE INTO categories (name, slug, description, sort_order)
                    VALUES
                      ('News', 'news', 'General news', 0),
                      ('Science & Tech', 'science-tech', 'Science and technology', 10)
                    """
                )
                conn.commit()

                # Fetch a category id for demo
                cur.execute(
                    "SELECT id FROM categories WHERE slug = %s LIMIT 1", ("news",))
                row = cur.fetchone()
                _consume_all(cur)
                category_id = row[0] if row else None

                # Seed article
                cur.execute(
                    """
                    INSERT INTO articles (title, slug, summary, content, status, published_at)
                    VALUES (%s, %s, %s, %s, 'published', NOW())
                    """,
                    (
                        "Welcome to News",
                        "welcome-to-news",
                        "A demo article to confirm the module is working.",
                        "<p>This is a demo article. Edit or delete it from the admin panel.</p>",
                    ),
                )
                article_id = cur.lastrowid
                conn.commit()

                # Link article -> category (primary)
                if category_id and article_id:
                    cur.execute(
                        """
                        INSERT IGNORE INTO article_categories (article_id, category_id, is_primary)
                        VALUES (%s, %s, 1)
                        """,
                        (article_id, category_id),
                    )
                    conn.commit()

                print("Demo data seeded.")
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def uninstall(drop_data: bool = False):
    if not drop_data:
        return

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        for t in ["article_tags", "article_categories", "tags", "categories", "articles", MIGRATIONS_TABLE]:
            print(f"Dropping table: {t}")
            cur.execute(f"DROP TABLE IF EXISTS `{t}`")
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()
        print("All tables dropped.")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def upgrade():
    """
    Ensures all tables/columns/indexes/FKs exist (non-destructive).
    Safe to run repeatedly.
    """
    conn = get_db_connection()
    try:
        create_migrations_table(conn)

        # Ensure tables exist
        if not table_exists(conn, "articles"):
            create_table(conn, "articles", ARTICLES_COLUMNS)
        if not table_exists(conn, "categories"):
            create_table(conn, "categories", CATEGORIES_COLUMNS)
        if not table_exists(conn, "tags"):
            create_table(conn, "tags", TAGS_COLUMNS)
        if not table_exists(conn, "article_categories"):
            create_table(conn, "article_categories",
                         ARTICLE_CATEGORIES_COLUMNS)
        if not table_exists(conn, "article_tags"):
            create_table(conn, "article_tags", ARTICLE_TAGS_COLUMNS)

        # Ensure columns exist
        add_missing_columns(conn, "articles", ARTICLES_COLUMNS)
        add_missing_columns(conn, "categories", CATEGORIES_COLUMNS)
        add_missing_columns(conn, "tags", TAGS_COLUMNS)
        # V1 hardening: include join tables too
        add_missing_columns(conn, "article_categories",
                            ARTICLE_CATEGORIES_COLUMNS)
        add_missing_columns(conn, "article_tags", ARTICLE_TAGS_COLUMNS)

        # Ensure indexes exist
        ensure_indexes(conn, "articles", ARTICLES_INDEXES)
        ensure_indexes(conn, "categories", CATEGORIES_INDEXES)
        ensure_indexes(conn, "tags", TAGS_INDEXES)
        ensure_indexes(conn, "article_categories", ARTICLE_CATEGORIES_INDEXES)
        ensure_indexes(conn, "article_tags", ARTICLE_TAGS_INDEXES)

        # Ensure foreign keys exist
        ensure_foreign_keys(conn)

        print("Upgrade complete. All tables/columns/indexes/FKs checked and up to date.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# =============================================================================
# CLI Entrypoint
# =============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="News/Blog Module Installer")
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
