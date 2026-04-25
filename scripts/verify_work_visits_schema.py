#!/usr/bin/env python3
"""Exit 0 if work_visits + work_photos.visit_id exist; non-zero otherwise."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for p in (str(ROOT), str(ROOT / "app")):
    if p not in sys.path:
        sys.path.insert(0, p)

from app.objects import get_db_connection  # noqa: E402


def main() -> int:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'work_visits'")
        if not cur.fetchone():
            print("FAIL: work_visits table missing — run: python -m app.plugins.work_module.install upgrade")
            return 1
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'work_photos' AND COLUMN_NAME = 'visit_id'
            """
        )
        if not cur.fetchone()[0]:
            print("FAIL: work_photos.visit_id missing — run: python -m app.plugins.work_module.install upgrade")
            return 1
        print("OK: work_visits + work_photos.visit_id present")
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
