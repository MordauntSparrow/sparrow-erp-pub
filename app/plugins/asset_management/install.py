"""Schema for serial equipment extensions is created by inventory_control.install."""

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


def install():
    from app.plugins.inventory_control.install import _ensure_asset_extension_schema

    conn = get_db_connection()
    try:
        _ensure_asset_extension_schema(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def upgrade():
    install()


if __name__ == "__main__":
    install()
