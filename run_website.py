# run_website.py
import os
import sys

_app_pkg = os.path.abspath(os.path.join(os.path.dirname(__file__), "app"))
try:
    from app.storage_paths import bind_persistent_directories

    bind_persistent_directories(_app_pkg)
except Exception as e:
    print(f"[WARN] Persistent storage bind failed: {e}", file=sys.stderr)

from app.plugins.website_module import WebsiteServer

if __name__ == '__main__':
    server = WebsiteServer(port=8080)
    print("Starting website server in debug mode (blocking)...")
    server.start(debug=True)
