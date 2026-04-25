# run_website.py
from app.plugins.website_module import WebsiteServer
import os
import sys

_app_pkg = os.path.abspath(os.path.join(os.path.dirname(__file__), "app"))
try:
    from app.storage_paths import bind_persistent_directories

    bind_persistent_directories(_app_pkg)
except Exception as e:
    print(f"[WARN] Persistent storage bind failed: {e}", file=sys.stderr)


if __name__ == '__main__':
    # Flask listen port (default 80). MUST differ from Railway PORT / nginx listen — docker-entrypoint
    # rewrites nginx to listen on $PORT (e.g. 8080). If WEBSITE_PORT equals PORT, bind fails: "address already in use".
    _port = int((os.environ.get("WEBSITE_PORT") or "80").strip() or "80")
    server = WebsiteServer(port=_port)
    print("Starting website server in debug mode (blocking)...")
    server.start(debug=True)
