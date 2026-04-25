import os
import threading
import traceback

from app import create_app


def _plugins_dir():
    base = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(base, "app", "plugins")


def _start_website():
    """Start the WebsiteServer in a background thread if the website module is enabled."""
    try:
        from app.objects import PluginManager
    except Exception as e:
        print(f"[WARN] PluginManager unavailable: {e}")
        return

    try:
        pm = PluginManager(plugins_dir=_plugins_dir())
        plugins = {p.get("system_name"): p for p in (
            pm.get_all_plugins() or [])}
        web = plugins.get("website_module")

        if not web or not web.get("installed") or not web.get("enabled"):
            print(
                "[INFO] Website module not installed or not enabled. Skipping WebsiteServer startup.")
            return

        try:
            from app.plugins.website_module import WebsiteServer
        except ImportError as ie:
            print(
                f"[WARN] Website module import failed: {ie}. Skipping WebsiteServer startup.")
            return

        web_port = int(os.environ.get("WEB_PORT", "80"))
        ws = WebsiteServer(port=web_port)

        def _run_website():
            try:
                ws.start()
                print(f"[INFO] Website server started on port {web_port}. Demo site and employee login: http://<host>:{web_port}/")
            except Exception:
                print("[ERROR] WebsiteServer failed to start:")
                traceback.print_exc()

        t = threading.Thread(target=_run_website, name="WebsiteServerThread", daemon=True)
        t.start()
        print(f"[INFO] Website server thread started (listening on port {web_port}).")

    except Exception:
        print("[ERROR] Failed to start WebsiteServer:")
        traceback.print_exc()


# Create Flask app for Gunicorn
app = create_app()

# Start WebsiteServer in background
_start_website()

# Allow running directly (useful for local dev); Gunicorn will import `app` from this file
if __name__ == "__main__":
    port = int(os.getenv("PORT", "82"))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
