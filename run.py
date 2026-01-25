from app import create_app
import threading
import time
import os
import traceback

def _plugins_dir():
    """Resolve absolute path to app/plugins."""
    base = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(base, "app", "plugins")

def run_admin_app():
    """Start the admin Flask app."""
    admin_app = create_app()
    admin_app.run(host="0.0.0.0", port=82, debug=True, use_reloader=False)

def try_start_website():
    """Attempt to start the website module if installed and enabled."""
    try:
        # Import PluginManager from the actual module location
        from app.objects import PluginManager  # adjust if necessary
    except Exception as e:
        print(f"[WARN] PluginManager unavailable: {e}")
        return

    try:
        pm = PluginManager(plugins_dir=_plugins_dir())
        plugins = {p.get("system_name"): p for p in (pm.get_all_plugins() or [])}
        web = plugins.get("website_module")

        if not web or not web.get("installed") or not web.get("enabled"):
            print("[INFO] Website module not installed or not enabled. Skipping WebsiteServer startup.")
            return

        try:
            from app.plugins.website_module import WebsiteServer
        except ImportError as ie:
            print(f"[WARN] Website module import failed: {ie}. Skipping WebsiteServer startup.")
            return

        # Run WebsiteServer in a separate daemon thread
        def _run_website():
            try:
                ws = WebsiteServer()
                ws.start()
                print("[INFO] WebsiteServer started.")
            except Exception:
                print("[ERROR] WebsiteServer failed to start:")
                traceback.print_exc()

        t = threading.Thread(target=_run_website, name="WebsiteServerThread", daemon=True)
        t.start()

    except Exception:
        print("[ERROR] Failed to start WebsiteServer:")
        traceback.print_exc()

if __name__ == "__main__":
    # Start the admin app in a separate daemon thread
    admin_thread = threading.Thread(target=run_admin_app, name="AdminFlaskThread", daemon=True)
    admin_thread.start()

    # Optionally start website module (safe if uninstalled/disabled)
    try_start_website()

    # Keep the main thread alive without busy-waiting
    try:
        while admin_thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down.")
