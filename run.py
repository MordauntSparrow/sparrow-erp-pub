import os
import sys

# Bind Railway / volume paths before any code imports plugin routes that touch uploads.
_app_pkg = os.path.abspath(os.path.join(os.path.dirname(__file__), "app"))
try:
    from app.storage_paths import bind_persistent_directories

    bind_persistent_directories(_app_pkg)
except Exception as e:
    print(f"[WARN] Persistent storage bind failed: {e}", file=sys.stderr)

from app import create_app
import threading
import time
import traceback


# ----------------------------
# Path helpers
# ----------------------------

def _app_root():
    """Resolve absolute path to project root (folder containing this run.py)."""
    return os.path.abspath(os.path.dirname(__file__))


def _plugins_dir():
    """Resolve absolute path to app/plugins."""
    return os.path.join(_app_root(), "app", "plugins")


def _restart_flag_path():
    """Restart trigger file path (touched by PluginManager lifecycle actions)."""
    return os.path.join(_app_root(), "app", "config", "restart.flag")


# ----------------------------
# Restart helper (Windows-safe)
# ----------------------------

def request_restart(reason: str = ""):
    """
    Hard restart the current Python process.

    Why this exists:
    - Flask does NOT reliably support registering new routes/blueprints after the app starts.
    - Enabling/installing/uninstalling/updating plugins changes what routes should exist.
    - So we restart to re-register routes cleanly.

    Windows-safe behavior:
    - Spawn a new process with the same interpreter + argv
    - Then hard-exit the current process
    This is more reliable than os.execv on Windows when threads are running.
    """
    msg = "[RESTART] Restart requested"
    if reason:
        msg += f": {reason}"
    print(msg)

    try:
        import subprocess

        python = sys.executable
        args = [python] + sys.argv

        # Diagnostics (helps when restart fails on client)
        print(f"[RESTART] Spawning: {args}")
        print(f"[RESTART] CWD: {os.getcwd()}")

        subprocess.Popen(args, cwd=os.getcwd(), close_fds=True)
    except Exception:
        print("[RESTART] Failed to spawn restart process:")
        traceback.print_exc()
    finally:
        # Ensure the old process dies even if threads are running
        os._exit(0)


# ----------------------------
# Restart flag watcher (bulletproof)
# ----------------------------

def ensure_restart_flag_exists():
    """
    Bulletproof creation of app/config/restart.flag.
    Safe to call repeatedly.
    """
    flag = _restart_flag_path()
    try:
        os.makedirs(os.path.dirname(flag), exist_ok=True)
        if not os.path.exists(flag):
            with open(flag, "w", encoding="utf-8") as f:
                f.write("restart flag\n")
        return True
    except Exception:
        print("[WATCH] Failed to ensure restart.flag exists:")
        traceback.print_exc()
        return False


def get_restart_flag_mtime():
    """
    Return restart.flag mtime, creating the file if missing.
    Returns float timestamp (or 0 on failure).
    """
    flag = _restart_flag_path()

    # Ensure it exists first
    if not os.path.exists(flag):
        if not ensure_restart_flag_exists():
            return 0

    try:
        return os.path.getmtime(flag)
    except Exception:
        print("[WATCH] Failed to stat restart.flag:")
        traceback.print_exc()
        return 0


def watch_for_restart_flag(poll_seconds: float = 0.5):
    """
    Watches app/config/restart.flag for mtime changes.
    Only lifecycle actions should touch this file (install/enable/disable/uninstall/update),
    so minor settings changes in manifest.json won't cause restarts.
    """
    if not ensure_restart_flag_exists():
        print("[WATCH] Restart-flag watcher disabled (could not create restart.flag).")
        return

    flag = _restart_flag_path()
    last = get_restart_flag_mtime()

    print(f"[WATCH] Restart-flag watcher enabled. Watching: {flag}")

    while True:
        time.sleep(poll_seconds)

        # If the file was deleted, recreate it and continue
        if not os.path.exists(flag):
            ensure_restart_flag_exists()

        now = get_restart_flag_mtime()
        if now != last and now != 0:
            request_restart(reason="restart.flag touched")
            return


# ----------------------------
# Admin server thread
# ----------------------------

def run_admin_app():
    """Start the admin Flask app."""
    admin_app = create_app()
    # If socketio is available, use it to run the app (supports WebSockets).
    try:
        from app import socketio
        socketio.run(admin_app, host="0.0.0.0", port=82, debug=True)
    except Exception:
        # Fall back to Flask built-in server if SocketIO isn't available
        admin_app.run(host="0.0.0.0", port=82, debug=True, use_reloader=False)


# ----------------------------
# Website server thread
# ----------------------------

def try_start_website():
    """Attempt to start the website module if installed and enabled."""
    try:
        from app.objects import PluginManager
    except Exception as e:
        print(f"[WARN] PluginManager unavailable: {e}")
        return

    try:
        pm = PluginManager(plugins_dir=_plugins_dir())

        # IMPORTANT: get_all_plugins() should reload manifests from disk (fresh state)
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
                print(
                    f"[INFO] Website server started on port {web_port}. Demo site and employee login: http://<host>:{web_port}/")
            except Exception:
                print("[ERROR] WebsiteServer failed to start:")
                traceback.print_exc()

        t = threading.Thread(target=_run_website,
                             name="WebsiteServerThread", daemon=True)
        t.start()
        print(
            f"[INFO] Website server thread started (listening on port {web_port}).")

    except Exception:
        print("[ERROR] Failed to start WebsiteServer:")
        traceback.print_exc()


# ----------------------------
# Main
# ----------------------------

if __name__ == "__main__":
    # Start the admin app in a separate daemon thread
    admin_thread = threading.Thread(
        target=run_admin_app, name="AdminFlaskThread", daemon=True)
    admin_thread.start()

    # Start website module (default port 80; nginx/Railway PORT is separate). Override with WEB_PORT.
    # Pre-deployment (e.g. Railway): re-comment the line below so the website server is not started
    # from this script (deployment typically runs the admin app only, or starts the website separately).
    # try_start_website()

    # Always watch restart.flag (PluginManager lifecycle actions should touch it)
    watcher_thread = threading.Thread(
        target=watch_for_restart_flag,
        kwargs={"poll_seconds": 0.5},
        name="RestartFlagWatcher",
        daemon=True
    )
    watcher_thread.start()

    # Keep the main thread alive without busy-waiting
    try:
        while admin_thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down.")
