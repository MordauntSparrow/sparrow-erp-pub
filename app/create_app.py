import os
import json
import subprocess
import sys
import re

from flask import Flask
from flask_login import LoginManager
from flask_cors import CORS

from dotenv import load_dotenv, set_key

from app.objects import User  # Your user model with get_user_by_id


# ---------------------------------------------------------------------
# Environment (.env) loading
# ---------------------------------------------------------------------

ENV_PATH = os.path.join(os.path.dirname(__file__), "config", ".env")
load_dotenv(dotenv_path=ENV_PATH)


def update_env_var(key, value):
    """
    Updates the given key in .env and reloads environment variables
    so that os.environ reflects the change in this process.
    """
    set_key(str(ENV_PATH), key, value)
    load_dotenv(ENV_PATH, override=True)


def restart_application():
    """
    Hard restart the current process.
    NOTE: Prefer the restart.flag watcher in run.py for normal operations.
    """
    print("Restarting application to apply changes...")
    os.execv(sys.executable, [sys.executable] + sys.argv)


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------

def _ensure_core_manifest(config_dir: str) -> str:
    """
    Ensures app/config/manifest.json exists. Returns its path.
    """
    core_manifest_path = os.path.join(config_dir, "manifest.json")
    os.makedirs(config_dir, exist_ok=True)

    if not os.path.exists(core_manifest_path):
        core_manifest = {
            "name": "Core Module",
            "system_name": "Sparrow_ERP_Core",
            "version": "1.0.0",
            "theme_settings": {
                "theme": "default",
                "custom_css_path": ""
            },
            "site_settings": {
                "company_name": "Sparrow ERP",
                "branding": "name",
                "logo_path": ""
            }
        }
        with open(core_manifest_path, 'w', encoding="utf-8") as f:
            json.dump(core_manifest, f, indent=4)
        print(f"Core manifest created at {core_manifest_path}")

    return core_manifest_path


def _run_dependency_handler(app_root: str) -> None:
    """
    Runs dependency_handler.py during startup.
    """
    dependency_handler_path = os.path.join(app_root, "dependency_handler.py")
    if not os.path.exists(dependency_handler_path):
        print(
            f"Error: Dependency handler not found at {dependency_handler_path}")
        sys.exit(1)

    try:
        print("Running dependency handler during application startup...")
        subprocess.check_call([sys.executable, dependency_handler_path])
    except subprocess.CalledProcessError as e:
        print(f"Dependency handler failed: {e}")
        sys.exit(1)


def _install_missing_dependency_from_import_error(e: ImportError) -> None:
    """
    Attempts to pip install the missing module from an ImportError, then restarts.
    """
    msg = str(e)
    # Typical ImportError: "No module named 'xyz'"
    missing = None
    if "'" in msg:
        try:
            missing = msg.split("'")[1]
        except Exception:
            missing = None

    if not missing:
        raise e

    print(
        f"Error importing module: {e}. Attempting to install missing dependency: {missing}")
    subprocess.check_call([sys.executable, "-m", "pip", "install", missing])
    print(
        f"Installed missing dependency: {missing}. Restarting application...")
    restart_application()


def _register_jinja_filters(app: Flask) -> None:
    def sort_keys(d):
        """Sort dictionary items so that keys with 'time' or 'date' come first."""
        def keyfunc(item):
            k, _v = item
            if 'time' in k.lower() or 'date' in k.lower():
                return (0, k.lower())
            return (1, k.lower())
        return sorted(d.items(), key=keyfunc)

    def format_timestamp(ts):
        if isinstance(ts, str):
            return ts.replace('T', ' ')
        return ts

    def regex_replace(value, pattern, repl):
        return re.sub(pattern, repl, value)

    @app.template_filter('fromjson')
    def fromjson_filter(s):
        return json.loads(s)

    app.jinja_env.filters['sort_keys'] = sort_keys
    app.jinja_env.filters['format_timestamp'] = format_timestamp
    app.jinja_env.filters['regex_replace'] = regex_replace


# ---------------------------------------------------------------------
# Flask app factory
# ---------------------------------------------------------------------

def create_app():
    """
    Create and configure the Flask admin app.
    """
    app_root = os.path.abspath(os.path.dirname(__file__))
    config_dir = os.path.join(app_root, "config")
    plugins_dir = os.path.abspath(os.path.join(app_root, "plugins"))

    # Ensure core config exists
    _ensure_core_manifest(config_dir)

    # Dependency handler (your existing behaviour)
    # _run_dependency_handler(app_root)

    # Import modules that may not exist until dependencies are installed
    try:
        from app.objects import PluginManager
    except ImportError as e:
        _install_missing_dependency_from_import_error(e)

    # Create Flask app
    app = Flask(__name__)

    # Jinja filters
    _register_jinja_filters(app)

    # Config
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'defaultsecretkey')
    app.config['PUBLIC_SERVER_URL'] = os.environ.get(
        'PUBLIC_SERVER_URL', 'http://localhost:80')

    # Flask-Login
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = "routes.login"

    @login_manager.user_loader
    def load_user(user_id):
        return User.get_user_by_id(user_id)

    # Register blueprints
    from app.routes import routes, api_bp
    app.register_blueprint(routes)
    app.register_blueprint(api_bp)

    # Plugins
    plugin_manager = PluginManager(plugins_dir=plugins_dir)
    plugin_manager.register_admin_routes(app)

    # CORS
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

    return app
