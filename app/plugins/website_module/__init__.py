from flask import Flask, session
from werkzeug.serving import make_server
import threading
import os
import json
from .routes import *
from ...objects import PluginManager
from flask_login import LoginManager
import logging

try:
    from flask_seasurf import SeaSurf
except Exception:
    SeaSurf = None


def _discover_core_templates_dir(app):
    """
    Directory containing shared Jinja partials (e.g. partials/sparrow_csrf_head.html).
    The website Flask app only loads website_module/templates by default; the ERP core
    lives under app/templates/. Resolves paths for local dev and Docker (e.g. /app/app/...).

    Override with env SPARROW_CORE_TEMPLATES=/absolute/path/to/app/templates
    """
    candidates = []
    env_dir = (os.environ.get("SPARROW_CORE_TEMPLATES") or "").strip()
    if env_dir:
        candidates.append(os.path.abspath(env_dir))

    here = os.path.dirname(os.path.abspath(__file__))
    # .../app/plugins/website_module -> .../app/templates
    candidates.append(os.path.abspath(os.path.join(here, "..", "..", "templates")))

    rp = getattr(app, "root_path", None) or ""
    if rp:
        candidates.append(os.path.abspath(os.path.join(rp, "..", "..", "templates")))

    seen = set()
    for c in candidates:
        if not c or c in seen:
            continue
        seen.add(c)
        partial = os.path.join(c, "partials", "sparrow_csrf_head.html")
        if os.path.isfile(partial):
            return c
    return None


def _append_core_templates_loader(app):
    """
    After all blueprints are registered, wrap the Jinja loader so includes like
    {% include 'partials/sparrow_csrf_head.html' %} resolve to app/templates/.
    (Patching app.jinja_loader alone is fragile because jinja_loader is a cached_property
    and the Environment uses DispatchingJinjaLoader over app + blueprints.)
    """
    core = _discover_core_templates_dir(app)
    if not core:
        logging.warning(
            "Website app: could not find core templates (partials/sparrow_csrf_head.html). "
            "Set SPARROW_CORE_TEMPLATES to the absolute path of the folder that contains "
            "partials/ (normally app/templates). Tried SPARROW_CORE_TEMPLATES and paths "
            "relative to website_module."
        )
        return
    from jinja2 import ChoiceLoader, FileSystemLoader

    app.jinja_env.loader = ChoiceLoader(
        [app.jinja_env.loader, FileSystemLoader(core)]
    )


def create_website_app(plugins_dir=None):
    """
    Creates and configures the Flask app for the website module.
    Dynamically loads plugin routes and login loaders if they exist.
    """
    if plugins_dir is None:
        plugins_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    # Same volume bind as admin app (run_website.py also calls this early).
    try:
        from app.storage_paths import bind_persistent_directories

        _app_pkg = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        bind_persistent_directories(_app_pkg)
    except Exception as e:
        logging.warning("Persistent storage bind failed on website app: %s", e)

    app = Flask(__name__,
                static_url_path='/static',
                static_folder=os.path.join(os.path.dirname(__file__), '../../static'))

    import re

    def sort_keys(d):
        """Sort dictionary items so that keys with 'time' or 'date' come first."""
        def keyfunc(item):
            k, v = item
            if 'time' in k.lower() or 'date' in k.lower():
                return (0, k.lower())
            return (1, k.lower())
        return sorted(d.items(), key=keyfunc)
    app.jinja_env.filters['sort_keys'] = sort_keys

    def format_timestamp(ts):
        if isinstance(ts, str):
            return ts.replace('T', ' ')
        return ts
    app.jinja_env.filters['format_timestamp'] = format_timestamp

    def regex_replace(value, pattern, repl):
        return re.sub(pattern, repl, value)

    app.jinja_env.filters['regex_replace'] = regex_replace

    # Set your secret key for sessions and token generation
    app.secret_key = os.environ.get('WEB_SECRET_KEY') or 'your_secret_key_here'
    # Ensure session cookie is sent for all paths (e.g. /employee-portal and /time-billing share auth)
    app.config.setdefault('SESSION_COOKIE_PATH', '/')
    app.config.setdefault('SESSION_COOKIE_SAMESITE', 'Lax')

    # CSRF: provide csrf_token() in templates so shared templates (e.g. forms) don't raise UndefinedError
    if SeaSurf:
        try:
            SeaSurf(app)
        except Exception as e:
            logging.warning("SeaSurf init failed on website app: %s", e)
    if 'csrf_token' not in app.jinja_env.globals:
        def _csrf_token():
            return ''
        app.jinja_env.globals['csrf_token'] = _csrf_token

    # Initialize Flask-Login and configure the user loader.
    login_manager = LoginManager()
    login_manager.init_app(app)
    # No default login view here; each plugin handles its own.

    # Instantiate PluginManager.
    pluginManager = PluginManager(plugins_dir)
    # Now load the actual plugin modules.
    plugin_modules = pluginManager.load_plugin_modules()  # Assumes your PluginManager now has load_plugin_modules()

    # Build a mapping of login loaders: {login_prefix: get_user_by_id_callable}
    plugin_login_loaders = {}
    for plugin in plugin_modules:
        if hasattr(plugin, 'login_prefix') and hasattr(plugin, 'get_user_by_id'):
            plugin_login_loaders[plugin.login_prefix] = plugin.get_user_by_id

    # Store the mapping in app config for access in the user loader.
    app.config['PLUGIN_LOGIN_LOADERS'] = plugin_login_loaders

    @login_manager.user_loader
    def load_user(user_id):
        """
        Expects composite IDs in the format 'prefix:id' (e.g., 'Vita-Care-Portal:123').
        Splits the ID to determine which plugin's loader to use.
        """
        try:
            prefix, uid = user_id.split(":", 1)
        except ValueError:
            return None
        loaders = app.config.get('PLUGIN_LOGIN_LOADERS', {})
        if prefix in loaders:
            user = loaders[prefix](uid)
            if user:
                user.role = prefix  # Optionally tag the user with its module prefix.
            return user
        return None

    # Load website module manifest.
    manifest_path = os.path.join(os.path.dirname(__file__), 'manifest.json')
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    control_mode = manifest.get('control_mode', 'erp')

    # Dynamic route registration based on control mode.
    if control_mode == 'erp':
        pluginManager.register_public_routes(app)
        app.register_blueprint(website_public_routes, url_prefix="/")
        print("ERP Mode Intialised Successfully!")
    elif control_mode == 'moderate':
        print("Moderate mode: Developers can manually add routes.")
        pluginManager.register_public_routes(app)
        app.register_blueprint(website_public_routes, url_prefix="/")
        app.register_blueprint(website_public_added_routes, url_prefix="/")
        print("Moderate Mode Loaded Successfully")
    elif control_mode == 'independent':
        print("Independent mode: Website module provides no predefined routes.")
        app.register_blueprint(website_public_added_routes, url_prefix="/")
        print("Moderate Mode Loaded Successfully")

    # Must run after blueprints exist: add app/templates for shared {% include %} partials.
    _append_core_templates_loader(app)

    # Public base template uses this instead of url_for('website_public.static') for portability.
    app.jinja_env.globals["website_static_url"] = website_static_url
    app.jinja_env.globals["website_public_page_url"] = website_public_page_url
    app.jinja_env.globals["website_public_asset_url"] = website_public_asset_url

    return app


class WebsiteServer:
    """
    Manages the website Flask application lifecycle.
    """
    def __init__(self, port=8080, plugins_dir=os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))):
        self.port = port
        self.plugins_dir = plugins_dir
        self.app = create_website_app(plugins_dir)
        self.server = None
        self.thread = None

    def start(self, debug=False):
        """
        Starts the website server.
        - If debug is True, runs the Flask development server in blocking mode with debug=True.
        - Otherwise, runs the server in a background thread.
        """
        if debug:
            print(f"Starting website server in debug mode on port {self.port} (blocking)...")
            # Run the Flask development server. This will use app.run,
            # which in debug mode will enable the interactive debugger and reloader.
            self.app.run(host="0.0.0.0", port=self.port, debug=True, use_reloader=False)
        else:
            def _run():
                from werkzeug.serving import make_server
                self.server = make_server("0.0.0.0", self.port, self.app)
                print(f"Website server running on port {self.port}...")
                self.server.serve_forever()
            print(f"Starting website server on port {self.port} in background thread...")
            self.thread = threading.Thread(target=_run)
            self.thread.daemon = True
            self.thread.start()
            print("Website server started.")

    def stop(self):
        """
        Stops the website server gracefully.
        """
        if self.server:
            print("Stopping website server...")
            self.server.shutdown()
            if self.thread:
                self.thread.join()
            print("Website server stopped.")
        else:
            print("Website server is not running.")
