from flask import Flask, session
from werkzeug.serving import make_server
import threading
import os
import json
from .routes import *
from ...objects import PluginManager
from flask_login import LoginManager
import logging

def create_website_app(plugins_dir=None):
    """
    Creates and configures the Flask app for the website module.
    Dynamically loads plugin routes and login loaders if they exist.
    """
    if plugins_dir is None:
        plugins_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

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
    app.secret_key = 'your_secret_key_here'

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
    return app


class WebsiteServer:
    """
    Manages the website Flask application lifecycle.
    """
    def __init__(self, port=80, plugins_dir=os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))):
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