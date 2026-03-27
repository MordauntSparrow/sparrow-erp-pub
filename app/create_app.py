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

from flask_session import Session as FlaskSession
import redis as _redis

# Security and monitoring
try:
    from flask_seasurf import SeaSurf
except Exception:
    SeaSurf = None
try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
except Exception:
    Limiter = None
    get_remote_address = None
try:
    import sentry_sdk
    from sentry_sdk.integrations.flask import FlaskIntegration
except Exception:
    sentry_sdk = None
try:
    from prometheus_flask_exporter import PrometheusMetrics
except Exception:
    PrometheusMetrics = None


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
            },
            "ai_settings": {
                "chat_model": ""
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

    if os.environ.get("SPARROW_AUTO_PIP_INSTALL", "").lower() not in (
        "1",
        "true",
        "yes",
    ):
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

    # Railway volume: symlink upload dirs before blueprints import app.routes (mkdirs, etc.)
    try:
        from app.storage_paths import bind_persistent_directories

        bind_persistent_directories(app.root_path)
    except Exception as e:
        print(f"[WARN] Persistent storage bind failed: {e}")

    # Jinja filters
    _register_jinja_filters(app)

    # Config
    _default_secret = "defaultsecretkey"
    _secret_key = os.environ.get("SECRET_KEY", _default_secret)
    _flask_env = (os.environ.get("FLASK_ENV") or "").strip().lower()
    _railway_env = (os.environ.get("RAILWAY_ENVIRONMENT") or "").strip().lower()
    _treat_as_prod = _flask_env == "production" or _railway_env == "production"
    if _treat_as_prod and (
        not os.environ.get("SECRET_KEY")
        or _secret_key == _default_secret
    ):
        raise RuntimeError(
            "SECRET_KEY must be set to a strong random value in production "
            "(FLASK_ENV=production or RAILWAY_ENVIRONMENT=production)."
        )
    app.config["SECRET_KEY"] = _secret_key
    app.config['PUBLIC_SERVER_URL'] = os.environ.get(
        'PUBLIC_SERVER_URL', 'http://localhost:80')

    # Session / remember-me cookies (GDPR/DSPT: secure transport in production)
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    _sec_ck = (os.environ.get("SESSION_COOKIE_SECURE") or "").strip().lower()
    if _sec_ck == "true":
        app.config["SESSION_COOKIE_SECURE"] = True
    elif _sec_ck == "false":
        app.config["SESSION_COOKIE_SECURE"] = False
    else:
        app.config["SESSION_COOKIE_SECURE"] = bool(_treat_as_prod)
    app.config["REMEMBER_COOKIE_HTTPONLY"] = True
    app.config["REMEMBER_COOKIE_SAMESITE"] = "Lax"
    app.config["REMEMBER_COOKIE_SECURE"] = app.config["SESSION_COOKIE_SECURE"]

    # Flask-Login
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = "routes.login"

    @login_manager.user_loader
    def load_user(user_id):
        return User.get_user_by_id(user_id)

    @login_manager.unauthorized_handler
    def _admin_unauthorized_redirect():
        """Friendly redirect when session/Flask-Login requires login (e.g. after timeout)."""
        from flask import flash, redirect, request, url_for

        flash(
            "Your session has expired or you need to sign in. Please log in again.",
            "warning",
        )
        extra = {}
        if (
            request.method == "GET"
            and request.path
            and request.path not in ("/login", "/reset-password")
        ):
            extra["next"] = request.path
        return redirect(url_for("routes.login", **extra))

    # Register blueprints
    from app.routes import routes, api_bp
    app.register_blueprint(routes)
    app.register_blueprint(api_bp)

    _DEFAULT_ADMIN_SITE_SETTINGS = {
        "company_name": "Sparrow ERP",
        "branding": "name",
        "logo_path": "",
    }

    @app.before_request
    def _ensure_admin_session_site_settings():
        """
        After long inactivity, server-side session (or Redis) may be gone while
        Flask-Login 'remember me' still restores the user. Templates then hit
        session['site_settings'] and crash. Repopulate from core manifest.
        """
        from flask import request, session

        from flask_login import current_user

        ep = request.endpoint or ""
        if ep == "static" or str(ep).endswith(".static"):
            return None
        if not getattr(current_user, "is_authenticated", False):
            return None
        ss = session.get("site_settings")
        if isinstance(ss, dict) and (ss.get("company_name") or "").strip():
            return None
        try:
            pm = PluginManager(plugins_dir=plugins_dir)
            cm = pm.get_core_manifest() or {}
            session["site_settings"] = cm.get(
                "site_settings", dict(_DEFAULT_ADMIN_SITE_SETTINGS)
            )
            if not session.get("core_manifest"):
                session["core_manifest"] = cm
            session.modified = True
        except Exception:
            session["site_settings"] = dict(_DEFAULT_ADMIN_SITE_SETTINGS)
            session.modified = True
        return None

    # Browser security headers (defence in depth; CSP left to edge/nginx if needed)
    _hdr_default = "true" if _treat_as_prod else "false"
    if (os.environ.get("SECURITY_HEADERS_ENABLED") or _hdr_default).lower() in (
        "1",
        "true",
        "yes",
    ):

        @app.after_request
        def _security_headers(response):
            response.headers.setdefault("X-Content-Type-Options", "nosniff")
            response.headers.setdefault(
                "X-Frame-Options",
                (os.environ.get("X_FRAME_OPTIONS") or "SAMEORIGIN").strip(),
            )
            response.headers.setdefault(
                "Referrer-Policy",
                (
                    os.environ.get("REFERRER_POLICY")
                    or "strict-origin-when-cross-origin"
                ).strip(),
            )
            response.headers.setdefault(
                "Permissions-Policy",
                (
                    os.environ.get("PERMISSIONS_POLICY")
                    or "accelerometer=(), camera=(), geolocation=(), gyroscope=(), "
                    "magnetometer=(), microphone=(), payment=(), usb=()"
                ).strip(),
            )
            if (os.environ.get("ENABLE_HSTS") or "").lower() in ("1", "true", "yes"):
                max_age = (os.environ.get("HSTS_MAX_AGE") or "31536000").strip()
                response.headers.setdefault(
                    "Strict-Transport-Security",
                    f"max-age={max_age}; includeSubDomains",
                )
            return response

    # Plugins
    plugin_manager = PluginManager(plugins_dir=plugins_dir)
    plugin_manager.register_admin_routes(app)
    # Public plugin blueprints (e.g. recruitment /vacancies, employee portal paths) — required for url_for from admin templates
    plugin_manager.register_public_routes(app)

    _PLUGIN_URL_ACCESS_EXEMPT = frozenset(
        {"settings", "enable", "disable", "install", "uninstall", "install-remote"}
    )
    # JSON APIs under these prefixes authenticate inside the plugin (Bearer JWT, API tokens, etc.);
    # Flask-Login session is often absent. Redirecting anonymous /plugin/… requests to /login causes
    # 302 on health checks, OPTIONS (CORS), and login/token POSTs — breaking mobile/SPA clients.
    _PLUGIN_ROUTE_LEVEL_JSON_API_PREFIXES = (
        "/plugin/medical_records_module/api/",
        "/plugin/inventory_control/api/",
        "/plugin/ventus_response_module/api/",
        "/plugin/fleet_management/api/",
        "/plugin/asset_management/api/",
    )

    @app.before_request
    def _enforce_plugin_module_access():
        """Require access_permission (or default <plugin>.access) for /plugin/<name>/…"""
        from flask import flash, redirect, request, url_for
        from flask_login import current_user

        if not request.path.startswith("/plugin/"):
            return None
        if not getattr(current_user, "is_authenticated", False):
            path = request.path or ""
            if any(path.startswith(p) for p in _PLUGIN_ROUTE_LEVEL_JSON_API_PREFIXES):
                return None
            return redirect(
                url_for("routes.login", next=request.path)
            )
        parts = [p for p in request.path.split("/") if p]
        if len(parts) < 2:
            return None
        system_name = parts[1]
        if len(parts) >= 3 and parts[2] in _PLUGIN_URL_ACCESS_EXEMPT:
            return None
        try:
            from app.permissions_registry import user_can_access_plugin
        except ImportError:
            return None
        plugin_manager.plugins = plugin_manager.load_plugins() or {}
        manifest = None
        for folder, m in plugin_manager.plugins.items():
            if not isinstance(m, dict):
                continue
            sn = (m.get("system_name") or folder or "").strip()
            if sn == system_name or folder == system_name:
                manifest = m
                break
        if manifest is None:
            return None
        if not manifest.get("enabled"):
            return None
        if user_can_access_plugin(current_user, manifest, system_name):
            return None
        flash(
            "You do not have access to this module. Contact your administrator if you need access.",
            "danger",
        )
        return redirect(url_for("routes.dashboard"))

    # CORS: optional; API clients use JWT in Authorization (no credentialed cookies cross-origin).
    # Production: prefer CORS_ALLOWED_ORIGINS=comma-separated origins for least privilege.
    # If unset in production, we still enable CORS for core /api/* and /plugin/<name>/api/* only
    # (origins *, no credentials) so SPAs are not dead while logs show OPTIONS 200 with no POST.
    # Development: defaults to "*" on all routes when unset.
    _cors_raw = (os.environ.get("CORS_ALLOWED_ORIGINS") or "").strip()
    if not _cors_raw:
        _cors_raw = "*" if not _treat_as_prod else ""
    _cors_allow_headers = [
        "Content-Type",
        "Authorization",
        "Accept",
        "X-Requested-With",
        # Cura case create/update/close uses this header for idempotent writes.
        "Idempotency-Key",
    ]
    _cors_methods = [
        "GET",
        "HEAD",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "OPTIONS",
    ]
    _cors_resource_api_only = {
        # Core JSON: POST /api/login, /api/ping, etc.
        r"/api.*": {
            "origins": "*",
            "allow_headers": _cors_allow_headers,
            "methods": _cors_methods,
        },
        # Plugin JSON (Cura, Ventus MDT, inventory, …) — not /plugin/<name>/admin HTML.
        r"/plugin/[^/]+/api.*": {
            "origins": "*",
            "allow_headers": _cors_allow_headers,
            "methods": _cors_methods,
        },
    }
    if _cors_raw:
        if _cors_raw == "*":
            CORS(
                app,
                resources={
                    r"/*": {
                        "origins": "*",
                        "allow_headers": _cors_allow_headers,
                        "methods": _cors_methods,
                    }
                },
                supports_credentials=False,
            )
        else:
            _cors_origins = [o.strip() for o in _cors_raw.split(",") if o.strip()]
            if _cors_origins:
                CORS(
                    app,
                    resources={
                        r"/*": {
                            "origins": _cors_origins,
                            "allow_headers": _cors_allow_headers,
                            "methods": _cors_methods,
                        }
                    },
                    supports_credentials=False,
                )
    elif _treat_as_prod:
        CORS(
            app,
            resources=_cors_resource_api_only,
            supports_credentials=False,
        )
        app.logger.warning(
            "CORS_ALLOWED_ORIGINS is unset in production: using permissive CORS (*, no credentials) "
            "only for /api* and /plugin/*/api* so SPAs can complete preflight (POST /api/login, Cura token, etc.). "
            "Set CORS_ALLOWED_ORIGINS to explicit origins when you can (tighter than *)."
        )

    # ------------------------------------------------------------------
    # Session store (Redis) for multi-instance deployments
    # ------------------------------------------------------------------
    redis_url = os.environ.get('REDIS_URL') or os.environ.get('REDIS_URLS')
    if redis_url:
        try:
            app.config['SESSION_TYPE'] = 'redis'
            app.config['SESSION_REDIS'] = _redis.from_url(redis_url)
            app.config['SESSION_COOKIE_HTTPONLY'] = True
            app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
            _rs = (os.environ.get("SESSION_COOKIE_SECURE") or "").strip().lower()
            if _rs == "true":
                app.config["SESSION_COOKIE_SECURE"] = True
            elif _rs == "false":
                app.config["SESSION_COOKIE_SECURE"] = False
            else:
                app.config["SESSION_COOKIE_SECURE"] = bool(_treat_as_prod)
            FlaskSession(app)
        except Exception as e:
            print(f"[WARN] Redis session setup failed: {e}")

    # ------------------------------------------------------------------
    # CSRF protection (SeaSurf) and rate limiting
    # ------------------------------------------------------------------
    if SeaSurf:
        try:
            csrf = SeaSurf(app)
            # Core JSON auth: no CSRF cookie/header from mobile or scripted clients.
            for endpoint in ("api.api_login", "api.api_logout"):
                view = app.view_functions.get(endpoint)
                if view and not getattr(view, "_csrf_exempt", False):
                    csrf.exempt(view)
                    view._csrf_exempt = True
            # Plugin JSON under /plugin/<name>/api/… (Cura, EPCR, inventory, Ventus MDT, …) and
            # Ventus root /api/mdt|messages|ping aliases: JWT Bearer or token auth — same CSRF model as
            # api_login (SeaSurf still runs globally unless views are exempt).
            _csrf_exempt_path_patterns = (
                re.compile(r"^/plugin/[^/]+/api(?:/.*)?$"),
                re.compile(r"^/api/mdt(?:/.*)?$"),
                re.compile(r"^/api/messages(?:/.*)?$"),
                re.compile(r"^/api/ping$"),
            )
            _csrf_state_changing = frozenset(
                {"POST", "PUT", "PATCH", "DELETE"}
            )
            for rule in app.url_map.iter_rules():
                if not (rule.methods & _csrf_state_changing):
                    continue
                path = rule.rule or ""
                if not any(p.match(path) for p in _csrf_exempt_path_patterns):
                    continue
                view = app.view_functions.get(rule.endpoint)
                if view and not getattr(view, "_csrf_exempt", False):
                    csrf.exempt(view)
                    view._csrf_exempt = True
        except Exception as e:
            print(f"[WARN] SeaSurf init failed: {e}")
    # Ensure csrf_token() exists in Jinja so templates never raise UndefinedError (e.g. when SeaSurf not installed or init failed)
    if "csrf_token" not in app.jinja_env.globals:
        def _csrf_token():
            return ""
        app.jinja_env.globals["csrf_token"] = _csrf_token

    # Basic rate limiting + stricter caps on authentication surfaces
    if Limiter:
        try:
            _rl_storage = os.environ.get("RATELIMIT_STORAGE_URI") or os.environ.get(
                "REDIS_URL"
            )
            limiter = Limiter(
                key_func=get_remote_address,
                default_limits=[
                    os.environ.get("RATE_LIMIT", "200 per minute"),
                ],
                storage_uri=_rl_storage,
            )
            limiter.init_app(app)

            def _skip_non_post():
                from flask import request as _rq

                return (_rq.method or "").upper() != "POST"

            _api_login_lim = os.environ.get("API_LOGIN_RATE_LIMIT", "30 per minute")
            _form_login_lim = os.environ.get("FORM_LOGIN_RATE_LIMIT", "15 per minute")
            _pw_reset_lim = os.environ.get("PASSWORD_RESET_RATE_LIMIT", "5 per minute")
            _v = app.view_functions.get("api.api_login")
            if _v:
                limiter.limit(_api_login_lim, methods=["POST"])(_v)
            _v2 = app.view_functions.get("routes.login")
            if _v2:
                limiter.limit(_form_login_lim, exempt_when=_skip_non_post)(_v2)
            _v3 = app.view_functions.get("routes.reset_password_request")
            if _v3:
                limiter.limit(_pw_reset_lim, exempt_when=_skip_non_post)(_v3)
            # Contractor crew search (time billing): tighter cap per IP (env override)
            _tb_cs_lim = os.environ.get(
                "TB_CONTRACTOR_SEARCH_RATE_LIMIT", "60 per minute"
            )
            _v_tb_cs = app.view_functions.get(
                "public_time_billing.api_public_refs_contractors"
            )
            if _v_tb_cs:
                limiter.limit(_tb_cs_lim, methods=["GET"])(_v_tb_cs)
            _tb_cs_admin_lim = os.environ.get(
                "TB_ADMIN_CONTRACTOR_SEARCH_RATE_LIMIT", "120 per minute"
            )
            _v_tb_adm = app.view_functions.get(
                "internal_time_billing.api_admin_refs_contractors"
            )
            if _v_tb_adm:
                limiter.limit(_tb_cs_admin_lim, methods=["GET"])(_v_tb_adm)
        except Exception as e:
            print(f"[WARN] Limiter init failed: {e}")

    # ------------------------------------------------------------------
    # Sentry (optional)
    # ------------------------------------------------------------------
    try:
        sentry_dsn = os.environ.get('SENTRY_DSN')
        if sentry_sdk and sentry_dsn:
            sentry_sdk.init(dsn=sentry_dsn, integrations=[
                            FlaskIntegration()], traces_sample_rate=0.1)
    except Exception as e:
        print(f"[WARN] Sentry init failed: {e}")

    # ------------------------------------------------------------------
    # Prometheus metrics (optional)
    # ------------------------------------------------------------------
    try:
        if PrometheusMetrics:
            PrometheusMetrics(app)
    except Exception as e:
        print(f"[WARN] Prometheus metrics init failed: {e}")

    # ------------------------------------------------------------------
    # Structured audit logging (rotating JSON file)
    # ------------------------------------------------------------------
    try:
        import logging
        import json
        from logging.handlers import RotatingFileHandler

        logs_dir = os.path.join(app_root, 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        audit_path = os.path.join(logs_dir, 'audit.log')

        class JsonFormatter(logging.Formatter):
            def format(self, record):
                payload = {
                    'time': self.formatTime(record, self.datefmt),
                    'level': record.levelname,
                    'logger': record.name,
                    'message': record.getMessage()
                }
                # include extra fields if present
                if hasattr(record, 'extra') and isinstance(record.extra, dict):
                    payload.update(record.extra)
                return json.dumps(payload, default=str)

        audit_handler = RotatingFileHandler(
            audit_path, maxBytes=10*1024*1024, backupCount=5)
        audit_handler.setLevel(logging.INFO)
        audit_handler.setFormatter(JsonFormatter())

        audit_logger = logging.getLogger('audit')
        audit_logger.setLevel(logging.INFO)
        audit_logger.addHandler(audit_handler)
        # make available on app for use
        app.audit_logger = audit_logger
    except Exception as e:
        print(f"[WARN] Audit logging setup failed: {e}")
    # ------------------------------------------------------------------
    # Socket.IO initialization (optional Redis message queue)
    # ------------------------------------------------------------------
    try:
        from . import socketio
        redis_url = os.environ.get('REDIS_URL') or os.environ.get('REDIS_URLS')
        socketio_opts = {}
        # If a Redis message queue is configured, provide it so SocketIO can
        # scale across processes/instances.
        if redis_url:
            socketio_opts['message_queue'] = redis_url
        # Preferred async mode may be set via env var (default to eventlet)
        async_mode = os.environ.get('SOCKETIO_ASYNC_MODE', 'eventlet')
        _sio = (os.environ.get("SOCKETIO_CORS_ORIGINS") or "").strip()
        if not _sio:
            _sio = (os.environ.get("CORS_ALLOWED_ORIGINS") or "").strip()
        if not _sio or _sio == "*":
            sio_cors = "*" if not _treat_as_prod else []
        else:
            sio_cors = [x.strip() for x in _sio.split(",") if x.strip()]
        socketio.init_app(
            app,
            cors_allowed_origins=sio_cors,
            async_mode=async_mode,
            **socketio_opts,
        )
        # Authenticated sockets only; panel sync scoped per user (no cross-account broadcast)
        try:
            from flask_login import current_user
            from flask_socketio import disconnect, join_room, emit
            from flask_socketio import request as _socketio_request

            def _panel_user_room():
                if not getattr(current_user, "is_authenticated", False):
                    return None
                try:
                    return f"panel_user_{current_user.get_id()}"
                except Exception:
                    return None

            @socketio.on('connect')
            def _on_connect():
                try:
                    if not getattr(current_user, 'is_authenticated', False):
                        disconnect()
                        return
                    room = _panel_user_room()
                    if room:
                        join_room(room)
                except Exception:
                    disconnect()

            @socketio.on('panel_message')
            def _on_panel_message(msg):
                try:
                    if not getattr(current_user, 'is_authenticated', False):
                        return
                    if not isinstance(msg, dict):
                        return
                    raw = json.dumps(msg, default=str, separators=(',', ':'))
                    if len(raw) > 65536:
                        return
                    room = _panel_user_room()
                    if not room:
                        return
                    emit(
                        'panel_message',
                        msg,
                        room=room,
                        skip_sid=_socketio_request.sid,
                    )
                except Exception:
                    pass
        except Exception:
            pass
    except Exception as e:
        # Non-fatal: if SocketIO imports fail, continue without realtime features.
        print(f"[WARN] SocketIO initialization failed: {e}")

    @app.cli.command("fleet-asset-reminders")
    def fleet_asset_reminders_command():
        """
        Email fleet compliance + asset maintenance reminders (requires SMTP env).
        Recipients: FLEET_ASSET_REMINDER_EMAILS or all admin/superuser emails.
        """
        try:
            from app.plugins.fleet_management.reminders import run_reminders
        except ImportError as e:
            print(f"[ERROR] fleet_management.reminders not available: {e}")
            return
        out = run_reminders(send_email=True)
        print(out.get("message", out))
        if out.get("error"):
            print(f"[WARN] {out['error']}")
        if out.get("sent"):
            print("[OK] Email sent.")
        elif not out.get("lines"):
            print("[OK] Nothing to send.")

    return app
