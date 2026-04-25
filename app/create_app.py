import logging
import os
import json
import subprocess
import sys
import re
import time

from flask import Flask
from flask_login import LoginManager
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix

from dotenv import load_dotenv, set_key

from app.objects import DatabaseTemporarilyUnavailable, User

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
# Non-empty SMTP_* before loading app .env (e.g. Railway Variables) must not be overwritten by
# the volume file. Keys that exist but are empty must NOT block the volume—otherwise placeholder
# vars in the dashboard hide saved settings after redeploy.
_PLATFORM_SMTP_KEYS_NONEMPTY_AT_BOOT = frozenset(
    k
    for k in os.environ
    if k.startswith("SMTP_") and str(os.environ.get(k, "") or "").strip()
)
load_dotenv(dotenv_path=ENV_PATH)

from app.storage_paths import load_volume_smtp_into_os_environ

load_volume_smtp_into_os_environ(skip_keys=_PLATFORM_SMTP_KEYS_NONEMPTY_AT_BOOT)


def _railway_or_stdout_access_logs() -> bool:
    """
    Railway UI treats stderr as “errors” (red). Werkzeug’s HTTP access log defaults to stderr,
    which inflates error-rate metrics. Opt out with SPARROW_ACCESS_LOG_STDOUT=0/false.
    Opt in on any host with SPARROW_ACCESS_LOG_STDOUT=1/true.
    """
    v = (os.environ.get("SPARROW_ACCESS_LOG_STDOUT") or "").strip().lower()
    if v in ("0", "false", "no"):
        return False
    if v in ("1", "true", "yes"):
        return True
    return bool(
        os.environ.get("RAILWAY_ENVIRONMENT")
        or os.environ.get("RAILWAY_PROJECT_ID")
        or os.environ.get("RAILWAY_SERVICE_ID")
    )


def _configure_werkzeug_access_log_stdout() -> None:
    """Send Werkzeug request lines (GET … 200) to stdout instead of stderr."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    log = logging.getLogger("werkzeug")
    log.handlers.clear()
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False


def _should_trust_proxy_headers() -> bool:
    """
    Enable trusted proxy header handling when running behind Railway/nginx unless
    explicitly disabled.
    """
    v = (os.environ.get("TRUST_PROXY_HEADERS") or "").strip().lower()
    if v in ("0", "false", "no"):
        return False
    if v in ("1", "true", "yes"):
        return True
    return bool(
        os.environ.get("RAILWAY_ENVIRONMENT")
        or os.environ.get("RAILWAY_PROJECT_ID")
        or os.environ.get("RAILWAY_SERVICE_ID")
    )


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return bool(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.environ.get(name, default)).strip())
    except Exception:
        return int(default)


_SCANNER_GUARD_SUSPICIOUS_PATH_RE = re.compile(
    r"^/(?:"
    r"\.(?!well-known/)"
    r"|wp-"
    r"|wordpress"
    r"|phpmyadmin"
    r"|cgi-bin"
    r"|vendor(?:/|\.js$)"
    r"|node_modules/"
    r"|composer\.(?:json|lock)"
    r"|package(?:-lock)?\.json"
    r"|yarn\.lock"
    r"|pnpm-lock\.yaml"
    r"|Dockerfile"
    r"|docker-compose\.ya?ml"
    r"|server-status"
    r"|config(?:/|\.|$)"
    r"|backup(?:/|\.|$)"
    r"|api-docs/swagger\.json"
    r"|swagger(?:/|\.|$)"
    r"|openapi(?:\.json|/)"
    r"|manifest\.json"
    r"|asset-manifest\.json"
    r"|env\.json"
    r"|\.env"
    r"|\.git"
    r"|\.svn"
    r"|\.hg"
    r"|@vite/client"
    r"|\.vite/"
    r")",
    re.IGNORECASE,
)
_LOCAL_SCANNER_GUARD_STATE = {}


def _scanner_guard_enabled() -> bool:
    return _env_truthy("SPARROW_SCANNER_GUARD_ENABLED", default=True)


def _is_suspicious_probe_path(path: str) -> bool:
    path = str(path or "").strip()
    if not path.startswith("/"):
        path = f"/{path}"
    return bool(_SCANNER_GUARD_SUSPICIOUS_PATH_RE.match(path))


def _scanner_guard_store_get(key: str):
    now = time.time()
    item = _LOCAL_SCANNER_GUARD_STATE.get(key)
    if not item:
        return None
    expires_at, value = item
    if expires_at <= now:
        _LOCAL_SCANNER_GUARD_STATE.pop(key, None)
        return None
    return value


def _scanner_guard_store_set(key: str, value, ttl_seconds: int) -> None:
    _LOCAL_SCANNER_GUARD_STATE[key] = (time.time() + max(1, int(ttl_seconds)), value)


def _scanner_guard_store_incr(key: str, ttl_seconds: int) -> int:
    current = _scanner_guard_store_get(key)
    new_value = int(current or 0) + 1
    _scanner_guard_store_set(key, new_value, ttl_seconds)
    return new_value


def _resolve_socketio_async_mode() -> str:
    """
    Default was eventlet, but eventlet is not in requirements.txt — that caused
    “Invalid async_mode specified”. Prefer installed libs, else threading (works with socketio.run).
    """
    import importlib.util

    def _has(mod: str) -> bool:
        try:
            return importlib.util.find_spec(mod) is not None
        except Exception:
            return False

    raw = (os.environ.get("SOCKETIO_ASYNC_MODE") or "").strip().lower()
    if raw == "threading":
        return "threading"
    if raw == "eventlet":
        return "eventlet" if _has("eventlet") else "threading"
    if raw == "gevent":
        return "gevent" if _has("gevent") else "threading"
    if _has("eventlet"):
        return "eventlet"
    if _has("gevent"):
        return "gevent"
    return "threading"


def update_env_var(key, value):
    """
    Persist ``key`` to disk and mirror it into ``os.environ`` for this process.

    ``SMTP_*`` keys are written to ``{persistent_volume}/config/.env.smtp`` when a persistent
    data root is configured (Railway volume, etc.), so email settings survive redeploys;
    otherwise they go to ``app/config/.env`` like other keys.

    Only the given key is updated in the environment. Do **not** call
    ``load_dotenv(..., override=True)`` here: that reapplies every entry in ``.env`` and
    overwrites platform-injected variables (e.g. Railway's ``DB_HOST``) with stale values
    such as ``localhost`` left in the file from local dev—saving SMTP or any other setting
    would then break the database until restart (and can persist broken values on disk).
    """
    from app.storage_paths import get_persistent_smtp_env_path

    k = str(key)
    if k.startswith("SMTP_"):
        smtp_path = get_persistent_smtp_env_path()
        target = smtp_path if smtp_path else str(ENV_PATH)
        os.makedirs(os.path.dirname(os.path.abspath(target)), exist_ok=True)
        set_key(target, k, "" if value is None else str(value))
        if value is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = str(value)
        return

    set_key(str(ENV_PATH), k, value)
    if value is None:
        os.environ.pop(k, None)
    else:
        os.environ[k] = str(value)


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
    Ensures app/config/manifest.json exists (or symlink to volume after bind_persistent_directories).
    """
    from app.storage_paths import write_default_core_manifest_file

    core_manifest_path = os.path.join(config_dir, "manifest.json")
    os.makedirs(config_dir, exist_ok=True)

    if not os.path.exists(core_manifest_path):
        write_default_core_manifest_file(core_manifest_path)
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

    def unique_flashes(messages):
        """Collapse duplicate (category, message) pairs from the flash queue."""
        if not messages:
            return []
        seen = set()
        out = []
        for item in messages:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            cat, msg = item[0], item[1]
            key = (str(cat), str(msg))
            if key in seen:
                continue
            seen.add(key)
            out.append((cat, msg))
        return out

    def admin_visible_flashes(messages):
        """Same as unique_flashes but drops success toasts (green banners) on the main admin shell."""
        skip = frozenset(("success", "plugins_page_success"))
        return [
            (cat, msg)
            for cat, msg in unique_flashes(messages)
            if str(cat or "").lower() not in skip
        ]

    def fleet_vdi_suppress_flash(msg) -> bool:
        """Hide global/admin flashes on the crew VDI layout (standalone page)."""
        if msg is None:
            return True
        m = str(msg)
        needles = (
            "Upgrade scripts completed",
            "Upgrade run completed",
            "Upgrade run failed",
            "Welcome back",
            "VDI form saved",
            "VDI form restored",
            "default template",
        )
        return any(n in m for n in needles)

    @app.template_filter('fromjson')
    def fromjson_filter(s):
        return json.loads(s)

    app.jinja_env.filters['sort_keys'] = sort_keys
    app.jinja_env.filters['format_timestamp'] = format_timestamp
    app.jinja_env.filters['regex_replace'] = regex_replace
    app.jinja_env.filters['unique_flashes'] = unique_flashes
    app.jinja_env.filters['admin_visible_flashes'] = admin_visible_flashes
    app.jinja_env.filters['fleet_vdi_suppress_flash'] = fleet_vdi_suppress_flash

    from app.static_upload_paths import (
        normalize_manifest_static_path,
        static_upload_file_exists,
    )

    app.jinja_env.globals["normalize_manifest_static_path"] = (
        normalize_manifest_static_path
    )
    app.jinja_env.globals["static_upload_file_exists"] = static_upload_file_exists

    from app.branding_utils import auth_logo_inline_style, navbar_logo_inline_style

    app.jinja_env.globals["navbar_logo_inline_style"] = navbar_logo_inline_style
    app.jinja_env.globals["auth_logo_inline_style"] = auth_logo_inline_style


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

    # Railway volume: bind before core manifest so app/config/manifest.json can symlink to the volume.
    try:
        from app.storage_paths import bind_persistent_directories

        bind_persistent_directories(app_root)
    except Exception as e:
        print(f"[WARN] Persistent storage bind failed: {e}")

    # Ensure core config exists (writes to volume when manifest.json is symlinked)
    _ensure_core_manifest(config_dir)
    try:
        from app.organization_profile import (
            migrate_core_manifest_organization_profile_defaults,
        )

        migrate_core_manifest_organization_profile_defaults(
            os.path.join(config_dir, "manifest.json")
        )
    except Exception as _org_mig_err:
        print(f"[WARN] organization_profile manifest migration skipped: {_org_mig_err}")

    # Dependency handler (your existing behaviour)
    # _run_dependency_handler(app_root)

    # Import modules that may not exist until dependencies are installed
    try:
        from app.objects import PluginManager
    except ImportError as e:
        _install_missing_dependency_from_import_error(e)

    # Create Flask app
    app = Flask(__name__)
    if _should_trust_proxy_headers():
        # Railway sits in front of the container and nginx sits in front of Flask inside
        # the container, so trust one hop of forwarded headers by default there.
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    try:
        from app.storage_paths import load_volume_smtp_into_os_environ

        # Same skip set as at module import; re-apply after bind (no-op if no volume file).
        load_volume_smtp_into_os_environ(skip_keys=_PLATFORM_SMTP_KEYS_NONEMPTY_AT_BOOT)
    except Exception as e:
        print(f"[WARN] Volume SMTP re-load failed: {e}")

    # Jinja filters
    _register_jinja_filters(app)

    @app.context_processor
    def inject_branding_logo_style_helpers():
        """
        Also expose logo sizing helpers via context (not only jinja_env.globals).
        Public plugin blueprints (inventory, scheduling, fleet, …) extend
        employee_portal_module/base_public.html; app-level context processors
        run for every request so those layouts always receive these names.
        """
        from app.branding_utils import auth_logo_inline_style, navbar_logo_inline_style

        return {
            "navbar_logo_inline_style": navbar_logo_inline_style,
            "auth_logo_inline_style": auth_logo_inline_style,
        }

    @app.context_processor
    def inject_user_management_nav():
        from app.permissions_registry import (
            user_can_open_org_admin_nav,
            user_can_open_user_management,
        )

        return {
            "can_open_user_management": user_can_open_user_management,
            "can_open_org_admin_nav": user_can_open_org_admin_nav,
        }

    @app.context_processor
    def inject_copyright_year():
        from datetime import datetime

        return {"current_year": datetime.now().year}

    @app.context_processor
    def inject_organization_industries():
        from flask import current_app

        from app.organization_profile import (
            normalize_organization_industries,
            tenant_matches_industry,
        )

        def industry_visible(*slugs: str) -> bool:
            if not slugs:
                return True
            return tenant_matches_industry(
                current_app.config.get("organization_industries"),
                *slugs,
            )

        ids = normalize_organization_industries(
            current_app.config.get("organization_industries")
        )
        return {
            "organization_industries": ids,
            "industry_visible": industry_visible,
        }

    @app.before_request
    def _scanner_guard_before_request():
        """
        Short-circuit obvious exploit probes and temporarily ban repeat offenders.
        Uses Redis when available so bans survive worker/process restarts.
        """
        from flask import request

        if not _scanner_guard_enabled():
            return None
        path = request.path or "/"
        if path.startswith("/static/") or path.startswith("/.well-known/"):
            return None
        client_ip = (
            request.headers.get("CF-Connecting-IP")
            or request.headers.get("True-Client-IP")
            or request.remote_addr
            or "unknown"
        )
        key_prefix = (
            os.environ.get("SPARROW_SCANNER_GUARD_KEY_PREFIX")
            or "sparrow:scanner_guard"
        ).strip()
        ban_key = f"{key_prefix}:ban:{client_ip}"
        counter_key = f"{key_prefix}:probe:{client_ip}"
        ban_seconds = max(60, _env_int("SPARROW_SCANNER_BAN_SECONDS", 3600))
        window_seconds = max(60, _env_int("SPARROW_SCANNER_WINDOW_SECONDS", 600))
        threshold = max(3, _env_int("SPARROW_SCANNER_BAN_THRESHOLD", 12))
        status_code = _env_int("SPARROW_SCANNER_BLOCK_STATUS", 404)
        if status_code not in (400, 403, 404, 410, 429):
            status_code = 404

        redis_client = app.config.get("SESSION_REDIS")

        def _is_banned() -> bool:
            if redis_client is not None:
                try:
                    return bool(redis_client.exists(ban_key))
                except Exception:
                    pass
            return bool(_scanner_guard_store_get(ban_key))

        def _register_probe() -> int:
            if redis_client is not None:
                try:
                    pipe = redis_client.pipeline()
                    pipe.incr(counter_key)
                    pipe.ttl(counter_key)
                    count, ttl = pipe.execute()
                    if int(ttl) < 0:
                        redis_client.expire(counter_key, window_seconds)
                    count = int(count)
                    if count >= threshold:
                        redis_client.setex(ban_key, ban_seconds, "1")
                    return count
                except Exception:
                    pass
            count = _scanner_guard_store_incr(counter_key, window_seconds)
            if count >= threshold:
                _scanner_guard_store_set(ban_key, "1", ban_seconds)
            return count

        if _is_banned():
            return ("", status_code)

        if not _is_suspicious_probe_path(path):
            return None

        count = _register_probe()
        if count >= threshold:
            try:
                app.logger.warning(
                    "scanner_guard banned ip=%s after suspicious path=%s count=%s",
                    client_ip,
                    path,
                    count,
                )
            except Exception:
                pass
        return ("", status_code)

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

    try:
        app.config["ADMIN_STAFF_AUDIT_RETENTION_DAYS"] = int(
            os.environ.get("ADMIN_STAFF_AUDIT_RETENTION_DAYS", "90")
        )
    except ValueError:
        app.config["ADMIN_STAFF_AUDIT_RETENTION_DAYS"] = 90
    app.config["ADMIN_STAFF_AUDIT_RETENTION_DAYS"] = max(
        1, min(int(app.config["ADMIN_STAFF_AUDIT_RETENTION_DAYS"]), 3650)
    )

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

    from app.branding_utils import merge_site_settings_defaults

    _DEFAULT_ADMIN_SITE_SETTINGS = merge_site_settings_defaults({})

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
            session["site_settings"] = merge_site_settings_defaults(
                cm.get("site_settings")
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

    # Admin/partials (e.g. sparrow_admin_nav_styles.html) use config.theme_settings; Jinja's
    # `config` is Flask app.config, not the core manifest dict passed to some core routes.
    try:
        _core_m = plugin_manager.get_core_manifest() or {}
        _ts = _core_m.get("theme_settings")
        if not isinstance(_ts, dict):
            _ts = {}
        _ts = dict(_ts)
        _ts.setdefault("theme", "default")
        _ts.setdefault("custom_css_path", "")
        _ts.setdefault("dashboard_background_mode", "slideshow")
        _ts.setdefault("dashboard_background_color", "#1e293b")
        _ts.setdefault("dashboard_background_image_path", "")
        app.config["theme_settings"] = _ts
        from app.organization_profile import normalize_organization_industries

        _op = _core_m.get("organization_profile")
        _inds = (
            (_op or {}).get("industries")
            if isinstance(_op, dict)
            else None
        )
        app.config["organization_industries"] = normalize_organization_industries(_inds)
    except Exception as _theme_err:
        print(f"[WARN] Could not set app.config['theme_settings']: {_theme_err}")
        app.config["theme_settings"] = {
            "theme": "default",
            "custom_css_path": "",
            "dashboard_background_mode": "slideshow",
            "dashboard_background_color": "#1e293b",
            "dashboard_background_image_path": "",
        }
        from app.organization_profile import normalize_organization_industries

        app.config["organization_industries"] = normalize_organization_industries(None)

    plugin_manager.register_admin_routes(app)
    # Public plugin blueprints (e.g. recruitment /vacancies, employee portal paths) — required for url_for from admin templates
    plugin_manager.register_public_routes(app)

    try:
        from app.plugins.hr_module.dbs_scheduler import init_dbs_status_scheduler

        init_dbs_status_scheduler(app)
    except Exception as _dbs_sched_err:
        print(f"[WARN] DBS Update Service scheduler not started: {_dbs_sched_err}")

    try:
        from app.plugins.hr_module.hcpc_scheduler import init_hcpc_status_scheduler

        init_hcpc_status_scheduler(app)
    except Exception as _hcpc_sched_err:
        print(f"[WARN] HCPC scheduler not started: {_hcpc_sched_err}")

    try:
        from app.plugins.hr_module.appraisal_scheduler import (
            init_appraisal_reminder_scheduler,
        )

        init_appraisal_reminder_scheduler(app)
    except Exception as _appr_sched_err:
        print(f"[WARN] HR appraisal reminder scheduler not started: {_appr_sched_err}")

    try:
        from app.plugins.compliance_audit_module.scheduler import (
            init_compliance_audit_scheduler,
        )

        init_compliance_audit_scheduler(app)
    except Exception as _ca_sched_err:
        print(f"[WARN] Compliance audit scheduler not started: {_ca_sched_err}")

    try:
        from app.plugins.time_billing_module.timesheet_rota_sync_scheduler import (
            init_timesheet_rota_sync_scheduler,
        )

        init_timesheet_rota_sync_scheduler(app)
    except Exception as _tb_rota_sched_err:
        print(f"[WARN] Time Billing rota sync scheduler not started: {_tb_rota_sched_err}")

    try:
        from app.plugins.scheduling_module.shift_portal_push_scheduler import (
            init_shift_portal_push_scheduler,
        )

        init_shift_portal_push_scheduler(app)
    except Exception as _sched_push_err:
        print(f"[WARN] Scheduling shift reminder push scheduler not started: {_sched_push_err}")

    _PLUGIN_URL_ACCESS_EXEMPT = frozenset(
        {"settings", "enable", "disable", "install", "uninstall", "install-remote"}
    )
    # JSON APIs under these prefixes authenticate inside the plugin (Bearer JWT, API tokens, etc.);
    # Flask-Login session is often absent. Redirecting anonymous /plugin/… requests to /login causes
    # 302 on health checks, OPTIONS (CORS), and login/token POSTs — breaking mobile/SPA clients.
    #
    # When adding a plugin with agent/JSON endpoints, append ``/plugin/<system_name>/api/`` here and
    # align CORS (``r"/plugin/[^/]+/api.*"``) / CSRF exempt patterns in this file. Session JWTs from
    # ``POST /api/login`` must carry non-empty ``username`` and ``role`` (``app.auth_jwt``).
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
        # Ventus MDT: per-device session after sign-on (duplicate-login prevention).
        "X-MDT-Session-Id",
        # SeaSurf / sparrow_csrf_head.html: credentialed or cross-origin preflights may need these.
        "X-CSRFToken",
        "X-CSRF-Token",
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
        # Use stdout so hosted dashboards (e.g. Railway) do not count this as stderr “errors”.
        print(
            "INFO (create_app): CORS_ALLOWED_ORIGINS is unset in production: permissive CORS (*, no "
            "credentials) for /api* and /plugin/*/api* only. Set CORS_ALLOWED_ORIGINS to explicit "
            "origins when you can.",
            flush=True,
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
            # SeaSurf defaults to strict Referer validation on HTTPS. That runs before token checks and
            # breaks common cases: reverse proxies where url_root/host differs from the public URL,
            # browsers or extensions stripping Referer, and some fetch/form flows without Referer.
            # Token + session validation still protects against CSRF. Opt in to referer checks with
            # CSRF_CHECK_REFERER=1 when your deployment needs that extra constraint.
            _csrf_ref = (os.environ.get("CSRF_CHECK_REFERER") or "").strip().lower()
            if _csrf_ref in ("1", "true", "yes", "on"):
                app.config["CSRF_CHECK_REFERER"] = True
            else:
                app.config["CSRF_CHECK_REFERER"] = False
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
                # Medical Records: EPCR Caldicott flows use fetch() + JSON + session cookie (no CSRF
                # header). Without exempt, SeaSurf returns HTML and the client sees "not valid JSON".
                re.compile(
                    r"^/plugin/medical_records_module/(?:"
                    r"admin/(?:request_epcr_access_code|unlock_epcr_case|withdraw_epcr_access_request)|"
                    r"clinical/(?:review_epcr_access_request|epcr/delete/<int:case_id>)"
                    r")$"
                ),
                # Ventus Response: browser fetch() + JSON + session cookie for /dispatch/* and /response/*
                # (not under /plugin/.../api/). Matches medical EPCR exemption when SeaSurf would otherwise
                # reject POST/DELETE without a valid X-CSRFToken.
                re.compile(r"^/plugin/ventus_response_module/dispatch/"),
                re.compile(r"^/plugin/ventus_response_module/response/"),
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

    # Pop only categories meant for the main admin shell — keeps Sling sync toasts on that page only.
    _ADMIN_SHELL_FLASH_CATEGORIES = (
        "message",
        "success",
        "info",
        "warning",
        "danger",
        "error",
        "plugins_page_success",
        "plugins_page_danger",
        "plugins_page_error",
    )

    @app.template_global()
    def get_shell_flashed_messages(with_categories=True):
        from flask import get_flashed_messages as _gfm

        return _gfm(
            with_categories=with_categories,
            category_filter=list(_ADMIN_SHELL_FLASH_CATEGORIES),
        )

    # Basic rate limiting + stricter caps on authentication surfaces
    if Limiter:
        try:
            _rl_storage = (
                os.environ.get("RATELIMIT_STORAGE_URI")
                or os.environ.get("REDIS_URL")
                or ""
            ).strip()
            # Explicit storage avoids Flask-Limiter UserWarning (“in-memory… not recommended”)
            # when None is passed. Use Redis when REDIS_URL is set (Railway plugin / same as sessions).
            # memory:// is fine for single-instance; multi-instance needs Redis.
            if not _rl_storage:
                _rl_storage = "memory://"
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

            _api_login_lim = os.environ.get("API_LOGIN_RATE_LIMIT", "10 per minute")
            _form_login_lim = os.environ.get("FORM_LOGIN_RATE_LIMIT", "5 per minute")
            _pw_reset_lim = os.environ.get("PASSWORD_RESET_RATE_LIMIT", "3 per hour")
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

            def _epcr_rate_key():
                from flask_login import current_user as _cu

                if getattr(_cu, "is_authenticated", False):
                    _uid = getattr(_cu, "id", None)
                    return f"epcr:{_uid}:{get_remote_address()}"
                return f"epcr:anon:{get_remote_address()}"

            _epcr_unlock_lim = os.environ.get("EPCR_UNLOCK_RATE_LIMIT", "15 per minute")
            _v_epcr_unlock = app.view_functions.get(
                "medical_records_internal.unlock_epcr_case"
            )
            if _v_epcr_unlock:
                limiter.limit(_epcr_unlock_lim, methods=["POST"], key_func=_epcr_rate_key)(
                    _v_epcr_unlock
                )
            _epcr_req_lim = os.environ.get(
                "EPCR_ACCESS_REQUEST_RATE_LIMIT", "20 per minute"
            )
            _v_epcr_req = app.view_functions.get(
                "medical_records_internal.request_epcr_access_code"
            )
            if _v_epcr_req:
                limiter.limit(_epcr_req_lim, methods=["POST"], key_func=_epcr_rate_key)(
                    _v_epcr_req
                )
            _v_epcr_wd = app.view_functions.get(
                "medical_records_internal.withdraw_epcr_access_request"
            )
            if _v_epcr_wd:
                limiter.limit(_epcr_req_lim, methods=["POST"], key_func=_epcr_rate_key)(
                    _v_epcr_wd
                )
            _epcr_rev_lim = os.environ.get(
                "EPCR_ACCESS_REVIEW_RATE_LIMIT", "40 per minute"
            )
            _v_epcr_rev = app.view_functions.get(
                "medical_records_internal.review_epcr_access_request"
            )
            if _v_epcr_rev:
                limiter.limit(_epcr_rev_lim, methods=["POST"], key_func=_epcr_rate_key)(
                    _v_epcr_rev
                )
            _core_set_lim = (
                os.environ.get("CORE_SETTINGS_POST_RATE_LIMIT") or "40 per minute"
            ).strip()
            _v_core_set = app.view_functions.get("routes.core_module_settings")
            if _v_core_set and _core_set_lim:
                limiter.limit(_core_set_lim, methods=["POST"])(_v_core_set)
            for _ep in (
                "routes.core_integrations_settings",
                "routes.core_integrations_oauth_client",
                "routes.core_integrations_disconnect",
                "routes.core_integrations_health",
                "routes.account_whats_new_ack",
            ):
                _v_int = app.view_functions.get(_ep)
                if _v_int and _core_set_lim:
                    limiter.limit(_core_set_lim, methods=["POST"])(_v_int)
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
        app.audit_logger = None
    # Plugins: ``getattr(current_app, "audit_logger", None)`` — log with ``.info(..., extra={"extra": {...}})``
    # only when non-None; there is no separate registration API.
    # ------------------------------------------------------------------
    # Socket.IO initialization (optional Redis message queue)
    # ------------------------------------------------------------------
    try:
        from . import socketio

        # Vendored Socket.IO v4 browser bundle (same line as MDT client / python-socketio 5.12).
        _sio_vendor_dir = os.path.join(app.root_path, "static", "vendor")
        _sio_vendor_name = "socket.io-4.7.5.min.js"
        _sio_vendor_path = os.path.join(_sio_vendor_dir, _sio_vendor_name)

        redis_url = os.environ.get('REDIS_URL') or os.environ.get('REDIS_URLS')
        socketio_opts = {}
        # If a Redis message queue is configured, provide it so SocketIO can
        # scale across processes/instances.
        if redis_url:
            socketio_opts['message_queue'] = redis_url
        async_mode = _resolve_socketio_async_mode()
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

        # Flask-SocketIO replaces app.wsgi_app with middleware that forwards ALL /socket.io/*
        # to Engine.IO before Flask routes run — so @app.route("/socket.io/socket.io.js") never
        # executes. Wrap the stack to serve the client bundle with application/javascript first.
        class _VentusSocketIOBrowserBundleMiddleware:
            __slots__ = ("_inner", "_vendor_path")

            def __init__(self, inner, vendor_abs_path):
                self._inner = inner
                self._vendor_path = vendor_abs_path

            def __call__(self, environ, start_response):
                if environ.get("PATH_INFO") == "/socket.io/socket.io.js":
                    vp = self._vendor_path
                    if os.path.isfile(vp):
                        try:
                            with open(vp, "rb") as _sf:
                                _body = _sf.read()
                        except OSError:
                            pass
                        else:
                            start_response(
                                "200 OK",
                                [
                                    (
                                        "Content-Type",
                                        "application/javascript; charset=utf-8",
                                    ),
                                    ("Cache-Control", "public, max-age=86400"),
                                ],
                            )
                            return [_body]
                return self._inner(environ, start_response)

        if os.path.isfile(_sio_vendor_path):
            app.wsgi_app = _VentusSocketIOBrowserBundleMiddleware(
                app.wsgi_app, _sio_vendor_path
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

            # Panic audio: map JWT MDT socket sid → callsign; WebRTC publish/listen state (in-process)
            _socketio_sid_callsign = {}
            _panic_audio_publishers = {}
            _panic_audio_listener_by_sid = {}
            _panic_audio_listeners_by_cad = {}
            _panic_audio_ice = [{"urls": "stun:stun.l.google.com:19302"}]
            try:
                _ice_raw = (os.environ.get("PANIC_AUDIO_ICE_SERVERS_JSON") or "").strip()
                if _ice_raw:
                    _panic_audio_ice = json.loads(_ice_raw)
            except Exception:
                pass

            @socketio.on('connect')
            def _on_connect(auth=None):
                """
                CAD: Flask-Login session joins panel_user_<id> for targeted panel sync.
                MDT / API clients: same JWT as /api/mdt/* via Socket.IO `auth` or query
                (?token= / ?jwt=) so they receive mdt_event (e.g. callsign_changed).
                Optional callSign/callsign on auth or query joins room mdt_callsign_<CS>
                for dispatch-initiated session updates.
                """
                try:
                    import re as _re
                    from flask import request as _flask_request

                    if getattr(current_user, 'is_authenticated', False):
                        room = _panel_user_room()
                        if room:
                            join_room(room)
                        return
                    auth_d = auth if isinstance(auth, dict) else {}
                    token = (
                        (auth_d.get('token') or auth_d.get('jwt') or auth_d.get('bearer') or '')
                        if auth_d
                        else ''
                    )
                    token = str(token or '').strip()
                    if not token:
                        token = (
                            (_flask_request.args.get('token') or _flask_request.args.get('jwt') or '')
                            .strip()
                        )
                    if token:
                        try:
                            from app.auth_jwt import decode_session_token as _decode_socket_jwt
                            payload = _decode_socket_jwt(token)
                        except Exception:
                            payload = None
                        if payload:
                            uid = payload.get('sub')
                            if uid is not None:
                                join_room(f"mdt_user_{uid}")
                            raw_cs = (
                                auth_d.get('callSign')
                                or auth_d.get('callsign')
                                or _flask_request.args.get('callSign')
                                or _flask_request.args.get('callsign')
                                or ''
                            )
                            cs_clean = _re.sub(
                                r'[^A-Za-z0-9_-]', '', str(raw_cs or '').strip()
                            ).upper()[:64]
                            if cs_clean:
                                join_room(f"mdt_callsign_{cs_clean}")
                                _socketio_sid_callsign[_socketio_request.sid] = cs_clean
                            return
                    disconnect()
                except Exception:
                    disconnect()

            @socketio.on('disconnect')
            def _panic_audio_on_disconnect():
                try:
                    sid = _socketio_request.sid
                    _socketio_sid_callsign.pop(sid, None)
                    for cad, psid in list(_panic_audio_publishers.items()):
                        if psid == sid:
                            _panic_audio_publishers.pop(cad, None)
                            for lsid in list(_panic_audio_listeners_by_cad.pop(cad, set())):
                                _panic_audio_listener_by_sid.pop(lsid, None)
                                emit("panic_audio_ended", {"cad": cad}, room=lsid)
                            return
                    lcad = _panic_audio_listener_by_sid.pop(sid, None)
                    if lcad is not None:
                        s = _panic_audio_listeners_by_cad.get(lcad)
                        if s:
                            s.discard(sid)
                        pub = _panic_audio_publishers.get(lcad)
                        if pub:
                            emit(
                                "panic_audio_listener_left",
                                {"cad": lcad, "listener_sid": sid},
                                room=pub,
                            )
                except Exception:
                    pass

            @socketio.on("panic_audio_publish")
            def _panic_audio_publish(data):
                try:
                    if not isinstance(data, dict):
                        return
                    cad = int(data.get("cad"))
                    cs = str(data.get("callsign") or "").strip().upper()
                    sid_c = _socketio_sid_callsign.get(_socketio_request.sid)
                    if not sid_c or sid_c != cs:
                        emit("panic_audio_error", {"message": "callsign mismatch"})
                        return
                    _panic_audio_publishers[cad] = _socketio_request.sid
                    emit(
                        "panic_audio_published",
                        {"cad": cad, "ice_servers": _panic_audio_ice},
                    )
                except Exception:
                    try:
                        emit("panic_audio_error", {"message": "publish failed"})
                    except Exception:
                        pass

            @socketio.on("panic_audio_listen")
            def _panic_audio_listen(data):
                try:
                    if not getattr(current_user, "is_authenticated", False):
                        emit("panic_audio_listen_failed", {"reason": "auth"})
                        return
                    cad = int((data or {}).get("cad"))
                    pub = _panic_audio_publishers.get(cad)
                    if not pub:
                        emit(
                            "panic_audio_listen_failed",
                            {"cad": cad, "reason": "no_uplink"},
                        )
                        return
                    lsid = _socketio_request.sid
                    _panic_audio_listener_by_sid[lsid] = cad
                    _panic_audio_listeners_by_cad.setdefault(cad, set()).add(lsid)
                    emit(
                        "panic_audio_listen_ok",
                        {
                            "cad": cad,
                            "publisher_sid": pub,
                            "ice_servers": _panic_audio_ice,
                        },
                    )
                    emit(
                        "panic_audio_listener_joined",
                        {
                            "cad": cad,
                            "listener_sid": lsid,
                            "ice_servers": _panic_audio_ice,
                        },
                        room=pub,
                    )
                except Exception:
                    try:
                        emit("panic_audio_listen_failed", {"reason": "server"})
                    except Exception:
                        pass

            @socketio.on("panic_audio_offer")
            def _panic_audio_offer_fwd(d):
                try:
                    if not isinstance(d, dict):
                        return
                    cad = int(d["cad"])
                    to_sid = d.get("to_sid")
                    if _panic_audio_publishers.get(cad) != _socketio_request.sid:
                        return
                    emit("panic_audio_offer", {"cad": cad, "sdp": d.get("sdp")}, room=to_sid)
                except Exception:
                    pass

            @socketio.on("panic_audio_answer")
            def _panic_audio_answer_fwd(d):
                try:
                    if not isinstance(d, dict):
                        return
                    cad = int(d["cad"])
                    to_sid = d.get("to_sid")
                    if _panic_audio_listener_by_sid.get(_socketio_request.sid) != cad:
                        return
                    emit(
                        "panic_audio_answer",
                        {
                            "cad": cad,
                            "sdp": d.get("sdp"),
                            "from_listener_sid": _socketio_request.sid,
                        },
                        room=to_sid,
                    )
                except Exception:
                    pass

            @socketio.on("panic_audio_ice")
            def _panic_audio_ice_fwd(d):
                try:
                    if not isinstance(d, dict):
                        return
                    cad = int(d["cad"])
                    to_sid = d.get("to_sid")
                    cand = d.get("candidate")
                    sid = _socketio_request.sid
                    if _panic_audio_publishers.get(cad) == sid:
                        emit(
                            "panic_audio_ice",
                            {"cad": cad, "candidate": cand, "from_sid": sid},
                            room=to_sid,
                        )
                        return
                    if _panic_audio_listener_by_sid.get(sid) == cad:
                        emit(
                            "panic_audio_ice",
                            {
                                "cad": cad,
                                "candidate": cand,
                                "from_listener_sid": sid,
                            },
                            room=to_sid,
                        )
                except Exception:
                    pass

            @socketio.on("panic_audio_stop")
            def _panic_audio_stop(d):
                try:
                    if not isinstance(d, dict):
                        return
                    cad = int(d.get("cad"))
                    sid = _socketio_request.sid
                    if _panic_audio_publishers.get(cad) == sid:
                        _panic_audio_publishers.pop(cad, None)
                        for lsid in list(_panic_audio_listeners_by_cad.pop(cad, set())):
                            _panic_audio_listener_by_sid.pop(lsid, None)
                            emit("panic_audio_ended", {"cad": cad}, room=lsid)
                        return
                    if _panic_audio_listener_by_sid.get(sid) == cad:
                        s = _panic_audio_listeners_by_cad.get(cad)
                        if s:
                            s.discard(sid)
                        _panic_audio_listener_by_sid.pop(sid, None)
                        pub = _panic_audio_publishers.get(cad)
                        if pub:
                            emit(
                                "panic_audio_listener_left",
                                {"cad": cad, "listener_sid": sid},
                                room=pub,
                            )
                except Exception:
                    pass

            @socketio.on("panic_voice_recording_finalize")
            def _panic_voice_recording_finalize(data):
                """CAD (Flask-Login): seal current panic mic archive segment on MDT; uplink stays live."""
                try:
                    if not getattr(current_user, "is_authenticated", False):
                        emit("panic_voice_finalize_failed", {"reason": "auth"})
                        return
                    if not isinstance(data, dict):
                        emit("panic_voice_finalize_failed", {"reason": "bad_request"})
                        return
                    cad = int(data.get("cad"))
                    pub = _panic_audio_publishers.get(cad)
                    if not pub:
                        emit(
                            "panic_voice_finalize_failed",
                            {"cad": cad, "reason": "no_uplink"},
                        )
                        return
                    emit("panic_voice_finalize_segment", {"cad": cad}, room=pub)
                    emit("panic_voice_finalize_ok", {"cad": cad})
                except Exception:
                    try:
                        emit("panic_voice_finalize_failed", {"reason": "server"})
                    except Exception:
                        pass

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
        Also includes kit consumable expiry lines on serial assets unless
        FLEET_REMINDER_SKIP_KIT_CONSUMABLES=1.
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

    @app.cli.command("inventory-kit-consumable-alerts")
    def inventory_kit_consumable_alerts_command():
        """
        Email kit consumable expiry alerts only (dated lines on serial equipment).
        Recipients: INVENTORY_KIT_CONSUMABLE_ALERT_EMAILS, else FLEET_ASSET_REMINDER_EMAILS,
        else admin/superuser emails. Disabled when INVENTORY_KIT_CONSUMABLE_ALERTS_DISABLED=1.
        """
        try:
            from app.plugins.inventory_control.kit_consumable_alerts import (
                run_kit_consumable_alerts,
            )
        except ImportError as e:
            print(f"[ERROR] inventory_control.kit_consumable_alerts not available: {e}")
            return
        out = run_kit_consumable_alerts(send_email=True)
        print(out.get("message", out))
        if out.get("error"):
            print(f"[WARN] {out['error']}")
        if out.get("skipped"):
            print("[OK] Skipped (disabled).")
        elif out.get("sent"):
            print("[OK] Email sent.")
        elif not out.get("lines"):
            print("[OK] Nothing to send.")

    @app.cli.command("hr-appraisal-reminders")
    def hr_appraisal_reminders_command():
        """
        Run the daily appraisal reminder scan once (logs + optional digest email).
        Respects HR_APPRAISAL_REMINDERS_DISABLED and manifest appraisal_digest_email_enabled.
        """
        try:
            from app.plugins.hr_module.appraisals_reminders import (
                run_appraisal_daily_reminder_scan,
            )
        except ImportError as e:
            print(f"[ERROR] hr_module appraisals_reminders not available: {e}")
            return
        out = run_appraisal_daily_reminder_scan(send_email=True)
        print(out)
        if out.get("skip") == "already_ran_today":
            print("[OK] Today's scan already ran — no duplicate.")
        elif out.get("email_sent"):
            print("[OK] Digest email sent.")
        elif out.get("items", 0) == 0:
            print("[OK] Nothing due.")

    @app.cli.command("hr-appraisal-reminders-now")
    def hr_appraisal_reminders_now_command():
        """Same as hr-appraisal-reminders but forces another scan today (extra log + email if due)."""
        try:
            from app.plugins.hr_module.appraisals_reminders import (
                run_appraisal_daily_reminder_scan,
            )
        except ImportError as e:
            print(f"[ERROR] hr_module appraisals_reminders not available: {e}")
            return
        out = run_appraisal_daily_reminder_scan(send_email=True, force=True)
        print(out)

    try:
        from app import admin_staff_audit as _admin_staff_audit

        @app.after_request
        def _admin_staff_audit_after(response):
            return _admin_staff_audit.after_request_record(response)
    except Exception as _asa_err:
        app.logger.warning("admin_staff_audit hook not registered: %s", _asa_err)

    try:
        from app.schema_upgrade_recovery import register_schema_upgrade_recovery

        register_schema_upgrade_recovery(app)
    except Exception as _sur_exc:
        app.logger.warning("schema_upgrade_recovery not registered: %s", _sur_exc)

    @app.errorhandler(DatabaseTemporarilyUnavailable)
    def _database_temporarily_unavailable(exc):
        """Cold start / DB still waking (e.g. Railway): avoid raw MySQL tracebacks."""
        from flask import jsonify, render_template, request

        path = request.path or ""
        if path.startswith("/api/") or "/api/" in path:
            return jsonify(
                {
                    "error": "service_unavailable",
                    "message": (
                        "The database is starting up. Please retry in a few seconds."
                    ),
                }
            ), 503
        accept = (request.headers.get("Accept") or "").lower()
        if "application/json" in accept and "text/html" not in accept:
            return jsonify(
                {
                    "error": "service_unavailable",
                    "message": (
                        "The database is starting up. Please retry in a few seconds."
                    ),
                }
            ), 503
        return render_template(
            "database_warming.html",
            detail=(str(exc) if app.debug else None),
        ), 503

    if _railway_or_stdout_access_logs():
        _configure_werkzeug_access_log_stdout()

    try:
        from app.support_access import ensure_support_access_schema

        ensure_support_access_schema()
    except Exception as _sa_exc:
        print(f"[WARN] support_access schema ensure failed (will retry on use): {_sa_exc}")

    try:
        from app.user_notification_preferences import ensure_notification_preferences_schema

        ensure_notification_preferences_schema()
    except Exception as _np_exc:
        print(f"[WARN] notification_preferences schema ensure failed: {_np_exc}")

    try:
        from app.user_whats_new import ensure_whats_new_schema

        ensure_whats_new_schema()
    except Exception as _wn_exc:
        print(f"[WARN] whats_new schema ensure failed: {_wn_exc}")

    return app
