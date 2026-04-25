"""
app/routes.py

Core routes for Sparrow ERP.

Sections:
  1. Authentication & Password Reset
      - /login
      - /reset-password and /reset-password/<token>
  2. Dashboard, Logout & Account
      - / (dashboard)
      - /logout
      - /account/personal-pin -> Set or change own 6-digit personal PIN (not manageable by admins)
      - /account/notifications -> Email notification preferences (recruitment / HR where applicable)
      - /account/whats-new/ack -> Acknowledge release notes (What's new modal on core dashboard only)
  3. User Management (Admin Only)
      - /users           -> Combined management page (live search + modals for add/edit/delete)
      - /users/search    -> AJAX endpoint for live search
      - /users/add       -> Process adding a new user (includes first_name and last_name; personal PIN via /account/personal-pin only)
      - /users/edit/<user_id>  -> Process editing a user (updates email, first_name, last_name, role, permissions, and optionally password; never personal PIN)
      - /users/delete/<user_id> -> Delete a user
  4. Version Management (Admin Only)
      - /version         -> View/apply updates
  5. Core Module Settings (Admin Only)
      - /core/settings   -> Tabbed: manifest (branding, theme, AI model id) + .env (SMTP, LLM, security)
  5b. Core Integrations (Admin Only)
      - /core/integrations -> Xero / FreeAgent OAuth, health, deployment defaults (see PRD core-integrations-accounting-prd.md)
  6. SMTP/Email Configuration (Admin Only)
      - Core settings Email tab persists SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD,
        SMTP_USE_SSL, SMTP_USE_TLS (via connection mode: STARTTLS / implicit SSL / plain),
        SMTP_TIMEOUT, SMTP_FROM, SMTP_FROM_NAME to app/config/.env (same keys as EmailManager).
      - /smtp-config     -> Legacy; redirects to /core/settings?tab=email
  7. Plugin Management (Admin Only)
      - /plugins         -> Manage plugins (listing, enabling/disabling, etc.)
      - /plugin/<plugin_system_name>/settings
      - /plugin/<plugin_system_name>/enable
      - /plugin/<plugin_system_name>/disable
      - /plugin/<plugin_system_name>/install
      - /plugin/<plugin_system_name>/uninstall
  8. Static File Serving for Plugins
      - /plugins/<plugin_name>/<filename>

Most privileged routes use admin_only() (includes time-limited vendor support role).
Customer-only surfaces (users, core settings, version, plugin install) use customer_super_admin_only()
(admin or superuser, including time-limited vendor support when logged in as the shadow superuser).
The core manifest is passed as "config" to all templates.
"""

from app.ai_config import (
    CHAT_MODEL_DROPDOWN_CHOICES,
    chat_model_dropdown_value_set,
    sanitize_chat_model_id,
)
from app.create_app import update_env_var
from app.branding_utils import (
    apply_logo_dimensions_to_site_settings,
    merge_site_settings_defaults,
)
from app.organization_profile import INDUSTRY_OPTIONS, normalize_organization_industries
from app.static_upload_paths import normalize_manifest_static_path
from app.storage_paths import get_persistent_data_root, get_persistent_smtp_env_path
from app.auth_jwt import encode_session_token
from app.compliance_audit import log_security_event, pseudonymous_username_hint
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
from flask_login import login_user, logout_user, current_user, login_required
from app.objects import *
from werkzeug.utils import secure_filename
from flask import (
    Blueprint, current_app, render_template, request, redirect, url_for,
    session, flash, jsonify, send_from_directory, abort
)
import re
import uuid
from collections import defaultdict
import sys
import os
import json
import threading
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Constants and folder paths
_APP_PKG_DIR = os.path.dirname(os.path.abspath(__file__))
PLUGINS_FOLDER = os.path.join(_APP_PKG_DIR, "plugins")
# Under Docker/Railway this path is symlinked to the volume — see
# app.storage_paths.get_persistent_data_root() (RAILWAY_VOLUME_MOUNT_PATH=/volume, etc.).
STATIC_UPLOAD_FOLDER = os.path.join(_APP_PKG_DIR, "static", "uploads")
ALLOWED_LOGO_EXTENSIONS = {'png', 'jpg', 'jpeg', 'svg'}
ALLOWED_DASHBOARD_BG_EXTENSIONS = ALLOWED_LOGO_EXTENSIONS | {'webp'}
ALLOWED_CSS_EXTENSIONS = {'css'}

os.makedirs(STATIC_UPLOAD_FOLDER, exist_ok=True)

_DASH_BG_MODE_OK = frozenset({"slideshow", "solid", "image"})


def _sanitize_dashboard_background_mode(raw) -> str:
    v = (raw or "slideshow").strip().lower()
    return v if v in _DASH_BG_MODE_OK else "slideshow"


def _sanitize_dashboard_background_color(raw, fallback: str) -> str:
    s = (raw or "").strip()
    if re.match(r"^#[0-9A-Fa-f]{3}$", s) or re.match(
        r"^#[0-9A-Fa-f]{6}$", s
    ) or re.match(r"^#[0-9A-Fa-f]{8}$", s):
        return s
    fb = (fallback or "").strip()
    if re.match(r"^#[0-9A-Fa-f]{3}$", fb) or re.match(
        r"^#[0-9A-Fa-f]{6}$", fb
    ) or re.match(r"^#[0-9A-Fa-f]{8}$", fb):
        return fb
    return "#1e293b"


def _env_nonempty(key: str) -> bool:
    return bool((os.environ.get(key) or "").strip())


def _smtp_connection_mode_for_ui() -> str:
    """
    Values: starttls | ssl | plain — aligned with EmailManager (objects.py) defaults.
    """
    ssl_raw = (os.environ.get("SMTP_USE_SSL") or "").strip().lower()
    if ssl_raw in ("1", "true", "yes"):
        return "ssl"
    if ssl_raw in ("0", "false", "no"):
        use_tls = (os.environ.get("SMTP_USE_TLS", "true")
                   or "true").lower() == "true"
        return "starttls" if use_tls else "plain"
    try:
        p = int((os.environ.get("SMTP_PORT") or "587").strip() or "587")
        if p == 465:
            return "ssl"
    except ValueError:
        pass
    use_tls = (os.environ.get("SMTP_USE_TLS", "true")
               or "true").lower() == "true"
    return "starttls" if use_tls else "plain"


def _settings_env_context():
    """Snapshot for Core Settings UI (.env-backed fields shown on each tab)."""
    _mode = _smtp_connection_mode_for_ui()
    _use_tls = _mode == "starttls"
    return {
        "smtp": {
            "host": os.environ.get("SMTP_HOST", "") or "",
            "port": os.environ.get("SMTP_PORT", "") or "",
            "username": os.environ.get("SMTP_USERNAME", "") or "",
            "password_set": _env_nonempty("SMTP_PASSWORD"),
            "use_tls": _use_tls,
            "connection_mode": _mode,
            "timeout": (os.environ.get("SMTP_TIMEOUT") or "30").strip() or "30",
            "from_email": (os.environ.get("SMTP_FROM") or "").strip(),
            "from_name": (os.environ.get("SMTP_FROM_NAME") or "").strip(),
            "stored_on_volume": bool(get_persistent_smtp_env_path()),
        },
        "llm": {
            "openai_key_set": _env_nonempty("OPENAI_API_KEY"),
            "sparrow_key_set": _env_nonempty("SPARROW_OPENAI_API_KEY"),
            "gemini_key_set": _env_nonempty("GEMINI_API_KEY"),
            "base_url": os.environ.get("OPENAI_BASE_URL", "") or "",
            "preset": (os.environ.get("OPENAI_BASE_URL_PRESET") or "").strip(),
            "model": os.environ.get("OPENAI_MODEL", "") or "",
            "openrouter_referer": os.environ.get("OPENROUTER_HTTP_REFERER")
            or os.environ.get("OPENROUTER_SITE_URL")
            or "",
            "openrouter_title": os.environ.get("OPENROUTER_SITE_TITLE")
            or os.environ.get("OPENROUTER_APP_NAME")
            or "",
        },
        "security": {
            "secret_set": _env_nonempty("SECRET_KEY"),
            "jwt_set": _env_nonempty("JWT_SECRET_KEY"),
        },
        "persistent_storage": {
            "configured": bool(get_persistent_data_root()),
        },
    }


def _update_secret_env_if_nonempty(key: str, form_value) -> bool:
    v = (form_value or "").strip()
    if not v:
        return False
    update_env_var(key, v)
    return True


def _save_smtp_from_form(form) -> None:
    def _g(*keys, default=""):
        for k in keys:
            v = (form.get(k) or "").strip()
            if v:
                return v
        return default

    update_env_var("SMTP_HOST", _g("smtp_host", "host"))
    update_env_var("SMTP_PORT", _g("smtp_port", "port") or "587")
    update_env_var("SMTP_USERNAME", _g("smtp_username", "username"))
    pw = _g("smtp_password", "password")
    if pw:
        update_env_var("SMTP_PASSWORD", pw)

    mode = (form.get("smtp_connection_mode") or "").strip().lower()
    if mode not in ("starttls", "ssl", "plain"):
        # Legacy single checkbox from older forms
        ut = (form.get("smtp_use_tls") or form.get("use_tls") or "true").lower()
        mode = "starttls" if ut == "true" else "plain"
    if mode == "ssl":
        update_env_var("SMTP_USE_SSL", "true")
        update_env_var("SMTP_USE_TLS", "false")
    elif mode == "plain":
        update_env_var("SMTP_USE_SSL", "false")
        update_env_var("SMTP_USE_TLS", "false")
    else:
        update_env_var("SMTP_USE_SSL", "false")
        update_env_var("SMTP_USE_TLS", "true")

    try:
        to = int((form.get("smtp_timeout") or "30").strip() or "30")
        to = max(5, min(to, 300))
    except ValueError:
        to = 30
    update_env_var("SMTP_TIMEOUT", str(to))

    update_env_var("SMTP_FROM", (form.get("smtp_from") or "").strip())
    update_env_var("SMTP_FROM_NAME",
                   (form.get("smtp_from_name") or "").strip())


# Create blueprint for core routes
routes = Blueprint('routes', __name__)


@routes.route('/plugin/ventus_response_module/jobs/history', methods=['GET'])
@routes.route('/plugin/ventus_response_module/history', methods=['GET'])
@login_required
def ventus_history_compat():
    """
    Compatibility history endpoint when plugin route registration is stale.
    Mirrors plugin jobs/history: dispatch division scope + same SQL shape so any
    logged-in ERP user cannot read all cleared incidents org-wide.
    """
    try:
        from app.plugins.ventus_response_module.routes import (
            _request_division_scope,
            _enforce_dispatch_scope,
            _extract_job_division,
        )
    except ImportError:
        return jsonify({"error": "Ventus module not available"}), 503

    selected_division, include_external = _request_division_scope()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        selected_division, include_external, _ = _enforce_dispatch_scope(
            cur, selected_division, include_external
        )
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'claimedBy'")
        has_claimed_by = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
        has_updated_at = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'data'")
        has_data = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_division = cur.fetchone() is not None

        claimed_by_sql = "claimedBy" if has_claimed_by else "NULL AS claimedBy"
        updated_at_sql = "updated_at" if has_updated_at else "NULL AS updated_at"
        data_sql = "data" if has_data else "NULL AS data"
        division_sql = (
            "LOWER(TRIM(COALESCE(division, 'general'))) AS division"
            if has_division
            else "'general' AS division"
        )
        order_by_sql = "updated_at DESC" if has_updated_at else "created_at DESC"

        sql = f"""
            SELECT cad,
                   TRIM(COALESCE(status, '')) AS status,
                   {data_sql},
                   created_at,
                   {updated_at_sql},
                   {claimed_by_sql},
                   {division_sql}
            FROM mdt_jobs
            WHERE LOWER(TRIM(COALESCE(status, ''))) = 'cleared'
        """
        args = []
        if selected_division and not include_external:
            if has_division:
                sql += " AND LOWER(TRIM(COALESCE(division, 'general'))) = %s"
                args.append(selected_division)
            else:
                if selected_division != "general":
                    return jsonify([])
        sql += f" ORDER BY {order_by_sql} LIMIT 500"
        cur.execute(sql, tuple(args))
        jobs = cur.fetchall()

        for job in jobs:
            payload = job.get("data")
            reason_for_call = None
            chief_complaint = None
            outcome = None
            lat = None
            lng = None

            try:
                if isinstance(payload, (bytes, bytearray)):
                    payload = payload.decode("utf-8", errors="ignore")
                if isinstance(payload, str):
                    payload = json.loads(payload) if payload else {}
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}

            try:
                reason_for_call = payload.get("reason_for_call")
                chief_complaint = payload.get("chief_complaint")
                outcome = payload.get("outcome")
                coords = payload.get("coordinates") or {}
                if isinstance(coords, dict):
                    lat = coords.get("lat")
                    lng = coords.get("lng")
            except Exception:
                pass

            try:
                lat = float(lat) if lat is not None else None
                lng = float(lng) if lng is not None else None
            except Exception:
                lat = None
                lng = None

            job["reason_for_call"] = reason_for_call
            job["chief_complaint"] = chief_complaint
            job["outcome"] = outcome
            job["lat"] = lat
            job["lng"] = lng
            job["completedAt"] = job.get("updated_at")
            job_division = _extract_job_division(
                payload, fallback=job.get("division") or "general"
            )
            job["division"] = job_division
            job["is_external"] = bool(
                selected_division and job_division != selected_division
            )
            job.pop("data", None)

        return jsonify(jobs)
    except Exception:
        return jsonify([])
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

##############################################################################
# Restart scheduling helper (touch restart.flag after response)
##############################################################################


_restart_lock = threading.Lock()
_restart_scheduled = False


def schedule_restart_flag(config_dir: str, reason: str = "", delay_seconds: float = 0.75) -> None:
    """
    Schedules a restart by touching app/config/restart.flag after a short delay.
    - Short-lived daemon thread (ends after it runs).
    - Debounced so multiple rapid actions don't spawn many threads.
    """
    global _restart_scheduled

    if not config_dir:
        return

    with _restart_lock:
        if _restart_scheduled:
            return
        _restart_scheduled = True

    def _go():
        global _restart_scheduled
        try:
            time.sleep(float(delay_seconds))
            flag = os.path.join(config_dir, "restart.flag")
            os.makedirs(os.path.dirname(flag), exist_ok=True)
            with open(flag, "a", encoding="utf-8") as f:
                f.write(f"{time.time()} {reason}\n")
        except Exception as e:
            print(f"[WARN] Failed to touch restart.flag: {e}")
        finally:
            with _restart_lock:
                _restart_scheduled = False

    threading.Thread(target=_go, daemon=True).start()


##############################################################################
# SECTION 1: Authentication & Password Reset
##############################################################################


def _safe_post_login_next(raw):
    """
    Allow only same-origin relative paths (open-redirect safe).
    Used after login when ?next= or form next= was set (e.g. plugin access gate).
    """
    if raw is None:
        return None
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s or not s.startswith("/") or s.startswith("//"):
        return None
    if "\n" in s or "\r" in s or "\t" in s:
        return None
    if "://" in s or s.lower().startswith("javascript:"):
        return None
    if "@" in s.split("/")[0]:
        return None
    return s


@routes.route('/login', methods=['GET', 'POST'])
def login():
    """
    Authenticate users via username and password.
    Loads site settings from the core manifest.
    On success, stores user data (including first_name and last_name) in session,
    updates the last_login timestamp, and redirects to the dashboard.
    """
    if current_user.is_authenticated:
        nxt = _safe_post_login_next(request.args.get("next", ""))
        if nxt:
            return redirect(nxt)
        return redirect(url_for('routes.dashboard'))

    plugin_manager = PluginManager(PLUGINS_FOLDER)
    core_manifest = plugin_manager.get_core_manifest() or {}

    site_settings = merge_site_settings_defaults(core_manifest.get("site_settings"))

    session['site_settings'] = site_settings
    session['core_manifest'] = core_manifest

    # Preserve ?next= across failed login (hidden field on POST).
    login_next_value = _safe_post_login_next(
        request.form.get("next", "") or request.args.get("next", "")
    ) or ""

    if request.method == 'POST':
        try:
            username = request.form.get('username', '').strip()
            password = request.form.get('password', '')

            user_data = User.get_user_for_web_login(username)
            if user_data and AuthManager.verify_password(user_data["password_hash"], password):
                from app.support_access import support_login_blocked_reason

                _sup_err = support_login_blocked_reason(user_data)
                if _sup_err:
                    flash(_sup_err, "error")
                    return render_template(
                        "login.html",
                        site_settings=site_settings,
                        config=core_manifest,
                        login_next=login_next_value,
                    )
                permissions = []
                if user_data.get('permissions'):
                    try:
                        permissions = json.loads(user_data['permissions'])
                    except Exception:
                        permissions = []

                user = User(
                    user_data['id'],
                    user_data['username'],
                    user_data['email'],
                    user_data['role'],
                    permissions
                )
                remember = request.form.get('remember') == 'on'
                login_user(user, remember=remember)

                log_security_event(
                    "session_login_success",
                    user_id=user_data.get("id"),
                    role=user_data.get("role"),
                )
                try:
                    from app.plugins.compliance_audit_module.login_audit_hook import (
                        record_compliance_login,
                    )

                    record_compliance_login(
                        success=True,
                        channel="web_form",
                        user_id=str(user_data.get("id")),
                        username=user_data.get("username"),
                        ip=request.remote_addr,
                        user_agent=request.headers.get("User-Agent"),
                    )
                except Exception:
                    pass
                if int(user_data.get("billable_exempt") or 0):
                    session["support_shadow"] = True
                    log_security_event(
                        "support_shadow_session_login",
                        user_id=user_data.get("id"),
                        username=user_data.get("username"),
                    )
                else:
                    session.pop("support_shadow", None)

                session['first_name'] = user_data.get('first_name', '')
                session['last_name'] = user_data.get('last_name', '')
                session['role'] = user_data.get('role', '')
                session['theme'] = user_data.get('theme', 'default')

                # Update last_login timestamp
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE users SET last_login = %s WHERE id = %s",
                    (datetime.now(), user_data['id'])
                )
                conn.commit()
                cursor.close()
                conn.close()

                if user_data.get('first_name') and user_data.get('last_name'):
                    site_settings['user_name'] = f"{user_data['first_name']} {user_data['last_name']}"

                flash(
                    f"Welcome back, {user_data.get('first_name', user_data['username'])}!", 'success')

                if (user_data.get("role") or "").lower() == "crew":
                    return redirect('/plugin/ventus_response_module/response')

                if login_next_value:
                    return redirect(login_next_value)

                return redirect(url_for('routes.dashboard'))

            try:
                from app.plugins.compliance_audit_module.login_audit_hook import (
                    record_compliance_login,
                )

                record_compliance_login(
                    success=False,
                    channel="web_form",
                    username=username,
                    ip=request.remote_addr,
                    user_agent=request.headers.get("User-Agent"),
                )
            except Exception:
                pass
            flash(
                'Invalid username or password. Please check your credentials and try again.', 'error')
        except Exception:
            flash('An unexpected error occurred. Please try again later.', 'error')

    return render_template(
        'login.html',
        site_settings=site_settings,
        config=core_manifest,
        login_next=login_next_value,
    )


def generate_reset_token(email):
    secret_key = os.environ.get('SECRET_KEY')
    serializer = URLSafeTimedSerializer(secret_key)
    return serializer.dumps(email, salt='password-reset-salt')


def verify_reset_token(token, expiration=3600):
    secret_key = os.environ.get('SECRET_KEY')
    serializer = URLSafeTimedSerializer(secret_key)
    try:
        email = serializer.loads(
            token, salt='password-reset-salt', max_age=expiration)
    except (SignatureExpired, BadSignature):
        return None
    return email


@routes.route('/reset-password', methods=['GET', 'POST'])
def reset_password_request():
    """
    Password reset request: user submits email to receive a reset link.

    Behaviour:
    - If the email exists, generate a token + email the reset link via EmailManager.
    - Always show the same message to avoid user enumeration.
    - Simple session throttle: 1 request per 60 seconds.
    """
    if request.method == 'POST':
        email = request.form.get('email', '').strip()

        # Throttle (per session)
        now = time.time()
        last = float(session.get("pw_reset_last_ts", 0) or 0)
        if (now - last) < 60:
            flash(
                "Please wait a moment before requesting another reset email.", "warning")
            return redirect(url_for('routes.login'))
        session["pw_reset_last_ts"] = now

        log_security_event("password_reset_requested")

        # Always show the same message (prevents account discovery)
        flash("If an account exists for that email, a password reset link has been sent.", "info")

        user_data = User.get_user_by_email(email)
        if user_data and not int(user_data.get("billable_exempt") or 0):
            try:
                token = generate_reset_token(email)
                reset_link = url_for(
                    'routes.reset_password', token=token, _external=True)

                plugin_manager = PluginManager(PLUGINS_FOLDER)
                core_manifest = plugin_manager.get_core_manifest() or {}
                company_name = (core_manifest.get("site_settings") or {}).get(
                    "company_name") or "Sparrow ERP"

                subject = f"Reset your {company_name} password"

                text_body = (
                    f"A password reset was requested for your {company_name} account.\n\n"
                    f"Reset your password using this link:\n{reset_link}\n\n"
                    "If you did not request this, you can ignore this email."
                )

                html_body = f"""
                <p>A password reset was requested for your <strong>{company_name}</strong> account.</p>
                <p><a href="{reset_link}">Click here to reset your password</a></p>
                <p>If you did not request this, you can ignore this email.</p>
                """

                EmailManager().send_email(
                    subject=subject,
                    body=text_body,
                    recipients=[email],
                    html_body=html_body
                )

            except Exception as e:
                print(f"[ERROR] Failed to send reset email to {email}: {e}")

        return redirect(url_for('routes.login'))

    plugin_manager = PluginManager(PLUGINS_FOLDER)
    core_manifest = plugin_manager.get_core_manifest() or {}
    _ss = merge_site_settings_defaults(core_manifest.get("site_settings"))
    return render_template(
        "reset_password_request.html",
        config=core_manifest,
        site_settings=_ss,
    )


@routes.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    plugin_manager = PluginManager(PLUGINS_FOLDER)
    core_manifest = plugin_manager.get_core_manifest() or {}

    email = verify_reset_token(token)
    if not email:
        flash("The password reset link is invalid or has expired.", "danger")
        return redirect(url_for('routes.reset_password_request'))

    _pw_user = User.get_user_by_email(email)
    if _pw_user and int(_pw_user.get("billable_exempt") or 0):
        flash("The password reset link is invalid or has expired.", "danger")
        return redirect(url_for('routes.reset_password_request'))

    if request.method == 'POST':
        new_password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')

        if new_password != confirm_password:
            flash("Passwords do not match.", "danger")
            return render_template(
                "reset_password.html",
                token=token,
                config=core_manifest,
                site_settings=merge_site_settings_defaults(
                    core_manifest.get("site_settings")
                ),
            )

        user_data = User.get_user_by_email(email)
        if user_data and int(user_data.get("billable_exempt") or 0):
            flash("The password reset link is invalid or has expired.", "danger")
            return redirect(url_for('routes.reset_password_request'))
        if user_data:
            new_hash = AuthManager.hash_password(new_password)
            User.update_password(user_data['id'], new_hash)
            log_security_event(
                "password_reset_completed", user_id=user_data.get("id")
            )
            flash("Your password has been updated successfully!", "success")
            return redirect(url_for('routes.login'))

        flash("User not found.", "danger")
        return redirect(url_for('routes.reset_password_request'))

    return render_template(
        "reset_password.html",
        token=token,
        config=core_manifest,
        site_settings=merge_site_settings_defaults(
            core_manifest.get("site_settings")
        ),
    )


##############################################################################
# SECTION 2: Dashboard & Logout
##############################################################################

@routes.route('/')
@login_required
def dashboard():
    user_info = {
        'username': current_user.username,
        'first_name': session.get('first_name', 'Guest'),
        'last_name': session.get('last_name', 'User'),
        'theme': session.get('theme', 'default')
    }

    plugin_manager = PluginManager(PLUGINS_FOLDER)
    plugins = plugin_manager.get_enabled_plugins() or []
    plugin_manager.plugins = plugin_manager.load_plugins() or {}

    from app.permissions_registry import user_can_access_plugin

    def _manifest_for_system(sn: str):
        for folder, m in plugin_manager.plugins.items():
            if not isinstance(m, dict):
                continue
            key = (m.get("system_name") or folder or "").strip()
            if key == sn or folder == sn:
                return m
        return {}

    plugins = [
        p
        for p in plugins
        if user_can_access_plugin(
            current_user, _manifest_for_system(
                p.get("system_name", "")), p.get("system_name", "")
        )
    ]

    core_manifest = plugin_manager.get_core_manifest() or {}
    core_version = core_manifest.get('version', '0.0.1')

    by_cat = defaultdict(list)
    for p in plugins:
        by_cat[p.get('dashboard_category') or 'Modules'].append(p)
    plugin_categories = sorted(by_cat.items(), key=lambda kv: kv[0].lower())

    from app.user_whats_new import build_whats_new_template_context

    _wn_ctx = build_whats_new_template_context()
    return render_template(
        'dashboard.html',
        plugins=plugins,
        plugin_categories=plugin_categories,
        config=core_manifest,
        user=user_info,
        core_version=core_version,
        **_wn_ctx,
    )


@routes.route('/logout')
@login_required
def logout():
    try:
        log_security_event("session_logout", user_id=getattr(
            current_user, "id", None))
    except Exception:
        pass
    try:
        session.pop("support_shadow", None)
    except Exception:
        pass
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('routes.login'))


def _personal_pin_format_ok(pin: str) -> tuple:
    s = (pin or "").strip()
    if len(s) != 6 or not s.isdigit():
        return False, "Personal PIN must be exactly 6 digits."
    return True, ""


@routes.route('/account/personal-pin', methods=['GET', 'POST'])
@login_required
def account_personal_pin():
    """Only the signed-in user may set or change their personal PIN (never via user management)."""
    uid = str(getattr(current_user, "id", "") or "")
    if not uid:
        flash("Could not determine your account.", "danger")
        return redirect(url_for("routes.dashboard"))

    if request.method == "POST":
        current_pin = (request.form.get("current_personal_pin") or "").strip()
        new_pin = (request.form.get("new_personal_pin") or "").strip()
        confirm = (request.form.get("confirm_personal_pin") or "").strip()

        ok, err = _personal_pin_format_ok(new_pin)
        if not ok:
            flash(err, "danger")
            return redirect(url_for("routes.account_personal_pin"))
        if new_pin != confirm:
            flash("New PIN and confirmation do not match.", "danger")
            return redirect(url_for("routes.account_personal_pin"))

        u = User.get_user_by_id(uid)
        if not u:
            flash("Could not load your account.", "danger")
            return redirect(url_for("routes.dashboard"))

        ph = u.personal_pin_hash
        has_pin = bool(ph and str(ph).strip())

        if has_pin:
            if not current_pin:
                flash("Enter your current personal PIN to change it.", "danger")
                return redirect(url_for("routes.account_personal_pin"))
            if not AuthManager.verify_password(ph, current_pin):
                flash("Current personal PIN is incorrect.", "danger")
                return redirect(url_for("routes.account_personal_pin"))

        new_hash = AuthManager.hash_password(new_pin)
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET personal_pin_hash = %s WHERE id = %s",
            (new_hash, uid),
        )
        conn.commit()
        cursor.close()
        conn.close()

        log_security_event(
            "personal_pin_updated",
            user_id=uid,
            change_type="initial_set" if not has_pin else "changed",
            via="account_personal_pin",
        )

        session.pop("crew_pin_verified_at", None)
        session.pop("safeguarding_oversight_pin_verified_at", None)

        flash("Your personal PIN has been updated.", "success")
        return redirect(url_for("routes.account_personal_pin"))

    u = User.get_user_by_id(uid)
    pin_is_set = bool(
        u and u.personal_pin_hash and str(u.personal_pin_hash).strip()
    )
    return render_template(
        "account_personal_pin.html",
        personal_pin_is_set=pin_is_set,
    )


@routes.route("/account/notifications", methods=["GET", "POST"])
@login_required
def account_notification_settings():
    """Opt in/out of internal recruitment and HR-related emails (requires SMTP; respects module access)."""
    from app.plugins.recruitment_module.notifications import (
        user_qualifies_for_recruitment_internal_list,
    )
    from app.user_notification_preferences import (
        HR_CONTRACTOR_EMAIL_KINDS,
        RECRUITMENT_INTERNAL_EMAIL_KINDS,
        HR_CONTRACTOR_KIND_LABELS,
        RECRUITMENT_INTERNAL_KIND_LABELS,
        get_notification_preferences_for_user,
        save_notification_preferences_for_user,
        user_qualifies_for_hr_staff_context,
    )

    uid = str(getattr(current_user, "id", "") or "")
    if not uid:
        flash("Could not determine your account.", "danger")
        return redirect(url_for("routes.dashboard"))

    show_rec = user_qualifies_for_recruitment_internal_list(current_user)
    linked_cid = None
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT contractor_id FROM users WHERE id = %s LIMIT 1", (uid,))
        row = cur.fetchone() or {}
        linked_cid = row.get("contractor_id")
    finally:
        cur.close()
        conn.close()
    show_hr_doc = bool(
        linked_cid and user_qualifies_for_hr_staff_context(current_user))

    if request.method == "POST":
        prefs = get_notification_preferences_for_user(uid)
        if show_rec:
            for k in RECRUITMENT_INTERNAL_EMAIL_KINDS:
                prefs["email"]["recruitment_internal"][k] = (
                    request.form.get(f"rec_int_{k}") == "1"
                )
        if show_hr_doc:
            for k in HR_CONTRACTOR_EMAIL_KINDS:
                prefs["email"]["hr_contractor"][k] = (
                    request.form.get(f"hr_co_{k}") == "1"
                )
        if save_notification_preferences_for_user(uid, prefs):
            flash("Notification preferences saved.", "success")
        else:
            flash("Could not save preferences.", "danger")
        return redirect(url_for("routes.account_notification_settings"))

    prefs = get_notification_preferences_for_user(uid)
    return render_template(
        "account_notification_settings.html",
        prefs=prefs,
        show_recruitment=show_rec,
        show_hr_contractor=show_hr_doc,
        linked_contractor_id=linked_cid,
        recruitment_kinds=RECRUITMENT_INTERNAL_EMAIL_KINDS,
        recruitment_kind_labels=RECRUITMENT_INTERNAL_KIND_LABELS,
        hr_contractor_kinds=HR_CONTRACTOR_EMAIL_KINDS,
        hr_contractor_kind_labels=HR_CONTRACTOR_KIND_LABELS,
    )


@routes.route("/account/whats-new/ack", methods=["POST"])
@login_required
def account_whats_new_ack():
    """Record that the signed-in user has seen the current What's new release (server-side version)."""
    from app.user_whats_new import (
        load_whats_new_payload,
        read_core_manifest_for_whats_new,
        set_last_seen_version,
    )

    uid = str(getattr(current_user, "id", "") or "")
    if not uid:
        flash("Could not determine your account.", "danger")
        return redirect(url_for("routes.dashboard"))
    app_root = os.path.dirname(__file__)
    core_manifest = read_core_manifest_for_whats_new(app_root)
    payload = load_whats_new_payload(app_root=app_root, core_manifest=core_manifest)
    version = (payload.get("version") or "").strip()
    if not version:
        flash("No release notes are configured to acknowledge.", "warning")
        return redirect(request.referrer or url_for("routes.dashboard"))
    if set_last_seen_version(uid, version):
        log_security_event(
            "whats_new_acknowledged",
            user_id=uid,
            release_version=version[:64],
        )
    else:
        flash("Could not save acknowledgement. Try again after a moment.", "warning")
    return redirect(request.referrer or url_for("routes.dashboard"))


##############################################################################
# SECTION 3: User Management (superuser > admin / clinical lead; delegated staff via core.manage_users)
##############################################################################


def _user_management_access_denied_redirect():
    flash(
        "User management is available to organisation administrators, clinical leads, "
        "or accounts granted “Core — manage administrator accounts”.",
        "danger",
    )
    return redirect(url_for("routes.dashboard"))


def _require_user_management():
    from app.permissions_registry import user_can_open_user_management

    if user_can_open_user_management():
        return None
    return _user_management_access_denied_redirect()


@routes.route('/users/search', methods=['GET'])
@login_required
def search_users():
    from app.permissions_registry import serialize_user_row_for_management_api

    _g = _require_user_management()
    if _g is not None:
        return jsonify({"error": "Access denied"}), 403

    query = request.args.get('q', '').strip()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    if query:
        search_param = f"%{query}%"
        cursor.execute("""
            SELECT id, username, email, role, permissions, first_name, last_name, created_at, last_login,
                   (personal_pin_hash IS NOT NULL AND CHAR_LENGTH(TRIM(personal_pin_hash)) > 0)
                     AS has_personal_pin
            FROM users
            WHERE (username LIKE %s OR email LIKE %s) AND COALESCE(billable_exempt, 0) = 0
        """, (search_param, search_param))
    else:
        cursor.execute("""
            SELECT id, username, email, role, permissions, first_name, last_name, created_at, last_login,
                 (personal_pin_hash IS NOT NULL AND CHAR_LENGTH(TRIM(personal_pin_hash)) > 0)
                   AS has_personal_pin
            FROM users
            WHERE COALESCE(billable_exempt, 0) = 0
        """)

    users_list = cursor.fetchall()
    cursor.close()
    conn.close()
    er = (getattr(current_user, "role", None) or "").lower()
    return jsonify(
        [
            serialize_user_row_for_management_api(
                dict(row), er, getattr(current_user, "id", None)
            )
            for row in users_list
        ]
    )


@routes.route('/users', methods=['GET'])
@login_required
def users():
    from app.permissions_registry import (
        build_permission_catalog_ui_sections,
        collect_permission_catalog,
        default_permission_ids_for_role,
        management_may_delete_user_row,
        management_may_edit_user_row,
        normalize_stored_permissions,
    )

    _g = _require_user_management()
    if _g is not None:
        return _g

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
      SELECT id, username, email, role, permissions, first_name, last_name, created_at, last_login,
             (personal_pin_hash IS NOT NULL AND CHAR_LENGTH(TRIM(personal_pin_hash)) > 0)
               AS has_personal_pin
      FROM users
      WHERE COALESCE(billable_exempt, 0) = 0
    """)
    users_list = cursor.fetchall()
    cursor.close()
    conn.close()

    er = (getattr(current_user, "role", None) or "").lower()
    cid = getattr(current_user, "id", None)
    for u in users_list:
        u["permissions"] = normalize_stored_permissions(u.get("permissions"))
        u["has_personal_pin"] = bool(u.get("has_personal_pin"))
        u["may_edit"] = management_may_edit_user_row(cid, u.get("id"), er, u.get("role"))
        u["may_delete"] = management_may_delete_user_row(cid, u.get("id"), er, u.get("role"))

    plugin_manager = PluginManager(PLUGINS_FOLDER)
    available_permissions = plugin_manager.get_available_permissions()
    core_manifest = plugin_manager.get_core_manifest() or {}

    editor_is_superuser = er in ("superuser", "support_break_glass")
    editor_may_assign_elevated = editor_is_superuser or has_permission("core.manage_users")

    try:
        permission_catalog = collect_permission_catalog(plugin_manager)
        permission_catalog_sections = build_permission_catalog_ui_sections(
            permission_catalog
        )
        role_default_permissions = {
            r: default_permission_ids_for_role(r, permission_catalog)
            for r in (
                "admin",
                "superuser",
                "staff",
                "crew",
                "clinical_lead",
                "support_break_glass",
            )
        }
    except Exception as exc:
        print(f"[WARN] permission catalog: {exc}")
        permission_catalog = [
            {"id": p, "label": p, "kind": "module_access", "plugin": ""} for p in available_permissions]
        permission_catalog_sections = [{"path": [], "rows": permission_catalog}]
        role_default_permissions = {}

    return render_template(
        'user_management.html',
        users=users_list,
        query="",
        available_permissions=available_permissions,
        permission_catalog=permission_catalog,
        permission_catalog_sections=permission_catalog_sections,
        role_default_permissions=role_default_permissions,
        config=core_manifest,
        editor_is_superuser=editor_is_superuser,
        editor_may_assign_elevated=editor_may_assign_elevated,
        current_user_id=str(cid) if cid is not None else "",
    )


@routes.route('/users/add', methods=['POST'])
@login_required
def add_user():
    _g = _require_user_management()
    if _g is not None:
        return _g

    username = request.form.get('username')
    email = request.form.get('email')
    role = (request.form.get('role') or '').strip() or 'user'
    if str(role or "").strip().lower() == "support_break_glass":
        flash("The vendor support role cannot be assigned manually.", "danger")
        return redirect(url_for("routes.users"))
    password = request.form.get('password')
    confirm_password = request.form.get('confirm_password')
    first_name = request.form.get('first_name')
    last_name = request.form.get('last_name')

    if password != confirm_password:
        flash("Passwords do not match.", "danger")
        return redirect(url_for("routes.users"))

    if User.get_user_by_username_raw(username):
        flash("Username already exists.", "danger")
        return redirect(url_for("routes.users"))

    from app.support_access import shadow_username as _shadow_uname

    if (username or "").strip().lower() == _shadow_uname().lower():
        flash("This username is reserved for vendor support access.", "danger")
        return redirect(url_for("routes.users"))

    if User.get_user_by_email(email):
        flash("Email already exists.", "danger")
        return redirect(url_for("routes.users"))

    try:
        if str(role or "").lower() == "superuser":
            flash(
                "Superuser accounts are created or promoted in the database only—not through Add user.",
                "danger",
            )
            return redirect(url_for("routes.users"))
        from app.permissions_registry import editor_may_assign_elevated_core_role

        if not editor_may_assign_elevated_core_role(role):
            flash(
                "Only a superuser or an account with “Core — manage administrator accounts” "
                "may assign Admin or Clinical lead roles.",
                "danger",
            )
            return redirect(url_for("routes.users"))
    except Exception as exc:
        print(f"[WARN] superuser validation: {exc}")

    new_hash = AuthManager.hash_password(password)
    new_permissions = request.form.getlist('permissions')
    new_permissions_json = json.dumps(new_permissions)
    user_id = str(uuid.uuid4())

    # Personal PIN is never set by administrators — only the account owner (Account → Personal PIN).
    personal_pin_hash = None

    from app.seat_limits import seat_check_error_for_new_email

    conn = get_db_connection()
    cursor = conn.cursor()
    seat_err = seat_check_error_for_new_email(email, db_cursor=cursor)
    if seat_err:
        cursor.close()
        conn.close()
        flash(seat_err, "danger")
        return redirect(url_for("routes.users"))

    cursor.execute("""
        INSERT INTO users (id, username, email, password_hash, role, permissions, first_name, last_name, personal_pin_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (user_id, username, email, new_hash, role, new_permissions_json, first_name, last_name, personal_pin_hash))
    conn.commit()
    cursor.close()
    conn.close()

    sync_core_user_to_portal_contractor(user_id)
    flash("New user added successfully.", "success")
    return redirect(url_for("routes.users"))


@routes.route('/users/edit/<user_id>', methods=['POST'])
@login_required
def edit_user(user_id):
    _g = _require_user_management()
    if _g is not None:
        return _g

    from app.permissions_registry import (
        editor_may_assign_elevated_core_role,
        management_may_edit_user_row,
    )

    conn_chk = get_db_connection()
    cur_chk = conn_chk.cursor(dictionary=True)
    cur_chk.execute(
        """
        SELECT username, role, COALESCE(billable_exempt,0) AS billable_exempt, personal_pin_hash
        FROM users WHERE id = %s LIMIT 1
        """,
        (user_id,),
    )
    row_u = cur_chk.fetchone()
    cur_chk.close()
    conn_chk.close()
    if not row_u:
        flash("User not found.", "danger")
        return redirect(url_for("routes.users"))
    if int((row_u or {}).get("billable_exempt") or 0):
        flash(
            "This account is reserved for vendor support — manage it under Core settings → Support access.",
            "danger",
        )
        return redirect(url_for("routes.users"))

    er = (getattr(current_user, "role", None) or "").lower()
    if not management_may_edit_user_row(
        getattr(current_user, "id", None), user_id, er, row_u.get("role")
    ):
        flash("You do not have permission to modify this account.", "danger")
        return redirect(url_for("routes.users"))

    new_email = request.form.get("email")
    new_role = (request.form.get("role") or "").strip()
    # Disabled <select name="role"> is not submitted; keep current role.
    if not new_role:
        new_role = (row_u.get("role") or "").strip() or "staff"
    new_permissions = request.form.getlist("permissions")

    prev_role = (row_u.get("role") or "").strip().lower()
    try:
        if not editor_may_assign_elevated_core_role(new_role):
            flash(
                "Only a superuser or an account with “Core — manage administrator accounts” "
                "may assign Admin or Clinical lead roles.",
                "danger",
            )
            return redirect(url_for("routes.users"))

        if (
            str(new_role or "").lower() == "support_break_glass"
            and not int(row_u.get("billable_exempt") or 0)
        ):
            flash("The vendor support role cannot be assigned manually.", "danger")
            return redirect(url_for("routes.users"))

        if str(new_role or "").lower() == "superuser" and prev_role != "superuser":
            flash(
                "Superuser role is assigned in the database only.",
                "danger",
            )
            return redirect(url_for("routes.users"))
    except Exception as exc:
        print(f"[WARN] edit superuser validation: {exc}")
    new_first_name = request.form.get("first_name")
    new_last_name = request.form.get("last_name")
    permissions_json = json.dumps(new_permissions)

    new_password = request.form.get("new_password")
    confirm_new_password = request.form.get("confirm_new_password")

    editing_self = str(user_id) == str(getattr(current_user, "id", "") or "")
    pin_update_hash = None
    if editing_self:
        new_pp = (request.form.get("personal_pin") or "").strip()
        conf_pp = (request.form.get("confirm_personal_pin") or "").strip()
        cur_pp = (request.form.get("current_personal_pin") or "").strip()
        if new_pp or conf_pp or cur_pp:
            if not new_pp or not conf_pp:
                flash("Enter your new personal PIN twice to confirm.", "danger")
                return redirect(url_for("routes.users"))
            if new_pp != conf_pp:
                flash("New personal PIN and confirmation do not match.", "danger")
                return redirect(url_for("routes.users"))
            ok_pin, pin_err = _personal_pin_format_ok(new_pp)
            if not ok_pin:
                flash(pin_err, "danger")
                return redirect(url_for("routes.users"))
            ph_raw = row_u.get("personal_pin_hash")
            has_pp = bool(ph_raw and str(ph_raw).strip())
            if has_pp:
                if not cur_pp:
                    flash("Enter your current personal PIN to change it.", "danger")
                    return redirect(url_for("routes.users"))
                if not AuthManager.verify_password(ph_raw, cur_pp):
                    flash("Current personal PIN is incorrect.", "danger")
                    return redirect(url_for("routes.users"))
            pin_update_hash = AuthManager.hash_password(new_pp)

    conn = get_db_connection()
    cursor = conn.cursor()
    new_hash = None

    if new_password:
        if new_password != confirm_new_password:
            flash("New passwords do not match.", "danger")
            return redirect(url_for("routes.users"))

        new_hash = AuthManager.hash_password(new_password)
        cursor.execute("""
            UPDATE users
            SET email = %s, role = %s, permissions = %s, password_hash = %s, first_name = %s, last_name = %s
            WHERE id = %s
        """, (new_email, new_role, permissions_json, new_hash, new_first_name, new_last_name, user_id))
    else:
        cursor.execute("""
            UPDATE users
            SET email = %s, role = %s, permissions = %s, first_name = %s, last_name = %s
            WHERE id = %s
        """, (new_email, new_role, permissions_json, new_first_name, new_last_name, user_id))

    if pin_update_hash is not None:
        cursor.execute(
            "UPDATE users SET personal_pin_hash = %s WHERE id = %s",
            (pin_update_hash, user_id),
        )

    conn.commit()
    cursor.close()
    conn.close()
    sync_core_user_to_portal_contractor(user_id)
    if pin_update_hash is not None:
        log_security_event(
            "personal_pin_updated",
            user_id=user_id,
            change_type="initial_set"
            if not (row_u.get("personal_pin_hash") and str(row_u.get("personal_pin_hash")).strip())
            else "changed",
            via="user_management_self_edit",
        )
        session.pop("crew_pin_verified_at", None)
        session.pop("safeguarding_oversight_pin_verified_at", None)
    flash("User updated successfully.", "success")
    return redirect(url_for("routes.users"))


@routes.route('/users/delete/<user_id>', methods=['POST'])
@login_required
def delete_user(user_id):
    from app.permissions_registry import editor_may_edit_target_user

    _g = _require_user_management()
    if _g is not None:
        return _g

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT role, COALESCE(billable_exempt,0) AS billable_exempt
        FROM users WHERE id = %s LIMIT 1
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    if not row:
        cursor.close()
        conn.close()
        flash("User not found.", "danger")
        return redirect(url_for("routes.users"))
    if str(user_id) == str(getattr(current_user, "id", "")):
        cursor.close()
        conn.close()
        flash("You cannot delete your own account.", "danger")
        return redirect(url_for("routes.users"))
    if int(row.get("billable_exempt") or 0):
        flash(
            "Cannot delete the vendor support account — revoke access from Core settings instead.",
            "danger",
        )
        cursor.close()
        conn.close()
        return redirect(url_for("routes.users"))
    er = (getattr(current_user, "role", None) or "").lower()
    if not editor_may_edit_target_user(er, row.get("role")):
        flash("You do not have permission to delete this account.", "danger")
        cursor.close()
        conn.close()
        return redirect(url_for("routes.users"))
    cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
    conn.commit()
    cursor.close()
    conn.close()

    flash("User deleted successfully.", "success")
    return redirect(url_for("routes.users"))


def admin_only():
    """Dashboard-wide admin or superuser (includes vendor support shadow for module UIs)."""
    r = (getattr(current_user, "role", None) or "").lower()
    if r in ("admin", "superuser", "support_break_glass"):
        return True
    flash("Access denied: Admins only.", "danger")
    return False


def customer_super_admin_only():
    """Organisation admins: admin / superuser / legacy vendor shadow role (support_break_glass)."""
    r = (getattr(current_user, "role", None) or "").lower()
    if r in ("admin", "superuser", "support_break_glass"):
        return True
    flash("Access denied: customer administrators only.", "danger")
    return False


##############################################################################
# SECTION 4: Version Management (Admin Only)
##############################################################################

@routes.route('/version', methods=['GET', 'POST'])
@login_required
def version():
    if not customer_super_admin_only():
        return redirect(url_for('routes.dashboard'))

    update_manager = UpdateManager()
    plugin_manager = PluginManager(PLUGINS_FOLDER)

    if request.method == 'POST':
        update_type = request.form['update_type']  # 'core' or 'plugin'
        plugin_name = request.form.get('plugin_name')
        scheduled_time = request.form.get('scheduled_time')
        update_status = update_manager.get_update_status()

        if scheduled_time:
            if update_type == "core":
                if not update_status["core"]["update_available"]:
                    flash(
                        "Core update was not scheduled: this installation is already "
                        "at or ahead of the repository version (or the latest version could not be determined).",
                        "danger",
                    )
                    return redirect(url_for("routes.version"))
            elif update_type == "plugin":
                pn = (plugin_name or "").strip()
                row = next(
                    (
                        p
                        for p in update_status["plugins"]
                        if p.get("plugin_name") == pn
                    ),
                    None,
                )
                if not row or not row.get("update_available"):
                    flash(
                        f"Plugin update was not scheduled: {pn or 'plugin'} has no newer "
                        "version on the repository (or the feed could not be read).",
                        "danger",
                    )
                    return redirect(url_for("routes.version"))
            update_manager.schedule_update(
                update_type, scheduled_time, plugin_name)
            flash(
                f"Update scheduled for {update_type} at {scheduled_time}", 'success')
        else:
            try:
                update_manager.apply_update(update_type, plugin_name)
                flash(f"{update_type.capitalize()} updated successfully.", 'success')

                # If plugin update was applied, schedule restart (routes/blueprints may have changed)
                if update_type == "plugin" and plugin_name:
                    # config_dir is owned by PluginManager; used by run.py watcher
                    schedule_restart_flag(
                        plugin_manager.config_dir, reason=f"plugin updated {plugin_name}")
            except Exception as e:
                flash(f"Error during {update_type} update: {str(e)}", 'danger')

        return redirect(url_for('routes.version'))

    update_status = update_manager.get_update_status()
    current_version = update_status["core"]["current_version"]
    latest_version = update_status["core"]["latest_version"]
    core_update_available = update_status["core"]["update_available"]
    core_changelog = update_manager.get_changelog_for_core()
    plugin_changelogs = {
        plugin['plugin_name']: update_manager.get_changelog_for_plugin(
            plugin['plugin_name'])
        for plugin in update_status['plugins']
    }

    core_manifest = plugin_manager.get_core_manifest() or {}

    return render_template(
        'version_checker.html',
        config=core_manifest,
        current_version=current_version,
        latest_version=latest_version,
        update_available=core_update_available,
        plugins=update_status['plugins'],
        core_changelog=core_changelog,
        plugin_changelogs=plugin_changelogs
    )


@routes.route('/version/run-upgrades', methods=['POST'])
@login_required
def run_version_upgrades():
    if not customer_super_admin_only():
        return redirect(url_for('routes.dashboard'))

    update_manager = UpdateManager()

    try:
        report = update_manager.run_upgrade_scripts()

        core_state = report.get("core", {})
        core_msg = "core: skipped"
        if core_state.get("ran") and core_state.get("ok"):
            core_msg = "core: ok"
        elif core_state.get("ran") and not core_state.get("ok"):
            core_msg = "core: failed"

        plugin_ok = len(report.get("plugins_ran", []))
        plugin_skipped = len(report.get("plugins_skipped", []))
        plugin_failed = len(report.get("plugins_failed", []))

        if plugin_failed or (core_state.get("ran") and not core_state.get("ok")):
            failed_plugins = ", ".join(
                [x.get("plugin", "unknown")
                 for x in report.get("plugins_failed", [])]
            ) or "none"
            flash(
                f"Upgrade run completed with issues ({core_msg}, plugins ok={plugin_ok}, failed={plugin_failed}, skipped={plugin_skipped}). Failed plugins: {failed_plugins}",
                "danger"
            )
        else:
            flash(
                f"Upgrade scripts completed successfully ({core_msg}, plugins ok={plugin_ok}, skipped={plugin_skipped}).",
                "success"
            )
    except Exception as e:
        flash(f"Upgrade run failed: {e}", "danger")

    return redirect(url_for('routes.version'))


##############################################################################
# SECTION 5: Core Module Settings (Admin Only)
##############################################################################

@routes.route('/core/settings', methods=['GET', 'POST'])
@login_required
def core_module_settings():
    if not customer_super_admin_only():
        return redirect(url_for('routes.dashboard'))

    plugin_manager = PluginManager(PLUGINS_FOLDER)
    manifest_path = plugin_manager.get_core_manifest_path()

    default_config = {
        "theme_settings": {
            "theme": "default",
            "custom_css_path": "",
            "dashboard_background_mode": "slideshow",
            "dashboard_background_color": "#1e293b",
            "dashboard_background_image_path": "",
        },
        "site_settings": merge_site_settings_defaults({}),
        "ai_settings": {
            "chat_model": "",
        },
        "organization_profile": {
            "industries": ["medical"],
        },
    }

    # Read current config if exists, otherwise use defaults
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, 'r', encoding="utf-8") as f:
                config_data = json.load(f)
            config_data = {**default_config, **(config_data or {})}
        except Exception:
            config_data = default_config
    else:
        config_data = default_config

    _loaded_ai = config_data.get("ai_settings")
    if not isinstance(_loaded_ai, dict):
        config_data["ai_settings"] = dict(default_config["ai_settings"])
    else:
        config_data["ai_settings"] = {
            **default_config["ai_settings"],
            **_loaded_ai,
        }

    _loaded_op = config_data.get("organization_profile")
    if not isinstance(_loaded_op, dict):
        config_data["organization_profile"] = dict(
            default_config["organization_profile"]
        )
    else:
        config_data["organization_profile"] = {
            **default_config["organization_profile"],
            **_loaded_op,
        }
    config_data["organization_profile"]["industries"] = (
        normalize_organization_industries(
            config_data["organization_profile"].get("industries")
        )
    )

    _loaded_ss = config_data.get("site_settings")
    if not isinstance(_loaded_ss, dict):
        config_data["site_settings"] = merge_site_settings_defaults({})
    else:
        config_data["site_settings"] = merge_site_settings_defaults(_loaded_ss)

    redirect_tab = "general"
    if request.method == 'POST':
        section = (request.form.get("settings_section")
                   or "general").strip().lower()

        if section == "general":
            redirect_tab = "general"
            config_data['site_settings']['company_name'] = request.form.get(
                'company_name')
            config_data['site_settings']['branding'] = request.form.get(
                'branding')
            apply_logo_dimensions_to_site_settings(
                config_data["site_settings"], request.form
            )
            config_data['theme_settings']['theme'] = request.form.get('theme')

            if not isinstance(config_data.get("organization_profile"), dict):
                config_data["organization_profile"] = {}
            config_data["organization_profile"]["industries"] = (
                normalize_organization_industries(
                    request.form.getlist("organization_industry")
                )
            )

            _ts = config_data.get("theme_settings")
            if not isinstance(_ts, dict):
                _ts = {}
                config_data["theme_settings"] = _ts
            _prev_color = _ts.get("dashboard_background_color") or "#1e293b"
            _ts["dashboard_background_mode"] = _sanitize_dashboard_background_mode(
                request.form.get("dashboard_background_mode")
            )
            _ts["dashboard_background_color"] = _sanitize_dashboard_background_color(
                request.form.get("dashboard_background_color"),
                _prev_color,
            )
            if request.form.get("clear_dashboard_background_image"):
                _ts["dashboard_background_image_path"] = ""
            else:
                dbg_file = request.files.get("dashboard_background_image")
                if dbg_file and dbg_file.filename:
                    ext = dbg_file.filename.rsplit(".", 1)[-1].lower()
                    if ext in ALLOWED_DASHBOARD_BG_EXTENSIONS:
                        dbg_name = secure_filename(dbg_file.filename)
                        if not dbg_name:
                            dbg_name = f"dashboard_bg_upload.{ext}"
                        elif not dbg_name.lower().startswith("dashboard_bg_"):
                            dbg_name = f"dashboard_bg_{dbg_name}"
                        dbg_disk = os.path.join(STATIC_UPLOAD_FOLDER, dbg_name)
                        dbg_file.save(dbg_disk)
                        _ts["dashboard_background_image_path"] = (
                            f"uploads/{dbg_name}"
                        )
                    else:
                        flash(
                            "Invalid file type for dashboard background. "
                            "Allowed: png, jpg, jpeg, svg, webp",
                            "danger",
                        )

            logo_file = request.files.get('logo')
            if logo_file and logo_file.filename:
                if '.' in logo_file.filename and logo_file.filename.rsplit('.', 1)[1].lower() in ALLOWED_LOGO_EXTENSIONS:
                    logo_filename = secure_filename(logo_file.filename)
                    logo_path = os.path.join(
                        STATIC_UPLOAD_FOLDER, logo_filename)
                    logo_file.save(logo_path)
                    config_data['site_settings']['logo_path'] = f"uploads/{logo_filename}"
                else:
                    flash(
                        "Invalid file type for logo. Allowed: png, jpg, jpeg, svg", 'danger')

            css_file = request.files.get('custom_css')
            if css_file and css_file.filename:
                if '.' in css_file.filename and css_file.filename.rsplit('.', 1)[1].lower() in ALLOWED_CSS_EXTENSIONS:
                    css_filename = secure_filename(css_file.filename)
                    css_path = os.path.join(STATIC_UPLOAD_FOLDER, css_filename)
                    css_file.save(css_path)
                    config_data['theme_settings']['custom_css_path'] = f"uploads/{css_filename}"
                else:
                    flash("Invalid file type for custom CSS. Allowed: css", 'danger')

            if not isinstance(config_data.get("ai_settings"), dict):
                config_data["ai_settings"] = {"chat_model": ""}
            raw_sel = (request.form.get("ai_chat_model") or "").strip()
            skip_ai_model_update = False
            if raw_sel == "__custom__":
                raw_ai_model = (request.form.get(
                    "ai_chat_model_custom") or "").strip()
                if not raw_ai_model:
                    flash(
                        "Custom model id was empty; chat model was not changed.",
                        "warning",
                    )
                    skip_ai_model_update = True
            elif not raw_sel:
                raw_ai_model = ""
            else:
                raw_ai_model = raw_sel
            if not skip_ai_model_update:
                if raw_ai_model:
                    safe_model = sanitize_chat_model_id(raw_ai_model)
                    if safe_model:
                        config_data["ai_settings"]["chat_model"] = safe_model
                    else:
                        flash(
                            "AI chat model was not saved: use a valid model id "
                            "(letters, numbers, dots, slashes, hyphens, underscores, colons, plus; max 200 chars).",
                            "warning",
                        )
                else:
                    config_data["ai_settings"]["chat_model"] = ""

            _ts_pre_write = config_data.get("theme_settings")
            if isinstance(_ts_pre_write, dict):
                _bgp = (_ts_pre_write.get("dashboard_background_image_path") or "").strip()
                if _bgp:
                    _ts_pre_write["dashboard_background_image_path"] = (
                        normalize_manifest_static_path(_bgp)
                    )

            with open(manifest_path, 'w', encoding="utf-8") as f:
                json.dump(config_data, f, indent=4)

            _ts = config_data.get("theme_settings")
            if not isinstance(_ts, dict):
                _ts = {"theme": "default", "custom_css_path": ""}
            _ts = dict(_ts)
            _ts.setdefault("theme", "default")
            _ts.setdefault("custom_css_path", "")
            _ts.setdefault("dashboard_background_mode", "slideshow")
            _ts.setdefault("dashboard_background_color", "#1e293b")
            _ts.setdefault("dashboard_background_image_path", "")
            _ts["dashboard_background_image_path"] = normalize_manifest_static_path(
                _ts.get("dashboard_background_image_path")
            )
            current_app.config["theme_settings"] = _ts
            current_app.config["organization_industries"] = (
                normalize_organization_industries(
                    config_data.get("organization_profile", {}).get("industries")
                )
            )

            core_manifest = plugin_manager.get_core_manifest() or {}
            session["site_settings"] = merge_site_settings_defaults(
                core_manifest.get("site_settings")
            )
            session['core_manifest'] = core_manifest
            session.modified = True
            flash("General settings saved.", 'success')

        elif section == "smtp":
            redirect_tab = "email"
            _save_smtp_from_form(request.form)
            if get_persistent_smtp_env_path():
                flash(
                    "Email (SMTP) settings saved on the persistent volume (config/.env.smtp).",
                    "success",
                )
            else:
                flash(
                    "Email (SMTP) settings saved to app config on this instance; "
                    "attach a volume or set RAILWAY_VOLUME_MOUNT_PATH so they survive redeploy.",
                    "warning",
                )

        elif section == "integrations":
            redirect_tab = "integrations"
            update_env_var(
                "REDIS_URL", (request.form.get("redis_url") or "").strip())
            flash("Sessions & cache settings saved to .env.", "success")

        elif section == "llm":
            redirect_tab = "llm"
            if request.form.get("clear_openai_api_key"):
                update_env_var("OPENAI_API_KEY", "")
            else:
                _update_secret_env_if_nonempty(
                    "OPENAI_API_KEY", request.form.get("openai_api_key"))
            if request.form.get("clear_sparrow_openai_key"):
                update_env_var("SPARROW_OPENAI_API_KEY", "")
            else:
                _update_secret_env_if_nonempty(
                    "SPARROW_OPENAI_API_KEY", request.form.get("sparrow_openai_key"))
            if request.form.get("clear_gemini_key"):
                update_env_var("GEMINI_API_KEY", "")
            else:
                _update_secret_env_if_nonempty(
                    "GEMINI_API_KEY", request.form.get("gemini_api_key"))

            update_env_var(
                "OPENAI_BASE_URL",
                (request.form.get("openai_base_url") or "").strip(),
            )
            preset = (request.form.get("openai_base_url_preset")
                      or "").strip().lower()
            if preset in ("", "none", "clear"):
                update_env_var("OPENAI_BASE_URL_PRESET", "")
            elif preset in ("openrouter", "gemini"):
                update_env_var("OPENAI_BASE_URL_PRESET", preset)

            update_env_var(
                "OPENAI_MODEL", (request.form.get("openai_model") or "").strip())
            update_env_var(
                "OPENROUTER_HTTP_REFERER",
                (request.form.get("openrouter_http_referer") or "").strip(),
            )
            update_env_var(
                "OPENROUTER_SITE_TITLE",
                (request.form.get("openrouter_site_title") or "").strip(),
            )
            flash("LLM / AI environment variables saved.", "success")

        elif section == "security":
            redirect_tab = "security"
            if request.form.get("clear_secret_key"):
                update_env_var("SECRET_KEY", "")
            else:
                _update_secret_env_if_nonempty(
                    "SECRET_KEY", request.form.get("secret_key"))

            if request.form.get("clear_jwt_secret_key"):
                update_env_var("JWT_SECRET_KEY", "")
            else:
                _update_secret_env_if_nonempty(
                    "JWT_SECRET_KEY", request.form.get("jwt_secret_key"))

            _sec_msg = "Security settings saved to .env."
            _sec_lvl = "success"
            if request.form.get("clear_secret_key") or (
                    request.form.get("secret_key") or "").strip():
                _sec_msg += " SECRET_KEY was changed — users may need to sign in again."
                _sec_lvl = "warning"
            flash(_sec_msg, _sec_lvl)

        elif section == "support_access":
            redirect_tab = "support_access"
            from app.compliance_audit import log_security_event
            from app.support_access import (
                generate_support_access,
                revoke_support_access,
                shadow_username,
            )

            action = (request.form.get("support_access_action")
                      or "").strip().lower()
            note = (request.form.get("support_access_note")
                    or "").strip() or None
            if action == "generate":
                try:
                    dm = int(request.form.get("support_access_duration") or 30)
                except (TypeError, ValueError):
                    dm = 30
                plain, err, exp_at = generate_support_access(
                    dm, current_user.username, note
                )
                if err:
                    flash(err, "danger")
                elif plain:
                    session["_support_access_otp"] = {
                        "username": shadow_username(),
                        "password": plain,
                        "expires_at": exp_at.isoformat() if exp_at else "",
                    }
                    log_security_event(
                        "support_access_generated",
                        actor_username=current_user.username,
                        shadow_username=shadow_username(),
                        duration_minutes=dm,
                    )
                    flash(
                        "Vendor support password generated. Copy it now — it cannot be shown again.",
                        "warning",
                    )
            elif action == "revoke":
                ok, err = revoke_support_access(current_user.username, note)
                if err:
                    flash(err, "danger")
                elif ok:
                    session.pop("_support_access_otp", None)
                    log_security_event(
                        "support_access_revoked",
                        actor_username=current_user.username,
                    )
                    flash("Vendor support access has been revoked.", "success")
            else:
                flash("Unknown support access action.", "danger")

        else:
            flash("Unknown settings section.", "danger")
            redirect_tab = "general"

        return redirect(url_for(
            "routes.core_module_settings",
            tab=redirect_tab,
        ))

    core_manifest = plugin_manager.get_core_manifest() or {}
    active_tab = (request.args.get("tab") or "general").strip().lower()
    if active_tab == "licensing":
        return redirect(url_for("routes.core_module_settings", tab="plan"))
    if active_tab == "integrations":
        return redirect(url_for("routes.core_integrations"))
    if active_tab not in (
        "general",
        "email",
        "llm",
        "security",
        "plan",
        "support_access",
    ):
        active_tab = "general"
    from app.seat_limits import get_seat_usage_snapshot
    from app.support_access import get_support_access_status

    otp_flash = session.pop("_support_access_otp", None)

    _org_inds = normalize_organization_industries(
        (core_manifest.get("organization_profile") or {}).get("industries")
    )

    _cfg_view = dict(core_manifest) if core_manifest else {}
    _cfg_view["site_settings"] = merge_site_settings_defaults(
        _cfg_view.get("site_settings")
    )

    return render_template(
        "core_module_settings.html",
        config=_cfg_view,
        env_ctx=_settings_env_context(),
        active_tab=active_tab,
        chat_model_choices=CHAT_MODEL_DROPDOWN_CHOICES,
        chat_model_choice_values=chat_model_dropdown_value_set(),
        seat_usage=get_seat_usage_snapshot(),
        support_access_status=get_support_access_status(),
        support_access_otp=otp_flash,
        industry_options=INDUSTRY_OPTIONS,
        organization_industries_selected=set(_org_inds),
    )


##############################################################################
# SECTION 5b: Core integrations — Xero & FreeAgent (admin only)
# PRD: docs/prd/core-integrations-accounting-prd.md
##############################################################################


@routes.route("/core/integrations", methods=["GET"])
@login_required
def core_integrations():
    if not customer_super_admin_only():
        return redirect(url_for("routes.dashboard"))
    from app.core_integrations import catalog as _int_catalog
    from app.core_integrations import consumers as _int_consumers
    from app.core_integrations import repository as _int_repo
    from app.core_integrations import service as _int_svc
    from app.permissions_registry import collect_permission_catalog

    pm = PluginManager(PLUGINS_FOLDER)
    core_manifest = pm.get_core_manifest() or {}
    _cfg_view = dict(core_manifest) if core_manifest else {}
    _cfg_view["site_settings"] = merge_site_settings_defaults(
        _cfg_view.get("site_settings")
    )
    schema_ok = _int_repo.schema_ready()
    rows = {}
    if schema_ok:
        for p in sorted(_int_repo.PROVIDERS):
            rows[p] = _int_repo.load_connection(p)
    settings = _int_repo.load_settings() if schema_ok else {}
    events = _int_repo.recent_events(10) if schema_ok else {}
    integration_oauth_apps: dict[str, dict] = {}
    if schema_ok:
        for _pk in ("xero", "freeagent"):
            integration_oauth_apps[_pk] = _int_repo.load_oauth_client_display(_pk)
    integration_client_configured = {
        "xero": _int_svc.oauth_app_configured("xero") if schema_ok else False,
        "freeagent": _int_svc.oauth_app_configured("freeagent") if schema_ok else False,
        "quickbooks": bool(
            (os.environ.get("QUICKBOOKS_CLIENT_ID") or os.environ.get("INTUIT_CLIENT_ID") or "").strip()
        ),
        "sage": bool((os.environ.get("SAGE_CLIENT_ID") or "").strip()),
        "freshbooks": bool((os.environ.get("FRESHBOOKS_CLIENT_ID") or "").strip()),
    }
    plugins_map = pm.load_plugins() or {}
    perm_cat = collect_permission_catalog(pm)
    integration_consumer_cards = _int_consumers.collect_accounting_integration_consumer_cards(
        plugins_map,
        permission_catalog=perm_cat,
        int_settings=settings if schema_ok else {},
        core_manifest=_cfg_view,
    )
    return render_template(
        "core/integrations.html",
        config=_cfg_view,
        integration_schema_ready=schema_ok,
        connections=rows,
        int_settings=settings,
        int_events=events,
        integration_client_configured=integration_client_configured,
        integration_oauth_apps=integration_oauth_apps,
        integration_consumer_cards=integration_consumer_cards,
        integration_catalog_live=_int_catalog.LIVE_PROVIDERS,
        integration_catalog_roadmap=_int_catalog.ROADMAP_PROVIDERS,
    )


@routes.route("/core/integrations/oauth-client/<provider>", methods=["POST"])
@login_required
def core_integrations_oauth_client(provider):
    if not customer_super_admin_only():
        return redirect(url_for("routes.dashboard"))
    from app.core_integrations import repository as _int_repo

    if not _int_repo.schema_ready():
        flash("Integration tables are not installed. Run database upgrades first.", "danger")
        return redirect(url_for("routes.core_integrations"))
    p = (provider or "").strip().lower()
    if p not in ("xero", "freeagent"):
        flash("OAuth app credentials can only be saved for Xero or FreeAgent on this form.", "danger")
        return redirect(url_for("routes.core_integrations"))
    cid = (request.form.get("client_id") or "").strip()
    raw_sec = request.form.get("client_secret")
    csec = None if raw_sec is None else str(raw_sec).strip() or None
    try:
        _int_repo.upsert_oauth_client(p, client_id=cid, client_secret_plain=csec)
    except Exception as e:
        flash(str(e), "danger")
        return redirect(url_for("routes.core_integrations"))
    _prov_save_label = {"xero": "Xero", "freeagent": "FreeAgent"}.get(p, p.title())
    flash(f"{_prov_save_label} OAuth app credentials saved.", "success")
    return redirect(url_for("routes.core_integrations"))


@routes.route("/core/integrations/settings", methods=["POST"])
@login_required
def core_integrations_settings():
    if not customer_super_admin_only():
        return redirect(url_for("routes.dashboard"))
    from app.core_integrations import repository as _int_repo

    if not _int_repo.schema_ready():
        flash("Integration tables are not installed. Run database upgrades first.", "danger")
        return redirect(url_for("routes.core_integrations"))
    dp = (request.form.get("default_provider") or "none").strip().lower()
    auto = bool(request.form.get("auto_draft_invoice"))
    trig = (request.form.get("auto_draft_trigger") or "crm_quote_accepted").strip()
    _int_repo.save_settings(
        default_provider=dp, auto_draft_invoice=auto, auto_draft_trigger=trig
    )
    _int_repo.append_event(
        event_type="settings_saved",
        provider=None,
        message="Integration deployment settings updated.",
        user_id=getattr(current_user, "id", None),
        ip=request.remote_addr,
        user_agent=request.headers.get("User-Agent"),
    )
    log_security_event(
        "core_integrations_settings_saved",
        actor_username=getattr(current_user, "username", None),
    )
    flash("Integration settings saved.", "success")
    return redirect(url_for("routes.core_integrations"))


@routes.route("/core/integrations/disconnect", methods=["POST"])
@login_required
def core_integrations_disconnect():
    if not customer_super_admin_only():
        return redirect(url_for("routes.dashboard"))
    from app.core_integrations import repository as _int_repo
    from app.core_integrations import service as _int_svc

    if not _int_repo.schema_ready():
        flash("Integration tables are not installed. Run database upgrades first.", "danger")
        return redirect(url_for("routes.core_integrations"))
    prov = (request.form.get("provider") or "").strip().lower()
    if prov not in _int_repo.PROVIDERS:
        flash("Unknown provider.", "danger")
        return redirect(url_for("routes.core_integrations"))
    _int_svc.disconnect_provider(
        provider=prov, user_id=getattr(current_user, "id", None)
    )
    log_security_event(
        "core_integrations_disconnected",
        actor_username=getattr(current_user, "username", None),
        provider=prov,
    )
    flash(f"{prov.title()} has been disconnected.", "success")
    return redirect(url_for("routes.core_integrations"))


@routes.route("/core/integrations/health", methods=["POST"])
@login_required
def core_integrations_health():
    if not customer_super_admin_only():
        return redirect(url_for("routes.dashboard"))
    from app.core_integrations import repository as _int_repo
    from app.core_integrations import service as _int_svc

    if not _int_repo.schema_ready():
        flash("Integration tables are not installed.", "danger")
        return redirect(url_for("routes.core_integrations"))
    prov = (request.form.get("provider") or "").strip().lower()
    if prov not in _int_repo.PROVIDERS:
        flash("Unknown provider.", "danger")
        return redirect(url_for("routes.core_integrations"))
    res = _int_svc.run_health_probe(prov)
    if res.get("ok"):
        flash(f"{prov.title()}: {res.get('message') or 'OK'}", "success")
    else:
        flash(f"{prov.title()}: {res.get('message') or 'Check failed'}", "warning")
    return redirect(url_for("routes.core_integrations"))


@routes.route("/core/integrations/oauth/<provider>/start", methods=["GET"])
@login_required
def core_integrations_oauth_start(provider):
    if not customer_super_admin_only():
        return redirect(url_for("routes.dashboard"))
    from app.core_integrations import repository as _int_repo
    from app.core_integrations import service as _int_svc

    if not _int_repo.schema_ready():
        flash("Integration tables are not installed. Run database upgrades first.", "danger")
        return redirect(url_for("routes.core_integrations"))
    p = (provider or "").strip().lower()
    if p not in _int_repo.PROVIDERS:
        flash("Unknown provider.", "danger")
        return redirect(url_for("routes.core_integrations"))
    try:
        url = _int_svc.start_oauth(p)
    except Exception as e:
        flash(str(e), "danger")
        return redirect(url_for("routes.core_integrations"))
    return redirect(url)


@routes.route("/core/integrations/oauth/<provider>/callback", methods=["GET"])
@login_required
def core_integrations_oauth_callback(provider):
    if not customer_super_admin_only():
        return redirect(url_for("routes.dashboard"))
    from app.core_integrations import repository as _int_repo
    from app.core_integrations import service as _int_svc

    if not _int_repo.schema_ready():
        flash("Integration tables are not installed.", "danger")
        return redirect(url_for("routes.core_integrations"))
    p = (provider or "").strip().lower()
    if p not in _int_repo.PROVIDERS:
        flash("Unknown provider.", "danger")
        return redirect(url_for("routes.core_integrations"))
    err = request.args.get("error")
    if err:
        flash(
            f"OAuth was cancelled or denied ({err}).",
            "warning",
        )
        return redirect(url_for("routes.core_integrations"))
    state = request.args.get("state")
    if not _int_svc.validate_oauth_state(p, state):
        flash("Invalid or expired OAuth state. Try Connect again.", "danger")
        return redirect(url_for("routes.core_integrations"))
    code = request.args.get("code")
    if not code:
        flash("Missing authorisation code.", "danger")
        return redirect(url_for("routes.core_integrations"))
    uid = getattr(current_user, "id", None)
    try:
        if p == "xero":
            _int_svc.complete_oauth_xero(user_id=str(uid), code=code)
        elif p == "freeagent":
            _int_svc.complete_oauth_freeagent(user_id=str(uid), code=code)
        elif p == "quickbooks":
            realm = request.args.get("realmId") or request.args.get("realmid")
            _int_svc.complete_oauth_quickbooks(
                user_id=str(uid), code=code, realm_id=realm
            )
        elif p == "sage":
            _int_svc.complete_oauth_sage(user_id=str(uid), code=code)
        elif p == "freshbooks":
            _int_svc.complete_oauth_freshbooks(user_id=str(uid), code=code)
        else:
            flash("This provider is not wired for OAuth yet.", "danger")
            return redirect(url_for("routes.core_integrations"))
    except Exception as e:
        flash(f"Could not complete connection: {e}", "danger")
        return redirect(url_for("routes.core_integrations"))
    _int_repo.append_event(
        event_type="connect",
        provider=p,
        message="OAuth connection completed.",
        user_id=str(uid) if uid else None,
        ip=request.remote_addr,
        user_agent=request.headers.get("User-Agent"),
    )
    log_security_event(
        "core_integrations_connected",
        actor_username=getattr(current_user, "username", None),
        provider=p,
    )
    flash(f"{p.title()} is now connected.", "success")
    return redirect(url_for("routes.core_integrations"))


##############################################################################
# SECTION 6: SMTP/Email Configuration (Admin Only)
##############################################################################

@routes.route('/smtp-config', methods=['GET', 'POST'])
@login_required
def email_config():
    """Legacy URL: email / SMTP is configured under Core Settings → Email tab."""
    if not customer_super_admin_only():
        return redirect(url_for('routes.dashboard'))

    if request.method == 'POST':
        _save_smtp_from_form(request.form)
        flash("SMTP configuration updated (see Core Settings → Email).", "success")
        return redirect(url_for("routes.core_module_settings", tab="email"))

    return redirect(url_for("routes.core_module_settings", tab="email"))

##############################################################################
# SECTION 7: Plugin Management (Admin Only)
##############################################################################


# Flash categories shown on /plugins only (avoids stacking login/core/version messages).
PLUGIN_PAGE_FLASH_SUCCESS = "plugins_page_success"
PLUGIN_PAGE_DANGER = "plugins_page_danger"
PLUGIN_PAGE_ERROR = "plugins_page_error"


@routes.route('/plugins', methods=['GET'])
@login_required
def plugins():
    if not customer_super_admin_only():
        return redirect(url_for('routes.dashboard'))

    pm = PluginManager(PLUGINS_FOLDER)
    um = UpdateManager()

    local_plugins = pm.get_all_plugins() or []
    try:
        marketplace_plugins = um.get_available_plugins_details()
    except Exception as e:
        print(f"[ERROR] Marketplace fetch failed: {e}")
        marketplace_plugins = []

    core_manifest = pm.get_core_manifest() or {}

    return render_template(
        'plugins.html',
        plugins=local_plugins,
        available_plugins=marketplace_plugins,
        config=core_manifest
    )


@routes.route('/plugin/<plugin_system_name>/install-remote', methods=['POST'])
@login_required
def install_plugin_remote(plugin_system_name):
    if not customer_super_admin_only():
        return redirect(url_for('routes.dashboard'))

    pm = PluginManager(PLUGINS_FOLDER)
    um = UpdateManager()

    try:
        # NOTE: This is remote install via UpdateManager.
        # If it changes routes/blueprints, schedule restart on success.
        um.install_plugin(plugin_system_name)
        flash(
            f'{plugin_system_name} has been installed successfully!',
            PLUGIN_PAGE_FLASH_SUCCESS,
        )
        schedule_restart_flag(
            pm.config_dir, reason=f"remote install {plugin_system_name}")
    except Exception as e:
        flash(
            f'Failed to install {plugin_system_name}: {e}', PLUGIN_PAGE_DANGER)

    return redirect(url_for('routes.plugins'))


@routes.route('/plugin/<plugin_system_name>/settings', methods=['GET', 'POST'])
@login_required
def plugin_settings(plugin_system_name):
    """
    Manage settings for a specific plugin (Admin Only).
    IMPORTANT: settings updates should NOT restart the app.
    """
    if not customer_super_admin_only():
        return redirect(url_for('routes.dashboard'))

    plugin_manager = PluginManager(PLUGINS_FOLDER)
    manifest = plugin_manager.get_plugin(plugin_system_name)
    if not manifest:
        flash(
            f'Plugin manifest for {plugin_system_name} not found.',
            PLUGIN_PAGE_ERROR,
        )
        return redirect(url_for('routes.plugins'))

    if request.method == 'POST':
        plugin_manager.update_plugin_settings(plugin_system_name, request.form)
        flash(
            f'{plugin_system_name} settings updated successfully!',
            PLUGIN_PAGE_FLASH_SUCCESS,
        )

    core_manifest = plugin_manager.get_core_manifest() or {}
    return render_template(
        'plugin_settings.html',
        plugin_name=plugin_system_name,
        settings=manifest,
        config=core_manifest
    )


@routes.route('/plugin/<plugin_system_name>/enable', methods=['POST'])
@login_required
def enable_plugin(plugin_system_name):
    """
    Enable a plugin (Admin Only).
    Schedules a restart on success (routes/blueprints may change).
    """
    if not customer_super_admin_only():
        return redirect(url_for('routes.dashboard'))

    plugin_manager = PluginManager(PLUGINS_FOLDER)

    # Support both return styles:
    # - legacy: True/False
    # - newer: (True/False, message)
    result = plugin_manager.enable_plugin(plugin_system_name)
    if isinstance(result, tuple):
        success, msg = result[0], result[1]
    else:
        success, msg = bool(result), None

    if success:
        flash(
            msg or f'{plugin_system_name} has been enabled.',
            PLUGIN_PAGE_FLASH_SUCCESS,
        )
        schedule_restart_flag(plugin_manager.config_dir,
                              reason=f"enabled {plugin_system_name}")
    else:
        flash(
            msg or f'{plugin_system_name} is not installed or manifest is missing.',
            PLUGIN_PAGE_ERROR,
        )

    return redirect(url_for('routes.plugins'))


@routes.route('/plugin/<plugin_system_name>/disable', methods=['POST'])
@login_required
def disable_plugin(plugin_system_name):
    """
    Disable a plugin (Admin Only).
    Schedules a restart on success (routes/blueprints may change).
    """
    if not customer_super_admin_only():
        return redirect(url_for('routes.dashboard'))

    plugin_manager = PluginManager(PLUGINS_FOLDER)

    result = plugin_manager.disable_plugin(plugin_system_name)
    if isinstance(result, tuple):
        success, msg = result[0], result[1]
    else:
        success, msg = bool(result), None

    if success:
        flash(
            msg or f'{plugin_system_name} has been disabled.',
            PLUGIN_PAGE_FLASH_SUCCESS,
        )
        schedule_restart_flag(plugin_manager.config_dir,
                              reason=f"disabled {plugin_system_name}")
    else:
        flash(
            msg or f'{plugin_system_name} is not installed or manifest is missing.',
            PLUGIN_PAGE_ERROR,
        )

    return redirect(url_for('routes.plugins'))


@routes.route('/plugin/<plugin_system_name>/install', methods=['POST'])
@login_required
def install_plugin(plugin_system_name):
    """
    Install a plugin (Admin Only).
    Schedules a restart on success (routes/blueprints may change).
    """
    if not customer_super_admin_only():
        return redirect(url_for('routes.dashboard'))

    plugin_manager = PluginManager(PLUGINS_FOLDER)

    result = plugin_manager.install_plugin(plugin_system_name)
    if isinstance(result, tuple):
        success, msg = result[0], result[1]
    else:
        success, msg = bool(result), None

    if success:
        flash(
            msg or f'{plugin_system_name} has been installed successfully!',
            PLUGIN_PAGE_FLASH_SUCCESS,
        )
        schedule_restart_flag(plugin_manager.config_dir,
                              reason=f"installed {plugin_system_name}")
    else:
        flash(
            msg or f'Failed to install {plugin_system_name}. Please check the plugin files.',
            PLUGIN_PAGE_ERROR,
        )

    return redirect(url_for('routes.plugins'))


@routes.route('/plugin/<plugin_system_name>/uninstall', methods=['POST'])
@login_required
def uninstall_plugin(plugin_system_name):
    """
    Uninstall a plugin (Admin Only).
    Schedules a restart on success (routes/blueprints may change).
    """
    if not customer_super_admin_only():
        return redirect(url_for('routes.dashboard'))

    plugin_manager = PluginManager(PLUGINS_FOLDER)

    result = plugin_manager.uninstall_plugin(plugin_system_name)
    if isinstance(result, tuple):
        success, msg = result[0], result[1]
    else:
        success, msg = bool(result), None

    if success:
        flash(
            msg or f'{plugin_system_name} has been uninstalled.',
            PLUGIN_PAGE_FLASH_SUCCESS,
        )
        schedule_restart_flag(plugin_manager.config_dir,
                              reason=f"uninstalled {plugin_system_name}")
    else:
        flash(
            msg or f'{plugin_system_name} is not installed or manifest is missing.',
            PLUGIN_PAGE_ERROR,
        )

    return redirect(url_for('routes.plugins'))


##############################################################################
# SECTION 8: Static File Serving for Plugins
##############################################################################

_PLUGIN_NAME_SAFE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,127}$")


@routes.route('/plugins/<plugin_name>/<filename>')
@login_required
def serve_plugin_icon(plugin_name, filename):
    """
    Serve static plugin assets (e.g. icons). Admin-only; path-safe plugin and file names.
    """
    if getattr(current_user, "role", None) not in (
        "admin",
        "superuser",
        "support_break_glass",
    ):
        abort(403)
    if not _PLUGIN_NAME_SAFE.match(plugin_name or ""):
        abort(404)
    safe_file = secure_filename(filename or "")
    if not safe_file:
        abort(400)
    plugin_folder = os.path.join(PLUGINS_FOLDER, plugin_name)
    if not os.path.isdir(plugin_folder):
        abort(404)
    return send_from_directory(plugin_folder, safe_file)
##############################################################################
# SECTION 9: API For PWA
##############################################################################


def _api_jwt_role_from_contractor_role(tb_role: str) -> str:
    r = (tb_role or "").strip().lower()
    if r in ("superuser", "admin", "clinical_lead", "support_break_glass"):
        return r
    return "crew"


api_bp = Blueprint('api', __name__, url_prefix='/api')


@api_bp.route('/login', methods=['POST'])
def api_login():
    """
    API login: JWT-only, no session or cookies. For mobile, MDT, module agents, and other API clients.
    Send JSON: {"username": "...", "password": "..."}. Use the returned token as Authorization: Bearer <token>.

    Accepts Sparrow ``users.username`` (case-insensitive) or contractor ``tb_contractors`` by
    **username** (case-insensitive), or **email** if no username row matches (same habit as the
    employee portal; MDT/Cura often send email). If one login string matches both a Sparrow user and
    a contractor, login is rejected **unless** they are the same person (``users.contractor_id`` set
    to that contractor, or same non-empty email); then a **Sparrow user** JWT is issued after
    verifying the **user** password.

    The JWT always includes stable claims ``sub``, ``username``, ``role``, ``iat``, and ``exp``
    (see ``app.auth_jwt``). Module JSON APIs should decode with ``decode_session_token`` and must
    not assume extra custom claims unless the issuer adds them separately. Contractor tokens use
    ``sub`` of the form ``c:<contractor_id>`` so IDs do not collide with ``users.id``.
    """
    data = request.get_json(silent=True) or {}
    login_key = (data.get("username") or "").strip().lower()
    password = data.get("password") or ""

    if not login_key or not password:
        return jsonify({"status": "error", "message": "Username and password required."}), 400

    user_data = User.get_user_by_username_ci(login_key)
    contractor_rows = find_tb_contractors_for_api_login(login_key)

    if len(contractor_rows) > 1:
        return jsonify({"status": "error", "message": "Invalid credentials."}), 401

    if user_data and contractor_rows:
        if not linked_user_contractor_pair(user_data, contractor_rows[0]):
            return jsonify({
                "status": "error",
                "message": (
                    "This login matches both a Sparrow user and a contractor. "
                    "Change one of the accounts so the login name is unique, or link the user to "
                    "the contractor (users.contractor_id / matching email), then try again."
                ),
            }), 409

    if user_data:
        if not AuthManager.verify_password(user_data["password_hash"], password):
            return jsonify({"status": "error", "message": "Invalid credentials."}), 401

        from app.support_access import support_login_blocked_reason

        _api_sup = support_login_blocked_reason(user_data)
        if _api_sup:
            return jsonify({"status": "error", "message": _api_sup}), 403

        token = encode_session_token(
            user_data["id"],
            user_data["username"],
            user_data["role"],
        )
        if not token:
            return jsonify({
                "status": "error",
                "message": "JWT not available. Install PyJWT on the server for API login.",
            }), 503

        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET last_login = %s WHERE id = %s",
                (datetime.now(), user_data['id'])
            )
            conn.commit()
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            conn.close()

        log_security_event(
            "api_login_success",
            user_id=user_data.get("id"),
            role=user_data.get("role"),
        )
        try:
            from app.plugins.compliance_audit_module.login_audit_hook import (
                record_compliance_login,
            )

            record_compliance_login(
                success=True,
                channel="api_jwt",
                user_id=str(user_data.get("id")),
                username=user_data.get("username"),
                ip=request.remote_addr,
                user_agent=request.headers.get("User-Agent"),
            )
        except Exception:
            pass
        if int(user_data.get("billable_exempt") or 0):
            log_security_event(
                "support_shadow_api_login",
                user_id=user_data.get("id"),
                username=user_data.get("username"),
            )

        permissions = []
        if user_data.get('permissions'):
            try:
                permissions = json.loads(user_data['permissions'])
            except Exception:
                permissions = []

        return jsonify({
            "status": "success",
            "message": f"Welcome back, {user_data.get('first_name', user_data['username'])}!",
            "token": token,
            "user": {
                "id": user_data['id'],
                "username": user_data['username'],
                "email": user_data['email'],
                "role": user_data['role'],
                "first_name": user_data.get('first_name', ''),
                "last_name": user_data.get('last_name', ''),
                "theme": user_data.get('theme', 'default'),
                "permissions": permissions,
            },
        }), 200

    if contractor_rows:
        c = contractor_rows[0]
        cid = int(c["id"])
        ph = c.get("password_hash")
        try:
            pw_ok = bool(ph) and AuthManager.verify_password(ph, password)
        except (ValueError, TypeError, AttributeError):
            pw_ok = False
        if not pw_ok:
            return jsonify({"status": "error", "message": "Invalid credentials."}), 401
        if str(c.get("status") or "").lower() not in ("active", "1", "true", "yes"):
            return jsonify({"status": "error", "message": "Invalid credentials."}), 401

        tb_role = get_contractor_effective_role(cid)
        jwt_role = _api_jwt_role_from_contractor_role(tb_role)
        email = (c.get("email") or "").strip()
        canonical_username = (
            c.get("username") or "").strip().lower() or login_key
        token = encode_session_token(f"c:{cid}", canonical_username, jwt_role)
        if not token:
            return jsonify({
                "status": "error",
                "message": "JWT not available. Install PyJWT on the server for API login.",
            }), 503

        log_security_event(
            "api_login_success",
            contractor_id=cid,
            role=jwt_role,
        )
        try:
            from app.plugins.compliance_audit_module.login_audit_hook import (
                record_compliance_login,
            )

            record_compliance_login(
                success=True,
                channel="api_jwt_contractor",
                contractor_id=int(cid),
                username=canonical_username,
                ip=request.remote_addr,
                user_agent=request.headers.get("User-Agent"),
            )
        except Exception:
            pass
        display_name = (c.get("name") or "").strip()
        parts = display_name.split(None, 1) if display_name else []
        first_name = parts[0] if parts else ""
        last_name = parts[1] if len(parts) > 1 else ""
        greet = first_name or display_name or canonical_username

        return jsonify({
            "status": "success",
            "message": f"Welcome back, {greet}!",
            "token": token,
            "user": {
                "id": cid,
                "username": (c.get("username") or "").strip() or canonical_username,
                "email": email,
                "role": jwt_role,
                "first_name": first_name,
                "last_name": last_name,
                "theme": "default",
                "permissions": [],
            },
        }), 200

    try:
        from app.plugins.compliance_audit_module.login_audit_hook import (
            record_compliance_login,
        )

        record_compliance_login(
            success=False,
            channel="api_jwt",
            username=login_key,
            ip=request.remote_addr,
            user_agent=request.headers.get("User-Agent"),
        )
    except Exception:
        pass
    return jsonify({"status": "error", "message": "Invalid credentials."}), 401


@api_bp.route('/logout', methods=['POST'])
def api_logout():
    """
    API logout: no server session to clear (API login is JWT-only). Client should discard the token.
    """
    log_security_event("api_logout_requested")
    return jsonify({"status": "success", "message": "You have been logged out."}), 200


@api_bp.route('/ping', methods=['GET'])
def ping():
    return jsonify({"status": "success", "message": "Server is reachable"}), 200
