"""
app/routes.py

Core routes for Sparrow ERP.

Sections:
  1. Authentication & Password Reset
      - /login
      - /reset-password and /reset-password/<token>
  2. Dashboard & Logout
      - / (dashboard)
      - /logout
  3. User Management (Admin Only)
      - /users           -> Combined management page (live search + modals for add/edit/delete)
      - /users/search    -> AJAX endpoint for live search
      - /users/add       -> Process adding a new user (includes first_name and last_name)
      - /users/edit/<user_id>  -> Process editing a user (updates email, first_name, last_name, role, permissions, and optionally password)
      - /users/delete/<user_id> -> Delete a user
  4. Version Management (Admin Only)
      - /version         -> View/apply updates
  5. Core Module Settings (Admin Only)
      - /core/settings   -> Tabbed: manifest (branding, theme, AI model id) + .env (SMTP, GitLab, LLM, security)
  6. SMTP/Email Configuration (Admin Only)
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

All admin-only routes are protected using the helper function admin_only().
The core manifest is passed as "config" to all templates.
"""

from app.ai_config import (
    CHAT_MODEL_DROPDOWN_CHOICES,
    chat_model_dropdown_value_set,
    sanitize_chat_model_id,
)
from app.create_app import update_env_var
from app.auth_jwt import encode_session_token
from app.compliance_audit import log_security_event, pseudonymous_username_hint
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
from flask_login import login_user, logout_user, current_user, login_required
from app.objects import *
from werkzeug.utils import secure_filename
from flask import (
    Blueprint, render_template, request, redirect, url_for,
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
# Under Docker/Railway this path is symlinked to the volume when SPARROW_DATA_ROOT / RAILWAY_VOLUME_MOUNT_PATH is set.
STATIC_UPLOAD_FOLDER = os.path.join(_APP_PKG_DIR, "static", "uploads")
ALLOWED_LOGO_EXTENSIONS = {'png', 'jpg', 'jpeg', 'svg'}
ALLOWED_CSS_EXTENSIONS = {'css'}

os.makedirs(STATIC_UPLOAD_FOLDER, exist_ok=True)


def _env_nonempty(key: str) -> bool:
    return bool((os.environ.get(key) or "").strip())


def _settings_env_context():
    """Snapshot for Core Settings UI. GitLab read token is included for admin prefill (see integrations tab)."""
    _g_user, _g_pass = effective_gitlab_basic_credentials()
    return {
        "smtp": {
            "host": os.environ.get("SMTP_HOST", "") or "",
            "port": os.environ.get("SMTP_PORT", "") or "",
            "username": os.environ.get("SMTP_USERNAME", "") or "",
            "password_set": _env_nonempty("SMTP_PASSWORD"),
            "use_tls": (os.environ.get("SMTP_USE_TLS", "true") or "true").lower()
            == "true",
        },
        "gitlab": {
            "token_set": bool(
                (effective_gitlab_read_token() or "").strip()
            ),
            # Prefill: env or built-in official-registry defaults (same as UpdateManager).
            "token_current": effective_gitlab_read_token(),
            "username": _g_user,
            "password_set": bool(_g_pass),
            "project_id": effective_core_project_id(),
            "core_manifest_url": os.environ.get("CORE_MANIFEST_REMOTE_URL", "") or "",
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
        "infra": {
            "redis_url": os.environ.get("REDIS_URL") or os.environ.get("REDIS_URLS") or "",
        },
        "security": {
            "secret_set": _env_nonempty("SECRET_KEY"),
            "jwt_set": _env_nonempty("JWT_SECRET_KEY"),
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
    ut = (form.get("smtp_use_tls") or form.get("use_tls") or "true").lower()
    use_tls = ut == "true"
    update_env_var("SMTP_USE_TLS", "true" if use_tls else "false")


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

    site_settings = core_manifest.get('site_settings', {
        'company_name': 'Sparrow ERP',
        'branding': 'name',
        'logo_path': ''
    })

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

            user_data = User.get_user_by_username_raw(username)
            if user_data and AuthManager.verify_password(user_data["password_hash"], password):
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

                if user_data['role'] == "crew":
                    return redirect('/plugin/ventus_response_module/response')

                if login_next_value:
                    return redirect(login_next_value)

                return redirect(url_for('routes.dashboard'))

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
        if user_data:
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
    return render_template('reset_password_request.html', config=core_manifest)


@routes.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    plugin_manager = PluginManager(PLUGINS_FOLDER)
    core_manifest = plugin_manager.get_core_manifest() or {}

    email = verify_reset_token(token)
    if not email:
        flash("The password reset link is invalid or has expired.", "danger")
        return redirect(url_for('routes.reset_password_request'))

    if request.method == 'POST':
        new_password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')

        if new_password != confirm_password:
            flash("Passwords do not match.", "danger")
            return render_template('reset_password.html', token=token, config=core_manifest)

        user_data = User.get_user_by_email(email)
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

    return render_template('reset_password.html', token=token, config=core_manifest)


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
            current_user, _manifest_for_system(p.get("system_name", "")), p.get("system_name", "")
        )
    ]

    core_manifest = plugin_manager.get_core_manifest() or {}
    core_version = core_manifest.get('version', '0.0.1')

    by_cat = defaultdict(list)
    for p in plugins:
        by_cat[p.get('dashboard_category') or 'Modules'].append(p)
    plugin_categories = sorted(by_cat.items(), key=lambda kv: kv[0].lower())

    return render_template(
        'dashboard.html',
        plugins=plugins,
        plugin_categories=plugin_categories,
        config=core_manifest,
        user=user_info,
        core_version=core_version
    )


@routes.route('/logout')
@login_required
def logout():
    try:
        log_security_event("session_logout", user_id=getattr(current_user, "id", None))
    except Exception:
        pass
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('routes.login'))


##############################################################################
# SECTION 3: User Management (Admin Only)
##############################################################################

@routes.route('/users/search', methods=['GET'])
@login_required
def search_users():
    if not admin_only():
        return jsonify({"error": "Access denied"}), 403

    query = request.args.get('q', '').strip()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    if query:
        search_param = f"%{query}%"
        cursor.execute("""
            SELECT id, username, email, role, permissions, first_name, last_name, created_at, last_login
            FROM users
            WHERE username LIKE %s OR email LIKE %s
        """, (search_param, search_param))
    else:
        cursor.execute("""
            SELECT id, username, email, role, permissions, first_name, last_name, created_at, last_login
            FROM users
        """)

    users_list = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(users_list)


@routes.route('/users', methods=['GET'])
@login_required
def users():
    if not admin_only():
        return redirect(url_for('routes.dashboard'))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
      SELECT id, username, email, role, permissions, first_name, last_name, created_at, last_login
      FROM users
    """)
    users_list = cursor.fetchall()
    cursor.close()
    conn.close()

    plugin_manager = PluginManager(PLUGINS_FOLDER)
    available_permissions = plugin_manager.get_available_permissions()
    core_manifest = plugin_manager.get_core_manifest() or {}

    try:
        from app.permissions_registry import (
            collect_permission_catalog,
            default_permission_ids_for_role,
        )

        permission_catalog = collect_permission_catalog(plugin_manager)
        role_default_permissions = {
            r: default_permission_ids_for_role(r, permission_catalog)
            for r in (
                "admin",
                "superuser",
                "staff",
                "crew",
                "clinical_lead",
            )
        }
    except Exception as exc:
        print(f"[WARN] permission catalog: {exc}")
        permission_catalog = [{"id": p, "label": p, "kind": "module_access", "plugin": ""} for p in available_permissions]
        role_default_permissions = {}

    return render_template(
        'user_management.html',
        users=users_list,
        query="",
        available_permissions=available_permissions,
        permission_catalog=permission_catalog,
        role_default_permissions=role_default_permissions,
        config=core_manifest
    )


@routes.route('/users/add', methods=['POST'])
@login_required
def add_user():
    if not admin_only():
        return redirect(url_for('routes.dashboard'))

    username = request.form.get('username')
    email = request.form.get('email')
    role = request.form.get('role')
    password = request.form.get('password')
    confirm_password = request.form.get('confirm_password')
    first_name = request.form.get('first_name')
    last_name = request.form.get('last_name')
    personal_pin = request.form.get('personal_pin')

    if password != confirm_password:
        flash("Passwords do not match.", "danger")
        return redirect(url_for("routes.users"))

    if User.get_user_by_username_raw(username):
        flash("Username already exists.", "danger")
        return redirect(url_for("routes.users"))

    if User.get_user_by_email(email):
        flash("Email already exists.", "danger")
        return redirect(url_for("routes.users"))

    try:
        from app.permissions_registry import (
            editor_may_assign_superuser_role,
            is_superuser_username_allowed,
        )

        if str(role or "").lower() == "superuser":
            if not editor_may_assign_superuser_role(current_user):
                flash("Only a superuser can assign the superuser role.", "danger")
                return redirect(url_for("routes.users"))
            if not is_superuser_username_allowed(username):
                flash(
                    "This username is not allowed to hold the superuser role. "
                    "Set SPARROW_SUPERUSER_USERNAMES if appropriate, or choose another role.",
                    "danger",
                )
                return redirect(url_for("routes.users"))
    except Exception as exc:
        print(f"[WARN] superuser validation: {exc}")

    new_hash = AuthManager.hash_password(password)
    new_permissions = request.form.getlist('permissions')
    new_permissions_json = json.dumps(new_permissions)
    user_id = str(uuid.uuid4())

    personal_pin_hash = None
    if personal_pin and personal_pin.strip():
        personal_pin_hash = AuthManager.hash_password(personal_pin.strip())

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO users (id, username, email, password_hash, role, permissions, first_name, last_name, personal_pin_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (user_id, username, email, new_hash, role, new_permissions_json, first_name, last_name, personal_pin_hash))
    conn.commit()
    cursor.close()
    conn.close()

    flash("New user added successfully.", "success")
    return redirect(url_for("routes.users"))


@routes.route('/users/edit/<user_id>', methods=['POST'])
@login_required
def edit_user(user_id):
    if not admin_only():
        return redirect(url_for('routes.dashboard'))

    new_email = request.form.get("email")
    new_role = request.form.get("role")
    new_permissions = request.form.getlist("permissions")

    try:
        from app.permissions_registry import (
            editor_may_assign_superuser_role,
            is_superuser_username_allowed,
        )

        conn_chk = get_db_connection()
        cur_chk = conn_chk.cursor(dictionary=True)
        cur_chk.execute(
            "SELECT username FROM users WHERE id = %s LIMIT 1", (user_id,)
        )
        row_u = cur_chk.fetchone()
        cur_chk.close()
        conn_chk.close()
        target_username = (row_u or {}).get("username") or ""

        if str(new_role or "").lower() == "superuser":
            if not editor_may_assign_superuser_role(current_user):
                flash("Only a superuser can assign the superuser role.", "danger")
                return redirect(url_for("routes.users"))
            if not is_superuser_username_allowed(target_username):
                flash(
                    "This user cannot be set to superuser. Allowed names are configured via SPARROW_SUPERUSER_USERNAMES.",
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
    new_personal_pin = request.form.get("personal_pin")

    conn = get_db_connection()
    cursor = conn.cursor()

    if new_password:
        if new_password != confirm_new_password:
            flash("New passwords do not match.", "danger")
            return redirect(url_for("routes.users"))

        new_hash = AuthManager.hash_password(new_password)

        if new_personal_pin and new_personal_pin.strip():
            new_personal_pin_hash = AuthManager.hash_password(
                new_personal_pin.strip())
            cursor.execute("""
                UPDATE users
                SET email = %s, role = %s, permissions = %s, password_hash = %s, first_name = %s, last_name = %s, personal_pin_hash = %s
                WHERE id = %s
            """, (new_email, new_role, permissions_json, new_hash, new_first_name, new_last_name, new_personal_pin_hash, user_id))
        else:
            cursor.execute("""
                UPDATE users
                SET email = %s, role = %s, permissions = %s, password_hash = %s, first_name = %s, last_name = %s
                WHERE id = %s
            """, (new_email, new_role, permissions_json, new_hash, new_first_name, new_last_name, user_id))
    else:
        if new_personal_pin and new_personal_pin.strip():
            new_personal_pin_hash = AuthManager.hash_password(
                new_personal_pin.strip())
            cursor.execute("""
                UPDATE users
                SET email = %s, role = %s, permissions = %s, first_name = %s, last_name = %s, personal_pin_hash = %s
                WHERE id = %s
            """, (new_email, new_role, permissions_json, new_first_name, new_last_name, new_personal_pin_hash, user_id))
        else:
            cursor.execute("""
                UPDATE users
                SET email = %s, role = %s, permissions = %s, first_name = %s, last_name = %s
                WHERE id = %s
            """, (new_email, new_role, permissions_json, new_first_name, new_last_name, user_id))

    conn.commit()
    cursor.close()
    conn.close()
    flash("User updated successfully.", "success")
    return redirect(url_for("routes.users"))


@routes.route('/users/delete/<user_id>', methods=['POST'])
@login_required
def delete_user(user_id):
    if not admin_only():
        return redirect(url_for('routes.dashboard'))

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
    conn.commit()
    cursor.close()
    conn.close()

    flash("User deleted successfully.", "success")
    return redirect(url_for("routes.users"))


def admin_only():
    if current_user.role != 'admin' and current_user.role != 'superuser':
        flash("Access denied: Admins only.", "danger")
        return False
    return True


##############################################################################
# SECTION 4: Version Management (Admin Only)
##############################################################################

@routes.route('/version', methods=['GET', 'POST'])
@login_required
def version():
    if not admin_only():
        return redirect(url_for('routes.dashboard'))

    update_manager = UpdateManager()
    plugin_manager = PluginManager(PLUGINS_FOLDER)

    if request.method == 'POST':
        update_type = request.form['update_type']  # 'core' or 'plugin'
        plugin_name = request.form.get('plugin_name')
        scheduled_time = request.form.get('scheduled_time')

        if scheduled_time:
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
    if not admin_only():
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
    if not admin_only():
        return redirect(url_for('routes.dashboard'))

    plugin_manager = PluginManager(PLUGINS_FOLDER)
    manifest_path = plugin_manager.get_core_manifest_path()

    default_config = {
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
            "chat_model": "",
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

    redirect_tab = "general"
    if request.method == 'POST':
        section = (request.form.get("settings_section") or "general").strip().lower()

        if section == "general":
            redirect_tab = "general"
            config_data['site_settings']['company_name'] = request.form.get(
                'company_name')
            config_data['site_settings']['branding'] = request.form.get('branding')
            config_data['theme_settings']['theme'] = request.form.get('theme')

            logo_file = request.files.get('logo')
            if logo_file and logo_file.filename:
                if '.' in logo_file.filename and logo_file.filename.rsplit('.', 1)[1].lower() in ALLOWED_LOGO_EXTENSIONS:
                    logo_filename = secure_filename(logo_file.filename)
                    logo_path = os.path.join(STATIC_UPLOAD_FOLDER, logo_filename)
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
                raw_ai_model = (request.form.get("ai_chat_model_custom") or "").strip()
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

            with open(manifest_path, 'w', encoding="utf-8") as f:
                json.dump(config_data, f, indent=4)

            core_manifest = plugin_manager.get_core_manifest() or {}
            session['site_settings'] = core_manifest.get('site_settings', {
                'company_name': 'Sparrow ERP',
                'branding': 'name',
                'logo_path': ''
            })
            flash("General settings saved.", 'success')

        elif section == "smtp":
            redirect_tab = "email"
            _save_smtp_from_form(request.form)
            flash("Email (SMTP) settings saved to .env.", "success")

        elif section == "integrations":
            redirect_tab = "integrations"
            if request.form.get("clear_gitlab_token"):
                update_env_var("GITLAB_TOKEN", "")
            else:
                _update_secret_env_if_nonempty(
                    "GITLAB_TOKEN", request.form.get("gitlab_token"))

            update_env_var(
                "GITLAB_USERNAME",
                (request.form.get("gitlab_username") or "").strip(),
            )
            if request.form.get("clear_gitlab_password"):
                update_env_var("GITLAB_PASSWORD", "")
            else:
                _update_secret_env_if_nonempty(
                    "GITLAB_PASSWORD", request.form.get("gitlab_password"))

            gpid = (request.form.get("gitlab_project_id") or "").strip()
            update_env_var("GITLAB_PROJECT_ID", gpid)
            update_env_var("CORE_PROJECT_ID", gpid)
            update_env_var(
                "CORE_MANIFEST_REMOTE_URL",
                (request.form.get("core_manifest_remote_url") or "").strip(),
            )
            update_env_var("REDIS_URL", (request.form.get("redis_url") or "").strip())
            flash("Integrations saved to .env.", "success")

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
            preset = (request.form.get("openai_base_url_preset") or "").strip().lower()
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

        else:
            flash("Unknown settings section.", "danger")
            redirect_tab = "general"

        return redirect(url_for(
            "routes.core_module_settings",
            tab=redirect_tab,
        ))

    core_manifest = plugin_manager.get_core_manifest() or {}
    active_tab = (request.args.get("tab") or "general").strip().lower()
    if active_tab not in (
        "general", "email", "integrations", "llm", "security",
    ):
        active_tab = "general"
    return render_template(
        "core_module_settings.html",
        config=core_manifest,
        env_ctx=_settings_env_context(),
        active_tab=active_tab,
        chat_model_choices=CHAT_MODEL_DROPDOWN_CHOICES,
        chat_model_choice_values=chat_model_dropdown_value_set(),
    )


##############################################################################
# SECTION 6: SMTP/Email Configuration (Admin Only)
##############################################################################

@routes.route('/smtp-config', methods=['GET', 'POST'])
@login_required
def email_config():
    """Legacy URL: email / SMTP is configured under Core Settings → Email tab."""
    if not admin_only():
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
    if not admin_only():
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
    if not admin_only():
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
        flash(f'Failed to install {plugin_system_name}: {e}', PLUGIN_PAGE_DANGER)

    return redirect(url_for('routes.plugins'))


@routes.route('/plugin/<plugin_system_name>/settings', methods=['GET', 'POST'])
@login_required
def plugin_settings(plugin_system_name):
    """
    Manage settings for a specific plugin (Admin Only).
    IMPORTANT: settings updates should NOT restart the app.
    """
    if not admin_only():
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
    if not admin_only():
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
    if not admin_only():
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
    if not admin_only():
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
    if not admin_only():
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
    if getattr(current_user, "role", None) not in ("admin", "superuser"):
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


api_bp = Blueprint('api', __name__, url_prefix='/api')


@api_bp.route('/login', methods=['POST'])
def api_login():
    """
    API login: JWT-only, no session or cookies. For mobile, MDT, and other API clients.
    Send JSON: {"username": "...", "password": "..."}. Use the returned token as Authorization: Bearer <token>.
    """
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not username or not password:
        return jsonify({"status": "error", "message": "Username and password required."}), 400

    user_data = User.get_user_by_username_raw(username)
    if not (user_data and AuthManager.verify_password(user_data["password_hash"], password)):
        return jsonify({"status": "error", "message": "Invalid credentials."}), 401

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

    # Update last login timestamp (audit only; no session)
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
