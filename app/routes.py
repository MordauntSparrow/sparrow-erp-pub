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
      - /core/settings   -> Update site settings (theme, branding, etc.)
  6. SMTP/Email Configuration (Admin Only)
      - /smtp-config     -> Update email configuration
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

from app.create_app import update_env_var
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
from flask_login import login_user, logout_user, current_user, login_required
from app.objects import *
from werkzeug.utils import secure_filename
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    session, flash, jsonify, send_from_directory
)
import uuid
import sys
import os
import json
import threading
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Constants and folder paths
PLUGINS_FOLDER = os.path.join(os.path.dirname(__file__), "plugins")
STATIC_UPLOAD_FOLDER = os.path.join('app', 'static', 'uploads')
ALLOWED_LOGO_EXTENSIONS = {'png', 'jpg', 'jpeg', 'svg'}
ALLOWED_CSS_EXTENSIONS = {'css'}

os.makedirs(STATIC_UPLOAD_FOLDER, exist_ok=True)

# Create blueprint for core routes
routes = Blueprint('routes', __name__)

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

@routes.route('/login', methods=['GET', 'POST'])
def login():
    """
    Authenticate users via username and password.
    Loads site settings from the core manifest.
    On success, stores user data (including first_name and last_name) in session,
    updates the last_login timestamp, and redirects to the dashboard.
    """
    if current_user.is_authenticated:
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

    if request.method == 'POST':
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
            login_user(user)

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

            return redirect(url_for('routes.dashboard'))

        flash('Invalid credentials.', 'error')

    return render_template('login.html', site_settings=site_settings, config=core_manifest)


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

    if current_user.role != 'admin':
        plugins = [
            plugin for plugin in plugins
            if (not plugin.get('permission_required')) or has_permission(plugin.get('permission_required'))
        ]

    core_manifest = plugin_manager.get_core_manifest() or {}
    core_version = core_manifest.get('version', '0.0.1')

    return render_template(
        'dashboard.html',
        plugins=plugins,
        config=core_manifest,
        user=user_info,
        core_version=core_version
    )


@routes.route('/logout')
@login_required
def logout():
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

    return render_template(
        'user_management.html',
        users=users_list,
        query="",
        available_permissions=available_permissions,
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

    current_version = update_manager.get_current_version()
    latest_version = update_manager.get_latest_version()
    if current_version is None or latest_version is None:
        return jsonify({"error": "Current or latest version not found."}), 500

    core_update_available = current_version < latest_version
    update_status = update_manager.get_update_status()
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
        }
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

    if request.method == 'POST':
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

        # Write updated config to manifest
        with open(manifest_path, 'w', encoding="utf-8") as f:
            json.dump(config_data, f, indent=4)

        # Refresh session settings
        core_manifest = plugin_manager.get_core_manifest() or {}
        session['site_settings'] = core_manifest.get('site_settings', {
            'company_name': 'Sparrow ERP',
            'branding': 'name',
            'logo_path': ''
        })

        flash("Core module settings updated successfully!", 'success')

    core_manifest = plugin_manager.get_core_manifest() or {}
    return render_template('core_module_settings.html', config=core_manifest)


##############################################################################
# SECTION 6: SMTP/Email Configuration (Admin Only)
##############################################################################

@routes.route('/smtp-config', methods=['GET', 'POST'])
@login_required
def email_config():
    if not admin_only():
        return redirect(url_for('routes.dashboard'))

    if request.method == 'POST':
        host = request.form.get("host", "").strip()
        port = request.form.get("port", "").strip()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        use_tls = request.form.get("use_tls", "false").lower() == "true"

        update_env_var("SMTP_HOST", host)
        update_env_var("SMTP_PORT", port)
        update_env_var("SMTP_USERNAME", username)
        update_env_var("SMTP_PASSWORD", password)
        update_env_var("SMTP_USE_TLS", str(use_tls).lower())

        flash("SMTP configuration updated via .env", "success")
        return redirect(url_for('routes.email_config'))

    current_email_config = {
        "host": os.environ.get("SMTP_HOST", ""),
        "port": os.environ.get("SMTP_PORT", ""),
        "username": os.environ.get("SMTP_USERNAME", ""),
        "password": os.environ.get("SMTP_PASSWORD", ""),
        "use_tls": os.environ.get("SMTP_USE_TLS", "true").lower() == "true"
    }

    plugin_manager = PluginManager(PLUGINS_FOLDER)
    core_manifest = plugin_manager.get_core_manifest() or {}

    return render_template(
        "email_config.html",
        email_config=current_email_config,
        config=core_manifest
    )

##############################################################################
# SECTION 7: Plugin Management (Admin Only)
##############################################################################


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
        flash(f'{plugin_system_name} has been installed successfully!', 'success')
        schedule_restart_flag(
            pm.config_dir, reason=f"remote install {plugin_system_name}")
    except Exception as e:
        flash(f'Failed to install {plugin_system_name}: {e}', 'danger')

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
        flash(f'Plugin manifest for {plugin_system_name} not found.', 'error')
        return redirect(url_for('routes.plugins'))

    if request.method == 'POST':
        plugin_manager.update_plugin_settings(plugin_system_name, request.form)
        flash(f'{plugin_system_name} settings updated successfully!', 'success')

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
        flash(msg or f'{plugin_system_name} has been enabled.', 'success')
        schedule_restart_flag(plugin_manager.config_dir,
                              reason=f"enabled {plugin_system_name}")
    else:
        flash(
            msg or f'{plugin_system_name} is not installed or manifest is missing.', 'error')

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
        flash(msg or f'{plugin_system_name} has been disabled.', 'success')
        schedule_restart_flag(plugin_manager.config_dir,
                              reason=f"disabled {plugin_system_name}")
    else:
        flash(
            msg or f'{plugin_system_name} is not installed or manifest is missing.', 'error')

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
            msg or f'{plugin_system_name} has been installed successfully!', 'success')
        schedule_restart_flag(plugin_manager.config_dir,
                              reason=f"installed {plugin_system_name}")
    else:
        flash(
            msg or f'Failed to install {plugin_system_name}. Please check the plugin files.', 'error')

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
        flash(msg or f'{plugin_system_name} has been uninstalled.', 'success')
        schedule_restart_flag(plugin_manager.config_dir,
                              reason=f"uninstalled {plugin_system_name}")
    else:
        flash(
            msg or f'{plugin_system_name} is not installed or manifest is missing.', 'error')

    return redirect(url_for('routes.plugins'))


##############################################################################
# SECTION 8: Static File Serving for Plugins
##############################################################################

@routes.route('/plugins/<plugin_name>/<filename>')
def serve_plugin_icon(plugin_name, filename):
    """
    Serve static plugin icon files.
    """
    plugin_folder = os.path.join(os.getcwd(), 'app', 'plugins', plugin_name)
    return send_from_directory(plugin_folder, filename)
##############################################################################
# SECTION 9: API For PWA
##############################################################################


api_bp = Blueprint('api', __name__, url_prefix='/api')


@api_bp.route('/login', methods=['POST'])
def api_login():
    # If already authenticated, clear session first
    if current_user.is_authenticated:
        logout_user()

    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not username or not password:
        return jsonify({"status": "error", "message": "Username and password required."}), 400

    user_data = User.get_user_by_username_raw(username)
    if not (user_data and AuthManager.verify_password(user_data["password_hash"], password)):
        return jsonify({"status": "error", "message": "Invalid credentials."}), 401

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
    login_user(user)

    session['first_name'] = user_data.get('first_name', '')
    session['last_name'] = user_data.get('last_name', '')
    session['theme'] = user_data.get('theme', 'default')

    # Update last login timestamp
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE users SET last_login = %s WHERE id = %s",
        (datetime.now(), user_data['id'])
    )
    conn.commit()
    cursor.close()
    conn.close()

    # Provide site settings/core manifest for PWA clients
    plugin_manager = PluginManager(PLUGINS_FOLDER)
    core_manifest = plugin_manager.get_core_manifest() or {}
    site_settings = core_manifest.get('site_settings', {})

    response_data = {
        "status": "success",
        "message": f"Welcome back, {user_data.get('first_name', user_data['username'])}!",
        "user": {
            "id": user_data['id'],
            "username": user_data['username'],
            "email": user_data['email'],
            "role": user_data['role'],
            "first_name": user_data.get('first_name', ''),
            "last_name": user_data.get('last_name', ''),
            "theme": user_data.get('theme', 'default'),
            "permissions": permissions
        },
        "site_settings": site_settings,
        "core_manifest": core_manifest
    }

    return jsonify(response_data), 200


@api_bp.route('/logout', methods=['POST'])
def api_logout():
    """
    API endpoint for logout. Logs out the user and returns a JSON response.
    Note: @login_required is removed to prevent redirection.
    """
    logout_user()
    return jsonify({"status": "success", "message": "You have been logged out."}), 200


@api_bp.route('/ping', methods=['GET'])
def ping():
    return jsonify({"status": "success", "message": "Server is reachable"}), 200
