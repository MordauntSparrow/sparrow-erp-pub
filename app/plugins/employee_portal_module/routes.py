from functools import wraps
import logging
import os
from flask import (
    Blueprint, request, jsonify,
    render_template, redirect, url_for, flash, session, current_app
)
from flask_login import current_user, login_required
from app.objects import get_db_connection, AuthManager, PluginManager

from .services import (
    get_messages,
    get_todos,
    count_pending_todos,
    get_module_links,
    get_pending_counts,
    get_pending_training_count,
    is_scheduling_enabled,
    safe_next_url,
    safe_profile_picture_path,
    get_ep_setting,
    set_ep_setting,
    get_contractor_theme,
    set_contractor_theme,
    admin_search_contractors,
    admin_list_contractors_for_select,
    admin_list_messages,
    admin_send_message,
    admin_soft_delete_message,
    admin_restore_message,
    admin_list_todos,
    admin_create_todo,
    admin_get_todo,
    admin_update_todo,
    admin_set_todo_complete,
    admin_bulk_complete_todos,
    admin_get_portal_stats,
    admin_get_report_stats,
    get_dashboard_data_for_contractor,
    get_message_by_id_for_contractor,
    mark_message_read,
)

logger = logging.getLogger(__name__)


def _get_website_settings():
    """Return website_settings for templates (website_public_base.html). From website_module or safe default."""
    try:
        from app.plugins.website_module.routes import get_website_settings
        return get_website_settings()
    except Exception:
        pass
    _keys = (
        "favicon_path", "default_og_image", "schema_json", "cookie_bar_colors", "cookie_bar_text",
        "cookie_bar_accept_text", "cookie_bar_decline_text", "cookie_policy", "analytics_code",
        "facebook_url", "instagram_url", "linkedin_url", "twitter_url", "youtube_url", "tiktok_url",
        "pinterest_url", "whatsapp_url", "threads_url", "reddit_url", "snapchat_url", "telegram_url",
        "discord_url", "tumblr_url", "github_url", "medium_url", "vimeo_url", "dribbble_url",
        "behance_url", "soundcloud_url", "slack_url", "mastodon_url",
    )
    return {k: None for k in _keys}

# =============================================================================
# Blueprints
# =============================================================================
_template_folder = os.path.join(os.path.dirname(__file__), "templates")
internal_bp = Blueprint(
    "internal_employee_portal",
    __name__,
    url_prefix="/plugin/employee_portal_module",
    template_folder=_template_folder,
)
public_bp = Blueprint(
    "public_employee_portal",
    __name__,
    url_prefix="/employee-portal",
    template_folder=_template_folder,
)

plugin_manager = PluginManager(os.path.abspath("app/plugins"))
core_manifest = plugin_manager.get_core_manifest()

# =============================================================================
# Auth (reuse tb_contractors / session tb_user)
# =============================================================================


def current_ep_user():
    return session.get("tb_user") or None


def current_ep_user_id():
    u = current_ep_user()
    return int(u["id"]) if u and u.get("id") is not None else None


def staff_required_ep(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        u = current_ep_user()
        if not u:
            target = url_for("public_employee_portal.login_page")
            next_path = safe_next_url(request.path if request else "", "")
            if next_path:
                from urllib.parse import quote
                target = target + "?next=" + quote(next_path, safe="/")
            return redirect(target)
        return view(*args, **kwargs)
    return wrapped


# =============================================================================
# Public: Login / Logout
# =============================================================================


@public_bp.get("/login")
def login_page():
    if current_ep_user():
        # Already logged in: always go to dashboard (do not use next - avoids redirect loop to /time-billing/)
        return redirect(url_for("public_employee_portal.dashboard"))
    site_settings = (core_manifest or {}).get("site_settings") or {}
    site_settings = {
        "company_name": site_settings.get("company_name") or "Employee Portal",
        "branding": site_settings.get("branding") or "name",
        "logo_path": site_settings.get("logo_path") or "",
    }
    return render_template(
        "employee_portal_module/public/login.html",
        config=core_manifest,
        site_settings=site_settings,
        website_settings=_get_website_settings(),
    )


@public_bp.post("/set-theme")
def set_theme():
    """Backward-compatible alias; core handler persists tb_contractors.ui_theme + cookie."""
    from app.contractor_ui_theme import contractor_set_theme_response

    return contractor_set_theme_response()


@public_bp.post("/login")
def login_submit():
    email = (request.form.get("email") or "").strip().lower()
    password = request.form.get("password") or ""

    if not email or not password:
        flash("Please enter your email and password.", "error")
        return redirect(url_for("public_employee_portal.login_page"))

    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT id, email, name, initials, status, password_hash,
                       profile_picture_path
                FROM tb_contractors
                WHERE email = %s
                LIMIT 1
            """, (email,))
            u = cur.fetchone()
        finally:
            cur.close()
            conn.close()

        if not u or not u.get("password_hash") or not AuthManager.verify_password(
            u["password_hash"], password
        ):
            logger.warning("Employee portal login failed for email=%s", email[:3] + "***" if len(email) > 3 else "***")
            flash("Invalid email or password.", "error")
            return redirect(url_for("public_employee_portal.login_page"))

        if str(u.get("status", "")).lower() not in ("active", "1", "true", "yes"):
            logger.info("Employee portal login rejected (inactive): contractor_id=%s", u.get("id"))
            flash("Your account is inactive. Please contact an administrator.", "error")
            return redirect(url_for("public_employee_portal.login_page"))

        if request.form.get("remember") == "on":
            session.permanent = True

        display = (u.get("name") or "").strip() or u.get("email") or ""
        try:
            from app.plugins.time_billing_module.routes import _contractor_effective_role
            role = _contractor_effective_role(int(u["id"]))
        except Exception:
            role = "staff"
        safe_avatar = safe_profile_picture_path(u.get("profile_picture_path"))
        session["tb_user"] = {
            "id": int(u["id"]),
            "email": u["email"],
            "name": display,
            "initials": (u.get("initials") or "").strip(),
            "profile_picture_path": safe_avatar,
            "role": role,
        }
        saved_theme = get_contractor_theme(int(u["id"]))
        if saved_theme in ("light", "dark", "auto"):
            session["portal_theme"] = saved_theme
        elif saved_theme == "system":
            session["portal_theme"] = "auto"
        session.modified = True  # Ensure session is persisted so time_billing and other modules see tb_user

        default_next = url_for("public_employee_portal.dashboard")
        next_param = request.form.get("next") or request.args.get("next")
        next_url = safe_next_url(next_param, default_next, request)
        logger.info("Employee portal login success: contractor_id=%s", u["id"])
        resp = redirect(next_url)
        # Fallback cookie so time-billing can restore session when session cookie is not sent (e.g. path/port)
        try:
            from itsdangerous import URLSafeTimedSerializer
            s = URLSafeTimedSerializer(current_app.secret_key)
            token = s.dumps(int(u["id"]), salt="tb_cid")
            resp.set_cookie("tb_cid", token, path="/", max_age=60 * 60 * 24 * 7, httponly=True, samesite="Lax")
        except Exception:
            pass
        return resp
    except Exception as e:
        logger.exception("Employee portal login error: %s", e)
        flash("An unexpected error occurred. Please try again.", "error")
        return redirect(url_for("public_employee_portal.login_page"))


@public_bp.get("/logout")
def logout():
    session.pop("tb_user", None)
    resp = redirect(url_for("public_employee_portal.login_page"))
    resp.delete_cookie("tb_cid", path="/")
    flash("You have been logged out.", "success")
    return resp


# =============================================================================
# Public: Launch (one-time token so time-billing gets auth without relying on session cookie)
# =============================================================================


@public_bp.get("/go/<slug>")
@staff_required_ep
def go_to_module(slug):
    """Redirect to a module with a one-time launch token so the target app can restore session."""
    if slug != "time-billing":
        return redirect(url_for("public_employee_portal.dashboard"))
    uid = current_ep_user_id()
    if not uid:
        return redirect(url_for("public_employee_portal.login_page"))
    try:
        from itsdangerous import URLSafeTimedSerializer
        s = URLSafeTimedSerializer(current_app.secret_key)
        token = s.dumps(uid, salt="tb_launch")
        return redirect(f"/time-billing/?launch={token}")
    except Exception:
        return redirect("/time-billing/")


# =============================================================================
# Public: Dashboard (mobile-first)
# =============================================================================


@public_bp.get("/")
@staff_required_ep
def dashboard():
    uid = current_ep_user_id()
    user = current_ep_user()
    if not user:
        return redirect(url_for("public_employee_portal.login_page"))

    # Use safe profile path in context (may already be set in session; ensure no path traversal)
    user = dict(user)
    user["profile_picture_path"] = safe_profile_picture_path(user.get("profile_picture_path"))

    # Default pending-only: faster query, shorter list on mobile (completed via Completed / All)
    todo_filter = request.args.get("todo_filter") or "pending"
    filter_completed = None
    if todo_filter == "pending":
        filter_completed = False
    elif todo_filter == "completed":
        filter_completed = True
    messages = get_messages(uid)
    todos = get_todos(uid, filter_completed=filter_completed)
    unread_message_count = sum(1 for m in messages if not m.get("read_at"))
    pending_todo_count = count_pending_todos(uid)
    pending_todos_truncated = filter_completed is False and pending_todo_count > len(todos)
    pending_policies, pending_hr_requests = get_pending_counts(uid)
    pending_training = get_pending_training_count(uid)
    try:
        from .services import get_dashboard_summary_context
        summary_context = get_dashboard_summary_context(uid)
    except Exception:
        summary_context = {}
    ai_summary = None
    ai_available = False
    try:
        from .portal_ai import is_ai_available, get_ai_dashboard_summary
        ai_available = is_ai_available()
        if ai_available and summary_context:
            ai_summary = get_ai_dashboard_summary(uid, summary_context)
    except Exception:
        pass
    module_links = get_module_links(plugin_manager)
    for mod in module_links:
        if mod.get("launch_slug") and mod.get("enabled"):
            mod["url"] = url_for("public_employee_portal.go_to_module", slug=mod["launch_slug"])
    scheduling_enabled = is_scheduling_enabled(plugin_manager)
    welcome_message = get_ep_setting("welcome_message")

    return render_template(
        "employee_portal_module/public/dashboard.html",
        config=core_manifest or {},
        user=user,
        messages=messages,
        todos=todos,
        todo_filter=todo_filter,
        pending_todos_truncated=pending_todos_truncated,
        unread_message_count=unread_message_count,
        pending_todo_count=pending_todo_count,
        welcome_message=welcome_message,
        module_links=module_links,
        pending_policies=pending_policies,
        pending_hr_requests=pending_hr_requests,
        pending_training=pending_training,
        summary_context=summary_context,
        ai_summary=ai_summary,
        ai_available=ai_available,
        scheduling_enabled=scheduling_enabled,
        website_settings=_get_website_settings(),
    )


@public_bp.get("/messages/<int:msg_id>")
@staff_required_ep
def message_detail(msg_id):
    """Full message view; marks as read when viewed."""
    uid = current_ep_user_id()
    if not uid:
        return redirect(url_for("public_employee_portal.login_page"))
    msg = get_message_by_id_for_contractor(uid, msg_id)
    if not msg:
        flash("Message not found.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    mark_message_read(msg_id, uid)
    msg["read_at"] = True  # show as read
    return render_template(
        "employee_portal_module/public/message_detail.html",
        config=core_manifest or {},
        user=current_ep_user(),
        message=msg,
        website_settings=_get_website_settings(),
    )


# =============================================================================
# Public: Mark message read / todo complete (optional)
# =============================================================================


@public_bp.post("/api/messages/<int:msg_id>/read")
@staff_required_ep
def api_mark_message_read(msg_id):
    if msg_id <= 0:
        return jsonify({"error": "Invalid message id"}), 400
    uid = current_ep_user_id()
    if not uid:
        return jsonify({"error": "Not authenticated"}), 401
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE ep_messages SET read_at = NOW() WHERE id = %s AND contractor_id = %s",
            (msg_id, uid),
        )
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({"error": "Message not found"}), 404
    except Exception as e:
        logger.exception("api_mark_message_read: %s", e)
        return jsonify({"error": "Server error"}), 500
    finally:
        cur.close()
        conn.close()
    return jsonify({"ok": True})


@public_bp.post("/api/todos/<int:todo_id>/complete")
@staff_required_ep
def api_todo_complete(todo_id):
    if todo_id <= 0:
        return jsonify({"error": "Invalid todo id"}), 400
    uid = current_ep_user_id()
    if not uid:
        return jsonify({"error": "Not authenticated"}), 401
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE ep_todos SET completed_at = NOW() WHERE id = %s AND contractor_id = %s",
            (todo_id, uid),
        )
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({"error": "Todo not found"}), 404
    except Exception as e:
        logger.exception("api_todo_complete: %s", e)
        return jsonify({"error": "Server error"}), 500
    finally:
        cur.close()
        conn.close()
    return jsonify({"ok": True})


# =============================================================================
# Public: Portal AI assistant
# =============================================================================


@public_bp.get("/assistant")
@staff_required_ep
def assistant_page():
    """Portal assistant chat: ask what you need to do, get guidance."""
    try:
        from .portal_ai import is_ai_available
        ai_available = is_ai_available()
    except Exception:
        ai_available = False
    return render_template(
        "employee_portal_module/public/assistant.html",
        config=core_manifest or {},
        user=current_ep_user(),
        ai_available=ai_available,
        website_settings=_get_website_settings(),
    )


@public_bp.post("/api/assistant/chat")
@staff_required_ep
def api_assistant_chat():
    uid = current_ep_user_id()
    if not uid:
        return jsonify({"error": "Not authenticated", "reply": None}), 401
    try:
        from .portal_ai import is_ai_available, assistant_chat
        if not is_ai_available():
            return jsonify({"error": "AI assistance is not set up or enabled yet.", "reply": None}), 503
    except Exception:
        return jsonify({"error": "AI assistance is not available right now.", "reply": None}), 503
    data = request.get_json() or {}
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify({"error": "Message is required.", "reply": None}), 400
    history = data.get("history") or []
    if not isinstance(history, list):
        history = []
    messages = []
    for h in history[-20:]:
        if isinstance(h, dict) and h.get("role") in ("user", "assistant") and h.get("content"):
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": message})
    reply = assistant_chat(uid, messages)
    if reply is None:
        return jsonify({"error": "The assistant is unavailable. Please try again.", "reply": None}), 503
    return jsonify({"reply": reply})


# =============================================================================
# Internal (admin) – require core admin/superuser, show landing page
# =============================================================================


def _admin_required_ep(view):
    """For admin app: require core user with role admin/superuser (Flask-Login)."""
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for("routes.login"))
        role = (getattr(current_user, "role", None) or "").lower()
        if role not in ("admin", "superuser"):
            flash("Admin access required.", "error")
            return redirect(url_for("routes.dashboard"))
        return view(*args, **kwargs)
    return wrapped


@internal_bp.get("/")
@login_required
@_admin_required_ep
def admin_index():
    stats = admin_get_portal_stats()
    return render_template(
        "admin/index.html",
        module_name="Employee Portal",
        module_description="Central hub for staff: profile, time billing, HR, compliance, training, scheduling, messages and todos.",
        plugin_system_name="employee_portal_module",
        config=core_manifest or {},
        stats=stats,
    )


@internal_bp.get("/reports")
@login_required
@_admin_required_ep
def admin_reports():
    report = admin_get_report_stats()
    return render_template(
        "admin/reports.html",
        messages_by_module=report["messages_by_module"],
        todos_by_module=report["todos_by_module"],
        config=core_manifest or {},
    )


@internal_bp.get("/settings")
@login_required
@_admin_required_ep
def admin_portal_settings_form():
    welcome_message = get_ep_setting("welcome_message") or ""
    return render_template(
        "admin/portal_settings.html",
        welcome_message=welcome_message,
        config=core_manifest or {},
    )


@internal_bp.post("/settings")
@login_required
@_admin_required_ep
def admin_portal_settings_save():
    welcome_message = (request.form.get("welcome_message") or "").strip() or None
    set_ep_setting("welcome_message", welcome_message)
    flash("Portal settings saved.", "success")
    return redirect(url_for("internal_employee_portal.admin_portal_settings_form"))


# -----------------------------------------------------------------------------
# Admin: Contractors search and portal preview
# -----------------------------------------------------------------------------


@internal_bp.get("/contractors")
@login_required
@_admin_required_ep
def admin_contractors():
    q = (request.args.get("q") or "").strip()
    contractors = admin_search_contractors(q, limit=50) if q else []
    return render_template(
        "admin/contractors.html",
        q=q,
        contractors=contractors,
        config=core_manifest or {},
    )


@internal_bp.get("/contractors/<int:cid>/portal")
@login_required
@_admin_required_ep
def admin_contractor_portal(cid):
    data = get_dashboard_data_for_contractor(cid, plugin_manager)
    if not data:
        flash("Contractor not found.", "error")
        return redirect(url_for("internal_employee_portal.admin_contractors"))
    # Resolve launch URLs for module links (same as public dashboard)
    for mod in data.get("module_links") or []:
        if mod.get("launch_slug") and mod.get("enabled"):
            mod["url"] = url_for("public_employee_portal.go_to_module", slug=mod["launch_slug"])
    return render_template(
        "admin/portal_preview.html",
        contractor_id=cid,
        **data,
        config=core_manifest or {},
    )


# -----------------------------------------------------------------------------
# Admin: Messages
# -----------------------------------------------------------------------------


@internal_bp.get("/messages")
@login_required
@_admin_required_ep
def admin_messages():
    contractor_id = request.args.get("contractor_id", type=int)
    source_module = (request.args.get("source_module") or "").strip() or None
    read_status = request.args.get("read_status") or None
    show_deleted = request.args.get("show_deleted") == "1"
    date_from_s = request.args.get("date_from") or ""
    date_to_s = request.args.get("date_to") or ""
    page = max(1, request.args.get("page", type=int) or 1)
    per_page = 50
    date_from = None
    date_to = None
    if date_from_s:
        try:
            from datetime import datetime
            date_from = datetime.strptime(date_from_s, "%Y-%m-%d").date()
        except ValueError:
            pass
    if date_to_s:
        try:
            from datetime import datetime
            date_to = datetime.strptime(date_to_s, "%Y-%m-%d").date()
        except ValueError:
            pass
    rows, total = admin_list_messages(
        contractor_id=contractor_id,
        source_module=source_module,
        read_status=read_status,
        date_from=date_from,
        date_to=date_to,
        include_deleted=show_deleted,
        limit=per_page,
        offset=(page - 1) * per_page,
    )
    total_pages = (total + per_page - 1) // per_page if total else 1
    return render_template(
        "admin/messages.html",
        messages=rows,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        contractor_id=contractor_id,
        source_module=source_module,
        read_status=read_status,
        show_deleted=show_deleted,
        date_from=date_from_s,
        date_to=date_to_s,
        config=core_manifest or {},
    )


@internal_bp.get("/messages/send")
@login_required
@_admin_required_ep
def admin_messages_send_form():
    contractors = admin_list_contractors_for_select()
    return render_template(
        "admin/messages_send.html",
        contractors=contractors,
        config=core_manifest or {},
    )


@internal_bp.post("/messages/<int:msg_id>/delete")
@login_required
@_admin_required_ep
def admin_message_delete(msg_id):
    if admin_soft_delete_message(msg_id):
        flash("Message removed.", "success")
    else:
        flash("Message not found or already removed.", "warning")
    return redirect(request.referrer or url_for("internal_employee_portal.admin_messages"))


@internal_bp.post("/messages/<int:msg_id>/restore")
@login_required
@_admin_required_ep
def admin_message_restore(msg_id):
    if admin_restore_message(msg_id):
        flash("Message restored.", "success")
    else:
        flash("Message not found.", "warning")
    return redirect(request.referrer or url_for("internal_employee_portal.admin_messages"))


@internal_bp.post("/messages/send")
@login_required
@_admin_required_ep
def admin_messages_send():
    subject = (request.form.get("subject") or "").strip()
    body = (request.form.get("body") or "").strip()
    send_to = request.form.get("send_to")  # all | or comma ids
    if not subject:
        flash("Subject is required.", "error")
        return redirect(url_for("internal_employee_portal.admin_messages_send_form"))
    contractor_ids = []
    if send_to == "all":
        contractor_ids = [c["id"] for c in admin_list_contractors_for_select()]
    else:
        for part in (request.form.get("contractor_ids") or "").split(","):
            part = part.strip()
            if part.isdigit():
                contractor_ids.append(int(part))
    if not contractor_ids:
        flash("Select at least one recipient (or All staff).", "error")
        return redirect(url_for("internal_employee_portal.admin_messages_send_form"))
    sent_by = getattr(current_user, "id", None)
    if sent_by is not None:
        sent_by = int(sent_by)
    count = admin_send_message(contractor_ids, subject, body=body or None, sent_by_user_id=sent_by)
    flash(f"Message sent to {count} recipient(s).", "success")
    return redirect(url_for("internal_employee_portal.admin_messages"))


# -----------------------------------------------------------------------------
# Admin: Todos
# -----------------------------------------------------------------------------


@internal_bp.get("/todos")
@login_required
@_admin_required_ep
def admin_todos():
    contractor_id = request.args.get("contractor_id", type=int)
    source_module = (request.args.get("source_module") or "").strip() or None
    completed = request.args.get("completed")
    if completed == "1":
        completed = True
    elif completed == "0":
        completed = False
    else:
        completed = None
    due_date_from_s = request.args.get("due_date_from") or ""
    due_date_to_s = request.args.get("due_date_to") or ""
    due_date_from = None
    due_date_to = None
    if due_date_from_s:
        try:
            from datetime import datetime
            due_date_from = datetime.strptime(due_date_from_s, "%Y-%m-%d").date()
        except ValueError:
            pass
    if due_date_to_s:
        try:
            from datetime import datetime
            due_date_to = datetime.strptime(due_date_to_s, "%Y-%m-%d").date()
        except ValueError:
            pass
    page = max(1, request.args.get("page", type=int) or 1)
    per_page = 50
    rows, total = admin_list_todos(
        contractor_id=contractor_id,
        source_module=source_module,
        completed=completed,
        due_date_from=due_date_from,
        due_date_to=due_date_to,
        limit=per_page,
        offset=(page - 1) * per_page,
    )
    total_pages = (total + per_page - 1) // per_page if total else 1
    return render_template(
        "admin/todos.html",
        todos=rows,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        contractor_id=contractor_id,
        source_module=source_module,
        completed=completed,
        due_date_from=due_date_from_s,
        due_date_to=due_date_to_s,
        config=core_manifest or {},
    )


@internal_bp.get("/todos/new")
@login_required
@_admin_required_ep
def admin_todos_new_form():
    contractors = admin_list_contractors_for_select()
    return render_template(
        "admin/todos_form.html",
        todo=None,
        contractors=contractors,
        config=core_manifest or {},
    )


@internal_bp.post("/todos/new")
@login_required
@_admin_required_ep
def admin_todos_new():
    title = (request.form.get("title") or "").strip()
    link_url = (request.form.get("link_url") or "").strip() or None
    due_date_s = (request.form.get("due_date") or "").strip() or None
    due_date = None
    if due_date_s:
        try:
            from datetime import datetime
            due_date = datetime.strptime(due_date_s, "%Y-%m-%d").date()
        except ValueError:
            pass
    send_to_all = request.form.get("all_contractors") == "on"
    contractor_ids = []
    if send_to_all:
        contractor_ids = [c["id"] for c in admin_list_contractors_for_select()]
    else:
        for part in request.form.getlist("contractor_ids"):
            if str(part).strip().isdigit():
                contractor_ids.append(int(part))
    if not title:
        flash("Title is required.", "error")
        return redirect(url_for("internal_employee_portal.admin_todos_new_form"))
    if not contractor_ids:
        flash("Select at least one recipient (or check All staff).", "error")
        return redirect(url_for("internal_employee_portal.admin_todos_new_form"))
    created_by = getattr(current_user, "id", None)
    if created_by is not None:
        created_by = int(created_by)
    count = admin_create_todo(contractor_ids, title, link_url=link_url, due_date=due_date, created_by_user_id=created_by)
    flash(f"Todo created for {count} recipient(s).", "success")
    return redirect(url_for("internal_employee_portal.admin_todos"))


@internal_bp.get("/todos/<int:tid>/edit")
@login_required
@_admin_required_ep
def admin_todo_edit_form(tid):
    todo = admin_get_todo(tid)
    if not todo:
        flash("Todo not found.", "error")
        return redirect(url_for("internal_employee_portal.admin_todos"))
    return render_template(
        "admin/todos_form.html",
        todo=todo,
        contractors=None,
        config=core_manifest or {},
    )


@internal_bp.post("/todos/<int:tid>/edit")
@login_required
@_admin_required_ep
def admin_todo_edit(tid):
    todo = admin_get_todo(tid)
    if not todo:
        flash("Todo not found.", "error")
        return redirect(url_for("internal_employee_portal.admin_todos"))
    title = (request.form.get("title") or "").strip()
    link_url = (request.form.get("link_url") or "").strip() or None
    due_date_s = (request.form.get("due_date") or "").strip() or None
    due_date = None
    if due_date_s:
        try:
            from datetime import datetime
            due_date = datetime.strptime(due_date_s, "%Y-%m-%d").date()
        except ValueError:
            pass
    admin_update_todo(tid, title=title or todo.get("title"), link_url=link_url, due_date=due_date)
    flash("Todo updated.", "success")
    return redirect(url_for("internal_employee_portal.admin_todos"))


@internal_bp.post("/todos/<int:tid>/complete")
@login_required
@_admin_required_ep
def admin_todo_complete(tid):
    admin_set_todo_complete(tid, complete=True)
    flash("Todo marked complete.", "success")
    return redirect(request.referrer or url_for("internal_employee_portal.admin_todos"))


@internal_bp.post("/todos/<int:tid>/reopen")
@login_required
@_admin_required_ep
def admin_todo_reopen(tid):
    admin_set_todo_complete(tid, complete=False)
    flash("Todo reopened.", "success")
    return redirect(request.referrer or url_for("internal_employee_portal.admin_todos"))


@internal_bp.post("/todos/bulk-complete")
@login_required
@_admin_required_ep
def admin_todos_bulk_complete():
    ids = []
    for part in request.form.getlist("todo_ids"):
        if str(part).strip().isdigit():
            ids.append(int(part))
    complete = request.form.get("complete") != "0"
    if ids:
        count = admin_bulk_complete_todos(ids, complete=complete)
        flash(f"{count} todo(s) marked as {'complete' if complete else 'pending'}.", "success")
    else:
        flash("Select at least one todo.", "warning")
    return redirect(request.referrer or url_for("internal_employee_portal.admin_todos"))


# =============================================================================
# Blueprint registration
# =============================================================================


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_bp
