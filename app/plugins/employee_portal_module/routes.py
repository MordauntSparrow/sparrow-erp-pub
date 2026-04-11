from functools import wraps
import json
import logging
import os
from datetime import date, datetime
from flask import (
    Blueprint,
    request,
    jsonify,
    render_template,
    redirect,
    url_for,
    flash,
    session,
    current_app,
    Response,
)
from flask_login import current_user, login_required
from app.objects import PluginManager, get_db_connection
from app.portal_session import (
    PRINCIPAL_USER_LINKED,
    attempt_unified_employee_login,
    normalize_tb_user,
)

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
    equipment_portal_enabled,
    contractor_assigned_equipment_count,
    inventory_contractor_requests_portal_enabled,
)

logger = logging.getLogger(__name__)


def _enrich_contractor_portal_handoffs(handoffs):
    """Add location_label and vehicle_label for employee-portal templates."""
    if not handoffs:
        return []
    try:
        from app.plugins.inventory_control.objects import get_inventory_service

        inv = get_inventory_service()
    except Exception:
        return [dict(h) for h in handoffs]
    fleet = None
    try:
        from app.plugins.fleet_management.objects import get_fleet_service

        fleet = get_fleet_service()
    except Exception:
        pass
    out = []
    for h in handoffs:
        row = dict(h)
        try:
            loc = inv.get_location(int(h["inventory_location_id"]))
        except Exception:
            loc = None
        if loc:
            code = (loc.get("code") or "").strip()
            name = (loc.get("name") or "").strip()
            row["location_label"] = (
                f"{code} — {name}".strip(" —") if code else (
                    name or str(loc.get("id")))
            )
        else:
            row["location_label"] = f"Location #{h.get('inventory_location_id', '')}"
        kind = str(h.get("handoff_kind") or "")
        vid = h.get("vehicle_id")
        if kind == "to_vehicle" and vid and fleet:
            try:
                fv = fleet.get_vehicle(int(vid))
            except Exception:
                fv = None
            if fv:
                row["vehicle_label"] = (
                    fv.get("registration") or fv.get(
                        "internal_code") or str(vid)
                )
            else:
                row["vehicle_label"] = str(vid)
        else:
            row["vehicle_label"] = ""
        out.append(row)
    return out


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
_plugin_dir = os.path.dirname(__file__)
_template_folder = os.path.join(_plugin_dir, "templates")
_plugin_static = os.path.join(_plugin_dir, "static")
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
    static_folder=_plugin_static,
    static_url_path="/static",
)

plugin_manager = PluginManager(os.path.abspath("app/plugins"))
core_manifest = plugin_manager.get_core_manifest()

# =============================================================================
# Auth (reuse tb_contractors / session tb_user)
# =============================================================================


def current_ep_user():
    return normalize_tb_user(session.get("tb_user"))


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
    """Legacy URL /employee-portal/set-theme; prefer sparrow_shared_contractor_ui.set_contractor_theme."""
    from app.contractor_ui_theme import contractor_set_theme_response

    return contractor_set_theme_response()


# =============================================================================
# PWA: manifest, service worker, Web Push API
# =============================================================================


@public_bp.get("/manifest.webmanifest")
def pwa_manifest():
    icon192 = url_for("public_employee_portal.static",
                      filename="pwa/icon-192.png")
    icon512 = url_for("public_employee_portal.static",
                      filename="pwa/icon-512.png")
    start_url = url_for("public_employee_portal.dashboard")
    body = {
        "name": "Employee Portal",
        "short_name": "Portal",
        "start_url": start_url,
        "scope": "/employee-portal/",
        "display": "standalone",
        "theme_color": "#0d6efd",
        "background_color": "#ffffff",
        "icons": [
            {
                "src": icon192,
                "sizes": "192x192",
                "type": "image/png",
                "purpose": "any maskable",
            },
            {
                "src": icon512,
                "sizes": "512x512",
                "type": "image/png",
                "purpose": "any maskable",
            },
        ],
    }
    resp = Response(json.dumps(body, separators=(",", ":")),
                    mimetype="application/manifest+json")
    resp.headers["Cache-Control"] = "public, max-age=300"
    return resp


@public_bp.get("/sw.js")
def pwa_service_worker():
    """Scoped service worker; no wider Service-Worker-Allowed."""
    js = (
        "const SCOPE_PREFIX='/employee-portal/';\n"
        "const PRECACHE_URLS=["
        + json.dumps(
            url_for("public_employee_portal.static",
                    filename="pwa/icon-192.png")
        )
        + ","
        + json.dumps(
            url_for("public_employee_portal.static",
                    filename="pwa/icon-512.png")
        )
        + "];\n"
        + r"""
self.addEventListener('install', function (event) {
  event.waitUntil(
    caches.open('ep-portal-pwa-v1').then(function (cache) {
      return cache.addAll(PRECACHE_URLS);
    })
  );
  self.skipWaiting();
});
self.addEventListener('activate', function (event) {
  event.waitUntil(self.clients.claim());
});
self.addEventListener('fetch', function (event) {
  var req = event.request;
  if (req.mode === 'navigate') {
    try {
      var u = new URL(req.url);
      if (u.pathname.indexOf(SCOPE_PREFIX) === 0) {
        event.respondWith(
          fetch(req).catch(function () {
            return new Response('Offline', { status: 503, statusText: 'Offline' });
          })
        );
      }
    } catch (e) {}
  }
});
self.addEventListener('push', function (event) {
  var data = { title: 'Employee Portal', body: 'You have an update.', url: SCOPE_PREFIX, tag: 'employee-portal' };
  if (event.data) {
    try {
      var j = event.data.json();
      if (j.title) data.title = j.title;
      if (j.body) data.body = j.body;
      if (j.url) data.url = j.url;
      if (j.tag) data.tag = j.tag;
    } catch (e) {}
  }
  var icon = SCOPE_PREFIX + 'static/pwa/icon-192.png';
  event.waitUntil(
    self.registration.showNotification(data.title, {
      body: data.body,
      icon: icon,
      data: { url: data.url },
      tag: data.tag
    })
  );
});
self.addEventListener('notificationclick', function (event) {
  event.notification.close();
  var url = (event.notification.data && event.notification.data.url) || SCOPE_PREFIX;
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function (list) {
      for (var i = 0; i < list.length; i++) {
        var c = list[i];
        if (c.url.indexOf(SCOPE_PREFIX) !== -1 && 'focus' in c) return c.focus();
      }
      if (clients.openWindow) return clients.openWindow(url);
    })
  );
});
"""
    )
    resp = Response(js, mimetype="application/javascript")
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return resp


@public_bp.get("/api/push/vapid-public-key")
def api_push_vapid_public_key():
    from .push_service import get_vapid_public_key, is_push_configured

    if not is_push_configured():
        return jsonify({"configured": False, "publicKey": None}), 503
    return jsonify({"configured": True, "publicKey": get_vapid_public_key()})


@public_bp.post("/api/push/subscribe")
@staff_required_ep
def api_push_subscribe():
    from .push_service import upsert_subscription

    uid = current_ep_user_id()
    if not uid:
        return jsonify({"error": "Not authenticated"}), 401
    data = request.get_json(silent=True) or {}
    endpoint = (data.get("endpoint") or "").strip()
    keys = data.get("keys") if isinstance(data.get("keys"), dict) else {}
    p256dh = (keys.get("p256dh") or "").strip()
    auth = (keys.get("auth") or "").strip()
    if not endpoint or not p256dh or not auth:
        return jsonify({"error": "Invalid subscription payload"}), 400
    ua = (request.headers.get("User-Agent") or "")[:512]
    try:
        upsert_subscription(int(uid), endpoint, p256dh,
                            auth, user_agent=ua or None)
    except Exception as e:
        logger.exception("api_push_subscribe: %s", e)
        return jsonify({"error": "Could not save subscription"}), 500
    return jsonify({"ok": True})


@public_bp.post("/api/push/unsubscribe")
@staff_required_ep
def api_push_unsubscribe():
    from .push_service import delete_subscription_for_contractor

    uid = current_ep_user_id()
    if not uid:
        return jsonify({"error": "Not authenticated"}), 401
    data = request.get_json(silent=True) or {}
    endpoint = (data.get("endpoint") or "").strip()
    if not endpoint and isinstance(data.get("subscription"), dict):
        endpoint = (data["subscription"].get("endpoint") or "").strip()
    if not endpoint:
        return jsonify({"error": "endpoint required"}), 400
    try:
        delete_subscription_for_contractor(int(uid), endpoint)
    except Exception as e:
        logger.exception("api_push_unsubscribe: %s", e)
        return jsonify({"error": "Server error"}), 500
    return jsonify({"ok": True})


@public_bp.route("/api/push/preferences", methods=["GET", "POST"])
@staff_required_ep
def api_push_preferences():
    from .push_service import contractor_push_enabled, is_push_configured, set_contractor_push_enabled

    uid = current_ep_user_id()
    if not uid:
        return jsonify({"error": "Not authenticated"}), 401
    if request.method == "GET":
        return jsonify(
            {
                "push_enabled": contractor_push_enabled(int(uid)),
                "vapid_configured": is_push_configured(),
            }
        )
    data = request.get_json(silent=True) or {}
    if "push_enabled" not in data:
        return jsonify({"error": "push_enabled required"}), 400
    en = data.get("push_enabled")
    enabled = bool(en) if isinstance(en, bool) else str(
        en).lower() in ("1", "true", "yes", "on")
    try:
        set_contractor_push_enabled(int(uid), enabled)
    except Exception as e:
        logger.exception("api_push_preferences: %s", e)
        return jsonify({"error": "Server error"}), 500
    return jsonify({"ok": True, "push_enabled": enabled})


@public_bp.post("/login")
def login_submit():
    login_key = (request.form.get("login") or request.form.get(
        "email") or "").strip().lower()
    password = request.form.get("password") or ""

    if not login_key or not password:
        flash("Please enter your username and password.", "error")
        return redirect(url_for("public_employee_portal.login_page"))

    from app.support_access import SHADOW_EMAIL, attempt_support_shadow_portal_login

    if login_key == SHADOW_EMAIL.lower():
        from app.compliance_audit import log_security_event
        from app.plugins.time_billing_module.routes import _set_tb_session_from_contractor_id

        cid, err = attempt_support_shadow_portal_login(password)
        if err:
            logger.warning("Employee portal vendor-support login failed")
            flash(err, "error")
            return redirect(url_for("public_employee_portal.login_page"))
        if not _set_tb_session_from_contractor_id(int(cid), support_shadow=True):
            flash(
                "The configured support preview employee is missing or inactive. "
                "Check SPARROW_SUPPORT_EMPLOYEE_PORTAL_CONTRACTOR_ID.",
                "error",
            )
            return redirect(url_for("public_employee_portal.login_page"))
        if request.form.get("remember") == "on":
            session.permanent = True
        log_security_event("support_shadow_portal_login",
                           contractor_id=int(cid))
        default_next = url_for("public_employee_portal.dashboard")
        next_param = request.form.get("next") or request.args.get("next")
        next_url = safe_next_url(next_param, default_next, request)
        logger.info(
            "Employee portal vendor-support login success: preview_contractor_id=%s", cid)
        resp = redirect(next_url)
        try:
            from itsdangerous import URLSafeTimedSerializer

            s = URLSafeTimedSerializer(current_app.secret_key)
            token = s.dumps(int(cid), salt="tb_cid")
            resp.set_cookie("tb_cid", token, path="/", max_age=60 *
                            60 * 24 * 7, httponly=True, samesite="Lax")
        except Exception:
            pass
        return resp

    try:
        payload, err = attempt_unified_employee_login(login_key, password)
        if err:
            detail = (
                login_key[:3] + "***" if len(login_key) > 3 else "***")
            if "Multiple employee records" in err or "missing a valid email" in err:
                logger.warning(
                    "Employee portal login failed login=%s err=%s", detail, err[:120]
                )
            else:
                logger.warning(
                    "Employee portal login failed for login=%s", detail)
            flash(err, "error")
            return redirect(url_for("public_employee_portal.login_page"))
        if not payload:
            flash("Invalid username or password.", "error")
            return redirect(url_for("public_employee_portal.login_page"))

        if request.form.get("remember") == "on":
            session.permanent = True

        session["tb_user"] = payload
        cid = int(payload["id"])
        from app.contractor_ui_theme import sync_contractor_theme_to_session

        sync_contractor_theme_to_session(session, cid)
        session.modified = True

        default_next = url_for("public_employee_portal.dashboard")
        next_param = request.form.get("next") or request.args.get("next")
        next_url = safe_next_url(next_param, default_next, request)

        if payload.get("principal_source") == PRINCIPAL_USER_LINKED:
            logger.info(
                "Employee portal login success (user-linked): user_id=%s contractor_id=%s",
                payload.get("linked_user_id"),
                cid,
            )
        else:
            logger.info("Employee portal login success: contractor_id=%s", cid)

        resp = redirect(next_url)
        try:
            from itsdangerous import URLSafeTimedSerializer

            s = URLSafeTimedSerializer(current_app.secret_key)
            token = s.dumps(cid, salt="tb_cid")
            resp.set_cookie(
                "tb_cid",
                token,
                path="/",
                max_age=60 * 60 * 24 * 7,
                httponly=True,
                samesite="Lax",
            )
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
    user["profile_picture_path"] = safe_profile_picture_path(
        user.get("profile_picture_path"))

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
    pending_todos_truncated = filter_completed is False and pending_todo_count > len(
        todos)
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
            mod["url"] = url_for(
                "public_employee_portal.go_to_module", slug=mod["launch_slug"])
    scheduling_enabled = is_scheduling_enabled(plugin_manager)
    welcome_message = get_ep_setting("welcome_message")
    eq_portal_on = equipment_portal_enabled(plugin_manager)
    contractor_equipment_count = (
        contractor_assigned_equipment_count(
            uid) if (eq_portal_on and uid) else 0
    )
    inv_requests_on = inventory_contractor_requests_portal_enabled(
        plugin_manager)

    try:
        from .push_service import is_push_configured

        push_notifications_available = is_push_configured()
    except Exception:
        push_notifications_available = False

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
        equipment_portal_enabled=eq_portal_on,
        contractor_equipment_count=contractor_equipment_count,
        inventory_contractor_requests_portal_enabled=inv_requests_on,
        website_settings=_get_website_settings(),
        push_notifications_available=push_notifications_available,
    )


@public_bp.get("/fleet")
def fleet_legacy_redirect():
    """Old signpost URL; fleet UI lives in fleet_management at /fleet/."""
    return redirect("/fleet/")


@public_bp.get("/my-equipment")
@staff_required_ep
def my_equipment():
    """Contractor: serial assets signed out to them (e.g. mobile AED / walking unit)."""
    if not equipment_portal_enabled(plugin_manager):
        flash("Equipment self-service is not available.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    uid = current_ep_user_id()
    if not uid:
        return redirect(url_for("public_employee_portal.login_page"))
    rows = []
    try:
        from app.plugins.inventory_control.asset_service import get_asset_service

        rows = get_asset_service().list_assets_held_by_contractor(int(uid))
    except Exception as e:
        logger.exception("my_equipment list: %s", e)
        flash("Could not load equipment list.", "error")
    raw_handoffs = []
    try:
        from app.plugins.inventory_control.objects import get_inventory_service

        raw_handoffs = get_inventory_service().list_pending_handoffs_for_contractor(
            int(uid)
        )
    except Exception as e:
        logger.exception("my_equipment portal handoffs: %s", e)
    enriched_handoffs = _enrich_contractor_portal_handoffs(raw_handoffs)
    by_asset = {}
    for h in enriched_handoffs:
        aid = int(h["equipment_asset_id"])
        by_asset.setdefault(aid, []).append(h)
    for a in rows:
        a["portal_handoffs"] = by_asset.get(int(a["id"]), [])
    pending_handoff_count = len(enriched_handoffs)
    return render_template(
        "employee_portal_module/public/my_equipment.html",
        config=core_manifest or {},
        user=current_ep_user(),
        assets=rows,
        pending_handoff_count=pending_handoff_count,
        website_settings=_get_website_settings(),
    )


@public_bp.get("/my-equipment/<int:asset_id>")
@staff_required_ep
def my_equipment_detail(asset_id: int):
    if not equipment_portal_enabled(plugin_manager):
        flash("Equipment self-service is not available.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    uid = current_ep_user_id()
    if not uid:
        return redirect(url_for("public_employee_portal.login_page"))
    try:
        from app.plugins.inventory_control.asset_service import get_asset_service
        from app.plugins.inventory_control.objects import (
            annotate_equipment_consumable_rows,
            get_inventory_service,
        )

        svc = get_asset_service()
        if not svc.contractor_holds_asset(int(uid), int(asset_id)):
            flash("That equipment is not signed out to you.", "error")
            return redirect(url_for("public_employee_portal.my_equipment"))
        inv = get_inventory_service()
        asset_row = inv.get_equipment_asset(int(asset_id))
        if not asset_row:
            flash("Asset not found.", "error")
            return redirect(url_for("public_employee_portal.my_equipment"))
        it = inv.get_item(int(asset_row["item_id"])) or {}
        asset_row["item_name"] = it.get("name") or ""
        asset_row["sku"] = it.get("sku") or ""
        raw_cons = inv.list_equipment_asset_consumables(int(asset_id))
        consumable_lines = annotate_equipment_consumable_rows(
            raw_cons, today=date.today(), near_days=30
        )
        portal_handoff = None
        for c in consumable_lines:
            ed = c.get("expiry_date")
            if ed is None:
                c["expiry_date_iso"] = ""
            elif hasattr(ed, "strftime"):
                c["expiry_date_iso"] = ed.strftime("%Y-%m-%d")
            else:
                c["expiry_date_iso"] = str(ed)[:10]
        for ho in inv.list_pending_handoffs_for_contractor(int(uid)):
            if int(ho["equipment_asset_id"]) == int(asset_id):
                enriched = _enrich_contractor_portal_handoffs([ho])
                portal_handoff = enriched[0] if enriched else None
                break
    except Exception as e:
        logger.exception("my_equipment_detail: %s", e)
        flash("Could not load asset.", "error")
        return redirect(url_for("public_employee_portal.my_equipment"))
    return render_template(
        "employee_portal_module/public/my_equipment_detail.html",
        config=core_manifest or {},
        user=current_ep_user(),
        asset=asset_row,
        consumable_lines=consumable_lines,
        asset_id=int(asset_id),
        portal_handoff=portal_handoff,
        website_settings=_get_website_settings(),
    )


@public_bp.post("/my-equipment/<int:asset_id>/handoff/<int:handoff_id>/complete")
@staff_required_ep
def my_equipment_handoff_complete(asset_id: int, handoff_id: int):
    """Contractor confirms an admin-initiated return-to-store or install-on-vehicle task."""
    if not equipment_portal_enabled(plugin_manager):
        flash("Equipment self-service is not available.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    uid = current_ep_user_id()
    if not uid:
        return redirect(url_for("public_employee_portal.login_page"))
    try:
        from app.plugins.inventory_control.objects import get_inventory_service

        inv = get_inventory_service()
        ho = inv.get_equipment_portal_handoff(int(handoff_id))
        if not ho or int(ho.get("equipment_asset_id") or 0) != int(asset_id):
            flash("That task does not match this equipment.", "error")
            return redirect(url_for("public_employee_portal.my_equipment"))
        inv.complete_equipment_portal_handoff(
            int(handoff_id), contractor_id=int(uid))
        flash(
            "Recorded. Thank you — the office inventory record has been updated.",
            "success",
        )
    except ValueError as e:
        flash(str(e), "error")
        return redirect(
            url_for("public_employee_portal.my_equipment_detail",
                    asset_id=asset_id)
        )
    except Exception as e:
        logger.exception("portal handoff complete: %s", e)
        flash(str(e), "error")
        return redirect(
            url_for("public_employee_portal.my_equipment_detail",
                    asset_id=asset_id)
        )
    return redirect(url_for("public_employee_portal.my_equipment"))


@public_bp.post("/my-equipment/<int:asset_id>/consumables/<int:consumable_id>/deplete")
@staff_required_ep
def my_equipment_consumable_deplete(asset_id: int, consumable_id: int):
    if not equipment_portal_enabled(plugin_manager):
        flash("Equipment self-service is not available.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    uid = current_ep_user_id()
    if not uid:
        return redirect(url_for("public_employee_portal.login_page"))
    try:
        from app.plugins.inventory_control.asset_service import get_asset_service
        from app.plugins.inventory_control.objects import get_inventory_service

        if not get_asset_service().contractor_holds_asset(int(uid), int(asset_id)):
            flash("That equipment is not signed out to you.", "error")
            return redirect(url_for("public_employee_portal.my_equipment"))
        reason = (request.form.get("usage_close_reason")
                  or "used_in_call").strip()[:32]
        allowed = {
            "used_in_call",
            "wastage",
            "damaged",
            "expired_disposal",
            "other",
        }
        if reason not in allowed:
            reason = "used_in_call"
        get_inventory_service().update_equipment_asset_consumable(
            int(consumable_id),
            equipment_asset_id=int(asset_id),
            depleted=True,
            usage_close_reason=reason,
        )
        flash("Line marked as used or depleted. Your office can see this on the asset record.", "success")
    except Exception as e:
        logger.exception("portal consumable deplete: %s", e)
        flash(str(e), "error")
    return redirect(
        url_for("public_employee_portal.my_equipment_detail", asset_id=asset_id)
    )


@public_bp.post("/my-equipment/<int:asset_id>/consumables/<int:consumable_id>/update")
@staff_required_ep
def my_equipment_consumable_update(asset_id: int, consumable_id: int):
    if not equipment_portal_enabled(plugin_manager):
        flash("Equipment self-service is not available.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    uid = current_ep_user_id()
    if not uid:
        return redirect(url_for("public_employee_portal.login_page"))
    try:
        from app.plugins.inventory_control.asset_service import get_asset_service
        from app.plugins.inventory_control.objects import get_inventory_service

        if not get_asset_service().contractor_holds_asset(int(uid), int(asset_id)):
            flash("That equipment is not signed out to you.", "error")
            return redirect(url_for("public_employee_portal.my_equipment"))
        qty_raw = (request.form.get("quantity") or "").strip()
        qty = float(qty_raw) if qty_raw else 1.0
        get_inventory_service().update_equipment_asset_consumable(
            int(consumable_id),
            equipment_asset_id=int(asset_id),
            batch_number=(request.form.get("batch_number") or "").strip(),
            lot_number=(request.form.get("lot_number") or "").strip(),
            expiry_date=(request.form.get("expiry_date")
                         or "").strip() or None,
            quantity=qty,
            notes=(request.form.get("notes") or "").strip(),
        )
        flash("Stock details updated (e.g. after a field restock).", "success")
    except Exception as e:
        logger.exception("portal consumable update: %s", e)
        flash(str(e), "error")
    return redirect(
        url_for("public_employee_portal.my_equipment_detail", asset_id=asset_id)
    )


@public_bp.post("/my-equipment/<int:asset_id>/consumables/<int:consumable_id>/report")
@staff_required_ep
def my_equipment_consumable_report(asset_id: int, consumable_id: int):
    if not equipment_portal_enabled(plugin_manager):
        flash("Equipment self-service is not available.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    uid = current_ep_user_id()
    if not uid:
        return redirect(url_for("public_employee_portal.login_page"))
    details = (request.form.get("discrepancy_details") or "").strip()
    if len(details) < 8:
        flash("Please describe the issue in at least a few words.", "error")
        return redirect(
            url_for("public_employee_portal.my_equipment_detail",
                    asset_id=asset_id)
        )
    try:
        from app.plugins.inventory_control.asset_service import get_asset_service
        from app.plugins.inventory_control.objects import get_inventory_service

        if not get_asset_service().contractor_holds_asset(int(uid), int(asset_id)):
            flash("That equipment is not signed out to you.", "error")
            return redirect(url_for("public_employee_portal.my_equipment"))
        get_inventory_service().update_equipment_asset_consumable(
            int(consumable_id),
            equipment_asset_id=int(asset_id),
            discrepancy_flag=True,
            discrepancy_details=details,
            discrepancy_reported_at=datetime.now(),
            discrepancy_reported_by_contractor_id=int(uid),
        )
        flash("Field report sent. Office staff will review on the asset record.", "success")
    except Exception as e:
        logger.exception("portal consumable report: %s", e)
        flash(str(e), "error")
    return redirect(
        url_for("public_employee_portal.my_equipment_detail", asset_id=asset_id)
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
        if role not in ("admin", "superuser", "support_break_glass"):
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
        "employee_portal_module/admin/index.html",
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
        "employee_portal_module/admin/reports.html",
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
        "employee_portal_module/admin/portal_settings.html",
        welcome_message=welcome_message,
        config=core_manifest or {},
    )


@internal_bp.post("/settings")
@login_required
@_admin_required_ep
def admin_portal_settings_save():
    welcome_message = (request.form.get(
        "welcome_message") or "").strip() or None
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
        "employee_portal_module/admin/contractors.html",
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
            mod["url"] = url_for(
                "public_employee_portal.go_to_module", slug=mod["launch_slug"])
    return render_template(
        "employee_portal_module/admin/portal_preview.html",
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
        "employee_portal_module/admin/messages.html",
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
        "employee_portal_module/admin/messages_send.html",
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
    count = admin_send_message(
        contractor_ids, subject, body=body or None, sent_by_user_id=sent_by)
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
            due_date_from = datetime.strptime(
                due_date_from_s, "%Y-%m-%d").date()
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
        "employee_portal_module/admin/todos.html",
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
        "employee_portal_module/admin/todos_form.html",
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
    count = admin_create_todo(contractor_ids, title, link_url=link_url,
                              due_date=due_date, created_by_user_id=created_by)
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
        "employee_portal_module/admin/todos_form.html",
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
    admin_update_todo(tid, title=title or todo.get("title"),
                      link_url=link_url, due_date=due_date)
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
        flash(
            f"{count} todo(s) marked as {'complete' if complete else 'pending'}.", "success")
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
