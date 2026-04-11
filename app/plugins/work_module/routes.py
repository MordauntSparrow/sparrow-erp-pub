import os
import uuid
import csv
import io
from datetime import date, datetime, time, timedelta
from functools import wraps
from flask import (
    Blueprint,
    request,
    jsonify,
    render_template,
    redirect,
    session,
    current_app,
    flash,
    url_for,
    Response,
)
from flask_login import current_user, login_required
from app.objects import PluginManager
from app.portal_session import contractor_id_from_tb_user
from . import services as work_services

_plugin_manager = PluginManager(os.path.abspath("app/plugins"))
_core_manifest = _plugin_manager.get_core_manifest() or {}


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

_template = os.path.join(os.path.dirname(__file__), "templates")
internal_bp = Blueprint(
    "internal_work",
    __name__,
    url_prefix="/plugin/work_module",
    template_folder=_template,
)
public_bp = Blueprint(
    "public_work",
    __name__,
    url_prefix="/work",
    template_folder=_template,
)


def _staff_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("tb_user"):
            return redirect("/employee-portal/login?next=" + request.path)
        return view(*args, **kwargs)
    return wrapped


def _current_contractor_id():
    return contractor_id_from_tb_user(session.get("tb_user"))


def _admin_required_work(view):
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


# ---------- Internal (admin) ----------


@internal_bp.get("/")
@login_required
@_admin_required_work
def admin_index():
    return render_template(
        "work_module/admin/index.html",
        module_name="Work",
        module_description="Recorded stops, gaps, photos, and reporting.",
        plugin_system_name="work_module",
        config=_core_manifest,
    )


# ---------- Admin: Recorded stops ----------


@internal_bp.get("/visits")
@login_required
@_admin_required_work
def admin_visits():
    contractor_id = request.args.get("contractor_id", type=int)
    client_id = request.args.get("client_id", type=int)
    date_from_s = request.args.get("date_from")
    date_to_s = request.args.get("date_to")
    date_from = date.fromisoformat(date_from_s) if date_from_s else None
    date_to = date.fromisoformat(date_to_s) if date_to_s else None
    if not date_from:
        date_from = date.today() - timedelta(days=6)
    if not date_to:
        date_to = date.today()
    work_services.ensure_visits_for_shifts_in_range(
        contractor_id=contractor_id,
        client_id=client_id,
        date_from=date_from,
        date_to=date_to,
    )
    visits = work_services.list_visits_admin(
        contractor_id=contractor_id,
        client_id=client_id,
        date_from=date_from,
        date_to=date_to,
    )
    contractors = _get_contractors()
    clients = _get_clients()
    return render_template(
        "admin/visits.html",
        visits=visits,
        contractors=contractors,
        clients=clients,
        contractor_id=contractor_id,
        client_id=client_id,
        date_from=date_from,
        date_to=date_to,
        config=_core_manifest,
    )


@internal_bp.get("/stops")
@login_required
@_admin_required_work
def admin_stops():
    contractor_id = request.args.get("contractor_id", type=int)
    client_id = request.args.get("client_id", type=int)
    date_from_s = request.args.get("date_from")
    date_to_s = request.args.get("date_to")
    date_from = date.fromisoformat(date_from_s) if date_from_s else None
    date_to = date.fromisoformat(date_to_s) if date_to_s else None
    if not date_from:
        date_from = date.today() - timedelta(days=6)
    if not date_to:
        date_to = date.today()
    stops = work_services.list_stops_admin(
        contractor_id=contractor_id,
        client_id=client_id,
        date_from=date_from,
        date_to=date_to,
    )
    contractors = _get_contractors()
    clients = _get_clients()
    return render_template(
        "admin/stops.html",
        stops=stops,
        contractors=contractors,
        clients=clients,
        contractor_id=contractor_id,
        client_id=client_id,
        date_from=date_from,
        date_to=date_to,
        config=_core_manifest,
    )


@internal_bp.get("/stops/<int:shift_id>")
@login_required
@_admin_required_work
def admin_stop_detail(shift_id):
    shift = work_services.get_shift_for_admin(shift_id)
    if not shift:
        flash("Shift not found.", "error")
        return redirect(url_for("internal_work.admin_stops"))
    visit_id = work_services.ensure_visit_for_shift(shift_id)
    photos = work_services.list_photos_for_shift(shift_id)
    return render_template(
        "admin/stop_detail.html",
        shift=shift,
        visit_id=visit_id,
        photos=photos,
        config=_core_manifest,
    )


@internal_bp.post("/stops/<int:shift_id>/override")
@login_required
@_admin_required_work
def admin_stop_override(shift_id):
    actual_start_s = request.form.get("actual_start")
    actual_end_s = request.form.get("actual_end")
    notes = (request.form.get("notes") or "").strip() or None
    actual_start = _parse_time(actual_start_s) if actual_start_s else None
    actual_end = _parse_time(actual_end_s) if actual_end_s else None
    work_services.update_shift_times_admin(shift_id, actual_start=actual_start, actual_end=actual_end, notes=notes)
    flash("Times updated and synced to Time Billing.", "success")
    return redirect(url_for("internal_work.admin_stop_detail", shift_id=shift_id))


def _parse_time(s):
    if not s or not isinstance(s, str):
        return None
    try:
        parts = s.strip()[:8].split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        sec = int(parts[2]) if len(parts) > 2 else 0
        return time(h, m, sec)
    except (ValueError, TypeError):
        return None


def _get_contractors():
    try:
        from app.plugins.scheduling_module.services import ScheduleService
        return ScheduleService.list_contractors()
    except Exception:
        return []


def _get_clients():
    try:
        from app.plugins.scheduling_module.services import ScheduleService
        c, _ = ScheduleService.list_clients_and_sites()
        return c
    except Exception:
        return []


# ---------- Admin: Gaps report ----------


def _notify_contractor_work(contractor_id: int, subject: str, body: str = ""):
    try:
        from app.plugins.employee_portal_module.services import admin_send_message
        from flask_login import current_user
        admin_send_message(
            [contractor_id],
            subject[:255],
            (body or "")[:65535],
            source_module="work_module",
            sent_by_user_id=getattr(current_user, "id", None),
        )
    except Exception:
        pass


@internal_bp.get("/gaps")
@login_required
@_admin_required_work
def admin_gaps():
    date_from_s = request.args.get("date_from")
    date_to_s = request.args.get("date_to")
    contractor_id = request.args.get("contractor_id", type=int)
    date_from = date.fromisoformat(date_from_s) if date_from_s else (date.today() - timedelta(days=6))
    date_to = date.fromisoformat(date_to_s) if date_to_s else date.today()
    gaps = work_services.list_gaps(date_from=date_from, date_to=date_to, contractor_id=contractor_id)
    contractors = _get_contractors()
    return render_template(
        "admin/gaps.html",
        gaps=gaps,
        contractors=contractors,
        contractor_id=contractor_id,
        date_from=date_from,
        date_to=date_to,
        config=_core_manifest,
    )


@internal_bp.post("/gaps/remind/<int:shift_id>")
@login_required
@_admin_required_work
def admin_gaps_remind(shift_id):
    shift = work_services.get_shift_for_admin(shift_id)
    if not shift:
        flash("Shift not found.", "error")
        return redirect(url_for("internal_work.admin_gaps"))
    cid = shift.get("contractor_id")
    if cid:
        _notify_contractor_work(
            cid,
            "Please record your times",
            "You have a shift with no clock-in or clock-out recorded. Please record your times in the Work app.",
        )
        flash("Reminder sent.", "success")
    return redirect(url_for("internal_work.admin_gaps"))


# ---------- Admin: Photo gallery ----------


@internal_bp.get("/photos")
@login_required
@_admin_required_work
def admin_photos():
    contractor_id = request.args.get("contractor_id", type=int)
    shift_id = request.args.get("shift_id", type=int)
    visit_id = request.args.get("visit_id", type=int)
    date_from_s = request.args.get("date_from")
    date_to_s = request.args.get("date_to")
    date_from = date.fromisoformat(date_from_s) if date_from_s else None
    date_to = date.fromisoformat(date_to_s) if date_to_s else None
    photos = work_services.list_photos_admin(
        contractor_id=contractor_id,
        shift_id=shift_id,
        visit_id=visit_id,
        date_from=date_from,
        date_to=date_to,
    )
    contractors = _get_contractors()
    return render_template(
        "admin/photos.html",
        photos=photos,
        contractors=contractors,
        contractor_id=contractor_id,
        shift_id=shift_id,
        visit_id=visit_id,
        date_from=date_from,
        date_to=date_to,
        config=_core_manifest,
    )


# ---------- Admin: Reporting ----------


@internal_bp.get("/report")
@login_required
@_admin_required_work
def admin_report():
    date_from_s = request.args.get("date_from")
    date_to_s = request.args.get("date_to")
    contractor_id = request.args.get("contractor_id", type=int)
    client_id = request.args.get("client_id", type=int)
    date_from = date.fromisoformat(date_from_s) if date_from_s else (date.today() - timedelta(days=6))
    date_to = date.fromisoformat(date_to_s) if date_to_s else date.today()
    rows = work_services.report_hours(
        date_from=date_from,
        date_to=date_to,
        contractor_id=contractor_id,
        client_id=client_id,
    )
    contractors = _get_contractors()
    clients = _get_clients()
    return render_template(
        "admin/report.html",
        rows=rows,
        contractors=contractors,
        clients=clients,
        contractor_id=contractor_id,
        client_id=client_id,
        date_from=date_from,
        date_to=date_to,
        config=_core_manifest,
    )


@internal_bp.get("/report/export")
@login_required
@_admin_required_work
def admin_report_export():
    date_from_s = request.args.get("date_from")
    date_to_s = request.args.get("date_to")
    contractor_id = request.args.get("contractor_id", type=int)
    client_id = request.args.get("client_id", type=int)
    date_from = date.fromisoformat(date_from_s) if date_from_s else (date.today() - timedelta(days=6))
    date_to = date.fromisoformat(date_to_s) if date_to_s else date.today()
    rows = work_services.report_hours(
        date_from=date_from,
        date_to=date_to,
        contractor_id=contractor_id,
        client_id=client_id,
    )
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Date", "Visit ID", "Shift ID", "Contractor", "Client", "Site", "Start", "End", "Hours", "Notes"])
    for r in rows:
        wd = r.get("work_date")
        wd_str = wd.isoformat() if hasattr(wd, "isoformat") else str(wd)
        start = r.get("actual_start")
        end = r.get("actual_end")
        w.writerow([
            wd_str,
            r.get("visit_id") or "",
            r.get("shift_id") or "",
            r.get("contractor_name") or "",
            r.get("client_name") or "",
            r.get("site_name") or "",
            r.get("actual_start_str") or "",
            r.get("actual_end_str") or "",
            r.get("hours") or "",
            (r.get("notes") or "")[:200],
        ])
    return Response(out.getvalue(), mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=work_hours.csv"})


# ---------- Public: My day (list of stops from scheduling) ----------


@public_bp.get("/")
@_staff_required
def index():
    cid = _current_contractor_id()
    if not cid:
        return redirect("/employee-portal/login?next=/work/")
    stops = work_services.get_my_stops_for_today(cid)
    for s in stops:
        work_services.ensure_visit_for_shift(int(s["id"]))
    work_services.enrich_stops_with_visit_ids(stops)
    # Enrich each stop with photo count for planner view
    for s in stops:
        s["photo_count"] = len(work_services.list_photos_for_shift(s["id"]))
    return render_template(
        "work_module/public/index.html",
        stops=stops,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


# ---------- Public: Record stop (times, notes, photos) ----------


@public_bp.get("/stop/<int:shift_id>")
@_staff_required
def stop_page(shift_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect("/employee-portal/login?next=/work/stop/" + str(shift_id))
    shift = work_services.get_shift_for_stop(shift_id, cid)
    if not shift:
        return redirect("/work/")
    visit_id = work_services.ensure_visit_for_shift(shift_id)
    photos = work_services.list_photos_for_shift(shift_id)
    return render_template(
        "work_module/public/stop.html",
        shift=shift,
        visit_id=visit_id,
        photos=photos,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.post("/api/stop/<int:shift_id>/record")
@_staff_required
def api_record_stop(shift_id):
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Not authenticated"}), 401
    data = request.get_json() or request.form
    actual_start = data.get("actual_start")
    actual_end = data.get("actual_end")
    notes = data.get("notes")
    if actual_start and isinstance(actual_start, str) and len(actual_start) <= 8:
        from datetime import time
        parts = actual_start.split(":")
        actual_start = time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0, int(parts[2]) if len(parts) > 2 else 0)
    else:
        actual_start = None
    if actual_end and isinstance(actual_end, str) and len(actual_end) <= 8:
        from datetime import time
        parts = actual_end.split(":")
        actual_end = time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0, int(parts[2]) if len(parts) > 2 else 0)
    else:
        actual_end = None
    ok = work_services.record_stop(shift_id, cid, actual_start=actual_start, actual_end=actual_end, notes=notes)
    if not ok:
        return jsonify({"error": "Forbidden or not found"}), 403
    return jsonify({"ok": True})


@public_bp.post("/api/stop/<int:shift_id>/photo")
@_staff_required
def api_upload_photo(shift_id):
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Not authenticated"}), 401
    shift = work_services.get_shift_for_stop(shift_id, cid)
    if not shift:
        return jsonify({"error": "Not found"}), 404
    file = request.files.get("photo")
    if not file or not file.filename:
        return jsonify({"error": "No file"}), 400
    base = getattr(current_app, "root_path", None) or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    upload_dir = os.path.join(base, "static", "uploads", "work_photos")
    os.makedirs(upload_dir, exist_ok=True)
    ext = os.path.splitext(file.filename)[1] or ".jpg"
    safe_name = f"{shift_id}_{uuid.uuid4().hex[:12]}{ext}"
    rel_path = os.path.join("uploads", "work_photos", safe_name)
    full_path = os.path.join(base, "static", rel_path)
    file.save(full_path)
    caption = request.form.get("caption")
    pid = work_services.add_photo(shift_id, cid, rel_path, file_name=file.filename, mime_type=file.content_type, caption=caption)
    return jsonify({"ok": True, "id": pid, "path": "/static/" + rel_path})


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_bp
