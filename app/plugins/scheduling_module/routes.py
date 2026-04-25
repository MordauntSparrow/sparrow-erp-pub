import os
import json
from datetime import date, timedelta
from functools import wraps
from flask import (
    Blueprint,
    request,
    jsonify,
    render_template,
    redirect,
    url_for,
    session,
    flash,
    send_file,
    current_app,
    has_request_context,
)
from flask_login import current_user, login_required
from app.objects import PluginManager, get_db_connection
from app.portal_session import contractor_id_from_tb_user
from .services import ScheduleService, SlingSyncService
from .ai_chat import chat as ai_chat, is_ai_available

_plugin_manager = PluginManager(os.path.abspath("app/plugins"))


def _safe_url_for(endpoint: str, **values):
    """Build admin URL when another plugin blueprint is registered; else None."""
    try:
        return str(url_for(endpoint, **values))
    except Exception:
        return None


def _ensure_scheduling_tables():
    """Ensure scheduling DB tables exist (idempotent). Run on admin load so week/month view work without manual install."""
    try:
        from app.plugins.scheduling_module.install import ensure_tables
        conn = get_db_connection()
        try:
            ensure_tables(conn)
        finally:
            conn.close()
    except Exception:
        pass
_core_manifest = _plugin_manager.get_core_manifest() or {}


def _flash_sling(message: str, *, ok: bool = True) -> None:
    """Queue a flash shown only on the Sling sync page (not the core admin shell)."""
    flash(message, "sling_success" if ok else "sling_warning")


def _scheduling_tenant_industries():
    """Normalised industry slugs: runtime config when in a request, else Core manifest on disk."""
    try:
        from app.organization_profile import normalize_organization_industries

        if has_request_context():
            return normalize_organization_industries(
                current_app.config.get("organization_industries")
            )
    except Exception:
        pass
    from app.organization_profile import industries_from_manifest

    return industries_from_manifest(_core_manifest)


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
    "internal_scheduling",
    __name__,
    url_prefix="/plugin/scheduling_module",
    template_folder=_template,
)


@internal_bp.context_processor
def _inject_assignment_label():
    """Provide assignment_label (Client / Base / Location) to all scheduling admin templates."""
    try:
        return {"assignment_label": ScheduleService.get_assignment_label()}
    except Exception:
        return {"assignment_label": "Client"}


public_bp = Blueprint(
    "public_scheduling",
    __name__,
    url_prefix="/scheduling",
    template_folder=_template,
)


def _staff_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("tb_user"):
            return redirect("/employee-portal/login?next=/scheduling/")
        return view(*args, **kwargs)
    return wrapped


def _current_contractor_id():
    return contractor_id_from_tb_user(session.get("tb_user"))


def _admin_required_scheduling(view):
    """For internal (admin app, port 82): require core user with role admin/superuser (Flask-Login)."""
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        role = (getattr(current_user, "role", None) or "").lower()
        if role not in ("admin", "superuser", "support_break_glass"):
            flash("Admin access required.", "error")
            return redirect(url_for("routes.dashboard"))
        return view(*args, **kwargs)
    return wrapped


# ---------- Public: My shifts (for Work module and staff view) ----------


@public_bp.get("/")
@_staff_required
def public_index():
    """Sling-style dashboard: upcoming shifts, full schedule link, team, and actions."""
    cid = _current_contractor_id()
    today = date.today()
    upcoming_shifts = []
    if cid:
        from datetime import timedelta
        end = today + timedelta(days=14)
        raw = ScheduleService.list_shifts(contractor_id=cid, date_from=today, date_to=end)
        from datetime import timedelta
        for s in raw:
            if (s.get("status") or "").lower() == "cancelled":
                continue
            s = dict(s)
            wd = s.get("work_date")
            if wd and hasattr(wd, "weekday"):
                s["week_monday"] = (wd - timedelta(days=wd.weekday())).isoformat()
            else:
                s["week_monday"] = None
            upcoming_shifts.append(s)
        upcoming_shifts.sort(key=lambda x: (x.get("work_date") or today, x.get("scheduled_start") or ""))
        upcoming_shifts = upcoming_shifts[:10]  # next 10 shifts on dashboard
    try:
        ai_available = is_ai_available()
    except Exception:
        ai_available = False
    return render_template(
        "scheduling_module/public/index.html",
        module_name="Scheduling & Shifts",
        module_description="View your shifts and schedule.",
        upcoming_shifts=upcoming_shifts,
        today=today,
        time_display=_time_display,
        website_settings=_get_website_settings(),
        config=_core_manifest,
        ai_available=ai_available,
    )


@public_bp.get("/my-schedule")
@_staff_required
def public_schedule_week():
    """Read-only week view of the contractor's shifts (Sling-style full schedule)."""
    cid = _current_contractor_id()
    if not cid:
        return redirect("/employee-portal/login?next=/scheduling/my-schedule")
    from datetime import timedelta
    week_s = request.args.get("week")
    if week_s:
        try:
            week_start = date.fromisoformat(week_s)
            if week_start.weekday() != 0:
                week_start -= timedelta(days=week_start.weekday())
        except ValueError:
            week_start = date.today()
            while week_start.weekday() != 0:
                week_start -= timedelta(days=1)
    else:
        week_start = date.today()
        while week_start.weekday() != 0:
            week_start -= timedelta(days=1)
    week_end = week_start + timedelta(days=6)
    shifts = ScheduleService.list_shifts(contractor_id=cid, date_from=week_start, date_to=week_end)
    week_days = [week_start + timedelta(days=i) for i in range(7)]
    shifts_by_day = {}
    for s in shifts:
        if (s.get("status") or "").lower() == "cancelled":
            continue
        wd = s.get("work_date")
        if wd:
            iso = wd.isoformat() if hasattr(wd, "isoformat") else str(wd)
            shifts_by_day.setdefault(iso, []).append(s)
    today = date.today()
    return render_template(
        "scheduling_module/public/schedule_week.html",
        week_start=week_start,
        week_end=week_end,
        week_days=week_days,
        shifts_by_day=shifts_by_day,
        today=today,
        timedelta=timedelta,
        time_display=_time_display,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.get("/team")
@_staff_required
def public_team():
    """List colleagues (no email — privacy). Names link to published-shift week view."""
    contractors = ScheduleService.list_contractors_for_team_directory()
    return render_template(
        "scheduling_module/public/team.html",
        colleagues=contractors or [],
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.get("/team/<int:contractor_id>/schedule")
@_staff_required
def public_team_member_schedule(contractor_id: int):
    """Read-only week of a colleague's published shifts. Time off / unavailability show as OFF only."""
    cid = _current_contractor_id()
    if not cid:
        return redirect("/employee-portal/login?next=" + request.path)
    allowed = set(ScheduleService.get_active_contractor_team_ids())
    if contractor_id not in allowed:
        return (
            render_template(
                "scheduling_module/public/shift_not_found.html",
                config=_core_manifest,
            ),
            404,
        )
    if contractor_id == cid:
        wk = (request.args.get("week") or "").strip()
        return redirect(
            url_for("public_scheduling.public_schedule_week", week=wk or None)
        )

    week_s = (request.args.get("week") or "").strip()
    if week_s:
        try:
            week_start = date.fromisoformat(week_s[:10])
            if week_start.weekday() != 0:
                week_start -= timedelta(days=week_start.weekday())
        except ValueError:
            week_start = date.today()
            while week_start.weekday() != 0:
                week_start -= timedelta(days=1)
    else:
        week_start = date.today()
        while week_start.weekday() != 0:
            week_start -= timedelta(days=1)
    week_end = week_start + timedelta(days=6)

    shifts = ScheduleService.list_shifts(
        contractor_id=contractor_id,
        date_from=week_start,
        date_to=week_end,
        status="published",
    )
    shifts = [s for s in shifts if (s.get("status") or "").lower() != "cancelled"]
    week_days = [week_start + timedelta(days=i) for i in range(7)]
    shifts_by_day = {}
    for s in shifts:
        wd = s.get("work_date")
        if wd:
            iso = wd.isoformat() if hasattr(wd, "isoformat") else str(wd)
            shifts_by_day.setdefault(iso, []).append(s)

    privacy_off_days = ScheduleService.contractor_privacy_absence_dates(
        contractor_id, week_start
    )
    member_name = "Team member"
    for c in ScheduleService.list_contractors_for_team_directory():
        if int(c["id"]) == contractor_id:
            member_name = (c.get("name") or c.get("initials") or "Team member").strip() or "Team member"
            break

    today = date.today()
    return render_template(
        "scheduling_module/public/team_member_schedule_week.html",
        member_name=member_name,
        member_id=contractor_id,
        week_start=week_start,
        week_end=week_end,
        week_days=week_days,
        shifts_by_day=shifts_by_day,
        privacy_off_days=privacy_off_days,
        today=today,
        timedelta=timedelta,
        time_display=_time_display,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.get("/ai")
@_staff_required
def ai_chat_page():
    """Contractor can chat with AI about availability, shifts, and coverage."""
    return render_template(
        "scheduling_module/public/ai_chat.html",
        ai_available=is_ai_available(),
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.post("/api/ai/chat")
@_staff_required
def api_ai_chat():
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Unauthorized"}), 401
    if not is_ai_available():
        return jsonify({"error": "AI assistance is not set up or enabled yet.", "reply": None}), 503
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
    reply = ai_chat(cid, messages)
    if reply is None:
        return jsonify({"error": "The assistant is unavailable. Please try again.", "reply": None}), 503
    return jsonify({"reply": reply})


@public_bp.get("/api/my-shifts")
@_staff_required
def api_my_shifts():
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Not authenticated"}), 401
    work_date = request.args.get("date")
    date_from = request.args.get("from")
    date_to = request.args.get("to")
    if work_date:
        try:
            d = date.fromisoformat(work_date)
            shifts = ScheduleService.get_my_shifts_for_date(cid, d)
        except ValueError:
            shifts = []
    elif date_from and date_to:
        try:
            df = date.fromisoformat(date_from)
            dt = date.fromisoformat(date_to)
            shifts = ScheduleService.list_shifts(contractor_id=cid, date_from=df, date_to=dt)
        except ValueError:
            shifts = []
    else:
        today = date.today()
        shifts = ScheduleService.get_my_shifts_for_date(cid, today)
    return jsonify({"shifts": shifts})


@public_bp.get("/my-requests")
@_staff_required
def my_requests():
    cid = _current_contractor_id()
    if not cid:
        return redirect("/employee-portal/login?next=/scheduling/my-requests")
    time_off_list = ScheduleService.list_time_off(contractor_id=cid)
    for r in time_off_list:
        r["start_time_display"] = _time_display(r.get("start_time")) if r.get("start_time") else None
        r["end_time_display"] = _time_display(r.get("end_time")) if r.get("end_time") else None
    return render_template(
        "scheduling_module/public/my_requests.html",
        time_off_list=time_off_list,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.post("/time-off/<int:tid>/cancel")
@_staff_required
def cancel_time_off(tid):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_requests"))
    if ScheduleService.cancel_time_off(tid, cid):
        flash("Request cancelled.", "success")
    else:
        flash("Could not cancel (only pending requests can be cancelled).", "warning")
    return redirect(url_for("public_scheduling.my_requests"))


# ---------- Shift swap (contractor) ----------


@public_bp.get("/swap")
@_staff_required
def my_swaps():
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.public_index"))
    my_list = ScheduleService.list_swap_requests(contractor_id=cid)
    available = ScheduleService.list_swap_requests(contractor_id=cid, for_claimer=True)
    my_shifts = ScheduleService.list_shifts(contractor_id=cid, date_from=date.today(), date_to=date.today() + timedelta(days=13))
    return render_template(
        "scheduling_module/public/swap.html",
        my_swaps=my_list,
        available_to_claim=available,
        my_shifts=my_shifts,
        time_display=_time_display,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.get("/open-shifts")
@_staff_required
def open_shifts_page():
    """List published open shifts for staff to claim."""
    # Default: next 14 days
    df_s = request.args.get("from")
    dt_s = request.args.get("to")
    try:
        df = date.fromisoformat(df_s) if df_s else date.today()
    except ValueError:
        df = date.today()
    try:
        dt = date.fromisoformat(dt_s) if dt_s else (df + timedelta(days=13))
    except ValueError:
        dt = df + timedelta(days=13)
    if dt < df:
        dt = df
    client_id = request.args.get("client_id", type=int)
    job_type_id = request.args.get("job_type_id", type=int)
    shifts = ScheduleService.list_open_shifts(df, dt, client_id=client_id, job_type_id=job_type_id, status="published")
    cid = _current_contractor_id()
    for s in shifts:
        if cid:
            eligible, reason = ScheduleService.check_open_shift_eligibility(cid, s)
            s["eligible"] = eligible
            s["ineligible_reason"] = reason or ""
            s["pay_preview"] = None
            wd = s.get("work_date")
            if s.get("job_type_id") is not None and wd and hasattr(wd, "toordinal"):
                try:
                    from app.plugins.time_billing_module.services import RunsheetService

                    br = s.get("break_mins") or 0
                    try:
                        br = int(br)
                    except (TypeError, ValueError):
                        br = 0
                    s["pay_preview"] = RunsheetService.contractor_shift_pay_preview(
                        int(cid),
                        job_type_id=int(s["job_type_id"]),
                        work_date=wd,
                        scheduled_start=s.get("scheduled_start"),
                        scheduled_end=s.get("scheduled_end"),
                        break_mins=br,
                        runsheet_id=s.get("runsheet_id"),
                    )
                except Exception:
                    s["pay_preview"] = None
        else:
            s["eligible"] = False
            s["ineligible_reason"] = "Not signed in."
            s["pay_preview"] = None
    clients, sites = ScheduleService.list_clients_and_sites()
    job_types = ScheduleService.list_job_types()
    return render_template(
        "scheduling_module/public/open_shifts.html",
        open_shifts=shifts,
        date_from=df,
        date_to=dt,
        clients=clients,
        job_types=job_types,
        client_id=client_id,
        job_type_id=job_type_id,
        show_eligible_filter=bool(cid),
        time_display=_time_display,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.post("/open-shifts/<int:shift_id>/claim")
@_staff_required
def claim_open_shift(shift_id: int):
    """Claim a published open shift (first-come, first-served)."""
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.open_shifts_page"))
    mode = ScheduleService.get_open_shift_claim_mode()
    if mode == "manager":
        ok, msg = ScheduleService.create_open_shift_claim(shift_id, cid)
        # Notify claimant in portal
        try:
            from app.plugins.employee_portal_module.services import admin_send_message
            admin_send_message([cid], "Open shift claim submitted", f"Your claim for open shift #{shift_id} was submitted for manager approval.", source_module="scheduling_module")
        except Exception:
            pass
    else:
        ok, msg = ScheduleService.claim_open_shift(shift_id, cid)
        if ok:
            try:
                from app.plugins.employee_portal_module.services import admin_send_message
                admin_send_message([cid], "Open shift claimed", f"You successfully claimed open shift #{shift_id}. It is now on your schedule.", source_module="scheduling_module")
            except Exception:
                pass
    flash(msg, "success" if ok else "warning")
    return redirect(url_for("public_scheduling.open_shifts_page"))


@public_bp.post("/swap/offer")
@_staff_required
def offer_shift():
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_swaps"))
    shift_id = request.form.get("shift_id", type=int)
    notes = (request.form.get("notes") or "").strip() or None
    if not shift_id:
        flash("Shift required.", "error")
        return redirect(url_for("public_scheduling.my_swaps"))
    sid = ScheduleService.create_swap_request(shift_id, cid, notes=notes)
    if sid:
        flash("Shift offered for swap. Others can claim it.", "success")
    else:
        flash("Could not offer (already offered or not your shift).", "warning")
    return redirect(url_for("public_scheduling.my_swaps"))


@public_bp.post("/swap/<int:swap_id>/claim")
@_staff_required
def claim_swap(swap_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_swaps"))
    if ScheduleService.claim_swap(swap_id, cid):
        flash("You claimed this shift. Waiting for manager approval.", "success")
    else:
        flash("Could not claim.", "warning")
    return redirect(url_for("public_scheduling.my_swaps"))


@public_bp.post("/swap/<int:swap_id>/cancel")
@_staff_required
def cancel_swap(swap_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_swaps"))
    if ScheduleService.cancel_swap(swap_id, cid):
        flash("Swap cancelled.", "success")
    return redirect(url_for("public_scheduling.my_swaps"))


@public_bp.get("/request-time-off")
@_staff_required
def request_time_off_page():
    return render_template(
        "scheduling_module/public/request_time_off.html",
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.post("/request-time-off")
@_staff_required
def request_time_off_submit():
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.request_time_off_page"))
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    reason = request.form.get("reason", "").strip() or None
    whole_day = request.form.get("whole_day", "1") == "1"
    start_time = _parse_time(request.form.get("start_time")) if not whole_day else None
    end_time = _parse_time(request.form.get("end_time")) if not whole_day else None
    if not start_date or not end_date:
        return redirect(url_for("public_scheduling.request_time_off_page"))
    try:
        sd = date.fromisoformat(start_date)
        ed = date.fromisoformat(end_date)
        if ed < sd:
            ed = sd
        if whole_day or start_time is None or end_time is None:
            ScheduleService.create_time_off(cid, sd, ed, reason=reason, type="annual")
        else:
            ScheduleService.create_time_off(cid, sd, ed, reason=reason, type="annual", start_time=start_time, end_time=end_time)
    except ValueError:
        pass
    return redirect(url_for("public_scheduling.my_requests"))


@public_bp.get("/report-sickness")
@_staff_required
def report_sickness_page():
    today = date.today().isoformat()
    return render_template(
        "scheduling_module/public/report_sickness.html",
        today=today,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.post("/report-sickness")
@_staff_required
def report_sickness_submit():
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.report_sickness_page"))
    start_date = request.form.get("start_date") or date.today().isoformat()
    end_date = request.form.get("end_date") or start_date
    reason = request.form.get("reason", "").strip() or None
    whole_day = request.form.get("whole_day", "1") == "1"
    start_time = _parse_time(request.form.get("start_time")) if not whole_day else None
    end_time = _parse_time(request.form.get("end_time")) if not whole_day else None
    try:
        sd = date.fromisoformat(start_date)
        ed = date.fromisoformat(end_date)
        if ed < sd:
            ed = sd
        if whole_day or start_time is None or end_time is None:
            ScheduleService.create_time_off(cid, sd, ed, reason=reason or "Sickness", type="sickness")
        else:
            ScheduleService.create_time_off(cid, sd, ed, reason=reason or "Sickness", type="sickness", start_time=start_time, end_time=end_time)
    except ValueError:
        pass
    return redirect(url_for("public_scheduling.my_requests"))


@public_bp.get("/my-day")
@_staff_required
def my_day():
    """Mobile-first 'My day' page: list of shifts for a selected day."""
    cid = _current_contractor_id()
    if not cid:
        return redirect("/employee-portal/login?next=/scheduling/my-day")
    day = date.today()
    date_s = (request.args.get("date") or "").strip()
    if date_s:
        try:
            day = date.fromisoformat(date_s[:10])
        except (TypeError, ValueError):
            pass
    shifts = ScheduleService.get_my_shifts_for_date(cid, day)
    return render_template(
        "scheduling_module/public/my_day.html",
        shifts=shifts,
        today=day,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.get("/shifts/<int:shift_id>")
@_staff_required
def shift_view(shift_id: int):
    """View a single shift (details, notes, assignment instructions). Allowed for assigned shifts or published open shifts."""
    cid = _current_contractor_id()
    if not cid:
        return redirect("/employee-portal/login?next=" + request.path)
    shift = ScheduleService.get_shift(shift_id)
    if not shift:
        return render_template("scheduling_module/public/shift_not_found.html", config=_core_manifest), 404
    status = (shift.get("status") or "").lower()
    assignments = shift.get("assignments") or []
    assignee_ids = [a.get("contractor_id") for a in assignments if a.get("contractor_id") is not None]
    if not assignee_ids and shift.get("contractor_id") is not None:
        assignee_ids = [shift["contractor_id"]]
    is_assigned_to_me = cid is not None and int(cid) in [int(x) for x in assignee_ids]
    required = int(shift.get("required_count") or 1)
    is_open = len(assignee_ids) < required and status == "published"
    if not is_assigned_to_me and not is_open:
        return render_template("scheduling_module/public/shift_not_found.html", config=_core_manifest), 404
    assignment_instructions = ScheduleService.get_assignment_instructions(shift["client_id"]) if shift.get("client_id") else None
    shift_tasks = ScheduleService.list_shift_tasks(shift_id) if is_assigned_to_me else []
    eligible, ineligible_reason = False, None
    if is_open and cid:
        eligible, ineligible_reason = ScheduleService.check_open_shift_eligibility(cid, shift)
    assignment_label = ScheduleService.get_assignment_label()
    from datetime import timedelta
    wd = shift.get("work_date")
    week_monday = (wd - timedelta(days=wd.weekday())).isoformat() if wd and hasattr(wd, "weekday") else None
    pay_preview = None
    if cid and shift.get("job_type_id") is not None and wd and hasattr(wd, "toordinal"):
        try:
            from app.plugins.time_billing_module.services import RunsheetService

            br = shift.get("break_mins") or 0
            try:
                br = int(br)
            except (TypeError, ValueError):
                br = 0
            pay_preview = RunsheetService.contractor_shift_pay_preview(
                int(cid),
                job_type_id=int(shift["job_type_id"]),
                work_date=wd,
                scheduled_start=shift.get("scheduled_start"),
                scheduled_end=shift.get("scheduled_end"),
                break_mins=br,
                runsheet_id=shift.get("runsheet_id"),
            )
        except Exception:
            pay_preview = None
    return render_template(
        "scheduling_module/public/shift_view.html",
        shift=shift,
        shift_tasks=shift_tasks,
        contractor_id=cid,
        assignment_instructions=assignment_instructions,
        assignment_label=assignment_label,
        is_open=is_open,
        eligible=eligible,
        ineligible_reason=ineligible_reason,
        week_monday=week_monday,
        pay_preview=pay_preview,
        time_display=_time_display,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


# ---------- Public: My availability (contractor self-service) ----------


def _time_display(t):
    """Format time or timedelta for display (HH:MM)."""
    if t is None:
        return ""
    if hasattr(t, "strftime"):
        return t.strftime("%H:%M")
    if hasattr(t, "total_seconds"):
        s = int(t.total_seconds()) % (24 * 3600)
        return f"{s // 3600:02d}:{(s % 3600) // 60:02d}"
    # String from DB e.g. "5:00:00" -> str(t)[:5] would be "5:00:"; avoid trailing colon
    s = str(t).strip()[:5]
    return s.rstrip(":") if s.endswith(":") else s


def _shift_for_json(shift):
    """Return a copy of shift dict with date/timedelta/datetime converted for JSON."""
    out = dict(shift)
    for key in ("work_date",):
        v = out.get(key)
        if v is not None and hasattr(v, "isoformat"):
            out[key] = v.isoformat()
    for key in ("scheduled_start", "scheduled_end", "actual_start", "actual_end"):
        v = out.get(key)
        if v is not None:
            out[key] = _time_display(v) or None
    for key in ("created_at", "updated_at", "sent_at", "voided_at", "claimed_at", "resolved_at"):
        v = out.get(key)
        if v is not None and hasattr(v, "isoformat"):
            out[key] = v.isoformat()
    return out


@public_bp.get("/my-availability")
@_staff_required
def my_availability():
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.public_index"))
    availability_mode = ScheduleService.get_availability_mode(cid)
    availability = ScheduleService.list_availability(cid)
    unavailability = ScheduleService.list_unavailability(cid)
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    for a in availability:
        a["start_time_display"] = _time_display(a.get("start_time"))
        a["end_time_display"] = _time_display(a.get("end_time"))
        a["day_name"] = day_names[a["day_of_week"]] if 0 <= a.get("day_of_week", -1) <= 6 else ""
        a["effective_from_display"] = a["effective_from"].strftime("%Y-%m-%d") if a.get("effective_from") and hasattr(a["effective_from"], "strftime") else str(a.get("effective_from", ""))[:10]
        a["effective_to_display"] = (a["effective_to"].strftime("%Y-%m-%d") if a["effective_to"] and hasattr(a["effective_to"], "strftime") else str(a.get("effective_to", ""))[:10]) if a.get("effective_to") else ""
    for u in unavailability:
        u["start_time_display"] = _time_display(u.get("start_time"))
        u["end_time_display"] = _time_display(u.get("end_time"))
        u["day_name"] = day_names[u["day_of_week"]] if 0 <= u.get("day_of_week", -1) <= 6 else ""
        u["effective_from_display"] = u["effective_from"].strftime("%Y-%m-%d") if u.get("effective_from") and hasattr(u["effective_from"], "strftime") else str(u.get("effective_from", ""))[:10]
        u["effective_to_display"] = (u["effective_to"].strftime("%Y-%m-%d") if u["effective_to"] and hasattr(u["effective_to"], "strftime") else str(u.get("effective_to", ""))[:10]) if u.get("effective_to") else ""
    return render_template(
        "scheduling_module/public/my_availability.html",
        availability=availability,
        unavailability=unavailability,
        availability_mode=availability_mode,
        day_names=day_names,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@public_bp.post("/my-availability/set-mode")
@_staff_required
def my_availability_set_mode():
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.public_index"))
    mode = (request.form.get("mode") or "").strip().lower()
    if mode in ("availability", "unavailability"):
        ScheduleService.set_availability_mode(cid, mode)
        flash("Switched to defining " + ("when you're available" if mode == "availability" else "when you're unavailable") + ".", "success")
    return redirect(url_for("public_scheduling.my_availability"))


def _parse_time(s):
    if not s:
        return None
    try:
        from datetime import datetime as dt
        return dt.strptime(s.strip()[:5], "%H:%M").time()
    except (ValueError, TypeError):
        return None


@public_bp.get("/availability/add")
@_staff_required
def availability_add_form():
    return render_template(
        "scheduling_module/public/availability_form.html",
        avail=None,
        config=_core_manifest,
    )


@public_bp.post("/availability/add")
@_staff_required
def availability_add():
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_availability"))
    days_raw = request.form.getlist("days")
    day_of_week = request.form.get("day_of_week", type=int)
    if days_raw:
        day_list = []
        for d in days_raw:
            try:
                v = int(d)
                if 0 <= v <= 6 and v not in day_list:
                    day_list.append(v)
            except (ValueError, TypeError):
                pass
        if not day_list:
            day_list = [day_of_week] if day_of_week is not None and 0 <= day_of_week <= 6 else None
    else:
        day_list = [day_of_week] if day_of_week is not None and 0 <= day_of_week <= 6 else None
    if not day_list:
        flash("Please select at least one day.", "error")
        return redirect(url_for("public_scheduling.availability_add_form"))
    start_time = _parse_time(request.form.get("start_time"))
    end_time = _parse_time(request.form.get("end_time"))
    effective_from_s = request.form.get("effective_from")
    effective_to_s = request.form.get("effective_to") or None
    if start_time is None or end_time is None or not effective_from_s:
        flash("Please fill in start time, end time, and effective from date.", "error")
        return redirect(url_for("public_scheduling.availability_add_form"))
    try:
        effective_from = date.fromisoformat(effective_from_s)
        effective_to = date.fromisoformat(effective_to_s) if effective_to_s else None
    except ValueError:
        flash("Invalid date.", "error")
        return redirect(url_for("public_scheduling.availability_add_form"))
    for dow in day_list:
        ScheduleService.add_availability(cid, dow, start_time, end_time, effective_from, effective_to)
    flash("Availability added." if len(day_list) == 1 else f"Availability added for {len(day_list)} days.", "success")
    return redirect(url_for("public_scheduling.my_availability"))


@public_bp.get("/availability/<int:avail_id>/edit")
@_staff_required
def availability_edit_form(avail_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_availability"))
    avail = ScheduleService.get_availability(avail_id, contractor_id=cid)
    if not avail:
        flash("Not found.", "error")
        return redirect(url_for("public_scheduling.my_availability"))
    avail["start_time_str"] = _time_display(avail.get("start_time"))
    avail["end_time_str"] = _time_display(avail.get("end_time"))
    if avail.get("effective_from"):
        avail["effective_from_iso"] = avail["effective_from"].isoformat() if hasattr(avail["effective_from"], "isoformat") else str(avail["effective_from"])[:10]
    else:
        avail["effective_from_iso"] = ""
    avail["effective_to_iso"] = ""
    if avail.get("effective_to"):
        avail["effective_to_iso"] = avail["effective_to"].isoformat() if hasattr(avail["effective_to"], "isoformat") else str(avail["effective_to"])[:10]
    return render_template(
        "scheduling_module/public/availability_form.html",
        avail=avail,
        config=_core_manifest,
    )


@public_bp.post("/availability/<int:avail_id>/edit")
@_staff_required
def availability_edit(avail_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_availability"))
    avail = ScheduleService.get_availability(avail_id, contractor_id=cid)
    if not avail:
        flash("Not found.", "error")
        return redirect(url_for("public_scheduling.my_availability"))
    day_of_week = request.form.get("day_of_week", type=int)
    start_time = _parse_time(request.form.get("start_time"))
    end_time = _parse_time(request.form.get("end_time"))
    effective_from_s = request.form.get("effective_from")
    effective_to_s = request.form.get("effective_to") or None
    updates = {}
    if day_of_week is not None and 0 <= day_of_week <= 6:
        updates["day_of_week"] = day_of_week
    if start_time is not None:
        updates["start_time"] = start_time
    if end_time is not None:
        updates["end_time"] = end_time
    if effective_from_s:
        try:
            updates["effective_from"] = date.fromisoformat(effective_from_s)
        except ValueError:
            pass
    if effective_to_s:
        try:
            updates["effective_to"] = date.fromisoformat(effective_to_s)
        except ValueError:
            pass
    if updates:
        ScheduleService.update_availability(avail_id, cid, **updates)
        flash("Availability updated.", "success")
    return redirect(url_for("public_scheduling.my_availability"))


@public_bp.post("/availability/<int:avail_id>/delete")
@_staff_required
def availability_delete(avail_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_availability"))
    if ScheduleService.delete_availability(avail_id, cid):
        flash("Availability removed.", "success")
    else:
        flash("Could not remove.", "warning")
    return redirect(url_for("public_scheduling.my_availability"))


# ---------- Public: Unavailability (when I'm not available) ----------

@public_bp.get("/unavailability/add")
@_staff_required
def unavailability_add_form():
    return render_template(
        "scheduling_module/public/availability_form.html",
        avail=None,
        mode="unavailability",
        config=_core_manifest,
    )


@public_bp.post("/unavailability/add")
@_staff_required
def unavailability_add():
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_availability"))
    days_raw = request.form.getlist("days")
    day_of_week = request.form.get("day_of_week", type=int)
    if days_raw:
        day_list = []
        for d in days_raw:
            try:
                v = int(d)
                if 0 <= v <= 6 and v not in day_list:
                    day_list.append(v)
            except (ValueError, TypeError):
                pass
        if not day_list:
            day_list = [day_of_week] if day_of_week is not None and 0 <= day_of_week <= 6 else None
    else:
        day_list = [day_of_week] if day_of_week is not None and 0 <= day_of_week <= 6 else None
    if not day_list:
        flash("Please select at least one day.", "error")
        return redirect(url_for("public_scheduling.unavailability_add_form"))
    start_time = _parse_time(request.form.get("start_time"))
    end_time = _parse_time(request.form.get("end_time"))
    effective_from_s = request.form.get("effective_from")
    effective_to_s = request.form.get("effective_to") or None
    if start_time is None or end_time is None or not effective_from_s:
        flash("Please fill in start time, end time, and effective from date.", "error")
        return redirect(url_for("public_scheduling.unavailability_add_form"))
    try:
        effective_from = date.fromisoformat(effective_from_s)
        effective_to = date.fromisoformat(effective_to_s) if effective_to_s else None
    except ValueError:
        flash("Invalid date.", "error")
        return redirect(url_for("public_scheduling.unavailability_add_form"))
    for dow in day_list:
        ScheduleService.add_unavailability(cid, dow, start_time, end_time, effective_from, effective_to)
    flash("Unavailability added." if len(day_list) == 1 else f"Unavailability added for {len(day_list)} days.", "success")
    return redirect(url_for("public_scheduling.my_availability"))


@public_bp.get("/unavailability/<int:una_id>/edit")
@_staff_required
def unavailability_edit_form(una_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_availability"))
    una = ScheduleService.get_unavailability(una_id, contractor_id=cid)
    if not una:
        flash("Not found.", "error")
        return redirect(url_for("public_scheduling.my_availability"))
    avail = {
        "id": una["id"],
        "day_of_week": una["day_of_week"],
        "start_time": una["start_time"],
        "end_time": una["end_time"],
        "effective_from": una["effective_from"],
        "effective_to": una.get("effective_to"),
        "start_time_str": _time_display(una.get("start_time")),
        "end_time_str": _time_display(una.get("end_time")),
        "effective_from_iso": una["effective_from"].isoformat() if hasattr(una["effective_from"], "isoformat") else str(una.get("effective_from", ""))[:10],
        "effective_to_iso": una["effective_to"].isoformat() if una.get("effective_to") and hasattr(una["effective_to"], "isoformat") else (str(una.get("effective_to", ""))[:10] if una.get("effective_to") else ""),
    }
    return render_template(
        "scheduling_module/public/availability_form.html",
        avail=avail,
        mode="unavailability",
        config=_core_manifest,
    )


@public_bp.post("/unavailability/<int:una_id>/edit")
@_staff_required
def unavailability_edit(una_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_availability"))
    una = ScheduleService.get_unavailability(una_id, contractor_id=cid)
    if not una:
        flash("Not found.", "error")
        return redirect(url_for("public_scheduling.my_availability"))
    day_of_week = request.form.get("day_of_week", type=int)
    start_time = _parse_time(request.form.get("start_time"))
    end_time = _parse_time(request.form.get("end_time"))
    effective_from_s = request.form.get("effective_from")
    effective_to_s = request.form.get("effective_to") or None
    updates = {}
    if day_of_week is not None and 0 <= day_of_week <= 6:
        updates["day_of_week"] = day_of_week
    if start_time is not None:
        updates["start_time"] = start_time
    if end_time is not None:
        updates["end_time"] = end_time
    if effective_from_s:
        try:
            updates["effective_from"] = date.fromisoformat(effective_from_s)
        except ValueError:
            pass
    if effective_to_s:
        try:
            updates["effective_to"] = date.fromisoformat(effective_to_s)
        except ValueError:
            pass
    if updates:
        ScheduleService.update_unavailability(una_id, cid, **updates)
        flash("Unavailability updated.", "success")
    return redirect(url_for("public_scheduling.my_availability"))


@public_bp.post("/unavailability/<int:una_id>/delete")
@_staff_required
def unavailability_delete(una_id):
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_scheduling.my_availability"))
    if ScheduleService.delete_unavailability(una_id, cid):
        flash("Unavailability removed.", "success")
    else:
        flash("Could not remove.", "warning")
    return redirect(url_for("public_scheduling.my_availability"))


# ---------- Public: Single shift (for Work module to record times) ----------


def _shift_assigned_to_contractor(shift, cid):
    """True if contractor cid is assigned to this shift (single or multi-assign)."""
    if not cid:
        return False
    assignees = shift.get("assignments") or []
    if not assignees and shift.get("contractor_id") is not None:
        return int(shift["contractor_id"]) == int(cid)
    return any(int(a.get("contractor_id")) == int(cid) for a in assignees if a.get("contractor_id") is not None)


@public_bp.get("/api/shifts/<int:shift_id>")
@_staff_required
def api_get_shift(shift_id):
    shift = ScheduleService.get_shift(shift_id)
    if not shift:
        return jsonify({"error": "Not found"}), 404
    cid = _current_contractor_id()
    if not _shift_assigned_to_contractor(shift, cid):
        return jsonify({"error": "Forbidden"}), 403
    return jsonify(_shift_for_json(shift))


@public_bp.patch("/api/shifts/<int:shift_id>")
@_staff_required
def api_patch_shift(shift_id):
    shift = ScheduleService.get_shift(shift_id)
    if not shift:
        return jsonify({"error": "Not found"}), 404
    cid = _current_contractor_id()
    if not _shift_assigned_to_contractor(shift, cid):
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json() or {}
    updates = {}
    if "actual_start" in data:
        updates["actual_start"] = data["actual_start"]
    if "actual_end" in data:
        updates["actual_end"] = data["actual_end"]
    if "notes" in data:
        updates["notes"] = data["notes"]
    if updates:
        ScheduleService.update_shift(shift_id, updates, portal_notify=False)
    return jsonify({"ok": True})


@public_bp.get("/api/shifts/<int:shift_id>/tasks")
@_staff_required
def api_shift_tasks(shift_id):
    shift = ScheduleService.get_shift(shift_id)
    if not shift:
        return jsonify({"error": "Not found"}), 404
    cid = _current_contractor_id()
    if not _shift_assigned_to_contractor(shift, cid):
        return jsonify({"error": "Forbidden"}), 403
    tasks = ScheduleService.list_shift_tasks(shift_id)
    return jsonify({"tasks": tasks})


@public_bp.post("/api/shifts/<int:shift_id>/tasks/<int:task_id>/complete")
@_staff_required
def api_shift_task_complete(shift_id, task_id):
    shift = ScheduleService.get_shift(shift_id)
    if not shift:
        return jsonify({"error": "Not found"}), 404
    cid = _current_contractor_id()
    if not _shift_assigned_to_contractor(shift, cid):
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json() or {}
    complete = data.get("complete", True)
    ok = ScheduleService.set_shift_task_complete(task_id, cid, complete=complete)
    if not ok:
        return jsonify({"error": "Task not found"}), 404
    if complete:
        try:
            from app.plugins.employee_portal_module.services import complete_todo_by_reference
            complete_todo_by_reference("scheduling_module", "schedule_shift_task", str(task_id))
        except Exception:
            pass
    return jsonify({"ok": True})


@public_bp.post("/api/shifts/<int:shift_id>/clock-in")
@_staff_required
def api_clock_in(shift_id):
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json() or {}
    lat = data.get("lat") if data.get("lat") is not None else request.args.get("lat", type=float)
    lng = data.get("lng") if data.get("lng") is not None else request.args.get("lng", type=float)
    ok, msg = ScheduleService.clock_in_shift(shift_id, cid, lat=lat, lng=lng)
    if not ok:
        return jsonify({"error": msg}), 400
    shift = ScheduleService.get_shift(shift_id)
    return jsonify({"ok": True, "message": msg, "shift": shift})


@public_bp.post("/api/shifts/<int:shift_id>/clock-out")
@_staff_required
def api_clock_out(shift_id):
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json() or {}
    lat = data.get("lat") if data.get("lat") is not None else request.args.get("lat", type=float)
    lng = data.get("lng") if data.get("lng") is not None else request.args.get("lng", type=float)
    ok, msg = ScheduleService.clock_out_shift(shift_id, cid, lat=lat, lng=lng)
    if not ok:
        return jsonify({"error": msg}), 400
    shift = ScheduleService.get_shift(shift_id)
    return jsonify({"ok": True, "message": msg, "shift": shift})


# ---------- Public API (contractor app) ----------


def _serialize_availability(a):
    out = dict(a)
    out["start_time"] = _time_display(a.get("start_time"))
    out["end_time"] = _time_display(a.get("end_time"))
    for k in ("effective_from", "effective_to"):
        if out.get(k) and hasattr(out[k], "isoformat"):
            out[k] = out[k].isoformat()
    return out


@public_bp.get("/api/my-time-off")
@_staff_required
def api_my_time_off():
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Unauthorized"}), 401
    date_from_s = request.args.get("from")
    date_to_s = request.args.get("to")
    date_from = date.fromisoformat(date_from_s) if date_from_s else None
    date_to = date.fromisoformat(date_to_s) if date_to_s else None
    rows = ScheduleService.list_time_off(contractor_id=cid, date_from=date_from, date_to=date_to)
    for r in rows:
        for k in ("start_date", "end_date"):
            if r.get(k) and hasattr(r[k], "isoformat"):
                r[k] = r[k].isoformat()
    return jsonify({"time_off": rows})


@public_bp.post("/api/time-off")
@_staff_required
def api_create_time_off():
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json() or {}
    start_s = data.get("start_date")
    end_s = data.get("end_date") or start_s
    reason = data.get("reason")
    type_val = data.get("type") or "annual"
    if not start_s:
        return jsonify({"error": "start_date required"}), 400
    try:
        start_date = date.fromisoformat(start_s)
        end_date = date.fromisoformat(end_s) if end_s else start_date
        if end_date < start_date:
            end_date = start_date
        tid = ScheduleService.create_time_off(cid, start_date, end_date, reason=reason, type=type_val)
        return jsonify({"id": tid}), 201
    except ValueError:
        return jsonify({"error": "Invalid date"}), 400


@public_bp.post("/api/time-off/<int:tid>/cancel")
@_staff_required
def api_cancel_time_off(tid):
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Unauthorized"}), 401
    if ScheduleService.cancel_time_off(tid, cid):
        return jsonify({"ok": True})
    return jsonify({"error": "Not found or cannot cancel"}), 404


@public_bp.post("/api/report-sickness")
@_staff_required
def api_report_sickness():
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json() or {}
    start_s = data.get("start_date")
    end_s = data.get("end_date") or start_s
    reason = (data.get("reason") or "").strip() or "Sickness"
    if not start_s:
        return jsonify({"error": "start_date required"}), 400
    try:
        start_date = date.fromisoformat(start_s)
        end_date = date.fromisoformat(end_s) if end_s else start_date
        if end_date < start_date:
            end_date = start_date
        tid = ScheduleService.create_time_off(cid, start_date, end_date, reason=reason, type="sickness")
        return jsonify({"id": tid}), 201
    except ValueError:
        return jsonify({"error": "Invalid date"}), 400


@public_bp.get("/api/availability")
@_staff_required
def api_list_availability():
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Unauthorized"}), 401
    rows = ScheduleService.list_availability(cid)
    return jsonify({"availability": [_serialize_availability(a) for a in rows]})


@public_bp.post("/api/availability")
@_staff_required
def api_create_availability():
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json() or {}
    day_of_week = data.get("day_of_week")
    start_time_s = data.get("start_time")
    end_time_s = data.get("end_time")
    effective_from_s = data.get("effective_from")
    effective_to_s = data.get("effective_to")
    if day_of_week is None or not start_time_s or not end_time_s or not effective_from_s:
        return jsonify({"error": "day_of_week, start_time, end_time, effective_from required"}), 400
    start_time = _parse_time(start_time_s) if isinstance(start_time_s, str) else None
    end_time = _parse_time(end_time_s) if isinstance(end_time_s, str) else None
    if start_time is None or end_time is None:
        return jsonify({"error": "Invalid start_time or end_time (use HH:MM)"}), 400
    try:
        effective_from = date.fromisoformat(effective_from_s)
        effective_to = date.fromisoformat(effective_to_s) if effective_to_s else None
    except ValueError:
        return jsonify({"error": "Invalid effective_from or effective_to"}), 400
    if not (0 <= day_of_week <= 6):
        return jsonify({"error": "day_of_week must be 0-6 (Mon-Sun)"}), 400
    ScheduleService.add_availability(cid, day_of_week, start_time, end_time, effective_from, effective_to)
    return jsonify({"ok": True}), 201


@public_bp.put("/api/availability/<int:avail_id>")
@_staff_required
def api_update_availability(avail_id):
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Unauthorized"}), 401
    avail = ScheduleService.get_availability(avail_id, contractor_id=cid)
    if not avail:
        return jsonify({"error": "Not found"}), 404
    data = request.get_json() or {}
    updates = {}
    if "day_of_week" in data and 0 <= data["day_of_week"] <= 6:
        updates["day_of_week"] = data["day_of_week"]
    if data.get("start_time") is not None:
        t = _parse_time(data["start_time"]) if isinstance(data["start_time"], str) else None
        if t is not None:
            updates["start_time"] = t
    if data.get("end_time") is not None:
        t = _parse_time(data["end_time"]) if isinstance(data["end_time"], str) else None
        if t is not None:
            updates["end_time"] = t
    if data.get("effective_from"):
        try:
            updates["effective_from"] = date.fromisoformat(data["effective_from"])
        except ValueError:
            pass
    if data.get("effective_to") is not None:
        try:
            updates["effective_to"] = date.fromisoformat(data["effective_to"]) if data["effective_to"] else None
        except ValueError:
            pass
    if updates:
        ScheduleService.update_availability(avail_id, cid, **updates)
    return jsonify({"ok": True})


@public_bp.delete("/api/availability/<int:avail_id>")
@_staff_required
def api_delete_availability(avail_id):
    cid = _current_contractor_id()
    if not cid:
        return jsonify({"error": "Unauthorized"}), 401
    if ScheduleService.delete_availability(avail_id, cid):
        return jsonify({"ok": True})
    return jsonify({"error": "Not found"}), 404


# ---------- Internal (admin) ----------


@internal_bp.get("/")
@_admin_required_scheduling
def admin_index():
    _ensure_scheduling_tables()
    pending_claims = 0
    try:
        pending_claims = int(ScheduleService.count_open_shift_claims("claimed") or 0)
    except Exception:
        pending_claims = 0
    return render_template(
        "scheduling_module/admin/index.html",
        pending_open_shift_claims=pending_claims,
        config=_core_manifest,
        url_tb_runsheets_board=_safe_url_for(
            "internal_time_billing.runsheets_page", view="board"
        ),
        url_tb_staffing_overview=_safe_url_for(
            "internal_time_billing.runsheets_staffing_overview_page"
        ),
        url_mr_cura_hub=_safe_url_for("medical_records_internal.cura_ops_hub"),
    )


@internal_bp.get("/open-shifts/claims")
@_admin_required_scheduling
def admin_open_shift_claims():
    _ensure_scheduling_tables()
    status = request.args.get("status") or "claimed"
    if status not in ("claimed", "approved", "rejected", "cancelled", "all"):
        status = "claimed"
    claims = ScheduleService.list_open_shift_claims(status=None if status == "all" else status, limit=200)
    mode = ScheduleService.get_open_shift_claim_mode()
    return render_template(
        "scheduling_module/admin/open_shift_claims.html",
        claims=claims,
        status=status,
        mode=mode,
        config=_core_manifest,
    )


@internal_bp.post("/open-shifts/claims/<int:claim_id>/resolve")
@_admin_required_scheduling
def admin_resolve_open_shift_claim(claim_id: int):
    _ensure_scheduling_tables()
    action = request.form.get("action") or ""
    notes = (request.form.get("admin_notes") or "").strip() or None
    user_id = getattr(current_user, "id", None)
    ok, msg = ScheduleService.resolve_open_shift_claim(claim_id, action, resolved_by_user_id=user_id, admin_notes=notes)
    # Notify claimant
    if ok:
        try:
            # Load claim to get claimer id
            rows = [c for c in ScheduleService.list_open_shift_claims(status=None, limit=500) if int(c.get("id") or 0) == int(claim_id)]
            claimer_id = int(rows[0].get("claimer_contractor_id")) if rows else None
            if claimer_id:
                from app.plugins.employee_portal_module.services import admin_send_message
                subj = "Open shift claim approved" if (action or "").lower() == "approve" else "Open shift claim rejected"
                body = f"Your open shift claim (request #{claim_id}) was {('approved' if (action or '').lower() == 'approve' else 'rejected')}."
                if notes:
                    body += f"\n\nManager notes: {notes}"
                admin_send_message([claimer_id], subj, body, source_module="scheduling_module", sent_by_user_id=user_id)
        except Exception:
            pass
    flash(msg, "success" if ok else "warning")
    return redirect(url_for("internal_scheduling.admin_open_shift_claims"))


@internal_bp.post("/settings/open-shift-claim-mode")
@_admin_required_scheduling
def admin_set_open_shift_claim_mode():
    _ensure_scheduling_tables()
    mode = request.form.get("mode") or "auto"
    if ScheduleService.set_open_shift_claim_mode(mode):
        flash("Settings saved.", "success")
    else:
        flash("Invalid mode.", "warning")
    return redirect(url_for("internal_scheduling.admin_open_shift_claims"))


@internal_bp.get("/job-type-requirements")
@_admin_required_scheduling
def admin_job_type_requirements():
    _ensure_scheduling_tables()
    job_types = ScheduleService.list_job_types()
    reqs = {}
    for jt in job_types:
        reqs[int(jt["id"])] = ScheduleService.get_job_type_requirements(int(jt["id"]))
    return render_template(
        "scheduling_module/admin/job_type_requirements.html",
        job_types=job_types,
        reqs=reqs,
        config=_core_manifest,
    )


@internal_bp.post("/job-type-requirements/<int:job_type_id>")
@_admin_required_scheduling
def admin_job_type_requirements_save(job_type_id: int):
    _ensure_scheduling_tables()
    if request.form.get("clear"):
        skills, quals = [], []
    else:
        skills_raw = (request.form.get("skills") or "").strip()
        quals_raw = (request.form.get("qualifications") or "").strip()
        skills = [s.strip() for s in skills_raw.split(",") if s.strip()]
        quals = [q.strip() for q in quals_raw.split(",") if q.strip()]
    if ScheduleService.set_job_type_requirements(job_type_id, skills, quals):
        flash("Requirements cleared." if request.form.get("clear") else "Requirements saved.", "success")
    else:
        flash("Unable to save requirements.", "warning")
    return redirect(url_for("internal_scheduling.admin_job_type_requirements"))


@internal_bp.get("/schedule/week")
@_admin_required_scheduling
def admin_schedule_week():
    """Week calendar view: pick week, see shifts by day. Sling-style."""
    _ensure_scheduling_tables()
    from datetime import timedelta
    week_start_s = request.args.get("week")  # YYYY-MM-DD (Monday)
    if week_start_s:
        try:
            week_start = date.fromisoformat(week_start_s)
        except ValueError:
            week_start = date.today()
            while week_start.weekday() != 0:
                week_start -= timedelta(days=1)
    else:
        week_start = date.today()
        while week_start.weekday() != 0:
            week_start -= timedelta(days=1)
    week_end = week_start + timedelta(days=6)
    contractor_id = request.args.get("contractor_id", type=int)
    client_id = request.args.get("client_id", type=int)
    view_mode = (request.args.get("view") or "staff").strip().lower()
    if view_mode not in ("staff", "job"):
        view_mode = "staff"
    shifts = ScheduleService.list_shifts(
        date_from=week_start,
        date_to=week_end,
        contractor_id=contractor_id,
        client_id=client_id,
    )
    job_bundles = (
        ScheduleService.week_job_bundle_rows(shifts, week_start, week_end)
        if view_mode == "job"
        else []
    )
    contractors = ScheduleService.list_contractors()
    # Add pseudo-row for unassigned (open) shifts.
    contractors = [{"id": 0, "name": "Open shifts", "email": ""}] + (contractors or [])
    clients, sites = ScheduleService.list_clients_and_sites()
    job_types = ScheduleService.list_job_types()
    week_days = [week_start + timedelta(days=i) for i in range(7)]
    # Key (contractor_id, date_iso) -> list of shifts for template (multi-assign: shift appears in each assignee's row and in Open if under required_count)
    from collections import defaultdict
    shift_ids = [s["id"] for s in shifts]
    assignments_map = ScheduleService.get_assignments_for_shifts(shift_ids) if shift_ids else {}
    shifts_by = defaultdict(list)
    for s in shifts:
        wd = s.get("work_date")
        if not wd:
            continue
        date_iso = wd.isoformat() if hasattr(wd, "isoformat") else str(wd)
        assignees = assignments_map.get(s["id"]) or []
        if not assignees and s.get("contractor_id") is not None:
            assignees = [{"contractor_id": s["contractor_id"]}]
        for a in assignees:
            cid_key = int(a["contractor_id"])
            shifts_by[(cid_key, date_iso)].append(s)
        required = int(s.get("required_count") or 1)
        if len(assignees) < required:
            shifts_by[(0, date_iso)].append(s)
    # Time-off overlay: (contractor_id, date_iso) -> list of time-off (type, etc.)
    time_off_list = ScheduleService.list_time_off(date_from=week_start, date_to=week_end, status=None)
    time_off_by = defaultdict(list)
    for to in time_off_list:
        cid = to.get("contractor_id")
        start = to.get("start_date")
        end = to.get("end_date")
        if cid is None or not start or not end:
            continue
        for i in range(7):
            d = week_start + timedelta(days=i)
            if start <= d <= end and to.get("status") in ("requested", "approved"):
                time_off_by[(cid, d.isoformat())].append(to)
    overlap_shift_ids = ScheduleService.find_overlapping_shift_ids(shifts)
    labor_totals = ScheduleService.get_weekly_labor_totals(week_start)
    contractor_ids_for_unavail = [c["id"] for c in contractors if c.get("id")]
    unavailability_by_contractor = ScheduleService.get_unavailability_for_week(contractor_ids_for_unavail, week_start)
    job_type_palette = [
        "rgba(234,179,8,0.25)", "rgba(168,85,247,0.25)", "rgba(34,197,94,0.25)",
        "rgba(239,68,68,0.2)", "rgba(59,130,246,0.25)", "rgba(236,72,153,0.2)",
    ]
    today = date.today()
    return render_template(
        "scheduling_module/admin/schedule_week.html",
        week_start=week_start,
        week_end=week_end,
        week_days=week_days,
        today=today,
        shifts=shifts,
        shifts_by=dict(shifts_by),
        time_off_by=dict(time_off_by),
        contractors=contractors,
        clients=clients,
        sites=sites,
        job_types=job_types,
        job_type_palette=job_type_palette,
        overlap_shift_ids=overlap_shift_ids,
        labor_totals=labor_totals,
        unavailability_by_contractor=unavailability_by_contractor,
        contractor_id=contractor_id,
        client_id=client_id,
        view_mode=view_mode,
        job_bundles=job_bundles,
        timedelta=timedelta,
        time_display=_time_display,
        config=_core_manifest,
    )


@internal_bp.get("/schedule/month")
@_admin_required_scheduling
def admin_schedule_month():
    """Month calendar view: pick month, see shift counts per day, click to week or add shift."""
    _ensure_scheduling_tables()
    from datetime import timedelta
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)
    if not year or not month:
        today = date.today()
        year, month = today.year, today.month
    try:
        first = date(year, month, 1)
    except (ValueError, TypeError):
        first = date.today().replace(day=1)
    # Last day of month
    if month == 12:
        last = date(year, 12, 31)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    contractor_id = request.args.get("contractor_id", type=int)
    client_id = request.args.get("client_id", type=int)
    shifts = ScheduleService.list_shifts(
        date_from=first,
        date_to=last,
        contractor_id=contractor_id,
        client_id=client_id,
    )
    job_type_palette = [
        "rgba(234,179,8,0.85)", "rgba(168,85,247,0.85)", "rgba(34,197,94,0.85)",
        "rgba(239,68,68,0.75)", "rgba(59,130,246,0.85)", "rgba(236,72,153,0.75)",
    ]
    jt_counts_by_day = ScheduleService.list_shift_jobtype_counts_by_day(
        first, last, contractor_id=contractor_id, client_id=client_id
    )
    # Count per day: day_iso -> count
    from collections import defaultdict
    shifts_per_day = defaultdict(int)
    for s in shifts:
        wd = s.get("work_date")
        if wd:
            shifts_per_day[wd.isoformat() if hasattr(wd, "isoformat") else str(wd)] += 1
    # Shifts per day for block display (exclude cancelled)
    shifts_by_day = defaultdict(list)
    for s in shifts:
        if (s.get("status") or "").lower() == "cancelled":
            continue
        wd = s.get("work_date")
        if wd:
            iso = wd.isoformat() if hasattr(wd, "isoformat") else str(wd)
            shifts_by_day[iso].append(s)
    for iso in shifts_by_day:
        shifts_by_day[iso].sort(key=lambda x: (x.get("scheduled_start") or ""))
    # Calendar grid: weeks (each week Mon-Sun), first week may start in prev month
    week_start = first - timedelta(days=first.weekday())
    weeks = []
    current = week_start
    while current <= last or current.month == month or (current - week_start).days < 35:
        week_days = []
        for i in range(7):
            d = current + timedelta(days=i)
            iso = d.isoformat()
            week_days.append({
                "date": d,
                "iso": iso,
                "count": shifts_per_day.get(iso, 0),
                "job_types": jt_counts_by_day.get(iso, {}),
                "in_month": d.month == month,
                "shifts": shifts_by_day.get(iso, []),
            })
        weeks.append(week_days)
        current += timedelta(days=7)
        if current > last and current.month != month:
            break
    contractors = ScheduleService.list_contractors()
    clients, _ = ScheduleService.list_clients_and_sites()
    today = date.today()
    return render_template(
        "scheduling_module/admin/schedule_month.html",
        year=year,
        month=month,
        first=first,
        last=last,
        weeks=weeks,
        today=today,
        shifts_per_day=dict(shifts_per_day),
        contractors=contractors,
        clients=clients,
        job_type_palette=job_type_palette,
        contractor_id=contractor_id,
        client_id=client_id,
        timedelta=timedelta,
        time_display=_time_display,
        config=_core_manifest,
    )


@internal_bp.post("/schedule/copy-week")
@_admin_required_scheduling
def admin_copy_week():
    """Copy previous week's shifts into the current week (as drafts)."""
    from datetime import timedelta
    week_s = request.form.get("week") or request.args.get("week")
    if not week_s:
        flash("Week required.", "error")
        return redirect(url_for("internal_scheduling.admin_schedule_week"))
    try:
        to_monday = date.fromisoformat(week_s)
        if to_monday.weekday() != 0:
            to_monday = to_monday - timedelta(days=to_monday.weekday())
        from_monday = to_monday - timedelta(days=7)
    except ValueError:
        flash("Invalid week.", "error")
        return redirect(url_for("internal_scheduling.admin_schedule_week"))
    count = ScheduleService.copy_week_shifts(from_monday, to_monday)
    flash(f"Copied {count} shift(s) from previous week as drafts.", "success")
    return redirect(url_for("internal_scheduling.admin_schedule_week", week=to_monday.isoformat()))


@internal_bp.get("/schedule/week/save-as-template")
@_admin_required_scheduling
def admin_save_week_as_template_form():
    """Form to create a template from the current week's shifts."""
    week_s = request.args.get("week")
    if not week_s:
        flash("Choose a week from the week view first.", "error")
        return redirect(url_for("internal_scheduling.admin_schedule_week"))
    try:
        week_start = date.fromisoformat(week_s)
        if week_start.weekday() != 0:
            week_start = week_start - timedelta(days=week_start.weekday())
    except ValueError:
        flash("Invalid week.", "error")
        return redirect(url_for("internal_scheduling.admin_schedule_week"))
    contractors = ScheduleService.list_contractors()
    return render_template(
        "scheduling_module/admin/save_week_as_template.html",
        week_start=week_start,
        contractors=contractors,
        timedelta=timedelta,
        config=_core_manifest,
    )


@internal_bp.post("/schedule/week/save-as-template")
@_admin_required_scheduling
def admin_save_week_as_template():
    week_s = request.form.get("week_monday")
    contractor_id = request.form.get("contractor_id", type=int) or None
    name = (request.form.get("name") or "").strip() or None
    if not week_s:
        flash("Week required.", "error")
        return redirect(url_for("internal_scheduling.admin_schedule_week"))
    try:
        week_monday = date.fromisoformat(week_s)
        if week_monday.weekday() != 0:
            week_monday = week_monday - timedelta(days=week_monday.weekday())
    except ValueError:
        flash("Invalid week.", "error")
        return redirect(url_for("internal_scheduling.admin_schedule_week"))
    template_id = ScheduleService.create_template_from_week(week_monday, contractor_id=contractor_id, name=name)
    if not template_id:
        flash("No shifts in that week (or for that contractor). Add shifts first.", "error")
        return redirect(url_for("internal_scheduling.admin_save_week_as_template_form", week=week_monday.isoformat()))
    flash("Template created from week. You can edit it below.", "success")
    return redirect(url_for("internal_scheduling.admin_template_edit", template_id=template_id))


# ---------- Templates ----------


@internal_bp.get("/templates")
@_admin_required_scheduling
def admin_templates():
    templates = ScheduleService.list_templates()
    for t in templates:
        t["slot_count"] = len(ScheduleService.list_template_slots(t["id"]))
    return render_template(
        "scheduling_module/admin/templates.html",
        templates=templates,
        config=_core_manifest,
    )


@internal_bp.get("/templates/new")
@_admin_required_scheduling
def admin_template_new():
    clients, sites = ScheduleService.list_clients_and_sites()
    job_types = ScheduleService.list_job_types()
    return render_template(
        "scheduling_module/admin/template_form.html",
        template=None,
        clients=clients,
        sites=sites,
        job_types=job_types,
        config=_core_manifest,
    )


@internal_bp.post("/templates/new")
@_admin_required_scheduling
def admin_template_create():
    name = (request.form.get("name") or "").strip()
    if not name:
        flash("Name required.", "error")
        return redirect(url_for("internal_scheduling.admin_template_new"))
    client_id = request.form.get("client_id", type=int)
    site_id = request.form.get("site_id", type=int)
    job_type_id = request.form.get("job_type_id", type=int)
    tid = ScheduleService.create_template(name, client_id=client_id or None, site_id=site_id or None, job_type_id=job_type_id or None)
    flash("Template created. Add slots below.", "success")
    return redirect(url_for("internal_scheduling.admin_template_edit", template_id=tid))


def _parse_time_internal(s):
    if not s or not isinstance(s, str):
        return None
    try:
        from datetime import datetime as dt
        return dt.strptime(s.strip()[:5], "%H:%M").time()
    except (ValueError, TypeError):
        return None


@internal_bp.get("/templates/<int:template_id>/edit")
@_admin_required_scheduling
def admin_template_edit(template_id):
    template = ScheduleService.get_template(template_id)
    if not template:
        flash("Template not found.", "error")
        return redirect(url_for("internal_scheduling.admin_templates"))
    slots = ScheduleService.list_template_slots(template_id)
    for slot in slots:
        t = slot.get("start_time")
        slot["start_time_str"] = t.strftime("%H:%M") if t and hasattr(t, "strftime") else (str(t)[:5] if t else "")
        t = slot.get("end_time")
        slot["end_time_str"] = t.strftime("%H:%M") if t and hasattr(t, "strftime") else (str(t)[:5] if t else "")
    clients, sites = ScheduleService.list_clients_and_sites()
    job_types = ScheduleService.list_job_types()
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return render_template(
        "scheduling_module/admin/template_edit.html",
        template=template,
        slots=slots,
        clients=clients,
        sites=sites,
        job_types=job_types,
        day_names=day_names,
        config=_core_manifest,
    )


@internal_bp.post("/templates/<int:template_id>/edit")
@_admin_required_scheduling
def admin_template_update(template_id):
    template = ScheduleService.get_template(template_id)
    if not template:
        flash("Template not found.", "error")
        return redirect(url_for("internal_scheduling.admin_templates"))
    name = (request.form.get("name") or "").strip()
    client_id = request.form.get("client_id", type=int)
    site_id = request.form.get("site_id", type=int)
    job_type_id = request.form.get("job_type_id", type=int)
    ScheduleService.update_template(template_id, name=name or None, client_id=client_id or None, site_id=site_id or None, job_type_id=job_type_id or None)
    flash("Template updated.", "success")
    return redirect(url_for("internal_scheduling.admin_template_edit", template_id=template_id))


@internal_bp.post("/templates/<int:template_id>/slots/add")
@_admin_required_scheduling
def admin_template_slot_add(template_id):
    day_of_week = request.form.get("day_of_week", type=int)
    start_time = _parse_time_internal(request.form.get("start_time"))
    end_time = _parse_time_internal(request.form.get("end_time"))
    position_label = (request.form.get("position_label") or "").strip() or None
    if day_of_week is None or start_time is None or end_time is None:
        flash("Day, start time and end time required.", "error")
        return redirect(url_for("internal_scheduling.admin_template_edit", template_id=template_id))
    if not (0 <= day_of_week <= 6):
        flash("Invalid day.", "error")
        return redirect(url_for("internal_scheduling.admin_template_edit", template_id=template_id))
    ScheduleService.add_template_slot(template_id, day_of_week, start_time, end_time, position_label)
    flash("Slot added.", "success")
    return redirect(url_for("internal_scheduling.admin_template_edit", template_id=template_id))


@internal_bp.post("/templates/<int:template_id>/slots/<int:slot_id>/delete")
@_admin_required_scheduling
def admin_template_slot_delete(template_id, slot_id):
    ScheduleService.delete_template_slot(slot_id)
    flash("Slot removed.", "success")
    return redirect(url_for("internal_scheduling.admin_template_edit", template_id=template_id))


@internal_bp.post("/templates/<int:template_id>/clone")
@_admin_required_scheduling
def admin_template_clone(template_id):
    template = ScheduleService.get_template(template_id)
    if not template:
        flash("Template not found.", "error")
        return redirect(url_for("internal_scheduling.admin_templates"))
    new_id = ScheduleService.clone_template(template_id)
    if not new_id:
        flash("Could not clone template.", "error")
        return redirect(url_for("internal_scheduling.admin_templates"))
    flash("Template cloned. Edit the copy below.", "success")
    return redirect(url_for("internal_scheduling.admin_template_edit", template_id=new_id))


@internal_bp.get("/schedule/analytics")
@_admin_required_scheduling
def admin_analytics():
    """Scheduling analytics: weekly summary, coverage heatmap."""
    _ensure_scheduling_tables()
    num_weeks = min(26, max(1, request.args.get("weeks", type=int) or 12))
    weekly = ScheduleService.get_analytics_weekly_summary(num_weeks=num_weeks)
    # Coverage: default last 14 days
    today = date.today()
    date_to = today
    date_from = today - timedelta(days=13)
    from_s = request.args.get("from")
    to_s = request.args.get("to")
    if from_s:
        try:
            date_from = date.fromisoformat(from_s)
        except ValueError:
            pass
    if to_s:
        try:
            date_to = date.fromisoformat(to_s)
        except ValueError:
            pass
    if date_to < date_from:
        date_to = date_from
    client_id = request.args.get("client_id", type=int)
    coverage = ScheduleService.get_analytics_coverage(date_from, date_to, client_id=client_id)
    this_week = ScheduleService.get_weekly_labor_totals(today - timedelta(days=today.weekday()))
    clients, _ = ScheduleService.list_clients_and_sites()
    return render_template(
        "scheduling_module/admin/analytics.html",
        weekly_summary=weekly,
        coverage=coverage,
        this_week=this_week,
        num_weeks=num_weeks,
        date_from=date_from,
        date_to=date_to,
        client_id=client_id,
        clients=clients,
        config=_core_manifest,
    )


@internal_bp.get("/schedule/shifts-search")
@_admin_required_scheduling
def admin_shifts_search():
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify([])
    return jsonify(ScheduleService.search_shifts_for_picker(q, limit=20))


@internal_bp.get("/schedule/audit")
@_admin_required_scheduling
def admin_audit_log():
    """Recent shift audit log (create/update/cancel). Optional filter by shift_id."""
    shift_id = request.args.get("shift_id", type=int)
    limit = min(200, max(1, request.args.get("limit", type=int) or 100))
    entries = ScheduleService.list_shift_audit_recent(limit=limit, shift_id=shift_id)
    shift_filter_label = None
    if shift_id:
        s = ScheduleService.get_shift(int(shift_id))
        if s:
            wd = s.get("work_date")
            day = (
                wd.isoformat()[:10]
                if hasattr(wd, "isoformat")
                else (str(wd)[:10] if wd else "—")
            )
            shift_filter_label = (
                f"#{int(shift_id)} · {day} · "
                f"{(s.get('client_name') or '').strip() or '—'} · "
                f"{(s.get('contractor_name') or '').strip() or '—'} · "
                f"{(s.get('status') or '').strip()}"
            )
        else:
            shift_filter_label = f"Shift #{int(shift_id)}"
    return render_template(
        "scheduling_module/admin/audit_log.html",
        entries=entries,
        shift_id=shift_id,
        shift_filter_label=shift_filter_label,
        limit=limit,
        config=_core_manifest,
    )


@internal_bp.get("/schedule/labor-settings")
@_admin_required_scheduling
def admin_labor_settings():
    overtime = ScheduleService.get_overtime_hours_per_week()
    budget = ScheduleService.get_weekly_budget()
    assignment_label = ScheduleService.get_assignment_label()
    return render_template(
        "scheduling_module/admin/labor_settings.html",
        overtime_hours_per_week=overtime,
        weekly_budget=budget,
        assignment_label=assignment_label,
        config=_core_manifest,
    )


@internal_bp.post("/schedule/labor-settings")
@_admin_required_scheduling
def admin_labor_settings_save():
    overtime = request.form.get("overtime_hours_per_week", type=int)
    budget_s = request.form.get("weekly_budget", "").strip()
    label_s = (request.form.get("assignment_label") or "").strip()
    if overtime is not None and 0 <= overtime <= 168:
        ScheduleService.set_overtime_hours_per_week(overtime)
    if budget_s == "":
        ScheduleService.set_weekly_budget(None)
    else:
        try:
            ScheduleService.set_weekly_budget(float(budget_s))
        except (TypeError, ValueError):
            pass
    if label_s:
        ScheduleService.set_assignment_label(label_s)
    flash("Labor settings saved.", "success")
    return redirect(url_for("internal_scheduling.admin_labor_settings"))


@internal_bp.get("/schedule/assignment-instructions")
@_admin_required_scheduling
def admin_assignment_instructions():
    """List all assignments (clients/bases) with their location/instruction notes."""
    _ensure_scheduling_tables()
    items = ScheduleService.list_assignment_instructions()
    return render_template(
        "scheduling_module/admin/assignment_instructions.html",
        items=items,
        config=_core_manifest,
    )


@internal_bp.get("/schedule/assignment-instructions/<int:client_id>/edit")
@_admin_required_scheduling
def admin_assignment_instructions_edit(client_id):
    clients, _ = ScheduleService.list_clients_and_sites()
    client = next((c for c in clients if c["id"] == client_id), None)
    if not client:
        flash("Not found.", "error")
        return redirect(url_for("internal_scheduling.admin_assignment_instructions"))
    instructions = ScheduleService.get_assignment_instructions(client_id)
    return render_template(
        "scheduling_module/admin/assignment_instructions_edit.html",
        client=client,
        instructions=instructions or "",
        config=_core_manifest,
    )


@internal_bp.post("/schedule/assignment-instructions/<int:client_id>/edit")
@_admin_required_scheduling
def admin_assignment_instructions_save(client_id):
    clients, _ = ScheduleService.list_clients_and_sites()
    if not any(c["id"] == client_id for c in clients):
        flash("Not found.", "error")
        return redirect(url_for("internal_scheduling.admin_assignment_instructions"))
    text = (request.form.get("instructions") or "").strip()
    ScheduleService.set_assignment_instructions(client_id, text or None)
    flash("Instructions saved.", "success")
    return redirect(url_for("internal_scheduling.admin_assignment_instructions"))


@internal_bp.get("/templates/<int:template_id>/apply")
@_admin_required_scheduling
def admin_template_apply_form(template_id):
    template = ScheduleService.get_template(template_id)
    if not template:
        flash("Template not found.", "error")
        return redirect(url_for("internal_scheduling.admin_templates"))
    contractors = ScheduleService.list_contractors()
    slots = ScheduleService.list_template_slots(template_id)
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    for s in slots:
        s["start_time_str"] = s.get("start_time").strftime("%H:%M") if s.get("start_time") else ""
        s["end_time_str"] = s.get("end_time").strftime("%H:%M") if s.get("end_time") else ""
        s["day_name"] = day_names[s.get("day_of_week", 0)] if s.get("day_of_week") is not None else ""
    return render_template(
        "scheduling_module/admin/template_apply.html",
        template=template,
        contractors=contractors,
        slots=slots,
        day_names=day_names,
        config=_core_manifest,
    )


@internal_bp.post("/templates/<int:template_id>/apply")
@_admin_required_scheduling
def admin_template_apply(template_id):
    week_s = request.form.get("week_monday")
    contractor_id = request.form.get("contractor_id", type=int)
    if not week_s or not contractor_id:
        flash("Week start (Monday) and default contractor required.", "error")
        return redirect(url_for("internal_scheduling.admin_template_apply_form", template_id=template_id))
    try:
        week_monday = date.fromisoformat(week_s)
        if week_monday.weekday() != 0:
            week_monday = week_monday - timedelta(days=week_monday.weekday())
    except ValueError:
        flash("Invalid date.", "error")
        return redirect(url_for("internal_scheduling.admin_template_apply_form", template_id=template_id))
    slot_assignments = {}
    for key, val in request.form.items():
        if key.startswith("slot_contractor_") and val:
            try:
                slot_id = int(key.replace("slot_contractor_", ""))
                cid = int(val)
                if cid != contractor_id:
                    slot_assignments[slot_id] = cid
            except ValueError:
                pass
    count = ScheduleService.apply_template_to_week(
        template_id, week_monday, contractor_id, slot_assignments or None
    )
    flash(f"Created {count} draft shift(s) from template.", "success")
    return redirect(url_for("internal_scheduling.admin_schedule_week", week=week_monday.isoformat()))


@internal_bp.post("/shifts/<int:shift_id>/repeat")
@_admin_required_scheduling
def admin_shift_repeat(shift_id):
    num_weeks = request.form.get("num_weeks", type=int) or request.args.get("num_weeks", type=int) or 0
    if num_weeks < 1 or num_weeks > 52:
        flash("Number of weeks must be 1–52.", "error")
        return redirect(request.referrer or url_for("internal_scheduling.admin_shifts"))
    count = ScheduleService.repeat_shift(shift_id, num_weeks)
    flash(f"Created {count} repeat shift(s) as drafts.", "success")
    return redirect(request.referrer or url_for("internal_scheduling.admin_shifts"))


@internal_bp.get("/shifts/<int:shift_id>/delete")
@_admin_required_scheduling
def admin_shift_delete(shift_id):
    """Confirm delete: show options for this / future / all (if part of series)."""
    shift = ScheduleService.get_shift(shift_id)
    if not shift:
        flash("Shift not found.", "error")
        return redirect(url_for("internal_scheduling.admin_schedule_week"))
    series = []
    future_count = 0
    if shift.get("recurrence_id"):
        series = ScheduleService.list_shifts_in_series(shift["recurrence_id"])
        wd = shift.get("work_date")
        if wd is not None and hasattr(wd, "isoformat"):
            future_count = sum(1 for s in series if (s.get("work_date") or wd) >= wd)
        else:
            future_count = len(series)
    return render_template(
        "scheduling_module/admin/shift_delete.html",
        shift=shift,
        series=series,
        future_count=future_count,
        time_display=_time_display,
        config=_core_manifest,
    )


@internal_bp.post("/shifts/<int:shift_id>/delete")
@_admin_required_scheduling
def admin_shift_delete_post(shift_id):
    scope = (request.form.get("scope") or "this").strip().lower()
    if scope not in ("this", "future", "all"):
        scope = "this"
    count = ScheduleService.delete_shift(shift_id, scope=scope)
    if count:
        flash(f"Deleted {count} shift(s).", "success")
    else:
        flash("Shift not found or already deleted.", "warning")
    return redirect(url_for("internal_scheduling.admin_schedule_week"))


@internal_bp.get("/shifts")
@_admin_required_scheduling
def admin_shifts():
    clients, sites = ScheduleService.list_clients_and_sites()
    job_types = ScheduleService.list_job_types()
    contractors = ScheduleService.list_contractors()
    return render_template(
        "scheduling_module/admin/shifts.html",
        clients=clients,
        sites=sites,
        job_types=job_types,
        contractors=contractors,
        website_settings=_get_website_settings(),
        config=_core_manifest,
    )


@internal_bp.get("/shifts/new")
@_admin_required_scheduling
def admin_shift_new():
    clients, sites = ScheduleService.list_clients_and_sites()
    job_types = ScheduleService.list_job_types()
    contractors = ScheduleService.list_contractors()
    default_date = request.args.get("date") or date.today().isoformat()
    default_contractor_id = request.args.get("contractor_id", type=int)
    default_client_id = request.args.get("client_id", type=int)
    assigned_contractor_ids = [default_contractor_id] if default_contractor_id else []
    return render_template(
        "scheduling_module/admin/shift_form.html",
        shift=None,
        clients=clients,
        sites=sites,
        job_types=job_types,
        contractors=contractors,
        assigned_contractor_ids=assigned_contractor_ids,
        default_date=default_date,
        default_contractor_id=default_contractor_id,
        default_client_id=default_client_id,
        assignment_instructions=None,
        shift_start_time="09:00",
        shift_end_time="17:00",
        config=_core_manifest,
    )


@internal_bp.post("/shifts/new")
@_admin_required_scheduling
def admin_shift_create():
    from flask import session
    labour_val = request.form.get("labour_cost")
    labour_cost = None
    if labour_val not in (None, "") and str(labour_val).strip():
        try:
            labour_cost = float(labour_val)
        except (TypeError, ValueError):
            labour_cost = None
    contractor_ids = [int(x) for x in request.form.getlist("contractor_ids") if x and str(x).strip().isdigit()]
    slab_raw = (request.form.get("shared_labour_hours") or "").strip()
    shared_labour_hours = None
    if slab_raw:
        try:
            shared_labour_hours = float(slab_raw)
        except (TypeError, ValueError):
            shared_labour_hours = None
    data = {
        "client_id": request.form.get("client_id", type=int),
        "site_id": request.form.get("site_id", type=int) or None,
        "job_type_id": request.form.get("job_type_id", type=int),
        "work_date": request.form.get("work_date"),
        "scheduled_start": request.form.get("scheduled_start"),
        "scheduled_end": request.form.get("scheduled_end"),
        "break_mins": request.form.get("break_mins", type=int) or 0,
        "notes": request.form.get("notes") or None,
        "status": request.form.get("status") or "draft",
        "labour_cost": labour_cost,
        "required_count": max(1, request.form.get("required_count", type=int) or 1),
        "contractor_ids": contractor_ids,
    }
    if shared_labour_hours is not None and shared_labour_hours > 0:
        data["shared_labour_hours"] = shared_labour_hours
    # Require client, job type, date, and times
    if not data["client_id"] or not data["job_type_id"] or not data["work_date"] or not data["scheduled_start"] or not data["scheduled_end"]:
        flash("Please fill in client, job type, date, and start/end times.", "error")
        return redirect(url_for("internal_scheduling.admin_shift_new"))
    try:
        data["work_date"] = date.fromisoformat(data["work_date"])
    except (TypeError, ValueError):
        flash("Invalid date.", "error")
        return redirect(url_for("internal_scheduling.admin_shift_new"))
    try:
        from datetime import timedelta
        uid = getattr(current_user, "id", None)
        uname = getattr(current_user, "username", None) or getattr(current_user, "email", None)
        shift_id = ScheduleService.create_shift(data, actor_user_id=uid, actor_username=uname)
        flash("Shift created.", "success")
        w = data["work_date"]
        monday = w - timedelta(days=w.weekday())
        return redirect(url_for("internal_scheduling.admin_schedule_week") + "?week=" + monday.strftime("%Y-%m-%d"))
    except Exception as e:
        flash(str(e), "error")
        return redirect(url_for("internal_scheduling.admin_shift_new"))


def _scheduling_rota_eligibility_context():
    """Allow run sheet crew assignment outside role ladder when admin creates team events (ROTA-ROLE-001)."""
    uid = None
    try:
        if getattr(current_user, "is_authenticated", False):
            uid = getattr(current_user, "id", None)
    except Exception:
        pass
    return {"allow_admin_override": True, "staff_user_id": uid}


@internal_bp.get("/team-event")
@_admin_required_scheduling
def admin_team_event_new():
    _ensure_scheduling_tables()
    clients, sites = ScheduleService.list_clients_and_sites()
    job_types = ScheduleService.list_job_types()
    contractors = ScheduleService.list_contractors()
    default_date = request.args.get("date") or date.today().isoformat()
    return render_template(
        "scheduling_module/admin/team_event_form.html",
        clients=clients,
        sites=sites,
        job_types=job_types,
        contractors=contractors,
        default_date=default_date,
        default_start="09:00",
        default_end="17:00",
        runsheets_admin_url=_safe_url_for("internal_time_billing.runsheets_edit_page"),
        config=_core_manifest,
    )


@internal_bp.post("/team-event")
@_admin_required_scheduling
def admin_team_event_create():
    _ensure_scheduling_tables()
    client_id = request.form.get("client_id", type=int)
    site_id = request.form.get("site_id", type=int) or None
    job_type_id = request.form.get("job_type_id", type=int)
    work_date_s = (request.form.get("work_date") or "").strip()
    scheduled_start = (request.form.get("scheduled_start") or "").strip() or None
    scheduled_end = (request.form.get("scheduled_end") or "").strip() or None
    notes = (request.form.get("notes") or "").strip() or None
    contractor_ids = [
        int(x) for x in request.form.getlist("contractor_ids") if x and str(x).strip().isdigit()
    ]
    if not contractor_ids:
        flash("Select at least one staff member for the team event.", "error")
        return redirect(url_for("internal_scheduling.admin_team_event_new"))
    shift_status = (request.form.get("shift_status") or "published").strip().lower()
    if shift_status not in ("draft", "published"):
        shift_status = "published"
    publish_runsheet = (request.form.get("publish_runsheet") or "") == "1"
    cura_eid_raw = (request.form.get("cura_operational_event_id") or "").strip()
    cura_operational_event_id = int(cura_eid_raw) if cura_eid_raw.isdigit() else None
    lead_user_id = request.form.get("lead_user_id", type=int) or None

    if (
        not client_id
        or not job_type_id
        or not work_date_s
        or not scheduled_start
        or not scheduled_end
    ):
        flash("Please fill in client, job type, date, start and end times.", "error")
        return redirect(url_for("internal_scheduling.admin_team_event_new"))
    try:
        work_date = date.fromisoformat(work_date_s)
    except (TypeError, ValueError):
        flash("Invalid date.", "error")
        return redirect(url_for("internal_scheduling.admin_team_event_new"))

    uid = getattr(current_user, "id", None)
    uname = getattr(current_user, "username", None) or getattr(current_user, "email", None)

    try:
        out = ScheduleService.create_team_event_with_runsheet(
            client_id=client_id,
            site_id=site_id,
            job_type_id=job_type_id,
            work_date=work_date,
            scheduled_start=scheduled_start,
            scheduled_end=scheduled_end,
            contractor_ids=contractor_ids,
            notes=notes,
            shift_status=shift_status,
            publish_runsheet=publish_runsheet,
            cura_operational_event_id=cura_operational_event_id,
            lead_user_id=lead_user_id,
            actor_user_id=uid,
            actor_username=uname,
            eligibility_context=_scheduling_rota_eligibility_context(),
        )
    except Exception as e:
        flash(str(e), "error")
        return redirect(url_for("internal_scheduling.admin_team_event_new"))

    n = len(out.get("shift_ids") or [])
    rs_id = out.get("runsheet_id")
    if publish_runsheet:
        flash(
            f"Team event: published run sheet #{rs_id} with {n} linked rota shifts. "
            "Staff can enter actual start/end on the run sheet in the employee portal; timesheets follow those actuals.",
            "success",
        )
    else:
        flash(
            f"Team event: {n} rota shifts created, linked to draft run sheet #{rs_id}. "
            "Scheduling is the plan; publish the run sheet in Time Billing when you want run-sheet timesheet rows—or use standard timesheets from scheduled shifts.",
            "success",
        )
    monday = work_date - timedelta(days=work_date.weekday())
    return redirect(
        url_for("internal_scheduling.admin_schedule_week") + "?week=" + monday.strftime("%Y-%m-%d")
    )


@internal_bp.get("/shifts/<int:shift_id>/edit")
@_admin_required_scheduling
def admin_shift_edit(shift_id):
    shift = ScheduleService.get_shift(shift_id)
    if not shift:
        flash("Shift not found.", "error")
        return redirect(url_for("internal_scheduling.admin_shifts"))
    clients, sites = ScheduleService.list_clients_and_sites()
    job_types = ScheduleService.list_job_types()
    contractors = ScheduleService.list_contractors()
    shift_audit = ScheduleService.list_shift_audit(shift_id, limit=20)
    shift_tasks = ScheduleService.list_shift_tasks(shift_id)
    assignment_instructions = ScheduleService.get_assignment_instructions(shift["client_id"]) if shift.get("client_id") else None
    shift_start_time = _time_display(shift.get("scheduled_start")) or "09:00"
    shift_end_time = _time_display(shift.get("scheduled_end")) or "17:00"
    assigned_contractor_ids = [a["contractor_id"] for a in (shift.get("assignments") or []) if a.get("contractor_id") is not None]
    if not assigned_contractor_ids and shift.get("contractor_id") is not None:
        assigned_contractor_ids = [shift["contractor_id"]]
    return render_template(
        "scheduling_module/admin/shift_form.html",
        shift=shift,
        clients=clients,
        sites=sites,
        job_types=job_types,
        contractors=contractors,
        assigned_contractor_ids=assigned_contractor_ids,
        default_date=shift.get("work_date"),
        default_client_id=None,
        assignment_instructions=assignment_instructions,
        shift_start_time=shift_start_time,
        shift_end_time=shift_end_time,
        shift_audit=shift_audit,
        shift_tasks=shift_tasks,
        config=_core_manifest,
    )


@internal_bp.post("/shifts/<int:shift_id>/edit")
@_admin_required_scheduling
def admin_shift_update(shift_id):
    shift = ScheduleService.get_shift(shift_id)
    if not shift:
        flash("Shift not found.", "error")
        return redirect(url_for("internal_scheduling.admin_shifts"))
    data = {}
    for key in (
        "client_id",
        "site_id",
        "job_type_id",
        "work_date",
        "scheduled_start",
        "scheduled_end",
        "break_mins",
        "notes",
        "status",
        "labour_cost",
        "required_count",
        "shared_labour_hours",
    ):
        val = request.form.get(key)
        if val is not None:
            if key == "break_mins":
                data[key] = int(val) if val != "" else 0
            elif key == "required_count":
                data[key] = max(1, int(val) if val != "" else 1)
            elif key == "work_date":
                try:
                    data[key] = date.fromisoformat(val) if val else None
                except (TypeError, ValueError):
                    pass
            elif key == "labour_cost":
                try:
                    data[key] = float(val) if val not in (None, "") else None
                except (TypeError, ValueError):
                    data[key] = None
            elif key == "shared_labour_hours":
                try:
                    data[key] = float(val) if val not in (None, "") and str(val).strip() else None
                except (TypeError, ValueError):
                    data[key] = None
            else:
                data[key] = val
    # site_id "Any" sends ""; DB expects NULL not empty string
    if "site_id" in data and (data["site_id"] == "" or data["site_id"] is None):
        data["site_id"] = None
    elif "site_id" in data and data["site_id"] != "":
        try:
            data["site_id"] = int(data["site_id"])
        except (TypeError, ValueError):
            data["site_id"] = None
    contractor_ids = [int(x) for x in request.form.getlist("contractor_ids") if x and str(x).strip().isdigit()]
    uid = getattr(current_user, "id", None)
    uname = getattr(current_user, "username", None) or getattr(current_user, "email", None)
    if data:
        ScheduleService.update_shift(shift_id, data, actor_user_id=uid, actor_username=uname)
    ScheduleService.set_shift_assignments(shift_id, contractor_ids)
    if data and data.get("status") == "published":
        try:
            shift_after = ScheduleService.get_shift(shift_id)
            assignee_ids = [a["contractor_id"] for a in (shift_after.get("assignments") or []) if a.get("contractor_id")]
            if not assignee_ids and shift_after.get("contractor_id"):
                assignee_ids = [shift_after["contractor_id"]]
            if assignee_ids:
                from app.plugins.employee_portal_module.services import get_todo_by_reference, admin_create_todo
                link_url = url_for("public_scheduling.shift_view", shift_id=shift_id, _external=False) + "#tasks"
                for task in ScheduleService.list_shift_tasks(shift_id):
                    if get_todo_by_reference("scheduling_module", "schedule_shift_task", str(task["id"])):
                        continue
                    admin_create_todo(
                        assignee_ids,
                        "Shift task: " + ((task.get("title") or "").strip()[:200] or "Task"),
                        link_url=link_url,
                        source_module="scheduling_module",
                        reference_type="schedule_shift_task",
                        reference_id=str(task["id"]),
                    )
        except Exception:
            pass
    flash("Shift updated.", "success")
    return redirect(request.referrer or url_for("internal_scheduling.admin_schedule_week"))


@internal_bp.post("/shifts/<int:shift_id>/tasks/add")
@_admin_required_scheduling
def admin_shift_task_add(shift_id):
    shift = ScheduleService.get_shift(shift_id)
    if not shift:
        flash("Shift not found.", "error")
        return redirect(url_for("internal_scheduling.admin_shifts"))
    title = (request.form.get("task_title") or "").strip()
    if not title:
        flash("Task title required.", "error")
        return redirect(url_for("internal_scheduling.admin_shift_edit", shift_id=shift_id))
    task_id = ScheduleService.add_shift_task(shift_id, title)
    assignee_ids = [a["contractor_id"] for a in (shift.get("assignments") or []) if a.get("contractor_id")]
    if not assignee_ids and shift.get("contractor_id"):
        assignee_ids = [shift["contractor_id"]]
    if task_id and (shift.get("status") or "").lower() == "published" and assignee_ids:
        try:
            from app.plugins.employee_portal_module.services import get_todo_by_reference, admin_create_todo
            if not get_todo_by_reference("scheduling_module", "schedule_shift_task", str(task_id)):
                link_url = url_for("public_scheduling.shift_view", shift_id=shift_id, _external=False) + "#tasks"
                admin_create_todo(
                    assignee_ids,
                    "Shift task: " + (title or "").strip()[:200],
                    link_url=link_url,
                    source_module="scheduling_module",
                    reference_type="schedule_shift_task",
                    reference_id=str(task_id),
                )
        except Exception:
            pass
    flash("Task added.", "success")
    return redirect(url_for("internal_scheduling.admin_shift_edit", shift_id=shift_id))


@internal_bp.post("/shifts/<int:shift_id>/tasks/<int:task_id>/delete")
@_admin_required_scheduling
def admin_shift_task_delete(shift_id, task_id):
    ScheduleService.delete_shift_task(task_id)
    flash("Task removed.", "success")
    return redirect(url_for("internal_scheduling.admin_shift_edit", shift_id=shift_id))


@internal_bp.get("/schedule/announcements")
@_admin_required_scheduling
def admin_announcements():
    contractors = ScheduleService.list_contractors()
    today = date.today()
    week_monday = today - timedelta(days=today.weekday())
    staff_this_week = ScheduleService.get_contractor_ids_with_shifts_in_week(week_monday)
    return render_template(
        "scheduling_module/admin/announcements.html",
        contractors=contractors,
        staff_this_week_count=len(staff_this_week),
        config=_core_manifest,
    )


@internal_bp.post("/schedule/announcements/send")
@_admin_required_scheduling
def admin_announcements_send():
    send_to = request.form.get("send_to")
    subject = (request.form.get("subject") or "").strip()
    body = (request.form.get("body") or "").strip()
    if not subject:
        flash("Subject is required.", "error")
        return redirect(url_for("internal_scheduling.admin_announcements"))
    contractor_ids = []
    if send_to == "all":
        contractor_ids = [c["id"] for c in ScheduleService.list_contractors()]
    elif send_to == "this_week":
        today = date.today()
        week_monday = today - timedelta(days=today.weekday())
        contractor_ids = ScheduleService.get_contractor_ids_with_shifts_in_week(week_monday)
    else:
        for part in request.form.getlist("contractor_ids") or (request.form.get("contractor_ids") or "").split(","):
            part = str(part).strip()
            if part.isdigit():
                contractor_ids.append(int(part))
    if not contractor_ids:
        flash("No recipients. Select All staff, Staff with shifts this week, or specific staff.", "error")
        return redirect(url_for("internal_scheduling.admin_announcements"))
    try:
        from app.plugins.employee_portal_module.services import admin_send_message
        uid = getattr(current_user, "id", None)
        count = admin_send_message(
            contractor_ids, subject, body=body or None,
            source_module="scheduling_module", sent_by_user_id=uid,
        )
        flash(f"Message sent to {count} recipient(s). They will see it in the Employee Portal.", "success")
    except Exception as e:
        flash(f"Could not send: {e}", "error")
    return redirect(url_for("internal_scheduling.admin_announcements"))


@internal_bp.get("/timesheets/export")
@_admin_required_scheduling
def admin_timesheet_export():
    """Export timesheets as CSV (date range, optional contractor). With from/to params returns CSV; else shows form."""
    from io import BytesIO
    import csv
    date_from_s = request.args.get("from") or ""
    date_to_s = request.args.get("to") or ""
    contractor_id = request.args.get("contractor_id", type=int)
    try:
        date_from = date.fromisoformat(date_from_s) if date_from_s else (date.today() - timedelta(days=13))
        date_to = date.fromisoformat(date_to_s) if date_to_s else date.today()
    except ValueError:
        date_from = date.today() - timedelta(days=13)
        date_to = date.today()
    if date_to < date_from:
        date_to = date_from
    if not date_from_s and not date_to_s:
        contractors = ScheduleService.list_contractors()
        return render_template(
            "scheduling_module/admin/timesheet_export.html",
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            contractor_id=contractor_id,
            contractors=contractors,
            config=_core_manifest,
        )
    shifts = ScheduleService.list_timesheet_shifts(date_from, date_to, contractor_id=contractor_id or None)
    buf = BytesIO()
    writer = csv.writer(buf)
    writer.writerow([
        "Date", "Contractor", "Client", "Site", "Job type",
        "Scheduled start", "Scheduled end", "Actual start", "Actual end",
        "Break (min)", "Hours", "Status", "Notes",
    ])
    for s in shifts:
        start = s.get("scheduled_start")
        end = s.get("scheduled_end")
        astart = s.get("actual_start")
        aend = s.get("actual_end")
        break_mins = int(s.get("break_mins") or 0)
        if astart and aend and hasattr(astart, "hour") and hasattr(aend, "hour"):
            mins = (aend.hour * 60 + aend.minute) - (astart.hour * 60 + astart.minute) - break_mins
            hours = round(max(0, mins) / 60.0, 2)
        else:
            hours = ""
        wd = s.get("work_date")
        writer.writerow([
            wd.isoformat() if wd and hasattr(wd, "isoformat") else "",
            s.get("contractor_name") or s.get("contractor_id") or "",
            s.get("client_name") or "",
            s.get("site_name") or "",
            s.get("job_type_name") or "",
            start.strftime("%H:%M") if start else "",
            end.strftime("%H:%M") if end else "",
            astart.strftime("%H:%M") if astart else "",
            aend.strftime("%H:%M") if aend else "",
            break_mins,
            hours,
            s.get("status") or "",
            (s.get("notes") or "")[:200],
        ])
    buf.seek(0)
    return send_file(
        buf,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"timesheets_{date_from.isoformat()}_{date_to.isoformat()}.csv",
    )


@internal_bp.get("/schedule/clients/add")
@_admin_required_scheduling
def admin_add_client_form():
    """Show form to add a client; after save, redirect to new shift with this client selected."""
    return render_template(
        "scheduling_module/admin/add_client.html",
        config=_core_manifest,
    )


@internal_bp.post("/schedule/clients/add")
@_admin_required_scheduling
def admin_add_client():
    """Create a client and redirect to new shift with that client selected."""
    name = (request.form.get("client_name") or "").strip()
    if not name:
        flash("Client name is required.", "error")
        return redirect(url_for("internal_scheduling.admin_add_client_form"))
    client_id = ScheduleService.create_client(name)
    if not client_id:
        flash("Could not add client.", "error")
        return redirect(url_for("internal_scheduling.admin_add_client_form"))
    instructions = (request.form.get("instructions") or "").strip()
    if instructions:
        ScheduleService.set_assignment_instructions(client_id, instructions)
    flash(f"Client “{name}” added. You can now create a shift for them.", "success")
    kwargs = {"client_id": client_id}
    if request.form.get("date"):
        kwargs["date"] = request.form.get("date", "")
    return redirect(url_for("internal_scheduling.admin_shift_new", **kwargs))


@internal_bp.get("/schedule/clock-locations")
@_admin_required_scheduling
def admin_clock_locations():
    """Geofence: set lat/lng/radius per site for clock-in validation."""
    locations = ScheduleService.list_clock_locations()
    clients, sites = ScheduleService.list_clients_and_sites()
    return render_template(
        "scheduling_module/admin/clock_locations.html",
        locations=locations,
        sites=sites,
        clients=clients,
        config=_core_manifest,
    )


@internal_bp.post("/schedule/clock-locations/add-site")
@_admin_required_scheduling
def admin_clock_locations_add_site():
    """Create a site so it can be used for a clock location (when none exist)."""
    name = (request.form.get("site_name") or "").strip()
    client_id = request.form.get("client_id", type=int)
    if not name or not client_id:
        flash("Site name and client are required.", "error")
        return redirect(url_for("internal_scheduling.admin_clock_locations"))
    site_id = ScheduleService.create_site(client_id, name)
    if site_id:
        flash(f"Site “{name}” added. You can now set its clock location below.", "success")
    else:
        flash("Could not add site. Check that clients exist.", "error")
    return redirect(url_for("internal_scheduling.admin_clock_locations"))


@internal_bp.post("/schedule/clock-locations")
@_admin_required_scheduling
def admin_clock_locations_save():
    site_id = request.form.get("site_id", type=int)
    lat_s = request.form.get("latitude", "").strip()
    lng_s = request.form.get("longitude", "").strip()
    radius = request.form.get("radius_meters", type=int) or 100
    if not site_id or not lat_s or not lng_s:
        flash("Site, latitude and longitude required.", "error")
        return redirect(url_for("internal_scheduling.admin_clock_locations"))
    try:
        lat, lng = float(lat_s), float(lng_s)
    except ValueError:
        flash("Invalid latitude or longitude.", "error")
        return redirect(url_for("internal_scheduling.admin_clock_locations"))
    ScheduleService.set_clock_location(site_id, lat, lng, radius_meters=radius)
    flash("Clock location saved. Staff must be within radius to clock in at this site.", "success")
    return redirect(url_for("internal_scheduling.admin_clock_locations"))


@internal_bp.get("/time-off")
@_admin_required_scheduling
def admin_time_off():
    contractor_id = request.args.get("contractor_id", type=int)
    status = request.args.get("status") or ""
    type_filter = request.args.get("type") or ""
    date_from_s = request.args.get("date_from") or ""
    date_to_s = request.args.get("date_to") or ""
    date_from = date.fromisoformat(date_from_s) if date_from_s else None
    date_to = date.fromisoformat(date_to_s) if date_to_s else None
    time_off_list = ScheduleService.list_time_off(
        contractor_id=contractor_id,
        date_from=date_from,
        date_to=date_to,
        status=status or None,
        type_filter=type_filter or None,
    )
    for r in time_off_list:
        r["start_time_display"] = _time_display(r.get("start_time")) if r.get("start_time") else None
        r["end_time_display"] = _time_display(r.get("end_time")) if r.get("end_time") else None
    contractors = ScheduleService.list_contractors()
    return render_template(
        "scheduling_module/admin/time_off.html",
        time_off_list=time_off_list,
        contractors=contractors,
        contractor_id=contractor_id,
        status=status,
        type_filter=type_filter,
        date_from=date_from_s,
        date_to=date_to_s,
        config=_core_manifest,
    )


def _notify_contractor(contractor_id: int, subject: str, body: str = ""):
    """Send a portal message to the contractor (if employee_portal is available)."""
    try:
        from app.plugins.employee_portal_module.services import admin_send_message
        from flask_login import current_user
        admin_send_message(
            [contractor_id],
            subject[:255],
            body[:65535] if body else "",
            sent_by_user_id=getattr(current_user, "id", None),
            source_module="scheduling_module",
        )
    except Exception:
        pass


@internal_bp.post("/time-off/<int:tid>/approve")
@_admin_required_scheduling
def admin_time_off_approve(tid):
    from flask_login import current_user
    admin_notes = (request.form.get("admin_notes") or "").strip() or None
    user_id = getattr(current_user, "id", None)
    to = ScheduleService.get_time_off(tid)
    contractor_id = to.get("contractor_id") if to else None
    if ScheduleService.approve_time_off(tid, reviewed_by_user_id=user_id, admin_notes=admin_notes):
        if contractor_id:
            _notify_contractor(contractor_id, "Time off approved", "Your time off request has been approved.")
        flash("Time off approved.", "success")
    else:
        flash("Could not approve (maybe already processed).", "warning")
    return redirect(request.referrer or url_for("internal_scheduling.admin_time_off"))


@internal_bp.post("/time-off/<int:tid>/reject")
@_admin_required_scheduling
def admin_time_off_reject(tid):
    from flask_login import current_user
    admin_notes = (request.form.get("admin_notes") or "").strip() or None
    user_id = getattr(current_user, "id", None)
    to = ScheduleService.get_time_off(tid)
    contractor_id = to.get("contractor_id") if to else None
    if ScheduleService.reject_time_off(tid, reviewed_by_user_id=user_id, admin_notes=admin_notes):
        if contractor_id:
            _notify_contractor(contractor_id, "Time off request declined", admin_notes or "Your time off request was not approved.")
        flash("Time off rejected.", "success")
    else:
        flash("Could not reject (maybe already processed).", "warning")
    return redirect(request.referrer or url_for("internal_scheduling.admin_time_off"))


@internal_bp.get("/time-off/new")
@_admin_required_scheduling
def admin_time_off_new():
    contractors = ScheduleService.list_contractors()
    return render_template(
        "scheduling_module/admin/time_off_new.html",
        contractors=contractors,
        config=_core_manifest,
    )


@internal_bp.post("/time-off/new")
@_admin_required_scheduling
def admin_time_off_create():
    contractor_id = request.form.get("contractor_id", type=int)
    start_s = request.form.get("start_date")
    end_s = request.form.get("end_date")
    type_val = request.form.get("type") or "annual"
    reason = (request.form.get("reason") or "").strip() or None
    if not contractor_id or not start_s:
        flash("Contractor and start date required.", "error")
        return redirect(url_for("internal_scheduling.admin_time_off_new"))
    end_s = end_s or start_s
    whole_day = request.form.get("whole_day", "1") == "1"
    start_time = _parse_time(request.form.get("start_time")) if not whole_day else None
    end_time = _parse_time(request.form.get("end_time")) if not whole_day else None
    try:
        start_date = date.fromisoformat(start_s)
        end_date = date.fromisoformat(end_s)
        if end_date < start_date:
            end_date = start_date
        if whole_day or start_time is None or end_time is None:
            ScheduleService.create_time_off_on_behalf(contractor_id, start_date, end_date, type=type_val, reason=reason, status="approved")
        else:
            ScheduleService.create_time_off_on_behalf(contractor_id, start_date, end_date, type=type_val, reason=reason, status="approved", start_time=start_time, end_time=end_time)
        flash("Time off added.", "success")
    except ValueError:
        flash("Invalid date.", "error")
    return redirect(url_for("internal_scheduling.admin_time_off"))


@internal_bp.get("/swap")
@_admin_required_scheduling
def admin_swaps():
    status = request.args.get("status")
    swaps = ScheduleService.list_swap_requests(status=status)
    return render_template(
        "scheduling_module/admin/swap.html",
        swaps=swaps,
        status=status,
        config=_core_manifest,
    )


@internal_bp.post("/swap/<int:swap_id>/approve")
@_admin_required_scheduling
def admin_swap_approve(swap_id):
    if ScheduleService.approve_swap(swap_id):
        flash("Swap approved. Shift reassigned.", "success")
    else:
        flash("Could not approve.", "warning")
    return redirect(url_for("internal_scheduling.admin_swaps"))


@internal_bp.post("/swap/<int:swap_id>/reject")
@_admin_required_scheduling
def admin_swap_reject(swap_id):
    if ScheduleService.reject_swap(swap_id):
        flash("Swap rejected.", "success")
    return redirect(url_for("internal_scheduling.admin_swaps"))


@internal_bp.get("/api/shifts")
@_admin_required_scheduling
def api_list_shifts():
    contractor_id = request.args.get("contractor_id", type=int)
    client_id = request.args.get("client_id", type=int)
    work_date = request.args.get("date")
    date_from = request.args.get("from")
    date_to = request.args.get("to")
    status = request.args.get("status")
    kwargs = {}
    if contractor_id is not None:
        kwargs["contractor_id"] = contractor_id
    if client_id is not None:
        kwargs["client_id"] = client_id
    if work_date:
        try:
            kwargs["work_date"] = date.fromisoformat(work_date)
        except ValueError:
            pass
    if date_from:
        try:
            kwargs["date_from"] = date.fromisoformat(date_from)
        except ValueError:
            pass
    if date_to:
        try:
            kwargs["date_to"] = date.fromisoformat(date_to)
        except ValueError:
            pass
    if status:
        kwargs["status"] = status
    shifts = ScheduleService.list_shifts(**kwargs)
    return jsonify({"shifts": [_shift_for_json(s) for s in shifts]})


@internal_bp.post("/api/shifts")
@_admin_required_scheduling
def api_create_shift():
    data = request.get_json() or {}
    required = ["client_id", "job_type_id", "work_date", "scheduled_start", "scheduled_end"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        return jsonify({"error": f"Missing: {', '.join(missing)}"}), 400
    if not data.get("contractor_ids") and data.get("contractor_id") is None:
        data["contractor_id"] = None
    elif data.get("contractor_ids") is None and data.get("contractor_id") is not None:
        data["contractor_ids"] = [data["contractor_id"]]
    try:
        uid = getattr(current_user, "id", None)
        uname = getattr(current_user, "username", None) or getattr(current_user, "email", None)
        shift_id = ScheduleService.create_shift(data, actor_user_id=uid, actor_username=uname)
        return jsonify({"id": shift_id}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.get("/api/shifts/<int:shift_id>")
@_admin_required_scheduling
def api_get_shift_admin(shift_id):
    shift = ScheduleService.get_shift(shift_id)
    if not shift:
        return jsonify({"error": "Not found"}), 404
    return jsonify(_shift_for_json(shift))


@internal_bp.put("/api/shifts/<int:shift_id>")
@_admin_required_scheduling
def api_update_shift(shift_id):
    shift = ScheduleService.get_shift(shift_id)
    if not shift:
        return jsonify({"error": "Not found"}), 404
    data = request.get_json() or {}
    contractor_id = data.get("contractor_id")
    if "contractor_id" in data:
        ScheduleService.set_shift_assignments(shift_id, [contractor_id] if contractor_id else [])
    try:
        uid = getattr(current_user, "id", None)
        uname = getattr(current_user, "username", None) or getattr(current_user, "email", None)
        ScheduleService.update_shift(shift_id, data, actor_user_id=uid, actor_username=uname)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.get("/api/time-off")
@_admin_required_scheduling
def api_list_time_off():
    contractor_id = request.args.get("contractor_id", type=int)
    date_from_s = request.args.get("from")
    date_to_s = request.args.get("to")
    status = request.args.get("status")
    type_filter = request.args.get("type")
    date_from = date.fromisoformat(date_from_s) if date_from_s else None
    date_to = date.fromisoformat(date_to_s) if date_to_s else None
    rows = ScheduleService.list_time_off(
        contractor_id=contractor_id,
        date_from=date_from,
        date_to=date_to,
        status=status or None,
        type_filter=type_filter or None,
    )
    for r in rows:
        for k in ("start_date", "end_date"):
            if r.get(k) and hasattr(r[k], "isoformat"):
                r[k] = r[k].isoformat()
    return jsonify({"time_off": rows})


@internal_bp.patch("/api/time-off/<int:tid>")
@_admin_required_scheduling
def api_time_off_action(tid):
    from flask_login import current_user
    data = request.get_json() or {}
    action = data.get("action")
    admin_notes = data.get("admin_notes")
    user_id = getattr(current_user, "id", None)
    if action == "approve":
        if ScheduleService.approve_time_off(tid, reviewed_by_user_id=user_id, admin_notes=admin_notes):
            return jsonify({"ok": True})
    elif action == "reject":
        if ScheduleService.reject_time_off(tid, reviewed_by_user_id=user_id, admin_notes=admin_notes):
            return jsonify({"ok": True})
    return jsonify({"error": "Not found or invalid action"}), 400


@internal_bp.get("/api/contractors")
@_admin_required_scheduling
def api_list_contractors():
    contractors = ScheduleService.list_contractors()
    return jsonify({"contractors": contractors})


@internal_bp.get("/api/clients")
@_admin_required_scheduling
def api_list_clients():
    clients, _ = ScheduleService.list_clients_and_sites()
    return jsonify({"clients": clients})


@internal_bp.get("/api/sites")
@_admin_required_scheduling
def api_list_sites():
    _, sites = ScheduleService.list_clients_and_sites()
    return jsonify({"sites": sites})


@internal_bp.get("/api/assignment-instructions/<int:client_id>")
@_admin_required_scheduling
def api_get_assignment_instructions(client_id):
    instructions = ScheduleService.get_assignment_instructions(client_id)
    return jsonify({"instructions": instructions or ""})


@internal_bp.get("/api/job-types")
@_admin_required_scheduling
def api_list_job_types():
    job_types = ScheduleService.list_job_types()
    return jsonify({"job_types": job_types})


@internal_bp.get("/api/suggest-contractors")
@_admin_required_scheduling
def api_suggest_contractors():
    work_date_s = request.args.get("date")
    start_time_s = request.args.get("start")
    end_time_s = request.args.get("end")
    client_id = request.args.get("client_id", type=int)
    job_type_id = request.args.get("job_type_id", type=int)
    if not work_date_s or not start_time_s or not end_time_s:
        return jsonify({"error": "date, start, end required (e.g. date=2025-03-10&start=09:00&end=17:00)"}), 400
    try:
        work_date = date.fromisoformat(work_date_s)
    except ValueError:
        return jsonify({"error": "Invalid date"}), 400
    start_time = _parse_time(start_time_s)
    end_time = _parse_time(end_time_s)
    if start_time is None or end_time is None:
        return jsonify({"error": "Invalid start or end time (use HH:MM)"}), 400
    contractors = ScheduleService.suggest_available_contractors(
        work_date=work_date,
        start_time=start_time,
        end_time=end_time,
        client_id=client_id,
        job_type_id=job_type_id,
    )
    return jsonify({"contractors": contractors})


@internal_bp.get("/api/check-conflicts")
@_admin_required_scheduling
def api_check_conflicts():
    contractor_id = request.args.get("contractor_id", type=int)
    work_date_s = request.args.get("date")
    start_time_s = request.args.get("start")
    end_time_s = request.args.get("end")
    exclude_shift_id = request.args.get("exclude_shift_id", type=int)
    if not contractor_id or not work_date_s or not start_time_s or not end_time_s:
        return jsonify({"error": "contractor_id, date, start, end required"}), 400
    try:
        work_date = date.fromisoformat(work_date_s)
    except ValueError:
        return jsonify({"error": "Invalid date"}), 400
    start_time = _parse_time(start_time_s)
    end_time = _parse_time(end_time_s)
    if start_time is None or end_time is None:
        return jsonify({"error": "Invalid start or end time (use HH:MM)"}), 400
    conflicts = ScheduleService.check_shift_conflicts(
        contractor_id=contractor_id,
        work_date=work_date,
        scheduled_start=start_time,
        scheduled_end=end_time,
        exclude_shift_id=exclude_shift_id,
    )
    overlap_shifts = ScheduleService.get_contractor_shift_overlap_rows(
        contractor_id=contractor_id,
        work_date=work_date,
        scheduled_start=start_time,
        scheduled_end=end_time,
        exclude_shift_id=exclude_shift_id,
    )
    return jsonify({"conflicts": conflicts, "overlap_shifts": overlap_shifts})


@internal_bp.get("/api/contractors-slot-status")
@_admin_required_scheduling
def api_contractors_slot_status():
    """Who is free vs already booked vs time off for a proposed slot (week view / shift form)."""
    work_date_s = request.args.get("date")
    start_time_s = request.args.get("start")
    end_time_s = request.args.get("end")
    exclude_shift_id = request.args.get("exclude_shift_id", type=int)
    if not work_date_s or not start_time_s or not end_time_s:
        return jsonify({"error": "date, start, end required"}), 400
    try:
        work_date = date.fromisoformat(work_date_s)
    except ValueError:
        return jsonify({"error": "Invalid date"}), 400
    start_time = _parse_time(start_time_s)
    end_time = _parse_time(end_time_s)
    if start_time is None or end_time is None:
        return jsonify({"error": "Invalid start or end time (use HH:MM)"}), 400
    rows = ScheduleService.list_contractors_slot_status(
        work_date=work_date,
        scheduled_start=start_time,
        scheduled_end=end_time,
        exclude_shift_id=exclude_shift_id,
    )
    return jsonify({"contractors": rows})


@internal_bp.post("/api/unassign-slot-overlap")
@_admin_required_scheduling
def api_unassign_slot_overlap():
    """
    Remove the contractor from existing shifts that overlap this slot (scheduler override).
    JSON: contractor_id, date, start, end, exclude_shift_id (optional).
    """
    data = request.get_json(silent=True) or {}
    contractor_id = data.get("contractor_id")
    work_date_s = (data.get("date") or "").strip()
    start_time_s = (data.get("start") or "").strip()
    end_time_s = (data.get("end") or "").strip()
    exclude_shift_id = data.get("exclude_shift_id")
    if exclude_shift_id is not None and str(exclude_shift_id).strip().isdigit():
        exclude_shift_id = int(exclude_shift_id)
    else:
        exclude_shift_id = None
    try:
        contractor_id = int(contractor_id)
    except (TypeError, ValueError):
        return jsonify({"error": "contractor_id required"}), 400
    if not work_date_s or not start_time_s or not end_time_s:
        return jsonify({"error": "date, start, end required"}), 400
    try:
        work_date = date.fromisoformat(work_date_s)
    except ValueError:
        return jsonify({"error": "Invalid date"}), 400
    start_time = _parse_time(start_time_s)
    end_time = _parse_time(end_time_s)
    if start_time is None or end_time is None:
        return jsonify({"error": "Invalid start or end time"}), 400
    uid = getattr(current_user, "id", None)
    uname = getattr(current_user, "username", None) or getattr(current_user, "email", None)
    cleared = ScheduleService.unassign_contractor_from_overlapping_shifts(
        contractor_id,
        work_date,
        start_time,
        end_time,
        exclude_shift_id=exclude_shift_id,
        actor_user_id=uid,
        actor_username=uname,
    )
    return jsonify({"ok": True, "cleared_shift_ids": cleared})


# ---------- Sling sync (admin) ----------

@internal_bp.get("/sling-sync")
@_admin_required_scheduling
def admin_sling_sync():
    _ensure_scheduling_tables()

    today = date.today()
    # Default: next Mon-Sun week (Sling sync is easier in weekly chunks).
    date_from = today
    while date_from.weekday() != 0:  # 0=Mon
        date_from = date_from + timedelta(days=1)
    date_to = date_from + timedelta(days=6)

    job_types = ScheduleService.list_job_types()
    clients, sites = ScheduleService.list_clients_and_sites()

    settings = SlingSyncService._get_sling_settings()
    creds = None
    try:
        creds = SlingSyncService.load_credentials()
    except Exception:
        creds = None
    credentials_saved = bool(creds)
    recent_runs = SlingSyncService.list_recent_sync_runs(20)

    return render_template(
        "scheduling_module/admin/sling_sync.html",
        job_types=job_types,
        clients=clients,
        sites=sites,
        default_job_type_id=settings.get("default_job_type_id"),
        default_client_id=settings.get("default_client_id"),
        default_site_id=settings.get("default_site_id"),
        cancel_missing=bool(settings.get("cancel_missing", 1)),
        import_filter_mode=settings.get("import_filter_mode") or "all",
        import_filter_patterns_raw=settings.get("import_filter_patterns_raw") or "",
        credentials_saved=credentials_saved,
        stored_email=(creds or {}).get("email") if creds else "",
        stored_org_id=(creds or {}).get("org_id") if creds else None,
        date_from=date_from.isoformat(),
        date_to=date_to.isoformat(),
        discover_url=url_for("internal_scheduling.admin_sling_discover"),
        recent_runs=recent_runs,
        config=_core_manifest,
    )


@internal_bp.post("/sling-sync/save-credentials")
@_admin_required_scheduling
def admin_sling_sync_save_credentials():
    _ensure_scheduling_tables()
    email = (request.form.get("email") or "").strip()
    password = request.form.get("password") or ""
    org_id = None
    org_raw = (request.form.get("org_id") or "").strip()
    if org_raw:
        try:
            org_id = int(org_raw)
        except ValueError:
            _flash_sling(
                "Sling organisation ID must be a whole number, or leave the field blank.",
                ok=False,
            )
            return redirect(url_for("internal_scheduling.admin_sling_sync"))
    try:
        SlingSyncService.save_credentials(email=email, password=password, org_id=org_id)
        _flash_sling("Sling credentials saved.", ok=True)
    except Exception as e:
        _flash_sling(f"Unable to save Sling credentials: {e}", ok=False)
    return redirect(url_for("internal_scheduling.admin_sling_sync"))


@internal_bp.post("/sling-sync/save-settings")
@_admin_required_scheduling
def admin_sling_sync_save_settings():
    _ensure_scheduling_tables()

    default_job_type_id = request.form.get("default_job_type_id", type=int)
    # Client/site mapping not reliably available from Sling free plan.
    # We still persist optional defaults for forward compatibility, but sync always imports under a placeholder client.
    default_client_id = None
    default_site_id = None

    cancel_missing = (request.form.get("cancel_missing") or "") == "1"
    import_filter_mode = (request.form.get("import_filter_mode") or "all").strip().lower()
    if import_filter_mode not in ("all", "include", "exclude"):
        import_filter_mode = "all"
    import_filter_patterns = request.form.get("import_filter_patterns") or ""

    try:
        SlingSyncService.update_sling_settings(
            default_job_type_id=default_job_type_id,
            default_client_id=default_client_id,
            default_site_id=default_site_id,
            cancel_missing=cancel_missing,
            import_filter_mode=import_filter_mode,
            import_filter_patterns=import_filter_patterns,
        )
        _flash_sling("Sling sync settings saved.", ok=True)
    except Exception as e:
        _flash_sling(f"Unable to save Sling settings: {e}", ok=False)

    return redirect(url_for("internal_scheduling.admin_sling_sync"))


@internal_bp.post("/sling-sync/run")
@_admin_required_scheduling
def admin_sling_sync_run():
    _ensure_scheduling_tables()

    date_from_s = (request.form.get("date_from") or "").strip()
    date_to_s = (request.form.get("date_to") or "").strip()
    dry_run = (request.form.get("dry_run") or "") == "1"

    try:
        date_from = date.fromisoformat(date_from_s)
        date_to = date.fromisoformat(date_to_s)
        if date_to < date_from:
            _flash_sling("Invalid date range.", ok=False)
            return redirect(url_for("internal_scheduling.admin_sling_sync"))

        actor = (
            (getattr(current_user, "username", None) or getattr(current_user, "email", None) or "")
            .strip()
            or None
        )
        res = SlingSyncService.sync_published_shifts(
            date_from=date_from,
            date_to=date_to,
            dry_run=dry_run,
            actor_username=actor,
        )
        sk = int(res.get("skipped_by_filter") or 0)
        sk_part = f" Skipped by filter: {sk}." if sk else ""
        if dry_run:
            _flash_sling(
                f"Dry run complete: {res.get('processed_shifts')} shifts would be written.{sk_part}",
                ok=True,
            )
        else:
            rid = res.get("sync_run_id")
            rev_part = f" Run #{rid} can be reverted below if needed." if rid else ""
            _flash_sling(
                f"Sync complete: created {res.get('created')}, updated {res.get('updated')}, "
                f"cancelled {res.get('cancelled')}.{sk_part}{rev_part}",
                ok=True,
            )
    except Exception as e:
        _flash_sling(f"Sling sync failed: {e}", ok=False)

    return redirect(url_for("internal_scheduling.admin_sling_sync"))


@internal_bp.post("/sling-sync/test-connection")
@_admin_required_scheduling
def admin_sling_sync_test_connection():
    _ensure_scheduling_tables()
    try:
        res = SlingSyncService.test_connection()
        if res.get("ok"):
            _flash_sling(res.get("message") or "Sling connection OK.", ok=True)
        else:
            _flash_sling(res.get("error") or "Connection failed.", ok=False)
    except Exception as e:
        _flash_sling(f"Sling connection test failed: {e}", ok=False)
    return redirect(url_for("internal_scheduling.admin_sling_sync"))


@internal_bp.post("/sling-sync/discover")
@_admin_required_scheduling
def admin_sling_discover():
    _ensure_scheduling_tables()
    date_from_s = (request.form.get("date_from") or "").strip()
    date_to_s = (request.form.get("date_to") or "").strip()
    try:
        date_from = date.fromisoformat(date_from_s)
        date_to = date.fromisoformat(date_to_s)
        if date_to < date_from:
            return jsonify({"ok": False, "error": "Invalid date range."}), 400
        out = SlingSyncService.discover_shifts(date_from=date_from, date_to=date_to)
        return jsonify(out)
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid dates."}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@internal_bp.post("/sling-sync/save-position-mapping")
@_admin_required_scheduling
def admin_sling_sync_save_position_mapping():
    _ensure_scheduling_tables()
    sling_position_id = (request.form.get("sling_position_id") or "").strip()
    sling_position_name = (request.form.get("sling_position_name") or "").strip() or None
    job_type_id = request.form.get("job_type_id", type=int)
    try:
        if not job_type_id:
            raise ValueError("Job type is required.")
        SlingSyncService.upsert_position_job_type_mapping(
            sling_position_id=sling_position_id,
            job_type_id=int(job_type_id),
            sling_position_name=sling_position_name,
        )
        _flash_sling("Position → job type mapping saved.", ok=True)
    except Exception as e:
        _flash_sling(f"Could not save mapping: {e}", ok=False)
    return redirect(url_for("internal_scheduling.admin_sling_sync"))


@internal_bp.post("/sling-sync/revert/<int:run_id>")
@_admin_required_scheduling
def admin_sling_sync_revert(run_id: int):
    _ensure_scheduling_tables()
    try:
        out = SlingSyncService.revert_sync_run(int(run_id))
        _flash_sling(
            "Revert complete: "
            f"removed {out.get('deleted_shifts', 0)} shift(s) created in that run, "
            f"restored {out.get('restored_updates', 0)} update(s) and "
            f"{out.get('restored_status', 0)} cancelled shift(s).",
            ok=True,
        )
    except Exception as e:
        _flash_sling(f"Revert failed: {e}", ok=False)
    return redirect(url_for("internal_scheduling.admin_sling_sync"))


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_bp
