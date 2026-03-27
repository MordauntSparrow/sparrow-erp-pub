from werkzeug.exceptions import HTTPException
from functools import wraps
from datetime import datetime, date
from io import BytesIO
import os
import json
from flask import (
    Blueprint, request, jsonify, send_file,
    render_template, redirect, url_for, flash, session, abort,
    current_app, has_request_context,
)
from flask_login import current_user, login_required
from app.objects import get_db_connection, AuthManager, PluginManager
from .services import (
    TimesheetService,
    RunsheetService,
    TemplateService,
    ExportService,
    InvoiceService,
    MinimalRateResolver,
    _dec,
)


def _tb_safe_api_error(exc: Exception, *, status: int = 400, log_message: str = "time_billing API"):
    """Log full exception; return generic error body when not in debug (production-safe)."""
    current_app.logger.exception("%s: %s", log_message, exc)
    detail = str(exc) if current_app.debug else "Something went wrong. Please try again or contact support."
    return jsonify({"ok": False, "error": detail}), status


# =============================================================================
# Blueprints
# =============================================================================
internal_template_folder = os.path.join(os.path.dirname(__file__), 'templates')
internal_bp = Blueprint("internal_time_billing", __name__,
                        url_prefix="/plugin/time_billing_module",
                        template_folder=internal_template_folder
                        )
public_bp = Blueprint("public_time_billing", __name__,
                      url_prefix="/time-billing",
                      template_folder=internal_template_folder
                      )

# =============================================================================
# Helpers: site settings bootstrap (for branding in base templates)
# =============================================================================

# Instantiate PluginManager and load core manifest.
plugin_manager = PluginManager(os.path.abspath('app/plugins'))
core_manifest = plugin_manager.get_core_manifest()


def _employee_portal_enabled():
    """
    True if employee_portal_module is installed, enabled in the plugin manifest,
    and its public blueprint is registered (avoids broken nav links).
    """
    try:
        plugs = plugin_manager.load_plugins() or {}
        info = plugs.get("employee_portal_module")
        if not info or not bool(info.get("enabled", False)):
            return False
        try:
            if has_request_context() and getattr(current_app, "blueprints", None):
                if "public_employee_portal" not in current_app.blueprints:
                    return False
        except Exception:
            pass
        return True
    except Exception:
        return False


@public_bp.context_processor
def _inject_tb_public_nav():
    """Contractor UI: show 'Back to portal' only when employee portal is on."""
    return {"tb_employee_portal_available": _employee_portal_enabled()}


# =============================================================================
# Auth (tb_contractors)
# =============================================================================


def current_tb_user():
    return session.get('tb_user') or None


def current_tb_user_id():
    u = current_tb_user()
    return int(u['id']) if u and u.get('id') is not None else None


def _set_tb_session_from_contractor_id(contractor_id: int) -> bool:
    """Load contractor by id, set session['tb_user'] if active. Returns True if session was set."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, email, name, initials, status, profile_picture_path
            FROM tb_contractors WHERE id = %s LIMIT 1
        """, (int(contractor_id),))
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if not row or str(row.get("status") or "").lower() not in ("active", "1", "true", "yes"):
        return False
    role = _contractor_effective_role(int(row["id"]))
    try:
        from app.plugins.employee_portal_module.services import safe_profile_picture_path
        safe_avatar = safe_profile_picture_path(row.get("profile_picture_path"))
    except Exception:
        safe_avatar = None
    display = (row.get("name") or "").strip() or row.get("email") or ""
    session["tb_user"] = {
        "id": int(row["id"]),
        "email": row["email"],
        "name": display,
        "initials": (row.get("initials") or "").strip(),
        "profile_picture_path": safe_avatar,
        "role": role,
    }
    session.modified = True
    from app.contractor_ui_theme import sync_contractor_theme_to_session

    sync_contractor_theme_to_session(session, int(row["id"]))
    return True


def staff_required_tb(view):
    """Require tb_user (contractor). No roles—contractors have one view; staff/admin/superuser are admin app (port 82) only."""
    @wraps(view)
    def wrapped(*args, **kwargs):
        u = current_tb_user()
        portal = _employee_portal_enabled()

        if u:
            return view(*args, **kwargs)

        if portal:
            # No session, portal on: try launch token or tb_cid cookie, else send to portal login
            launch = request.args.get("launch") if request else None
            if launch:
                try:
                    from flask import current_app
                    from itsdangerous import URLSafeTimedSerializer
                    uid = URLSafeTimedSerializer(current_app.secret_key).loads(launch, salt="tb_launch", max_age=60)
                    if _set_tb_session_from_contractor_id(uid):
                        return redirect("/time-billing/")
                except Exception:
                    pass
            token = request.cookies.get("tb_cid") if request else None
            if token:
                try:
                    from flask import current_app
                    from itsdangerous import URLSafeTimedSerializer
                    cid = URLSafeTimedSerializer(current_app.secret_key).loads(token, salt="tb_cid", max_age=60 * 60 * 24 * 7)
                    if _set_tb_session_from_contractor_id(cid):
                        return view(*args, **kwargs)
                except Exception:
                    pass
            return redirect(url_for("public_employee_portal.login_page"))

        # No session, portal off: use time-billing login
        return redirect("/time-billing/login")
    return wrapped


def _contractor_effective_role(contractor_id: int) -> str:
    """Resolve contractor's role from tb_contractor_roles (many-to-many) and role_id (single)."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT LOWER(TRIM(r.name)) AS name FROM tb_contractor_roles cr
            JOIN roles r ON r.id = cr.role_id WHERE cr.contractor_id = %s
        """, (contractor_id,))
        names = [row[0] for row in (cur.fetchall() or []) if row and row[0]]
        if 'superuser' in names:
            return 'superuser'
        if 'admin' in names:
            return 'admin'
        if names:
            return names[0] or 'staff'
        cur.execute("""
            SELECT LOWER(TRIM(r.name)) FROM tb_contractors c
            LEFT JOIN roles r ON r.id = c.role_id WHERE c.id = %s
        """, (contractor_id,))
        row = cur.fetchone()
        return (row[0] or 'staff') if row else 'staff'
    finally:
        cur.close()
        conn.close()


def admin_required_tb(view):
    """For internal (admin app, port 82): require core user with role admin/superuser (Flask-Login)."""
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        role = (getattr(current_user, 'role', None) or '').lower()
        if role not in ('admin', 'superuser'):
            flash('Admin access required.', 'error')
            return redirect(url_for('routes.dashboard'))
        return view(*args, **kwargs)
    return wrapped

# =============================================================================
# Public authentication routes (tb_contractors)
# Portal off: /time-billing/login exists, normal form login + role check.
# Portal on: GET/POST /login redirect to portal; no form here.
# =============================================================================


@public_bp.get("/login")
def login_page():
    if _employee_portal_enabled():
        return redirect(url_for('public_employee_portal.login_page'))
    if current_tb_user():
        return redirect(url_for('public_time_billing.public_dashboard_page'))
    return render_template("public/auth/login.html", config=core_manifest)


@public_bp.post("/login")
def login_submit():
    if _employee_portal_enabled():
        return redirect(url_for('public_employee_portal.login_page'))
    email = (request.form.get('email') or '').strip().lower()
    password = request.form.get('password') or ''

    if not email or not password:
        flash('Please enter your email and password.', 'error')
        return redirect(url_for('public_time_billing.login_page'))

    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT id, email, name, status, password_hash
                FROM tb_contractors
                WHERE email=%s
                LIMIT 1
            """, (email,))
            u = cur.fetchone()
        finally:
            cur.close()
            conn.close()

        if not u or not u.get('password_hash') or not AuthManager.verify_password(u['password_hash'], password):
            flash('Invalid email or password. Please check your credentials and try again.', 'error')
            return redirect(url_for('public_time_billing.login_page'))

        if str(u.get('status')).lower() not in ('active', '1', 'true', 'yes'):
            flash('Your account is inactive. Please contact an administrator.', 'error')
            return redirect(url_for('public_time_billing.login_page'))

        if request.form.get('remember') == 'on':
            session.permanent = True

        display = (u.get('name') or '').strip() or u['email']
        role = _contractor_effective_role(int(u['id']))
        session['tb_user'] = {
            "id": int(u['id']),
            "email": u['email'],
            "name": display,
            "role": role
        }
        from app.contractor_ui_theme import sync_contractor_theme_to_session

        sync_contractor_theme_to_session(session, int(u["id"]))

        ss = session.get('site_settings', {})
        ss['user_name'] = display
        session['site_settings'] = ss

        next_url = request.args.get('next') or url_for(
            'public_time_billing.public_dashboard_page')
        return redirect(next_url)
    except Exception:
        flash('An unexpected error occurred. Please try again later.', 'error')
        return redirect(url_for('public_time_billing.login_page'))


@public_bp.get("/logout")
def logout():
    session.pop('tb_user', None)
    flash('You have been logged out.', 'success')
    if _employee_portal_enabled():
        resp = redirect(url_for('public_employee_portal.login_page'))
        resp.delete_cookie('tb_cid', path='/')
        return resp
    # Portal off: back to time-billing login
    return redirect(url_for('public_time_billing.login_page'))

# Optional: password set/reset endpoints


@public_bp.post("/account/set-password")
@staff_required_tb
def set_password():
    pwd = request.form.get('password') or ''
    if len(pwd) < 8:
        return jsonify({"ok": False, "message": "Password must be at least 8 characters."}), 400

    uid = current_tb_user_id()
    hashv = AuthManager.hash_password(pwd)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE tb_contractors SET password_hash=%s WHERE id=%s", (hashv, uid))
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True})

# =============================================================================
# Internal/Admin routes (HTML + JSON)
# =============================================================================


@internal_bp.get("/")
@admin_required_tb
def admin_dashboard_page():
    iso_year, iso_week, _ = date.today().isocalendar()
    week_id = f"{iso_year}{iso_week:02d}"

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # KPIs
        cur.execute(
            "SELECT COUNT(*) AS c FROM tb_timesheet_weeks WHERE status='submitted'")
        pending_approvals = (cur.fetchone() or {}).get("c", 0)

        cur.execute("""
            SELECT COUNT(*) AS c
            FROM tb_timesheet_weeks
            WHERE status='approved' AND YEARWEEK(approved_at, 3) = YEARWEEK(CURDATE(), 3)
        """)
        approved_this_week = (cur.fetchone() or {}).get("c", 0)

        cur.execute("""
            SELECT COUNT(*) AS c
            FROM runsheets
            WHERE work_date >= CURDATE() AND status IN ('draft','published')
        """)
        active_runsheets = (cur.fetchone() or {}).get("c", 0)

        cur.execute("""
            SELECT COALESCE(SUM(e.pay),0) AS tp
            FROM tb_timesheet_entries e
            JOIN tb_timesheet_weeks w ON w.id = e.week_id
            WHERE w.week_id = %s
        """, (week_id,))
        total_pay_week = float((cur.fetchone() or {}).get("tp", 0))

        cur.execute("""
            SELECT user_id, week_id
            FROM tb_timesheet_weeks
            ORDER BY updated_at DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            example_user_id = row["user_id"]
            example_week_id = row["week_id"]
        else:
            cur.execute(
                "SELECT id FROM tb_contractors ORDER BY id ASC LIMIT 1")
            r2 = cur.fetchone()
            example_user_id = r2["id"] if r2 else 0
            example_week_id = week_id
    finally:
        cur.close()
        conn.close()

    kpis = {
        "pending_approvals": pending_approvals,
        "approved_this_week": approved_this_week,
        "active_runsheets": active_runsheets,
        "total_pay_week": total_pay_week,
        "example_user_id": example_user_id,
        "example_week_id": example_week_id,
    }

    return render_template("admin/dashboard/index.html", kpis=kpis, now=datetime.utcnow(), config=core_manifest)

# =============================================================================
# Admin week page (HTML)
# =============================================================================


@internal_bp.get("/timesheets/<int:user_id>/<week_id>")
@admin_required_tb
def admin_week_page(user_id, week_id):
    data = TimesheetService.get_week_payload(user_id, week_id, is_admin=True)
    data["week_user_id"] = user_id
    return render_template("admin/timesheets/week.html", data=data, config=core_manifest)

# HTML page


@internal_bp.get("/timesheets")
@admin_required_tb
def timesheets_overview_page():
    return render_template("admin/timesheets/overview.html", config=core_manifest)


@internal_bp.get("/api/timesheets/overview")
@admin_required_tb
def api_timesheets_overview():
    q = (request.args.get("q") or "").strip()
    status = request.args.get("status") or ""
    from_week = request.args.get("from_week_id") or ""
    to_week = request.args.get("to_week_id") or ""
    user_id = (request.args.get("user_id") or "").strip()
    page = max(1, int(request.args.get("page") or 1))
    page_size = max(1, min(200, int(request.args.get("page_size") or 25)))
    offset = (page - 1) * page_size

    # -------------------------------------------------------------------------
    # WHERE clause builder
    # -------------------------------------------------------------------------
    where = ["LOWER(CAST(c.status AS CHAR)) IN ('active','1')"]
    params = []

    if q:
        where.append("(c.name LIKE %s OR c.email LIKE %s)")
        params += [f"%{q}%", f"%{q}%"]

    if status:
        where.append("w.status = %s")
        params.append(status)

    if from_week:
        where.append("w.week_id >= %s")
        params.append(from_week)

    if to_week:
        where.append("w.week_id <= %s")
        params.append(to_week)

    if user_id:
        where.append("w.user_id = %s")
        params.append(user_id)

    where_sql = " AND ".join(where) if where else "1=1"

    # -------------------------------------------------------------------------
    # Query DB
    # -------------------------------------------------------------------------
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # Fetch paginated list
        cur.execute(
            f"""
            SELECT 
                w.id,
                w.user_id,
                w.week_id,
                w.week_ending AS week_ending,
                w.status,
                COALESCE(w.total_hours, 0) AS total_hours,
                COALESCE(w.total_pay, 0) AS total_pay,
                c.email AS email,
                c.name  AS user_name
            FROM tb_timesheet_weeks w
            JOIN tb_contractors c ON c.id = w.user_id
            WHERE {where_sql}
            ORDER BY w.week_id DESC, user_name ASC
            LIMIT %s OFFSET %s
            """,
            (*params, page_size, offset),
        )
        items = cur.fetchall() or []

        # Normalize numeric types
        for r in items:
            we = r.get("week_ending")
            if hasattr(we, "isoformat"):
                r["week_ending"] = we.isoformat()
            r["total_hours"] = float(r.get("total_hours") or 0)
            r["total_pay"] = float(r.get("total_pay") or 0)

        # Count total
        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM tb_timesheet_weeks w
            JOIN tb_contractors c ON c.id = w.user_id
            WHERE {where_sql}
            """,
            tuple(params),
        )
        total = int((cur.fetchone() or {}).get("cnt", 0))
        has_more = page * page_size < total

    finally:
        cur.close()
        conn.close()

    print({
        "items": items,
        "total": total,
        "has_more": has_more,
        "page": page,
    })

    return jsonify(
        {
            "items": items,
            "total": total,
            "has_more": has_more,
            "page": page,
        }
    )

# API: issue one week


@internal_bp.post("/api/timesheets/issue")
@admin_required_tb
def api_issue_timesheet():
    data = request.get_json(force=True) or {}
    user_id = data.get('user_id')
    week_id = data.get('week_id')
    if not user_id or not week_id:
        return jsonify({"ok": False, "error": "user_id and week_id required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM tb_timesheet_weeks WHERE user_id=%s AND week_id=%s LIMIT 1",
            (user_id, week_id)
        )
        if cur.fetchone():
            return jsonify({"ok": True, "skipped": True, "reason": "exists"}), 200

        cur.execute("""
            INSERT INTO tb_timesheet_weeks (user_id, week_id, status, total_hours, total_pay)
            VALUES (%s, %s, 'draft', 0, 0)
        """, (user_id, week_id))
        conn.commit()

        # Training: auto-assign by role change (v1).
        try:
            if role_id is not None:
                from app.plugins.training_module.services import TrainingService

                TrainingService.apply_role_assignment_rules(
                    contractor_id=int(user_id),
                    role_id=int(role_id),
                    assigned_by_user_id=getattr(current_user, "id", None),
                )
        except Exception:
            pass
        return jsonify({"ok": True})
    finally:
        cur.close()
        conn.close()


# API: issue bulk
@internal_bp.post("/api/timesheets/issue-bulk")
@admin_required_tb
def api_issue_timesheet_bulk():
    data = request.get_json(force=True) or {}
    all_active = bool(data.get("all_active"))
    user_ids = data.get("user_ids") or []
    from_week = data.get("from_week_id")
    to_week = data.get("to_week_id")

    if not from_week or not to_week:
        return (
            jsonify({"ok": False, "error": "from_week_id and to_week_id required"}),
            400,
        )

    # -------------------------------------------------------------------------
    # Helper: compute week ending date (Sunday) from ISO week "YYYYWW"
    # -------------------------------------------------------------------------
    from datetime import date, timedelta

    def week_ending_from_week_id(week_id_str: str) -> date:
        y = int(week_id_str[:4])
        w = int(week_id_str[4:])
        # ISO week: Monday=1..Sunday=7; we want Sunday as week ending
        return date.fromisocalendar(y, w, 7)

    # -------------------------------------------------------------------------
    # Helper: week id iterator inclusive
    # -------------------------------------------------------------------------
    def week_iter(w_from: str, w_to: str):
        y, w = int(w_from[:4]), int(w_from[4:])
        y2, w2 = int(w_to[:4]), int(w_to[4:])
        cur_y, cur_w = y, w
        while True:
            yield f"{cur_y}{cur_w:02d}"
            if cur_y == y2 and cur_w == w2:
                break
            cur_w += 1
            # ISO weeks can be 52 or 53 depending on year
            # Move to week 1 of next year when exceeding the year's ISO week count
            try:
                date.fromisocalendar(cur_y, cur_w, 7)
            except ValueError:
                cur_y += 1
                cur_w = 1

    # -------------------------------------------------------------------------
    # Main logic
    # -------------------------------------------------------------------------
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if all_active:
            cur.execute(
                "SELECT id FROM tb_contractors WHERE status IN ('active','1',1)"
            )
            user_ids = [r["id"] for r in (cur.fetchall() or [])]

        if not user_ids:
            return jsonify({"ok": False, "error": "No users to issue"}), 400

        ins = conn.cursor()
        for uid in user_ids:
            for wid in week_iter(from_week, to_week):
                ins.execute(
                    "SELECT 1 FROM tb_timesheet_weeks WHERE user_id=%s AND week_id=%s",
                    (uid, wid),
                )
                if not ins.fetchone():
                    we = week_ending_from_week_id(wid)  # DATE object
                    ins.execute(
                        """
                        INSERT INTO tb_timesheet_weeks
                          (user_id, week_id, week_ending, status, total_hours, total_pay, created_at, updated_at)
                        VALUES
                          (%s, %s, %s, 'draft', 0, 0, NOW(), NOW())
                        """,
                        (uid, wid, we),
                    )

        conn.commit()
        ins.close()
        return jsonify({"ok": True, "issued_for_users": len(user_ids)})
    finally:
        cur.close()
        conn.close()


# =============================================================================
# Admin JSON APIs
# =============================================================================


@internal_bp.get("/api/timesheets/<int:user_id>/<week_id>")
@admin_required_tb
def admin_get_week(user_id, week_id):
    payload = TimesheetService.get_week_payload(
        user_id, week_id, is_admin=True)
    return jsonify(payload)


@internal_bp.patch("/api/timesheets/entry/<int:entry_id>")
@admin_required_tb
def admin_patch_entry(entry_id):
    data = request.get_json(force=True) or {}
    admin_id = current_tb_user_id()
    res = TimesheetService.admin_patch_entry(
        entry_id, updates=data.get("updates") or data, admin_id=admin_id)
    return jsonify(res), (200 if res.get("ok") else 400)


@internal_bp.post("/api/timesheets/<int:user_id>/<week_id>/approve")
@admin_required_tb
def admin_approve_week(user_id, week_id):
    admin_id = current_tb_user_id()
    pdf_bytes, meta = TimesheetService.approve_week(
        admin_id=admin_id, user_id=user_id, week_id=week_id)
    return send_file(BytesIO(pdf_bytes), mimetype="application/pdf", as_attachment=True,
                     download_name=meta.get("filename", "timesheet.pdf"))


@internal_bp.post("/api/timesheets/<int:user_id>/<week_id>/reject")
@admin_required_tb
def admin_reject_week(user_id, week_id):
    data = request.get_json(force=True) or {}
    admin_id = current_tb_user_id()
    TimesheetService.reject_week(
        admin_id=admin_id, user_id=user_id, week_id=week_id, reason=data.get("reason", ""))
    return jsonify({"ok": True})

# =============================================================================
# Runsheets (admin)
# =============================================================================


@internal_bp.get("/runsheets")
@admin_required_tb
def runsheets_page():
    return render_template("admin/runsheets/list.html", config=core_manifest)


@internal_bp.get("/runsheets/edit")
@admin_required_tb
def runsheets_edit_page():
    return render_template("admin/runsheets/edit.html", config=core_manifest)


@internal_bp.get("/api/runsheets")
@admin_required_tb
def api_list_runsheets():
    res = RunsheetService.list_runsheets(
        client_name=request.args.get("client_name"),
        job_type_id=request.args.get("job_type_id"),
        week_id=request.args.get("week_id")
    )
    return jsonify(res)


@internal_bp.get("/api/runsheets/<int:rs_id>")
@admin_required_tb
def api_get_runsheet(rs_id):
    rs = RunsheetService.get_runsheet(rs_id)
    if not rs or not rs.get("id"):
        return jsonify({"error": "Runsheet not found"}), 404
    return jsonify(rs)


@internal_bp.post("/api/runsheets")
@admin_required_tb
def api_create_runsheet():
    data = request.get_json(force=True) or {}
    rs_id = RunsheetService.create_runsheet(data)
    return jsonify({"ok": True, "id": rs_id}), 201


@internal_bp.put("/api/runsheets/<int:rs_id>")
@admin_required_tb
def api_update_runsheet(rs_id):
    data = request.get_json(force=True) or {}
    RunsheetService.update_runsheet(rs_id, data)
    return jsonify({"ok": True})


@internal_bp.post("/api/runsheets/<int:rs_id>/publish")
@admin_required_tb
def api_publish_runsheet(rs_id):
    published_by = current_tb_user_id()
    res = RunsheetService.publish_runsheet(rs_id, published_by=published_by)
    return jsonify(res), (200 if res.get("ok") else 400)

# =============================================================================
# Configuration hub (job types, rates, policies, templates)
# =============================================================================


@internal_bp.get("/config/page")
@admin_required_tb
def config_page():
    """Single hub for all Time Billing configuration."""
    return render_template("admin/config/index.html", config=core_manifest)


@internal_bp.get("/job-types/page")
@admin_required_tb
def job_types_page():
    return render_template("admin/job_types/list.html", config=core_manifest)


@internal_bp.get("/api/job-types")
@admin_required_tb
def api_list_job_types():
    return jsonify(TemplateService.list_job_types())


@internal_bp.post("/api/job-types")
@admin_required_tb
def api_create_job_type():
    data = request.get_json(force=True) or {}
    try:
        jid = TemplateService.create_job_type(data)
        return jsonify({"id": jid}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.put("/api/job-types/<int:jid>")
@admin_required_tb
def api_update_job_type(jid):
    data = request.get_json(force=True) or {}
    data["id"] = jid
    try:
        TemplateService.update_job_type(data)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.delete("/api/job-types/<int:jid>")
@admin_required_tb
def api_delete_job_type(jid):
    try:
        TemplateService.delete_job_type(jid)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# =============================================================================
# Rate cards / policies (admin)
# =============================================================================


@internal_bp.get("/rates/wage-cards/page")
@admin_required_tb
def wage_cards_page():
    return render_template("admin/rates/wage_cards.html", config=core_manifest)


@internal_bp.get("/rates/bill-cards/page")
@admin_required_tb
def bill_cards_page():
    return render_template("admin/rates/bill_cards.html", config=core_manifest)


@internal_bp.get("/api/wage-cards")
@admin_required_tb
def get_wage_cards():
    return jsonify(TemplateService.list_wage_cards())


@internal_bp.post("/api/wage-cards")
@admin_required_tb
def create_wage_card():
    data = request.get_json(force=True) or {}
    return jsonify({"id": TemplateService.create_wage_card(data)})


@internal_bp.get("/api/wage-cards/<int:card_id>/rows")
@admin_required_tb
def get_wage_card_rows(card_id):
    return jsonify(TemplateService.list_wage_rows(card_id))


@internal_bp.post("/api/wage-cards/<int:card_id>/rows")
@admin_required_tb
def add_wage_card_row(card_id):
    data = request.get_json(force=True) or {}
    return jsonify({"id": TemplateService.add_wage_row(card_id, data)})


@internal_bp.put("/api/wage-cards/<int:card_id>")
@admin_required_tb
def update_wage_card_api(card_id):
    data = request.get_json(force=True) or {}
    try:
        TemplateService.update_wage_card(card_id, data)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.delete("/api/wage-cards/<int:card_id>")
@admin_required_tb
def delete_wage_card_api(card_id):
    try:
        TemplateService.delete_wage_card(card_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.put("/api/wage-cards/<int:card_id>/rows/<int:row_id>")
@admin_required_tb
def update_wage_card_row_api(card_id, row_id):
    data = request.get_json(force=True) or {}
    try:
        TemplateService.update_wage_row(card_id, row_id, data)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.delete("/api/wage-cards/<int:card_id>/rows/<int:row_id>")
@admin_required_tb
def delete_wage_card_row_api(card_id, row_id):
    try:
        TemplateService.delete_wage_row(card_id, row_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.get("/api/bill-cards")
@admin_required_tb
def get_bill_cards():
    return jsonify(TemplateService.list_bill_cards())


@internal_bp.post("/api/bill-cards")
@admin_required_tb
def create_bill_card():
    data = request.get_json(force=True) or {}
    return jsonify({"id": TemplateService.create_bill_card(data)})


@internal_bp.get("/api/bill-cards/<int:card_id>/rows")
@admin_required_tb
def get_bill_card_rows(card_id):
    return jsonify(TemplateService.list_bill_rows(card_id))


@internal_bp.post("/api/bill-cards/<int:card_id>/rows")
@admin_required_tb
def add_bill_card_row(card_id):
    data = request.get_json(force=True) or {}
    return jsonify({"id": TemplateService.add_bill_row(card_id, data)})


@internal_bp.put("/api/bill-cards/<int:card_id>")
@admin_required_tb
def update_bill_card_api(card_id):
    data = request.get_json(force=True) or {}
    try:
        TemplateService.update_bill_card(card_id, data)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.delete("/api/bill-cards/<int:card_id>")
@admin_required_tb
def delete_bill_card_api(card_id):
    try:
        TemplateService.delete_bill_card(card_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.put("/api/bill-cards/<int:card_id>/rows/<int:row_id>")
@admin_required_tb
def update_bill_card_row_api(card_id, row_id):
    data = request.get_json(force=True) or {}
    try:
        TemplateService.update_bill_row(card_id, row_id, data)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.delete("/api/bill-cards/<int:card_id>/rows/<int:row_id>")
@admin_required_tb
def delete_bill_card_row_api(card_id, row_id):
    try:
        TemplateService.delete_bill_row(card_id, row_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.get("/api/policies")
@admin_required_tb
def get_policies():
    return jsonify(TemplateService.list_policies())


@internal_bp.post("/api/policies")
@admin_required_tb
def create_policy():
    data = request.get_json(force=True) or {}
    return jsonify({"id": TemplateService.create_policy(data)})


@internal_bp.get("/policies/page")
@admin_required_tb
def policies_list_page():
    return render_template("admin/policies/list.html", config=core_manifest)


@internal_bp.get("/policies/edit")
@admin_required_tb
def policies_edit_page():
    return render_template("admin/policies/edit.html", config=core_manifest)

# =============================================================================
# Templates (runsheet templates manager) – admin
# =============================================================================


@internal_bp.get("/templates/page")
@admin_required_tb
def templates_list_page():
    return render_template("admin/templates/list.html", config=core_manifest)


@internal_bp.get("/templates/edit")
@admin_required_tb
def templates_edit_page():
    return render_template("admin/templates/edit.html", config=core_manifest)


@internal_bp.get("/templates/preview")
@admin_required_tb
def templates_preview_page():
    return render_template("admin/templates/preview.html", config=core_manifest)


@internal_bp.get("/api/templates")
@admin_required_tb
def list_templates():
    return jsonify(TemplateService.list_runsheet_templates())


@internal_bp.post("/api/templates")
@admin_required_tb
def create_template():
    data = request.get_json(force=True) or {}
    try:
        return jsonify({"id": TemplateService.create_runsheet_template(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.get("/api/templates/<int:tpl_id>")
@admin_required_tb
def get_template(tpl_id):
    return jsonify(TemplateService.get_runsheet_template(tpl_id))


@internal_bp.put("/api/templates/<int:tpl_id>")
@admin_required_tb
def update_template(tpl_id):
    data = request.get_json(force=True) or {}
    TemplateService.update_runsheet_template(tpl_id, data)
    return jsonify({"ok": True})


@internal_bp.get("/api/templates/<int:tpl_id>/fields")
@admin_required_tb
def get_template_fields(tpl_id):
    return jsonify(TemplateService.list_template_fields(tpl_id))


@internal_bp.post("/api/templates/<int:tpl_id>/fields")
@admin_required_tb
def add_template_field(tpl_id):
    data = request.get_json(force=True) or {}
    return jsonify({"id": TemplateService.add_template_field(tpl_id, data)})


@internal_bp.put("/api/templates/<int:tpl_id>/fields/<int:field_id>")
@admin_required_tb
def update_template_field(tpl_id, field_id):
    data = request.get_json(force=True) or {}
    TemplateService.update_template_field(tpl_id, field_id, data)
    return jsonify({"ok": True})


@internal_bp.delete("/api/templates/<int:tpl_id>/fields/<int:field_id>")
@admin_required_tb
def delete_template_field(tpl_id, field_id):
    TemplateService.delete_template_field(tpl_id, field_id)
    return jsonify({"ok": True})

# =============================================================================
# Contractor Management (admin)
# =============================================================================

# Contractor pages


@internal_bp.get("/contractors")
@admin_required_tb
def contractors_page():
    return render_template("admin/contractors/list.html", config=core_manifest)


@internal_bp.get("/contractors/edit")
@admin_required_tb
def contractors_edit_page():
    return render_template("admin/contractors/edit.html", config=core_manifest)


# =============================================================================
# Contractor invoices (admin)
# =============================================================================


@internal_bp.get("/invoices")
@admin_required_tb
def admin_invoices_list_page():
    cid = request.args.get("contractor_id", type=int)
    st = (request.args.get("status") or "").strip().lower() or None
    rows = InvoiceService.list_invoices_admin(contractor_id=cid, status=st, limit=400)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, name, email FROM tb_contractors
            ORDER BY name ASC, email ASC
            LIMIT 500
            """
        )
        contractors = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return render_template(
        "admin/invoices/list.html",
        invoices=rows,
        contractors=contractors,
        filter_contractor_id=cid,
        filter_status=st,
        config=core_manifest,
    )


@internal_bp.get("/invoices/analytics")
@admin_required_tb
def admin_invoices_analytics_page():
    y = request.args.get("year", type=int)
    if not y:
        y = datetime.utcnow().year
    data = InvoiceService.admin_invoice_analytics(year=y)
    return render_template(
        "admin/invoices/analytics.html",
        analytics=data,
        selected_year=y,
        config=core_manifest,
    )


@internal_bp.get("/invoices/<int:invoice_id>")
@admin_required_tb
def admin_invoice_detail_page(invoice_id):
    inv = InvoiceService.get_invoice_detail_admin(invoice_id)
    if not inv:
        flash("Invoice not found.", "error")
        return redirect(url_for("internal_time_billing.admin_invoices_list_page"))
    return render_template(
        "admin/invoices/detail.html",
        invoice=inv,
        config=core_manifest,
    )


@internal_bp.get("/invoices/<int:invoice_id>.pdf")
@admin_required_tb
def admin_invoice_pdf(invoice_id):
    pdf_bytes, fname = InvoiceService.generate_invoice_pdf(invoice_id, contractor_id=None)
    if not pdf_bytes:
        abort(404)
    return send_file(
        BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=fname,
    )


@internal_bp.post("/invoices/<int:invoice_id>/void")
@admin_required_tb
def admin_invoice_void(invoice_id):
    reason = (request.form.get("reason") or "").strip() or "Voided by administrator."
    if InvoiceService.admin_void_invoice(invoice_id, reason):
        flash("Invoice voided. Contractor can create a new invoice after the week is approved again.", "success")
    else:
        flash("Could not void invoice (already void or missing).", "error")
    return redirect(url_for("internal_time_billing.admin_invoices_list_page"))


# =============================================================================
# API Endpoints (roles are many-to-many via roles + tb_contractor_roles)
# =============================================================================

# Get single contractor (with roles array)
@internal_bp.get("/api/contractors/<int:user_id>")
@admin_required_tb
def api_get_contractor(user_id):
    import mysql.connector
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        try:
            cur.execute("""
                SELECT
                    c.id,
                    c.email,
                    c.name,
                    c.status,
                    c.role_id,
                    c.wage_rate_card_id,
                    COALESCE(c.employment_type, 'paye') AS employment_type,
                    CASE WHEN c.status = 'active' THEN 1 ELSE 0 END AS is_active,
                    DATE_FORMAT(c.created_at, '%%Y-%%m-%%d %%H:%%i') AS created_at
                FROM tb_contractors c
                WHERE c.id = %s
            """, (user_id,))
        except (mysql.connector.Error, Exception):
            cur.execute("""
                SELECT
                    c.id,
                    c.email,
                    c.name,
                    c.status,
                    c.role_id,
                    c.wage_rate_card_id,
                    CASE WHEN c.status = 'active' THEN 1 ELSE 0 END AS is_active,
                    DATE_FORMAT(c.created_at, '%%Y-%%m-%%d %%H:%%i') AS created_at
                FROM tb_contractors c
                WHERE c.id = %s
            """, (user_id,))
        row = cur.fetchone()

        if not row:
            return jsonify({"error": "not found"}), 404
        if "employment_type" not in row:
            row["employment_type"] = "self_employed"

        # Optionally keep roles array (for display/back-compat)
        cur.execute("""
            SELECT LOWER(r.name) AS name
            FROM tb_contractor_roles cr
            JOIN roles r ON r.id = cr.role_id
            WHERE cr.contractor_id = %s
            ORDER BY r.name
        """, (user_id,))
        role_names = [r["name"] for r in (cur.fetchall() or [])]

        row["roles"] = role_names
        row["roles_array"] = role_names

        return jsonify(row)
    finally:
        cur.close()
        conn.close()


# List contractors (with aggregated roles, filterable by a role name)


@internal_bp.get("/api/contractors")
@admin_required_tb
def api_list_contractors():
    q = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    role_filter = (request.args.get("role") or "").strip().lower()
    page = max(1, int(request.args.get("page") or 1))
    page_size = max(1, min(200, int(request.args.get("page_size") or 25)))
    offset = (page - 1) * page_size

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["1=1"]
        params = []

        if q:
            where.append("(c.name LIKE %s OR c.email LIKE %s)")
            params.extend([f"%{q}%", f"%{q}%"])

        if status in ("active", "inactive"):
            where.append("c.status = %s")
            params.append(status)

        if role_filter:
            where.append("""
                EXISTS (
                  SELECT 1
                  FROM tb_contractor_roles cr
                  JOIN roles r ON r.id = cr.role_id
                  WHERE cr.contractor_id = c.id AND LOWER(r.name) = LOWER(%s)
                )
            """)
            params.append(role_filter)

        where_sql = " AND ".join(where)

        cur.execute(f"""
            SELECT 
                c.id,
                c.email,
                c.name,
                c.status,
                CASE WHEN c.status='active' THEN 1 ELSE 0 END AS is_active,
                DATE_FORMAT(c.created_at,'%%Y-%%m-%%d %%H:%%i') AS created_at,
                COALESCE((
                    SELECT GROUP_CONCAT(LOWER(r.name) ORDER BY r.name SEPARATOR ', ')
                    FROM tb_contractor_roles cr
                    JOIN roles r ON r.id = cr.role_id
                    WHERE cr.contractor_id = c.id
                ), '') AS roles,
                COALESCE((
                    SELECT JSON_ARRAYAGG(name)
                    FROM (
                        SELECT LOWER(r.name) AS name
                        FROM tb_contractor_roles cr
                        JOIN roles r ON r.id = cr.role_id
                        WHERE cr.contractor_id = c.id
                        ORDER BY r.name
                    ) sub
                ), JSON_ARRAY()) AS roles_array
            FROM tb_contractors c
            WHERE {where_sql}
            ORDER BY c.created_at DESC, c.id DESC
            LIMIT %s OFFSET %s
        """, (*params, page_size, offset))

        items = cur.fetchall() or []   # 👈 fetch results here

        # MySQL 5.7 fallback: build roles_array with an extra query
        # ids = [i["id"] for i in items]
        # if ids:
        #     cur.execute(f"""
        #         SELECT cr.contractor_id AS cid, LOWER(r.name) AS role_name
        #         FROM tb_contractor_roles cr
        #         JOIN roles r ON r.id = cr.role_id
        #         WHERE cr.contractor_id IN ({",".join(["%s"]*len(ids))})
        #         ORDER BY r.name
        #     """, tuple(ids))
        #     roles_map = {}
        #     for rr in (cur.fetchall() or []):
        #         roles_map.setdefault(rr["cid"], []).append(rr["role_name"])
        #     for i in items:
        #         i["roles_array"] = roles_map.get(i["id"], [])
        #         i["roles"] = ", ".join(i["roles_array"])

        cur.execute(f"""
            SELECT COUNT(*) AS cnt
            FROM tb_contractors c
            WHERE {where_sql}
        """, tuple(params))
        total = int((cur.fetchone() or {}).get("cnt", 0))
        has_more = page * page_size < total

        return jsonify({
            "items": items,
            "total": total,
            "has_more": has_more,
            "page": page
        })

    finally:
        cur.close()
        conn.close()


# Create contractor (accepts roles: list[str])
@internal_bp.post("/api/contractors")
@admin_required_tb
def api_create_contractor():
    data = request.get_json(force=True) or {}

    # Inputs
    email = (data.get("email") or "").strip().lower()
    name = (data.get("name") or "").strip()
    status_raw = data.get("status") or "active"
    status = "active" if str(status_raw).lower() in (
        "active", "1", "true") else "inactive"
    password = data.get("password") or ""

    # Single role required. If legacy array provided, pick first.
    role_single = (data.get("role") or "").strip().lower()
    roles_array = data.get("roles") or []
    if (not role_single) and isinstance(roles_array, list) and roles_array:
        role_single = str(roles_array[0]).strip().lower()

    # Wage rate card (optional)
    wage_rate_card_id = data.get("wage_rate_card_id")
    wage_rate_card_id = int(wage_rate_card_id) if str(
        wage_rate_card_id).isdigit() else None

    # Validation
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Valid email required"}), 400
    if not name:
        return jsonify({"ok": False, "error": "Name required"}), 400
    if not role_single:
        return jsonify({"ok": False, "error": "Role required"}), 400

    pwd_hash = AuthManager.hash_password(password) if password else None

    conn = get_db_connection()
    try:
        # 1) Unique email check
        cur_check = conn.cursor(dictionary=True)
        cur_check.execute(
            "SELECT 1 FROM tb_contractors WHERE email=%s LIMIT 1", (email,)
        )
        if cur_check.fetchone():
            cur_check.close()
            return jsonify({"ok": False, "error": "Email already exists"}), 409
        cur_check.close()

        # 2) Resolve role_id (find or create)
        cur_role = conn.cursor()
        cur_role.execute(
            "SELECT id FROM roles WHERE name=%s LIMIT 1", (role_single,))
        row = cur_role.fetchone()
        if row:
            role_id = row[0] if isinstance(row, tuple) else row.get("id")
        else:
            cur_role.execute(
                "INSERT INTO roles (name) VALUES (%s)", (role_single,))
            role_id = cur_role.lastrowid
        cur_role.close()

        # 3) Insert contractor
        cur_ins = conn.cursor()
        cur_ins.execute(
            """
            INSERT INTO tb_contractors (
                email, name, status, password_hash, role_id, wage_rate_card_id, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """,
            (email, name, status, pwd_hash, role_id, wage_rate_card_id),
        )
        contractor_id = cur_ins.lastrowid

        # 4) Also link role
        cur_ins.execute(
            """
            INSERT IGNORE INTO tb_contractor_roles (contractor_id, role_id)
            VALUES (%s, %s)
            """,
            (contractor_id, role_id),
        )

        conn.commit()
        cur_ins.close()

        return jsonify({"ok": True, "id": contractor_id})

    finally:
        conn.close()


# Update contractor (replaces roles with provided list)


@internal_bp.put("/api/contractors/<int:user_id>")
@admin_required_tb
def api_update_contractor(user_id):
    data = request.get_json(force=True) or {}
    name = (data.get("name") or "").strip()
    status_raw = data.get("status") or "active"
    status = "active" if str(status_raw).lower() in (
        "active", "1", "true") else "inactive"

    role_id = data.get("role_id")
    wage_rate_card_id = data.get("wage_rate_card_id")
    employment_type = (data.get("employment_type") or "self_employed").strip().lower()
    if employment_type not in ("paye", "self_employed"):
        employment_type = "self_employed"
    # legacy roles support (optional)
    roles = data.get("roles") or []

    if not name:
        return jsonify({"ok": False, "error": "Name required"}), 400

    import mysql.connector
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # update base fields (employment_type if column exists)
        try:
            cur.execute("""
                UPDATE tb_contractors
                SET name = %s, status = %s, employment_type = %s
                WHERE id = %s
            """, (name, status, employment_type, user_id))
        except (mysql.connector.Error, Exception):
            cur.execute("""
                UPDATE tb_contractors
                SET name = %s, status = %s
                WHERE id = %s
            """, (name, status, user_id))

        # update primary role_id if provided
        if role_id is not None:
            cur.execute("""
                UPDATE tb_contractors
                SET role_id = %s
                WHERE id = %s
            """, (role_id, user_id))

            # keep join table in sync with primary role
            cur.execute(
                "DELETE FROM tb_contractor_roles WHERE contractor_id = %s", (user_id,))
            cur.execute("""
                INSERT IGNORE INTO tb_contractor_roles (contractor_id, role_id)
                VALUES (%s, %s)
            """, (user_id, role_id))

        # update wage card if provided (nullable)
        if wage_rate_card_id is not None:
            cur.execute("""
                UPDATE tb_contractors
                SET wage_rate_card_id = %s
                WHERE id = %s
            """, (wage_rate_card_id, user_id))

        # optional legacy array support (only if role_id not provided)
        if role_id is None and isinstance(roles, list):
            cur.execute(
                "DELETE FROM tb_contractor_roles WHERE contractor_id = %s", (user_id,))
            norm = sorted(set([str(r).strip().lower()
                          for r in roles if str(r).strip()]))
            for rn in norm:
                cur2 = conn.cursor()
                cur2.execute(
                    "SELECT id FROM roles WHERE name = %s LIMIT 1", (rn,))
                row = cur2.fetchone()
                rid = row[0] if row else None
                if not rid:
                    cur2.execute("INSERT INTO roles (name) VALUES (%s)", (rn,))
                    rid = cur2.lastrowid
                cur2.execute("""
                    INSERT IGNORE INTO tb_contractor_roles (contractor_id, role_id)
                    VALUES (%s, %s)
                """, (user_id, rid))
                cur2.close()

        conn.commit()
        return jsonify({"ok": True})
    finally:
        cur.close()
        conn.close()


@internal_bp.get("/api/roles")
@admin_required_tb
def api_list_roles():
    q = (request.args.get("q") or "").strip().lower()
    limit = max(1, min(200, int(request.args.get("limit") or 200)))

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["1=1"]
        params = []

        if q:
            where.append("LOWER(name) LIKE %s")
            params.append(f"%{q}%")

        where_sql = " AND ".join(where)

        cur.execute(
            f"""
            SELECT id, LOWER(name) AS name
            FROM roles
            WHERE {where_sql}
            ORDER BY name
            LIMIT %s
            """,
            (*params, limit),
        )
        items = cur.fetchall() or []
        return jsonify({"items": items})
    finally:
        cur.close()
        conn.close()

# Set password


@internal_bp.post("/api/contractors/<int:user_id>/set-password")
@admin_required_tb
def api_contractor_set_password(user_id):
    data = request.get_json(force=True) or {}
    password = data.get("password") or ""
    if len(password) < 8:
        return jsonify({"ok": False, "error": "Password must be at least 8 characters"}), 400

    pwd_hash = AuthManager.hash_password(password)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE tb_contractors
            SET password_hash=%s
            WHERE id=%s
        """, (pwd_hash, user_id))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        cur.close()
        conn.close()


# Toggle status
@internal_bp.post("/api/contractors/<int:user_id>/toggle")
@admin_required_tb
def api_contractor_toggle(user_id):
    data = request.get_json(force=True) or {}
    status = (data.get("status") or "").lower()
    if status not in ("active", "inactive"):
        return jsonify({"ok": False, "error": "status must be active|inactive"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE tb_contractors
            SET status=%s
            WHERE id=%s
        """, (status, user_id))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        cur.close()
        conn.close()

# =============================================================================
# Public / Staff Dashboard & Timesheets
# =============================================================================


@public_bp.get("/")
@staff_required_tb
def public_dashboard_page():
    uid = current_tb_user_id()
    iso_year, iso_week, _ = date.today().isocalendar()
    current_week_id = f"{iso_year}{iso_week:02d}"

    # Current week payload (for submission status; also keeps example_week_id)
    payload = TimesheetService.get_week_payload(
        uid, current_week_id, is_admin=False)
    week = payload.get("week", {})

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # Year-to-date totals (ISO year). To use calendar year, filter by YEAR(week_ending) instead.
        cur.execute("""
            SELECT
              COALESCE(SUM(w.total_hours), 0) AS hours_ytd,
              COALESCE(SUM(w.total_pay),   0) AS pay_ytd
            FROM tb_timesheet_weeks w
            WHERE w.user_id=%s AND LEFT(w.week_id, 4)=%s
        """, (uid, str(iso_year)))
        ytd = cur.fetchone() or {"hours_ytd": 0, "pay_ytd": 0}
        hours_ytd = round(float(ytd.get("hours_ytd") or 0), 2)
        pay_ytd = float(ytd.get("pay_ytd") or 0)

        # All weeks for the current ISO year (remove LIMIT; filter by LEFT(week_id,4))
        cur.execute("""
            SELECT
              week_id,
              week_ending AS week_ending,
              COALESCE(total_hours,0) AS total_hours,
              COALESCE(total_pay,  0) AS total_pay,
              status
            FROM tb_timesheet_weeks
            WHERE user_id = %s
              AND LEFT(week_id, 4) = %s
            ORDER BY week_ending DESC
        """, (uid, str(iso_year)))
        recent_weeks = cur.fetchall() or []

        # Normalize for template
        for r in recent_weeks:
            we = r.get("week_ending")
            if hasattr(we, "isoformat"):
                r["week_ending"] = we.isoformat()
            r["total_hours"] = round(float(r.get("total_hours") or 0), 2)
            r["total_pay"] = float(r.get("total_pay") or 0)

        # Latest available week for buttons (fallback to current if none)
        cur.execute("""
            SELECT week_id
            FROM tb_timesheet_weeks
            WHERE user_id=%s
            ORDER BY week_ending DESC
            LIMIT 1
        """, (uid,))
        row = cur.fetchone()
        latest_week_id = (row or {}).get("week_id") or current_week_id

    finally:
        cur.close()
        conn.close()

    kpis = {
        "hours_year": hours_ytd,
        "pay_year": pay_ytd,
        "submission_status": week.get("status", "draft"),
    }

    return render_template(
        "public/dashboard/index.html",
        # keep as "this week" if you still want that button
        example_week_id=current_week_id,
        latest_week_id=latest_week_id,     # new: use for “Open Latest Week” buttons
        kpis=kpis,
        recent_weeks=recent_weeks,
        config=core_manifest,
    )


@public_bp.get("/runsheets")
@staff_required_tb
def public_runsheets_list_page():
    uid = current_tb_user_id()
    return render_template(
        "public/runsheets/list.html",
        config=core_manifest,
        current_contractor_id=int(uid),
    )


@public_bp.get("/runsheets/new")
@staff_required_tb
def public_runsheets_new_page():
    uid = current_tb_user_id()
    return render_template(
        "public/runsheets/edit.html",
        config=core_manifest,
        rs_id=None,
        current_contractor_id=int(uid),
    )


@public_bp.get("/runsheets/<int:rs_id>")
@staff_required_tb
def public_runsheets_edit_page(rs_id):
    uid = current_tb_user_id()
    if not RunsheetService.contractor_can_access_runsheet(rs_id, int(uid)):
        abort(403)
    return render_template(
        "public/runsheets/edit.html",
        config=core_manifest,
        rs_id=rs_id,
        current_contractor_id=int(uid),
    )


# -------------------------
# Public Timesheet Pages
# -------------------------


@public_bp.get("/weeks/<week_id>")
@staff_required_tb
def public_week_page(week_id):
    uid = current_tb_user_id()
    data = TimesheetService.get_week_payload(uid, week_id, is_admin=False)
    return render_template("public/timesheets/week.html", data=data, config=core_manifest)


@public_bp.get("/weeks/<week_id>/spreadsheet")
@staff_required_tb
def public_week_readonly(week_id):
    uid = current_tb_user_id()
    data = TimesheetService.get_week_payload(uid, week_id, is_admin=False)
    return render_template("public/timesheets/spreadsheet_view.html", data=data, config=core_manifest)

# -------------------------
# Public JSON APIs
# -------------------------


@public_bp.delete("/api/weeks/<week_id>/entry/<int:entry_id>")
@staff_required_tb
def api_public_delete_entry(week_id, entry_id):
    """
    Delete a timesheet entry for the current user.

    Args:
        week_id (str): Week identifier
        entry_id (int): Entry identifier

    Returns:
        JSON response with success status and updated totals.
    """
    uid = current_tb_user_id()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # Ensure the week exists and get PK
        wk = TimesheetService._ensure_week(uid, week_id)

        # Check ownership and week match
        cur.execute(
            "SELECT id, user_id, week_id, source, runsheet_id FROM tb_timesheet_entries WHERE id=%s",
            (entry_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"ok": False, "message": "not_found"}), 404
        if row["user_id"] != uid or row["week_id"] != wk["id"]:
            return jsonify({"ok": False, "message": "not_owner"}), 403

        # If this was a scheduler-prefilled entry, record the removal so
        # auto-prefill won't re-create it the next time the week is opened.
        if (row.get("source") or "").lower() == "scheduler" and row.get("runsheet_id"):
            cur.execute("SHOW TABLES LIKE 'tb_scheduler_shift_removals'")
            if cur.fetchone():
                cur.execute(
                    """
                    INSERT INTO tb_scheduler_shift_removals (user_id, schedule_shift_id)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE created_at=CURRENT_TIMESTAMP
                    """,
                    (uid, int(row["runsheet_id"])),
                )

        # Delete the entry
        cur.execute("DELETE FROM tb_timesheet_entries WHERE id=%s", (entry_id,))

        # Refresh week totals
        TimesheetService._refresh_week_totals(cur, uid, wk["id"])
        conn.commit()

        # Return updated totals for immediate UI refresh
        cur.execute(
            """
            SELECT 
                COALESCE(SUM(actual_hours), 0)       AS th,
                COALESCE(SUM(pay), 0)                AS tp,
                COALESCE(SUM(travel_parking), 0)     AS tt
            FROM tb_timesheet_entries
            WHERE user_id=%s AND week_id=%s
            """,
            (uid, wk["id"]),
        )
        agg = cur.fetchone() or {}
        totals = {
            "hours": float(agg.get("th") or 0),
            "pay": float(_dec(agg.get("tp") or 0)),
            "travel": float(_dec(agg.get("tt") or 0)),
        }
        return jsonify({"ok": True, "totals": totals})

    finally:
        cur.close()
        conn.close()


@public_bp.get("/api/weeks/<week_id>")
@staff_required_tb
def api_public_get_week(week_id):
    uid = current_tb_user_id()
    return jsonify(TimesheetService.get_week_payload(uid, week_id, is_admin=False))


@public_bp.post("/api/weeks/<week_id>/upsert")
@staff_required_tb
def api_public_upsert(week_id):
    uid = current_tb_user_id()
    data = request.get_json(force=True) or {}
    res = TimesheetService.batch_upsert(
        uid, week_id, data.get("entries") or [], data.get("client_updated_at")
    )
    return jsonify(res), (200 if res.get("ok") else 409)


@public_bp.post("/api/weeks/<week_id>/submit")
@staff_required_tb
def api_public_submit(week_id):
    uid = current_tb_user_id()
    TimesheetService.submit_week(uid, week_id)
    return jsonify({"ok": True})


# -------------------------
# Self-employed invoice (mobile-first)
# -------------------------


@public_bp.get("/weeks/<week_id>/invoice")
@staff_required_tb
def public_week_invoice_page(week_id):
    """Mobile-first page to create an invoice with the timesheet (submitted or approved). Self-employed only."""
    uid = current_tb_user_id()
    wk = TimesheetService._ensure_week(uid, week_id)
    status = (wk.get("status") or "").lower()
    if status not in ("submitted", "approved"):
        flash("Submit your timesheet first, then create an invoice to send for approval with it.", "warning")
        return redirect(url_for("public_time_billing.public_week_page", week_id=week_id))
    if InvoiceService.get_contractor_employment_type(uid) != "self_employed":
        flash("Invoicing is for self-employed contractors only.", "info")
        return redirect(url_for("public_time_billing.public_week_page", week_id=week_id))
    entries = InvoiceService.get_uninvoiced_entries(wk["id"], uid)
    if not entries:
        flash("No uninvoiced entries for this week.", "info")
        return redirect(url_for("public_time_billing.public_week_page", week_id=week_id))
    total = sum((e.get("pay") or 0) + (e.get("travel_parking") or 0) for e in entries)
    suggested = InvoiceService.get_next_invoice_number(uid)
    invoice_info = InvoiceService.get_week_invoice_info(wk["id"])
    we = wk.get("week_ending")
    if hasattr(we, "strftime"):
        we = we.strftime("%Y-%m-%d")
    return render_template(
        "public/invoice/create.html",
        week_id=week_id,
        week_ending=we,
        entries=entries,
        total=total,
        suggested_invoice_number=suggested,
        has_voided_invoice=invoice_info.get("has_voided_invoice"),
        week_status=status,
        config=core_manifest,
    )


@public_bp.post("/api/weeks/<week_id>/invoice")
@staff_required_tb
def api_public_create_invoice(week_id):
    """Create invoice with timesheet: draft when week submitted, sent when week already approved."""
    uid = current_tb_user_id()
    wk = TimesheetService._ensure_week(uid, week_id)
    status = (wk.get("status") or "").lower()
    if status not in ("submitted", "approved"):
        return jsonify({"ok": False, "message": "Submit your timesheet first"}), 400
    if InvoiceService.get_contractor_employment_type(uid) != "self_employed":
        return jsonify({"ok": False, "message": "Self-employed only"}), 403
    data = request.get_json() or {}
    invoice_number = (data.get("invoice_number") or "").strip() or InvoiceService.get_next_invoice_number(uid)
    mark_sent = status == "approved"
    try:
        inv = InvoiceService.create_invoice(uid, wk["id"], invoice_number, mark_sent=mark_sent)
        return jsonify({"ok": True, "invoice": inv})
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400


@public_bp.post("/weeks/<week_id>/invoice/finalize")
@staff_required_tb
def public_week_invoice_finalize(week_id):
    """
    Promote draft → sent when the week is already approved (submitted-then-approved invoice flow).
    """
    uid = current_tb_user_id()
    wk = TimesheetService._ensure_week(uid, week_id)
    try:
        out = InvoiceService.finalize_draft_invoice_for_week(uid, int(wk["id"]))
        if out:
            flash(
                "Invoice #%s finalized and marked sent. This week is now marked invoiced."
                % (out.get("invoice_number") or out.get("id")),
                "success",
            )
        else:
            flash("No draft invoice was found for this week to finalize.", "warning")
    except ValueError as e:
        flash(str(e), "error")
    return redirect(url_for("public_time_billing.public_week_page", week_id=week_id))


@public_bp.get("/invoices")
@staff_required_tb
def public_invoices_list_page():
    uid = current_tb_user_id()
    rows = InvoiceService.list_invoices_for_contractor(uid)
    return render_template(
        "public/invoice/list.html",
        invoices=rows,
        config=core_manifest,
    )


@public_bp.get("/invoices/<int:invoice_id>")
@staff_required_tb
def public_invoice_detail_page(invoice_id):
    uid = current_tb_user_id()
    inv = InvoiceService.get_invoice_detail_for_contractor(invoice_id, uid)
    if not inv:
        flash("Invoice not found.", "error")
        return redirect(url_for("public_time_billing.public_invoices_list_page"))
    return render_template(
        "public/invoice/detail.html",
        invoice=inv,
        config=core_manifest,
    )


@public_bp.get("/invoices/<int:invoice_id>/download")
@staff_required_tb
def public_invoice_pdf_download(invoice_id):
    uid = current_tb_user_id()
    pdf_bytes, fname = InvoiceService.generate_invoice_pdf(invoice_id, contractor_id=uid)
    if not pdf_bytes:
        abort(404)
    return send_file(
        BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=fname,
    )


@public_bp.get("/settings/billing")
@staff_required_tb
def public_billing_settings_page():
    uid = current_tb_user_id()
    profile = InvoiceService.get_contractor_billing_profile(uid)
    suggested = InvoiceService.get_next_invoice_number(uid)
    return render_template(
        "public/settings/billing.html",
        profile=profile,
        suggested_next_invoice_number=suggested,
        config=core_manifest,
    )


@public_bp.post("/settings/billing")
@staff_required_tb
def public_billing_settings_save():
    uid = current_tb_user_id()
    InvoiceService.save_contractor_billing_profile(
        uid,
        {
            "invoice_business_name": request.form.get("invoice_business_name"),
            "invoice_address_line1": request.form.get("invoice_address_line1"),
            "invoice_address_line2": request.form.get("invoice_address_line2"),
            "invoice_city": request.form.get("invoice_city"),
            "invoice_postcode": request.form.get("invoice_postcode"),
            "invoice_country": request.form.get("invoice_country"),
        },
    )
    flash("Billing details saved. These appear on invoice PDFs.", "success")
    return redirect(url_for("public_time_billing.public_billing_settings_page"))


@public_bp.get("/api/refs/clients")
@staff_required_tb
def api_refs_clients():
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, name
            FROM clients
            WHERE active IN (1, '1', 'active', TRUE)
            ORDER BY name ASC
        """)
        items = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return jsonify({"items": items})


@public_bp.get("/api/refs/sites")
@staff_required_tb
def api_refs_sites():
    client_name = request.args.get("client_name")
    client_id = request.args.get("client_id", type=int)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if client_id:
            cur.execute("""
                SELECT id, name
                FROM sites
                WHERE active IN (1, '1', 'active', TRUE)
                  AND client_id = %s
                ORDER BY name ASC
            """, (client_id,))
        elif client_name:
            cur.execute("""
                SELECT id, name
                FROM sites
                WHERE active IN (1, '1', 'active', TRUE)
                  AND client_name = %s
                ORDER BY name ASC
            """, (client_name,))
        else:
            cur.execute("""
                SELECT id, name
                FROM sites
                WHERE active IN (1, '1', 'active', TRUE)
                ORDER BY name ASC
            """)
        items = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return jsonify({"items": items})


@public_bp.get("/api/refs/job-types")
@internal_bp.get("/api/refs/job-types")
def api_refs_job_types():
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, name, colour_hex
            FROM job_types
            WHERE active IN (1, '1', 'active', TRUE)
            ORDER BY name ASC
        """)
        items = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return jsonify({"items": items})


@internal_bp.get("/api/refs/clients")
@admin_required_tb
def api_admin_refs_clients():
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, name
            FROM clients
            WHERE active IN (1, '1', 'active', TRUE)
            ORDER BY name ASC
        """)
        items = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return jsonify({"items": items})


@internal_bp.get("/api/refs/sites")
@admin_required_tb
def api_admin_refs_sites():
    client_id = request.args.get("client_id", type=int)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if client_id:
            cur.execute("""
                SELECT id, name, client_id
                FROM sites
                WHERE active IN (1, '1', 'active', TRUE)
                  AND client_id = %s
                ORDER BY name ASC
            """, (client_id,))
        else:
            cur.execute("""
                SELECT id, name, client_id
                FROM sites
                WHERE active IN (1, '1', 'active', TRUE)
                ORDER BY name ASC
            """)
        items = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return jsonify({"items": items})


@internal_bp.get("/api/refs/contractors")
@admin_required_tb
def api_admin_refs_contractors():
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify({"items": []})
    like = f"%{q}%"
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, name, email, initials
            FROM tb_contractors
            WHERE status = 'active'
              AND (
                name LIKE %s OR email LIKE %s OR initials LIKE %s OR CAST(id AS CHAR) LIKE %s
              )
            ORDER BY name ASC
            LIMIT 30
        """, (like, like, like, like))
        items = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return jsonify({"items": items})


@public_bp.get("/api/my/runsheet-templates")
@staff_required_tb
def api_my_runsheet_templates_list():
    try:
        return jsonify({"items": TemplateService.list_active_runsheet_templates()})
    except Exception as e:
        return _tb_safe_api_error(e, status=500, log_message="list_active_runsheet_templates")


@public_bp.get("/api/my/runsheet-templates/<int:tpl_id>/schema")
@staff_required_tb
def api_my_runsheet_template_schema(tpl_id):
    try:
        return jsonify(TemplateService.render_form_schema(tpl_id))
    except Exception as e:
        current_app.logger.warning(
            "runsheets template schema failed tpl_id=%s: %s", tpl_id, e, exc_info=True
        )
        return jsonify({"error": "Template not found or unavailable."}), 404


@public_bp.get("/api/refs/contractors")
@staff_required_tb
def api_public_refs_contractors():
    """Contractor search for runsheet crew (field UI). Same rules as admin ref."""
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify({"items": []})
    like = f"%{q}%"
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, name, email, initials
            FROM tb_contractors
            WHERE status = 'active'
              AND (
                name LIKE %s OR email LIKE %s OR initials LIKE %s OR CAST(id AS CHAR) LIKE %s
              )
            ORDER BY name ASC
            LIMIT 30
            """,
            (like, like, like, like),
        )
        items = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return jsonify({"items": items})


@public_bp.get("/api/my/runsheets")
@staff_required_tb
def api_my_runsheets_list():
    uid = current_tb_user_id()
    if not uid:
        return jsonify({"items": []}), 401
    try:
        rows = RunsheetService.list_runsheets_for_contractor(int(uid))
        return jsonify({"items": rows})
    except Exception as e:
        return _tb_safe_api_error(e, status=500, log_message="api_my_runsheets_list")


@public_bp.get("/api/my/runsheets/<int:rs_id>")
@staff_required_tb
def api_my_runsheets_get(rs_id):
    uid = current_tb_user_id()
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    if not RunsheetService.contractor_can_access_runsheet(rs_id, int(uid)):
        return jsonify({"error": "Forbidden"}), 403
    try:
        rs = RunsheetService.get_runsheet(rs_id)
    except Exception as e:
        return _tb_safe_api_error(e, status=500, log_message="api_my_runsheets_get")
    if not rs:
        return jsonify({"error": "Not found"}), 404
    return jsonify(rs)


@public_bp.post("/api/my/runsheets")
@staff_required_tb
def api_my_runsheets_create():
    uid = current_tb_user_id()
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json(force=True) or {}
    data = dict(data)
    data["lead_user_id"] = int(uid)
    try:
        rs_id = RunsheetService.create_runsheet(data)
        RunsheetService.ensure_lead_assignment(rs_id, int(uid))
        return jsonify({"ok": True, "id": rs_id}), 201
    except Exception as e:
        current_app.logger.warning("api_my_runsheets_create: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 400


@public_bp.patch("/api/my/runsheets/<int:rs_id>/my-assignment")
@staff_required_tb
def api_my_runsheets_patch_my_assignment(rs_id):
    """Non-lead crew: save own actual times and notes only."""
    uid = current_tb_user_id()
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    if not RunsheetService.contractor_can_access_runsheet(rs_id, int(uid)):
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json(force=True) or {}
    try:
        RunsheetService.update_own_assignment_times(rs_id, int(uid), data)
        return jsonify({"ok": True})
    except Exception as e:
        current_app.logger.warning(
            "api_my_runsheets_patch_my_assignment rs_id=%s: %s", rs_id, e, exc_info=True
        )
        return jsonify({"ok": False, "error": str(e)}), 400


@public_bp.put("/api/my/runsheets/<int:rs_id>")
@staff_required_tb
def api_my_runsheets_update(rs_id):
    uid = current_tb_user_id()
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    if not RunsheetService.contractor_may_edit_runsheet_header(rs_id, int(uid)):
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json(force=True) or {}
    try:
        RunsheetService.update_runsheet(rs_id, data)
        return jsonify({"ok": True})
    except Exception as e:
        current_app.logger.warning(
            "api_my_runsheets_update rs_id=%s: %s", rs_id, e, exc_info=True
        )
        return jsonify({"ok": False, "error": str(e)}), 400


@public_bp.post("/api/my/runsheets/<int:rs_id>/publish")
@staff_required_tb
def api_my_runsheets_publish(rs_id):
    uid = current_tb_user_id()
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    if not RunsheetService.contractor_may_edit_runsheet_header(rs_id, int(uid)):
        return jsonify({"error": "Forbidden"}), 403
    try:
        res = RunsheetService.publish_runsheet(rs_id, published_by=int(uid))
        return jsonify(res), (200 if res.get("ok") else 400)
    except Exception as e:
        return _tb_safe_api_error(e, status=500, log_message="api_my_runsheets_publish")


@public_bp.post("/api/my/runsheets/<int:rs_id>/assignments/<int:ra_id>/withdraw")
@staff_required_tb
def api_my_runsheets_assignment_withdraw(rs_id, ra_id):
    uid = current_tb_user_id()
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    if not RunsheetService.contractor_can_access_runsheet(rs_id, int(uid)):
        return jsonify({"error": "Forbidden"}), 403
    try:
        res = RunsheetService.withdraw_runsheet_assignment(rs_id, ra_id, int(uid))
        return jsonify(res), (200 if res.get("ok") else 400)
    except Exception as e:
        return _tb_safe_api_error(e, status=500, log_message="assignment_withdraw")


@public_bp.post("/api/my/runsheets/<int:rs_id>/assignments/<int:ra_id>/reactivate")
@staff_required_tb
def api_my_runsheets_assignment_reactivate(rs_id, ra_id):
    uid = current_tb_user_id()
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    if not RunsheetService.contractor_can_access_runsheet(rs_id, int(uid)):
        return jsonify({"error": "Forbidden"}), 403
    try:
        res = RunsheetService.reactivate_runsheet_assignment(rs_id, ra_id, int(uid))
        return jsonify(res), (200 if res.get("ok") else 400)
    except Exception as e:
        return _tb_safe_api_error(e, status=500, log_message="assignment_reactivate")


@public_bp.get("/api/rates")
@staff_required_tb
def api_public_contractor_rate():
    """
    Effective £/h wage rate for the logged-in contractor, job type, and work date.
    Uses the same card lookup as saving a timesheet row (MinimalRateResolver).
    Query: job_type_id (int), on=YYYY-MM-DD
    """
    uid = current_tb_user_id()
    job_type_id = request.args.get("job_type_id", type=int)
    on_raw = (request.args.get("on") or "").strip()
    if not uid or not job_type_id or not on_raw:
        return jsonify({"ok": False, "rate": 0.0, "message": "job_type_id and on (YYYY-MM-DD) required"}), 400
    try:
        work_date = datetime.strptime(on_raw, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"ok": False, "rate": 0.0, "message": "invalid on date"}), 400
    rate = MinimalRateResolver.resolve_rate(uid, job_type_id, work_date)
    return jsonify({"ok": True, "rate": float(rate)})


# -------------------------
# Public Exports
# -------------------------


@public_bp.get("/api/weeks/<week_id>/export.csv")
@staff_required_tb
def api_public_export_csv(week_id):
    uid = current_tb_user_id()
    csv_bytes, fname = ExportService.export_week_csv(uid, week_id)
    return send_file(BytesIO(csv_bytes), mimetype="text/csv",
                     as_attachment=True, download_name=fname)


@public_bp.get("/api/weeks/<week_id>/export.pdf")
@staff_required_tb
def api_public_export_pdf(week_id):
    uid = current_tb_user_id()
    pdf_bytes, fname = ExportService.export_week_pdf(uid, week_id)
    return send_file(BytesIO(pdf_bytes), mimetype="application/pdf",
                     as_attachment=True, download_name=fname)


# =============================================================================
# Blueprint Registration Functions
# =============================================================================


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_bp
