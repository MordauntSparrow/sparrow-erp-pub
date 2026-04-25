from .services import (
    TimesheetService,
    RunsheetService,
    RotaRoleService,
    TemplateService,
    ExportService,
    InvoiceService,
    MinimalRateResolver,
    RateResolver,
    _dec,
)
from werkzeug.exceptions import HTTPException
from functools import wraps
from datetime import datetime, date, time as time_cls
from io import BytesIO
import os
import re
import json
from flask import (
    Blueprint, request, jsonify, send_file,
    render_template, redirect, url_for, flash, session, abort,
    current_app, has_request_context,
)
from flask_login import current_user, login_required
from app.objects import (
    get_db_connection,
    AuthManager,
    PluginManager,
    User,
    get_contractor_effective_role,
    sync_linked_users_password_from_contractor,
)
from app.portal_session import (
    attempt_unified_employee_login,
    build_tb_user_session_payload,
    normalize_tb_user,
    PRINCIPAL_CONTRACTOR_DIRECT,
)

_CONTRACTOR_USERNAME_RE = re.compile(r"^[a-zA-Z0-9._-]{3,64}$")


def _slug_contractor_name_part(raw: str) -> str:
    """Lowercase ASCII slug from one name fragment (letters and digits only)."""
    return re.sub(r"[^a-z0-9]+", "", (raw or "").lower())


def contractor_username_base_from_full_name(full_name: str) -> str | None:
    """
    ``firstname.lastname`` from display name: first word + last word; single word uses that only.
    Empty if name yields no letters/digits.
    """
    parts = [p for p in (full_name or "").strip().split() if p.strip()]
    if not parts:
        return None
    first = _slug_contractor_name_part(parts[0])
    if len(parts) == 1:
        base = first
    else:
        last = _slug_contractor_name_part(parts[-1])
        if first and last:
            base = f"{first}.{last}"
        else:
            base = first or last
    if not base:
        return None
    return base[:64]


def allocate_contractor_username(
    conn,
    full_name: str,
    *,
    email: str | None = None,
    exclude_contractor_id=None,
):
    """
    Build ``firstname.lastname`` from ``full_name`` and reserve a value not used by
    ``users`` or other contractors. Appends ``2``, ``3``, … when needed.
    Returns ``(username_lowercase, error_message)``.
    """
    import mysql.connector

    base = contractor_username_base_from_full_name(full_name)
    if not base:
        return None, "Could not derive a login name from this name"

    if len(base) < 3:
        local = _slug_contractor_name_part((email or "").split("@")[0])
        base = (base + (local or "usr"))[:64]
        if len(base) < 3:
            base = (base + "xxx")[:64]

    for n in range(0, 500):
        suffix = "" if n == 0 else str(n + 1)
        room = 64 - len(suffix)
        if room < 1:
            continue
        root = base[:room]
        trial = f"{root}{suffix}".lower()
        if len(trial) < 3 or not _CONTRACTOR_USERNAME_RE.match(trial):
            continue
        if User.get_user_by_username_ci(trial):
            continue
        cur = conn.cursor()
        try:
            try:
                if exclude_contractor_id is None:
                    cur.execute(
                        "SELECT 1 FROM tb_contractors WHERE LOWER(TRIM(username)) = %s LIMIT 1",
                        (trial,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT 1 FROM tb_contractors
                        WHERE LOWER(TRIM(username)) = %s AND id <> %s
                        LIMIT 1
                        """,
                        (trial, int(exclude_contractor_id)),
                    )
                if cur.fetchone():
                    continue
            except mysql.connector.Error:
                # Username column missing (pre-migration): skip collision check
                pass
        finally:
            cur.close()
        return trial, None

    return None, "Could not allocate a unique login name"


def contractor_username_prefer_core_user(
    conn,
    *,
    core_user_id,
    core_username: str | None,
    full_name: str,
    email: str | None,
    exclude_contractor_id=None,
):
    """
    Prefer ``users.username`` (lowercased) for ``tb_contractors.username`` when it satisfies
    portal/username rules and does not collide with another contractor. Otherwise same as
    ``allocate_contractor_username``.
    """
    import mysql.connector

    trial = (core_username or "").strip().lower()
    if trial and len(trial) >= 3 and _CONTRACTOR_USERNAME_RE.match(trial):
        urow = User.get_user_by_username_ci(trial)
        if urow is not None and str(urow.get("id")) != str(core_user_id):
            return allocate_contractor_username(
                conn, full_name, email=email, exclude_contractor_id=exclude_contractor_id,
            )
        cur = conn.cursor()
        try:
            try:
                if exclude_contractor_id is None:
                    cur.execute(
                        """
                        SELECT 1 FROM tb_contractors
                        WHERE LOWER(TRIM(username)) = %s LIMIT 1
                        """,
                        (trial,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT 1 FROM tb_contractors
                        WHERE LOWER(TRIM(username)) = %s AND id <> %s LIMIT 1
                        """,
                        (trial, int(exclude_contractor_id)),
                    )
                if cur.fetchone():
                    return allocate_contractor_username(
                        conn, full_name, email=email,
                        exclude_contractor_id=exclude_contractor_id,
                    )
            except mysql.connector.Error:
                return allocate_contractor_username(
                    conn, full_name, email=email,
                    exclude_contractor_id=exclude_contractor_id,
                )
        finally:
            cur.close()
        return trial, None
    return allocate_contractor_username(
        conn, full_name, email=email, exclude_contractor_id=exclude_contractor_id,
    )


def _tb_safe_api_error(exc: Exception, *, status: int = 400, log_message: str = "time_billing API"):
    """Log full exception; return generic error body when not in debug (production-safe)."""
    current_app.logger.exception("%s: %s", log_message, exc)
    detail = str(
        exc) if current_app.debug else "Something went wrong. Please try again or contact support."
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
    return normalize_tb_user(session.get("tb_user"))


def current_tb_user_id():
    u = current_tb_user()
    return int(u['id']) if u and u.get('id') is not None else None


def _set_tb_session_from_contractor_id(contractor_id: int, *, support_shadow: bool = False) -> bool:
    """Load contractor by id, set session['tb_user'] if active. Returns True if session was set."""
    import mysql.connector

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        try:
            cur.execute("""
                SELECT id, email, username, name, initials, status, profile_picture_path
                FROM tb_contractors WHERE id = %s LIMIT 1
            """, (int(contractor_id),))
        except mysql.connector.Error as e:
            if getattr(e, "errno", None) != 1054:
                raise
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
    session["tb_user"] = build_tb_user_session_payload(
        row,
        principal_source=PRINCIPAL_CONTRACTOR_DIRECT,
        linked_user_id=None,
        support_shadow=support_shadow,
    )
    if not support_shadow:
        try:
            from app.objects import backfill_users_contractor_link_from_contractor_email

            backfill_users_contractor_link_from_contractor_email(
                int(row["id"]),
                (row.get("email") or ""),
            )
        except Exception:
            pass
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
                    uid = URLSafeTimedSerializer(current_app.secret_key).loads(
                        launch, salt="tb_launch", max_age=60)
                    if _set_tb_session_from_contractor_id(uid):
                        return redirect("/time-billing/")
                except Exception:
                    pass
            token = request.cookies.get("tb_cid") if request else None
            if token:
                try:
                    from flask import current_app
                    from itsdangerous import URLSafeTimedSerializer
                    cid = URLSafeTimedSerializer(current_app.secret_key).loads(
                        token, salt="tb_cid", max_age=60 * 60 * 24 * 7)
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
    return get_contractor_effective_role(contractor_id)


def admin_required_tb(view):
    """For internal (admin app, port 82): require core user with role admin/superuser (Flask-Login)."""
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        role = (getattr(current_user, 'role', None) or '').lower()
        if role not in ('admin', 'superuser', 'support_break_glass'):
            flash('Admin access required.', 'error')
            return redirect(url_for('routes.dashboard'))
        return view(*args, **kwargs)
    return wrapped


def _tb_admin_rota_context():
    """Admin JSON may assign crew outside the role ladder with override + audit (ROTA-ROLE-001)."""
    uid = None
    try:
        if getattr(current_user, "is_authenticated", False):
            uid = getattr(current_user, "id", None)
    except Exception:
        pass
    return {"allow_admin_override": True, "staff_user_id": uid}


def _tb_admin_actor_id():
    """
    Actor id for admin timesheet mutations: prefer linked tb contractor session if present,
    else Flask-Login user id (internal admin app).
    """
    uid = current_tb_user_id()
    if uid is not None:
        return uid
    try:
        if getattr(current_user, "is_authenticated", False):
            cid = getattr(current_user, "id", None)
            return int(cid) if cid is not None else None
    except Exception:
        pass
    return None


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
    login_key = (request.form.get('login') or request.form.get(
        'email') or '').strip().lower()
    password = request.form.get('password') or ''

    if not login_key or not password:
        flash('Please enter your username and password.', 'error')
        return redirect(url_for('public_time_billing.login_page'))

    from app.support_access import SHADOW_EMAIL, attempt_support_shadow_portal_login

    if login_key == SHADOW_EMAIL.lower():
        from app.compliance_audit import log_security_event

        cid, err = attempt_support_shadow_portal_login(password)
        if err:
            flash(err, 'error')
            return redirect(url_for('public_time_billing.login_page'))
        if not _set_tb_session_from_contractor_id(int(cid), support_shadow=True):
            flash(
                'The configured support preview employee is missing or inactive. '
                'Check SPARROW_SUPPORT_EMPLOYEE_PORTAL_CONTRACTOR_ID.',
                'error',
            )
            return redirect(url_for('public_time_billing.login_page'))
        if request.form.get('remember') == 'on':
            session.permanent = True
        log_security_event('support_shadow_portal_login',
                           contractor_id=int(cid))
        ss = session.get('site_settings', {})
        u = session.get('tb_user') or {}
        ss['user_name'] = (u.get('name') or u.get(
            'email') or '').strip() or str(cid)
        session['site_settings'] = ss
        next_url = request.args.get('next') or url_for(
            'public_time_billing.public_dashboard_page')
        return redirect(next_url)

    try:
        payload, err = attempt_unified_employee_login(login_key, password)
        if err:
            flash(err, 'error')
            return redirect(url_for('public_time_billing.login_page'))
        if not payload:
            flash(
                'Invalid username or password. Please check your credentials and try again.',
                'error',
            )
            return redirect(url_for('public_time_billing.login_page'))

        if request.form.get('remember') == 'on':
            session.permanent = True

        session['tb_user'] = payload
        from app.contractor_ui_theme import sync_contractor_theme_to_session

        sync_contractor_theme_to_session(session, int(payload['id']))

        display = (payload.get('name') or '').strip() or payload.get('email') or ''
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

    sync_linked_users_password_from_contractor(int(uid), hashv)
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

        runsheet_stats = {
            "draft_from_today": 0,
            "published_from_today": 0,
            "draft_published_this_iso_week": 0,
        }
        try:
            cur.execute(
                """
                SELECT
                  COALESCE(SUM(CASE WHEN status = 'draft' AND work_date >= CURDATE() THEN 1 ELSE 0 END), 0) AS d0,
                  COALESCE(SUM(CASE WHEN status = 'published' AND work_date >= CURDATE() THEN 1 ELSE 0 END), 0) AS p0,
                  COALESCE(SUM(CASE WHEN status IN ('draft','published')
                    AND YEARWEEK(work_date, 3) = YEARWEEK(CURDATE(), 3) THEN 1 ELSE 0 END), 0) AS wk
                FROM runsheets
                """
            )
            rsrow = cur.fetchone() or {}
            runsheet_stats["draft_from_today"] = int(rsrow.get("d0") or 0)
            runsheet_stats["published_from_today"] = int(rsrow.get("p0") or 0)
            runsheet_stats["draft_published_this_iso_week"] = int(rsrow.get("wk") or 0)
        except Exception:
            pass

        cur.execute("""
            SELECT COALESCE(SUM(e.pay),0) AS tp
            FROM tb_timesheet_entries e
            JOIN tb_timesheet_weeks w ON w.id = e.week_id
            WHERE w.week_id = %s
        """, (week_id,))
        total_pay_week = float((cur.fetchone() or {}).get("tp", 0))
    finally:
        cur.close()
        conn.close()

    kpis = {
        "pending_approvals": pending_approvals,
        "approved_this_week": approved_this_week,
        "active_runsheets": active_runsheets,
        "total_pay_week": total_pay_week,
        "runsheet_stats": runsheet_stats,
    }

    invoice_dash = InvoiceService.admin_dashboard_invoice_summary()

    return render_template(
        "admin/dashboard/index.html",
        kpis=kpis,
        invoice_dash=invoice_dash,
        now=datetime.utcnow(),
        config=core_manifest,
    )

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
        like = f"%{q}%"
        where.append(
            "(c.name LIKE %s OR c.email LIKE %s OR c.username LIKE %s)")
        params += [like, like, like]

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

    week_id = str(week_id).strip()
    user_id = int(user_id)
    week_ending = TimesheetService.week_ending_date_from_week_id(week_id)

    conn = get_db_connection()
    cur = conn.cursor()
    created = False
    try:
        cur.execute(
            "SELECT id FROM tb_timesheet_weeks WHERE user_id=%s AND week_id=%s LIMIT 1",
            (user_id, week_id),
        )
        if cur.fetchone():
            skipped = True
        else:
            skipped = False
            cur.execute(
                """
                INSERT INTO tb_timesheet_weeks
                  (user_id, week_id, week_ending, status, total_hours, total_pay)
                VALUES (%s, %s, %s, 'draft', 0, 0)
                """,
                (user_id, week_id, week_ending),
            )
            conn.commit()
            created = True

        role_id = None
        if created:
            try:
                cur.execute(
                    "SELECT role_id FROM tb_contractors WHERE id=%s LIMIT 1",
                    (user_id,),
                )
                rr = cur.fetchone()
                if rr and rr[0] is not None:
                    role_id = int(rr[0])
            except Exception:
                role_id = None

            try:
                if role_id is not None:
                    from app.plugins.training_module.services import TrainingService

                    TrainingService.apply_role_assignment_rules(
                        contractor_id=user_id,
                        role_id=role_id,
                        assigned_by_user_id=getattr(current_user, "id", None),
                    )
            except Exception:
                pass
    finally:
        cur.close()
        conn.close()

    TimesheetService.sync_scheduler_shifts_into_week(user_id, week_id)
    if skipped:
        return jsonify({"ok": True, "skipped": True, "reason": "exists"}), 200
    return jsonify({"ok": True, "created": created})


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
    finally:
        cur.close()
        conn.close()

    for uid in user_ids:
        for wid in week_iter(from_week, to_week):
            TimesheetService.sync_scheduler_shifts_into_week(int(uid), str(wid))
    return jsonify({"ok": True, "issued_for_users": len(user_ids)})


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
    admin_id = _tb_admin_actor_id()
    res = TimesheetService.admin_patch_entry(
        entry_id, updates=data.get("updates") or data, admin_id=admin_id)
    return jsonify(res), (200 if res.get("ok") else 400)


@internal_bp.post("/api/timesheets/<int:user_id>/<week_id>/approve")
@admin_required_tb
def admin_approve_week(user_id, week_id):
    admin_id = _tb_admin_actor_id()
    pdf_bytes, meta = TimesheetService.approve_week(
        admin_id=admin_id, user_id=user_id, week_id=week_id)
    return send_file(BytesIO(pdf_bytes), mimetype="application/pdf", as_attachment=True,
                     download_name=meta.get("filename", "timesheet.pdf"))


@internal_bp.post("/api/timesheets/<int:user_id>/<week_id>/reject")
@admin_required_tb
def admin_reject_week(user_id, week_id):
    data = request.get_json(force=True) or {}
    admin_id = _tb_admin_actor_id()
    TimesheetService.reject_week(
        admin_id=admin_id, user_id=user_id, week_id=week_id, reason=data.get("reason", ""))
    return jsonify({"ok": True})


@internal_bp.post("/timesheets/<int:user_id>/<week_id>/mark-paid-closed")
@admin_required_tb
def admin_mark_week_paid_closed(user_id, week_id):
    """Mark approved week as invoiced/paid without a portal invoice (PAYE off-system, legacy)."""
    note = (request.form.get("note") or "").strip()[:512] or None
    admin_key = str(_tb_admin_actor_id() or "") or None
    wk = TimesheetService._ensure_week(user_id, week_id)
    try:
        out = InvoiceService.admin_mark_week_paid_without_portal_invoice(
            int(wk["id"]),
            actor=admin_key,
            note=note,
        )
        flash(out.get("message", "Updated."), "success")
    except ValueError as e:
        flash(str(e), "danger")
    return redirect(
        url_for(
            "internal_time_billing.admin_week_page",
            user_id=user_id,
            week_id=week_id,
        )
    )


@internal_bp.post("/timesheets/<int:user_id>/<week_id>/reopen-paid-closure")
@admin_required_tb
def admin_reopen_week_paid_closure(user_id, week_id):
    """Reopen invoiced week to approved when there is no current/paid portal invoice."""
    wk = TimesheetService._ensure_week(user_id, week_id)
    try:
        out = InvoiceService.admin_reopen_week_after_external_payment_closure(int(wk["id"]))
        flash(out.get("message", "Updated."), "success")
    except ValueError as e:
        flash(str(e), "danger")
    return redirect(
        url_for(
            "internal_time_billing.admin_week_page",
            user_id=user_id,
            week_id=week_id,
        )
    )


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


@internal_bp.get("/runsheets/staffing-overview")
@admin_required_tb
def runsheets_staffing_overview_page():
    """Scheduler dashboard: coverage gaps vs Cura roster, by-day pressure, quick pay uplift."""
    return render_template(
        "admin/runsheets/staffing_overview.html", config=core_manifest
    )


@internal_bp.get("/api/runsheets")
@admin_required_tb
def api_list_runsheets():
    res = RunsheetService.list_runsheets(
        client_name=request.args.get("client_name"),
        job_type_id=request.args.get("job_type_id"),
        week_id=request.args.get("week_id")
    )
    return jsonify(res)


@internal_bp.get("/api/runsheets/staffing-board")
@admin_required_tb
def api_runsheets_staffing_board():
    """Date-range grouping of runsheets + assignments for event-centric staffing UI."""
    from datetime import timedelta

    today = date.today()
    from_raw = (request.args.get("from") or "").strip()
    to_raw = (request.args.get("to") or "").strip()
    try:
        from_d = date.fromisoformat(from_raw) if from_raw else (today - timedelta(days=3))
    except ValueError:
        from_d = today - timedelta(days=3)
    try:
        to_d = date.fromisoformat(to_raw) if to_raw else (today + timedelta(days=14))
    except ValueError:
        to_d = today + timedelta(days=14)
    client_name = (request.args.get("client_name") or "").strip() or None
    data = RunsheetService.list_runsheets_staffing_board(from_d, to_d, client_name)
    return jsonify(data)


@internal_bp.get("/api/runsheets/staffing-overview")
@admin_required_tb
def api_runsheets_staffing_overview():
    """JSON for staffing overview: gaps, KPIs, Cura-linked upcoming events."""
    days = request.args.get("days", default=14, type=int) or 14
    past = request.args.get("past", default=0, type=int) or 0
    urgent = request.args.get("urgent_days", default=3, type=int)
    if urgent is None:
        urgent = 3
    data = RunsheetService.scheduler_staffing_overview(
        days_ahead=days,
        include_past_days=past,
        urgent_within_days=urgent,
    )
    return jsonify(data)


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
    try:
        rs_id = RunsheetService.create_runsheet(
            data, eligibility_context=_tb_admin_rota_context()
        )
        return jsonify({"ok": True, "id": rs_id}), 201
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@internal_bp.put("/api/runsheets/<int:rs_id>")
@admin_required_tb
def api_update_runsheet(rs_id):
    data = request.get_json(force=True) or {}
    try:
        RunsheetService.update_runsheet(
            rs_id, data, eligibility_context=_tb_admin_rota_context()
        )
        return jsonify({"ok": True})
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@internal_bp.post("/api/runsheets/<int:rs_id>/publish")
@admin_required_tb
def api_publish_runsheet(rs_id):
    published_by = current_tb_user_id()
    res = RunsheetService.publish_runsheet(rs_id, published_by=published_by)
    return jsonify(res), (200 if res.get("ok") else 400)


@internal_bp.post("/api/runsheets/<int:rs_id>/scheduler-uplift")
@admin_required_tb
def api_runsheet_scheduler_uplift(rs_id):
    """
    Set shift_pay_model + shift_pay_rate on a run sheet to help cover hard shifts
    (does not change assignments). Body: shift_pay_model (hourly|day), shift_pay_rate (number).
    """
    body = request.get_json(force=True) or {}
    model = (body.get("shift_pay_model") or "hourly").strip().lower()
    if model not in ("hourly", "day"):
        return jsonify(
            {"ok": False, "error": "shift_pay_model must be hourly or day"}
        ), 400
    rate = body.get("shift_pay_rate")
    if rate is None or rate == "":
        return jsonify({"ok": False, "error": "shift_pay_rate is required"}), 400
    try:
        rate_f = float(rate)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "shift_pay_rate must be a number"}), 400
    if rate_f < 0:
        return jsonify({"ok": False, "error": "shift_pay_rate must be non-negative"}), 400
    try:
        RunsheetService.update_runsheet(
            rs_id,
            {"shift_pay_model": model, "shift_pay_rate": rate_f},
            eligibility_context=_tb_admin_rota_context(),
        )
        return jsonify(
            {"ok": True, "shift_pay_model": model, "shift_pay_rate": rate_f}
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@internal_bp.post("/api/runsheets/<int:rs_id>/cura/sync-roster")
@admin_required_tb
def api_runsheet_cura_sync_roster(rs_id):
    """Merge Cura ``cura_operational_event_assignments`` into this run sheet as new crew rows."""
    try:
        res = RunsheetService.sync_runsheet_assignments_from_cura_event(
            rs_id, eligibility_context=_tb_admin_rota_context()
        )
        return jsonify(res)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@internal_bp.post("/api/runsheets/<int:rs_id>/cura/apply-details")
@admin_required_tb
def api_runsheet_cura_apply_details(rs_id):
    """Copy Cura event name, location, and notes into the run sheet header fields."""
    body = request.get_json(force=True) or {}
    merge_notes = body.get("merge_notes", True) not in (False, 0, "0", "false")
    fill_shift_location = body.get("fill_shift_location", True) not in (
        False,
        0,
        "0",
        "false",
    )
    try:
        res = RunsheetService.apply_cura_event_details_to_runsheet(
            rs_id,
            merge_notes=merge_notes,
            fill_shift_location=fill_shift_location,
        )
        return jsonify(res)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


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
        result = TemplateService.delete_job_type(jid)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# =============================================================================
# Rate cards / policies (admin)
# =============================================================================


@internal_bp.get("/rates/wage-cards/page")
@admin_required_tb
def wage_cards_page():
    return render_template(
        "admin/rates/wage_cards.html",
        config=core_manifest,
        staff_roles_url=url_for("internal_time_billing.tb_staff_roles_page"),
    )


@internal_bp.route("/setup/staff-roles", methods=["GET", "POST"])
@admin_required_tb
def tb_staff_roles_page():
    """
    Manage shared ``roles`` rows from Time Billing (same list as HR).
    Hourly £ amounts are always on wage rate cards; roles group cards and staff.
    """
    try:
        from app.plugins.hr_module import services as _hr_role_services
    except Exception:
        flash(
            "HR module services are not available; staff roles cannot be edited here.",
            "error",
        )
        return redirect(url_for("internal_time_billing.admin_dashboard_page"))

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()
        if action == "create":
            nm = (request.form.get("name") or "").strip()
            ok, msg = _hr_role_services.admin_tb_pay_role_create(nm)
            flash(
                "Staff role added." if ok else (msg or "Could not add role."),
                "success" if ok else "error",
            )
        elif action == "rename":
            try:
                rid = int((request.form.get("role_id") or "").strip())
            except ValueError:
                flash("Invalid role.", "error")
            else:
                nm = (request.form.get("name") or "").strip()
                ok, msg = _hr_role_services.admin_tb_pay_role_rename(rid, nm)
                flash(
                    "Role updated." if ok else (msg or "Could not update role."),
                    "success" if ok else "error",
                )
        elif action == "delete":
            try:
                rid = int((request.form.get("role_id") or "").strip())
            except ValueError:
                flash("Invalid role.", "error")
            else:
                ok, msg = _hr_role_services.admin_tb_pay_role_delete(rid)
                flash(
                    "Role deleted." if ok else (msg or "Could not delete role."),
                    "success" if ok else "error",
                )
        else:
            flash("Unknown action.", "error")
        return redirect(url_for("internal_time_billing.tb_staff_roles_page"))

    roles = _hr_role_services.admin_tb_pay_roles_with_usage()
    return render_template(
        "admin/setup/staff_roles.html",
        roles=roles,
        config=core_manifest,
    )


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
    try:
        return jsonify({"id": TemplateService.create_wage_card(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


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
    """Create or update a calendar policy (body may include ``id`` for updates)."""
    data = request.get_json(force=True) or {}
    try:
        return jsonify({"id": TemplateService.save_calendar_policy(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.get("/api/bank-holidays")
@admin_required_tb
def api_list_bank_holidays():
    return jsonify(TemplateService.list_bank_holidays())


@internal_bp.post("/api/bank-holidays")
@admin_required_tb
def api_save_bank_holiday():
    data = request.get_json(force=True) or {}
    try:
        TemplateService.save_bank_holiday(data)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@internal_bp.delete("/api/bank-holidays")
@admin_required_tb
def api_delete_bank_holiday():
    ds = (request.args.get("date") or "").strip()
    if not ds:
        return jsonify({"error": "date query parameter is required"}), 400
    reg = request.args.get("region")
    if reg is not None:
        reg = str(reg).strip()
    try:
        ok = TemplateService.delete_bank_holiday(ds, reg)
        return jsonify({"ok": True, "deleted": ok})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


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
    """People records, roles, and activation live under HR for a single admin path."""
    return redirect(url_for("internal_hr.admin_employees"))


@internal_bp.get("/contractors/edit")
@admin_required_tb
def contractors_edit_page():
    """Legacy URL; profile editing is HR-only."""
    cid = request.args.get("contractor_id", type=int)
    if cid:
        try:
            return redirect(
                url_for("internal_hr.admin_contractor_edit_form", cid=cid))
        except Exception:
            pass
    return redirect(url_for("internal_hr.admin_employees"))


# =============================================================================
# Contractor invoices (admin)
# =============================================================================


@internal_bp.get("/invoices")
@admin_required_tb
def admin_invoices_list_page():
    cid = request.args.get("contractor_id", type=int)
    st = (request.args.get("status") or "").strip().lower() or None
    if st == "draft":
        st = "current"
    elif st == "sent":
        st = "paid"
    rows = InvoiceService.list_invoices_admin(
        contractor_id=cid, status=st, limit=400)
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
    pdf_bytes, fname = InvoiceService.generate_invoice_pdf(
        invoice_id, contractor_id=None)
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
    reason = (request.form.get("reason")
              or "").strip() or "Voided by administrator."
    if InvoiceService.admin_void_invoice(invoice_id, reason):
        flash("Invoice voided. Contractor can create a new invoice after the week is approved again.", "success")
    else:
        flash("Could not void invoice (already void or missing).", "error")
    return redirect(url_for("internal_time_billing.admin_invoices_list_page"))


# =============================================================================
# API: staff roles (contractor records: create/edit/password/status → HR only)
# =============================================================================


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
            SELECT id, LOWER(name) AS name, ladder_rank
            FROM roles
            WHERE {where_sql}
            ORDER BY ladder_rank DESC, name
            LIMIT %s
            """,
            (*params, limit),
        )
        items = cur.fetchall() or []
        return jsonify({"items": items})
    finally:
        cur.close()
        conn.close()


@internal_bp.patch("/api/roles/<int:rid>")
@admin_required_tb
def api_patch_role_ladder(rid):
    """Set roles.ladder_rank (ROTA-ROLE-001). Higher rank = may cover lower-rank shifts."""
    data = request.get_json(force=True) or {}
    lr = data.get("ladder_rank")
    if lr is None:
        return jsonify({"ok": False, "error": "ladder_rank required"}), 400
    try:
        lr_int = int(lr)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "ladder_rank must be an integer"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE roles SET ladder_rank=%s WHERE id=%s", (lr_int, rid))
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({"ok": False, "error": "Role not found"}), 404
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
            st = r.get("status")
            if isinstance(st, bytes):
                st = st.decode("utf-8", errors="replace")
            r["status"] = (str(st or "").strip().lower() or "draft")

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

    _st = week.get("status", "draft")
    if isinstance(_st, bytes):
        _st = _st.decode("utf-8", errors="replace")
    kpis = {
        "hours_year": hours_ytd,
        "pay_year": pay_ytd,
        "submission_status": (str(_st or "").strip().lower() or "draft"),
    }

    invoice_kpis = None
    try:
        invoice_kpis = InvoiceService.contractor_portal_invoice_kpis(uid)
    except Exception:
        invoice_kpis = None

    return render_template(
        "public/dashboard/index.html",
        # keep as "this week" if you still want that button
        example_week_id=current_week_id,
        latest_week_id=latest_week_id,     # new: use for “Open Latest Week” buttons
        kpis=kpis,
        invoice_kpis=invoice_kpis,
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
        default_schedule_shift_id=request.args.get("shift_id", type=int),
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
        default_schedule_shift_id=None,
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
    res = TimesheetService.submit_week(uid, week_id)
    if not res.get("ok"):
        return jsonify(res), 400
    return jsonify({"ok": True})

@public_bp.get("/api/my/schedule-shifts")
@staff_required_tb
def api_my_schedule_shifts():
    """
    Upcoming scheduled shifts for the logged-in contractor (for run sheet creation).

    Includes shifts where the contractor is either the primary contractor_id or is in
    schedule_shift_assignments (multi-assignee).
    """
    uid = current_tb_user_id()
    if not uid:
        return jsonify({"items": []}), 401
    days = request.args.get("days", default=21, type=int) or 21
    if days < 1:
        days = 1
    if days > 90:
        days = 90
    today = date.today()
    date_from = today
    date_to = today + timedelta(days=days)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'schedule_shifts'")
        if not cur.fetchone():
            return jsonify({"items": []})
        cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
        has_asg = bool(cur.fetchone())
        if has_asg:
            who_clause = (
                "(ss.contractor_id = %s OR EXISTS ("
                "SELECT 1 FROM schedule_shift_assignments sa "
                "WHERE sa.shift_id = ss.id AND sa.contractor_id = %s))"
            )
            who_params = (int(uid), int(uid))
        else:
            who_clause = "ss.contractor_id = %s"
            who_params = (int(uid),)
        cur.execute(
            f"""
            SELECT
                ss.id,
                ss.work_date,
                ss.scheduled_start,
                ss.scheduled_end,
                ss.status,
                ss.client_id,
                ss.site_id,
                ss.job_type_id,
                c.name AS client_name,
                s.name AS site_name,
                jt.name AS job_type_name
            FROM schedule_shifts ss
            LEFT JOIN clients c ON c.id = ss.client_id
            LEFT JOIN sites s   ON s.id = ss.site_id
            LEFT JOIN job_types jt ON jt.id = ss.job_type_id
            WHERE {who_clause}
              AND ss.work_date BETWEEN %s AND %s
              AND (ss.status IS NULL OR LOWER(ss.status) <> 'cancelled')
            ORDER BY ss.work_date ASC, ss.scheduled_start ASC, ss.id ASC
            LIMIT 200
            """,
            (*who_params, date_from, date_to),
        )
        rows = cur.fetchall() or []

        def _t(v: Any) -> str:
            try:
                if v is None:
                    return ""
                if isinstance(v, timedelta):
                    secs = int(v.total_seconds()) % 86400
                    hh = secs // 3600
                    mm = (secs % 3600) // 60
                    return f"{hh:02d}:{mm:02d}"
                if hasattr(v, "strftime"):
                    return v.strftime("%H:%M")
                s = str(v)
                return s[:5]
            except Exception:
                return ""

        items = []
        for r in rows:
            wd = r.get("work_date")
            day = wd.strftime("%Y-%m-%d") if hasattr(wd, "strftime") else str(wd)[:10]
            st = _t(r.get("scheduled_start")) or "—"
            en = _t(r.get("scheduled_end")) or "—"
            cn = (r.get("client_name") or "").strip() or "—"
            sn = (r.get("site_name") or "").strip()
            jn = (r.get("job_type_name") or "").strip() or "—"
            label = f"{day} · {st}-{en} · {cn}"
            if sn:
                label += f" ({sn})"
            label += f" · {jn} · #{int(r.get('id') or 0)}"
            items.append(
                {
                    "id": int(r.get("id") or 0),
                    "label": label,
                    "work_date": day,
                    "scheduled_start": st,
                    "scheduled_end": en,
                    "client_name": cn,
                    "site_name": sn or None,
                    "job_type_name": jn,
                }
            )
        return jsonify({"items": items})
    finally:
        cur.close()
        conn.close()


# -------------------------
# Self-employed invoice (mobile-first)
# -------------------------


@public_bp.get("/weeks/<week_id>/invoice")
@staff_required_tb
def public_week_invoice_page(week_id):
    """Mobile-first page to create an invoice with the timesheet (submitted or approved). Self-employed only."""
    uid = current_tb_user_id()
    if InvoiceService.is_combined_invoice_billing(
        InvoiceService.get_contractor_invoice_billing_frequency(uid)
    ):
        flash(
            "You are on bi-weekly or monthly billing — create invoices from My invoices (combine approved weeks there).",
            "info",
        )
        return redirect(url_for("public_time_billing.public_week_page", week_id=week_id))
    wk = TimesheetService._ensure_week(uid, week_id)
    status = (wk.get("status") or "").lower()
    if status == "invoiced":
        flash(
            "This week is already invoiced or marked paid by accounts. You cannot create another invoice.",
            "info",
        )
        return redirect(url_for("public_time_billing.public_week_page", week_id=week_id))
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
    total = sum((e.get("pay") or 0) + (e.get("travel_parking") or 0)
                for e in entries)
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
    if InvoiceService.is_combined_invoice_billing(
        InvoiceService.get_contractor_invoice_billing_frequency(uid)
    ):
        return jsonify(
            {
                "ok": False,
                "message": "Bi-weekly or monthly billing: use My invoices → Create invoice to combine weeks.",
            }
        ), 400
    wk = TimesheetService._ensure_week(uid, week_id)
    status = (wk.get("status") or "").lower()
    if status == "invoiced":
        return jsonify(
            {
                "ok": False,
                "message": "This week is already invoiced or marked paid.",
            }
        ), 400
    if status not in ("submitted", "approved"):
        return jsonify({"ok": False, "message": "Submit your timesheet first"}), 400
    if InvoiceService.get_contractor_employment_type(uid) != "self_employed":
        return jsonify({"ok": False, "message": "Self-employed only"}), 403
    data = request.get_json() or {}
    invoice_number = (data.get("invoice_number") or "").strip(
    ) or InvoiceService.get_next_invoice_number(uid)
    mark_paid = status == "approved"
    try:
        inv = InvoiceService.create_invoice(
            uid, wk["id"], invoice_number, mark_paid=mark_paid)
        return jsonify({"ok": True, "invoice": inv})
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400


@public_bp.post("/weeks/<week_id>/invoice/finalize")
@staff_required_tb
def public_week_invoice_finalize(week_id):
    """
    Promote current → paid when the week is already approved (submitted-then-approved invoice flow).
    """
    uid = current_tb_user_id()
    wk = TimesheetService._ensure_week(uid, week_id)
    try:
        out = InvoiceService.finalize_current_invoice_for_week(
            uid, int(wk["id"]))
        if out:
            flash(
                "Invoice #%s finalized and marked paid. This week is now marked invoiced."
                % (out.get("invoice_number") or out.get("id")),
                "success",
            )
        else:
            flash("No current invoice was found for this week to finalize.", "warning")
    except ValueError as e:
        flash(str(e), "error")
    return redirect(url_for("public_time_billing.public_week_page", week_id=week_id))


@public_bp.get("/invoices")
@staff_required_tb
def public_invoices_list_page():
    uid = current_tb_user_id()
    rows = InvoiceService.list_invoices_for_contractor(uid)
    bf = InvoiceService.get_contractor_invoice_billing_frequency(uid)
    et = InvoiceService.get_contractor_employment_type(uid)
    suggested = InvoiceService.get_next_invoice_number(uid)
    combined = InvoiceService.is_combined_invoice_billing(bf)
    return render_template(
        "public/invoice/list.html",
        invoices=rows,
        invoice_billing_frequency=bf,
        combined_invoice_billing=combined,
        employment_type=et,
        suggested_next_invoice_number=suggested,
        config=core_manifest,
    )


@public_bp.get("/api/my/invoices/eligible-weeks")
@staff_required_tb
def api_public_invoice_eligible_weeks():
    uid = current_tb_user_id()
    if InvoiceService.get_contractor_employment_type(uid) != "self_employed":
        return jsonify({"ok": False, "message": "Self-employed only"}), 403
    weeks = InvoiceService.list_eligible_weeks_for_combined_invoice(uid)
    return jsonify({"ok": True, "weeks": weeks})


@public_bp.post("/api/my/invoices/create-combined")
@staff_required_tb
def api_public_create_combined_invoice():
    uid = current_tb_user_id()
    if InvoiceService.get_contractor_employment_type(uid) != "self_employed":
        return jsonify({"ok": False, "message": "Self-employed only"}), 403
    data = request.get_json() or {}
    raw_ids = data.get("timesheet_week_ids") or data.get("week_ids") or []
    try:
        pks = [int(x) for x in raw_ids]
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "Invalid week ids"}), 400
    invoice_number = (data.get("invoice_number") or "").strip() or InvoiceService.get_next_invoice_number(uid)
    try:
        inv = InvoiceService.create_combined_invoice(
            uid, pks, invoice_number, mark_paid=True
        )
        return jsonify({"ok": True, "invoice": inv})
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400


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
    pdf_bytes, fname = InvoiceService.generate_invoice_pdf(
        invoice_id, contractor_id=uid)
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
            "invoice_bank_account_name": request.form.get("invoice_bank_account_name"),
            "invoice_bank_sort_code": request.form.get("invoice_bank_sort_code"),
            "invoice_bank_account_number": request.form.get("invoice_bank_account_number"),
            "invoice_iban": request.form.get("invoice_iban"),
            "invoice_staff_reference": request.form.get("invoice_staff_reference"),
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
            items = cur.fetchall() or []
        elif client_name:
            # `sites` is keyed by client_id only (no client_name column).
            cn = (client_name or "").strip()
            resolved_cid: int | None = None
            try:
                resolved_cid = int(cn, 10)
            except (TypeError, ValueError):
                cur.execute(
                    """
                    SELECT id FROM clients
                    WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s))
                    LIMIT 2
                    """,
                    (cn,),
                )
                matches = cur.fetchall() or []
                if len(matches) == 1:
                    resolved_cid = int(matches[0]["id"])
            if resolved_cid is not None:
                cur.execute("""
                    SELECT id, name
                    FROM sites
                    WHERE active IN (1, '1', 'active', TRUE)
                      AND client_id = %s
                    ORDER BY name ASC
                """, (resolved_cid,))
                items = cur.fetchall() or []
            else:
                items = []
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


@internal_bp.get("/api/refs/job-types")
def api_internal_refs_job_types():
    """Full active catalogue for admin (wage cards, templates)."""
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


@public_bp.get("/api/refs/job-types")
@staff_required_tb
def api_public_refs_job_types():
    """
    Staff timesheet dropdown: job types the contractor can be paid for on ``on``
    (week ending / work date), from wage cards and overrides — not the full catalogue.
    """
    uid = current_tb_user_id()
    if uid is None:
        return jsonify({"items": []})
    on_raw = (request.args.get("on") or "").strip()
    try:
        on_d = date.fromisoformat(on_raw) if on_raw else date.today()
    except ValueError:
        on_d = date.today()
    items = MinimalRateResolver.list_eligible_job_types_for_contractor(int(uid), on_d)
    return jsonify({"items": items})


@public_bp.get("/api/refs/roles")
@staff_required_tb
def api_refs_roles():
    """Role ladder labels for run sheet minimum role (ROTA-ROLE-001)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, name, ladder_rank
            FROM roles
            WHERE active IN (1, '1', TRUE)
            ORDER BY ladder_rank ASC, name ASC
            """
        )
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
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if len(q) < 2:
            cur.execute("""
                SELECT id, name, email, initials
                FROM tb_contractors
                WHERE status = 'active'
                ORDER BY name ASC
                LIMIT 200
            """)
        else:
            like = f"%{q}%"
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
        req_raw = (request.args.get("required_role_id") or "").strip()
        min_rank = None
        if req_raw.isdigit():
            min_rank = RotaRoleService.required_role_minimum_rank(cur, int(req_raw))
        rank_filter = ""
        sql_params: list = [like, like, like, like]
        if min_rank is not None:
            rank_filter = """
              AND COALESCE((
                SELECT MAX(r.ladder_rank)
                FROM (
                  SELECT tc.role_id AS rid FROM tb_contractors tc
                  WHERE tc.id = c.id AND tc.role_id IS NOT NULL
                  UNION
                  SELECT cr.role_id FROM tb_contractor_roles cr WHERE cr.contractor_id = c.id
                ) x
                JOIN roles r ON r.id = x.rid
              ), 0) >= %s
            """
            sql_params.append(min_rank)
        cur.execute(
            f"""
            SELECT c.id, c.name, c.email, c.initials
            FROM tb_contractors c
            WHERE c.status = 'active'
              AND (
                c.name LIKE %s OR c.email LIKE %s OR c.initials LIKE %s OR CAST(c.id AS CHAR) LIKE %s
              )
              {rank_filter}
            ORDER BY c.name ASC
            LIMIT 30
            """,
            tuple(sql_params),
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
    data.pop("cura_operational_event_id", None)
    data["lead_user_id"] = int(uid)
    try:
        rs_id = RunsheetService.create_runsheet(data)
        ra_id = RunsheetService.ensure_lead_assignment(rs_id, int(uid))
        # If this runsheet was created from a scheduled shift, link them so the shift becomes
        # "runsheet-backed" and scheduling changes can mirror scheduled window into the runsheet bundle.
        schedule_shift_id = data.get("schedule_shift_id")
        if schedule_shift_id is not None:
            try:
                from app.plugins.time_billing_module.services import TimesheetService

                conn = get_db_connection()
                cur = conn.cursor()
                try:
                    cur.execute("SHOW TABLES LIKE 'schedule_shifts'")
                    if cur.fetchone():
                        cur.execute(
                            """
                            UPDATE schedule_shifts
                            SET runsheet_id=%s, runsheet_assignment_id=%s
                            WHERE id=%s
                            """,
                            (int(rs_id), int(ra_id), int(schedule_shift_id)),
                        )
                        conn.commit()
                finally:
                    cur.close()
                    conn.close()

                # Remove any scheduler-prefilled timesheet row for this shift so it doesn't
                # duplicate the runsheet-backed payroll line.
                try:
                    conn2 = get_db_connection()
                    cur2 = conn2.cursor(dictionary=True)
                    try:
                        cur2.execute(
                            "SELECT work_date FROM schedule_shifts WHERE id=%s LIMIT 1",
                            (int(schedule_shift_id),),
                        )
                        r2 = cur2.fetchone() or {}
                        wd = r2.get("work_date")
                    finally:
                        cur2.close()
                        conn2.close()
                    if wd is not None:
                        TimesheetService.delete_scheduler_prefill_for_shift(
                            int(schedule_shift_id), int(uid), wd
                        )
                except Exception:
                    pass
            except Exception:
                pass
        return jsonify({"ok": True, "id": rs_id}), 201
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        current_app.logger.warning(
            "api_my_runsheets_create: %s", e, exc_info=True)
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
    data.pop("cura_operational_event_id", None)
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
        res = RunsheetService.withdraw_runsheet_assignment(
            rs_id, ra_id, int(uid))
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
        res = RunsheetService.reactivate_runsheet_assignment(
            rs_id, ra_id, int(uid))
        return jsonify(res), (200 if res.get("ok") else 400)
    except Exception as e:
        return _tb_safe_api_error(e, status=500, log_message="assignment_reactivate")


@public_bp.get("/api/rates")
@staff_required_tb
def api_public_contractor_rate():
    """
    Indicative £/h for the logged-in contractor (same resolver stack as saving a row).

    Uses ``RateResolver`` with a nominal 09:00–17:00 window (0 break) so weekend/BH/night
    uplifts match saved entries; falls back to ``MinimalRateResolver`` on error.
    Query: job_type_id (int), on=YYYY-MM-DD, optional client_id (int).
    """
    uid = current_tb_user_id()
    job_type_id = request.args.get("job_type_id", type=int)
    on_raw = (request.args.get("on") or "").strip()
    client_id = request.args.get("client_id", type=int)
    if not uid or not job_type_id or not on_raw:
        return jsonify({"ok": False, "rate": 0.0, "message": "job_type_id and on (YYYY-MM-DD) required"}), 400
    try:
        work_date = datetime.strptime(on_raw, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"ok": False, "rate": 0.0, "message": "invalid on date"}), 400
    try:
        contractor = TimesheetService._get_contractor(int(uid))
        rate_dec, _, _ = RateResolver.resolve_rate_and_pay(
            int(uid),
            contractor.get("role_id"),
            job_type_id,
            client_id,
            work_date,
            time_cls(9, 0, 0),
            time_cls(17, 0, 0),
            0,
        )
        rate_f = float(rate_dec)
    except Exception:
        rate_f = float(
            MinimalRateResolver.resolve_rate(
                int(uid), job_type_id, work_date, client_id=client_id
            )
        )
    return jsonify({"ok": True, "rate": rate_f})


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
