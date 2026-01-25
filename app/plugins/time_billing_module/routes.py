from werkzeug.exceptions import HTTPException
from functools import wraps
from datetime import datetime, date
from io import BytesIO
import os
import json
from flask import (
    Blueprint, request, jsonify, send_file,
    render_template, redirect, url_for, flash, session
)
from app.objects import get_db_connection, AuthManager, PluginManager
from .services import TimesheetService, RunsheetService, TemplateService, ExportService, _dec

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


# =============================================================================
# Auth (tb_contractors)
# =============================================================================


def current_tb_user():
    return session.get('tb_user') or None


def current_tb_user_id():
    u = current_tb_user()
    return int(u['id']) if u and u.get('id') is not None else None


def staff_required_tb(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        u = current_tb_user()
        if not u:
            return redirect('/time-billing/login')
        role = (u.get('role') or '').lower()
        if role not in ('staff', 'admin', 'superuser'):
            flash('Not authorized for staff portal.', 'error')
            return redirect(url_for('public_time_billing.login_page'))
        return view(*args, **kwargs)
    return wrapped


def admin_required_tb(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        role = session.get('role')
        # if role != 'admin' and role != 'superuser':
        #     flash('Admin access required.', 'error')
        #     return redirect('/')
        return view(*args, **kwargs)
    return wrapped

# =============================================================================
# Public authentication routes (tb_contractors)
# =============================================================================


@public_bp.get("/login")
def login_page():
    return render_template("public/auth/login.html", config=core_manifest)


@public_bp.post("/login")
def login_submit():
    email = (request.form.get('email') or '').strip().lower()
    password = request.form.get('password') or ''

    if not email or not password:
        flash('Email and password are required.', 'error')
        return redirect(url_for('public_time_billing.login_page'))

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
        flash('Invalid credentials.', 'error')
        return redirect(url_for('public_time_billing.login_page'))

    if str(u.get('status')).lower() not in ('active', '1', 'true', 'yes'):
        flash('Account inactive. Contact admin.', 'error')
        return redirect(url_for('public_time_billing.login_page'))

    display = (u.get('name') or '').strip() or u['email']
    session['tb_user'] = {
        "id": int(u['id']),
        "email": u['email'],
        "name": display,
        "role": "staff"  # harmless default so staff_required_tb passes
    }

    ss = session.get('site_settings', {})
    ss['user_name'] = display
    session['site_settings'] = ss

    next_url = request.args.get('next') or url_for(
        'public_time_billing.public_dashboard_page')
    return redirect(next_url)


@public_bp.get("/logout")
def logout():
    session.pop('tb_user', None)
    flash('You have been logged out.', 'success')
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
    return jsonify(RunsheetService.get_runsheet(rs_id))


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
    return jsonify({"id": TemplateService.create_runsheet_template(data)})


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
# API Endpoints (roles are many-to-many via roles + tb_contractor_roles)
# =============================================================================

# Get single contractor (with roles array)
@internal_bp.get("/api/contractors/<int:user_id>")
@admin_required_tb
def api_get_contractor(user_id):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
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
    # legacy roles support (optional)
    roles = data.get("roles") or []

    if not name:
        return jsonify({"ok": False, "error": "Name required"}), 400

    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # update base fields
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
            "SELECT id, user_id, week_id FROM tb_timesheet_entries WHERE id=%s",
            (entry_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"ok": False, "message": "not_found"}), 404
        if row["user_id"] != uid or row["week_id"] != wk["id"]:
            return jsonify({"ok": False, "message": "not_owner"}), 403

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
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if client_name:
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
            SELECT id, name
            FROM job_types
            WHERE active IN (1, '1', 'active', TRUE)
            ORDER BY name ASC
        """)
        items = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return jsonify({"items": items})

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
