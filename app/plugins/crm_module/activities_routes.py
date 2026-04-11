# CRM activities (calls, meetings, tasks, notes)
from __future__ import annotations

from datetime import datetime

from flask import flash, redirect, render_template, request, url_for
from flask_login import login_required

from app.objects import get_db_connection

from .crm_common import can_edit, crm_access_required, crm_edit_required, uid


def register_crm_activities_routes(crm_bp):
    @crm_bp.route("/activities")
    @login_required
    @crm_access_required
    def activities_list():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT a.id, a.activity_type, a.subject, a.due_at, a.completed_at,
                       a.created_at, ac.name AS account_name,
                       CONCAT(IFNULL(c.first_name,''),' ',IFNULL(c.last_name,'')) AS contact_name
                FROM crm_activities a
                LEFT JOIN crm_accounts ac ON ac.id = a.account_id
                LEFT JOIN crm_contacts c ON c.id = a.contact_id
                ORDER BY COALESCE(a.due_at, a.created_at) DESC
                LIMIT 300
                """
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()
        return render_template(
            "admin/crm_activities_list.html",
            activities=rows,
            can_edit=can_edit(),
        )

    @crm_bp.route("/activities/new", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def activity_new():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT id, name FROM crm_accounts ORDER BY name")
            accounts = cur.fetchall() or []
            cur.execute(
                "SELECT id, first_name, last_name, account_id FROM crm_contacts ORDER BY last_name, first_name"
            )
            contacts = cur.fetchall() or []
            cur.execute(
                """SELECT o.id, o.name, o.account_id, ac.name AS account_name
                FROM crm_opportunities o JOIN crm_accounts ac ON ac.id = o.account_id
                ORDER BY ac.name, o.name"""
            )
            opps = cur.fetchall() or []
            cur.execute("SELECT id, title FROM crm_quotes ORDER BY updated_at DESC LIMIT 100")
            quotes = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        types = (
            ("call", "Call"),
            ("meeting", "Meeting"),
            ("email", "Email"),
            ("task", "Task"),
            ("note", "Note"),
        )
        if request.method == "POST":
            at = (request.form.get("activity_type") or "note").strip()
            allowed = {x[0] for x in types}
            if at not in allowed:
                at = "note"
            subject = (request.form.get("subject") or "").strip() or "Activity"
            body = (request.form.get("body") or "").strip() or None
            due = _parse_dt(request.form.get("due_at"))
            completed = _parse_dt(request.form.get("completed_at"))
            acc = _int_or_none(request.form.get("account_id"))
            con = _int_or_none(request.form.get("contact_id"))
            opp = _int_or_none(request.form.get("opportunity_id"))
            qid = _int_or_none(request.form.get("quote_id"))
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """INSERT INTO crm_activities
                    (activity_type, subject, body, due_at, completed_at,
                     account_id, contact_id, opportunity_id, quote_id, created_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        at,
                        subject,
                        body,
                        due,
                        completed,
                        acc,
                        con,
                        opp,
                        qid,
                        uid(),
                    ),
                )
                conn.commit()
                flash("Activity saved.", "success")
                return redirect(url_for("crm_module.activities_list"))
            finally:
                cur.close()
                conn.close()

        return render_template(
            "admin/crm_activity_form.html",
            activity=None,
            accounts=accounts,
            contacts=contacts,
            opportunities=opps,
            quotes=quotes,
            types=types,
            can_edit=True,
        )

    @crm_bp.route("/activities/<int:act_id>/edit", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def activity_edit(act_id: int):
        if request.method == "POST" and not can_edit():
            flash("You do not have permission to edit activities.", "danger")
            return redirect(url_for("crm_module.activity_edit", act_id=act_id))

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM crm_activities WHERE id=%s", (act_id,))
            act = cur.fetchone()
            cur.execute("SELECT id, name FROM crm_accounts ORDER BY name")
            accounts = cur.fetchall() or []
            cur.execute(
                "SELECT id, first_name, last_name, account_id FROM crm_contacts ORDER BY last_name, first_name"
            )
            contacts = cur.fetchall() or []
            cur.execute(
                """SELECT o.id, o.name, o.account_id, ac.name AS account_name
                FROM crm_opportunities o JOIN crm_accounts ac ON ac.id = o.account_id
                ORDER BY ac.name, o.name"""
            )
            opps = cur.fetchall() or []
            cur.execute("SELECT id, title FROM crm_quotes ORDER BY updated_at DESC LIMIT 100")
            quotes = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        if not act:
            flash("Activity not found.", "danger")
            return redirect(url_for("crm_module.activities_list"))

        types = (
            ("call", "Call"),
            ("meeting", "Meeting"),
            ("email", "Email"),
            ("task", "Task"),
            ("note", "Note"),
        )

        if request.method == "POST" and can_edit():
            at = (request.form.get("activity_type") or "note").strip()
            allowed = {x[0] for x in types}
            if at not in allowed:
                at = "note"
            subject = (request.form.get("subject") or "").strip() or "Activity"
            body = (request.form.get("body") or "").strip() or None
            due = _parse_dt(request.form.get("due_at"))
            completed = _parse_dt(request.form.get("completed_at"))
            acc = _int_or_none(request.form.get("account_id"))
            con = _int_or_none(request.form.get("contact_id"))
            opp = _int_or_none(request.form.get("opportunity_id"))
            qid = _int_or_none(request.form.get("quote_id"))
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """UPDATE crm_activities SET activity_type=%s, subject=%s, body=%s,
                    due_at=%s, completed_at=%s, account_id=%s, contact_id=%s,
                    opportunity_id=%s, quote_id=%s WHERE id=%s""",
                    (at, subject, body, due, completed, acc, con, opp, qid, act_id),
                )
                conn.commit()
                flash("Activity updated.", "success")
                return redirect(url_for("crm_module.activities_list"))
            finally:
                cur.close()
                conn.close()

        return render_template(
            "admin/crm_activity_form.html",
            activity=act,
            accounts=accounts,
            contacts=contacts,
            opportunities=opps,
            quotes=quotes,
            types=types,
            can_edit=can_edit(),
        )

    @crm_bp.route("/activities/<int:act_id>/delete", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def activity_delete(act_id: int):
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM crm_activities WHERE id=%s", (act_id,))
            conn.commit()
            flash("Activity deleted.", "success")
        finally:
            cur.close()
            conn.close()
        return redirect(url_for("crm_module.activities_list"))


def _int_or_none(raw: str | None) -> int | None:
    s = (raw or "").strip()
    return int(s) if s.isdigit() else None


def _parse_dt(raw: str | None):
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None
