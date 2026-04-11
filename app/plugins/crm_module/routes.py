# CRM — admin UI (R1: accounts, contacts, opportunities)
from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for
from flask_login import login_required

from app.objects import get_db_connection

from .crm_common import can_edit, crm_access_required, crm_edit_required, uid
from .crm_event_plan_links import fetch_plans_for_opportunity
from .crm_event_risk import enrich_lead_meta_with_staffing_breakdown, parse_public_calculator_form
from .crm_guide_staffing_costs import estimate_guide_staffing_costs
from .crm_lead_intake import intake_from_parsed_form
from .crm_stage_history import log_opportunity_stage_change

BASE_PATH = "/plugin/crm_module"
crm_bp = Blueprint(
    "crm_module",
    __name__,
    url_prefix=BASE_PATH,
    template_folder="templates",
)


def _decimal_or_none(s: str | None):
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    try:
        return Decimal(t)
    except InvalidOperation:
        return None


def _opportunity_lead_meta(row: dict | None) -> dict | None:
    if not row:
        return None
    lm = row.get("lead_meta_json")
    if lm is None:
        return None
    if isinstance(lm, dict):
        return lm
    if isinstance(lm, str) and lm.strip():
        try:
            o = json.loads(lm)
            return o if isinstance(o, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _lead_meta_for_opportunity_view(row: dict | None) -> dict | None:
    """Parsed lead_meta with staffing_breakdown filled in for legacy JSON rows."""
    lm = _opportunity_lead_meta(row)
    if not lm:
        return None
    return enrich_lead_meta_with_staffing_breakdown(lm)


def _guide_cost_estimate(lead_meta: dict | None):
    if not lead_meta:
        return None
    return estimate_guide_staffing_costs(lead_meta)


def _maybe_tb_wage_cards_url():
    try:
        return url_for("internal_time_billing.wage_cards_page")
    except Exception:
        return None


def _list_accounts(conn):
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, name, website, phone, notes, created_at FROM crm_accounts ORDER BY name ASC"
        )
        return cur.fetchall() or []
    finally:
        cur.close()


def _list_contacts(conn):
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT c.id, c.account_id, c.first_name, c.last_name, c.email, c.phone, c.job_title,
                   a.name AS account_name
            FROM crm_contacts c
            LEFT JOIN crm_accounts a ON a.id = c.account_id
            ORDER BY c.last_name ASC, c.first_name ASC
            """
        )
        return cur.fetchall() or []
    finally:
        cur.close()


def _list_opportunities(conn):
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT o.id, o.account_id, o.name, o.stage, o.amount, o.notes, o.updated_at,
                   a.name AS account_name
            FROM crm_opportunities o
            JOIN crm_accounts a ON a.id = o.account_id
            ORDER BY o.updated_at DESC
            """
        )
        return cur.fetchall() or []
    finally:
        cur.close()


# Pipeline stages (dashboard, lists, kanban, opportunity forms)
OPP_STAGES = (
    ("prospecting", "Prospecting"),
    ("qualification", "Qualification"),
    ("proposal", "Proposal"),
    ("negotiation", "Negotiation"),
    ("won", "Won"),
    ("lost", "Lost"),
)

# Heuristic win weights for open stages only (industry-style weighted pipeline; tune per org).
PIPELINE_STAGE_WEIGHTS = {
    "prospecting": 0.25,
    "qualification": 0.40,
    "proposal": 0.65,
    "negotiation": 0.85,
}


_QUOTE_STATUS_ORDER = (
    ("draft", "Draft"),
    ("sent", "Sent"),
    ("accepted", "Accepted"),
    ("lost", "Lost"),
    ("rejected", "Rejected"),
)
_ACTIVITY_TYPE_LABELS = {
    "call": "Calls",
    "meeting": "Meetings",
    "email": "Emails",
    "task": "Tasks",
    "note": "Notes",
}


def _crm_dashboard_extras(conn) -> dict:
    """Pipeline counts, values, recents, quote outcomes, activity mix, open tasks."""
    out: dict = {
        "pipeline_stage_counts": {s: 0 for s, _ in OPP_STAGES},
        "pipeline_value_by_stage": {s: 0.0 for s, _ in OPP_STAGES},
        "pipeline_active_total": 0,
        "pipeline_open_value_total": 0.0,
        "pipeline_bar_max": 1,
        "recent_opportunities": [],
        "recent_quotes": [],
        "open_tasks": [],
        "quote_status_summary": [],
        "activity_mix_30d": [],
        "activity_mix_30d_total": 0,
        "weighted_pipeline_open": 0.0,
        "moves_to_won_90d": 0,
        "moves_to_lost_90d": 0,
        "recent_stage_moves": [],
        "stage_metrics_available": False,
    }
    active_stages = {"prospecting", "qualification", "proposal", "negotiation"}
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT stage, COUNT(*) AS c FROM crm_opportunities GROUP BY stage")
        for row in cur.fetchall() or []:
            k = row.get("stage")
            if k in out["pipeline_stage_counts"]:
                out["pipeline_stage_counts"][k] = int(row["c"])
        out["pipeline_active_total"] = sum(
            out["pipeline_stage_counts"].get(s, 0) for s in active_stages
        )
        counts = list(out["pipeline_stage_counts"].values())
        out["pipeline_bar_max"] = max(counts) if counts and max(counts) > 0 else 1

        cur.execute(
            """
            SELECT stage, COALESCE(SUM(amount), 0) AS v
            FROM crm_opportunities
            GROUP BY stage
            """
        )
        for row in cur.fetchall() or []:
            k = row.get("stage")
            if k in out["pipeline_value_by_stage"]:
                raw = row.get("v")
                try:
                    out["pipeline_value_by_stage"][k] = float(raw or 0)
                except (TypeError, ValueError):
                    out["pipeline_value_by_stage"][k] = 0.0
        out["pipeline_open_value_total"] = sum(
            out["pipeline_value_by_stage"].get(s, 0.0) for s in active_stages
        )
        out["weighted_pipeline_open"] = sum(
            out["pipeline_value_by_stage"].get(s, 0.0) * PIPELINE_STAGE_WEIGHTS.get(s, 0.0)
            for s in active_stages
        )

        cur.execute(
            """
            SELECT o.id, o.name, o.stage, o.amount, o.updated_at, a.name AS account_name
            FROM crm_opportunities o
            JOIN crm_accounts a ON a.id = o.account_id
            ORDER BY o.updated_at DESC LIMIT 8
            """
        )
        out["recent_opportunities"] = cur.fetchall() or []
        cur.execute(
            """
            SELECT q.id, q.title, q.status, q.updated_at, a.name AS account_name
            FROM crm_quotes q
            LEFT JOIN crm_accounts a ON a.id = q.account_id
            ORDER BY q.updated_at DESC LIMIT 6
            """
        )
        out["recent_quotes"] = cur.fetchall() or []
        cur.execute(
            """
            SELECT id, subject, activity_type, due_at, opportunity_id
            FROM crm_activities
            WHERE completed_at IS NULL AND due_at IS NOT NULL
            ORDER BY due_at ASC LIMIT 6
            """
        )
        out["open_tasks"] = cur.fetchall() or []

        qcounts: dict[str, int] = {}
        cur.execute("SELECT status, COUNT(*) AS c FROM crm_quotes GROUP BY status")
        for row in cur.fetchall() or []:
            st = row.get("status")
            if st:
                qcounts[str(st)] = int(row["c"])
        out["quote_status_summary"] = [
            {"status": k, "label": lab, "count": qcounts.get(k, 0)}
            for k, lab in _QUOTE_STATUS_ORDER
        ]

        mix_total = 0
        mix_rows: list[dict] = []
        cur.execute(
            """
            SELECT activity_type, COUNT(*) AS c
            FROM crm_activities
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY activity_type
            """
        )
        for row in cur.fetchall() or []:
            at = row.get("activity_type") or "note"
            c = int(row["c"])
            mix_total += c
            mix_rows.append(
                {
                    "type": at,
                    "label": _ACTIVITY_TYPE_LABELS.get(str(at), str(at).title()),
                    "count": c,
                }
            )
        mix_rows.sort(key=lambda x: -x["count"])
        out["activity_mix_30d"] = mix_rows
        out["activity_mix_30d_total"] = mix_total

        try:
            cur.execute(
                """
                SELECT COUNT(*) AS c FROM crm_opportunity_stage_history
                WHERE to_stage = 'won' AND changed_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)
                """
            )
            rw = cur.fetchone()
            out["moves_to_won_90d"] = int(rw["c"]) if rw else 0
            cur.execute(
                """
                SELECT COUNT(*) AS c FROM crm_opportunity_stage_history
                WHERE to_stage = 'lost' AND changed_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)
                """
            )
            rw = cur.fetchone()
            out["moves_to_lost_90d"] = int(rw["c"]) if rw else 0
            cur.execute(
                """
                SELECT h.opportunity_id, h.from_stage, h.to_stage, h.changed_at, o.name AS opp_name
                FROM crm_opportunity_stage_history h
                JOIN crm_opportunities o ON o.id = h.opportunity_id
                ORDER BY h.changed_at DESC LIMIT 10
                """
            )
            out["recent_stage_moves"] = cur.fetchall() or []
            out["stage_metrics_available"] = True
        except Exception:
            out["stage_metrics_available"] = False
    except Exception:
        pass
    finally:
        cur.close()
    return out


@crm_bp.route("/")
@crm_bp.route("/index")
@login_required
@crm_access_required
def dashboard():
    conn = get_db_connection()
    n_quotes = 0
    n_activities = 0
    n_event_plans = 0
    n_event_plans_draft = 0
    n_accounts = n_contacts = n_opps = 0
    extras = _crm_dashboard_extras(conn)
    try:
        n_accounts = len(_list_accounts(conn))
        n_contacts = len(_list_contacts(conn))
        n_opps = len(_list_opportunities(conn))
        cur = None
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM crm_quotes")
            row = cur.fetchone()
            n_quotes = int(row[0]) if row else 0
            cur.execute("SELECT COUNT(*) FROM crm_activities")
            row = cur.fetchone()
            n_activities = int(row[0]) if row else 0
            cur.execute("SELECT COUNT(*) FROM crm_event_plans")
            row = cur.fetchone()
            n_event_plans = int(row[0]) if row else 0
            cur.execute("SELECT COUNT(*) FROM crm_event_plans WHERE status='draft'")
            row = cur.fetchone()
            n_event_plans_draft = int(row[0]) if row else 0
        except Exception:
            pass
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
    finally:
        conn.close()
    try:
        public_enquiry_url = url_for("crm_public.public_event_risk_calculator")
    except Exception:
        public_enquiry_url = None
    return render_template(
        "admin/crm_dashboard.html",
        n_accounts=n_accounts,
        n_contacts=n_contacts,
        n_opportunities=n_opps,
        n_quotes=n_quotes,
        n_activities=n_activities,
        n_event_plans=n_event_plans,
        n_event_plans_draft=n_event_plans_draft,
        can_edit=can_edit(),
        stage_labels=dict(OPP_STAGES),
        public_enquiry_url=public_enquiry_url,
        show_financials=can_edit(),
        **extras,
    )


def _crm_search_like_term(raw_q: str) -> str:
    """Strip SQL LIKE metacharacters so user input cannot broaden matches to whole tables."""
    t = (raw_q or "")[:200].replace("%", "").replace("_", "").replace("\\", "")
    return t.strip()


@crm_bp.route("/search", methods=["GET"])
@login_required
@crm_access_required
def crm_search():
    raw = (request.args.get("q") or "").strip()
    q = _crm_search_like_term(raw)
    hits_accounts: list = []
    hits_contacts: list = []
    hits_opportunities: list = []
    hits_quotes: list = []
    if len(q) >= 2:
        like = f"%{q}%"
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, name FROM crm_accounts WHERE name LIKE %s ORDER BY name LIMIT 25",
                (like,),
            )
            hits_accounts = cur.fetchall() or []
            cur.execute(
                """
                SELECT c.id, c.first_name, c.last_name, c.email, a.name AS account_name
                FROM crm_contacts c
                LEFT JOIN crm_accounts a ON a.id = c.account_id
                WHERE c.first_name LIKE %s OR c.last_name LIKE %s OR c.email LIKE %s
                ORDER BY c.last_name, c.first_name LIMIT 25
                """,
                (like, like, like),
            )
            hits_contacts = cur.fetchall() or []
            cur.execute(
                """
                SELECT o.id, o.name, o.stage, a.name AS account_name
                FROM crm_opportunities o
                JOIN crm_accounts a ON a.id = o.account_id
                WHERE o.name LIKE %s
                ORDER BY o.updated_at DESC LIMIT 25
                """,
                (like,),
            )
            hits_opportunities = cur.fetchall() or []
            cur.execute(
                """
                SELECT q.id, q.title, q.status, a.name AS account_name
                FROM crm_quotes q
                LEFT JOIN crm_accounts a ON a.id = q.account_id
                WHERE q.title LIKE %s
                ORDER BY q.updated_at DESC LIMIT 25
                """,
                (like,),
            )
            hits_quotes = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()
    return render_template(
        "admin/crm_search_results.html",
        q=q,
        q_display=raw[:200] if raw else q,
        hits_accounts=hits_accounts,
        hits_contacts=hits_contacts,
        hits_opportunities=hits_opportunities,
        hits_quotes=hits_quotes,
        can_edit=can_edit(),
    )


# --- Accounts ---


@crm_bp.route("/accounts")
@login_required
@crm_access_required
def accounts_list():
    conn = get_db_connection()
    try:
        rows = _list_accounts(conn)
    finally:
        conn.close()
    return render_template(
        "admin/crm_accounts_list.html", accounts=rows, can_edit=can_edit()
    )


@crm_bp.route("/accounts/new", methods=["GET", "POST"])
@login_required
@crm_access_required
@crm_edit_required
def account_new():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        if not name:
            flash("Account name is required.", "danger")
            return render_template("admin/crm_account_form.html", account=None, can_edit=True)
        website = (request.form.get("website") or "").strip() or None
        phone = (request.form.get("phone") or "").strip() or None
        notes = (request.form.get("notes") or "").strip() or None
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO crm_accounts (name, website, phone, notes) VALUES (%s,%s,%s,%s)",
                (name, website, phone, notes),
            )
            conn.commit()
            new_id = int(cur.lastrowid)
            flash("Account created.", "success")
            return redirect(url_for("crm_module.account_edit", account_id=new_id))
        finally:
            cur.close()
            conn.close()
    return render_template("admin/crm_account_form.html", account=None, can_edit=True)


@crm_bp.route("/accounts/<int:account_id>/edit", methods=["GET", "POST"])
@login_required
@crm_access_required
@crm_edit_required
def account_edit(account_id: int):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM crm_accounts WHERE id = %s", (account_id,))
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if not row:
        flash("Account not found.", "danger")
        return redirect(url_for("crm_module.accounts_list"))
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        if not name:
            flash("Account name is required.", "danger")
            return render_template(
                "admin/crm_account_form.html", account=row, can_edit=True
            )
        website = (request.form.get("website") or "").strip() or None
        phone = (request.form.get("phone") or "").strip() or None
        notes = (request.form.get("notes") or "").strip() or None
        conn = get_db_connection()
        cur2 = conn.cursor()
        try:
            cur2.execute(
                """UPDATE crm_accounts SET name=%s, website=%s, phone=%s, notes=%s WHERE id=%s""",
                (name, website, phone, notes, account_id),
            )
            conn.commit()
            flash("Account updated.", "success")
            return redirect(url_for("crm_module.account_edit", account_id=account_id))
        finally:
            cur2.close()
            conn.close()
    return render_template("admin/crm_account_form.html", account=row, can_edit=True)


@crm_bp.route("/accounts/<int:account_id>/delete", methods=["POST"])
@login_required
@crm_access_required
@crm_edit_required
def account_delete(account_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM crm_accounts WHERE id = %s", (account_id,))
        conn.commit()
        if cur.rowcount:
            flash("Account deleted.", "success")
        else:
            flash("Account not found.", "warning")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("crm_module.accounts_list"))


# --- Contacts ---


@crm_bp.route("/contacts")
@login_required
@crm_access_required
def contacts_list():
    conn = get_db_connection()
    try:
        rows = _list_contacts(conn)
    finally:
        conn.close()
    return render_template(
        "admin/crm_contacts_list.html", contacts=rows, can_edit=can_edit()
    )


@crm_bp.route("/contacts/new", methods=["GET", "POST"])
@login_required
@crm_access_required
@crm_edit_required
def contact_new():
    conn = get_db_connection()
    try:
        accounts = _list_accounts(conn)
    finally:
        conn.close()
    if request.method == "POST":
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        if not first_name and not last_name:
            flash("First name or last name is required.", "danger")
            return render_template(
                "admin/crm_contact_form.html",
                contact=None,
                accounts=accounts,
                can_edit=True,
            )
        account_id_raw = (request.form.get("account_id") or "").strip()
        account_id = int(account_id_raw) if account_id_raw.isdigit() else None
        email = (request.form.get("email") or "").strip() or None
        phone = (request.form.get("phone") or "").strip() or None
        job_title = (request.form.get("job_title") or "").strip() or None
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO crm_contacts
                (account_id, first_name, last_name, email, phone, job_title)
                VALUES (%s,%s,%s,%s,%s,%s)""",
                (account_id, first_name, last_name, email, phone, job_title),
            )
            conn.commit()
            new_id = int(cur.lastrowid)
            flash("Contact created.", "success")
            return redirect(url_for("crm_module.contact_edit", contact_id=new_id))
        finally:
            cur.close()
            conn.close()
    return render_template(
        "admin/crm_contact_form.html",
        contact=None,
        accounts=accounts,
        can_edit=True,
    )


@crm_bp.route("/contacts/<int:contact_id>/edit", methods=["GET", "POST"])
@login_required
@crm_access_required
@crm_edit_required
def contact_edit(contact_id: int):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    accounts_cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM crm_contacts WHERE id = %s", (contact_id,))
        row = cur.fetchone()
        accounts_cur.execute("SELECT id, name FROM crm_accounts ORDER BY name")
        accounts = accounts_cur.fetchall() or []
    finally:
        accounts_cur.close()
        cur.close()
        conn.close()
    if not row:
        flash("Contact not found.", "danger")
        return redirect(url_for("crm_module.contacts_list"))
    if request.method == "POST":
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        if not first_name and not last_name:
            flash("First name or last name is required.", "danger")
            return render_template(
                "admin/crm_contact_form.html",
                contact=row,
                accounts=accounts,
                can_edit=True,
            )
        account_id_raw = (request.form.get("account_id") or "").strip()
        account_id = int(account_id_raw) if account_id_raw.isdigit() else None
        email = (request.form.get("email") or "").strip() or None
        phone = (request.form.get("phone") or "").strip() or None
        job_title = (request.form.get("job_title") or "").strip() or None
        conn = get_db_connection()
        cur2 = conn.cursor()
        try:
            cur2.execute(
                """UPDATE crm_contacts SET account_id=%s, first_name=%s, last_name=%s,
                email=%s, phone=%s, job_title=%s WHERE id=%s""",
                (account_id, first_name, last_name, email, phone, job_title, contact_id),
            )
            conn.commit()
            flash("Contact updated.", "success")
            return redirect(url_for("crm_module.contact_edit", contact_id=contact_id))
        finally:
            cur2.close()
            conn.close()
    return render_template(
        "admin/crm_contact_form.html",
        contact=row,
        accounts=accounts,
        can_edit=True,
    )


@crm_bp.route("/contacts/<int:contact_id>/delete", methods=["POST"])
@login_required
@crm_access_required
@crm_edit_required
def contact_delete(contact_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM crm_contacts WHERE id = %s", (contact_id,))
        conn.commit()
        if cur.rowcount:
            flash("Contact deleted.", "success")
        else:
            flash("Contact not found.", "warning")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("crm_module.contacts_list"))


# --- Opportunities ---


@crm_bp.route("/opportunities")
@login_required
@crm_access_required
def opportunities_list():
    conn = get_db_connection()
    try:
        rows = _list_opportunities(conn)
    finally:
        conn.close()
    stage_labels = dict(OPP_STAGES)
    return render_template(
        "admin/crm_opportunities_list.html",
        opportunities=rows,
        stage_labels=stage_labels,
        can_edit=can_edit(),
    )


@crm_bp.route("/opportunities/board")
@login_required
@crm_access_required
def opportunities_board():
    conn = get_db_connection()
    try:
        rows = _list_opportunities(conn)
    finally:
        conn.close()
    by_stage: dict[str, list] = {s: [] for s, _ in OPP_STAGES}
    for o in rows:
        st = (o.get("stage") or "prospecting").strip()
        if st not in by_stage:
            st = "prospecting"
        by_stage[st].append(o)
    _m = 91919191
    _u = url_for("crm_module.api_opportunity_stage", opp_id=_m)
    kanban_api_url_tmpl = _u.replace(str(_m), "__OPP__")
    return render_template(
        "admin/crm_opportunities_kanban.html",
        columns=OPP_STAGES,
        by_stage=by_stage,
        stage_labels=dict(OPP_STAGES),
        can_edit=can_edit(),
        kanban_api_url_tmpl=kanban_api_url_tmpl,
    )


@crm_bp.route("/api/opportunities/<int:opp_id>/stage", methods=["POST"])
@login_required
@crm_access_required
@crm_edit_required
def api_opportunity_stage(opp_id: int):
    if not request.is_json:
        return jsonify({"ok": False, "error": "json_required"}), 400
    body = request.get_json(silent=True) or {}
    stage = (body.get("stage") or "").strip()
    valid = {s for s, _ in OPP_STAGES}
    if stage not in valid:
        return jsonify({"ok": False, "error": "invalid_stage"}), 400
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT stage FROM crm_opportunities WHERE id=%s", (opp_id,))
        prev = cur.fetchone()
        if not prev:
            return jsonify({"ok": False, "error": "not_found"}), 404
        old_stage = prev[0]
        if old_stage == stage:
            return jsonify({"ok": True, "stage": stage})
        cur.execute(
            "UPDATE crm_opportunities SET stage=%s WHERE id=%s",
            (stage, opp_id),
        )
        conn.commit()
        if not cur.rowcount:
            return jsonify({"ok": False, "error": "not_found"}), 404
    finally:
        cur.close()
        conn.close()
    log_opportunity_stage_change(opp_id, old_stage, stage, uid())
    return jsonify({"ok": True, "stage": stage})


@crm_bp.route("/opportunities/intake", methods=["GET", "POST"])
@login_required
@crm_access_required
def opportunity_intake():
    """Staff / call-centre intake — same payload shape as the public calculator."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, name FROM crm_accounts ORDER BY name")
        accounts = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    if request.method == "POST":
        if not can_edit():
            flash("You do not have permission to create pipeline records.", "danger")
            return redirect(url_for("crm_module.opportunities_board"))
        parsed = parse_public_calculator_form(request.form)
        acc_raw = (request.form.get("account_id") or "").strip()
        acc_override = int(acc_raw) if acc_raw.isdigit() else None
        stage = (request.form.get("stage") or "prospecting").strip()
        if stage not in {s for s, _ in OPP_STAGES}:
            stage = "prospecting"
        try:
            opp_id, _cid, _meta = intake_from_parsed_form(
                parsed,
                source="staff_intake",
                stage=stage,
                account_id_override=acc_override,
            )
            flash(f"Opportunity #{opp_id} created.", "success")
            return redirect(url_for("crm_module.opportunity_edit", opp_id=opp_id))
        except ValueError:
            flash("Organisation, contact, email, and event name are required.", "danger")
        except Exception as ex:
            flash(f"Could not create opportunity: {ex}", "danger")

    return render_template(
        "admin/crm_opportunity_intake.html",
        accounts=accounts,
        stages=OPP_STAGES,
        can_edit=can_edit(),
    )


@crm_bp.route("/opportunities/new", methods=["GET", "POST"])
@login_required
@crm_access_required
@crm_edit_required
def opportunity_new():
    conn = get_db_connection()
    try:
        accounts = _list_accounts(conn)
    finally:
        conn.close()
    if not accounts:
        flash("Create at least one account before adding an opportunity.", "warning")
        return redirect(url_for("crm_module.account_new"))
    if request.method == "POST":
        account_id_raw = (request.form.get("account_id") or "").strip()
        account_id = int(account_id_raw) if account_id_raw.isdigit() else None
        name = (request.form.get("name") or "").strip()
        stage = (request.form.get("stage") or "prospecting").strip()
        valid_stages = {s for s, _ in OPP_STAGES}
        if stage not in valid_stages:
            stage = "prospecting"
        amount = _decimal_or_none(request.form.get("amount"))
        notes = (request.form.get("notes") or "").strip() or None
        if not account_id or not name:
            flash("Account and opportunity name are required.", "danger")
            return render_template(
                "admin/crm_opportunity_form.html",
                opportunity=None,
                accounts=accounts,
                stages=OPP_STAGES,
                linked_event_plans=[],
                lead_meta=None,
                guide_cost_estimate=None,
                tb_wage_cards_url=_maybe_tb_wage_cards_url(),
                can_edit=True,
            )
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO crm_opportunities (account_id, name, stage, amount, notes)
                VALUES (%s,%s,%s,%s,%s)""",
                (account_id, name, stage, amount, notes),
            )
            conn.commit()
            new_id = int(cur.lastrowid)
            log_opportunity_stage_change(new_id, None, stage, uid())
            flash("Opportunity created.", "success")
            return redirect(url_for("crm_module.opportunity_edit", opp_id=new_id))
        finally:
            cur.close()
            conn.close()
    return render_template(
        "admin/crm_opportunity_form.html",
        opportunity=None,
        accounts=accounts,
        stages=OPP_STAGES,
        linked_event_plans=[],
        lead_meta=None,
        guide_cost_estimate=None,
        tb_wage_cards_url=_maybe_tb_wage_cards_url(),
        can_edit=True,
    )


@crm_bp.route("/opportunities/<int:opp_id>/edit", methods=["GET", "POST"])
@login_required
@crm_access_required
@crm_edit_required
def opportunity_edit(opp_id: int):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    accounts_cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM crm_opportunities WHERE id = %s", (opp_id,))
        row = cur.fetchone()
        accounts_cur.execute("SELECT id, name FROM crm_accounts ORDER BY name")
        accounts = accounts_cur.fetchall() or []
    finally:
        accounts_cur.close()
        cur.close()
        conn.close()
    if not row:
        flash("Opportunity not found.", "danger")
        return redirect(url_for("crm_module.opportunities_list"))
    if request.method == "POST":
        account_id_raw = (request.form.get("account_id") or "").strip()
        account_id = int(account_id_raw) if account_id_raw.isdigit() else None
        name = (request.form.get("name") or "").strip()
        stage = (request.form.get("stage") or "prospecting").strip()
        valid_stages = {s for s, _ in OPP_STAGES}
        if stage not in valid_stages:
            stage = "prospecting"
        amount = _decimal_or_none(request.form.get("amount"))
        notes = (request.form.get("notes") or "").strip() or None
        if not account_id or not name:
            flash("Account and opportunity name are required.", "danger")
            c_ep = get_db_connection()
            try:
                linked_event_plans = fetch_plans_for_opportunity(c_ep, opp_id)
            finally:
                c_ep.close()
            lm = _lead_meta_for_opportunity_view(row)
            return render_template(
                "admin/crm_opportunity_form.html",
                opportunity=row,
                accounts=accounts,
                stages=OPP_STAGES,
                linked_event_plans=linked_event_plans,
                lead_meta=lm,
                guide_cost_estimate=_guide_cost_estimate(lm),
                tb_wage_cards_url=_maybe_tb_wage_cards_url(),
                can_edit=True,
            )
        old_stage = (row.get("stage") or "").strip()
        conn = get_db_connection()
        cur2 = conn.cursor()
        try:
            cur2.execute(
                """UPDATE crm_opportunities SET account_id=%s, name=%s, stage=%s,
                amount=%s, notes=%s WHERE id=%s""",
                (account_id, name, stage, amount, notes, opp_id),
            )
            conn.commit()
            if old_stage != stage:
                log_opportunity_stage_change(opp_id, old_stage, stage, uid())
            flash("Opportunity updated.", "success")
            return redirect(url_for("crm_module.opportunity_edit", opp_id=opp_id))
        finally:
            cur2.close()
            conn.close()
    c_ep = get_db_connection()
    try:
        linked_event_plans = fetch_plans_for_opportunity(c_ep, opp_id)
    finally:
        c_ep.close()
    lm = _lead_meta_for_opportunity_view(row)
    return render_template(
        "admin/crm_opportunity_form.html",
        opportunity=row,
        accounts=accounts,
        stages=OPP_STAGES,
        linked_event_plans=linked_event_plans,
        lead_meta=lm,
        guide_cost_estimate=_guide_cost_estimate(lm),
        tb_wage_cards_url=_maybe_tb_wage_cards_url(),
        can_edit=True,
    )


@crm_bp.route("/opportunities/<int:opp_id>/delete", methods=["POST"])
@login_required
@crm_access_required
@crm_edit_required
def opportunity_delete(opp_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM crm_opportunities WHERE id = %s", (opp_id,))
        conn.commit()
        if cur.rowcount:
            flash("Opportunity deleted.", "success")
        else:
            flash("Opportunity not found.", "warning")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("crm_module.opportunities_list"))


from .quote_routes import register_crm_quote_routes  # noqa: E402
from .activities_routes import register_crm_activities_routes  # noqa: E402
from .planner_routes import register_crm_planner_routes  # noqa: E402
from .crm_settings_routes import register_crm_settings_routes  # noqa: E402

register_crm_quote_routes(crm_bp)
register_crm_activities_routes(crm_bp)
register_crm_planner_routes(crm_bp)
register_crm_settings_routes(crm_bp)


@crm_bp.context_processor
def _crm_cross_module_nav():
    """Expose Cura ops hub URL when medical_records internal blueprint is registered."""
    from .crm_cura_links import safe_cura_ops_hub_url

    return {
        "crm_cura_ops_hub_url": safe_cura_ops_hub_url(),
        "crm_can_edit": can_edit(),
    }


def get_blueprints():
    from .crm_public_routes import crm_public_bp

    return [crm_public_bp, crm_bp]
