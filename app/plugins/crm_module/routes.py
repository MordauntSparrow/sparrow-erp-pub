# CRM — admin UI (R1: accounts, contacts, opportunities)
from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any

from flask import (
    Blueprint,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_login import current_user, login_required

from app.objects import get_db_connection

from .crm_common import (
    can_edit,
    crm_access_required,
    crm_edit_required,
    crm_medical_surface_available,
    uid,
)
from .crm_event_plan_links import fetch_plans_for_opportunity
from .crm_geo_utils import resolve_w3w_to_latlng, routing_hint_for_intake
from .crm_event_risk import (
    calculator_template_values,
    compute_event_risk_from_parsed,
    enrich_lead_meta_with_staffing_breakdown,
    parse_public_calculator_form,
)
from .crm_guide_staffing_costs import estimate_guide_staffing_costs
from .crm_lead_intake import (
    LeadIntakeInvalidAccount,
    LeadIntakeMissingRequiredFields,
    build_lead_meta,
    intake_from_parsed_form,
)
from .crm_private_transfer_intake import parse_private_transfer_form
from .crm_pipeline_flow import (
    flow_active,
    opportunity_edit_url,
    quote_new_url,
)
from .crm_purple_guide import (
    TIER_MEDIC_BASE,
    TIER_VEHICLE_BASE,
    public_tier_resource_rows,
)
from .crm_event_intake_layout import (
    collect_intake_extras_from_form,
    load_event_intake_layout,
    load_private_transfer_intake_layout,
    validate_required_extras,
)
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


def _accounts_select_options(limit: int = 400) -> list[dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, name FROM crm_accounts ORDER BY name ASC LIMIT %s",
            (int(limit),),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def _crm_scheduling_form_context(
    opportunity_row: dict | None, lead_meta: dict | None
) -> dict[str, Any]:
    """Template kwargs for linking an opportunity to ``schedule_shifts`` (when TB + scheduling exist)."""
    from datetime import datetime, timedelta

    from .crm_scheduling_bridge import (
        list_active_clients,
        list_active_sites,
        list_job_types,
        scheduling_stack_available,
    )

    empty: dict[str, Any] = {
        "scheduling_available": False,
        "schedule_clients": [],
        "schedule_job_types": [],
        "schedule_sites": [],
        "schedule_shift_edit_url": None,
        "schedule_work_date_default": "",
        "schedule_start_default": "",
        "schedule_end_default": "",
    }
    if not opportunity_row:
        return empty
    if not scheduling_stack_available():
        return empty
    out = {
        "scheduling_available": True,
        "schedule_clients": list_active_clients(),
        "schedule_job_types": list_job_types(),
        "schedule_sites": list_active_sites(),
        "schedule_shift_edit_url": None,
        "schedule_work_date_default": "",
        "schedule_start_default": "",
        "schedule_end_default": "",
    }
    sid = opportunity_row.get("schedule_shift_id")
    if sid:
        try:
            out["schedule_shift_edit_url"] = url_for(
                "internal_scheduling.admin_shift_edit", shift_id=int(sid)
            )
        except Exception:
            out["schedule_shift_edit_url"] = None
    pt: dict[str, Any] = {}
    if isinstance(lead_meta, dict):
        raw_pt = lead_meta.get("private_transfer")
        if isinstance(raw_pt, dict):
            pt = raw_pt
    out["schedule_work_date_default"] = (pt.get("transfer_date") or "").strip()
    st_raw = (pt.get("pickup_time") or "").strip()
    if len(st_raw) >= 5:
        out["schedule_start_default"] = st_raw[:5]
        try:
            t0 = datetime.strptime(st_raw[:5], "%H:%M")
            base = datetime(2000, 1, 1, t0.hour, t0.minute)
            out["schedule_end_default"] = (base + timedelta(hours=4)).strftime("%H:%M")
        except ValueError:
            out["schedule_end_default"] = ""
    return out


def _pipeline_flow_template_kwargs(
    request,
    *,
    step: str,
    opp_id: int | None,
    quote_id: int | None = None,
) -> dict[str, Any]:
    return {
        "pipeline_flow_enabled": flow_active(request),
        "pipeline_step": step,
        "pipeline_opp_id": opp_id,
        "pipeline_quote_id": quote_id,
    }


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
    n_accounts = n_contacts = n_opps = 0
    n_intake_forms = 0
    n_intake_forms_public = 0
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
            try:
                cur.execute(
                    "SELECT COUNT(*) FROM crm_intake_form_definitions WHERE is_active=1"
                )
                row = cur.fetchone()
                n_intake_forms = int(row[0]) if row else 0
                cur.execute(
                    "SELECT COUNT(*) FROM crm_intake_form_definitions WHERE is_active=1 AND is_public=1"
                )
                row = cur.fetchone()
                n_intake_forms_public = int(row[0]) if row else 0
            except Exception:
                pass
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
        n_intake_forms=n_intake_forms,
        n_intake_forms_public=n_intake_forms_public,
        can_edit=can_edit(),
        stage_labels=dict(OPP_STAGES),
        public_enquiry_url=public_enquiry_url,
        show_financials=can_edit(),
        **extras,
    )


def _operating_base_from_form() -> tuple[str | None, str | None, float | None, float | None]:
    label = (request.form.get("operating_base_label") or "").strip() or None
    postcode = (request.form.get("operating_base_postcode") or "").strip() or None
    lat_raw = (request.form.get("operating_base_lat") or "").strip()
    lng_raw = (request.form.get("operating_base_lng") or "").strip()
    lat: float | None = None
    lng: float | None = None
    if lat_raw and lng_raw:
        try:
            lat = float(lat_raw)
            lng = float(lng_raw)
            if not (-90 <= lat <= 90 and -180 <= lng <= 180):
                lat, lng = None, None
        except (TypeError, ValueError):
            lat, lng = None, None
    return label, postcode, lat, lng


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
            return render_template(
                "admin/crm_account_form.html",
                account=None,
                can_edit=True,
                w3w_resolve_url=url_for("crm_module.opportunity_intake_resolve_w3w"),
            )
        website = (request.form.get("website") or "").strip() or None
        phone = (request.form.get("phone") or "").strip() or None
        notes = (request.form.get("notes") or "").strip() or None
        obl, obp, obla, obln = _operating_base_from_form()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO crm_accounts
                (name, website, phone, notes, operating_base_label, operating_base_postcode,
                 operating_base_lat, operating_base_lng)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (name, website, phone, notes, obl, obp, obla, obln),
            )
            conn.commit()
            new_id = int(cur.lastrowid)
            flash("Account created.", "success")
            return redirect(url_for("crm_module.account_edit", account_id=new_id))
        finally:
            cur.close()
            conn.close()
    return render_template(
        "admin/crm_account_form.html",
        account=None,
        can_edit=True,
        w3w_resolve_url=url_for("crm_module.opportunity_intake_resolve_w3w"),
    )


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
                "admin/crm_account_form.html",
                account=row,
                can_edit=True,
                w3w_resolve_url=url_for("crm_module.opportunity_intake_resolve_w3w"),
            )
        website = (request.form.get("website") or "").strip() or None
        phone = (request.form.get("phone") or "").strip() or None
        notes = (request.form.get("notes") or "").strip() or None
        obl, obp, obla, obln = _operating_base_from_form()
        conn = get_db_connection()
        cur2 = conn.cursor()
        try:
            cur2.execute(
                """UPDATE crm_accounts SET name=%s, website=%s, phone=%s, notes=%s,
                operating_base_label=%s, operating_base_postcode=%s,
                operating_base_lat=%s, operating_base_lng=%s WHERE id=%s""",
                (name, website, phone, notes, obl, obp, obla, obln, account_id),
            )
            conn.commit()
            flash("Account updated.", "success")
            return redirect(url_for("crm_module.account_edit", account_id=account_id))
        finally:
            cur2.close()
            conn.close()
    return render_template(
        "admin/crm_account_form.html",
        account=row,
        can_edit=True,
        w3w_resolve_url=url_for("crm_module.opportunity_intake_resolve_w3w"),
    )


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


def _intake_account_search_rows(q_like: str) -> list[dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, name FROM crm_accounts WHERE name LIKE %s ORDER BY name LIMIT 30",
            (q_like,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


@crm_bp.route("/opportunities/intake/accounts-search", methods=["GET"])
@login_required
@crm_access_required
def opportunity_intake_accounts_search():
    raw = (request.args.get("q") or "").strip()
    q = _crm_search_like_term(raw)
    if len(q) < 2:
        return jsonify({"ok": True, "accounts": []})
    rows = _intake_account_search_rows(f"%{q}%")
    return jsonify({"ok": True, "accounts": rows})


@crm_bp.route("/opportunities/intake/resolve-w3w", methods=["POST"])
@login_required
@crm_access_required
def opportunity_intake_resolve_w3w():
    words = (request.form.get("words") or "").strip()
    if not words:
        return jsonify({"ok": False, "error": "Enter a what3words address"}), 400
    result = resolve_w3w_to_latlng(words)
    if "error" in result:
        code = 503 if "not installed" in (result.get("error") or "") else 400
        return jsonify({"ok": False, "error": result["error"]}), code
    return jsonify(
        {
            "ok": True,
            "lat": result["lat"],
            "lng": result["lng"],
            "normalized": result.get("normalized", words),
        }
    )


@crm_bp.route("/opportunities/intake/account-summary", methods=["GET"])
@login_required
@crm_access_required
def opportunity_intake_account_summary():
    raw = (request.args.get("account_id") or "").strip()
    if not raw.isdigit():
        return jsonify({"ok": False, "error": "account_id required"}), 400
    account_id = int(raw)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id FROM crm_accounts WHERE id=%s", (account_id,))
        if not cur.fetchone():
            return jsonify({"ok": False, "error": "not_found"}), 404
        cur.execute(
            """
            SELECT id, name, stage, updated_at, lead_meta_json
            FROM crm_opportunities
            WHERE account_id=%s
            ORDER BY updated_at DESC
            LIMIT 8
            """,
            (account_id,),
        )
        rows = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    out_rows: list[dict[str, Any]] = []
    for r in rows:
        snippet = ""
        raw_meta = r.get("lead_meta_json")
        if raw_meta:
            try:
                if isinstance(raw_meta, (bytes, bytearray)):
                    raw_meta = raw_meta.decode("utf-8", errors="replace")
                meta = json.loads(raw_meta) if isinstance(raw_meta, str) else raw_meta
                if isinstance(meta, dict):
                    ri = meta.get("risk") if isinstance(meta.get("risk"), dict) else {}
                    lbl = ri.get("label")
                    if lbl:
                        snippet = f"Guide risk band on file: {lbl}"
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        dt = r.get("updated_at")
        out_rows.append(
            {
                "id": r["id"],
                "name": r["name"],
                "stage": r["stage"],
                "updated_at": dt.isoformat() if hasattr(dt, "isoformat") else None,
                "snippet": snippet,
            }
        )
    return jsonify({"ok": True, "opportunities": out_rows})


@crm_bp.route("/opportunities/intake/dispatch-bases", methods=["GET"])
@login_required
@crm_access_required
def opportunity_intake_dispatch_bases():
    """Tenant dispatch departure points for intake mileage (dropdown when more than one)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, label, postcode, lat, lng FROM crm_dispatch_bases "
            "ORDER BY sort_order ASC, id ASC"
        )
        rows = cur.fetchall() or []
    except Exception:
        rows = []
    finally:
        cur.close()
        conn.close()
    return jsonify({"ok": True, "bases": rows})


@crm_bp.route("/opportunities/intake/saved-venues", methods=["GET"])
@login_required
@crm_access_required
def opportunity_intake_saved_venues():
    raw = (request.args.get("account_id") or "").strip()
    if not raw.isdigit():
        return jsonify({"ok": True, "venues": []})
    account_id = int(raw)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id FROM crm_accounts WHERE id=%s", (account_id,))
        if not cur.fetchone():
            return jsonify({"ok": False, "error": "not_found"}), 404
        cur.execute(
            """
            SELECT id, label, venue_address, venue_postcode, venue_what3words,
                   venue_lat, venue_lng
            FROM crm_account_saved_venues
            WHERE account_id=%s
            ORDER BY label ASC, id ASC
            """,
            (account_id,),
        )
        rows = cur.fetchall() or []
    except Exception:
        rows = []
    finally:
        cur.close()
        conn.close()
    return jsonify({"ok": True, "venues": rows})


@crm_bp.route("/opportunities/intake/save-venue", methods=["POST"])
@login_required
@crm_access_required
def opportunity_intake_save_venue():
    """Persist current venue fields as a reusable template on the linked CRM account."""
    if not can_edit():
        return jsonify({"ok": False, "error": "read_only"}), 403
    aid_raw = (request.form.get("account_id") or "").strip()
    if not aid_raw.isdigit():
        return jsonify({"ok": False, "error": "account_id required"}), 400
    account_id = int(aid_raw)
    label = (request.form.get("label") or "").strip()
    if not label:
        return jsonify({"ok": False, "error": "label required"}), 400
    va = (request.form.get("venue_address") or "").strip() or None
    vpc = (request.form.get("venue_postcode") or "").strip() or None
    vw3w = (request.form.get("venue_what3words") or "").strip() or None
    lat_r = (request.form.get("venue_lat") or "").strip()
    lng_r = (request.form.get("venue_lng") or "").strip()
    lat = lng = None
    if lat_r and lng_r:
        try:
            lat = float(lat_r)
            lng = float(lng_r)
            if not (-90 <= lat <= 90 and -180 <= lng <= 180):
                lat = lng = None
        except (TypeError, ValueError):
            lat = lng = None
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM crm_accounts WHERE id=%s", (account_id,))
        if not cur.fetchone():
            return jsonify({"ok": False, "error": "not_found"}), 404
        cur.execute(
            """
            INSERT INTO crm_account_saved_venues
            (account_id, label, venue_address, venue_postcode, venue_what3words, venue_lat, venue_lng)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                account_id,
                label[:255],
                va[:512] if va else None,
                vpc[:32] if vpc else None,
                vw3w[:128] if vw3w else None,
                lat,
                lng,
            ),
        )
        conn.commit()
        new_id = int(cur.lastrowid)
    finally:
        cur.close()
        conn.close()
    return jsonify({"ok": True, "id": new_id})


@crm_bp.route("/opportunities/intake", methods=["GET", "POST"])
@login_required
@crm_access_required
def opportunity_intake():
    """Staff / call-centre intake — same payload shape as the public calculator."""

    app = current_app._get_current_object()
    event_intake_hidden, event_intake_extra_fields = load_event_intake_layout(app)

    def _intake_extra_prefill() -> dict[str, str]:
        if request.method != "POST":
            return {
                str(fd.get("key") or ""): ""
                for fd in event_intake_extra_fields
                if fd.get("key")
            }
        return {
            str(fd["key"]): (request.form.get(f"intake_extra_{fd['key']}") or "")
            for fd in event_intake_extra_fields
            if fd.get("key")
        }

    form_values = {}
    if request.method == "POST":
        if not can_edit():
            flash("You do not have permission to create pipeline records.", "danger")
            return redirect(url_for("crm_module.opportunities_board"))
        miss_ex = validate_required_extras(request.form, event_intake_extra_fields)
        if miss_ex:
            flash(
                "Please complete required extra fields: " + ", ".join(miss_ex) + ".",
                "danger",
            )
            form_values = calculator_template_values(
                request.form,
                extra={
                    "account_id": (request.form.get("account_id") or "").strip(),
                    "stage": (request.form.get("stage") or "prospecting").strip(),
                },
            )
            prefill_account_name = None
            aid = (form_values.get("account_id") or "").strip()
            if aid.isdigit():
                c2 = get_db_connection()
                c2c = c2.cursor()
                try:
                    c2c.execute("SELECT name FROM crm_accounts WHERE id=%s", (int(aid),))
                    row = c2c.fetchone()
                    if row:
                        prefill_account_name = row[0]
                finally:
                    c2c.close()
                    c2.close()
            return render_template(
                "admin/crm_opportunity_intake.html",
                stages=OPP_STAGES,
                can_edit=can_edit(),
                form_values=form_values,
                prefill_account_name=prefill_account_name,
                tier_resource_rows=public_tier_resource_rows(),
                tier_medic_base=dict(TIER_MEDIC_BASE),
                tier_vehicle_base=dict(TIER_VEHICLE_BASE),
                intake_preview_url=url_for("crm_module.opportunity_intake_preview"),
                intake_accounts_search_url=url_for(
                    "crm_module.opportunity_intake_accounts_search"
                ),
                intake_resolve_w3w_url=url_for("crm_module.opportunity_intake_resolve_w3w"),
                intake_account_summary_url=url_for(
                    "crm_module.opportunity_intake_account_summary"
                ),
                intake_dispatch_bases_url=url_for(
                    "crm_module.opportunity_intake_dispatch_bases"
                ),
                intake_saved_venues_url=url_for(
                    "crm_module.opportunity_intake_saved_venues"
                ),
                intake_save_venue_url=url_for("crm_module.opportunity_intake_save_venue"),
                tb_wage_cards_url=_maybe_tb_wage_cards_url(),
                event_intake_hidden=event_intake_hidden,
                event_intake_extra_fields=event_intake_extra_fields,
                intake_extra_prefill=_intake_extra_prefill(),
            )
        parsed = parse_public_calculator_form(request.form)
        acc_raw = (request.form.get("account_id") or "").strip()
        acc_override = int(acc_raw) if acc_raw.isdigit() else None
        stage = (request.form.get("stage") or "prospecting").strip()
        if stage not in {s for s, _ in OPP_STAGES}:
            stage = "prospecting"
        extras_rows = collect_intake_extras_from_form(
            request.form, event_intake_extra_fields
        )
        meta_add: dict[str, Any] | None = (
            {"event_intake_extras": extras_rows} if event_intake_extra_fields else None
        )
        try:
            opp_id, _cid, _meta = intake_from_parsed_form(
                parsed,
                source="staff_intake",
                stage=stage,
                account_id_override=acc_override,
                lead_meta_additions=meta_add,
            )
            flash(f"Opportunity #{opp_id} created.", "success")
            return redirect(opportunity_edit_url(opp_id=int(opp_id), flow=True))
        except LeadIntakeMissingRequiredFields:
            flash("Organisation, contact, email, and event name are required.", "danger")
        except LeadIntakeInvalidAccount:
            flash("That CRM account was not found.", "danger")
        except Exception as ex:
            flash(f"Could not create opportunity: {ex}", "danger")
        form_values = calculator_template_values(
            request.form,
            extra={
                "account_id": (request.form.get("account_id") or "").strip(),
                "stage": (request.form.get("stage") or "prospecting").strip(),
            },
        )

    prefill_account_name = None
    aid = (form_values.get("account_id") or "").strip()
    if aid.isdigit():
        c2 = get_db_connection()
        c2c = c2.cursor()
        try:
            c2c.execute("SELECT name FROM crm_accounts WHERE id=%s", (int(aid),))
            row = c2c.fetchone()
            if row:
                prefill_account_name = row[0]
        finally:
            c2c.close()
            c2.close()

    return render_template(
        "admin/crm_opportunity_intake.html",
        stages=OPP_STAGES,
        can_edit=can_edit(),
        form_values=form_values,
        prefill_account_name=prefill_account_name,
        tier_resource_rows=public_tier_resource_rows(),
        tier_medic_base=dict(TIER_MEDIC_BASE),
        tier_vehicle_base=dict(TIER_VEHICLE_BASE),
        intake_preview_url=url_for("crm_module.opportunity_intake_preview"),
        intake_accounts_search_url=url_for("crm_module.opportunity_intake_accounts_search"),
        intake_resolve_w3w_url=url_for("crm_module.opportunity_intake_resolve_w3w"),
        intake_account_summary_url=url_for("crm_module.opportunity_intake_account_summary"),
        intake_dispatch_bases_url=url_for("crm_module.opportunity_intake_dispatch_bases"),
        intake_saved_venues_url=url_for("crm_module.opportunity_intake_saved_venues"),
        intake_save_venue_url=url_for("crm_module.opportunity_intake_save_venue"),
        tb_wage_cards_url=_maybe_tb_wage_cards_url(),
        event_intake_hidden=event_intake_hidden,
        event_intake_extra_fields=event_intake_extra_fields,
        intake_extra_prefill=_intake_extra_prefill(),
    )


@crm_bp.route("/opportunities/intake-private-transfer", methods=["GET", "POST"])
@login_required
@crm_access_required
def opportunity_intake_private_transfer():
    """Staff intake for private patient transfer / non-event (bookAmbulance-style fields; no Cura)."""
    app = current_app._get_current_object()
    pt_intake_hidden, pt_intake_extra_fields = load_private_transfer_intake_layout(app)

    def _pt_intake_extra_prefill() -> dict[str, str]:
        if request.method != "POST":
            return {
                str(fd.get("key") or ""): ""
                for fd in pt_intake_extra_fields
                if fd.get("key")
            }
        return {
            str(fd["key"]): (request.form.get(f"intake_extra_{fd['key']}") or "")
            for fd in pt_intake_extra_fields
            if fd.get("key")
        }

    form_values: dict[str, Any] = {}
    if request.method == "POST":
        if not can_edit():
            flash("You do not have permission to create pipeline records.", "danger")
            return redirect(url_for("crm_module.opportunities_board"))
        miss_ex = validate_required_extras(request.form, pt_intake_extra_fields)
        if miss_ex:
            flash(
                "Please complete required extra fields: " + ", ".join(miss_ex) + ".",
                "danger",
            )
            form_values = {}
            for k in request.form.keys():
                if k == "infectious[]":
                    continue
                form_values[k] = request.form.get(k) or ""
            form_values["infectious_sel"] = request.form.getlist("infectious[]")
            accounts = _accounts_select_options()
            return render_template(
                "admin/crm_opportunity_intake_private_transfer.html",
                stages=OPP_STAGES,
                can_edit=can_edit(),
                form_values=form_values,
                intake_private_accounts=accounts,
                tb_wage_cards_url=_maybe_tb_wage_cards_url(),
                pt_intake_hidden=pt_intake_hidden,
                pt_intake_extra_fields=pt_intake_extra_fields,
                intake_extra_prefill=_pt_intake_extra_prefill(),
            )
        parsed = parse_private_transfer_form(request.form)
        acc_raw = (request.form.get("account_id") or "").strip()
        acc_override = int(acc_raw) if acc_raw.isdigit() else None
        stage = (request.form.get("stage") or "prospecting").strip()
        if stage not in {s for s, _ in OPP_STAGES}:
            stage = "prospecting"
        meta_add: dict[str, Any] | None = None
        if pt_intake_extra_fields:
            meta_add = {
                "private_transfer_intake_extras": collect_intake_extras_from_form(
                    request.form, pt_intake_extra_fields
                )
            }
        try:
            opp_id, _cid, _meta = intake_from_parsed_form(
                parsed,
                source="staff_intake_private_transfer",
                stage=stage,
                account_id_override=acc_override,
                lead_meta_additions=meta_add,
            )
            flash(
                f"Opportunity #{opp_id} created (private transfer). "
                "Use Scheduling on the opportunity to roster work in job view.",
                "success",
            )
            return redirect(opportunity_edit_url(opp_id=int(opp_id), flow=True))
        except LeadIntakeMissingRequiredFields:
            flash(
                "Applicant name, email, payee / organisation, and patient + journey fields are required.",
                "danger",
            )
        except LeadIntakeInvalidAccount:
            flash("That CRM account was not found.", "danger")
        except Exception as ex:
            flash(f"Could not create opportunity: {ex}", "danger")
        form_values: dict[str, Any] = {}
        for k in request.form.keys():
            if k == "infectious[]":
                continue
            form_values[k] = request.form.get(k) or ""
        form_values["infectious_sel"] = request.form.getlist("infectious[]")

    accounts = _accounts_select_options()
    return render_template(
        "admin/crm_opportunity_intake_private_transfer.html",
        stages=OPP_STAGES,
        can_edit=can_edit(),
        form_values=form_values,
        intake_private_accounts=accounts,
        tb_wage_cards_url=_maybe_tb_wage_cards_url(),
        pt_intake_hidden=pt_intake_hidden,
        pt_intake_extra_fields=pt_intake_extra_fields,
        intake_extra_prefill=_pt_intake_extra_prefill(),
    )


@crm_bp.route("/opportunities/<int:opp_id>/schedule-sync", methods=["POST"])
@login_required
@crm_access_required
@crm_edit_required
def opportunity_schedule_sync(opp_id: int):
    from datetime import datetime as dt_mod

    from .crm_scheduling_bridge import scheduling_stack_available, sync_opportunity_to_schedule_shift

    if not scheduling_stack_available():
        flash("Scheduling module or schedule_shifts table is not available.", "warning")
        return redirect(
            opportunity_edit_url(
                opp_id=opp_id,
                flow=(request.form.get("crm_flow_active") or "").strip() == "1",
            )
        )
    cid_raw = (request.form.get("schedule_client_id") or "").strip()
    jt_raw = (request.form.get("schedule_job_type_id") or "").strip()
    wd_raw = (request.form.get("schedule_work_date") or "").strip()
    st_raw = (request.form.get("schedule_start") or "").strip()
    en_raw = (request.form.get("schedule_end") or "").strip()
    br_raw = (request.form.get("schedule_break_mins") or "0").strip()
    rc_raw = (request.form.get("schedule_required_count") or "1").strip()
    status = (request.form.get("schedule_status") or "draft").strip()
    site_raw = (request.form.get("schedule_site_id") or "").strip()
    if not cid_raw.isdigit() or not jt_raw.isdigit():
        flash("Client and job type are required for scheduling.", "danger")
        return redirect(
            opportunity_edit_url(
                opp_id=opp_id,
                flow=(request.form.get("crm_flow_active") or "").strip() == "1",
            )
        )
    try:
        work_date = dt_mod.strptime(wd_raw, "%Y-%m-%d").date()
        scheduled_start = dt_mod.strptime(st_raw, "%H:%M").time()
        scheduled_end = dt_mod.strptime(en_raw, "%H:%M").time()
    except ValueError:
        flash("Work date must be YYYY-MM-DD and times HH:MM (24h).", "danger")
        return redirect(
            opportunity_edit_url(
                opp_id=opp_id,
                flow=(request.form.get("crm_flow_active") or "").strip() == "1",
            )
        )
    site_id = int(site_raw) if site_raw.isdigit() else None
    try:
        break_mins = max(0, int(br_raw))
    except ValueError:
        break_mins = 0
    try:
        required_count = max(1, int(rc_raw))
    except ValueError:
        required_count = 1
    actor_uid = None
    actor_uname = None
    if getattr(current_user, "is_authenticated", False):
        try:
            actor_uid = int(getattr(current_user, "id", 0) or 0) or None
        except (TypeError, ValueError):
            actor_uid = None
        actor_uname = (
            str(getattr(current_user, "username", "") or "").strip()
            or str(getattr(current_user, "email", "") or "").strip()
            or None
        )
    res = sync_opportunity_to_schedule_shift(
        opportunity_id=opp_id,
        client_id=int(cid_raw),
        job_type_id=int(jt_raw),
        work_date=work_date,
        scheduled_start=scheduled_start,
        scheduled_end=scheduled_end,
        site_id=site_id,
        break_mins=break_mins,
        required_count=required_count,
        status=status or "draft",
        actor_user_id=actor_uid,
        actor_username=actor_uname,
    )
    if res.get("ok"):
        flash(
            "Scheduling shift saved. Open Shifts / roster to assign staff — this path does not use Cura.",
            "success",
        )
    else:
        flash(res.get("error") or "Could not save scheduling shift.", "danger")
    keep_flow = (request.form.get("crm_flow_active") or "").strip() == "1"
    nxt = (request.form.get("crm_flow_next") or "").strip().lower()
    if res.get("ok") and nxt == "quote":
        return redirect(quote_new_url(opportunity_id=int(opp_id), flow=True))
    return redirect(opportunity_edit_url(opp_id=opp_id, flow=keep_flow))


@crm_bp.route("/opportunities/intake-preview", methods=["POST"])
@login_required
@crm_access_required
def opportunity_intake_preview():
    """JSON tier + rough wage cost for call-centre intake (same pipeline logic as create)."""
    parsed = parse_public_calculator_form(request.form)
    risk = compute_event_risk_from_parsed(parsed)
    lm = build_lead_meta(source="intake_preview", parsed=parsed, risk=risk)
    lm = enrich_lead_meta_with_staffing_breakdown(lm)
    cost = estimate_guide_staffing_costs(lm) or {}
    pg = risk.get("purple_guide") or {}
    acc_raw = (request.form.get("account_id") or "").strip()
    acc_id = int(acc_raw) if acc_raw.isdigit() else None
    routing_hint = None
    conn_r = get_db_connection()
    try:
        routing_hint = routing_hint_for_intake(
            conn_r,
            dispatch_base_id=parsed.get("dispatch_base_id"),
            account_id=acc_id,
            venue=lm.get("venue") if isinstance(lm.get("venue"), dict) else None,
        )
    except Exception:
        routing_hint = None
    finally:
        conn_r.close()
    return jsonify(
        {
            "ok": True,
            "purple_score": pg.get("purple_score"),
            "tier": pg.get("tier"),
            "tier_title": pg.get("tier_title"),
            "tier_summary": ((pg.get("tier_summary") or "")[:400]),
            "conversion_row": pg.get("conversion_row"),
            "band_label": risk.get("label"),
            "suggested_medics": risk.get("suggested_medics"),
            "suggested_vehicles": risk.get("suggested_vehicles"),
            "customer_resource_matrix": risk.get("customer_resource_matrix"),
            "cost": cost,
            "routing_hint": routing_hint,
        }
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
                **_crm_scheduling_form_context(None, None),
                **_pipeline_flow_template_kwargs(
                    request, step="opportunity", opp_id=None
                ),
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
            nxt = (request.form.get("crm_flow_next") or "stay").strip().lower()
            if nxt == "quote":
                return redirect(quote_new_url(opportunity_id=new_id, flow=True))
            if nxt == "plan":
                if crm_medical_surface_available():
                    flash(
                        "Create a quote first, then start the event plan from the quote.",
                        "info",
                    )
                    return redirect(quote_new_url(opportunity_id=new_id, flow=True))
                flash(
                    "Event plans are available when Medical is enabled for this tenant.",
                    "info",
                )
            keep = (request.form.get("crm_flow_active") or "").strip() == "1"
            return redirect(opportunity_edit_url(opp_id=new_id, flow=keep or flow_active(request)))
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
        **_crm_scheduling_form_context(None, None),
        **_pipeline_flow_template_kwargs(request, step="opportunity", opp_id=None),
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
                **_crm_scheduling_form_context(row, lm),
                **_pipeline_flow_template_kwargs(
                    request, step="opportunity", opp_id=opp_id
                ),
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
            nxt = (request.form.get("crm_flow_next") or "stay").strip().lower()
            if nxt == "quote":
                return redirect(quote_new_url(opportunity_id=opp_id, flow=True))
            if nxt == "plan":
                if crm_medical_surface_available():
                    flash(
                        "Create a quote first, then start the event plan from the quote.",
                        "info",
                    )
                    return redirect(quote_new_url(opportunity_id=opp_id, flow=True))
                flash(
                    "Event plans are available when Medical is enabled for this tenant.",
                    "info",
                )
            keep = (request.form.get("crm_flow_active") or "").strip() == "1"
            return redirect(opportunity_edit_url(opp_id=opp_id, flow=keep))
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
        **_crm_scheduling_form_context(row, lm),
        **_pipeline_flow_template_kwargs(request, step="opportunity", opp_id=opp_id),
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
from .planner_routes import register_crm_event_planner_legacy_redirects  # noqa: E402
from .crm_settings_routes import register_crm_settings_routes  # noqa: E402
from .crm_intake_forms_routes import register_crm_intake_form_routes  # noqa: E402

register_crm_quote_routes(crm_bp)
register_crm_activities_routes(crm_bp)
register_crm_event_planner_legacy_redirects(crm_bp)
register_crm_settings_routes(crm_bp)
register_crm_intake_form_routes(crm_bp)


@crm_bp.context_processor
def _crm_cross_module_nav():
    """Expose Cura ops hub URL when medical_records internal blueprint is registered."""
    from .crm_cura_links import safe_cura_ops_hub_url

    return {
        "crm_cura_ops_hub_url": safe_cura_ops_hub_url(),
        "crm_can_edit": can_edit(),
    }


def get_blueprints():
    """Admin ERP app only (`register_admin_routes`). Public calculator is `get_public_blueprints`."""
    return [crm_bp]


def get_public_blueprints():
    """Website / public host + admin app second pass (`register_public_routes`): /event-risk-calculator, /quoting."""
    from .crm_public_routes import crm_public_bp

    return [crm_public_bp]
