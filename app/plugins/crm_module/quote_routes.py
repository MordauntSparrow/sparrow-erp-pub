# CRM R2 — quote rule sets, quotes, calculator, status history
from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from itertools import zip_longest

from flask import flash, redirect, render_template, request, url_for
from flask_login import login_required

from app.objects import get_db_connection

from .crm_common import can_edit, crm_access_required, crm_edit_required, uid
from .crm_event_risk import enrich_lead_meta_with_staffing_breakdown
from .crm_guide_staffing_costs import estimate_guide_staffing_costs
from .crm_event_plan_links import fetch_plans_for_quote
from .crm_quote_services import compute_quote_total
from .crm_rule_conditions import build_conditions_from_form

RULE_TYPES = (
    ("fixed_add", "Fixed add-on (£)"),
    ("per_head", "Per head (rate × crowd size)"),
    ("per_hour", "Per hour (rate × duration hours)"),
    ("percent_surcharge", "Percent surcharge (% of running subtotal)"),
    ("minimum_charge", "Minimum charge (floor £)"),
    ("vat", "VAT (% of running subtotal before this line)"),
)

QUOTE_STATUSES = (
    ("draft", "Draft"),
    ("sent", "Sent"),
    ("accepted", "Accepted"),
    ("lost", "Lost"),
    ("rejected", "Rejected"),
)

STATUS_LABELS = dict(QUOTE_STATUSES)


def _tb_wage_cards_url() -> str | None:
    try:
        return url_for("internal_time_billing.wage_cards_page")
    except Exception:
        return None


def _staffing_sidebar_for_opportunity(conn, opportunity_id: int | None):
    """Context for quote sidebar: link to opportunity + guide / wage hint."""
    if not opportunity_id:
        return None
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, lead_meta_json FROM crm_opportunities WHERE id=%s",
            (int(opportunity_id),),
        )
        row = cur.fetchone()
    finally:
        cur.close()
    if not row:
        return None
    lm = row.get("lead_meta_json")
    if isinstance(lm, str) and lm.strip():
        try:
            lm = json.loads(lm)
        except json.JSONDecodeError:
            lm = {}
    elif not isinstance(lm, dict):
        lm = {}
    enriched = enrich_lead_meta_with_staffing_breakdown(lm)
    est = estimate_guide_staffing_costs(enriched)
    return {
        "opportunity_id": int(row["id"]),
        "lead_meta": enriched,
        "guide_cost_estimate": est,
    }


def _parse_decimal(s: str | None, default: str = "0") -> Decimal:
    t = (s or "").strip()
    if not t:
        t = default
    try:
        return Decimal(t)
    except InvalidOperation:
        return Decimal(default)


def _optional_duration_hours(raw: str | None) -> Decimal | None:
    t = (raw or "").strip()
    if not t:
        return None
    try:
        return Decimal(t)
    except InvalidOperation:
        return None


def _quote_prefill_from_opportunity(opp_id: int) -> dict | None:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """SELECT id, account_id, name, lead_meta_json FROM crm_opportunities WHERE id=%s""",
            (opp_id,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if not row:
        return None
    lm = row.get("lead_meta_json")
    if isinstance(lm, str) and lm.strip():
        try:
            lm = json.loads(lm)
        except json.JSONDecodeError:
            lm = {}
    elif not isinstance(lm, dict):
        lm = {}
    ea = lm.get("expected_attendees")
    crowd = int(ea) if ea is not None and str(ea).strip().isdigit() else None
    dh_str = (lm.get("duration_hours") or "").strip()
    duration_hours = None
    if dh_str:
        try:
            duration_hours = Decimal(dh_str)
        except InvalidOperation:
            duration_hours = None
    title = (lm.get("event_name") or row.get("name") or "Quote").strip()[:255]
    notes_lines: list[str] = []
    src = lm.get("source")
    if src:
        notes_lines.append(f"Lead source: {src}")
    r = lm.get("risk") or {}
    if r.get("label"):
        notes_lines.append(
            f"Risk band (guide): {r.get('label')} (score {r.get('score', '—')})"
        )
    if lm.get("message"):
        notes_lines.append(f"Organiser note: {lm.get('message')}")
    internal_notes = "\n".join(notes_lines) if notes_lines else None
    return {
        "opportunity_id": int(row["id"]),
        "account_id": row.get("account_id"),
        "title": title,
        "crowd_size": crowd,
        "duration_hours": duration_hours,
        "internal_notes": internal_notes,
    }


def _pad_line_rows(rows: list[dict], minimum: int = 5) -> list[dict]:
    out = []
    for r in rows:
        out.append(dict(r))
    while len(out) < minimum:
        out.append(
            {
                "description": "",
                "quantity": Decimal("1"),
                "unit_price": Decimal("0"),
            }
        )
    return out


def _parse_lines_from_form() -> list[dict]:
    descs = request.form.getlist("line_description[]")
    qtys = request.form.getlist("line_quantity[]")
    prices = request.form.getlist("line_unit_price[]")
    rows = []
    so = 0
    for d, q, p in zip_longest(descs, qtys, prices, fillvalue=""):
        desc = (d or "").strip()
        if not desc:
            continue
        try:
            qty = Decimal(str(q or "1").strip() or "1")
        except InvalidOperation:
            qty = Decimal("1")
        try:
            price = Decimal(str(p or "0").strip() or "0")
        except InvalidOperation:
            price = Decimal("0")
        rows.append(
            {
                "sort_order": so,
                "description": desc[:512],
                "quantity": qty,
                "unit_price": price,
            }
        )
        so += 1
    return rows


def _replace_line_items(conn, quote_id: int, items: list[dict]) -> None:
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM crm_quote_line_items WHERE quote_id = %s", (quote_id,))
        for it in items:
            cur.execute(
                """INSERT INTO crm_quote_line_items
                (quote_id, sort_order, description, quantity, unit_price)
                VALUES (%s,%s,%s,%s,%s)""",
                (
                    quote_id,
                    it["sort_order"],
                    it["description"],
                    str(it["quantity"]),
                    str(it["unit_price"]),
                ),
            )
        conn.commit()
    finally:
        cur.close()


def _load_lines(conn, quote_id: int) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """SELECT id, sort_order, description, quantity, unit_price
            FROM crm_quote_line_items WHERE quote_id = %s ORDER BY sort_order, id""",
            (quote_id,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()


def _load_rules(conn, rule_set_id: int) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """SELECT id, sort_order, rule_type, amount, rate, percent, label, conditions_json
            FROM crm_quote_rules WHERE rule_set_id = %s ORDER BY sort_order, id""",
            (rule_set_id,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()


def _ensure_quote_group(conn, qid: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE crm_quotes SET quote_group_id=%s WHERE id=%s AND (quote_group_id IS NULL OR quote_group_id=0)",
            (qid, qid),
        )
        conn.commit()
    finally:
        cur.close()


def _quote_group_id_for(conn, quote_row: dict) -> int:
    g = quote_row.get("quote_group_id")
    if g is not None and int(g) > 0:
        return int(g)
    return int(quote_row["id"])


def _list_quote_revisions(conn, quote_row: dict) -> list[dict]:
    gid = _quote_group_id_for(conn, quote_row)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, revision, title, status, updated_at
            FROM crm_quotes
            WHERE quote_group_id = %s OR id = %s
            ORDER BY revision ASC, id ASC
            """,
            (gid, gid),
        )
        return cur.fetchall() or []
    finally:
        cur.close()


def _duplicate_quote_as_revision(conn, source_id: int) -> int | None:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM crm_quotes WHERE id=%s", (source_id,))
        src = cur.fetchone()
        if not src:
            return None
        gid = int(src.get("quote_group_id") or src["id"])
        if src.get("quote_group_id") is None:
            cur2 = conn.cursor()
            cur2.execute(
                "UPDATE crm_quotes SET quote_group_id=%s WHERE id=%s",
                (gid, src["id"]),
            )
            conn.commit()
            cur2.close()
        new_rev = int(src.get("revision") or 1) + 1
        cur2 = conn.cursor()
        cur2.execute(
            """INSERT INTO crm_quotes
            (title, account_id, opportunity_id, rule_set_id, status, revision,
             quote_group_id, parent_quote_id, crowd_size, duration_hours, internal_notes,
             total_amount, created_by)
            VALUES (%s,%s,%s,%s,'draft',%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                src.get("title") or "Quote",
                src.get("account_id"),
                src.get("opportunity_id"),
                src.get("rule_set_id"),
                new_rev,
                gid,
                source_id,
                src.get("crowd_size"),
                str(src["duration_hours"])
                if src.get("duration_hours") is not None
                else None,
                src.get("internal_notes"),
                str(src["total_amount"])
                if src.get("total_amount") is not None
                else None,
                uid(),
            ),
        )
        new_id = cur2.lastrowid
        cur.execute(
            """SELECT sort_order, description, quantity, unit_price
            FROM crm_quote_line_items WHERE quote_id=%s ORDER BY sort_order, id""",
            (source_id,),
        )
        lines = cur.fetchall() or []
        for ln in lines:
            cur2.execute(
                """INSERT INTO crm_quote_line_items
                (quote_id, sort_order, description, quantity, unit_price)
                VALUES (%s,%s,%s,%s,%s)""",
                (
                    new_id,
                    ln["sort_order"],
                    ln["description"],
                    str(ln["quantity"]),
                    str(ln["unit_price"]),
                ),
            )
        conn.commit()
        cur2.close()
        _append_status_history(
            conn,
            new_id,
            None,
            "draft",
            f"Revision {new_rev} (from quote #{source_id})",
        )
        return new_id
    finally:
        cur.close()


def _append_status_history(
    conn, quote_id: int, prev: str | None, new: str, note: str | None
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO crm_quote_status_history
            (quote_id, previous_status, new_status, changed_by, note)
            VALUES (%s,%s,%s,%s,%s)""",
            (quote_id, prev, new, uid(), (note or "").strip() or None),
        )
        conn.commit()
    finally:
        cur.close()


def register_crm_quote_routes(crm_bp):
    @crm_bp.route("/quote-rule-sets")
    @login_required
    @crm_access_required
    def quote_rule_sets_list():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """SELECT id, name, is_active, updated_at FROM crm_quote_rule_sets
                ORDER BY name ASC"""
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()
        return render_template(
            "admin/crm_quote_rule_sets_list.html",
            rule_sets=rows,
            can_edit=can_edit(),
        )

    @crm_bp.route("/quote-rule-sets/new", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def quote_rule_set_new():
        if request.method == "POST":
            name = (request.form.get("name") or "").strip()
            if not name:
                flash("Rule set name is required.", "danger")
                return render_template("admin/crm_quote_rule_set_form.html", rs=None)
            active = 1 if request.form.get("is_active") == "1" else 0
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    "INSERT INTO crm_quote_rule_sets (name, is_active) VALUES (%s,%s)",
                    (name, active),
                )
                conn.commit()
                rid = cur.lastrowid
                flash("Rule set created. Add rules below.", "success")
                return redirect(url_for("crm_module.quote_rule_set_edit", rs_id=rid))
            finally:
                cur.close()
                conn.close()
        return render_template("admin/crm_quote_rule_set_form.html", rs=None)

    @crm_bp.route("/quote-rule-sets/<int:rs_id>/edit", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def quote_rule_set_edit(rs_id: int):
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM crm_quote_rule_sets WHERE id = %s",
                (rs_id,),
            )
            rs = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        if not rs:
            flash("Rule set not found.", "danger")
            return redirect(url_for("crm_module.quote_rule_sets_list"))
        if request.method == "POST" and not can_edit():
            flash("You do not have permission to edit rule sets.", "danger")
            return redirect(url_for("crm_module.quote_rule_set_edit", rs_id=rs_id))
        if request.method == "POST" and can_edit():
            name = (request.form.get("name") or "").strip()
            if not name:
                flash("Name is required.", "danger")
            else:
                active = 1 if request.form.get("is_active") == "1" else 0
                conn = get_db_connection()
                cur2 = conn.cursor()
                try:
                    cur2.execute(
                        "UPDATE crm_quote_rule_sets SET name=%s, is_active=%s WHERE id=%s",
                        (name, active, rs_id),
                    )
                    conn.commit()
                    flash("Rule set updated.", "success")
                    return redirect(url_for("crm_module.quote_rule_set_edit", rs_id=rs_id))
                finally:
                    cur2.close()
                    conn.close()
        conn = get_db_connection()
        try:
            rules = _load_rules(conn, rs_id)
        finally:
            conn.close()
        return render_template(
            "admin/crm_quote_rule_set_edit.html",
            rs=rs,
            rules=rules,
            rule_types=RULE_TYPES,
            can_edit=can_edit(),
        )

    @crm_bp.route("/quote-rule-sets/<int:rs_id>/rules/add", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def quote_rule_add(rs_id: int):
        rt = (request.form.get("rule_type") or "").strip()
        allowed = {x[0] for x in RULE_TYPES}
        if rt not in allowed:
            flash("Invalid rule type.", "danger")
            return redirect(url_for("crm_module.quote_rule_set_edit", rs_id=rs_id))
        label = (request.form.get("label") or "").strip() or None
        amount = _parse_decimal(request.form.get("amount"), "0")
        rate = _parse_decimal(request.form.get("rate"), "0")
        percent = _parse_decimal(request.form.get("percent"), "0")
        cond_json = build_conditions_from_form(request.form)
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        cur2 = conn.cursor()
        try:
            cur.execute(
                "SELECT COALESCE(MAX(sort_order), -1) + 1 AS n FROM crm_quote_rules WHERE rule_set_id=%s",
                (rs_id,),
            )
            row = cur.fetchone()
            so = int(row["n"] if row else 0)
            cur2.execute(
                """INSERT INTO crm_quote_rules
                (rule_set_id, sort_order, rule_type, amount, rate, percent, label, conditions_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (rs_id, so, rt, str(amount), str(rate), str(percent), label, cond_json),
            )
            conn.commit()
            flash("Rule added.", "success")
        finally:
            cur2.close()
            cur.close()
            conn.close()
        return redirect(url_for("crm_module.quote_rule_set_edit", rs_id=rs_id))

    @crm_bp.route("/quote-rule-sets/<int:rs_id>/rules/<int:rule_id>/delete", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def quote_rule_delete(rs_id: int, rule_id: int):
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM crm_quote_rules WHERE id=%s AND rule_set_id=%s",
                (rule_id, rs_id),
            )
            conn.commit()
            flash("Rule removed.", "success")
        finally:
            cur.close()
            conn.close()
        return redirect(url_for("crm_module.quote_rule_set_edit", rs_id=rs_id))

    @crm_bp.route(
        "/quote-rule-sets/<int:rs_id>/rules/<int:rule_id>/conditions",
        methods=["POST"],
    )
    @login_required
    @crm_access_required
    @crm_edit_required
    def quote_rule_update_conditions(rs_id: int, rule_id: int):
        cond_json = build_conditions_from_form(request.form)
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """UPDATE crm_quote_rules SET conditions_json=%s
                WHERE id=%s AND rule_set_id=%s""",
                (cond_json, rule_id, rs_id),
            )
            conn.commit()
            flash("Rule conditions updated.", "success")
        finally:
            cur.close()
            conn.close()
        return redirect(url_for("crm_module.quote_rule_set_edit", rs_id=rs_id))

    @crm_bp.route("/quote-rule-sets/<int:rs_id>/rules/<int:rule_id>/move", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def quote_rule_move(rs_id: int, rule_id: int):
        direction = (request.form.get("direction") or "").strip()
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, sort_order FROM crm_quote_rules WHERE rule_set_id=%s ORDER BY sort_order, id",
                (rs_id,),
            )
            rows = cur.fetchall() or []
            ids = [r["id"] for r in rows]
            if rule_id not in ids:
                return redirect(url_for("crm_module.quote_rule_set_edit", rs_id=rs_id))
            idx = ids.index(rule_id)
            if direction == "up" and idx > 0:
                j = idx - 1
            elif direction == "down" and idx < len(ids) - 1:
                j = idx + 1
            else:
                return redirect(url_for("crm_module.quote_rule_set_edit", rs_id=rs_id))
            cur.execute(
                "SELECT sort_order FROM crm_quote_rules WHERE id=%s",
                (ids[idx],),
            )
            a = cur.fetchone()
            cur.execute(
                "SELECT sort_order FROM crm_quote_rules WHERE id=%s",
                (ids[j],),
            )
            b = cur.fetchone()
            so_a, so_b = int(a["sort_order"]), int(b["sort_order"])
            cur2 = conn.cursor()
            cur2.execute(
                "UPDATE crm_quote_rules SET sort_order=%s WHERE id=%s", (so_b, ids[idx])
            )
            cur2.execute(
                "UPDATE crm_quote_rules SET sort_order=%s WHERE id=%s", (so_a, ids[j])
            )
            conn.commit()
            cur2.close()
        finally:
            cur.close()
            conn.close()
        return redirect(url_for("crm_module.quote_rule_set_edit", rs_id=rs_id))

    @crm_bp.route("/quote-rule-sets/<int:rs_id>/test", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def quote_rule_set_test(rs_id: int):
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM crm_quote_rule_sets WHERE id=%s", (rs_id,))
            rs = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        if not rs:
            flash("Rule set not found.", "danger")
            return redirect(url_for("crm_module.quote_rule_sets_list"))
        conn = get_db_connection()
        try:
            rules = _load_rules(conn, rs_id)
        finally:
            conn.close()

        line_sub = Decimal("0")
        crowd = None
        hours = None
        if request.method == "POST":
            line_sub = _parse_decimal(request.form.get("lines_subtotal"), "0")
            crowd_raw = (request.form.get("crowd_size") or "").strip()
            crowd = int(crowd_raw) if crowd_raw.isdigit() else 0
            hours = _parse_decimal(request.form.get("duration_hours"), "0")
            total, breakdown = compute_quote_total(
                line_rows=(
                    [{"quantity": 1, "unit_price": line_sub}] if line_sub != 0 else []
                ),
                crowd_size=crowd,
                duration_hours=hours,
                rule_rows=rules,
            )
        else:
            total, breakdown = compute_quote_total(
                line_rows=[],
                crowd_size=None,
                duration_hours=None,
                rule_rows=rules,
            )

        return render_template(
            "admin/crm_quote_rule_set_test.html",
            rs=rs,
            rules=rules,
            rule_type_labels=dict(RULE_TYPES),
            line_sub=line_sub,
            crowd_size=crowd,
            duration_hours=hours,
            total=total,
            breakdown=breakdown,
        )

    @crm_bp.route("/quotes")
    @login_required
    @crm_access_required
    def quotes_list():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT q.id, q.title, q.status, q.total_amount, q.updated_at,
                       a.name AS account_name, o.name AS opp_name
                FROM crm_quotes q
                LEFT JOIN crm_accounts a ON a.id = q.account_id
                LEFT JOIN crm_opportunities o ON o.id = q.opportunity_id
                ORDER BY q.updated_at DESC
                LIMIT 200
                """
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()
        return render_template(
            "admin/crm_quotes_list.html",
            quotes=rows,
            status_labels=STATUS_LABELS,
            can_edit=can_edit(),
        )

    @crm_bp.route("/quotes/new", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def quote_new():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT id, name FROM crm_accounts ORDER BY name")
            accounts = cur.fetchall() or []
            cur.execute(
                """SELECT o.id, o.name, o.account_id, a.name AS account_name
                FROM crm_opportunities o JOIN crm_accounts a ON a.id = o.account_id
                ORDER BY a.name, o.name"""
            )
            opps = cur.fetchall() or []
            cur.execute(
                "SELECT id, name FROM crm_quote_rule_sets WHERE is_active=1 ORDER BY name"
            )
            rule_sets = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        quote_prefill = None
        qp_raw = (request.args.get("opportunity_id") or "").strip()
        if qp_raw.isdigit():
            quote_prefill = _quote_prefill_from_opportunity(int(qp_raw))

        if request.method == "POST":
            title = (request.form.get("title") or "").strip() or "Quote"
            acc_raw = (request.form.get("account_id") or "").strip()
            account_id = int(acc_raw) if acc_raw.isdigit() else None
            opp_raw = (request.form.get("opportunity_id") or "").strip()
            opportunity_id = int(opp_raw) if opp_raw.isdigit() else None
            rs_raw = (request.form.get("rule_set_id") or "").strip()
            rule_set_id = int(rs_raw) if rs_raw.isdigit() else None
            crowd_raw = (request.form.get("crowd_size") or "").strip()
            crowd_size = int(crowd_raw) if crowd_raw.isdigit() else None
            duration_hours = _optional_duration_hours(request.form.get("duration_hours"))
            notes = (request.form.get("internal_notes") or "").strip() or None
            lines = _parse_lines_from_form()
            rule_rows = []
            if rule_set_id:
                c2 = get_db_connection()
                try:
                    rule_rows = _load_rules(c2, rule_set_id)
                finally:
                    c2.close()
            total, _ = compute_quote_total(
                line_rows=lines,
                crowd_size=crowd_size,
                duration_hours=duration_hours,
                rule_rows=rule_rows,
            )

            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """INSERT INTO crm_quotes
                    (title, account_id, opportunity_id, rule_set_id, status, crowd_size,
                     duration_hours, internal_notes, total_amount, created_by)
                    VALUES (%s,%s,%s,%s,'draft',%s,%s,%s,%s,%s)""",
                    (
                        title,
                        account_id,
                        opportunity_id,
                        rule_set_id,
                        crowd_size,
                        str(duration_hours) if duration_hours is not None else None,
                        notes,
                        str(total),
                        uid(),
                    ),
                )
                qid = cur.lastrowid
                _replace_line_items(conn, qid, lines)
                _ensure_quote_group(conn, qid)
                _append_status_history(conn, qid, None, "draft", None)
                conn.commit()
                flash("Quote created.", "success")
                return redirect(url_for("crm_module.quote_edit", quote_id=qid))
            finally:
                cur.close()
                conn.close()

        crowd_p = quote_prefill.get("crowd_size") if quote_prefill else None
        dh_p = quote_prefill.get("duration_hours") if quote_prefill else None
        t0, br0 = compute_quote_total(
            line_rows=[],
            crowd_size=crowd_p,
            duration_hours=dh_p,
            rule_rows=[],
        )
        ss_conn = get_db_connection()
        try:
            pre_oid = (quote_prefill or {}).get("opportunity_id")
            staffing_sidebar = (
                _staffing_sidebar_for_opportunity(ss_conn, pre_oid) if pre_oid else None
            )
        finally:
            ss_conn.close()
        return render_template(
            "admin/crm_quote_form.html",
            quote=None,
            lines=_pad_line_rows([], 5),
            history=[],
            revisions=[],
            linked_event_plans=[],
            accounts=accounts,
            opportunities=opps,
            rule_sets=rule_sets,
            statuses=QUOTE_STATUSES,
            status_labels=STATUS_LABELS,
            rule_types=RULE_TYPES,
            breakdown=br0,
            total=t0,
            quote_prefill=quote_prefill,
            staffing_sidebar=staffing_sidebar,
            tb_wage_cards_url=_tb_wage_cards_url(),
            can_edit=True,
        )

    @crm_bp.route("/quotes/<int:quote_id>/edit", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def quote_edit(quote_id: int):
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM crm_quotes WHERE id=%s", (quote_id,))
            quote = cur.fetchone()
            cur.execute("SELECT id, name FROM crm_accounts ORDER BY name")
            accounts = cur.fetchall() or []
            cur.execute(
                """SELECT o.id, o.name, o.account_id, a.name AS account_name
                FROM crm_opportunities o JOIN crm_accounts a ON a.id = o.account_id
                ORDER BY a.name, o.name"""
            )
            opps = cur.fetchall() or []
            cur.execute(
                """SELECT id, name, is_active FROM crm_quote_rule_sets ORDER BY name"""
            )
            rule_sets_all = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        if not quote:
            flash("Quote not found.", "danger")
            return redirect(url_for("crm_module.quotes_list"))

        if request.method == "POST" and not can_edit():
            flash("You do not have permission to edit quotes.", "danger")
            return redirect(url_for("crm_module.quote_edit", quote_id=quote_id))

        conn = get_db_connection()
        try:
            lines = _load_lines(conn, quote_id)
            revisions = _list_quote_revisions(conn, quote)
            hcur = conn.cursor(dictionary=True)
            hcur.execute(
                """SELECT previous_status, new_status, changed_by, note, created_at
                FROM crm_quote_status_history WHERE quote_id=%s ORDER BY created_at ASC, id ASC""",
                (quote_id,),
            )
            history = hcur.fetchall() or []
            hcur.close()
        finally:
            conn.close()

        rs_id = quote.get("rule_set_id")
        rule_rows = []
        if rs_id:
            c3 = get_db_connection()
            try:
                rule_rows = _load_rules(c3, int(rs_id))
            finally:
                c3.close()

        if request.method == "POST" and can_edit():
            prev_status = quote.get("status")
            title = (request.form.get("title") or "").strip() or "Quote"
            acc_raw = (request.form.get("account_id") or "").strip()
            account_id = int(acc_raw) if acc_raw.isdigit() else None
            opp_raw = (request.form.get("opportunity_id") or "").strip()
            opportunity_id = int(opp_raw) if opp_raw.isdigit() else None
            rs_raw = (request.form.get("rule_set_id") or "").strip()
            rule_set_id = int(rs_raw) if rs_raw.isdigit() else None
            crowd_raw = (request.form.get("crowd_size") or "").strip()
            crowd_size = int(crowd_raw) if crowd_raw.isdigit() else None
            duration_hours = _optional_duration_hours(
                request.form.get("duration_hours")
            )
            notes = (request.form.get("internal_notes") or "").strip() or None
            new_status = (request.form.get("status") or "draft").strip()
            allowed_s = {s for s, _ in QUOTE_STATUSES}
            if new_status not in allowed_s:
                new_status = "draft"
            status_note = (request.form.get("status_note") or "").strip() or None
            new_lines = _parse_lines_from_form()
            rr = []
            if rule_set_id:
                c4 = get_db_connection()
                try:
                    rr = _load_rules(c4, rule_set_id)
                finally:
                    c4.close()
            total, breakdown = compute_quote_total(
                line_rows=new_lines,
                crowd_size=crowd_size,
                duration_hours=duration_hours,
                rule_rows=rr,
            )

            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """UPDATE crm_quotes SET title=%s, account_id=%s, opportunity_id=%s,
                    rule_set_id=%s, status=%s, crowd_size=%s, duration_hours=%s,
                    internal_notes=%s, total_amount=%s WHERE id=%s""",
                    (
                        title,
                        account_id,
                        opportunity_id,
                        rule_set_id,
                        new_status,
                        crowd_size,
                        str(duration_hours) if duration_hours is not None else None,
                        notes,
                        str(total),
                        quote_id,
                    ),
                )
                _replace_line_items(conn, quote_id, new_lines)
                if prev_status != new_status:
                    _append_status_history(
                        conn, quote_id, prev_status, new_status, status_note
                    )
                conn.commit()
                flash("Quote saved.", "success")
                return redirect(url_for("crm_module.quote_edit", quote_id=quote_id))
            finally:
                cur.close()
                conn.close()

        def _quote_duration_hours(q: dict) -> Decimal | None:
            v = q.get("duration_hours")
            if v is None:
                return None
            try:
                return Decimal(str(v))
            except InvalidOperation:
                return None

        total, breakdown = compute_quote_total(
            line_rows=lines,
            crowd_size=quote.get("crowd_size"),
            duration_hours=_quote_duration_hours(quote),
            rule_rows=rule_rows,
        )

        c_ep = get_db_connection()
        try:
            linked_event_plans = fetch_plans_for_quote(c_ep, quote_id)
        finally:
            c_ep.close()

        ss_conn = get_db_connection()
        try:
            staffing_sidebar = _staffing_sidebar_for_opportunity(
                ss_conn, quote.get("opportunity_id")
            )
        finally:
            ss_conn.close()

        return render_template(
            "admin/crm_quote_form.html",
            quote=quote,
            lines=_pad_line_rows(lines, 5),
            history=history,
            revisions=revisions,
            linked_event_plans=linked_event_plans,
            accounts=accounts,
            opportunities=opps,
            rule_sets=rule_sets_all,
            statuses=QUOTE_STATUSES,
            status_labels=STATUS_LABELS,
            rule_types=RULE_TYPES,
            breakdown=breakdown,
            total=total,
            quote_prefill=None,
            staffing_sidebar=staffing_sidebar,
            tb_wage_cards_url=_tb_wage_cards_url(),
            can_edit=can_edit(),
        )

    @crm_bp.route("/quotes/<int:quote_id>/accept-for-cura", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def quote_accept_for_cura(quote_id: int):
        """One-step Accepted status so event plans can Send to Cura (client agreed pricing)."""
        return_plan_raw = (request.form.get("return_plan_id") or "").strip()
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT id, status FROM crm_quotes WHERE id=%s", (quote_id,))
            q = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        if not q:
            flash("Quote not found.", "danger")
            return redirect(url_for("crm_module.quotes_list"))
        prev = (q.get("status") or "").strip().lower()
        if prev == "accepted":
            flash("This quote is already Accepted.", "info")
            if return_plan_raw.isdigit():
                return redirect(
                    url_for(
                        "crm_module.event_plan_edit",
                        plan_id=int(return_plan_raw),
                        step=8,
                    )
                )
            return redirect(url_for("crm_module.quote_edit", quote_id=quote_id))

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE crm_quotes SET status=%s WHERE id=%s",
                ("accepted", quote_id),
            )
            _append_status_history(
                conn,
                quote_id,
                q.get("status"),
                "accepted",
                "Client agreed to pricing (ready for Cura / operations handoff)",
            )
        finally:
            cur.close()
            conn.close()
        flash(
            "Quote marked Accepted. You can return to the event plan and use Send to Cura.",
            "success",
        )
        if return_plan_raw.isdigit():
            return redirect(
                url_for(
                    "crm_module.event_plan_edit",
                    plan_id=int(return_plan_raw),
                    step=8,
                )
            )
        return redirect(url_for("crm_module.quote_edit", quote_id=quote_id))

    @crm_bp.route("/quotes/<int:quote_id>/new-revision", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def quote_new_revision(quote_id: int):
        conn = get_db_connection()
        try:
            new_id = _duplicate_quote_as_revision(conn, quote_id)
        finally:
            conn.close()
        if not new_id:
            flash("Could not create revision.", "danger")
            return redirect(url_for("crm_module.quote_edit", quote_id=quote_id))
        flash("New revision created as draft.", "success")
        return redirect(url_for("crm_module.quote_edit", quote_id=new_id))

    @crm_bp.route("/quotes/<int:quote_id>/delete", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def quote_delete(quote_id: int):
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM crm_quotes WHERE id=%s", (quote_id,))
            conn.commit()
            if cur.rowcount:
                flash("Quote deleted.", "success")
            else:
                flash("Quote not found.", "warning")
        finally:
            cur.close()
            conn.close()
        return redirect(url_for("crm_module.quotes_list"))
