"""Prefill CRM event plan fields from linked quote / opportunity (lead_meta)."""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

# Must match seeded `question_text` in crm_module/install.py::_seed_event_plan_questions.
_POSTURE_CHECKLIST_QUESTION = (
    "Is the event primarily seated, standing, or mixed?"
)

from .crm_event_display_name import friendly_event_display_name


def _parse_json(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        try:
            v = json.loads(s)
            return dict(v) if isinstance(v, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _blank_plan_value(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, (int, float, Decimal)) and val == 0:
        return False
    s = str(val).strip()
    return not s


def _parse_checklist_answers(raw: Any) -> dict[str, Any]:
    """Normalize checklist_answers_json to str-keyed dict (matches planner_routes._parse_answers_json)."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(k): v for k, v in raw.items()}
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return {}
        try:
            v = json.loads(s)
            return {str(k): val for k, val in v.items()} if isinstance(v, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _checklist_answer_from_audience_posture(lead: dict[str, Any] | None) -> str | None:
    """Map CRM intake / risk calculator `audience_posture` to checklist prose."""
    if not lead:
        return None
    ap = str(lead.get("audience_posture") or "").strip().lower()
    if not ap or ap == "unknown":
        return None
    labels = {
        "seated": "Primarily seated.",
        "standing": "Primarily standing.",
        "mixed": "Mixed (seated and standing).",
    }
    if ap in labels:
        return labels[ap]
    return ap.replace("_", " ").strip().title()


def _resolve_posture_question_id(cur) -> int | None:
    cur.execute(
        """
        SELECT id FROM crm_event_plan_questions
        WHERE is_active=1 AND question_text=%s
        LIMIT 1
        """,
        (_POSTURE_CHECKLIST_QUESTION,),
    )
    row = cur.fetchone()
    if not row:
        return None
    try:
        return int(row["id"])
    except (TypeError, ValueError, KeyError):
        return None


def _venue_dict(lead: dict[str, Any] | None) -> dict[str, Any]:
    if not lead:
        return {}
    v = lead.get("venue")
    return dict(v) if isinstance(v, dict) else {}


def _guide_questions(lead: dict[str, Any] | None) -> dict[str, Any]:
    if not lead:
        return {}
    gq = lead.get("guide_questions")
    return dict(gq) if isinstance(gq, dict) else {}


def _risk_dict(lead: dict[str, Any] | None) -> dict[str, Any]:
    if not lead:
        return {}
    r = lead.get("risk")
    return dict(r) if isinstance(r, dict) else {}


def build_prefill_column_updates(
    *,
    quote_row: dict[str, Any] | None,
    opp_row: dict[str, Any] | None,
    account_name: str | None,
) -> dict[str, Any]:
    """Return crm_event_plans column -> value for prefill (caller applies only_if_empty)."""
    out: dict[str, Any] = {}
    lead: dict[str, Any] | None = None
    if opp_row:
        lead = _parse_json(opp_row.get("lead_meta_json"))
    gq = _guide_questions(lead)
    venue = _venue_dict(lead)
    risk = _risk_dict(lead)

    if quote_row:
        cs = quote_row.get("crowd_size")
        if cs is not None:
            try:
                out["expected_attendance"] = int(cs)
            except (TypeError, ValueError):
                pass
        notes = (quote_row.get("internal_notes") or "").strip()
        if notes:
            out["risk_summary"] = notes[:65000]

    if lead:
        ev = (lead.get("event_name") or "").strip()
        org = (lead.get("organisation_name") or "").strip()
        if org and not out.get("event_organiser"):
            out["event_organiser"] = org[:255]
        elif ev and not out.get("event_organiser"):
            out["event_organiser"] = ev[:255]
        att = lead.get("expected_attendees")
        if att is not None and "expected_attendance" not in out:
            try:
                out["expected_attendance"] = int(att)
            except (TypeError, ValueError):
                pass
        cp = (lead.get("crowd_profile") or "").strip()
        if cp:
            out["demographics_notes"] = cp[:65000]
        cpl = (gq.get("crowd_profile_label") or "").strip()
        if cpl and "demographics_notes" in out:
            prev_d = str(out["demographics_notes"] or "").strip()
            if cpl.lower() not in prev_d.lower():
                out["demographics_notes"] = (
                    prev_d + ("\n" if prev_d else "") + cpl
                )[:65000]
        elif cpl and "demographics_notes" not in out:
            out["demographics_notes"] = cpl[:65000]

        addr = (venue.get("address") or "").strip()[:255]
        if addr:
            out["address_line1"] = addr
        pc = (venue.get("postcode") or "").strip()[:32]
        if pc:
            out["postcode"] = pc
        w3w = (venue.get("what3words") or "").strip()[:64]
        if w3w:
            out["what3words"] = w3w

        pt = gq.get("purple_tier")
        if pt is not None and str(pt).strip():
            out["purple_guide_tier"] = str(pt).strip()[:64]

        evn = (lead.get("event_name") or "").strip()
        ecs_parts: list[str] = []
        if evn:
            ecs_parts.append(f"Event: {evn}")
        msg = (lead.get("message") or "").strip()
        if msg:
            ecs_parts.append(msg)
        if ecs_parts and "event_content_summary" not in out:
            out["event_content_summary"] = "\n\n".join(ecs_parts)[:8000]

        ik = (lead.get("intake_kind") or "").strip().lower()
        if ik and "event_type" not in out:
            ik_map = {
                "private_transfer": "Private transfer",
                "event_risk": "Event (risk calculator)",
                "staff_intake": "Staff / internal",
            }
            out["event_type"] = (ik_map.get(ik) or ik.replace("_", " ").title())[:128]

        op_lines: list[str] = []
        dh = (lead.get("duration_hours") or "").strip()
        if dh:
            op_lines.append(f"Declared duration (hours): {dh}")
        dspan = (gq.get("duration_span") or "").strip()
        if dspan:
            op_lines.append(f"Duration span: {dspan.replace('_', ' ')}")
        if gq.get("late_finish"):
            op_lines.append("Late finish event: yes")
        if gq.get("alcohol_served"):
            op_lines.append("Alcohol served: yes")
        if op_lines and "operational_timings" not in out:
            out["operational_timings"] = "\n".join(op_lines)[:65000]

        rl = (risk.get("label") or "").strip()
        rs = risk.get("score")
        if rl or rs is not None:
            if "risk_summary" not in out:
                bits = []
                if rl:
                    bits.append(f"Risk band (organiser guide): {rl}")
                if rs is not None and str(rs).strip():
                    bits.append(f"Guide score: {rs}")
                out["risk_summary"] = " · ".join(bits)[:65000]
            if "risk_score" not in out and rs is not None:
                try:
                    out["risk_score"] = int(rs)
                except (TypeError, ValueError):
                    try:
                        out["risk_score"] = int(float(str(rs)))
                    except (TypeError, ValueError):
                        pass

    qtitle = (quote_row.get("title") or "").strip() if quote_row else ""
    oname = (opp_row.get("name") or "").strip() if opp_row else ""
    disp = friendly_event_display_name(
        lead_meta=lead,
        quote_title=qtitle or None,
        opportunity_name=oname or None,
        plan_title=None,
        fallback=None,
    )
    if disp and disp != "Event":
        out["title"] = disp[:255]

    return out


def load_prefill_sources(
    conn,
    quote_id: int | None,
    opportunity_id: int | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]:
    """Load quote row, opportunity row, account display name."""
    quote_row = None
    opp_row = None
    account_name = None
    cur = conn.cursor(dictionary=True)
    try:
        if quote_id:
            cur.execute(
                """
                SELECT q.*, a.name AS account_name
                FROM crm_quotes q
                LEFT JOIN crm_accounts a ON a.id = q.account_id
                WHERE q.id=%s
                """,
                (int(quote_id),),
            )
            quote_row = cur.fetchone()
            if quote_row:
                account_name = (quote_row.get("account_name") or "").strip() or None
                oid = quote_row.get("opportunity_id")
                if oid:
                    cur.execute(
                        "SELECT * FROM crm_opportunities WHERE id=%s",
                        (int(oid),),
                    )
                    opp_row = cur.fetchone()
        elif opportunity_id:
            cur.execute(
                """
                SELECT o.*, a.name AS account_name
                FROM crm_opportunities o
                JOIN crm_accounts a ON a.id = o.account_id
                WHERE o.id=%s
                """,
                (int(opportunity_id),),
            )
            opp_row = cur.fetchone()
            if opp_row:
                account_name = (opp_row.get("account_name") or "").strip() or None
    finally:
        cur.close()
    return quote_row, opp_row, account_name


def apply_prefill_to_plan(
    conn,
    plan_id: int,
    *,
    quote_id: int | None,
    opportunity_id: int | None,
    only_if_empty: bool = True,
) -> int:
    """
    Merge CRM quote/opportunity data into plan columns and checklist answers.
    Returns number of fields updated (each updated DB column counts as one).
    """
    quote_row, opp_row, aname = load_prefill_sources(conn, quote_id, opportunity_id)
    updates = build_prefill_column_updates(
        quote_row=quote_row, opp_row=opp_row, account_name=aname
    )
    lead: dict[str, Any] | None = None
    if opp_row:
        lead = _parse_json(opp_row.get("lead_meta_json"))

    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM crm_event_plans WHERE id=%s", (int(plan_id),))
        plan = cur.fetchone()
        if not plan:
            return 0
        set_parts: list[str] = []
        vals: list[Any] = []
        for col, val in updates.items():
            if only_if_empty and not _blank_plan_value(plan.get(col)):
                continue
            set_parts.append(f"`{col}`=%s")
            vals.append(val)

        answers_before = _parse_checklist_answers(plan.get("checklist_answers_json"))
        answers_merged = dict(answers_before)
        posture_line = _checklist_answer_from_audience_posture(lead)
        qid_posture = _resolve_posture_question_id(cur) if posture_line else None
        checklist_changed = False
        if qid_posture is not None and posture_line:
            key = str(qid_posture)
            if not only_if_empty or _blank_plan_value(answers_merged.get(key)):
                answers_merged[key] = posture_line
                before_s = json.dumps(answers_before, sort_keys=True, ensure_ascii=False)
                after_s = json.dumps(answers_merged, sort_keys=True, ensure_ascii=False)
                checklist_changed = before_s != after_s
        if checklist_changed:
            set_parts.append("`checklist_answers_json`=%s")
            vals.append(json.dumps(answers_merged, ensure_ascii=False))

        if not set_parts:
            return 0
        vals.append(int(plan_id))
        cur.execute(
            f"UPDATE crm_event_plans SET {', '.join(set_parts)} WHERE id=%s",
            vals,
        )
        conn.commit()
        return len(set_parts)
    finally:
        cur.close()


def resolve_account_and_opp_for_links(
    conn, quote_id: int | None, opportunity_id: int | None
) -> tuple[int | None, int | None, int | None]:
    """Return (account_id, opportunity_id, quote_id) for persisting plan links."""
    account_id = None
    opp_id: int | None = None
    qid = quote_id
    oid = opportunity_id
    cur = conn.cursor(dictionary=True)
    try:
        if qid:
            cur.execute(
                "SELECT account_id, opportunity_id FROM crm_quotes WHERE id=%s",
                (int(qid),),
            )
            r = cur.fetchone()
            if r:
                account_id = r.get("account_id")
                opp_id = r.get("opportunity_id")
        elif oid:
            cur.execute(
                "SELECT account_id FROM crm_opportunities WHERE id=%s",
                (int(oid),),
            )
            r = cur.fetchone()
            if r:
                account_id = r.get("account_id")
                opp_id = int(oid)
    finally:
        cur.close()
    return account_id, opp_id, qid
