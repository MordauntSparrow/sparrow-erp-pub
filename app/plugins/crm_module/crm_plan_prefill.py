"""Prefill CRM event plan fields from linked quote / opportunity (lead_meta)."""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from app.objects import get_db_connection


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

    if quote_row:
        cs = quote_row.get("crowd_size")
        if cs is not None:
            try:
                out["expected_attendance"] = int(cs)
            except (TypeError, ValueError):
                pass
        qtitle = (quote_row.get("title") or "").strip()
        if qtitle:
            out["title"] = qtitle[:255]
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
        demo_parts = []
        if ev:
            demo_parts.append(f"Event: {ev}")
        cp = (lead.get("crowd_profile") or "").strip()
        if cp:
            demo_parts.append(f"Crowd profile: {cp}")
        if demo_parts:
            out["demographics_notes"] = "\n".join(demo_parts)[:65000]
        env_parts = []
        vt = (lead.get("venue_type") or "").strip()
        if vt:
            env_parts.append(f"Venue: {vt}")
        dh = lead.get("duration_hours")
        if dh is not None:
            env_parts.append(f"Duration (hours): {dh}")
        ds = (lead.get("duration_span") or "").strip()
        if ds:
            env_parts.append(f"Duration span: {ds}")
        if env_parts:
            out["environment_notes"] = "\n".join(env_parts)[:65000]

    if opp_row and account_name:
        oname = (opp_row.get("name") or "").strip()
        if oname:
            bits = [account_name, oname]
            line = " — ".join(b for b in bits if b)
            if line and "title" not in out:
                out["title"] = line[:255]

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
    Merge CRM quote/opportunity data into plan columns.
    Returns number of columns updated.
    """
    quote_row, opp_row, aname = load_prefill_sources(conn, quote_id, opportunity_id)
    updates = build_prefill_column_updates(
        quote_row=quote_row, opp_row=opp_row, account_name=aname
    )
    if not updates:
        return 0
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
