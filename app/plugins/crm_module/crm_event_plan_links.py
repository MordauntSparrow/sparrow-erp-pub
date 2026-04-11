"""Lookups for event plan navigation (quote / opportunity cross-links)."""
from __future__ import annotations

from typing import Any


def fetch_plans_for_quote(conn, quote_id: int) -> list[dict[str, Any]]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, title, status, updated_at FROM crm_event_plans
            WHERE quote_id=%s ORDER BY updated_at DESC LIMIT 20
            """,
            (quote_id,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()


def fetch_plans_for_opportunity(conn, opp_id: int) -> list[dict[str, Any]]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, title, status, updated_at FROM (
                SELECT id, title, status, updated_at FROM crm_event_plans
                WHERE opportunity_id=%s
                UNION
                SELECT p.id, p.title, p.status, p.updated_at FROM crm_event_plans p
                INNER JOIN crm_quotes q ON q.id = p.quote_id AND q.opportunity_id=%s
            ) AS ep_union ORDER BY updated_at DESC LIMIT 20
            """,
            (opp_id, opp_id),
        )
        return cur.fetchall() or []
    finally:
        cur.close()


def fetch_plan_link_labels(cur, plan: dict[str, Any]) -> dict[str, Any]:
    """Titles/names for CRM records linked on the plan row."""
    out: dict[str, Any] = {"quote_title": None, "opportunity_name": None}
    qid = plan.get("quote_id")
    if qid is not None:
        try:
            qid_int = int(qid)
        except (TypeError, ValueError):
            qid_int = None
        if qid_int:
            cur.execute("SELECT title FROM crm_quotes WHERE id=%s", (qid_int,))
            row = cur.fetchone()
            if row:
                out["quote_title"] = row.get("title")
    oid = plan.get("opportunity_id")
    if oid is not None:
        try:
            oid_int = int(oid)
        except (TypeError, ValueError):
            oid_int = None
        if oid_int:
            cur.execute("SELECT name FROM crm_opportunities WHERE id=%s", (oid_int,))
            row = cur.fetchone()
            if row:
                out["opportunity_name"] = row.get("name")
    return out
