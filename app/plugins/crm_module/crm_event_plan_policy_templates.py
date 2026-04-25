"""Reusable H&S and privacy/IPC annex snippets for medical event plans."""
from __future__ import annotations

from typing import Any

POLICY_CATEGORY_HEALTH_SAFETY = "health_safety_work"
POLICY_CATEGORY_PRIVACY_IPC = "privacy_ipc"

_ALLOWED = frozenset(
    {POLICY_CATEGORY_HEALTH_SAFETY, POLICY_CATEGORY_PRIVACY_IPC}
)


def list_policy_templates(conn, category: str) -> list[dict[str, Any]]:
    if category not in _ALLOWED:
        raise ValueError("Invalid policy template category.")
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, category, title, body, sort_order
            FROM crm_event_plan_policy_templates
            WHERE category=%s
            ORDER BY sort_order, id
            """,
            (category,),
        )
        rows = cur.fetchall() or []
        for r in rows:
            b = r.get("body")
            if isinstance(b, (bytes, bytearray)):
                r["body"] = b.decode("utf-8", errors="replace")
        return rows
    finally:
        cur.close()


def insert_policy_template(conn, category: str, title: str, body: str) -> None:
    if category not in _ALLOWED:
        raise ValueError("Invalid policy template category.")
    t = (title or "").strip()[:255] or "Policy template"
    b = (body or "").strip()
    if not b:
        raise ValueError("Policy text is required.")
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT IFNULL(MAX(sort_order), -1) + 1
            FROM crm_event_plan_policy_templates
            WHERE category=%s
            """,
            (category,),
        )
        row = cur.fetchone()
        so = int(row[0]) if row and row[0] is not None else 0
        cur.execute(
            """
            INSERT INTO crm_event_plan_policy_templates
            (category, title, body, sort_order)
            VALUES (%s, %s, %s, %s)
            """,
            (category, t, b[:65000], so),
        )
        conn.commit()
    finally:
        cur.close()


def delete_policy_template(conn, template_id: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM crm_event_plan_policy_templates WHERE id=%s",
            (int(template_id),),
        )
        conn.commit()
    finally:
        cur.close()
