"""Reusable plan purpose (scope of cover) snippets for medical event plans."""
from __future__ import annotations

from typing import Any


def list_purpose_templates(conn) -> list[dict[str, Any]]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, title, body, sort_order
            FROM crm_event_plan_purpose_templates
            ORDER BY sort_order, id
            """
        )
        rows = cur.fetchall() or []
        for r in rows:
            b = r.get("body")
            if isinstance(b, (bytes, bytearray)):
                r["body"] = b.decode("utf-8", errors="replace")
        return rows
    finally:
        cur.close()


def insert_purpose_template(conn, title: str, body: str) -> int:
    """Insert a row; return new template ``id``."""
    t = (title or "").strip()[:255] or "Purpose template"
    b = (body or "").strip()
    if not b:
        raise ValueError("Purpose text is required.")
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT IFNULL(MAX(sort_order), -1) + 1 FROM crm_event_plan_purpose_templates"
        )
        row = cur.fetchone()
        so = int(row[0]) if row and row[0] is not None else 0
        cur.execute(
            """
            INSERT INTO crm_event_plan_purpose_templates (title, body, sort_order)
            VALUES (%s, %s, %s)
            """,
            (t, b[:65000], so),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        cur.close()


def delete_purpose_template(conn, template_id: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM crm_event_plan_purpose_templates WHERE id=%s",
            (int(template_id),),
        )
        conn.commit()
    finally:
        cur.close()
