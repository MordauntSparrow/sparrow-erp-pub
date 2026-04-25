"""Persistence and validation for CRM custom intake form definitions."""
from __future__ import annotations

import json
import re
from typing import Any

from app.objects import get_db_connection

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")
_ALLOWED_TYPES = frozenset({"text", "email", "tel", "textarea", "number"})
_DEFAULT_FIELDS: list[dict[str, Any]] = [
    {"name": "name", "label": "Your name", "type": "text", "required": True},
    {"name": "email", "label": "Email", "type": "email", "required": True},
    {"name": "company", "label": "Organisation", "type": "text", "required": False},
    {"name": "phone", "label": "Phone", "type": "tel", "required": False},
    {"name": "message", "label": "How can we help?", "type": "textarea", "required": True},
]

_STAGES = frozenset(
    {"prospecting", "qualification", "proposal", "negotiation", "won", "lost"}
)


def default_fields_json() -> list[dict[str, Any]]:
    return [dict(x) for x in _DEFAULT_FIELDS]


def validate_slug(slug: str) -> str:
    s = (slug or "").strip().lower()
    if not s or not _SLUG_RE.match(s):
        raise ValueError(
            "Slug must be 1–63 characters: lowercase letters, digits, hyphen, underscore; "
            "must start with a letter or digit."
        )
    if s in ("intake", "quoting", "event-risk-calculator"):
        raise ValueError("This slug is reserved.")
    return s


def normalize_fields(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list) or not raw:
        return default_fields_json()
    out: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip().lower()
        if not name or name in ("form_id", "website", "cf-turnstile-response", "_csrf_token"):
            continue
        label = (str(row.get("label") or "").strip() or name.replace("_", " ").title())[:255]
        typ = str(row.get("type") or "text").strip().lower()
        if typ not in _ALLOWED_TYPES:
            typ = "text"
        req = bool(row.get("required"))
        out.append({"name": name[:64], "label": label, "type": typ, "required": req})
    return out if out else default_fields_json()


def list_intake_forms(*, active_only: bool = False) -> list[dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        q = "SELECT * FROM crm_intake_form_definitions"
        if active_only:
            q += " WHERE is_active = 1"
        q += " ORDER BY title ASC, id ASC"
        cur.execute(q)
        rows = cur.fetchall() or []
        for r in rows:
            r["fields"] = _parse_fields_row(r.get("fields_json"))
        return rows
    finally:
        cur.close()
        conn.close()


def _parse_fields_row(blob: Any) -> list[dict[str, Any]]:
    if blob is None:
        return default_fields_json()
    if isinstance(blob, list):
        return normalize_fields(blob)
    if isinstance(blob, str) and blob.strip():
        try:
            j = json.loads(blob)
            return normalize_fields(j)
        except json.JSONDecodeError:
            return default_fields_json()
    if isinstance(blob, (bytes, bytearray)):
        try:
            j = json.loads(blob.decode("utf-8"))
            return normalize_fields(j)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return default_fields_json()
    return default_fields_json()


def get_intake_form_by_id(form_id: int) -> dict[str, Any] | None:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT * FROM crm_intake_form_definitions WHERE id = %s",
            (int(form_id),),
        )
        row = cur.fetchone()
        if row:
            row["fields"] = _parse_fields_row(row.get("fields_json"))
        return row
    finally:
        cur.close()
        conn.close()


def get_intake_form_by_slug(slug: str) -> dict[str, Any] | None:
    s = (slug or "").strip().lower()
    if not s:
        return None
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT * FROM crm_intake_form_definitions WHERE slug = %s",
            (s,),
        )
        row = cur.fetchone()
        if row:
            row["fields"] = _parse_fields_row(row.get("fields_json"))
        return row
    finally:
        cur.close()
        conn.close()


def normalize_stage(raw: str | None) -> str:
    s = (raw or "prospecting").strip().lower()
    return s if s in _STAGES else "prospecting"


def insert_intake_form(
    *,
    slug: str,
    title: str,
    description: str | None,
    fields: list[dict[str, Any]],
    default_stage: str,
    company_field: str,
    is_public: bool,
    is_active: bool,
) -> int:
    slug = validate_slug(slug)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO crm_intake_form_definitions
              (slug, title, description, fields_json, default_stage, company_field, is_public, is_active)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                slug,
                (title or "").strip()[:255] or slug,
                (description or "").strip() or None,
                json.dumps(normalize_fields(fields), separators=(",", ":")),
                normalize_stage(default_stage),
                (company_field or "company").strip()[:64] or "company",
                1 if is_public else 0,
                1 if is_active else 0,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        cur.close()
        conn.close()


def update_intake_form(
    form_id: int,
    *,
    slug: str,
    title: str,
    description: str | None,
    fields: list[dict[str, Any]],
    default_stage: str,
    company_field: str,
    is_public: bool,
    is_active: bool,
) -> None:
    slug = validate_slug(slug)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE crm_intake_form_definitions SET
              slug=%s, title=%s, description=%s, fields_json=%s,
              default_stage=%s, company_field=%s, is_public=%s, is_active=%s
            WHERE id=%s
            """,
            (
                slug,
                (title or "").strip()[:255] or slug,
                (description or "").strip() or None,
                json.dumps(normalize_fields(fields), separators=(",", ":")),
                normalize_stage(default_stage),
                (company_field or "company").strip()[:64] or "company",
                1 if is_public else 0,
                1 if is_active else 0,
                int(form_id),
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def delete_intake_form(form_id: int) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM crm_intake_form_definitions WHERE id=%s", (int(form_id),))
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()
