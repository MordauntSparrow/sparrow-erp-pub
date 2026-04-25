"""Website module contact forms → CRM opportunities (sales leads)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.objects import get_db_connection

from .crm_common import uid
from .crm_event_risk import split_contact_name
from .crm_lead_intake import ensure_account
from .crm_stage_history import log_opportunity_stage_change

_ALLOWED_STAGES = frozenset(
    {"prospecting", "qualification", "proposal", "negotiation", "won", "lost"}
)

_META_KEYS_SKIP = frozenset(
    {
        "form_id",
        "remote_ip",
        "referrer",
        "page",
        "timestamp",
        "cf-turnstile-response",
        "website",
    }
)


def _json_dumps(obj: dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def _extract_contact_name(data: dict[str, Any]) -> str:
    for k in ("name", "full_name", "fullname", "customer_name"):
        v = (data.get(k) or "").strip()
        if v:
            return v
    return ""


def _extract_email(data: dict[str, Any]) -> str:
    for k in ("email", "email_address"):
        v = (data.get(k) or "").strip()
        if v:
            return v
    return ""


def _extract_phone(data: dict[str, Any]) -> str | None:
    for k in ("phone", "telephone", "mobile", "phone_number"):
        v = (data.get(k) or "").strip()
        if v:
            return v[:64]
    return None


def _extract_company(data: dict[str, Any], field_key: str | None) -> str:
    keys: list[str] = []
    fk = (field_key or "").strip()
    if fk:
        keys.append(fk)
    keys.extend(
        [
            "company",
            "organisation",
            "organization",
            "organisation_name",
            "business_name",
        ]
    )
    for k in keys:
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()[:255]
    cn = _extract_contact_name(data)
    return (cn or "Website enquiry")[:255]


def _submission_snapshot(data: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in (data or {}).items():
        kk = str(k).strip()
        if not kk or kk.lower() in _META_KEYS_SKIP:
            continue
        if isinstance(v, (dict, list)):
            try:
                out[kk] = _json_dumps(v) if isinstance(v, dict) else str(v)[:4000]
            except TypeError:
                out[kk] = str(v)[:2000]
        else:
            out[kk] = str(v)[:4000] if v is not None else ""
    return out


def _build_notes(form_id: str, snap: dict[str, Any]) -> str:
    lines = [
        "Source: Website contact form",
        f"Form id: {form_id}",
        f"Logged (UTC): {datetime.now(timezone.utc).isoformat()}",
        "",
        "Submitted fields:",
    ]
    for k in sorted(snap.keys(), key=lambda x: x.lower()):
        val = snap.get(k)
        if val is None or val == "":
            continue
        lines.append(f"- {k}: {val}")
    return "\n".join(lines)[:65000]


def create_lead_from_website_submission(
    submission: dict[str, Any],
    *,
    form_id: str,
    stage: str = "prospecting",
    company_field: str | None = None,
    linked_account_id: int | None = None,
) -> tuple[int, int | None] | None:
    """
    Create CRM account (unless ``linked_account_id`` is set), opportunity, and contact.

    Returns ``(opportunity_id, contact_id)``, or ``None`` if there is not enough identity
    to create a lead (no name and no email).
    """
    contact_name = _extract_contact_name(submission)
    email = _extract_email(submission)
    phone = _extract_phone(submission)
    if not contact_name.strip() and not email.strip():
        return None

    stage_clean = (stage or "prospecting").strip().lower()
    if stage_clean not in _ALLOWED_STAGES:
        stage_clean = "prospecting"

    conn = get_db_connection()
    try:
        if linked_account_id is not None:
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT id FROM crm_accounts WHERE id=%s LIMIT 1",
                    (int(linked_account_id),),
                )
                row = cur.fetchone()
                if not row:
                    return None
                account_id = int(row[0])
            finally:
                cur.close()
        else:
            comp = _extract_company(submission, company_field)
            account_id = ensure_account(conn, comp)
    finally:
        conn.close()

    snap = _submission_snapshot(submission)
    fn, ln = split_contact_name(contact_name or (email.split("@")[0] if email else "?"))

    lead_meta: dict[str, Any] = {
        "source": "website_contact_form",
        "form_id": form_id,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "contact": {"name": contact_name, "email": email, "phone": phone},
        "company": _extract_company(submission, company_field),
        "fields_snapshot": snap,
        "page": submission.get("page"),
        "referrer": submission.get("referrer"),
        "remote_ip": submission.get("remote_ip"),
    }
    notes = _build_notes(form_id, snap)
    label = contact_name or email or "Visitor"
    opp_name = f"Web: {form_id} — {label}"[:255]

    conn = get_db_connection()
    cur = conn.cursor()
    contact_id: int | None = None
    try:
        cur.execute(
            """INSERT INTO crm_opportunities
            (account_id, name, stage, amount, notes, lead_meta_json)
            VALUES (%s,%s,%s,NULL,%s,%s)""",
            (account_id, opp_name, stage_clean, notes, _json_dumps(lead_meta)),
        )
        opp_id = int(cur.lastrowid)
        if email or fn or ln:
            cur.execute(
                """INSERT INTO crm_contacts
                (account_id, first_name, last_name, email, phone)
                VALUES (%s,%s,%s,%s,%s)""",
                (
                    account_id,
                    (fn or "?")[:128],
                    (ln or "")[:128],
                    email or None,
                    phone,
                ),
            )
            contact_id = int(cur.lastrowid)
        conn.commit()
        log_opportunity_stage_change(opp_id, None, stage_clean, uid())
        return opp_id, contact_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
