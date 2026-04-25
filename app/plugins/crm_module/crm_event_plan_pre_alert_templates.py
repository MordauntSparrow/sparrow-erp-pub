"""Reusable pre-alert (ASHICE / ATMIST / custom) snippets for medical event plans."""
from __future__ import annotations

import json
from typing import Any

from .crm_clinical_handover import _ASHICE_KEYS, _ATMIST_KEYS

_MAX_ASHICE_FIELD = 4000
_MAX_ASHICE_SHORT = 2000
_MAX_CUSTOM_DETAIL = 2000


def _coerce_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8", errors="replace")
        except Exception:
            return {}
    if isinstance(raw, str) and raw.strip():
        try:
            v = json.loads(raw)
            return v if isinstance(v, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def normalize_pre_alert_template_record(
    policy: str, payload: Any
) -> tuple[str, dict[str, Any]]:
    """
    Validate policy + payload from client. Returns ``(policy, stored_payload)``.

    ``stored_payload`` is only the data needed to re-apply the template:
    ``ashice`` dict, ``atmist`` dict, and/or ``custom_pre_alert`` list.
    """
    pol = (policy or "").strip().lower()
    if pol not in ("ashice", "atmist", "custom"):
        raise ValueError("Invalid pre-alert policy.")
    data = _coerce_payload(payload)
    out: dict[str, Any] = {}
    if pol == "ashice":
        src = data.get("ashice") if isinstance(data.get("ashice"), dict) else data
        if not isinstance(src, dict):
            src = {}
        ash: dict[str, str] = {}
        for k in _ASHICE_KEYS:
            v = str(src.get(k) or "").strip()
            lim = _MAX_ASHICE_SHORT if k in ("age", "sex") else _MAX_ASHICE_FIELD
            ash[k] = v[:lim]
        if not any(ash.values()):
            raise ValueError("ASHICE template must include at least one filled line.")
        out["ashice"] = ash
    elif pol == "atmist":
        src = data.get("atmist") if isinstance(data.get("atmist"), dict) else data
        if not isinstance(src, dict):
            src = {}
        atm: dict[str, str] = {}
        for k in _ATMIST_KEYS:
            v = str(src.get(k) or "").strip()
            lim = _MAX_ASHICE_SHORT if k == "age" else _MAX_ASHICE_FIELD
            atm[k] = v[:lim]
        if not any(atm.values()):
            raise ValueError("ATMIST template must include at least one filled line.")
        out["atmist"] = atm
    else:
        lines_raw = data.get("custom_pre_alert")
        if not isinstance(lines_raw, list):
            lines_raw = []
        lines: list[dict[str, str]] = []
        for x in lines_raw:
            if not isinstance(x, dict):
                continue
            letter = str(x.get("letter") or "").strip()[:16]
            detail = str(x.get("detail") or "").strip()[:_MAX_CUSTOM_DETAIL]
            if letter or detail:
                lines.append({"letter": letter, "detail": detail})
        if not lines:
            raise ValueError("Custom template must include at least one letter or description line.")
        out["custom_pre_alert"] = lines
    return pol, out


def list_pre_alert_templates(conn) -> list[dict[str, Any]]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, title, policy, payload_json, sort_order
            FROM crm_event_plan_pre_alert_templates
            ORDER BY sort_order, id
            """
        )
        rows = cur.fetchall() or []
        for r in rows:
            pj = r.get("payload_json")
            if isinstance(pj, str):
                try:
                    r["payload_json"] = json.loads(pj)
                except json.JSONDecodeError:
                    r["payload_json"] = {}
            elif pj is None:
                r["payload_json"] = {}
        return rows
    finally:
        cur.close()


def insert_pre_alert_template(
    conn, title: str, policy: str, payload: Any
) -> None:
    pol, stored = normalize_pre_alert_template_record(policy, payload)
    t = (title or "").strip()[:255] or "Pre-alert template"
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT IFNULL(MAX(sort_order), -1) + 1 FROM crm_event_plan_pre_alert_templates"
        )
        row = cur.fetchone()
        so = int(row[0]) if row and row[0] is not None else 0
        cur.execute(
            """
            INSERT INTO crm_event_plan_pre_alert_templates (title, policy, payload_json, sort_order)
            VALUES (%s, %s, %s, %s)
            """,
            (t, pol, json.dumps(stored, ensure_ascii=False), so),
        )
        conn.commit()
    finally:
        cur.close()


def delete_pre_alert_template(conn, template_id: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM crm_event_plan_pre_alert_templates WHERE id=%s",
            (int(template_id),),
        )
        conn.commit()
    finally:
        cur.close()
