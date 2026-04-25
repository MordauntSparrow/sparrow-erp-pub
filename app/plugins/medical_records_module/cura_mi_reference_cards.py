"""Reusable minor-injury reference / advice cards (per-event assignment)."""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from .cura_util import safe_json

logger = logging.getLogger("medical_records_module.cura_mi_reference_cards")

DEFAULT_PACK_SLUGS: tuple[str, ...] = (
    "head_injury_red_flags",
    "safeguarding_triggers",
    "when_to_escalate",
    "common_presentations",
    "welfare_mental_health",
    "event_risk_profile",
)


def parse_card_schema(raw: Any) -> tuple[dict[str, Any] | None, str | None]:
    if raw is None:
        return None, "schema is required"
    if isinstance(raw, dict):
        data = raw
    elif isinstance(raw, str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None, "schema_json is not valid JSON"
    else:
        return None, "schema must be an object"
    if not isinstance(data, dict):
        return None, "schema must be an object"
    variant = (data.get("variant") or "").strip().lower()
    if variant not in ("bullets", "event_risk_profile"):
        return None, "schema.variant must be bullets or event_risk_profile"
    title = (data.get("title") or "").strip()
    if not title:
        return None, "schema.title is required"
    data["title"] = title
    data["variant"] = variant
    if variant == "bullets":
        items = data.get("items")
        if items is None:
            data["items"] = []
            items = []
        if not isinstance(items, list):
            return None, "schema.items must be an array"
        for i, line in enumerate(items):
            if not isinstance(line, str) or not line.strip():
                return None, f"schema.items[{i}] must be a non-empty string"
        co = data.get("callout")
        if co is not None:
            if not isinstance(co, dict):
                return None, "schema.callout must be an object"
            sev = (co.get("severity") or "info").strip().lower()
            if sev not in ("error", "warning", "info", "success"):
                return None, "schema.callout.severity must be error, warning, info, or success"
            txt = (co.get("text") or "").strip()
            if not txt:
                return None, "schema.callout.text is required when callout is set"
            data["callout"] = {"severity": sev, "text": txt}
    else:
        hint = data.get("emptyHint")
        if hint is not None and not isinstance(hint, str):
            return None, "schema.emptyHint must be a string"
    hc = data.get("headerColor")
    if hc is not None and (not isinstance(hc, str) or not re.match(r"^#[0-9a-fA-F]{3,8}$", hc.strip())):
        return None, "schema.headerColor must be a hex colour like #1565c0"
    if isinstance(hc, str):
        data["headerColor"] = hc.strip()
    icon = data.get("icon")
    if icon is not None:
        if not isinstance(icon, str) or not icon.strip():
            return None, "schema.icon must be a non-empty string"
        data["icon"] = icon.strip().lower()
    return data, None


def row_to_template_item(r: tuple) -> dict[str, Any]:
    sid, slug, name, desc, schema_raw, is_active, cb, ub, ca, ua = r
    schema = safe_json(schema_raw) if schema_raw else {}
    if not isinstance(schema, dict):
        schema = {}
    return {
        "id": sid,
        "slug": slug,
        "name": name,
        "description": desc or "",
        "schema": schema,
        "is_active": bool(is_active),
        "created_by": cb,
        "updated_by": ub,
        "created_at": ca.isoformat() if ca else None,
        "updated_at": ua.isoformat() if ua else None,
    }


def ensure_default_reference_cards_for_event(cur, event_id: int, *, actor: str | None = None) -> None:
    """If the MI event has no reference cards assigned, attach the default pack (when templates exist)."""
    eid = int(event_id)
    cur.execute(
        "SELECT COUNT(*) FROM cura_mi_event_reference_cards WHERE event_id = %s",
        (eid,),
    )
    row = cur.fetchone()
    if row and int(row[0] or 0) > 0:
        return
    act = (actor or "").strip() or None
    for order, slug in enumerate(DEFAULT_PACK_SLUGS):
        cur.execute(
            """
            SELECT id FROM cura_mi_reference_card_templates
            WHERE slug = %s AND is_active = 1
            """,
            (slug,),
        )
        tr = cur.fetchone()
        if not tr:
            continue
        try:
            cur.execute(
                """
                INSERT INTO cura_mi_event_reference_cards (event_id, template_id, sort_order, created_by)
                VALUES (%s, %s, %s, %s)
                """,
                (eid, int(tr[0]), order, act),
            )
        except Exception as ex:
            logger.debug("ensure_default_reference_cards insert: %s", ex)


def list_assigned_cards_for_event(cur, event_id: int, *, fill_defaults: bool = True) -> list[dict[str, Any]]:
    eid = int(event_id)
    if fill_defaults:
        ensure_default_reference_cards_for_event(cur, eid)
    cur.execute(
        """
        SELECT t.id, t.slug, t.name, t.description, t.schema_json, t.is_active,
               t.created_by, t.updated_by, t.created_at, t.updated_at, a.sort_order
        FROM cura_mi_event_reference_cards a
        INNER JOIN cura_mi_reference_card_templates t ON t.id = a.template_id
        WHERE a.event_id = %s AND t.is_active = 1
        ORDER BY a.sort_order ASC, t.id ASC
        """,
        (eid,),
    )
    out: list[dict[str, Any]] = []
    for r in cur.fetchall() or []:
        base = row_to_template_item(r[:10])
        base["sort_order"] = r[10]
        out.append(base)
    return out


def replace_event_reference_cards(
    cur,
    mi_event_id: int,
    template_ids: list[int],
    actor: str | None,
) -> str | None:
    eid = int(mi_event_id)
    cur.execute("SELECT 1 FROM cura_mi_events WHERE id = %s", (eid,))
    if not cur.fetchone():
        return "MI event not found"
    seen: set[int] = set()
    clean: list[int] = []
    for raw in template_ids:
        try:
            tid = int(raw)
        except (TypeError, ValueError):
            return "template_ids must be integers"
        if tid <= 0 or tid in seen:
            continue
        seen.add(tid)
        clean.append(tid)
    for tid in clean:
        cur.execute(
            "SELECT id, is_active FROM cura_mi_reference_card_templates WHERE id = %s",
            (tid,),
        )
        tr = cur.fetchone()
        if not tr:
            return f"Reference card template id={tid} not found"
        if not tr[1]:
            return f"Reference card template id={tid} is inactive"
    cur.execute("DELETE FROM cura_mi_event_reference_cards WHERE event_id = %s", (eid,))
    act = (actor or "").strip() or None
    for order, tid in enumerate(clean):
        cur.execute(
            """
            INSERT INTO cura_mi_event_reference_cards (event_id, template_id, sort_order, created_by)
            VALUES (%s, %s, %s, %s)
            """,
            (eid, tid, order, act),
        )
    return None
