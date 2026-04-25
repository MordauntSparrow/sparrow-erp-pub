"""Minor injury custom form templates (Ventus-style JSON schema) and per-event assignment."""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from .cura_util import safe_json

logger = logging.getLogger("medical_records_module.cura_mi_custom_forms")

SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$", re.I)


def normalize_slug(raw: str | None) -> str | None:
    s = (raw or "").strip()
    return s if s and SLUG_RE.match(s) else None


def parse_schema_blob(raw: Any) -> tuple[dict[str, Any] | None, str | None]:
    """Return (schema dict with ``questions`` list, error message)."""
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
    qs = data.get("questions")
    if qs is None:
        data = {**data, "questions": []}
        qs = data["questions"]
    if not isinstance(qs, list):
        return None, "schema.questions must be an array"
    for i, q in enumerate(qs):
        if not isinstance(q, dict):
            return None, f"questions[{i}] must be an object"
        key = (q.get("key") or "").strip()
        if not key:
            return None, f"questions[{i}].key is required"
        qtype = (q.get("type") or "text").strip().lower()
        if qtype not in ("text", "textarea", "number", "select"):
            return None, f"questions[{i}].type must be text, textarea, number, or select"
        if qtype == "select":
            opts = q.get("options")
            if not isinstance(opts, list) or not all(isinstance(x, str) for x in opts):
                return None, f"questions[{i}].options must be an array of strings"
            if not opts:
                return None, f"questions[{i}].options must have at least one choice for select"
        if qtype == "number":
            for opt_key in ("min", "max"):
                v = q.get(opt_key)
                if v is None or v == "":
                    q.pop(opt_key, None)
                    continue
                if isinstance(v, bool):
                    return None, f"questions[{i}].{opt_key} must be a number"
                if isinstance(v, (int, float)):
                    continue
                try:
                    q[opt_key] = float(v)
                except (TypeError, ValueError):
                    return None, f"questions[{i}].{opt_key} must be a number"
    return data, None


def row_to_template_item(r: tuple) -> dict[str, Any]:
    """Map SELECT row (10 cols: id … updated_at) to API item."""
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


def list_assigned_templates_for_event(cur, event_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT t.id, t.slug, t.name, t.description, t.schema_json, t.is_active,
               t.created_by, t.updated_by, t.created_at, t.updated_at, a.sort_order
        FROM cura_mi_event_form_assignments a
        INNER JOIN cura_mi_form_templates t ON t.id = a.form_template_id
        WHERE a.event_id = %s AND t.is_active = 1
        ORDER BY a.sort_order ASC, t.id ASC
        """,
        (int(event_id),),
    )
    out: list[dict[str, Any]] = []
    for r in cur.fetchall() or []:
        base = row_to_template_item(r[:10])
        base["sort_order"] = r[10]
        out.append(base)
    return out


def replace_event_assignments(
    cur,
    mi_event_id: int,
    template_ids: list[int],
    actor: str | None,
) -> str | None:
    """Replace all assignments. Returns error message or None on success."""
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
            "SELECT id, is_active FROM cura_mi_form_templates WHERE id = %s",
            (tid,),
        )
        tr = cur.fetchone()
        if not tr:
            return f"Form template id={tid} not found"
        if not tr[1]:
            return f"Form template id={tid} is inactive"
    cur.execute("DELETE FROM cura_mi_event_form_assignments WHERE event_id = %s", (eid,))
    act = (actor or "").strip() or None
    for order, tid in enumerate(clean):
        cur.execute(
            """
            INSERT INTO cura_mi_event_form_assignments (event_id, form_template_id, sort_order, created_by)
            VALUES (%s, %s, %s, %s)
            """,
            (eid, tid, order, act),
        )
    return None


def _answer_nonempty(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, bool):
        return True
    if isinstance(val, (int, float)):
        return True
    if isinstance(val, str):
        return bool(val.strip())
    return True


def validate_custom_form_responses(cur, event_id: int, payload: dict[str, Any]) -> str | None:
    """
    Ensure ``payload['customFormResponses']`` satisfies required questions for templates
    assigned to this MI event. Keys: template id as string -> { questionKey: value }.
    """
    assigned = list_assigned_templates_for_event(cur, event_id)
    if not assigned:
        return None
    raw_resp = payload.get("customFormResponses")
    if raw_resp is None:
        raw_resp = payload.get("custom_form_responses")
    if raw_resp is None:
        responses: dict[str, Any] = {}
    elif isinstance(raw_resp, dict):
        responses = {str(k): v for k, v in raw_resp.items() if v is not None}
    else:
        return "customFormResponses must be an object"

    for tpl in assigned:
        tid = tpl["id"]
        sk = str(tid)
        answers = responses.get(sk)
        if answers is None and tid is not None:
            try:
                answers = responses.get(str(int(tid)))
            except (TypeError, ValueError):
                answers = None
        if not isinstance(answers, dict):
            answers = {}
        schema = tpl.get("schema") or {}
        questions = schema.get("questions") if isinstance(schema, dict) else None
        if not isinstance(questions, list):
            continue
        for q in questions:
            if not isinstance(q, dict):
                continue
            if not q.get("required"):
                continue
            key = (q.get("key") or "").strip()
            if not key:
                continue
            val = answers.get(key)
            if not _answer_nonempty(val):
                return f"Required add-on field missing: {tpl.get('name') or sk} — {q.get('label') or key}"
    return None
