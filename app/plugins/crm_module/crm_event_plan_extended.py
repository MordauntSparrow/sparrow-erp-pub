"""Optional extended event-plan fields for PDF parity with full medical plan templates (e.g. access/RVP tables, risk register)."""
from __future__ import annotations

import json
from typing import Any

from flask import request

_RISK_ROW_KEYS = ("risk_type", "risk_level", "risk_comment")


def risk_register_list_from_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    """Normalise ``risk_register_json`` to a list of row dicts."""
    raw = plan.get("risk_register_json")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [_normalise_risk_row(x) for x in raw if isinstance(x, dict)]
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            v = json.loads(s)
            if isinstance(v, list):
                return [_normalise_risk_row(x) for x in v if isinstance(x, dict)]
        except json.JSONDecodeError:
            return []
    return []


def _normalise_risk_row(d: dict) -> dict[str, str]:
    return {
        "risk_type": str(d.get("risk_type") or "").strip()[:255],
        "risk_level": str(d.get("risk_level") or "").strip()[:64],
        "risk_comment": str(d.get("risk_comment") or "").strip()[:1024],
    }


def risk_row_nonempty(r: dict[str, str]) -> bool:
    return any(r.get(k) for k in _RISK_ROW_KEYS)


def risk_register_json_from_form() -> str | None:
    types = request.form.getlist("rr_type")
    levels = request.form.getlist("rr_level")
    comments = request.form.getlist("rr_comment")
    n = max(len(types), len(levels), len(comments))
    rows: list[dict[str, str]] = []
    for i in range(n):
        t = (types[i] if i < len(types) else "").strip()
        lv = (levels[i] if i < len(levels) else "").strip()
        c = (comments[i] if i < len(comments) else "").strip()
        if not (t or lv or c):
            continue
        rows.append(
            {
                "risk_type": t[:255],
                "risk_level": lv[:64],
                "risk_comment": c[:1024],
            }
        )
    if not rows:
        return None
    return json.dumps(rows, ensure_ascii=False)


def pad_risk_register(
    plan: dict[str, Any], size: int = 16
) -> list[dict[str, str] | None]:
    rows = [r for r in risk_register_list_from_plan(plan) if risk_row_nonempty(r)]
    out: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        out[i] = r
    return out
