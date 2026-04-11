"""Evaluate rule conditions_json (internal) against quote calculator inputs."""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Any


def _parse_conditions(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return {}
        try:
            v = json.loads(s)
            return v if isinstance(v, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def rule_conditions_met(
    conditions_raw: Any,
    *,
    crowd_size: int | None,
    duration_hours: Decimal | None,
) -> bool:
    """
    If any bound is set, the quote inputs must satisfy all of them.
    Keys: min_crowd, max_crowd, min_hours, max_hours (optional integers / decimals).
    """
    c = _parse_conditions(conditions_raw)
    if not c:
        return True

    crowd = int(crowd_size or 0)
    try:
        hours = float(duration_hours or 0)
    except Exception:
        hours = 0.0

    if c.get("min_crowd") is not None and str(c["min_crowd"]).strip() != "":
        try:
            if crowd < int(c["min_crowd"]):
                return False
        except (TypeError, ValueError):
            pass

    if c.get("max_crowd") is not None and str(c["max_crowd"]).strip() != "":
        try:
            if crowd > int(c["max_crowd"]):
                return False
        except (TypeError, ValueError):
            pass

    if c.get("min_hours") is not None and str(c["min_hours"]).strip() != "":
        try:
            if hours < float(c["min_hours"]):
                return False
        except (TypeError, ValueError):
            pass

    if c.get("max_hours") is not None and str(c["max_hours"]).strip() != "":
        try:
            if hours > float(c["max_hours"]):
                return False
        except (TypeError, ValueError):
            pass

    return True


def build_conditions_from_form(form) -> str | None:
    """Build internal JSON from admin form fields (no raw JSON for operators)."""
    parts: dict[str, Any] = {}

    for form_key, json_key in (
        ("cond_min_crowd", "min_crowd"),
        ("cond_max_crowd", "max_crowd"),
    ):
        v = (form.get(form_key) or "").strip()
        if v.isdigit() or (v.startswith("-") and v[1:].isdigit()):
            parts[json_key] = int(v)

    v = (form.get("cond_min_hours") or "").strip()
    if v:
        try:
            parts["min_hours"] = float(v)
        except ValueError:
            pass
    v = (form.get("cond_max_hours") or "").strip()
    if v:
        try:
            parts["max_hours"] = float(v)
        except ValueError:
            pass

    if not parts:
        return None
    return json.dumps(parts, separators=(",", ":"))
