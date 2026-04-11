"""Parse / serialise optional clinical handover block on event plans (ASHICE, medical control)."""
from __future__ import annotations

import json
from typing import Any

_ASHICE_KEYS = ("age", "sex", "history", "injuries", "current", "expected")


def default_clinical_handover() -> dict[str, Any]:
    return {
        "medical_control": "",
        "ashice": {k: "" for k in _ASHICE_KEYS},
    }


def parse_clinical_handover(raw: Any) -> dict[str, Any]:
    out = default_clinical_handover()
    if raw is None:
        return out
    if isinstance(raw, dict):
        data = raw
    elif isinstance(raw, (bytes, bytearray)):
        try:
            data = json.loads(raw.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            return out
        if not isinstance(data, dict):
            return out
    elif isinstance(raw, str) and raw.strip():
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return out
        if not isinstance(data, dict):
            return out
    else:
        return out
    mc = data.get("medical_control")
    if mc is not None and str(mc).strip():
        out["medical_control"] = str(mc).strip()
    ash = data.get("ashice") if isinstance(data.get("ashice"), dict) else {}
    for k in _ASHICE_KEYS:
        v = ash.get(k)
        if v is not None and str(v).strip():
            out["ashice"][k] = str(v).strip()
    return out


def clinical_handover_has_content(ch: dict[str, Any]) -> bool:
    if (ch.get("medical_control") or "").strip():
        return True
    ash = ch.get("ashice") or {}
    return any((ash.get(k) or "").strip() for k in _ASHICE_KEYS)


def summary_lines_for_handoff(ch: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    mc = (ch.get("medical_control") or "").strip()
    if mc:
        lines.append(f"Medical control: {mc}")
    ash = ch.get("ashice") or {}
    labels = {
        "age": "A (Age)",
        "sex": "S (Sex)",
        "history": "H (History)",
        "injuries": "I (Injuries / findings)",
        "current": "C (Current condition)",
        "expected": "E (Expected)",
    }
    ash_parts = []
    for k in _ASHICE_KEYS:
        v = (ash.get(k) or "").strip()
        if v:
            ash_parts.append(f"{labels[k]}: {v}")
    if ash_parts:
        lines.append("ASHICE: " + " | ".join(ash_parts))
    return lines
