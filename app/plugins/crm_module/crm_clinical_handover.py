"""Parse / serialise optional clinical handover block on event plans (pre-alert policy, medical control)."""
from __future__ import annotations

import json
from typing import Any

_ASHICE_KEYS = ("age", "sex", "history", "injuries", "current", "expected")
_ATMIST_KEYS = ("age", "time", "mechanism", "injuries", "signs", "treatment")

_ASHICE_LABELS = {
    "age": "A — Age",
    "sex": "S — Sex",
    "history": "H — History",
    "injuries": "I — Injuries / findings",
    "current": "C — Current condition",
    "expected": "E — Expected / ETA",
}

_ATMIST_LABELS = {
    "age": "A — Age",
    "time": "T — Time (of incident / injury)",
    "mechanism": "M — Mechanism of injury",
    "injuries": "I — Injuries / problem",
    "signs": "S — Signs / observations",
    "treatment": "T — Treatment given",
}


def _pad_custom_pre_alert_edit(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Enough blank rows to edit without truncating saved rows (capped)."""
    cleaned: list[dict[str, str]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        letter = str(r.get("letter") or "").strip()[:16]
        detail = str(r.get("detail") or "").strip()[:2000]
        cleaned.append({"letter": letter, "detail": detail})
    n = len(cleaned)
    size = max(2, min(24, n + 2))
    out = list(cleaned[:size])
    while len(out) < size:
        out.append({"letter": "", "detail": ""})
    return out


def default_clinical_handover() -> dict[str, Any]:
    return {
        "medical_control": "",
        "pre_alert_policy": "ashice",
        "ashice": {k: "" for k in _ASHICE_KEYS},
        "atmist": {k: "" for k in _ATMIST_KEYS},
        "custom_pre_alert": [],
        "custom_pre_alert_edit": _pad_custom_pre_alert_edit([]),
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
    pol = data.get("pre_alert_policy")
    if isinstance(pol, str) and pol.strip().lower() in ("ashice", "atmist", "custom"):
        out["pre_alert_policy"] = pol.strip().lower()
    ash = data.get("ashice") if isinstance(data.get("ashice"), dict) else {}
    for k in _ASHICE_KEYS:
        v = ash.get(k)
        if v is not None and str(v).strip():
            out["ashice"][k] = str(v).strip()
    atm = data.get("atmist") if isinstance(data.get("atmist"), dict) else {}
    for k in _ATMIST_KEYS:
        v = atm.get(k)
        if v is not None and str(v).strip():
            out["atmist"][k] = str(v).strip()
    custom_raw = data.get("custom_pre_alert")
    custom: list[dict[str, str]] = []
    if isinstance(custom_raw, list):
        for x in custom_raw:
            if not isinstance(x, dict):
                continue
            letter = str(x.get("letter") or "").strip()[:16]
            detail = str(x.get("detail") or "").strip()[:2000]
            if letter or detail:
                custom.append({"letter": letter, "detail": detail})
    out["custom_pre_alert"] = custom
    out["custom_pre_alert_edit"] = _pad_custom_pre_alert_edit(custom)
    return out


def clinical_pre_alert_has_display(ch: dict[str, Any]) -> bool:
    """True when the pre-alert policy block (ASHICE / ATMIST / custom) should appear on PDF."""
    pol = (ch.get("pre_alert_policy") or "ashice").strip().lower()
    if pol == "custom":
        for r in ch.get("custom_pre_alert") or []:
            if not isinstance(r, dict):
                continue
            if (str(r.get("letter") or "").strip() or str(r.get("detail") or "").strip()):
                return True
        return False
    if pol == "atmist":
        atm = ch.get("atmist") or {}
        return any((atm.get(k) or "").strip() for k in _ATMIST_KEYS)
    ash = ch.get("ashice") or {}
    return any((ash.get(k) or "").strip() for k in _ASHICE_KEYS)


def clinical_handover_has_content(ch: dict[str, Any]) -> bool:
    if (ch.get("medical_control") or "").strip():
        return True
    return clinical_pre_alert_has_display(ch)


def summary_lines_for_handoff(ch: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    mc = (ch.get("medical_control") or "").strip()
    if mc:
        lines.append(f"Medical control: {mc}")
    pol = (ch.get("pre_alert_policy") or "ashice").strip().lower()
    if pol == "custom":
        parts: list[str] = []
        for r in ch.get("custom_pre_alert") or []:
            if not isinstance(r, dict):
                continue
            letter = (r.get("letter") or "").strip()
            detail = (r.get("detail") or "").strip()
            if not letter and not detail:
                continue
            if letter and detail:
                parts.append(f"{letter}: {detail}")
            elif detail:
                parts.append(detail)
            else:
                parts.append(letter)
        if parts:
            lines.append("Pre-alert (custom): " + " | ".join(parts))
        return lines
    if pol == "atmist":
        atm = ch.get("atmist") or {}
        labels = {
            "age": "A (Age)",
            "time": "T (Time)",
            "mechanism": "M (Mechanism)",
            "injuries": "I (Injuries)",
            "signs": "S (Signs)",
            "treatment": "T (Treatment)",
        }
        atm_parts = []
        for k in _ATMIST_KEYS:
            v = (atm.get(k) or "").strip()
            if v:
                atm_parts.append(f"{labels[k]}: {v}")
        if atm_parts:
            lines.append("ATMIST: " + " | ".join(atm_parts))
        return lines
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


def ashice_editor_rows() -> list[tuple[str, str]]:
    """Ordered (field key, letter column label) for plan editor / templates."""
    return [(k, _ASHICE_LABELS[k]) for k in _ASHICE_KEYS]


def atmist_editor_rows() -> list[tuple[str, str]]:
    return [(k, _ATMIST_LABELS[k]) for k in _ATMIST_KEYS]


def ashice_pdf_rows(ch: dict[str, Any]) -> list[tuple[str, str]]:
    ash = ch.get("ashice") or {}
    return [(_ASHICE_LABELS[k], (ash.get(k) or "").strip() or "—") for k in _ASHICE_KEYS]


def atmist_pdf_rows(ch: dict[str, Any]) -> list[tuple[str, str]]:
    atm = ch.get("atmist") or {}
    return [(_ATMIST_LABELS[k], (atm.get(k) or "").strip() or "—") for k in _ATMIST_KEYS]


def custom_pre_alert_pdf_rows(ch: dict[str, Any]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for r in ch.get("custom_pre_alert") or []:
        if not isinstance(r, dict):
            continue
        letter = (r.get("letter") or "").strip() or "—"
        detail = (r.get("detail") or "").strip() or "—"
        rows.append((letter, detail))
    return rows
