"""Redaction profiles for exports (PRD §4.4)."""
from __future__ import annotations

import re
from typing import Any

_NHS = re.compile(r"\b\d{3}\s?\d{3}\s?\d{4}\b")
_EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_PHONE = re.compile(r"\b(?:\+44\s?|0)\d{2,4}\s?\d{3,4}\s?\d{3,4}\b")
_UK_POST = re.compile(
    r"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b",
    re.IGNORECASE,
)

PROFILES = ("minimal", "standard", "strict")


def _redact(s: str | None, level: str) -> str:
    if not s:
        return ""
    t = str(s)
    if level == "minimal":
        return t
    t = _NHS.sub("[NHS]", t)
    t = _EMAIL.sub("[EMAIL]", t)
    if level in ("standard", "strict"):
        t = _PHONE.sub("[TEL]", t)
    if level == "strict":
        t = _UK_POST.sub("[POSTCODE]", t)
    return t


def apply_redaction_profile(events: list[dict[str, Any]], profile: str) -> list[dict[str, Any]]:
    p = (profile or "standard").strip().lower()
    if p not in PROFILES:
        p = "standard"
    if p == "minimal":
        return events
    out = []
    for ev in events:
        e2 = dict(ev)
        for k in ("summary", "actor", "detail_ref", "integrity_hint"):
            if k in e2:
                e2[k] = _redact(e2.get(k), p)
        out.append(e2)
    return out
