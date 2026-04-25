"""Short, user-facing event labels for Cura / Ventus (avoid CRM compound titles in ops UI)."""
from __future__ import annotations

import json
import re
from typing import Any

_TITLE_SPLIT_RE = re.compile(r"\s*[–—]\s*")


def parse_lead_meta_json(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        try:
            v = json.loads(s)
            return dict(v) if isinstance(v, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _title_segments(title: str) -> list[str]:
    t = (title or "").strip()
    if not t:
        return []
    return [p.strip() for p in _TITLE_SPLIT_RE.split(t) if p.strip()]


def collapse_redundant_compound_title(title: str | None) -> str:
    """
    Turn strings like ``Event — Event 2026 — Event`` into ``Event``.
    If the first and last segment match (case-insensitive), use the first segment only.
    Otherwise return the first segment when there are multiple parts (CRM convention:
    ``{event} — {org}`` on opportunities).
    """
    parts = _title_segments(title or "")
    if not parts:
        return (title or "").strip()
    if len(parts) >= 2 and parts[0].casefold() == parts[-1].casefold():
        return parts[0]
    if len(parts) >= 2:
        return parts[0]
    return parts[0]


def friendly_event_display_name(
    *,
    lead_meta: dict[str, Any] | None,
    quote_title: str | None = None,
    opportunity_name: str | None = None,
    plan_title: str | None = None,
    fallback: str | None = None,
    max_len: int = 255,
) -> str:
    """
    Single label for Cura operational periods, Ventus division names, and similar UI.

    Prefers ``lead_meta['event_name']``, then shortens compound CRM strings.
    """
    if isinstance(lead_meta, dict):
        ev = str(lead_meta.get("event_name") or "").strip()
        if ev:
            return ev[:max_len]

    for raw in (quote_title, opportunity_name, plan_title):
        s = (raw or "").strip()
        if not s:
            continue
        collapsed = collapse_redundant_compound_title(s)
        if collapsed:
            return collapsed[:max_len]

    fb = (fallback or "").strip()
    return fb[:max_len] if fb else "Event"
