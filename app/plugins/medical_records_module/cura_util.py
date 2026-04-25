"""Shared helpers for Cura JSON routes (avoid circular imports)."""
from __future__ import annotations

import json


def safe_json(s):
    if s is None:
        return None
    if isinstance(s, (dict, list)):
        return s
    try:
        return json.loads(s)
    except Exception:
        return s
