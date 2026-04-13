"""CRM settings stored in the core manifest (``crm_module_settings``)."""
from __future__ import annotations

import json
import os
from typing import Any

from flask import Flask


def core_manifest_path(app: Flask) -> str:
    return os.path.join(app.root_path, "config", "manifest.json")


def get_crm_module_settings(app: Flask) -> dict[str, Any]:
    """Defaults merged with ``manifest.json`` → ``crm_module_settings``."""
    defaults: dict[str, Any] = {
        "event_plan_pdf_about_us": "",
        "event_plan_pdf_tagline": "",
    }
    path = core_manifest_path(app)
    if not os.path.isfile(path):
        return dict(defaults)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return dict(defaults)
    if not isinstance(data, dict):
        return dict(defaults)
    raw = data.get("crm_module_settings")
    if not isinstance(raw, dict):
        return dict(defaults)
    out = {**defaults, **raw}
    for k in defaults:
        if k in out and isinstance(out[k], str):
            continue
        out[k] = defaults[k]
    return out


def update_crm_module_settings(app: Flask, updates: dict[str, Any]) -> None:
    """Merge ``updates`` into ``crm_module_settings`` and write manifest."""
    path = core_manifest_path(app)
    data: dict[str, Any] = {}
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                data = loaded
        except (OSError, json.JSONDecodeError):
            data = {}
    cur = data.get("crm_module_settings")
    if not isinstance(cur, dict):
        cur = {}
    merged = {**cur, **{k: v for k, v in updates.items() if v is not None}}
    data["crm_module_settings"] = merged
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
