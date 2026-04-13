"""
Tenant industry slugs for install-time seeding (PRD: organisation_profile.industries).

Same manifest path and normalisation as Core / Time & Billing — use only from install
or other offline contexts (no Flask request).
"""
from __future__ import annotations

import json
from pathlib import Path

from app.organization_profile import industries_from_manifest, normalize_organization_industries


def _core_manifest_path() -> Path:
    here = Path(__file__).resolve()
    app_root = here.parents[2]
    return app_root / "config" / "manifest.json"


def load_tenant_industries_for_install() -> list[str]:
    path = _core_manifest_path()
    if not path.is_file():
        return normalize_organization_industries(None)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError, TypeError):
        return normalize_organization_industries(None)
    return industries_from_manifest(data if isinstance(data, dict) else {})
