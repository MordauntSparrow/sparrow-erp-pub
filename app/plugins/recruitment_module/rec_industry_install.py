"""
Tenant industry slugs for recruitment install-time seeding (Core ``organization_profile.industries``).

Same manifest path as Core / HR / Time & Billing — use ``app.organization_profile``.
"""
from __future__ import annotations

from app.organization_profile import (  # noqa: F401
    industries_from_manifest,
    load_core_manifest_dict,
    load_tenant_industries_for_install,
    normalize_organization_industries,
    tenant_matches_industry,
)

__all__ = [
    "industries_from_manifest",
    "load_core_manifest_dict",
    "load_tenant_industries_for_install",
    "normalize_organization_industries",
    "tenant_matches_industry",
]
