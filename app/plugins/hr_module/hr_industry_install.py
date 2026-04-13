"""
Tenant industry slugs for HR install-time seeding (Core manifest ``organization_profile.industries``).

Delegates to ``app.organization_profile`` so Time Billing and other modules share one manifest path.
"""
from __future__ import annotations

from app.organization_profile import (  # noqa: F401
    industries_from_manifest,
    load_core_manifest_dict,
    load_tenant_industries_for_install,
    normalize_organization_industries,
)

__all__ = [
    "industries_from_manifest",
    "load_core_manifest_dict",
    "load_tenant_industries_for_install",
    "normalize_organization_industries",
]
