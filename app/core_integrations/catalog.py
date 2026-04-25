"""
Accounting integration catalog for admin UI.

Live providers have OAuth implemented in ``app.core_integrations.service``.
"""
from __future__ import annotations

from typing import Any

LIVE_PROVIDERS: tuple[dict[str, Any], ...] = (
    {
        "id": "xero",
        "title": "Xero",
        "blurb": "UK-focused cloud accounting; MTD and filing stay in Xero.",
        "vendor_url": "https://go.xero.com",
        "docs_url": "https://developer.xero.com/documentation/guides/oauth2/auth-flow",
    },
    {
        "id": "freeagent",
        "title": "FreeAgent",
        "blurb": "UK accounting for small businesses and freelancers.",
        "vendor_url": "https://login.freeagent.com",
        "docs_url": "https://dev.freeagent.com/docs/oauth",
    },
    {
        "id": "quickbooks",
        "title": "QuickBooks Online",
        "blurb": "Intuit QuickBooks Online — OAuth 2.0; set INTUIT_ENVIRONMENT=sandbox for sandbox apps.",
        "vendor_url": "https://quickbooks.intuit.com",
        "docs_url": "https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0",
    },
    {
        "id": "sage",
        "title": "Sage (Business Cloud Accounting)",
        "blurb": "Sage Accounting API 3.1 — OAuth 2.0 (full_access scope for v1 connect).",
        "vendor_url": "https://www.sage.com",
        "docs_url": "https://developer.sage.com/accounting/guides/authentication/oauth/",
    },
    {
        "id": "freshbooks",
        "title": "FreshBooks",
        "blurb": "Cloud invoicing and accounting — OAuth 2.0 (JSON token endpoint).",
        "vendor_url": "https://www.freshbooks.com",
        "docs_url": "https://www.freshbooks.com/api/authentication",
    },
)

# Reserved for future catalogue entries (no cards when empty).
ROADMAP_PROVIDERS: tuple[dict[str, Any], ...] = ()
