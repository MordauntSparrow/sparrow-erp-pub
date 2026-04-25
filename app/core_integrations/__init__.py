"""
Core accounting integrations (Xero, FreeAgent) — OAuth and deployment settings.

OAuth references (official):
- Xero: https://developer.xero.com/documentation/guides/oauth2/auth-flow
- FreeAgent: https://dev.freeagent.com/docs/oauth

CRM and other modules should import helpers from here instead of duplicating OAuth::

    from app.core_integrations import get_active_provider, push_draft_invoice

To appear on the admin Integrations page with permissions / resolved options, declare
``accounting_integration_consumer`` in the plugin manifest (see ``consumers.py``).
"""
from __future__ import annotations

from app.core_integrations.service import (
    get_active_provider,
    push_draft_invoice,
)

__all__ = ("get_active_provider", "push_draft_invoice")
