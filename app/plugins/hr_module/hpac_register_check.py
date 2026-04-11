"""
HPAC (Health Practice Associates Council, https://www.hpac-uk.org/) register checks.

The public site describes "Check the Register" and an organisation portal; there is no
published REST/JSON API documented for automated employer verification. Certificate
validation exists for e-LfH-style certificates (see hpac-uk.org), not a bulk registrant API.

If HPAC provides a partner API or data feed in future, wire it here and set
``automated_check_available`` from configuration.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple


def hpac_register_integration_info() -> Dict[str, Any]:
    """Metadata for admins / logs (not an HTTP call to HPAC)."""
    return {
        "automated_check_available": False,
        "register_url": "https://www.hpac-uk.org/check-the-register",
        "organisation_portal_url": "https://www.hpac-uk.org/",
        "notes": (
            "No employer API is documented on hpac-uk.org for Sparrow to poll registration "
            "status. Use the HPAC register or organisation portal, then record the outcome "
            "in HR. Contact HPAC if you need machine-readable access."
        ),
    }


def run_hpac_register_status_check(contractor_id: int) -> Tuple[bool, str]:
    """
    Placeholder for a future HPAC API. Today always returns unsupported.

    Returns (ok, message) where ok False means no automated update was applied.
    """
    _ = int(contractor_id)
    info = hpac_register_integration_info()
    return False, info["notes"]
