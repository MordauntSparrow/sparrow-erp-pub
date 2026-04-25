"""
UK GOV fleet APIs — Sparrow-level defaults (not per-tenant UI).

When DVSA / DVLA approve your application, paste the issued keys here and ship a
normal software update. All deployments of that build share the same keys.

Resolution order (see ``_uk_lookup_config`` in routes):
  1. Non-empty string in this module (canonical for production)
  2. Flask ``app.config`` (``FLEET_UK_*``)
  3. Environment variables (optional local / CI override)

``FLEET_UK_*`` env or ``app.config`` still overrides any value below when set.

Embedded Sparrow build: DVSA-approved MOT History credentials ship here so the registration
checker works out of the box. ``FLEET_UK_*`` env or ``app.config`` still overrides when set.
Logs never print keys or Bearer tokens.
"""

# DVSA MOT History API (OAuth2 + X-API-Key) — see uk_vehicle_lookup.py.
# Values must match the DVSA approval email (token URL tenant GUID is exact).
MOT_API_KEY: str = "82nIDi8y1o3jhlXM3dUiu1ZuHRObHkFXiieJMgFj"
MOT_API_BASE_URL: str = "https://tapi.dvsa.gov.uk"
MOT_CLIENT_ID: str = "4bee9e41-821f-4ff6-93df-1ad26e496748"
MOT_CLIENT_SECRET: str = "SOf8Q~HkWKEY.g4PkhkxaVaxy2BPzfxz76v_Wa5X"
# If Azure returns AADSTS90002 (tenant not found), confirm this URL matches the DVSA email exactly.
MOT_TOKEN_URL: str = (
    "https://login.microsoftonline.com/a455b827-244f-4c97-b5b4-ce5d13b4d00c/oauth2/v2.0/token"
)
MOT_SCOPE: str = "https://tapi.dvsa.gov.uk/.default"

# DVLA Vehicle Enquiry Service — POST JSON, x-api-key header
DVLA_API_KEY: str = "DCJizfkdht8TkuOoy43kB9QKd51Gv2vC99RrBd32"

DVLA_VEHICLE_ENQUIRY_URL: str = (
    "https://driver-vehicle-licensing.api.gov.uk/vehicle-enquiry/v1/vehicles"
)
