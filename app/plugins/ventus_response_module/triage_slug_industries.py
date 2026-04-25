"""
Triage intake profile slug → tenant industries (OR) for Ventus Response.

Kept separate from routes.py so install.py can filter DB seeds without importing
the full blueprint. Must stay aligned with product rules for which sectors see
which intake profiles.
"""

# Slug → industries that should see it (OR). Omitted slug = all tenants.
VENTUS_TRIAGE_SLUG_INDUSTRIES = {
    "urgent_care": ("medical",),
    "emergency_999": ("medical",),
    "event_medical": ("medical", "hospitality"),
    "patient_transport": ("medical",),
    "mental_health_support": ("medical",),
    "security_response": ("security",),
    "private_police": ("security",),
    "vehicle_recovery": ("medical", "security", "cleaning", "hospitality"),
    "welfare_check": ("medical", "security", "hospitality"),
    "fire_support": ("medical", "security", "cleaning"),
    "search_and_rescue": ("medical", "security"),
    "facilities_cleaning": ("cleaning",),
    "venue_guest_incident": ("hospitality",),
    "general_dispatch": ("medical", "security", "cleaning", "hospitality"),
    "training_simulation": ("medical", "security", "cleaning", "hospitality"),
    "multi_agency_coordination": ("medical", "security", "cleaning", "hospitality"),
}
