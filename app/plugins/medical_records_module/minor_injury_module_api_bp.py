"""
Handover base: ``/plugin/minor_injury_module/api/...`` (same handlers as Cura MI under medical_records).

Registered alongside ``internal_bp`` so legacy SPA base URLs keep working.
"""
from __future__ import annotations

from flask import Blueprint

minor_injury_module_api_bp = Blueprint(
    "minor_injury_module_api",
    __name__,
    url_prefix="/plugin/minor_injury_module/api",
)
