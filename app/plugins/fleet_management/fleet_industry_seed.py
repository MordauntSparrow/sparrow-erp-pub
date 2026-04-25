"""
Industry-aware starter rows for ``fleet_vehicle_types``.

Reads tenant industries from the core manifest (``organization_profile.industries``),
same source as Core settings. Used on first empty seed from ``install`` migrations.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from app.organization_profile import industries_from_manifest, tenant_matches_industry

# (name, service_interval_days, service_interval_miles, safety_check_interval_days, sort_order)
_FleetTypeRow = Tuple[str, Any, Any, int, int]


def _core_manifest_path() -> Path:
    # .../app/plugins/fleet_management/fleet_industry_seed.py -> app/
    app_dir = Path(__file__).resolve().parents[2]
    return app_dir / "config" / "manifest.json"


def _load_core_manifest() -> Dict[str, Any]:
    p = _core_manifest_path()
    if not p.is_file():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def load_tenant_industries_for_fleet_seed() -> List[str]:
    return industries_from_manifest(_load_core_manifest())


def build_fleet_vehicle_type_seed_rows(
    tenant_industries: Sequence[str],
) -> List[_FleetTypeRow]:
    """
    Vehicle type presets tagged by industry. Always includes a neutral baseline
    plus logistics/HGV; adds ambulance-style types for medical, patrol vans for
    security, crew vans for cleaning, and shuttle/catering for hospitality.
    """
    rows: List[_FleetTypeRow] = []
    so = 0

    def add(
        name: str,
        service_days: Any,
        service_miles: Any,
        safety_days: int,
    ) -> None:
        nonlocal so
        rows.append((name, service_days, service_miles, safety_days, so))
        so += 10

    add("Unclassified", None, None, 42)

    if tenant_matches_industry(tenant_industries, "medical"):
        add("Blue light ambulance", 90, 6000, 21)
        add("Rapid response car", 90, 8000, 21)
        add("Patient transport ambulance", 120, 10000, 42)

    if tenant_matches_industry(tenant_industries, "security"):
        add("Patrol / response vehicle", 90, 10000, 42)
        add("Security operations van", 180, 12000, 56)

    if tenant_matches_industry(tenant_industries, "cleaning"):
        add("Cleaning crew van", 180, 15000, 56)
        add("Site facilities van", 180, 12000, 56)

    if tenant_matches_industry(tenant_industries, "hospitality"):
        add("Shuttle / minibus", 90, 8000, 42)
        add("Catering / events vehicle", 90, 6000, 42)

    add("Logistics van", 180, 12000, 56)
    add("HGV / lorry", 90, 25000, 42)
    return rows
