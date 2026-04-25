"""Human-readable labels for equipment / serial-asset UI (DB stores snake_case codes)."""

from __future__ import annotations

from typing import Optional

# inventory_equipment_assets.status
ASSET_STATUS_LABELS = {
    "in_stock": "In stock",
    "assigned": "Assigned",
    "loaned": "Loaned",
    "maintenance": "Maintenance",
    "retired": "Retired",
    "lost": "Lost",
}

# asset_equipment_issues.status
ISSUE_STATUS_LABELS = {
    "open": "Open",
    "monitoring": "Monitoring",
    "fix_planned": "Fix planned",
    "off_service": "Off service",
    "sent_external": "Sent external / repair",
    "resolved": "Resolved",
    "closed": "Closed",
}

ISSUE_SEVERITY_LABELS = {
    "info": "Info",
    "low": "Low",
    "medium": "Medium",
    "high": "High",
    "critical": "Critical",
}

HOLDER_TYPE_LABELS = {
    "vehicle": "Fleet vehicle",
    "user": "User",
    "contractor": "Contractor",
    "location": "Location",
}

# inventory_transactions.transaction_type (common values)
TRANSACTION_TYPE_LABELS = {
    "in": "Stock in",
    "out": "Sign out",
    "return": "Return",
    "transfer": "Transfer",
    "adjustment": "Adjustment",
    "repack": "Repack",
}

# inventory_locations.type (extensible)
LOCATION_TYPE_LABELS = {
    "holding": "Holding",
    "virtual": "Virtual",
    "storeroom": "Storeroom",
    "warehouse": "Warehouse",
    "vehicle": "Vehicle",
    "site": "Site",
    "training": "Training pool (non-patient-facing stock)",
}


def _norm(code: Optional[str]) -> str:
    return str(code or "").strip().lower()


def equipment_asset_status_label(code: Optional[str]) -> str:
    k = _norm(code)
    if not k:
        return "—"
    return ASSET_STATUS_LABELS.get(k, str(code).replace("_", " ").strip().title() or "—")


def equipment_issue_status_label(code: Optional[str]) -> str:
    k = _norm(code)
    if not k:
        return "—"
    return ISSUE_STATUS_LABELS.get(k, str(code).replace("_", " ").strip().title() or "—")


def equipment_issue_severity_label(code: Optional[str]) -> str:
    k = _norm(code)
    if not k:
        return "—"
    return ISSUE_SEVERITY_LABELS.get(k, str(code).replace("_", " ").strip().title() or "—")


def equipment_holder_type_label(code: Optional[str]) -> str:
    k = _norm(code)
    if not k:
        return "—"
    return HOLDER_TYPE_LABELS.get(k, str(code).replace("_", " ").strip().title() or "—")


def inventory_transaction_type_label(code: Optional[str]) -> str:
    k = _norm(code)
    if not k:
        return "—"
    return TRANSACTION_TYPE_LABELS.get(k, str(code).replace("_", " ").strip().title() or "—")


def inventory_location_type_label(code: Optional[str]) -> str:
    k = _norm(code)
    if not k:
        return "—"
    return LOCATION_TYPE_LABELS.get(k, str(code).replace("_", " ").strip().title() or "—")


def contractor_kit_request_status_label(code: Optional[str]) -> str:
    k = _norm(code)
    return {
        "pending": "Pending",
        "fulfilled": "Fulfilled",
        "declined": "Declined",
    }.get(k, str(code).replace("_", " ").strip().title() or "—")
