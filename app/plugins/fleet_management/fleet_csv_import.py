"""CSV fleet vehicle import: parse file and map columns to fleet_vehicles + fleet_compliance."""

from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

# CSV column header -> internal field (value shown in mapping UI)
FLEET_IMPORT_FIELDS: List[Tuple[str, str]] = [
    ("", "— Ignore column —"),
    ("registration", "Registration (optional for unregistered assets)"),
    ("internal_code", "Unit name / callsign (internal code)"),
    ("make", "Make"),
    ("model", "Model"),
    ("year", "Year"),
    ("fuel_type", "Fuel type"),
    ("status", "Status (active, off_road, maintenance, decommissioned, pending_road_test)"),
    ("vin", "VIN"),
    ("notes", "Notes"),
    ("mot_expiry", "MOT expiry (YYYY-MM-DD)"),
    ("tax_expiry", "Tax due / expiry date"),
    ("insurance_expiry", "Insurance expiry"),
    ("last_service_date", "Last service date"),
    ("last_service_mileage", "Last service mileage"),
    ("next_service_due_date", "Next service due (date)"),
    ("next_service_due_mileage", "Next service due (mileage)"),
    ("servicing_notes", "Servicing notes"),
]

_STATUS_ALIASES = {
    "active": "active",
    "on road": "active",
    "in service": "active",
    "road": "active",
    "off road": "off_road",
    "off_road": "off_road",
    "sorn": "off_road",
    "vor": "off_road",
    "maintenance": "maintenance",
    "workshop": "maintenance",
    "decommissioned": "decommissioned",
    "sold": "decommissioned",
    "pending": "pending_road_test",
    "pending_road_test": "pending_road_test",
    "road test": "pending_road_test",
}


def normalize_registration(raw: str) -> str:
    return re.sub(r"\s+", "", (raw or "").strip().upper())


def parse_csv_file(content: bytes, max_rows: int = 2500) -> Tuple[List[str], List[Dict[str, str]]]:
    """Return (headers, rows as dicts). Tries utf-8-sig then latin-1."""
    text = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        text = content.decode("utf-8", errors="replace")

    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel

    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    headers = [h.strip() for h in (reader.fieldnames or []) if h is not None]
    rows: List[Dict[str, str]] = []
    for i, row in enumerate(reader):
        if i >= max_rows:
            break
        clean = {
            (k or "").strip(): (v or "").strip()
            for k, v in row.items()
            if k is not None
        }
        if any(clean.values()):
            rows.append(clean)
    return headers, rows


def _parse_date(val: str) -> Optional[str]:
    if not val or not str(val).strip():
        return None
    s = str(val).strip()[:32]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date().isoformat()
        except ValueError:
            continue
    try:
        if s.replace(".", "").replace("-", "").isdigit():
            n = float(s)
            if 30000 < n < 60000:
                from datetime import date as ddate, timedelta

                base = ddate(1899, 12, 30)
                return (base + timedelta(days=int(n))).isoformat()
    except Exception:
        pass
    return None


def _parse_int(val: str) -> Optional[int]:
    if not val or not str(val).strip():
        return None
    try:
        return int(float(str(val).strip().replace(",", "")))
    except ValueError:
        return None


def _map_status(val: str) -> str:
    key = (val or "").strip().lower().replace("-", " ")
    return _STATUS_ALIASES.get(key, "active")


def build_row_payload(
    row: Dict[str, str], header_to_field: Dict[str, str]
) -> Dict[str, Any]:
    """Map one CSV row using header -> system field."""
    acc: Dict[str, Any] = {}
    for header, field in header_to_field.items():
        if not field or field == "":
            continue
        raw = row.get(header, "")
        if raw is None:
            continue
        s = str(raw).strip()
        if not s:
            continue
        if field == "registration":
            acc["registration"] = normalize_registration(s)
        elif field == "internal_code":
            acc["internal_code"] = s[:64]
        elif field == "year":
            acc["year"] = _parse_int(s)
        elif field == "status":
            acc["status"] = _map_status(s)
        elif field in (
            "mot_expiry",
            "tax_expiry",
            "insurance_expiry",
            "last_service_date",
            "next_service_due_date",
        ):
            d = _parse_date(s)
            if d:
                acc[field] = d
        elif field in ("last_service_mileage", "next_service_due_mileage"):
            acc[field] = _parse_int(s)
        else:
            acc[field] = s[:512] if field != "notes" else s
    return acc


def import_fleet_rows(
    rows: List[Dict[str, str]],
    header_to_field: Dict[str, str],
    *,
    update_existing: bool,
    user_id: Optional[str],
    create_vehicle: Callable[..., int],
    update_vehicle: Callable[..., None],
    get_vehicle_by_registration: Callable[[str], Any],
    get_vehicle_by_internal_code: Callable[[str], Any],
) -> Dict[str, Any]:
    created = 0
    updated = 0
    skipped = 0
    errors: List[str] = []

    for idx, row in enumerate(rows, start=2):
        payload = build_row_payload(row, header_to_field)
        reg = payload.get("registration")
        ic = (payload.get("internal_code") or "").strip() or None
        existing = None
        if reg:
            existing = get_vehicle_by_registration(reg)
        if existing is None and ic:
            existing = get_vehicle_by_internal_code(ic)
        row_key = reg or ic or "?"
        if not reg and not ic:
            skipped += 1
            errors.append(
                f"Row {idx}: need a registration and/or unit name (internal code) in mapped columns"
            )
            continue
        try:
            if existing:
                if not update_existing:
                    skipped += 1
                    continue
                vid = int(existing["id"])
                update_vehicle(
                    vid,
                    internal_code=payload.get("internal_code"),
                    registration=payload.get("registration"),
                    make=payload.get("make"),
                    model=payload.get("model"),
                    year=payload.get("year"),
                    fuel_type=payload.get("fuel_type"),
                    status=payload.get("status"),
                    notes=payload.get("notes"),
                    vin=payload.get("vin"),
                    mot_expiry=payload.get("mot_expiry"),
                    tax_expiry=payload.get("tax_expiry"),
                    insurance_expiry=payload.get("insurance_expiry"),
                    last_service_date=payload.get("last_service_date"),
                    last_service_mileage=payload.get("last_service_mileage"),
                    next_service_due_date=payload.get("next_service_due_date"),
                    next_service_due_mileage=payload.get("next_service_due_mileage"),
                    servicing_notes=payload.get("servicing_notes"),
                    user_id=user_id,
                )
                updated += 1
            else:
                create_vehicle(
                    registration=reg,
                    make=payload.get("make"),
                    model=payload.get("model"),
                    year=payload.get("year"),
                    fuel_type=payload.get("fuel_type"),
                    status=payload.get("status", "active"),
                    notes=payload.get("notes"),
                    internal_code=payload.get("internal_code"),
                    mot_expiry=payload.get("mot_expiry"),
                    tax_expiry=payload.get("tax_expiry"),
                    insurance_expiry=payload.get("insurance_expiry"),
                    vin=payload.get("vin"),
                    last_service_date=payload.get("last_service_date"),
                    last_service_mileage=payload.get("last_service_mileage"),
                    next_service_due_date=payload.get("next_service_due_date"),
                    next_service_due_mileage=payload.get("next_service_due_mileage"),
                    servicing_notes=payload.get("servicing_notes"),
                    user_id=user_id,
                )
                created += 1
        except Exception as e:
            errors.append(f"Row {idx} ({row_key}): {e}")

    return {
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "errors": errors[:200],
    }
