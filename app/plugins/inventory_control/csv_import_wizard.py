"""
Server-side CSV import helpers for Inventory (items, serial equipment, locations).
Used by the admin mapping wizard — not a public API.
"""

from __future__ import annotations

import csv
import io
import os
import re
import tempfile
from typing import Any, Dict, List, Optional, Tuple

SESSION_KEY = "inv_csv_import_wizard_v1"

# entity -> list of {key, label, required}
IMPORT_FIELD_SPECS: Dict[str, List[Dict[str, Any]]] = {
    "items": [
        {"key": "sku", "label": "SKU / code", "required": True},
        {"key": "name", "label": "Name", "required": True},
        {"key": "description", "label": "Description", "required": False},
        {"key": "unit", "label": "Unit (ea, box…)", "required": False},
        {"key": "barcode", "label": "Barcode", "required": False},
        {"key": "category", "label": "Category (text)", "required": False},
        {"key": "is_equipment", "label": "Equipment model (yes/no)", "required": False},
        {"key": "requires_serial", "label": "Requires serial (yes/no)", "required": False},
        {"key": "reorder_point", "label": "Reorder point", "required": False},
        {"key": "reorder_quantity", "label": "Reorder qty", "required": False},
        {"key": "standard_cost", "label": "Standard cost", "required": False},
    ],
    "equipment_assets": [
        {"key": "item_sku", "label": "Equipment model SKU", "required": True},
        {"key": "serial_number", "label": "Serial number", "required": True},
        {"key": "public_asset_code", "label": "Asset tag / public code", "required": False},
        {"key": "make", "label": "Make", "required": False},
        {"key": "model", "label": "Model", "required": False},
        {"key": "purchase_date", "label": "Purchase date", "required": False},
        {"key": "warranty_expiry", "label": "Warranty until", "required": False},
        {"key": "warranty_start_date", "label": "Warranty start", "required": False},
        {"key": "warranty_months", "label": "Warranty (months)", "required": False},
        {"key": "service_interval_days", "label": "Service interval (days)", "required": False},
        {"key": "next_service_due_date", "label": "Next service due", "required": False},
        {"key": "condition", "label": "Condition", "required": False},
    ],
    "locations": [
        {"key": "code", "label": "Location code", "required": True},
        {"key": "name", "label": "Location name", "required": True},
        {"key": "type", "label": "Type (warehouse/store/bin/holding/virtual)", "required": False},
        {"key": "address", "label": "Address / notes", "required": False},
    ],
}


def allowed_entities() -> Tuple[str, ...]:
    return tuple(IMPORT_FIELD_SPECS.keys())


def parse_bool_cell(val: Any) -> Optional[bool]:
    if val is None:
        return None
    s = str(val).strip().lower()
    if not s:
        return None
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return None


def parse_float_cell(val: Any) -> Optional[float]:
    if val is None or str(val).strip() == "":
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except ValueError:
        return None


def parse_int_cell(val: Any) -> Optional[int]:
    if val is None or str(val).strip() == "":
        return None
    try:
        return int(float(str(val).strip()))
    except ValueError:
        return None


def normalize_date(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            from datetime import datetime

            return datetime.strptime(s[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def sniff_read_headers_and_sample(
    raw_bytes: bytes, max_sample_rows: int = 8
) -> Tuple[List[str], List[Dict[str, str]], str]:
    """Return (headers, sample row dicts, encoding note)."""
    text = raw_bytes.decode("utf-8-sig", errors="replace")
    if not text.strip():
        return [], [], "utf-8"
    f = io.StringIO(text)
    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    f.seek(0)
    reader = csv.DictReader(f, dialect=dialect)
    headers = [h for h in (reader.fieldnames or []) if h is not None]
    rows: List[Dict[str, str]] = []
    for i, row in enumerate(reader):
        if i >= max_sample_rows:
            break
        clean = {k: (v or "").strip() if isinstance(v, str) else str(v or "") for k, v in row.items()}
        rows.append(clean)
    return headers, rows, "utf-8"


def row_from_mapping(
    row: Dict[str, str], mapping: Dict[str, str], entity: str
) -> Dict[str, Any]:
    """Map CSV row to system keys using mapping system_key -> csv column name."""
    out: Dict[str, Any] = {}
    for spec in IMPORT_FIELD_SPECS.get(entity, []):
        key = spec["key"]
        col = mapping.get(key) or ""
        if not col or col == "__skip__":
            continue
        out[key] = row.get(col, "")
    return out


def _item_id_for_sku(inv, sku: str) -> Optional[int]:
    sku = (sku or "").strip()
    if not sku:
        return None
    conn = inv._connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM inventory_items WHERE sku = %s LIMIT 1", (sku,))
        r = cur.fetchone()
        return int(r[0]) if r else None
    finally:
        cur.close()


def run_import(
    inv,
    *,
    entity: str,
    file_path: str,
    mapping: Dict[str, str],
) -> Dict[str, Any]:
    """
    Execute import. Returns counts and error lines.
    """
    result: Dict[str, Any] = {
        "created": 0,
        "updated": 0,
        "skipped": 0,
        "errors": [],
    }
    if entity not in IMPORT_FIELD_SPECS:
        result["errors"].append("Unknown import type.")
        return result
    if not os.path.isfile(file_path):
        result["errors"].append("Upload file missing. Start again.")
        return result

    raw = open(file_path, "rb").read()
    text = raw.decode("utf-8-sig", errors="replace")
    f = io.StringIO(text)
    try:
        dialect = csv.Sniffer().sniff(text[:8192], delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    f.seek(0)
    reader = csv.DictReader(f, dialect=dialect)
    line_no = 1

    rows_done = 0
    for row in reader:
        line_no += 1
        rows_done += 1
        if rows_done > max_rows:
            result["errors"].append(
                f"Import stopped after {max_rows} rows. Run again with another file for the rest."
            )
            break
        clean = {
            k: (v or "").strip() if isinstance(v, str) else str(v or "")
            for k, v in (row or {}).items()
        }
        data = row_from_mapping(clean, mapping, entity)
        try:
            if entity == "items":
                sku = (data.get("sku") or "").strip()
                name = (data.get("name") or "").strip()
                if not sku or not name:
                    result["skipped"] += 1
                    continue
                existing = _item_id_for_sku(inv, sku)
                payload = {
                    "sku": sku,
                    "name": name,
                    "description": (data.get("description") or "").strip() or None,
                    "unit": (data.get("unit") or "").strip() or "ea",
                    "barcode": (data.get("barcode") or "").strip() or None,
                    "category": (data.get("category") or "").strip() or None,
                    "is_active": True,
                    "is_equipment": parse_bool_cell(data.get("is_equipment")),
                    "requires_serial": parse_bool_cell(data.get("requires_serial")),
                    "reorder_point": parse_float_cell(data.get("reorder_point")) or 0,
                    "reorder_quantity": parse_float_cell(data.get("reorder_quantity")) or 0,
                    "standard_cost": parse_float_cell(data.get("standard_cost")),
                }
                if payload["is_equipment"] is None:
                    payload["is_equipment"] = False
                if payload["requires_serial"] is None:
                    payload["requires_serial"] = bool(payload["is_equipment"])
                if existing:
                    inv.update_item(
                        int(existing),
                        {
                            "name": payload["name"],
                            "description": payload["description"],
                            "unit": payload["unit"],
                            "barcode": payload["barcode"],
                            "category": payload["category"],
                            "is_equipment": payload["is_equipment"],
                            "requires_serial": payload["requires_serial"],
                            "reorder_point": payload["reorder_point"],
                            "reorder_quantity": payload["reorder_quantity"],
                            "standard_cost": payload["standard_cost"],
                        },
                    )
                    result["updated"] += 1
                else:
                    inv.create_item(payload)
                    result["created"] += 1

            elif entity == "equipment_assets":
                item_sku = (data.get("item_sku") or "").strip()
                serial = (data.get("serial_number") or "").strip()
                if not item_sku or not serial:
                    result["skipped"] += 1
                    continue
                iid = _item_id_for_sku(inv, item_sku)
                if not iid:
                    result["errors"].append(f"Line {line_no}: unknown model SKU '{item_sku}'")
                    continue
                item = inv.get_item(int(iid))
                if not item or not int(item.get("is_equipment") or 0):
                    result["errors"].append(
                        f"Line {line_no}: '{item_sku}' is not an equipment model"
                    )
                    continue
                try:
                    inv.create_equipment_asset(
                        item_id=int(iid),
                        serial_number=serial,
                        make=(data.get("make") or "").strip() or None,
                        model=(data.get("model") or "").strip() or None,
                        purchase_date=normalize_date(data.get("purchase_date")),
                        warranty_expiry=normalize_date(data.get("warranty_expiry")),
                        warranty_start_date=normalize_date(data.get("warranty_start_date")),
                        warranty_months=parse_int_cell(data.get("warranty_months")),
                        service_interval_days=parse_int_cell(data.get("service_interval_days")),
                        next_service_due_date=normalize_date(data.get("next_service_due_date")),
                        condition=(data.get("condition") or "").strip() or None,
                        public_asset_code=(data.get("public_asset_code") or "").strip() or None,
                    )
                    result["created"] += 1
                except Exception as ex:
                    result["errors"].append(f"Line {line_no}: {ex}")

            elif entity == "locations":
                code = (data.get("code") or "").strip()
                name = (data.get("name") or "").strip()
                if not code or not name:
                    result["skipped"] += 1
                    continue
                conn = inv._connection()
                cur = conn.cursor()
                try:
                    cur.execute(
                        "SELECT id FROM inventory_locations WHERE code = %s LIMIT 1",
                        (code,),
                    )
                    ex = cur.fetchone()
                    loc_type = (data.get("type") or "warehouse").strip().lower() or "warehouse"
                    if loc_type not in (
                        "warehouse",
                        "store",
                        "bin",
                        "holding",
                        "virtual",
                    ):
                        loc_type = "warehouse"
                    addr = (data.get("address") or "").strip() or None
                    if ex:
                        cur.execute(
                            """
                            UPDATE inventory_locations
                            SET name = %s, type = %s, address = %s
                            WHERE id = %s
                            """,
                            (name, loc_type, addr, int(ex[0])),
                        )
                        conn.commit()
                        result["updated"] += 1
                    else:
                        inv.create_location(
                            {
                                "name": name,
                                "code": code,
                                "type": loc_type,
                                "address": addr,
                                "metadata": None,
                            }
                        )
                        result["created"] += 1
                finally:
                    cur.close()
        except Exception as ex:
            result["errors"].append(f"Line {line_no}: {ex}")

    return result


def clear_session(session: Any) -> None:
    session.pop(SESSION_KEY, None)


def validate_mapping(entity: str, mapping: Dict[str, str]) -> List[str]:
    errs: List[str] = []
    for spec in IMPORT_FIELD_SPECS.get(entity, []):
        if not spec.get("required"):
            continue
        key = spec["key"]
        col = mapping.get(key) or ""
        if not col or col == "__skip__":
            errs.append(f"Map required field: {spec.get('label', key)}")
    return errs
