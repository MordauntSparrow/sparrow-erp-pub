"""
Build tabular editor sections from Cura tenant dataset JSON and apply edits / CSV imports.

Shapes follow Cura client defaults (see Cura ecpr-fixit-buddy datasetService).
"""
from __future__ import annotations

import csv
import io
import json
import logging
import re
from typing import Any

from .cura_baseline_datasets import (
    get_cura_baseline_payload,
    is_cura_dataset_payload_unset,
)

logger = logging.getLogger("medical_records_module.cura_settings_dataset_editor")

_DRUG_VALUE_KEYS = ("presentation", "cost", "billingType", "presentationAmount", "unit")


def _num(v: Any) -> Any:
    if v is None or v == "":
        return v
    if isinstance(v, (int, float)):
        return v
    s = str(v).strip()
    if re.match(r"^-?\d+$", s):
        try:
            return int(s)
        except ValueError:
            return v
    try:
        return float(s)
    except ValueError:
        return v


def build_editor_sections(dataset_name: str, payload: Any) -> list[dict[str, Any]]:
    """
    Each section: section_id, title, columns[{key,label}], rows[list of flat dicts].
    """
    sections: list[dict[str, Any]] = []
    if payload is None:
        payload = _default_payload_for_editor(dataset_name)

    if dataset_name == "drugs" and isinstance(payload, dict):
        cols = [
            {"key": "name", "label": "Drug name"},
            {"key": "presentation", "label": "Presentation"},
            {"key": "cost", "label": "Cost"},
            {"key": "billingType", "label": "Billing type"},
            {"key": "presentationAmount", "label": "Amount"},
            {"key": "unit", "label": "Unit"},
        ]
        rows = []
        for drug_name in sorted(payload.keys(), key=lambda x: str(x).lower()):
            meta = payload.get(drug_name)
            if not isinstance(meta, dict):
                continue
            row = {"name": str(drug_name)}
            for k in _DRUG_VALUE_KEYS:
                row[k] = meta.get(k, "")
            rows.append(row)
        sections.append(
            {
                "section_id": "__drugs__",
                "title": "Drug list",
                "columns": cols,
                "rows": rows,
            }
        )
        return sections

    if dataset_name == "iv_fluids" and isinstance(payload, list):
        cols = [
            {"key": "name", "label": "Fluid name"},
            {"key": "cost", "label": "Cost"},
        ]
        rows = []
        for item in payload:
            if isinstance(item, dict):
                rows.append({"name": item.get("name", ""), "cost": item.get("cost", "")})
        sections.append(
            {"section_id": "__iv_fluids__", "title": "IV fluids", "columns": cols, "rows": rows}
        )
        return sections

    if dataset_name == "incident_response_options" and isinstance(payload, dict):
        other_v = payload.get("otherValue", "")
        opts = payload.get("responseOptions")
        cols = [
            {"key": "value", "label": "Value (stored)"},
            {"key": "label", "label": "Label (shown)"},
        ]
        rows = []
        if isinstance(opts, list):
            for item in opts:
                if isinstance(item, dict):
                    rows.append(
                        {"value": item.get("value", ""), "label": item.get("label", "")}
                    )
        sections.append(
            {
                "section_id": "__incident_meta__",
                "title": "Incident response — “Other” label",
                "columns": [{"key": "otherValue", "label": "Other option label"}],
                "rows": [{"otherValue": other_v}],
            }
        )
        sections.append(
            {
                "section_id": "__incident_options__",
                "title": "Incident response options",
                "columns": cols,
                "rows": rows,
            }
        )
        return sections

    if isinstance(payload, dict):
        for key in sorted(payload.keys(), key=lambda x: str(x)):
            v = payload[key]
            title = key.replace("_", " ")
            if isinstance(v, list) and v:
                if all(isinstance(x, str) for x in v):
                    sections.append(
                        {
                            "section_id": key,
                            "title": title,
                            "columns": [{"key": "value", "label": "Value"}],
                            "rows": [{"value": x} for x in v],
                        }
                    )
                elif all(isinstance(x, dict) for x in v):
                    keys_set: set[str] = set()
                    for x in v:
                        keys_set.update(x.keys())
                    col_keys = sorted(keys_set)
                    cols = [{"key": k, "label": k.replace("_", " ")} for k in col_keys]
                    rows = []
                    for x in v:
                        rows.append({k: x.get(k, "") for k in col_keys})
                    sections.append(
                        {"section_id": key, "title": title, "columns": cols, "rows": rows}
                    )
            elif isinstance(v, list) and not v:
                sections.append(
                    {
                        "section_id": key,
                        "title": title,
                        "columns": [{"key": "value", "label": "Value"}],
                        "rows": [],
                    }
                )
        if sections:
            return sections

    if isinstance(payload, list) and payload and all(isinstance(x, dict) for x in payload):
        keys_set = set()
        for x in payload:
            keys_set.update(x.keys())
        col_keys = sorted(keys_set)
        cols = [{"key": k, "label": k.replace("_", " ")} for k in col_keys]
        rows = [{k: x.get(k, "") for k in col_keys} for x in payload]
        sections.append(
            {"section_id": "__list__", "title": "Items", "columns": cols, "rows": rows}
        )
        return sections

    return []


def _default_payload_for_editor(dataset_name: str) -> Any:
    if dataset_name == "drugs":
        return {}
    if dataset_name == "iv_fluids":
        return []
    if dataset_name == "incident_response_options":
        return {"otherValue": "Other", "responseOptions": []}
    if dataset_name in ("clinical_options", "clinical_indicators"):
        return {}
    return {}


def apply_section_to_payload(
    dataset_name: str,
    payload: Any,
    section_id: str,
    rows: list[dict[str, Any]],
) -> Any:
    """Return updated full payload for this dataset after saving one section."""
    if dataset_name == "drugs" and section_id == "__drugs__":
        out: dict[str, Any] = {}
        for r in rows:
            name = (r.get("name") or "").strip()
            if not name:
                continue
            out[name] = {
                "presentation": r.get("presentation", "") or "",
                "cost": _num(r.get("cost", 0)),
                "billingType": (r.get("billingType", "") or "aggregate"),
                "presentationAmount": _num(r.get("presentationAmount", 0)),
                "unit": (r.get("unit", "") or ""),
            }
        return out

    if dataset_name == "iv_fluids" and section_id == "__iv_fluids__":
        out_list = []
        for r in rows:
            n = (r.get("name") or "").strip()
            if not n:
                continue
            out_list.append({"name": n, "cost": _num(r.get("cost", 0))})
        return out_list

    if dataset_name == "incident_response_options":
        base = dict(payload) if isinstance(payload, dict) else {}
        if section_id == "__incident_meta__":
            if rows:
                base["otherValue"] = (rows[0].get("otherValue") or "Other") or "Other"
            return base
        if section_id == "__incident_options__":
            opts = []
            for r in rows:
                v = (r.get("value") or "").strip()
                lab = (r.get("label") or "").strip()
                if not v and not lab:
                    continue
                opts.append({"value": v or lab, "label": lab or v})
            base["responseOptions"] = opts
            if "otherValue" not in base:
                base["otherValue"] = "Other"
            return base

    if isinstance(payload, dict):
        base = json.loads(json.dumps(payload))
        if section_id not in base:
            base[section_id] = []
        v = base.get(section_id)
        if isinstance(v, list):
            all_str = not v or all(isinstance(x, str) for x in v)
            all_dict = bool(v) and all(isinstance(x, dict) for x in v)
            if all_str:
                base[section_id] = [
                    (row.get("value") or "").strip()
                    for row in rows
                    if (row.get("value") or "").strip() != ""
                ]
            elif all_dict or not v:
                keys_set = set()
                for r in rows:
                    keys_set.update(r.keys())
                col_keys = sorted(k for k in keys_set if k)
                new_list = []
                for r in rows:
                    if not col_keys or not any(str(r.get(k) or "").strip() for k in col_keys):
                        continue
                    new_list.append({k: r.get(k, "") for k in col_keys})
                base[section_id] = new_list
            else:
                base[section_id] = []
        return base

    if isinstance(payload, list) and section_id == "__list__":
        keys_set = set()
        for r in rows:
            keys_set.update(r.keys())
        col_keys = sorted(k for k in keys_set if k)
        return [
            {k: r.get(k, "") for k in col_keys}
            for r in rows
            if any(str(r.get(k) or "").strip() for k in col_keys)
        ]

    return payload


def merge_csv_into_section(
    dataset_name: str,
    payload: Any,
    section_id: str,
    csv_text: str,
    column_map: dict[str, str],
    mode: str,
    *,
    skip_header_row: bool = False,
) -> tuple[Any, str | None]:
    """
    column_map: CSV column index (as str "0","1",...) -> field key or "_skip".
    mode: replace | append
    skip_header_row: when True, drop the first row (e.g. column titles) before mapping.
    Returns (new_payload, error_message).
    """
    mode = (mode or "replace").strip().lower()
    if mode not in ("replace", "append"):
        mode = "replace"

    try:
        reader = csv.reader(io.StringIO(csv_text.strip()))
        grid = list(reader)
    except Exception as e:
        return payload, f"Could not read CSV: {e}"
    if not grid:
        return payload, "CSV is empty."
    if skip_header_row:
        grid = grid[1:]
        if not grid:
            return payload, "No data rows after skipping the header row."

    width = max(len(r) for r in grid)
    parsed_rows: list[dict[str, Any]] = []
    for r in grid:
        padded = list(r) + [""] * (width - len(r))
        obj: dict[str, Any] = {}
        for i in range(width):
            field = column_map.get(str(i), "_skip")
            if not field or field == "_skip":
                continue
            obj[field] = padded[i].strip() if i < len(padded) else ""
        if any(str(v).strip() for v in obj.values()):
            parsed_rows.append(obj)

    if not parsed_rows:
        return payload, "No data rows after mapping — check column mapping."

    if mode == "replace":
        new_payload = apply_section_to_payload(dataset_name, payload, section_id, parsed_rows)
        return new_payload, None

    # append
    cur_sections = build_editor_sections(dataset_name, payload)
    cur_rows: list[dict[str, Any]] = []
    for s in cur_sections:
        if s["section_id"] == section_id:
            cur_rows = [dict(x) for x in s.get("rows") or []]
            break
    merged = cur_rows + parsed_rows
    new_payload = apply_section_to_payload(dataset_name, payload, section_id, merged)
    return new_payload, None


def payload_for_dataset_or_default(dataset_name: str, raw_json: str | None) -> Any:
    if not raw_json:
        return get_cura_baseline_payload(dataset_name)
    try:
        parsed = json.loads(raw_json)
    except Exception:
        return get_cura_baseline_payload(dataset_name)
    if is_cura_dataset_payload_unset(dataset_name, parsed):
        return get_cura_baseline_payload(dataset_name)
    return parsed
