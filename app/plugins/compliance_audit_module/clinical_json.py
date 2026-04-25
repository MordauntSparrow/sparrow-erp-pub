"""
Derive audit-line events from cases.data JSON (ePCR / Cura payload) without inventing clinical facts.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

_SIGNAL = re.compile(
    r"(amend|signature|reason|dnar|consent|medication|medicine|drug|version|signed|closure|override|refusal|allerg)",
    re.IGNORECASE,
)
_MAX_NODES = 400
_MAX_DEPTH = 6


def _event(
    occurred_at,
    domain: str,
    actor: str | None,
    action: str,
    entity_type: str,
    entity_id: str,
    summary: str,
    detail_ref: str | None = None,
    integrity_hint: str | None = None,
) -> dict[str, Any]:
    return {
        "occurred_at": occurred_at,
        "domain": domain,
        "actor": actor or "",
        "action": action,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "summary": summary[:4000],
        "detail_ref": (detail_ref or "")[:2000],
        "integrity_hint": (integrity_hint or "")[:500],
    }


def extract_case_json_events(
    case_id: int | str,
    data_raw: Any,
    *,
    record_version: int | None,
    updated_at: datetime | None,
    status: str | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    base_t = updated_at
    if data_raw is None:
        return out
    if isinstance(data_raw, (bytes, str)):
        try:
            data = json.loads(data_raw)
        except Exception:
            return out
    elif isinstance(data_raw, dict):
        data = data_raw
    else:
        return out
    if not isinstance(data, dict):
        return out

    rv = record_version if record_version is not None else data.get("recordVersion") or data.get("record_version")
    if rv is not None:
        out.append(
            _event(
                base_t,
                "epcr_json",
                None,
                "record_version",
                "cases",
                str(case_id),
                f"Case {case_id} record_version={rv} status={status or ''}",
                integrity_hint=f"case:{case_id}:version",
            )
        )

    nodes = [0]

    def walk(obj: Any, path: str, depth: int) -> None:
        if nodes[0] >= _MAX_NODES or depth > _MAX_DEPTH:
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                if nodes[0] >= _MAX_NODES:
                    break
                p = f"{path}.{k}" if path else str(k)
                if _SIGNAL.search(str(k)):
                    nodes[0] += 1
                    preview = ""
                    if isinstance(v, (str, int, float, bool)):
                        preview = str(v)[:220]
                    elif isinstance(v, dict) and v:
                        preview = f"keys={list(v.keys())[:8]}"
                    elif isinstance(v, list) and v:
                        preview = f"len={len(v)}"
                    out.append(
                        _event(
                            base_t,
                            "epcr_json",
                            None,
                            "json_signal",
                            "cases",
                            str(case_id),
                            f"Case {case_id} path {p} → {preview}",
                            detail_ref=p[:500],
                            integrity_hint=f"case:{case_id}:json",
                        )
                    )
                walk(v, p, depth + 1)
        elif isinstance(obj, list):
            for i, v in enumerate(obj[:30]):
                walk(v, f"{path}[{i}]", depth + 1)

    walk(data, "$", 0)
    return out
