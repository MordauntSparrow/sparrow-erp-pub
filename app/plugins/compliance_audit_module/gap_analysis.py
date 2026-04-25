"""
Gap analysis: cases missing expected structural fields (PRD §4.1 — no invented text).
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any


def _parse_data(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, str)):
        try:
            v = json.loads(raw)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def _has_patient_block(d: dict[str, Any]) -> bool:
    return bool(d.get("PatientInfo") or d.get("patientInfo"))


def _has_assigned(d: dict[str, Any]) -> bool:
    au = d.get("assignedUsers") or d.get("assigned_users")
    return isinstance(au, list) and len(au) > 0


def _closure_context(d: dict[str, Any], status: str | None) -> bool:
    st = (status or "").lower()
    if "clos" not in st:
        return True
    return bool(
        d.get("closureReason")
        or d.get("closure_reason")
        or d.get("CloseReason")
        or d.get("clinicalOutcome")
        or d.get("clinical_outcome")
    )


def analyze_case_row(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Return gap records for one case (0..n)."""
    cid = row.get("id")
    status = row.get("status")
    data = _parse_data(row.get("data"))
    gaps: list[dict[str, Any]] = []
    if not _has_patient_block(data):
        gaps.append(
            {
                "case_id": cid,
                "gap_code": "missing_patient_block",
                "detail": "No PatientInfo / patientInfo in cases.data",
                "severity": "high",
            }
        )
    if not _has_assigned(data):
        gaps.append(
            {
                "case_id": cid,
                "gap_code": "missing_assigned_users",
                "detail": "No assignedUsers list in cases.data",
                "severity": "medium",
            }
        )
    if not _closure_context(data, status):
        gaps.append(
            {
                "case_id": cid,
                "gap_code": "closed_without_outcome_fields",
                "detail": "Status suggests closure but no closure/outcome keys in JSON",
                "severity": "medium",
            }
        )
    return gaps


def analyze_cases(
    cur,
    *,
    date_from=None,
    date_to=None,
    limit: int = 500,
) -> tuple[list[dict[str, Any]], int]:
    cur.execute(
        """
        SELECT 1 FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cases'
        """
    )
    if not cur.fetchone():
        return [], 0
    # Sorting full rows that include large JSON `data` exhausts MySQL sort_buffer (HY001).
    lim = max(1, min(int(limit), 5000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("updated_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("updated_at <= %s")
        params.append(date_to)
    wclause = " AND ".join(where)
    cur.execute(
        f"""
        SELECT id FROM cases
        WHERE {wclause}
        ORDER BY updated_at DESC
        LIMIT {lim}
        """,
        params,
    )
    id_rows = cur.fetchall() or []
    ids = [r["id"] for r in id_rows if r.get("id") is not None]
    if not ids:
        return [], 0
    ph = ",".join(["%s"] * len(ids))
    cur.execute(
        f"""
        SELECT id, status, data, record_version, updated_at
        FROM cases
        WHERE id IN ({ph})
        """,
        tuple(ids),
    )
    by_id = {r["id"]: r for r in (cur.fetchall() or [])}
    rows = [by_id[i] for i in ids if i in by_id]
    all_gaps: list[dict[str, Any]] = []
    for r in rows:
        for g in analyze_case_row(r):
            g["updated_at"] = r.get("updated_at")
            all_gaps.append(g)
    return all_gaps, len(rows)
