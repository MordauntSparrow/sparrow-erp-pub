"""
MPI + location linkage and operational risk flags for case-first / patient-linked architecture.

- Cases remain the encounter record; `cases.mpi_patient_id` / `cases.cura_location_id` index linkage.
- `cura_locations`: premises anchor for address-scoped flags (needles, scene hazards).
- `cura_mpi_risk_flags`: structured alerts on patient and/or location (source, severity, review).
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

logger = logging.getLogger("medical_records_module.cura_mpi")


def _norm_pc(val: Any) -> str | None:
    if val is None or str(val).strip() == "":
        return None
    s = re.sub(r"\s+", "", str(val).strip().upper())
    return s or None


def _addr_fingerprint(address: str, postcode: str | None) -> str:
    base = f"{(address or '').strip().lower()}|{(postcode or '').strip().upper()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:32]


def _extract_ptinfo(case_data: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(case_data, dict):
        return None
    secs = case_data.get("sections")
    if isinstance(secs, list):
        for sec in secs:
            if not isinstance(sec, dict):
                continue
            name = (sec.get("name") or "").strip()
            if name in ("PatientInfo", "Patient Information"):
                content = sec.get("content")
                if isinstance(content, dict):
                    pti = content.get("ptInfo")
                    return pti if isinstance(pti, dict) else content
    return None


def _ensure_patientinfo_section(case_data: dict[str, Any]) -> dict[str, Any]:
    """Return content dict for PatientInfo section (mutates case_data)."""
    if not isinstance(case_data, dict):
        case_data = {}
    secs = case_data.get("sections")
    if not isinstance(secs, list):
        secs = []
        case_data["sections"] = secs
    for sec in secs:
        if isinstance(sec, dict) and (sec.get("name") or "").strip() in (
            "PatientInfo",
            "Patient Information",
        ):
            if not isinstance(sec.get("content"), dict):
                sec["content"] = {}
            c = sec["content"]
            if "ptInfo" not in c or not isinstance(c.get("ptInfo"), dict):
                c["ptInfo"] = c.get("ptInfo") if isinstance(c.get("ptInfo"), dict) else {}
            sec["name"] = "PatientInfo"
            return c["ptInfo"]
    secs.append({"name": "PatientInfo", "content": {"ptInfo": {}}})
    return secs[-1]["content"]["ptInfo"]


def _column_exists(cursor, table: str, column: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s",
        (table, column),
    )
    return cursor.fetchone() is not None


def _table_exists(cursor, table: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        (table,),
    )
    return cursor.fetchone() is not None


def find_or_create_location(cursor, address: str, postcode: str | None) -> int | None:
    if not _table_exists(cursor, "cura_locations"):
        return None
    pc = _norm_pc(postcode)
    addr_s = (address or "").strip()
    if not pc and len(addr_s) < 8:
        return None
    fp = _addr_fingerprint(addr_s, pc or "")
    if not fp:
        return None
    cursor.execute(
        "SELECT id FROM cura_locations WHERE address_fingerprint = %s LIMIT 1",
        (fp,),
    )
    row = cursor.fetchone()
    if row:
        return int(row[0])
    line = addr_s[:2000] if addr_s else ""
    try:
        cursor.execute(
            """
            INSERT INTO cura_locations (postcode_norm, address_fingerprint, address_line, created_at, updated_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (pc, fp, line),
        )
        return int(cursor.lastrowid)
    except Exception as exc:
        # Concurrency-safe: if two writers race, the second sees duplicate key on uq_cura_loc_fp.
        # Re-select the row and continue rather than bubbling a 5xx / log spam.
        errno = getattr(exc, "errno", None)
        if errno in (1062, 1205, 1213):  # dup entry / lock wait timeout / deadlock
            cursor.execute(
                "SELECT id FROM cura_locations WHERE address_fingerprint = %s LIMIT 1",
                (fp,),
            )
            row2 = cursor.fetchone()
            if row2:
                return int(row2[0])
        raise


def resolve_mpi_patient_id(cursor, pt: dict[str, Any]) -> int | None:
    """
    Legacy resolution against a separate ``patients`` table has been removed (case-first EPCR only).
    ``enrich_case_payload_mpi`` still honours ``mpiPatientId`` / ``mpi_patient_id`` already on the payload.
    """
    return None


def enrich_case_payload_mpi(cursor, case_data: dict[str, Any]) -> tuple[int | None, int | None]:
    """
    Mutate case JSON PatientInfo.ptInfo with mpiPatientId / curaLocationId when resolvable.
    Returns (mpi_patient_id, cura_location_id) for cases table columns.
    """
    if not isinstance(case_data, dict):
        return None, None
    if not _column_exists(cursor, "cases", "mpi_patient_id"):
        return None, None

    pt = _extract_ptinfo(case_data)
    if not pt:
        return None, None

    pid = None
    if pt.get("mpiPatientId") is not None:
        try:
            pid = int(pt["mpiPatientId"])
        except (TypeError, ValueError):
            pid = None
    if pid is None:
        pid = resolve_mpi_patient_id(cursor, pt)
    if pid is not None:
        pt["mpiPatientId"] = pid

    ha = pt.get("homeAddress") if isinstance(pt.get("homeAddress"), dict) else {}
    addr = (ha.get("address") or "").strip()
    postcode = ha.get("postcode") if isinstance(ha, dict) else None
    lid = None
    if pt.get("curaLocationId") is not None:
        try:
            lid = int(pt["curaLocationId"])
        except (TypeError, ValueError):
            lid = None
    if lid is None and (addr or postcode):
        lid = find_or_create_location(cursor, addr, postcode)
    if lid is not None:
        pt["curaLocationId"] = lid

    return pid, lid


def update_cases_mpi_columns(cursor, case_id: int, mpi_patient_id: int | None, cura_location_id: int | None) -> None:
    if not _column_exists(cursor, "cases", "mpi_patient_id"):
        return
    cursor.execute(
        "UPDATE cases SET mpi_patient_id = %s, cura_location_id = %s WHERE id = %s",
        (mpi_patient_id, cura_location_id, case_id),
    )


def merge_mpi_into_case_json(case_data: dict[str, Any], mpi_patient_id, cura_location_id) -> None:
    if mpi_patient_id is None and cura_location_id is None:
        return
    pt = _ensure_patientinfo_section(case_data)
    if mpi_patient_id is not None:
        pt["mpiPatientId"] = mpi_patient_id
    if cura_location_id is not None:
        pt["curaLocationId"] = cura_location_id


def fetch_active_flags_for_bundle(cursor, mpi_patient_id: int | None, location_id: int | None) -> list[dict[str, Any]]:
    if not _table_exists(cursor, "cura_mpi_risk_flags"):
        return []
    clauses = []
    params: list[Any] = []
    if mpi_patient_id is not None:
        clauses.append("patient_row_id = %s")
        params.append(mpi_patient_id)
    if location_id is not None:
        clauses.append("location_id = %s")
        params.append(location_id)
    if not clauses:
        return []
    where = "(" + " OR ".join(clauses) + ") AND active = 1"
    cursor.execute(
        f"""
        SELECT id, scope, category, severity, summary, detail, source_type, source_reference, review_at, created_at
        FROM cura_mpi_risk_flags
        WHERE {where}
        ORDER BY
            CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
            id DESC
        LIMIT 50
        """,
        tuple(params),
    )
    rows = cursor.fetchall() or []
    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "scope": r[1],
                "category": r[2],
                "severity": r[3],
                "summary": r[4],
                "detail": r[5],
                "source_type": r[6],
                "source_reference": r[7],
                "review_at": r[8].isoformat() if hasattr(r[8], "isoformat") else r[8],
                "created_at": r[9].isoformat() if hasattr(r[9], "isoformat") else r[9],
            }
        )
    return out


def build_flags_bundle_from_triage_fields(cursor, fields: dict[str, Any]) -> dict[str, Any]:
    """
    fields: mpi_patient_id, nhs_number, patient_dob, postcode, address, phone, surname, forename
    """
    mpi_patient_id = fields.get("mpi_patient_id")
    if mpi_patient_id is not None:
        try:
            mpi_patient_id = int(mpi_patient_id)
        except (TypeError, ValueError):
            mpi_patient_id = None

    pt: dict[str, Any] = {
        "mpiPatientId": mpi_patient_id,
        "nhsNumber": fields.get("nhs_number") or fields.get("nhsNumber"),
        "dob": fields.get("patient_dob") or fields.get("dob"),
        "surname": fields.get("last_name") or fields.get("surname"),
        "forename": fields.get("first_name") or fields.get("forename"),
        "homeAddress": {
            "address": fields.get("address") or "",
            "postcode": fields.get("postcode") or "",
            "telephone": fields.get("phone_number") or fields.get("phone") or "",
        },
    }
    if mpi_patient_id is None:
        mpi_patient_id = resolve_mpi_patient_id(cursor, pt)

    addr = (fields.get("address") or "").strip()
    pc = fields.get("postcode")
    lid = find_or_create_location(cursor, addr, pc) if (addr or _norm_pc(pc)) else None

    flags = fetch_active_flags_for_bundle(cursor, mpi_patient_id, lid)
    display_lines = []
    for f in flags:
        sev = (f.get("severity") or "").upper()
        summ = f.get("summary") or ""
        if sev and summ:
            display_lines.append(f"[{sev}] {summ}")
        elif summ:
            display_lines.append(summ)

    return {
        "mpiPatientId": mpi_patient_id,
        "curaLocationId": lid,
        "flags": flags,
        "risk_flag_lines": display_lines,
    }


def audit_log_mpi_access(cursor, user: str, action: str, detail: dict[str, Any] | None) -> None:
    try:
        if not _table_exists(cursor, "audit_logs"):
            return
        cursor.execute(
            "INSERT INTO audit_logs (user, action, patient_id, timestamp) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)",
            (
                user or "unknown",
                action[:2000] if action else "",
                (detail or {}).get("mpi_patient_id"),
            ),
        )
    except Exception:
        logger.debug("audit_log_mpi_access failed", exc_info=True)
