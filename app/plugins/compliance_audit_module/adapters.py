"""
Read-only adapters: normalise rows into audit event dicts for the assurance console.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

_NHS_RE = re.compile(r"\b\d{3}\s?\d{3}\s?\d{4}\b")


def redact_text(s: str | None) -> str:
    if not s:
        return ""
    return _NHS_RE.sub("[REDACTED]", str(s))


def _table_exists(cur, name: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        """,
        (name,),
    )
    return cur.fetchone() is not None


def _column_names(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT COLUMN_NAME AS column_name FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        """,
        (table,),
    )
    rows = cur.fetchall() or []
    out: set[str] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        v = r.get("column_name") or r.get("COLUMN_NAME")
        if v:
            out.add(str(v))
    return out


def _sql_ident(col: str) -> str:
    if col == "user" or col == "timestamp":
        return f"`{col}`"
    return col


def _event(
    *,
    occurred_at: datetime | None,
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
        "summary": summary,
        "detail_ref": detail_ref or "",
        "integrity_hint": integrity_hint or "",
    }


def fetch_admin_staff_audit(
    cur,
    *,
    date_from=None,
    date_to=None,
    path_like: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "admin_staff_action_logs"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("created_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("created_at <= %s")
        params.append(date_to)
    if path_like:
        where.append("path LIKE %s")
        params.append(f"%{path_like}%")
    sql = f"""
        SELECT id, created_at, actor_user_id, actor_username, actor_contractor_id, actor_channel,
               inferred_permission, http_method, path, http_status, endpoint, blueprint
        FROM admin_staff_action_logs
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        actor_bits = [
            x
            for x in (
                r.get("actor_username"),
                r.get("actor_user_id"),
                f"contractor:{r.get('actor_contractor_id')}" if r.get("actor_contractor_id") else None,
            )
            if x
        ]
        actor = " / ".join(str(x) for x in actor_bits) if actor_bits else (r.get("actor_channel") or "")
        summ = f"{r.get('http_method') or ''} {r.get('path') or ''} → {r.get('http_status')}"
        out.append(
            _event(
                occurred_at=r.get("created_at"),
                domain="admin_audit",
                actor=actor,
                action="http_request",
                entity_type="admin_staff_action_logs",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                detail_ref=r.get("endpoint"),
                integrity_hint=f"row:{r.get('id')}",
            )
        )
    return out


def fetch_mdt_jobs(
    cur,
    *,
    date_from=None,
    date_to=None,
    cad: int | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "mdt_jobs"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if cad is not None:
        where.append("cad = %s")
        params.append(int(cad))
    if date_from:
        where.append("COALESCE(updated_at, created_at) >= %s")
        params.append(date_from)
    if date_to:
        where.append("COALESCE(updated_at, created_at) <= %s")
        params.append(date_to)
    sql = f"""
        SELECT cad, status, division, created_at, updated_at, claimedBy, chief_complaint, outcome
        FROM mdt_jobs
        WHERE {' AND '.join(where)}
        ORDER BY COALESCE(updated_at, created_at) DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        cc = r.get("chief_complaint") or ""
        summ = f"CAD {r.get('cad')} status={r.get('status')} div={r.get('division') or ''}"
        if cc:
            summ += f" — {cc}"[:500]
        out.append(
            _event(
                occurred_at=r.get("updated_at") or r.get("created_at"),
                domain="cad",
                actor=r.get("claimedBy"),
                action="job_snapshot",
                entity_type="mdt_jobs",
                entity_id=str(r.get("cad")),
                summary=summ[:2000],
                integrity_hint=f"cad:{r.get('cad')}",
            )
        )
    return out


def fetch_mdt_job_comms(
    cur,
    *,
    date_from=None,
    date_to=None,
    cad: int | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "mdt_job_comms"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if cad is not None:
        where.append("cad = %s")
        params.append(int(cad))
    if date_from:
        where.append("created_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("created_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT id, cad, message_type, sender_role, sender_user, LEFT(message_text, 400) AS excerpt,
               created_at
        FROM mdt_job_comms
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        excerpt = (r.get("excerpt") or "").replace("\n", " ")
        summ = f"CAD {r.get('cad')} [{r.get('message_type')}] {excerpt}"
        out.append(
            _event(
                occurred_at=r.get("created_at"),
                domain="cad_comms",
                actor=r.get("sender_user") or r.get("sender_role"),
                action="message",
                entity_type="mdt_job_comms",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                integrity_hint=f"comm:{r.get('id')}",
            )
        )
    return out


def fetch_response_triage(
    cur,
    *,
    date_from=None,
    date_to=None,
    limit: int = 3000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "response_triage"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("created_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("created_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT id, reason_for_call, decision, created_by, created_at
        FROM response_triage
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        summ = f"Triage #{r.get('id')} decision={r.get('decision') or ''} reason={r.get('reason_for_call') or ''}"
        out.append(
            _event(
                occurred_at=r.get("created_at"),
                domain="intake",
                actor=r.get("created_by"),
                action="triage_record",
                entity_type="response_triage",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                integrity_hint=f"triage:{r.get('id')}",
            )
        )
    return out


def fetch_cases(
    cur,
    *,
    date_from=None,
    date_to=None,
    case_id: int | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "cases"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if case_id is not None:
        where.append("id = %s")
        params.append(int(case_id))
    if date_from:
        where.append("updated_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("updated_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT id, status, dispatch_reference, primary_callsign, created_at, updated_at, closed_at
        FROM cases
        WHERE {' AND '.join(where)}
        ORDER BY updated_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        summ = (
            f"Case {r.get('id')} status={r.get('status')} "
            f"dispatch={r.get('dispatch_reference') or ''} callsign={r.get('primary_callsign') or ''}"
        )
        out.append(
            _event(
                occurred_at=r.get("updated_at") or r.get("created_at"),
                domain="epcr",
                actor=None,
                action="case_snapshot",
                entity_type="cases",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                integrity_hint=f"case:{r.get('id')}",
            )
        )
    return out


def fetch_epcr_clinical_audit_logs(
    cur,
    *,
    date_from=None,
    date_to=None,
    case_id: int | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """
    Medical Records `audit_logs`: EPCR / Cura access and API actions (same source as the clinical audit log UI).
    Column set varies by deployment (legacy vs extended schema).
    """
    if not _table_exists(cur, "audit_logs"):
        return []
    cols = _column_names(cur, "audit_logs")
    if "id" not in cols or "action" not in cols or "timestamp" not in cols:
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("`timestamp` >= %s")
        params.append(date_from)
    if date_to:
        where.append("`timestamp` <= %s")
        params.append(date_to)
    if case_id is not None and "case_id" in cols:
        where.append("case_id = %s")
        params.append(int(case_id))
    want = [
        "id",
        "user",
        "principal_role",
        "action",
        "case_id",
        "patient_id",
        "route",
        "ip",
        "reason",
        "user_agent",
        "timestamp",
    ]
    select_parts = [_sql_ident(c) for c in want if c in cols]
    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM audit_logs
        WHERE {' AND '.join(where)}
        ORDER BY `timestamp` DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        action_txt = redact_text(str(r.get("action") or "").replace("\n", " "))[:1800]
        actor_bits = [x for x in (r.get("user"), r.get("principal_role")) if x]
        actor = " / ".join(str(x) for x in actor_bits) if actor_bits else ""
        summ_parts = [action_txt]
        if r.get("case_id") is not None:
            summ_parts.append(f"case_id={r.get('case_id')}")
        if r.get("patient_id") is not None and "patient_id" in cols:
            summ_parts.append(f"patient_id={r.get('patient_id')}")
        if r.get("route"):
            summ_parts.append(f"route={redact_text(str(r.get('route')))[:200]}")
        summ = " · ".join(x for x in summ_parts if x)[:2000]
        detail = None
        if r.get("reason"):
            detail = redact_text(str(r.get("reason")).replace("\n", " "))[:1500]
        out.append(
            _event(
                occurred_at=r.get("timestamp"),
                domain="epcr_clinical_audit",
                actor=actor,
                action="clinical_audit_log",
                entity_type="audit_logs",
                entity_id=str(r.get("id")),
                summary=summ,
                detail_ref=detail,
                integrity_hint=f"audit_log:{r.get('id')}",
            )
        )
    return out


def fetch_epcr_access_request_events(
    cur,
    *,
    date_from=None,
    date_to=None,
    case_id: int | None = None,
    limit: int = 3000,
) -> list[dict[str, Any]]:
    """EPCR break-glass / access request workflow (request → review → code)."""
    if not _table_exists(cur, "epcr_access_requests"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if case_id is not None:
        where.append("case_id = %s")
        params.append(int(case_id))
    if date_from:
        where.append("created_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("created_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT request_id, case_id, requested_by, requester_role, justification, status,
               reviewed_by, review_note, created_at, reviewed_at
        FROM epcr_access_requests
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        j = redact_text((r.get("justification") or "").replace("\n", " "))[:280]
        summ = (
            f"EPCR access request status={r.get('status')} case={r.get('case_id')} "
            f"by {r.get('requested_by') or ''}"
        )
        if r.get("requester_role"):
            summ += f" role={r.get('requester_role')}"
        if r.get("reviewed_by"):
            summ += f" reviewed_by={r.get('reviewed_by')}"
        if j:
            summ += f" — {j}"
        summ = summ[:2000]
        rn = redact_text((r.get("review_note") or "").replace("\n", " "))[:800] or None
        out.append(
            _event(
                occurred_at=r.get("created_at"),
                domain="epcr_access",
                actor=r.get("requested_by"),
                action="epcr_access_request",
                entity_type="epcr_access_requests",
                entity_id=str(r.get("request_id") or ""),
                summary=summ,
                detail_ref=rn,
                integrity_hint=str(r.get("request_id") or ""),
            )
        )
    return out


def fetch_inventory_audit(
    cur,
    *,
    date_from=None,
    date_to=None,
    limit: int = 3000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "inventory_audit"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("created_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("created_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT id, user_id, action, item_id, location_id, transaction_id, created_at
        FROM inventory_audit
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        summ = f"{r.get('action')} item={r.get('item_id')} loc={r.get('location_id')} tx={r.get('transaction_id')}"
        out.append(
            _event(
                occurred_at=r.get("created_at"),
                domain="inventory",
                actor=str(r.get("user_id")) if r.get("user_id") is not None else None,
                action="inventory_audit",
                entity_type="inventory_audit",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                integrity_hint=f"inv_audit:{r.get('id')}",
            )
        )
    return out


def fetch_cura_operational_events(
    cur,
    *,
    date_from=None,
    date_to=None,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "cura_operational_events"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("updated_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("updated_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT id, name, status, starts_at, ends_at, created_at, updated_at
        FROM cura_operational_events
        WHERE {' AND '.join(where)}
        ORDER BY updated_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        summ = f"Op event #{r.get('id')} {r.get('name')} status={r.get('status')}"
        out.append(
            _event(
                occurred_at=r.get("updated_at") or r.get("created_at"),
                domain="cura_event",
                actor=None,
                action="operational_event",
                entity_type="cura_operational_events",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                integrity_hint=f"oe:{r.get('id')}",
            )
        )
    return out


def fetch_cura_safeguarding(
    cur,
    *,
    date_from=None,
    date_to=None,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "cura_safeguarding_referrals"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("updated_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("updated_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT id, public_id, status, created_at, updated_at
        FROM cura_safeguarding_referrals
        WHERE {' AND '.join(where)}
        ORDER BY updated_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        summ = f"Safeguarding {r.get('public_id')} status={r.get('status')}"
        out.append(
            _event(
                occurred_at=r.get("updated_at") or r.get("created_at"),
                domain="safeguarding",
                actor=None,
                action="safeguarding_referral",
                entity_type="cura_safeguarding_referrals",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                integrity_hint=str(r.get("public_id")),
            )
        )
    return out


def fetch_case_json_audit_events(
    cur,
    *,
    date_from=None,
    date_to=None,
    case_id: int | None = None,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    from .clinical_json import extract_case_json_events

    if not _table_exists(cur, "cases"):
        return []
    # Sorting rows that include large JSON `data` exhausts MySQL sort_buffer (HY001). Order/limit on id only, then fetch by PK.
    lim = max(1, min(int(limit), 5000))
    where = ["1=1"]
    params: list[Any] = []
    if case_id is not None:
        where.append("id = %s")
        params.append(int(case_id))
    if date_from:
        where.append("updated_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("updated_at <= %s")
        params.append(date_to)
    wclause = " AND ".join(where)
    sql_ids = f"""
        SELECT id FROM cases
        WHERE {wclause}
        ORDER BY updated_at DESC
        LIMIT {lim}
    """
    cur.execute(sql_ids, params)
    id_rows = cur.fetchall() or []
    ids = [r["id"] for r in id_rows if r.get("id") is not None]
    if not ids:
        return []
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
    out: list[dict[str, Any]] = []
    for r in rows:
        out.extend(
            extract_case_json_events(
                r.get("id"),
                r.get("data"),
                record_version=r.get("record_version"),
                updated_at=r.get("updated_at"),
                status=r.get("status"),
            )
        )
    return out


def fetch_mdt_json_signals(
    cur,
    *,
    date_from=None,
    date_to=None,
    cad: int | None = None,
    limit: int = 3000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "mdt_jobs"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if cad is not None:
        where.append("cad = %s")
        params.append(int(cad))
    if date_from:
        where.append("COALESCE(updated_at, created_at) >= %s")
        params.append(date_from)
    if date_to:
        where.append("COALESCE(updated_at, created_at) <= %s")
        params.append(date_to)
    wclause = " AND ".join(where)
    # Same as cases: ORDER BY with wide `data` JSON can blow sort_buffer; rank by cad only then load rows.
    sql_cads = f"""
        SELECT cad FROM mdt_jobs
        WHERE {wclause}
        ORDER BY COALESCE(updated_at, created_at) DESC
        LIMIT {lim}
    """
    cur.execute(sql_cads, params)
    cad_rows = cur.fetchall() or []
    cads = [r["cad"] for r in cad_rows if r.get("cad") is not None]
    cads = list(dict.fromkeys(cads))
    if not cads:
        return []
    ph = ",".join(["%s"] * len(cads))
    cur.execute(
        f"""
        SELECT cad, status, data, division, created_at, updated_at
        FROM mdt_jobs
        WHERE cad IN ({ph})
        """,
        tuple(cads),
    )
    by_cad = {r["cad"]: r for r in (cur.fetchall() or [])}
    rows = [by_cad[c] for c in cads if c in by_cad]
    out: list[dict[str, Any]] = []
    interest = (
        "mdt_panic_callsign",
        "mdt_grade1_panic_active",
        "priority_source",
        "mdt_panic_on_existing_job",
        "mdt_panic_on_patient_cad",
    )
    for r in rows:
        raw = r.get("data")
        d: dict[str, Any] | None = None
        if raw is None:
            continue
        if isinstance(raw, dict):
            d = raw
        elif isinstance(raw, str):
            try:
                d = json.loads(raw)
            except Exception:
                d = None
        if not isinstance(d, dict):
            continue
        hits = [k for k in interest if d.get(k) not in (None, "", False, [], {})]
        if not hits:
            continue
        summ = f"CAD {r.get('cad')} job.data keys: {', '.join(hits)} status={r.get('status')} div={r.get('division')}"
        out.append(
            _event(
                occurred_at=r.get("updated_at") or r.get("created_at"),
                domain="cad_json",
                actor=None,
                action="json_signal",
                entity_type="mdt_jobs",
                entity_id=str(r.get("cad")),
                summary=summ[:2000],
                integrity_hint=f"cad:{r.get('cad')}:data",
            )
        )
    return out


def fetch_cura_mi_events(
    cur,
    *,
    date_from=None,
    date_to=None,
    limit: int = 1500,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "cura_mi_events"):
        return []
    lim = max(1, min(int(limit), 10000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("updated_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("updated_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT id, name, status, starts_at, ends_at, created_at, updated_at
        FROM cura_mi_events
        WHERE {' AND '.join(where)}
        ORDER BY updated_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        summ = f"MI event #{r.get('id')} {r.get('name')} status={r.get('status')}"
        out.append(
            _event(
                occurred_at=r.get("updated_at") or r.get("created_at"),
                domain="mi",
                actor=None,
                action="mi_event",
                entity_type="cura_mi_events",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                integrity_hint=f"mi_ev:{r.get('id')}",
            )
        )
    return out


def fetch_cura_mi_reports(
    cur,
    *,
    date_from=None,
    date_to=None,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "cura_mi_reports"):
        return []
    lim = max(1, min(int(limit), 10000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("updated_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("updated_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT id, event_id, public_id, status, record_version, created_at, updated_at, submitted_by
        FROM cura_mi_reports
        WHERE {' AND '.join(where)}
        ORDER BY updated_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        summ = (
            f"MI report {r.get('public_id')} event={r.get('event_id')} "
            f"status={r.get('status')} v={r.get('record_version')}"
        )
        out.append(
            _event(
                occurred_at=r.get("updated_at") or r.get("created_at"),
                domain="mi",
                actor=r.get("submitted_by"),
                action="mi_report",
                entity_type="cura_mi_reports",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                integrity_hint=str(r.get("public_id")),
            )
        )
    return out


def fetch_compliance_login_audit(
    cur,
    *,
    date_from=None,
    date_to=None,
    limit: int = 3000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "compliance_login_audit"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("occurred_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("occurred_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT id, occurred_at, user_id, contractor_id, username_hash, success, channel, ip_address
        FROM compliance_login_audit
        WHERE {' AND '.join(where)}
        ORDER BY occurred_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        who = r.get("user_id") or (
            f"contractor:{r.get('contractor_id')}" if r.get("contractor_id") else (r.get("username_hash") or "")
        )
        summ = f"Login {'OK' if r.get('success') else 'fail'} channel={r.get('channel')} ip={r.get('ip_address')}"
        out.append(
            _event(
                occurred_at=r.get("occurred_at"),
                domain="identity",
                actor=str(who),
                action="login_audit",
                entity_type="compliance_login_audit",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                integrity_hint=f"login:{r.get('id')}",
            )
        )
    return out


def fetch_training_audit(
    cur,
    *,
    date_from=None,
    date_to=None,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "trn_audit_log"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("created_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("created_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT id, event_type, entity_table, entity_id, contractor_id, actor_user_id, created_at
        FROM trn_audit_log
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        summ = f"{r.get('event_type')} {r.get('entity_table')}:{r.get('entity_id')} contractor={r.get('contractor_id')}"
        out.append(
            _event(
                occurred_at=r.get("created_at"),
                domain="training",
                actor=str(r.get("actor_user_id")) if r.get("actor_user_id") is not None else None,
                action="trn_audit",
                entity_type="trn_audit_log",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                integrity_hint=f"trn:{r.get('id')}",
            )
        )
    return out


def fetch_compliance_export_log(
    cur,
    *,
    date_from=None,
    date_to=None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    if not _table_exists(cur, "compliance_export_log"):
        return []
    lim = max(1, min(int(limit), 2000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("created_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("created_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT id, user_id, created_at, export_format, scope_json, row_count, pin_step_up_ok, file_hash
        FROM compliance_export_log
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        uid = r.get("user_id")
        actor = str(uid) if uid else "system"
        summ = (
            f"Governance export #{r.get('id')}: {r.get('export_format')} "
            f"rows={r.get('row_count')} pin_ok={r.get('pin_step_up_ok')}"
        )
        out.append(
            _event(
                occurred_at=r.get("created_at"),
                domain="governance_exports",
                actor=actor,
                action="export",
                entity_type="compliance_export_log",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                integrity_hint=r.get("file_hash"),
            )
        )
    return out


def fetch_compliance_policy_lifecycle(
    cur,
    *,
    date_from=None,
    date_to=None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Published-document lifecycle & publication trail (CQC / governance)."""
    if not _table_exists(cur, "comp_policy_lifecycle_audit"):
        return []
    lim = max(1, min(int(limit), 20000))
    where = ["1=1"]
    params: list[Any] = []
    if date_from:
        where.append("a.created_at >= %s")
        params.append(date_from)
    if date_to:
        where.append("a.created_at <= %s")
        params.append(date_to)
    sql = f"""
        SELECT a.id, a.policy_id, a.from_status, a.to_status, a.reason, a.actor_label,
               a.created_at, p.title AS policy_title
        FROM comp_policy_lifecycle_audit a
        LEFT JOIN comp_policies p ON p.id = a.policy_id
        WHERE {' AND '.join(where)}
        ORDER BY a.created_at DESC
        LIMIT {lim}
    """
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        pid = r.get("policy_id")
        title = redact_text(str(r.get("policy_title") or "")).strip()
        actor = redact_text(str(r.get("actor_label") or "")).strip() or "—"
        fs = r.get("from_status") or ""
        ts = r.get("to_status") or ""
        summ = (
            f"Policy doc #{pid}"
            + (f" «{title[:120]}»" if title else "")
            + f": {fs} → {ts}"
        )
        reason_snip = redact_text(str(r.get("reason") or ""))[:400]
        if reason_snip:
            summ = f"{summ}. {reason_snip}"
        out.append(
            _event(
                occurred_at=r.get("created_at"),
                domain="compliance_policies",
                actor=actor,
                action="lifecycle_change",
                entity_type="comp_policy_lifecycle_audit",
                entity_id=str(r.get("id")),
                summary=summ[:2000],
                detail_ref=f"policy_id:{pid}",
                integrity_hint=f"pla:{r.get('id')}",
            )
        )
    return out


DOMAIN_FETCHERS = {
    "admin_audit": fetch_admin_staff_audit,
    "cad": fetch_mdt_jobs,
    "cad_json": fetch_mdt_json_signals,
    "cad_comms": fetch_mdt_job_comms,
    "intake": fetch_response_triage,
    "epcr": fetch_cases,
    "epcr_json": fetch_case_json_audit_events,
    "epcr_clinical_audit": fetch_epcr_clinical_audit_logs,
    "epcr_access": fetch_epcr_access_request_events,
    "mi": fetch_cura_mi_events,
    "mi_reports": fetch_cura_mi_reports,
    "inventory": fetch_inventory_audit,
    "cura_event": fetch_cura_operational_events,
    "safeguarding": fetch_cura_safeguarding,
    "identity": fetch_compliance_login_audit,
    "training": fetch_training_audit,
    "governance_exports": fetch_compliance_export_log,
    "compliance_policies": fetch_compliance_policy_lifecycle,
}

ALL_DOMAIN_KEYS = list(DOMAIN_FETCHERS.keys())


def merge_timeline(
    cur,
    *,
    domains: set[str] | None,
    date_from=None,
    date_to=None,
    cad: int | None = None,
    case_id: int | None = None,
    path_like: str | None = None,
    limit: int = 2500,
    per_domain_limit: int = 5000,
) -> list[dict[str, Any]]:
    all_keys = set(DOMAIN_FETCHERS.keys())
    if domains is None:
        active = all_keys
    else:
        active = set(domains) & all_keys
    merged: list[dict[str, Any]] = []
    kw_common = {"date_from": date_from, "date_to": date_to, "limit": per_domain_limit}
    if "admin_audit" in active:
        merged.extend(
            fetch_admin_staff_audit(cur, path_like=path_like, **kw_common)
        )
    if "cad" in active:
        merged.extend(fetch_mdt_jobs(cur, cad=cad, **kw_common))
    if "cad_comms" in active:
        merged.extend(fetch_mdt_job_comms(cur, cad=cad, **kw_common))
    if "intake" in active:
        merged.extend(fetch_response_triage(cur, **kw_common))
    if "epcr" in active:
        merged.extend(fetch_cases(cur, case_id=case_id, **kw_common))
    if "epcr_json" in active:
        merged.extend(
            fetch_case_json_audit_events(cur, case_id=case_id, **kw_common)
        )
    if "epcr_clinical_audit" in active:
        merged.extend(
            fetch_epcr_clinical_audit_logs(cur, case_id=case_id, **kw_common)
        )
    if "epcr_access" in active:
        merged.extend(
            fetch_epcr_access_request_events(cur, case_id=case_id, **kw_common)
        )
    if "cad_json" in active:
        merged.extend(fetch_mdt_json_signals(cur, cad=cad, **kw_common))
    if "mi" in active:
        merged.extend(fetch_cura_mi_events(cur, **kw_common))
    if "mi_reports" in active:
        merged.extend(fetch_cura_mi_reports(cur, **kw_common))
    if "identity" in active:
        merged.extend(fetch_compliance_login_audit(cur, **kw_common))
    if "training" in active:
        merged.extend(fetch_training_audit(cur, **kw_common))
    if "inventory" in active:
        merged.extend(fetch_inventory_audit(cur, **kw_common))
    if "cura_event" in active:
        merged.extend(fetch_cura_operational_events(cur, **kw_common))
    if "safeguarding" in active:
        merged.extend(fetch_cura_safeguarding(cur, **kw_common))
    if "governance_exports" in active:
        merged.extend(
            fetch_compliance_export_log(
                cur,
                date_from=date_from,
                date_to=date_to,
                limit=min(per_domain_limit, 800),
            )
        )
    if "compliance_policies" in active:
        merged.extend(
            fetch_compliance_policy_lifecycle(
                cur,
                date_from=date_from,
                date_to=date_to,
                limit=per_domain_limit,
            )
        )

    def _sort_key(ev: dict[str, Any]):
        t = ev.get("occurred_at")
        if isinstance(t, datetime):
            return t
        try:
            return datetime.min
        except Exception:
            return datetime.min

    merged.sort(key=_sort_key, reverse=True)
    cap = max(1, min(int(limit), 20000))
    return merged[:cap]


def events_for_export(
    events: list[dict[str, Any]],
    *,
    redaction_profile: str = "minimal",
    legacy_redact_checkbox: bool = False,
) -> list[dict[str, Any]]:
    from .redaction import apply_redaction_profile

    prof = (redaction_profile or "minimal").strip().lower()
    if prof not in ("minimal", "standard", "strict"):
        prof = "standard"
    if legacy_redact_checkbox and prof == "minimal":
        prof = "standard"
    return apply_redaction_profile(events, prof)


def manifest_dict(
    *,
    filters: dict[str, Any],
    row_count: int,
    generator_version: str,
) -> dict[str, Any]:
    return {
        "generator_version": generator_version,
        "row_count": row_count,
        "filters": filters,
    }
