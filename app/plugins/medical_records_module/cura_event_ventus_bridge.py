"""Bridge Cura operational events ↔ Ventus MDT (`mdts_signed_on`, `mdt_dispatch_divisions`)."""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any

logger = logging.getLogger("medical_records_module.cura_event_ventus")

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$", re.I)


def table_exists(cursor, table: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        (table,),
    )
    return cursor.fetchone() is not None


def normalize_division_slug(value: str | None, fallback: str = "general") -> str:
    if not value or not str(value).strip():
        return fallback
    s = str(value).strip().lower().replace(" ", "_")
    return s[:64] if s else fallback


def parse_mdts_crew_usernames(crew_raw: Any) -> list[str]:
    """Extract usernames from mdts_signed_on.crew JSON (list of objects or strings)."""
    if crew_raw is None:
        return []
    if isinstance(crew_raw, (bytes, bytearray)):
        crew_raw = crew_raw.decode("utf-8", errors="ignore")
    if isinstance(crew_raw, str):
        try:
            crew_raw = json.loads(crew_raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(crew_raw, list):
        return []
    out: list[str] = []
    for member in crew_raw:
        if isinstance(member, dict):
            u = str(member.get("username") or member.get("user") or "").strip()
        else:
            u = str(member).strip()
        if u:
            out.append(u.lower())
    return out


def fetch_mdts_signed_on_row(cursor, callsign: str) -> dict[str, Any] | None:
    if not table_exists(cursor, "mdts_signed_on"):
        return None
    cs = (callsign or "").strip().upper()
    if not cs:
        return None
    cursor.execute(
        "SELECT callSign, crew, division, status, signOnTime, lastSeenAt FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
        (cs,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return {
        "callSign": row[0],
        "crew": row[1],
        "division": row[2] if len(row) > 2 else None,
        "status": row[3] if len(row) > 3 else None,
        "signOnTime": row[4] if len(row) > 4 else None,
        "lastSeenAt": row[5] if len(row) > 5 else None,
    }


def _mdts_has_assigned_incident_column(cursor) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mdts_signed_on' "
        "AND COLUMN_NAME = 'assignedIncident' LIMIT 1"
    )
    return cursor.fetchone() is not None


def fetch_mdts_signed_on_dispatch_row(cursor, callsign: str) -> dict[str, Any] | None:
    """
    MDT sign-on row including ``assignedIncident`` when the column exists (Ventus CAD link).
    """
    if not table_exists(cursor, "mdts_signed_on"):
        return None
    cs = (callsign or "").strip().upper()
    if not cs:
        return None
    if _mdts_has_assigned_incident_column(cursor):
        cursor.execute(
            """
            SELECT callSign, crew, division, status, signOnTime, lastSeenAt, assignedIncident
            FROM mdts_signed_on WHERE callSign = %s LIMIT 1
            """,
            (cs,),
        )
    else:
        cursor.execute(
            "SELECT callSign, crew, division, status, signOnTime, lastSeenAt FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
            (cs,),
        )
    row = cursor.fetchone()
    if not row:
        return None
    out: dict[str, Any] = {
        "callSign": row[0],
        "crew": row[1],
        "division": row[2] if len(row) > 2 else None,
        "status": row[3] if len(row) > 3 else None,
        "signOnTime": row[4] if len(row) > 4 else None,
        "lastSeenAt": row[5] if len(row) > 5 else None,
    }
    if len(row) > 6 and row[6] is not None:
        try:
            out["assignedIncident"] = int(row[6])
        except (TypeError, ValueError):
            out["assignedIncident"] = None
    else:
        out["assignedIncident"] = None
    return out


def load_mdt_job_bundle_by_cad(cursor, cad: int) -> dict[str, Any] | None:
    """Return cad, status, data (dict), chief_complaint (if column exists)."""
    if not table_exists(cursor, "mdt_jobs"):
        return None
    try:
        cad_int = int(cad)
    except (TypeError, ValueError):
        return None
    cursor.execute(
        "SELECT 1 FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mdt_jobs' AND COLUMN_NAME = 'chief_complaint' LIMIT 1"
    )
    has_chief = cursor.fetchone() is not None
    if has_chief:
        cursor.execute(
            "SELECT cad, status, data, chief_complaint FROM mdt_jobs WHERE cad = %s LIMIT 1",
            (cad_int,),
        )
    else:
        cursor.execute(
            "SELECT cad, status, data FROM mdt_jobs WHERE cad = %s LIMIT 1",
            (cad_int,),
        )
    row = cursor.fetchone()
    if not row:
        return None
    raw_data = row[2]
    parsed: dict[str, Any] | None = None
    if raw_data is not None:
        if isinstance(raw_data, dict):
            parsed = raw_data
        elif isinstance(raw_data, (bytes, str)):
            try:
                v = json.loads(raw_data)
                parsed = v if isinstance(v, dict) else {}
            except Exception:
                parsed = {}
        else:
            parsed = {}
    if parsed is None:
        parsed = {}
    cc = None
    if has_chief and len(row) > 3:
        cc = row[3]
    return {
        "cad": int(row[0]) if row[0] is not None else cad_int,
        "status": row[1],
        "data": parsed,
        "chief_complaint": (cc or "").strip() if isinstance(cc, str) else (cc or None),
    }


def username_in_mdts_crew(crew_raw: Any, username: str) -> bool:
    want = (username or "").strip().lower()
    if not want:
        return False
    return want in parse_mdts_crew_usernames(crew_raw)


def event_config_dict(config_raw: Any) -> dict[str, Any]:
    if config_raw is None:
        return {}
    if isinstance(config_raw, dict):
        return config_raw
    if isinstance(config_raw, str):
        try:
            v = json.loads(config_raw)
            return v if isinstance(v, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


_EPCR_SIGNON_FIELD_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def _derive_epcr_signon_storage_key(label: str, used: set[str], seq: int) -> str:
    """
    Build a valid snake_case storage key from the crew-facing label (unique within ``used``).
    Operators only need to type the question text; this keeps responseTypeMeta keys stable and valid.
    """
    t = (label or "").strip().lower()
    base = re.sub(r"[^a-z0-9]+", "_", t).strip("_")
    if not base:
        base = f"field_{seq}"
    if not base[0].isalpha():
        tail = re.sub(r"^[^a-z]+", "", base)
        base = ("f_" + tail) if tail else f"field_{seq}"
    base = base[:48]
    k = base
    n = 1
    while k in used or not _EPCR_SIGNON_FIELD_KEY_RE.match(k):
        suffix = f"_{n}"
        k = (base + suffix)[:62]
        n += 1
        if n > 400:
            k = f"field_{seq}"
            while k in used:
                seq += 1
                k = f"field_{seq}"[:62]
            break
    return k[:62]


def normalize_epcr_signon_incident_fields(raw: Any, *, max_fields: int = 20) -> list[dict[str, Any]]:
    """
    Per–operational-period Incident Log extras (runner/bib, bike tag, marshal name, etc.).
    Stored on ``cura_operational_events.config["epcr_signon_incident_fields"]`` and returned on
    ``/api/cura/me/operational-context`` so Cura does not rely on the static incident_response_options dataset.

    Each item may include optional ``response_type`` (lowercase) to show the field only when the chart’s
    Incident Log response type matches; omit or empty means all response types.

    If ``key`` is missing or invalid, a unique key is derived from ``label`` so admins do not need to
    author snake_case identifiers. Existing saved keys are preserved when still valid and unique.
    """
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    used: set[str] = set()
    for idx, row in enumerate(raw):
        if len(out) >= max_fields:
            break
        if not isinstance(row, dict):
            continue
        key_in = str(row.get("key") or "").strip().lower()
        label_raw = str(row.get("label") or "").strip()
        if not label_raw and not key_in:
            continue
        if (
            key_in
            and _EPCR_SIGNON_FIELD_KEY_RE.match(key_in)
            and key_in not in used
        ):
            key = key_in
        else:
            slug_src = label_raw if label_raw else key_in.replace("_", " ")
            key = _derive_epcr_signon_storage_key(slug_src, used, idx + 1)
        used.add(key)
        label = (label_raw or key.replace("_", " ").title())[:120]
        typ = str(row.get("type") or "text").strip().lower()
        typ_out = "number" if typ == "number" else "text"
        item: dict[str, Any] = {"key": key, "label": label, "type": typ_out}
        ph = str(row.get("placeholder") or "").strip()[:200]
        if ph:
            item["placeholder"] = ph
        rt = str(row.get("response_type") or row.get("responseType") or "").strip().lower()
        if rt:
            item["response_type"] = rt[:80]
        out.append(item)
    return out


def _naive_dt(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None):
        return dt.replace(tzinfo=None)
    return dt


def _operational_event_status_is_liveish(status: str | None) -> bool:
    """Statuses that should keep the linked Ventus dispatch division active (incl. test / sim periods)."""
    st = (status or "").strip().lower()
    return st in (
        "active",
        "live",
        "open",
        "running",
        "test",
        "testing",
        "simulation",
        "sim",
        "rehearsal",
        "exercise",
        "trial",
    )


def event_active_for_automation(
    *,
    status: str | None,
    starts_at: datetime | None,
    ends_at: datetime | None,
    now: datetime | None = None,
) -> bool:
    """Whether we auto-activate a Ventus division for this event."""
    now_n = _naive_dt(now or datetime.utcnow())
    if not _operational_event_status_is_liveish(status):
        return False
    sa = _naive_dt(starts_at)
    ea = _naive_dt(ends_at)
    if sa and now_n and now_n < sa:
        return False
    if ea and now_n and now_n > ea:
        return False
    return True


def division_matches_event(mdts_division: Any, event_slug: str | None) -> bool:
    if not (event_slug or "").strip():
        return True
    md = (str(mdts_division).strip() if mdts_division is not None else "") or ""
    if not md:
        return False
    return normalize_division_slug(md, "") == normalize_division_slug(event_slug, "")


def ensure_ventus_division_for_event(
    cursor,
    *,
    slug: str,
    name: str,
    color: str,
    is_active: int,
    updated_by: str | None,
    cura_operational_event_id: int | None = None,
    event_window_start=None,
    event_window_end=None,
) -> bool:
    """
    Upsert mdt_dispatch_divisions. Returns False if table missing.

    When ``cura_operational_event_id`` is set (API ``ventus-division/sync``), persist the same linkage and
    time window as ``provision_operational_event_dispatch_division`` so Cura ``/me/operational-context`` and
    MI ``events/assigned`` slug resolution see ``operational_event_id`` on each division row.
    """
    if not table_exists(cursor, "mdt_dispatch_divisions"):
        logger.warning("mdt_dispatch_divisions missing; Ventus may not be installed")
        return False
    slug = normalize_division_slug(slug, fallback="")
    if not slug or not _SLUG_RE.match(slug):
        raise ValueError("Invalid ventus_division_slug")
    nm = (name or slug).strip()[:120] or slug
    col = (color or "#6366f1").strip()[:16] if color else "#6366f1"
    act = 1 if int(is_active or 0) else 0
    who = (updated_by or "cura_event_sync")[:120]
    eid = None
    if cura_operational_event_id is not None:
        try:
            eid = int(cura_operational_event_id)
        except (TypeError, ValueError):
            eid = None
        if eid is not None and eid <= 0:
            eid = None
    if eid is not None:
        try:
            from app.plugins.ventus_response_module.routes import (
                _ensure_dispatch_divisions_table,
                _upgrade_mdt_dispatch_divisions_event_scope,
            )

            _ensure_dispatch_divisions_table(cursor)
            _upgrade_mdt_dispatch_divisions_event_scope(cursor)
        except Exception as ex:
            logger.warning("ensure_ventus_division_for_event: scope upgrade: %s", ex)
        cursor.execute(
            """
            INSERT INTO mdt_dispatch_divisions
              (slug, name, color, is_active, is_default, created_by,
               cura_operational_event_id, event_window_start, event_window_end)
            VALUES (%s, %s, %s, %s, 0, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                color = VALUES(color),
                is_active = VALUES(is_active),
                cura_operational_event_id = VALUES(cura_operational_event_id),
                event_window_start = VALUES(event_window_start),
                event_window_end = VALUES(event_window_end),
                updated_at = CURRENT_TIMESTAMP
            """,
            (slug, nm, col, act, who, eid, event_window_start, event_window_end),
        )
        return True
    cursor.execute(
        """
        INSERT INTO mdt_dispatch_divisions (slug, name, color, is_active, is_default, created_by)
        VALUES (%s, %s, %s, %s, 0, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            color = VALUES(color),
            is_active = VALUES(is_active),
            updated_at = CURRENT_TIMESTAMP
        """,
        (slug, nm, col, act, who),
    )
    return True


def fetch_cad_dispatch_correlation_for_event(
    cursor,
    event_id: int,
    *,
    max_cads: int = 60,
    comms_per_cad: int = 24,
    max_comms_total: int = 1500,
) -> dict[str, Any]:
    """
    Correlate a Cura operational period with Ventus CAD jobs: ``mdt_jobs.division`` vs
    ``config.ventus_division_slug``, plus recent rows from ``mdt_job_comms``.

    If ``comms_per_cad`` is 0, only per-CAD comms **counts** are loaded (no message samples).

    Requires Ventus tables; returns a structured ``available: False`` payload when prerequisites are missing.
    """
    out: dict[str, Any] = {
        "available": False,
        "operational_event_id": event_id,
        "ventus_division_slug": None,
        "time_window": {"starts_at": None, "ends_at": None},
        "cads": [],
        "totals": {"cads": 0, "comms_included": 0},
        "note": "",
    }
    cursor.execute(
        "SELECT starts_at, ends_at, config FROM cura_operational_events WHERE id = %s",
        (event_id,),
    )
    row = cursor.fetchone()
    if not row:
        out["reason"] = "operational_event_not_found"
        return out

    window_start, window_end, config_raw = row[0], row[1], row[2]
    cfg = event_config_dict(config_raw)
    slug_raw = cfg.get("ventus_division_slug")
    slug = normalize_division_slug(slug_raw, "") if (slug_raw or "").strip() else ""
    out["ventus_division_slug"] = slug or None
    out["time_window"] = {
        "starts_at": window_start.isoformat() if window_start and hasattr(window_start, "isoformat") else None,
        "ends_at": window_end.isoformat() if window_end and hasattr(window_end, "isoformat") else None,
    }

    if not slug:
        out["reason"] = "no_ventus_division_slug"
        out["note"] = "Set config.ventus_division_slug on the operational period (use Ventus division sync) to enable CAD correlation."
        return out

    if not table_exists(cursor, "mdt_jobs"):
        out["reason"] = "mdt_jobs_missing"
        out["note"] = "Ventus mdt_jobs table not found."
        return out

    cursor.execute(
        "SELECT 1 FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mdt_jobs' AND COLUMN_NAME = 'division'"
    )
    if not cursor.fetchone():
        out["reason"] = "mdt_jobs_no_division_column"
        out["note"] = "mdt_jobs.division column missing; upgrade Ventus dispatch schema."
        return out

    cursor.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mdt_jobs' "
        "AND COLUMN_NAME IN ('created_at','updated_at')"
    )
    time_cols = {r[0] for r in (cursor.fetchall() or [])}
    has_created = "created_at" in time_cols
    has_updated = "updated_at" in time_cols
    ts_expr = None
    if has_updated and has_created:
        ts_expr = "COALESCE(j.updated_at, j.created_at)"
    elif has_updated:
        ts_expr = "j.updated_at"
    elif has_created:
        ts_expr = "j.created_at"

    time_parts: list[str] = []
    params: list[Any] = [slug]
    if ts_expr and window_start is not None:
        time_parts.append(f"{ts_expr} >= %s")
        params.append(window_start)
    if ts_expr and window_end is not None:
        time_parts.append(f"{ts_expr} <= %s")
        params.append(window_end)
    time_sql = (" AND " + " AND ".join(time_parts)) if time_parts else ""

    order_expr = ts_expr or "j.cad"
    params.append(max(1, min(int(max_cads), 500)))

    sel_created = "j.created_at" if has_created else "NULL AS created_at"
    sel_updated = "j.updated_at" if has_updated else "NULL AS updated_at"

    sql = f"""
        SELECT j.cad, j.status,
               LOWER(TRIM(COALESCE(j.division, 'general'))) AS job_division,
               {sel_created}, {sel_updated}
        FROM mdt_jobs j
        WHERE LOWER(TRIM(COALESCE(j.division, 'general'))) = %s
        {time_sql}
        ORDER BY {order_expr} DESC
        LIMIT %s
    """
    try:
        cursor.execute(sql, tuple(params))
        job_rows = list(cursor.fetchall() or [])
    except Exception as ex:
        logger.warning("fetch_cad_dispatch_correlation_for_event jobs query: %s", ex)
        out["reason"] = "mdt_jobs_query_failed"
        out["note"] = str(ex)[:200]
        return out

    cads = [int(r[0]) for r in job_rows if r[0] is not None]
    out["available"] = True
    out["reason"] = None
    if not cads:
        out["note"] = "No CAD jobs in this division for the selected time window."
        out["totals"] = {"cads": 0, "comms_included": 0}
        return out

    comms_by_cad: dict[int, list[dict[str, Any]]] = {c: [] for c in cads}
    comms_counts: dict[int, int] = {c: 0 for c in cads}

    if table_exists(cursor, "mdt_job_comms") and cads:
        placeholders = ",".join(["%s"] * len(cads))
        try:
            cursor.execute(
                f"SELECT cad, COUNT(*) FROM mdt_job_comms WHERE cad IN ({placeholders}) GROUP BY cad",
                tuple(cads),
            )
            for cad_id, cnt in cursor.fetchall() or []:
                if cad_id is not None:
                    comms_counts[int(cad_id)] = int(cnt)
        except Exception as ex:
            logger.warning("fetch_cad_dispatch_correlation_for_event comms counts: %s", ex)

        if int(comms_per_cad) > 0:
            try:
                cursor.execute(
                    f"""
                    SELECT id, cad, message_type, sender_role, sender_user, message_text, created_at
                    FROM mdt_job_comms
                    WHERE cad IN ({placeholders})
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    tuple(cads) + (max(50, min(int(max_comms_total), 8000)),),
                )
                raw_comms = list(cursor.fetchall() or [])
            except Exception as ex:
                logger.warning("fetch_cad_dispatch_correlation_for_event comms list: %s", ex)
                raw_comms = []

            per_cap = max(1, min(int(comms_per_cad), 200))
            for r in raw_comms:
                cid = int(r[1]) if r[1] is not None else None
                if cid is None or cid not in comms_by_cad:
                    continue
                if len(comms_by_cad[cid]) >= per_cap:
                    continue
                txt = r[5] or ""
                if not isinstance(txt, str):
                    txt = str(txt)
                ca = r[6]
                comms_by_cad[cid].append(
                    {
                        "id": r[0],
                        "message_type": (r[2] or "")[:32],
                        "sender_role": (r[3] or "")[:64],
                        "sender_user": (r[4] or "")[:120],
                        "message_text": txt[:600],
                        "created_at": ca.isoformat() if ca and hasattr(ca, "isoformat") else ca,
                    }
                )

    items: list[dict[str, Any]] = []
    total_included = 0
    for r in job_rows:
        cid = int(r[0])
        ca_c = r[3]
        ua = r[4]
        items.append(
            {
                "cad": cid,
                "status": (r[1] or "")[:64],
                "division": (r[2] or "")[:64],
                "created_at": ca_c.isoformat() if ca_c and hasattr(ca_c, "isoformat") else ca_c,
                "updated_at": ua.isoformat() if ua and hasattr(ua, "isoformat") else ua,
                "job_comms_total": comms_counts.get(cid, 0),
                "job_comms_sample": comms_by_cad.get(cid, []),
            }
        )
        total_included += len(comms_by_cad.get(cid, []))

    if not ts_expr:
        out["note"] = "Time window not applied (mdt_jobs has no created_at/updated_at)."
    else:
        out["note"] = "CAD jobs filtered by mdt_jobs.division and operational period time window where timestamps exist."

    out["cads"] = items
    out["totals"] = {"cads": len(items), "comms_included": total_included}
    return out


def epcr_patient_prefill_from_mdt_job(job_data: dict[str, Any], chief_complaint: str | None) -> dict[str, Any]:
    """
    Map Ventus ``mdt_jobs.data`` triage fields into Cura EPCR ``PatientInfo.ptInfo``-shaped JSON.
    """
    d = job_data if isinstance(job_data, dict) else {}
    pt: dict[str, Any] = {}
    fn = str(d.get("first_name") or "").strip()
    mn = str(d.get("middle_name") or "").strip()
    ln = str(d.get("last_name") or "").strip()
    if fn:
        pt["forename"] = fn
    if mn:
        pt["middleNames"] = mn
    if ln:
        pt["surname"] = ln
    dob = str(d.get("patient_dob") or "").strip()
    if dob:
        pt["dob"] = dob
    age_raw = d.get("patient_age")
    if age_raw is not None and str(age_raw).strip() != "":
        pt["age"] = str(age_raw).strip()
    gender = str(d.get("patient_gender") or "").strip()
    if gender:
        pt["gender"] = gender
    phone = str(d.get("phone_number") or "").strip()
    addr = str(d.get("address") or "").strip()
    pc = str(d.get("postcode") or "").strip()
    w3w = str(d.get("what3words") or "").strip()
    home: dict[str, Any] = {}
    if addr:
        home["address"] = addr
    if pc:
        home["postcode"] = pc
    if phone:
        home["telephone"] = phone
    if w3w:
        home["what3words"] = w3w
    if home:
        pt["homeAddress"] = home
    mpi = d.get("mpi_patient_id")
    if mpi is not None and str(mpi).strip() != "":
        pt["mpiPatientId"] = str(mpi).strip()
    out: dict[str, Any] = {}
    if pt:
        out["ptInfo"] = pt
    reason = str(d.get("reason_for_call") or "").strip()
    cc = (chief_complaint or "").strip() if isinstance(chief_complaint, str) else ""
    hint = reason or cc
    if hint:
        out["presentingComplaintHint"] = hint
    return out


def _mdts_callsign_dispatch_eligibility(
    cursor,
    *,
    username: str,
    callsign: str,
    operational_event_id: int | None,
) -> tuple[bool, str | None]:
    """
    Read-only checks aligned with ``validate-mdts-callsign`` (no audit log).
    Returns (eligible, reason_code).
    """
    uname = (username or "").strip()
    cs = (callsign or "").strip().upper()
    if not cs:
        return False, "no_callsign"
    md = fetch_mdts_signed_on_dispatch_row(cursor, cs)
    if not md:
        return False, "unit_not_signed_on"
    if not username_in_mdts_crew(md.get("crew"), uname):
        return False, "username_not_in_crew"
    if operational_event_id is not None:
        cursor.execute(
            "SELECT id, config FROM cura_operational_events WHERE id = %s",
            (int(operational_event_id),),
        )
        evr = cursor.fetchone()
        if evr:
            cfg = event_config_dict(evr[1])
            want_slug = (cfg.get("ventus_division_slug") or "").strip()
            if want_slug and md.get("division") is not None:
                if not division_matches_event(md.get("division"), want_slug):
                    return False, "division_mismatch"
        cursor.execute(
            """
            SELECT expected_callsign FROM cura_operational_event_assignments
            WHERE operational_event_id = %s AND LOWER(principal_username) = LOWER(%s)
            LIMIT 1
            """,
            (int(operational_event_id), uname),
        )
        erow = cursor.fetchone()
        if erow and erow[0]:
            exp = str(erow[0]).strip().upper()
            if exp and exp != cs:
                return False, "expected_callsign_mismatch"
    return True, None


def suggested_cad_rows_for_epcr(
    cursor,
    *,
    username: str,
    callsign: str,
    operational_event_id: int | None = None,
) -> dict[str, Any]:
    """
    When the unit is **on scene** on an assigned CAD, return one dashboard row the Cura client can
    show as "Create CAD case" with MDT-linked prefill (Bearer EPCR API).
    """
    out: dict[str, Any] = {"items": [], "skip_reason": None}
    ok, reason = _mdts_callsign_dispatch_eligibility(
        cursor, username=username, callsign=callsign, operational_event_id=operational_event_id
    )
    if not ok:
        out["skip_reason"] = reason
        return out
    md = fetch_mdts_signed_on_dispatch_row(cursor, (callsign or "").strip().upper())
    if not md:
        out["skip_reason"] = "unit_not_signed_on"
        return out
    unit_st = str(md.get("status") or "").strip().lower()
    if unit_st != "on_scene":
        out["skip_reason"] = "not_on_scene"
        return out
    cad = md.get("assignedIncident")
    if cad is None:
        out["skip_reason"] = "no_assigned_incident"
        return out
    try:
        cad_int = int(cad)
    except (TypeError, ValueError):
        out["skip_reason"] = "invalid_assigned_incident"
        return out
    job = load_mdt_job_bundle_by_cad(cursor, cad_int)
    if not job:
        out["skip_reason"] = "mdt_job_not_found"
        return out
    data = job.get("data") if isinstance(job.get("data"), dict) else {}
    chief = job.get("chief_complaint")
    chief_s = str(chief).strip() if chief is not None else ""
    prefill = epcr_patient_prefill_from_mdt_job(data, chief_s or None)
    cs_disp = str(md.get("callSign") or callsign or "").strip().upper()
    item = {
        "kind": "suggested_cad",
        "cad": cad_int,
        "unit_status": unit_st,
        "mdt_job_status": str(job.get("status") or "").strip().lower() or None,
        "dispatch_reference": str(cad_int),
        "primary_callsign": cs_disp,
        "patient_prefill": prefill,
    }
    out["items"] = [item]
    return out


def provision_operational_event_dispatch_division(
    cur,
    conn,
    event_id: int,
    actor: str,
    *,
    color_hint: str | None = None,
    do_commit: bool = True,
    force_resync: bool = False,
) -> dict[str, Any]:
    """
    Register a time-scoped Ventus ``mdt_dispatch_divisions`` row for this Cura operational event and
    persist linkage on ``cura_operational_events.config``.

    Cura and MDT show this division by **name** in sign-on lists only while the event is active/draft and
    the current time is inside the configured start/end window (see Ventus ``_catalog_visible_cura_event_division``).
    Clinical paperwork still resolves to the same underlying operational event for reporting.
    """
    try:
        from app.plugins.ventus_response_module.routes import (
            _ensure_dispatch_divisions_table,
            _upgrade_mdt_dispatch_divisions_event_scope,
            _normalize_hex_color,
        )
    except Exception as ex:
        logger.warning("provision_operational_event_dispatch_division: ventus import %s", ex)
        return {"ok": False, "error": "ventus_module_unavailable"}

    _ensure_dispatch_divisions_table(cur)
    _upgrade_mdt_dispatch_divisions_event_scope(cur)

    cur.execute(
        """
        SELECT id, name, slug, starts_at, ends_at, status, config
        FROM cura_operational_events WHERE id = %s
        """,
        (int(event_id),),
    )
    row = cur.fetchone()
    if not row:
        return {"ok": False, "error": "operational_event_not_found"}

    eid = int(row[0])
    ename = (row[1] or "").strip() or f"Event {eid}"
    eslug = (row[2] or "").strip()
    starts_at = row[3]
    ends_at = row[4]
    cfg = event_config_dict(row[6])
    color = (color_hint or "").strip() or cfg.get("ventus_division_color") or "#0ea5e9"
    color = _normalize_hex_color(color, "#0ea5e9")
    div_name = ename[:120]

    existing = (cfg.get("ventus_division_slug") or "").strip()
    if existing:
        ex_slug = normalize_division_slug(existing, "")
        if ex_slug:
            cur.execute(
                "SELECT 1 FROM mdt_dispatch_divisions WHERE slug = %s LIMIT 1",
                (ex_slug,),
            )
            if cur.fetchone():
                if force_resync:
                    cur.execute(
                        """
                        UPDATE mdt_dispatch_divisions SET
                          name = %s, color = %s,
                          event_window_start = %s, event_window_end = %s,
                          cura_operational_event_id = %s,
                          is_active = 1,
                          updated_at = CURRENT_TIMESTAMP
                        WHERE slug = %s
                        """,
                        (div_name, color, starts_at, ends_at, eid, ex_slug),
                    )
                    cfg["ventus_division_slug"] = ex_slug
                    cfg["ventus_division_name"] = div_name
                    cfg["ventus_division_color"] = color
                    cur.execute(
                        "UPDATE cura_operational_events SET config = %s, updated_by = %s WHERE id = %s",
                        (json.dumps(cfg), (actor or "")[:120], eid),
                    )
                    if do_commit:
                        conn.commit()
                    return {
                        "ok": True,
                        "slug": ex_slug,
                        "name": div_name,
                        "color": color,
                        "resynced": True,
                        "already": False,
                    }
                return {
                    "ok": True,
                    "slug": ex_slug,
                    "name": cfg.get("ventus_division_name") or ename,
                    "already": True,
                }

    base = f"op{eid}"
    tail = normalize_division_slug(eslug, "") if eslug else ""
    cand = f"{base}_{tail}"[:64] if tail else base[:64]
    slug = cand
    for i in range(40):
        cur.execute(
            "SELECT 1 FROM mdt_dispatch_divisions WHERE slug = %s LIMIT 1",
            (slug,),
        )
        if not cur.fetchone():
            break
        suffix = f"_{i + 2}"
        slug = (cand[: max(0, 64 - len(suffix))] + suffix)[:64]

    cur.execute(
        """
        INSERT INTO mdt_dispatch_divisions
          (slug, name, color, is_active, is_default, created_by,
           cura_operational_event_id, event_window_start, event_window_end)
        VALUES (%s, %s, %s, 1, 0, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          name = VALUES(name),
          color = VALUES(color),
          is_active = 1,
          cura_operational_event_id = VALUES(cura_operational_event_id),
          event_window_start = VALUES(event_window_start),
          event_window_end = VALUES(event_window_end),
          updated_at = CURRENT_TIMESTAMP
        """,
        (slug, div_name, color, (actor or "")[:120], eid, starts_at, ends_at),
    )
    cfg["ventus_division_slug"] = slug
    cfg["ventus_division_name"] = div_name
    cfg["ventus_division_color"] = color
    cur.execute(
        "UPDATE cura_operational_events SET config = %s, updated_by = %s WHERE id = %s",
        (json.dumps(cfg), (actor or "")[:120], eid),
    )
    if do_commit:
        conn.commit()
    return {"ok": True, "slug": slug, "name": div_name, "color": color, "already": False}


def withdraw_operational_event_services(
    cur,
    conn,
    event_id: int,
    actor: str,
    *,
    do_commit: bool = True,
) -> dict[str, Any]:
    """
    Early stand-down: mark the operational event closed, cap the end time to now, and deactivate the
    linked Ventus dispatch division so it drops off MDT/Cura sign-on immediately (cancellation, etc.).
    """
    try:
        from app.plugins.ventus_response_module.routes import (
            _ensure_dispatch_divisions_table,
            _upgrade_mdt_dispatch_divisions_event_scope,
        )
    except Exception as ex:
        logger.warning("withdraw_operational_event_services: ventus import %s", ex)
        return {"ok": False, "error": "ventus_module_unavailable"}

    eid = int(event_id)
    cur.execute(
        "SELECT id, config FROM cura_operational_events WHERE id = %s LIMIT 1",
        (eid,),
    )
    ev_row = cur.fetchone()
    if not ev_row:
        return {"ok": False, "error": "operational_event_not_found"}
    raw_ev_cfg = ev_row[1]

    try:
        from .cura_event_inventory_bridge import (
            release_event_kit_pool_if_configured,
            strip_event_kit_pool_config,
        )

        rel = release_event_kit_pool_if_configured(
            eid, raw_ev_cfg, performed_by=actor
        )
        if not rel.get("ok"):
            return {
                "ok": False,
                "error": rel.get("error") or "Could not release inventory kit pool for this event.",
            }
        cfg_after = strip_event_kit_pool_config(raw_ev_cfg)
    except Exception as ex:
        logger.warning("withdraw_operational_event_services kit pool: %s", ex)
        return {"ok": False, "error": f"Kit pool release failed: {ex}"}

    _ensure_dispatch_divisions_table(cur)
    _upgrade_mdt_dispatch_divisions_event_scope(cur)

    cur.execute(
        """
        UPDATE cura_operational_events
        SET status = 'closed',
            ends_at = LEAST(COALESCE(ends_at, UTC_TIMESTAMP()), UTC_TIMESTAMP()),
            config = %s,
            updated_by = %s
        WHERE id = %s
        """,
        (cfg_after, (actor or "")[:120], eid),
    )
    try:
        cur.execute(
            """
            SELECT name, location_summary, starts_at, ends_at, status
            FROM cura_operational_events WHERE id = %s
            """,
            (eid,),
        )
        snap = cur.fetchone()
        if snap:
            from . import cura_event_debrief as _ced_mi

            _ced_mi.push_operational_snapshot_to_mi_events(
                cur,
                eid,
                name=(snap[0] or "").strip() or f"Event {eid}",
                location_summary=(snap[1] or "").strip() or None,
                starts_at=snap[2],
                ends_at=snap[3],
                operational_status=snap[4],
                actor=(actor or "")[:128] or None,
            )
    except Exception as ex:
        logger.warning("withdraw_operational_event_services MI sync: %s", ex)
    try:
        cur.execute(
            """
            UPDATE mdt_dispatch_divisions
            SET is_active = 0,
                event_window_end = LEAST(COALESCE(event_window_end, UTC_TIMESTAMP()), UTC_TIMESTAMP()),
                updated_at = CURRENT_TIMESTAMP
            WHERE cura_operational_event_id = %s
            """,
            (eid,),
        )
    except Exception as ex:
        logger.warning("withdraw_operational_event_services: mdt_dispatch_divisions: %s", ex)

    if do_commit:
        conn.commit()
    return {"ok": True, "event_id": eid}


def list_cura_signon_dispatch_divisions(cursor_dict) -> list[dict[str, Any]]:
    """
    Divisions visible on MDT / Cura sign-on (time-window filter for event-linked rows).
    ``cursor_dict`` must be from ``conn.cursor(dictionary=True)``.
    """
    try:
        from app.plugins.ventus_response_module.routes import _list_dispatch_divisions

        return _list_dispatch_divisions(
            cursor_dict, include_inactive=False, filter_event_window=True
        )
    except Exception as ex:
        logger.warning("list_cura_signon_dispatch_divisions: %s", ex)
        return []
