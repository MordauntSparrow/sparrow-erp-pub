from __future__ import annotations

import csv
import io
import json
import logging
import re
from pathlib import Path
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from flask import current_app

from app.objects import get_db_connection
from app.organization_profile import normalize_organization_industries, tenant_matches_industry

from .constants import (
    ALLOWED_TRANSITIONS,
    HARM_GRADE_DEFAULTS,
    INCIDENT_FORM_VISIBILITY_ROLE_CHOICES,
    INCIDENT_MODES,
    MEDICAL_CATEGORIES,
    MERP_FIELD_KEYS,
    ORG_SEVERITY_DEFAULTS,
    STATUS_LABELS,
    TERMINAL_STATUSES,
    UNIVERSAL_CATEGORIES,
)
from .safeguarding_bridge import create_safeguarding_referral_for_contractor

logger = logging.getLogger(__name__)


def tenant_has_medical() -> bool:
    try:
        ids = normalize_organization_industries(
            current_app.config.get("organization_industries")
        )
        return tenant_matches_industry(ids, "medical")
    except Exception:
        return False


def categories_for_tenant() -> List[Dict[str, str]]:
    rows = [{"slug": s, "label": l} for s, l in UNIVERSAL_CATEGORIES]
    if tenant_has_medical():
        rows.extend([{"slug": s, "label": l} for s, l in MEDICAL_CATEGORIES])
    return rows


def parse_datetime_local_for_mysql(val: Optional[str]) -> Optional[str]:
    """Parse HTML ``datetime-local`` or date-only string to MySQL ``DATETIME`` (naive)."""
    v = (val or "").strip()
    if not v:
        return None
    if "T" not in v and len(v) == 10 and v[4] == "-" and v[7] == "-":
        return f"{v} 00:00:00"
    try:
        s = v.replace("Z", "")
        if len(s) == 16 and s[10] == "T":
            s = s + ":00"
        dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def pack_merp_from_flat(form_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Normalise MERP-style keys from a flat form mapping into a JSON-ready dict."""
    text_any = any(
        form_dict.get(k) not in (None, "") and str(form_dict.get(k)).strip()
        for k in MERP_FIELD_KEYS
        if k != "high_alert"
    )
    if not text_any and "high_alert" not in form_dict:
        return {}
    out: Dict[str, Any] = {}
    for k in MERP_FIELD_KEYS:
        v = form_dict.get(k)
        if k == "high_alert":
            if "high_alert" in form_dict:
                out[k] = bool(v) if isinstance(v, bool) else str(v or "").lower() in (
                    "1",
                    "true",
                    "yes",
                    "on",
                )
        elif v is not None and str(v).strip():
            out[k] = str(v).strip()[:512]
    return out


def merge_medication_payload(
    existing: Any,
    form_dict: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    merp = pack_merp_from_flat(form_dict)
    if not merp:
        if isinstance(existing, dict):
            return existing
        if isinstance(existing, str) and existing.strip():
            try:
                parsed = json.loads(existing)
                return parsed if isinstance(parsed, dict) else None
            except Exception:
                return None
        return None
    base: Dict[str, Any] = {}
    if isinstance(existing, dict):
        base = {str(k): v for k, v in existing.items() if k not in MERP_FIELD_KEYS}
    elif isinstance(existing, str) and existing.strip():
        try:
            parsed = json.loads(existing)
            if isinstance(parsed, dict):
                base = {str(k): v for k, v in parsed.items() if k not in MERP_FIELD_KEYS}
        except Exception:
            base = {}
    merged = {**base, **merp}
    return merged if merged else None


def merp_display(existing: Any) -> Dict[str, Any]:
    """Flatten ``medication_json`` for templates."""
    if isinstance(existing, dict):
        return {k: existing.get(k) for k in MERP_FIELD_KEYS}
    if isinstance(existing, str) and existing.strip():
        try:
            d = json.loads(existing)
            if isinstance(d, dict):
                return {k: d.get(k) for k in MERP_FIELD_KEYS}
        except Exception:
            pass
    return {k: None for k in MERP_FIELD_KEYS}


def _narrow_sql_clause(
    narrow: Optional[Dict[str, Any]],
    *,
    table_alias: Optional[str] = None,
) -> Tuple[str, List[Any]]:
    """
    Row-level scope for ``view_own`` / ``view_site`` (PRD §12).
    Returns ``(sql_fragment, params)`` where ``sql_fragment`` is safe to ``AND``-join
    (no leading ``AND``).

    ``narrow`` shapes:
    - ``None``: no restriction (empty fragment)
    - ``{"type": "deny"}`` / ``site_unset``: match nothing
    - ``{"type": "own", "user_id": str, "contractor_id": Optional[int]}``
    - ``{"type": "site", "site_match": str}``
    - ``{"type": "own_or_site", ...}`` — union of own and site clauses
    """
    if not narrow:
        return "", []
    p = f"{table_alias}." if table_alias else ""
    t = (narrow.get("type") or "").strip()
    if t in ("deny", "site_unset"):
        return "1=0", []
    uid = narrow.get("user_id")
    cid = narrow.get("contractor_id")
    site = (narrow.get("site_match") or "").strip()
    own_sql = (
        f"({p}reporter_user_id = %s OR (%s IS NOT NULL AND {p}reporter_contractor_id = %s))"
    )
    own_params: List[Any] = [uid, cid, cid]
    if t == "own":
        return own_sql, own_params
    if t == "site" and site:
        like = f"%{site}%"
        return (
            f"({p}site_label LIKE %s OR {p}division_label LIKE %s)",
            [like, like],
        )
    if t == "own_or_site" and site:
        like = f"%{site}%"
        return (
            f"({own_sql} OR ({p}site_label LIKE %s OR {p}division_label LIKE %s))",
            own_params + [like, like],
        )
    if t == "own_or_site" and not site:
        return own_sql, own_params
    return "1=0", []


def incident_readable_by_narrow(row: dict, narrow: Optional[Dict[str, Any]]) -> bool:
    if not narrow or not (narrow.get("type") or "").strip():
        return True
    t = narrow["type"]
    if t in ("deny", "site_unset"):
        return False
    uid = str(narrow.get("user_id") or "")
    cid = narrow.get("contractor_id")
    site = (narrow.get("site_match") or "").strip()
    own_ok = (str(row.get("reporter_user_id") or "") == uid) or (
        cid is not None
        and int(row.get("reporter_contractor_id") or 0) == int(cid)
    )
    sl = (row.get("site_label") or "").lower()
    dl = (row.get("division_label") or "").lower()
    site_ok = bool(site) and (site.lower() in sl or site.lower() in dl)
    if t == "own":
        return own_ok
    if t == "site":
        return site_ok
    if t == "own_or_site":
        return own_ok or site_ok
    return False


def subscriber_events_include(events_cell: Optional[str], event_key: str) -> bool:
    """Return True if a subscription row should receive this event_key (e.g. status_change)."""
    ek = (event_key or "").strip().lower()
    if not ek:
        return True
    raw = (events_cell or "").strip().lower()
    if not raw or raw == "all":
        return True
    parts = {p.strip().lower() for p in raw.split(",") if p.strip()}
    return "all" in parts or ek in parts


def notify_subscribers_for_incident(
    incident_id: int,
    event_key: str,
    subject: str,
    body: str,
    *,
    exclude_user_id: Optional[str] = None,
) -> int:
    """
    Resolve incident_subscriptions to users.contractor_id and insert ep_messages.
    Skips subscribers with no linked contractor_id. Returns rows inserted.
    """
    subs = list_subscriptions(incident_id)
    if not subs:
        return 0
    ex = str(exclude_user_id).strip() if exclude_user_id else ""
    user_ids: List[str] = []
    for s in subs:
        uid = str(s.get("user_id") or "").strip()
        if not uid or (ex and uid == ex):
            continue
        if not subscriber_events_include(s.get("events"), event_key):
            continue
        user_ids.append(uid)
    if not user_ids:
        return 0
    user_ids = list(dict.fromkeys(user_ids))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        ph = ",".join(["%s"] * len(user_ids))
        cur.execute(
            f"""
            SELECT DISTINCT contractor_id FROM users
            WHERE id IN ({ph}) AND contractor_id IS NOT NULL
            """,
            user_ids,
        )
        cids = [int(r[0]) for r in (cur.fetchall() or []) if r and r[0] is not None]
    finally:
        cur.close()
        conn.close()
    cids = list(dict.fromkeys(cids))
    if not cids:
        return 0
    try:
        from app.plugins.employee_portal_module.services import admin_send_message

        return admin_send_message(
            cids,
            (subject or "").strip()[:255],
            (body or "")[:65535],
            source_module="incident_reporting_module",
            sent_by_user_id=None,
        )
    except Exception as ex:
        logger.warning("notify_subscribers_for_incident: %s", ex)
        return 0


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _audit(
    cur,
    incident_id: int,
    action: str,
    actor: str,
    detail: Optional[dict] = None,
) -> None:
    cur.execute(
        """
        INSERT INTO incident_audit_log (incident_id, action, detail_json, actor_label)
        VALUES (%s, %s, %s, %s)
        """,
        (incident_id, action, json.dumps(detail) if detail else None, actor[:255]),
    )


def _append_status_history(
    cur,
    incident_id: int,
    from_status: Optional[str],
    to_status: str,
    actor: str,
    note: Optional[str] = None,
) -> None:
    cur.execute(
        """
        INSERT INTO incident_status_history (incident_id, from_status, to_status, actor_label, note)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (incident_id, from_status, to_status, actor[:255], (note or "")[:512] or None),
    )


def next_reference_code(cur) -> str:
    year = datetime.utcnow().year
    prefix = f"INC-{year}-"
    cur.execute(
        "SELECT reference_code FROM incidents WHERE reference_code LIKE %s ORDER BY reference_code DESC LIMIT 1",
        (prefix + "%",),
    )
    row = cur.fetchone()
    last = 0
    if row and row[0]:
        tail = str(row[0]).replace(prefix, "").strip()
        if tail.isdigit():
            last = int(tail)
    n = last + 1
    return f"{prefix}{n:05d}"


def transition_allowed(from_status: str, to_status: str) -> bool:
    fs = (from_status or "").strip()
    ts = (to_status or "").strip()
    return ts in ALLOWED_TRANSITIONS.get(fs, frozenset())


def admin_dashboard_metrics(
    narrow: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    narrow_sql, narrow_params = _narrow_sql_clause(narrow)
    extra_sql = f" AND ({narrow_sql})" if narrow_sql else ""
    extra_params = narrow_params
    out: Dict[str, Any] = {
        "total_open": 0,
        "draft": 0,
        "submitted": 0,
        "investigation": 0,
        "closed_30d": 0,
        "near_miss_ratio": None,
        "sla_breaches": 0,
        "good_catches_30d": 0,
    }
    try:
        cur.execute(
            """
            SELECT status, COUNT(*) AS c FROM incidents
            WHERE merged_into_id IS NULL
            """
            + extra_sql
            + """
            GROUP BY status
            """,
            tuple(extra_params),
        )
        for r in cur.fetchall() or []:
            st = (r.get("status") or "").lower()
            c = int(r.get("c") or 0)
            if st == "draft":
                out["draft"] = c
            if st == "submitted":
                out["submitted"] = c
            if st == "investigation":
                out["investigation"] = c
            if st not in ("closed", "withdrawn", "merged"):
                out["total_open"] += c
        cur.execute(
            """
            SELECT COUNT(*) AS c FROM incidents
            WHERE merged_into_id IS NULL AND status = 'closed'
              AND closed_at >= UTC_TIMESTAMP() - INTERVAL 30 DAY
            """
            + extra_sql,
            tuple(extra_params),
        )
        r = cur.fetchone()
        out["closed_30d"] = int((r or {}).get("c") or 0)
        cur.execute(
            """
            SELECT
              SUM(CASE WHEN incident_mode IN ('near_miss','hazard','good_catch') THEN 1 ELSE 0 END) AS nm,
              COUNT(*) AS tot
            FROM incidents
            WHERE merged_into_id IS NULL AND created_at >= UTC_TIMESTAMP() - INTERVAL 90 DAY
            """
            + extra_sql,
            tuple(extra_params),
        )
        r2 = cur.fetchone() or {}
        tot = int(r2.get("tot") or 0)
        nm = int(r2.get("nm") or 0)
        out["near_miss_ratio"] = round(nm / tot, 3) if tot else None
        cur.execute(
            """
            SELECT COUNT(*) AS c FROM incidents
            WHERE merged_into_id IS NULL AND sla_due_at IS NOT NULL
              AND sla_due_at < UTC_TIMESTAMP()
              AND status NOT IN ('closed','withdrawn','merged')
            """
            + extra_sql,
            tuple(extra_params),
        )
        out["sla_breaches"] = int((cur.fetchone() or {}).get("c") or 0)
        cur.execute(
            """
            SELECT COUNT(*) AS c FROM incidents
            WHERE merged_into_id IS NULL AND incident_mode = 'good_catch'
              AND created_at >= UTC_TIMESTAMP() - INTERVAL 30 DAY
            """
            + extra_sql,
            tuple(extra_params),
        )
        out["good_catches_30d"] = int((cur.fetchone() or {}).get("c") or 0)
    except Exception as e:
        logger.warning("admin_dashboard_metrics: %s", e)
    finally:
        cur.close()
        conn.close()
    return out


def parse_hr_involved_contractor_ids_from_row(row: Optional[Dict[str, Any]]) -> List[int]:
    """Normalise ``hr_involved_contractor_ids`` JSON on an incident row to a list of ints."""
    if not row:
        return []
    raw = row.get("hr_involved_contractor_ids")
    if raw is None:
        return []
    if isinstance(raw, list):
        out: List[int] = []
        for x in raw:
            try:
                out.append(int(x))
            except (TypeError, ValueError):
                continue
        return sorted(set(out))
    if isinstance(raw, str) and raw.strip():
        try:
            v = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if isinstance(v, list):
            return parse_hr_involved_contractor_ids_from_row({"hr_involved_contractor_ids": v})
    return []


def hr_involved_contractor_ids_from_form(form: Any, prefix: str = "hr_cid_", max_slots: int = 24) -> List[int]:
    """Collect unique contractor ids from ``hr_cid_0`` … indexed POST fields."""
    ids: List[int] = []
    for i in range(max_slots):
        v = (form.get(f"{prefix}{i}") or "").strip()
        if not v or not v.isdigit():
            continue
        ids.append(int(v))
    return sorted(set(ids))


def contractor_labels_for_ids(ids: Sequence[int]) -> Dict[int, Dict[str, Optional[str]]]:
    """Resolve ``tb_contractors`` id → display name / email for incident UI."""
    clean = sorted({int(x) for x in ids if x is not None})
    if not clean:
        return {}
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    out: Dict[int, Dict[str, Optional[str]]] = {}
    try:
        ph = ",".join(["%s"] * len(clean))
        cur.execute(
            f"""
            SELECT id, name, email, username
            FROM tb_contractors
            WHERE id IN ({ph})
            """,
            tuple(clean),
        )
        for r in cur.fetchall() or []:
            iid = int(r["id"])
            out[iid] = {
                "name": (r.get("name") or "").strip() or None,
                "email": (r.get("email") or "").strip() or None,
                "username": (r.get("username") or "").strip() or None,
            }
    except Exception as ex:
        logger.warning("contractor_labels_for_ids: %s", ex)
    finally:
        cur.close()
        conn.close()
    return out


def contractor_ids_not_in_database(ids: Sequence[int]) -> List[int]:
    """Return subset of ``ids`` that are not present in ``tb_contractors``."""
    clean = sorted({int(x) for x in ids if x is not None})
    if not clean:
        return []
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        ph = ",".join(["%s"] * len(clean))
        cur.execute(f"SELECT id FROM tb_contractors WHERE id IN ({ph})", tuple(clean))
        found = {int(r[0]) for r in (cur.fetchall() or [])}
    except Exception as ex:
        logger.warning("contractor_ids_not_in_database: %s", ex)
        return []
    finally:
        cur.close()
        conn.close()
    return [i for i in clean if i not in found]


def ir1_supplementary_from_form(form: Any) -> Optional[str]:
    """Build JSON object string from ``ir1x_key_N`` / ``ir1x_val_N`` POST fields."""
    idxs: Set[int] = set()
    try:
        key_iter = list(form.keys())
    except Exception:
        key_iter = []
    for k in key_iter:
        m = re.match(r"^ir1x_key_(\d+)$", str(k))
        if m:
            idxs.add(int(m.group(1)))
    out: Dict[str, str] = {}
    for i in sorted(idxs):
        kk = (form.get(f"ir1x_key_{i}") or "").strip()
        if not kk:
            continue
        out[kk] = (form.get(f"ir1x_val_{i}") or "").strip()
    if not out:
        return None
    return json.dumps(out, ensure_ascii=False)


def ir1_supplementary_form_rows(raw: Any) -> List[Tuple[str, str]]:
    """Key/value pairs for IR1 supplementary editor, padded with blank rows."""
    d: Dict[str, Any] = {}
    if isinstance(raw, dict):
        d = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            d = parsed
    items: List[Tuple[str, str]] = [
        (str(k), "" if v is None else str(v)) for k, v in d.items()
    ]
    while len(items) < 4:
        items.append(("", ""))
    items.append(("", ""))
    return items


def list_incidents_linked_to_contractor(contractor_id: int, limit: int = 25) -> List[dict]:
    """Incidents where this person is reporter or listed on ``hr_involved_contractor_ids``."""
    cid = int(contractor_id)
    lim = min(max(limit, 1), 100)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, public_uuid, reference_code, incident_mode, category_slug, status, title,
                   org_severity_code, safeguarding_required, created_at, updated_at, sla_due_at
            FROM incidents
            WHERE merged_into_id IS NULL
              AND (
                reporter_contractor_id = %s
                OR JSON_CONTAINS(COALESCE(hr_involved_contractor_ids, JSON_ARRAY()), CAST(%s AS JSON), '$')
              )
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (cid, cid, lim),
        )
        return list(cur.fetchall() or [])
    except Exception as ex:
        logger.warning("list_incidents_linked_to_contractor: %s", ex)
        return []
    finally:
        cur.close()
        conn.close()


def admin_list_incidents(
    *,
    status: Optional[str] = None,
    status_in: Optional[Sequence[str]] = None,
    category: Optional[str] = None,
    mode: Optional[str] = None,
    q: Optional[str] = None,
    review: Optional[str] = None,
    limit: int = 200,
    narrow: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["merged_into_id IS NULL"]
        params: List[Any] = []
        n_sql, n_params = _narrow_sql_clause(narrow)
        if n_sql:
            where.append(n_sql)
            params.extend(n_params)
        if status_in:
            clean = [str(s).strip() for s in status_in if str(s).strip()]
            if clean:
                ph = ",".join(["%s"] * len(clean))
                where.append(f"status IN ({ph})")
                params.extend(clean)
        elif status:
            where.append("status = %s")
            params.append(status)
        if category:
            where.append("category_slug = %s")
            params.append(category)
        if mode:
            where.append("incident_mode = %s")
            params.append(mode)
        if q:
            qs = q.strip()
            like = f"%{qs}%"
            id_match: Optional[int] = None
            if qs.isdigit():
                try:
                    cand = int(qs)
                    if 1 <= cand <= 2147483647:
                        id_match = cand
                except (TypeError, ValueError):
                    id_match = None
            if id_match is not None:
                where.append(
                    "(id = %s OR title LIKE %s OR narrative LIKE %s OR reference_code LIKE %s)"
                )
                params.extend([id_match, like, like, like])
            else:
                where.append("(title LIKE %s OR narrative LIKE %s OR reference_code LIKE %s)")
                params.extend([like, like, like])
        if review == "sla":
            where.append(
                "sla_due_at IS NOT NULL AND sla_due_at < UTC_TIMESTAMP() "
                "AND status NOT IN ('closed','withdrawn','merged')"
            )
        sql = f"""
            SELECT id, public_uuid, reference_code, incident_mode, category_slug, status, title,
                   org_severity_code, harm_grade_code, safeguarding_required,
                   linked_safeguarding_referral_id, linked_safeguarding_public_id,
                   reporter_contractor_id, reporter_user_id, created_at, submitted_at, closed_at, sla_due_at
            FROM incidents
            WHERE {' AND '.join(where)}
            ORDER BY updated_at DESC
            LIMIT %s
        """
        params.append(min(max(limit, 1), 500))
        cur.execute(sql, tuple(params))
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def get_incident(incident_id: int) -> Optional[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT * FROM incidents WHERE id = %s LIMIT 1", (int(incident_id),)
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def get_incident_by_uuid(public_uuid: str) -> Optional[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT * FROM incidents WHERE public_uuid = %s LIMIT 1",
            (public_uuid.strip(),),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def create_draft_portal(
    *,
    contractor_id: int,
    reporter_user_id: Optional[str],
    actor_label: str,
) -> str:
    pub = _new_uuid()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO incidents (public_uuid, status, reporter_contractor_id, reporter_user_id, title)
            VALUES (%s, 'draft', %s, %s, %s)
            """,
            (pub, int(contractor_id), reporter_user_id, "New incident"),
        )
        iid = cur.lastrowid
        _audit(cur, int(iid), "create_draft", actor_label, {"portal": True})
        conn.commit()
        return pub
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def create_draft_admin(
    *,
    reporter_user_id: Optional[str],
    reporter_contractor_id: Optional[int],
    actor_label: str,
) -> int:
    """Create a draft incident from the staff admin console (same record type as portal drafts). Returns new ``id``."""
    pub = _new_uuid()
    uid = (reporter_user_id or "").strip() or None
    if uid is not None:
        uid = uid[:36]
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO incidents (public_uuid, status, reporter_contractor_id, reporter_user_id, title)
            VALUES (%s, 'draft', %s, %s, %s)
            """,
            (pub, reporter_contractor_id, uid, "New incident"),
        )
        iid = int(cur.lastrowid)
        _audit(cur, iid, "create_draft", actor_label, {"admin": True})
        conn.commit()
        return iid
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def update_incident_core(
    incident_id: int,
    fields: Dict[str, Any],
    actor_label: str,
) -> None:
    allowed = {
        "title",
        "narrative",
        "immediate_actions",
        "environment_notes",
        "incident_mode",
        "category_slug",
        "org_severity_code",
        "harm_grade_code",
        "patient_involved",
        "deidentified_narrative",
        "medication_json",
        "barrier_notes",
        "five_whys",
        "site_label",
        "division_label",
        "shift_reference",
        "vehicle_reference",
        "crm_site_id",
        "compliance_policy_id",
        "safeguarding_required",
        "operational_event_id",
        "hr_involved_contractor_ids",
        "incident_occurred_at",
        "incident_discovered_at",
        "exact_location_detail",
        "witnesses_text",
        "equipment_involved",
        "riddor_notifiable",
        "reporter_job_title",
        "reporter_department",
        "reporter_contact_phone",
        "people_affected_count",
        "ir1_supplementary_json",
    }
    sets = []
    vals: List[Any] = []
    for k, v in fields.items():
        if k not in allowed:
            continue
        if k in (
            "patient_involved",
            "deidentified_narrative",
            "safeguarding_required",
            "riddor_notifiable",
        ):
            v = 1 if str(v).lower() in ("1", "true", "yes", "on") else 0
        if k == "people_affected_count":
            if v is None or str(v).strip() == "":
                v = None
            else:
                try:
                    v = int(v)
                except (TypeError, ValueError):
                    v = None
        if k == "compliance_policy_id":
            if v is None or str(v).strip() == "":
                v = None
            else:
                try:
                    v = int(v)
                except (TypeError, ValueError):
                    v = None
        if k in ("incident_occurred_at", "incident_discovered_at"):
            if v is None or (isinstance(v, str) and not str(v).strip()):
                v = None
            elif isinstance(v, str):
                v = parse_datetime_local_for_mysql(v)
        if k == "ir1_supplementary_json":
            if v is None or (isinstance(v, str) and not v.strip()):
                v = None
            elif isinstance(v, str):
                try:
                    v = json.dumps(json.loads(v))
                except json.JSONDecodeError:
                    v = None
            elif isinstance(v, (dict, list)):
                v = json.dumps(v)
        if k == "medication_json" and v is not None and not isinstance(v, str):
            v = json.dumps(v)
        if k == "hr_involved_contractor_ids" and isinstance(v, list):
            v = json.dumps(v)
        sets.append(f"{k} = %s")
        vals.append(v)
    if not sets:
        return
    vals.append(int(incident_id))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        sql = f"UPDATE incidents SET {', '.join(sets)} WHERE id = %s"
        cur.execute(sql, tuple(vals))
        _audit(cur, int(incident_id), "update_fields", actor_label, {"keys": list(fields.keys())})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def link_safeguarding(
    incident_id: int,
    referral_id: int,
    public_id: str,
    actor_label: str,
) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE incidents SET linked_safeguarding_referral_id = %s,
              linked_safeguarding_public_id = %s WHERE id = %s
            """,
            (int(referral_id), public_id[:64], int(incident_id)),
        )
        _audit(
            cur,
            int(incident_id),
            "link_safeguarding",
            actor_label,
            {"referral_id": referral_id, "public_id": public_id},
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def portal_create_safeguarding_and_link(
    *,
    incident_public_uuid: str,
    contractor_username: str,
    payload: Dict[str, Any],
    operational_event_id: Optional[int] = None,
) -> Tuple[bool, str]:
    row = get_incident_by_uuid(incident_public_uuid)
    if not row:
        return False, "Incident not found"
    if int(row.get("reporter_contractor_id") or 0) <= 0:
        return False, "Incident has no portal reporter"
    rid, pub, err = create_safeguarding_referral_for_contractor(
        contractor_username=contractor_username,
        payload=payload,
        subject_type="safeguarding_from_incident",
        operational_event_id=operational_event_id,
        idempotency_key=f"incident:{row['public_uuid']}:sg",
    )
    if err or not rid or not pub:
        return False, err or "Create failed"
    link_safeguarding(int(row["id"]), rid, pub, contractor_username)
    return True, pub


def apply_routing_sla(cur, incident_id: int, category_slug: Optional[str], org_severity: Optional[str]) -> None:
    cat = (category_slug or "").strip()
    cur.execute(
        """
        SELECT sla_hours FROM incident_routing_rules
        WHERE active = 1 AND (category_slug IS NULL OR category_slug = %s)
        ORDER BY priority_order ASC, id ASC
        LIMIT 1
        """,
        (cat,),
    )
    r = cur.fetchone()
    sla = int(r[0]) if r and r[0] is not None else None
    if not sla:
        cur.execute(
            """
            SELECT sla_hours FROM incident_routing_rules
            WHERE active = 1 AND org_severity_codes IS NOT NULL
              AND FIND_IN_SET(%s, org_severity_codes)
            ORDER BY priority_order ASC, id ASC
            LIMIT 1
            """,
            ((org_severity or "").strip(),),
        )
        r2 = cur.fetchone()
        if r2 and r2[0] is not None:
            sla = int(r2[0])
    if sla:
        due = datetime.utcnow() + timedelta(hours=sla)
        cur.execute(
            "UPDATE incidents SET sla_due_at = %s WHERE id = %s",
            (due.strftime("%Y-%m-%d %H:%M:%S"), int(incident_id)),
        )


def change_status(
    incident_id: int,
    to_status: str,
    actor_label: str,
    *,
    force: bool = False,
    note: Optional[str] = None,
    actor_user_id: Optional[str] = None,
) -> Tuple[bool, str]:
    to_status = (to_status or "").strip()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM incidents WHERE id = %s FOR UPDATE", (int(incident_id),))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return False, "Not found"
        fs = (row.get("status") or "").strip()
        if int(row.get("legal_hold") or 0) and to_status != "legal_hold" and not force:
            conn.rollback()
            return False, "Legal hold is active"
        if not transition_allowed(fs, to_status) and not force:
            conn.rollback()
            return False, f"Cannot move from {fs} to {to_status}"
        if to_status == "submitted":
            if int(row.get("safeguarding_required") or 0) and not (
                row.get("linked_safeguarding_referral_id")
            ):
                conn.rollback()
                return (
                    False,
                    "Safeguarding referral is required before submit — complete the Cura referral step.",
                )
            ref = row.get("reference_code")
            if not ref:
                ref = next_reference_code(cur)
                cur.execute(
                    "UPDATE incidents SET reference_code = %s WHERE id = %s",
                    (ref, int(incident_id)),
                )
            cur.execute(
                "UPDATE incidents SET submitted_at = COALESCE(submitted_at, UTC_TIMESTAMP()) WHERE id = %s",
                (int(incident_id),),
            )
            apply_routing_sla(
                cur,
                int(incident_id),
                row.get("category_slug"),
                row.get("org_severity_code"),
            )
        if to_status == "closed":
            cur.execute(
                "UPDATE incidents SET closed_at = UTC_TIMESTAMP() WHERE id = %s",
                (int(incident_id),),
            )
        cur.execute(
            "UPDATE incidents SET status = %s WHERE id = %s",
            (to_status, int(incident_id)),
        )
        _append_status_history(cur, int(incident_id), fs, to_status, actor_label, note)
        _audit(
            cur,
            int(incident_id),
            "status_change",
            actor_label,
            {"from": fs, "to": to_status},
        )
        conn.commit()
        merged = {**dict(row), "status": to_status}
        try:
            ref = (merged.get("reference_code") or "").strip() or f"#{incident_id}"
            title = ((merged.get("title") or "") or "Incident")[:120]
            sbj = f"Incident update: {ref} — {title}"
            lines = [f"Status changed from {fs} to {to_status}."]
            if (note or "").strip():
                lines.append(f"Note: {note.strip()}")
            lines.append(
                f"Open in Sparrow: /plugin/incident_reporting_module/incidents/{int(incident_id)}"
            )
            notify_subscribers_for_incident(
                int(incident_id),
                "status_change",
                sbj,
                "\n".join(lines),
                exclude_user_id=actor_user_id,
            )
        except Exception as ex:
            logger.warning("subscriber notify: %s", ex)
        return True, "OK"
    except Exception as e:
        conn.rollback()
        logger.exception("change_status: %s", e)
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def merge_incidents(keep_id: int, merge_id: int, actor_label: str) -> Tuple[bool, str]:
    if keep_id == merge_id:
        return False, "Invalid merge"
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE incidents SET merged_into_id = %s, status = 'merged' WHERE id = %s",
            (int(keep_id), int(merge_id)),
        )
        _audit(
            cur,
            int(merge_id),
            "merged_into",
            actor_label,
            {"keep_id": int(keep_id)},
        )
        _audit(cur, int(keep_id), "absorbed_merge", actor_label, {"merged_id": int(merge_id)})
        conn.commit()
        return True, "OK"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def list_comments(incident_id: int) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT * FROM incident_comments WHERE incident_id = %s ORDER BY created_at ASC
            """,
            (int(incident_id),),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def add_comment(
    incident_id: int,
    body: str,
    author_label: str,
    author_user_id: Optional[str] = None,
    author_contractor_id: Optional[int] = None,
) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO incident_comments (incident_id, body, author_label, author_user_id, author_contractor_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                int(incident_id),
                body,
                author_label[:255],
                author_user_id,
                author_contractor_id,
            ),
        )
        _audit(cur, int(incident_id), "comment", author_label, {})
        conn.commit()
        try:
            row = get_incident(incident_id)
            ref = (
                ((row or {}).get("reference_code") or "").strip() or f"#{incident_id}"
            )
            title = (((row or {}).get("title") or "") or "Incident")[:120]
            excerpt = (body or "").strip().replace("\r\n", "\n")
            if len(excerpt) > 400:
                excerpt = excerpt[:397] + "..."
            sbj = f"New comment on {ref} — {title}"
            msg = f"{author_label} wrote:\n{excerpt}\n\nOpen: /plugin/incident_reporting_module/incidents/{int(incident_id)}"
            notify_subscribers_for_incident(
                int(incident_id),
                "comment",
                sbj,
                msg,
                exclude_user_id=author_user_id,
            )
        except Exception as ex:
            logger.warning("subscriber notify (comment): %s", ex)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def list_actions(incident_id: int) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT * FROM incident_actions WHERE incident_id = %s ORDER BY id ASC",
            (int(incident_id),),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def upsert_action(
    incident_id: int,
    data: Dict[str, Any],
    actor_label: str,
    action_id: Optional[int] = None,
    *,
    actor_user_id: Optional[str] = None,
) -> int:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if action_id:
            cur.execute(
                """
                UPDATE incident_actions SET title=%s, description=%s, owner_label=%s, due_date=%s, status=%s,
                  effectiveness_review=%s WHERE id=%s AND incident_id=%s
                """,
                (
                    data.get("title"),
                    data.get("description"),
                    data.get("owner_label"),
                    data.get("due_date"),
                    data.get("status") or "open",
                    data.get("effectiveness_review"),
                    int(action_id),
                    int(incident_id),
                ),
            )
            iid = int(action_id)
        else:
            cur.execute(
                """
                INSERT INTO incident_actions (incident_id, title, description, owner_label, due_date, status)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(incident_id),
                    data.get("title") or "Action",
                    data.get("description"),
                    data.get("owner_label"),
                    data.get("due_date"),
                    data.get("status") or "open",
                ),
            )
            iid = int(cur.lastrowid)
        _audit(cur, int(incident_id), "action_upsert", actor_label, {"action_id": iid})
        conn.commit()
        try:
            row = get_incident(incident_id)
            ref = (
                ((row or {}).get("reference_code") or "").strip() or f"#{incident_id}"
            )
            title = (((row or {}).get("title") or "") or "Incident")[:120]
            atitle = (data.get("title") or "Action")[:200]
            sbj = f"CAPA update on {ref} — {atitle}"
            msg = (
                f"{actor_label} saved CAPA action #{iid}: {atitle}\n"
                f"Open: /plugin/incident_reporting_module/incidents/{int(incident_id)}"
            )
            notify_subscribers_for_incident(
                int(incident_id),
                "action",
                sbj,
                msg,
                exclude_user_id=actor_user_id,
            )
        except Exception as ex:
            logger.warning("subscriber notify (action): %s", ex)
        return iid
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def list_timeline(incident_id: int) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT * FROM incident_timeline_events WHERE incident_id = %s
            ORDER BY event_time ASC, sort_order ASC, id ASC
            """,
            (int(incident_id),),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def add_timeline_event(incident_id: int, data: Dict[str, Any], actor_label: str) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO incident_timeline_events (incident_id, event_time, label, body, actor_label, sort_order)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (
                int(incident_id),
                data.get("event_time") or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                (data.get("label") or "Event")[:255],
                data.get("body"),
                actor_label[:255],
                int(data.get("sort_order") or 0),
            ),
        )
        _audit(cur, int(incident_id), "timeline_add", actor_label, {})
        conn.commit()
    finally:
        cur.close()
        conn.close()


def list_factor_definitions(pack: Optional[str] = None) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if pack:
            cur.execute(
                "SELECT * FROM incident_factor_definitions WHERE active = 1 AND pack = %s ORDER BY sort_order, id",
                (pack,),
            )
        else:
            cur.execute(
                "SELECT * FROM incident_factor_definitions WHERE active = 1 ORDER BY pack, sort_order, id"
            )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def list_selected_factors(incident_id: int) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT * FROM incident_incident_factors WHERE incident_id = %s",
            (int(incident_id),),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def set_incident_factors(incident_id: int, codes: Sequence[str], actor_label: str) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM incident_incident_factors WHERE incident_id = %s",
            (int(incident_id),),
        )
        for c in codes:
            cc = (c or "").strip()
            if not cc:
                continue
            cur.execute(
                "INSERT INTO incident_incident_factors (incident_id, factor_code) VALUES (%s,%s)",
                (int(incident_id), cc[:64]),
            )
        _audit(cur, int(incident_id), "factors_set", actor_label, {"codes": list(codes)})
        conn.commit()
    finally:
        cur.close()
        conn.close()


def list_audit(incident_id: int) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT * FROM incident_audit_log WHERE incident_id = %s ORDER BY id DESC LIMIT 500
            """,
            (int(incident_id),),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def list_attachments(incident_id: int) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT * FROM incident_attachments WHERE incident_id = %s ORDER BY id DESC",
            (int(incident_id),),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def add_attachment(
    incident_id: int, rel_path: str, file_name: str, uploaded_by_label: str
) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO incident_attachments (incident_id, file_path, file_name, uploaded_by_label)
            VALUES (%s,%s,%s,%s)
            """,
            (int(incident_id), rel_path[:512], file_name[:255], uploaded_by_label[:255]),
        )
        _audit(cur, int(incident_id), "attachment_add", uploaded_by_label, {"path": rel_path})
        conn.commit()
    finally:
        cur.close()
        conn.close()


def list_routing_rules() -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM incident_routing_rules ORDER BY priority_order, id")
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def upsert_routing_rule(data: Dict[str, Any], rule_id: Optional[int] = None) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if rule_id:
            cur.execute(
                """
                UPDATE incident_routing_rules SET name=%s, category_slug=%s, org_severity_codes=%s,
                  assignee_label=%s, sla_hours=%s, priority_order=%s, active=%s WHERE id=%s
                """,
                (
                    data.get("name"),
                    data.get("category_slug"),
                    data.get("org_severity_codes"),
                    data.get("assignee_label"),
                    data.get("sla_hours"),
                    int(data.get("priority_order") or 0),
                    1 if data.get("active") else 0,
                    int(rule_id),
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO incident_routing_rules
                  (name, category_slug, org_severity_codes, assignee_label, sla_hours, priority_order, active)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    data.get("name") or "Rule",
                    data.get("category_slug"),
                    data.get("org_severity_codes"),
                    data.get("assignee_label"),
                    data.get("sla_hours"),
                    int(data.get("priority_order") or 0),
                    1 if data.get("active") else 0,
                ),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def delete_routing_rule(rule_id: int) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM incident_routing_rules WHERE id = %s", (int(rule_id),))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def analytics_pareto(
    limit: int = 15,
    narrow: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    ns, npar = _narrow_sql_clause(narrow, table_alias="i")
    extra = f" AND ({ns})" if ns else ""
    lim = min(max(limit, 3), 50)
    try:
        cur.execute(
            """
            SELECT f.factor_code AS code, d.label, COUNT(*) AS c
            FROM incident_incident_factors f
            LEFT JOIN incident_factor_definitions d ON d.code = f.factor_code
            JOIN incidents i ON i.id = f.incident_id AND i.merged_into_id IS NULL
            """
            + extra
            + """
            GROUP BY f.factor_code, d.label
            ORDER BY c DESC
            LIMIT %s
            """,
            tuple(npar) + (lim,),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def analytics_time_to_close(
    days: int = 90,
    narrow: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    conn = get_db_connection()
    cur = conn.cursor()
    ns, npar = _narrow_sql_clause(narrow)
    extra = f" AND ({ns})" if ns else ""
    try:
        cur.execute(
            """
            SELECT AVG(TIMESTAMPDIFF(HOUR, submitted_at, closed_at)) AS avg_h,
                   MIN(TIMESTAMPDIFF(HOUR, submitted_at, closed_at)) AS min_h,
                   MAX(TIMESTAMPDIFF(HOUR, submitted_at, closed_at)) AS max_h,
                   COUNT(*) AS n
            FROM incidents
            WHERE merged_into_id IS NULL AND status = 'closed'
              AND submitted_at IS NOT NULL AND closed_at IS NOT NULL
              AND closed_at >= UTC_TIMESTAMP() - INTERVAL %s DAY
            """
            + extra,
            (int(days),) + tuple(npar),
        )
        r = cur.fetchone()
        if not r:
            return {"avg_hours": None, "min_hours": None, "max_hours": None, "n": 0}
        return {
            "avg_hours": float(r[0]) if r[0] is not None else None,
            "min_hours": float(r[1]) if r[1] is not None else None,
            "max_hours": float(r[2]) if r[2] is not None else None,
            "n": int(r[3] or 0),
        }
    finally:
        cur.close()
        conn.close()


def export_incidents_csv(filters: Dict[str, Any]) -> bytes:
    rows = admin_list_incidents(
        status=filters.get("status"),
        category=filters.get("category"),
        mode=filters.get("mode"),
        q=filters.get("q"),
        review=filters.get("review"),
        limit=5000,
        narrow=filters.get("narrow"),
    )
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "id",
            "reference_code",
            "public_uuid",
            "status",
            "mode",
            "category",
            "title",
            "incident_occurred_at",
            "incident_discovered_at",
            "exact_location_detail",
            "riddor_notifiable",
            "people_affected_count",
            "created_at",
            "submitted_at",
            "closed_at",
        ]
    )
    for r in rows:
        w.writerow(
            [
                r.get("id"),
                r.get("reference_code"),
                r.get("public_uuid"),
                r.get("status"),
                r.get("incident_mode"),
                r.get("category_slug"),
                (r.get("title") or "").replace("\n", " ")[:500],
                r.get("incident_occurred_at"),
                r.get("incident_discovered_at"),
                (r.get("exact_location_detail") or "").replace("\n", " ")[:500],
                r.get("riddor_notifiable"),
                r.get("people_affected_count"),
                r.get("created_at"),
                r.get("submitted_at"),
                r.get("closed_at"),
            ]
        )
    return buf.getvalue().encode("utf-8", errors="replace")


def set_legal_hold(incident_id: int, on: bool, actor_label: str) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE incidents SET legal_hold = %s WHERE id = %s",
            (1 if on else 0, int(incident_id)),
        )
        _audit(cur, int(incident_id), "legal_hold", actor_label, {"on": bool(on)})
        conn.commit()
    finally:
        cur.close()
        conn.close()


def list_subscriptions(incident_id: int) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT user_id, events, created_at FROM incident_subscriptions
            WHERE incident_id = %s ORDER BY created_at ASC
            """,
            (int(incident_id),),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def user_is_subscribed(incident_id: int, user_id: str) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1 FROM incident_subscriptions
            WHERE incident_id = %s AND user_id = %s LIMIT 1
            """,
            (int(incident_id), str(user_id)),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def set_user_subscribed(
    incident_id: int,
    user_id: str,
    on: bool,
    *,
    events: str = "all",
    actor_label: str = "",
) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if on:
            cur.execute(
                """
                INSERT INTO incident_subscriptions (incident_id, user_id, events)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE events = VALUES(events)
                """,
                (int(incident_id), str(user_id), (events or "all")[:255]),
            )
        else:
            cur.execute(
                "DELETE FROM incident_subscriptions WHERE incident_id = %s AND user_id = %s",
                (int(incident_id), str(user_id)),
            )
        if actor_label:
            _audit(
                cur,
                int(incident_id),
                "subscription_toggle",
                actor_label,
                {"user_id": str(user_id), "on": bool(on)},
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def default_hse_walkaround_checklist() -> List[Dict[str, Any]]:
    """Starter checklist (HSE-style workplace inspection); tenants edit items in the admin UI."""
    return [
        {
            "id": "access_escape",
            "label": "Access routes and emergency exits clear and usable",
            "type": "bool",
            "required": True,
        },
        {
            "id": "housekeeping",
            "label": "Good housekeeping — slips/trips hazards controlled",
            "type": "bool",
            "required": True,
        },
        {
            "id": "work_equipment",
            "label": "Work equipment / guards / local exhaust appear suitable and in use",
            "type": "bool",
            "required": False,
        },
        {
            "id": "first_aid",
            "label": "First-aid arrangements visible / adequate for area",
            "type": "bool",
            "required": False,
        },
        {
            "id": "findings",
            "label": "Findings / follow-up notes (free text)",
            "type": "text",
            "required": False,
        },
    ]


def walkaround_checklist_form_display_rows(stored: Any, extra_blanks: int = 0) -> List[Dict[str, Any]]:
    """Saved checklist rows, optionally plus trailing blank row(s); prefer the Add-row control when ``extra_blanks`` is 0."""
    parsed = json.loads(_normalise_walkaround_checklist_json(stored))
    norm: List[Dict[str, Any]] = []
    for x in parsed:
        if not isinstance(x, dict):
            continue
        d = dict(x)
        if not str(d.get("id") or "").strip() and str(d.get("code") or "").strip():
            d["id"] = str(d.get("code")).strip()[:64]
        norm.append(d)
    blank = {"id": "", "label": "", "type": "bool", "required": False}
    n = max(0, int(extra_blanks or 0))
    return norm + [dict(blank) for _ in range(n)]


def _walkaround_stable_item_id(raw_id: str, label: str, row_index: int) -> str:
    rid = (raw_id or "").strip()
    if rid:
        s = re.sub(r"[^a-zA-Z0-9_-]+", "-", rid).strip("-")
        if s:
            return s[:64]
    base = re.sub(r"[^a-zA-Z0-9]+", "_", (label or "").lower()).strip("_")[:48]
    if base:
        return base[:64]
    return f"chk_{row_index}"


def walkaround_checklist_from_form_prefix(form: Any, prefix: str) -> str:
    """Build stored ``checklist_json`` from POST fields ``{prefix}label_{i}`` etc.

    Indices are discovered from submitted keys so sparse rows (after JS deletes) work.
    """
    pat = re.compile("^" + re.escape(prefix) + r"label_(\d+)$")
    idxs: Set[int] = set()
    for k in form.keys():
        m = pat.match(k)
        if m:
            idxs.add(int(m.group(1)))
    items: List[Dict[str, Any]] = []
    for i in sorted(idxs):
        label = (form.get(f"{prefix}label_{i}") or "").strip()
        if not label:
            continue
        typ = (form.get(f"{prefix}type_{i}") or "bool").strip().lower()
        if typ not in ("bool", "text"):
            typ = "bool"
        required = form.get(f"{prefix}required_{i}") == "1"
        raw_id = (form.get(f"{prefix}id_{i}") or "").strip()
        items.append(
            {
                "id": _walkaround_stable_item_id(raw_id, label, i),
                "label": label[:512],
                "type": typ,
                "required": bool(required),
            }
        )
    seen: Set[str] = set()
    for it in items:
        bid = str(it.get("id") or "item")
        base = bid
        n = 0
        while bid in seen:
            n += 1
            bid = f"{base}_{n}"
        seen.add(bid)
        it["id"] = bid[:64]
    if not items:
        return json.dumps(default_hse_walkaround_checklist())
    return json.dumps(items)


def _normalise_walkaround_checklist_json(raw: Any) -> str:
    if raw is None:
        return json.dumps(default_hse_walkaround_checklist())
    if isinstance(raw, (list, dict)):
        return json.dumps(raw if isinstance(raw, list) else [raw])
    s = str(raw).strip()
    if not s:
        return json.dumps(default_hse_walkaround_checklist())
    try:
        parsed = json.loads(s)
        if isinstance(parsed, dict):
            parsed = [parsed]
        if not isinstance(parsed, list):
            return json.dumps(default_hse_walkaround_checklist())
        return json.dumps(parsed)
    except json.JSONDecodeError:
        return json.dumps(default_hse_walkaround_checklist())


def _ensure_pending_walkaround_record(cur, template_id: int) -> None:
    cur.execute(
        """
        SELECT COUNT(*) FROM hse_walkaround_records
        WHERE template_id = %s AND status = 'pending'
        """,
        (int(template_id),),
    )
    n = int((cur.fetchone() or [0])[0] or 0)
    if n > 0:
        return
    cur.execute(
        """
        SELECT interval_days, site_label, active FROM hse_walkaround_templates
        WHERE id = %s LIMIT 1
        """,
        (int(template_id),),
    )
    row = cur.fetchone()
    if not row or not int(row[2] or 0):
        return
    interval = max(1, int(row[0] or 7))
    site = row[1]
    due = (datetime.utcnow().date() + timedelta(days=interval)).strftime("%Y-%m-%d")
    cur.execute(
        """
        INSERT INTO hse_walkaround_records (template_id, due_at, status, site_label)
        VALUES (%s, %s, 'pending', %s)
        """,
        (int(template_id), due, site),
    )


def list_walkaround_templates(active_only: bool = False) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        q = "SELECT * FROM hse_walkaround_templates"
        if active_only:
            q += " WHERE active = 1"
        q += " ORDER BY name ASC"
        cur.execute(q)
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def get_walkaround_template(template_id: int) -> Optional[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT * FROM hse_walkaround_templates WHERE id = %s LIMIT 1",
            (int(template_id),),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def upsert_walkaround_template(
    template_id: Optional[int],
    data: Dict[str, Any],
    actor_label: str,
) -> int:
    name = (data.get("name") or "").strip()[:255] or "Walkaround"
    desc = data.get("description")
    site = (data.get("site_label") or "").strip()[:255] or None
    interval_days = max(1, int(data.get("interval_days") or 7))
    active = 1 if str(data.get("active") or "").lower() in ("1", "true", "yes", "on") else 0
    checklist = _normalise_walkaround_checklist_json(data.get("checklist_json"))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if template_id:
            cur.execute(
                """
                UPDATE hse_walkaround_templates SET
                  name=%s, description=%s, site_label=%s, checklist_json=%s,
                  interval_days=%s, active=%s WHERE id=%s
                """,
                (name, desc, site, checklist, interval_days, active, int(template_id)),
            )
            tid = int(template_id)
        else:
            cur.execute(
                """
                INSERT INTO hse_walkaround_templates
                  (name, description, site_label, checklist_json, interval_days, active)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (name, desc, site, checklist, interval_days, active),
            )
            tid = int(cur.lastrowid)
        if active:
            _ensure_pending_walkaround_record(cur, tid)
        conn.commit()
        logger.info("walkaround template saved id=%s by %s", tid, actor_label[:80])
        return tid
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def delete_walkaround_template(template_id: int) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM hse_walkaround_templates WHERE id = %s", (int(template_id),)
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def list_walkaround_records(
    *,
    template_id: Optional[int] = None,
    status: Optional[str] = None,
    overdue_pending_only: bool = False,
    limit: int = 200,
) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        lim = min(max(int(limit), 1), 500)
        where = ["1=1"]
        params: List[Any] = []
        if template_id is not None:
            where.append("r.template_id = %s")
            params.append(int(template_id))
        if status:
            where.append("r.status = %s")
            params.append(status.strip())
        if overdue_pending_only:
            where.append("r.status = 'pending' AND r.due_at < CURDATE()")
        w = " AND ".join(where)
        if (status or "").strip().lower() == "complete":
            order_sql = "r.completed_at DESC, r.id DESC"
        else:
            order_sql = "r.due_at ASC, r.id DESC"
        cur.execute(
            f"""
            SELECT r.*, t.name AS template_name, t.interval_days AS template_interval_days,
                   t.checklist_json AS template_checklist_json
            FROM hse_walkaround_records r
            JOIN hse_walkaround_templates t ON t.id = r.template_id
            WHERE {w}
            ORDER BY {order_sql}
            LIMIT {lim}
            """,
            tuple(params),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def walkaround_operational_dashboard() -> Dict[str, Any]:
    """Counts, next pending due per active template, and recent observation rows for the walkaround hub."""
    out: Dict[str, Any] = {
        "pending_total": 0,
        "overdue_pending": 0,
        "completed_total": 0,
        "template_next": [],
        "finding_rows": [],
    }
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT COUNT(*) AS c FROM hse_walkaround_records WHERE status = 'pending'"
        )
        out["pending_total"] = int((cur.fetchone() or {}).get("c") or 0)
        cur.execute(
            """
            SELECT COUNT(*) AS c FROM hse_walkaround_records
            WHERE status = 'pending' AND due_at < CURDATE()
            """
        )
        out["overdue_pending"] = int((cur.fetchone() or {}).get("c") or 0)
        cur.execute(
            "SELECT COUNT(*) AS c FROM hse_walkaround_records WHERE status = 'complete'"
        )
        out["completed_total"] = int((cur.fetchone() or {}).get("c") or 0)
        cur.execute(
            """
            SELECT t.id AS template_id, t.name AS template_name, t.site_label,
                   MIN(r.due_at) AS next_due,
                   SUM(CASE WHEN r.id IS NOT NULL AND r.due_at < CURDATE() THEN 1 ELSE 0 END) AS overdue_pending
            FROM hse_walkaround_templates t
            LEFT JOIN hse_walkaround_records r
              ON r.template_id = t.id AND r.status = 'pending'
            WHERE t.active = 1
            GROUP BY t.id, t.name, t.site_label
            ORDER BY t.name
            """
        )
        for row in cur.fetchall() or []:
            nd = row.get("next_due")
            row["_next_due_cmp"] = None
            if nd is not None:
                if isinstance(nd, datetime):
                    row["_next_due_cmp"] = nd.date()
                elif isinstance(nd, date):
                    row["_next_due_cmp"] = nd
                elif isinstance(nd, str) and len(str(nd)) >= 10:
                    try:
                        row["_next_due_cmp"] = date.fromisoformat(str(nd)[:10])
                    except ValueError:
                        row["_next_due_cmp"] = None
            out["template_next"].append(row)
        cur.execute(
            """
            SELECT r.id AS record_id, r.completed_at, r.findings_json, r.linked_incident_id,
                   i.reference_code AS linked_incident_ref,
                   t.name AS template_name
            FROM hse_walkaround_records r
            JOIN hse_walkaround_templates t ON t.id = r.template_id
            LEFT JOIN incidents i ON i.id = r.linked_incident_id AND i.merged_into_id IS NULL
            WHERE r.status = 'complete' AND r.findings_json IS NOT NULL
            ORDER BY r.completed_at DESC
            LIMIT 100
            """
        )
        fr: List[Dict[str, Any]] = []
        for r0 in cur.fetchall() or []:
            findings = _parse_walkaround_findings_json(r0.get("findings_json"))
            if not findings:
                continue
            rid = int(r0["record_id"])
            ref = walkaround_report_reference(rid)
            for f in findings:
                pt = (f.get("point") or "").strip()
                if not pt:
                    continue
                fr.append(
                    {
                        "record_id": rid,
                        "report_ref": ref,
                        "template_name": (r0.get("template_name") or "").strip(),
                        "completed_at": r0.get("completed_at"),
                        "point": pt[:4000],
                        "severity": (f.get("severity") or "low").strip(),
                        "rectify_by": (f.get("rectify_by") or "").strip() or None,
                        "guidance_ref": (f.get("guidance_ref") or "").strip() or None,
                        "linked_incident_id": r0.get("linked_incident_id"),
                        "linked_incident_ref": (r0.get("linked_incident_ref") or "").strip()
                        or None,
                    }
                )
        out["finding_rows"] = fr[:60]
    except Exception as e:
        logger.warning("walkaround_operational_dashboard: %s", e)
    finally:
        cur.close()
        conn.close()
    return out


def list_walkaround_attachments(record_id: int) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT * FROM hse_walkaround_attachments
            WHERE record_id = %s
            ORDER BY (finding_sort IS NOT NULL), finding_sort ASC, id ASC
            """,
            (int(record_id),),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def walkaround_checklist_human_rows(template_checklist: Any, answers: Any) -> List[Dict[str, str]]:
    """Turn checklist definition + saved answers into printable rows (plain language, no raw JSON)."""
    chk: List[Any]
    if isinstance(template_checklist, list):
        chk = template_checklist
    elif isinstance(template_checklist, str) and template_checklist.strip():
        try:
            v = json.loads(template_checklist)
            chk = v if isinstance(v, list) else []
        except json.JSONDecodeError:
            chk = []
    else:
        chk = []
    ans: Dict[str, Any] = {}
    if isinstance(answers, dict):
        ans = answers
    elif isinstance(answers, str) and answers.strip():
        try:
            v = json.loads(answers)
            ans = v if isinstance(v, dict) else {}
        except json.JSONDecodeError:
            ans = {}
    rows: List[Dict[str, str]] = []
    for item in chk:
        if not isinstance(item, dict):
            continue
        cid = str(item.get("id") or item.get("code") or "").strip()
        label = (item.get("label") or "Check").strip()[:512]
        qtype = (item.get("type") or "text").strip().lower()
        if not cid:
            continue
        raw = ans.get(cid)
        if qtype == "bool":
            if raw in (True, "true", "True", "1", 1, "yes", "on"):
                display = "Yes"
            elif raw in (False, "false", "False", "0", 0, "no", "off"):
                display = "No"
            else:
                display = "—"
        else:
            s = (str(raw).strip() if raw is not None else "")[:2000]
            display = s if s else "—"
        rows.append({"label": label, "answer": display})
    return rows


def walkaround_report_reference(record_id: int) -> str:
    """Stable human-facing reference (not an internal table id slug in prose)."""
    return f"WA-{int(record_id):05d}"


def _parse_walkaround_findings_json(raw: Any) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, str) and raw.strip():
        try:
            v = json.loads(raw)
            return [x for x in v if isinstance(x, dict)] if isinstance(v, list) else []
        except json.JSONDecodeError:
            return []
    return []


def get_walkaround_report_detail(record_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT r.*, t.name AS template_name, t.description AS template_description,
                   t.interval_days AS template_interval_days,
                   t.checklist_json AS template_checklist_json
            FROM hse_walkaround_records r
            JOIN hse_walkaround_templates t ON t.id = r.template_id
            WHERE r.id = %s LIMIT 1
            """,
            (int(record_id),),
        )
        row = cur.fetchone()
        if not row or (row.get("status") or "").lower() != "complete":
            return None
        row["findings_parsed"] = _parse_walkaround_findings_json(row.get("findings_json"))
        try:
            row["attachments"] = list_walkaround_attachments(int(record_id))
        except Exception as ex:
            logger.warning("walkaround attachments load: %s", ex)
            row["attachments"] = []
        row["checklist_human"] = walkaround_checklist_human_rows(
            row.get("template_checklist_json"),
            row.get("answers_json"),
        )
        row["report_reference"] = walkaround_report_reference(int(record_id))
        return row
    finally:
        cur.close()
        conn.close()


def complete_walkaround_record(
    record_id: int,
    *,
    answers: Optional[Dict[str, Any]],
    notes: Optional[str],
    actor_label: str,
    actor_user_id: Optional[str],
    findings: Optional[List[Dict[str, Any]]] = None,
    attachment_rows: Optional[Sequence[Tuple[Optional[int], str, str]]] = None,
    linked_incident_id: Optional[int] = None,
) -> Tuple[bool, str]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT r.id, r.template_id, r.due_at, r.status, r.site_label, r.notes,
                   r.linked_incident_id, t.interval_days AS tpl_interval
            FROM hse_walkaround_records r
            JOIN hse_walkaround_templates t ON t.id = r.template_id
            WHERE r.id = %s FOR UPDATE
            """,
            (int(record_id),),
        )
        rec = cur.fetchone()
        if not rec:
            conn.rollback()
            return False, "Record not found"
        if (rec.get("status") or "").lower() != "pending":
            conn.rollback()
            return False, "Already completed or not pending"
        interval = max(1, int(rec.get("tpl_interval") or 7))
        findings_json = json.dumps(findings or [])
        cur.execute(
            """
            UPDATE hse_walkaround_records SET
              status = 'complete',
              completed_at = UTC_TIMESTAMP(),
              completed_by_label = %s,
              completed_by_user_id = %s,
              answers_json = %s,
              findings_json = %s,
              notes = %s,
              linked_incident_id = %s
            WHERE id = %s
            """,
            (
                actor_label[:255],
                actor_user_id,
                json.dumps(answers or {}),
                findings_json,
                (notes or "")[:8000] or None,
                int(linked_incident_id) if linked_incident_id else None,
                int(record_id),
            ),
        )
        for fs, path, fn in attachment_rows or ():
            cur.execute(
                """
                INSERT INTO hse_walkaround_attachments (record_id, finding_sort, file_path, file_name)
                VALUES (%s, %s, %s, %s)
                """,
                (int(record_id), fs, path[:512], (fn or "")[:255]),
            )
        tpl_id = int(rec["template_id"])
        site = rec.get("site_label")
        next_due = (datetime.utcnow().date() + timedelta(days=interval)).strftime("%Y-%m-%d")
        cur.execute(
            """
            INSERT INTO hse_walkaround_records (template_id, due_at, status, site_label)
            VALUES (%s, %s, 'pending', %s)
            """,
            (tpl_id, next_due, site),
        )
        conn.commit()
        return True, "OK"
    except Exception as e:
        conn.rollback()
        logger.exception("complete_walkaround_record: %s", e)
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def portal_list_my(contractor_id: int, limit: int = 100) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, public_uuid, reference_code, incident_mode, category_slug, status, title, created_at
            FROM incidents
            WHERE reporter_contractor_id = %s AND merged_into_id IS NULL
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (int(contractor_id), min(max(limit, 1), 300)),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def safeguarding_badge_state(row: dict) -> str:
    if not int(row.get("safeguarding_required") or 0):
        return "not_required"
    if row.get("linked_safeguarding_referral_id"):
        return "linked"
    if (row.get("status") or "").lower() in ("draft", "pending_safeguarding"):
        return "pending"
    return "not_linked"


_INCIDENT_FORM_VISIBILITY_ELEVATED_ROLES = frozenset(
    {"admin", "superuser", "clinical_lead", "support_break_glass"}
)


def _normalize_json_role_list(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        items = list(raw)
    elif isinstance(raw, str):
        try:
            v = json.loads(raw)
            items = v if isinstance(v, list) else []
        except Exception:
            return []
    else:
        return []
    out: List[str] = []
    for x in items:
        s = (str(x) or "").strip().lower()
        if s:
            out.append(s)
    return sorted(set(out))


def user_role_category_definitions() -> List[dict]:
    """``user_role_categories`` from this plugin manifest (parent id → child categories)."""
    path = Path(__file__).resolve().parent / "manifest.json"
    try:
        with open(path, "r", encoding="utf-8") as fh:
            m = json.load(fh)
    except Exception:
        return []
    raw = m.get("user_role_categories")
    if not isinstance(raw, list):
        return []
    out: List[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        rid = (item.get("id") or "").strip()
        if not rid:
            continue
        roles_raw = item.get("roles")
        roles: List[str] = []
        if isinstance(roles_raw, list):
            for x in roles_raw:
                s = (str(x) or "").strip().lower()
                if s:
                    roles.append(s)
        try:
            sort = int(item.get("sort", 999))
        except (TypeError, ValueError):
            sort = 999
        parent = (item.get("parent") or "").strip() or None
        out.append(
            {
                "id": rid,
                "label": (item.get("label") or rid).strip(),
                "parent": parent,
                "sort": sort,
                "roles": sorted(set(roles)),
            }
        )
    out.sort(key=lambda d: (int(d.get("sort") or 999), (d.get("id") or "").lower()))
    return out


def _parse_visibility_slot(raw: Any) -> Tuple[List[str], List[str]]:
    """DB JSON → (direct user roles, category ids). Legacy: JSON array = roles only."""
    if raw is None:
        return [], []
    if isinstance(raw, (list, tuple)):
        return _normalize_json_role_list(raw), []
    if isinstance(raw, dict):
        r = _normalize_json_role_list(raw.get("roles"))
        cats: List[str] = []
        cr = raw.get("categories")
        if isinstance(cr, list):
            for x in cr:
                s = (str(x) or "").strip()
                if s:
                    cats.append(s)
        return r, sorted(set(cats))
    if isinstance(raw, str):
        try:
            v = json.loads(raw)
            return _parse_visibility_slot(v)
        except Exception:
            return [], []
    return [], []


def _descendant_category_ids(root_id: str, defs: Sequence[dict]) -> Set[str]:
    by_id = {(d.get("id") or "").strip(): d for d in defs if (d.get("id") or "").strip()}
    rid = (root_id or "").strip()
    if rid not in by_id:
        return set()
    out: Set[str] = {rid}
    changed = True
    while changed:
        changed = False
        for iid, d in by_id.items():
            p = (d.get("parent") or "").strip()
            if p in out and iid not in out:
                out.add(iid)
                changed = True
    return out


def _expand_category_ids_to_roles(
    cat_ids: Sequence[str], defs: Sequence[dict]
) -> Set[str]:
    roles: Set[str] = set()
    allowed_ids = {(d.get("id") or "").strip() for d in defs if (d.get("id") or "").strip()}
    for cid in cat_ids:
        cid = (cid or "").strip()
        if not cid or cid not in allowed_ids:
            continue
        for sub in _descendant_category_ids(cid, defs):
            item = next((x for x in defs if (x.get("id") or "").strip() == sub), None)
            if not item:
                continue
            for r in item.get("roles") or []:
                s = (str(r) or "").strip().lower()
                if s:
                    roles.add(s)
    return roles


def form_visibility_category_rows() -> List[dict]:
    """Rows for settings UI: ``id``, ``label``, ``depth`` (0 = root category)."""
    defs = user_role_category_definitions()
    by_id = {d["id"]: d for d in defs}

    def depth(iid: str) -> int:
        d = 0
        cur = (by_id.get(iid) or {}).get("parent")
        while cur:
            d += 1
            cur = (by_id.get(cur) or {}).get("parent")
        return d

    rows: List[dict] = []
    for d in defs:
        iid = d.get("id") or ""
        if not iid:
            continue
        rows.append({"id": iid, "label": d.get("label") or iid, "depth": depth(iid)})
    rows.sort(
        key=lambda r: (
            depth(r["id"]),
            int(by_id.get(r["id"], {}).get("sort") or 999),
            (r["id"] or "").lower(),
        )
    )
    return rows


def get_form_role_visibility() -> Dict[str, Any]:
    """Return IR1/HSE visibility: direct ``*_roles`` plus ``*_categories`` from manifest.

    When both role and category selections are empty for a form, every Sparrow role
    that already has module access may use that workspace area.
    """
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT ir1_roles_json, hse_roles_json FROM incident_form_role_visibility WHERE id = 1 LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return {
                "ir1_roles": [],
                "ir1_categories": [],
                "hse_roles": [],
                "hse_categories": [],
            }
        ir1_r, ir1_c = _parse_visibility_slot(row.get("ir1_roles_json"))
        hse_r, hse_c = _parse_visibility_slot(row.get("hse_roles_json"))
        return {
            "ir1_roles": ir1_r,
            "ir1_categories": ir1_c,
            "hse_roles": hse_r,
            "hse_categories": hse_c,
        }
    finally:
        cur.close()
        conn.close()


def set_form_role_visibility(
    ir1_roles: Sequence[str],
    hse_roles: Sequence[str],
    ir1_categories: Optional[Sequence[str]] = None,
    hse_categories: Optional[Sequence[str]] = None,
) -> None:
    allowed_roles = {x[0] for x in INCIDENT_FORM_VISIBILITY_ROLE_CHOICES}
    defs = user_role_category_definitions()
    allowed_cats = {(d.get("id") or "").strip() for d in defs if (d.get("id") or "").strip()}

    def norm_roles(seq: Sequence[str]) -> List[str]:
        return sorted(
            {
                (x or "").strip().lower()
                for x in seq
                if (x or "").strip().lower() in allowed_roles
            }
        )

    def norm_cats(seq: Optional[Sequence[str]]) -> List[str]:
        if not seq:
            return []
        return sorted(
            {
                (x or "").strip()
                for x in seq
                if (x or "").strip() in allowed_cats
            }
        )

    ir1_obj = {
        "roles": norm_roles(ir1_roles),
        "categories": norm_cats(ir1_categories),
    }
    hse_obj = {
        "roles": norm_roles(hse_roles),
        "categories": norm_cats(hse_categories),
    }
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO incident_form_role_visibility (id, ir1_roles_json, hse_roles_json)
            VALUES (1, %s, %s)
            ON DUPLICATE KEY UPDATE
            ir1_roles_json = VALUES(ir1_roles_json),
            hse_roles_json = VALUES(hse_roles_json)
            """,
            (json.dumps(ir1_obj), json.dumps(hse_obj)),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def role_may_see_incident_workspace_form(
    user_role: Optional[str],
    form: str,
    rules: Optional[Dict[str, Any]] = None,
) -> bool:
    """``form`` is ``ir1`` or ``hse``. Uses direct roles + expanded manifest categories."""
    ur = (user_role or "").strip().lower()
    if ur in _INCIDENT_FORM_VISIBILITY_ELEVATED_ROLES:
        return True
    r = rules if rules is not None else get_form_role_visibility()
    defs = user_role_category_definitions()
    if (form or "").strip().lower() == "ir1":
        dr = [x for x in (r.get("ir1_roles") or []) if x]
        dc = [x for x in (r.get("ir1_categories") or []) if x]
    else:
        dr = [x for x in (r.get("hse_roles") or []) if x]
        dc = [x for x in (r.get("hse_categories") or []) if x]
    if not dr and not dc:
        return True
    allowed = set(dr) | _expand_category_ids_to_roles(dc, defs)
    if not allowed:
        return False
    return ur in allowed
