"""
Operational-period rollup payload shared by the Cura incident-report JSON API and the clinical ops hub debrief export.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable

from . import cura_patient_trace as cpt
from .cura_event_ventus_bridge import event_config_dict
from .cura_util import safe_json

logger = logging.getLogger("medical_records_module.cura_event_debrief")


def linked_mi_event_ids_for_operational_event(cur, operational_event_id: int) -> list[int]:
    """``cura_mi_events`` rows whose ``config_json`` points at this operational period (Cura app link)."""
    ev = str(int(operational_event_id))
    try:
        cur.execute(
            """
            SELECT id FROM cura_mi_events
            WHERE
              TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operational_event_id')), '')) = %s
              OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operationalEventId')), '')) = %s
            ORDER BY id
            """,
            (ev, ev),
        )
        return [int(r[0]) for r in cur.fetchall() or []]
    except Exception as ex:
        logger.warning("linked_mi_event_ids_for_operational_event: %s", ex)
        return []


def operational_status_to_mi_event_status(operational_status: str | None) -> str:
    """Map ``cura_operational_events.status`` to ``cura_mi_events.status`` (Cura client expects MI values)."""
    s = (operational_status or "draft").strip().lower()
    if s == "active":
        return "active"
    if s in ("closed", "archived"):
        return "closed"
    return "upcoming"


def ensure_mi_event_for_operational_period(
    cur,
    operational_event_id: int,
    *,
    name: str,
    location_summary: str | None,
    starts_at,
    ends_at,
    operational_status: str | None,
    actor: str | None,
) -> int | None:
    """
    If no ``cura_mi_events`` row is linked to this operational period via ``config_json``,
    insert one so Minor Injury and dispatch share the same operational id in config.

    The MI table keeps its own primary key; linkage is ``config_json.operational_event_id``.
    """
    oid = int(operational_event_id)
    try:
        existing = linked_mi_event_ids_for_operational_event(cur, oid)
        if existing:
            return int(existing[0])
        mi_status = operational_status_to_mi_event_status(operational_status)
        cfg = json.dumps({"operational_event_id": oid})
        act = (actor or "").strip() or None
        cur.execute(
            """
            INSERT INTO cura_mi_events (name, location_summary, starts_at, ends_at, status, config_json, created_by, updated_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                (name or f"Event {oid}")[:255],
                location_summary if location_summary else None,
                starts_at,
                ends_at,
                mi_status,
                cfg,
                act,
                act,
            ),
        )
        lid = cur.lastrowid
        if lid:
            try:
                from . import cura_mi_reference_cards as _mirfc

                _mirfc.ensure_default_reference_cards_for_event(cur, int(lid), actor=act)
            except Exception as _rex:
                logger.warning("ensure_mi_event reference cards id=%s: %s", lid, _rex)
        return int(lid) if lid else None
    except Exception as ex:
        logger.warning("ensure_mi_event_for_operational_period id=%s: %s", oid, ex)
        return None


def sync_linked_mi_events_from_operational_period(
    cur,
    operational_event_id: int,
    *,
    name: str,
    location_summary: str | None,
    starts_at,
    ends_at,
    operational_status: str | None,
    actor: str | None,
) -> int:
    """
    Update every ``cura_mi_events`` row linked to this operational id via ``config_json``.
    Authoritative fields mirror the operational period (name, window, mapped status).
    """
    oid = int(operational_event_id)
    ev = str(oid)
    mi_status = operational_status_to_mi_event_status(operational_status)
    act = (actor or "").strip() or None
    try:
        cur.execute(
            """
            UPDATE cura_mi_events
            SET name = %s,
                location_summary = %s,
                starts_at = %s,
                ends_at = %s,
                status = %s,
                updated_by = %s
            WHERE
              TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operational_event_id')), '')) = %s
              OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operationalEventId')), '')) = %s
            """,
            (
                (name or f"Event {oid}")[:255],
                location_summary if location_summary else None,
                starts_at,
                ends_at,
                mi_status,
                act,
                ev,
                ev,
            ),
        )
        return int(cur.rowcount or 0)
    except Exception as ex:
        logger.warning("sync_linked_mi_events_from_operational_period id=%s: %s", oid, ex)
        return -1


def push_operational_snapshot_to_mi_events(
    cur,
    operational_event_id: int,
    *,
    name: str,
    location_summary: str | None,
    starts_at,
    ends_at,
    operational_status: str | None,
    actor: str | None,
) -> None:
    """Create a linked MI shell if missing, then align all linked MI rows with the operational period."""
    oid = int(operational_event_id)
    ensure_mi_event_for_operational_period(
        cur,
        oid,
        name=name,
        location_summary=location_summary,
        starts_at=starts_at,
        ends_at=ends_at,
        operational_status=operational_status,
        actor=actor,
    )
    sync_linked_mi_events_from_operational_period(
        cur,
        oid,
        name=name,
        location_summary=location_summary,
        starts_at=starts_at,
        ends_at=ends_at,
        operational_status=operational_status,
        actor=actor,
    )


def _build_merged_mi_analytics_block(cur, mi_ids: list[int]) -> dict[str, Any] | None:
    """Aggregate submitted MI reports across one or more ``cura_mi_events`` ids."""
    if not mi_ids:
        return None

    injury_counts: dict[str, int] = {}
    outcome_counts: dict[str, int] = {}
    body_loc_counts: dict[str, int] = {}
    linked_meta: list[dict[str, Any]] = []

    for mid in mi_ids:
        cur.execute("SELECT id, name, status FROM cura_mi_events WHERE id = %s", (mid,))
        mir = cur.fetchone()
        if not mir:
            continue
        linked_meta.append({"mi_event_id": mir[0], "name": mir[1], "status": mir[2]})
        cur.execute(
            """
            SELECT payload_json FROM cura_mi_reports
            WHERE event_id = %s AND status = 'submitted'
            """,
            (mid,),
        )
        for (pj,) in cur.fetchall():
            p = safe_json(pj) if pj else {}
            if not isinstance(p, dict):
                continue
            it = (p.get("injuryType") or p.get("injury_type") or "unknown") or "unknown"
            oc = (p.get("outcome") or p.get("disposition") or "unknown") or "unknown"
            bl = (
                p.get("bodyLocation")
                or p.get("body_location")
                or p.get("bodyRegion")
                or p.get("body_region")
                or "unknown"
            ) or "unknown"
            injury_counts[it] = injury_counts.get(it, 0) + 1
            outcome_counts[oc] = outcome_counts.get(oc, 0) + 1
            body_loc_counts[bl] = body_loc_counts.get(bl, 0) + 1

    total = sum(injury_counts.values())
    if total == 0 and not linked_meta:
        return None

    def pct_map(d: dict[str, int]) -> list[dict[str, Any]]:
        denom = total if total > 0 else 1
        return [
            {"key": k, "count": v, "percentage": round(100.0 * v / denom, 1)}
            for k, v in sorted(d.items())
        ]

    out: dict[str, Any] = {
        "linked_mi_events": linked_meta,
        "injuryTypes": pct_map(injury_counts),
        "outcomes": pct_map(outcome_counts),
        "bodyLocations": pct_map(body_loc_counts),
        "submittedReportCount": total,
    }
    if len(mi_ids) == 1:
        out["mi_event_id"] = mi_ids[0]
    return out


def suppress_small_counts(counts: dict, min_cell: int) -> dict:
    if min_cell <= 0:
        return dict(counts)
    return {k: (None if v is not None and int(v) < min_cell else int(v)) for k, v in counts.items()}


def incident_response_fields_for_event_pack(case_data: dict) -> dict[str, Any]:
    """
    Pull response type + configured meta (e.g. runner/bib) from Incident Log for debrief / ops exports.
    Values are normalised to short strings; keys are stringified for JSON safety.
    """
    sm: dict[str, Any] = {}
    for sec in case_data.get("sections") or []:
        if not isinstance(sec, dict):
            continue
        n = sec.get("name")
        if isinstance(n, str) and n.strip():
            sm[n.strip()] = sec.get("content") if isinstance(sec.get("content"), dict) else {}
    inc = sm.get("Incident Log") or {}
    incident = inc.get("incident") if isinstance(inc.get("incident"), dict) else {}
    rt = incident.get("responseType") or incident.get("response_type") or ""
    meta = incident.get("responseTypeMeta") or incident.get("response_type_meta") or {}
    if not isinstance(meta, dict):
        meta = {}
    meta_out: dict[str, str] = {}
    for mk, mv in meta.items():
        ks = str(mk).strip()
        if not ks:
            continue
        meta_out[ks] = ("" if mv is None else str(mv).strip())[:200]
    rts = str(rt).strip()[:200] if rt else ""
    return {"responseType": rts, "responseTypeMeta": meta_out}


def _cases_updated_at_scan_window(starts_at, ends_at):
    """
    ``(lower, upper)`` bounds on ``cases.updated_at`` so MySQL sorts a bounded row set — not every row
    from a loose lower bound up to "now" (that pattern still exhausts sort buffer on large DBs).
    """
    from datetime import datetime, timedelta

    now = datetime.utcnow()
    oldest_allowed = now - timedelta(days=480)
    margin_before = timedelta(days=45)
    margin_after = timedelta(days=21)
    max_span = timedelta(days=95)

    if not starts_at and not ends_at:
        upper = now + timedelta(days=1)
        lower = now - timedelta(days=21)
        return max(lower, oldest_allowed), upper

    ref_lo = starts_at or ends_at
    ref_hi = ends_at or starts_at
    try:
        lower = ref_lo - margin_before
        upper = ref_hi + margin_after
    except Exception:
        upper = now + timedelta(days=1)
        lower = now - timedelta(days=21)
        return max(lower, oldest_allowed), upper

    if upper > now + timedelta(days=2):
        upper = now + timedelta(days=2)
    if lower < oldest_allowed:
        lower = oldest_allowed
    if lower >= upper:
        lower = upper - timedelta(days=1)

    span = upper - lower
    if span > max_span:
        mid = ref_hi or ref_lo
        try:
            half = timedelta(seconds=max_span.total_seconds() / 2.0)
            lower = mid - half
            upper = mid + half
        except Exception:
            lower = now - timedelta(days=30)
            upper = now + timedelta(days=1)
        if upper > now + timedelta(days=2):
            upper = now + timedelta(days=2)
        if lower < oldest_allowed:
            lower = oldest_allowed
        if lower >= upper:
            lower = upper - timedelta(days=1)
    return lower, upper


def build_operational_event_incident_body(
    cur,
    event_id: int,
    *,
    min_cell: int = 5,
    mi_event_id: int | None = None,
    may_include_case: Callable[[dict], bool],
    case_scan_limit: int = 1500,
    epcr_hits_limit: int = 200,
) -> dict | None:
    """
    Build the incident-report body (no schema_version). Returns None if the operational event row is missing.
    """
    cur.execute(
        """
        SELECT id, slug, name, location_summary, starts_at, ends_at, status, config,
               enforce_assignments, created_by, updated_by, created_at, updated_at
        FROM cura_operational_events WHERE id = %s
        """,
        (event_id,),
    )
    ev = cur.fetchone()
    if not ev:
        return None

    cur.execute(
        """
        SELECT COALESCE(status, ''), COUNT(*) FROM cura_patient_contact_reports
        WHERE operational_event_id = %s GROUP BY status
        """,
        (event_id,),
    )
    pcr_raw = {row[0] or "unknown": row[1] for row in cur.fetchall()}
    cur.execute(
        """
        SELECT COALESCE(status, ''), COUNT(*) FROM cura_safeguarding_referrals
        WHERE operational_event_id = %s GROUP BY status
        """,
        (event_id,),
    )
    sg_raw = {row[0] or "unknown": row[1] for row in cur.fetchall()}
    pcr_sup = suppress_small_counts(pcr_raw, min_cell)
    sg_sup = suppress_small_counts(sg_raw, min_cell)

    scan_limit = max(50, min(int(case_scan_limit), 1200))
    lower_updated, upper_updated = _cases_updated_at_scan_window(ev[4], ev[5])
    epcr_hits: list[dict[str, Any]] = []
    primary_snomed_counts: dict[str, int] = {}
    primary_snomed_labels: dict[str, str] = {}
    # Suspected: at most one count per concept per case (matches section dedupe).
    suspected_snomed_counts: dict[str, int] = {}
    suspected_snomed_labels: dict[str, str] = {}
    cases_with_suspected_snomed = 0
    epcr_scan_degraded = False
    epcr_skipped_by_env = (os.environ.get("CURA_INCIDENT_REPORT_SKIP_EPCR") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    case_rows: list[tuple[Any, ...]] = []
    if epcr_skipped_by_env:
        case_rows = []
    else:
        try:
            cur.execute(
                """
                SELECT id, data, status, updated_at
                FROM cases
                WHERE updated_at >= %s AND updated_at <= %s
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (lower_updated, upper_updated, scan_limit),
            )
            case_rows = list(cur.fetchall() or [])
        except Exception as ex:
            err = str(ex)
            if "1038" in err or "Out of sort memory" in err or "sort memory" in err.lower():
                logger.warning(
                    "build_operational_event_incident_body cases scan skipped (sort/memory): %s",
                    ex,
                )
            else:
                logger.exception("build_operational_event_incident_body cases scan: %s", ex)
            epcr_scan_degraded = True
            case_rows = []

    for cid, raw_data, st, upd in case_rows:
        data = safe_json(raw_data)
        if not isinstance(data, dict):
            continue
        oid = data.get("operationalEventId")
        if oid is None:
            oid = data.get("operational_event_id")
        try:
            if oid is None or int(oid) != int(event_id):
                continue
        except (TypeError, ValueError):
            continue
        if not may_include_case(data):
            continue
        cand = cpt.extract_ptinfo_from_case_payload(data)
        label = cpt._name_hint(cand.get("forename"), cand.get("surname")) or f"Case {cid}"
        ir = incident_response_fields_for_event_pack(data)
        snb = cpt.extract_presenting_snomed_bundle(data)
        pr = snb.get("primary") if isinstance(snb.get("primary"), dict) else None
        if isinstance(pr, dict) and pr.get("conceptId"):
            pcid = str(pr["conceptId"]).strip()
            if pcid:
                primary_snomed_counts[pcid] = primary_snomed_counts.get(pcid, 0) + 1
                primary_snomed_labels.setdefault(pcid, (pr.get("pt") or pcid)[:200])
        sus_list = snb.get("suspected") if isinstance(snb.get("suspected"), list) else []
        sus_clean = [x for x in sus_list if isinstance(x, dict)]
        seen_sus_case: set[str] = set()
        case_had_suspected = False
        for s in sus_clean:
            scid = str(s.get("conceptId") or "").strip()
            if not scid or scid in seen_sus_case:
                continue
            seen_sus_case.add(scid)
            case_had_suspected = True
            suspected_snomed_counts[scid] = suspected_snomed_counts.get(scid, 0) + 1
            suspected_snomed_labels.setdefault(scid, (str(s.get("pt") or "").strip() or scid)[:200])
        if case_had_suspected:
            cases_with_suspected_snomed += 1
        epcr_hits.append(
            {
                "caseId": cid,
                "status": st,
                "updatedAt": upd.isoformat() if upd and hasattr(upd, "isoformat") else None,
                "patientNameHint": label,
                "presentingComplaintSnippet": cpt.extract_presenting_snippet(data),
                "presentingSnomedPrimary": pr if isinstance(pr, dict) else None,
                "presentingSnomedSuspected": sus_clean,
                "responseType": ir.get("responseType") or "",
                "responseTypeMeta": ir.get("responseTypeMeta") if isinstance(ir.get("responseTypeMeta"), dict) else {},
            }
        )
    epcr_cases_matched = len(epcr_hits)
    epcr_hits = epcr_hits[:epcr_hits_limit]

    def _snomed_pct_rows(
        counts: dict[str, int], labels: dict[str, str], denom: int
    ) -> list[dict[str, Any]]:
        d = max(1, int(denom))
        rows: list[dict[str, Any]] = []
        for cid, n in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            rows.append(
                {
                    "conceptId": cid,
                    "pt": labels.get(cid, cid),
                    "count": int(n),
                    "percentage": round(100.0 * int(n) / d, 1),
                }
            )
        return rows

    epcr_presenting_snomed: dict[str, Any] = {
        "epcrCasesMatched": epcr_cases_matched,
        "casesWithPrimarySnomed": sum(primary_snomed_counts.values()),
        "casesWithAnySuspectedSnomed": cases_with_suspected_snomed,
        "primaryPresentations": _snomed_pct_rows(
            primary_snomed_counts, primary_snomed_labels, epcr_cases_matched
        ),
        "suspectedConditions": _snomed_pct_rows(
            suspected_snomed_counts, suspected_snomed_labels, epcr_cases_matched
        ),
    }

    mi_block = None
    if mi_event_id is not None:
        cur.execute("SELECT id, name, status FROM cura_mi_events WHERE id = %s", (mi_event_id,))
        mir = cur.fetchone()
        if mir:
            cur.execute(
                """
                SELECT payload_json FROM cura_mi_reports
                WHERE event_id = %s AND status = 'submitted'
                """,
                (mi_event_id,),
            )
            injury_counts: dict[str, int] = {}
            outcome_counts: dict[str, int] = {}
            body_loc_counts: dict[str, int] = {}
            for (pj,) in cur.fetchall():
                p = safe_json(pj) if pj else {}
                if not isinstance(p, dict):
                    continue
                it = (p.get("injuryType") or p.get("injury_type") or "unknown") or "unknown"
                oc = (p.get("outcome") or p.get("disposition") or "unknown") or "unknown"
                bl = (
                    (p.get("bodyLocation") or p.get("body_location") or p.get("bodyRegion") or p.get("body_region"))
                    or "unknown"
                ) or "unknown"
                injury_counts[it] = injury_counts.get(it, 0) + 1
                outcome_counts[oc] = outcome_counts.get(oc, 0) + 1
                body_loc_counts[bl] = body_loc_counts.get(bl, 0) + 1
            total = sum(injury_counts.values()) or 1

            def pct_map(d: dict[str, int]) -> list[dict[str, Any]]:
                return [
                    {"key": k, "count": v, "percentage": round(100.0 * v / total, 1)}
                    for k, v in sorted(d.items())
                ]

            mi_block = {
                "mi_event_id": mir[0],
                "name": mir[1],
                "status": mir[2],
                "injuryTypes": pct_map(injury_counts),
                "outcomes": pct_map(outcome_counts),
                "bodyLocations": pct_map(body_loc_counts),
                "submittedReportCount": total,
            }

    _cfg = event_config_dict(ev[7])
    return {
        "operational_event_id": event_id,
        "operational_event": {
            "id": ev[0],
            "slug": ev[1],
            "name": ev[2],
            "location_summary": ev[3],
            "starts_at": ev[4].isoformat() if ev[4] and hasattr(ev[4], "isoformat") else None,
            "ends_at": ev[5].isoformat() if ev[5] and hasattr(ev[5], "isoformat") else None,
            "status": ev[6],
            "enforce_assignments": bool(ev[8]),
            "ventus_division_slug": _cfg.get("ventus_division_slug"),
        },
        "min_cell": min_cell,
        "patient_contact_reports_by_status": pcr_sup,
        "safeguarding_referrals_by_status": sg_sup,
        "totals": {
            "patient_contact_reports": sum(pcr_raw.values()),
            "safeguarding_referrals": sum(sg_raw.values()),
            "epcr_cases_linked": len(epcr_hits),
            "epcr_cases_matched_before_limit": epcr_cases_matched,
        },
        "epcr_cases": epcr_hits,
        "epcr_presenting_snomed": epcr_presenting_snomed,
        "minor_injury": mi_block,
        "note": (
            "This summary is scoped to this operational deployment. "
            "Patient contacts and safeguarding counts apply to this period only. "
            "Clinical encounter lines are records linked to this deployment; published exports may list a subset. "
            "Ask your medical operations team if you need a full clinical record search or a different time window."
        ),
        "methodology_notes": (
            "Technical scope (for integrations): EPCR rows require case JSON with operationalEventId matching this event. "
            "Only cases updated in a bounded window around this operational period are scanned (keeps large databases stable). "
            "Optional query mi_event_id limits minor-injury rollup to one MI event."
            + (
                " EPCR case hints are disabled (CURA_INCIDENT_REPORT_SKIP_EPCR)."
                if epcr_skipped_by_env
                else (
                    " EPCR case hints were omitted this request because the database could not complete the scan safely."
                    if epcr_scan_degraded
                    else ""
                )
            )
        ),
    }


def fetch_event_assignments(cur, event_id: int) -> list[dict[str, Any]]:
    rows: list[tuple[Any, ...]] = []
    try:
        cur.execute(
            """
            SELECT id, principal_username, assigned_by, expected_callsign, created_at
            FROM cura_operational_event_assignments
            WHERE operational_event_id = %s
            ORDER BY id
            """,
            (event_id,),
        )
        rows = list(cur.fetchall() or [])
    except Exception as ex:
        if "expected_callsign" in str(ex) or "Unknown column" in str(ex):
            try:
                cur.execute(
                    """
                    SELECT id, principal_username, assigned_by, created_at
                    FROM cura_operational_event_assignments
                    WHERE operational_event_id = %s
                    ORDER BY id
                    """,
                    (event_id,),
                )
                rows = list(cur.fetchall() or [])
            except Exception as e2:
                logger.warning("fetch_event_assignments fallback: %s", e2)
                return []
        else:
            logger.warning("fetch_event_assignments: %s", ex)
            return []

    out: list[dict[str, Any]] = []
    if rows and len(rows[0]) == 5:
        for r in rows:
            ca = r[4]
            out.append(
                {
                    "id": r[0],
                    "principal_username": r[1] or "",
                    "assigned_by": r[2],
                    "expected_callsign": r[3],
                    "created_at": ca.isoformat() if ca and hasattr(ca, "isoformat") else ca,
                }
            )
    else:
        for r in rows:
            ca = r[3]
            out.append(
                {
                    "id": r[0],
                    "principal_username": r[1] or "",
                    "assigned_by": r[2],
                    "expected_callsign": None,
                    "created_at": ca.isoformat() if ca and hasattr(ca, "isoformat") else ca,
                }
            )
    return out


def fetch_event_callsign_validation_log(cur, event_id: int, limit: int = 250) -> list[dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT id, username, callsign, ok, reason_code, detail_json, created_at
            FROM cura_callsign_mdt_validation_log
            WHERE operational_event_id = %s
            ORDER BY id DESC
            LIMIT %s
            """,
            (event_id, limit),
        )
    except Exception as ex:
        logger.warning("fetch_event_callsign_validation_log: %s", ex)
        return []

    out: list[dict[str, Any]] = []
    for r in cur.fetchall() or []:
        dj = r[5]
        if hasattr(dj, "decode"):
            dj = safe_json(dj)
        elif isinstance(dj, (bytes, bytearray)):
            dj = safe_json(dj.decode("utf-8", errors="replace"))
        elif isinstance(dj, str):
            dj = safe_json(dj)
        ca = r[6]
        out.append(
            {
                "id": r[0],
                "username": r[1] or "",
                "callsign": r[2] or "",
                "ok": bool(r[3]),
                "reason_code": r[4] or "",
                "detail": dj if isinstance(dj, dict) else None,
                "created_at": ca.isoformat() if ca and hasattr(ca, "isoformat") else ca,
            }
        )
    return out
