"""
Anonymous operational-event statistics for Sparrow admin reporting (no patient identifiers in aggregates).

Files live under app/static/uploads/cura_operational_events/… — symlinked to Railway volume when configured.
"""
from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Any

from .cura_event_debrief import suppress_small_counts
from .cura_patient_trace import extract_presenting_snippet, extract_presenting_snomed_bundle
from .cura_util import safe_json

logger = logging.getLogger("medical_records_module.cura_ops_reporting")

_EPCR_OID_SQL = """
SELECT id, data, status, created_at, updated_at, closed_at
FROM cases
WHERE
  TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data, '$.operational_event_id')), '')) = %s
  OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data, '$.operationalEventId')), '')) = %s
ORDER BY updated_at DESC
LIMIT %s
"""


def _section_map(case_data: dict) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for sec in case_data.get("sections") or []:
        if not isinstance(sec, dict):
            continue
        n = sec.get("name")
        if isinstance(n, str) and n.strip():
            out[n.strip()] = sec.get("content") if isinstance(sec.get("content"), dict) else {}
    return out


def _norm_key(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return s[:200] if s else ""


# Display order for operational report charts (handover outcome bucketing).
_EPCR_DISPOSITION_DISPLAY_ORDER: tuple[str, ...] = (
    "Discharged on scene",
    "Discharged at medical tent",
    "Conveyed to hospital",
    "Admitted in medical treatment bay",
    "Other",
    "Not recorded",
)


def _bucket_handover_disposition(raw: str) -> str:
    """
    Map free-text clinical handover outcome into coarse disposition buckets for charts.
    Matching is keyword-based; uncategorised coded values fall under Other.
    """
    s = (raw or "").strip().lower()
    if not s:
        return "Not recorded"
    if any(
        k in s
        for k in (
            "treatment bay",
            "medical treatment bay",
            "medical bay",
            "admitted to bay",
            "mtb",
            "held in bay",
        )
    ):
        return "Admitted in medical treatment bay"
    if any(
        k in s
        for k in (
            "hospital",
            "conveyed",
            "convey ",
            "conveyance",
            "ambulance to",
            "999 to hospital",
            "emergency department",
            "a&e",
            " to ed",
            "to ed",
            "admitted to hospital",
        )
    ):
        return "Conveyed to hospital"
    if "tent" in s or "medical tent" in s:
        return "Discharged at medical tent"
    if any(
        k in s
        for k in (
            "scene",
            "on scene",
            "discharged on scene",
            " left scene",
            "walked away",
            "own transport",
            "self discharge",
        )
    ):
        return "Discharged on scene"
    return "Other"


def _age_band_from_dob(dob_raw: Any) -> str:
    if not dob_raw:
        return "unknown"
    s = str(dob_raw).strip()[:32]
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if not m:
        return "unknown"
    try:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        born = date(y, mo, d)
        today = date.today()
        age = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
        if age < 0:
            return "unknown"
        if age < 16:
            return "0-15"
        if age < 65:
            return "16-64"
        return "65+"
    except Exception:
        return "unknown"


def build_deep_analytics_for_operational_event(
    cur,
    event_id: int,
    *,
    scan_limit: int = 8000,
    min_cell: int = 0,
) -> dict[str, Any]:
    """
    Aggregate EPCR case JSON for one operational_event_id (anonymous histograms).
    min_cell: pass >0 to suppress small buckets (same semantics as debrief helpers).
    """
    ev_key = str(int(event_id))
    cur.execute(_EPCR_OID_SQL, (ev_key, ev_key, int(scan_limit)))
    rows = cur.fetchall() or []

    case_status = Counter()
    response_type = Counter()
    response_other = Counter()
    handover_outcome = Counter()
    receiving_hospital = Counter()
    receiving_ward = Counter()
    age_band = Counter()
    sex_hint = Counter()
    drugs_admin_bucket = Counter()
    has_billing_section = Counter()
    has_clinical_handover = Counter()
    section_names_present = Counter()
    created_by_day: dict[str, int] = defaultdict(int)
    created_by_hour_of_day: dict[str, int] = defaultdict(int)
    updated_by_hour: dict[str, int] = defaultdict(int)
    disposition_category = Counter()
    incident_module_hits = Counter()
    presenting_snippet_top = Counter()
    incident_response_meta_filled = Counter()
    primary_snomed = Counter()
    suspected_snomed = Counter()

    for _cid, raw_data, st, created_at, updated_at, _closed_at in rows:
        case_status[(st or "unknown").strip() or "unknown"] += 1

        data = safe_json(raw_data)
        if not isinstance(data, dict):
            continue
        sm = _section_map(data)

        for sec_name, content in sm.items():
            section_names_present[sec_name] += 1

        inc = sm.get("Incident Log") or {}
        incident = inc.get("incident") if isinstance(inc.get("incident"), dict) else {}
        rt = _norm_key(incident.get("responseType") or incident.get("response_type"))
        if rt:
            response_type[rt.lower()] += 1
        ro = _norm_key(incident.get("responseOther") or incident.get("response_other"))
        if ro:
            response_other[ro[:120]] += 1

        rtm = incident.get("responseTypeMeta") or incident.get("response_type_meta") or {}
        if isinstance(rtm, dict):
            for mk, mv in rtm.items():
                k = _norm_key(mk)[:80]
                if not k:
                    continue
                if _norm_key(mv):
                    incident_response_meta_filled[k] += 1

        ch = sm.get("Clinical Handover") or {}
        ho = ch.get("handoverData") if isinstance(ch.get("handoverData"), dict) else {}
        if ho:
            has_clinical_handover["yes"] += 1
            oc = _norm_key(ho.get("outcome"))
            if oc:
                handover_outcome[oc[:160]] += 1
                disposition_category[_bucket_handover_disposition(oc)] += 1
            else:
                disposition_category["Not recorded"] += 1
            rh = ho.get("receivingHospital") if isinstance(ho.get("receivingHospital"), dict) else {}
            h = _norm_key(rh.get("hospital"))
            if h:
                receiving_hospital[h[:200]] += 1
            w = _norm_key(rh.get("ward") or rh.get("otherWard"))
            if w:
                receiving_ward[w[:160]] += 1
        else:
            has_clinical_handover["no"] += 1
            disposition_category["Not recorded"] += 1

        pi = sm.get("PatientInfo") or {}
        pt = pi.get("ptInfo") if isinstance(pi.get("ptInfo"), dict) else {}
        age_band[_age_band_from_dob(pt.get("dob"))] += 1
        sx = _norm_key(pt.get("sex") or pt.get("gender"))
        if sx:
            sex_hint[sx.lower()[:40]] += 1

        drugs_sec = sm.get("Drugs Administered") or {}
        dlist = drugs_sec.get("drugsAdministered")
        n = len(dlist) if isinstance(dlist, list) else 0
        if n == 0:
            drugs_admin_bucket["0"] += 1
        elif n == 1:
            drugs_admin_bucket["1"] += 1
        elif n <= 5:
            drugs_admin_bucket["2-5"] += 1
        else:
            drugs_admin_bucket["6+"] += 1

        if "Billing" in sm:
            has_billing_section["yes"] += 1
        else:
            has_billing_section["no"] += 1

        for mod_key, labels in (
            ("falls", ("Falls in Older Adults",)),
            ("stroke", ("Stroke",)),
            ("rtc", ("RTC",)),
            ("oohca", ("OOHCA/ROLE", "OOHCA")),
            ("hypoglycaemia", ("Hypoglycaemia",)),
            ("femur", ("Fractured Neck Of Femur", "Femur")),
        ):
            if any(name in sm for name in labels):
                incident_module_hits[mod_key] += 1

        ps = extract_presenting_snippet(data, max_len=100)
        if ps:
            key = " ".join(ps.split())[:72].lower()
            if key:
                presenting_snippet_top[key] += 1

        snb = extract_presenting_snomed_bundle(data)
        pr = snb.get("primary") if isinstance(snb.get("primary"), dict) else None
        if isinstance(pr, dict) and pr.get("conceptId"):
            cid = str(pr["conceptId"]).strip()
            if cid:
                label = (pr.get("pt") or cid)[:180]
                primary_snomed[f"{label} [{cid}]"] += 1
        seen_sus: set[str] = set()
        for s in snb.get("suspected") or []:
            if isinstance(s, dict) and s.get("conceptId"):
                cid = str(s["conceptId"]).strip()
                if cid and cid not in seen_sus:
                    seen_sus.add(cid)
                    label = (s.get("pt") or cid)[:180]
                    suspected_snomed[f"{label} [{cid}]"] += 1

        if created_at and hasattr(created_at, "strftime"):
            created_by_day[created_at.strftime("%Y-%m-%d")] += 1
            created_by_hour_of_day[f"{int(created_at.hour):02d}:00"] += 1
        if updated_at and hasattr(updated_at, "strftime"):
            updated_by_hour[updated_at.strftime("%Y-%m-%d %H:00")] += 1

    def _finalize(ct: Counter) -> dict[str, Any]:
        d = {k: int(v) for k, v in ct.items()}
        if min_cell > 0:
            return suppress_small_counts(d, min_cell)
        return d

    top_hosp = receiving_hospital.most_common(25)
    top_out = handover_outcome.most_common(40)

    hour_series = {f"{h:02d}:00": int(created_by_hour_of_day.get(f"{h:02d}:00", 0)) for h in range(24)}

    disp_fin = _finalize(disposition_category)

    return {
        "epcr_cases_scanned": len(rows),
        "epcr_case_status": _finalize(case_status),
        "epcr_response_type": _finalize(response_type),
        "epcr_incident_response_meta_coverage": _finalize(incident_response_meta_filled),
        "epcr_response_other_top": _finalize(Counter({k: v for k, v in response_other.most_common(30)})),
        "epcr_handover_outcome_top": _finalize(Counter({k: v for k, v in top_out})),
        "epcr_receiving_hospital_top": _finalize(Counter({k: v for k, v in top_hosp})),
        "epcr_receiving_ward_top": _finalize(Counter({k: v for k, v in receiving_ward.most_common(25)})),
        "epcr_age_band": _finalize(age_band),
        "epcr_sex_hint": _finalize(sex_hint),
        "epcr_drugs_administered_count_bucket": _finalize(drugs_admin_bucket),
        "epcr_has_billing_section": _finalize(has_billing_section),
        "epcr_has_clinical_handover_content": _finalize(has_clinical_handover),
        "epcr_clinical_indicator_modules_present": _finalize(incident_module_hits),
        "epcr_section_tab_popularity": _finalize(section_names_present),
        "epcr_presenting_complaint_top": _finalize(
            Counter({k: v for k, v in presenting_snippet_top.most_common(20)})
        ),
        "epcr_presenting_snomed_primary": _finalize(
            Counter({k: v for k, v in primary_snomed.most_common(40)})
        ),
        "epcr_presenting_snomed_suspected": _finalize(
            Counter({k: v for k, v in suspected_snomed.most_common(40)})
        ),
        "epcr_cases_created_by_day": dict(sorted(created_by_day.items())),
        "epcr_cases_created_by_hour_of_day": hour_series,
        "epcr_cases_updated_by_hour_bucket": dict(sorted(updated_by_hour.items())[-168:]),
        "epcr_disposition_category": disp_fin,
        "epcr_disposition_category_order": list(_EPCR_DISPOSITION_DISPLAY_ORDER),
        "note": "Aggregates exclude free-text beyond short categorical keys; totals are case counts tied to operational_event_id on stored JSON. "
        "Charts use case created_at in the server timezone for hourly and daily views. "
        "epcr_incident_response_meta_coverage counts cases with a non-empty configured Incident Log meta value per key (e.g. runner number); values themselves are not listed here. "
        "epcr_presenting_snomed_* counts SNOMED concepts from the Presenting Complaint / History section (primary = one per case; suspected = one row per distinct concept per case).",
    }


def mi_anonymous_stats_for_operational_event(cur, event_id: int, *, min_cell: int = 0) -> dict[str, Any]:
    """Minor injury reports for MI events whose config_json links to this operational period."""
    ev = str(int(event_id))
    mi_ids: list[int] = []
    try:
        cur.execute(
            """
            SELECT id FROM cura_mi_events
            WHERE
              TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operational_event_id')), '')) = %s
              OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operationalEventId')), '')) = %s
            """,
            (ev, ev),
        )
        mi_ids = [int(r[0]) for r in cur.fetchall() or []]
    except Exception as ex:
        logger.warning("mi_anonymous_stats: list mi events: %s", ex)
        return {"mi_events_linked": 0, "note": str(ex)[:200]}

    if not mi_ids:
        return {"mi_events_linked": 0, "mi_submitted_reports": 0}

    injury = Counter()
    outcome = Counter()
    body_region = Counter()
    submitted = 0
    report_rows = 0
    ph = ",".join(["%s"] * len(mi_ids))
    try:
        cur.execute(
            f"""
            SELECT payload_json, status FROM cura_mi_reports
            WHERE event_id IN ({ph})
            """,
            tuple(mi_ids),
        )
        for pj, st in cur.fetchall() or []:
            report_rows += 1
            if (st or "").lower() == "submitted":
                submitted += 1
            p = safe_json(pj) if pj else {}
            if not isinstance(p, dict):
                continue
            inj = _norm_key(p.get("injuryType") or p.get("injury_type"))
            if inj:
                injury[inj[:120]] += 1
            oc = _norm_key(p.get("outcome") or p.get("disposition"))
            if oc:
                outcome[oc[:120]] += 1
            br = _norm_key(p.get("bodyRegion") or p.get("body_region"))
            if br:
                body_region[br[:120]] += 1
    except Exception as ex:
        logger.warning("mi_anonymous_stats: scan reports: %s", ex)
        return {"mi_events_linked": len(mi_ids), "error": str(ex)[:200]}

    def fin(c: Counter) -> dict[str, Any]:
        d = dict(c)
        if min_cell > 0:
            return suppress_small_counts({k: int(v) for k, v in d.items()}, min_cell)
        return {k: int(v) for k, v in d.items()}

    return {
        "mi_events_linked": len(mi_ids),
        "mi_report_rows_scanned": report_rows,
        "mi_submitted_reports": submitted,
        "mi_injury_type": fin(injury),
        "mi_outcome": fin(outcome),
        "mi_body_region": fin(body_region),
    }


def merge_pcr_sg_sql_counts(cur, event_id: int, min_cell: int) -> dict[str, Any]:
    """Reuse SQL group-bys for patient contact + safeguarding."""
    cur.execute(
        """
        SELECT COALESCE(status, ''), COUNT(*) FROM cura_patient_contact_reports
        WHERE operational_event_id = %s GROUP BY status
        """,
        (event_id,),
    )
    pcr_raw = {row[0] or "unknown": row[1] for row in cur.fetchall() or []}
    cur.execute(
        """
        SELECT COALESCE(status, ''), COUNT(*) FROM cura_safeguarding_referrals
        WHERE operational_event_id = %s GROUP BY status
        """,
        (event_id,),
    )
    sg_raw = {row[0] or "unknown": row[1] for row in cur.fetchall() or []}
    pcr_sup = suppress_small_counts(pcr_raw, min_cell) if min_cell > 0 else pcr_raw
    sg_sup = suppress_small_counts(sg_raw, min_cell) if min_cell > 0 else sg_raw
    return {
        "patient_contact_reports_by_status": pcr_sup,
        "safeguarding_referrals_by_status": sg_sup,
        "totals": {
            "patient_contact_reports": sum(pcr_raw.values()),
            "safeguarding_referrals": sum(sg_raw.values()),
        },
    }


def _epcr_linked_predicate_sql() -> str:
    return """(
      TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data, '$.operational_event_id')), '')) NOT IN ('', 'null', '0')
      OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data, '$.operationalEventId')), '')) NOT IN ('', 'null', '0')
    )"""


def hub_yoy_paperwork_counts(cur, *, ref: date | None = None) -> dict[str, Any]:
    """
    Year-to-date vs same calendar window last year for paperwork tied to any operational event.
    Uses case `created_at` and PCR/SG `created_at`.
    """
    ref = ref or date.today()
    start_cy = datetime(ref.year, 1, 1)
    end_cy_excl = datetime.combine(ref + timedelta(days=1), datetime.min.time())
    start_py = datetime(ref.year - 1, 1, 1)
    try:
        py_ref = date(ref.year - 1, ref.month, ref.day)
    except ValueError:
        import calendar

        last_d = calendar.monthrange(ref.year - 1, ref.month)[1]
        py_ref = date(ref.year - 1, ref.month, last_d)
    end_py_excl = datetime.combine(py_ref + timedelta(days=1), datetime.min.time())

    pred = _epcr_linked_predicate_sql()

    def _one(sql: str, params: tuple) -> int:
        try:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        except Exception as ex:
            logger.warning("hub_yoy_paperwork_counts: %s", ex)
            return 0

    epcr_cy = _one(
        f"SELECT COUNT(*) FROM cases WHERE created_at >= %s AND created_at < %s AND {pred}",
        (start_cy, end_cy_excl),
    )
    epcr_py = _one(
        f"SELECT COUNT(*) FROM cases WHERE created_at >= %s AND created_at < %s AND {pred}",
        (start_py, end_py_excl),
    )

    pcr_cy = _one(
        """
        SELECT COUNT(*) FROM cura_patient_contact_reports
        WHERE operational_event_id IS NOT NULL
          AND created_at >= %s AND created_at < %s
        """,
        (start_cy, end_cy_excl),
    )
    pcr_py = _one(
        """
        SELECT COUNT(*) FROM cura_patient_contact_reports
        WHERE operational_event_id IS NOT NULL
          AND created_at >= %s AND created_at < %s
        """,
        (start_py, end_py_excl),
    )

    sg_cy = _one(
        """
        SELECT COUNT(*) FROM cura_safeguarding_referrals
        WHERE operational_event_id IS NOT NULL
          AND created_at >= %s AND created_at < %s
        """,
        (start_cy, end_cy_excl),
    )
    sg_py = _one(
        """
        SELECT COUNT(*) FROM cura_safeguarding_referrals
        WHERE operational_event_id IS NOT NULL
          AND created_at >= %s AND created_at < %s
        """,
        (start_py, end_py_excl),
    )

    return {
        "ref_date": ref.isoformat(),
        "current_year": ref.year,
        "compare_year": ref.year - 1,
        "window_label": f"1 Jan–{ref.day} {ref.strftime('%b %Y')} vs same window in {ref.year - 1}",
        "epcr_cases": {"ytd": epcr_cy, "prior_year_window": epcr_py},
        "patient_contact": {"ytd": pcr_cy, "prior_year_window": pcr_py},
        "safeguarding": {"ytd": sg_cy, "prior_year_window": sg_py},
    }
