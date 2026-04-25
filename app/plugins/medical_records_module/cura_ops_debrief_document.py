"""
Client-facing debrief exports (PDF tables, CSV) built from the same payload as the legacy JSON pack.
"""
from __future__ import annotations

import csv
import json
import logging
import os
import re
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

logger = logging.getLogger("medical_records_module.cura_ops_debrief_document")

_BAD_XML_RE = re.compile(r"[^\x09\x0a\x0d\x20-\ud7ff\ue000-\ufffd]")

# Omit from organiser-facing tables (still present in JSON API for engineers).
_CLIENT_HIDDEN_TOTAL_KEYS = frozenset({"epcr_cases_matched_before_limit", "min_cell"})

_CHART_PALETTE = ["#1565C0", "#2E7D32", "#ED6C02", "#6A1B9A", "#C62828", "#455A64"]


def format_debrief_datetime(value: Any) -> str:
    """Human-readable timestamps for medical debrief exports (no raw ISO microseconds)."""
    if value is None:
        return "—"
    s = str(value).strip()
    if not s:
        return "—"
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
            suf = " UTC"
        else:
            suf = ""
        return dt.strftime("%d %b %Y, %H:%M") + suf
    except Exception:
        return _short(s, 80) if len(s) > 80 else s


# Omit from client debrief tables (internal routing / URL key, not useful on PDF/CSV).
_DEBRIEF_HIDDEN_EVENT_KEYS = frozenset({"slug"})


def _format_operational_event_display(ev: dict[str, Any]) -> dict[str, Any]:
    out = {k: v for k, v in dict(ev).items() if k not in _DEBRIEF_HIDDEN_EVENT_KEYS}
    for k in ("starts_at", "ends_at", "created_at", "updated_at"):
        if k in out and out[k] is not None:
            out[k] = format_debrief_datetime(out.get(k))
    return out


def _client_facing_totals(totals: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(totals, dict):
        return {}
    return {k: v for k, v in totals.items() if k not in _CLIENT_HIDDEN_TOTAL_KEYS}


def _ventus_client_summary(vc: dict[str, Any] | None) -> str:
    if not isinstance(vc, dict):
        return ""
    if not vc.get("available"):
        return "Dispatch desk activity summary was not attached for this export (integration or time window)."
    note = (vc.get("note") or "").strip()
    low = note.lower()
    if "ventus_division_slug" in low or "mdt_jobs" in low or "division column" in low:
        return (
            "Dispatch desk linkage is available for this period. Job and message samples appear in the tables below."
        )
    if "no cad" in low or "not found" in low:
        return "No dispatch (CAD) jobs were matched to this period for the linked division."
    return note[:400] if note else "Dispatch desk samples are summarised in the tables below."


def _snomed_client_kv(esn: dict[str, Any]) -> list[list[str]]:
    """Replace engineer-oriented SNOMED field names with plain language."""
    if not isinstance(esn, dict):
        return []
    matched = esn.get("epcrCasesMatched")
    prim = esn.get("casesWithPrimarySnomed")
    sus = esn.get("casesWithAnySuspectedSnomed")
    rows = [
        ["Encounters included in this clinical summary", str(matched if matched is not None else "")],
        ["Of those, with a primary presenting SNOMED code recorded", str(prim if prim is not None else "")],
        ["Of those, with at least one suspected SNOMED condition recorded", str(sus if sus is not None else "")],
    ]
    return rows


def _status_chart_segments(
    labels: list[str], values: list[float]
) -> list[dict[str, Any]]:
    """Rows for HTML/CSS bar chart (width vs max in series)."""
    if not values:
        return []
    mx = max(float(v) for v in values if v is not None) or 1.0
    out: list[dict[str, Any]] = []
    for i, (lbl, val) in enumerate(zip(labels, values)):
        try:
            v = float(val) if val is not None else 0.0
        except (TypeError, ValueError):
            v = 0.0
        disp = int(v) if v == int(v) else round(v, 2)
        out.append(
            {
                "label": lbl,
                "value": disp,
                "bar_pct": min(100.0, round(100.0 * v / mx, 2)) if mx > 0 else 0.0,
                "color": _CHART_PALETTE[i % len(_CHART_PALETTE)],
            }
        )
    return out


# Explicit labels for pack keys so CSV/PDF read like a report, not API field names.
_DEBRIEF_FIELD_LABELS: dict[str, str] = {
    "id": "ID",
    "slug": "Slug",
    "name": "Name",
    "location_summary": "Location summary",
    "starts_at": "Starts",
    "ends_at": "Ends",
    "status": "Status",
    "enforce_assignments": "Enforce assignments",
    "ventus_division_slug": "Ventus division",
    "operational_event_id": "Operational event ID",
    "mi_event_id": "Minor injury event ID",
    "submittedReportCount": "Submitted reports",
    "injuryTypes": "Injury types",
    "outcomes": "Outcomes",
    "bodyLocations": "Body locations",
    "linked_mi_events": "Linked minor injury events",
    "detail": "Detail",
    "patient_contact_reports": "Patient contact reports",
    "safeguarding_referrals": "Safeguarding referrals",
    "epcr_cases_linked": "EPCR cases linked",
    "epcr_cases_matched_before_limit": "EPCR cases matched (before limit)",
    "epcrCasesMatched": "EPCR cases matched",
    "casesWithPrimarySnomed": "Cases with primary SNOMED",
    "casesWithAnySuspectedSnomed": "Cases with any suspected SNOMED",
    "primaryPresentations": "Primary presentations",
    "suspectedConditions": "Suspected conditions",
    "conceptId": "Concept ID",
    "caseId": "Case ID",
    "updatedAt": "Updated",
    "patientNameHint": "Patient name hint",
    "presentingComplaintSnippet": "Presenting complaint (snippet)",
    "presentingSnomedPrimary": "SNOMED (primary)",
    "presentingSnomedSuspected": "SNOMED (suspected)",
    "responseType": "Response type",
    "responseTypeMeta": "Response / event details",
    "principal_username": "Principal username",
    "expected_callsign": "Expected callsign",
    "assigned_by": "Assigned by",
    "created_at": "Created",
    "updated_at": "Updated",
    "username": "Username",
    "callsign": "Callsign",
    "ok": "OK",
    "reason_code": "Reason",
    "detail_json": "Detail",
    "message_type": "Message type",
    "sender_role": "Sender role",
    "sender_user": "Sender user",
    "message_text": "Message text",
    "job_comms_total": "Comms total",
    "min_cell": "Privacy minimum cell",
    "generated_at": "Generated at",
    "exported_by": "Exported by",
    "available": "Available",
    "note": "Note",
    "window_starts": "Window starts",
    "window_ends": "Window ends",
    "total_cads": "Total CADs",
    "comms_included": "Comms included",
    "comm_id": "Comm ID",
}


def humanize_debrief_field(key: str) -> str:
    k = str(key).strip()
    if not k:
        return ""
    if k in _DEBRIEF_FIELD_LABELS:
        return _DEBRIEF_FIELD_LABELS[k]
    if "_" in k:
        return " ".join(part.capitalize() for part in k.split("_"))
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", k)
    parts = spaced.split()
    out: list[str] = []
    for p in parts:
        low = p.lower()
        if low == "id":
            out.append("ID")
        elif low == "snomed":
            out.append("SNOMED")
        elif low == "epcr":
            out.append("EPCR")
        elif low == "api":
            out.append("API")
        elif low == "cad":
            out.append("CAD")
        elif low == "mi":
            out.append("MI")
        elif low == "ok":
            out.append("OK")
        else:
            out.append(p.capitalize())
    return " ".join(out) if out else k


def humanize_rollup_status(raw: Any) -> str:
    t = "" if raw is None else str(raw).strip()
    if not t:
        return "—"
    if "_" in t:
        return " ".join(w.capitalize() for w in t.split("_"))
    if t.islower() and len(t) > 1:
        return t.capitalize()
    return t


def _format_response_meta_pretty(meta: dict[str, Any] | None) -> str:
    if not isinstance(meta, dict) or not meta:
        return ""
    bits: list[str] = []
    for mk in sorted(meta.keys(), key=lambda x: str(x).lower()):
        mv = meta[mk]
        label = humanize_debrief_field(str(mk))
        if isinstance(mv, (dict, list)):
            bits.append(f"{label}: {_short(json.dumps(mv, ensure_ascii=True, default=str), 600)}")
        else:
            bits.append(f"{label}: {_short(mv, 400)}")
    return " · ".join(bits)


def _mi_rollup_rows(mi: dict[str, Any]) -> list[list[str]]:
    """Flatten minor injury rollup into label / value rows (no raw JSON blobs for list rollups)."""
    rows: list[list[str]] = []
    order = [
        "mi_event_id",
        "name",
        "status",
        "linked_mi_events",
        "submittedReportCount",
        "injuryTypes",
        "outcomes",
        "bodyLocations",
    ]
    seen = set(mi.keys())
    for k in order:
        if k not in mi:
            continue
        seen.discard(k)
        v = mi[k]
        label = humanize_debrief_field(k)
        if k == "linked_mi_events" and isinstance(v, list):
            rows.append([label, ""])
            for item in v:
                if not isinstance(item, dict):
                    continue
                nm = (item.get("name") or "").strip()
                mid = item.get("mi_event_id", "")
                st = (item.get("status") or "").strip()
                title = nm or (f"Event #{mid}" if mid != "" else "—")
                rows.append([f"  · {title}", st or "—"])
            rows.append(["", ""])
        elif k in ("injuryTypes", "outcomes", "bodyLocations") and isinstance(v, list):
            rows.append([label, ""])
            for item in v:
                if not isinstance(item, dict):
                    continue
                key = item.get("key", "")
                rows.append(
                    [
                        f"  · {humanize_rollup_status(key)}",
                        f"{item.get('count', '')} ({item.get('percentage', '')}%)",
                    ]
                )
            rows.append(["", ""])
        else:
            rows.append([label, str(v) if v is not None else ""])
    for k in sorted(seen, key=lambda x: str(x).lower()):
        v = mi[k]
        label = humanize_debrief_field(k)
        if isinstance(v, list):
            rows.append([label, _short(json.dumps(v, default=str), 8000)])
        else:
            rows.append([label, str(v) if v is not None else ""])
    while rows and rows[-1] == ["", ""]:
        rows.pop()
    return rows


def _xml_safe(s: str) -> str:
    if not s:
        return ""
    s = (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
    return _BAD_XML_RE.sub("", s)


def _short(s: Any, n: int = 200) -> str:
    t = "" if s is None else str(s).replace("\r\n", "\n").replace("\r", "\n")
    t = t.strip()
    if len(t) <= n:
        return t
    return t[: n - 1] + "…"


def _debrief_pdf_logo_src(branding: dict[str, Any] | None, app: Any | None = None) -> str | None:
    """
    Image URL for WeasyPrint cover logo. Prefer ``file:///…`` so resolution does not depend on ``base_url``.
    """
    branding = branding or {}
    lap = branding.get("logo_abs_path")
    if isinstance(lap, str) and lap.strip():
        pp = Path(lap.strip())
        if pp.is_file():
            try:
                return pp.resolve().as_uri()
            except (OSError, ValueError):
                pass
    lfu = (branding.get("logo_file_uri") or "").strip()
    if lfu:
        return lfu
    rel = (branding.get("logo_path") or "").strip().lstrip("/\\").replace("\\", "/")
    if rel.lower().startswith("static/"):
        rel = rel[7:].lstrip("/")
    if not rel:
        return None
    app_obj = app
    if app_obj is None:
        try:
            from flask import has_request_context, current_app

            if has_request_context():
                app_obj = current_app._get_current_object()
        except Exception:
            app_obj = None
    if app_obj is not None:
        try:
            from app.plugins.crm_module.crm_static_paths import crm_static_dir_for_app

            for root in (
                Path(crm_static_dir_for_app(app_obj)),
                Path(app_obj.static_folder or Path(app_obj.root_path) / "static"),
                Path(app_obj.root_path) / "static",
            ):
                try:
                    r = root.resolve()
                    cand = (r / rel).resolve()
                    cand.relative_to(r)
                    if cand.is_file():
                        return cand.as_uri()
                except Exception:
                    continue
        except Exception:
            pass
    return rel


def _resolve_logo_abs_path(branding: dict[str, Any] | None) -> str | None:
    """
    Absolute path to company logo on disk (for existence checks; PDF uses ``_debrief_pdf_logo_src``).
    ``get_site_branding()`` usually sets ``logo_abs_path``; this adds Flask static_folder
    fallback when path resolution failed (e.g. Windows path checks).
    """
    branding = branding or {}
    p = branding.get("logo_abs_path")
    if p and isinstance(p, str) and os.path.isfile(p):
        return p
    rel = (branding.get("logo_path") or "").strip().lstrip("/\\")
    if not rel:
        return None
    try:
        from flask import has_request_context, current_app

        if has_request_context():
            app = current_app
            static_root = Path(app.static_folder or (Path(app.root_path) / "static")).resolve()
            cand = (static_root / rel).resolve()
            cand.relative_to(static_root)
            if cand.is_file():
                return str(cand)
    except Exception:
        pass
    return None


def _dict_to_rows(d: dict[str, Any] | None, *, key_mode: str = "raw") -> list[list[str]]:
    """key_mode: raw | field (API → readable label) | status (rollup status codes → Title Case)."""
    if not d:
        return []
    out: list[list[str]] = []
    for k in sorted(d.keys(), key=lambda x: str(x).lower()):
        v = d[k]
        if key_mode == "field":
            left = humanize_debrief_field(str(k))
        elif key_mode == "status":
            left = humanize_rollup_status(str(k))
        else:
            left = str(k)
        out.append([left, "" if v is None else str(v)])
    return out


def build_debrief_pdf_context(
    pack: dict[str, Any],
    branding: dict[str, Any] | None,
    *,
    app: Any | None = None,
) -> dict[str, Any]:
    """Flatten debrief pack for Jinja + WeasyPrint (same content as CSV/JSON export)."""
    branding = branding or {}
    company_name = (branding.get("company_name") or "Medical operations").strip()
    logo_img_src = _debrief_pdf_logo_src(branding, app=app)

    dex = pack.get("debrief_export") if isinstance(pack.get("debrief_export"), dict) else {}
    ev = pack.get("operational_event") if isinstance(pack.get("operational_event"), dict) else {}
    event_name = (ev.get("name") or "Operational deployment").strip()
    event_location = (ev.get("location_summary") or "").strip()
    period_start = format_debrief_datetime(ev.get("starts_at"))
    period_end = format_debrief_datetime(ev.get("ends_at"))
    generated_at = format_debrief_datetime(dex.get("generated_at"))
    exported_by = (dex.get("exported_by") or "").strip()

    totc = _client_facing_totals(pack.get("totals") if isinstance(pack.get("totals"), dict) else {})
    try:
        pcr_n = int(totc.get("patient_contact_reports") or 0)
    except (TypeError, ValueError):
        pcr_n = 0
    try:
        sg_n = int(totc.get("safeguarding_referrals") or 0)
    except (TypeError, ValueError):
        sg_n = 0
    try:
        epcr_n = int(totc.get("epcr_cases_linked") or 0)
    except (TypeError, ValueError):
        epcr_n = 0

    summary_bullets = [
        f"Patient contact reports logged against this deployment: {pcr_n}.",
        f"Safeguarding referrals recorded: {sg_n}.",
        f"Clinical encounter summaries linked to this deployment (included in this export): {epcr_n}.",
    ]
    hc = pack.get("handover_context") if isinstance(pack.get("handover_context"), dict) else {}
    try:
        mi_n = int(hc.get("linked_minor_injury_events") or 0)
    except (TypeError, ValueError):
        mi_n = 0
    if mi_n > 0:
        summary_bullets.append(
            f"Minor injury reporting streams merged for this debrief: {mi_n} linked event(s)."
        )
    if hc.get("mi_export_filtered_to_single"):
        summary_bullets.append(
            "Note: this export was filtered to a single minor-injury event; totals may not represent the full deployment."
        )

    crm_ctx = None
    crm_raw = hc.get("crm_event_plan") if isinstance(hc.get("crm_event_plan"), dict) else None
    if crm_raw:
        crm_ctx = {
            "title": crm_raw.get("title") or "",
            "handoff_status": crm_raw.get("handoff_status") or "",
            "start_datetime": crm_raw.get("start_datetime"),
            "end_datetime": crm_raw.get("end_datetime"),
            "window_start": format_debrief_datetime(crm_raw.get("start_datetime")),
            "window_end": format_debrief_datetime(crm_raw.get("end_datetime")),
            "handoff_at": crm_raw.get("handoff_at"),
            "handoff_at_fmt": format_debrief_datetime(crm_raw.get("handoff_at")),
        }

    pcr_d = (
        pack.get("patient_contact_reports_by_status")
        if isinstance(pack.get("patient_contact_reports_by_status"), dict)
        else {}
    )
    sg_d = (
        pack.get("safeguarding_referrals_by_status")
        if isinstance(pack.get("safeguarding_referrals_by_status"), dict)
        else {}
    )
    pcr_labels: list[str] = []
    pcr_vals: list[float] = []
    for k, v in sorted(pcr_d.items(), key=lambda x: str(x[0]).lower()):
        try:
            n = float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            n = 0.0
        if n > 0 or len(pcr_d) <= 4:
            pcr_labels.append(humanize_rollup_status(str(k)))
            pcr_vals.append(n)
    sg_labels: list[str] = []
    sg_vals: list[float] = []
    for k, v in sorted(sg_d.items(), key=lambda x: str(x[0]).lower()):
        try:
            n = float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            n = 0.0
        if n > 0 or len(sg_d) <= 4:
            sg_labels.append(humanize_rollup_status(str(k)))
            sg_vals.append(n)

    pcr_segments = (
        _status_chart_segments(pcr_labels, pcr_vals)
        if pcr_vals and sum(pcr_vals) > 0
        else []
    )
    sg_segments = (
        _status_chart_segments(sg_labels, sg_vals) if sg_vals and sum(sg_vals) > 0 else []
    )

    kpi_max = max(float(pcr_n), float(sg_n), float(epcr_n), 1.0)
    kpi_bars = [
        {
            "label": "Patient contacts",
            "value": pcr_n,
            "width_pct": min(100.0, round(100.0 * float(pcr_n) / kpi_max, 2)),
        },
        {
            "label": "Safeguarding",
            "value": sg_n,
            "width_pct": min(100.0, round(100.0 * float(sg_n) / kpi_max, 2)),
        },
        {
            "label": "Clinical summaries",
            "value": epcr_n,
            "width_pct": min(100.0, round(100.0 * float(epcr_n) / kpi_max, 2)),
        },
    ]

    deployment_rows = _dict_to_rows(_format_operational_event_display(ev), key_mode="field")
    totals_rows = _dict_to_rows(totc, key_mode="field")
    pcr_status_rows = _dict_to_rows(pcr_d, key_mode="status")
    sg_status_rows = _dict_to_rows(sg_d, key_mode="status")

    mi = pack.get("minor_injury")
    mi_rows = (
        [[a, b] for a, b in _mi_rollup_rows(mi)] if isinstance(mi, dict) and mi else []
    )

    esn_pdf = (
        pack.get("epcr_presenting_snomed")
        if isinstance(pack.get("epcr_presenting_snomed"), dict)
        else {}
    )
    snomed_summary_rows = _snomed_client_kv(esn_pdf) if esn_pdf else []
    snomed_pp_rows: list[list[str]] = []
    snomed_sc_rows: list[list[str]] = []
    if esn_pdf:
        pp = esn_pdf.get("primaryPresentations") if isinstance(esn_pdf.get("primaryPresentations"), list) else []
        snomed_pp_rows = [
            [
                _short(r.get("conceptId"), 24),
                _short(r.get("pt"), 200),
                str(r.get("count", "")),
                str(r.get("percentage", "")),
            ]
            for r in pp
            if isinstance(r, dict)
        ]
        sc = esn_pdf.get("suspectedConditions") if isinstance(esn_pdf.get("suspectedConditions"), list) else []
        snomed_sc_rows = [
            [
                _short(r.get("conceptId"), 24),
                _short(r.get("pt"), 200),
                str(r.get("count", "")),
                str(r.get("percentage", "")),
            ]
            for r in sc
            if isinstance(r, dict)
        ]

    case_rows: list[list[str]] = []
    for c in pack.get("epcr_cases") if isinstance(pack.get("epcr_cases"), list) else []:
        if not isinstance(c, dict):
            continue
        meta = c.get("responseTypeMeta")
        meta_s = (
            _format_response_meta_pretty(meta)
            if isinstance(meta, dict)
            else ("" if meta is None else str(meta))
        )
        pr = c.get("presentingSnomedPrimary")
        pr_s = _short(json.dumps(pr, ensure_ascii=True), 120) if isinstance(pr, dict) else ""
        su = c.get("presentingSnomedSuspected")
        su_s = _short(json.dumps(su, ensure_ascii=True), 160) if isinstance(su, list) else ""
        case_rows.append(
            [
                str(c.get("caseId", "")),
                _short(c.get("status"), 40),
                format_debrief_datetime(c.get("updatedAt")),
                _short(c.get("responseType"), 36),
                _short(meta_s, 320),
                _short(c.get("patientNameHint"), 40),
                _short(c.get("presentingComplaintSnippet"), 160),
                pr_s,
                su_s,
            ]
        )

    roster_rows: list[list[str]] = []
    for a in pack.get("assignments") if isinstance(pack.get("assignments"), list) else []:
        if not isinstance(a, dict):
            continue
        roster_rows.append(
            [
                str(a.get("id", "")),
                _short(a.get("principal_username"), 64),
                _short(a.get("expected_callsign"), 24),
                _short(a.get("assigned_by"), 40),
                format_debrief_datetime(a.get("created_at")),
            ]
        )

    validation_rows: list[list[str]] = []
    for r in pack.get("callsign_validation_log") if isinstance(pack.get("callsign_validation_log"), list) else []:
        if not isinstance(r, dict):
            continue
        det = r.get("detail")
        det_s = json.dumps(det, default=str) if det is not None else ""
        validation_rows.append(
            [
                str(r.get("id", "")),
                _short(r.get("username"), 40),
                _short(r.get("callsign"), 20),
                "yes" if r.get("ok") else "no",
                _short(r.get("reason_code"), 40),
                _short(det_s, 200),
                format_debrief_datetime(r.get("created_at")),
            ]
        )

    vc = pack.get("ventus_cad_correlation") if isinstance(pack.get("ventus_cad_correlation"), dict) else {}
    dispatch_summary = _ventus_client_summary(vc)
    cad_job_rows: list[list[str]] = []
    for job in vc.get("cads") or []:
        if not isinstance(job, dict):
            continue
        cad_job_rows.append(
            [
                str(job.get("cad", "")),
                _short(job.get("status"), 32),
                _short(job.get("division"), 24),
                format_debrief_datetime(job.get("created_at")),
                format_debrief_datetime(job.get("updated_at")),
                str(job.get("job_comms_total", "")),
            ]
        )
    comm_rows: list[list[str]] = []
    for job in vc.get("cads") or []:
        if not isinstance(job, dict):
            continue
        cid = job.get("cad", "")
        for comm in job.get("job_comms_sample") or []:
            if not isinstance(comm, dict):
                continue
            comm_rows.append(
                [
                    str(cid),
                    str(comm.get("id", "")),
                    _short(comm.get("message_type"), 20),
                    _short(comm.get("sender_role"), 28),
                    _short(comm.get("sender_user"), 36),
                    format_debrief_datetime(comm.get("created_at")),
                    _short(comm.get("message_text"), 420),
                ]
            )

    return {
        "company_name": company_name,
        "logo_img_src": logo_img_src,
        "event_name": event_name,
        "event_location": event_location,
        "period_start": period_start,
        "period_end": period_end,
        "generated_at": generated_at,
        "exported_by": exported_by,
        "summary_intro": (
            "This report summarises activity recorded against the operational period: clinical encounters, "
            "governance contacts, roster checks, and dispatch samples where linked. It is intended for "
            "organisers, sponsors, and post-event review."
        ),
        "summary_bullets": summary_bullets,
        "crm": crm_ctx,
        "pre_charts_note": (
            "The following pages combine headline charts with detailed tables. "
            "Figures are drawn from operational systems at export time."
        ),
        "pcr_segments": pcr_segments,
        "sg_segments": sg_segments,
        "kpi_bars": kpi_bars,
        "deployment_rows": deployment_rows,
        "totals_rows": totals_rows,
        "pcr_status_rows": pcr_status_rows,
        "sg_status_rows": sg_status_rows,
        "mi_rows": mi_rows,
        "snomed_summary_rows": snomed_summary_rows,
        "snomed_pp_rows": snomed_pp_rows,
        "snomed_sc_rows": snomed_sc_rows,
        "case_rows": case_rows,
        "roster_rows": roster_rows,
        "validation_rows": validation_rows,
        "dispatch_summary": dispatch_summary,
        "cad_job_rows": cad_job_rows,
        "comm_rows": comm_rows,
        "closing_text": (
            "This medical debrief is produced from live operational data. If you need a deeper clinical records review, "
            "a different time window, or a bespoke management report, your medical operations team can generate "
            "additional exports or walk through these figures with you."
        ),
    }


def render_debrief_pack_csv(pack: dict[str, Any]) -> str:
    """Plain UTF-8 text; encode as utf-8-sig at send time for Excel."""
    buf = StringIO()
    w = csv.writer(buf)
    w.writerow(["# Medical debrief — data tables"])

    dex = pack.get("debrief_export") if isinstance(pack.get("debrief_export"), dict) else {}
    w.writerow(
        [
            humanize_debrief_field("generated_at"),
            format_debrief_datetime(dex.get("generated_at")),
            humanize_debrief_field("exported_by"),
            dex.get("exported_by") or "",
        ]
    )
    w.writerow([])

    hc = pack.get("handover_context") if isinstance(pack.get("handover_context"), dict) else {}
    crm = hc.get("crm_event_plan") if isinstance(hc.get("crm_event_plan"), dict) else None
    if crm:
        w.writerow(["## Linked CRM event plan"])
        w.writerow(["Field", "Value"])
        w.writerow(["Plan title", crm.get("title") or ""])
        w.writerow(["Handoff status", crm.get("handoff_status") or "—"])
        w.writerow(["Plan start", format_debrief_datetime(crm.get("start_datetime"))])
        w.writerow(["Plan end", format_debrief_datetime(crm.get("end_datetime"))])
        w.writerow([])

    ev = pack.get("operational_event") if isinstance(pack.get("operational_event"), dict) else {}
    w.writerow(["## Operational deployment"])
    w.writerow(["Field", "Value"])
    for row in _dict_to_rows(_format_operational_event_display(ev), key_mode="field"):
        w.writerow(row)
    w.writerow([])

    totals = _client_facing_totals(pack.get("totals") if isinstance(pack.get("totals"), dict) else {})
    w.writerow(["## Activity totals"])
    w.writerow(["Metric", "Count"])
    for row in _dict_to_rows(totals, key_mode="field"):
        w.writerow(row)
    w.writerow([])

    w.writerow(["## Patient contact reports by status"])
    w.writerow(["Status", "Count"])
    for row in _dict_to_rows(
        pack.get("patient_contact_reports_by_status")
        if isinstance(pack.get("patient_contact_reports_by_status"), dict)
        else {},
        key_mode="status",
    ):
        w.writerow(row)
    w.writerow([])

    w.writerow(["## Safeguarding referrals by status"])
    w.writerow(["Status", "Count"])
    for row in _dict_to_rows(
        pack.get("safeguarding_referrals_by_status")
        if isinstance(pack.get("safeguarding_referrals_by_status"), dict)
        else {},
        key_mode="status",
    ):
        w.writerow(row)
    w.writerow([])

    mi = pack.get("minor_injury")
    if isinstance(mi, dict) and mi:
        w.writerow(["## Minor injury reporting (rollup)"])
        w.writerow(["Item", "Detail"])
        for row in _mi_rollup_rows(mi):
            w.writerow(row)
        w.writerow([])

    esn = pack.get("epcr_presenting_snomed") if isinstance(pack.get("epcr_presenting_snomed"), dict) else {}
    if esn:
        w.writerow(["## Clinical encounter coding (SNOMED overview)"])
        w.writerow(["Measure", "Value"])
        for row in _snomed_client_kv(esn):
            w.writerow(row)
        for block_title, key in (
            ("Primary presentations (SNOMED)", "primaryPresentations"),
            ("Suspected conditions (SNOMED)", "suspectedConditions"),
        ):
            rows = esn.get(key)
            if isinstance(rows, list) and rows:
                w.writerow([])
                w.writerow([f"### {block_title}", ""])
                w.writerow(
                    [
                        humanize_debrief_field("conceptId"),
                        "Preferred term",
                        "Count",
                        "% of cases",
                    ]
                )
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    w.writerow(
                        [
                            row.get("conceptId", ""),
                            _short(row.get("pt"), 500),
                            row.get("count", ""),
                            row.get("percentage", ""),
                        ]
                    )
        w.writerow([])

    cases = pack.get("epcr_cases") if isinstance(pack.get("epcr_cases"), list) else []
    w.writerow(["## EPCR cases (summary rows)"])
    if cases:
        keys = [
            "caseId",
            "status",
            "updatedAt",
            "patientNameHint",
            "presentingComplaintSnippet",
            "presentingSnomedPrimary",
            "presentingSnomedSuspected",
            "responseType",
        ]
        w.writerow([humanize_debrief_field(k) for k in keys] + [humanize_debrief_field("responseTypeMeta")])
        for c in cases:
            if not isinstance(c, dict):
                continue
            meta = c.get("responseTypeMeta")
            meta_s = (
                _format_response_meta_pretty(meta)
                if isinstance(meta, dict)
                else ("" if meta is None else str(meta))
            )
            pr = c.get("presentingSnomedPrimary")
            pr_s = json.dumps(pr, ensure_ascii=True) if isinstance(pr, dict) else ""
            su = c.get("presentingSnomedSuspected")
            su_s = json.dumps(su, ensure_ascii=True) if isinstance(su, list) else ""
            vals = [
                _short(c.get(keys[0]), 4000),
                _short(c.get(keys[1]), 4000),
                format_debrief_datetime(c.get(keys[2])),
                _short(c.get(keys[3]), 4000),
                _short(c.get(keys[4]), 4000),
            ] + [_short(pr_s, 2000), _short(su_s, 4000)]
            vals += [_short(c.get("responseType"), 4000), _short(meta_s, 8000)]
            w.writerow(vals)
    else:
        w.writerow(["(none)"])
    w.writerow([])

    assigns = pack.get("assignments") if isinstance(pack.get("assignments"), list) else []
    w.writerow(["## Roster assignments"])
    if assigns:
        keys = list(assigns[0].keys()) if assigns and isinstance(assigns[0], dict) else []
        if keys:
            w.writerow([humanize_debrief_field(k) for k in keys])
            for a in assigns:
                if isinstance(a, dict):
                    row_out = [_short(a.get(k), 500) for k in keys]
                    if "created_at" in keys:
                        idx = keys.index("created_at")
                        row_out[idx] = format_debrief_datetime(a.get("created_at"))
                    w.writerow(row_out)
    else:
        w.writerow(["(none)"])
    w.writerow([])

    vlog = pack.get("callsign_validation_log") if isinstance(pack.get("callsign_validation_log"), list) else []
    w.writerow(["## Callsign / MDT validation log"])
    if vlog:
        keys = ["id", "username", "callsign", "ok", "reason_code", "detail", "created_at"]
        w.writerow([humanize_debrief_field(k) for k in keys])
        for r in vlog:
            if not isinstance(r, dict):
                continue
            det = r.get("detail")
            det_s = json.dumps(det, default=str) if det is not None else ""
            w.writerow(
                [
                    r.get("id", ""),
                    r.get("username", ""),
                    r.get("callsign", ""),
                    r.get("ok", ""),
                    r.get("reason_code", ""),
                    _short(det_s, 2000),
                    format_debrief_datetime(r.get("created_at")),
                ]
            )
    else:
        w.writerow(["(none)"])
    w.writerow([])

    vc = pack.get("ventus_cad_correlation") if isinstance(pack.get("ventus_cad_correlation"), dict) else {}
    w.writerow(["## Dispatch desk (Ventus) summary"])
    w.writerow([humanize_debrief_field("available"), vc.get("available", "")])
    w.writerow([humanize_debrief_field("ventus_division_slug"), vc.get("ventus_division_slug") or ""])
    w.writerow(["Summary", _ventus_client_summary(vc)])
    tw = vc.get("time_window") if isinstance(vc.get("time_window"), dict) else {}
    w.writerow([humanize_debrief_field("window_starts"), format_debrief_datetime(tw.get("starts_at"))])
    w.writerow([humanize_debrief_field("window_ends"), format_debrief_datetime(tw.get("ends_at"))])
    tot = vc.get("totals") if isinstance(vc.get("totals"), dict) else {}
    w.writerow([humanize_debrief_field("total_cads"), tot.get("cads", "")])
    w.writerow([humanize_debrief_field("comms_included"), tot.get("comms_included", "")])
    w.writerow([])
    w.writerow(["### CAD jobs"])
    w.writerow(
        [
            "CAD",
            humanize_debrief_field("status"),
            humanize_debrief_field("division"),
            humanize_debrief_field("created_at"),
            humanize_debrief_field("updated_at"),
            humanize_debrief_field("job_comms_total"),
        ]
    )
    for job in vc.get("cads") or []:
        if not isinstance(job, dict):
            continue
        w.writerow(
            [
                job.get("cad", ""),
                job.get("status", ""),
                job.get("division", ""),
                format_debrief_datetime(job.get("created_at")),
                format_debrief_datetime(job.get("updated_at")),
                job.get("job_comms_total", ""),
            ]
        )
    w.writerow([])
    w.writerow(["### CAD message samples"])
    w.writerow(
        [
            "CAD",
            humanize_debrief_field("comm_id"),
            humanize_debrief_field("message_type"),
            humanize_debrief_field("sender_role"),
            humanize_debrief_field("sender_user"),
            humanize_debrief_field("created_at"),
            humanize_debrief_field("message_text"),
        ]
    )
    for job in vc.get("cads") or []:
        if not isinstance(job, dict):
            continue
        cid = job.get("cad", "")
        for comm in job.get("job_comms_sample") or []:
            if not isinstance(comm, dict):
                continue
            w.writerow(
                [
                    cid,
                    comm.get("id", ""),
                    comm.get("message_type", ""),
                    comm.get("sender_role", ""),
                    comm.get("sender_user", ""),
                    format_debrief_datetime(comm.get("created_at")),
                    _short(comm.get("message_text"), 4000),
                ]
            )

    return buf.getvalue()


def render_debrief_pack_pdf(
    pack: dict[str, Any],
    *,
    branding: dict[str, Any] | None = None,
    app: Any | None = None,
) -> bytes:
    """Client debrief PDF via WeasyPrint + HTML (same branding approach as CRM event plan PDFs)."""
    from flask import current_app, render_template

    from app.plugins.crm_module.crm_static_paths import crm_static_dir_for_app

    try:
        from weasyprint import HTML
    except ImportError as e:
        raise RuntimeError("WeasyPrint is required for operational debrief PDF") from e

    app_obj = app or current_app._get_current_object()
    ctx = build_debrief_pdf_context(pack, branding, app=app_obj)
    html = render_template("pdf/cura_ops_debrief.html", **ctx)
    static_root = Path(crm_static_dir_for_app(app_obj)).resolve()
    base_uri = static_root.as_uri() + "/"
    try:
        return HTML(string=html, base_url=base_uri).write_pdf()
    except Exception as ex:
        logger.exception("render_debrief_pack_pdf: %s", ex)
        raise

