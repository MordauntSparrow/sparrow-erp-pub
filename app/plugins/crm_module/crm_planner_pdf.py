"""Medical event plan PDF (WeasyPrint + Jinja) with core branding.

Print CSS matches the green clinical PDF theme used by the medical debrief export
(``medical_records_module`` ``pdf/cura_ops_debrief.html``) for a consistent client look.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import render_template

from app.objects import get_db_connection

from .crm_branding import get_site_branding
from .crm_clinical_handover import (
    ashice_pdf_rows,
    atmist_pdf_rows,
    clinical_pre_alert_has_display,
    custom_pre_alert_pdf_rows,
    parse_clinical_handover,
)
from .crm_event_plan_extended import risk_register_list_from_plan, risk_row_nonempty
from .crm_manifest_settings import get_crm_module_settings
from .crm_plan_pdf_context import (
    load_plan_pdf_labels,
    management_support_list_from_plan,
    management_support_row_nonempty,
    staff_roster_list_from_plan,
)
from .crm_event_plan_accommodation import (
    accommodation_pdf_visible,
    accommodation_room_pdf_rows,
    accommodation_venue_pdf_rows,
)
from .crm_event_plan_major_incident import (
    coerce_major_incident_detail,
    major_incident_extra_pdf_rows,
    major_incident_methane_pdf_rows,
)
from .crm_event_plan_media import diagram_pdf_src
from .crm_static_paths import (
    crm_event_map_path_is_allowed,
    crm_event_plan_diagram_src_allowed,
    crm_event_plan_pdf_logo_path_is_allowed,
    crm_event_plan_pdf_relative_subpath,
    crm_event_plan_pdf_write_dir,
    crm_plan_cover_image_path_is_allowed,
    crm_static_dir_for_app,
)


def _parse_json_array_field(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            v = json.loads(s)
            return v if isinstance(v, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _hospital_facility_lines_from_dict(it: dict[str, Any]) -> list[str]:
    raw = it.get("facility_lines")
    if isinstance(raw, list):
        out: list[str] = []
        for x in raw:
            if isinstance(x, str):
                s = x.strip()[:255]
                if s:
                    out.append(s)
            elif isinstance(x, dict):
                s = str(x.get("line") or x.get("text") or "").strip()[:255]
                if s:
                    out.append(s)
        if out:
            return out
    legacy = str(it.get("facilities") or it.get("type_facilities") or "").strip()
    if legacy:
        return [legacy[:255]]
    return []


def _hospital_contact_lines_from_dict(it: dict[str, Any]) -> list[dict[str, str]]:
    lines: list[dict[str, str]] = []
    raw = it.get("contact_lines")
    if isinstance(raw, list):
        for x in raw:
            if not isinstance(x, dict):
                continue
            t = str(x.get("title") or "").strip()[:128]
            d = str(x.get("detail") or x.get("value") or "").strip()[:255]
            if t or d:
                lines.append({"title": t, "detail": d})
    legacy = str(it.get("contact") or "").strip()
    if legacy and not lines:
        lines.append({"title": "", "detail": legacy[:255]})
    return lines


def hospital_table_rows_from_plan(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Normalise ``hospitals_json`` to rows for PDF / edit UI.

    Supports legacy list of strings (``Name — Postcode`` or single line)
    and structured list of dicts with name, facilities, address, contact,
    and optional ``contact_lines`` (``[{title, detail}, …]``),
    ``facility_lines`` (``["A&E", …]``) with legacy single ``facilities`` string.
    """
    items = _parse_json_array_field(plan.get("hospitals_json"))
    out: list[dict[str, Any]] = []
    for it in items:
        if isinstance(it, dict):
            contact_lines = _hospital_contact_lines_from_dict(it)
            facility_lines = _hospital_facility_lines_from_dict(it)
            contact_one_line = "\n".join(
                (f"{cl['title']} — {cl['detail']}" if cl["title"] else cl["detail"])
                for cl in contact_lines
                if (cl.get("title") or "").strip() or (cl.get("detail") or "").strip()
            )
            facilities_joined = "\n".join(facility_lines)
            out.append(
                {
                    "name": str(it.get("name") or "").strip(),
                    "facilities": facilities_joined,
                    "facility_lines": facility_lines,
                    "address": str(it.get("address") or "").strip(),
                    "contact": contact_one_line,
                    "contact_lines": contact_lines,
                }
            )
            continue
        s = str(it).strip()
        if not s:
            continue
        if " — " in s:
            a, b = s.split(" — ", 1)
            out.append(
                {
                    "name": a.strip(),
                    "facilities": "",
                    "facility_lines": [],
                    "address": b.strip(),
                    "contact": "",
                    "contact_lines": [],
                }
            )
        else:
            out.append(
                {
                    "name": s,
                    "facilities": "",
                    "facility_lines": [],
                    "address": "",
                    "contact": "",
                    "contact_lines": [],
                }
            )
    return [r for r in out if any(r.values())]


def pad_hospital_rows(
    rows: list[dict[str, Any]], size: int = 20
) -> list[dict[str, Any] | None]:
    out: list[dict[str, Any] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        out[i] = r
    return out


def pad_hospital_rows_for_edit(rows: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
    """Default a few blank rows; grow with saved data without truncating (capped)."""
    n = len(rows)
    size = max(3, min(40, n + 3))
    return pad_hospital_rows(rows, size=size)


def _val_to_datetime_local_input(val: Any) -> str:
    """Format stored value for HTML ``datetime-local`` inputs."""
    if val is None:
        return ""
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%dT%H:%M")
    s = str(val).strip()
    if not s:
        return ""
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo:
            d = d.replace(tzinfo=None)
        return d.strftime("%Y-%m-%dT%H:%M")
    except ValueError:
        return s[:16] if len(s) >= 16 else s


def _parse_datetime_local_input_value(s: Any) -> datetime | None:
    """Parse ``datetime-local`` / ISO-ish strings to naive ``datetime``."""
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    try:
        d = datetime.fromisoformat(t.replace("Z", "+00:00"))
        if d.tzinfo:
            d = d.replace(tzinfo=None)
        return d
    except ValueError:
        pass
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(t, fmt)
        except ValueError:
            continue
    return None


def summary_datetimes_from_padded_event_day_rows(
    rows: list[dict[str, str] | None],
) -> tuple[datetime | None, datetime | None]:
    """First row start (day 1) and last row end (last day), in daily timings table order."""
    if not rows:
        return None, None
    first = rows[0]
    start_dt = None
    if isinstance(first, dict):
        start_dt = _parse_datetime_local_input_value(first.get("start_input"))
    last_dict: dict[str, str] | None = None
    for r in reversed(rows):
        if isinstance(r, dict):
            last_dict = r
            break
    end_dt = None
    if last_dict is not None:
        end_dt = _parse_datetime_local_input_value(last_dict.get("end_input"))
    return start_dt, end_dt


def event_days_list_for_editor(plan: dict[str, Any]) -> list[dict[str, str]]:
    """Rows for per-day start/end editor (label + datetime-local value strings)."""
    items = _parse_json_array_field(plan.get("event_days_json"))
    out: list[dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        lbl = str(it.get("label") or "").strip()[:64]
        st = _val_to_datetime_local_input(it.get("start"))
        en = _val_to_datetime_local_input(it.get("end"))
        if lbl or st or en:
            out.append({"label": lbl, "start_input": st, "end_input": en})
    return out


def pad_event_day_rows_for_edit(
    rows: list[dict[str, str]],
) -> list[dict[str, str] | None]:
    """Two empty day slots when none saved; otherwise one slot per saved row (capped at 40)."""
    n = len(rows)
    size = max(2, min(40, n))
    padded: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        padded[i] = r
    return padded


def _date_yyyy_mm_dd_from_datetime_local(s: str) -> str:
    """Calendar date from ``datetime-local`` value (``YYYY-MM-DDTHH:MM``)."""
    t = (s or "").strip()
    if len(t) >= 10 and t[4] == "-" and t[7] == "-":
        return t[:10]
    return ""


def attendance_by_day_list_for_editor(plan: dict[str, Any]) -> list[dict[str, str]]:
    """Rows aligned with daily timings: label + date from event day when missing, plus amount."""
    items = _parse_json_array_field(plan.get("attendance_by_day_json"))
    raw: list[dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        lbl = str(it.get("label") or "").strip()[:64]
        d = str(it.get("date") or "").strip()[:10]
        amt_raw = it.get("amount")
        amt_s = ""
        if amt_raw is not None and str(amt_raw).strip() != "":
            try:
                amt_s = str(int(amt_raw))
            except (TypeError, ValueError):
                amt_s = str(amt_raw).strip()
        if d or amt_s or lbl:
            raw.append({"label_val": lbl, "date_val": d, "amount_val": amt_s})

    ev = event_days_list_for_editor(plan)
    n = max(len(raw), len(ev), 2)
    out: list[dict[str, str]] = []
    for i in range(n):
        r = raw[i] if i < len(raw) else {"label_val": "", "date_val": "", "amount_val": ""}
        e = ev[i] if i < len(ev) else None
        ev_lbl = ""
        ev_date = ""
        if e:
            ev_lbl = str(e.get("label") or "").strip()[:64]
            ev_date = _date_yyyy_mm_dd_from_datetime_local(str(e.get("start_input") or ""))
        lbl_v = (r.get("label_val") or "").strip() or ev_lbl
        date_v = (r.get("date_val") or "").strip() or ev_date
        amt_v = str(r.get("amount_val") or "").strip()
        out.append(
            {
                "label_val": lbl_v,
                "date_val": date_v,
                "amount_val": amt_v,
                "sync_row": bool(e),
            }
        )
    return out


def pad_attendance_by_day_rows_for_edit(
    rows: list[dict[str, str]],
) -> list[dict[str, str] | None]:
    """Two empty date/amount slots when none saved; otherwise one slot per saved row (capped at 40)."""
    n = len(rows)
    size = max(2, min(40, n))
    padded: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        padded[i] = r
    return padded


def attendance_by_day_json_has_rows(raw: Any) -> bool:
    """True if plan JSON column holds at least one object row."""
    return bool(_parse_json_array_field(raw))


def operational_timings_list_for_editor(plan: dict[str, Any]) -> list[dict[str, str]]:
    """Rows for date + time + description (``operational_timings_json``)."""
    items = _parse_json_array_field(plan.get("operational_timings_json"))
    out: list[dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        d = str(it.get("date") or "").strip()[:10]
        tm_raw = str(it.get("time") or "").strip()
        if len(tm_raw) > 5 and tm_raw[5] == ":":
            tm_raw = tm_raw[:5]
        elif len(tm_raw) > 8:
            tm_raw = tm_raw[:8]
        desc = str(it.get("description") or "").strip()[:4000]
        if d or tm_raw or desc:
            out.append({"date_val": d, "time_val": tm_raw, "desc_val": desc})
    return out


def pad_operational_timings_rows_for_edit(
    rows: list[dict[str, str]],
) -> list[dict[str, str] | None]:
    n = len(rows)
    size = max(4, min(24, n + 4))
    padded: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        padded[i] = r
    return padded


def operational_timings_json_has_rows(raw: Any) -> bool:
    return bool(_parse_json_array_field(raw))


def access_egress_structured_rows_from_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    """Normalise ``access_egress_json`` for PDF and editors."""
    items = _parse_json_array_field(plan.get("access_egress_json"))
    out: list[dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or it.get("access_point") or "").strip()[:256]
        addr = str(it.get("address") or "").strip()[:512]
        w3w = str(it.get("what3words") or it.get("w3w") or "").strip()[:128]
        if name or addr or w3w:
            out.append({"name": name, "address": addr, "what3words": w3w})
    return out


def access_egress_list_for_editor(plan: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "name_val": r["name"],
            "address_val": r["address"],
            "w3w_val": r["what3words"],
        }
        for r in access_egress_structured_rows_from_plan(plan)
    ]


def pad_access_egress_rows_for_edit(
    rows: list[dict[str, str]],
) -> list[dict[str, str] | None]:
    n = len(rows)
    size = max(4, min(24, n + 4))
    padded: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        padded[i] = r
    return padded


def access_egress_json_has_rows(raw: Any) -> bool:
    return bool(_parse_json_array_field(raw))


def access_egress_text_from_structured_rows(
    rows: list[dict[str, Any]],
) -> str | None:
    """Plain multi-line fallback for ``access_egress_text`` (handoff / search)."""
    lines: list[str] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or it.get("access_point") or "").strip()
        addr = str(it.get("address") or "").strip()
        w3w = str(it.get("what3words") or it.get("w3w") or "").strip()
        if not name and not addr and not w3w:
            continue
        parts: list[str] = []
        if name:
            parts.append(name)
        if addr:
            parts.append(addr)
        if w3w:
            parts.append(w3w)
        lines.append(" · ".join(parts))
    return "\n".join(lines) if lines else None


def rendezvous_structured_rows_from_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    items = _parse_json_array_field(plan.get("rendezvous_json"))
    out: list[dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or "").strip()[:256]
        addr = str(it.get("address") or "").strip()[:512]
        w3w = str(it.get("what3words") or it.get("w3w") or "").strip()[:128]
        if name or addr or w3w:
            out.append({"name": name, "address": addr, "what3words": w3w})
    return out


def rendezvous_list_for_editor(plan: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "name_val": r["name"],
            "address_val": r["address"],
            "w3w_val": r["what3words"],
        }
        for r in rendezvous_structured_rows_from_plan(plan)
    ]


def pad_rendezvous_rows_for_edit(
    rows: list[dict[str, str]],
) -> list[dict[str, str] | None]:
    n = len(rows)
    size = max(4, min(24, n + 4))
    padded: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        padded[i] = r
    return padded


def rendezvous_json_has_rows(raw: Any) -> bool:
    return bool(_parse_json_array_field(raw))


def rendezvous_text_from_structured_rows(
    rows: list[dict[str, Any]],
) -> str | None:
    """Plain multi-line fallback for ``rendezvous_text``."""
    lines: list[str] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or "").strip()
        addr = str(it.get("address") or "").strip()
        w3w = str(it.get("what3words") or it.get("w3w") or "").strip()
        if not (name or addr or w3w):
            continue
        parts: list[str] = []
        if name:
            parts.append(name)
        if addr:
            parts.append(addr)
        if w3w:
            parts.append(w3w)
        lines.append(" · ".join(parts))
    return "\n".join(lines) if lines else None


def staff_transport_structured_rows_from_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    items = _parse_json_array_field(plan.get("staff_transport_json"))
    out: list[dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        d = str(it.get("date") or "").strip()[:10]
        veh = str(it.get("vehicle") or "").strip()[:512]
        stf = str(it.get("staff") or "").strip()[:512]
        if d or veh or stf:
            out.append({"date": d, "vehicle": veh, "staff": stf})
    return out


def staff_transport_list_for_editor(plan: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "date_val": r["date"],
            "vehicle_val": r["vehicle"],
            "staff_val": r["staff"],
        }
        for r in staff_transport_structured_rows_from_plan(plan)
    ]


def pad_staff_transport_rows_for_edit(
    rows: list[dict[str, str]],
) -> list[dict[str, str] | None]:
    n = len(rows)
    size = max(4, min(24, n + 4))
    padded: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        padded[i] = r
    return padded


def staff_transport_json_has_rows(raw: Any) -> bool:
    return bool(_parse_json_array_field(raw))


def staff_transport_text_from_structured_rows(
    rows: list[dict[str, Any]],
) -> str | None:
    """Plain multi-line fallback for ``staff_transport_text``."""
    lines: list[str] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        d_raw = str(it.get("date") or "").strip()[:10]
        veh = str(it.get("vehicle") or "").strip()
        stf = str(it.get("staff") or "").strip()
        if not d_raw and not veh and not stf:
            continue
        date_uk = ""
        if d_raw:
            try:
                date_uk = datetime.strptime(d_raw, "%Y-%m-%d").date().strftime("%d/%m/%Y")
            except ValueError:
                date_uk = d_raw
        segs: list[str] = []
        if date_uk:
            segs.append(date_uk)
        if veh:
            segs.append(veh)
        if stf:
            segs.append(stf)
        lines.append(" — ".join(segs))
    return "\n".join(lines) if lines else None


def staff_transport_pdf_rows_from_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    """Rows for PDF table (display date as DD/MM/YYYY)."""
    out: list[dict[str, str]] = []
    for r in staff_transport_structured_rows_from_plan(plan):
        d = (r.get("date") or "").strip()[:10]
        d_disp = "—"
        if d:
            try:
                d_disp = datetime.strptime(d, "%Y-%m-%d").strftime("%d/%m/%Y")
            except ValueError:
                d_disp = d
        veh = (r.get("vehicle") or "").strip()
        stf = (r.get("staff") or "").strip()
        out.append({"date_disp": d_disp, "vehicle": veh, "staff": stf})
    return out


DEFAULT_UNIFORM_PPE_TYPES: tuple[str, ...] = (
    "Helmets",
    "Hi-Vis",
)


def uniform_ppe_structured_rows_from_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    """Rows from ``uniform_ppe_json`` only (for PDF / derived text)."""
    items = _parse_json_array_field(plan.get("uniform_ppe_json"))
    out: list[dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        typ = str(it.get("ppe_type") or it.get("type") or "").strip()[:256]
        yn = str(it.get("yes_no") or "").strip().lower()
        if yn not in ("yes", "no", ""):
            yn = ""
        notes = str(it.get("notes") or "").strip()[:512]
        if typ or yn or notes:
            out.append({"ppe_type": typ, "yes_no": yn, "notes": notes})
    return out


def uniform_ppe_list_for_editor(plan: dict[str, Any]) -> list[dict[str, str]]:
    """Editor rows: saved JSON, or Helmet + Hi-Vis defaults when nothing stored yet."""
    parsed = uniform_ppe_structured_rows_from_plan(plan)
    if parsed:
        return [
            {"type_val": r["ppe_type"], "yes_no_val": r["yes_no"], "notes_val": r["notes"]}
            for r in parsed
        ]
    return [
        {"type_val": t, "yes_no_val": "", "notes_val": ""} for t in DEFAULT_UNIFORM_PPE_TYPES
    ]


def pad_uniform_ppe_rows_for_edit(
    rows: list[dict[str, str]],
) -> list[dict[str, str] | None]:
    """Match row count to data (min two slots for the default Helmet / Hi-Vis pair); cap at 40."""
    n = len(rows)
    size = max(2, min(40, n))
    padded: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        padded[i] = r
    return padded


def uniform_ppe_json_has_rows(raw: Any) -> bool:
    return bool(_parse_json_array_field(raw))


def uniform_ppe_text_from_structured_rows(
    rows: list[dict[str, Any]],
) -> str | None:
    """Plain multi-line fallback for ``uniform_ppe_text``."""
    lines: list[str] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        typ = str(it.get("ppe_type") or it.get("type") or "").strip()
        yn = str(it.get("yes_no") or "").strip().lower()
        notes = str(it.get("notes") or "").strip()
        if not typ and not yn and not notes:
            continue
        yn_disp = "Yes" if yn == "yes" else ("No" if yn == "no" else "—")
        if notes:
            lines.append(f"{typ or '—'}: {yn_disp} — {notes}")
        else:
            lines.append(f"{typ or '—'}: {yn_disp}")
    return "\n".join(lines) if lines else None


def uniform_ppe_pdf_rows_from_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for r in uniform_ppe_structured_rows_from_plan(plan):
        yn = (r.get("yes_no") or "").strip().lower()
        yn_l = "Yes" if yn == "yes" else ("No" if yn == "no" else "—")
        out.append(
            {
                "ppe_type": (r.get("ppe_type") or "").strip() or "—",
                "yes_no": yn_l,
                "notes": (r.get("notes") or "").strip(),
            }
        )
    return out


def plan_distribution_lines_from_plan(plan: dict[str, Any]) -> list[str]:
    """Recipient lines from ``plan_distribution_json`` (array of strings or ``{line: …}``)."""
    items = _parse_json_array_field(plan.get("plan_distribution_json"))
    if not items:
        return []
    out: list[str] = []
    for it in items:
        if isinstance(it, str):
            s = it.strip()
            if s:
                out.append(s[:512])
        elif isinstance(it, dict):
            s = str(it.get("line") or it.get("recipient") or "").strip()
            if s:
                out.append(s[:512])
    return out


def plan_distribution_list_for_editor(plan: dict[str, Any]) -> list[dict[str, str]]:
    return [{"line_val": s} for s in plan_distribution_lines_from_plan(plan)]


def pad_plan_distribution_rows_for_edit(
    rows: list[dict[str, str]],
) -> list[dict[str, str] | None]:
    """Three empty recipient slots when none saved; otherwise one slot per line (capped at 40)."""
    n = len(rows)
    size = max(3, min(40, n))
    padded: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        padded[i] = r
    return padded


def plan_distribution_json_has_rows(raw: Any) -> bool:
    return bool(_parse_json_array_field(raw))


def _coerce_signoff_at_value(val: Any) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, (bytes, bytearray)):
        val = val.decode("utf-8", errors="replace")
    s = str(val).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19] if len(s) >= 19 else s, fmt)
        except ValueError:
            continue
    return None


def clinical_signoff_structured_rows_from_plan(
    plan: dict[str, Any],
) -> list[dict[str, Any]]:
    """Each row: name, role, at (datetime | None). Uses JSON or legacy ``signoff_*`` columns."""
    items = _parse_json_array_field(plan.get("clinical_signoff_json"))
    out: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or "").strip()[:255]
        role = str(it.get("role") or "").strip()[:255]
        at_dt = _coerce_signoff_at_value(it.get("at"))
        if name or role or at_dt:
            out.append({"name": name, "role": role, "at": at_dt})
    if out:
        return out
    sn = str(plan.get("signoff_name") or "").strip()[:255]
    sr = str(plan.get("signoff_role") or "").strip()[:255]
    sa = plan.get("signoff_at")
    at_dt = _coerce_signoff_at_value(sa)
    if sn or sr or at_dt:
        return [{"name": sn, "role": sr, "at": at_dt}]
    return []


def clinical_signoff_list_for_editor(plan: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for r in clinical_signoff_structured_rows_from_plan(plan):
        at_v = r.get("at")
        at_s = ""
        if isinstance(at_v, datetime):
            at_s = at_v.strftime("%Y-%m-%dT%H:%M")
        rows.append(
            {
                "name_val": r.get("name") or "",
                "role_val": r.get("role") or "",
                "at_val": at_s,
            }
        )
    return rows


def pad_clinical_signoff_rows_for_edit(
    rows: list[dict[str, str]],
) -> list[dict[str, str] | None]:
    """At least one editable slot; up to 20 rows."""
    n = len(rows)
    size = max(1, min(20, n if n > 0 else 1))
    padded: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        padded[i] = r
    return padded


def clinical_signoff_pdf_rows_from_plan(
    plan: dict[str, Any],
) -> list[dict[str, str]]:
    """Rows for PDF table: name, role, at_disp."""
    struct = clinical_signoff_structured_rows_from_plan(plan)
    out: list[dict[str, str]] = []
    for r in struct:
        name = (r.get("name") or "").strip() or "—"
        role = (r.get("role") or "").strip() or "—"
        at_v = r.get("at")
        if isinstance(at_v, datetime):
            at_disp = at_v.strftime("%d/%m/%Y %H:%M")
        else:
            at_disp = "—"
        out.append({"name": name, "role": role, "at_disp": at_disp})
    if not out:
        out.append({"name": "—", "role": "—", "at_disp": "—"})
    return out


def operational_timings_text_from_structured_rows(
    rows: list[dict[str, Any]],
) -> str | None:
    """One line per row for PDF / Cura handoff (``operational_timings``)."""
    lines: list[str] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        d_raw = str(it.get("date") or "").strip()[:10]
        tm = str(it.get("time") or "").strip()
        if len(tm) > 5 and tm[5] == ":":
            tm = tm[:5]
        desc = str(it.get("description") or "").strip()
        if not d_raw and not tm and not desc:
            continue
        date_uk = ""
        if d_raw:
            try:
                d_o = datetime.strptime(d_raw, "%Y-%m-%d").date()
                date_uk = d_o.strftime("%d/%m/%Y")
            except ValueError:
                date_uk = d_raw
        prefix_parts: list[str] = []
        if date_uk:
            prefix_parts.append(date_uk)
        if tm:
            prefix_parts.append(tm[:5] if len(tm) >= 5 else tm)
        prefix = " ".join(prefix_parts)
        if prefix and desc:
            lines.append(f"{prefix} - {desc}")
        elif desc:
            lines.append(desc)
        elif prefix:
            lines.append(prefix)
    return "\n".join(lines) if lines else None


def attendance_text_from_structured_rows(rows: list[dict[str, Any]]) -> str | None:
    """One line per day for PDF / Cura handoff (``attendance_by_day_text``)."""
    lines: list[str] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        lbl = str(it.get("label") or "").strip()
        d = str(it.get("date") or "").strip()
        amt = it.get("amount")
        if not d and amt is None and not lbl:
            continue
        amt_s = ""
        if amt is not None:
            try:
                n = int(amt)
                amt_s = f"{n:,}"
            except (TypeError, ValueError):
                amt_s = str(amt).strip()
        parts: list[str] = []
        if lbl:
            parts.append(lbl)
        if d:
            parts.append(d)
        head = " — ".join(parts) if parts else ""
        if head and amt_s:
            lines.append(f"{head} — {amt_s}")
        elif head:
            lines.append(head)
        elif amt_s:
            lines.append(amt_s)
    return "\n".join(lines) if lines else None


def _pdf_format_event_day_dt(val: Any) -> str:
    if val is None or val == "":
        return "—"
    if isinstance(val, datetime):
        return val.strftime("%d %b %Y %H:%M")
    s = str(val).strip()
    if not s:
        return "—"
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo:
            d = d.replace(tzinfo=None)
        return d.strftime("%d %b %Y %H:%M")
    except ValueError:
        return s[:120]


def event_days_display_rows_from_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    """Formatted rows for PDF daily timings table."""
    items = _parse_json_array_field(plan.get("event_days_json"))
    out: list[dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        lbl = (str(it.get("label") or "").strip() or "—")[:64]
        sd = _pdf_format_event_day_dt(it.get("start"))
        ed = _pdf_format_event_day_dt(it.get("end"))
        if sd != "—" or ed != "—" or (lbl and lbl != "—"):
            out.append({"label": lbl, "start": sd, "end": ed})
    return [r for r in out if r["start"] != "—" or r["end"] != "—"]


def _roster_row_nonempty(r: dict[str, Any]) -> bool:
    keys = (
        "callsign",
        "name",
        "role_grade",
        "post_assignment",
        "fleet_vehicle",
        "shift",
        "phone",
        "notes",
    )
    return any(str(r.get(k) or "").strip() for k in keys)


def plan_pdf_revision_display_label(plan: dict[str, Any]) -> str | None:
    """After each PDF generation, plan_pdf_revision is incremented; first issue is v1.0."""
    try:
        rev = int(plan.get("plan_pdf_revision") or 0)
    except (TypeError, ValueError):
        rev = 0
    if rev < 1:
        return None
    return f"1.{rev - 1}"


def diagram_rows_with_pdf_src(
    diagram_rows: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not diagram_rows:
        return out
    for row in diagram_rows:
        src = diagram_pdf_src(row)
        if src and crm_event_plan_diagram_src_allowed(src):
            r = dict(row)
            r["pdf_image_src"] = src
            out.append(r)
    return out


def _roster_role_counts(rows: list[dict[str, Any]]) -> list[tuple[str, int]]:
    roles = [
        (r.get("role_grade") or "").strip() or "—"
        for r in rows
        if isinstance(r, dict) and _roster_row_nonempty(r)
    ]
    if not roles:
        return []
    c = Counter(roles)
    return sorted(c.items(), key=lambda x: (-x[1], x[0]))


def build_event_plan_html(
    app,
    plan: dict[str, Any],
    questions: list[dict],
    answers: dict,
    *,
    diagram_rows: list[dict[str, Any]] | None = None,
    equipment_kit_url: str | None = None,
) -> str:
    branding = get_site_branding(app)
    crm_pdf = get_crm_module_settings(app)
    pdf_labels = load_plan_pdf_labels(plan)
    roster = staff_roster_list_from_plan(plan)
    try:
        conn = get_db_connection()
        try:
            from .crm_fleet_bridge import enrich_roster_rows_for_pdf

            roster = enrich_roster_rows_for_pdf(roster, conn)
        finally:
            conn.close()
    except Exception:
        roster = [dict(x) for x in roster if isinstance(x, dict)]
        for r in roster:
            fv = str(r.get("fleet_vehicle") or "").strip()
            if fv == "onfoot":
                r["fleet_vehicle_display"] = "On foot"
            elif fv.isdigit():
                r["fleet_vehicle_display"] = f"Vehicle #{fv}"
            else:
                r["fleet_vehicle_display"] = ""
    roster_filled = [r for r in roster if isinstance(r, dict) and _roster_row_nonempty(r)]
    mgmt_list = management_support_list_from_plan(plan)
    management_support_roster = [
        r for r in mgmt_list if isinstance(r, dict) and management_support_row_nonempty(r)
    ]
    raw_map = (plan.get("event_map_path") or "").strip().replace("\\", "/")
    event_map_pdf_src = raw_map if raw_map and crm_event_map_path_is_allowed(raw_map) else None
    pdf_logo_rel = (crm_pdf.get("event_plan_pdf_logo_path") or "").strip().replace(
        "\\", "/"
    )
    pdf_tenant_logo_src = (
        pdf_logo_rel
        if pdf_logo_rel and crm_event_plan_pdf_logo_path_is_allowed(pdf_logo_rel)
        else None
    )
    raw_cover = (plan.get("plan_cover_image_path") or "").strip().replace("\\", "/")
    plan_cover_image_pdf_src = (
        raw_cover
        if raw_cover and crm_plan_cover_image_path_is_allowed(raw_cover)
        else None
    )
    cap = (plan.get("event_map_caption") or "").strip()
    event_map_caption = cap or None
    risk_all = risk_register_list_from_plan(plan)
    risk_filled = [r for r in risk_all if risk_row_nonempty(r)]
    doc_version_label = plan_pdf_revision_display_label(plan)
    diagram_pdf_rows = diagram_rows_with_pdf_src(diagram_rows)
    event_days_pdf_rows = event_days_display_rows_from_plan(plan)
    access_egress_pdf_rows = access_egress_structured_rows_from_plan(plan)
    rendezvous_pdf_rows = rendezvous_structured_rows_from_plan(plan)
    staff_transport_pdf_rows = staff_transport_pdf_rows_from_plan(plan)
    uniform_ppe_pdf_rows = uniform_ppe_pdf_rows_from_plan(plan)
    clinical_signoff_pdf_rows = clinical_signoff_pdf_rows_from_plan(plan)
    ch = parse_clinical_handover(plan.get("clinical_handover_json"))
    mid = coerce_major_incident_detail(plan.get("major_incident_detail_json"))
    mi_methane_rows = major_incident_methane_pdf_rows(mid)
    mi_extra_rows = major_incident_extra_pdf_rows(mid)
    major_incident_pdf_visible = bool(
        (str(plan.get("major_incident_text") or "").strip())
        or mi_methane_rows
        or mi_extra_rows
    )
    acc_vis = accommodation_pdf_visible(plan)
    acc_venue_rows = accommodation_venue_pdf_rows(plan) if acc_vis else []
    acc_room_rows = accommodation_room_pdf_rows(plan) if acc_vis else []
    return render_template(
        "pdf/crm_event_plan.html",
        plan=plan,
        questions=questions,
        answers=answers,
        hospital_table_rows=hospital_table_rows_from_plan(plan),
        event_days_pdf_rows=event_days_pdf_rows,
        access_egress_pdf_rows=access_egress_pdf_rows,
        rendezvous_pdf_rows=rendezvous_pdf_rows,
        staff_transport_pdf_rows=staff_transport_pdf_rows,
        uniform_ppe_pdf_rows=uniform_ppe_pdf_rows,
        clinical_handover=ch,
        clinical_pre_alert_has_display=clinical_pre_alert_has_display(ch),
        pre_alert_policy=(ch.get("pre_alert_policy") or "ashice").strip().lower(),
        pre_alert_ashice_rows=ashice_pdf_rows(ch),
        pre_alert_atmist_rows=atmist_pdf_rows(ch),
        pre_alert_custom_rows=custom_pre_alert_pdf_rows(ch),
        branding=branding,
        pdf_tenant_logo_src=pdf_tenant_logo_src,
        plan_cover_image_pdf_src=plan_cover_image_pdf_src,
        crm_pdf=crm_pdf,
        pdf_labels=pdf_labels,
        staff_roster=roster_filled,
        management_support_roster=management_support_roster,
        roster_headcount=len(roster_filled),
        roster_role_counts=_roster_role_counts(roster),
        event_map_pdf_src=event_map_pdf_src,
        event_map_caption=event_map_caption,
        risk_register_rows=risk_filled,
        generated_at=datetime.utcnow(),
        doc_version_label=doc_version_label,
        diagram_pdf_rows=diagram_pdf_rows,
        equipment_kit_url=equipment_kit_url or None,
        clinical_signoff_pdf_rows=clinical_signoff_pdf_rows,
        major_incident_methane_pdf_rows=mi_methane_rows,
        major_incident_extra_pdf_rows=mi_extra_rows,
        major_incident_pdf_visible=major_incident_pdf_visible,
        accommodation_pdf_visible=acc_vis,
        accommodation_venue_pdf_rows=acc_venue_rows,
        accommodation_room_pdf_rows=acc_room_rows,
    )


def write_event_plan_pdf(
    app,
    plan: dict[str, Any],
    questions: list[dict],
    answers: dict,
    *,
    diagram_rows: list[dict[str, Any]] | None = None,
    equipment_kit_url: str | None = None,
) -> tuple[bytes, str]:
    """Return (pdf_bytes, sha256_hex)."""
    html = build_event_plan_html(
        app,
        plan,
        questions,
        answers,
        diagram_rows=diagram_rows,
        equipment_kit_url=equipment_kit_url,
    )
    try:
        from weasyprint import HTML
    except ImportError as e:
        raise RuntimeError("WeasyPrint is required for event plan PDF") from e

    static = crm_static_dir_for_app(app)
    base_uri = Path(static).resolve().as_uri() + "/"
    pdf_bytes = HTML(string=html, base_url=base_uri).write_pdf()
    h = hashlib.sha256(pdf_bytes).hexdigest()
    return pdf_bytes, h


def store_event_plan_pdf_file(
    app, plan_id: int, pdf_bytes: bytes, pdf_hash: str
) -> str:
    """Save under static/uploads/crm_event_plans/; return relative path from static/."""
    sub = Path(crm_event_plan_pdf_relative_subpath())
    dest_dir = Path(crm_event_plan_pdf_write_dir(app))
    dest_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"plan_{plan_id}_{ts}.pdf"
    full = dest_dir / fname
    full.write_bytes(pdf_bytes)
    rel = f"{sub.as_posix()}/{fname}".replace("\\", "/")
    return rel
