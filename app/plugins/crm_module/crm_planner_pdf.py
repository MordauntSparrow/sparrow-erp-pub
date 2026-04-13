"""Medical event plan PDF (WeasyPrint + Jinja) with core branding."""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import render_template

from .crm_branding import get_site_branding
from .crm_clinical_handover import parse_clinical_handover
from .crm_event_plan_extended import risk_register_list_from_plan, risk_row_nonempty
from .crm_manifest_settings import get_crm_module_settings
from .crm_plan_pdf_context import load_plan_pdf_labels, staff_roster_list_from_plan
from .crm_event_plan_media import diagram_pdf_src
from .crm_static_paths import (
    crm_event_map_path_is_allowed,
    crm_event_plan_diagram_src_allowed,
    crm_event_plan_pdf_relative_subpath,
    crm_event_plan_pdf_write_dir,
    crm_static_dir_for_app,
)


def hospital_facilities_from_plan(plan: dict[str, Any]) -> list[str]:
    """Normalise hospitals_json (list of strings) for templates."""
    raw = plan.get("hospitals_json")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            v = json.loads(s)
            if isinstance(v, list):
                return [str(x).strip() for x in v if str(x).strip()]
        except json.JSONDecodeError:
            return []
    return []


def _roster_row_nonempty(r: dict[str, Any]) -> bool:
    keys = (
        "callsign",
        "name",
        "role_grade",
        "post_assignment",
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
    roster_filled = [r for r in roster if isinstance(r, dict) and _roster_row_nonempty(r)]
    raw_map = (plan.get("event_map_path") or "").strip().replace("\\", "/")
    event_map_pdf_src = raw_map if raw_map and crm_event_map_path_is_allowed(raw_map) else None
    cap = (plan.get("event_map_caption") or "").strip()
    event_map_caption = cap or None
    risk_all = risk_register_list_from_plan(plan)
    risk_filled = [r for r in risk_all if risk_row_nonempty(r)]
    doc_version_label = plan_pdf_revision_display_label(plan)
    diagram_pdf_rows = diagram_rows_with_pdf_src(diagram_rows)
    return render_template(
        "pdf/crm_event_plan.html",
        plan=plan,
        questions=questions,
        answers=answers,
        hospital_facilities=hospital_facilities_from_plan(plan),
        clinical_handover=parse_clinical_handover(plan.get("clinical_handover_json")),
        branding=branding,
        crm_pdf=crm_pdf,
        pdf_labels=pdf_labels,
        staff_roster=roster_filled,
        roster_headcount=len(roster_filled),
        roster_role_counts=_roster_role_counts(roster),
        event_map_pdf_src=event_map_pdf_src,
        event_map_caption=event_map_caption,
        risk_register_rows=risk_filled,
        generated_at=datetime.utcnow(),
        doc_version_label=doc_version_label,
        diagram_pdf_rows=diagram_pdf_rows,
        equipment_kit_url=equipment_kit_url or None,
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
