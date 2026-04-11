"""Medical event plan PDF (WeasyPrint + Jinja) with core branding."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import render_template

from .crm_branding import get_site_branding
from .crm_clinical_handover import parse_clinical_handover
from .crm_static_paths import (
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


def build_event_plan_html(app, plan: dict[str, Any], questions: list[dict], answers: dict) -> str:
    branding = get_site_branding(app)
    return render_template(
        "pdf/crm_event_plan.html",
        plan=plan,
        questions=questions,
        answers=answers,
        hospital_facilities=hospital_facilities_from_plan(plan),
        clinical_handover=parse_clinical_handover(plan.get("clinical_handover_json")),
        branding=branding,
        generated_at=datetime.utcnow(),
    )


def write_event_plan_pdf(app, plan: dict[str, Any], questions: list[dict], answers: dict) -> tuple[bytes, str]:
    """Return (pdf_bytes, sha256_hex)."""
    html = build_event_plan_html(app, plan, questions, answers)
    try:
        from weasyprint import HTML
    except ImportError as e:
        raise RuntimeError("WeasyPrint is required for event plan PDF") from e

    static = crm_static_dir_for_app(app)
    pdf_bytes = HTML(string=html, base_url=static).write_pdf()
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
