"""Core site branding for CRM PDFs (company name + logo from manifest)."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from flask import current_app


def get_site_branding(app=None) -> dict[str, Any]:
    """
    Returns company_name, logo_path (relative static), logo_abs_path or None,
    logo_file_uri for WeasyPrint (file:///...) or None.
    """
    app = app or current_app
    name = "Sparrow ERP"
    logo_rel = ""
    try:
        from app.objects import PluginManager

        plugins_dir = os.path.join(app.root_path, "plugins")
        pm = PluginManager(plugins_dir)
        core = pm.get_core_manifest() or {}
        ss = core.get("site_settings") or {}
        if isinstance(ss, dict):
            name = (ss.get("company_name") or "").strip() or name
            logo_rel = (ss.get("logo_path") or "").strip().lstrip("/\\")
    except Exception:
        pass

    logo_abs = None
    logo_uri = None
    if logo_rel:
        static_root = Path(app.root_path) / "static"
        candidate = (static_root / logo_rel).resolve()
        try:
            static_root = static_root.resolve()
            if candidate.is_file() and str(candidate).startswith(str(static_root)):
                logo_abs = str(candidate)
                logo_uri = candidate.as_uri()
        except Exception:
            pass

    # Relative to static root — use with WeasyPrint ``base_url`` = static dir file URI + "/"
    logo_pdf_src = logo_rel.replace("\\", "/") if logo_rel else None

    return {
        "company_name": name,
        "logo_path": logo_rel,
        "logo_abs_path": logo_abs,
        "logo_file_uri": logo_uri,
        "logo_pdf_src": logo_pdf_src,
    }
