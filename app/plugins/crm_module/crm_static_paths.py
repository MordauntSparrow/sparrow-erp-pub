"""
Resolve ``app/static`` for CRM uploads (same idea as HR: avoid trusting ``root_path`` alone).

PDFs and downloads use ``uploads/crm_event_plans/…`` under the active static root, with
fallback roots for legacy layouts.
"""
from __future__ import annotations

import os
from pathlib import Path


def crm_app_package_static_dir() -> str:
    """``…/app/static`` derived from this plugin file location."""
    return str(Path(__file__).resolve().parent.parent.parent / "static")


def crm_static_dir_for_app(app) -> str:
    sf = getattr(app, "static_folder", None) if app is not None else None
    if sf:
        return os.path.abspath(sf)
    root = getattr(app, "root_path", None) if app is not None else None
    if root:
        return os.path.abspath(os.path.join(root, "static"))
    return crm_app_package_static_dir()


def crm_event_plan_pdf_relative_subpath() -> str:
    return "uploads/crm_event_plans"


def crm_event_plan_pdf_write_dir(app) -> str:
    return os.path.join(crm_static_dir_for_app(app), crm_event_plan_pdf_relative_subpath())


def _normalize_upload_rel(rel: str) -> str | None:
    rel = (rel or "").replace("\\", "/").strip().lstrip("/")
    if not rel or ".." in rel.split("/"):
        return None
    if rel.startswith("static/"):
        rel = rel[7:].lstrip("/")
    return rel


def _crm_static_search_roots(app) -> list[str]:
    roots: list[str] = []
    primary = os.path.abspath(crm_static_dir_for_app(app))
    roots.append(primary)
    pkg = os.path.abspath(crm_app_package_static_dir())
    if pkg not in roots:
        roots.append(pkg)
    # Repo root ``static/`` (legacy), sibling of ``app/``
    legacy = os.path.abspath(os.path.join(pkg, "..", "..", "static"))
    if legacy not in roots:
        roots.append(legacy)
    return roots


def crm_resolve_event_plan_pdf_path(app, rel: str) -> str | None:
    """Return absolute file path if ``rel`` is a whitelisted CRM plan PDF under a known static root."""
    rel = _normalize_upload_rel(rel)
    if not rel or not rel.startswith(f"{crm_event_plan_pdf_relative_subpath()}/"):
        return None
    segs = [s for s in rel.split("/") if s]
    for root in _crm_static_search_roots(app):
        candidate = os.path.abspath(os.path.join(root, *segs))
        try:
            if os.path.commonpath([candidate, root]) != root:
                continue
        except ValueError:
            continue
        if os.path.isfile(candidate):
            return candidate
    return None
