"""
Public marketing-site pages for organisational documents (opt-in per policy).

Mounted at site root: ``/policies``, ``/policies/<slug>/file``.
Templates live under ``website_module/templates/public`` so they extend
``website_public_base.html`` with the same shell as the rest of the site.
"""
from __future__ import annotations

import datetime
import os

from flask import Blueprint, abort, render_template, send_file

from . import services as comp_svc

_WB_PUBLIC_TEMPLATES = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "website_module", "templates", "public")
)

website_compliance_policies_bp = Blueprint(
    "website_compliance_policies",
    __name__,
    url_prefix="",
    template_folder=_WB_PUBLIC_TEMPLATES,
)


def _app_static_dir() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "static"))


def _website_shell_ctx() -> dict:
    from app.plugins.website_module.routes import (
        build_website_public_config,
        get_website_settings,
        _load_pages_json,
    )

    return {
        "website_settings": get_website_settings(),
        "config": build_website_public_config(),
        "pages": _load_pages_json(),
        "current_year": datetime.date.today().year,
    }


@website_compliance_policies_bp.get("/policies")
def website_public_policies_index():
    rows = comp_svc.list_website_public_policies()
    ctx = _website_shell_ctx()
    return render_template(
        "compliance_website_policies_index.html",
        policies=rows,
        title="Published documents",
        description=(
            "Official documents published for transparency. "
            "Content is maintained from our central policy library."
        ),
        keywords="policies, governance, transparency, organisation",
        **ctx,
    )


@website_compliance_policies_bp.get("/policies/<slug>/file")
def website_public_policy_file(slug: str):
    row = comp_svc.get_website_public_policy_by_slug(slug)
    if not row or not row.get("file_path"):
        abort(404)
    rel = str(row["file_path"]).replace("\\", "/").lstrip("/")
    if ".." in rel.split("/"):
        abort(404)
    root = os.path.abspath(_app_static_dir())
    full = os.path.abspath(os.path.join(root, *rel.split("/")))
    try:
        if os.path.normcase(os.path.commonpath([full, root])) != os.path.normcase(root):
            abort(404)
    except ValueError:
        abort(404)
    if not os.path.isfile(full):
        abort(404)
    return send_file(full, as_attachment=False)


@website_compliance_policies_bp.get("/policies/<slug>")
def website_public_policy_detail(slug: str):
    row = comp_svc.get_website_public_policy_by_slug(slug)
    if not row:
        abort(404)
    topic_display = comp_svc.policy_topic_display(row)
    ctx = _website_shell_ctx()
    title = (row.get("title") or "Document").strip()
    return render_template(
        "compliance_website_policy_detail.html",
        policy=row,
        topic_display=topic_display,
        title=title,
        description=f"{title} — official published document.",
        keywords="policy, document, governance",
        **ctx,
    )
