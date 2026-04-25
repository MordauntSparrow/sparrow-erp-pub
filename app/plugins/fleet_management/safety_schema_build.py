"""Merge fleet workshop safety JSON from admin POST (per vehicle type).

Contractor-only portal forms are edited under the global VDI schema, not here.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

from werkzeug.datastructures import ImmutableMultiDict

from app.plugins.fleet_management.vdi_schema_form import (
    _slug,
    schema_from_builder_form,
    schema_from_builder_form_prefixed,
)

PRIMARY_FORM_KEY = "workshop"
_RESERVED_FORM_IDS = frozenset({PRIMARY_FORM_KEY, "primary", "default"})


def _portal_roles_from_text(raw: str) -> List[str]:
    if not raw or not str(raw).strip():
        return []
    out: List[str] = []
    for part in re.split(r"[,;\n]+", str(raw)):
        t = part.strip().lower()
        if t:
            out.append(t)
    return out


def parse_additional_contractor_forms_from_post(
    form: ImmutableMultiDict,
) -> List[Dict[str, Any]]:
    """
    Parse xf_* POST blocks into additional_forms[] (contractor portal, role-targeted).
    """
    additional: List[Dict[str, Any]] = []
    seen_ids: set[str] = {PRIMARY_FORM_KEY}

    for i in range(0, 40):
        px = f"xf_{i}_"
        raw_id = (form.get(f"{px}form_id") or "").strip().lower()
        if not raw_id:
            continue
        form_id = _slug(raw_id, fallback=f"form_{i}")
        if form_id in _RESERVED_FORM_IDS:
            raise ValueError(
                f'Form id “{form_id}” is reserved. Use another id (not “{PRIMARY_FORM_KEY}”).'
            )
        if form_id in seen_ids:
            raise ValueError(f"Duplicate contractor form id “{form_id}”.")
        seen_ids.add(form_id)

        inner = schema_from_builder_form_prefixed(form, px)
        roles = _portal_roles_from_text(form.get(f"{px}portal_roles") or "")
        if not roles:
            raise ValueError(
                f'Contractor form “{form_id}” needs at least one portal role '
                "(comma-separated, e.g. driver, delivery)."
            )
        additional.append(
            {
                "form_id": form_id,
                "title": inner.get("title") or form_id.replace("_", " ").title(),
                "portal_roles": roles,
                "version": inner.get("version", 1),
                "sections": inner.get("sections") or [],
            }
        )

    return additional


def merge_vehicle_type_safety_schema_from_post(form: ImmutableMultiDict) -> Dict[str, Any]:
    """
    Primary workshop form + optional portal_roles for the employee portal.
    """
    primary = schema_from_builder_form(form)
    pr = _portal_roles_from_text(form.get("primary_portal_roles") or "")
    if pr:
        primary["portal_roles"] = pr
    elif "portal_roles" in primary:
        del primary["portal_roles"]

    primary.pop("additional_forms", None)

    return primary
