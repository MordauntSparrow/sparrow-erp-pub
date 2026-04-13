"""Merge global VDI schema JSON from admin POST (main inspection + contractor portal forms)."""

from __future__ import annotations

from typing import Any, Dict, List

from werkzeug.datastructures import ImmutableMultiDict

from app.plugins.fleet_management.safety_schema_build import (
    parse_additional_contractor_forms_from_post,
)
from app.plugins.fleet_management.vdi_schema_form import schema_from_builder_form


def merge_vdi_schema_from_post(form: ImmutableMultiDict) -> Dict[str, Any]:
    """
    Primary VDI checklist (drivers /VDIs) plus optional additional_forms[]
    for employee-portal role-targeted inspections (same shape as former safety extras).
    """
    base = schema_from_builder_form(form)
    additional: List[Dict[str, Any]] = parse_additional_contractor_forms_from_post(form)
    base["additional_forms"] = additional
    return base
