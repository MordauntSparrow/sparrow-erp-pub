"""CRM plugin auth helpers (admin UI)."""

from __future__ import annotations

from functools import wraps

from flask import current_app, flash, redirect, url_for
from flask_login import current_user

from app.objects import has_permission
from app.organization_profile import (
    normalize_organization_industries,
    tenant_matches_industry,
)

PERM_ACCESS = "crm_module.access"
PERM_EDIT = "crm_module.edit"


def uid() -> str | None:
    if not getattr(current_user, "is_authenticated", False):
        return None
    return str(getattr(current_user, "id", "") or "") or None


def can_access() -> bool:
    if not getattr(current_user, "is_authenticated", False):
        return False
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return has_permission(PERM_ACCESS) or has_permission(PERM_EDIT)


def can_edit() -> bool:
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return has_permission(PERM_EDIT)


def crm_access_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not can_access():
            flash("You do not have access to CRM.", "danger")
            return redirect(url_for("routes.dashboard"))
        return f(*args, **kwargs)

    return wrapper


def crm_edit_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not can_edit():
            flash("You do not have permission to change CRM records.", "danger")
            return redirect(url_for("crm_module.dashboard"))
        return f(*args, **kwargs)

    return wrapper


def crm_medical_surface_available() -> bool:
    """Event medical plans, Cura handoff, and related CRM surfaces (tenant industry profile)."""
    raw = current_app.config.get("organization_industries")
    ids = normalize_organization_industries(raw)
    return tenant_matches_industry(ids, "medical")


def crm_medical_surface_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not crm_medical_surface_available():
            flash(
                "Event medical plans are available when Medical is enabled under "
                "Core settings → General (Industry & categories).",
                "info",
            )
            return redirect(url_for("crm_module.dashboard"))
        return f(*args, **kwargs)

    return wrapper
