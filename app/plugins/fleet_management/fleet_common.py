"""Shared Fleet plugin auth helpers (used by admin routes and /VDIs)."""

from __future__ import annotations

from functools import wraps

from flask import flash, redirect, url_for
from flask_login import current_user

from app.objects import has_permission

PERM_ACCESS = "fleet_management.access"
PERM_EDIT = "fleet_management.edit"
PERM_DRIVER = "fleet_management.driver"
PERM_TRANSACT = "fleet_management.transactions"


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
    return has_permission(PERM_ACCESS) or has_permission(PERM_DRIVER)


def can_edit() -> bool:
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return has_permission(PERM_EDIT)


def can_fleet_transactions() -> bool:
    """Inventory-linked moves from fleet (equipment return, parts with stock deduct)."""
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return has_permission(PERM_TRANSACT) or has_permission(PERM_EDIT)


def can_driver() -> bool:
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return has_permission(PERM_DRIVER) or has_permission(PERM_EDIT)


def can_record_safety_check() -> bool:
    """Workshop safety checks: anyone who can open fleet admin (access or driver)."""
    return can_access()


def can_submit_vdi() -> bool:
    """Crew: driver permission or full access."""
    return can_access()


def fleet_access_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not can_access():
            flash("You do not have access to Fleet Management.", "danger")
            return redirect(url_for("routes.dashboard"))
        return f(*args, **kwargs)

    return wrapper


def fleet_edit_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not can_edit():
            flash("You do not have permission to edit fleet records.", "danger")
            return redirect(url_for("fleet_management.dashboard"))
        return f(*args, **kwargs)

    return wrapper


def fleet_transact_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not can_fleet_transactions():
            flash(
                "You do not have permission for fleet stock moves (return equipment / parts).",
                "danger",
            )
            return redirect(url_for("fleet_management.dashboard"))
        return f(*args, **kwargs)

    return wrapper


def fleet_driver_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not can_driver():
            flash("You do not have driver fleet permissions.", "danger")
            return redirect(url_for("routes.dashboard"))
        return f(*args, **kwargs)

    return wrapper


def fleet_safety_record_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not can_record_safety_check():
            flash("You do not have permission to record fleet safety checks.", "danger")
            return redirect(url_for("fleet_management.dashboard"))
        return f(*args, **kwargs)

    return wrapper


def vdi_submit_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not can_submit_vdi():
            flash("You do not have access to vehicle inspections.", "danger")
            return redirect(url_for("routes.dashboard"))
        return f(*args, **kwargs)

    return wrapper
