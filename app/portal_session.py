"""
Unified employee / contractor portal session (PRD: principal on ``session['tb_user']``).

- ``id`` / ``contractor_id``: ``tb_contractors.id`` (required for portal DB writes).
- ``principal_source``: ``contractor_direct`` | ``user_linked`` | ``support_shadow``.
- ``linked_user_id``: core ``users.id`` when login was via core account.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

TB_USER_SESSION_KEY = "tb_user"

PRINCIPAL_CONTRACTOR_DIRECT = "contractor_direct"
PRINCIPAL_USER_LINKED = "user_linked"
PRINCIPAL_SUPPORT_SHADOW = "support_shadow"


def normalize_tb_user(tb_user: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Return a copy of ``tb_user`` with canonical principal fields for templates and services.
    Legacy sessions without ``principal_source`` are treated as contractor-direct (or shadow).
    """
    if not tb_user:
        return None
    out = dict(tb_user)
    cid = out.get("id")
    if cid is not None:
        out.setdefault("contractor_id", int(cid))
    ps = (out.get("principal_source") or "").strip()
    if not ps:
        if out.get("support_shadow"):
            out["principal_source"] = PRINCIPAL_SUPPORT_SHADOW
        else:
            out["principal_source"] = PRINCIPAL_CONTRACTOR_DIRECT
    out.setdefault("linked_user_id", None)
    return out


def contractor_id_from_tb_user(tb_user: Optional[Dict[str, Any]]) -> Optional[int]:
    n = normalize_tb_user(tb_user)
    if not n or n.get("id") is None:
        return None
    return int(n["id"])


def linked_core_user_id_from_tb_user(tb_user: Optional[Dict[str, Any]]) -> Optional[str]:
    n = normalize_tb_user(tb_user)
    if not n:
        return None
    lid = n.get("linked_user_id")
    return str(lid) if lid else None


def build_tb_user_session_payload(
    contractor_row: Dict[str, Any],
    *,
    principal_source: str,
    linked_user_id: Optional[str] = None,
    support_shadow: bool = False,
) -> Dict[str, Any]:
    """Build the canonical ``session['tb_user']`` dict (shared portal + time-billing)."""
    from app.objects import get_contractor_effective_role

    try:
        from app.plugins.employee_portal_module.services import safe_profile_picture_path

        safe_avatar = safe_profile_picture_path(
            contractor_row.get("profile_picture_path"))
    except Exception:
        safe_avatar = None
    cid = int(contractor_row["id"])
    role = get_contractor_effective_role(cid)
    display = (contractor_row.get("name") or "").strip() or (
        contractor_row.get("email") or ""
    )
    payload: Dict[str, Any] = {
        "id": cid,
        "contractor_id": cid,
        "email": contractor_row["email"],
        "username": (contractor_row.get("username") or "").strip() or None,
        "name": display,
        "initials": (contractor_row.get("initials") or "").strip(),
        "profile_picture_path": safe_avatar,
        "role": role,
        "principal_source": principal_source,
        "linked_user_id": linked_user_id,
    }
    if support_shadow:
        payload["support_shadow"] = True
        payload["principal_source"] = PRINCIPAL_SUPPORT_SHADOW
    return payload


def _contractor_row_active(row: Optional[Dict[str, Any]]) -> bool:
    if not row:
        return False
    return str(row.get("status", "")).lower() in (
        "active", "1", "true", "yes",
    )


def attempt_unified_employee_login(
    login_key: str,
    password: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    PRD login order: core ``users`` first (when not billable_exempt), then ``tb_contractors``.

    Returns:
        ``(session_payload, None)`` on success;
        ``(None, error_message)`` on failure (user-visible): link/integrity messages or generic invalid.
    """
    from app.objects import (
        AuthManager,
        backfill_users_contractor_link_from_contractor_email,
        find_sparrow_users_for_portal_login,
        find_tb_contractors_for_portal_login,
        resolve_or_link_contractor_for_portal_user,
    )

    login_key = (login_key or "").strip().lower()
    password = password or ""
    generic_invalid = "Invalid username or password."
    inactive_msg = "Your account is inactive. Please contact an administrator."

    if not login_key or not password:
        return None, generic_invalid

    user_rows = find_sparrow_users_for_portal_login(login_key)
    if len(user_rows) > 1:
        return None, generic_invalid

    if len(user_rows) == 1:
        ud = user_rows[0]
        if not int(ud.get("billable_exempt") or 0):
            pw_h = (ud.get("password_hash") or "").strip()
            if pw_h and AuthManager.verify_password(pw_h, password):
                u, link_err = resolve_or_link_contractor_for_portal_user(ud)
                if link_err:
                    return None, link_err
                if not u:
                    return None, generic_invalid
                if not _contractor_row_active(u):
                    return None, inactive_msg
                payload = build_tb_user_session_payload(
                    u,
                    principal_source=PRINCIPAL_USER_LINKED,
                    linked_user_id=str(ud["id"]),
                )
                return payload, None
            if pw_h:
                return None, generic_invalid

    rows = find_tb_contractors_for_portal_login(login_key)
    if len(rows) > 1:
        return None, generic_invalid
    u = rows[0] if rows else None

    if not u or not u.get("password_hash") or not AuthManager.verify_password(
        u["password_hash"], password
    ):
        return None, generic_invalid

    if not _contractor_row_active(u):
        return None, inactive_msg

    payload = build_tb_user_session_payload(
        u,
        principal_source=PRINCIPAL_CONTRACTOR_DIRECT,
        linked_user_id=None,
    )
    try:
        backfill_users_contractor_link_from_contractor_email(
            int(u["id"]), (u.get("email") or ""),
        )
    except Exception:
        pass
    return payload, None
