"""
Business logic for core accounting integrations (UI + future CRM invoice push).

Scopes: see ``docs/integrations/scopes.md``.
"""
from __future__ import annotations

import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from flask import has_request_context, request, session

from app.core_integrations import crypto, repository
from app.core_integrations.providers import (
    freshbooks_build_authorize_url,
    freshbooks_exchange_code,
    freshbooks_get_profile_label,
    freshbooks_refresh_token,
    freeagent_build_authorize_url,
    freeagent_create_draft_invoice,
    freeagent_exchange_code,
    freeagent_get_company,
    freeagent_refresh_token,
    parse_expires_at,
    parse_refresh_expires_at,
    quickbooks_build_authorize_url,
    quickbooks_exchange_code,
    quickbooks_get_company_label,
    quickbooks_refresh_token,
    sage_build_authorize_url,
    sage_exchange_code,
    sage_first_business,
    sage_refresh_token,
    xero_build_authorize_url,
    xero_create_draft_invoice,
    xero_exchange_code,
    xero_list_connections,
    xero_refresh_token,
)

# Must match docs/integrations/scopes.md
XERO_CONNECT_SCOPES = (
    "openid offline_access profile email "
    "accounting.settings accounting.contacts accounting.transactions"
)


def _merge_oauth_app_creds(
    env_id: str | None, env_secret: str | None, provider: str
) -> tuple[str, str]:
    """Prefer a full env pair; otherwise load encrypted pair from ``core_integration_oauth_clients``."""
    eid = (env_id or "").strip()
    esec = (env_secret or "").strip()
    if eid and esec:
        return eid, esec
    return repository.load_oauth_client_credentials(provider)


def _xero_creds() -> tuple[str, str]:
    return _merge_oauth_app_creds(
        os.environ.get("XERO_CLIENT_ID"),
        os.environ.get("XERO_CLIENT_SECRET"),
        "xero",
    )


def _freeagent_creds() -> tuple[str, str]:
    return _merge_oauth_app_creds(
        os.environ.get("FREEAGENT_CLIENT_ID"),
        os.environ.get("FREEAGENT_CLIENT_SECRET"),
        "freeagent",
    )


def oauth_app_configured(provider: str) -> bool:
    """True when this provider has both client id and secret (env or database)."""
    p = (provider or "").strip().lower()
    if p == "xero":
        cid, sec = _xero_creds()
    elif p == "freeagent":
        cid, sec = _freeagent_creds()
    else:
        return False
    return bool(cid and sec)


def _quickbooks_creds() -> tuple[str, str]:
    cid = (os.environ.get("QUICKBOOKS_CLIENT_ID") or os.environ.get("INTUIT_CLIENT_ID") or "").strip()
    sec = (
        os.environ.get("QUICKBOOKS_CLIENT_SECRET")
        or os.environ.get("INTUIT_CLIENT_SECRET")
        or ""
    ).strip()
    return cid, sec


def _sage_creds() -> tuple[str, str]:
    cid = (os.environ.get("SAGE_CLIENT_ID") or "").strip()
    sec = (os.environ.get("SAGE_CLIENT_SECRET") or "").strip()
    return cid, sec


def _freshbooks_creds() -> tuple[str, str]:
    cid = (os.environ.get("FRESHBOOKS_CLIENT_ID") or "").strip()
    sec = (os.environ.get("FRESHBOOKS_CLIENT_SECRET") or "").strip()
    return cid, sec


def oauth_callback_base_url() -> str:
    from app.public_base import resolve_public_base_url

    base = (resolve_public_base_url() or "").strip().rstrip("/")
    if base:
        return base
    if request:
        return request.url_root.rstrip("/")
    return ""


def oauth_redirect_uri(provider: str) -> str:
    return f"{oauth_callback_base_url()}/core/integrations/oauth/{provider}/callback"


def start_oauth(provider: str) -> str:
    """Return redirect URL to provider authorize page; stores CSRF state in session."""
    if provider not in repository.PROVIDERS:
        raise ValueError("unsupported provider")
    state = secrets.token_urlsafe(32)
    session[f"core_int_oauth_state_{provider}"] = state
    session.modified = True
    redir = oauth_redirect_uri(provider)
    if provider == "xero":
        cid, sec = _xero_creds()
        if not cid or not sec:
            raise RuntimeError(
                "Xero OAuth app is not configured. Set XERO_CLIENT_ID and XERO_CLIENT_SECRET "
                "in the environment, or save them on this page (Integrations → Xero card)."
            )
        return xero_build_authorize_url(
            client_id=cid,
            redirect_uri=redir,
            state=state,
            scopes=XERO_CONNECT_SCOPES,
        )
    if provider == "freeagent":
        cid, sec = _freeagent_creds()
        if not cid or not sec:
            raise RuntimeError(
                "FreeAgent OAuth app is not configured. Set FREEAGENT_CLIENT_ID and "
                "FREEAGENT_CLIENT_SECRET in the environment, or save them on this page."
            )
        return freeagent_build_authorize_url(client_id=cid, redirect_uri=redir, state=state)
    if provider == "quickbooks":
        cid, _ = _quickbooks_creds()
        if not cid:
            raise RuntimeError(
                "QUICKBOOKS_CLIENT_ID (or INTUIT_CLIENT_ID) is not configured for this deployment."
            )
        return quickbooks_build_authorize_url(
            client_id=cid, redirect_uri=redir, state=state, scopes=None
        )
    if provider == "sage":
        cid, _ = _sage_creds()
        if not cid:
            raise RuntimeError("SAGE_CLIENT_ID is not configured for this deployment.")
        return sage_build_authorize_url(client_id=cid, redirect_uri=redir, state=state)
    if provider == "freshbooks":
        cid, _ = _freshbooks_creds()
        if not cid:
            raise RuntimeError("FRESHBOOKS_CLIENT_ID is not configured for this deployment.")
        return freshbooks_build_authorize_url(client_id=cid, redirect_uri=redir, state=state)
    raise ValueError("unsupported provider")


def validate_oauth_state(provider: str, state: str | None) -> bool:
    key = f"core_int_oauth_state_{provider}"
    if not state:
        return False
    expected = session.pop(key, None)
    session.modified = True
    return bool(expected) and secrets.compare_digest(str(state), str(expected))


def _persist_tokens(
    *,
    provider: str,
    user_id: str,
    token_json: dict[str, Any],
    org_id: str | None,
    org_label: str | None,
    raw_token_response: dict[str, Any] | None = None,
) -> None:
    enc = crypto.encrypt_token_payload(token_json)
    raw = raw_token_response or token_json
    repository.upsert_connection_row(
        provider=provider,
        status="connected",
        encrypted_tokens=enc,
        provider_org_id=org_id,
        provider_org_label=org_label,
        connected_by_user_id=user_id,
        last_error=None,
        last_api_success_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        refresh_token_expires_at=parse_refresh_expires_at(raw),
    )


def complete_oauth_xero(*, user_id: str, code: str) -> None:
    cid, csec = _xero_creds()
    if not cid or not csec:
        raise RuntimeError("Xero client credentials are not configured.")
    redir = oauth_redirect_uri("xero")
    tok = xero_exchange_code(client_id=cid, client_secret=csec, code=code, redirect_uri=redir)
    access = (tok.get("access_token") or "").strip()
    refresh = (tok.get("refresh_token") or "").strip()
    if not access or not refresh:
        raise RuntimeError("Xero did not return tokens.")
    payload = _attach_expiry(
        {
            "access_token": access,
            "refresh_token": refresh,
            "expires_in": tok.get("expires_in"),
            "token_type": tok.get("token_type"),
        },
        tok,
    )
    conns = xero_list_connections(access)
    tenant_id = None
    tenant_name = None
    if conns:
        tenant_id = str(conns[0].get("tenantId") or "").strip() or None
        tenant_name = str(conns[0].get("tenantName") or "").strip() or None
    _persist_tokens(
        provider="xero",
        user_id=user_id,
        token_json=payload,
        org_id=tenant_id,
        org_label=tenant_name or tenant_id or "Xero",
        raw_token_response=tok,
    )


def complete_oauth_freeagent(*, user_id: str, code: str) -> None:
    cid, csec = _freeagent_creds()
    if not cid or not csec:
        raise RuntimeError("FreeAgent client credentials are not configured.")
    redir = oauth_redirect_uri("freeagent")
    tok = freeagent_exchange_code(
        client_id=cid, client_secret=csec, code=code, redirect_uri=redir
    )
    access = (tok.get("access_token") or "").strip()
    refresh = (tok.get("refresh_token") or "").strip()
    if not access or not refresh:
        raise RuntimeError("FreeAgent did not return tokens.")
    payload = _attach_expiry(
        {
            "access_token": access,
            "refresh_token": refresh,
            "expires_in": tok.get("expires_in"),
            "token_type": tok.get("token_type"),
            "refresh_token_expires_in": tok.get("refresh_token_expires_in"),
        },
        tok,
    )
    company = freeagent_get_company(access)
    comp = company.get("company") if isinstance(company.get("company"), dict) else company
    name = ""
    cid_str = ""
    if isinstance(comp, dict):
        name = str(comp.get("name") or "").strip()
        cid_str = str(comp.get("url") or comp.get("id") or "").strip()
    _persist_tokens(
        provider="freeagent",
        user_id=user_id,
        token_json=payload,
        org_id=cid_str or None,
        org_label=name or "FreeAgent",
        raw_token_response=tok,
    )


def complete_oauth_quickbooks(
    *, user_id: str, code: str, realm_id: str | None
) -> None:
    """Intuit redirects with ``realmId`` query param for QuickBooks Online."""
    cid, csec = _quickbooks_creds()
    if not cid or not csec:
        raise RuntimeError("QuickBooks client credentials are not configured.")
    rid = (realm_id or "").strip()
    if not rid:
        raise RuntimeError(
            "Missing QuickBooks company (realmId). Ensure your Intuit app redirect matches Sparrow and try again."
        )
    redir = oauth_redirect_uri("quickbooks")
    tok = quickbooks_exchange_code(
        client_id=cid, client_secret=csec, code=code, redirect_uri=redir
    )
    access = (tok.get("access_token") or "").strip()
    refresh = (tok.get("refresh_token") or "").strip()
    if not access or not refresh:
        raise RuntimeError("QuickBooks did not return tokens.")
    payload = _attach_expiry(
        {
            "access_token": access,
            "refresh_token": refresh,
            "expires_in": tok.get("expires_in"),
            "token_type": tok.get("token_type"),
            "realm_id": rid,
        },
        tok,
    )
    try:
        label = quickbooks_get_company_label(realm_id=rid, access_token=access)
    except Exception:
        label = rid
    _persist_tokens(
        provider="quickbooks",
        user_id=user_id,
        token_json=payload,
        org_id=rid,
        org_label=label or rid,
        raw_token_response=tok,
    )


def complete_oauth_sage(*, user_id: str, code: str) -> None:
    cid, csec = _sage_creds()
    if not cid or not csec:
        raise RuntimeError("Sage client credentials are not configured.")
    redir = oauth_redirect_uri("sage")
    tok = sage_exchange_code(
        client_id=cid, client_secret=csec, code=code, redirect_uri=redir
    )
    access = (tok.get("access_token") or "").strip()
    refresh = (tok.get("refresh_token") or "").strip()
    if not access or not refresh:
        raise RuntimeError("Sage did not return tokens.")
    payload = _attach_expiry(
        {
            "access_token": access,
            "refresh_token": refresh,
            "expires_in": tok.get("expires_in"),
            "token_type": tok.get("token_type"),
        },
        tok,
    )
    bid, name = sage_first_business(access)
    _persist_tokens(
        provider="sage",
        user_id=user_id,
        token_json=payload,
        org_id=bid,
        org_label=name or "Sage",
        raw_token_response=tok,
    )


def complete_oauth_freshbooks(*, user_id: str, code: str) -> None:
    cid, csec = _freshbooks_creds()
    if not cid or not csec:
        raise RuntimeError("FreshBooks client credentials are not configured.")
    redir = oauth_redirect_uri("freshbooks")
    tok = freshbooks_exchange_code(
        client_id=cid, client_secret=csec, code=code, redirect_uri=redir
    )
    access = (tok.get("access_token") or "").strip()
    refresh = (tok.get("refresh_token") or "").strip()
    if not access or not refresh:
        raise RuntimeError("FreshBooks did not return tokens.")
    payload = _attach_expiry(
        {
            "access_token": access,
            "refresh_token": refresh,
            "expires_in": tok.get("expires_in"),
            "token_type": tok.get("token_type"),
        },
        tok,
    )
    uid, label = freshbooks_get_profile_label(access)
    _persist_tokens(
        provider="freshbooks",
        user_id=user_id,
        token_json=payload,
        org_id=uid,
        org_label=label or "FreshBooks",
        raw_token_response=tok,
    )


def disconnect_provider(*, provider: str, user_id: str | None) -> None:
    repository.clear_connection(provider)
    repository.append_event(
        event_type="disconnect",
        provider=provider,
        message="Integration disconnected; local tokens removed.",
        user_id=user_id,
        ip=(request.remote_addr if request else None) or None,
        user_agent=(request.headers.get("User-Agent") if request else None) or None,
    )


def _token_needs_refresh(payload: dict[str, Any]) -> bool:
    exp = payload.get("expires_at")
    if not exp:
        return True
    try:
        dt = datetime.strptime(str(exp)[:19], "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return True
    return datetime.now(timezone.utc) >= dt - timedelta(seconds=120)


def _attach_expiry(payload: dict[str, Any], token_response: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    exp = parse_expires_at(token_response)
    if exp:
        out["expires_at"] = exp
    if token_response.get("refresh_token"):
        out["refresh_token"] = str(token_response["refresh_token"]).strip()
    elif out.get("refresh_token"):
        out["refresh_token"] = str(out["refresh_token"]).strip()
    if token_response.get("access_token"):
        out["access_token"] = str(token_response["access_token"]).strip()
    rt_exp = parse_refresh_expires_at(token_response)
    if rt_exp:
        out["refresh_expires_at"] = rt_exp
    return out


def refresh_access_token(provider: str, row: dict[str, Any]) -> dict[str, Any]:
    """Decrypt row, refresh if needed, persist new tokens; return decrypted payload or raise."""
    blob = row.get("encrypted_tokens")
    payload = crypto.decrypt_token_payload(blob)
    refresh = (payload.get("refresh_token") or "").strip()
    if not refresh:
        raise RuntimeError("No refresh token stored; reconnect required.")
    if provider == "xero":
        cid, csec = _xero_creds()
        if not cid or not csec:
            raise RuntimeError("Xero client credentials missing.")
        tok = xero_refresh_token(client_id=cid, client_secret=csec, refresh_token=refresh)
    elif provider == "freeagent":
        cid, csec = _freeagent_creds()
        if not cid or not csec:
            raise RuntimeError("FreeAgent client credentials missing.")
        tok = freeagent_refresh_token(client_id=cid, client_secret=csec, refresh_token=refresh)
    elif provider == "quickbooks":
        cid, csec = _quickbooks_creds()
        if not cid or not csec:
            raise RuntimeError("QuickBooks client credentials missing.")
        tok = quickbooks_refresh_token(client_id=cid, client_secret=csec, refresh_token=refresh)
    elif provider == "sage":
        cid, csec = _sage_creds()
        if not cid or not csec:
            raise RuntimeError("Sage client credentials missing.")
        tok = sage_refresh_token(client_id=cid, client_secret=csec, refresh_token=refresh)
    elif provider == "freshbooks":
        cid, csec = _freshbooks_creds()
        if not cid or not csec:
            raise RuntimeError("FreshBooks client credentials missing.")
        redir = oauth_redirect_uri("freshbooks")
        tok = freshbooks_refresh_token(
            client_id=cid,
            client_secret=csec,
            refresh_token=refresh,
            redirect_uri=redir,
        )
    else:
        raise RuntimeError("Unsupported provider for token refresh.")
    merged = _attach_expiry(payload, tok)
    enc = crypto.encrypt_token_payload(merged)
    repository.upsert_connection_row(
        provider=provider,
        status="connected",
        encrypted_tokens=enc,
        provider_org_id=row.get("provider_org_id"),
        provider_org_label=row.get("provider_org_label"),
        connected_by_user_id=row.get("connected_by_user_id"),
        last_error=None,
        last_api_success_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        refresh_token_expires_at=parse_refresh_expires_at(tok),
    )
    return merged


def run_health_probe(provider: str) -> dict[str, Any]:
    """
    Refresh token if needed, call a lightweight provider API, update last_api_success_at / errors.
    Returns a small dict for the UI: status, message, last_success.
    """
    row = repository.load_connection(provider)
    if not row or (row.get("status") or "") != "connected":
        return {"ok": False, "ui_status": "disconnected", "message": "Not connected."}
    payload = crypto.decrypt_token_payload(row.get("encrypted_tokens"))
    if not payload.get("access_token"):
        repository.upsert_connection_row(
            provider=provider,
            status="error",
            encrypted_tokens=row.get("encrypted_tokens"),
            provider_org_id=row.get("provider_org_id"),
            provider_org_label=row.get("provider_org_label"),
            connected_by_user_id=row.get("connected_by_user_id"),
            last_error="Missing access token. Reconnect required.",
            last_api_success_at=row.get("last_api_success_at"),
            refresh_token_expires_at=row.get("refresh_token_expires_at"),
        )
        return {"ok": False, "ui_status": "action_required", "message": "Reconnect required."}
    try:
        if _token_needs_refresh(payload):
            payload = refresh_access_token(provider, row)
            row = repository.load_connection(provider) or row
        if provider == "xero":
            access = payload.get("access_token")
            conns = xero_list_connections(str(access))
            label = row.get("provider_org_label")
            tid = row.get("provider_org_id")
            if conns:
                tid = str(conns[0].get("tenantId") or tid or "")
                label = str(conns[0].get("tenantName") or label or tid)
            enc = crypto.encrypt_token_payload(payload)
            repository.upsert_connection_row(
                provider=provider,
                status="connected",
                encrypted_tokens=enc,
                provider_org_id=tid,
                provider_org_label=label,
                connected_by_user_id=row.get("connected_by_user_id"),
                last_error=None,
                last_api_success_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                refresh_token_expires_at=row.get("refresh_token_expires_at"),
            )
        elif provider == "freeagent":
            access = payload.get("access_token")
            company = freeagent_get_company(str(access))
            comp = company.get("company") if isinstance(company.get("company"), dict) else company
            label = row.get("provider_org_label")
            oid = row.get("provider_org_id")
            if isinstance(comp, dict):
                label = str(comp.get("name") or label or "")
                oid = str(comp.get("url") or comp.get("id") or oid or "")
            enc = crypto.encrypt_token_payload(payload)
            repository.upsert_connection_row(
                provider=provider,
                status="connected",
                encrypted_tokens=enc,
                provider_org_id=oid,
                provider_org_label=label,
                connected_by_user_id=row.get("connected_by_user_id"),
                last_error=None,
                last_api_success_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                refresh_token_expires_at=row.get("refresh_token_expires_at"),
            )
        elif provider == "quickbooks":
            access = str(payload.get("access_token") or "")
            rid = str(
                payload.get("realm_id") or row.get("provider_org_id") or ""
            ).strip()
            if not rid:
                raise RuntimeError("Missing QuickBooks realmId in stored connection.")
            label = quickbooks_get_company_label(realm_id=rid, access_token=access)
            enc = crypto.encrypt_token_payload(payload)
            repository.upsert_connection_row(
                provider=provider,
                status="connected",
                encrypted_tokens=enc,
                provider_org_id=rid,
                provider_org_label=label,
                connected_by_user_id=row.get("connected_by_user_id"),
                last_error=None,
                last_api_success_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                refresh_token_expires_at=row.get("refresh_token_expires_at"),
            )
        elif provider == "sage":
            access = str(payload.get("access_token") or "")
            bid, name = sage_first_business(access)
            enc = crypto.encrypt_token_payload(payload)
            repository.upsert_connection_row(
                provider=provider,
                status="connected",
                encrypted_tokens=enc,
                provider_org_id=bid,
                provider_org_label=name,
                connected_by_user_id=row.get("connected_by_user_id"),
                last_error=None,
                last_api_success_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                refresh_token_expires_at=row.get("refresh_token_expires_at"),
            )
        elif provider == "freshbooks":
            access = str(payload.get("access_token") or "")
            uid, label = freshbooks_get_profile_label(access)
            enc = crypto.encrypt_token_payload(payload)
            repository.upsert_connection_row(
                provider=provider,
                status="connected",
                encrypted_tokens=enc,
                provider_org_id=uid,
                provider_org_label=label,
                connected_by_user_id=row.get("connected_by_user_id"),
                last_error=None,
                last_api_success_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                refresh_token_expires_at=row.get("refresh_token_expires_at"),
            )
        else:
            raise RuntimeError(f"Unsupported provider: {provider}")
        return {"ok": True, "ui_status": "healthy", "message": "Connected."}
    except Exception as e:
        msg = str(e).strip()[:500] or "Provider error"
        repository.upsert_connection_row(
            provider=provider,
            status="error",
            encrypted_tokens=row.get("encrypted_tokens"),
            provider_org_id=row.get("provider_org_id"),
            provider_org_label=row.get("provider_org_label"),
            connected_by_user_id=row.get("connected_by_user_id"),
            last_error=msg,
            last_api_success_at=row.get("last_api_success_at"),
            refresh_token_expires_at=row.get("refresh_token_expires_at"),
        )
        repository.append_event(
            event_type="health_error",
            provider=provider,
            message=msg,
            user_id=None,
            ip=(request.remote_addr if request else None) or None,
            user_agent=(request.headers.get("User-Agent") if request else None) or None,
        )
        return {"ok": False, "ui_status": "action_required", "message": msg}


def ui_status_for_row(row: Optional[dict[str, Any]]) -> str:
    if not row:
        return "disconnected"
    st = (row.get("status") or "disconnected").lower()
    if st == "error":
        return "action_required"
    if st == "connected":
        return "connected"
    return "disconnected"


def get_active_provider() -> str:
    """
    Default accounting provider if that integration is connected; else ``none``.
    For CRM / invoice push (future).
    """
    settings = repository.load_settings()
    pref = (settings.get("default_provider") or "none").lower()
    allowed = repository.ALLOWED_DEFAULT_PROVIDERS - {"none"}
    if pref not in allowed:
        return "none"
    row = repository.load_connection(pref)
    if row and (row.get("status") or "").lower() == "connected" and row.get("encrypted_tokens"):
        return pref
    return "none"


def _integration_audit_request_meta() -> tuple[str | None, str | None]:
    if not has_request_context():
        return None, None
    return (request.remote_addr or None), (request.headers.get("User-Agent") or None)


def _access_token_for_invoice_push(provider: str) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Return ``(access_token, decrypted_payload, connection_row)`` or raise."""
    row = repository.load_connection(provider)
    if not row or (row.get("status") or "").lower() != "connected":
        raise RuntimeError("Accounting provider is not connected.")
    payload = crypto.decrypt_token_payload(row.get("encrypted_tokens"))
    if not (payload.get("access_token") or "").strip():
        raise RuntimeError("Missing access token; reconnect required.")
    if _token_needs_refresh(payload):
        payload = refresh_access_token(provider, row)
        row = repository.load_connection(provider) or row
    access = str(payload.get("access_token") or "").strip()
    if not access:
        raise RuntimeError("Could not obtain access token.")
    return access, payload, row


def push_draft_invoice(sale_ref: str, **kwargs: Any) -> dict[str, Any]:
    """
    Create a draft sales invoice in the connected accounting app.

    ``sale_ref`` should be stable (e.g. ``crm_quote:123``) for idempotency keys and references.

    Keyword arguments:

    - ``provider`` — optional override; otherwise ``get_active_provider()`` is used.
    - ``contact_name`` — required customer / organisation label on the invoice.
    - ``contact_email`` — optional.
    - ``currency`` — ISO code, default ``GBP``.
    - ``line_items`` — list of dicts with ``description``, ``quantity``, ``unit_amount`` (decimal strings).
    """
    ref = (sale_ref or "").strip()
    if not ref:
        return {"ok": False, "reason": "invalid_ref", "message": "sale_ref is required."}

    contact_name = (kwargs.get("contact_name") or "").strip()
    if not contact_name:
        return {"ok": False, "reason": "missing_contact", "message": "contact_name is required."}

    raw_lines = kwargs.get("line_items")
    if not isinstance(raw_lines, list) or not raw_lines:
        return {"ok": False, "reason": "missing_lines", "message": "line_items must be a non-empty list."}

    override = (kwargs.get("provider") or "").strip().lower()
    provider = override if override in repository.PROVIDERS else ""
    if not provider or provider == "none":
        provider = get_active_provider()
    if provider in ("none", ""):
        return {
            "ok": False,
            "reason": "no_provider",
            "message": "No default accounting provider is connected. Open Integrations to connect Xero or FreeAgent.",
        }

    if provider not in ("xero", "freeagent"):
        return {
            "ok": False,
            "reason": "unsupported_provider",
            "message": f"Draft invoice push is not implemented for {provider} yet.",
        }

    norm_lines: list[dict[str, str]] = []
    for i, row in enumerate(raw_lines):
        if not isinstance(row, dict):
            continue
        desc = str(row.get("description") or "").strip()
        if not desc:
            continue
        qty = str(row.get("quantity") or "1").strip() or "1"
        amt = str(row.get("unit_amount") or row.get("unit_price") or "0").strip() or "0"
        norm_lines.append({"description": desc[:512], "quantity": qty, "unit_amount": amt})
    if not norm_lines:
        return {
            "ok": False,
            "reason": "missing_lines",
            "message": "line_items must include at least one line with a description.",
        }

    currency = (kwargs.get("currency") or "GBP").strip().upper() or "GBP"
    contact_email = (kwargs.get("contact_email") or "").strip() or None

    try:
        access, _payload, conn_row = _access_token_for_invoice_push(provider)
    except Exception as e:
        msg = str(e).strip()[:500] or "Token error"
        return {"ok": False, "reason": "auth", "message": msg}

    try:
        if provider == "xero":
            tenant_id = str(conn_row.get("provider_org_id") or "").strip()
            if not tenant_id:
                raise RuntimeError("Missing Xero organisation id; run Health check on Integrations.")
            out = xero_create_draft_invoice(
                access_token=access,
                tenant_id=tenant_id,
                contact_name=contact_name,
                contact_email=contact_email,
                reference=ref,
                currency=currency,
                line_items=norm_lines,
                idempotency_key=ref[:120],
            )
        else:
            out = freeagent_create_draft_invoice(
                access_token=access,
                contact_name=contact_name,
                contact_email=contact_email,
                reference=ref,
                currency=currency,
                line_items=norm_lines,
            )
    except Exception as e:
        msg = str(e).strip()[:500] or "Provider API error"
        _ip, _ua = _integration_audit_request_meta()
        repository.append_event(
            event_type="draft_invoice_error",
            provider=provider,
            message=msg,
            user_id=None,
            ip=_ip,
            user_agent=_ua,
            meta={"sale_ref": ref},
        )
        return {"ok": False, "reason": "provider_api", "message": msg}

    _ip, _ua = _integration_audit_request_meta()
    repository.append_event(
        event_type="draft_invoice_ok",
        provider=provider,
        message="Draft invoice created.",
        user_id=None,
        ip=_ip,
        user_agent=_ua,
        meta={"sale_ref": ref, "external_id": out.get("external_id")},
    )
    out["ok"] = True
    out["provider"] = provider
    return out
