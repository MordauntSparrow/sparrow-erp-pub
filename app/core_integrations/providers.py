"""
HTTP helpers for accounting OAuth token and identity calls.

Official docs:
- Xero: https://developer.xero.com/documentation/guides/oauth2/auth-flow
- FreeAgent: https://dev.freeagent.com/docs/oauth
- QuickBooks Online (Intuit): https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0
- Sage Accounting: https://developer.sage.com/accounting/guides/authentication/oauth/
- FreshBooks: https://www.freshbooks.com/api/authentication
"""
from __future__ import annotations

import base64
import os
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional
from urllib.parse import urlencode

import json

import requests

# Xero OAuth 2.0 (authorization code) — see scopes in docs/integrations/scopes.md
XERO_AUTH_URL = "https://login.xero.com/identity/connect/authorize"
XERO_TOKEN_URL = "https://identity.xero.com/connect/token"
XERO_CONNECTIONS_URL = "https://api.xero.com/connections"
XERO_ACCOUNTING_API = "https://api.accounting.xero.com/api.xro/2.0"

# FreeAgent production; override with FREEAGENT_API_BASE for sandbox (see FreeAgent OAuth doc)
FREEAGENT_API_BASE = (os.environ.get("FREEAGENT_API_BASE") or "https://api.freeagent.com").rstrip("/")


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def xero_build_authorize_url(
    *,
    client_id: str,
    redirect_uri: str,
    state: str,
    scopes: str,
) -> str:
    q = urlencode(
        {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scopes,
            "state": state,
        }
    )
    return f"{XERO_AUTH_URL}?{q}"


def xero_exchange_code(
    *, client_id: str, client_secret: str, code: str, redirect_uri: str
) -> dict[str, Any]:
    body = urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }
    )
    r = requests.post(
        XERO_TOKEN_URL,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": _basic_auth_header(client_id, client_secret),
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(_safe_oauth_error_message("xero", r))
    return r.json()


def xero_refresh_token(
    *, client_id: str, client_secret: str, refresh_token: str
) -> dict[str, Any]:
    body = urlencode({"grant_type": "refresh_token", "refresh_token": refresh_token})
    r = requests.post(
        XERO_TOKEN_URL,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": _basic_auth_header(client_id, client_secret),
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(_safe_oauth_error_message("xero", r))
    return r.json()


def xero_list_connections(access_token: str) -> list[dict[str, Any]]:
    r = requests.get(
        XERO_CONNECTIONS_URL,
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError("Could not read Xero organisations (connections).")
    data = r.json()
    return data if isinstance(data, list) else []


def freeagent_build_authorize_url(
    *, client_id: str, redirect_uri: str, state: str
) -> str:
    """FreeAgent authorisation endpoint — https://dev.freeagent.com/docs/oauth"""
    path = f"{FREEAGENT_API_BASE}/v2/approve_app"
    q = urlencode(
        {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "state": state,
        }
    )
    return f"{path}?{q}"


def freeagent_exchange_code(
    *, client_id: str, client_secret: str, code: str, redirect_uri: str
) -> dict[str, Any]:
    token_url = f"{FREEAGENT_API_BASE}/v2/token_endpoint"
    body = urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }
    )
    r = requests.post(
        token_url,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "Authorization": _basic_auth_header(client_id, client_secret),
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(_safe_oauth_error_message("freeagent", r))
    return r.json()


def freeagent_refresh_token(
    *, client_id: str, client_secret: str, refresh_token: str
) -> dict[str, Any]:
    token_url = f"{FREEAGENT_API_BASE}/v2/token_endpoint"
    body = urlencode({"grant_type": "refresh_token", "refresh_token": refresh_token})
    r = requests.post(
        token_url,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "Authorization": _basic_auth_header(client_id, client_secret),
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(_safe_oauth_error_message("freeagent", r))
    return r.json()


def freeagent_get_company(access_token: str) -> dict[str, Any]:
    """GET /v2/company — company profile for display label."""
    url = f"{FREEAGENT_API_BASE}/v2/company"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError("Could not read FreeAgent company profile.")
    data = r.json()
    return data if isinstance(data, dict) else {}


# ---------------------------------------------------------------------------
# QuickBooks Online (Intuit)
# ---------------------------------------------------------------------------
INTUIT_AUTH_URL = "https://appcenter.intuit.com/connect/oauth2"
INTUIT_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
# Scopes: https://developer.intuit.com/app/developer/qbo/docs/learn/scopes
INTUIT_QB_SCOPES = "com.intuit.quickbooks.accounting offline_access"


def quickbooks_api_base() -> str:
    """Production vs sandbox API host (use sandbox app keys with sandbox host)."""
    if (os.environ.get("INTUIT_ENVIRONMENT") or "").strip().lower() == "sandbox":
        return "https://sandbox-quickbooks.api.intuit.com"
    return "https://quickbooks.api.intuit.com"


def quickbooks_build_authorize_url(
    *, client_id: str, redirect_uri: str, state: str, scopes: str | None = None
) -> str:
    q = urlencode(
        {
            "client_id": client_id,
            "response_type": "code",
            "scope": scopes or INTUIT_QB_SCOPES,
            "redirect_uri": redirect_uri,
            "state": state,
        }
    )
    return f"{INTUIT_AUTH_URL}?{q}"


def quickbooks_exchange_code(
    *, client_id: str, client_secret: str, code: str, redirect_uri: str
) -> dict[str, Any]:
    body = urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }
    )
    r = requests.post(
        INTUIT_TOKEN_URL,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": _basic_auth_header(client_id, client_secret),
            "Accept": "application/json",
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(_safe_oauth_error_message("quickbooks", r))
    return r.json()


def quickbooks_refresh_token(
    *, client_id: str, client_secret: str, refresh_token: str
) -> dict[str, Any]:
    body = urlencode({"grant_type": "refresh_token", "refresh_token": refresh_token})
    r = requests.post(
        INTUIT_TOKEN_URL,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": _basic_auth_header(client_id, client_secret),
            "Accept": "application/json",
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(_safe_oauth_error_message("quickbooks", r))
    return r.json()


def quickbooks_get_company_label(*, realm_id: str, access_token: str) -> str:
    """Lightweight company name for display (QBO v3 companyinfo)."""
    base = quickbooks_api_base()
    url = f"{base}/v3/company/{realm_id}/companyinfo/{realm_id}"
    r = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        },
        params={"minorversion": "65"},
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError("Could not read QuickBooks company info.")
    data = r.json()
    if not isinstance(data, dict):
        return realm_id
    qr = data.get("QueryResponse") or {}
    ci = qr.get("CompanyInfo")
    if isinstance(ci, list) and ci and isinstance(ci[0], dict):
        return str(ci[0].get("CompanyName") or realm_id).strip() or realm_id
    if isinstance(ci, dict):
        return str(ci.get("CompanyName") or realm_id).strip() or realm_id
    return realm_id


# ---------------------------------------------------------------------------
# Sage Accounting (Business Cloud) — OAuth 2.0
# ---------------------------------------------------------------------------
SAGE_AUTH_URL = "https://www.sageone.com/oauth2/auth/central"
SAGE_TOKEN_URL = "https://oauth.accounting.sage.com/token"
SAGE_API_BASE = "https://api.accounting.sage.com/v3.1"


def sage_build_authorize_url(
    *,
    client_id: str,
    redirect_uri: str,
    state: str,
    scope: str = "full_access",
) -> str:
    q = urlencode(
        {
            "filter": "apiv3.1",
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scope,
            "state": state,
        }
    )
    return f"{SAGE_AUTH_URL}?{q}"


def sage_exchange_code(
    *, client_id: str, client_secret: str, code: str, redirect_uri: str
) -> dict[str, Any]:
    body = urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }
    )
    r = requests.post(
        SAGE_TOKEN_URL,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": _basic_auth_header(client_id, client_secret),
            "Accept": "application/json",
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(_safe_oauth_error_message("sage", r))
    return r.json()


def sage_refresh_token(
    *, client_id: str, client_secret: str, refresh_token: str
) -> dict[str, Any]:
    body = urlencode({"grant_type": "refresh_token", "refresh_token": refresh_token})
    r = requests.post(
        SAGE_TOKEN_URL,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": _basic_auth_header(client_id, client_secret),
            "Accept": "application/json",
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(_safe_oauth_error_message("sage", r))
    return r.json()


def sage_first_business(access_token: str) -> tuple[Optional[str], str]:
    """Return (business_id, display_name) from /businesses."""
    r = requests.get(
        f"{SAGE_API_BASE}/businesses",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError("Could not read Sage businesses.")
    data = r.json()
    rows = None
    if isinstance(data, dict):
        for k in ("$items", "items", "results", "$resources", "businesses"):
            v = data.get(k)
            if isinstance(v, list):
                rows = v
                break
        if rows is None and isinstance(data.get("business"), dict):
            return str(data["business"].get("id") or "").strip() or None, str(
                data["business"].get("name") or "Sage"
            ).strip()
    if isinstance(data, list):
        rows = data
    if not isinstance(rows, list) or not rows:
        return None, "Sage"
    first = rows[0] if isinstance(rows[0], dict) else {}
    bid = str(first.get("id") or first.get("Id") or "").strip() or None
    name = str(first.get("name") or first.get("Name") or "").strip() or "Sage"
    return bid, name


# ---------------------------------------------------------------------------
# FreshBooks — OAuth 2.0 (JSON token endpoint)
# ---------------------------------------------------------------------------
FRESHBOOKS_AUTH_URL = "https://auth.freshbooks.com/oauth/authorize"
FRESHBOOKS_TOKEN_URL = "https://api.freshbooks.com/auth/oauth/token"
FRESHBOOKS_DEFAULT_SCOPE = "user:profile:read"


def freshbooks_build_authorize_url(
    *, client_id: str, redirect_uri: str, state: str, scope: str | None = None
) -> str:
    sc = (
        (os.environ.get("FRESHBOOKS_OAUTH_SCOPE") or "").strip()
        or scope
        or FRESHBOOKS_DEFAULT_SCOPE
    )
    q = urlencode(
        {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "state": state,
            "scope": sc,
        }
    )
    return f"{FRESHBOOKS_AUTH_URL}?{q}"


def freshbooks_exchange_code(
    *, client_id: str, client_secret: str, code: str, redirect_uri: str
) -> dict[str, Any]:
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "redirect_uri": redirect_uri,
    }
    r = requests.post(
        FRESHBOOKS_TOKEN_URL,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(_safe_oauth_error_message("freshbooks", r))
    return r.json()


def freshbooks_refresh_token(
    *, client_id: str, client_secret: str, refresh_token: str, redirect_uri: str
) -> dict[str, Any]:
    payload = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "redirect_uri": redirect_uri,
    }
    r = requests.post(
        FRESHBOOKS_TOKEN_URL,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(_safe_oauth_error_message("freshbooks", r))
    return r.json()


def freshbooks_get_profile_label(access_token: str) -> tuple[Optional[str], str]:
    """Return (account_or_user_id, display label) from FreshBooks identity API."""
    url = "https://api.freshbooks.com/auth/api/v1/users/me"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError("Could not read FreshBooks user profile.")
    data = r.json()
    resp = data.get("response") if isinstance(data, dict) else None
    if not isinstance(resp, dict):
        return None, "FreshBooks"
    res = resp.get("result") or resp.get("results") or {}
    prof = None
    if isinstance(res, dict):
        prof = res.get("profile") if isinstance(res.get("profile"), dict) else None
        if prof is None and isinstance(res.get("id"), (str, int)):
            prof = res
    if not isinstance(prof, dict):
        return None, "FreshBooks"
    uid = str(prof.get("id") or prof.get("identity_id") or "").strip() or None
    first = str(prof.get("first_name") or "").strip()
    last = str(prof.get("last_name") or "").strip()
    email = str(prof.get("email") or "").strip()
    label = " ".join(x for x in (first, last) if x).strip() or email or "FreshBooks"
    return uid, label


# ---------------------------------------------------------------------------
# Draft sales invoices (CRM → Xero / FreeAgent)
# ---------------------------------------------------------------------------


def _xero_invoice_api_error(resp: requests.Response) -> str:
    try:
        j = resp.json()
        if isinstance(j, dict):
            invs = j.get("Invoices")
            if isinstance(invs, list) and invs:
                first = invs[0]
                if isinstance(first, dict) and first.get("ValidationErrors"):
                    ve = first["ValidationErrors"]
                    if isinstance(ve, list) and ve:
                        m0 = ve[0]
                        if isinstance(m0, dict) and m0.get("Message"):
                            return str(m0["Message"])[:400]
            if j.get("Message"):
                return str(j["Message"])[:400]
            if j.get("Detail"):
                return str(j["Detail"])[:400]
    except Exception:
        pass
    return f"Xero invoice API error (HTTP {resp.status_code})."


def xero_create_draft_invoice(
    *,
    access_token: str,
    tenant_id: str,
    contact_name: str,
    contact_email: str | None,
    reference: str,
    currency: str,
    line_items: list[dict[str, str]],
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """
    PUT ``/Invoices`` with Status DRAFT (ACCREC).
    ``XERO_DEFAULT_ACCOUNT_CODE`` sets the line ``AccountCode`` (chart-specific).
    """
    account_code = (os.environ.get("XERO_DEFAULT_ACCOUNT_CODE") or "200").strip() or "200"
    tax_type = (os.environ.get("XERO_DEFAULT_TAX_TYPE") or "").strip() or None
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    contact: dict[str, Any] = {"Name": contact_name[:255]}
    if contact_email:
        contact["EmailAddress"] = contact_email[:255]
    xero_lines: list[dict[str, Any]] = []
    for li in line_items:
        try:
            qty = float(Decimal(str(li.get("quantity") or "1")))
        except (InvalidOperation, ValueError, TypeError):
            qty = 1.0
        try:
            unit = float(Decimal(str(li.get("unit_amount") or "0")))
        except (InvalidOperation, ValueError, TypeError):
            unit = 0.0
        row: dict[str, Any] = {
            "Description": li.get("description", "")[:4000],
            "Quantity": qty,
            "UnitAmount": unit,
            "AccountCode": account_code,
        }
        if tax_type:
            row["TaxType"] = tax_type
        xero_lines.append(row)
    body = {
        "Invoices": [
            {
                "Type": "ACCREC",
                "Contact": contact,
                "Date": today,
                "DueDate": today,
                "Status": "DRAFT",
                "CurrencyCode": currency[:3],
                "Reference": reference[:255],
                "LineItems": xero_lines,
            }
        ]
    }
    url = f"{XERO_ACCOUNTING_API}/Invoices"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "xero-tenant-id": tenant_id,
    }
    if idempotency_key:
        safe_key = re.sub(r"[^\x21-\x7E]+", "-", idempotency_key)[:50]
        if safe_key:
            headers["Idempotency-Key"] = safe_key
    r = requests.put(url, headers=headers, data=json.dumps(body), timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(_xero_invoice_api_error(r))
    data = r.json()
    invs = data.get("Invoices") if isinstance(data, dict) else None
    if not isinstance(invs, list) or not invs:
        raise RuntimeError("Xero returned no invoice payload.")
    inv0 = invs[0]
    if not isinstance(inv0, dict):
        raise RuntimeError("Unexpected Xero invoice response.")
    if inv0.get("HasErrors"):
        raise RuntimeError(_xero_invoice_api_error(r))
    iid = str(inv0.get("InvoiceID") or "").strip()
    if not iid:
        raise RuntimeError("Xero did not return an invoice id.")
    online = str(inv0.get("OnlineInvoiceUrl") or "").strip() or None
    deep = f"https://go.xero.com/AccountsReceivable/View.aspx?InvoiceID={iid}" if iid else None
    return {"external_id": iid, "url": online or deep}


def freeagent_create_contact(
    access_token: str, *, organisation_name: str, email: str | None
) -> str:
    """POST /v2/contacts; returns contact ``url``."""
    url = f"{FREEAGENT_API_BASE}/v2/contacts"
    cblock: dict[str, Any] = {"organisation_name": organisation_name[:255]}
    if email:
        cblock["email"] = email[:255]
    body = {"contact": cblock}
    r = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        data=json.dumps(body),
        timeout=60,
    )
    if r.status_code >= 400:
        try:
            j = r.json()
            if isinstance(j, dict) and j.get("errors"):
                _raise_freeagent_errors(j["errors"])
        except RuntimeError:
            raise
        except Exception:
            pass
        raise RuntimeError(f"FreeAgent contact create failed (HTTP {r.status_code}).")
    data = r.json()
    contact = data.get("contact") if isinstance(data, dict) else None
    curl = ""
    if isinstance(contact, dict):
        curl = str(contact.get("url") or "").strip()
    if not curl:
        loc = r.headers.get("Location") or ""
        curl = str(loc).strip()
    if not curl:
        raise RuntimeError("FreeAgent did not return a contact URL.")
    return curl


def _raise_freeagent_errors(errors: Any) -> None:
    if isinstance(errors, dict):
        parts = []
        for k, v in errors.items():
            if isinstance(v, list):
                parts.append(f"{k}: {', '.join(str(x) for x in v)}")
            else:
                parts.append(f"{k}: {v}")
        if parts:
            raise RuntimeError("; ".join(parts)[:500])
    raise RuntimeError("FreeAgent validation error.")


def freeagent_create_draft_invoice(
    *,
    access_token: str,
    contact_name: str,
    contact_email: str | None,
    reference: str,
    currency: str,
    line_items: list[dict[str, str]],
) -> dict[str, Any]:
    """POST /v2/invoices (created as Draft)."""
    contact_url = freeagent_create_contact(
        access_token,
        organisation_name=contact_name,
        email=contact_email,
    )
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    items: list[dict[str, Any]] = []
    for li in line_items:
        try:
            qty = str(Decimal(str(li.get("quantity") or "1")))
        except (InvalidOperation, ValueError, TypeError):
            qty = "1"
        try:
            price = str(Decimal(str(li.get("unit_amount") or "0")))
        except (InvalidOperation, ValueError, TypeError):
            price = "0"
        items.append(
            {
                "description": li.get("description", "")[:500],
                "item_type": "Services",
                "quantity": qty,
                "price": price,
                "sales_tax_status": "EXEMPT",
            }
        )
    inv_body: dict[str, Any] = {
        "contact": contact_url,
        "dated_on": today,
        "payment_terms_in_days": 30,
        "currency": currency[:3],
        "reference": reference[:255],
        "status": "Draft",
        "invoice_items": items,
    }
    url = f"{FREEAGENT_API_BASE}/v2/invoices"
    r = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        data=json.dumps({"invoice": inv_body}),
        timeout=60,
    )
    if r.status_code >= 400:
        try:
            j = r.json()
            if isinstance(j, dict) and j.get("errors"):
                _raise_freeagent_errors(j["errors"])
        except RuntimeError:
            raise
        except Exception:
            pass
        raise RuntimeError(f"FreeAgent invoice create failed (HTTP {r.status_code}).")
    data = r.json()
    inv = data.get("invoice") if isinstance(data, dict) else None
    ext_url = ""
    if isinstance(inv, dict):
        ext_url = str(inv.get("url") or "").strip()
    if not ext_url:
        ext_url = (r.headers.get("Location") or "").strip()
    if not ext_url:
        raise RuntimeError("FreeAgent did not return an invoice URL.")
    m = re.search(r"/invoices/(\d+)$", ext_url)
    external_id = m.group(1) if m else ext_url
    return {"external_id": external_id, "url": ext_url}


def _safe_oauth_error_message(provider: str, resp: requests.Response) -> str:
    """Never include access tokens in returned text."""
    try:
        j = resp.json()
        if isinstance(j, dict):
            err = (j.get("error") or j.get("error_description") or "").strip()
            if err:
                return f"{provider.title()} OAuth error: {err[:280]}"
    except Exception:
        pass
    return f"{provider.title()} token request failed (HTTP {resp.status_code})."


def parse_expires_at(token_payload: dict[str, Any]) -> Optional[str]:
    """Return MySQL-friendly datetime string for access token expiry if present."""
    from datetime import datetime, timedelta, timezone

    sec = token_payload.get("expires_in")
    if sec is None:
        return None
    try:
        n = int(sec)
    except (TypeError, ValueError):
        return None
    dt = datetime.now(timezone.utc) + timedelta(seconds=max(0, n))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_refresh_expires_at(token_payload: dict[str, Any]) -> Optional[str]:
    """FreeAgent returns refresh_token_expires_in (seconds). Xero refresh expiry is policy-based — often omitted."""
    from datetime import datetime, timedelta, timezone

    sec = token_payload.get("refresh_token_expires_in")
    if sec is None:
        return None
    try:
        n = int(sec)
    except (TypeError, ValueError):
        return None
    dt = datetime.now(timezone.utc) + timedelta(seconds=max(0, n))
    return dt.strftime("%Y-%m-%d %H:%M:%S")
