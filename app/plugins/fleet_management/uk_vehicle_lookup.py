"""
UK fleet data enrichment: DVSA MOT trade API and DVLA Vehicle Enquiry Service (VES).

Canonical keys and URLs for Sparrow builds live in ``uk_api_defaults.py``. Optional
overrides: Flask ``FLEET_UK_*`` config keys or the same names in the environment.

MOT (two modes):
  • Legacy: ``x-api-key`` + ``Accept: application/json+v6`` on
    ``{base}/trade/vehicles/mot-tests`` (default base ``beta.check-mot.service.gov.uk``).
  • Current (DVSA approval emails from ~2025): OAuth2 client credentials (Entra ID) to
    obtain a Bearer token, then each request sends ``Authorization: Bearer …`` and
    ``x-api-key`` to ``{base}/v1/trade/vehicles/mot-tests`` (default base ``tapi.dvsa.gov.uk``).

Tax status from DVLA is returned as taxStatus / taxDueDate (not always a simple “expiry”).
We map taxDueDate to tax_expiry for fleet_compliance when parseable.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

logger = logging.getLogger("fleet_management.uk_lookup")

# Full tracebacks and HTTP bodies go here (server console / log file). Never log API keys,
# client secrets, or Bearer tokens.

_mot_oauth_lock = threading.Lock()
# (access_token, unix_timestamp_when_invalid)
_mot_oauth_token_cache: Tuple[str, float] = ("", 0.0)

# --- MOT “circuit breaker”: hide noisy DVSA UI after repeated infra failures ----------
_mot_circuit_lock = threading.Lock()
_mot_circuit_open_until = 0.0
_mot_circuit_consecutive = 0
MOT_CIRCUIT_FAILURE_THRESHOLD = 2
MOT_CIRCUIT_COOLDOWN_SEC = 900.0

# Do not count these toward blackout (normal “no data” or misconfiguration).
_MOT_ERRORS_IGNORE_FOR_CIRCUIT = frozenset(
    {
        "mot_empty_response",
        "mot_api_key_not_configured",
    }
)


def _mot_circuit_expired_unlocked(now: float) -> None:
    global _mot_circuit_open_until, _mot_circuit_consecutive
    if _mot_circuit_open_until and now >= _mot_circuit_open_until:
        _mot_circuit_open_until = 0.0
        _mot_circuit_consecutive = 0


def mot_circuit_breaker_disabled() -> bool:
    """Set FLEET_UK_MOT_CIRCUIT=0 (or false/off) in env or app.config to disable auto-suppress."""
    raw = (os.environ.get("FLEET_UK_MOT_CIRCUIT") or "").strip().lower()
    if raw in ("0", "false", "no", "off", "disabled"):
        return True
    try:
        from flask import current_app, has_request_context

        if has_request_context():
            cfg_val = current_app.config.get("FLEET_UK_MOT_CIRCUIT")
            if cfg_val is not None and str(cfg_val).strip().lower() in (
                "0",
                "false",
                "no",
                "off",
                "disabled",
            ):
                return True
    except Exception:
        pass
    return False


def mot_history_ui_suppressed_by_circuit() -> bool:
    """True while cooldown active after enough infra failures (skips DVSA fetches)."""
    if mot_circuit_breaker_disabled():
        return False
    with _mot_circuit_lock:
        now = time.monotonic()
        _mot_circuit_expired_unlocked(now)
        return bool(_mot_circuit_open_until and now < _mot_circuit_open_until)


def mot_error_should_open_circuit(err: Any) -> bool:
    """Whether a MOT error payload indicates DVSA transport/auth breakage (vs empty plate)."""
    if not err or not isinstance(err, dict):
        return True
    code = str(err.get("error") or "").strip()
    if code in _MOT_ERRORS_IGNORE_FOR_CIRCUIT:
        return False
    return True


def record_mot_infra_failure() -> None:
    if mot_circuit_breaker_disabled():
        return
    global _mot_circuit_open_until, _mot_circuit_consecutive
    with _mot_circuit_lock:
        now = time.monotonic()
        _mot_circuit_expired_unlocked(now)
        _mot_circuit_consecutive += 1
        if _mot_circuit_consecutive >= MOT_CIRCUIT_FAILURE_THRESHOLD:
            _mot_circuit_open_until = now + MOT_CIRCUIT_COOLDOWN_SEC
            logger.warning(
                "MOT API: suppressing Official MOT (DVSA) UI for %ss after %s consecutive "
                "infrastructure failures (set FLEET_UK_MOT_CIRCUIT=0 to disable).",
                int(MOT_CIRCUIT_COOLDOWN_SEC),
                _mot_circuit_consecutive,
            )


def record_mot_infra_success() -> None:
    global _mot_circuit_open_until, _mot_circuit_consecutive
    with _mot_circuit_lock:
        _mot_circuit_consecutive = 0
        _mot_circuit_open_until = 0.0


def format_mot_api_error_for_display(message: Optional[str]) -> Optional[str]:
    """Short, non-technical message for admin UI (full detail stays in logs)."""
    if not message:
        return None
    if "AADSTS90002" in message or (
        "tenant" in message.lower() and "not found" in message.lower()
    ):
        return (
            "DVSA MOT sign-in failed: the Microsoft tenant in server settings is not "
            "recognised. Check the token URL from your DVSA approval email, or contact "
            "DVSA MOT History API support."
        )
    low = message.lower()
    if (
        "nameresolutionerror" in low
        or "failed to resolve" in low
        or "getaddrinfo failed" in low
        or "name or service not known" in low
        or "temporary failure in name resolution" in low
        or "max retries exceeded" in low
        or "connection refused" in low
        or "connection timed out" in low
        or "tapi.dvsa.gov.uk" in low
    ):
        return (
            "This server could not reach the DVSA MOT service (DNS or network). "
            "Check outbound HTTPS and DNS for tapi.dvsa.gov.uk (and the legacy MOT host "
            "if you use the beta fallback), or ask your IT team to allow that traffic."
        )
    if len(message) > 220:
        return message[:220] + "…"
    return message


def normalize_uk_registration(reg: str) -> str:
    s = (reg or "").strip().upper()
    return re.sub(r"\s+", "", s)


def mot_oauth_configured(
    client_id: Optional[str],
    client_secret: Optional[str],
    token_url: Optional[str],
    scope: Optional[str],
) -> bool:
    return bool(
        (client_id or "").strip()
        and (client_secret or "").strip()
        and (token_url or "").strip()
        and (scope or "").strip()
    )


LEGACY_MOT_TRADE_BASE_URL = "https://beta.check-mot.service.gov.uk"

# When DVSA’s emailed tenant GUID is wrong or retired, Azure returns AADSTS90002. Some DVSA app
# registrations accept the multi-tenant “organizations” token host with the same client_id/secret.
MOT_OAUTH_TOKEN_URL_ORGANIZATIONS_FALLBACK = (
    "https://login.microsoftonline.com/organizations/oauth2/v2.0/token"
)


def _post_mot_oauth_token_request(
    token_endpoint: str,
    *,
    client_id: str,
    client_secret: str,
    scope: str,
    timeout: int,
) -> Tuple[int, Any]:
    """POST client_credentials; returns (status_code, parsed_json_or_None_on_non_json)."""
    ep = token_endpoint.strip()
    try:
        r = requests.post(
            ep,
            data={
                "client_id": client_id.strip(),
                "client_secret": client_secret.strip(),
                "scope": scope.strip(),
                "grant_type": "client_credentials",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=timeout,
        )
    except requests.RequestException:
        logger.exception(
            "MOT OAuth: token request network failure (host=%s)",
            urlparse(ep).netloc or ep[:60],
        )
        raise
    if r.status_code >= 400:
        logger.warning(
            "MOT OAuth: token HTTP %s from host=%s; body (truncated): %s",
            r.status_code,
            urlparse(ep).netloc or "?",
            (r.text or "")[:2500],
        )
    try:
        return r.status_code, r.json()
    except ValueError:
        if r.status_code < 400:
            logger.warning(
                "MOT OAuth: token HTTP %s non-JSON body (truncated): %s",
                r.status_code,
                (r.text or "")[:2500],
            )
        return r.status_code, None


def _get_mot_oauth_access_token(
    *,
    client_id: str,
    client_secret: str,
    token_url: str,
    scope: str,
    timeout: int = 30,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (access_token, error_code, error_detail) for DVSA MOT API (Entra client_credentials)."""
    global _mot_oauth_token_cache
    now = time.time()
    with _mot_oauth_lock:
        tok, until = _mot_oauth_token_cache
        if tok and until > now + 90:
            return tok, None, None

    def _store_token(access: str, body: Dict[str, Any]) -> str:
        try:
            exp_in = int(body.get("expires_in") or 3600)
        except (TypeError, ValueError):
            exp_in = 3600
        until = now + max(exp_in - 120, 300)
        with _mot_oauth_lock:
            _mot_oauth_token_cache = (access, until)
        return access

    try:
        status, body = _post_mot_oauth_token_request(
            token_url,
            client_id=client_id,
            client_secret=client_secret,
            scope=scope,
            timeout=timeout,
        )
    except requests.RequestException as e:
        # traceback already logged in _post_mot_oauth_token_request
        return None, "mot_oauth_token_failed", str(e)[:800]

    if status < 400 and isinstance(body, dict):
        access = body.get("access_token")
        if access and isinstance(access, str):
            _store_token(access, body)
            return access, None, None
        return None, "mot_oauth_no_access_token", None

    detail = ""
    if isinstance(body, dict):
        ed = body.get("error_description") or body.get("error")
        if ed:
            detail = str(ed)[:600]
    if not detail:
        detail = f"token HTTP {status}"
    logger.warning(
        "MOT OAuth token HTTP %s at configured URL: %s", status, detail[:200]
    )

    tenant_missing = "AADSTS90002" in detail or (
        "tenant" in detail.lower() and "not found" in detail.lower()
    )
    cfg_url = token_url.strip().rstrip("/").lower()
    org_url = MOT_OAUTH_TOKEN_URL_ORGANIZATIONS_FALLBACK.rstrip("/").lower()
    if tenant_missing and cfg_url != org_url:
        try:
            st2, body2 = _post_mot_oauth_token_request(
                MOT_OAUTH_TOKEN_URL_ORGANIZATIONS_FALLBACK,
                client_id=client_id,
                client_secret=client_secret,
                scope=scope,
                timeout=timeout,
            )
        except requests.RequestException:
            # Full traceback already logged by _post_mot_oauth_token_request
            return None, "mot_oauth_token_denied", detail

        if st2 < 400 and isinstance(body2, dict):
            access2 = body2.get("access_token")
            if access2 and isinstance(access2, str):
                logger.info(
                    "MOT OAuth: token from organizations endpoint "
                    "(configured tenant URL returned tenant-not-found)"
                )
                _store_token(access2, body2)
                return access2, None, None
        d2 = ""
        if isinstance(body2, dict):
            e2 = body2.get("error_description") or body2.get("error")
            if e2:
                d2 = str(e2)[:400]
        logger.warning(
            "MOT OAuth organizations fallback failed: %s %s",
            st2,
            d2 or str(body2)[:200],
        )

    return None, "mot_oauth_token_denied", detail


def _mot_legacy_trade_fetch_vehicle(
    reg: str,
    *,
    api_key: str,
    timeout: int = 30,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Pre–OAuth MOT trade API: x-api-key only against beta.check-mot.service.gov.uk.
    Used as fallback when Entra token fails or tapi rejects the request.
    """
    url = f"{LEGACY_MOT_TRADE_BASE_URL.rstrip('/')}/trade/vehicles/mot-tests"
    headers = {
        "Accept": "application/json+v6",
        "X-API-Key": api_key.strip(),
    }
    try:
        r = requests.get(
            url, params={"registration": reg}, headers=headers, timeout=timeout
        )
        if r.status_code == 404:
            return None, {"error": "not_found", "registration": reg}
        if r.status_code == 403:
            logger.warning(
                "Legacy MOT HTTP 403 reg=%s; body (truncated): %s",
                reg,
                (r.text or "")[:2500],
            )
            return None, {
                "error": "mot_forbidden",
                "detail": "Legacy MOT: invalid or missing API key",
            }
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        logger.exception(
            "Legacy MOT API network error (host=%s, registration=%s)",
            urlparse(LEGACY_MOT_TRADE_BASE_URL).netloc,
            reg,
        )
        return None, {"error": "mot_legacy_request_failed", "detail": str(e)[:800]}
    except ValueError:
        logger.warning(
            "Legacy MOT invalid JSON reg=%s; body (truncated): %s",
            reg,
            (r.text or "")[:2500],
        )
        return None, {"error": "mot_invalid_json"}
    vehicles_list = _coerce_mot_response_to_vehicle_list(data)
    if not vehicles_list:
        return None, {"error": "mot_empty_response", "registration": reg}
    return vehicles_list[0], None


def _mot_v1_get_vehicle(
    reg: str,
    *,
    bearer: str,
    api_key: str,
    base_url: str,
    timeout: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """GET tapi v1/trade/vehicles/mot-tests; try alternate Accept if 406/415."""
    path = "/v1/trade/vehicles/mot-tests"
    url = f"{base_url.rstrip('/')}{path}"
    last_err: Optional[Dict[str, Any]] = None
    for accept in ("application/json+v6", "application/json"):
        headers = {
            "Accept": accept,
            "X-API-Key": api_key.strip(),
            "Authorization": f"Bearer {bearer}",
        }
        try:
            r = requests.get(
                url, params={"registration": reg}, headers=headers, timeout=timeout
            )
        except requests.RequestException as e:
            logger.exception(
                "MOT tapi GET failed (host=%s, registration=%s, Accept=%s)",
                urlparse(url).netloc,
                reg,
                accept,
            )
            return None, {"error": "mot_request_failed", "detail": str(e)[:800]}
        if r.status_code in (406, 415):
            logger.warning(
                "MOT tapi HTTP %s for reg=%s Accept=%s; body (truncated): %s",
                r.status_code,
                reg,
                accept,
                (r.text or "")[:2500],
            )
            last_err = {
                "error": "mot_not_acceptable",
                "detail": f"HTTP {r.status_code} with Accept {accept!r}",
            }
            continue
        if r.status_code == 404:
            return None, {"error": "not_found", "registration": reg}
        if r.status_code == 403:
            logger.warning(
                "MOT tapi HTTP 403 for reg=%s (check API key + Bearer token + subscription); body (truncated): %s",
                reg,
                (r.text or "")[:2500],
            )
            return None, {
                "error": "mot_forbidden",
                "detail": "tapi: invalid token, API key, or subscription",
            }
        try:
            r.raise_for_status()
            data = r.json()
        except requests.HTTPError as e:
            resp = getattr(e, "response", None)
            tb = (resp.text or "")[:2500] if resp is not None else ""
            logger.warning(
                "MOT tapi HTTP error after GET reg=%s: %s; body (truncated): %s",
                reg,
                e,
                tb,
            )
            return None, {"error": "mot_request_failed", "detail": str(e)[:800]}
        except requests.RequestException as e:
            logger.exception("MOT tapi request failed (reg=%s)", reg)
            return None, {"error": "mot_request_failed", "detail": str(e)[:800]}
        except ValueError:
            logger.warning(
                "MOT tapi invalid JSON for reg=%s; body (truncated): %s",
                reg,
                (r.text or "")[:2500],
            )
            return None, {"error": "mot_invalid_json"}
        vehicles_list = _coerce_mot_response_to_vehicle_list(data)
        if vehicles_list:
            return vehicles_list[0], None
        last_err = {"error": "mot_empty_response", "registration": reg}
    return None, last_err or {"error": "mot_empty_response", "registration": reg}


def _normalize_fuel_for_form(raw: Any) -> Optional[str]:
    """Map DVLA/DVSA fuel strings to fleet form select values: petrol, diesel, electric, hybrid, other."""
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if not s:
        return None
    if "HYBRID" in s or "PHEV" in s:
        return "hybrid"
    if "PETROL" in s and "ELECTRIC" in s:
        return "hybrid"
    if "DIESEL" in s and "ELECTRIC" in s:
        return "hybrid"
    if s in ("ELECTRICITY", "ELECTRIC") or s.startswith("ELECTRIC"):
        return "electric"
    if "DIESEL" in s or s in ("HEAVY OIL",):
        return "diesel"
    if "PETROL" in s or s in ("GAS", "UNLEADED"):
        return "petrol"
    return "other"


def _coerce_mot_response_to_vehicle_list(data: Any) -> List[Dict[str, Any]]:
    """Accept legacy list responses or newer / wrapped MOT JSON objects."""
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("results", "data", "vehicles", "items"):
        inner = data.get(key)
        if isinstance(inner, list):
            out = [x for x in inner if isinstance(x, dict)]
            if out:
                return out
        if isinstance(inner, dict) and (
            inner.get("motTests") is not None or inner.get("make") is not None
        ):
            return [inner]
    if data.get("motTests") is not None or data.get("make") is not None:
        return [data]
    vehicle = data.get("vehicle")
    if isinstance(vehicle, dict):
        return [vehicle]
    return []


def _parse_yyyy_mm_dd(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    raw = str(val).strip().replace(".", "-")
    s = raw[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _mot_fetch_first_vehicle_record(
    registration: str,
    *,
    api_key: str,
    base_url: str = "https://beta.check-mot.service.gov.uk",
    timeout: int = 30,
    mot_client_id: Optional[str] = None,
    mot_client_secret: Optional[str] = None,
    mot_token_url: Optional[str] = None,
    mot_scope: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], str]:
    """
    DVSA MOT by registration: OAuth + tapi v1 when configured, else legacy trade only.
    On OAuth/token or tapi failure, falls back to legacy beta trade (x-api-key only) so
    model/MOT history can still populate when Entra tenant URL is wrong but the key works on beta.
    """
    reg = normalize_uk_registration(registration)
    if not reg:
        return None, {"error": "empty_registration"}, ""
    if not api_key:
        return None, {"error": "mot_api_key_not_configured"}, reg
    oauth = mot_oauth_configured(
        mot_client_id, mot_client_secret, mot_token_url, mot_scope
    )
    logger.info(
        "MOT _mot_fetch_first_vehicle_record: reg=%s oauth_mode=%s api_host=%s token_host=%s",
        reg,
        oauth,
        urlparse(base_url).netloc or "?",
        urlparse(mot_token_url or "").netloc if oauth else "n/a",
    )

    if not oauth:
        veh, err = _mot_legacy_trade_fetch_vehicle(reg, api_key=api_key, timeout=timeout)
        return veh, err, reg

    bearer, terr, tdet = _get_mot_oauth_access_token(
        client_id=mot_client_id or "",
        client_secret=mot_client_secret or "",
        token_url=mot_token_url or "",
        scope=mot_scope or "",
        timeout=timeout,
    )
    primary_oauth_err: Optional[Dict[str, Any]] = None
    if terr or not bearer:
        primary_oauth_err = {"error": terr or "mot_oauth_failed"}
        if tdet:
            primary_oauth_err["detail"] = tdet
        logger.info(
            "MOT OAuth unavailable (%s); trying legacy trade endpoint", terr or "?"
        )
    else:
        veh, err = _mot_v1_get_vehicle(
            reg,
            bearer=bearer,
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
        )
        if veh is not None:
            return veh, None, reg
        if err and err.get("error") == "not_found":
            return None, err, reg
        logger.info(
            "MOT tapi request did not return a vehicle (%s); trying legacy trade",
            err.get("error") if err else "?",
        )
        primary_oauth_err = err

    veh_l, err_l = _mot_legacy_trade_fetch_vehicle(reg, api_key=api_key, timeout=timeout)
    if veh_l is not None:
        return veh_l, None, reg
    if primary_oauth_err:
        out = dict(primary_oauth_err)
        out["legacy_detail"] = err_l
        return None, out, reg
    return None, err_l or {"error": "mot_unknown"}, reg


def _build_mot_history_rows(veh: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalise motTests from a DVSA vehicle blob for admin display (newest first)."""
    mot_tests = veh.get("motTests") or []
    if not isinstance(mot_tests, list):
        return []
    rows: List[Dict[str, Any]] = []
    for t in mot_tests:
        if not isinstance(t, dict):
            continue
        raw_dt = str(t.get("completedDate") or "").strip()
        exp_raw = t.get("expiryDate")
        exp_iso = (
            _parse_yyyy_mm_dd(str(exp_raw).replace(".", "-")) if exp_raw else None
        )
        odo = t.get("odometerValue")
        odo_mi: Optional[int] = None
        if odo is not None and str(odo).strip().isdigit():
            try:
                odo_mi = int(str(odo).strip())
            except ValueError:
                pass
        rfr = t.get("rfrAndComments") or t.get("defects") or []
        fail_n = adv_n = 0
        if isinstance(rfr, list):
            for item in rfr:
                if not isinstance(item, dict):
                    continue
                typ = str(item.get("type") or "").upper()
                if typ in ("FAIL", "MAJOR", "DANGEROUS"):
                    fail_n += 1
                elif typ == "ADVISORY":
                    adv_n += 1
        rows.append(
            {
                "completed_sort": raw_dt,
                "completed_display": raw_dt[:16] if raw_dt else "—",
                "result": (t.get("testResult") or "").strip() or "—",
                "expiry": exp_iso,
                "odometer_mi": odo_mi,
                "test_number": (t.get("motTestNumber") or "").strip() or None,
                "fail_count": fail_n,
                "advisory_count": adv_n,
            }
        )

    def _sort_key(row: Dict[str, Any]) -> str:
        return row.get("completed_sort") or ""

    rows.sort(key=_sort_key, reverse=True)
    return rows


def fetch_mot_test_history_for_vehicle(
    registration: str,
    *,
    api_key: str,
    base_url: str = "https://beta.check-mot.service.gov.uk",
    timeout: int = 30,
    mot_client_id: Optional[str] = None,
    mot_client_secret: Optional[str] = None,
    mot_token_url: Optional[str] = None,
    mot_scope: Optional[str] = None,
) -> Dict[str, Any]:
    """
    MOT test rows for vehicle detail page (live DVSA data; not stored in DB).
    Returns {"registration", "tests": [...]} or {"error": ...}.
    """
    veh, err, reg = _mot_fetch_first_vehicle_record(
        registration,
        api_key=api_key,
        base_url=base_url,
        timeout=timeout,
        mot_client_id=mot_client_id,
        mot_client_secret=mot_client_secret,
        mot_token_url=mot_token_url,
        mot_scope=mot_scope,
    )
    if err or veh is None:
        logger.warning(
            "fetch_mot_test_history_for_vehicle failed registration=%s error_payload=%s",
            reg,
            err or {"error": "mot_unknown"},
        )
        return err or {"error": "mot_unknown"}
    return {
        "registration": reg,
        "tests": _build_mot_history_rows(veh),
        "dvsa_make": (veh.get("make") or "").strip() or None,
        "dvsa_model": (veh.get("model") or "").strip() or None,
    }


def fetch_mot_trade(
    registration: str,
    *,
    api_key: str,
    base_url: str = "https://beta.check-mot.service.gov.uk",
    timeout: int = 30,
    mot_client_id: Optional[str] = None,
    mot_client_secret: Optional[str] = None,
    mot_token_url: Optional[str] = None,
    mot_scope: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Call DVSA MOT trade endpoint (legacy and/or OAuth2 + X-API-Key). Returns normalized dict or {"error": "..."}.
    """
    veh, err, reg = _mot_fetch_first_vehicle_record(
        registration,
        api_key=api_key,
        base_url=base_url,
        timeout=timeout,
        mot_client_id=mot_client_id,
        mot_client_secret=mot_client_secret,
        mot_token_url=mot_token_url,
        mot_scope=mot_scope,
    )
    if err or veh is None:
        return err or {"error": "mot_unknown"}

    make = (veh.get("make") or "").strip() or None
    model = (veh.get("model") or "").strip() or None
    fuel_raw = (veh.get("fuelType") or veh.get("fuel_type") or "").strip() or None
    fuel = _normalize_fuel_for_form(fuel_raw) if fuel_raw else None
    mot_tests = veh.get("motTests") or []
    mot_expiry: Optional[str] = None
    last_odometer: Optional[int] = None
    if isinstance(mot_tests, list):
        sorted_tests: List[Dict[str, Any]] = [
            t for t in mot_tests if isinstance(t, dict)
        ]

        def _completed(t: Dict[str, Any]) -> str:
            return str(t.get("completedDate") or "")

        sorted_tests.sort(key=_completed, reverse=True)
        for t in sorted_tests:
            if str(t.get("testResult") or "").upper() == "PASSED" and t.get("expiryDate"):
                mot_expiry = _parse_yyyy_mm_dd(str(t.get("expiryDate")).replace(".", "-"))
                break
        if sorted_tests:
            try:
                ov = sorted_tests[0].get("odometerValue")
                if ov is not None and str(ov).strip().isdigit():
                    last_odometer = int(str(ov).strip())
            except (TypeError, ValueError):
                pass

    year: Optional[int] = None
    for key in ("manufactureDate", "registrationDate", "firstUsedDate"):
        raw = veh.get(key)
        if raw:
            parsed = _parse_yyyy_mm_dd(str(raw).replace(".", "-"))
            if parsed:
                try:
                    year = int(parsed[:4])
                    break
                except ValueError:
                    pass

    return {
        "source_mot": True,
        "registration": reg,
        "make": make,
        "model": model,
        "fuel_type": fuel,
        "year": year,
        "mot_expiry": mot_expiry,
        "last_mot_odometer_mi": last_odometer,
        "raw_vehicle_id": veh.get("vehicleId"),
        "mot_tests_count": len(mot_tests) if isinstance(mot_tests, list) else 0,
    }


def fetch_dvla_vehicle(
    registration: str,
    *,
    api_key: str,
    api_url: str = "https://driver-vehicle-licensing.api.gov.uk/vehicle-enquiry/v1/vehicles",
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    DVLA Vehicle Enquiry Service — POST JSON { "registrationNumber": "..." }.
    """
    reg = normalize_uk_registration(registration)
    if not reg:
        return {"error": "empty_registration"}
    if not api_key:
        return {"error": "dvla_api_key_not_configured"}
    headers = {
        "x-api-key": api_key.strip(),
        "Content-Type": "application/json",
    }
    try:
        r = requests.post(
            api_url,
            json={"registrationNumber": reg},
            headers=headers,
            timeout=timeout,
        )
        if r.status_code == 404:
            return {"error": "dvla_not_found", "registration": reg}
        if r.status_code == 403:
            return {"error": "dvla_forbidden", "detail": "Invalid or missing API key"}
        r.raise_for_status()
        body = r.json()
    except requests.RequestException as e:
        logger.warning("DVLA VES request failed: %s", e)
        return {"error": "dvla_request_failed", "detail": str(e)}
    except ValueError:
        return {"error": "dvla_invalid_json"}

    if not isinstance(body, dict):
        return {"error": "dvla_unexpected_shape"}

    make = (body.get("make") or "").strip() or None
    tax_status = (body.get("taxStatus") or "").strip() or None
    tax_due = body.get("taxDueDate") or body.get("taxExpiryDate")
    tax_expiry = _parse_yyyy_mm_dd(str(tax_due)) if tax_due else None
    mot_due_raw = body.get("motExpiryDate") or body.get("mot_expiry_date")
    mot_expiry = None
    if mot_due_raw is not None and str(mot_due_raw).strip():
        mot_expiry = _parse_yyyy_mm_dd(str(mot_due_raw).replace(".", "-"))
    fuel_type = _normalize_fuel_for_form(body.get("fuelType") or body.get("fuel_type"))
    yom = body.get("yearOfManufacture")
    year: Optional[int] = None
    if yom is not None:
        try:
            year = int(str(yom)[:4])
        except ValueError:
            pass

    return {
        "source_dvla": True,
        "registration": reg,
        "make": make,
        "year": year,
        "fuel_type": fuel_type,
        "mot_expiry": mot_expiry,
        "tax_status": tax_status,
        "tax_expiry": tax_expiry,
        "co2_emissions": body.get("co2Emissions"),
        "euro_status": body.get("euroStatus"),
    }


def merge_uk_lookup(
    registration: str,
    *,
    mot_api_key: Optional[str] = None,
    mot_base_url: Optional[str] = None,
    mot_client_id: Optional[str] = None,
    mot_client_secret: Optional[str] = None,
    mot_token_url: Optional[str] = None,
    mot_scope: Optional[str] = None,
    dvla_api_key: Optional[str] = None,
    dvla_api_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run configured sources and merge into one payload for form autofill.
    """
    out: Dict[str, Any] = {
        "registration": normalize_uk_registration(registration),
        "sources": [],
        "errors": [],
    }
    mot_key = mot_api_key or os.environ.get("FLEET_UK_MOT_API_KEY")
    m_cid = mot_client_id or os.environ.get("FLEET_UK_MOT_CLIENT_ID")
    m_cs = mot_client_secret or os.environ.get("FLEET_UK_MOT_CLIENT_SECRET")
    m_tok = mot_token_url or os.environ.get("FLEET_UK_MOT_TOKEN_URL")
    m_scp = mot_scope or os.environ.get("FLEET_UK_MOT_SCOPE")
    if (m_cid or "").strip() and (m_cs or "").strip() and (m_tok or "").strip() and not (m_scp or "").strip():
        m_scp = "https://tapi.dvsa.gov.uk/.default"
    oauth_on = mot_oauth_configured(m_cid, m_cs, m_tok, m_scp)

    mot_base_env = mot_base_url or os.environ.get("FLEET_UK_MOT_API_BASE")
    if mot_base_env:
        mot_base = mot_base_env
    elif oauth_on:
        mot_base = "https://tapi.dvsa.gov.uk"
    else:
        mot_base = "https://beta.check-mot.service.gov.uk"
    dvla_key = dvla_api_key or os.environ.get("FLEET_UK_DVLA_API_KEY")
    dvla_url = dvla_api_url or os.environ.get(
        "FLEET_UK_DVLA_API_URL",
        "https://driver-vehicle-licensing.api.gov.uk/vehicle-enquiry/v1/vehicles",
    )

    if mot_key:
        m = fetch_mot_trade(
            registration,
            api_key=mot_key,
            base_url=mot_base,
            mot_client_id=m_cid,
            mot_client_secret=m_cs,
            mot_token_url=m_tok,
            mot_scope=m_scp,
        )
        if m.get("error"):
            out["errors"].append({"mot": m})
            if mot_error_should_open_circuit(m):
                record_mot_infra_failure()
        else:
            record_mot_infra_success()
            out["sources"].append("mot")
            for k in ("make", "model", "fuel_type", "year", "mot_expiry"):
                if m.get(k) is not None:
                    out[k] = m[k]
            if m.get("last_mot_odometer_mi") is not None:
                out["mot_last_odometer_mi"] = m["last_mot_odometer_mi"]

    if dvla_key:
        d = fetch_dvla_vehicle(registration, api_key=dvla_key, api_url=dvla_url)
        if d.get("error"):
            out["errors"].append({"dvla": d})
        else:
            out["sources"].append("dvla")
            if d.get("make") and not out.get("make"):
                out["make"] = d["make"]
            if d.get("year") and not out.get("year"):
                out["year"] = d["year"]
            if d.get("fuel_type") and not out.get("fuel_type"):
                out["fuel_type"] = d["fuel_type"]
            if d.get("mot_expiry") and not out.get("mot_expiry"):
                out["mot_expiry"] = d["mot_expiry"]
            if d.get("tax_expiry"):
                out["tax_expiry"] = d["tax_expiry"]
            if d.get("tax_status"):
                out["tax_status"] = d["tax_status"]

    if not out["sources"] and not out["errors"]:
        out["errors"].append(
            {
                "config": "no_uk_api_keys",
                "message": "Set MOT/DVLA keys in uk_api_defaults.py (or FLEET_UK_* config / env)",
            }
        )

    # If DVLA (or MOT) already returned usable data, do not surface MOT transport errors in the
    # browser — the form looks “broken” even when make/model/tax were filled from DVLA.
    if out["sources"]:
        before = len(out["errors"])
        out["errors"] = [e for e in out["errors"] if "mot" not in e]
        if len(out["errors"]) < before:
            logger.info(
                "merge_uk_lookup: MOT failed for reg=%s but another source succeeded; "
                "omitting MOT error from JSON response (detail remains in server logs).",
                out.get("registration"),
            )
    return out
