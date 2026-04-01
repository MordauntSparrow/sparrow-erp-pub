"""
DBS Multiple Status Check (CRSC) — read-only HTTP/XML integration.
See docs/dev/PRD_DBS_UPDATE_SERVICE_MONITORING.md. Do not submit applications from this path.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote, urlencode

import requests

from app.objects import PluginManager, get_db_connection

_logger = logging.getLogger(__name__)

KNOWN_RESULTS = frozenset({
    "BLANK_NO_NEW_INFO",
    "NON_BLANK_NO_NEW_INFO",
    "NEW_INFO",
})

_DEFAULT_CRSC_BASE = "https://secure.crbonline.gov.uk/crsc/api/status"


def is_dbs_update_service_enabled() -> bool:
    return (os.environ.get("DBS_UPDATE_SERVICE_ENABLED") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def crsc_api_base_url() -> str:
    return (os.environ.get("DBS_CRSC_API_BASE") or _DEFAULT_CRSC_BASE).strip().rstrip("/")


def scheduled_check_interval_label() -> str:
    """daily | weekly | off — off when unset or invalid."""
    v = (os.environ.get("DBS_STATUS_CHECK_INTERVAL") or "off").strip().lower()
    return v if v in ("daily", "weekly", "off") else "off"


def scheduled_checker_label() -> str:
    return (os.environ.get("DBS_SCHEDULED_CHECKER_LABEL") or "Scheduled DBS check").strip() or "Scheduled DBS check"


def organisation_name_for_request() -> str:
    org = (os.environ.get("DBS_ORGANISATION_NAME") or "").strip()
    if org:
        return org
    try:
        plugins_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..")
        )
        pm = PluginManager(plugins_dir=plugins_dir)
        cm = pm.get_core_manifest() or {}
        return (
            (cm.get("site_settings") or {}).get("company_name") or ""
        ).strip() or "Organisation"
    except Exception:
        return "Organisation"


def split_forename_surname(full_name: str) -> Tuple[str, str]:
    parts = (full_name or "").strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], parts[0]
    return " ".join(parts[:-1]), parts[-1]


def format_dob_dd_mm_yyyy(dob: Optional[date]) -> Optional[str]:
    if dob is None:
        return None
    return f"{dob.day:02d}/{dob.month:02d}/{dob.year}"


def parse_crsc_status_xml(body: str) -> Optional[str]:
    """
    Extract DBS result code from XML or plain text. Official guide uses structured XML
    with result enums; tolerate whitespace/casing variants.
    """
    if not body or not str(body).strip():
        return None
    text = str(body)
    upper = text.upper()
    for token in sorted(KNOWN_RESULTS, key=len, reverse=True):
        if token in upper:
            return token
    # Element-style: <...>BLANK_NO_NEW_INFO</...>
    m = re.search(
        r">(BLANK_NO_NEW_INFO|NON_BLANK_NO_NEW_INFO|NEW_INFO)<", upper)
    if m:
        return m.group(1)
    return None


def _normalize_body_for_hash(body: str) -> bytes:
    return (body or "").strip().encode("utf-8", errors="replace")


def disclosure_ref_from_profile(row: Dict[str, Any]) -> str:
    ref = (row.get("dbs_certificate_ref") or "").strip()
    if ref:
        return ref
    return (row.get("dbs_number") or "").strip()


def fetch_crsc_status(
    disclosure_ref: str,
    date_of_birth_dd_mm_yyyy: str,
    surname: str,
    employee_forename: str,
    employee_surname: str,
    organisation_name: str,
    timeout_s: float = 60.0,
) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    GET CRSC status endpoint. Returns (http_status, response_text, error_message).
    Does not log request URL or query (PII).
    """
    base = crsc_api_base_url()
    path_ref = quote(str(disclosure_ref).strip(), safe="")
    q = {
        "dateOfBirth": date_of_birth_dd_mm_yyyy,
        "surname": surname,
        "hasAgreedTermsAndConditions": "true",
        "organisationName": organisation_name,
        "employeeSurname": employee_surname,
        "employeeForename": employee_forename,
    }
    query = urlencode(q)
    url = f"{base}/{path_ref}?{query}"
    try:
        r = requests.get(
            url,
            timeout=timeout_s,
            headers={"Accept": "application/xml, text/xml, */*"},
        )
        return r.status_code, r.text, None
    except requests.RequestException as e:
        _logger.warning("DBS CRSC request failed (no URL logged): %s", e)
        return None, None, str(e)


def _load_staff_row_for_dbs(cur, contractor_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT c.id, c.name, c.email,
               h.date_of_birth, h.dbs_number, h.dbs_certificate_ref,
               h.dbs_update_service_subscribed, h.dbs_update_consent_at,
               h.dbs_last_status_code
        FROM tb_contractors c
        INNER JOIN hr_staff_details h ON h.contractor_id = c.id
        WHERE c.id = %s
        """,
        (contractor_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def list_subscribed_contractor_ids_for_scheduled_run(cur) -> list:
    cur.execute(
        """
        SELECT h.contractor_id
        FROM hr_staff_details h
        WHERE h.dbs_update_service_subscribed = 1
          AND h.dbs_update_consent_at IS NOT NULL
          AND h.date_of_birth IS NOT NULL
          AND (
            (h.dbs_certificate_ref IS NOT NULL AND TRIM(h.dbs_certificate_ref) != '')
            OR (h.dbs_number IS NOT NULL AND TRIM(h.dbs_number) != '')
          )
        """
    )
    return [int(r["contractor_id"]) for r in cur.fetchall()]


def run_dbs_status_check(
    contractor_id: int,
    *,
    channel: str,
    checker_user_id: Optional[str],
    checker_label: Optional[str],
) -> Dict[str, Any]:
    """
    Run one CRSC check, write dbs_status_check_log, update hr_staff_details summary fields.
    channel: 'manual' | 'scheduled'
    """
    out: Dict[str, Any] = {
        "ok": False,
        "contractor_id": contractor_id,
        "result_type": None,
        "http_status": None,
        "error": None,
        "new_info_alert": False,
    }
    if not is_dbs_update_service_enabled():
        out["error"] = "DBS Update Service integration is disabled (set DBS_UPDATE_SERVICE_ENABLED)."
        return out

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        row = _load_staff_row_for_dbs(cur, contractor_id)
        if not row:
            out["error"] = "Employee or HR details not found."
            return out
        if not int(row.get("dbs_update_service_subscribed") or 0):
            out["error"] = "Update Service is not enabled for this employee."
            return out
        if not row.get("dbs_update_consent_at"):
            out["error"] = "Record consent before running a status check."
            return out
        dob = row.get("date_of_birth")
        if dob is None:
            out["error"] = "Date of birth is required for DBS status checks."
            return out
        dob_s = format_dob_dd_mm_yyyy(dob)
        if not dob_s:
            out["error"] = "Date of birth is required for DBS status checks."
            return out

        disc = disclosure_ref_from_profile(row)
        if not disc:
            out["error"] = "Certificate / disclosure reference or DBS number is required."
            return out

        full_name = row.get("name") or ""
        forename, surname_emp = split_forename_surname(full_name)
        org = organisation_name_for_request()

        prev_status = (row.get("dbs_last_status_code") or "").strip().upper() or None

        http_status, body, err = fetch_crsc_status(
            disc,
            dob_s,
            surname_emp,
            forename,
            surname_emp,
            org,
        )
        out["http_status"] = http_status

        checked_at = datetime.utcnow()
        result_type = None
        error_message = err
        body_hash = None

        if error_message:
            pass
        elif http_status is not None and http_status >= 400:
            error_message = f"HTTP {http_status}"
        elif body is None:
            error_message = "Empty response"
        else:
            result_type = parse_crsc_status_xml(body)
            if not result_type:
                error_message = "Could not parse DBS status from response"
            else:
                body_hash = hashlib.sha256(_normalize_body_for_hash(body)).hexdigest()

        ch = channel if channel in ("manual", "scheduled") else "manual"
        checker_label = (checker_label or "").strip() or None

        cur.execute(
            """
            INSERT INTO dbs_status_check_log (
              contractor_id, checked_at, status_code, result_type, channel,
              checker_user_id, checker_label, http_status, error_message
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                contractor_id,
                checked_at,
                result_type,
                result_type,
                ch,
                checker_user_id,
                checker_label,
                http_status,
                error_message,
            ),
        )

        if result_type and not error_message:
            out["ok"] = True
            out["result_type"] = result_type
            cur.execute(
                """
                UPDATE hr_staff_details SET
                  dbs_last_check_at = %s,
                  dbs_last_status_code = %s,
                  dbs_last_response_hash = %s
                WHERE contractor_id = %s
                """,
                (checked_at, result_type, body_hash, contractor_id),
            )
            if result_type == "NEW_INFO" and prev_status != "NEW_INFO":
                out["new_info_alert"] = True
        else:
            out["error"] = error_message or "Check failed"
            cur.execute(
                """
                UPDATE hr_staff_details SET dbs_last_check_at = %s WHERE contractor_id = %s
                """,
                (checked_at, contractor_id),
            )

        conn.commit()
        if out.get("new_info_alert"):
            try:
                from app.compliance_audit import log_security_event

                log_security_event(
                    "dbs_update_service_new_info",
                    contractor_id=int(contractor_id),
                    channel=ch,
                )
            except Exception:
                pass
    except Exception as e:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        _logger.exception("DBS status check failed for contractor %s", contractor_id)
        out["error"] = str(e)
    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    return out


def run_scheduled_dbs_status_checks() -> None:
    if not is_dbs_update_service_enabled():
        return
    if scheduled_check_interval_label() == "off":
        return
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        ids = list_subscribed_contractor_ids_for_scheduled_run(cur)
    except Exception as e:
        _logger.warning("Scheduled DBS eligibility query failed: %s", e)
        return
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    label = scheduled_checker_label()
    for cid in ids:
        try:
            run_dbs_status_check(
                cid,
                channel="scheduled",
                checker_user_id=None,
                checker_label=label,
            )
        except Exception:
            _logger.exception("Scheduled DBS check failed for contractor %s", cid)
