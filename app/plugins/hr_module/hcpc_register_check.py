"""
HCPC register checks via the HCPC Online Register API.

Implements:
- one-off manual checks for a single employee;
- mass/manual checks for every employee with an HCPC registration number; and
- scheduled mass checks driven from the HR plugin manifest.

The API guidance document v2.6 (last updated February 2025) documents public GET
endpoints returning XML for individual lookups.

Other UK regulators (GMC, NMC, GPhC, etc.) do not expose an equivalent free
employer lookup API in Sparrow; those registrations are stored manually on
``hr_staff_details`` (see HR install upgrade columns).
"""

from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from app.objects import get_db_connection

_logger = logging.getLogger(__name__)

_HR_MANIFEST_PATH = Path(__file__).resolve().parent / "manifest.json"

_DEFAULT_HCPC_API_BASE = "https://prod2online-apm.hcpc-uk.org/online-register"

HCPC_PROFESSION_LABELS = {
    "AS": "Arts therapist",
    "BS": "Biomedical scientist",
    "CH": "Chiropodist / podiatrist",
    "CS": "Clinical scientist",
    "DT": "Dietitian",
    "HAD": "Hearing aid dispenser",
    "OT": "Occupational therapist",
    "ODP": "Operating department practitioner",
    "OR": "Orthoptist",
    "PA": "Paramedic",
    "PH": "Physiotherapist",
    "PO": "Prosthetist / orthotist",
    "PYL": "Practitioner psychologist",
    "RA": "Radiographer",
    "SL": "Speech and language therapist",
}

_HCPC_NOT_FOUND_PHRASE = "no professionals were found"
_AUTO_NOTE_PREFIX = "[HCPC register check]"


def is_hcpc_register_api_enabled() -> bool:
    """HTTP checks are enabled unless explicitly disabled server-wide."""
    v = (os.environ.get("HCPC_REGISTER_API_ENABLED") or "").strip().lower()
    if v in ("0", "false", "no", "off", "disabled"):
        return False
    return True


def hcpc_api_base_url() -> str:
    return (os.environ.get("HCPC_REGISTER_API_BASE") or _DEFAULT_HCPC_API_BASE).strip().rstrip("/")


def hcpc_request_timeout_seconds() -> float:
    raw = (os.environ.get("HCPC_REGISTER_TIMEOUT_SECONDS") or "30").strip()
    try:
        value = float(raw)
    except ValueError:
        return 30.0
    return max(5.0, min(value, 120.0))


def get_hcpc_mass_check_interval_days_from_manifest() -> int:
    """HR plugin manifest setting ``hcpc_mass_check_interval_days`` (0 = off, 1 = daily, N = every N days)."""
    try:
        with open(_HR_MANIFEST_PATH, encoding="utf-8") as f:
            m = json.load(f)
        raw = (m.get("settings") or {}).get("hcpc_mass_check_interval_days")
        if not isinstance(raw, dict):
            return 0
        v = str(raw.get("value") or "0").strip().lower()
        if v in ("", "off", "none", "false", "no", "disabled", "0"):
            return 0
        if v in ("daily", "everyday"):
            return 1
        if v == "weekly":
            return 7
        n = int(float(v))
        if n < 1:
            return 0
        return min(n, 90)
    except (ValueError, TypeError, OSError, json.JSONDecodeError):
        return 0


def set_hcpc_mass_check_interval_days_in_manifest(days: int) -> Tuple[bool, str]:
    """Persist interval to hr_module manifest (0-90). Creates settings entry if missing."""
    try:
        days_i = int(days)
    except (TypeError, ValueError):
        return False, "Invalid interval."
    days_i = max(0, min(days_i, 90))
    tooltip = (
        "Mass HCPC register checks for every employee with an HCPC registration number. "
        "0 = off, 1 = every day, 2-90 = every N days. Saved from HR People dashboard "
        "or plugin settings; picked up within about an hour."
    )
    try:
        with open(_HR_MANIFEST_PATH, encoding="utf-8") as f:
            m = json.load(f)
        settings = m.setdefault("settings", {})
        key = "hcpc_mass_check_interval_days"
        if key not in settings or not isinstance(settings[key], dict):
            settings[key] = {"value": str(days_i), "editable": True, "tooltip": tooltip}
        else:
            settings[key]["value"] = str(days_i)
            settings[key].setdefault("editable", True)
            settings[key].setdefault("tooltip", tooltip)
        with open(_HR_MANIFEST_PATH, "w", encoding="utf-8") as f:
            f.write(json.dumps(m, indent=4) + "\n")
        return True, ""
    except OSError as e:
        return False, str(e)


def hcpc_scheduled_check_interval_label() -> str:
    d = get_hcpc_mass_check_interval_days_from_manifest()
    if d <= 0:
        return "off"
    if d == 1:
        return "daily"
    return f"every {d} days"


def _fetch_max_scheduled_check_time() -> Optional[datetime]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'hcpc_register_check_log'")
        if not cur.fetchone():
            return None
        cur.execute(
            """
            SELECT MAX(checked_at) FROM hcpc_register_check_log
            WHERE channel = %s
            """,
            ("scheduled",),
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        t = row[0]
        if isinstance(t, datetime):
            return t.replace(tzinfo=None) if t.tzinfo else t
        return None
    except Exception:
        return None
    finally:
        cur.close()
        conn.close()


def hcpc_mass_run_is_due(interval_days: int) -> bool:
    if interval_days <= 0:
        return False
    last = _fetch_max_scheduled_check_time()
    if last is None:
        return True
    elapsed = (datetime.utcnow() - last).total_seconds()
    return elapsed >= interval_days * 86400


def run_scheduled_hcpc_register_checks_if_due() -> None:
    if not is_hcpc_register_api_enabled():
        return
    days = get_hcpc_mass_check_interval_days_from_manifest()
    if days <= 0 or not hcpc_mass_run_is_due(days):
        return
    run_mass_hcpc_register_checks(channel="scheduled", checker_label=scheduled_checker_label())


def scheduled_checker_label() -> str:
    return (os.environ.get("HCPC_SCHEDULED_CHECKER_LABEL") or "Scheduled HCPC check").strip() or "Scheduled HCPC check"


def hcpc_register_integration_info() -> Dict[str, Any]:
    return {
        "employer_check_api_documented": True,
        "automated_check_available": is_hcpc_register_api_enabled(),
        "public_api_docs_url": None,
        "register_url": "https://www.hcpc-uk.org/check-the-register/",
        "multiple_search_url": "https://www.hcpc-uk.org/multiple-registrant-search/",
        "employer_info_url": (
            "https://www.hcpc-uk.org/employers/registration/checking-your-employees-registration/"
        ),
        "contact_email": "web@hcpc-uk.org",
        "api_base_url": hcpc_api_base_url(),
        "notes": (
            "Sparrow uses the HCPC Online Register API documented in guidance v2.6. "
            "Set HCPC_REGISTER_API_ENABLED=0/false/no/off to disable HTTP lookups."
        ),
    }


def _normalize_body_for_hash(body: str) -> bytes:
    return (body or "").strip().encode("utf-8", errors="replace")


def _parse_hcpc_date(value: Optional[str]) -> Optional[date]:
    s = (value or "").strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _clean_status_text(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    text = html.unescape(str(raw))
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _extract_text(parent: ET.Element, tag_name: str) -> Optional[str]:
    for el in parent.iter():
        if el.tag.rsplit("}", 1)[-1].lower() == tag_name.lower():
            return (el.text or "").strip() or None
    return None


def _extract_modalities(parent: ET.Element) -> List[str]:
    values: List[str] = []
    for el in parent.iter():
        if el.tag.rsplit("}", 1)[-1].lower() != "modality":
            continue
        v = (el.text or "").strip()
        if v:
            values.append(v)
    return values


def normalize_hcpc_registration_number(raw: Optional[str]) -> Optional[Dict[str, str]]:
    """
    Parse `PA123456` / `PA 123456` / `pa012345`.
    Returns uppercase profcode + canonical registration number with six digits.
    """
    s = re.sub(r"[^A-Za-z0-9]", "", str(raw or "")).upper()
    if not s:
        return None
    m = re.fullmatch(r"([A-Z]{2,3})(\d{1,6})", s)
    if not m:
        return None
    profcode, digits = m.groups()
    if profcode not in HCPC_PROFESSION_LABELS:
        return None
    padded = digits.zfill(6)
    return {
        "profcode": profcode,
        "digits": padded,
        "registration_number": f"{profcode}{padded}",
        "display_profession": HCPC_PROFESSION_LABELS.get(profcode) or profcode,
    }


def _registration_number_candidates(raw: Optional[str]) -> List[Dict[str, str]]:
    """Exact-ish cleaned number first, then zero-padded canonical number if different."""
    s = re.sub(r"[^A-Za-z0-9]", "", str(raw or "")).upper()
    parsed = normalize_hcpc_registration_number(s)
    if not parsed:
        return []
    out: List[Dict[str, str]] = []
    if re.fullmatch(r"[A-Z]{2,3}\d{1,6}", s):
        out.append(
            {
                "profcode": parsed["profcode"],
                "digits": s[len(parsed["profcode"]):],
                "registration_number": s,
                "display_profession": parsed["display_profession"],
            }
        )
    if parsed["registration_number"] not in {x["registration_number"] for x in out}:
        out.append(parsed)
    return out


def map_hcpc_status(api_status_text: Optional[str], error_text: Optional[str] = None) -> str:
    if error_text and _HCPC_NOT_FOUND_PHRASE in error_text.lower():
        return "not_found"
    status = (api_status_text or "").strip().lower()
    if not status:
        return "uncertain"
    if "registered" in status and not any(x in status for x in ("not registered", "no longer", "lapsed", "removed", "suspended", "struck")):
        return "verified_on_register"
    if any(x in status for x in ("lapsed", "removed", "suspended", "struck", "expired", "no longer", "not currently registered", "registration ended")):
        return "no_longer_listed"
    return "uncertain"


def parse_hcpc_detail_xml(body: str) -> Dict[str, Any]:
    """
    Parse detail XML from `/details`.

    Returns a dict with `ok`, `detail`, `error_message`.
    """
    if not (body or "").strip():
        return {"ok": False, "detail": None, "error_message": "Empty HCPC response."}
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return {"ok": False, "detail": None, "error_message": "Invalid HCPC XML response."}

    error_el = None
    for el in root.iter():
        if el.tag.rsplit("}", 1)[-1].lower() == "error":
            error_el = el
            break
    if error_el is not None:
        msg = (error_el.text or "").strip() or "HCPC returned an error."
        return {"ok": False, "detail": None, "error_message": msg}

    detail_item = None
    for el in root.iter():
        if el.tag.rsplit("}", 1)[-1].lower() == "detailitem":
            detail_item = el
            break
    if detail_item is None:
        return {"ok": False, "detail": None, "error_message": "HCPC response did not include detailItem."}

    status_text = _clean_status_text(_extract_text(detail_item, "status"))
    modalities = _extract_modalities(detail_item)
    entitlements = []
    for api_name, label in (
        ("la", "POM-A"),
        ("pom", "POM-S"),
        ("sp", "SP"),
        ("ip", "IP"),
        ("ps", "PS"),
        ("me", "ME"),
    ):
        value = (_extract_text(detail_item, api_name) or "").strip().lower()
        if value == "yes":
            entitlements.append(label)

    profcode = (_extract_text(detail_item, "profcode") or "").upper() or None
    regnum_digits = _extract_text(detail_item, "regnum")
    registration_number = None
    if profcode and regnum_digits:
        registration_number = f"{profcode}{regnum_digits}"

    return {
        "ok": True,
        "detail": {
            "registration_number": registration_number,
            "regnum_digits": regnum_digits,
            "profcode": profcode,
            "name": _extract_text(detail_item, "name"),
            "town": _extract_text(detail_item, "town"),
            "status_text": status_text,
            "registered_from": _parse_hcpc_date(_extract_text(detail_item, "registeredFrom")),
            "registered_to": _parse_hcpc_date(_extract_text(detail_item, "registeredTo")),
            "modalities": modalities,
            "entitlements": entitlements,
            "profession": HCPC_PROFESSION_LABELS.get(profcode or "", profcode),
        },
        "error_message": None,
    }


def fetch_hcpc_registration_details(
    registration_number: str,
    profcode: str,
    timeout_s: Optional[float] = None,
) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    GET the HCPC detail endpoint. Returns (http_status, response_text, error_message).
    """
    if timeout_s is None:
        timeout_s = hcpc_request_timeout_seconds()
    try:
        r = requests.get(
            f"{hcpc_api_base_url()}/details",
            params={"FORMAT": "XML", "PROFCODE": profcode, "REGNUM": registration_number},
            headers={"Accept": "application/xml, text/xml, text/plain, */*"},
            timeout=timeout_s,
        )
        return r.status_code, r.text, None
    except requests.RequestException as e:
        return None, None, str(e) or "HCPC request failed."


def _generated_hcpc_note(summary: str) -> str:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return f"{_AUTO_NOTE_PREFIX} {ts} {summary}".strip()


def _merge_generated_note(existing: Optional[str], summary: Optional[str]) -> Optional[str]:
    existing_lines = [line for line in (existing or "").splitlines() if not line.strip().startswith(_AUTO_NOTE_PREFIX)]
    generated = _generated_hcpc_note(summary) if summary else None
    parts = []
    if generated:
        parts.append(generated)
    remainder = "\n".join(existing_lines).strip()
    if remainder:
        parts.append(remainder)
    merged = "\n\n".join(parts).strip()
    if not merged:
        return None
    return merged[:65000]


def _employee_row_for_hcpc_check(contractor_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT c.id, c.name, c.email,
                   h.hcpc_registration_number, h.hcpc_registered_on, h.hcpc_register_profession,
                   h.hcpc_register_status, h.hcpc_register_check_notes
            FROM tb_contractors c
            LEFT JOIN hr_staff_details h ON h.contractor_id = c.id
            WHERE c.id = %s
            """,
            (contractor_id,),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def _eligible_mass_hcpc_rows() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT c.id, c.name, c.email, h.hcpc_registration_number
            FROM tb_contractors c
            INNER JOIN hr_staff_details h ON h.contractor_id = c.id
            WHERE h.hcpc_registration_number IS NOT NULL
              AND TRIM(h.hcpc_registration_number) <> ''
            ORDER BY c.name ASC, c.id ASC
            """
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def _write_hcpc_check_result(
    contractor_id: int,
    *,
    mapped_status: Optional[str],
    check_notes: Optional[str],
    checked_at: datetime,
    profession: Optional[str],
    registered_from: Optional[date],
    checker_log: Dict[str, Any],
) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM tb_contractors WHERE id = %s", (contractor_id,))
        if not cur.fetchone():
            return
        cur.execute(
            """
            INSERT INTO hr_staff_details (
                contractor_id, hcpc_registered_on, hcpc_register_profession,
                hcpc_register_last_checked_at, hcpc_register_status, hcpc_register_check_notes
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                hcpc_registered_on = COALESCE(VALUES(hcpc_registered_on), hcpc_registered_on),
                hcpc_register_profession = COALESCE(VALUES(hcpc_register_profession), hcpc_register_profession),
                hcpc_register_last_checked_at = VALUES(hcpc_register_last_checked_at),
                hcpc_register_status = VALUES(hcpc_register_status),
                hcpc_register_check_notes = VALUES(hcpc_register_check_notes)
            """,
            (
                contractor_id,
                registered_from,
                profession,
                checked_at,
                mapped_status,
                check_notes,
            ),
        )
        cur.execute("SHOW TABLES LIKE 'hcpc_register_check_log'")
        if cur.fetchone():
            cur.execute(
                """
                INSERT INTO hcpc_register_check_log (
                    contractor_id, checked_at, result_type, channel, checker_user_id,
                    checker_label, http_status, registration_number, profession_code,
                    api_status_text, error_message, response_hash
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    contractor_id,
                    checked_at,
                    checker_log.get("result_type"),
                    checker_log.get("channel"),
                    checker_log.get("checker_user_id"),
                    checker_log.get("checker_label"),
                    checker_log.get("http_status"),
                    checker_log.get("registration_number"),
                    checker_log.get("profession_code"),
                    checker_log.get("api_status_text"),
                    checker_log.get("error_message"),
                    checker_log.get("response_hash"),
                ),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def list_hcpc_register_check_logs(contractor_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'hcpc_register_check_log'")
        if not cur.fetchone():
            return []
        cur.execute(
            """
            SELECT checked_at, result_type, channel, checker_user_id, checker_label,
                   http_status, registration_number, profession_code, api_status_text, error_message
            FROM hcpc_register_check_log
            WHERE contractor_id = %s
            ORDER BY checked_at DESC, id DESC
            LIMIT %s
            """,
            (contractor_id, max(1, int(limit))),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def _success_summary(detail: Dict[str, Any]) -> str:
    parts = []
    if detail.get("status_text"):
        parts.append(f"status {detail['status_text']}")
    if detail.get("registration_number"):
        parts.append(f"reg {detail['registration_number']}")
    if detail.get("profession"):
        parts.append(f"profession {detail['profession']}")
    if detail.get("registered_to"):
        parts.append(f"to {detail['registered_to'].isoformat()}")
    if detail.get("town"):
        parts.append(f"town {detail['town']}")
    if detail.get("entitlements"):
        parts.append(f"entitlements {', '.join(detail['entitlements'])}")
    if detail.get("modalities"):
        parts.append(f"modalities {', '.join(detail['modalities'])}")
    return "; ".join(parts) if parts else "successful check"


def run_hcpc_register_status_check(
    contractor_id: int,
    *,
    channel: str = "manual",
    checker_user_id: Optional[str] = None,
    checker_label: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run one HCPC register lookup, persist summary fields, and write `hcpc_register_check_log`.
    """
    contractor_id = int(contractor_id)
    checked_at = datetime.utcnow()

    if not is_hcpc_register_api_enabled():
        return {
            "ok": False,
            "result_type": "disabled",
            "message": "HCPC HTTP checks are disabled on this server.",
        }

    row = _employee_row_for_hcpc_check(contractor_id)
    if not row:
        return {"ok": False, "result_type": "missing_employee", "message": "Employee not found."}

    candidates = _registration_number_candidates(row.get("hcpc_registration_number"))
    if not candidates:
        return {
            "ok": False,
            "result_type": "missing_registration_number",
            "message": "No valid HCPC registration number is recorded on this profile.",
        }

    last_http_status = None
    last_error = None
    last_body = None
    selected_detail = None
    selected_candidate = None

    for candidate in candidates:
        http_status, body, error_message = fetch_hcpc_registration_details(
            candidate["registration_number"],
            candidate["profcode"],
        )
        last_http_status = http_status
        last_error = error_message
        last_body = body
        if error_message:
            continue
        parsed = parse_hcpc_detail_xml(body or "")
        if parsed["ok"]:
            selected_detail = parsed["detail"]
            selected_candidate = candidate
            break
        last_error = parsed.get("error_message")
        if last_error and _HCPC_NOT_FOUND_PHRASE in last_error.lower():
            continue

    response_hash = hashlib.sha256(_normalize_body_for_hash(last_body or "")).hexdigest() if last_body else None
    existing_notes = row.get("hcpc_register_check_notes")

    if selected_detail:
        mapped_status = map_hcpc_status(selected_detail.get("status_text"))
        profession = selected_detail.get("profession") or row.get("hcpc_register_profession")
        notes = _merge_generated_note(existing_notes, _success_summary(selected_detail))
        _write_hcpc_check_result(
            contractor_id,
            mapped_status=mapped_status,
            check_notes=notes,
            checked_at=checked_at,
            profession=profession,
            registered_from=selected_detail.get("registered_from"),
            checker_log={
                "result_type": mapped_status,
                "channel": channel,
                "checker_user_id": checker_user_id,
                "checker_label": checker_label,
                "http_status": last_http_status,
                "registration_number": selected_detail.get("registration_number") or (selected_candidate or {}).get("registration_number"),
                "profession_code": selected_detail.get("profcode") or (selected_candidate or {}).get("profcode"),
                "api_status_text": selected_detail.get("status_text"),
                "error_message": None,
                "response_hash": response_hash,
            },
        )
        return {
            "ok": True,
            "result_type": mapped_status,
            "api_status_text": selected_detail.get("status_text"),
            "http_status": last_http_status,
            "detail": selected_detail,
            "message": selected_detail.get("status_text") or "HCPC register updated.",
        }

    error_text = last_error or "HCPC lookup did not return a matching registrant."
    mapped_status = map_hcpc_status(None, error_text)
    notes = _merge_generated_note(existing_notes, error_text)
    _write_hcpc_check_result(
        contractor_id,
        mapped_status=mapped_status,
        check_notes=notes,
        checked_at=checked_at,
        profession=row.get("hcpc_register_profession"),
        registered_from=row.get("hcpc_registered_on"),
        checker_log={
            "result_type": mapped_status,
            "channel": channel,
            "checker_user_id": checker_user_id,
            "checker_label": checker_label,
            "http_status": last_http_status,
            "registration_number": candidates[-1]["registration_number"],
            "profession_code": candidates[-1]["profcode"],
            "api_status_text": None,
            "error_message": error_text,
            "response_hash": response_hash,
        },
    )
    return {
        "ok": bool(
            mapped_status == "not_found"
            or (
                last_http_status is not None
                and 200 <= int(last_http_status) < 300
                and error_text
                and _HCPC_NOT_FOUND_PHRASE in error_text.lower()
            )
        ),
        "result_type": mapped_status,
        "http_status": last_http_status,
        "message": error_text,
    }


def run_mass_hcpc_register_checks(
    *,
    channel: str = "manual_mass",
    checker_user_id: Optional[str] = None,
    checker_label: Optional[str] = None,
) -> Dict[str, int]:
    rows = _eligible_mass_hcpc_rows()
    summary = {
        "total": len(rows),
        "processed": 0,
        "verified_on_register": 0,
        "not_found": 0,
        "no_longer_listed": 0,
        "uncertain": 0,
        "failed": 0,
    }
    for row in rows:
        out = run_hcpc_register_status_check(
            int(row["id"]),
            channel=channel,
            checker_user_id=checker_user_id,
            checker_label=checker_label,
        )
        summary["processed"] += 1
        key = str(out.get("result_type") or "").strip().lower()
        if key in summary:
            summary[key] += 1
        elif not out.get("ok"):
            summary["failed"] += 1
    return summary
