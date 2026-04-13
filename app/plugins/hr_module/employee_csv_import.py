"""CSV employee import: parse file, map columns, create/update tb_contractors + HR profile fields."""

from __future__ import annotations

import csv
import io
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# (internal_key, label for mapping UI)
HR_EMPLOYEE_IMPORT_FIELDS: List[Tuple[str, str]] = [
    ("", "— Ignore column —"),
    ("name", "Full name (required for new rows)"),
    ("email", "Email — unique login (required)"),
    ("role_name", "Time Billing role (optional — default applied if empty)"),
    ("password", "Not used — new staff get a temporary password by email"),
    ("phone", "Phone"),
    ("status", "Status (active / inactive)"),
    ("employment_type", "Employment type (paye / self_employed)"),
    ("job_title", "Job title"),
    ("department", "Department"),
    ("address_line1", "Address line 1"),
    ("address_line2", "Address line 2"),
    ("postcode", "Postcode"),
    ("date_of_birth", "Date of birth"),
    ("emergency_contact_name", "Emergency contact name"),
    ("emergency_contact_phone", "Emergency contact phone"),
    ("manager_email", "Manager email (must match existing employee)"),
    ("driving_licence_number", "Driving licence number"),
    ("driving_licence_expiry", "Driving licence expiry"),
    ("dbs_number", "DBS number"),
    ("dbs_expiry", "DBS expiry"),
    ("contract_start", "Contract start date"),
    ("contract_end", "Contract end date"),
]

_STATUS_ALIASES = {
    "active": "active",
    "inactive": "inactive",
    "1": "active",
    "0": "inactive",
    "yes": "active",
    "no": "inactive",
}

_ET_ALIASES = {
    "paye": "paye",
    "p.a.y.e": "paye",
    "employed": "paye",
    "self_employed": "self_employed",
    "self employed": "self_employed",
    "self-employed": "self_employed",
    "contractor": "self_employed",
    "cis": "self_employed",
}

_PROFILE_KEYS_FOR_UPDATE = frozenset(
    {
        "phone",
        "job_title",
        "department",
        "address_line1",
        "address_line2",
        "postcode",
        "date_of_birth",
        "emergency_contact_name",
        "emergency_contact_phone",
        "driving_licence_number",
        "driving_licence_expiry",
        "dbs_number",
        "dbs_expiry",
        "contract_start",
        "contract_end",
    }
)


def parse_csv_file(content: bytes, max_rows: int = 2500) -> Tuple[List[str], List[Dict[str, str]]]:
    """Return (headers, rows as dicts). Tries utf-8-sig then latin-1."""
    text = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        text = content.decode("utf-8", errors="replace")

    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel

    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    headers = [h.strip() for h in (reader.fieldnames or []) if h is not None]
    rows: List[Dict[str, str]] = []
    for i, row in enumerate(reader):
        if i >= max_rows:
            break
        clean = {(k or "").strip(): (v or "").strip() for k, v in row.items() if k is not None}
        if any(clean.values()):
            rows.append(clean)
    return headers, rows


def _normalize_date_str(val: str) -> Optional[str]:
    """Return YYYY-MM-DD for HR profile date fields, or None."""
    if not val or not str(val).strip():
        return None
    s = str(val).strip()[:32]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date().isoformat()
        except ValueError:
            continue
    try:
        if s.replace(".", "").replace("-", "").isdigit():
            n = float(s)
            if 30000 < n < 60000:
                base = date(1899, 12, 30)
                return (base + timedelta(days=int(n))).isoformat()
    except Exception:
        pass
    return None


def _norm_status(val: str) -> Optional[str]:
    if not val or not str(val).strip():
        return None
    k = str(val).strip().lower().replace("-", "_").replace(" ", "_")
    if k in _STATUS_ALIASES:
        return _STATUS_ALIASES[k]
    if k in ("active", "inactive"):
        return k
    return None


def _norm_employment(val: str) -> Optional[str]:
    if not val or not str(val).strip():
        return None
    k = re.sub(r"\s+", " ", str(val).strip().lower())
    k = k.replace("-", " ").replace(".", "")
    k2 = k.replace(" ", "_")
    if k2 in _ET_ALIASES:
        return _ET_ALIASES[k2]
    if k in _ET_ALIASES:
        return _ET_ALIASES[k]
    return None


def build_hr_import_payload(
    row: Dict[str, str], header_to_field: Dict[str, str]
) -> Dict[str, Any]:
    acc: Dict[str, Any] = {}
    for header, field in header_to_field.items():
        if not field:
            continue
        raw = row.get(header, "")
        if raw is None:
            continue
        s = str(raw).strip()
        if not s:
            continue
        if field == "email":
            acc["email"] = s.lower()
        elif field == "name":
            acc["name"] = s[:255]
        elif field == "role_name":
            acc["role_name"] = s.lower()
        elif field == "password":
            acc["password"] = s  # not used for new rows; import always generates + emails
        elif field == "phone":
            acc["phone"] = s[:64]
        elif field == "status":
            st = _norm_status(s)
            if st:
                acc["status"] = st
        elif field == "employment_type":
            et = _norm_employment(s)
            if et:
                acc["employment_type"] = et
        elif field == "manager_email":
            acc["manager_email"] = s.lower()[:255]
        elif field in (
            "date_of_birth",
            "driving_licence_expiry",
            "dbs_expiry",
            "contract_start",
            "contract_end",
        ):
            d = _normalize_date_str(s)
            if d:
                acc[field] = d
        elif field == "address_line1":
            acc[field] = s[:255]
        elif field == "address_line2":
            acc[field] = s[:255]
        elif field == "postcode":
            acc[field] = s[:32]
        elif field in ("job_title", "department", "emergency_contact_name"):
            acc[field] = s[:128]
        elif field == "emergency_contact_phone":
            acc[field] = s[:64]
        elif field == "driving_licence_number":
            acc[field] = s[:64]
        elif field == "dbs_number":
            acc[field] = s[:64]
    return acc


def import_hr_employee_rows(
    rows: List[Dict[str, str]],
    header_to_field: Dict[str, str],
    *,
    update_existing: bool,
) -> Dict[str, Any]:
    from . import services as hr_svc

    created = 0
    updated = 0
    skipped = 0
    errors: List[str] = []
    credentials_emailed = 0
    email_warnings: List[str] = []

    for idx, row in enumerate(rows, start=2):
        payload = build_hr_import_payload(row, header_to_field)
        email = payload.get("email")
        if not email or "@" not in email:
            skipped += 1
            errors.append(f"Row {idx}: missing or invalid email (map an email column).")
            continue

        cid_existing = hr_svc.admin_get_contractor_id_by_email(email)

        if cid_existing:
            if not update_existing:
                skipped += 1
                continue
            try:
                ok, msg = hr_svc.admin_update_contractor_basic(
                    cid_existing,
                    name=payload.get("name"),
                    status=payload.get("status"),
                    role_name=payload.get("role_name"),
                )
                if not ok:
                    errors.append(f"Row {idx} ({email}): {msg}")
                    skipped += 1
                    continue
                profile: Dict[str, Any] = {}
                for k in _PROFILE_KEYS_FOR_UPDATE:
                    if k in payload:
                        profile[k] = payload[k]
                mgr_email = payload.get("manager_email")
                if mgr_email:
                    mid = hr_svc.admin_get_contractor_id_by_email(mgr_email)
                    if mid and mid != cid_existing:
                        profile["manager_contractor_id"] = str(mid)
                if profile:
                    hr_svc.admin_update_staff_profile(cid_existing, profile)
                if payload.get("employment_type") in ("paye", "self_employed"):
                    hr_svc.admin_update_contractor_employment_type(
                        cid_existing, payload["employment_type"]
                    )
                updated += 1
            except Exception as e:
                errors.append(f"Row {idx} ({email}): {e}")
                skipped += 1
            continue

        name = (payload.get("name") or "").strip()
        role_opt = (payload.get("role_name") or "").strip() or None
        if not name:
            skipped += 1
            errors.append(f"Row {idx} ({email}): name is required for new employees.")
            continue

        pwd_plain = hr_svc.generate_hr_portal_password()
        try:
            ok, msg, new_id = hr_svc.admin_create_contractor_employee(
                name,
                email,
                role_name=role_opt,
                password=pwd_plain,
                phone=payload.get("phone"),
                status=payload.get("status") or "active",
                employment_type=payload.get("employment_type"),
            )
            if not ok or not new_id:
                errors.append(f"Row {idx} ({email}): {msg}")
                skipped += 1
                continue
            created += 1
            try:
                profile = {}
                for k in _PROFILE_KEYS_FOR_UPDATE:
                    if k in payload:
                        profile[k] = payload[k]
                mgr_email = payload.get("manager_email")
                if mgr_email:
                    mid = hr_svc.admin_get_contractor_id_by_email(mgr_email)
                    if mid and mid != int(new_id):
                        profile["manager_contractor_id"] = str(mid)
                if profile:
                    hr_svc.admin_update_staff_profile(int(new_id), profile)
            except Exception as prof_e:
                email_warnings.append(
                    f"Row {idx} ({email}): account created but some HR fields failed: {prof_e}"
                )
            sent, send_msg = hr_svc.send_hr_import_portal_credentials_email(
                email, name, pwd_plain
            )
            if sent:
                credentials_emailed += 1
            else:
                email_warnings.append(
                    f"Row {idx} ({email}): account created but welcome email not sent — {send_msg} "
                    "Set a portal password in HR (employee profile) or use your organisation’s reset flow if available."
                )
        except Exception as e:
            errors.append(f"Row {idx} ({email}): {e}")
            skipped += 1

    return {
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "errors": errors[:200],
        "credentials_emailed": credentials_emailed,
        "email_warnings": email_warnings[:200],
    }
