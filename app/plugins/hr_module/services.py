import json
import logging
import os
import re
import secrets
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from app.objects import get_db_connection
from app.public_base import EMPLOYEE_PORTAL_PUBLIC_PATH, resolve_public_base_url

logger = logging.getLogger(__name__)

# Cross-plugin expectations: see compliance_integration_contract.py


def _coerce_profile_date(val: Any) -> Optional[date]:
    """Normalize DB date/datetime/str to date for expiry comparisons."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        s = val.strip()
        if len(s) >= 10:
            try:
                return date.fromisoformat(s[:10])
            except ValueError:
                return None
    return None


def profile_expired_compliance_items(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fields on the staff profile whose stored end/expiry date is strictly before today.
    Used for admin and portal detail views (badges / callouts).
    """
    today = date.today()
    out: List[Dict[str, Any]] = []
    checks = (
        ("driving_licence", "driving_licence_expiry", "Driving licence"),
        ("right_to_work", "right_to_work_expiry", "Right to work"),
        ("dbs", "dbs_expiry", "DBS"),
        ("contract_end", "contract_end", "Contract end"),
    )
    for doc_type, field, label in checks:
        d = _coerce_profile_date(profile.get(field))
        if d is not None and d < today:
            out.append({"doc_type": doc_type, "field": field,
                       "label": label, "expiry_date": d})
    return out


def default_employee_tb_role_name() -> str:
    """Time Billing role for new employees when none is supplied (CSV or manual add)."""
    raw = (os.environ.get("HR_DEFAULT_EMPLOYEE_ROLE")
           or "staff").strip().lower()
    return raw if raw else "staff"


def generate_hr_portal_password(length: int = 14) -> str:
    """Cryptographically random password for portal login (avoids ambiguous 0/O, 1/l/I)."""
    alphabet = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789"
    n = max(10, min(int(length), 24))
    return "".join(secrets.choice(alphabet) for _ in range(n))


def send_hr_import_portal_credentials_email(
    to_email: str,
    display_name: str,
    password_plain: str,
) -> Tuple[bool, str]:
    """
    Email a one-time welcome with portal URL + temporary password (CSV import and
    optional “generate & email” on manual add-employee).

    Set HR_IMPORT_WELCOME_EMAIL_DISABLED=1 or HR_PORTAL_WELCOME_EMAIL_DISABLED=1 to skip.
    Requires core SMTP_* env vars.
    """
    for key in ("HR_IMPORT_WELCOME_EMAIL_DISABLED", "HR_PORTAL_WELCOME_EMAIL_DISABLED"):
        if (os.environ.get(key) or "").strip().lower() in ("1", "true", "yes", "on"):
            return False, f"Welcome emails disabled ({key})."
    try:
        from app.objects import EmailManager

        em = EmailManager()
    except Exception as e:
        logger.warning("HR import welcome email: SMTP not available: %s", e)
        return False, "SMTP not configured."
    base = resolve_public_base_url()
    portal_url = (
        f"{base}{EMPLOYEE_PORTAL_PUBLIC_PATH}"
        if base
        else f"{EMPLOYEE_PORTAL_PUBLIC_PATH} (on this site)"
    )
    name = (display_name or "").strip() or "there"
    subject = "Your employee portal login"
    body = (
        f"Hello {name},\n\n"
        "An account has been created for you on our employee portal.\n\n"
        f"Employee portal: {portal_url}\n"
        f"Email (login): {to_email.strip().lower()}\n"
        f"Temporary password: {password_plain}\n\n"
        "Sign in with the details above. Change your password after login if your organisation asks you to.\n\n"
        "If you were not expecting this message, contact your manager or HR.\n"
    )
    try:
        from app.email_branding import send_branded_email

        send_branded_email(
            em,
            subject,
            body,
            [to_email.strip().lower()],
            preheader=subject,
        )
        return True, "ok"
    except Exception as e:
        logger.warning("HR import welcome email send failed: %s", e)
        return False, str(e) or "Send failed."


def _normalize_stored_relative_upload_path(path: Optional[Any]) -> Optional[str]:
    """Store uploads/* paths with forward slashes (Windows-safe for URLs and static resolution)."""
    if path is None:
        return None
    s = str(path).strip()
    if not s:
        return None
    return s.replace("\\", "/").strip()


def _truncate_str(val: Any, max_len: int) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s[:max_len] if len(s) > max_len else s


def _optional_manager_contractor_id(raw: Any, self_contractor_id: int) -> Optional[int]:
    """Parse manager id from form; reject self-reference and invalid ids."""
    if raw is None or str(raw).strip() == "":
        return None
    try:
        mid = int(raw)
    except (ValueError, TypeError):
        return None
    if mid == int(self_contractor_id):
        return None
    if mid < 1:
        return None
    return mid


def hr_safe_profile_picture_path(path: Optional[str]) -> Optional[str]:
    """Same rules as employee portal static paths (for templates / HR admin)."""
    try:
        from app.plugins.employee_portal_module.services import safe_profile_picture_path

        return safe_profile_picture_path(path)
    except Exception:
        return None


# Employee portal ep_todos reference (source_module must match upsert/complete calls)
_EP_HR_SOURCE = "hr_module"
_EP_HR_REF_TYPE = "hr_document_request"


def _hr_request_portal_path(request_id: int) -> str:
    return f"/hr/request/{int(request_id)}"


def _hr_ep_upsert_request_todo(
    contractor_id: int,
    request_id: int,
    title: str,
    *,
    rejected: bool = False,
    due_date: Optional[date] = None,
) -> None:
    """Ensure a pending portal todo points at this HR document request."""
    try:
        from app.plugins.employee_portal_module.services import upsert_pending_todo_for_reference

        if rejected:
            todo_title = f"HR: Document rejected — please re-upload — {title}"
        else:
            todo_title = f"HR: Submit document — {title}"
        upsert_pending_todo_for_reference(
            int(contractor_id),
            _EP_HR_SOURCE,
            _EP_HR_REF_TYPE,
            str(int(request_id)),
            todo_title,
            link_url=_hr_request_portal_path(request_id),
            due_date=due_date,
        )
    except Exception as e:
        logger.warning(
            "HR: could not upsert employee-portal todo for request %s: %s", request_id, e)


def _hr_ep_complete_request_todo(request_id: int) -> None:
    try:
        from app.plugins.employee_portal_module.services import complete_todo_by_reference

        complete_todo_by_reference(
            _EP_HR_SOURCE, _EP_HR_REF_TYPE, str(int(request_id)))
    except Exception as e:
        logger.warning(
            "HR: could not complete employee-portal todo for request %s: %s", request_id, e)


def _hr_ep_message_approved(contractor_id: int, request_id: int, title: str) -> None:
    try:
        from app.plugins.employee_portal_module.services import admin_send_message

        link = _hr_request_portal_path(request_id)
        admin_send_message(
            [int(contractor_id)],
            f"HR: Document approved — {title}",
            f'Your document for "{title}" has been approved. Open HR to view details and files: {link}',
            source_module="hr_module",
            sent_by_user_id=None,
        )
    except Exception as e:
        logger.warning(
            "HR: could not send approval message for request %s: %s", request_id, e)


def _hr_ep_message_rejected(
    contractor_id: int, request_id: int, title: str, admin_notes: Optional[str]
) -> None:
    try:
        from app.plugins.employee_portal_module.services import admin_send_message

        link = _hr_request_portal_path(request_id)
        body = f"Please upload a new file in HR: {link}"
        if admin_notes and str(admin_notes).strip():
            body += f"\n\nNote: {str(admin_notes).strip()}"
        admin_send_message(
            [int(contractor_id)],
            f"HR: Document rejected — {title}",
            body,
            source_module="hr_module",
            sent_by_user_id=None,
        )
    except Exception as e:
        logger.warning(
            "HR: could not send rejection message for request %s: %s", request_id, e)


# Request types for document requests
REQUEST_TYPES = ["right_to_work", "driving_licence",
                 "dbs", "contract", "profile_picture", "other"]

# Map document-request type → hr_staff_details column for the linked file path
REQUEST_TYPE_TO_STAFF_DOCUMENT_COLUMN = {
    "right_to_work": "right_to_work_document_path",
    "driving_licence": "driving_licence_document_path",
    "dbs": "dbs_document_path",
    "contract": "contract_document_path",
}

# Stored file categories (hr_employee_documents)
EMPLOYEE_DOCUMENT_CATEGORIES = [
    "general",
    "identification",
    "profile_picture",
    "contract",
    "dbs",
    "driving_licence",
    "right_to_work",
    "medical",
    "training",
    "other",
]

# --- Human-readable UI labels (do not show snake_case / system codes to users) ---


def _snake_to_title_words(code: str) -> str:
    """Fallback: unknown_code -> 'Unknown Code'."""
    if not code:
        return "—"
    parts = [p for p in str(code).strip().lower().replace(
        "-", "_").split("_") if p]
    if not parts:
        return str(code)
    return " ".join(p.capitalize() for p in parts)


REQUEST_TYPE_LABELS = {
    "right_to_work": "Right to work",
    "driving_licence": "Driving licence",
    "dbs": "DBS",
    "contract": "Contract",
    "profile_picture": "Profile photo",
    "other": "Other",
}

DOCUMENT_REQUEST_STATUS_LABELS = {
    "pending": "Pending",
    "uploaded": "Uploaded",
    "approved": "Approved",
    "rejected": "Rejected",
    "overdue": "Overdue",
}

DOCUMENT_UPLOAD_TYPE_LABELS = {
    "primary": "Primary",
    "replacement": "Replacement",
}

EMPLOYEE_DOCUMENT_CATEGORY_LABELS = {
    "general": "General",
    "identification": "Identification",
    "profile_picture": "Profile photo",
    "contract": "Contract",
    "dbs": "DBS",
    "driving_licence": "Driving licence",
    "right_to_work": "Right to work",
    "medical": "Medical",
    "training": "Training",
    "other": "Other",
}

EXPIRY_DOC_TYPE_LABELS = {
    "driving_licence": "Driving licence",
    "right_to_work": "Right to work",
    "dbs": "DBS",
    "contract_end": "Contract end",
}


def human_request_type(value: Optional[str]) -> str:
    if value is None or str(value).strip() == "":
        return REQUEST_TYPE_LABELS["other"]
    k = str(value).strip().lower()
    return REQUEST_TYPE_LABELS.get(k, _snake_to_title_words(k))


def human_document_request_status(value: Optional[str]) -> str:
    if value is None or str(value).strip() == "":
        return "—"
    k = str(value).strip().lower()
    return DOCUMENT_REQUEST_STATUS_LABELS.get(k, _snake_to_title_words(k))


def human_upload_document_type(value: Optional[str]) -> str:
    if value is None or str(value).strip() == "":
        return DOCUMENT_UPLOAD_TYPE_LABELS["primary"]
    k = str(value).strip().lower()
    return DOCUMENT_UPLOAD_TYPE_LABELS.get(k, _snake_to_title_words(k))


def human_employee_document_category(value: Optional[str]) -> str:
    if value is None or str(value).strip() == "":
        return "—"
    k = str(value).strip().lower()
    return EMPLOYEE_DOCUMENT_CATEGORY_LABELS.get(k, _snake_to_title_words(k))


def human_expiry_doc_type(value: Optional[str]) -> str:
    if value is None or str(value).strip() == "":
        return "—"
    k = str(value).strip().lower()
    return EXPIRY_DOC_TYPE_LABELS.get(k, _snake_to_title_words(k))


def human_contractor_status(value: Optional[str]) -> str:
    """tb_contractors.status: active / inactive."""
    if value is None or str(value).strip() == "":
        return "—"
    k = str(value).strip().lower()
    return {"active": "Active", "inactive": "Inactive"}.get(k, _snake_to_title_words(k))


def human_profile_field(value: Optional[str]) -> str:
    """
    Free-text profile values (e.g. right_to_work_type, contract_type): prettify snake_case
    but leave normal phrases (e.g. 'UK Passport') unchanged.
    """
    if value is None or str(value).strip() == "":
        return "—"
    s = str(value).strip()
    if "_" in s or "-" in s:
        return _snake_to_title_words(s)
    if s.islower():
        return " ".join(w.capitalize() for w in s.split())
    return s


HPAC_REGISTER_STATUS_VALUES = frozenset(
    {
        "not_checked",
        "verified_on_register",
        "not_found",
        "no_longer_listed",
        "uncertain",
    }
)

HCPC_REGISTER_STATUS_VALUES = HPAC_REGISTER_STATUS_VALUES


def human_hpac_register_status(value: Optional[str]) -> str:
    """Labels for ``hpac_register_status`` on staff profiles."""
    if value is None or str(value).strip() == "":
        return "Not checked"
    k = str(value).strip().lower()
    return {
        "not_checked": "Not checked",
        "verified_on_register": "Verified on HPAC register",
        "not_found": "Not found on register",
        "no_longer_listed": "No longer listed / removed",
        "uncertain": "Uncertain — needs review",
    }.get(k, _snake_to_title_words(k))


def human_hcpc_register_status(value: Optional[str]) -> str:
    """Labels for ``hcpc_register_status`` on staff profiles."""
    if value is None or str(value).strip() == "":
        return "Not checked"
    k = str(value).strip().lower()
    return {
        "not_checked": "Not checked",
        "verified_on_register": "Verified on HCPC register",
        "not_found": "Not found on register",
        "no_longer_listed": "No longer listed / removed",
        "uncertain": "Uncertain — needs review",
    }.get(k, _snake_to_title_words(k))


def human_generic_register_status(value: Optional[str]) -> str:
    """Same value set as HPAC/HCPC, neutral wording for GMC / NMC / GPhC (manual checks)."""
    if value is None or str(value).strip() == "":
        return "Not set"
    k = str(value).strip().lower()
    return {
        "not_checked": "Not checked",
        "verified_on_register": "Verified on official register",
        "not_found": "Not found on register",
        "no_longer_listed": "No longer listed / removed",
        "uncertain": "Uncertain — needs review",
    }.get(k, _snake_to_title_words(k))


def _normalize_custom_field_review_date_str(raw: Any) -> Optional[str]:
    """Return ``YYYY-MM-DD`` or None for storage / form round-trip."""
    if raw is None:
        return None
    s = str(raw).strip()[:10]
    if not s:
        return None
    try:
        date.fromisoformat(s)
        return s
    except ValueError:
        return None


def staff_custom_fields_items_from_storage(raw: Any) -> List[Dict[str, Any]]:
    """
    Decode ``hr_staff_details.custom_fields_json`` into a list of
    ``{"label", "value", "review_date"}`` (review_date optional ISO string).

    Supports v2 ``{"v":2,"items":[...]}`` and legacy flat ``{"Label":"value"}`` dicts.
    """
    if raw is None or str(raw).strip() == "":
        return []
    try:
        o = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if isinstance(o, dict) and o.get("v") == 2 and isinstance(o.get("items"), list):
        out: List[Dict[str, Any]] = []
        for it in o["items"]:
            if not isinstance(it, dict):
                continue
            lab = (str(it.get("label") or "")).strip()[:128]
            if not lab:
                continue
            val = str(it.get("value") if it.get("value") is not None else "").strip()[:4000]
            rd = it.get("review_date")
            rd_s = _normalize_custom_field_review_date_str(rd)
            out.append({"label": lab, "value": val, "review_date": rd_s})
        return out
    if isinstance(o, dict):
        legacy: List[Dict[str, Any]] = []
        for k, v in sorted(o.items()):
            if k in ("v", "items"):
                continue
            if not isinstance(k, str):
                continue
            ks = k.strip()
            if not ks:
                continue
            if v is None:
                vs = ""
            elif isinstance(v, (str, int, float, bool)):
                vs = str(v)
            else:
                vs = str(v)[:4000]
            legacy.append({"label": ks, "value": vs, "review_date": None})
        return legacy
    return []


def annotate_staff_custom_fields_review_flags(
    items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Set ``review_overdue`` and ``review_due_soon`` (30 days) for profile display."""
    today = date.today()
    soon_end = today + timedelta(days=30)
    for it in items:
        rd_s = it.get("review_date")
        it["review_overdue"] = False
        it["review_due_soon"] = False
        if not rd_s:
            continue
        try:
            d = date.fromisoformat(str(rd_s)[:10])
        except ValueError:
            continue
        it["review_overdue"] = d < today
        it["review_due_soon"] = today <= d <= soon_end
    return items


def build_staff_custom_fields_json_from_form(
    labels: List[Any],
    values: List[Any],
    review_dates: Optional[List[Any]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Build stored JSON for custom fields from parallel label / value / optional review date lists.
    Stored shape: ``{"v":2,"items":[{"label","value","review_date"},...]}``.
    Returns (json_or_none, error_message_or_none).
    """
    rd_list = review_dates or []
    items: List[Dict[str, Any]] = []
    n = max(len(labels or []), len(values or []), len(rd_list))
    for i in range(n):
        lab = labels[i] if i < len(labels) else None
        val = values[i] if i < len(values) else None
        k = (str(lab) if lab is not None else "").strip()[:128]
        if not k:
            continue
        vs = (str(val) if val is not None else "").strip()[:4000]
        rd_raw = rd_list[i] if i < len(rd_list) else None
        rd_s = _normalize_custom_field_review_date_str(rd_raw)
        items.append({"label": k, "value": vs, "review_date": rd_s})
    if len(items) > 80:
        return None, "Too many custom fields (max 80 rows)."
    if not items:
        return None, None
    try:
        body = json.dumps({"v": 2, "items": items}, ensure_ascii=False)
    except (TypeError, ValueError):
        return None, "Could not save custom fields."
    if len(body) > 60000:
        return None, "Custom fields data is too large (max ~60KB)."
    return body, None


def get_staff_profile(contractor_id: int) -> Dict[str, Any]:
    """Contractor-facing profile: core + HR details (including doc/expiry fields for read-only summary)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        try:
            cur.execute(
                "SELECT id, name, email, initials, status, profile_picture_path FROM tb_contractors WHERE id = %s",
                (contractor_id,),
            )
        except Exception:
            cur.execute(
                "SELECT id, name, email, initials, status FROM tb_contractors WHERE id = %s",
                (contractor_id,),
            )
        row = cur.fetchone() or {}
        try:
            cur.execute(
                """
                SELECT h.phone, h.address_line1, h.address_line2, h.postcode, h.emergency_contact_name, h.emergency_contact_phone,
                       h.date_of_birth,
                       h.driving_licence_number, h.driving_licence_expiry, h.driving_licence_document_path,
                       h.right_to_work_type, h.right_to_work_expiry, h.right_to_work_document_path,
                       h.dbs_level, h.dbs_number, h.dbs_expiry, h.dbs_document_path,
                       h.contract_type, h.contract_start, h.contract_end, h.contract_document_path,
                       h.job_title, h.department, h.manager_contractor_id,
                       m.name AS manager_name, m.email AS manager_email
                FROM hr_staff_details h
                LEFT JOIN tb_contractors m ON m.id = h.manager_contractor_id
                WHERE h.contractor_id = %s
                """,
                (contractor_id,),
            )
        except Exception:
            try:
                cur.execute("""
                    SELECT phone, address_line1, address_line2, postcode, emergency_contact_name, emergency_contact_phone,
                           date_of_birth,
                           driving_licence_number, driving_licence_expiry, driving_licence_document_path,
                           right_to_work_type, right_to_work_expiry, right_to_work_document_path,
                           dbs_level, dbs_number, dbs_expiry, dbs_document_path,
                           contract_type, contract_start, contract_end, contract_document_path
                    FROM hr_staff_details WHERE contractor_id = %s
                """, (contractor_id,))
            except Exception:
                cur.execute(
                    "SELECT phone, address_line1, address_line2, postcode, emergency_contact_name, emergency_contact_phone "
                    "FROM hr_staff_details WHERE contractor_id = %s",
                    (contractor_id,),
                )
        extra = cur.fetchone()
        if extra:
            row.update(extra)
        return row
    finally:
        cur.close()
        conn.close()


def update_staff_details(contractor_id: int, data: Dict[str, Any]) -> None:
    """Contractor self-service: update phone, address, emergency contact only."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO hr_staff_details (contractor_id, phone, address_line1, address_line2, postcode, emergency_contact_name, emergency_contact_phone)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            phone = COALESCE(VALUES(phone), phone),
            address_line1 = COALESCE(VALUES(address_line1), address_line1),
            address_line2 = COALESCE(VALUES(address_line2), address_line2),
            postcode = COALESCE(VALUES(postcode), postcode),
            emergency_contact_name = COALESCE(VALUES(emergency_contact_name), emergency_contact_name),
            emergency_contact_phone = COALESCE(VALUES(emergency_contact_phone), emergency_contact_phone)
        """, (
            contractor_id,
            data.get("phone"),
            data.get("address_line1"),
            data.get("address_line2"),
            data.get("postcode"),
            data.get("emergency_contact_name"),
            data.get("emergency_contact_phone"),
        ))
        conn.commit()
    finally:
        cur.close()
        conn.close()


# -----------------------------------------------------------------------------
# Admin: contractor search and full profile
# -----------------------------------------------------------------------------


def admin_list_contractors_for_select(limit: int = 500) -> List[Dict[str, Any]]:
    """List contractors for admin dropdowns (id, name, email)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, name, email FROM tb_contractors ORDER BY name LIMIT %s",
            (limit,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_list_time_billing_roles_for_select(limit: int = 200) -> List[Dict[str, Any]]:
    """Time billing `roles` table — used when adding a new employee from HR."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, name FROM roles ORDER BY name LIMIT %s",
            (limit,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_staff_role_display_names_ordered(
    tb_roles: Sequence[Dict[str, Any]],
) -> List[str]:
    """Unique Time Billing role names (pay / training), preserving first-seen order."""
    seen: Set[str] = set()
    out: List[str] = []
    for r in tb_roles or ():
        n = (r.get("name") or "").strip()
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def hr_job_title_preset_form_value(
    job_title: Optional[str],
    tb_role_name: Optional[str],
    tb_roles: Sequence[Dict[str, Any]],
) -> str:
    """
    Value for the job-title quick-pick <select>: '' (custom), '__sync_pay__', or an exact
    staff role name from ``tb_roles``.
    """
    jt = (job_title or "").strip()
    pay = (tb_role_name or "").strip()
    names = set(admin_staff_role_display_names_ordered(tb_roles))
    if pay and (not jt or jt == pay):
        return "__sync_pay__"
    if jt and jt in names:
        return jt
    return ""


def admin_list_wage_rate_cards_for_select(limit: int = 250) -> List[Dict[str, Any]]:
    """Time Billing wage_rate_cards for HR edit form (optional module)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'wage_rate_cards'")
        if not cur.fetchone():
            return []
        lim = max(1, min(int(limit or 250), 500))
        cur.execute(
            """
            SELECT wrc.id, wrc.name, wrc.active, wrc.role_id, r.name AS role_name
            FROM wage_rate_cards wrc
            LEFT JOIN roles r ON r.id = wrc.role_id
            ORDER BY wrc.name, wrc.id
            LIMIT %s
            """,
            (lim,),
        )
        return cur.fetchall() or []
    except Exception:
        return []
    finally:
        cur.close()
        conn.close()


def admin_tb_pay_roles_with_usage(limit: int = 300) -> List[Dict[str, Any]]:
    """Time Billing ``roles`` rows with usage counts for HR admin."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'roles'")
        if not cur.fetchone():
            return []
        lim = max(1, min(int(limit or 300), 500))
        cur.execute("SHOW TABLES LIKE 'wage_rate_cards'")
        has_wrc = bool(cur.fetchone())
        if has_wrc:
            sql = """
                SELECT r.id, r.name, r.code, COALESCE(r.active, 1) AS active,
                  (SELECT COUNT(*) FROM tb_contractors c WHERE c.role_id = r.id) AS primary_contractors,
                  (SELECT COUNT(*) FROM tb_contractor_roles cr WHERE cr.role_id = r.id) AS m2m_contractors,
                  (SELECT COUNT(*) FROM wage_rate_cards w WHERE w.role_id = r.id) AS wage_cards
                FROM roles r
                ORDER BY LOWER(r.name)
                LIMIT %s
            """
        else:
            sql = """
                SELECT r.id, r.name, r.code, COALESCE(r.active, 1) AS active,
                  (SELECT COUNT(*) FROM tb_contractors c WHERE c.role_id = r.id) AS primary_contractors,
                  (SELECT COUNT(*) FROM tb_contractor_roles cr WHERE cr.role_id = r.id) AS m2m_contractors,
                  0 AS wage_cards
                FROM roles r
                ORDER BY LOWER(r.name)
                LIMIT %s
            """
        cur.execute(sql, (lim,))
        return cur.fetchall() or []
    except Exception as e:
        logger.warning("admin_tb_pay_roles_with_usage: %s", e)
        return []
    finally:
        cur.close()
        conn.close()


def admin_tb_pay_role_create(name: str) -> Tuple[bool, str]:
    nm = (name or "").strip()
    if not nm:
        return False, "Name is required."
    if len(nm) > 100:
        return False, "Name is too long (max 100 characters)."
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM roles WHERE LOWER(name) = LOWER(%s) LIMIT 1", (nm,))
        if cur.fetchone():
            return False, "A role with this name already exists."
        cur.execute("INSERT INTO roles (name) VALUES (%s)", (nm,))
        conn.commit()
        return True, "ok"
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("admin_tb_pay_role_create: %s", e)
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def admin_tb_pay_role_rename(role_id: int, new_name: str) -> Tuple[bool, str]:
    try:
        rid = int(role_id)
    except (TypeError, ValueError):
        return False, "Invalid role."
    nm = (new_name or "").strip()
    if not nm:
        return False, "Name is required."
    if len(nm) > 100:
        return False, "Name is too long (max 100 characters)."
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM roles WHERE id = %s LIMIT 1", (rid,))
        if not cur.fetchone():
            return False, "Role not found."
        cur.execute(
            "SELECT id FROM roles WHERE LOWER(name) = LOWER(%s) AND id <> %s LIMIT 1",
            (nm, rid),
        )
        if cur.fetchone():
            return False, "Another role already uses this name."
        cur.execute("UPDATE roles SET name = %s WHERE id = %s", (nm, rid))
        conn.commit()
        return True, "ok"
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("admin_tb_pay_role_rename: %s", e)
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def admin_tb_pay_role_delete(role_id: int) -> Tuple[bool, str]:
    try:
        rid = int(role_id)
    except (TypeError, ValueError):
        return False, "Invalid role."
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM roles WHERE id = %s LIMIT 1", (rid,))
        if not cur.fetchone():
            return False, "Role not found."
        cur.execute(
            "SELECT COUNT(*) FROM tb_contractors WHERE role_id = %s", (rid,))
        n_primary = cur.fetchone()
        if n_primary and int(n_primary[0]) > 0:
            return (
                False,
                "This role is the primary staff role for one or more employees. Reassign them in HR first.",
            )
        cur.execute(
            "SELECT COUNT(*) FROM tb_contractor_roles WHERE role_id = %s", (rid,))
        n_m2m = cur.fetchone()
        if n_m2m and int(n_m2m[0]) > 0:
            return (
                False,
                "This role is still linked to employees. Update HR profiles first.",
            )
        cur.execute("SHOW TABLES LIKE 'wage_rate_cards'")
        if cur.fetchone():
            cur.execute(
                "SELECT COUNT(*) FROM wage_rate_cards WHERE role_id = %s", (rid,))
            n_w = cur.fetchone()
            if n_w and int(n_w[0]) > 0:
                return (
                    False,
                    "Unlink this role from wage rate cards in Time Billing before deleting.",
                )
        cur.execute("DELETE FROM roles WHERE id = %s", (rid,))
        conn.commit()
        return True, "ok"
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("admin_tb_pay_role_delete: %s", e)
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def admin_hr_update_contractor_core(
    contractor_id: int,
    *,
    name: str,
    email: str,
    status: str,
    employment_type: Optional[str] = None,
    role_id_raw: Optional[str] = None,
    wage_rate_card_id_raw: Optional[str] = None,
    invoice_billing_frequency: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    Update tb_contractors identity + primary Time Billing role / wage card from HR.
    Login ``username`` is fixed after employee creation; this path preserves it. Rows missing
    a username (legacy) get one allocated from the current ``name``.
    """
    import mysql.connector

    from app.plugins.time_billing_module.routes import (
        allocate_contractor_username,
        contractor_username_prefer_core_user,
    )

    cid = int(contractor_id)
    name = (name or "").strip()
    email = (email or "").strip().lower()
    st = (status or "active").strip().lower()
    if st not in ("active", "inactive"):
        st = "active"
    if not name:
        return False, "Name is required."
    if not email or "@" not in email:
        return False, "Valid work email is required."

    et = (employment_type or "").strip().lower()
    if et not in ("paye", "self_employed", ""):
        et = ""

    role_id: Optional[int] = None
    if role_id_raw is not None and str(role_id_raw).strip() != "":
        try:
            role_id = int(role_id_raw)
        except (TypeError, ValueError):
            return False, "Invalid role selection."

    wage_id: Optional[int] = None
    wage_clear = False
    if wage_rate_card_id_raw is not None:
        wr = str(wage_rate_card_id_raw).strip()
        if wr == "":
            wage_clear = True
        else:
            try:
                wage_id = int(wr)
            except (TypeError, ValueError):
                return False, "Invalid wage rate card selection."

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        try:
            cur.execute(
                "SELECT id, name, email, username FROM tb_contractors WHERE id = %s LIMIT 1",
                (cid,),
            )
        except mysql.connector.Error:
            cur.execute(
                "SELECT id, name, email FROM tb_contractors WHERE id = %s LIMIT 1",
                (cid,),
            )
        row = cur.fetchone()
        if not row:
            return False, "Employee not found."

        cur.execute(
            "SELECT id FROM tb_contractors WHERE LOWER(TRIM(email)) = %s AND id <> %s LIMIT 1",
            (email, cid),
        )
        if cur.fetchone():
            return False, "Another person already uses this email."

        existing_un = (row.get("username") or "").strip()
        if existing_un:
            username_un = existing_un
        else:
            core_u = None
            ucur = conn.cursor(dictionary=True)
            try:
                ucur.execute(
                    "SELECT id, username FROM users WHERE LOWER(TRIM(email)) = %s LIMIT 1",
                    (email,),
                )
                core_u = ucur.fetchone()
            finally:
                ucur.close()
            if core_u:
                username_un, uerr = contractor_username_prefer_core_user(
                    conn,
                    core_user_id=core_u.get("id"),
                    core_username=core_u.get("username"),
                    full_name=name,
                    email=email,
                    exclude_contractor_id=cid,
                )
            else:
                username_un, uerr = allocate_contractor_username(
                    conn, name, email=email, exclude_contractor_id=cid,
                )
            if uerr:
                return False, uerr

        if role_id is not None:
            cur.execute("SELECT id FROM roles WHERE id = %s LIMIT 1", (role_id,))
            if not cur.fetchone():
                return False, "Selected role does not exist."

        if wage_id is not None:
            cur.execute("SHOW TABLES LIKE 'wage_rate_cards'")
            if cur.fetchone():
                cur.execute(
                    "SELECT id FROM wage_rate_cards WHERE id = %s LIMIT 1",
                    (wage_id,),
                )
                if not cur.fetchone():
                    return False, "Selected wage rate card does not exist."

        cur2 = conn.cursor()
        try:
            try:
                cur2.execute(
                    """
                    UPDATE tb_contractors
                    SET name = %s, email = %s, status = %s, username = %s
                    WHERE id = %s
                    """,
                    (name, email, st, username_un, cid),
                )
            except mysql.connector.Error:
                cur2.execute(
                    """
                    UPDATE tb_contractors
                    SET name = %s, email = %s, status = %s
                    WHERE id = %s
                    """,
                    (name, email, st, cid),
                )

            if et in ("paye", "self_employed"):
                try:
                    cur2.execute(
                        "UPDATE tb_contractors SET employment_type = %s WHERE id = %s",
                        (et, cid),
                    )
                except mysql.connector.Error:
                    pass

            ibf = (invoice_billing_frequency or "weekly").strip().lower()
            if ibf not in ("weekly", "biweekly", "monthly"):
                ibf = "weekly"
            try:
                cur2.execute(
                    """
                    UPDATE tb_contractors SET invoice_billing_frequency = %s WHERE id = %s
                    """,
                    (ibf, cid),
                )
            except mysql.connector.Error:
                pass

            if role_id is not None:
                cur2.execute(
                    "UPDATE tb_contractors SET role_id = %s WHERE id = %s",
                    (role_id, cid),
                )
                cur2.execute(
                    "DELETE FROM tb_contractor_roles WHERE contractor_id = %s", (cid,))
                cur2.execute(
                    """
                    INSERT IGNORE INTO tb_contractor_roles (contractor_id, role_id)
                    VALUES (%s, %s)
                    """,
                    (cid, role_id),
                )

            if wage_clear:
                try:
                    cur2.execute(
                        "UPDATE tb_contractors SET wage_rate_card_id = NULL WHERE id = %s",
                        (cid,),
                    )
                except mysql.connector.Error:
                    pass
            elif wage_id is not None:
                try:
                    cur2.execute(
                        """
                        UPDATE tb_contractors SET wage_rate_card_id = %s WHERE id = %s
                        """,
                        (wage_id, cid),
                    )
                except mysql.connector.Error:
                    pass

            conn.commit()
        finally:
            cur2.close()
        if role_id is not None:
            _apply_training_role_assignment_rules(cid, role_id)
        sync_ventus_crew_profile_from_hr_training(cid)
        return True, "ok"
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("admin_hr_update_contractor_core: %s", e)
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def admin_create_contractor_employee(
    name: str,
    email: str,
    *,
    role_name: Optional[str] = None,
    role_id: Optional[int] = None,
    password: Optional[str] = None,
    phone: Optional[str] = None,
    status: str = "active",
    employment_type: Optional[str] = None,
) -> Tuple[bool, str, Optional[int]]:
    """
    Create a new row in tb_contractors plus HR shell (existing staff not hired via recruitment).
    Behaviour aligned with Time Billing contractor creation.

    Prefer ``role_id`` when supplied (HR pay role picker). Otherwise resolve ``role_name``:
    if empty, uses default_employee_tb_role_name(); if no matching row, inserts into ``roles``.
    """
    from app.objects import AuthManager

    name = (name or "").strip()
    email = (email or "").strip().lower()
    if not name:
        return False, "Name is required.", None
    if not email or "@" not in email:
        return False, "Valid email is required.", None

    st = (status or "active").strip().lower()
    if st not in ("active", "inactive"):
        st = "active"

    pwd_plain = (password or "").strip()
    if len(pwd_plain) < 8:
        return False, "Password is required (minimum 8 characters).", None
    pwd_hash = AuthManager.hash_password(pwd_plain)

    from app.seat_limits import seat_check_error_for_new_email
    from app.plugins.time_billing_module.routes import (
        allocate_contractor_username,
        contractor_username_prefer_core_user,
    )

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        seat_err = seat_check_error_for_new_email(email, db_cursor=cur)
        if seat_err:
            return False, seat_err, None

        cur.execute(
            "SELECT 1 FROM tb_contractors WHERE email=%s LIMIT 1", (email,))
        if cur.fetchone():
            return False, "A person with this email already exists.", None

        core_u = None
        dcur = conn.cursor(dictionary=True)
        try:
            dcur.execute(
                "SELECT * FROM users WHERE LOWER(TRIM(email)) = %s LIMIT 1",
                (email,),
            )
            core_u = dcur.fetchone()
        finally:
            dcur.close()

        if core_u:
            username_un, uerr = contractor_username_prefer_core_user(
                conn,
                core_user_id=core_u.get("id"),
                core_username=core_u.get("username"),
                full_name=name,
                email=email,
                exclude_contractor_id=None,
            )
        else:
            username_un, uerr = allocate_contractor_username(
                conn, name, email=email, exclude_contractor_id=None,
            )
        if uerr:
            return False, uerr, None

        if role_id is not None:
            try:
                rid_pick = int(role_id)
            except (TypeError, ValueError):
                return False, "Invalid pay role.", None
            cur.execute("SELECT id FROM roles WHERE id=%s LIMIT 1", (rid_pick,))
            row_r = cur.fetchone()
            if not row_r:
                return False, "Pay role not found.", None
            tb_role_id = row_r[0]
        else:
            rn = (role_name or "").strip().lower()
            if not rn:
                rn = default_employee_tb_role_name()
            cur.execute("SELECT id FROM roles WHERE name=%s LIMIT 1", (rn,))
            row = cur.fetchone()
            if row:
                tb_role_id = row[0]
            else:
                cur.execute("INSERT INTO roles (name) VALUES (%s)", (rn,))
                tb_role_id = cur.lastrowid

        try:
            cur.execute(
                """
                INSERT INTO tb_contractors (
                    email, username, name, status, password_hash, role_id, wage_rate_card_id, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (email, username_un, name, st, pwd_hash, tb_role_id, None),
            )
        except Exception:
            cur.execute(
                """
                INSERT INTO tb_contractors (
                    email, name, status, password_hash, role_id, wage_rate_card_id, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """,
                (email, name, st, pwd_hash, tb_role_id, None),
            )
        contractor_id = int(cur.lastrowid)

        try:
            cur.execute(
                "UPDATE tb_contractors SET username = %s WHERE id = %s",
                (username_un, contractor_id),
            )
        except Exception:
            pass

        cur.execute(
            """
            INSERT IGNORE INTO tb_contractor_roles (contractor_id, role_id)
            VALUES (%s, %s)
            """,
            (contractor_id, tb_role_id),
        )

        et = (employment_type or "").strip(
        ).lower() if employment_type else None
        if et in ("paye", "self_employed"):
            try:
                cur.execute(
                    "UPDATE tb_contractors SET employment_type = %s WHERE id = %s",
                    (et, contractor_id),
                )
            except Exception:
                pass

        cur.execute(
            "INSERT IGNORE INTO hr_staff_details (contractor_id) VALUES (%s)",
            (contractor_id,),
        )
        ph = _truncate_str(phone, 64)
        if ph:
            cur.execute(
                "UPDATE hr_staff_details SET phone = %s WHERE contractor_id = %s",
                (ph, contractor_id),
            )

        conn.commit()
        _apply_training_role_assignment_rules(contractor_id, tb_role_id)
        sync_ventus_crew_profile_from_hr_training(contractor_id)
        return True, "ok", contractor_id
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("admin_create_contractor_employee: %s", e)
        return False, str(e), None
    finally:
        cur.close()
        conn.close()


def admin_set_contractor_portal_password(contractor_id: int, new_password: str) -> Tuple[bool, str]:
    """Set ``tb_contractors.password_hash`` (employee portal / API login)."""
    from app.objects import AuthManager

    pw = (new_password or "").strip()
    if len(pw) < 8:
        return False, "Password must be at least 8 characters."
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM tb_contractors WHERE id = %s",
                    (int(contractor_id),))
        if not cur.fetchone():
            return False, "Employee not found."
        h = AuthManager.hash_password(pw)
        cur.execute(
            "UPDATE tb_contractors SET password_hash = %s WHERE id = %s",
            (h, int(contractor_id)),
        )
        conn.commit()
        try:
            from app.objects import sync_linked_users_password_from_contractor

            sync_linked_users_password_from_contractor(int(contractor_id), h)
        except Exception:
            pass
        return True, "ok"
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("admin_set_contractor_portal_password: %s", e)
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def admin_delete_contractor_employee(contractor_id: int, *, confirm_email: str) -> Tuple[bool, str]:
    """
    Remove ``tb_contractors`` row (cascades to time billing / HR links per schema).
    ``confirm_email`` must match the contractor email (case-insensitive).
    """
    cid = int(contractor_id)
    ce = (confirm_email or "").strip().lower()
    if not ce or "@" not in ce:
        return False, "Type the employee work email to confirm deletion."
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, email FROM tb_contractors WHERE id = %s", (cid,))
        row = cur.fetchone()
        if not row:
            return False, "Employee not found."
        if (row.get("email") or "").strip().lower() != ce:
            return False, "Confirmation email does not match this employee."
        cur.execute("DELETE FROM tb_contractors WHERE id = %s", (cid,))
        conn.commit()
        return True, "ok"
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("admin_delete_contractor_employee: %s", e)
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def admin_get_contractor_id_by_email(email: str) -> Optional[int]:
    e = (email or "").strip().lower()
    if not e or "@" not in e:
        return None
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM tb_contractors WHERE LOWER(TRIM(email)) = %s LIMIT 1",
            (e,),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    finally:
        cur.close()
        conn.close()


def admin_update_contractor_basic(
    contractor_id: int,
    *,
    name: Optional[str] = None,
    status: Optional[str] = None,
    role_name: Optional[str] = None,
) -> Tuple[bool, str]:
    """Patch core contractor fields used by CSV import / bulk update."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM tb_contractors WHERE id = %s",
                    (int(contractor_id),))
        if not cur.fetchone():
            return False, "Contractor not found."
        if name is not None and str(name).strip():
            cur.execute(
                "UPDATE tb_contractors SET name = %s WHERE id = %s",
                (str(name).strip()[:255], int(contractor_id)),
            )
        if status is not None:
            st = str(status).strip().lower()
            if st in ("active", "inactive"):
                cur.execute(
                    "UPDATE tb_contractors SET status = %s WHERE id = %s",
                    (st, int(contractor_id)),
                )
        if role_name is not None and str(role_name).strip():
            rn = str(role_name).strip().lower()
            cur.execute("SELECT id FROM roles WHERE name = %s LIMIT 1", (rn,))
            row = cur.fetchone()
            if row:
                role_id = int(row[0])
            else:
                cur.execute("INSERT INTO roles (name) VALUES (%s)", (rn,))
                role_id = int(cur.lastrowid)
            cur.execute(
                "UPDATE tb_contractors SET role_id = %s WHERE id = %s",
                (role_id, int(contractor_id)),
            )
            cur.execute(
                """
                INSERT IGNORE INTO tb_contractor_roles (contractor_id, role_id)
                VALUES (%s, %s)
                """,
                (int(contractor_id), role_id),
            )
        conn.commit()
        return True, "ok"
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("admin_update_contractor_basic: %s", e)
        return False, str(e) or "Update failed."
    finally:
        cur.close()
        conn.close()


def admin_search_contractors(q: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Search contractors by name, email; optionally join phone from hr_staff_details."""
    if not q or not q.strip():
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        term = "%" + q.strip() + "%"
        cur.execute("""
            SELECT c.id, c.name, c.email, c.status, h.phone
            FROM tb_contractors c
            LEFT JOIN hr_staff_details h ON h.contractor_id = c.id
            WHERE c.name LIKE %s OR c.email LIKE %s
            ORDER BY c.name
            LIMIT %s
        """, (term, term, limit))
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_contractor_display_line(contractor_id: int) -> Optional[str]:
    """Short label for filters (name · id N)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, name, email FROM tb_contractors WHERE id = %s LIMIT 1",
            (int(contractor_id),),
        )
        r = cur.fetchone()
        if not r:
            return None
        nm = (str(r.get("name") or "").strip() or str(r.get("email") or "").strip() or "—")
        return f"{nm} · id {int(r['id'])}"
    finally:
        cur.close()
        conn.close()


def admin_list_direct_reports(manager_contractor_id: int) -> List[Dict[str, Any]]:
    """Employees who list this contractor as manager (hr_staff_details.manager_contractor_id)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT c.id, c.name, c.email, c.status
            FROM hr_staff_details h
            JOIN tb_contractors c ON c.id = h.contractor_id
            WHERE h.manager_contractor_id = %s
            ORDER BY c.name
            """,
            (int(manager_contractor_id),),
        )
        return cur.fetchall() or []
    except Exception:
        return []
    finally:
        cur.close()
        conn.close()


def get_contractor_employment_type_for_contractor(contractor_id: int) -> Optional[str]:
    """Time Billing / HR: read employment_type from tb_contractors."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        try:
            cur.execute(
                "SELECT employment_type FROM tb_contractors WHERE id = %s LIMIT 1",
                (int(contractor_id),),
            )
        except Exception:
            return None
        row = cur.fetchone()
        if not row:
            return None
        v = (row.get("employment_type") or "").strip().lower()
        return v if v in ("paye", "self_employed") else None
    finally:
        cur.close()
        conn.close()


def get_contractor_invoice_address_lines(contractor_id: int) -> List[str]:
    """Fallback 'from' address for Time Billing invoice PDFs when portal billing fields are empty."""
    p = admin_get_staff_profile(contractor_id)
    if not p:
        return []
    lines: List[str] = []
    name = (p.get("name") or "").strip()
    if name:
        lines.append(name)
    a1 = (p.get("address_line1") or "").strip()
    a2 = (p.get("address_line2") or "").strip()
    pc = (p.get("postcode") or "").strip()
    if a1:
        lines.append(a1)
    if a2:
        lines.append(a2)
    if pc:
        lines.append(pc)
    return lines


def admin_update_contractor_employment_type(contractor_id: int, employment_type: str) -> bool:
    """Set tb_contractors.employment_type (PAYE vs self-employed)."""
    et = (employment_type or "").strip().lower()
    if et not in ("paye", "self_employed"):
        et = "self_employed"
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        try:
            cur.execute(
                "UPDATE tb_contractors SET employment_type = %s WHERE id = %s",
                (et, int(contractor_id)),
            )
            conn.commit()
            return cur.rowcount > 0
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False
    finally:
        cur.close()
        conn.close()


def _ventus_register_lines_from_json(val: Any) -> List[str]:
    """Turn Ventus ``skills_json`` / ``qualifications_json`` into display lines (strings or {grade, role, …})."""
    if val is None:
        return []
    raw: Any = val
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8", errors="replace")
        except Exception:
            return []
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return []
        try:
            parsed: Any = json.loads(raw)
        except Exception:
            return [raw]
    else:
        parsed = raw
    out: List[str] = []
    if isinstance(parsed, list):
        for x in parsed:
            if isinstance(x, dict):
                bits: List[str] = []
                for key in (
                    "grade",
                    "role",
                    "name",
                    "label",
                    "title",
                    "skill",
                    "qualification",
                ):
                    v = x.get(key)
                    if v is not None and str(v).strip():
                        bits.append(str(v).strip())
                line = " · ".join(bits) if bits else None
                if line:
                    out.append(line)
            elif x is not None:
                s = str(x).strip()
                if s:
                    out.append(s)
    elif isinstance(parsed, dict):
        for key in ("grade", "role", "name", "label", "title"):
            v = parsed.get(key)
            if v is not None and str(v).strip():
                out.append(f"{key.replace('_', ' ').title()}: {str(v).strip()}")
    else:
        s = str(parsed).strip()
        if s:
            out.append(s)
    return out


def admin_ventus_crew_profile_for_contractor(
    contractor_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Read-through snapshot of ``mdt_crew_profiles`` for this contractor (filled automatically by
    ``sync_ventus_crew_profile_from_hr_training`` from HR + Training).
    """
    cid = int(contractor_id)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'mdt_crew_profiles'")
        if not cur.fetchone():
            return None
        cur.execute(
            """
            SELECT username, gender, skills_json, qualifications_json, updated_at
            FROM mdt_crew_profiles
            WHERE contractor_id = %s
            LIMIT 1
            """,
            (cid,),
        )
        row = cur.fetchone()
        if not row:
            return None
        skills = _ventus_register_lines_from_json(row.get("skills_json"))
        quals = _ventus_register_lines_from_json(row.get("qualifications_json"))
        return {
            "username": (row.get("username") or "").strip() or None,
            "gender": (row.get("gender") or "").strip() or None,
            "skills_lines": skills,
            "qualifications_lines": quals,
            "updated_at": row.get("updated_at"),
        }
    except Exception as e:
        logger.warning("admin_ventus_crew_profile_for_contractor: %s", e)
        return None
    finally:
        cur.close()
        conn.close()


def _apply_training_role_assignment_rules(
    contractor_id: int, role_id: Optional[int]
) -> None:
    """Apply Training module auto-assign rules when pay role (``roles.id``) is set from HR."""
    if not role_id:
        return
    try:
        from app.plugins.training_module.services import TrainingService

        TrainingService.apply_role_assignment_rules(
            int(contractor_id),
            int(role_id),
            assigned_by_user_id=None,
        )
    except Exception as e:
        logger.warning("_apply_training_role_assignment_rules: %s", e)


def sync_ventus_crew_profile_from_hr_training(contractor_id: int) -> None:
    """
    Push HR pay role, job title, and Training person competencies into ``mdt_crew_profiles``
    so Ventus CAD / response UIs read skills, qualifications, and role/grade from the DB only.
    Safe to call often; overwrites skills_json and qualifications_json for that username.
    """
    cid = int(contractor_id)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'mdt_crew_profiles'")
        if not cur.fetchone():
            return
        cur.execute(
            """
            SELECT c.id, TRIM(c.username) AS username, r.name AS pay_role_name,
                   h.job_title
            FROM tb_contractors c
            LEFT JOIN roles r ON r.id = c.role_id
            LEFT JOIN hr_staff_details h ON h.contractor_id = c.id
            WHERE c.id = %s
            LIMIT 1
            """,
            (cid,),
        )
        row = cur.fetchone()
        if not row:
            return
        uname = (row.get("username") or "").strip()
        if not uname:
            return
        job_title = (row.get("job_title") or "").strip() or None
        pay_role = (row.get("pay_role_name") or "").strip() or None

        crew_entry: Dict[str, Any] = {"skills": [], "qualifications": []}
        try:
            from app.plugins.training_module.services import TrainingService

            if TrainingService.person_competencies_table_exists():
                TrainingService.cad_merge_competencies_into_crew_entry(
                    cur, crew_entry, cid, job_title
                )
        except Exception as e:
            logger.warning("sync_ventus_crew_profile_from_hr_training merge: %s", e)

        skills = crew_entry.get("skills") or []
        if not isinstance(skills, list):
            skills = []

        qualifications: List[Any] = []
        for rec in crew_entry.get("qualification_records") or []:
            if isinstance(rec, dict):
                qualifications.append(rec)
        clinical = crew_entry.get("clinical_grade")
        if clinical:
            qualifications.append(
                {
                    "grade": str(clinical),
                    "role": job_title or pay_role,
                    "source": "training_clinical_grade",
                }
            )
        if pay_role:
            qualifications.append(
                {
                    "role": pay_role,
                    "grade": pay_role,
                    "source": "hr_pay_role",
                }
            )

        skills_json = json.dumps(skills, ensure_ascii=False)
        quals_json = json.dumps(qualifications, ensure_ascii=False, default=str)

        ucur = conn.cursor()
        try:
            ucur.execute(
                """
                INSERT INTO mdt_crew_profiles
                  (username, contractor_id, gender, skills_json, qualifications_json, profile_picture_path)
                VALUES (%s, %s, NULL, %s, %s, NULL)
                ON DUPLICATE KEY UPDATE
                  contractor_id = VALUES(contractor_id),
                  skills_json = VALUES(skills_json),
                  qualifications_json = VALUES(qualifications_json)
                """,
                (uname, cid, skills_json, quals_json),
            )
            conn.commit()
        finally:
            ucur.close()
    except Exception as e:
        logger.warning("sync_ventus_crew_profile_from_hr_training: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()


def admin_get_staff_profile(contractor_id: int) -> Optional[Dict[str, Any]]:
    """Full HR profile for admin: core + all staff_details columns."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        try:
            cur.execute(
                """
                SELECT c.id, c.name, c.email, c.username, c.initials, c.status, c.profile_picture_path,
                       COALESCE(c.employment_type, 'self_employed') AS employment_type,
                       COALESCE(c.invoice_billing_frequency, 'weekly') AS invoice_billing_frequency,
                       c.role_id AS tb_role_id, c.wage_rate_card_id AS tb_wage_rate_card_id,
                       r.name AS tb_role_name
                FROM tb_contractors c
                LEFT JOIN roles r ON r.id = c.role_id
                WHERE c.id = %s
                """,
                (contractor_id,),
            )
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                cur.execute(
                    """
                    SELECT c.id, c.name, c.email, c.initials, c.status, c.profile_picture_path,
                           COALESCE(c.invoice_billing_frequency, 'weekly') AS invoice_billing_frequency,
                           c.role_id AS tb_role_id, c.wage_rate_card_id AS tb_wage_rate_card_id,
                           r.name AS tb_role_name
                    FROM tb_contractors c
                    LEFT JOIN roles r ON r.id = c.role_id
                    WHERE c.id = %s
                    """,
                    (contractor_id,),
                )
            except Exception:
                try:
                    cur.execute(
                        "SELECT id, name, email, initials, status, profile_picture_path FROM tb_contractors WHERE id = %s",
                        (contractor_id,),
                    )
                except Exception:
                    cur.execute(
                        "SELECT id, name, email, initials, status FROM tb_contractors WHERE id = %s",
                        (contractor_id,),
                    )
        row = cur.fetchone()
        if not row:
            return None
        if "invoice_billing_frequency" not in row:
            row["invoice_billing_frequency"] = "weekly"
            try:
                cur.execute(
                    """
                    SELECT COALESCE(invoice_billing_frequency, 'weekly') AS ibf
                    FROM tb_contractors WHERE id = %s
                    """,
                    (contractor_id,),
                )
                one = cur.fetchone()
                if one and one.get("ibf"):
                    row["invoice_billing_frequency"] = one["ibf"]
            except Exception:
                pass
        try:
            cur.execute(
                """
                SELECT h.phone, h.address_line1, h.address_line2, h.postcode, h.emergency_contact_name, h.emergency_contact_phone,
                       h.date_of_birth,
                       h.driving_licence_number, h.driving_licence_expiry, h.driving_licence_document_path,
                       h.right_to_work_type, h.right_to_work_expiry, h.right_to_work_document_path,
                       h.dbs_level, h.dbs_number, h.dbs_expiry, h.dbs_document_path,
                       h.dbs_update_service_subscribed, h.dbs_certificate_ref, h.dbs_update_consent_at,
                       h.dbs_last_check_at, h.dbs_last_status_code,
                       h.contract_type, h.contract_start, h.contract_end, h.contract_document_path, h.updated_at,
                       h.job_title, h.department, h.manager_contractor_id,
                       h.hpac_registration_number, h.hpac_registered_on, h.hpac_register_grade,
                       h.hpac_register_last_checked_at, h.hpac_register_status, h.hpac_register_check_notes,
                       h.hcpc_registration_number, h.hcpc_registered_on, h.hcpc_register_profession,
                       h.hcpc_register_last_checked_at, h.hcpc_register_status, h.hcpc_register_check_notes,
                       h.custom_fields_json,
                       h.gmc_number, h.gmc_registered_on, h.gmc_register_last_checked_at,
                       h.gmc_register_status, h.gmc_register_check_notes,
                       h.nmc_pin, h.nmc_registered_on, h.nmc_register_last_checked_at,
                       h.nmc_register_status, h.nmc_register_check_notes,
                       h.gphc_registration_number, h.gphc_registered_on, h.gphc_register_last_checked_at,
                       h.gphc_register_status, h.gphc_register_check_notes,
                       m.name AS manager_name, m.email AS manager_email
                FROM hr_staff_details h
                LEFT JOIN tb_contractors m ON m.id = h.manager_contractor_id
                WHERE h.contractor_id = %s
                """,
                (contractor_id,),
            )
        except Exception:
            try:
                cur.execute("""
                    SELECT phone, address_line1, address_line2, postcode, emergency_contact_name, emergency_contact_phone,
                           date_of_birth,
                           driving_licence_number, driving_licence_expiry, driving_licence_document_path,
                           right_to_work_type, right_to_work_expiry, right_to_work_document_path,
                           dbs_level, dbs_number, dbs_expiry, dbs_document_path,
                           dbs_update_service_subscribed, dbs_certificate_ref, dbs_update_consent_at,
                           dbs_last_check_at, dbs_last_status_code,
                           contract_type, contract_start, contract_end, contract_document_path, updated_at
                    FROM hr_staff_details WHERE contractor_id = %s
                """, (contractor_id,))
            except Exception:
                cur.execute(
                    "SELECT phone, address_line1, address_line2, postcode, emergency_contact_name, emergency_contact_phone, updated_at "
                    "FROM hr_staff_details WHERE contractor_id = %s",
                    (contractor_id,),
                )
        extra = cur.fetchone()
        if extra:
            row.update(extra)
        cf_items = staff_custom_fields_items_from_storage(row.get("custom_fields_json"))
        row["custom_fields_items"] = annotate_staff_custom_fields_review_flags(
            [dict(x) for x in cf_items]
        )
        form_rows: List[Dict[str, Any]] = [
            {
                "label": x.get("label") or "",
                "value": x.get("value") or "",
                "review_date": x.get("review_date") or "",
            }
            for x in cf_items
        ]
        # One blank row for the next entry (or the only row if none saved); "Add field" adds more.
        if len(form_rows) < 80:
            form_rows.append({"label": "", "value": "", "review_date": ""})
        row["custom_fields_form_rows"] = form_rows[:80]
        if "username" not in row:
            row["username"] = None
        for _k in ("tb_role_name", "tb_role_id", "tb_wage_rate_card_id"):
            if _k not in row:
                row[_k] = None
        if row.get("tb_role_name") is None and row.get("tb_role_id") is None:
            try:
                cur.execute(
                    """
                    SELECT c.role_id AS tb_role_id, c.wage_rate_card_id AS tb_wage_rate_card_id,
                           r.name AS tb_role_name
                    FROM tb_contractors c
                    LEFT JOIN roles r ON r.id = c.role_id
                    WHERE c.id = %s
                    """,
                    (contractor_id,),
                )
                patch = cur.fetchone() or {}
                for _k, _v in patch.items():
                    row[_k] = _v
            except Exception:
                pass
        row["linked_core_user"] = None
        try:
            cur.execute(
                """
                SELECT id, username, email, role, COALESCE(billable_exempt,0) AS billable_exempt
                FROM users WHERE contractor_id = %s LIMIT 1
                """,
                (contractor_id,),
            )
            lu = cur.fetchone()
            if lu:
                row["linked_core_user"] = lu
        except Exception:
            pass
        return row
    finally:
        cur.close()
        conn.close()


def _allocate_unique_core_username(
    cur: Any, base: str, exclude_user_id: Optional[str] = None
) -> str:
    """Pick a ``users.username`` not yet taken (case-insensitive), max 50 chars."""
    raw = (base or "").strip().lower() or "user"
    raw = re.sub(r"[^a-z0-9._-]+", "", raw) or "user"
    raw = raw[:50]
    candidate = raw[:50]
    n = 0
    while True:
        cur.execute(
            "SELECT id FROM users WHERE LOWER(TRIM(username)) = LOWER(TRIM(%s)) LIMIT 1",
            (candidate,),
        )
        row = cur.fetchone()
        if not row:
            return candidate[:50]
        uid = row[0] if not isinstance(row, dict) else row.get("id")
        if exclude_user_id and str(uid) == str(exclude_user_id):
            return candidate[:50]
        n += 1
        suffix = f"_{n}"
        candidate = (raw[: 50 - len(suffix)] + suffix)[:50]


def _core_user_fields_from_contractor(
    cur: Any,
    c: dict[str, Any],
    contractor_id: int,
    *,
    exclude_user_id: Optional[str] = None,
) -> tuple[str, str, Optional[str], Optional[str]]:
    """
    Map ``tb_contractors`` identity → ``users.email``, ``users.username``, ``first_name``, ``last_name``.

    Mirrors the admin-user ↔ portal-contractor pattern: core row carries the same login name and
    display split as the contractor profile (``name`` → first/last; ``username`` preferred for
    ``users.username`` when unique).
    """
    norm_email = (c.get("email") or "").strip().lower()
    try:
        from app.support_access import shadow_username as _shadow_uname

        shadow = _shadow_uname().lower()
    except Exception:
        shadow = ""
    base_un = (c.get("username") or "").strip() or (
        norm_email.split("@", 1)[0] if "@" in norm_email else "user"
    )
    if base_un.lower() == shadow:
        base_un = f"staff_{contractor_id}"
    core_un = _allocate_unique_core_username(cur, base_un, exclude_user_id)
    if core_un.lower() == shadow:
        core_un = _allocate_unique_core_username(cur, f"staff_{contractor_id}", exclude_user_id)

    raw_name = (c.get("name") or "").strip() or norm_email
    parts = raw_name.split(None, 1)
    fn = (parts[0][:45] if parts else None) or None
    ln = (parts[1][:45] if len(parts) > 1 else None) or None
    return norm_email, core_un[:50], fn, ln


def promote_contractor_to_core_admin_user(
    contractor_id: int,
) -> Tuple[bool, str, Optional[str]]:
    """
    Create or update a ``users`` row so this contractor can log into the ERP as admin, with
    ``users.contractor_id`` pointing at this ``tb_contractors`` row (reverse of an admin user
    who already has a linked contractor for portal access).

    Copies portal identity: ``tb_contractors.username`` → ``users.username`` (unique),
    ``tb_contractors.email`` → ``users.email``, and ``tb_contractors.name`` split into
    ``users.first_name`` / ``users.last_name`` (first token / remainder), matching how core
    users sync back to the portal.

    Superuser-only at the route layer. Requires a work email and a stored portal password hash.

    Returns ``(ok, message, user_id_or_none)``.
    """
    import mysql.connector

    from app.seat_limits import seat_check_error_for_new_email

    cid = int(contractor_id)
    conn = get_db_connection()
    dcur = conn.cursor(dictionary=True)
    xcur = conn.cursor()
    try:
        try:
            dcur.execute(
                """
                SELECT id, email, username, password_hash, name
                FROM tb_contractors WHERE id = %s LIMIT 1
                """,
                (cid,),
            )
        except mysql.connector.Error as e:
            if getattr(e, "errno", None) == 1054:
                return False, "Database schema is missing expected contractor columns.", None
            raise
        c = dcur.fetchone()
        if not c:
            return False, "Contractor not found.", None
        norm_email = (c.get("email") or "").strip().lower()
        if not norm_email or "@" not in norm_email:
            return False, "A valid work email is required before creating a core login.", None
        pwd = (c.get("password_hash") or "").strip()
        if not pwd:
            return (
                False,
                "No portal password on file — set one from the profile (portal password) "
                "before creating a core admin login.",
                None,
            )

        dcur.execute(
            """
            SELECT id, username, email, role, contractor_id,
                   COALESCE(billable_exempt,0) AS billable_exempt
            FROM users WHERE contractor_id = %s LIMIT 1
            """,
            (cid,),
        )
        by_cid = dcur.fetchone()
        if by_cid:
            if int(by_cid.get("billable_exempt") or 0):
                return (
                    False,
                    "This contractor is linked to a vendor support account — "
                    "it cannot be changed here.",
                    str(by_cid["id"]),
                )
            r = (by_cid.get("role") or "").strip().lower()
            if r == "superuser":
                return (
                    False,
                    "The linked core user is a superuser — role cannot be changed from HR.",
                    str(by_cid["id"]),
                )
            if r == "support_break_glass":
                return False, "The linked account is reserved for vendor support access.", str(
                    by_cid["id"]
                )
            if r == "admin":
                return (
                    True,
                    "This contractor already has a core Admin login linked "
                    f"({by_cid.get('username') or by_cid.get('email')}).",
                    str(by_cid["id"]),
                )
            _, core_un, fn, ln = _core_user_fields_from_contractor(
                xcur, c, cid, exclude_user_id=str(by_cid["id"])
            )
            xcur.execute(
                """
                UPDATE users SET role = %s, password_hash = %s, billable_exempt = 0,
                    contractor_id = %s, email = %s, username = %s,
                    first_name = %s, last_name = %s
                WHERE id = %s
                """,
                ("admin", pwd, cid, norm_email, core_un, fn, ln, str(by_cid["id"])),
            )
            conn.commit()
            try:
                from app.objects import sync_core_user_to_portal_contractor

                sync_core_user_to_portal_contractor(str(by_cid["id"]))
            except Exception:
                pass
            return (
                True,
                "Linked core user updated to Admin; they can sign in at /login using their "
                "username or work email and the same password as the employee portal.",
                str(by_cid["id"]),
            )

        dcur.execute(
            """
            SELECT id, username, email, role, contractor_id,
                   COALESCE(billable_exempt,0) AS billable_exempt
            FROM users WHERE LOWER(TRIM(email)) = %s LIMIT 1
            """,
            (norm_email,),
        )
        by_email = dcur.fetchone()
        if by_email:
            if int(by_email.get("billable_exempt") or 0):
                return (
                    False,
                    "A core account with this email is reserved for vendor support.",
                    str(by_email["id"]),
                )
            er = (by_email.get("role") or "").strip().lower()
            if er == "superuser":
                return (
                    False,
                    "A superuser already uses this email — cannot attach this contractor.",
                    str(by_email["id"]),
                )
            oc = by_email.get("contractor_id")
            if oc is not None and int(oc) != cid:
                return (
                    False,
                    "This email is already linked to a different contractor in users.",
                    str(by_email["id"]),
                )
            dcur.execute(
                "SELECT id FROM users WHERE contractor_id = %s AND id <> %s LIMIT 1",
                (cid, str(by_email["id"])),
            )
            if dcur.fetchone():
                return (
                    False,
                    "Another core user is already linked to this contractor ID.",
                    str(by_email["id"]),
                )
            try:
                from app.support_access import shadow_username as _shadow_uname

                if (
                    (by_email.get("username") or "").strip().lower()
                    == _shadow_uname().lower()
                ):
                    return False, "This username is reserved for vendor support access.", str(
                        by_email["id"]
                    )
            except Exception:
                pass
            _, core_un, fn, ln = _core_user_fields_from_contractor(
                xcur, c, cid, exclude_user_id=str(by_email["id"])
            )
            xcur.execute(
                """
                UPDATE users
                SET contractor_id = %s, role = %s, password_hash = %s,
                    email = %s, username = %s, first_name = %s, last_name = %s,
                    billable_exempt = 0
                WHERE id = %s
                """,
                (cid, "admin", pwd, norm_email, core_un, fn, ln, str(by_email["id"])),
            )
            conn.commit()
            try:
                from app.objects import sync_core_user_to_portal_contractor

                sync_core_user_to_portal_contractor(str(by_email["id"]))
            except Exception:
                pass
            return (
                True,
                "Existing core user linked to this contractor and set to Admin "
                "(password aligned with the employee portal; adjust in Users if needed).",
                str(by_email["id"]),
            )

        seat_err = seat_check_error_for_new_email(norm_email, db_cursor=xcur)
        if seat_err:
            return False, seat_err, None

        _, new_un, fn, ln = _core_user_fields_from_contractor(
            xcur, c, cid, exclude_user_id=None
        )
        new_id = str(uuid.uuid4())
        perms = json.dumps([])
        try:
            xcur.execute(
                """
                INSERT INTO users (
                    id, username, email, password_hash, role, permissions,
                    first_name, last_name, personal_pin_hash, contractor_id,
                    billable_exempt
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, 0)
                """,
                (
                    new_id,
                    new_un[:50],
                    norm_email,
                    pwd,
                    "admin",
                    perms,
                    fn,
                    ln,
                    cid,
                ),
            )
            conn.commit()
        except mysql.connector.Error as e:
            conn.rollback()
            if getattr(e, "errno", None) == 1062:
                return (
                    False,
                    "Could not create the account (username or email conflict). "
                    "Try changing the contractor username or email, then retry.",
                    None,
                )
            raise
        try:
            from app.objects import sync_core_user_to_portal_contractor

            sync_core_user_to_portal_contractor(new_id)
        except Exception:
            pass
        return (
            True,
            f"Core Admin user created (linked to contractor #{cid}; sign in at /login as "
            f"{new_un!r} or the work email, same password as the employee portal.",
            new_id,
        )
    except mysql.connector.Error as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("promote_contractor_to_core_admin_user: %s", e)
        return False, "Database error while creating the core user.", None
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("promote_contractor_to_core_admin_user: %s", e)
        return False, str(e) or "Unexpected error.", None
    finally:
        try:
            dcur.close()
        except Exception:
            pass
        try:
            xcur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def admin_update_staff_profile(contractor_id: int, data: Dict[str, Any]) -> bool:
    """Update all admin-editable HR fields. Returns True if contractor exists."""
    import mysql.connector

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM tb_contractors WHERE id = %s",
                    (contractor_id,))
        if not cur.fetchone():
            return False
        dl_path = _normalize_stored_relative_upload_path(
            data.get("driving_licence_document_path"))
        rtw_path = _normalize_stored_relative_upload_path(
            data.get("right_to_work_document_path"))
        dbs_path = _normalize_stored_relative_upload_path(
            data.get("dbs_document_path"))
        contract_path = _normalize_stored_relative_upload_path(
            data.get("contract_document_path"))
        job_title = _truncate_str(data.get("job_title"), 128)
        department = _truncate_str(data.get("department"), 128)
        manager_id = _optional_manager_contractor_id(
            data.get("manager_contractor_id"), contractor_id)
        if manager_id is not None:
            cur.execute(
                "SELECT id FROM tb_contractors WHERE id = %s", (manager_id,))
            if not cur.fetchone():
                manager_id = None
        dbs_sub = 1 if data.get("dbs_update_service_subscribed") else 0
        dbs_cert_ref = _truncate_str(data.get("dbs_certificate_ref"), 64)
        dbs_consent = data.get("dbs_update_consent_at")
        hpac_num = _truncate_str(data.get("hpac_registration_number"), 64)
        hpac_on = _parse_date(data.get("hpac_registered_on"))
        hpac_grade = _truncate_str(data.get("hpac_register_grade"), 64)
        hpac_chk_at = data.get("hpac_register_last_checked_at")
        hpac_st = (data.get("hpac_register_status") or "").strip().lower()
        if hpac_st and hpac_st not in HPAC_REGISTER_STATUS_VALUES:
            hpac_st = None
        elif not hpac_st:
            hpac_st = None
        hpac_notes = (data.get("hpac_register_check_notes") or "").strip() or None
        if hpac_notes and len(hpac_notes) > 65000:
            hpac_notes = hpac_notes[:65000]
        hcpc_num = _truncate_str(data.get("hcpc_registration_number"), 64)
        hcpc_on = _parse_date(data.get("hcpc_registered_on"))
        hcpc_prof = _truncate_str(data.get("hcpc_register_profession"), 64)
        hcpc_chk_at = data.get("hcpc_register_last_checked_at")
        hcpc_st = (data.get("hcpc_register_status") or "").strip().lower()
        if hcpc_st and hcpc_st not in HCPC_REGISTER_STATUS_VALUES:
            hcpc_st = None
        elif not hcpc_st:
            hcpc_st = None
        hcpc_notes = (data.get("hcpc_register_check_notes") or "").strip() or None
        if hcpc_notes and len(hcpc_notes) > 65000:
            hcpc_notes = hcpc_notes[:65000]
        custom_cf = data.get("custom_fields_json")
        if custom_cf is not None:
            custom_cf = str(custom_cf).strip() or None
            if custom_cf and len(custom_cf) > 65000:
                custom_cf = custom_cf[:65000]
        gmc_num = _truncate_str(data.get("gmc_number"), 32)
        gmc_on = _parse_date(data.get("gmc_registered_on"))
        gmc_chk_at = data.get("gmc_register_last_checked_at")
        gmc_st = (data.get("gmc_register_status") or "").strip().lower()
        if gmc_st and gmc_st not in HPAC_REGISTER_STATUS_VALUES:
            gmc_st = None
        elif not gmc_st:
            gmc_st = None
        gmc_notes = (data.get("gmc_register_check_notes") or "").strip() or None
        if gmc_notes and len(gmc_notes) > 65000:
            gmc_notes = gmc_notes[:65000]
        nmc_pin = _truncate_str(data.get("nmc_pin"), 32)
        nmc_on = _parse_date(data.get("nmc_registered_on"))
        nmc_chk_at = data.get("nmc_register_last_checked_at")
        nmc_st = (data.get("nmc_register_status") or "").strip().lower()
        if nmc_st and nmc_st not in HPAC_REGISTER_STATUS_VALUES:
            nmc_st = None
        elif not nmc_st:
            nmc_st = None
        nmc_notes = (data.get("nmc_register_check_notes") or "").strip() or None
        if nmc_notes and len(nmc_notes) > 65000:
            nmc_notes = nmc_notes[:65000]
        gphc_num = _truncate_str(data.get("gphc_registration_number"), 32)
        gphc_on = _parse_date(data.get("gphc_registered_on"))
        gphc_chk_at = data.get("gphc_register_last_checked_at")
        gphc_st = (data.get("gphc_register_status") or "").strip().lower()
        if gphc_st and gphc_st not in HPAC_REGISTER_STATUS_VALUES:
            gphc_st = None
        elif not gphc_st:
            gphc_st = None
        gphc_notes = (data.get("gphc_register_check_notes") or "").strip() or None
        if gphc_notes and len(gphc_notes) > 65000:
            gphc_notes = gphc_notes[:65000]
        row_common = (
            contractor_id,
            data.get("phone"),
            data.get("address_line1"),
            data.get("address_line2"),
            data.get("postcode"),
            data.get("emergency_contact_name"),
            data.get("emergency_contact_phone"),
            _parse_date(data.get("date_of_birth")),
            data.get("driving_licence_number"),
            _parse_date(data.get("driving_licence_expiry")),
            dl_path,
            data.get("right_to_work_type"),
            _parse_date(data.get("right_to_work_expiry")),
            rtw_path,
            data.get("dbs_level"),
            data.get("dbs_number"),
            _parse_date(data.get("dbs_expiry")),
            dbs_path,
            data.get("contract_type"),
            _parse_date(data.get("contract_start")),
            _parse_date(data.get("contract_end")),
            contract_path,
            job_title,
            department,
            manager_id,
            hpac_num,
            hpac_on,
            hpac_grade,
            hpac_chk_at,
            hpac_st,
            hpac_notes,
            hcpc_num,
            hcpc_on,
            hcpc_prof,
            hcpc_chk_at,
            hcpc_st,
            hcpc_notes,
            custom_cf,
            gmc_num,
            gmc_on,
            gmc_chk_at,
            gmc_st,
            gmc_notes,
            nmc_pin,
            nmc_on,
            nmc_chk_at,
            nmc_st,
            nmc_notes,
            gphc_num,
            gphc_on,
            gphc_chk_at,
            gphc_st,
            gphc_notes,
        )
        try:
            cur.execute("""
                INSERT INTO hr_staff_details (
                    contractor_id, phone, address_line1, address_line2, postcode,
                    emergency_contact_name, emergency_contact_phone, date_of_birth,
                    driving_licence_number, driving_licence_expiry, driving_licence_document_path,
                    right_to_work_type, right_to_work_expiry, right_to_work_document_path,
                    dbs_level, dbs_number, dbs_expiry, dbs_document_path,
                    dbs_update_service_subscribed, dbs_certificate_ref, dbs_update_consent_at,
                    contract_type, contract_start, contract_end, contract_document_path,
                    job_title, department, manager_contractor_id,
                    hpac_registration_number, hpac_registered_on, hpac_register_grade,
                    hpac_register_last_checked_at, hpac_register_status, hpac_register_check_notes,
                    hcpc_registration_number, hcpc_registered_on, hcpc_register_profession,
                    hcpc_register_last_checked_at, hcpc_register_status, hcpc_register_check_notes,
                    custom_fields_json,
                    gmc_number, gmc_registered_on, gmc_register_last_checked_at, gmc_register_status, gmc_register_check_notes,
                    nmc_pin, nmc_registered_on, nmc_register_last_checked_at, nmc_register_status, nmc_register_check_notes,
                    gphc_registration_number, gphc_registered_on, gphc_register_last_checked_at, gphc_register_status, gphc_register_check_notes
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON DUPLICATE KEY UPDATE
                phone = VALUES(phone), address_line1 = VALUES(address_line1), address_line2 = VALUES(address_line2),
                postcode = VALUES(postcode), emergency_contact_name = VALUES(emergency_contact_name),
                emergency_contact_phone = VALUES(emergency_contact_phone),
                date_of_birth = VALUES(date_of_birth),
                driving_licence_number = VALUES(driving_licence_number), driving_licence_expiry = VALUES(driving_licence_expiry),
                driving_licence_document_path = VALUES(driving_licence_document_path),
                right_to_work_type = VALUES(right_to_work_type), right_to_work_expiry = VALUES(right_to_work_expiry),
                right_to_work_document_path = VALUES(right_to_work_document_path),
                dbs_level = VALUES(dbs_level), dbs_number = VALUES(dbs_number), dbs_expiry = VALUES(dbs_expiry),
                dbs_document_path = VALUES(dbs_document_path),
                dbs_update_service_subscribed = VALUES(dbs_update_service_subscribed),
                dbs_certificate_ref = VALUES(dbs_certificate_ref),
                dbs_update_consent_at = VALUES(dbs_update_consent_at),
                contract_type = VALUES(contract_type), contract_start = VALUES(contract_start),
                contract_end = VALUES(contract_end), contract_document_path = VALUES(contract_document_path),
                job_title = VALUES(job_title), department = VALUES(department), manager_contractor_id = VALUES(manager_contractor_id),
                hpac_registration_number = VALUES(hpac_registration_number),
                hpac_registered_on = VALUES(hpac_registered_on),
                hpac_register_grade = VALUES(hpac_register_grade),
                hpac_register_last_checked_at = VALUES(hpac_register_last_checked_at),
                hpac_register_status = VALUES(hpac_register_status),
                hpac_register_check_notes = VALUES(hpac_register_check_notes),
                hcpc_registration_number = VALUES(hcpc_registration_number),
                hcpc_registered_on = VALUES(hcpc_registered_on),
                hcpc_register_profession = VALUES(hcpc_register_profession),
                hcpc_register_last_checked_at = VALUES(hcpc_register_last_checked_at),
                hcpc_register_status = VALUES(hcpc_register_status),
                hcpc_register_check_notes = VALUES(hcpc_register_check_notes),
                custom_fields_json = VALUES(custom_fields_json),
                gmc_number = VALUES(gmc_number), gmc_registered_on = VALUES(gmc_registered_on),
                gmc_register_last_checked_at = VALUES(gmc_register_last_checked_at),
                gmc_register_status = VALUES(gmc_register_status), gmc_register_check_notes = VALUES(gmc_register_check_notes),
                nmc_pin = VALUES(nmc_pin), nmc_registered_on = VALUES(nmc_registered_on),
                nmc_register_last_checked_at = VALUES(nmc_register_last_checked_at),
                nmc_register_status = VALUES(nmc_register_status), nmc_register_check_notes = VALUES(nmc_register_check_notes),
                gphc_registration_number = VALUES(gphc_registration_number), gphc_registered_on = VALUES(gphc_registered_on),
                gphc_register_last_checked_at = VALUES(gphc_register_last_checked_at),
                gphc_register_status = VALUES(gphc_register_status), gphc_register_check_notes = VALUES(gphc_register_check_notes)
            """, row_common[:18] + (dbs_sub, dbs_cert_ref, dbs_consent) + row_common[18:])
            conn.commit()
            sync_ventus_crew_profile_from_hr_training(contractor_id)
            return True
        except mysql.connector.Error as exc:
            if getattr(exc, "errno", None) != 1054:
                logger.warning("admin_update_staff_profile: %s", exc)
                try:
                    conn.rollback()
                except Exception:
                    pass
                return False
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                cur.execute("""
                    INSERT INTO hr_staff_details (
                        contractor_id, phone, address_line1, address_line2, postcode,
                        emergency_contact_name, emergency_contact_phone, date_of_birth,
                        driving_licence_number, driving_licence_expiry, driving_licence_document_path,
                        right_to_work_type, right_to_work_expiry, right_to_work_document_path,
                        dbs_level, dbs_number, dbs_expiry, dbs_document_path,
                        contract_type, contract_start, contract_end, contract_document_path,
                        job_title, department, manager_contractor_id,
                        hpac_registration_number, hpac_registered_on, hpac_register_grade,
                        hpac_register_last_checked_at, hpac_register_status, hpac_register_check_notes,
                        hcpc_registration_number, hcpc_registered_on, hcpc_register_profession,
                        hcpc_register_last_checked_at, hcpc_register_status, hcpc_register_check_notes
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                    ON DUPLICATE KEY UPDATE
                    phone = VALUES(phone), address_line1 = VALUES(address_line1), address_line2 = VALUES(address_line2),
                    postcode = VALUES(postcode), emergency_contact_name = VALUES(emergency_contact_name),
                    emergency_contact_phone = VALUES(emergency_contact_phone),
                    date_of_birth = VALUES(date_of_birth),
                    driving_licence_number = VALUES(driving_licence_number), driving_licence_expiry = VALUES(driving_licence_expiry),
                    driving_licence_document_path = VALUES(driving_licence_document_path),
                    right_to_work_type = VALUES(right_to_work_type), right_to_work_expiry = VALUES(right_to_work_expiry),
                    right_to_work_document_path = VALUES(right_to_work_document_path),
                    dbs_level = VALUES(dbs_level), dbs_number = VALUES(dbs_number), dbs_expiry = VALUES(dbs_expiry),
                    dbs_document_path = VALUES(dbs_document_path),
                    contract_type = VALUES(contract_type), contract_start = VALUES(contract_start),
                    contract_end = VALUES(contract_end), contract_document_path = VALUES(contract_document_path),
                    job_title = VALUES(job_title), department = VALUES(department), manager_contractor_id = VALUES(manager_contractor_id),
                    hpac_registration_number = VALUES(hpac_registration_number),
                    hpac_registered_on = VALUES(hpac_registered_on),
                    hpac_register_grade = VALUES(hpac_register_grade),
                    hpac_register_last_checked_at = VALUES(hpac_register_last_checked_at),
                    hpac_register_status = VALUES(hpac_register_status),
                    hpac_register_check_notes = VALUES(hpac_register_check_notes),
                    hcpc_registration_number = VALUES(hcpc_registration_number),
                    hcpc_registered_on = VALUES(hcpc_registered_on),
                    hcpc_register_profession = VALUES(hcpc_register_profession),
                    hcpc_register_last_checked_at = VALUES(hcpc_register_last_checked_at),
                    hcpc_register_status = VALUES(hcpc_register_status),
                    hcpc_register_check_notes = VALUES(hcpc_register_check_notes)
                """, row_common[:25] + row_common[25:])
                conn.commit()
                sync_ventus_crew_profile_from_hr_training(contractor_id)
                logger.warning(
                    "admin_update_staff_profile: hr_staff_details missing DBS Update Service "
                    "columns; saved without them. Run: python app/plugins/hr_module/install.py install"
                )
                return True
            except Exception as exc2:
                logger.warning("admin_update_staff_profile (legacy): %s", exc2)
                try:
                    conn.rollback()
                except Exception:
                    pass
                return False
    except Exception as exc:
        logger.warning("admin_update_staff_profile: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        cur.close()
        conn.close()


def list_dbs_status_check_logs(contractor_id: int, limit: int = 50):
    """History rows for DBS Update Service checks (empty if table missing)."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SHOW TABLES LIKE 'dbs_status_check_log'")
        if not cur.fetchone():
            return []
        cur.execute(
            """
            SELECT id, checked_at, status_code, result_type, channel, checker_user_id,
                   checker_label, http_status, error_message
            FROM dbs_status_check_log
            WHERE contractor_id = %s
            ORDER BY checked_at DESC
            LIMIT %s
            """,
            (int(contractor_id), int(limit)),
        )
        rows = cur.fetchall()
        colnames = [
            "id",
            "checked_at",
            "status_code",
            "result_type",
            "channel",
            "checker_user_id",
            "checker_label",
            "http_status",
            "error_message",
        ]
        return [dict(zip(colnames, r)) for r in rows]
    except Exception:
        return []
    finally:
        cur.close()
        conn.close()


def admin_update_contractor_profile_picture(contractor_id: int, path: Optional[str]) -> bool:
    """
    Set or clear tb_contractors.profile_picture_path (column must exist).
    Pass path=None to clear. Relative paths are normalized to forward slashes.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if not _contractors_has_profile_picture_column(cur):
            return False
        cur.execute("SELECT id FROM tb_contractors WHERE id = %s",
                    (contractor_id,))
        if not cur.fetchone():
            return False
        norm = _normalize_stored_relative_upload_path(path) if path else None
        cur.execute(
            "UPDATE tb_contractors SET profile_picture_path = %s WHERE id = %s",
            (norm, contractor_id),
        )
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        cur.close()
        conn.close()


def _parse_date(v: Any) -> Optional[date]:
    if v is None or v == "":
        return None
    if isinstance(v, date):
        return v
    try:
        if isinstance(v, datetime):
            return v.date()
        return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def list_document_requests(contractor_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT r.id, r.title, r.description, r.required_by_date, r.status, r.created_at, r.request_type,
                   r.approved_at, r.rejected_at, r.admin_notes,
                   (SELECT COUNT(*) FROM hr_document_uploads u WHERE u.request_id = r.id) AS upload_count
            FROM hr_document_requests r
            WHERE r.contractor_id = %s
            ORDER BY
              CASE r.status
                WHEN 'rejected' THEN 0
                WHEN 'overdue' THEN 1
                WHEN 'pending' THEN 2
                WHEN 'uploaded' THEN 3
                ELSE 4
              END,
              r.required_by_date IS NULL ASC,
              r.required_by_date ASC,
              r.created_at DESC
        """, (contractor_id,))
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def add_upload(
    request_id: int,
    contractor_id: int,
    file_path: str,
    file_name: Optional[str] = None,
    document_type: str = "primary",
) -> int:
    """Contractor upload; allowed for pending or rejected (replacement). Sets status to 'uploaded'."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT contractor_id, status FROM hr_document_requests WHERE id = %s",
            (request_id,),
        )
        row = cur.fetchone()
        if not row or row["contractor_id"] != contractor_id:
            raise ValueError("Request not found or not yours")
        if row["status"] not in ("pending", "uploaded", "overdue", "rejected"):
            raise ValueError("Cannot upload for this request")
        doc_type = "replacement" if document_type == "replacement" else "primary"
        cur2 = conn.cursor()
        cur2.execute(
            "INSERT INTO hr_document_uploads (request_id, file_path, file_name, document_type) VALUES (%s, %s, %s, %s)",
            (request_id, file_path, file_name, doc_type),
        )
        upload_id = cur2.lastrowid
        cur2.execute(
            "UPDATE hr_document_requests SET status = 'uploaded', rejected_at = NULL, rejected_by_user_id = NULL, admin_notes = NULL WHERE id = %s",
            (request_id,),
        )
        conn.commit()
        cur2.close()
        _hr_ep_complete_request_todo(request_id)
        return upload_id
    finally:
        cur.close()
        conn.close()


# -----------------------------------------------------------------------------
# Admin: document requests
# -----------------------------------------------------------------------------


def admin_list_document_requests(
    contractor_id: Optional[int] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: int = 100,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    """List requests with filters. Returns (rows, total)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["1=1"]
        params: List[Any] = []
        if contractor_id is not None:
            where.append("r.contractor_id = %s")
            params.append(contractor_id)
        if status:
            where.append("r.status = %s")
            params.append(status)
        if date_from:
            where.append("r.required_by_date >= %s")
            params.append(date_from)
        if date_to:
            where.append("r.required_by_date <= %s")
            params.append(date_to)
        params.extend([limit, offset])
        cur.execute(f"""
            SELECT SQL_CALC_FOUND_ROWS r.id, r.contractor_id, r.title, r.description, r.required_by_date, r.status,
                   r.request_type, r.created_at, c.name AS contractor_name, c.email AS contractor_email
            FROM hr_document_requests r
            JOIN tb_contractors c ON c.id = r.contractor_id
            WHERE {" AND ".join(where)}
            ORDER BY r.required_by_date IS NULL ASC, r.required_by_date ASC, r.created_at DESC
            LIMIT %s OFFSET %s
        """, params)
        rows = cur.fetchall() or []
        cur.execute("SELECT FOUND_ROWS() AS total")
        total = (cur.fetchone() or {}).get("total") or 0
        return rows, total
    finally:
        cur.close()
        conn.close()


def admin_create_document_request(
    contractor_ids: List[int],
    title: str,
    description: Optional[str] = None,
    required_by_date: Optional[date] = None,
    request_type: str = "other",
) -> int:
    """Create one request per contractor. Returns count created."""
    if not contractor_ids or not title.strip():
        return 0
    req_type = request_type if request_type in REQUEST_TYPES else "other"
    ttl = title.strip()[:255]
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        count = 0
        created: List[Tuple[int, int]] = []
        for cid in contractor_ids:
            cur.execute("""
                INSERT INTO hr_document_requests (contractor_id, title, description, required_by_date, request_type)
                VALUES (%s, %s, %s, %s, %s)
            """, (cid, ttl, (description or "")[:65535] or None, required_by_date, req_type))
            count += cur.rowcount
            rid = cur.lastrowid
            if rid:
                created.append((int(cid), int(rid)))
        conn.commit()
        for cid, rid in created:
            _hr_ep_upsert_request_todo(
                cid, rid, ttl, rejected=False, due_date=required_by_date)
        try:
            from . import notifications as hr_notifications

            for cid, rid in created:
                hr_notifications.notify_contractor_document_request_created(
                    int(cid), int(rid), ttl
                )
        except Exception as e:
            logger.debug("HR document request notify skipped: %s", e)
        return count
    finally:
        cur.close()
        conn.close()


def _contractors_has_profile_picture_column(cur) -> bool:
    try:
        cur.execute(
            "SHOW COLUMNS FROM tb_contractors LIKE 'profile_picture_path'")
        return bool(cur.fetchone())
    except Exception:
        return False


def _coalesce_datetime_for_sort(t: Any) -> datetime:
    """Compare request upload time vs library created_at."""
    if isinstance(t, datetime):
        return t
    if isinstance(t, date):
        return datetime.combine(t, datetime.min.time())
    return datetime.min


def _reconcile_profile_picture_to_contractor(cur, cid: int) -> None:
    """
    Set tb_contractors.profile_picture_path from the newest of:
    - approved profile-photo document request (preferred upload row timestamp), or
    - document library row with category profile_picture (created_at).

    So an admin upload on Edit profile (stored as library + path) can override an older staff request.
    """
    if not _contractors_has_profile_picture_column(cur):
        return
    req_path: Optional[str] = None
    req_ts: Any = None
    cur.execute(
        """
        SELECT r.id FROM hr_document_requests r
        WHERE r.contractor_id = %s AND r.status = 'approved'
          AND LOWER(COALESCE(r.request_type, 'other')) = 'profile_picture'
          AND EXISTS (SELECT 1 FROM hr_document_uploads u WHERE u.request_id = r.id)
        ORDER BY (r.approved_at IS NULL), r.approved_at DESC, r.id DESC
        LIMIT 1
        """,
        (cid,),
    )
    row = cur.fetchone()
    if row:
        rid = int(row["id"])
        cur.execute(
            """
            SELECT file_path, document_type, uploaded_at FROM hr_document_uploads
            WHERE request_id = %s ORDER BY uploaded_at DESC
            """,
            (rid,),
        )
        ups = cur.fetchall() or []
        pu = _select_preferred_upload_row(ups)
        if pu:
            raw = (pu.get("file_path") or "").strip()
            req_path = _normalize_stored_relative_upload_path(
                raw) if raw else None
            req_ts = pu.get("uploaded_at")

    lib_path: Optional[str] = None
    lib_ts: Any = None
    try:
        cur.execute("SHOW TABLES LIKE 'hr_employee_documents'")
        if cur.fetchone():
            cur.execute(
                """
                SELECT file_path, created_at FROM hr_employee_documents
                WHERE contractor_id = %s AND LOWER(category) = 'profile_picture'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (cid,),
            )
            lib = cur.fetchone()
            if lib and (lib.get("file_path") or "").strip():
                raw = (lib.get("file_path") or "").strip()
                lib_path = _normalize_stored_relative_upload_path(
                    raw) if raw else None
                lib_ts = lib.get("created_at")
    except Exception:
        pass

    candidates: List[Tuple[datetime, str]] = []
    if req_path:
        candidates.append((_coalesce_datetime_for_sort(req_ts), req_path))
    if lib_path:
        candidates.append((_coalesce_datetime_for_sort(lib_ts), lib_path))
    if not candidates:
        return
    candidates.sort(key=lambda x: x[0], reverse=True)
    path = candidates[0][1]
    if path:
        cur.execute(
            "UPDATE tb_contractors SET profile_picture_path = %s WHERE id = %s",
            (path, cid),
        )


def _upload_row_sort_key(u: Dict[str, Any]):
    t = u.get("uploaded_at")
    return t if isinstance(t, datetime) else (t or "")


def _select_preferred_upload_row(uploads: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Prefer latest non-replacement upload row; else latest file row."""
    if not uploads:
        return None
    non_rep = [
        u
        for u in uploads
        if str(u.get("document_type") or "primary").strip().lower() != "replacement"
    ]
    pool = non_rep if non_rep else uploads
    pool = sorted(pool, key=_upload_row_sort_key, reverse=True)
    return pool[0]


def _select_preferred_upload_path(uploads: List[Dict[str, Any]]) -> Optional[str]:
    """Prefer latest non-replacement upload; else latest file."""
    row = _select_preferred_upload_row(uploads)
    if not row:
        return None
    p = (row.get("file_path") or "").strip()
    return p or None


def reconcile_staff_details_from_approved_requests(contractor_id: int) -> None:
    """
    Copy file paths from the latest approved document request (per type) into hr_staff_details
    so the profile and compliance checks reflect staff uploads — not only manual profile edits.

    Safe to call on every profile view (idempotent). Also called when a request is approved.
    """
    cid = int(contractor_id)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "INSERT IGNORE INTO hr_staff_details (contractor_id) VALUES (%s)",
            (cid,),
        )
        for req_type, col in REQUEST_TYPE_TO_STAFF_DOCUMENT_COLUMN.items():
            cur.execute(
                f"""
                SELECT r.id FROM hr_document_requests r
                WHERE r.contractor_id = %s AND r.status = 'approved'
                  AND LOWER(COALESCE(r.request_type, 'other')) = %s
                  AND EXISTS (SELECT 1 FROM hr_document_uploads u WHERE u.request_id = r.id)
                ORDER BY (r.approved_at IS NULL), r.approved_at DESC, r.id DESC
                LIMIT 1
                """,
                (cid, req_type),
            )
            row = cur.fetchone()
            if not row:
                continue
            rid = int(row["id"])
            cur.execute(
                """
                SELECT file_path, document_type, uploaded_at FROM hr_document_uploads
                WHERE request_id = %s ORDER BY uploaded_at DESC
                """,
                (rid,),
            )
            ups = cur.fetchall() or []
            path = _select_preferred_upload_path(ups)
            if not path:
                continue
            path = _normalize_stored_relative_upload_path(path)
            if not path:
                continue
            cur.execute(
                "UPDATE hr_staff_details SET `{}` = %s WHERE contractor_id = %s".format(
                    col),
                (path, cid),
            )
        _reconcile_profile_picture_to_contractor(cur, cid)
        conn.commit()
    except Exception as e:
        logger.warning(
            "HR: reconcile_staff_details_from_approved_requests(%s): %s", cid, e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()


def contractor_approved_request_types_with_upload(contractor_id: int) -> Set[str]:
    """Set of request_type keys that have at least one approved request with an upload (for compliance UI)."""
    cid = int(contractor_id)
    out: Set[str] = set()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT DISTINCT LOWER(COALESCE(r.request_type, 'other')) AS rt
            FROM hr_document_requests r
            INNER JOIN hr_document_uploads u ON u.request_id = r.id
            WHERE r.contractor_id = %s AND r.status = 'approved'
            """,
            (cid,),
        )
        for row in cur.fetchall() or []:
            rt = (row.get("rt") or "").strip().lower()
            if rt in REQUEST_TYPE_TO_STAFF_DOCUMENT_COLUMN or rt == "profile_picture":
                out.add(rt)
        return out
    finally:
        cur.close()
        conn.close()


def admin_active_request_id_for_type(contractor_id: int, request_type: str) -> Optional[int]:
    """
    If this person already has a document request of this type still in progress, return its id.
    Avoids duplicate onboarding clicks for the same category.
    """
    rt = (request_type or "other").strip().lower()
    if rt not in REQUEST_TYPES:
        rt = "other"
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id FROM hr_document_requests
            WHERE contractor_id = %s AND LOWER(COALESCE(request_type, 'other')) = %s
              AND status IN ('pending', 'uploaded', 'overdue')
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(contractor_id), rt),
        )
        row = cur.fetchone()
        if row and row[0]:
            return int(row[0])
        return None
    finally:
        cur.close()
        conn.close()


# Preset onboarding: pack id -> (request_type, title) — skips types that already have an open request.
HR_ONBOARDING_PACKS: Dict[str, List[Tuple[str, str]]] = {
    "office_standard": [
        ("right_to_work", "Right to work evidence"),
        ("dbs", "DBS certificate"),
        ("profile_picture", "Profile photo for ID and systems"),
    ],
    "field_based": [
        ("driving_licence", "Driving licence"),
        ("right_to_work", "Right to work evidence"),
        ("dbs", "DBS certificate"),
    ],
    "minimal": [
        ("right_to_work", "Right to work evidence"),
        ("contract", "Signed contract or written terms"),
    ],
    # Industry-tagged seeds (mirror install.py — used if DB row missing)
    "regulated_health_clinical": [
        ("right_to_work", "Right to work evidence"),
        ("dbs", "DBS certificate"),
        ("other", "Professional registration evidence (e.g. HCPC, GMC, NMC)"),
    ],
    "security_licence_hr": [
        ("right_to_work", "Right to work evidence"),
        ("dbs", "DBS certificate (if required)"),
        ("other", "Security licence evidence (e.g. SIA)"),
    ],
    "hospitality_guest_facing": [
        ("right_to_work", "Right to work evidence"),
        ("profile_picture", "Uniform or ID photo"),
    ],
    "cleaning_facilities": [
        ("right_to_work", "Right to work evidence"),
        ("dbs", "DBS certificate (if required)"),
    ],
}


_ONBOARDING_PACK_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


def ensure_hr_onboarding_packs_schema() -> None:
    """Create onboarding pack tables and seed defaults if missing (safe for live sites)."""
    try:
        from app.plugins.hr_module import install as hr_install
    except ImportError:
        return
    conn = get_db_connection()
    try:
        hr_install._run_sql(conn, hr_install.SQL_CREATE_HR_ONBOARDING_PACKS)
        hr_install._run_sql(
            conn, hr_install.SQL_CREATE_HR_ONBOARDING_PACK_ITEMS)
        hr_install._seed_hr_onboarding_packs_if_empty(conn)
    finally:
        conn.close()


def _resolve_onboarding_pack_items(pack_key: str) -> Optional[List[Tuple[str, str]]]:
    """Load pack lines from DB, or fall back to built-in HR_ONBOARDING_PACKS."""
    key = (pack_key or "").strip().lower()
    if not key:
        return None
    ensure_hr_onboarding_packs_schema()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT i.request_type, i.title
            FROM hr_onboarding_packs p
            JOIN hr_onboarding_pack_items i ON i.pack_id = p.id
            WHERE LOWER(p.pack_key) = %s AND p.active = 1
            ORDER BY i.sort_order ASC, i.id ASC
            """,
            (key,),
        )
        rows = cur.fetchall() or []
        if rows:
            return [(str(r[0] or "other").strip().lower(), str(r[1] or "").strip()) for r in rows]
    finally:
        cur.close()
        conn.close()
    builtin = HR_ONBOARDING_PACKS.get(key)
    return list(builtin) if builtin else None


def hr_onboarding_pack_choices() -> List[Dict[str, str]]:
    """Active packs for dropdowns (from database, seeded from built-ins)."""
    ensure_hr_onboarding_packs_schema()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT pack_key AS `key`, label
            FROM hr_onboarding_packs
            WHERE active = 1
            ORDER BY sort_order ASC, id ASC
            """
        )
        rows = cur.fetchall() or []
        if rows:
            return [
                {"key": str(r["key"]), "label": str(
                    r.get("label") or r["key"])}
                for r in rows
            ]
    finally:
        cur.close()
        conn.close()
    labels = {
        "office_standard": "Office / standard",
        "field_based": "Field / mobile",
        "minimal": "Minimal (right to work + contract)",
        "regulated_health_clinical": "Clinical / regulated roles",
        "security_licence_hr": "Security / guarding",
        "hospitality_guest_facing": "Hospitality / guest-facing",
        "cleaning_facilities": "Cleaning / facilities",
    }
    return [
        {"key": k, "label": labels.get(k, k.replace("_", " ").title())}
        for k in HR_ONBOARDING_PACKS
    ]


def admin_list_onboarding_packs_for_settings(include_inactive: bool = True) -> List[Dict[str, Any]]:
    """All packs for admin settings (with item counts)."""
    ensure_hr_onboarding_packs_schema()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        q = """
            SELECT p.id, p.pack_key, p.label, p.sort_order, p.active,
                   (SELECT COUNT(*) FROM hr_onboarding_pack_items i WHERE i.pack_id = p.id) AS item_count
            FROM hr_onboarding_packs p
        """
        if not include_inactive:
            q += " WHERE p.active = 1"
        q += " ORDER BY p.sort_order ASC, p.id ASC"
        cur.execute(q)
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def admin_get_onboarding_pack_for_edit(pack_id: int) -> Optional[Dict[str, Any]]:
    ensure_hr_onboarding_packs_schema()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, pack_key, label, sort_order, active
            FROM hr_onboarding_packs WHERE id = %s LIMIT 1
            """,
            (int(pack_id),),
        )
        pack = cur.fetchone()
        if not pack:
            return None
        cur.execute(
            """
            SELECT request_type, title, sort_order
            FROM hr_onboarding_pack_items
            WHERE pack_id = %s
            ORDER BY sort_order ASC, id ASC
            """,
            (int(pack_id),),
        )
        pack["items"] = list(cur.fetchall() or [])
        return pack
    finally:
        cur.close()
        conn.close()


def admin_delete_onboarding_pack(pack_id: int) -> Tuple[bool, str]:
    ensure_hr_onboarding_packs_schema()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM hr_onboarding_packs WHERE id = %s",
                    (int(pack_id),))
        conn.commit()
        if cur.rowcount:
            return True, "Pack deleted."
        return False, "Pack not found."
    except Exception as e:
        conn.rollback()
        logger.warning("delete onboarding pack %s: %s", pack_id, e)
        return False, str(e) or "Could not delete pack."
    finally:
        cur.close()
        conn.close()


def admin_save_onboarding_pack(
    pack_id: Optional[int],
    pack_key: str,
    label: str,
    sort_order: int,
    active: bool,
    item_pairs: List[Tuple[str, str]],
) -> Tuple[bool, str, Optional[int]]:
    """
    Create or replace a pack and its items. item_pairs: (request_type, title).
    """
    ensure_hr_onboarding_packs_schema()
    key = (pack_key or "").strip().lower()
    if not _ONBOARDING_PACK_KEY_RE.match(key):
        return False, "Pack key must start with a letter and use only lowercase letters, digits, and underscores (max 64 chars).", None
    lab = (label or "").strip()
    if not lab:
        return False, "Display name is required.", None
    cleaned: List[Tuple[str, str]] = []
    for rt, title in item_pairs:
        t = (title or "").strip()
        if not t:
            continue
        rtype = (rt or "other").strip().lower()
        if rtype not in REQUEST_TYPES:
            rtype = "other"
        cleaned.append((rtype, t[:255]))
    if not cleaned:
        return False, "Add at least one document line with a title.", None

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if pack_id:
            cur.execute(
                "SELECT id FROM hr_onboarding_packs WHERE id = %s LIMIT 1",
                (int(pack_id),),
            )
            if not cur.fetchone():
                return False, "Pack not found.", None
            cur.execute(
                "SELECT id FROM hr_onboarding_packs WHERE LOWER(pack_key) = %s AND id <> %s LIMIT 1",
                (key, int(pack_id)),
            )
            if cur.fetchone():
                return False, "Another pack already uses this key.", None
            cur.execute(
                """
                UPDATE hr_onboarding_packs
                SET label = %s, sort_order = %s, active = %s
                WHERE id = %s
                """,
                (lab[:255], int(sort_order), 1 if active else 0, int(pack_id)),
            )
            pid = int(pack_id)
        else:
            cur.execute(
                "SELECT id FROM hr_onboarding_packs WHERE LOWER(pack_key) = %s LIMIT 1",
                (key,),
            )
            if cur.fetchone():
                return False, "A pack with this key already exists.", None
            cur.execute(
                """
                INSERT INTO hr_onboarding_packs (pack_key, label, sort_order, active)
                VALUES (%s, %s, %s, %s)
                """,
                (key, lab[:255], int(sort_order), 1 if active else 0),
            )
            pid = int(cur.lastrowid)

        cur.execute(
            "DELETE FROM hr_onboarding_pack_items WHERE pack_id = %s", (pid,))
        for i, (rtype, t) in enumerate(cleaned):
            cur.execute(
                """
                INSERT INTO hr_onboarding_pack_items (pack_id, request_type, title, sort_order)
                VALUES (%s, %s, %s, %s)
                """,
                (pid, rtype[:32], t, i * 10),
            )
        conn.commit()
        return True, "Pack saved.", pid
    except Exception as e:
        conn.rollback()
        logger.warning("save onboarding pack: %s", e)
        return False, str(e) or "Could not save pack.", None
    finally:
        cur.close()
        conn.close()


def admin_apply_onboarding_pack(contractor_id: int, pack_key: str) -> Tuple[int, int, str]:
    """
    Create document requests from a preset pack. Returns (created_count, skipped_count, message).
    """
    pack = _resolve_onboarding_pack_items(pack_key)
    if not pack:
        return 0, 0, "Unknown onboarding pack."
    if not admin_get_staff_profile(contractor_id):
        return 0, 0, "Employee not found."
    created = 0
    skipped = 0
    for rt, title in pack:
        rtype = rt if rt in REQUEST_TYPES else "other"
        if admin_active_request_id_for_type(contractor_id, rtype):
            skipped += 1
            continue
        n = admin_create_document_request(
            [int(contractor_id)], title, request_type=rtype
        )
        created += int(n or 0)
    return created, skipped, f"Created {created} request(s). Skipped {skipped} (already open for that type)."


def contractor_open_request_button_state_by_type(contractor_id: int) -> Dict[str, str]:
    """
    For each request_type that has an open document request (pending / uploaded / overdue),
    return a short state for the compliance UI button:
    - 'awaiting_upload' — staff have not uploaded yet (or only pending/overdue rows).
    - 'awaiting_review' — at least one open request of this type has status uploaded (admin to review).
    """
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT LOWER(COALESCE(request_type, 'other')) AS rt, status
            FROM hr_document_requests
            WHERE contractor_id = %s AND status IN ('pending', 'uploaded', 'overdue')
            """,
            (int(contractor_id),),
        )
        rows = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    by_rt: Dict[str, Set[str]] = defaultdict(set)
    for row in rows or []:
        rt = str(row.get("rt") or "other").strip().lower()
        st = str(row.get("status") or "").strip().lower()
        by_rt[rt].add(st)
    out: Dict[str, str] = {}
    for rt, statuses in by_rt.items():
        if "uploaded" in statuses:
            out[rt] = "awaiting_review"
        else:
            out[rt] = "awaiting_upload"
    return out


def admin_get_request(request_id: int) -> Optional[Dict[str, Any]]:
    """Get request with uploads for admin."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT r.*, c.name AS contractor_name, c.email AS contractor_email
            FROM hr_document_requests r
            JOIN tb_contractors c ON c.id = r.contractor_id
            WHERE r.id = %s
        """, (request_id,))
        req = cur.fetchone()
        if not req:
            return None
        cur.execute(
            "SELECT id, file_path, file_name, document_type, uploaded_at FROM hr_document_uploads WHERE request_id = %s ORDER BY uploaded_at",
            (request_id,),
        )
        req["uploads"] = cur.fetchall() or []
        return req
    finally:
        cur.close()
        conn.close()


def admin_get_request_upload(request_id: int, upload_id: int) -> Optional[Dict[str, Any]]:
    """Single upload row if it belongs to the given request (for secure download)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT u.id, u.request_id, u.file_path, u.file_name, u.document_type, u.uploaded_at
            FROM hr_document_uploads u
            WHERE u.id = %s AND u.request_id = %s
            LIMIT 1
            """,
            (upload_id, request_id),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def contractor_get_document_request_type(contractor_id: int, request_id: int) -> Optional[str]:
    """Return normalized request_type for this contractor's request, or None if not found."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT LOWER(COALESCE(request_type, 'other')) FROM hr_document_requests
            WHERE id = %s AND contractor_id = %s
            """,
            (int(request_id), int(contractor_id)),
        )
        row = cur.fetchone()
        if not row:
            return None
        return str(row[0] or "other").strip().lower()
    finally:
        cur.close()
        conn.close()


def contractor_get_request_upload(
    contractor_id: int, request_id: int, upload_id: int
) -> Optional[Dict[str, Any]]:
    """Upload row only if the request belongs to this contractor (portal download)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT u.id, u.request_id, u.file_path, u.file_name, u.document_type, u.uploaded_at
            FROM hr_document_uploads u
            INNER JOIN hr_document_requests r ON r.id = u.request_id
            WHERE u.id = %s AND u.request_id = %s AND r.contractor_id = %s
            LIMIT 1
            """,
            (upload_id, request_id, contractor_id),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def admin_approve_request(
    request_id: int, user_id: Optional[str], admin_notes: Optional[str] = None
) -> bool:
    ok = False
    contractor_id = 0
    req_title = ""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT contractor_id, title FROM hr_document_requests WHERE id = %s",
            (request_id,),
        )
        meta = cur.fetchone()
        if not meta:
            return False
        contractor_id = int(meta["contractor_id"])
        req_title = (meta.get("title")
                     or "Document request").strip() or "Document request"
        cur2 = conn.cursor()
        cur2.execute("""
            UPDATE hr_document_requests
            SET status = 'approved', approved_at = NOW(), approved_by_user_id = %s,
                rejected_at = NULL, rejected_by_user_id = NULL, admin_notes = %s
            WHERE id = %s
        """, (user_id, (admin_notes or "").strip() or None, request_id))
        ok = cur2.rowcount > 0
        cur2.close()
        conn.commit()
    finally:
        cur.close()
        conn.close()
    if ok:
        try:
            reconcile_staff_details_from_approved_requests(contractor_id)
        except Exception as e:
            logger.warning("HR: reconcile after approve failed: %s", e)
        _hr_ep_complete_request_todo(request_id)
        _hr_ep_message_approved(contractor_id, request_id, req_title)
    return ok


def admin_reject_request(request_id: int, user_id: Optional[str], admin_notes: Optional[str] = None) -> bool:
    ok = False
    contractor_id = 0
    req_title = ""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT contractor_id, title FROM hr_document_requests WHERE id = %s",
            (request_id,),
        )
        meta = cur.fetchone()
        if not meta:
            return False
        contractor_id = int(meta["contractor_id"])
        req_title = (meta.get("title")
                     or "Document request").strip() or "Document request"
        cur2 = conn.cursor()
        cur2.execute("""
            UPDATE hr_document_requests
            SET status = 'rejected', rejected_at = NOW(), rejected_by_user_id = %s, admin_notes = %s,
                approved_at = NULL, approved_by_user_id = NULL
            WHERE id = %s
        """, (user_id, (admin_notes or "").strip() or None, request_id))
        ok = cur2.rowcount > 0
        cur2.close()
        conn.commit()
    finally:
        cur.close()
        conn.close()
    if ok:
        _hr_ep_upsert_request_todo(
            contractor_id, request_id, req_title, rejected=True, due_date=None
        )
        _hr_ep_message_rejected(
            contractor_id, request_id, req_title, admin_notes)
    return ok


def get_expiring_documents(days: int = 30) -> List[Dict[str, Any]]:
    """List staff with documents expiring within the next N days (today .. today+N inclusive)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        from datetime import timedelta

        today = date.today()
        end = today + timedelta(days=days)
        cur.execute(
            """
            SELECT contractor_id AS id, 'driving_licence' AS doc_type, driving_licence_expiry AS expiry_date
            FROM hr_staff_details WHERE driving_licence_expiry IS NOT NULL AND driving_licence_expiry BETWEEN %s AND %s
            UNION ALL
            SELECT contractor_id, 'right_to_work', right_to_work_expiry FROM hr_staff_details
            WHERE right_to_work_expiry IS NOT NULL AND right_to_work_expiry BETWEEN %s AND %s
            UNION ALL
            SELECT contractor_id, 'dbs', dbs_expiry FROM hr_staff_details
            WHERE dbs_expiry IS NOT NULL AND dbs_expiry BETWEEN %s AND %s
            UNION ALL
            SELECT contractor_id, 'contract_end', contract_end FROM hr_staff_details
            WHERE contract_end IS NOT NULL AND contract_end BETWEEN %s AND %s
            ORDER BY expiry_date
            """,
            (today, end, today, end, today, end, today, end),
        )
        rows = cur.fetchall() or []
        for r in rows:
            cur.execute(
                "SELECT name, email FROM tb_contractors WHERE id = %s", (r["id"],))
            c = cur.fetchone()
            r["contractor_name"] = c.get("name") if c else ""
            r["contractor_email"] = c.get("email") if c else ""
            r["row_kind"] = "expiring_soon"
        return rows
    except Exception:
        return []
    finally:
        cur.close()
        conn.close()


def get_expired_compliance_documents() -> List[Dict[str, Any]]:
    """Licence, right to work, DBS, or contract end dates strictly before today (already expired)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        today = date.today()
        cur.execute(
            """
            SELECT contractor_id AS id, 'driving_licence' AS doc_type, driving_licence_expiry AS expiry_date
            FROM hr_staff_details WHERE driving_licence_expiry IS NOT NULL AND driving_licence_expiry < %s
            UNION ALL
            SELECT contractor_id, 'right_to_work', right_to_work_expiry FROM hr_staff_details
            WHERE right_to_work_expiry IS NOT NULL AND right_to_work_expiry < %s
            UNION ALL
            SELECT contractor_id, 'dbs', dbs_expiry FROM hr_staff_details
            WHERE dbs_expiry IS NOT NULL AND dbs_expiry < %s
            UNION ALL
            SELECT contractor_id, 'contract_end', contract_end FROM hr_staff_details
            WHERE contract_end IS NOT NULL AND contract_end < %s
            ORDER BY expiry_date
            """,
            (today, today, today, today),
        )
        rows = cur.fetchall() or []
        for r in rows:
            cur.execute(
                "SELECT name, email FROM tb_contractors WHERE id = %s", (r["id"],))
            c = cur.fetchone()
            r["contractor_name"] = c.get("name") if c else ""
            r["contractor_email"] = c.get("email") if c else ""
            r["row_kind"] = "expired"
        return rows
    except Exception:
        return []
    finally:
        cur.close()
        conn.close()


def contractor_ids_with_expired_hr_dates(contractor_ids: List[int]) -> Set[int]:
    """Subset of contractor IDs that have at least one compliance end date before today."""
    ids = [int(x) for x in contractor_ids if x is not None]
    if not ids:
        return set()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        ph = ",".join(["%s"] * len(ids))
        cur.execute(
            f"""
            SELECT DISTINCT contractor_id FROM hr_staff_details
            WHERE contractor_id IN ({ph})
              AND (
                (driving_licence_expiry IS NOT NULL AND driving_licence_expiry < CURDATE())
                OR (right_to_work_expiry IS NOT NULL AND right_to_work_expiry < CURDATE())
                OR (dbs_expiry IS NOT NULL AND dbs_expiry < CURDATE())
                OR (contract_end IS NOT NULL AND contract_end < CURDATE())
              )
            """,
            tuple(ids),
        )
        return {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}
    except Exception:
        return set()
    finally:
        cur.close()
        conn.close()


def ensure_hr_shell_rows_for_all_contractors() -> int:
    """
    Every person in tb_contractors is the same record used across modules (time billing, HR, portal, Ventus).
    hr_staff_details is the extended HR row: ensure a shell exists for each contractor so counts and joins align.
    Returns number of rows inserted (best effort).
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO hr_staff_details (contractor_id)
            SELECT c.id FROM tb_contractors c
            LEFT JOIN hr_staff_details h ON h.contractor_id = c.id
            WHERE h.contractor_id IS NULL
            """
        )
        conn.commit()
        return int(cur.rowcount or 0)
    except Exception as e:
        logger.warning("HR: could not backfill hr_staff_details shells: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        cur.close()
        conn.close()


def hr_compliance_overview() -> Dict[str, Any]:
    """Counts for dashboard: staff in tb_contractors vs compliance evidence on hr_staff_details."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT COUNT(*) AS n FROM tb_contractors")
        total_contractors = (cur.fetchone() or {}).get("n") or 0
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM tb_contractors c
            WHERE LOWER(CAST(COALESCE(c.status,'') AS CHAR)) NOT IN ('inactive','0','false','off','disabled')
            """
        )
        active_contractors = (cur.fetchone() or {}).get("n") or 0
        # Shell rows (after backfill, should match total_contractors)
        cur.execute("SELECT COUNT(*) AS total FROM hr_staff_details")
        hr_shell_rows = (cur.fetchone() or {}).get("total") or 0
        # Meaningful data: at least one contact or compliance field filled
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM hr_staff_details
            WHERE (phone IS NOT NULL AND phone != '')
               OR (address_line1 IS NOT NULL AND address_line1 != '')
               OR date_of_birth IS NOT NULL
               OR (driving_licence_number IS NOT NULL AND driving_licence_number != '')
               OR driving_licence_expiry IS NOT NULL
               OR (driving_licence_document_path IS NOT NULL AND driving_licence_document_path != '')
               OR (right_to_work_type IS NOT NULL AND right_to_work_type != '')
               OR right_to_work_expiry IS NOT NULL
               OR (right_to_work_document_path IS NOT NULL AND right_to_work_document_path != '')
               OR (dbs_number IS NOT NULL AND dbs_number != '')
               OR dbs_expiry IS NOT NULL
               OR (dbs_document_path IS NOT NULL AND dbs_document_path != '')
               OR (contract_type IS NOT NULL AND contract_type != '')
               OR contract_start IS NOT NULL OR contract_end IS NOT NULL
               OR (contract_document_path IS NOT NULL AND contract_document_path != '')
            """
        )
        with_any_hr_data = (cur.fetchone() or {}).get("n") or 0
        cur.execute(
            "SELECT COUNT(DISTINCT contractor_id) AS n FROM hr_staff_details WHERE right_to_work_expiry IS NOT NULL AND right_to_work_expiry >= CURDATE()"
        )
        with_rtw = (cur.fetchone() or {}).get("n") or 0
        cur.execute(
            "SELECT COUNT(DISTINCT contractor_id) AS n FROM hr_staff_details WHERE dbs_expiry IS NOT NULL AND dbs_expiry >= CURDATE()"
        )
        with_dbs = (cur.fetchone() or {}).get("n") or 0
        cur.execute(
            "SELECT COUNT(DISTINCT contractor_id) AS n FROM hr_staff_details WHERE contract_end IS NOT NULL AND contract_end >= CURDATE()"
        )
        with_contract = (cur.fetchone() or {}).get("n") or 0
        cur.execute(
            """
            SELECT COUNT(DISTINCT contractor_id) AS n FROM hr_staff_details
            WHERE (driving_licence_expiry IS NOT NULL AND driving_licence_expiry < CURDATE())
               OR (right_to_work_expiry IS NOT NULL AND right_to_work_expiry < CURDATE())
               OR (dbs_expiry IS NOT NULL AND dbs_expiry < CURDATE())
               OR (contract_end IS NOT NULL AND contract_end < CURDATE())
            """
        )
        contractors_with_expired_compliance = (
            cur.fetchone() or {}).get("n") or 0
        return {
            "total_contractors": total_contractors,
            "active_contractors": active_contractors,
            "hr_shell_rows": hr_shell_rows,
            "with_any_hr_data": with_any_hr_data,
            "with_right_to_work": with_rtw,
            "with_dbs": with_dbs,
            "with_contract": with_contract,
            "contractors_with_expired_compliance": contractors_with_expired_compliance,
            # backwards compat for templates that still reference this key
            "staff_with_hr_record": hr_shell_rows,
        }
    finally:
        cur.close()
        conn.close()


def _employee_docs_has_category(documents: List[Dict[str, Any]], category: str) -> bool:
    c = (category or "").strip().lower()
    for d in documents or []:
        if str(d.get("category") or "").strip().lower() == c:
            return True
    return False


def admin_profile_compliance_gaps(
    profile: Dict[str, Any], employee_documents: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    Items still missing for a typical compliance view. Each gap can map to a document request type and/or upload category.

    Counts an **approved** document request with at least one staff upload for that request type (workflow) as satisfied,
    as well as profile fields and document-library categories.
    """
    docs = employee_documents or []
    gaps: List[Dict[str, Any]] = []
    cid = int(profile.get("id") or 0)
    approved_with_upload: Set[str] = contractor_approved_request_types_with_upload(
        cid) if cid else set()

    def _nonempty(x: Any) -> bool:
        return x is not None and str(x).strip() != ""

    pic_ok = (
        _nonempty(profile.get("profile_picture_path"))
        or "profile_picture" in approved_with_upload
        or _employee_docs_has_category(docs, "profile_picture")
    )
    if not pic_ok:
        gaps.append(
            {
                "key": "profile_picture",
                "title": "Profile photo",
                "hint": "Request a clear head-and-shoulders photo, upload one in the library (Profile photo), or approve a submitted request — it will show in the employee directory and portal.",
                "request_type": "profile_picture",
                "upload_category": "profile_picture",
            }
        )

    rtw_ok = (
        _nonempty(profile.get("right_to_work_document_path"))
        or profile.get("right_to_work_expiry") is not None
        or _employee_docs_has_category(docs, "right_to_work")
        or "right_to_work" in approved_with_upload
    )
    if not rtw_ok:
        gaps.append(
            {
                "key": "right_to_work",
                "title": "Right to work",
                "hint": "Add an expiry date or linked file on the profile, upload to the library, request a document from the employee, or approve their uploaded file on the request.",
                "request_type": "right_to_work",
                "upload_category": "right_to_work",
            }
        )

    dl_ok = (
        _nonempty(profile.get("driving_licence_document_path"))
        or (
            _nonempty(profile.get("driving_licence_number"))
            and profile.get("driving_licence_expiry") is not None
        )
        or _employee_docs_has_category(docs, "driving_licence")
        or "driving_licence" in approved_with_upload
    )
    if not dl_ok:
        gaps.append(
            {
                "key": "driving_licence",
                "title": "Driving licence",
                "hint": "Add licence details and expiry, a linked file, a library document, request from the employee, or approve their upload on the request.",
                "request_type": "driving_licence",
                "upload_category": "driving_licence",
            }
        )

    dbs_ok = (
        _nonempty(profile.get("dbs_document_path"))
        or (profile.get("dbs_expiry") is not None and _nonempty(profile.get("dbs_number")))
        or _employee_docs_has_category(docs, "dbs")
        or "dbs" in approved_with_upload
    )
    if not dbs_ok:
        gaps.append(
            {
                "key": "dbs",
                "title": "DBS",
                "hint": "Add DBS number, expiry, a linked file, a library document, request from the employee, or approve their upload on the request.",
                "request_type": "dbs",
                "upload_category": "dbs",
            }
        )

    contract_ok = (
        _nonempty(profile.get("contract_document_path"))
        or profile.get("contract_start") is not None
        or profile.get("contract_end") is not None
        or _nonempty(profile.get("contract_type"))
        or _employee_docs_has_category(docs, "contract")
        or "contract" in approved_with_upload
    )
    if not contract_ok:
        gaps.append(
            {
                "key": "contract",
                "title": "Contract",
                "hint": "Add contract type/dates, a linked file, a library document, request from the employee, or approve their upload on the request.",
                "request_type": "contract",
                "upload_category": "contract",
            }
        )

    return gaps


def pending_requests_count(contractor_id: int) -> int:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM hr_document_requests
            WHERE contractor_id = %s AND status IN ('pending', 'rejected', 'overdue')
            """,
            (contractor_id,),
        )
        return cur.fetchone()[0] or 0
    finally:
        cur.close()
        conn.close()


# -----------------------------------------------------------------------------
# Employee directory & stored documents (hr_employee_documents)
# -----------------------------------------------------------------------------


def list_employee_documents(contractor_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'hr_employee_documents'")
        if not cur.fetchone():
            return []
        cur.execute(
            """
            SELECT id, contractor_id, category, title, file_path, file_name, notes, uploaded_by_user_id, created_at
            FROM hr_employee_documents WHERE contractor_id = %s ORDER BY created_at DESC
            """,
            (contractor_id,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def get_employee_document(doc_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'hr_employee_documents'")
        if not cur.fetchone():
            return None
        cur.execute(
            "SELECT id, contractor_id, category, title, file_path, file_name, notes, created_at FROM hr_employee_documents WHERE id = %s",
            (doc_id,),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def add_employee_document(
    contractor_id: int,
    category: str,
    title: str,
    file_path: str,
    file_name: Optional[str] = None,
    notes: Optional[str] = None,
    uploaded_by_user_id: Optional[str] = None,
) -> Optional[int]:
    cat = (category or "general").strip()[:64] if category else "general"
    if cat not in EMPLOYEE_DOCUMENT_CATEGORIES:
        cat = "general"
    ttl = (title or "").strip()[:255] or "Document"
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'hr_employee_documents'")
        if not cur.fetchone():
            return None
        cur.execute(
            """
            INSERT INTO hr_employee_documents (contractor_id, category, title, file_path, file_name, notes, uploaded_by_user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                contractor_id,
                cat,
                ttl,
                file_path,
                (file_name or "")[:255] or None,
                (notes or "").strip()[:65535] or None,
                uploaded_by_user_id,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        cur.close()
        conn.close()


def delete_employee_document_and_file(doc_id: int, static_root: str) -> bool:
    """Remove DB row and file under static_root if path is uploads/hr_employee/..."""
    import os

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'hr_employee_documents'")
        if not cur.fetchone():
            return False
        cur.execute(
            "SELECT file_path FROM hr_employee_documents WHERE id = %s", (doc_id,))
        row = cur.fetchone()
        if not row:
            return False
        rel = (row.get("file_path") or "").replace("\\", "/").lstrip("/")
        cur2 = conn.cursor()
        cur2.execute(
            "DELETE FROM hr_employee_documents WHERE id = %s", (doc_id,))
        ok = cur2.rowcount > 0
        conn.commit()
        cur2.close()
        if ok and rel.startswith("uploads/hr_employee/"):
            full = os.path.join(static_root, rel)
            try:
                if os.path.isfile(full):
                    os.remove(full)
            except OSError:
                pass
        return ok
    finally:
        cur.close()
        conn.close()


def admin_list_employees(
    q: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 300,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """Directory list: all contractors with optional HR join (phone, DOB)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["1=1"]
        params: List[Any] = []
        if status is None:
            st = "all"
        else:
            st = (status or "active").strip().lower()
        if st == "inactive":
            where.append("LOWER(CAST(c.status AS CHAR)) = 'inactive'")
        elif st != "all":
            where.append("LOWER(CAST(c.status AS CHAR)) IN ('active','1')")
        if q and q.strip():
            term = "%" + q.strip() + "%"
            where.append("(c.name LIKE %s OR c.email LIKE %s)")
            params.extend([term, term])
        lim = max(1, min(int(limit or 300), 500))
        off = max(0, int(offset or 0))
        params.extend([lim, off])
        try:
            cur.execute(
                f"""
                SELECT c.id, c.name, c.email, c.initials, c.status, c.profile_picture_path,
                       h.phone, h.date_of_birth, h.job_title, h.department,
                       c.role_id AS tb_role_id, r.name AS tb_role_name
                FROM tb_contractors c
                LEFT JOIN hr_staff_details h ON h.contractor_id = c.id
                LEFT JOIN roles r ON r.id = c.role_id
                WHERE {" AND ".join(where)}
                ORDER BY c.name
                LIMIT %s OFFSET %s
                """,
                params,
            )
        except Exception:
            try:
                cur.execute(
                    f"""
                    SELECT c.id, c.name, c.email, c.initials, c.status, c.profile_picture_path,
                           h.phone, h.date_of_birth,
                           c.role_id AS tb_role_id, r.name AS tb_role_name
                    FROM tb_contractors c
                    LEFT JOIN hr_staff_details h ON h.contractor_id = c.id
                    LEFT JOIN roles r ON r.id = c.role_id
                    WHERE {" AND ".join(where)}
                    ORDER BY c.name
                    LIMIT %s OFFSET %s
                    """,
                    params,
                )
            except Exception:
                try:
                    cur.execute(
                        f"""
                        SELECT c.id, c.name, c.email, c.initials, c.status,
                               h.phone, h.date_of_birth
                        FROM tb_contractors c
                        LEFT JOIN hr_staff_details h ON h.contractor_id = c.id
                        WHERE {" AND ".join(where)}
                        ORDER BY c.name
                        LIMIT %s OFFSET %s
                        """,
                        params,
                    )
                except Exception:
                    cur.execute(
                        f"""
                        SELECT c.id, c.name, c.email, c.initials, c.status, h.phone
                        FROM tb_contractors c
                        LEFT JOIN hr_staff_details h ON h.contractor_id = c.id
                        WHERE {" AND ".join(where)}
                        ORDER BY c.name
                        LIMIT %s OFFSET %s
                        """,
                        params,
                    )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_upcoming_birthdays(within_days: int = 60) -> List[Dict[str, Any]]:
    """Staff with date_of_birth in the next within_days (calendar wrap)."""
    from datetime import timedelta

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        try:
            cur.execute(
                """
                SELECT c.id, c.name, c.email, h.date_of_birth
                FROM tb_contractors c
                INNER JOIN hr_staff_details h ON h.contractor_id = c.id
                WHERE h.date_of_birth IS NOT NULL AND COALESCE(c.status,'') = 'active'
                ORDER BY c.name
                """
            )
        except Exception:
            return []
        rows = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    today = date.today()
    end = today + timedelta(days=max(1, min(within_days, 366)))
    out: List[Dict[str, Any]] = []

    def next_occurrence(dob: date) -> Optional[date]:
        y = today.year
        try:
            occ = date(y, dob.month, dob.day)
        except ValueError:
            occ = date(y, dob.month, 28)
        if occ < today:
            try:
                occ = date(y + 1, dob.month, dob.day)
            except ValueError:
                occ = date(y + 1, dob.month, 28)
        return occ

    for r in rows:
        dob = r.get("date_of_birth")
        if not dob:
            continue
        if hasattr(dob, "date"):
            dob = dob.date()
        occ = next_occurrence(dob)
        if occ and occ <= end:
            r["next_birthday"] = occ
            out.append(r)
    out.sort(key=lambda x: x.get("next_birthday") or date.max)
    return out[:25]
