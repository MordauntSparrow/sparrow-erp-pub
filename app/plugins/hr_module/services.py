import json
import logging
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from app.objects import get_db_connection

logger = logging.getLogger(__name__)


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
        logger.warning("HR: could not upsert employee-portal todo for request %s: %s", request_id, e)


def _hr_ep_complete_request_todo(request_id: int) -> None:
    try:
        from app.plugins.employee_portal_module.services import complete_todo_by_reference

        complete_todo_by_reference(_EP_HR_SOURCE, _EP_HR_REF_TYPE, str(int(request_id)))
    except Exception as e:
        logger.warning("HR: could not complete employee-portal todo for request %s: %s", request_id, e)


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
        logger.warning("HR: could not send approval message for request %s: %s", request_id, e)


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
        logger.warning("HR: could not send rejection message for request %s: %s", request_id, e)

# Request types for document requests
REQUEST_TYPES = ["right_to_work", "driving_licence", "dbs", "contract", "profile_picture", "other"]

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
    parts = [p for p in str(code).strip().lower().replace("-", "_").split("_") if p]
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


def admin_get_staff_profile(contractor_id: int) -> Optional[Dict[str, Any]]:
    """Full HR profile for admin: core + all staff_details columns."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        try:
            cur.execute(
                """
                SELECT id, name, email, initials, status, profile_picture_path,
                       COALESCE(employment_type, 'self_employed') AS employment_type
                FROM tb_contractors WHERE id = %s
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
        try:
            cur.execute(
                """
                SELECT h.phone, h.address_line1, h.address_line2, h.postcode, h.emergency_contact_name, h.emergency_contact_phone,
                       h.date_of_birth,
                       h.driving_licence_number, h.driving_licence_expiry, h.driving_licence_document_path,
                       h.right_to_work_type, h.right_to_work_expiry, h.right_to_work_document_path,
                       h.dbs_level, h.dbs_number, h.dbs_expiry, h.dbs_document_path,
                       h.contract_type, h.contract_start, h.contract_end, h.contract_document_path, h.updated_at,
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
        return row
    finally:
        cur.close()
        conn.close()


def admin_update_staff_profile(contractor_id: int, data: Dict[str, Any]) -> bool:
    """Update all admin-editable HR fields. Returns True if contractor exists."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM tb_contractors WHERE id = %s", (contractor_id,))
        if not cur.fetchone():
            return False
        dl_path = _normalize_stored_relative_upload_path(data.get("driving_licence_document_path"))
        rtw_path = _normalize_stored_relative_upload_path(data.get("right_to_work_document_path"))
        dbs_path = _normalize_stored_relative_upload_path(data.get("dbs_document_path"))
        contract_path = _normalize_stored_relative_upload_path(data.get("contract_document_path"))
        job_title = _truncate_str(data.get("job_title"), 128)
        department = _truncate_str(data.get("department"), 128)
        manager_id = _optional_manager_contractor_id(data.get("manager_contractor_id"), contractor_id)
        if manager_id is not None:
            cur.execute("SELECT id FROM tb_contractors WHERE id = %s", (manager_id,))
            if not cur.fetchone():
                manager_id = None
        cur.execute("""
            INSERT INTO hr_staff_details (
                contractor_id, phone, address_line1, address_line2, postcode,
                emergency_contact_name, emergency_contact_phone, date_of_birth,
                driving_licence_number, driving_licence_expiry, driving_licence_document_path,
                right_to_work_type, right_to_work_expiry, right_to_work_document_path,
                dbs_level, dbs_number, dbs_expiry, dbs_document_path,
                contract_type, contract_start, contract_end, contract_document_path,
                job_title, department, manager_contractor_id
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
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
            job_title = VALUES(job_title), department = VALUES(department), manager_contractor_id = VALUES(manager_contractor_id)
        """, (
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
        ))
        conn.commit()
        return True
    except Exception:
        return False
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
        cur.execute("SELECT id FROM tb_contractors WHERE id = %s", (contractor_id,))
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
            _hr_ep_upsert_request_todo(cid, rid, ttl, rejected=False, due_date=required_by_date)
        return count
    finally:
        cur.close()
        conn.close()


def _contractors_has_profile_picture_column(cur) -> bool:
    try:
        cur.execute("SHOW COLUMNS FROM tb_contractors LIKE 'profile_picture_path'")
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
            req_path = _normalize_stored_relative_upload_path(raw) if raw else None
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
                lib_path = _normalize_stored_relative_upload_path(raw) if raw else None
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
                "UPDATE hr_staff_details SET `{}` = %s WHERE contractor_id = %s".format(col),
                (path, cid),
            )
        _reconcile_profile_picture_to_contractor(cur, cid)
        conn.commit()
    except Exception as e:
        logger.warning("HR: reconcile_staff_details_from_approved_requests(%s): %s", cid, e)
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
        req_title = (meta.get("title") or "Document request").strip() or "Document request"
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
        req_title = (meta.get("title") or "Document request").strip() or "Document request"
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
        _hr_ep_message_rejected(contractor_id, request_id, req_title, admin_notes)
    return ok


def get_expiring_documents(days: int = 30) -> List[Dict[str, Any]]:
    """List staff with documents expiring within the next N days (licence, right to work, DBS, contract end)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        from datetime import timedelta
        today = date.today()
        end = today + timedelta(days=days)
        cur.execute("""
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
        """, (today, end, today, end, today, end, today, end))
        rows = cur.fetchall() or []
        # Attach names
        for r in rows:
            cur.execute("SELECT name, email FROM tb_contractors WHERE id = %s", (r["id"],))
            c = cur.fetchone()
            r["contractor_name"] = c.get("name") if c else ""
            r["contractor_email"] = c.get("email") if c else ""
        return rows
    except Exception:
        return []
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
        return {
            "total_contractors": total_contractors,
            "active_contractors": active_contractors,
            "hr_shell_rows": hr_shell_rows,
            "with_any_hr_data": with_any_hr_data,
            "with_right_to_work": with_rtw,
            "with_dbs": with_dbs,
            "with_contract": with_contract,
            # backwards compat for templates that still reference this key
            "staff_with_hr_record": hr_shell_rows,
        }
    finally:
        cur.close()
        conn.close()


def get_ventus_crew_profiles_for_contractor(contractor_id: int) -> List[Dict[str, Any]]:
    """Linked Ventus CAD crew profiles (skills / qualifications), keyed by contractor_id when set."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    rows: List[Dict[str, Any]] = []
    try:
        cur.execute("SHOW TABLES LIKE 'mdt_crew_profiles'")
        if not cur.fetchone():
            return []
        cur.execute(
            """
            SELECT username, contractor_id, gender, skills_json, qualifications_json, profile_picture_path, updated_at
            FROM mdt_crew_profiles
            WHERE contractor_id = %s
            ORDER BY username
            """,
            (int(contractor_id),),
        )
        rows = cur.fetchall() or []
    except Exception as e:
        logger.debug("HR: Ventus crew profile lookup skipped: %s", e)
        return []
    finally:
        cur.close()
        conn.close()

    out: List[Dict[str, Any]] = []
    for r in rows:
        skills: List[Any] = []
        quals: List[Any] = []
        try:
            sj = r.get("skills_json")
            if sj:
                skills = json.loads(sj) if isinstance(sj, str) else (sj or [])
            qj = r.get("qualifications_json")
            if qj:
                quals = json.loads(qj) if isinstance(qj, str) else (qj or [])
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
        out.append(
            {
                "username": r.get("username"),
                "contractor_id": r.get("contractor_id"),
                "gender": r.get("gender"),
                "skills": skills if isinstance(skills, list) else [],
                "qualifications": quals if isinstance(quals, list) else [],
                "profile_picture_path": r.get("profile_picture_path"),
                "updated_at": r.get("updated_at"),
            }
        )
    return out


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
    approved_with_upload: Set[str] = contractor_approved_request_types_with_upload(cid) if cid else set()

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
        cur.execute("SELECT file_path FROM hr_employee_documents WHERE id = %s", (doc_id,))
        row = cur.fetchone()
        if not row:
            return False
        rel = (row.get("file_path") or "").replace("\\", "/").lstrip("/")
        cur2 = conn.cursor()
        cur2.execute("DELETE FROM hr_employee_documents WHERE id = %s", (doc_id,))
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
                       h.phone, h.date_of_birth, h.job_title, h.department
                FROM tb_contractors c
                LEFT JOIN hr_staff_details h ON h.contractor_id = c.id
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
