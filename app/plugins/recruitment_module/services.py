"""
Recruitment: job roles, openings, applicants, applications, form tasks, hire → contractor.
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
import shutil
import string
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from app.objects import AuthManager, get_db_connection

logger = logging.getLogger(__name__)

STAGES_ORDER = ["applied", "screening", "interview", "offer", "hired", "rejected"]
STAGE_LABELS = {
    "applied": "Applied",
    "screening": "Screening",
    "interview": "Interview",
    "offer": "Offer",
    "hired": "Hired",
    "rejected": "Rejected",
}


def recruitment_applicant_retention_days() -> int:
    """Days after hire/reject before recruitment-only PII may be purged (default 60)."""
    try:
        return max(30, int(os.environ.get("RECRUITMENT_APPLICANT_RETENTION_DAYS", "60")))
    except (TypeError, ValueError):
        return 60


# Pre-hire document types (align with HR where possible). policy_ack = confirm policies without a file.
PREHIRE_REQUEST_TYPES = (
    "right_to_work",
    "driving_licence",
    "dbs",
    "contract",
    "policy_ack",
    "profile_picture",
    "other",
)

PREHIRE_REQUEST_TYPE_LABELS = {
    "right_to_work": "Right to work",
    "driving_licence": "Driving licence",
    "dbs": "DBS",
    "contract": "Contract / offer letter",
    "policy_ack": "Policies — read & confirm",
    "profile_picture": "Profile photo",
    "other": "Other document",
}

PREHIRE_DOC_STATUSES = ("pending", "uploaded", "approved", "rejected", "overdue")


def _recruitment_static_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "static"))


def _normalize_upload_rel(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    s = str(path).strip().replace("\\", "/").lstrip("/")
    if not s or ".." in s.split("/"):
        return None
    if s.startswith("static/"):
        s = s[7:].lstrip("/")
    return s


def _safe_delete_static_file(static_root: str, rel: Optional[str]) -> None:
    rel = _normalize_upload_rel(rel)
    if not rel or not rel.startswith("uploads/"):
        return
    full = os.path.abspath(os.path.join(static_root, *rel.split("/")))
    try:
        if os.path.commonpath([full, os.path.abspath(static_root)]) != os.path.abspath(static_root):
            return
    except ValueError:
        return
    try:
        if os.path.isfile(full):
            os.remove(full)
    except OSError as e:
        logger.warning("Recruitment: could not delete file %s: %s", full, e)


def _copy_upload_to_hr_folder(static_root: str, src_rel: str, dest_subdir: str = "hr_documents") -> Optional[str]:
    """Copy a file from static-relative src to uploads/hr_documents/… Returns new relative path."""
    rel = _normalize_upload_rel(src_rel)
    if not rel or not rel.startswith("uploads/"):
        return None
    src_full = os.path.abspath(os.path.join(static_root, *rel.split("/")))
    try:
        if os.path.commonpath([src_full, os.path.abspath(static_root)]) != os.path.abspath(static_root):
            return None
    except ValueError:
        return None
    if not os.path.isfile(src_full):
        return None
    ext = os.path.splitext(src_full)[1].lower() or ".bin"
    safe_ext = ext if len(ext) <= 10 else ".bin"
    dest_name = f"{uuid.uuid4().hex}{safe_ext}"
    dest_rel = f"uploads/{dest_subdir}/{dest_name}".replace("\\", "/")
    dest_full = os.path.join(static_root, *dest_rel.split("/"))
    os.makedirs(os.path.dirname(dest_full), exist_ok=True)
    try:
        shutil.copy2(src_full, dest_full)
    except OSError as e:
        logger.error("Recruitment hire copy failed %s -> %s: %s", src_full, dest_full, e)
        return None
    return dest_rel


def list_prehire_requests_admin(application_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT r.*,
                   (SELECT u.file_path FROM rec_prehire_document_uploads u
                    WHERE u.request_id = r.id ORDER BY u.uploaded_at DESC LIMIT 1) AS latest_file_path,
                   (SELECT u.file_name FROM rec_prehire_document_uploads u
                    WHERE u.request_id = r.id ORDER BY u.uploaded_at DESC LIMIT 1) AS latest_file_name
            FROM rec_prehire_document_requests r
            WHERE r.application_id = %s
            ORDER BY r.created_at ASC, r.id ASC
            """,
            (application_id,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def list_prehire_requests_for_applicant(application_id: int, applicant_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id FROM rec_applications WHERE id = %s AND applicant_id = %s LIMIT 1",
            (application_id, applicant_id),
        )
        if not cur.fetchone():
            return []
        cur.execute(
            """
            SELECT r.*,
                   (SELECT u.file_path FROM rec_prehire_document_uploads u
                    WHERE u.request_id = r.id ORDER BY u.uploaded_at DESC LIMIT 1) AS latest_file_path,
                   (SELECT u.file_name FROM rec_prehire_document_uploads u
                    WHERE u.request_id = r.id ORDER BY u.uploaded_at DESC LIMIT 1) AS latest_file_name
            FROM rec_prehire_document_requests r
            WHERE r.application_id = %s
            ORDER BY r.created_at ASC, r.id ASC
            """,
            (application_id,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_create_prehire_request(
    application_id: int,
    title: str,
    description: Optional[str],
    request_type: str,
    required_by_date: Optional[date] = None,
) -> Tuple[bool, str]:
    title = (title or "").strip()
    if not title:
        return False, "Title required"
    rt = (request_type or "other").strip().lower()
    if rt not in PREHIRE_REQUEST_TYPES:
        rt = "other"
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, contractor_id FROM rec_applications WHERE id = %s LIMIT 1",
            (application_id,),
        )
        ar = cur.fetchone()
        if not ar:
            return False, "Application not found"
        cid = ar[1] if isinstance(ar, (list, tuple)) else ar.get("contractor_id")
        if cid:
            return False, "Application already hired — use HR for further documents"
        cur.execute(
            """
            INSERT INTO rec_prehire_document_requests
            (application_id, title, description, request_type, required_by_date, status)
            VALUES (%s, %s, %s, %s, %s, 'pending')
            """,
            (application_id, title, (description or "")[:65535] or None, rt, required_by_date),
        )
        conn.commit()
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def admin_prehire_approve(request_id: int, application_id: int, admin_notes: Optional[str] = None) -> Tuple[bool, str]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        note = (admin_notes or "").strip() or None
        if note:
            cur.execute(
                """
                UPDATE rec_prehire_document_requests
                SET status = 'approved', admin_notes = %s
                WHERE id = %s AND application_id = %s AND status = 'uploaded'
                """,
                (note, request_id, application_id),
            )
        else:
            cur.execute(
                """
                UPDATE rec_prehire_document_requests
                SET status = 'approved'
                WHERE id = %s AND application_id = %s AND status = 'uploaded'
                """,
                (request_id, application_id),
            )
        conn.commit()
        if cur.rowcount < 1:
            return False, "Request not found or not in a state HR can approve"
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def admin_prehire_reject(
    request_id: int, application_id: int, admin_notes: Optional[str] = None
) -> Tuple[bool, str]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE rec_prehire_document_requests
            SET status = 'rejected', admin_notes = %s
            WHERE id = %s AND application_id = %s
            """,
            ((admin_notes or "").strip() or None, request_id, application_id),
        )
        conn.commit()
        if cur.rowcount < 1:
            return False, "Request not found"
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def admin_authorize_hr_conversion(application_id: int, user_id: Optional[str]) -> Tuple[bool, str]:
    """HR confirms applicant may be converted to contractor (after documents reviewed)."""
    ok_docs, msg_docs = prehire_all_approved_or_empty(application_id)
    if not ok_docs:
        return False, msg_docs
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE rec_applications
            SET hr_conversion_authorized = 1,
                hr_conversion_authorized_at = NOW(),
                hr_conversion_authorized_by_user_id = %s
            WHERE id = %s AND contractor_id IS NULL
            """,
            ((str(user_id).strip()[:36] if user_id else None), application_id),
        )
        conn.commit()
        if cur.rowcount < 1:
            return False, "Application not found or already hired"
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def prehire_all_approved_or_empty(application_id: int) -> Tuple[bool, str]:
    """Every pre-hire row must be approved (if there are none, OK)."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) FROM rec_prehire_document_requests WHERE application_id = %s",
            (application_id,),
        )
        n = cur.fetchone()
        total = int(n[0] if n else 0)
        if total == 0:
            return True, "ok"
        cur.execute(
            """
            SELECT COUNT(*) FROM rec_prehire_document_requests
            WHERE application_id = %s AND status <> 'approved'
            """,
            (application_id,),
        )
        n2 = cur.fetchone()
        bad = int(n2[0] if n2 else 0)
        if bad:
            return False, "All pre-hire document requests must be HR-approved first."
        return True, "ok"
    finally:
        cur.close()
        conn.close()


def hire_precheck(application_id: int) -> Tuple[bool, str]:
    """Block hire until HR authorization, policies acknowledged, and pre-hire docs approved."""
    app = admin_get_application(application_id)
    if not app:
        return False, "Application not found"
    if app.get("contractor_id"):
        return False, "Already hired"
    if not app.get("hr_conversion_authorized"):
        return (
            False,
            "HR has not authorized contractor creation. Approve each pre-hire document, then use “Authorize hire”.",
        )
    if not app.get("policies_acknowledged_at"):
        return False, "Applicant must confirm policies on their portal before hire."
    ok, msg = prehire_all_approved_or_empty(application_id)
    if not ok:
        return False, msg
    return True, "ok"


def applicant_set_policies_acknowledged(application_id: int, applicant_id: int) -> Tuple[bool, str]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE rec_applications SET policies_acknowledged_at = NOW()
            WHERE id = %s AND applicant_id = %s AND contractor_id IS NULL
            """,
            (application_id, applicant_id),
        )
        conn.commit()
        if cur.rowcount < 1:
            return False, "Could not update application"
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def applicant_save_prehire_upload(
    application_id: int,
    applicant_id: int,
    request_id: int,
    stored_relative_path: str,
    original_filename: Optional[str],
) -> Tuple[bool, str]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT r.id, r.application_id, r.status, r.request_type
            FROM rec_prehire_document_requests r
            JOIN rec_applications a ON a.id = r.application_id
            WHERE r.id = %s AND r.application_id = %s AND a.applicant_id = %s
            LIMIT 1
            """,
            (request_id, application_id, applicant_id),
        )
        row = cur.fetchone()
        if not row:
            return False, "Request not found"
        if (row.get("request_type") or "").lower() == "policy_ack":
            return False, "Use the confirm button for policy acknowledgement"
        if row.get("status") == "approved":
            return False, "This request is already approved"
        cur2 = conn.cursor()
        cur2.execute("DELETE FROM rec_prehire_document_uploads WHERE request_id = %s", (request_id,))
        cur2.execute(
            """
            INSERT INTO rec_prehire_document_uploads (request_id, file_path, file_name, document_type)
            VALUES (%s, %s, %s, 'primary')
            """,
            (request_id, stored_relative_path, (original_filename or "")[:255] or None),
        )
        cur2.execute(
            """
            UPDATE rec_prehire_document_requests
            SET status = 'uploaded'
            WHERE id = %s AND status IN ('pending', 'rejected', 'uploaded')
            """,
            (request_id,),
        )
        conn.commit()
        cur2.close()
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def applicant_confirm_prehire_policy_ack(
    application_id: int, applicant_id: int, request_id: int
) -> Tuple[bool, str]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT r.id, r.request_type, r.status
            FROM rec_prehire_document_requests r
            JOIN rec_applications a ON a.id = r.application_id
            WHERE r.id = %s AND r.application_id = %s AND a.applicant_id = %s
            LIMIT 1
            """,
            (request_id, application_id, applicant_id),
        )
        row = cur.fetchone()
        if not row:
            return False, "Request not found"
        if (row.get("request_type") or "").lower() != "policy_ack":
            return False, "Not a policy acknowledgement request"
        if row.get("status") == "approved":
            return False, "Already approved"
        cur2 = conn.cursor()
        cur2.execute("DELETE FROM rec_prehire_document_uploads WHERE request_id = %s", (request_id,))
        cur2.execute(
            """
            UPDATE rec_prehire_document_requests
            SET status = 'uploaded', admin_notes = NULL
            WHERE id = %s
            """,
            (request_id,),
        )
        conn.commit()
        cur2.close()
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def schedule_recruitment_purge_eligible(application_id: int) -> None:
    """Set date when recruitment-only records may be purged (employee copy retained in HR)."""
    eligible = date.today() + timedelta(days=recruitment_applicant_retention_days())
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE rec_applications
            SET recruitment_data_purge_eligible_at = %s
            WHERE id = %s
            """,
            (eligible, application_id),
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()


def _gather_application_upload_paths(app_row: Dict[str, Any], tasks: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    cv = _normalize_upload_rel(app_row.get("cv_path"))
    if cv:
        out.append(cv)
    for t in tasks:
        raw = t.get("response_json")
        data = _json_loads_maybe(raw) if not isinstance(raw, dict) else raw
        if isinstance(data, dict):
            for _k, v in data.items():
                if isinstance(v, str) and v.startswith("uploads/"):
                    nv = _normalize_upload_rel(v)
                    if nv:
                        out.append(nv)
    return list(dict.fromkeys(out))


def _prehire_upload_paths_for_application(application_id: int) -> List[str]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT u.file_path
            FROM rec_prehire_document_uploads u
            JOIN rec_prehire_document_requests r ON r.id = u.request_id
            WHERE r.application_id = %s
            """,
            (application_id,),
        )
        rows = cur.fetchall() or []
        paths = []
        for row in rows:
            p = row[0] if isinstance(row, (list, tuple)) else row.get("file_path")
            np = _normalize_upload_rel(p)
            if np:
                paths.append(np)
        return list(dict.fromkeys(paths))
    finally:
        cur.close()
        conn.close()


def run_recruitment_data_retention_purge(static_root: Optional[str] = None) -> Dict[str, int]:
    """
    Delete recruitment application rows whose purge date has passed; remove applicant portal files;
    anonymize rec_applicants when they have no remaining applications.
    Does not delete tb_contractors or HR records.
    """
    root = static_root or _recruitment_static_root()
    deleted_apps = 0
    anonymized = 0
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT a.*, a.applicant_id AS _applicant_id
            FROM rec_applications a
            WHERE a.recruitment_data_purge_eligible_at IS NOT NULL
              AND a.recruitment_data_purge_eligible_at <= CURDATE()
            """
        )
        rows = cur.fetchall() or []
        for app_row in rows:
            app_id = int(app_row["id"])
            applicant_id = int(app_row["_applicant_id"])
            cur2 = conn.cursor(dictionary=True)
            cur2.execute(
                "SELECT * FROM rec_application_tasks WHERE application_id = %s",
                (app_id,),
            )
            tasks = cur2.fetchall() or []
            cur2.close()
            paths = _gather_application_upload_paths(app_row, tasks)
            paths.extend(_prehire_upload_paths_for_application(app_id))
            for rel in dict.fromkeys(paths):
                _safe_delete_static_file(root, rel)
            cur.execute("DELETE FROM rec_applications WHERE id = %s", (app_id,))
            conn.commit()
            deleted_apps += 1
            cur.execute(
                "SELECT COUNT(*) AS c FROM rec_applications WHERE applicant_id = %s",
                (applicant_id,),
            )
            cnt_row = cur.fetchone()
            remaining = int((cnt_row or {}).get("c", 0) or 0)
            if remaining == 0:
                purged_email = f"purged_{applicant_id}_{uuid.uuid4().hex[:10]}@invalid.local"
                cur.execute(
                    """
                    UPDATE rec_applicants
                    SET email = %s, name = 'Deleted applicant', phone = NULL, password_hash = NULL
                    WHERE id = %s
                    """,
                    (purged_email[:255], applicant_id),
                )
                conn.commit()
                anonymized += 1
        return {"applications_deleted": deleted_apps, "applicants_anonymized": anonymized}
    except Exception as e:
        logger.exception("Recruitment retention purge failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return {"applications_deleted": deleted_apps, "applicants_anonymized": anonymized, "error": str(e)}
    finally:
        cur.close()
        conn.close()


def _copy_prehire_and_cv_to_hr(
    static_root: str,
    application_id: int,
    contractor_id: int,
    app_row: Dict[str, Any],
) -> None:
    """After contractor row exists: copy approved pre-hire files into HR storage and staff profile."""
    try:
        from app.plugins.hr_module.services import REQUEST_TYPES as HR_REQUEST_TYPES
        from app.plugins.hr_module.services import REQUEST_TYPE_TO_STAFF_DOCUMENT_COLUMN
    except Exception:
        HR_REQUEST_TYPES = (
            "right_to_work",
            "driving_licence",
            "dbs",
            "contract",
            "profile_picture",
            "other",
        )
        REQUEST_TYPE_TO_STAFF_DOCUMENT_COLUMN = {
            "right_to_work": "right_to_work_document_path",
            "driving_licence": "driving_licence_document_path",
            "dbs": "dbs_document_path",
            "contract": "contract_document_path",
        }

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    staff_updates: Dict[str, str] = {}
    try:
        cur.execute(
            """
            SELECT r.* FROM rec_prehire_document_requests r
            WHERE r.application_id = %s AND r.status = 'approved'
            ORDER BY r.id ASC
            """,
            (application_id,),
        )
        pre_rows = cur.fetchall() or []
        for pr in pre_rows:
            rid = int(pr["id"])
            req_type = (pr.get("request_type") or "other").lower()
            cur.execute(
                """
                SELECT file_path, file_name FROM rec_prehire_document_uploads
                WHERE request_id = %s ORDER BY uploaded_at DESC LIMIT 1
                """,
                (rid,),
            )
            upl = cur.fetchone()
            new_rel: Optional[str] = None
            if upl and upl.get("file_path"):
                new_rel = _copy_upload_to_hr_folder(static_root, upl["file_path"], "hr_documents")
            col = REQUEST_TYPE_TO_STAFF_DOCUMENT_COLUMN.get(req_type)
            if col and new_rel:
                staff_updates[col] = new_rel
            hr_rt = req_type if req_type in HR_REQUEST_TYPES else "other"
            try:
                cur2 = conn.cursor()
                cur2.execute(
                    """
                    INSERT INTO hr_document_requests (
                      contractor_id, title, description, required_by_date, status, request_type,
                      approved_at
                    ) VALUES (%s, %s, %s, NULL, 'approved', %s, NOW())
                    """,
                    (
                        contractor_id,
                        (pr.get("title") or "Document")[:255],
                        pr.get("description"),
                        hr_rt,
                    ),
                )
                hr_req_id = cur2.lastrowid
                if new_rel and hr_req_id:
                    fn = (upl.get("file_name") if upl else None) or os.path.basename(new_rel)
                    cur2.execute(
                        """
                        INSERT INTO hr_document_uploads (request_id, file_path, file_name, document_type)
                        VALUES (%s, %s, %s, 'primary')
                        """,
                        (hr_req_id, new_rel, fn[:255] if fn else None),
                    )
                cur2.close()
            except Exception as e:
                logger.warning("HR document request mirror failed: %s", e)

        cv = _normalize_upload_rel(app_row.get("cv_path"))
        if cv:
            cv_copy = _copy_upload_to_hr_folder(static_root, cv, "hr_documents")
            if cv_copy:
                try:
                    cur.execute(
                        """
                        INSERT INTO hr_employee_documents (contractor_id, category, title, file_path, file_name)
                        VALUES (%s, 'identification', 'CV (from recruitment)', %s, %s)
                        """,
                        (contractor_id, cv_copy, "cv-from-recruitment"),
                    )
                except Exception as e:
                    logger.warning("HR employee document CV insert failed: %s", e)

        phone = (app_row.get("phone") or "").strip() or None
        cur.execute(
            "INSERT INTO hr_staff_details (contractor_id, phone) VALUES (%s, %s) "
            "ON DUPLICATE KEY UPDATE phone = COALESCE(VALUES(phone), hr_staff_details.phone)",
            (contractor_id, phone),
        )
        for col, path in staff_updates.items():
            cur.execute(
                f"UPDATE hr_staff_details SET `{col}` = %s WHERE contractor_id = %s",
                (path, contractor_id),
            )
        conn.commit()
    except Exception as e:
        logger.exception("copy_prehire_to_hr: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()


def _slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-") or "item"


def _json_loads_maybe(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return None
    return None


def parse_form_schema(schema_json: Any) -> List[Dict[str, Any]]:
    data = _json_loads_maybe(schema_json)
    if not data:
        return []
    if isinstance(data, dict) and "fields" in data:
        return list(data.get("fields") or [])
    if isinstance(data, list):
        return data
    return []


def schema_to_json_string(schema: Any) -> str:
    if isinstance(schema, str):
        return schema
    return json.dumps(schema or {"fields": []})


# Allowed types must match applicant_task.html rendering and applicant_task_post handling
RECRUITMENT_BUILDER_FIELD_TYPES = (
    "text",
    "textarea",
    "email",
    "tel",
    "url",
    "search",
    "number",
    "date",
    "time",
    "datetime",
    "month",
    "week",
    "color",
    "range",
    "checkbox",
    "select",
    "radio",
    "multiselect",
    "file",
)

RECRUITMENT_BUILDER_TYPE_LABELS = {
    "text": "Short text (one line)",
    "textarea": "Long text (paragraph)",
    "email": "Email address",
    "tel": "Phone number",
    "url": "Web link (URL)",
    "search": "Search box (single line)",
    "number": "Number",
    "date": "Date",
    "time": "Time",
    "datetime": "Date & time",
    "month": "Month",
    "week": "Week",
    "color": "Colour picker",
    "range": "Slider (0–100)",
    "checkbox": "Single tick box (yes/no)",
    "select": "Dropdown (choose one)",
    "radio": "Multiple choice (one option)",
    "multiselect": "List — pick one or more",
    "file": "File upload (PDF, Word, or image)",
}

# Types that need a list of choices (one per line in the builder)
RECRUITMENT_TYPES_WITH_OPTIONS = frozenset({"select", "radio", "multiselect"})


def default_template_builder_rows() -> List[Dict[str, Any]]:
    """One empty row for a new form template."""
    return [
        {
            "label": "",
            "type": "text",
            "options_multiline": "",
            "required": False,
        }
    ]


def prepare_template_builder_rows(schema_json: Any) -> List[Dict[str, Any]]:
    """Turn stored schema into rows for the friendly editor."""
    fields = parse_form_schema(schema_json)
    out: List[Dict[str, Any]] = []
    for f in fields:
        opts = f.get("options") or []
        if isinstance(opts, str):
            opts = [opts]
        opts_lines = "\n".join(str(o) for o in opts if o is not None and str(o).strip())
        ftype = (f.get("type") or "text").strip().lower()
        if ftype not in RECRUITMENT_BUILDER_FIELD_TYPES:
            ftype = "text"
        out.append(
            {
                "label": (f.get("label") or f.get("name") or "").strip(),
                "type": ftype,
                "options_multiline": opts_lines,
                "required": bool(f.get("required")),
            }
        )
    return out if out else default_template_builder_rows()


def _unique_builder_field_name(label: str, used: Set[str], index: int) -> str:
    base = _slugify(label)[:56] or f"question_{index + 1}"
    if base and base[0].isdigit():
        base = "q_" + base
    if not base:
        base = f"question_{index + 1}"
    name = base
    n = 2
    while name in used:
        name = f"{base}_{n}"
        n += 1
    return name


def build_schema_json_from_builder_form(form) -> Tuple[Optional[str], str]:
    """
    Build recruitment form template JSON from POST fields (no JSON typing required).
    Expects parallel lists: field_label, field_type, field_options, field_required.
    """
    labels = form.getlist("field_label")
    types = form.getlist("field_type")
    options_raw = form.getlist("field_options")
    required_raw = form.getlist("field_required")

    fields: List[Dict[str, Any]] = []
    used_names: Set[str] = set()
    row_display = 0
    for i, label in enumerate(labels):
        label = (label or "").strip()
        if not label:
            continue
        row_display += 1
        ftype = (types[i] if i < len(types) else "text").strip().lower()
        if ftype not in RECRUITMENT_BUILDER_FIELD_TYPES:
            ftype = "text"
        req_val = (required_raw[i] if i < len(required_raw) else "0").strip()
        required = req_val in ("1", "yes", "true", "on")
        name = _unique_builder_field_name(label, used_names, i)
        used_names.add(name)
        entry: Dict[str, Any] = {
            "name": name,
            "label": label,
            "type": ftype,
            "required": required,
        }
        if ftype in RECRUITMENT_TYPES_WITH_OPTIONS:
            raw = (options_raw[i] if i < len(options_raw) else "") or ""
            opts = []
            for line in raw.replace("\r\n", "\n").split("\n"):
                o = line.strip()
                if o:
                    opts.append(o)
            if not opts:
                return (
                    None,
                    f'For question {row_display} (“{label}”), you chose a list-style answer — add at least one choice (one per line).',
                )
            entry["options"] = opts
        fields.append(entry)

    if not fields:
        return None, "Add at least one question and type the question text applicants will see."

    return json.dumps({"fields": fields}), ""


def builder_rows_from_post(form) -> List[Dict[str, Any]]:
    """Repopulate the visual editor after a validation error."""
    labels = form.getlist("field_label")
    if not labels:
        return default_template_builder_rows()
    types = form.getlist("field_type")
    options_raw = form.getlist("field_options")
    required_raw = form.getlist("field_required")
    rows: List[Dict[str, Any]] = []
    for i, label in enumerate(labels):
        ftype = (types[i] if i < len(types) else "text").strip().lower()
        if ftype not in RECRUITMENT_BUILDER_FIELD_TYPES:
            ftype = "text"
        req = (required_raw[i] if i < len(required_raw) else "0").strip() in (
            "1",
            "yes",
            "true",
            "on",
        )
        rows.append(
            {
                "label": label or "",
                "type": ftype,
                "options_multiline": (options_raw[i] if i < len(options_raw) else "") or "",
                "required": req,
            }
        )
    return rows if rows else default_template_builder_rows()


# ---------------------------------------------------------------------------
# Public vacancies
# ---------------------------------------------------------------------------


def list_open_vacancies(limit: int = 200) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT o.id, o.title, o.slug, o.summary, o.published_at, o.closes_at,
                   r.title AS role_title, r.slug AS role_slug
            FROM rec_openings o
            JOIN rec_job_roles r ON r.id = o.job_role_id
            WHERE o.status = 'open' AND r.active = 1
            ORDER BY COALESCE(o.published_at, o.created_at) DESC, o.id DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def get_opening_public_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    slug = (slug or "").strip()
    if not slug:
        return None
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT o.*, r.title AS role_title, r.slug AS role_slug, r.description AS role_description
            FROM rec_openings o
            JOIN rec_job_roles r ON r.id = o.job_role_id
            WHERE o.slug = %s AND o.status = 'open' AND r.active = 1
            LIMIT 1
            """,
            (slug,),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Applicants
# ---------------------------------------------------------------------------


def get_applicant_by_id(applicant_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, email, name, phone, created_at FROM rec_applicants WHERE id = %s LIMIT 1",
            (applicant_id,),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def get_applicant_by_email(email: str) -> Optional[Dict[str, Any]]:
    email = (email or "").strip().lower()
    if not email:
        return None
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT * FROM rec_applicants WHERE email = %s LIMIT 1",
            (email,),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def register_applicant(
    email: str, name: str, phone: Optional[str], password: str
) -> Tuple[bool, str, Optional[int]]:
    email = (email or "").strip().lower()
    name = (name or "").strip()
    phone = (phone or "").strip() or None
    if not email or "@" not in email:
        return False, "Valid email required", None
    if not name:
        return False, "Name required", None
    if not password or len(password) < 8:
        return False, "Password must be at least 8 characters", None
    if get_applicant_by_email(email):
        return False, "An account already exists for this email", None
    pwd_hash = AuthManager.hash_password(password)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO rec_applicants (email, name, phone, password_hash)
            VALUES (%s, %s, %s, %s)
            """,
            (email, name, phone, pwd_hash),
        )
        conn.commit()
        return True, "ok", cur.lastrowid
    except Exception as e:
        conn.rollback()
        return False, str(e), None
    finally:
        cur.close()
        conn.close()


def verify_applicant_login(email: str, password: str) -> Optional[Dict[str, Any]]:
    row = get_applicant_by_email(email)
    if not row or not row.get("password_hash"):
        return None
    if not AuthManager.verify_password(row["password_hash"], password):
        return None
    return row


def ensure_applicant_password(applicant_id: int, password: str) -> Tuple[bool, str]:
    if not password or len(password) < 8:
        return False, "Password must be at least 8 characters"
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE rec_applicants SET password_hash = %s WHERE id = %s",
            (AuthManager.hash_password(password), applicant_id),
        )
        conn.commit()
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Applications
# ---------------------------------------------------------------------------


def create_application(
    opening_id: int,
    applicant_id: int,
    cover_note: Optional[str] = None,
    cv_path: Optional[str] = None,
) -> Tuple[bool, str, Optional[int]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    app_id: Optional[int] = None
    err: Optional[str] = None
    try:
        cur.execute(
            "SELECT id, status FROM rec_openings WHERE id = %s LIMIT 1",
            (opening_id,),
        )
        op = cur.fetchone()
        if not op or op.get("status") != "open":
            err = "This vacancy is not open for applications"
        else:
            cur.execute(
                """
                SELECT id FROM rec_applications
                WHERE opening_id = %s AND applicant_id = %s LIMIT 1
                """,
                (opening_id, applicant_id),
            )
            if cur.fetchone():
                err = "You have already applied for this role"
            else:
                cur2 = conn.cursor()
                cur2.execute(
                    """
                    INSERT INTO rec_applications (opening_id, applicant_id, stage, status, cover_note, cv_path)
                    VALUES (%s, %s, 'applied', 'active', %s, %s)
                    """,
                    (opening_id, applicant_id, cover_note or None, cv_path),
                )
                app_id = cur2.lastrowid
                conn.commit()
                cur2.close()
    except Exception as e:
        conn.rollback()
        err = str(e)
        app_id = None
    finally:
        cur.close()
        conn.close()
    if err:
        return False, err, None
    if app_id is not None:
        apply_auto_tasks_for_stage(app_id)
    return True, "ok", app_id


def list_applications_for_applicant(applicant_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT a.*, o.title AS opening_title, o.slug AS opening_slug, o.status AS opening_status
            FROM rec_applications a
            JOIN rec_openings o ON o.id = a.opening_id
            WHERE a.applicant_id = %s
            ORDER BY a.created_at DESC
            """,
            (applicant_id,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def get_application_for_applicant(
    application_id: int, applicant_id: int
) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT a.*, o.title AS opening_title, o.slug AS opening_slug, o.status AS opening_status,
                   o.id AS opening_id
            FROM rec_applications a
            JOIN rec_openings o ON o.id = a.opening_id
            WHERE a.id = %s AND a.applicant_id = %s
            LIMIT 1
            """,
            (application_id, applicant_id),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def get_application_tasks_for_applicant(
    application_id: int, applicant_id: int
) -> List[Dict[str, Any]]:
    app = get_application_for_applicant(application_id, applicant_id)
    if not app:
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT t.*, ft.name AS template_name, ft.purpose AS template_purpose, ft.schema_json
            FROM rec_application_tasks t
            JOIN rec_form_templates ft ON ft.id = t.form_template_id
            WHERE t.application_id = %s
            ORDER BY t.assigned_at ASC, t.id ASC
            """,
            (application_id,),
        )
        rows = cur.fetchall() or []
        for r in rows:
            r["schema_fields"] = parse_form_schema(r.get("schema_json"))
        return rows
    finally:
        cur.close()
        conn.close()


def update_application_cv(application_id: int, applicant_id: int, cv_path: str) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE rec_applications SET cv_path = %s
            WHERE id = %s AND applicant_id = %s
            """,
            (cv_path, application_id, applicant_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()


def collect_task_response_from_form(
    schema_fields: List[Dict[str, Any]], form
) -> Dict[str, Any]:
    """
    Map applicant POST data to stored response_json (string values).
    Handles checkbox, multiselect, and standard single-value inputs.
    """
    out: Dict[str, str] = {}
    for f in schema_fields:
        name = f.get("name")
        if not name:
            continue
        ftype = (f.get("type") or "text").strip().lower()
        if ftype == "file":
            # Filled from request.files in the route after this call
            continue
        if ftype == "checkbox":
            v = form.get(name)
            out[name] = "yes" if v in ("yes", "on", "1", "true") else "no"
        elif ftype == "multiselect":
            parts = form.getlist(name)
            out[name] = ", ".join(
                p.strip() for p in parts if p is not None and str(p).strip()
            )
        else:
            out[name] = (form.get(name) or "").strip()
    return out


def get_task_for_applicant(task_id: int, applicant_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT t.*, a.applicant_id, ft.name AS template_name, ft.purpose AS template_purpose, ft.schema_json
            FROM rec_application_tasks t
            JOIN rec_applications a ON a.id = t.application_id
            JOIN rec_form_templates ft ON ft.id = t.form_template_id
            WHERE t.id = %s AND a.applicant_id = %s
            LIMIT 1
            """,
            (task_id, applicant_id),
        )
        row = cur.fetchone()
        if row:
            row["schema_fields"] = parse_form_schema(row.get("schema_json"))
        return row
    finally:
        cur.close()
        conn.close()


def submit_task_response(
    task_id: int, applicant_id: int, response: Dict[str, Any]
) -> Tuple[bool, str]:
    task = get_task_for_applicant(task_id, applicant_id)
    if not task:
        return False, "Task not found"
    if task.get("status") == "completed":
        return False, "Already submitted"
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE rec_application_tasks
            SET status = 'completed', completed_at = NOW(), response_json = %s
            WHERE id = %s AND status = 'pending'
            """,
            (json.dumps(response), task_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            return False, "Could not update task"
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Stage rules & auto tasks
# ---------------------------------------------------------------------------


def apply_auto_tasks_for_stage(application_id: int) -> None:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT a.id, a.opening_id, a.stage
            FROM rec_applications a WHERE a.id = %s LIMIT 1
            """,
            (application_id,),
        )
        app = cur.fetchone()
        if not app:
            return
        opening_id = app["opening_id"]
        stage = app["stage"]
        cur.execute(
            """
            SELECT id, form_template_id FROM rec_opening_form_rules
            WHERE opening_id = %s AND trigger_stage = %s AND auto_assign = 1
            ORDER BY sort_order ASC, id ASC
            """,
            (opening_id, stage),
        )
        rules = cur.fetchall() or []
        for rule in rules:
            _ensure_task(conn, application_id, rule["form_template_id"])
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _ensure_task(conn, application_id: int, form_template_id: int) -> None:
    """Create pending task if none exists for this app+template (pending or completed)."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id FROM rec_application_tasks
            WHERE application_id = %s AND form_template_id = %s
            LIMIT 1
            """,
            (application_id, form_template_id),
        )
        if cur.fetchone():
            return
        cur.execute(
            """
            INSERT INTO rec_application_tasks (application_id, form_template_id, status)
            VALUES (%s, %s, 'pending')
            """,
            (application_id, form_template_id),
        )
    finally:
        cur.close()


def create_manual_task(application_id: int, form_template_id: int) -> Tuple[bool, str]:
    conn = get_db_connection()
    try:
        _ensure_task(conn, application_id, form_template_id)
        conn.commit()
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()


def force_assign_task(application_id: int, form_template_id: int) -> Tuple[bool, str]:
    """Always add a new pending task (admin 'send again')."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO rec_application_tasks (application_id, form_template_id, status)
            VALUES (%s, %s, 'pending')
            """,
            (application_id, form_template_id),
        )
        conn.commit()
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def set_application_stage(application_id: int, new_stage: str) -> Tuple[bool, str]:
    new_stage = (new_stage or "").strip().lower()
    if new_stage not in STAGES_ORDER:
        return False, "Invalid stage"
    conn = get_db_connection()
    cur = conn.cursor()
    err: Optional[str] = None
    try:
        cur.execute(
            "UPDATE rec_applications SET stage = %s WHERE id = %s",
            (new_stage, application_id),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        err = str(e)
    finally:
        cur.close()
        conn.close()
    if err:
        return False, err
    if new_stage == "rejected":
        schedule_recruitment_purge_eligible(application_id)
    apply_auto_tasks_for_stage(application_id)
    return True, "ok"

# ---------------------------------------------------------------------------
# Admin: roles
# ---------------------------------------------------------------------------


def admin_list_roles() -> List[Dict[str, Any]]:
    """Job roles with time billing link + recruitment usage counts (live openings, pipeline apps)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT r.*,
                   tb.name AS tb_role_name,
                   wrc.name AS default_wage_card_name,
                   (SELECT COUNT(*) FROM rec_openings o
                    WHERE o.job_role_id = r.id AND o.status = 'open') AS openings_live,
                   (SELECT COUNT(*) FROM rec_openings o
                    WHERE o.job_role_id = r.id) AS openings_total,
                   (SELECT COUNT(*) FROM rec_applications a
                    INNER JOIN rec_openings o ON o.id = a.opening_id
                    WHERE o.job_role_id = r.id
                      AND a.stage NOT IN ('rejected', 'hired')
                      AND (a.status IS NULL OR a.status = 'active')) AS applications_pipeline
            FROM rec_job_roles r
            LEFT JOIN roles tb ON tb.id = r.time_billing_role_id
            LEFT JOIN wage_rate_cards wrc ON wrc.id = r.default_wage_rate_card_id
            ORDER BY r.title ASC
            """
        )
        return cur.fetchall() or []
    except Exception as e:
        logger.warning("admin_list_roles (TB join may need migration): %s", e)
        cur.execute("SELECT * FROM rec_job_roles ORDER BY title ASC")
        rows = cur.fetchall() or []
        for row in rows:
            row.setdefault("tb_role_name", None)
            row.setdefault("default_wage_card_name", None)
            row.setdefault("openings_live", 0)
            row.setdefault("openings_total", 0)
            row.setdefault("applications_pipeline", 0)
        return rows
    finally:
        cur.close()
        conn.close()


def time_billing_roles_for_select() -> List[Dict[str, Any]]:
    """Active time billing roles (wage / portal)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, name, code FROM roles
            WHERE active = 1
            ORDER BY name ASC, id ASC
            """
        )
        return cur.fetchall() or []
    except Exception as e:
        logger.warning("time_billing_roles_for_select: %s", e)
        return []
    finally:
        cur.close()
        conn.close()


def time_billing_wage_cards_for_select() -> List[Dict[str, Any]]:
    """Wage rate cards for linking to recruitment job roles (filter by role in UI)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT w.id, w.name, w.role_id, w.active, r.name AS role_name
            FROM wage_rate_cards w
            LEFT JOIN roles r ON r.id = w.role_id
            WHERE w.active = 1
            ORDER BY r.name, w.name
            """
        )
        return cur.fetchall() or []
    except Exception as e:
        logger.warning("time_billing_wage_cards_for_select: %s", e)
        return []
    finally:
        cur.close()
        conn.close()


def _first_active_wage_card_for_role(conn, role_id: int) -> Optional[int]:
    if not role_id:
        return None
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id FROM wage_rate_cards
            WHERE role_id = %s AND active = 1
            ORDER BY id ASC
            LIMIT 1
            """,
            (role_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return int(row[0] if isinstance(row, (list, tuple)) else row.get("id"))
    finally:
        cur.close()


def resolve_hire_role_and_wage_for_application(
    app_row: Dict[str, Any],
    override_tb_role_id: Optional[int],
    override_wage_rate_card_id: Optional[int],
    legacy_role_name: str,
) -> Tuple[bool, str, Optional[int], Optional[int], str]:
    """
    Decide time billing role_id and wage_rate_card_id when hiring from an application.
    Uses job role link → optional hire overrides → legacy role name (create/find roles row).
    """
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        jr_tb = _parse_optional_int(app_row.get("job_role_time_billing_role_id"))
        jr_card = _parse_optional_int(app_row.get("job_role_default_wage_rate_card_id"))
        o_tb = _parse_optional_int(override_tb_role_id)
        resolved_role = o_tb or jr_tb

        if not resolved_role:
            name = (legacy_role_name or "staff").strip() or "staff"
            cur.execute(
                "SELECT id FROM roles WHERE LOWER(TRIM(name)) = %s LIMIT 1",
                (name.lower(),),
            )
            row = cur.fetchone()
            if row:
                resolved_role = int(row["id"])
            else:
                cur.execute("INSERT INTO roles (name) VALUES (%s)", (name,))
                conn.commit()
                resolved_role = int(cur.lastrowid)

        cur.execute("SELECT name FROM roles WHERE id = %s LIMIT 1", (resolved_role,))
        rn = cur.fetchone()
        display = (rn or {}).get("name") or str(resolved_role)

        wage_id: Optional[int] = None
        o_wc = _parse_optional_int(override_wage_rate_card_id)
        if o_wc:
            if not _wage_card_valid_for_role_cursor(cur, o_wc, resolved_role):
                return (
                    False,
                    "The selected wage rate card does not match the time billing role.",
                    None,
                    None,
                    "",
                )
            wage_id = o_wc
        else:
            if jr_tb and resolved_role == jr_tb and jr_card:
                if _wage_card_valid_for_role_cursor(cur, jr_card, resolved_role):
                    wage_id = jr_card
            if wage_id is None:
                wage_id = _first_active_wage_card_for_role(conn, resolved_role)

        return True, "ok", resolved_role, wage_id, display
    except Exception as e:
        return False, str(e), None, None, ""
    finally:
        cur.close()
        conn.close()


def admin_preview_hire_billing(application_id: int, legacy_role_name: str = "staff") -> Dict[str, Any]:
    """What time billing will receive on hire (no overrides)."""
    app = admin_get_application(application_id)
    if not app:
        return {"ok": False, "message": "Application not found"}
    ok, msg, rid, wcid, dname = resolve_hire_role_and_wage_for_application(
        app, None, None, legacy_role_name
    )
    return {
        "ok": ok,
        "message": msg,
        "role_id": rid,
        "wage_card_id": wcid,
        "role_name": dname,
        "rec_job_role_title": app.get("rec_job_role_title"),
        "linked_tb_role_name": app.get("job_role_tb_role_name"),
    }


def _wage_card_valid_for_role_cursor(cur, card_id: int, role_id: int) -> bool:
    cur.execute(
        "SELECT id, role_id, active FROM wage_rate_cards WHERE id = %s LIMIT 1",
        (card_id,),
    )
    row = cur.fetchone()
    if not row or not int(row.get("active") or 0):
        return False
    cr = row.get("role_id")
    if cr is None:
        return True
    return int(cr) == int(role_id)


def _wage_card_valid_for_role(conn, card_id: int, role_id: int) -> bool:
    cur = conn.cursor(dictionary=True)
    try:
        return _wage_card_valid_for_role_cursor(cur, card_id, role_id)
    finally:
        cur.close()


def _parse_optional_int(val: Any) -> Optional[int]:
    if val is None or val == "":
        return None
    try:
        i = int(val)
        return i if i > 0 else None
    except (TypeError, ValueError):
        return None


def admin_save_role(
    role_id: Optional[int],
    title: str,
    description: Optional[str],
    department: Optional[str],
    active: bool,
    slug: Optional[str] = None,
    time_billing_role_id: Optional[int] = None,
    default_wage_rate_card_id: Optional[int] = None,
) -> Tuple[bool, str, Optional[int]]:
    title = (title or "").strip()
    if not title:
        return False, "Title required", None
    slug = (slug or "").strip() or _slugify(title)
    tb_rid = _parse_optional_int(time_billing_role_id)
    wage_cid = _parse_optional_int(default_wage_rate_card_id)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if wage_cid and tb_rid:
            if not _wage_card_valid_for_role(conn, wage_cid, tb_rid):
                return (
                    False,
                    "Default wage card must belong to the selected time billing role (or clear one of them).",
                    None,
                )
        if wage_cid and not tb_rid:
            return False, "Select a time billing role before choosing a default wage card.", None

        if role_id:
            cur.execute(
                """
                UPDATE rec_job_roles
                SET title=%s, slug=%s, description=%s, department=%s, active=%s,
                    time_billing_role_id=%s, default_wage_rate_card_id=%s
                WHERE id=%s
                """,
                (
                    title,
                    slug,
                    description or None,
                    department or None,
                    1 if active else 0,
                    tb_rid,
                    wage_cid,
                    role_id,
                ),
            )
            conn.commit()
            return True, "ok", role_id
        cur.execute(
            """
            INSERT INTO rec_job_roles (
              title, slug, description, department, active,
              time_billing_role_id, default_wage_rate_card_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                title,
                slug,
                description or None,
                department or None,
                1 if active else 0,
                tb_rid,
                wage_cid,
            ),
        )
        conn.commit()
        return True, "ok", cur.lastrowid
    except Exception as e:
        conn.rollback()
        return False, str(e), None
    finally:
        cur.close()
        conn.close()


def admin_get_role(role_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM rec_job_roles WHERE id = %s", (role_id,))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Admin: openings
# ---------------------------------------------------------------------------


def admin_list_openings() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT o.*, r.title AS role_title
            FROM rec_openings o
            JOIN rec_job_roles r ON r.id = o.job_role_id
            ORDER BY o.updated_at DESC
            """
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_recruitment_dashboard_stats() -> Dict[str, Any]:
    """High-level counts for the recruitment home / header."""
    out: Dict[str, Any] = {
        "openings_open": 0,
        "openings_draft": 0,
        "applications_total": 0,
        "applications_new": 0,
        "applications_in_pipeline": 0,
        "pending_tasks": 0,
    }
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        try:
            cur.execute("SELECT COUNT(*) FROM rec_openings WHERE status = 'open'")
            out["openings_open"] = int((cur.fetchone() or [0])[0])
            cur.execute("SELECT COUNT(*) FROM rec_openings WHERE status = 'draft'")
            out["openings_draft"] = int((cur.fetchone() or [0])[0])
            cur.execute("SELECT COUNT(*) FROM rec_applications")
            out["applications_total"] = int((cur.fetchone() or [0])[0])
            cur.execute(
                """
                SELECT COUNT(*) FROM rec_applications
                WHERE stage = 'applied' AND (status IS NULL OR status = 'active')
                """
            )
            out["applications_new"] = int((cur.fetchone() or [0])[0])
            cur.execute(
                """
                SELECT COUNT(*) FROM rec_applications
                WHERE stage NOT IN ('rejected', 'hired') AND (status IS NULL OR status = 'active')
                """
            )
            out["applications_in_pipeline"] = int((cur.fetchone() or [0])[0])
            cur.execute(
                "SELECT COUNT(*) FROM rec_application_tasks WHERE status = 'pending'"
            )
            out["pending_tasks"] = int((cur.fetchone() or [0])[0])
        except Exception:
            pass
        return out
    finally:
        cur.close()
        conn.close()


def admin_list_openings_dashboard(
    q: Optional[str] = None,
    status_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Openings with application counts for the job board (Kanban-style UI).
    status_filter: all | open | draft | closed
    """
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["1=1"]
        params: List[Any] = []
        sf = (status_filter or "all").strip().lower()
        if sf == "open":
            where.append("o.status = 'open'")
        elif sf == "draft":
            where.append("o.status = 'draft'")
        elif sf == "closed":
            where.append("o.status = 'closed'")
        if q and q.strip():
            term = f"%{q.strip()}%"
            where.append(
                "(o.title LIKE %s OR o.slug LIKE %s OR r.title LIKE %s OR COALESCE(r.department,'') LIKE %s)"
            )
            params.extend([term, term, term, term])
        sql = f"""
            SELECT o.*, r.title AS role_title, r.department AS role_department, r.active AS role_active,
              (SELECT COUNT(*) FROM rec_applications a WHERE a.opening_id = o.id) AS application_count,
              (SELECT COUNT(*) FROM rec_applications a
                WHERE a.opening_id = o.id AND a.stage = 'applied'
                  AND (a.status IS NULL OR a.status = 'active')
              ) AS new_applications_count,
              (SELECT COUNT(*) FROM rec_applications a
                WHERE a.opening_id = o.id
                  AND a.stage NOT IN ('rejected', 'hired')
                  AND (a.status IS NULL OR a.status = 'active')
              ) AS pipeline_count,
              (SELECT COUNT(*) FROM rec_application_tasks t
                INNER JOIN rec_applications a ON a.id = t.application_id
                WHERE a.opening_id = o.id AND t.status = 'pending'
              ) AS pending_tasks_count,
              (SELECT COUNT(*) FROM rec_application_tasks t
                INNER JOIN rec_applications a ON a.id = t.application_id
                WHERE a.opening_id = o.id AND t.status = 'pending'
                  AND t.assigned_at < DATE_SUB(NOW(), INTERVAL 7 DAY)
              ) AS overdue_tasks_count
            FROM rec_openings o
            JOIN rec_job_roles r ON r.id = o.job_role_id
            WHERE {" AND ".join(where)}
            ORDER BY
              CASE o.status WHEN 'open' THEN 0 WHEN 'draft' THEN 1 ELSE 2 END,
              o.updated_at DESC
        """
        cur.execute(sql, tuple(params))
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_get_opening(opening_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT o.*, r.title AS role_title
            FROM rec_openings o
            JOIN rec_job_roles r ON r.id = o.job_role_id
            WHERE o.id = %s
            """,
            (opening_id,),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def admin_save_opening(
    opening_id: Optional[int],
    job_role_id: int,
    title: str,
    slug: Optional[str],
    summary: Optional[str],
    description: Optional[str],
    status: str,
    published_at: Optional[str],
    closes_at: Optional[str],
) -> Tuple[bool, str, Optional[int]]:
    title = (title or "").strip()
    if not title:
        return False, "Title required", None
    status = (status or "draft").strip().lower()
    if status not in ("draft", "open", "closed"):
        status = "draft"
    slug = (slug or "").strip() or _slugify(title)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if opening_id:
            cur.execute(
                """
                UPDATE rec_openings
                SET job_role_id=%s, title=%s, slug=%s, summary=%s, description=%s,
                    status=%s, published_at=%s, closes_at=%s
                WHERE id=%s
                """,
                (
                    job_role_id,
                    title,
                    slug,
                    summary or None,
                    description or None,
                    status,
                    published_at or None,
                    closes_at or None,
                    opening_id,
                ),
            )
            conn.commit()
            return True, "ok", opening_id
        cur.execute(
            """
            INSERT INTO rec_openings (
              job_role_id, title, slug, summary, description, status, published_at, closes_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                job_role_id,
                title,
                slug,
                summary or None,
                description or None,
                status,
                published_at or None,
                closes_at or None,
            ),
        )
        oid = cur.lastrowid
        conn.commit()
        return True, "ok", oid
    except Exception as e:
        conn.rollback()
        return False, str(e), None
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Admin: form templates & rules
# ---------------------------------------------------------------------------


def admin_list_templates() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM rec_form_templates ORDER BY name ASC")
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_get_template(template_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM rec_form_templates WHERE id = %s", (template_id,))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def admin_save_template(
    template_id: Optional[int],
    name: str,
    purpose: str,
    schema_json: str,
    active: bool,
) -> Tuple[bool, str, Optional[int]]:
    name = (name or "").strip()
    if not name:
        return False, "Name required", None
    purpose = (purpose or "survey").strip().lower()
    if purpose not in ("survey", "assessment"):
        purpose = "survey"
    try:
        parsed = json.loads(schema_json or "{}")
        if not isinstance(parsed, (dict, list)):
            raise ValueError("Schema must be JSON object or array")
    except Exception as e:
        return False, f"Invalid JSON: {e}", None
    if isinstance(parsed, dict) and "fields" not in parsed:
        parsed = {"fields": []}
    normalized = json.dumps(parsed)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if template_id:
            cur.execute(
                """
                UPDATE rec_form_templates
                SET name=%s, purpose=%s, schema_json=%s, active=%s
                WHERE id=%s
                """,
                (name, purpose, normalized, 1 if active else 0, template_id),
            )
            conn.commit()
            return True, "ok", template_id
        cur.execute(
            """
            INSERT INTO rec_form_templates (name, purpose, schema_json, active)
            VALUES (%s, %s, %s, %s)
            """,
            (name, purpose, normalized, 1 if active else 0),
        )
        tid = cur.lastrowid
        conn.commit()
        return True, "ok", tid
    except Exception as e:
        conn.rollback()
        return False, str(e), None
    finally:
        cur.close()
        conn.close()


def admin_list_rules(opening_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT r.*, ft.name AS template_name
            FROM rec_opening_form_rules r
            JOIN rec_form_templates ft ON ft.id = r.form_template_id
            WHERE r.opening_id = %s
            ORDER BY r.sort_order ASC, r.id ASC
            """,
            (opening_id,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_save_rule(
    rule_id: Optional[int],
    opening_id: int,
    trigger_stage: str,
    form_template_id: int,
    auto_assign: bool,
    sort_order: int,
) -> Tuple[bool, str]:
    trigger_stage = (trigger_stage or "").strip().lower()
    if trigger_stage not in STAGES_ORDER:
        return False, "Invalid stage"
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if rule_id:
            cur.execute(
                """
                UPDATE rec_opening_form_rules
                SET trigger_stage=%s, form_template_id=%s, auto_assign=%s, sort_order=%s
                WHERE id=%s AND opening_id=%s
                """,
                (
                    trigger_stage,
                    form_template_id,
                    1 if auto_assign else 0,
                    sort_order,
                    rule_id,
                    opening_id,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO rec_opening_form_rules
                  (opening_id, trigger_stage, form_template_id, auto_assign, sort_order)
                VALUES (%s,%s,%s,%s,%s)
                """,
                (
                    opening_id,
                    trigger_stage,
                    form_template_id,
                    1 if auto_assign else 0,
                    sort_order,
                ),
            )
        conn.commit()
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def admin_delete_rule(rule_id: int, opening_id: int) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM rec_opening_form_rules WHERE id = %s AND opening_id = %s",
            (rule_id, opening_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Admin: applications
# ---------------------------------------------------------------------------


def admin_list_applications(
    opening_id: Optional[int] = None, q: Optional[str] = None, limit: int = 300
) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["1=1"]
        params: List[Any] = []
        if opening_id:
            where.append("a.opening_id = %s")
            params.append(opening_id)
        if q:
            where.append("(ap.email LIKE %s OR ap.name LIKE %s OR o.title LIKE %s)")
            like = f"%{q}%"
            params.extend([like, like, like])
        sql = f"""
            SELECT a.*, ap.email, ap.name, o.title AS opening_title
            FROM rec_applications a
            JOIN rec_applicants ap ON ap.id = a.applicant_id
            JOIN rec_openings o ON o.id = a.opening_id
            WHERE {' AND '.join(where)}
            ORDER BY a.updated_at DESC
            LIMIT %s
        """
        params.append(limit)
        cur.execute(sql, tuple(params))
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_get_application(application_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = None
    try:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT a.*, ap.email, ap.name, ap.phone,
                       o.title AS opening_title, o.slug AS opening_slug,
                       o.job_role_id AS rec_opening_job_role_id,
                       jr.title AS rec_job_role_title,
                       jr.time_billing_role_id AS job_role_time_billing_role_id,
                       jr.default_wage_rate_card_id AS job_role_default_wage_rate_card_id,
                       tb.name AS job_role_tb_role_name
                FROM rec_applications a
                JOIN rec_applicants ap ON ap.id = a.applicant_id
                JOIN rec_openings o ON o.id = a.opening_id
                JOIN rec_job_roles jr ON jr.id = o.job_role_id
                LEFT JOIN roles tb ON tb.id = jr.time_billing_role_id
                WHERE a.id = %s
                """,
                (application_id,),
            )
        except Exception as e:
            logger.warning("admin_get_application (TB columns missing?): %s", e)
            cur.close()
            cur = conn.cursor(dictionary=True)
            cur.execute(
                """
                SELECT a.*, ap.email, ap.name, ap.phone, o.title AS opening_title, o.slug AS opening_slug
                FROM rec_applications a
                JOIN rec_applicants ap ON ap.id = a.applicant_id
                JOIN rec_openings o ON o.id = a.opening_id
                WHERE a.id = %s
                """,
                (application_id,),
            )
        row = cur.fetchone()
        if row:
            row.setdefault("rec_job_role_title", None)
            row.setdefault("job_role_time_billing_role_id", None)
            row.setdefault("job_role_default_wage_rate_card_id", None)
            row.setdefault("job_role_tb_role_name", None)
        return row
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass


def admin_set_application_notes(application_id: int, admin_notes: Optional[str]) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE rec_applications SET admin_notes = %s WHERE id = %s",
            (admin_notes or None, application_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()


def admin_list_application_tasks(application_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT t.*, ft.name AS template_name, ft.purpose
            FROM rec_application_tasks t
            JOIN rec_form_templates ft ON ft.id = t.form_template_id
            WHERE t.application_id = %s
            ORDER BY t.assigned_at DESC
            """,
            (application_id,),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Hire → contractor
# ---------------------------------------------------------------------------


def _unique_initials(conn, base: str) -> str:
    base = re.sub(r"[^A-Za-z0-9]", "", base)[:6].upper() or "U"
    cur = conn.cursor()
    try:
        for _ in range(30):
            suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
            cand = (base + suffix)[:32]
            cur.execute("SELECT id FROM tb_contractors WHERE initials = %s LIMIT 1", (cand,))
            if not cur.fetchone():
                return cand
        return (base + "".join(random.choices(string.ascii_uppercase + string.digits, k=8)))[:32]
    finally:
        cur.close()


def hire_application_as_contractor(
    application_id: int,
    role_name: str = "staff",
    password: Optional[str] = None,
    static_root: Optional[str] = None,
    override_time_billing_role_id: Optional[int] = None,
    override_wage_rate_card_id: Optional[int] = None,
) -> Tuple[bool, str, Optional[int]]:
    """
    Create tb_contractors row and link application.contractor_id. Sets stage to hired.
    Requires HR pre-hire authorization, applicant policy acknowledgement, and all pre-hire
    requests HR-approved. Copies approved documents and CV into HR employee record.
    """
    ok_pre, msg_pre = hire_precheck(application_id)
    if not ok_pre:
        return False, msg_pre, None

    app = admin_get_application(application_id)
    if not app:
        return False, "Application not found", None
    if app.get("contractor_id"):
        return False, "Already linked to a contractor", app.get("contractor_id")
    email = (app.get("email") or "").strip().lower()
    name = (app.get("name") or "").strip()
    if not email or not name:
        return False, "Applicant email/name missing", None

    role_name = (role_name or "staff").strip().lower()
    pwd_hash = AuthManager.hash_password(password) if password else None
    purge_eligible = date.today() + timedelta(days=recruitment_applicant_retention_days())
    root = static_root or _recruitment_static_root()

    o_tb = _parse_optional_int(override_time_billing_role_id)
    o_wc = _parse_optional_int(override_wage_rate_card_id)
    ok_r, msg_r, role_id, wage_card_id, _role_display = (
        resolve_hire_role_and_wage_for_application(app, o_tb, o_wc, role_name)
    )
    if not ok_r:
        return False, msg_r, None
    if not role_id:
        return False, "Could not resolve time billing role", None

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id FROM tb_contractors WHERE email = %s LIMIT 1", (email,))
        if cur.fetchone():
            return False, "A contractor with this email already exists", None

        initials = _unique_initials(conn, "".join(x[0] for x in name.split()[:2]) if name else "NB")

        cur2 = conn.cursor()
        cur2.execute(
            """
            INSERT INTO tb_contractors (
              email, name, status, password_hash, role_id, wage_rate_card_id, initials, created_at
            ) VALUES (%s, %s, 'active', %s, %s, %s, %s, NOW())
            """,
            (email, name, pwd_hash, role_id, wage_card_id, initials),
        )
        cid = cur2.lastrowid
        cur2.execute(
            """
            INSERT IGNORE INTO tb_contractor_roles (contractor_id, role_id)
            VALUES (%s, %s)
            """,
            (cid, role_id),
        )
        cur2.execute(
            """
            UPDATE rec_applications
            SET contractor_id = %s, stage = 'hired', status = 'hired',
                recruitment_data_purge_eligible_at = %s
            WHERE id = %s
            """,
            (cid, purge_eligible, application_id),
        )
        conn.commit()
        cur2.close()
        try:
            _copy_prehire_and_cv_to_hr(root, application_id, int(cid), app)
        except Exception as e:
            logger.exception("Post-hire HR copy failed (contractor %s created): %s", cid, e)

        # Training: auto-assign based on contractor role (v1).
        try:
            from app.plugins.training_module.services import TrainingService

            TrainingService.apply_role_assignment_rules(
                contractor_id=int(cid),
                role_id=int(role_id),
                assigned_by_user_id=None,
            )
        except Exception as e:
            logger.debug("Recruitment: training auto-assign skipped: %s", e)
        return True, "ok", cid
    except Exception as e:
        conn.rollback()
        return False, str(e), None
    finally:
        cur.close()
        conn.close()


def application_progress(stage: str) -> List[Dict[str, Any]]:
    """Steps for UI: applied → screening → interview → offer → hired, or rejected."""
    stage = (stage or "applied").lower()
    pipeline = ["applied", "screening", "interview", "offer", "hired"]
    if stage == "rejected":
        return [
            {
                "key": "rejected",
                "label": STAGE_LABELS["rejected"],
                "done": False,
                "current": True,
            }
        ]
    try:
        idx = pipeline.index(stage)
    except ValueError:
        idx = 0
    out = []
    for i, s in enumerate(pipeline):
        out.append(
            {
                "key": s,
                "label": STAGE_LABELS.get(s, s),
                "done": i < idx or (i == idx and stage == "hired"),
                "current": i == idx,
            }
        )
    return out
