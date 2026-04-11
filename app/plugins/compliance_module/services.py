"""
Compliance policies: publish, acknowledgements, employee portal todos.
"""
from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from app.objects import get_db_connection

logger = logging.getLogger(__name__)

SOURCE_MODULE = "compliance_module"
REF_TYPE_POLICY_ACK = "policy_ack"

# Legacy enum-era labels (DB `category` is now free-text topic; old rows were migrated on install).
_LEGACY_CATEGORY_LABELS = {
    "health_safety": "Health & safety",
    "safeguarding": "Safeguarding",
    "privacy": "Privacy",
    "data_protection": "Data protection",
    "equal_opportunities": "Equal opportunities",
    "disciplinary": "Disciplinary / conduct",
    "other": "",
}

# Kept for any external imports; prefer document types + topic.
POLICY_CATEGORIES = tuple(_LEGACY_CATEGORY_LABELS.keys())
CATEGORY_LABELS = dict(_LEGACY_CATEGORY_LABELS)


def human_category(code: Optional[str]) -> str:
    """Display helper for legacy slug-style category values (pre–free-text migration)."""
    if not code:
        return ""
    s = str(code).strip().lower()
    if s in _LEGACY_CATEGORY_LABELS:
        return _LEGACY_CATEGORY_LABELS[s] or "General"
    return str(code).strip()


def policy_topic_display(row: Optional[Dict[str, Any]]) -> str:
    """Free-text topic / area (stored in `category` column)."""
    if not row:
        return ""
    raw = (row.get("category") or "").strip()
    if not raw:
        return ""
    low = raw.lower().replace(" ", "_")
    if low in _LEGACY_CATEGORY_LABELS and _LEGACY_CATEGORY_LABELS[low]:
        return _LEGACY_CATEGORY_LABELS[low]
    return raw


def _policy_select_sql(extra_where: str = "", order_by: str = "p.category, p.title") -> str:
    return f"""
            SELECT p.*, dt.label AS document_type_label, dt.slug AS document_type_slug
            FROM comp_policies p
            LEFT JOIN comp_document_types dt ON dt.id = p.document_type_id
            {extra_where}
            ORDER BY {order_by}
            """


def list_document_types_for_policy_form(selected_type_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Active types for dropdown; include current type if it was deactivated."""
    active = list_document_types(active_only=True)
    if not selected_type_id:
        return active
    try:
        sid = int(selected_type_id)
    except (TypeError, ValueError):
        return active
    if any(int(x.get("id") or 0) == sid for x in active):
        return active
    row = get_document_type(sid)
    if not row:
        return active
    out = list(active) + [row]
    out.sort(key=lambda x: (int(x.get("sort_order") or 0), (x.get("label") or "").lower()))
    return out


def list_document_types(*, active_only: bool = True) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if active_only:
            cur.execute(
                """
                SELECT * FROM comp_document_types WHERE active = 1
                ORDER BY sort_order ASC, label ASC
                """
            )
        else:
            cur.execute(
                """
                SELECT * FROM comp_document_types
                ORDER BY active DESC, sort_order ASC, label ASC
                """
            )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def get_document_type(type_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM comp_document_types WHERE id = %s LIMIT 1", (int(type_id),))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def admin_create_document_type(label: str, slug: Optional[str] = None) -> Tuple[bool, str, Optional[int]]:
    label = (label or "").strip()
    if not label or len(label) > 128:
        return False, "Label required (max 128 characters).", None
    slug_use = (slug or "").strip().lower() or _slugify(label)
    slug_use = slug_use[:64]
    if not slug_use:
        return False, "Could not derive slug.", None
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(MAX(sort_order), 0) + 10 FROM comp_document_types")
        row = cur.fetchone()
        sort_order = int(row[0]) if row and row[0] is not None else 10
        cur.execute(
            """
            INSERT INTO comp_document_types (slug, label, sort_order, active)
            VALUES (%s, %s, %s, 1)
            """,
            (slug_use, label[:128], sort_order),
        )
        conn.commit()
        return True, "ok", cur.lastrowid
    except Exception as e:
        conn.rollback()
        return False, str(e), None
    finally:
        cur.close()
        conn.close()


def admin_update_document_type(
    type_id: int, label: str, sort_order: int, active: bool
) -> Tuple[bool, str]:
    label = (label or "").strip()
    if not label:
        return False, "Label required."
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE comp_document_types
            SET label = %s, sort_order = %s, active = %s
            WHERE id = %s
            """,
            (label[:128], int(sort_order), 1 if active else 0, int(type_id)),
        )
        conn.commit()
        return True, "ok"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def _valid_document_type_choice(type_id: int, current_type_id: Optional[Any] = None) -> bool:
    """Allow active types, or keeping an inactive type already on the policy."""
    row = get_document_type(type_id)
    if not row:
        return False
    if int(row.get("active") or 0) == 1:
        return True
    if current_type_id is None:
        return False
    try:
        return int(type_id) == int(current_type_id)
    except (TypeError, ValueError):
        return False


def admin_compliance_dashboard_metrics() -> Dict[str, Any]:
    today = date.today()
    horizon = today + timedelta(days=30)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM comp_policies WHERE published = 0")
        draft = int(cur.fetchone()[0])

        cur.execute("SELECT COUNT(*) FROM comp_policies WHERE published = 1")
        published = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT COUNT(*) FROM comp_policies
            WHERE published = 1 AND next_review_date IS NOT NULL AND next_review_date < %s
            """,
            (today,),
        )
        review_overdue = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT COUNT(*) FROM comp_policies
            WHERE published = 1 AND next_review_date IS NOT NULL
              AND next_review_date >= %s AND next_review_date <= %s
            """,
            (today, horizon),
        )
        review_due_30 = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT COUNT(*) FROM comp_policies
            WHERE published = 1 AND next_review_date IS NULL
            """
        )
        review_unscheduled = int(cur.fetchone()[0])

        return {
            "draft_count": draft,
            "published_count": published,
            "review_overdue_count": review_overdue,
            "review_due_30_count": review_due_30,
            "review_unscheduled_count": review_unscheduled,
            "today": today,
            "horizon_30": horizon,
        }
    finally:
        cur.close()
        conn.close()


def _slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-") or "policy"


def _todo_reference_id(policy_id: int, version: int, contractor_id: int) -> str:
    return f"p{int(policy_id)}_v{int(version)}_c{int(contractor_id)}"


def pending_policies_count(contractor_id: int) -> int:
    """Count published policies (mandatory) the contractor has not acknowledged at current version."""
    if not contractor_id:
        return 0
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM comp_policies p
            WHERE p.published = 1 AND p.mandatory = 1
              AND NOT EXISTS (
                SELECT 1 FROM comp_policy_acknowledgements a
                WHERE a.policy_id = p.id AND a.contractor_id = %s AND a.version = p.version
              )
            """,
            (int(contractor_id),),
        )
        row = cur.fetchone()
        return int(row[0] if row else 0)
    except Exception as e:
        logger.debug("pending_policies_count: %s", e)
        return 0
    finally:
        cur.close()
        conn.close()


def contractor_compliance_blocks_work(contractor_id: int) -> bool:
    """True if any mandatory published policy is not acknowledged (for HR / time billing gates)."""
    return pending_policies_count(contractor_id) > 0


def list_optional_published_policies(document_type_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Published policies that are not mandatory (reference / awareness)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        clauses = ["p.published = 1", "p.mandatory = 0"]
        params: List[Any] = []
        if document_type_id is not None:
            clauses.append("p.document_type_id = %s")
            params.append(int(document_type_id))
        where = " AND ".join(clauses)
        cur.execute(
            _policy_select_sql(f"WHERE {where}", "dt.sort_order IS NULL, dt.sort_order, p.title"),
            tuple(params),
        )
        return cur.fetchall() or []
    except Exception:
        return []
    finally:
        cur.close()
        conn.close()


def _parse_optional_date(value: Optional[str]) -> Optional[date]:
    """Parse HTML date input (YYYY-MM-DD) for MySQL DATE columns."""
    s = (value or "").strip()
    if not s:
        return None
    s = s[:10]
    try:
        y, m, d = s.split("-")
        return date(int(y), int(m), int(d))
    except (ValueError, TypeError):
        return None


def list_acknowledged_mandatory_for_contractor(
    contractor_id: int, document_type_id: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Published mandatory policies this contractor has acknowledged at the current version (policy library)."""
    if not contractor_id:
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        extra = ""
        params: List[Any] = [int(contractor_id)]
        if document_type_id is not None:
            extra = " AND p.document_type_id = %s"
            params.append(int(document_type_id))
        cur.execute(
            f"""
            SELECT p.*, a.acknowledged_at AS my_acknowledged_at,
                   dt.label AS document_type_label, dt.slug AS document_type_slug
            FROM comp_policies p
            INNER JOIN comp_policy_acknowledgements a
              ON a.policy_id = p.id AND a.contractor_id = %s AND a.version = p.version
            LEFT JOIN comp_document_types dt ON dt.id = p.document_type_id
            WHERE p.published = 1 AND p.mandatory = 1{extra}
            ORDER BY dt.sort_order IS NULL, dt.sort_order, p.category, p.title
            """,
            tuple(params),
        )
        return cur.fetchall() or []
    except Exception:
        return []
    finally:
        cur.close()
        conn.close()


def contractor_acknowledgement_at(
    contractor_id: int, policy_id: int, version: int
) -> Optional[datetime]:
    """When the contractor acknowledged this policy version, if at all."""
    if not contractor_id:
        return None
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT acknowledged_at FROM comp_policy_acknowledgements
            WHERE policy_id = %s AND contractor_id = %s AND version = %s
            LIMIT 1
            """,
            (int(policy_id), int(contractor_id), int(version)),
        )
        row = cur.fetchone()
        if not row:
            return None
        return row[0]
    finally:
        cur.close()
        conn.close()


def list_pending_policies_for_contractor(
    contractor_id: int, document_type_id: Optional[int] = None
) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        extra = ""
        params: List[Any] = [int(contractor_id)]
        if document_type_id is not None:
            extra = " AND p.document_type_id = %s"
            params.append(int(document_type_id))
        cur.execute(
            f"""
            SELECT p.*, dt.label AS document_type_label, dt.slug AS document_type_slug
            FROM comp_policies p
            LEFT JOIN comp_document_types dt ON dt.id = p.document_type_id
            WHERE p.published = 1 AND p.mandatory = 1
              AND NOT EXISTS (
                SELECT 1 FROM comp_policy_acknowledgements a
                WHERE a.policy_id = p.id AND a.contractor_id = %s AND a.version = p.version
              ){extra}
            ORDER BY dt.sort_order IS NULL, dt.sort_order, p.category, p.title
            """,
            tuple(params),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def get_published_policy(policy_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT p.*, dt.label AS document_type_label, dt.slug AS document_type_slug
            FROM comp_policies p
            LEFT JOIN comp_document_types dt ON dt.id = p.document_type_id
            WHERE p.id = %s AND p.published = 1
            LIMIT 1
            """,
            (int(policy_id),),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def contractor_has_acknowledged(contractor_id: int, policy_id: int, version: int) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1 FROM comp_policy_acknowledgements
            WHERE policy_id = %s AND contractor_id = %s AND version = %s LIMIT 1
            """,
            (int(policy_id), int(contractor_id), int(version)),
        )
        return bool(cur.fetchone())
    finally:
        cur.close()
        conn.close()


def acknowledge_policy(
    contractor_id: int,
    policy_id: int,
    remote_addr: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> Tuple[bool, str]:
    row = get_published_policy(policy_id)
    if not row:
        return False, "Policy not found or not published."
    ver = int(row["version"])
    if contractor_has_acknowledged(contractor_id, policy_id, ver):
        return True, "Already recorded."
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO comp_policy_acknowledgements
            (policy_id, contractor_id, version, ip_address, user_agent)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                int(policy_id),
                int(contractor_id),
                ver,
                (remote_addr or "")[:45] or None,
                (user_agent or "")[:512] or None,
            ),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cur.close()
        conn.close()

    try:
        from app.plugins.employee_portal_module.services import complete_todo_by_reference

        ref_id = _todo_reference_id(policy_id, ver, contractor_id)
        complete_todo_by_reference(SOURCE_MODULE, REF_TYPE_POLICY_ACK, ref_id)
    except Exception as e:
        logger.warning("Compliance: could not complete portal todo: %s", e)

    return True, "ok"


def _list_active_contractor_ids() -> List[int]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM tb_contractors WHERE status = 'active' ORDER BY id")
        return [int(r[0]) for r in (cur.fetchall() or [])]
    finally:
        cur.close()
        conn.close()


def _retire_pending_todos_for_policy_version(policy_id: int, old_version: int) -> None:
    """Mark obsolete portal todos complete for a superseded policy version."""
    prefix = f"p{int(policy_id)}_v{int(old_version)}_c"
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE ep_todos SET completed_at = NOW()
            WHERE source_module = %s AND reference_type = %s
              AND completed_at IS NULL
              AND reference_id LIKE %s
            """,
            (SOURCE_MODULE, REF_TYPE_POLICY_ACK, prefix + "%"),
        )
        conn.commit()
    except Exception as e:
        logger.warning("Compliance: retire old todos: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()


def _fan_out_portal_todos(policy_id: int, version: int, title: str, link_path: str) -> int:
    try:
        from app.plugins.employee_portal_module.services import upsert_pending_todo_for_reference
    except Exception as e:
        logger.error("Compliance: employee portal todos unavailable: %s", e)
        return 0
    count = 0
    ttl = (title or "Policy")[:240]
    for cid in _list_active_contractor_ids():
        ref = _todo_reference_id(policy_id, version, cid)
        try:
            upsert_pending_todo_for_reference(
                cid,
                SOURCE_MODULE,
                REF_TYPE_POLICY_ACK,
                ref,
                f"Read & acknowledge: {ttl} (v{version})",
                link_url=link_path,
                due_date=None,
            )
            count += 1
        except Exception as ex:
            logger.warning("Compliance: todo for contractor %s: %s", cid, ex)
    return count


# --- Admin ---


def admin_list_policies(
    document_type_id: Optional[int] = None,
    review_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    today = date.today()
    horizon = today + timedelta(days=30)
    clauses: List[str] = []
    params: List[Any] = []

    if document_type_id is not None:
        clauses.append("p.document_type_id = %s")
        params.append(int(document_type_id))

    rf = (review_filter or "").strip().lower()
    if rf == "overdue":
        clauses.append("p.published = 1")
        clauses.append("p.next_review_date IS NOT NULL")
        clauses.append("p.next_review_date < %s")
        params.append(today)
    elif rf == "due30":
        clauses.append("p.published = 1")
        clauses.append("p.next_review_date IS NOT NULL")
        clauses.append("p.next_review_date >= %s")
        clauses.append("p.next_review_date <= %s")
        params.extend([today, horizon])
    elif rf == "draft":
        clauses.append("p.published = 0")
    elif rf == "published":
        clauses.append("p.published = 1")
    elif rf == "unscheduled":
        clauses.append("p.published = 1")
        clauses.append("p.next_review_date IS NULL")

    where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    if rf in ("overdue", "due30", "unscheduled"):
        order_by = "p.next_review_date IS NULL, p.next_review_date ASC, p.title"
    else:
        order_by = "p.updated_at DESC"

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            f"""
            SELECT p.*,
              dt.label AS document_type_label,
              dt.slug AS document_type_slug,
              (SELECT COUNT(*) FROM comp_policy_acknowledgements a
               WHERE a.policy_id = p.id AND a.version = p.version) AS ack_count
            FROM comp_policies p
            LEFT JOIN comp_document_types dt ON dt.id = p.document_type_id
            {where_sql}
            ORDER BY {order_by}
            """,
            tuple(params),
        )
        rows = cur.fetchall() or []
        for row in rows:
            row["review_badge"] = None
            if not int(row.get("published") or 0):
                continue
            nrd = row.get("next_review_date")
            if not nrd:
                continue
            if isinstance(nrd, datetime):
                nrd_d = nrd.date()
            elif isinstance(nrd, date):
                nrd_d = nrd
            else:
                nrd_d = _parse_optional_date(str(nrd)[:10])
            if not nrd_d:
                continue
            if nrd_d < today:
                row["review_badge"] = "overdue"
            elif nrd_d <= horizon:
                row["review_badge"] = "due30"
        return rows
    finally:
        cur.close()
        conn.close()


def admin_get_policy(policy_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT p.*, dt.label AS document_type_label, dt.slug AS document_type_slug
            FROM comp_policies p
            LEFT JOIN comp_document_types dt ON dt.id = p.document_type_id
            WHERE p.id = %s
            LIMIT 1
            """,
            (int(policy_id),),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def _parse_document_type_id(raw: Any) -> Optional[int]:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def admin_save_policy(
    policy_id: Optional[int],
    title: str,
    document_type_id: Optional[Any],
    topic: Optional[str],
    summary: Optional[str],
    body_text: Optional[str],
    mandatory: bool,
    slug: Optional[str] = None,
    next_review_date: Optional[str] = None,
    last_reviewed_date: Optional[str] = None,
) -> Tuple[bool, str, Optional[int]]:
    title = (title or "").strip()
    if not title:
        return False, "Title required", None
    dtid = _parse_document_type_id(document_type_id)
    current_dtid = None
    if policy_id:
        existing = admin_get_policy(int(policy_id))
        if existing:
            current_dtid = existing.get("document_type_id")
    if dtid is None or not _valid_document_type_choice(dtid, current_dtid):
        return False, "Choose a document type from the list (configure types under Document types).", None
    topic_clean = (topic or "").strip()[:255]
    slug = (slug or "").strip() or _slugify(title)
    nrd = _parse_optional_date(next_review_date)
    lrd = _parse_optional_date(last_reviewed_date)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if policy_id:
            cur.execute(
                """
                UPDATE comp_policies
                SET title=%s, slug=%s, category=%s, document_type_id=%s, summary=%s, body_text=%s, mandatory=%s,
                    next_review_date=%s, last_reviewed_date=%s
                WHERE id=%s
                """,
                (
                    title[:255],
                    slug[:191],
                    topic_clean,
                    int(dtid),
                    summary or None,
                    body_text or None,
                    1 if mandatory else 0,
                    nrd,
                    lrd,
                    int(policy_id),
                ),
            )
            conn.commit()
            return True, "ok", int(policy_id)
        cur.execute(
            """
            INSERT INTO comp_policies
            (title, slug, category, document_type_id, summary, body_text, mandatory, published, version,
             next_review_date, last_reviewed_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 0, 1, %s, %s)
            """,
            (
                title[:255],
                slug[:191],
                topic_clean,
                int(dtid),
                summary or None,
                body_text or None,
                1 if mandatory else 0,
                nrd,
                lrd,
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


def admin_set_policy_file(policy_id: int, relative_path: Optional[str]) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE comp_policies SET file_path = %s WHERE id = %s",
            (relative_path, int(policy_id)),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()


def admin_issue_policy_to_staff(policy_id: int) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Publish or publish a new version: bump version if already published,
    create portal todos for all active contractors, retire old version todos.
    """
    row = admin_get_policy(policy_id)
    if not row:
        return False, "Policy not found", {}
    old_version = int(row.get("version") or 1)
    was_published = int(row.get("published") or 0) == 1
    new_version = old_version + 1 if was_published else old_version

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if was_published:
            _retire_pending_todos_for_policy_version(policy_id, old_version)
        cur.execute(
            """
            UPDATE comp_policies
            SET version = %s, published = 1, published_at = NOW()
            WHERE id = %s
            """,
            (new_version, int(policy_id)),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        return False, str(e), {}
    finally:
        cur.close()
        conn.close()

    link_path = f"/compliance/policy/{int(policy_id)}"
    n = 0
    if int(row.get("mandatory") or 0):
        n = _fan_out_portal_todos(
            policy_id, new_version, row.get("title") or "Policy", link_path
        )
        msg = f"Issued version {new_version}. Portal tasks created for {n} active staff."
    else:
        msg = f"Published version {new_version} (informational — no mandatory portal tasks)."
    return True, msg, {"version": new_version, "todos_created": n}


def admin_unpublish_policy(policy_id: int) -> Tuple[bool, str]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE comp_policies SET published = 0 WHERE id = %s",
            (int(policy_id),),
        )
        conn.commit()
        return True, "ok"
    except Exception as e:
        return False, str(e)
    finally:
        cur.close()
        conn.close()


def admin_list_acknowledgements_for_policy(policy_id: int, limit: int = 500) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT a.*, c.name AS contractor_name, c.email AS contractor_email
            FROM comp_policy_acknowledgements a
            JOIN tb_contractors c ON c.id = a.contractor_id
            WHERE a.policy_id = %s
            ORDER BY a.acknowledged_at DESC
            LIMIT %s
            """,
            (int(policy_id), int(limit)),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
