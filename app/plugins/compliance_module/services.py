"""
Compliance policies: publish, acknowledgements, employee portal todos.
"""
from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from app.objects import get_db_connection

logger = logging.getLogger(__name__)

SOURCE_MODULE = "compliance_module"
REF_TYPE_POLICY_ACK = "policy_ack"

POLICY_CATEGORIES = (
    "health_safety",
    "safeguarding",
    "privacy",
    "data_protection",
    "equal_opportunities",
    "disciplinary",
    "other",
)

CATEGORY_LABELS = {
    "health_safety": "Health & safety",
    "safeguarding": "Safeguarding",
    "privacy": "Privacy",
    "data_protection": "Data protection",
    "equal_opportunities": "Equal opportunities",
    "disciplinary": "Disciplinary / conduct",
    "other": "Other",
}


def human_category(code: Optional[str]) -> str:
    if not code:
        return CATEGORY_LABELS["other"]
    return CATEGORY_LABELS.get(str(code).strip().lower(), str(code).replace("_", " ").title())


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


def list_optional_published_policies() -> List[Dict[str, Any]]:
    """Published policies that are not mandatory (reference / awareness)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT * FROM comp_policies
            WHERE published = 1 AND mandatory = 0
            ORDER BY title
            """
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


def list_acknowledged_mandatory_for_contractor(contractor_id: int) -> List[Dict[str, Any]]:
    """Published mandatory policies this contractor has acknowledged at the current version (policy library)."""
    if not contractor_id:
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT p.*, a.acknowledged_at AS my_acknowledged_at
            FROM comp_policies p
            INNER JOIN comp_policy_acknowledgements a
              ON a.policy_id = p.id AND a.contractor_id = %s AND a.version = p.version
            WHERE p.published = 1 AND p.mandatory = 1
            ORDER BY p.category, p.title
            """,
            (int(contractor_id),),
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


def list_pending_policies_for_contractor(contractor_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT p.* FROM comp_policies p
            WHERE p.published = 1 AND p.mandatory = 1
              AND NOT EXISTS (
                SELECT 1 FROM comp_policy_acknowledgements a
                WHERE a.policy_id = p.id AND a.contractor_id = %s AND a.version = p.version
              )
            ORDER BY p.category, p.title
            """,
            (int(contractor_id),),
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
            "SELECT * FROM comp_policies WHERE id = %s AND published = 1 LIMIT 1",
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


def admin_list_policies() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT p.*,
              (SELECT COUNT(*) FROM comp_policy_acknowledgements a
               WHERE a.policy_id = p.id AND a.version = p.version) AS ack_count
            FROM comp_policies p
            ORDER BY p.updated_at DESC
            """
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_get_policy(policy_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM comp_policies WHERE id = %s", (int(policy_id),))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def admin_save_policy(
    policy_id: Optional[int],
    title: str,
    category: str,
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
    cat = (category or "other").strip().lower()
    if cat not in POLICY_CATEGORIES:
        cat = "other"
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
                SET title=%s, slug=%s, category=%s, summary=%s, body_text=%s, mandatory=%s,
                    next_review_date=%s, last_reviewed_date=%s
                WHERE id=%s
                """,
                (
                    title[:255],
                    slug[:191],
                    cat,
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
            (title, slug, category, summary, body_text, mandatory, published, version,
             next_review_date, last_reviewed_date)
            VALUES (%s, %s, %s, %s, %s, %s, 0, 1, %s, %s)
            """,
            (
                title[:255],
                slug[:191],
                cat,
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
