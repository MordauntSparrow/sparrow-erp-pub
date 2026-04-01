"""
Employee Portal services: dashboard data, module links, and safe helpers.
Production-ready: defensive loading, logging, no raw secrets in logs.
Admin: contractor search, message/todo list and CRUD.
"""
import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from app.objects import get_db_connection

logger = logging.getLogger(__name__)

# Limits for dashboard lists (avoid unbounded queries; mobile-friendly)
LIMIT_MESSAGES = 50
# Pending = default dashboard view (hide completed unless user opens Completed / All)
LIMIT_TODOS_PENDING = 40
LIMIT_TODOS_COMPLETED = 25
LIMIT_TODOS_ALL = 35

# Single source of truth for portal module links (name, url, icon, plugin system_name)
# use_launch=True: dashboard link goes via /employee-portal/go/<slug> so auth is passed by token (avoids session/cookie issues)
MODULE_LINKS_CONFIG = [
    {"name": "Time & Billing", "url": "/time-billing/", "icon": "bi-clock-history",
        "system_name": "time_billing_module", "launch_slug": "time-billing"},
    {"name": "Work", "url": "/work/", "icon": "bi-briefcase",
        "system_name": "work_module", "launch_slug": None},
    {"name": "HR", "url": "/hr/", "icon": "bi-person-badge",
        "system_name": "hr_module", "launch_slug": None},
    {"name": "Compliance & Policies", "url": "/compliance/", "icon": "bi-shield-check",
        "system_name": "compliance_module", "launch_slug": None},
    {"name": "Training", "url": "/training/", "icon": "bi-mortarboard",
        "system_name": "training_module", "launch_slug": None},
    {"name": "Scheduling & Shifts", "url": "/scheduling/", "icon": "bi-calendar-week",
        "system_name": "scheduling_module", "launch_slug": None},
    {"name": "Fleet", "url": "/fleet/", "icon": "bi-truck-front",
        "system_name": "fleet_management", "launch_slug": None},
]


def safe_profile_picture_path(path):
    """
    Return path only if it looks safe for static serving (no path traversal, no absolute).
    Otherwise return None so the UI falls back to initials.
    """
    if not path or not isinstance(path, str):
        return None
    cleaned = path.strip()
    if ".." in cleaned or cleaned.startswith("/") or re.match(r"^[a-zA-Z]:", cleaned):
        return None
    # Allow alphanumeric, slash, hyphen, underscore (e.g. uploads/contractors/123.jpg)
    if not re.match(r"^[\w/.\-]+$", cleaned):
        return None
    return cleaned


def safe_next_url(next_param, default, request=None):
    """
    Validate redirect target to prevent open redirects.
    Only allow relative paths (e.g. /employee-portal/ or /time-billing/).
    Reject //, protocol-relative, or URLs with scheme in first segment.
    """
    if not next_param or not isinstance(next_param, str):
        return default
    s = next_param.strip()
    if not s or not s.startswith("/") or s.startswith("//"):
        return default
    parts = s.split("/")
    if len(parts) < 2 or not parts[1]:
        return default
    if ":" in parts[1]:
        return default
    return s


def get_messages(contractor_id):
    """Load messages for the dashboard. Returns list; empty on error (logged)."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT id, subject, body, read_at, created_at, source_module
                FROM ep_messages
                WHERE contractor_id = %s AND (deleted_at IS NULL)
                ORDER BY created_at DESC
                LIMIT %s
            """, (contractor_id, LIMIT_MESSAGES))
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.warning(
            "Employee portal: messages unavailable for contractor %s (run install if needed): %s", contractor_id, e)
        return []


def count_pending_todos(contractor_id) -> int:
    """Cheap COUNT for badge and summaries (does not load row bodies)."""
    if not contractor_id:
        return 0
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT COUNT(*) FROM ep_todos
                WHERE contractor_id = %s AND completed_at IS NULL
                """,
                (int(contractor_id),),
            )
            row = cur.fetchone()
            return int(row[0] if row else 0)
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.warning(
            "Employee portal: pending todo count unavailable for contractor %s: %s",
            contractor_id,
            e,
        )
        return 0


def get_todos(contractor_id, filter_completed=None, limit=None):
    """
    Load todos for the dashboard.
    filter_completed: None=all (pending first, then recent completed), True=completed only, False=pending only.
    limit: override max rows (default depends on filter).
    """
    try:
        if filter_completed is True:
            cap = int(limit) if limit is not None else LIMIT_TODOS_COMPLETED
            order_sql = "ORDER BY completed_at DESC, created_at DESC"
        elif filter_completed is False:
            cap = int(limit) if limit is not None else LIMIT_TODOS_PENDING
            order_sql = "ORDER BY due_date IS NULL ASC, due_date ASC, created_at DESC"
        else:
            cap = int(limit) if limit is not None else LIMIT_TODOS_ALL
            order_sql = (
                "ORDER BY completed_at IS NULL DESC, due_date IS NULL ASC, "
                "due_date ASC, created_at DESC"
            )
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = "contractor_id = %s"
            params = [contractor_id]
            if filter_completed is True:
                where += " AND completed_at IS NOT NULL"
            elif filter_completed is False:
                where += " AND completed_at IS NULL"
            cur.execute(
                f"""
                SELECT id, source_module, title, link_url, due_date, completed_at, created_at
                FROM ep_todos
                WHERE {where}
                {order_sql}
                LIMIT %s
                """,
                tuple(params) + (cap,),
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.warning(
            "Employee portal: todos unavailable for contractor %s (run install if needed): %s", contractor_id, e)
        return []


def get_module_links(plugin_manager):
    """
    Return list of module link dicts with 'enabled' set from plugin manifest.
    Each item: name, url, icon, system_name, enabled.
    """
    result = []
    for mod in MODULE_LINKS_CONFIG:
        item = dict(mod)
        try:
            item["enabled"] = bool(
                plugin_manager.is_plugin_enabled(mod["system_name"]))
        except Exception:
            item["enabled"] = False
        result.append(item)
    return result


def get_pending_counts(contractor_id):
    """Return (pending_policies, pending_hr_requests). Uses 0 on import or runtime errors."""
    pending_policies = 0
    pending_hr_requests = 0
    try:
        from app.plugins.compliance_module.services import pending_policies_count
        pending_policies = pending_policies_count(contractor_id)
    except Exception as e:
        logger.debug(
            "Employee portal: compliance pending count unavailable: %s", e)
    try:
        from app.plugins.hr_module.services import pending_requests_count
        pending_hr_requests = pending_requests_count(contractor_id)
    except Exception as e:
        logger.debug("Employee portal: HR pending count unavailable: %s", e)
    return pending_policies, pending_hr_requests


def get_pending_training_count(contractor_id):
    """Return count of incomplete training assignments for the contractor. Uses 0 on import or runtime errors."""
    if not contractor_id:
        return 0
    try:
        from app.plugins.training_module.services import TrainingService

        return int(TrainingService.count_pending_for_contractor(int(contractor_id)))
    except Exception as e:
        logger.debug(
            "Employee portal: training pending count unavailable: %s", e)
        return 0


def get_dashboard_summary_context(contractor_id: int) -> Dict[str, Any]:
    """Return structured summary for dashboard and AI: counts and pending todo titles."""
    pending_policies, pending_hr_requests = get_pending_counts(contractor_id)
    pending_training = get_pending_training_count(contractor_id)
    messages = get_messages(contractor_id)
    unread = sum(1 for m in messages if not m.get("read_at"))
    pending_todo_count = count_pending_todos(contractor_id)
    pending_todos = get_todos(contractor_id, filter_completed=False, limit=10)
    todo_titles = [t.get("title") or "" for t in (
        pending_todos or []) if t.get("title")]
    out = {
        "pending_policies": pending_policies,
        "pending_hr_requests": pending_hr_requests,
        "pending_training": pending_training,
        "unread_messages": unread,
        "pending_todo_count": pending_todo_count,
        "todo_titles": todo_titles,
    }
    try:
        from app.plugins.scheduling_module.services import ScheduleService
        out["scheduling_summary"] = ScheduleService.get_contractor_portal_summary(
            contractor_id)
    except Exception:
        out["scheduling_summary"] = None
    return out


def is_scheduling_enabled(plugin_manager):
    """True if scheduling_module is enabled (for quick actions visibility)."""
    try:
        return bool(plugin_manager.is_plugin_enabled("scheduling_module"))
    except Exception:
        return False


def equipment_portal_enabled(plugin_manager) -> bool:
    """Serial kit / consumables self-service when inventory (includes serial equipment) is on."""
    try:
        return bool(plugin_manager.is_plugin_enabled("inventory_control"))
    except Exception:
        return False


def inventory_contractor_requests_portal_enabled(plugin_manager) -> bool:
    """Contractor stock/material requests live under inventory_control (/inventory/...), not the portal module."""
    try:
        return bool(plugin_manager.is_plugin_enabled("inventory_control"))
    except Exception:
        return False


def contractor_assigned_equipment_count(contractor_id: int) -> int:
    """How many serial assets are signed out to this contractor (for dashboard badge)."""
    if not contractor_id:
        return 0
    try:
        from app.plugins.inventory_control.asset_service import get_asset_service

        rows = get_asset_service().list_assets_held_by_contractor(int(contractor_id))
        return len(rows or [])
    except Exception as e:
        logger.debug("contractor_assigned_equipment_count: %s", e)
        return 0


# -----------------------------------------------------------------------------
# Admin: contractor search
# -----------------------------------------------------------------------------


def admin_list_contractors_for_select(limit: int = 500) -> List[Dict[str, Any]]:
    """List active contractors id, name, email for admin dropdowns (e.g. send message to)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, name, email
            FROM tb_contractors
            WHERE status IN ('active', '1', 1) OR status IS NULL
            ORDER BY name ASC
            LIMIT %s
        """, (limit,))
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def admin_search_contractors(
    q: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Search tb_contractors by name or email (active only). Returns id, name, email, initials, status."""
    if not q or not q.strip():
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        term = "%" + q.strip().lower() + "%"
        cur.execute("""
            SELECT id, name, email, initials, status
            FROM tb_contractors
            WHERE status IN ('active', '1', 1) OR status IS NULL
              AND (LOWER(name) LIKE %s OR LOWER(email) LIKE %s)
            ORDER BY name ASC
            LIMIT %s
        """, (term, term, limit))
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


# -----------------------------------------------------------------------------
# Admin: messages list and send
# -----------------------------------------------------------------------------


def admin_list_messages(
    contractor_id: Optional[int] = None,
    source_module: Optional[str] = None,
    read_status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    include_deleted: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    """List messages with filters. Returns (rows, total_count). If include_deleted, show soft-deleted too."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["1=1"]
        if not include_deleted:
            where.append("(m.deleted_at IS NULL)")
        params: List[Any] = []
        if contractor_id is not None:
            where.append("m.contractor_id = %s")
            params.append(contractor_id)
        if source_module:
            where.append("m.source_module = %s")
            params.append(source_module)
        if read_status == "read":
            where.append("m.read_at IS NOT NULL")
        elif read_status == "unread":
            where.append("m.read_at IS NULL")
        if date_from:
            where.append("DATE(m.created_at) >= %s")
            params.append(date_from)
        if date_to:
            where.append("DATE(m.created_at) <= %s")
            params.append(date_to)
        params_count = list(params)
        params.extend([limit, offset])
        cur.execute(f"""
            SELECT SQL_CALC_FOUND_ROWS m.id, m.contractor_id, m.source_module, m.subject, m.body,
                   m.read_at, m.created_at, m.sent_by_user_id, m.deleted_at,
                   c.name AS contractor_name, c.email AS contractor_email
            FROM ep_messages m
            JOIN tb_contractors c ON c.id = m.contractor_id
            WHERE {" AND ".join(where)}
            ORDER BY m.deleted_at IS NULL DESC, m.created_at DESC
            LIMIT %s OFFSET %s
        """, params)
        rows = cur.fetchall() or []
        cur.execute("SELECT FOUND_ROWS() AS total")
        total = (cur.fetchone() or {}).get("total") or 0
        return rows, total
    finally:
        cur.close()
        conn.close()


def admin_send_message(
    contractor_ids: List[int],
    subject: str,
    body: Optional[str] = None,
    source_module: str = "employee_portal_module",
    sent_by_user_id: Optional[int] = None,
) -> int:
    """Insert one message per contractor_id. Returns count inserted."""
    if not contractor_ids or not subject.strip():
        return 0
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        count = 0
        inserted: List[Tuple[int, int]] = []
        for cid in contractor_ids:
            cur.execute("""
                INSERT INTO ep_messages (contractor_id, source_module, subject, body, sent_by_user_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (cid, source_module, (subject or "").strip()[:255], (body or "")[:65535], sent_by_user_id))
            count += cur.rowcount
            mid = cur.lastrowid
            if mid:
                inserted.append((int(cid), int(mid)))
        conn.commit()
        if inserted:
            try:
                from .push_service import schedule_push_for_new_portal_messages

                schedule_push_for_new_portal_messages(inserted)
            except Exception:
                logger.debug(
                    "portal message push schedule skipped", exc_info=True)
        return count
    finally:
        cur.close()
        conn.close()


def admin_soft_delete_message(msg_id: int) -> bool:
    """Set deleted_at = NOW() for message. Returns True if updated."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE ep_messages SET deleted_at = NOW() WHERE id = %s", (msg_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()


def admin_restore_message(msg_id: int) -> bool:
    """Clear deleted_at for message. Returns True if updated."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE ep_messages SET deleted_at = NULL WHERE id = %s", (msg_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()


def get_message_by_id_for_contractor(contractor_id: int, msg_id: int) -> Optional[Dict[str, Any]]:
    """Get a single message by id belonging to contractor (not deleted)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, subject, body, read_at, created_at, source_module
            FROM ep_messages
            WHERE id = %s AND contractor_id = %s AND (deleted_at IS NULL)
        """, (msg_id, contractor_id))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def mark_message_read(msg_id: int, contractor_id: int) -> bool:
    """Set read_at = NOW() for message if it belongs to contractor. Returns True if updated."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE ep_messages SET read_at = NOW() WHERE id = %s AND contractor_id = %s",
            (msg_id, contractor_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()


# -----------------------------------------------------------------------------
# Admin: todos list and CRUD
# -----------------------------------------------------------------------------


def admin_list_todos(
    contractor_id: Optional[int] = None,
    source_module: Optional[str] = None,
    completed: Optional[bool] = None,
    due_date_from: Optional[date] = None,
    due_date_to: Optional[date] = None,
    limit: int = 100,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    """List todos with filters. Returns (rows, total_count)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["1=1"]
        params: List[Any] = []
        if contractor_id is not None:
            where.append("t.contractor_id = %s")
            params.append(contractor_id)
        if source_module:
            where.append("t.source_module = %s")
            params.append(source_module)
        if completed is True:
            where.append("t.completed_at IS NOT NULL")
        elif completed is False:
            where.append("t.completed_at IS NULL")
        if due_date_from is not None:
            where.append("t.due_date >= %s")
            params.append(due_date_from)
        if due_date_to is not None:
            where.append("t.due_date <= %s")
            params.append(due_date_to)
        params.extend([limit, offset])
        cur.execute(f"""
            SELECT SQL_CALC_FOUND_ROWS t.id, t.contractor_id, t.source_module, t.title, t.link_url,
                   t.due_date, t.completed_at, t.created_at, t.created_by_user_id,
                   c.name AS contractor_name, c.email AS contractor_email
            FROM ep_todos t
            JOIN tb_contractors c ON c.id = t.contractor_id
            WHERE {" AND ".join(where)}
            ORDER BY t.completed_at IS NULL DESC, t.due_date IS NULL ASC, t.due_date ASC, t.created_at DESC
            LIMIT %s OFFSET %s
        """, params)
        rows = cur.fetchall() or []
        cur.execute("SELECT FOUND_ROWS() AS total")
        total = (cur.fetchone() or {}).get("total") or 0
        return rows, total
    finally:
        cur.close()
        conn.close()


def admin_create_todo(
    contractor_ids: List[int],
    title: str,
    link_url: Optional[str] = None,
    due_date: Optional[date] = None,
    source_module: str = "employee_portal_module",
    created_by_user_id: Optional[int] = None,
    reference_type: Optional[str] = None,
    reference_id: Optional[str] = None,
) -> int:
    """Insert one todo per contractor_id. Returns count inserted. Optional reference_type/reference_id for linking (e.g. schedule_shift_task)."""
    if not contractor_ids or not title.strip():
        return 0
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        count = 0
        ref_type = (reference_type or "")[:64] or None
        ref_id = (reference_id or "")[:128] or None
        link_stored = (link_url or "")[:512] or None
        inserted: List[Tuple[int, int, Optional[str]]] = []
        for cid in contractor_ids:
            cur.execute("""
                INSERT INTO ep_todos (contractor_id, source_module, title, link_url, due_date, created_by_user_id, reference_type, reference_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (cid, source_module, (title or "").strip()[:255], link_stored, due_date, created_by_user_id, ref_type, ref_id))
            count += cur.rowcount
            tid = cur.lastrowid
            if tid:
                inserted.append((int(cid), int(tid), link_stored))
        conn.commit()
        if inserted:
            try:
                from .push_service import schedule_push_for_new_portal_todos

                schedule_push_for_new_portal_todos(inserted)
            except Exception:
                logger.debug(
                    "portal todo push schedule skipped", exc_info=True)
        return count
    finally:
        cur.close()
        conn.close()


def get_todo_by_reference(
    source_module: str,
    reference_type: str,
    reference_id: str,
) -> Optional[Dict[str, Any]]:
    """Return first matching todo (any contractor) with given reference, or None."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, contractor_id, source_module, title, completed_at
            FROM ep_todos
            WHERE source_module = %s AND reference_type = %s AND reference_id = %s
            LIMIT 1
        """, (source_module, (reference_type or "")[:64], (reference_id or "")[:128]))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def complete_todo_by_reference(
    source_module: str,
    reference_type: str,
    reference_id: str,
) -> bool:
    """Mark todo(s) matching reference as completed. Returns True if any row updated."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE ep_todos
            SET completed_at = NOW()
            WHERE source_module = %s AND reference_type = %s AND reference_id = %s AND completed_at IS NULL
        """, (source_module, (reference_type or "")[:64], (reference_id or "")[:128]))
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()


def upsert_pending_todo_for_reference(
    contractor_id: int,
    source_module: str,
    reference_type: str,
    reference_id: str,
    title: str,
    link_url: Optional[str] = None,
    due_date: Optional[date] = None,
) -> None:
    """
    If a pending todo exists for this contractor + reference, refresh title/link/due_date.
    Otherwise insert a new pending todo. Used by HR document requests (reject / new request).
    """
    if not title or not str(title).strip():
        return
    ttl = str(title).strip()[:255]
    link = (link_url or "")[:512] or None
    ref_type = (reference_type or "")[:64] or None
    ref_id = (reference_id or "")[:128] or None
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE ep_todos
            SET title = %s, link_url = %s, due_date = %s
            WHERE contractor_id = %s AND source_module = %s
              AND reference_type <=> %s AND reference_id <=> %s
              AND completed_at IS NULL
            """,
            (ttl, link, due_date, contractor_id, source_module, ref_type, ref_id),
        )
        if cur.rowcount == 0:
            cur.execute(
                """
                INSERT INTO ep_todos (
                    contractor_id, source_module, title, link_url, due_date,
                    created_by_user_id, reference_type, reference_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (contractor_id, source_module, ttl,
                 link, due_date, None, ref_type, ref_id),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def admin_get_todo(todo_id: int) -> Optional[Dict[str, Any]]:
    """Get a single todo by id."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT t.*, c.name AS contractor_name, c.email AS contractor_email
            FROM ep_todos t
            JOIN tb_contractors c ON c.id = t.contractor_id
            WHERE t.id = %s
        """, (todo_id,))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def admin_update_todo(
    todo_id: int,
    title: Optional[str] = None,
    link_url: Optional[str] = None,
    due_date: Optional[date] = None,
) -> bool:
    """Update todo title, link_url, due_date. Returns True if updated."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        updates = []
        params: List[Any] = []
        if title is not None:
            updates.append("title = %s")
            params.append((title or "").strip()[:255])
        if link_url is not None:
            updates.append("link_url = %s")
            params.append((link_url or "")[:512] or None)
        if due_date is not None:
            updates.append("due_date = %s")
            params.append(due_date)
        if not updates:
            return True
        params.append(todo_id)
        cur.execute("UPDATE ep_todos SET " +
                    ", ".join(updates) + " WHERE id = %s", params)
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()


def admin_set_todo_complete(todo_id: int, complete: bool = True) -> bool:
    """Set completed_at to NOW() or NULL. Returns True if updated."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if complete:
            cur.execute(
                "UPDATE ep_todos SET completed_at = NOW() WHERE id = %s", (todo_id,))
        else:
            cur.execute(
                "UPDATE ep_todos SET completed_at = NULL WHERE id = %s", (todo_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()


def admin_bulk_complete_todos(todo_ids: List[int], complete: bool = True) -> int:
    """Set completed_at for multiple todos. Returns count updated."""
    if not todo_ids:
        return 0
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        placeholders = ",".join(["%s"] * len(todo_ids))
        if complete:
            cur.execute(
                f"UPDATE ep_todos SET completed_at = NOW() WHERE id IN ({placeholders})",
                tuple(todo_ids),
            )
        else:
            cur.execute(
                f"UPDATE ep_todos SET completed_at = NULL WHERE id IN ({placeholders})",
                tuple(todo_ids),
            )
        conn.commit()
        return cur.rowcount
    finally:
        cur.close()
        conn.close()


def admin_get_portal_stats() -> Dict[str, Any]:
    """Return counts for admin dashboard: total_messages, unread_messages, total_todos, pending_todos."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM ep_messages WHERE deleted_at IS NULL) AS total_messages,
                (SELECT COUNT(*) FROM ep_messages WHERE deleted_at IS NULL AND read_at IS NULL) AS unread_messages,
                (SELECT COUNT(*) FROM ep_todos) AS total_todos,
                (SELECT COUNT(*) FROM ep_todos WHERE completed_at IS NULL) AS pending_todos
        """)
        row = cur.fetchone() or {}
        return {
            "total_messages": row.get("total_messages") or 0,
            "unread_messages": row.get("unread_messages") or 0,
            "total_todos": row.get("total_todos") or 0,
            "pending_todos": row.get("pending_todos") or 0,
        }
    finally:
        cur.close()
        conn.close()


def admin_get_report_stats() -> Dict[str, Any]:
    """Return message and todo stats by source_module for Reports page."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT source_module,
                   COUNT(*) AS total,
                   SUM(CASE WHEN read_at IS NOT NULL THEN 1 ELSE 0 END) AS read_count,
                   SUM(CASE WHEN read_at IS NULL THEN 1 ELSE 0 END) AS unread_count
            FROM ep_messages
            WHERE deleted_at IS NULL
            GROUP BY source_module
            ORDER BY source_module
        """)
        messages_by_module = cur.fetchall() or []
        cur.execute("""
            SELECT source_module,
                   COUNT(*) AS total,
                   SUM(CASE WHEN completed_at IS NOT NULL THEN 1 ELSE 0 END) AS completed_count,
                   SUM(CASE WHEN completed_at IS NULL THEN 1 ELSE 0 END) AS pending_count
            FROM ep_todos
            GROUP BY source_module
            ORDER BY source_module
        """)
        todos_by_module = cur.fetchall() or []
        return {"messages_by_module": messages_by_module, "todos_by_module": todos_by_module}
    finally:
        cur.close()
        conn.close()


def get_ep_setting(key: str) -> Optional[str]:
    """Get a value from ep_settings. Returns None if not set."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT setting_value FROM ep_settings WHERE setting_key = %s", (key,))
        row = cur.fetchone()
        return (row.get("setting_value") or "").strip() or None if row else None
    except Exception as e:
        logger.warning("get_ep_setting %s: %s", key, e)
        return None
    finally:
        cur.close()
        conn.close()


def set_ep_setting(key: str, value: Optional[str]) -> bool:
    """Set a value in ep_settings (insert or update). Returns True on success."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO ep_settings (setting_key, setting_value) VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value)
            """, (key, (value or "").strip() or None))
            conn.commit()
            return True
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.warning("set_ep_setting %s: %s", key, e)
        return False


def get_contractor_theme(contractor_id: int) -> Optional[str]:
    """Get stored portal theme: tb_contractors.ui_theme first, then legacy ep_settings key."""
    if not contractor_id:
        return None
    try:
        from app.contractor_ui_theme import get_stored_preference_for_contractor

        col = get_stored_preference_for_contractor(int(contractor_id))
        if col:
            return col
    except Exception:
        pass
    key = f"portal_theme:{contractor_id}"
    val = get_ep_setting(key)
    if val and val.lower() in ("light", "dark", "auto", "system"):
        pref = val.lower() if val.lower() != "system" else "auto"
        try:
            from app.contractor_ui_theme import set_contractor_ui_theme_column

            set_contractor_ui_theme_column(int(contractor_id), pref)
        except Exception:
            pass
        return pref
    return None


def set_contractor_theme(contractor_id: int, theme: str) -> bool:
    """Persist theme on contractor row; also ep_settings for any legacy readers."""
    if not contractor_id:
        return False
    try:
        from app.contractor_ui_theme import set_contractor_ui_theme_column

        set_contractor_ui_theme_column(int(contractor_id), theme)
    except Exception:
        pass
    key = f"portal_theme:{contractor_id}"
    return set_ep_setting(key, theme)


def resolve_theme_by_time() -> str:
    """Resolve 'auto' theme to 'light' or 'dark' by time of day (UTC). 06:00–22:00 = light, else dark. Consistent across cluster."""
    from datetime import datetime
    hour = datetime.utcnow().hour
    return "light" if 6 <= hour < 22 else "dark"


# -----------------------------------------------------------------------------
# Admin: portal preview (dashboard data for a contractor)
# -----------------------------------------------------------------------------


def get_dashboard_data_for_contractor(
    contractor_id: int,
    plugin_manager,
) -> Dict[str, Any]:
    """Return the same data structure the contractor dashboard needs: user, messages, todos, module_links, pending_policies, pending_hr_requests, scheduling_enabled."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, email, name, initials, status, profile_picture_path FROM tb_contractors WHERE id = %s",
            (contractor_id,),
        )
        row = cur.fetchone()
        if not row:
            return {}
    finally:
        cur.close()
        conn.close()
    user = dict(row)
    user["profile_picture_path"] = safe_profile_picture_path(
        user.get("profile_picture_path"))
    user["id"] = int(user["id"])
    messages = get_messages(contractor_id)
    todos = get_todos(contractor_id, filter_completed=False)
    unread_message_count = sum(1 for m in messages if not m.get("read_at"))
    pending_todo_count = count_pending_todos(contractor_id)
    pending_policies, pending_hr_requests = get_pending_counts(contractor_id)
    pending_training = get_pending_training_count(contractor_id)
    module_links = get_module_links(plugin_manager)
    scheduling_enabled = is_scheduling_enabled(plugin_manager)
    welcome_message = get_ep_setting("welcome_message")
    return {
        "user": user,
        "messages": messages,
        "todos": todos,
        "unread_message_count": unread_message_count,
        "pending_todo_count": pending_todo_count,
        "pending_policies": pending_policies,
        "pending_hr_requests": pending_hr_requests,
        "pending_training": pending_training,
        "module_links": module_links,
        "scheduling_enabled": scheduling_enabled,
        "welcome_message": welcome_message,
    }
