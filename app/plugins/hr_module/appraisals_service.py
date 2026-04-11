"""
Staff appraisals (HR-APPR-001): cycles, goals, signatures, optional attachment.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from app.objects import get_db_connection

logger = logging.getLogger(__name__)


def appraisal_tables_ready() -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'hr_appraisal'")
        return bool(cur.fetchone())
    finally:
        cur.close()
        conn.close()


def appraisal_event_log_ready() -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'hr_appraisal_event_log'")
        return bool(cur.fetchone())
    finally:
        cur.close()
        conn.close()


def human_appraisal_event_type(value: Optional[str]) -> str:
    if value is None or str(value).strip() == "":
        return "—"
    k = str(value).strip().lower()
    return {
        "created": "Created",
        "updated": "Updated",
        "deleted": "Deleted",
        "employee_signed": "Employee signed",
        "manager_signed": "Manager signed",
        "employee_sign_cleared": "Employee signature cleared",
        "manager_sign_cleared": "Manager signature cleared",
        "status_changed": "Status changed",
        "attachment_added": "Attachment added",
        "attachment_removed": "Attachment removed",
        "daily_appraisal_scan": "Daily reminder scan",
        "reminder_item": "Reminder item",
        "digest_email_sent": "Digest email sent",
        "digest_email_failed": "Digest email failed",
    }.get(k, k.replace("_", " ").title())


def log_appraisal_event(
    event_type: str,
    summary: str,
    *,
    appraisal_id: Optional[int] = None,
    contractor_id: Optional[int] = None,
    detail: Optional[str] = None,
    channel: Optional[str] = None,
    actor_user_id: Optional[str] = None,
) -> None:
    if not appraisal_event_log_ready():
        return
    cid = contractor_id
    if cid is None and appraisal_id is not None:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT contractor_id FROM hr_appraisal WHERE id = %s", (int(appraisal_id),)
            )
            r = cur.fetchone()
            if r:
                cid = int(r[0])
        except Exception:
            cid = None
        finally:
            cur.close()
            conn.close()
    et = (event_type or "event")[:64]
    sm = (summary or "")[:512]
    if len(detail or "") > 65000:
        detail = (detail or "")[:65000]
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO hr_appraisal_event_log
            (appraisal_id, contractor_id, event_type, summary, detail, channel, actor_user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(appraisal_id) if appraisal_id is not None else None,
                int(cid) if cid is not None else None,
                et,
                sm,
                detail,
                (channel or None) and (channel[:32]),
                (actor_user_id or None) and str(actor_user_id)[:36],
            ),
        )
        conn.commit()
    except Exception as exc:
        logger.warning("log_appraisal_event: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()


def list_appraisal_events(appraisal_id: int, limit: int = 100) -> List[Dict[str, Any]]:
    if not appraisal_event_log_ready():
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, appraisal_id, contractor_id, event_type, summary, detail, channel,
                   actor_user_id, created_at
            FROM hr_appraisal_event_log
            WHERE appraisal_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            (int(appraisal_id), int(limit)),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def list_all_appraisal_events(
    limit: int = 150,
    appraisal_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if not appraisal_event_log_ready():
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        sql = """
            SELECT e.id, e.appraisal_id, e.contractor_id, e.event_type, e.summary, e.detail,
                   e.channel, e.actor_user_id, e.created_at,
                   c.name AS employee_name, c.email AS employee_email
            FROM hr_appraisal_event_log e
            LEFT JOIN tb_contractors c ON c.id = e.contractor_id
        """
        params: Tuple[Any, ...] = ()
        if appraisal_id is not None:
            sql += " WHERE e.appraisal_id = %s"
            params = (int(appraisal_id),)
        sql += " ORDER BY e.created_at DESC, e.id DESC LIMIT %s"
        params = params + (int(limit),)
        cur.execute(sql, params)
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()


def list_appraisals_needing_attention(
    reminder_days: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    Open/draft appraisals: overdue (past period_end), due on reminder day windows,
    or missing period_end. Requires period_end for date-based buckets.
    """
    if not appraisal_tables_ready():
        return []
    from datetime import date as date_cls

    today = date_cls.today()
    days_set = set(reminder_days if reminder_days is not None else [30, 14, 7, 1, 0])
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT a.id, a.contractor_id, a.cycle_label, a.period_end, a.status,
                   c.name AS employee_name, c.email AS employee_email
            FROM hr_appraisal a
            INNER JOIN tb_contractors c ON c.id = a.contractor_id
            WHERE a.status IN ('draft', 'open')
            ORDER BY a.period_end IS NULL, a.period_end ASC, a.id ASC
            """
        )
        rows = list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()
    out: List[Dict[str, Any]] = []
    for r in rows:
        pe = _parse_opt_date(r.get("period_end"))
        aid = int(r["id"])
        cid = int(r["contractor_id"])
        name = r.get("employee_name") or r.get("employee_email") or f"#{cid}"
        label = (r.get("cycle_label") or "Appraisal")[:120]
        if pe is None:
            out.append(
                {
                    "appraisal_id": aid,
                    "contractor_id": cid,
                    "bucket": "missing_period_end",
                    "days_remaining": None,
                    "line": f"[No end date] #{aid} {name} — “{label}” has no period end; set a date for due reminders.",
                    "employee_name": name,
                    "cycle_label": label,
                    "period_end": None,
                }
            )
            continue
        delta = (pe - today).days
        if delta < 0:
            out.append(
                {
                    "appraisal_id": aid,
                    "contractor_id": cid,
                    "bucket": "overdue",
                    "days_remaining": delta,
                    "line": f"[OVERDUE {-delta}d] #{aid} {name} — “{label}” period ended {pe.isoformat()} (status: {r.get('status')}).",
                    "employee_name": name,
                    "cycle_label": label,
                    "period_end": pe,
                }
            )
        elif delta in days_set:
            word = "due today" if delta == 0 else f"due in {delta} day(s)"
            out.append(
                {
                    "appraisal_id": aid,
                    "contractor_id": cid,
                    "bucket": "due_soon",
                    "days_remaining": delta,
                    "line": f"[{word}] #{aid} {name} — “{label}” period ends {pe.isoformat()}.",
                    "employee_name": name,
                    "cycle_label": label,
                    "period_end": pe,
                }
            )
    return out


def appraisal_attention_counts() -> Dict[str, int]:
    """Uses manifest ``appraisal_reminder_days`` (same as list filter and daily scan)."""
    try:
        from .appraisals_reminders import get_reminder_days_from_manifest

        days = get_reminder_days_from_manifest()
    except Exception:
        days = [30, 14, 7, 1, 0]
    items = list_appraisals_needing_attention(reminder_days=days)
    c = {"missing_period_end": 0, "overdue": 0, "due_soon": 0}
    for it in items:
        b = it.get("bucket") or ""
        if b in c:
            c[b] += 1
    c["total"] = len(items)
    return c


def human_appraisal_status(value: Optional[str]) -> str:
    if value is None or str(value).strip() == "":
        return "—"
    k = str(value).strip().lower()
    return {
        "draft": "Draft",
        "open": "Open",
        "complete": "Complete",
    }.get(k, k.replace("_", " ").title())


def _parse_opt_date(val: Any) -> Optional[date]:
    if val is None or val == "":
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    s = str(val).strip()[:10]
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def list_appraisals(
    contractor_id: Optional[int] = None, limit: int = 200
) -> List[Dict[str, Any]]:
    if not appraisal_tables_ready():
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        sql = """
            SELECT a.id, a.contractor_id, a.manager_contractor_id, a.cycle_label,
                   a.period_start, a.period_end, a.status,
                   a.employee_signed_at, a.manager_signed_at, a.attachment_path,
                   a.created_at, a.updated_at,
                   c.name AS employee_name, c.email AS employee_email,
                   m.name AS manager_name
            FROM hr_appraisal a
            INNER JOIN tb_contractors c ON c.id = a.contractor_id
            LEFT JOIN tb_contractors m ON m.id = a.manager_contractor_id
        """
        params: Tuple[Any, ...] = ()
        if contractor_id is not None:
            sql += " WHERE a.contractor_id = %s"
            params = (int(contractor_id),)
        sql += " ORDER BY a.updated_at DESC LIMIT %s"
        params = params + (int(limit),)
        cur.execute(sql, params)
        return list(cur.fetchall() or [])
    except Exception as exc:
        logger.warning("list_appraisals: %s", exc)
        return []
    finally:
        cur.close()
        conn.close()


def get_appraisal(appraisal_id: int) -> Optional[Dict[str, Any]]:
    if not appraisal_tables_ready():
        return None
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT a.*, c.name AS employee_name, c.email AS employee_email,
                   m.name AS manager_name, m.email AS manager_email
            FROM hr_appraisal a
            INNER JOIN tb_contractors c ON c.id = a.contractor_id
            LEFT JOIN tb_contractors m ON m.id = a.manager_contractor_id
            WHERE a.id = %s
            """,
            (int(appraisal_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        cur.execute(
            """
            SELECT id, sort_order, title, description, employee_notes, manager_notes
            FROM hr_appraisal_goal
            WHERE appraisal_id = %s
            ORDER BY sort_order ASC, id ASC
            """,
            (int(appraisal_id),),
        )
        row["goals"] = list(cur.fetchall() or [])
        return row
    finally:
        cur.close()
        conn.close()


def _replace_goals(cur, appraisal_id: int, goals: List[Dict[str, Any]]) -> None:
    cur.execute("DELETE FROM hr_appraisal_goal WHERE appraisal_id = %s", (int(appraisal_id),))
    for i, g in enumerate(goals):
        title = (g.get("title") or "").strip()
        if not title:
            continue
        cur.execute(
            """
            INSERT INTO hr_appraisal_goal
            (appraisal_id, sort_order, title, description, employee_notes, manager_notes)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                int(appraisal_id),
                i * 10,
                title[:255],
                (g.get("description") or "").strip() or None,
                (g.get("employee_notes") or "").strip() or None,
                (g.get("manager_notes") or "").strip() or None,
            ),
        )


def create_appraisal(
    contractor_id: int,
    manager_contractor_id: Optional[int],
    cycle_label: str,
    period_start: Any,
    period_end: Any,
    status: str,
    employee_summary: Optional[str],
    manager_summary: Optional[str],
    goals: List[Dict[str, Any]],
    created_by_user_id: Optional[str],
) -> Tuple[Optional[int], str]:
    if not appraisal_tables_ready():
        return None, "Appraisal tables missing — run HR install/upgrade."
    st = (status or "draft").strip().lower()
    if st not in ("draft", "open", "complete"):
        st = "draft"
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO hr_appraisal (
                contractor_id, manager_contractor_id, cycle_label,
                period_start, period_end, status,
                employee_summary, manager_summary,
                created_by_user_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(contractor_id),
                int(manager_contractor_id) if manager_contractor_id else None,
                (cycle_label or "Annual appraisal")[:128],
                _parse_opt_date(period_start),
                _parse_opt_date(period_end),
                st,
                (employee_summary or "").strip() or None,
                (manager_summary or "").strip() or None,
                (created_by_user_id or None),
            ),
        )
        aid = int(cur.lastrowid)
        _replace_goals(cur, aid, goals)
        conn.commit()
        log_appraisal_event(
            "created",
            "Appraisal #%s created (%s)" % (aid, (cycle_label or "")[:80]),
            appraisal_id=aid,
            contractor_id=int(contractor_id),
            channel="admin_ui",
            actor_user_id=created_by_user_id,
        )
        return aid, "ok"
    except Exception as exc:
        logger.warning("create_appraisal: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None, str(exc) or "Could not create appraisal."
    finally:
        cur.close()
        conn.close()


def update_appraisal(
    appraisal_id: int,
    manager_contractor_id: Optional[int],
    cycle_label: str,
    period_start: Any,
    period_end: Any,
    status: str,
    employee_summary: Optional[str],
    manager_summary: Optional[str],
    goals: List[Dict[str, Any]],
    employee_signed_at: Optional[datetime] = None,
    manager_signed_at: Optional[datetime] = None,
    clear_employee_sign: bool = False,
    clear_manager_sign: bool = False,
    actor_user_id: Optional[str] = None,
) -> Tuple[bool, str]:
    if not appraisal_tables_ready():
        return False, "Appraisal tables missing — run HR install/upgrade."
    st = (status or "draft").strip().lower()
    if st not in ("draft", "open", "complete"):
        st = "draft"
    current = get_appraisal(appraisal_id)
    if not current:
        return False, "Appraisal not found."
    old_st = (current.get("status") or "").strip().lower()
    old_es = current.get("employee_signed_at")
    old_ms = current.get("manager_signed_at")
    es_out = current.get("employee_signed_at")
    ms_out = current.get("manager_signed_at")
    if clear_employee_sign:
        es_out = None
    elif employee_signed_at is not None:
        es_out = employee_signed_at
    if clear_manager_sign:
        ms_out = None
    elif manager_signed_at is not None:
        ms_out = manager_signed_at
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE hr_appraisal SET
                manager_contractor_id = %s,
                cycle_label = %s,
                period_start = %s,
                period_end = %s,
                status = %s,
                employee_summary = %s,
                manager_summary = %s,
                employee_signed_at = %s,
                manager_signed_at = %s
            WHERE id = %s
            """,
            (
                int(manager_contractor_id) if manager_contractor_id else None,
                (cycle_label or "Annual appraisal")[:128],
                _parse_opt_date(period_start),
                _parse_opt_date(period_end),
                st,
                (employee_summary or "").strip() or None,
                (manager_summary or "").strip() or None,
                es_out,
                ms_out,
                int(appraisal_id),
            ),
        )
        if cur.rowcount == 0:
            conn.rollback()
            return False, "Appraisal not found."
        _replace_goals(cur, appraisal_id, goals)
        conn.commit()
        cid = int(current["contractor_id"])
        aid = int(appraisal_id)
        if old_st != st:
            log_appraisal_event(
                "status_changed",
                "Status %s → %s" % (old_st or "—", st),
                appraisal_id=aid,
                contractor_id=cid,
                channel="admin_ui",
                actor_user_id=actor_user_id,
            )
        if old_es != es_out:
            if es_out and not old_es:
                log_appraisal_event(
                    "employee_signed",
                    "Employee signature recorded",
                    appraisal_id=aid,
                    contractor_id=cid,
                    channel="admin_ui",
                    actor_user_id=actor_user_id,
                )
            elif not es_out and old_es:
                log_appraisal_event(
                    "employee_sign_cleared",
                    "Employee signature cleared",
                    appraisal_id=aid,
                    contractor_id=cid,
                    channel="admin_ui",
                    actor_user_id=actor_user_id,
                )
        if old_ms != ms_out:
            if ms_out and not old_ms:
                log_appraisal_event(
                    "manager_signed",
                    "Manager signature recorded",
                    appraisal_id=aid,
                    contractor_id=cid,
                    channel="admin_ui",
                    actor_user_id=actor_user_id,
                )
            elif not ms_out and old_ms:
                log_appraisal_event(
                    "manager_sign_cleared",
                    "Manager signature cleared",
                    appraisal_id=aid,
                    contractor_id=cid,
                    channel="admin_ui",
                    actor_user_id=actor_user_id,
                )
        log_appraisal_event(
            "updated",
            "Appraisal saved",
            appraisal_id=aid,
            contractor_id=cid,
            channel="admin_ui",
            actor_user_id=actor_user_id,
        )
        return True, "ok"
    except Exception as exc:
        logger.warning("update_appraisal: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False, str(exc) or "Could not update appraisal."
    finally:
        cur.close()
        conn.close()


def set_appraisal_attachment(
    appraisal_id: int,
    relative_path: Optional[str],
    actor_user_id: Optional[str] = None,
) -> bool:
    if not appraisal_tables_ready():
        return False
    row = get_appraisal(appraisal_id)
    if not row:
        return False
    cid = int(row["contractor_id"])
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE hr_appraisal SET attachment_path = %s WHERE id = %s",
            (relative_path, int(appraisal_id)),
        )
        conn.commit()
        ok = cur.rowcount > 0
        if ok:
            if relative_path:
                log_appraisal_event(
                    "attachment_added",
                    "Attachment uploaded",
                    appraisal_id=int(appraisal_id),
                    contractor_id=cid,
                    detail=(relative_path or "")[:2000],
                    channel="admin_ui",
                    actor_user_id=actor_user_id,
                )
            else:
                log_appraisal_event(
                    "attachment_removed",
                    "Attachment removed",
                    appraisal_id=int(appraisal_id),
                    contractor_id=cid,
                    channel="admin_ui",
                    actor_user_id=actor_user_id,
                )
        return ok
    except Exception as exc:
        logger.warning("set_appraisal_attachment: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        cur.close()
        conn.close()


def delete_appraisal(
    appraisal_id: int,
    static_root: str,
    actor_user_id: Optional[str] = None,
) -> bool:
    row = get_appraisal(appraisal_id)
    if not row:
        return False
    cid = int(row["contractor_id"])
    path = (row.get("attachment_path") or "").replace("\\", "/").strip()
    log_appraisal_event(
        "deleted",
        "Appraisal #%s deleted" % int(appraisal_id),
        appraisal_id=int(appraisal_id),
        contractor_id=cid,
        channel="admin_ui",
        actor_user_id=actor_user_id,
    )
    conn = get_db_connection()
    cur = conn.cursor()
    ok = False
    try:
        cur.execute("DELETE FROM hr_appraisal WHERE id = %s", (int(appraisal_id),))
        conn.commit()
        ok = cur.rowcount > 0
    except Exception as exc:
        logger.warning("delete_appraisal: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()
    if ok and path.startswith("uploads/hr_appraisal/"):
        full = os.path.normpath(os.path.join(static_root, path.replace("/", os.sep)))
        root = os.path.normpath(static_root)
        if full.startswith(root) and os.path.isfile(full):
            try:
                os.remove(full)
            except OSError:
                pass
    return ok
