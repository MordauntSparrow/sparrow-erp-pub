"""
Web Push for the employee portal PWA: VAPID, subscription storage, generic payloads.
Requires env EMPLOYEE_PORTAL_VAPID_* and pywebpush (see requirements.txt).
"""
from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from app.objects import get_db_connection

logger = logging.getLogger(__name__)

# Allowed deep-link prefixes for notification click targets (same origin; PWA may open in browser).
_PORTAL_PREFIX = "/employee-portal/"
_SCHEDULING_PREFIX = "/scheduling/"


def _vapid_config() -> Tuple[Optional[str], Optional[str], str]:
    pub = (os.environ.get("EMPLOYEE_PORTAL_VAPID_PUBLIC_KEY") or "").strip()
    priv = (os.environ.get("EMPLOYEE_PORTAL_VAPID_PRIVATE_KEY") or "").strip()
    if priv and "\\n" in priv and "BEGIN" in priv:
        priv = priv.replace("\\n", "\n")
    subj = (os.environ.get("EMPLOYEE_PORTAL_VAPID_SUBJECT")
            or "mailto:portal@localhost").strip()
    if not subj.startswith("mailto:") and not subj.startswith("https:"):
        subj = "mailto:portal@localhost"
    return pub or None, priv or None, subj


def is_push_configured() -> bool:
    pub, priv, _ = _vapid_config()
    return bool(pub and priv)


def get_vapid_public_key() -> Optional[str]:
    return _vapid_config()[0]


def normalize_portal_push_url(relative_url: Optional[str]) -> str:
    """Restrict open-on-click URL to employee portal or public scheduling paths."""
    u = (relative_url or "").strip()
    if not u.startswith("/"):
        u = "/" + u
    for prefix in (_PORTAL_PREFIX, _SCHEDULING_PREFIX):
        if u.startswith(prefix):
            if len(u) > 512:
                return prefix
            return u
    return _PORTAL_PREFIX


def contractor_push_enabled(contractor_id: int) -> bool:
    if not contractor_id:
        return False
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT push_enabled FROM ep_notification_prefs WHERE contractor_id = %s",
                (int(contractor_id),),
            )
            row = cur.fetchone()
            if row is None:
                return True
            return bool(int(row.get("push_enabled") or 1))
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.warning(
            "push prefs read failed contractor_id=%s: %s", contractor_id, e)
        return True


def set_contractor_push_enabled(contractor_id: int, enabled: bool) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO ep_notification_prefs (contractor_id, push_enabled)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE push_enabled = VALUES(push_enabled), updated_at = CURRENT_TIMESTAMP
            """,
            (int(contractor_id), 1 if enabled else 0),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def upsert_subscription(
    contractor_id: int,
    endpoint: str,
    p256dh: str,
    auth_secret: str,
    user_agent: Optional[str] = None,
) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO ep_push_subscriptions
              (contractor_id, endpoint, p256dh, auth_secret, user_agent, last_seen_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
              contractor_id = VALUES(contractor_id),
              p256dh = VALUES(p256dh),
              auth_secret = VALUES(auth_secret),
              user_agent = VALUES(user_agent),
              last_seen_at = CURRENT_TIMESTAMP
            """,
            (
                int(contractor_id),
                endpoint[:768],
                p256dh,
                auth_secret[:255],
                (user_agent or "")[:512] or None,
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def delete_subscription_for_contractor(contractor_id: int, endpoint: str) -> int:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM ep_push_subscriptions WHERE contractor_id = %s AND endpoint = %s",
            (int(contractor_id), endpoint[:768]),
        )
        n = cur.rowcount
        conn.commit()
        return n
    finally:
        cur.close()
        conn.close()


def _delete_subscription_by_endpoint(endpoint: str) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM ep_push_subscriptions WHERE endpoint = %s", (endpoint[:768],))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def push_notify_contractor(
    contractor_id: int,
    *,
    title: str,
    body: str,
    relative_url: str,
    tag: Optional[str] = None,
) -> None:
    """Send push to all subscriptions for contractor (best-effort, same thread)."""
    _, vapid_private, vapid_subj = _vapid_config()
    if not vapid_private or not contractor_push_enabled(int(contractor_id)):
        return
    try:
        from pywebpush import WebPushException, webpush
    except ImportError:
        return

    rel = normalize_portal_push_url(relative_url)
    payload = json.dumps(
        {
            "title": (title or "Employee Portal")[:120],
            "body": (body or "You have an update.")[:500],
            "url": rel,
            "tag": (tag or "employee-portal")[:120],
        }
    )

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT endpoint, p256dh, auth_secret FROM ep_push_subscriptions
            WHERE contractor_id = %s
            """,
            (int(contractor_id),),
        )
        rows = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    for row in rows:
        ep = str(row.get("endpoint") or "").strip()
        p256 = str(row.get("p256dh") or "").strip()
        auth = str(row.get("auth_secret") or "").strip()
        if not ep or not p256 or not auth:
            continue
        info: Dict[str, Any] = {"endpoint": ep,
                                "keys": {"p256dh": p256, "auth": auth}}
        try:
            webpush(
                subscription_info=info,
                data=payload,
                vapid_private_key=vapid_private,
                vapid_claims={"sub": vapid_subj},
                ttl=86400,
            )
        except WebPushException as ex:
            st = getattr(ex, "response", None)
            code = getattr(st, "status_code", None) if st is not None else None
            if code in (404, 410):
                _delete_subscription_by_endpoint(ep)
        except Exception:
            pass


def _notify_contractor_thread(
    contractor_id: int,
    title: str,
    body: str,
    relative_url: str,
    tag: Optional[str],
) -> None:
    try:
        push_notify_contractor(
            contractor_id,
            title=title,
            body=body,
            relative_url=relative_url,
            tag=tag,
        )
    except Exception:
        logger.debug("portal push failed", exc_info=True)


def schedule_push_for_contractor(
    contractor_id: int,
    *,
    title: str,
    body: str,
    relative_url: str,
    tag: Optional[str] = None,
) -> None:
    """Fire-and-forget push on a daemon thread (non-blocking for request handlers)."""
    if not is_push_configured():
        return
    cid = int(contractor_id)
    threading.Thread(
        target=_notify_contractor_thread,
        args=(cid, title, body, relative_url, tag),
        daemon=True,
    ).start()


def schedule_push_for_new_portal_messages(pairs: Sequence[Tuple[int, int]]) -> None:
    """pairs: (contractor_id, ep_messages.id)."""
    for cid, msg_id in pairs:
        schedule_push_for_contractor(
            cid,
            title="New portal message",
            body="You have a new message in the employee portal.",
            relative_url=f"/employee-portal/messages/{int(msg_id)}",
            tag=f"ep-message-{int(msg_id)}",
        )


def schedule_push_for_new_portal_todos(rows: Sequence[Tuple[int, int, Optional[str]]]) -> None:
    """rows: (contractor_id, ep_todos.id, link_url from row)."""
    for cid, todo_id, link_url in rows:
        rel = normalize_portal_push_url(
            link_url) if link_url else _PORTAL_PREFIX
        schedule_push_for_contractor(
            cid,
            title="New task assigned",
            body="You have a new task on your portal.",
            relative_url=rel,
            tag=f"ep-todo-{int(todo_id)}",
        )


_SHIFT_PUSH_COPY: Dict[str, Tuple[str, str]] = {
    "assigned": (
        "New shift assignment",
        "You have been assigned a shift. Open the app for details.",
    ),
    "updated": (
        "Shift updated",
        "One of your shifts was changed. Open the app for details.",
    ),
    "cancelled": (
        "Shift cancelled",
        "A shift on your roster was cancelled or removed. Open the app for details.",
    ),
    "removed": (
        "Removed from shift",
        "You are no longer assigned to a shift. Open the app for details.",
    ),
    "starting_soon": (
        "Shift starting soon",
        "A shift is starting soon. Open the app for details.",
    ),
}


def schedule_push_for_shift_contractors(
    contractor_ids: Sequence[int],
    kind: str,
    shift_id: Optional[int] = None,
) -> None:
    """
    Web push to contractors who registered a portal device (``ep_push_subscriptions``).
    Payloads stay generic (no client/site names in the notification body).
    """
    pair = _SHIFT_PUSH_COPY.get((kind or "").strip().lower())
    if not pair:
        return
    title, body = pair
    rel = normalize_portal_push_url(
        "/scheduling/my-schedule" if shift_id else "/scheduling/"
    )
    tag = f"ep-shift-{int(shift_id)}" if shift_id else "ep-shift"
    seen: Set[int] = set()
    for raw in contractor_ids:
        try:
            cid = int(raw)
        except (TypeError, ValueError):
            continue
        if cid <= 0 or cid in seen:
            continue
        seen.add(cid)
        schedule_push_for_contractor(
            cid,
            title=title,
            body=body,
            relative_url=rel,
            tag=tag,
        )
