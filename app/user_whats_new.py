"""
Per-user "What's new" release notes (modal on the **core dashboard** ``/`` only).

The dashboard route merges ``build_whats_new_template_context()`` into the template; the
employee portal and other shells do not receive this context unless wired separately.

Content: ``app/config/whats_new.json`` (optional) merged with ``whats_new`` key from the
core manifest when present. Each object should include at least ``version`` (release id)
and one of ``summary``, ``bullets``, or ``title`` for something to show.

Acknowledgement: ``users.whats_new_last_seen_version`` stores the last ``version`` the
user dismissed; the modal appears again only when ``version`` compares strictly newer
(semver-like tuple comparison, same spirit as UpdateManager).
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

from flask import current_app
from flask_login import current_user

from app.objects import get_db_connection

logger = logging.getLogger(__name__)


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """,
        (table, column),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def ensure_whats_new_schema(conn=None) -> None:
    """Add ``users.whats_new_last_seen_version`` if missing."""
    own = conn is None
    if own:
        conn = get_db_connection()
    try:
        cur = conn.cursor()
        try:
            if not _column_exists(cur, "users", "whats_new_last_seen_version"):
                cur.execute(
                    "ALTER TABLE users ADD COLUMN whats_new_last_seen_version VARCHAR(64) NULL DEFAULT NULL"
                )
            if own:
                conn.commit()
        finally:
            cur.close()
    finally:
        if own:
            conn.close()


def _version_tuple(v: str | None) -> tuple:
    if not isinstance(v, str):
        return (0,)
    s = v.strip()
    if not s or s.lower() == "unknown":
        return (0,)
    parts: list[int] = []
    for p in s.split("."):
        try:
            parts.append(int(p))
        except ValueError:
            num = "".join(ch for ch in p if ch.isdigit())
            parts.append(int(num) if num else 0)
    return tuple(parts) if parts else (0,)


def _release_should_show(*, release: str, last_seen: str | None) -> bool:
    rel = (release or "").strip()
    if not rel or rel.lower() == "unknown":
        return False
    ls = (last_seen or "").strip()
    if not ls:
        return True
    return _version_tuple(rel) > _version_tuple(ls)


def _deep_merge_dict(base: dict[str, Any], overlay: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(base)
    if not isinstance(overlay, dict):
        return out
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge_dict(out[k], v)
        else:
            out[k] = v
    return out


def read_core_manifest_for_whats_new(app_root: str) -> dict[str, Any]:
    """Read ``config/manifest.json`` only (no plugin scan)."""
    path = os.path.join(app_root, "config", "manifest.json")
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}
    except Exception as e:
        logger.warning("manifest.json read failed for whats_new: %s", e)
        return {}


def load_whats_new_payload(*, app_root: str, core_manifest: dict[str, Any] | None) -> dict[str, Any]:
    """
    Return merged payload (may be empty dict). Expected keys: version, title, summary,
    bullets (list), sections (list of {heading, paragraphs}).
    """
    cm = core_manifest if isinstance(core_manifest, dict) else {}
    base: dict[str, Any] = {}
    path = os.path.join(app_root, "config", "whats_new.json")
    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                base = raw
        except Exception as e:
            logger.warning("whats_new.json read failed: %s", e)
    mw = cm.get("whats_new")
    if isinstance(mw, dict) and mw:
        base = _deep_merge_dict(base, mw)
    return base if isinstance(base, dict) else {}


def _normalize_whats_new_sections(raw: Any) -> list[dict[str, Any]]:
    """``sections`` in JSON: list of {heading, paragraphs: [str, ...]}."""
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        h = str(item.get("heading") or item.get("title") or "").strip()
        paras: list[str] = []
        p = item.get("paragraphs")
        if isinstance(p, list):
            paras = [str(x).strip() for x in p if str(x).strip()]
        if h or paras:
            out.append({"heading": h, "paragraphs": paras})
    return out


def _payload_has_visible_content(payload: dict[str, Any]) -> bool:
    title = (payload.get("title") or "").strip()
    summary = (payload.get("summary") or "").strip()
    bullets = payload.get("bullets")
    if _normalize_whats_new_sections(payload.get("sections")):
        return True
    if title or summary:
        return True
    if isinstance(bullets, list) and any(str(x).strip() for x in bullets):
        return True
    return False


def get_last_seen_version(user_id: str | None) -> str | None:
    uid = (user_id or "").strip()
    if not uid:
        return None
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if not _column_exists(cur, "users", "whats_new_last_seen_version"):
            return None
        cur.execute(
            "SELECT whats_new_last_seen_version FROM users WHERE id = %s LIMIT 1",
            (uid,),
        )
        row = cur.fetchone()
        if not row:
            return None
        v = row[0]
        return str(v).strip() if v is not None else None
    except Exception as e:
        logger.warning("get_last_seen_version failed: %s", e)
        return None
    finally:
        cur.close()
        conn.close()


def set_last_seen_version(user_id: str | None, version: str) -> bool:
    uid = (user_id or "").strip()
    ver = (version or "").strip()
    if not uid or not ver:
        return False
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if not _column_exists(cur, "users", "whats_new_last_seen_version"):
            return False
        cur.execute(
            "UPDATE users SET whats_new_last_seen_version = %s WHERE id = %s",
            (ver[:64], uid),
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        logger.warning("set_last_seen_version failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        cur.close()
        conn.close()


def build_whats_new_template_context() -> dict[str, Any]:
    """
    For Jinja / context processor. Returns ``{"whats_new": None}`` or a dict with
    show, version, title, summary, bullets, ack_url.
    """
    try:
        if not getattr(current_user, "is_authenticated", False):
            return {"whats_new": None}
        uid = str(getattr(current_user, "id", "") or "").strip()
        if not uid:
            return {"whats_new": None}
        app_root = current_app.root_path
        core_manifest = read_core_manifest_for_whats_new(app_root)
        payload = load_whats_new_payload(app_root=app_root, core_manifest=core_manifest)
        version = (payload.get("version") or "").strip()
        if not version or not _payload_has_visible_content(payload):
            return {"whats_new": None}
        last = get_last_seen_version(uid)
        if not _release_should_show(release=version, last_seen=last):
            return {"whats_new": None}
        bullets: list[str] = []
        raw_b = payload.get("bullets")
        if isinstance(raw_b, list):
            bullets = [str(x).strip() for x in raw_b if str(x).strip()]
        sections = _normalize_whats_new_sections(payload.get("sections"))
        return {
            "whats_new": {
                "show": True,
                "version": version,
                "title": (payload.get("title") or "What's new").strip() or "What's new",
                "summary": (payload.get("summary") or "").strip(),
                "bullets": bullets,
                "sections": sections,
            }
        }
    except Exception as e:
        logger.warning("build_whats_new_template_context failed: %s", e)
        return {"whats_new": None}
