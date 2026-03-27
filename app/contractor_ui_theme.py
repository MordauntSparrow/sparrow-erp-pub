"""
Contractor UI theme: tb_contractors.ui_theme + session.

- On successful contractor login, call sync_contractor_theme_to_session(session, contractor_id).
- Templates get portal_theme / portal_theme_preference from a tiny context processor (session only;
  re-resolves "auto" each request). No DB in the context processor.
- POST /contractor-ui/set-theme updates session + DB.

register_contractor_public_theme(app) runs from PluginManager.register_public_routes (not create_app).
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from flask import Blueprint, redirect, request, session as flask_session

logger = logging.getLogger(__name__)

VALID_PREFS = frozenset({"light", "dark", "auto"})


def resolve_auto_theme() -> str:
    hour = datetime.utcnow().hour
    return "light" if 6 <= hour < 22 else "dark"


def get_stored_preference_for_contractor(contractor_id: int) -> Optional[str]:
    """ui_theme column only (used by services and sync fallback)."""
    if not contractor_id:
        return None
    try:
        from app.objects import get_db_connection

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT ui_theme FROM tb_contractors WHERE id = %s LIMIT 1",
                (int(contractor_id),),
            )
            row = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        if not row:
            return None
        v = (row.get("ui_theme") or "").strip().lower()
        if v == "system":
            return "auto"
        return v if v in VALID_PREFS else None
    except Exception as e:
        logger.debug("ui_theme read skipped: %s", e)
        return None


def set_contractor_ui_theme_column(contractor_id: int, preference: str) -> bool:
    if not contractor_id or preference not in VALID_PREFS:
        return False
    try:
        from app.objects import get_db_connection

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE tb_contractors SET ui_theme = %s WHERE id = %s",
                (preference, int(contractor_id)),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.warning("ui_theme update failed: %s", e)
        return False


def sync_contractor_theme_to_session(session, contractor_id: int) -> None:
    """
    Call after contractor auth succeeds: read preference from DB.
    Uses employee_portal get_contractor_theme when that module exists (legacy ep_settings migration);
    otherwise column only. Defaults to light.
    """
    if not contractor_id:
        return
    pref = None
    try:
        from app.plugins.employee_portal_module.services import get_contractor_theme

        pref = get_contractor_theme(int(contractor_id))
    except ImportError:
        pass
    if pref == "system":
        pref = "auto"
    if pref not in VALID_PREFS:
        pref = get_stored_preference_for_contractor(int(contractor_id))
    if pref not in VALID_PREFS:
        pref = "light"
    session["portal_theme"] = pref
    session.modified = True


def portal_theme_template_vars(sess) -> Dict[str, Any]:
    """Jinja: from session only. Missing/invalid → light. 'auto' resolved per request."""
    pref = sess.get("portal_theme")
    if pref not in VALID_PREFS:
        pref = "light"
    resolved = resolve_auto_theme() if pref == "auto" else pref
    if resolved not in ("light", "dark"):
        resolved = "light"
    return {"portal_theme": resolved, "portal_theme_preference": pref}


def _safe_same_site_redirect_target() -> str:
    ref = request.referrer
    if not ref:
        return "/"
    try:
        p = urlparse(ref)
        base = urlparse(request.url_root)
        if p.scheme and p.netloc and (p.netloc != base.netloc):
            return "/"
        qs = parse_qs(p.query, keep_blank_values=True)
        qs.pop("theme", None)
        new_query = urlencode(qs, doseq=True)
        path = p.path or "/"
        if not path.startswith("/"):
            path = "/" + path
        out = urlunparse(("", "", path, p.params, new_query, p.fragment))
        return out if out.startswith("/") else "/"
    except Exception:
        return "/"


def contractor_set_theme_response():
    raw = (request.form.get("theme") or "").strip().lower()
    preference = "dark" if raw == "dark" else ("auto" if raw == "auto" else "light")
    flask_session["portal_theme"] = preference
    flask_session.modified = True
    cid = (flask_session.get("tb_user") or {}).get("id")
    if cid:
        set_contractor_ui_theme_column(int(cid), preference)
    return redirect(_safe_same_site_redirect_target(), code=303)


def register_contractor_public_theme(app) -> None:
    """One-shot: Jinja context + POST /contractor-ui/set-theme. Idempotent."""
    ext = app.extensions.setdefault("sparrow_contractor_theme", {})
    if ext.get("_registered"):
        return
    ext["_registered"] = True

    @app.context_processor
    def _inject_contractor_theme():
        from flask import session

        return portal_theme_template_vars(session)

    bp = Blueprint("sparrow_shared_contractor_ui", __name__)

    @bp.post("/contractor-ui/set-theme")
    def set_contractor_theme():
        return contractor_set_theme_response()

    app.register_blueprint(bp)
