"""
Contractor-facing inventory URLs under /inventory/ (session tb_user), same pattern as work_module /work/.
Employee portal remains the login hub; pages live in this plugin.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from functools import wraps

from flask import (
    Blueprint,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from app.objects import get_db_connection
from app.portal_session import contractor_id_from_tb_user, normalize_tb_user

logger = logging.getLogger("inventory_control.contractor_portal")

public_inventory_bp = Blueprint(
    "public_inventory",
    __name__,
    url_prefix="/inventory",
    template_folder="templates",
)

KIT_STATUS_PENDING = "pending"
KIT_STATUS_FULFILLED = "fulfilled"
KIT_STATUS_DECLINED = "declined"

_plugin_manager = None
_core_manifest = None


def _plugin_manager_and_manifest():
    global _plugin_manager, _core_manifest
    if _plugin_manager is None:
        from app.objects import PluginManager

        _plugin_manager = PluginManager(os.path.abspath("app/plugins"))
        _core_manifest = _plugin_manager.get_core_manifest()
    return _plugin_manager, _core_manifest


def inventory_contractor_portal_enabled() -> bool:
    try:
        pm, _ = _plugin_manager_and_manifest()
        return bool(pm.is_plugin_enabled("inventory_control"))
    except Exception:
        return False


def _staff_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("tb_user"):
            return redirect("/employee-portal/login?next=" + request.path)
        return view(*args, **kwargs)

    return wrapped


def _contractor_id():
    return contractor_id_from_tb_user(session.get("tb_user"))


def _website_settings():
    try:
        from app.plugins.website_module.routes import get_website_settings

        return get_website_settings()
    except Exception:
        return {"favicon_path": None}


def _parse_date(val):
    try:
        return datetime.strptime((val or "").strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


@public_inventory_bp.route("/kit-request", methods=["GET", "POST"])
@_staff_required
def contractor_kit_request():
    if not inventory_contractor_portal_enabled():
        flash("Inventory is not enabled for this site.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    uid = _contractor_id()
    if not uid:
        return redirect("/employee-portal/login?next=" + request.path)
    if request.method == "POST":
        nf = _parse_date(request.form.get("need_from"))
        nu = _parse_date(request.form.get("need_until"))
        body = (request.form.get("request_text") or "").strip()
        if not nf or not nu:
            flash("Please choose valid start and end dates.", "error")
            return redirect(url_for("public_inventory.contractor_kit_request"))
        if nu < nf:
            flash("End date must be on or after the start date.", "error")
            return redirect(url_for("public_inventory.contractor_kit_request"))
        if len(body) < 16:
            flash(
                "Please describe what you need, when, and why (at least a short paragraph).",
                "error",
            )
            return redirect(url_for("public_inventory.contractor_kit_request"))
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO inventory_contractor_kit_requests
                  (contractor_id, need_from, need_until, request_text, status)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (uid, nf, nu, body[:16000], KIT_STATUS_PENDING),
            )
            conn.commit()
            flash(
                "Request sent. Your team will arrange stock issue through normal inventory.",
                "success",
            )
            return redirect(url_for("public_inventory.contractor_kit_requests_list"))
        except Exception as e:
            conn.rollback()
            logger.exception("contractor_kit_request: %s", e)
            flash(
                "Could not save your request. If this persists, run the inventory database upgrade or contact support.",
                "error",
            )
        finally:
            cur.close()
            conn.close()
        return redirect(url_for("public_inventory.contractor_kit_request"))
    _, manifest = _plugin_manager_and_manifest()
    return render_template(
        "inventory_public/kit_request.html",
        config=manifest or {},
        user=session.get("tb_user"),
        website_settings=_website_settings(),
        today_iso=date.today().isoformat(),
    )


@public_inventory_bp.get("/my-kit-requests")
@_staff_required
def contractor_kit_requests_list():
    if not inventory_contractor_portal_enabled():
        flash("Inventory is not enabled for this site.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    uid = _contractor_id()
    if not uid:
        return redirect("/employee-portal/login?next=" + request.path)
    rows = []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, need_from, need_until, request_text, status, office_notes,
                   created_at, resolved_at
            FROM inventory_contractor_kit_requests
            WHERE contractor_id = %s
            ORDER BY created_at DESC
            LIMIT 50
            """,
            (uid,),
        )
        rows = cur.fetchall() or []
    except Exception as e:
        logger.exception("contractor_kit_requests_list: %s", e)
        flash("Could not load your requests.", "error")
    finally:
        cur.close()
        conn.close()
    _, manifest = _plugin_manager_and_manifest()
    return render_template(
        "inventory_public/my_kit_requests.html",
        config=manifest or {},
        user=normalize_tb_user(session.get("tb_user")),
        requests=rows,
        website_settings=_website_settings(),
    )
