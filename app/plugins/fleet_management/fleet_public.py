"""
Contractor-facing fleet UI at /fleet (session tb_user), mirroring work_module /work.
Templates live under fleet_management/templates/public/.
"""

from __future__ import annotations

import os
from datetime import date
from functools import wraps
from typing import Optional, Set
from urllib.parse import quote

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from app.objects import PluginManager, get_db_connection
from app.portal_session import contractor_id_from_tb_user, normalize_tb_user

_plugin_manager = PluginManager(os.path.abspath("app/plugins"))
_core_manifest = _plugin_manager.get_core_manifest() or {}

_template_folder = os.path.join(os.path.dirname(__file__), "templates")

public_fleet_bp = Blueprint(
    "public_fleet",
    __name__,
    url_prefix="/fleet",
    template_folder=_template_folder,
)


def _get_website_settings():
    try:
        from app.plugins.website_module.routes import get_website_settings

        return get_website_settings()
    except Exception:
        _keys = (
            "favicon_path",
            "default_og_image",
            "facebook_url",
            "instagram_url",
            "linkedin_url",
            "twitter_url",
            "youtube_url",
        )
        return {k: None for k in _keys}


def _fleet_enabled() -> bool:
    try:
        return bool(_plugin_manager.is_plugin_enabled("fleet_management"))
    except Exception:
        return False


def _staff_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("tb_user"):
            nxt = quote(request.path, safe="/")
            return redirect(f"/employee-portal/login?next={nxt}")
        return view(*args, **kwargs)

    return wrapped


def _current_contractor_id():
    return contractor_id_from_tb_user(session.get("tb_user"))


def _session_user_for_template():
    u = normalize_tb_user(session.get("tb_user"))
    if not u:
        return None
    from app.plugins.employee_portal_module.services import safe_profile_picture_path

    d = dict(u)
    d["profile_picture_path"] = safe_profile_picture_path(d.get("profile_picture_path"))
    return d


def _render_ctx():
    return {
        "config": _core_manifest or {},
        "user": _session_user_for_template(),
        "website_settings": _get_website_settings(),
    }


def _contractor_all_role_names(contractor_id: int) -> Set[str]:
    """Lowercased role names from tb_contractor_roles plus primary role_id on tb_contractors."""
    names: Set[str] = set()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT LOWER(TRIM(r.name)) FROM tb_contractor_roles cr
            JOIN roles r ON r.id = cr.role_id
            WHERE cr.contractor_id = %s
            """,
            (int(contractor_id),),
        )
        for row in cur.fetchall() or []:
            if row and row[0]:
                names.add(str(row[0]).strip())
        cur.execute(
            """
            SELECT LOWER(TRIM(r.name)) FROM tb_contractors c
            LEFT JOIN roles r ON r.id = c.role_id
            WHERE c.id = %s
            """,
            (int(contractor_id),),
        )
        row = cur.fetchone()
        if row and row[0]:
            names.add(str(row[0]).strip())
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
    return names


def _portal_role_hints_normalize(raw) -> list:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [x.strip().lower() for x in raw.replace(";", ",").split(",") if x.strip()]
    if isinstance(raw, (list, tuple)):
        return [str(x).strip().lower() for x in raw if str(x).strip()]
    return []


def contractor_matches_portal_role_hints(contractor_id: int, hints: list) -> bool:
    if not hints:
        return False
    for cr in _contractor_all_role_names(int(contractor_id)):
        cl = cr.lower()
        for h in hints:
            if not h:
                continue
            if h in cl or cl in h or h == cl:
                return True
    return False


def contractor_matches_fleet_safety_portal_db_rules(contractor_id: Optional[int]) -> bool:
    """DB + manifest rules from Fleet → Portal form access; empty slot = legacy mechanic/manager list."""
    if not contractor_id:
        return False
    from .fleet_portal_visibility import (
        fleet_portal_visibility_slot_configured,
        fleet_role_names_match_portal_rules,
        fleet_user_role_category_definitions,
    )
    from .objects import get_fleet_service

    vis = get_fleet_service().get_fleet_portal_form_visibility()
    roles = vis.get("safety_roles") or []
    cats = vis.get("safety_categories") or []
    if not fleet_portal_visibility_slot_configured(roles, cats):
        return portal_can_record_fleet_safety(contractor_id)
    defs = fleet_user_role_category_definitions()
    names = _contractor_all_role_names(int(contractor_id))
    return fleet_role_names_match_portal_rules(names, roles, cats, defs)


def contractor_may_access_fleet_portal_vdi(contractor_id: Optional[int]) -> bool:
    """Empty VDI slot on Portal form access = any logged-in contractor. Otherwise role/group match."""
    if not contractor_id:
        return False
    from .fleet_portal_visibility import (
        fleet_portal_visibility_slot_configured,
        fleet_role_names_match_portal_rules,
        fleet_user_role_category_definitions,
    )
    from .objects import get_fleet_service

    vis = get_fleet_service().get_fleet_portal_form_visibility()
    roles = vis.get("vdi_roles") or []
    cats = vis.get("vdi_categories") or []
    if not fleet_portal_visibility_slot_configured(roles, cats):
        return True
    defs = fleet_user_role_category_definitions()
    names = _contractor_all_role_names(int(contractor_id))
    return fleet_role_names_match_portal_rules(names, roles, cats, defs)


def contractor_can_use_primary_fleet_safety(contractor_id: int, raw_schema: dict) -> bool:
    if not contractor_matches_fleet_safety_portal_db_rules(contractor_id):
        return False
    hints = _portal_role_hints_normalize(raw_schema.get("portal_roles"))
    if hints:
        return contractor_matches_portal_role_hints(int(contractor_id), hints)
    return True


def contractor_can_use_additional_fleet_safety(contractor_id: int, form_blob: dict) -> bool:
    if not contractor_matches_fleet_safety_portal_db_rules(contractor_id):
        return False
    hints = _portal_role_hints_normalize(form_blob.get("portal_roles"))
    return bool(hints) and contractor_matches_portal_role_hints(int(contractor_id), hints)


def list_portal_safety_forms_for_vehicle(svc, vehicle_id: int, contractor_id: Optional[int]) -> list:
    """Buttons for contractor portal: primary workshop + role-matched custom forms."""
    from app.plugins.fleet_management.safety_schema_build import PRIMARY_FORM_KEY

    if not contractor_id:
        return []
    raw = svc.get_raw_safety_schema_for_vehicle(int(vehicle_id))
    out = []
    if contractor_can_use_primary_fleet_safety(int(contractor_id), raw):
        out.append(
            {
                "form_key": PRIMARY_FORM_KEY,
                "title": raw.get("title") or "Workshop safety check",
            }
        )
    for af in svc.get_additional_contractor_safety_forms(int(vehicle_id)):
        fk = str(af.get("form_id") or "").strip()
        if not fk:
            continue
        if contractor_can_use_additional_fleet_safety(int(contractor_id), af):
            out.append({"form_key": fk, "title": af.get("title") or fk})
    return out


def portal_can_record_fleet_safety(contractor_id: Optional[int]) -> bool:
    """
    Workshop safety checks from employee portal: admin/superuser, mechanic,
    fleet manager/supervisor/lead, or workshop manager roles (tb_contractor_roles / role_id).
    """
    if not contractor_id:
        return False
    for raw in _contractor_all_role_names(int(contractor_id)):
        n = raw.strip().lower()
        if not n:
            continue
        if n in ("admin", "superuser", "support_break_glass"):
            return True
        if "mechanic" in n:
            return True
        if "fleet" in n and any(
            x in n for x in ("manager", "supervisor", "lead", "director")
        ):
            return True
        if "workshop" in n and ("manager" in n or "lead" in n or n == "workshop"):
            return True
        if n in ("workshop", "garage", "maintenance_manager", "maintenance manager"):
            return True
    return False


@public_fleet_bp.get("/")
@_staff_required
def fleet_portal_index():
    if not _fleet_enabled():
        flash("Fleet is not installed or not enabled.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    from .objects import get_fleet_service

    svc = get_fleet_service()
    vehicles = svc.vehicles_for_crew_picklist()
    cid = _current_contractor_id()
    for row in vehicles:
        row["portal_safety_forms"] = list_portal_safety_forms_for_vehicle(
            svc, int(row["id"]), cid
        )
    ctx = _render_ctx()
    ctx["vehicles"] = vehicles
    ctx["can_record_portal_safety"] = bool(
        cid and contractor_matches_fleet_safety_portal_db_rules(cid)
    )
    ctx["contractor_has_any_portal_safety_form"] = any(
        len(row.get("portal_safety_forms") or []) > 0 for row in vehicles
    )
    ctx["fleet_portal_vdi_enabled"] = bool(
        cid and contractor_may_access_fleet_portal_vdi(cid)
    )
    return render_template("public/fleet_portal_home.html", **ctx)


@public_fleet_bp.route("/vdi/<int:vehicle_id>", methods=["GET", "POST"])
@_staff_required
def fleet_portal_vdi(vehicle_id: int):
    if not _fleet_enabled():
        flash("Fleet is not available.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    from .objects import get_fleet_service
    from .vdi_submit_logic import submit_vdi

    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_employee_portal.login_page"))
    if not contractor_may_access_fleet_portal_vdi(cid):
        flash(
            "Vehicle inspections are not available for your role. Ask fleet admin to grant access "
            "under Fleet → Portal form access.",
            "error",
        )
        return redirect(url_for("public_fleet.fleet_portal_index"))
    cid_s = str(int(cid))
    svc = get_fleet_service()
    v = svc.get_vehicle(vehicle_id)
    if not v or str(v.get("status")) == "decommissioned":
        flash("Vehicle not found.", "error")
        return redirect(url_for("public_fleet.fleet_portal_index"))

    schema = svc.get_vdi_schema()
    if request.method == "POST":
        err, sid = submit_vdi(
            svc,
            request,
            vehicle_id=vehicle_id,
            actor_type="contractor",
            actor_id=cid_s,
        )
        if err:
            flash(err, "error")
            ctx = _render_ctx()
            ctx["vehicle"] = v
            ctx["schema"] = schema
            return render_template("public/fleet_portal_vdi_form.html", **ctx)
        flash("Inspection submitted. Thank you.", "success")
        return redirect(
            url_for("public_fleet.fleet_portal_vdi_submission", submission_id=sid)
        )

    ctx = _render_ctx()
    ctx["vehicle"] = v
    ctx["schema"] = schema
    return render_template("public/fleet_portal_vdi_form.html", **ctx)


@public_fleet_bp.route("/safety-check/<int:vehicle_id>", methods=["GET", "POST"])
@_staff_required
def fleet_portal_safety_check(vehicle_id: int):
    """Workshop or custom inspection forms — visibility from vehicle type + contractor roles."""
    if not _fleet_enabled():
        flash("Fleet is not available.", "error")
        return redirect(url_for("public_employee_portal.dashboard"))
    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_employee_portal.login_page"))

    from .objects import get_fleet_service
    from .safety_schema_build import PRIMARY_FORM_KEY
    from .vdi_submit_logic import parse_vdi_form, save_fleet_safety_photos_after_submit

    svc = get_fleet_service()
    v = svc.get_vehicle(vehicle_id)
    if not v or str(v.get("status")) == "decommissioned":
        flash("Vehicle not found.", "error")
        return redirect(url_for("public_fleet.fleet_portal_index"))

    form_key = (request.values.get("form") or request.form.get("check_form_key") or PRIMARY_FORM_KEY).strip()
    allowed = list_portal_safety_forms_for_vehicle(svc, vehicle_id, cid)
    allowed_keys = {a["form_key"] for a in allowed}
    if form_key not in allowed_keys:
        flash(
            "You do not have access to this checklist for this vehicle, or it is not configured. "
            "Ask fleet admin to assign the right portal role or add a form for your role.",
            "error",
        )
        return redirect(url_for("public_fleet.fleet_portal_index"))

    schema = svc.get_safety_check_form_for_vehicle(vehicle_id, form_key)
    if not schema:
        flash("Checklist not found.", "error")
        return redirect(url_for("public_fleet.fleet_portal_index"))

    performer_ref = f"ep:{int(cid)}"
    if len(performer_ref) > 36:
        performer_ref = performer_ref[:36]

    if request.method == "POST":
        responses, mileage_val, err = parse_vdi_form(request, schema)
        if err:
            flash(err, "error")
        else:
            try:
                perf = (
                    (request.form.get("performed_at") or "").strip()
                    or date.today().isoformat()
                )
                extra_mi = (request.form.get("mileage_at_check") or "").strip()
                mile_store = int(extra_mi) if extra_mi else mileage_val
                check_id = svc.add_safety_check(
                    vehicle_id=vehicle_id,
                    performed_by_user_id=performer_ref,
                    performed_at=perf[:10],
                    mileage_at_check=mile_store,
                    responses=responses,
                    photo_paths=[],
                    summary_notes=request.form.get("summary_notes"),
                    user_id=performer_ref,
                    check_form_key=form_key,
                )
                paths = save_fleet_safety_photos_after_submit(
                    request, schema, check_id, vehicle_id
                )
                if paths:
                    svc.update_safety_check_photos(check_id, paths)
                flash("Safety check saved. Thank you.", "success")
                return redirect(url_for("public_fleet.fleet_portal_index"))
            except Exception:
                import logging

                logging.getLogger("fleet_management").exception("fleet_portal_safety_check")
                flash("Could not save the safety check. Please try again.", "error")

    ctx = _render_ctx()
    ctx["vehicle"] = v
    ctx["schema"] = schema
    ctx["safety_form_key"] = form_key
    ctx["now_date"] = date.today().isoformat()[:10]
    return render_template("public/fleet_portal_safety_form.html", **ctx)


@public_fleet_bp.get("/vdi-submission/<int:submission_id>")
@_staff_required
def fleet_portal_vdi_submission(submission_id: int):
    if not _fleet_enabled():
        return redirect(url_for("public_employee_portal.dashboard"))
    from .objects import get_fleet_service

    svc = get_fleet_service()
    row = svc.get_vdi_submission(submission_id)
    if not row:
        flash("Not found.", "error")
        return redirect(url_for("public_fleet.fleet_portal_index"))
    v = svc.get_vehicle(int(row["vehicle_id"]))
    schema = svc.get_vdi_schema()
    ctx = _render_ctx()
    ctx["submission"] = row
    ctx["vehicle"] = v
    ctx["schema"] = schema
    return render_template("public/fleet_portal_vdi_submission.html", **ctx)


@public_fleet_bp.post("/issue")
@_staff_required
def fleet_portal_report_issue():
    if not _fleet_enabled():
        return redirect(url_for("public_employee_portal.dashboard"))
    from .objects import get_fleet_service

    cid = _current_contractor_id()
    if not cid:
        return redirect(url_for("public_employee_portal.login_page"))
    cid_s = str(int(cid))
    svc = get_fleet_service()
    try:
        vid = int(request.form.get("vehicle_id") or 0)
        if vid <= 0:
            flash("Choose a unit to report an issue for.", "error")
            return redirect(
                request.referrer or url_for("public_fleet.fleet_portal_index")
            )
        v = svc.get_vehicle(vid)
        if not v or str(v.get("status")) == "decommissioned":
            flash("Vehicle not found.", "error")
            return redirect(
                request.referrer or url_for("public_fleet.fleet_portal_index")
            )
        svc.add_fleet_issue(
            vehicle_id=vid,
            actor_type="contractor",
            actor_id=cid_s,
            title=(request.form.get("title") or "Issue reported").strip(),
            description=request.form.get("description"),
            user_id=None,
        )
        flash("Issue reported to fleet. Thank you.", "success")
    except Exception as e:
        flash(str(e), "error")
    return redirect(
        request.referrer or url_for("public_fleet.fleet_portal_index")
    )
