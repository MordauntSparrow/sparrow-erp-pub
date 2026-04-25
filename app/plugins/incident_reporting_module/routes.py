from __future__ import annotations

import json
import os
import re
import uuid
from datetime import date, datetime
from functools import wraps
from typing import Any, Dict, List, Optional, Set, Tuple

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename

from app.objects import PluginManager, get_db_connection, has_permission
from app.portal_session import contractor_id_from_tb_user

from . import services as inc_svc
from .constants import (
    ALLOWED_TRANSITIONS,
    HARM_GRADE_DEFAULTS,
    INCIDENT_FORM_VISIBILITY_ROLE_CHOICES,
    INCIDENT_MODES,
    IR1_FIELD_LABELS,
    MERP_FIELD_LABELS,
    ORG_SEVERITY_DEFAULTS,
    PIPELINE_STAGE_GROUPS,
    STATUS_LABELS,
    WALKAROUND_SEVERITY_BADGE_CLASS,
    WALKAROUND_SEVERITY_BTN_OUTLINE,
    WALKAROUND_SEVERITY_CHOICES,
    WALKAROUND_SEVERITY_GUIDANCE,
    WALKAROUND_SEVERITY_SHORT,
)

_plugin_manager = PluginManager(os.path.abspath("app/plugins"))
_core_manifest = _plugin_manager.get_core_manifest() or {}

_template = os.path.join(os.path.dirname(__file__), "templates")

internal_bp = Blueprint(
    "internal_incident_reporting",
    __name__,
    url_prefix="/plugin/incident_reporting_module",
    template_folder=_template,
)
public_bp = Blueprint(
    "public_incident_reporting",
    __name__,
    url_prefix="/incidents",
    template_folder=_template,
)

P_ACCESS = "incident_reporting_module.access"
P_TRIAGE = "incident_reporting_module.triage"
P_INVESTIGATE = "incident_reporting_module.investigate"
P_CLOSE = "incident_reporting_module.close"
P_CONFIGURE = "incident_reporting_module.configure"
P_EXPORT = "incident_reporting_module.export_sensitive"
P_MERGE = "incident_reporting_module.merge"
P_LEGAL = "incident_reporting_module.legal_hold"
P_PORTAL = "incident_reporting_module.portal_submit"
P_VIEW_OWN = "incident_reporting_module.view_own"
P_VIEW_SITE = "incident_reporting_module.view_site"
SESSION_SITE_SCOPE = "incident_site_scope"


@internal_bp.context_processor
def incident_admin_nav_flags():
    try:
        if not getattr(current_user, "is_authenticated", False):
            return {}
    except Exception:
        return {}
    role = str(getattr(current_user, "role", "") or "")
    try:
        _vis = inc_svc.get_form_role_visibility()
    except Exception:
        _vis = {
            "ir1_roles": [],
            "ir1_categories": [],
            "hse_roles": [],
            "hse_categories": [],
        }
    return {
        "incident_nav_can_configure": has_permission(P_CONFIGURE) or has_permission(P_ACCESS),
        "incident_nav_can_export": has_permission(P_EXPORT) or has_permission(P_ACCESS),
        "incident_may_edit_form_role_visibility": has_permission(P_CONFIGURE),
        "incident_show_ir1_form": inc_svc.role_may_see_incident_workspace_form(
            role, "ir1", _vis
        ),
        "incident_show_hse_form": inc_svc.role_may_see_incident_workspace_form(
            role, "hse", _vis
        ),
    }


def _full_incident_admin() -> bool:
    return any(
        has_permission(p)
        for p in (
            P_ACCESS,
            P_TRIAGE,
            P_INVESTIGATE,
            P_CLOSE,
            P_CONFIGURE,
            P_EXPORT,
            P_MERGE,
            P_LEGAL,
        )
    )


def _current_user_contractor_id() -> Optional[int]:
    uid = getattr(current_user, "id", None)
    if not uid:
        return None
    cid = getattr(current_user, "contractor_id", None)
    if cid is not None:
        try:
            return int(cid)
        except (TypeError, ValueError):
            pass
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT contractor_id FROM users WHERE id = %s LIMIT 1", (str(uid),)
        )
        r = cur.fetchone()
        if r and r[0] is not None:
            return int(r[0])
    except Exception:
        return None
    finally:
        cur.close()
        conn.close()
    return None


def _incident_narrow_scope() -> Optional[Dict]:
    """Row-level read scope for list/metrics/export (not admins with full module perms)."""
    if _full_incident_admin():
        return None
    uid = str(getattr(current_user, "id", "") or "")
    cid = _current_user_contractor_id()
    has_own = has_permission(P_VIEW_OWN)
    has_site = has_permission(P_VIEW_SITE)
    site_match = (session.get(SESSION_SITE_SCOPE) or "").strip()
    if has_own and has_site and site_match:
        return {
            "type": "own_or_site",
            "user_id": uid,
            "contractor_id": cid,
            "site_match": site_match,
        }
    if has_site and site_match:
        return {"type": "site", "site_match": site_match}
    if has_own:
        return {"type": "own", "user_id": uid, "contractor_id": cid}
    if has_site and not site_match:
        return {"type": "site_unset"}
    return {"type": "deny"}


def _incident_mutation_guard(incident_id: int):
    """Require incident exists and is visible under row-level narrow scope."""
    row = inc_svc.get_incident(incident_id)
    if not row:
        flash("Incident not found.", "danger")
        return None, redirect(url_for("internal_incident_reporting.admin_browse"))
    if not inc_svc.incident_readable_by_narrow(row, _incident_narrow_scope()):
        flash("You do not have access to this incident.", "danger")
        return None, redirect(url_for("internal_incident_reporting.admin_browse"))
    return row, None


def _admin_view_perms() -> Set[str]:
    return {
        P_ACCESS,
        P_TRIAGE,
        P_INVESTIGATE,
        P_CLOSE,
        P_CONFIGURE,
        P_EXPORT,
        P_MERGE,
        P_LEGAL,
        "incident_reporting_module.view_own",
        "incident_reporting_module.view_site",
    }


def _admin_actor_label() -> str:
    if getattr(current_user, "is_authenticated", False):
        for a in ("email", "name", "username"):
            v = getattr(current_user, a, None)
            if v:
                return str(v)[:255]
        return f"user:{getattr(current_user, 'id', '')}"
    return "system"


def _admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for("routes.login", next=request.path))
        if not any(has_permission(p) for p in _admin_view_perms()):
            flash("You do not have access to Safety & Incidents.", "danger")
            return redirect(url_for("routes.dashboard"))
        return view(*args, **kwargs)

    return wrapped


def _require_any(*perms: str):
    def deco(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for("routes.login", next=request.path))
            if not any(has_permission(p) for p in perms):
                flash("Permission denied.", "danger")
                return redirect(url_for("internal_incident_reporting.admin_index"))
            return view(*args, **kwargs)

        return wrapped

    return deco


def _hse_role_guard_redirect():
    role = str(getattr(current_user, "role", "") or "")
    if not inc_svc.role_may_see_incident_workspace_form(role, "hse"):
        flash("HSE walkarounds are not enabled for your account role.", "warning")
        return redirect(url_for("internal_incident_reporting.admin_index"))
    return None


def _staff_ep(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("tb_user"):
            return redirect("/employee-portal/login?next=" + request.path)
        return view(*args, **kwargs)

    return wrapped


def _tb():
    return session.get("tb_user")


def _contractor_id() -> Optional[int]:
    return contractor_id_from_tb_user(_tb())


def _portal_username() -> str:
    cid = _contractor_id()
    if not cid:
        return ""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT LOWER(TRIM(COALESCE(NULLIF(TRIM(username), ''), TRIM(email)))) FROM tb_contractors WHERE id = %s",
            (int(cid),),
        )
        r = cur.fetchone()
        return (r[0] or "").strip() if r else ""
    finally:
        cur.close()
        conn.close()


def _default_sg_categories() -> Set[str]:
    raw = ""
    try:
        m = _plugin_manager.get_plugin_manifest(
            os.path.join(_plugin_manager.plugins_dir, "incident_reporting_module")
        ) or {}
        st = (m.get("settings") or {}).get("default_safeguarding_required_categories") or {}
        raw = (st.get("value") or "").strip()
    except Exception:
        pass
    return {x.strip() for x in raw.split(",") if x.strip()}


def _app_static_dir() -> str:
    app_pkg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return os.path.join(app_pkg_dir, "static")


_WALKAROUND_UPLOAD_EXT = {".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp"}
_WALKAROUND_MAX_FINDING_ROWS = 40


def _walkaround_finding_row_indices(rid: int) -> List[int]:
    """Indices present in POST for one pending record (sparse rows after JS add-row)."""
    r_int = int(rid)
    pfx = f"finding_point_{r_int}_"
    idxs: Set[int] = set()
    for k in request.form.keys():
        if k.startswith(pfx):
            tail = k[len(pfx) :]
            if tail.isdigit():
                idxs.add(int(tail))
    rxf = re.compile(rf"^finding_file_{r_int}_(\d+)$")
    for k in request.files.keys():
        m = rxf.match(k)
        if m:
            idxs.add(int(m.group(1)))
    if not idxs:
        return []
    return sorted(idxs)[:_WALKAROUND_MAX_FINDING_ROWS]


def _walkaround_findings_from_form(record_id: int) -> List[Dict[str, Any]]:
    """Field names are scoped by ``record_id`` so multiple pending cards do not collide."""
    allowed = {a[0] for a in WALKAROUND_SEVERITY_CHOICES}
    rid = int(record_id)
    out: List[Dict[str, Any]] = []
    for i in _walkaround_finding_row_indices(rid):
        pt = (request.form.get(f"finding_point_{rid}_{i}") or "").strip()
        if not pt:
            continue
        sev = (request.form.get(f"finding_severity_{rid}_{i}") or "low").strip()
        if sev not in allowed:
            sev = "low"
        rb = (request.form.get(f"finding_rectify_by_{rid}_{i}") or "").strip() or None
        g = (request.form.get(f"finding_guidance_{rid}_{i}") or "").strip() or None
        out.append(
            {
                "finding_sort": i,
                "point": pt[:4000],
                "severity": sev,
                "rectify_by": rb,
                "guidance_ref": (g[:512] if g else None),
            }
        )
    return out


def _delete_walkaround_saved_files(rows: List[Tuple[Optional[int], str, str]]) -> None:
    """Remove files written under static/ when a DB transaction fails after upload."""
    for _fs, rel_path, _fn in rows or ():
        try:
            abs_path = os.path.join(_app_static_dir(), rel_path.replace("/", os.sep))
            if os.path.isfile(abs_path):
                os.remove(abs_path)
        except OSError:
            pass


def _save_walkaround_uploads(record_id: int) -> List[Tuple[Optional[int], str, str]]:
    """Persist images for a walkaround row; returns rows for ``hse_walkaround_attachments``."""
    rid = int(record_id)
    rel_dir = os.path.join("uploads", "incident_reporting", "walkaround", str(rid))
    abs_dir = os.path.join(_app_static_dir(), rel_dir.replace("/", os.sep))
    rows: List[Tuple[Optional[int], str, str]] = []

    def _one(fobj, finding_sort: Optional[int], stem: str) -> None:
        if not fobj or not fobj.filename:
            return
        safe = secure_filename(fobj.filename)
        if not safe:
            return
        ext = os.path.splitext(safe)[1].lower()
        if ext not in _WALKAROUND_UPLOAD_EXT:
            raise ValueError(f"Walkaround file type not allowed: {safe}")
        os.makedirs(abs_dir, exist_ok=True)
        name = f"{stem}{ext}"
        rel_path = f"{rel_dir}/{name}".replace("\\", "/")
        fobj.save(os.path.join(abs_dir, name))
        rows.append((finding_sort, rel_path, safe))

    for i in _walkaround_finding_row_indices(rid):
        f = request.files.get(f"finding_file_{rid}_{i}")
        if f and f.filename:
            _one(f, i, f"f{i}_{uuid.uuid4().hex[:10]}")
    for j, f in enumerate(request.files.getlist(f"overview_file_{rid}")):
        if f and f.filename:
            _one(f, None, f"ov{j}_{uuid.uuid4().hex[:8]}")
    return rows


# -----------------------------------------------------------------------------
# Admin
# -----------------------------------------------------------------------------


@internal_bp.post("/incidents/new-draft")
@login_required
@_admin_required
def admin_incident_new_draft():
    """Start a draft incident from the admin console (IR1 fields live on the incident record)."""
    uid = str(getattr(current_user, "id", "") or "").strip() or None
    cid = _current_user_contractor_id()
    try:
        iid = inc_svc.create_draft_admin(
            reporter_user_id=uid,
            reporter_contractor_id=cid,
            actor_label=_admin_actor_label(),
        )
    except Exception:
        flash("Could not create draft incident.", "danger")
        return redirect(url_for("internal_incident_reporting.admin_browse"))
    role = str(getattr(current_user, "role", "") or "")
    open_tab = (
        "ir1"
        if inc_svc.role_may_see_incident_workspace_form(role, "ir1")
        else "summary"
    )
    flash(
        "Draft incident created."
        + (
            " Use the IR1 / Regulatory tab to record regulatory details."
            if open_tab == "ir1"
            else ""
        ),
        "success",
    )
    return redirect(
        url_for(
            "internal_incident_reporting.admin_detail",
            incident_id=iid,
            tab=open_tab,
        )
    )


@internal_bp.get("/")
@login_required
@_admin_required
def admin_index():
    metrics = inc_svc.admin_dashboard_metrics(narrow=_incident_narrow_scope())
    return render_template(
        "incident_reporting_module/admin/index.html",
        metrics=metrics,
        config=_core_manifest,
        can_export=has_permission(P_EXPORT) or has_permission(P_ACCESS),
        can_configure_area=has_permission(P_CONFIGURE) or has_permission(P_ACCESS),
    )


@internal_bp.post("/browse/site-scope")
@login_required
@_admin_required
def admin_browse_site_scope():
    if has_permission(P_VIEW_SITE):
        v = (request.form.get("site_scope") or "").strip()
        session[SESSION_SITE_SCOPE] = v[:255]
        session.modified = True
        flash("Site / division filter saved for this browser session.", "success")
    return redirect(url_for("internal_incident_reporting.admin_browse"))


@internal_bp.get("/browse")
@login_required
@_admin_required
def admin_browse():
    narrow = _incident_narrow_scope()
    if narrow and narrow.get("type") == "site_unset":
        flash(
            "You only have site-level access: set a site or division label filter below "
            "to see incidents for that location.",
            "info",
        )
    rows = inc_svc.admin_list_incidents(
        status=request.args.get("status") or None,
        category=request.args.get("category") or None,
        mode=request.args.get("mode") or None,
        q=request.args.get("q") or None,
        review=request.args.get("review") or None,
        narrow=narrow,
    )
    return render_template(
        "incident_reporting_module/admin/list.html",
        incidents=rows,
        status_labels=STATUS_LABELS,
        modes=INCIDENT_MODES,
        categories=inc_svc.categories_for_tenant(),
        filters={
            "status": request.args.get("status") or "",
            "category": request.args.get("category") or "",
            "mode": request.args.get("mode") or "",
            "q": request.args.get("q") or "",
            "review": request.args.get("review") or "",
        },
        site_scope_session=(session.get(SESSION_SITE_SCOPE) or ""),
        can_set_site_filter=has_permission(P_VIEW_SITE),
        config=_core_manifest,
    )


@internal_bp.get("/pipeline")
@login_required
@_admin_required
def admin_pipeline():
    """IR1-style workflow board: incidents by stage (kanban or flat list)."""
    narrow = _incident_narrow_scope()
    view = (request.args.get("view") or "kanban").strip().lower()
    columns = []
    for col_id, title, statuses in PIPELINE_STAGE_GROUPS:
        rows = inc_svc.admin_list_incidents(
            status_in=statuses,
            narrow=narrow,
            limit=120,
        )
        columns.append(
            {
                "id": col_id,
                "title": title,
                "statuses": statuses,
                "rows": rows,
            }
        )
    flat_rows = None
    if view == "list":
        flat_rows = inc_svc.admin_list_incidents(narrow=narrow, limit=400)
    return render_template(
        "incident_reporting_module/admin/pipeline.html",
        columns=columns,
        view=view,
        flat_rows=flat_rows,
        status_labels=STATUS_LABELS,
        config=_core_manifest,
    )


@internal_bp.get("/incidents/<int:incident_id>")
@login_required
@_admin_required
def admin_detail(incident_id: int):
    row = inc_svc.get_incident(incident_id)
    if not row:
        flash("Incident not found.", "warning")
        return redirect(url_for("internal_incident_reporting.admin_browse"))
    narrow = _incident_narrow_scope()
    if not inc_svc.incident_readable_by_narrow(row, narrow):
        flash("You do not have access to this incident.", "danger")
        return redirect(url_for("internal_incident_reporting.admin_browse"))
    rules = inc_svc.get_form_role_visibility()
    role = str(getattr(current_user, "role", "") or "")
    show_ir1 = inc_svc.role_may_see_incident_workspace_form(role, "ir1", rules)
    tab = (request.args.get("tab") or "summary").strip().lower()
    if tab == "ir1" and not show_ir1:
        flash("The IR1 / Regulatory workspace is not enabled for your role.", "warning")
        tab = "summary"
    sg_url = None
    rid = row.get("linked_safeguarding_referral_id")
    if rid:
        try:
            sg_url = url_for(
                "medical_records_internal.safeguarding_manager_referral",
                referral_id=int(rid),
            )
        except Exception:
            sg_url = None
    st_cur = (row.get("status") or "").strip()
    allowed_next = sorted(ALLOWED_TRANSITIONS.get(st_cur, []))
    med = False
    try:
        from flask import has_app_context

        if has_app_context():
            med = inc_svc.tenant_has_medical()
    except Exception:
        med = False
    tabs = [
        ("summary", "Summary"),
        ("ir1", "IR1 / Regulatory"),
        ("people", "People"),
        ("timeline", "Timeline"),
        ("factors", "Factors"),
        ("actions", "CAPA"),
        ("documents", "Documents"),
        ("discussion", "Discussion"),
        ("links", "Links"),
        ("audit", "Audit"),
    ]
    if med:
        tabs.insert(2, ("clinical", "Clinical"))
    if not show_ir1:
        tabs = [t for t in tabs if t[0] != "ir1"]
    compliance_policies: List[dict] = []
    try:
        from app.plugins.compliance_module import services as comp_svc

        compliance_policies = list((comp_svc.admin_list_policies() or [])[:200])
    except Exception:
        compliance_policies = []
    cpid = row.get("compliance_policy_id")
    compliance_policy_admin_url = None
    if cpid:
        try:
            compliance_policy_admin_url = url_for(
                "internal_compliance.admin_policy_edit", policy_id=int(cpid)
            )
        except Exception:
            compliance_policy_admin_url = None
    return render_template(
        "incident_reporting_module/admin/detail.html",
        incident=row,
        tab=tab,
        workspace_tabs=tabs,
        show_clinical_factors=med,
        allowed_next_statuses=allowed_next,
        status_labels=STATUS_LABELS,
        modes=INCIDENT_MODES,
        categories=inc_svc.categories_for_tenant(),
        org_severities=ORG_SEVERITY_DEFAULTS,
        harm_grades=HARM_GRADE_DEFAULTS,
        comments=inc_svc.list_comments(incident_id),
        actions=inc_svc.list_actions(incident_id),
        timeline=inc_svc.list_timeline(incident_id),
        factor_defs=inc_svc.list_factor_definitions(),
        selected_factors=inc_svc.list_selected_factors(incident_id),
        audit=inc_svc.list_audit(incident_id),
        attachments=inc_svc.list_attachments(incident_id),
        safeguarding_state=inc_svc.safeguarding_badge_state(row),
        safeguarding_manager_url=sg_url,
        config=_core_manifest,
        can_close=has_permission(P_CLOSE),
        can_investigate=has_permission(P_INVESTIGATE),
        can_triage=has_permission(P_TRIAGE),
        can_legal=has_permission(P_LEGAL),
        can_merge=has_permission(P_MERGE),
        can_configure=has_permission(P_CONFIGURE),
        subscribers=inc_svc.list_subscriptions(incident_id),
        user_subscribed=inc_svc.user_is_subscribed(
            incident_id, str(getattr(current_user, "id", "") or "")
        ),
        merp=inc_svc.merp_display(row.get("medication_json")),
        merp_labels=MERP_FIELD_LABELS,
        ir1_labels=IR1_FIELD_LABELS,
        hr_involved_ids=inc_svc.parse_hr_involved_contractor_ids_from_row(row),
        hr_involved_labels=inc_svc.contractor_labels_for_ids(
            inc_svc.parse_hr_involved_contractor_ids_from_row(row)
        ),
        ir1_supplementary_rows=inc_svc.ir1_supplementary_form_rows(
            row.get("ir1_supplementary_json")
        ),
        compliance_policies=compliance_policies,
        compliance_policy_admin_url=compliance_policy_admin_url,
    )


@internal_bp.post("/incidents/<int:incident_id>/save-core")
@login_required
@_require_any(P_TRIAGE, P_INVESTIGATE, P_ACCESS)
def admin_save_core(incident_id: int):
    row, redir = _incident_mutation_guard(incident_id)
    if redir is not None:
        return redir
    existing_med = row.get("medication_json")
    rt = (request.form.get("return_tab") or "summary").strip()
    if rt == "ir1":
        role = str(getattr(current_user, "role", "") or "")
        if not inc_svc.role_may_see_incident_workspace_form(role, "ir1"):
            flash("You cannot update IR1 / Regulatory fields for your role.", "danger")
            return redirect(
                url_for(
                    "internal_incident_reporting.admin_detail",
                    incident_id=incident_id,
                    tab="summary",
                )
            )
    core_fields: Dict[str, Any] = {}
    if rt == "summary":
        core_fields = {
            "title": request.form.get("title"),
            "narrative": request.form.get("narrative"),
            "immediate_actions": request.form.get("immediate_actions"),
            "incident_mode": request.form.get("incident_mode"),
            "category_slug": request.form.get("category_slug"),
            "org_severity_code": request.form.get("org_severity_code"),
            "site_label": request.form.get("site_label"),
            "shift_reference": request.form.get("shift_reference"),
            "vehicle_reference": request.form.get("vehicle_reference"),
        }
        if inc_svc.tenant_has_medical():
            core_fields["harm_grade_code"] = request.form.get("harm_grade_code")
            core_fields["patient_involved"] = request.form.get("patient_involved")
            core_fields["deidentified_narrative"] = request.form.get(
                "deidentified_narrative"
            )
            core_fields["safeguarding_required"] = request.form.get(
                "safeguarding_required"
            )
    elif rt == "ir1":
        core_fields = {
            "incident_occurred_at": request.form.get("incident_occurred_at"),
            "incident_discovered_at": request.form.get("incident_discovered_at"),
            "exact_location_detail": request.form.get("exact_location_detail"),
            "witnesses_text": request.form.get("witnesses_text"),
            "equipment_involved": request.form.get("equipment_involved"),
            "riddor_notifiable": request.form.get("riddor_notifiable"),
            "reporter_job_title": request.form.get("reporter_job_title"),
            "reporter_department": request.form.get("reporter_department"),
            "reporter_contact_phone": request.form.get("reporter_contact_phone"),
            "people_affected_count": request.form.get("people_affected_count"),
            "ir1_supplementary_json": inc_svc.ir1_supplementary_from_form(request.form),
        }
    elif rt == "clinical":
        if not inc_svc.tenant_has_medical():
            flash("Clinical tab is not enabled for this organisation.", "warning")
            return redirect(
                url_for(
                    "internal_incident_reporting.admin_detail",
                    incident_id=incident_id,
                    tab="summary",
                )
            )
        core_fields = {
            "patient_involved": request.form.get("patient_involved"),
            "deidentified_narrative": request.form.get("deidentified_narrative"),
            "barrier_notes": request.form.get("barrier_notes"),
            "five_whys": request.form.get("five_whys"),
        }
        raw = (request.form.get("medication_json") or "").strip()
        if raw:
            try:
                core_fields["medication_json"] = json.loads(raw)
            except json.JSONDecodeError:
                flash("Medication JSON was not valid JSON.", "warning")
        else:
            core_fields["medication_json"] = inc_svc.merge_medication_payload(
                existing_med, dict(request.form)
            )
    elif rt == "links":
        core_fields = {
            "compliance_policy_id": request.form.get("compliance_policy_id"),
        }
    else:
        flash("Nothing to save for this tab.", "warning")
        return redirect(
            url_for(
                "internal_incident_reporting.admin_detail",
                incident_id=incident_id,
                tab=(
                    rt
                    if rt
                    in {
                        "summary",
                        "ir1",
                        "people",
                        "clinical",
                        "timeline",
                        "factors",
                        "actions",
                        "documents",
                        "discussion",
                        "links",
                        "audit",
                    }
                    else "summary"
                ),
            )
        )
    inc_svc.update_incident_core(
        incident_id,
        core_fields,
        _admin_actor_label(),
    )
    flash("Incident saved.", "success")
    return redirect(
        url_for(
            "internal_incident_reporting.admin_detail",
            incident_id=incident_id,
            tab=request.form.get("return_tab") or "summary",
        )
    )


@internal_bp.post("/incidents/<int:incident_id>/people")
@login_required
@_require_any(P_TRIAGE, P_INVESTIGATE, P_ACCESS)
def admin_people_post(incident_id: int):
    row, redir = _incident_mutation_guard(incident_id)
    if redir is not None:
        return redir
    ids = inc_svc.hr_involved_contractor_ids_from_form(request.form)
    bad = inc_svc.contractor_ids_not_in_database(ids)
    if bad:
        flash(
            "Unknown employee id(s): " + ", ".join(str(x) for x in bad),
            "danger",
        )
        return redirect(
            url_for(
                "internal_incident_reporting.admin_detail",
                incident_id=incident_id,
                tab="people",
            )
        )
    inc_svc.update_incident_core(
        incident_id,
        {"hr_involved_contractor_ids": ids},
        _admin_actor_label(),
    )
    flash("Saved.", "success")
    return redirect(
        url_for(
            "internal_incident_reporting.admin_detail",
            incident_id=incident_id,
            tab="people",
        )
    )


@internal_bp.get("/contractors-search")
@login_required
@_require_any(P_TRIAGE, P_INVESTIGATE, P_ACCESS)
def admin_contractors_search():
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify([])
    try:
        from app.plugins.hr_module import services as hr_services

        rows = hr_services.admin_search_contractors(q, limit=25)
    except Exception:
        rows = []
    out: List[dict] = []
    for r in rows or []:
        try:
            cid = int(r.get("id"))
        except (TypeError, ValueError):
            continue
        nm = (r.get("name") or "").strip() or "—"
        out.append({"id": cid, "label": f"{nm} · id {cid}"})
    return jsonify(out)


@internal_bp.get("/incidents-search")
@login_required
@_require_any(P_CONFIGURE, P_ACCESS, P_TRIAGE, P_INVESTIGATE, P_MERGE)
def admin_incidents_search():
    """Autocomplete incidents visible under the current narrow scope (walkaround link picker, merge, etc.)."""
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify([])
    ex_raw = (request.args.get("exclude") or "").strip()
    exclude_id: Optional[int] = None
    if ex_raw.isdigit():
        try:
            exclude_id = int(ex_raw)
        except (TypeError, ValueError):
            exclude_id = None
    rows = inc_svc.admin_list_incidents(q=q, narrow=_incident_narrow_scope(), limit=20)
    out: List[dict] = []
    for r in rows or []:
        try:
            iid = int(r["id"])
        except (TypeError, ValueError):
            continue
        if exclude_id is not None and iid == exclude_id:
            continue
        ref = (r.get("reference_code") or "").strip() or ("#" + str(iid))
        title = ((r.get("title") or "") or "Incident").strip()[:100]
        st = (r.get("status") or "").strip()
        st_lab = STATUS_LABELS.get(st, st)
        out.append({"id": iid, "label": f"{ref} · {title} · {st_lab}"})
    return jsonify(out)


@internal_bp.post("/incidents/<int:incident_id>/status")
@login_required
@_require_any(P_TRIAGE, P_INVESTIGATE, P_CLOSE, P_ACCESS)
def admin_status_post(incident_id: int):
    _, redir = _incident_mutation_guard(incident_id)
    if redir is not None:
        return redir
    to_status = (request.form.get("to_status") or "").strip()
    if not to_status:
        flash("Choose a target status.", "warning")
        return redirect(
            url_for("internal_incident_reporting.admin_detail", incident_id=incident_id)
        )
    uid = str(getattr(current_user, "id", "") or "").strip() or None
    ok, msg = inc_svc.change_status(
        incident_id,
        to_status,
        _admin_actor_label(),
        note=request.form.get("note"),
        actor_user_id=uid,
    )
    flash("Status updated." if ok else msg, "success" if ok else "danger")
    return redirect(
        url_for("internal_incident_reporting.admin_detail", incident_id=incident_id)
    )


@internal_bp.post("/incidents/<int:incident_id>/comment")
@login_required
@_require_any(P_TRIAGE, P_INVESTIGATE, P_ACCESS)
def admin_comment_post(incident_id: int):
    _, redir = _incident_mutation_guard(incident_id)
    if redir is not None:
        return redir
    body = (request.form.get("body") or "").strip()
    if body:
        uid = str(getattr(current_user, "id", "") or "") or None
        inc_svc.add_comment(
            incident_id,
            body,
            _admin_actor_label(),
            author_user_id=uid,
        )
        flash("Comment added.", "success")
    return redirect(
        url_for(
            "internal_incident_reporting.admin_detail",
            incident_id=incident_id,
            tab="discussion",
        )
    )


@internal_bp.post("/incidents/<int:incident_id>/action")
@login_required
@_require_any(P_INVESTIGATE, P_TRIAGE, P_ACCESS)
def admin_action_post(incident_id: int):
    _, redir = _incident_mutation_guard(incident_id)
    if redir is not None:
        return redir
    data = {
        "title": request.form.get("title"),
        "description": request.form.get("description"),
        "owner_label": request.form.get("owner_label"),
        "due_date": request.form.get("due_date") or None,
        "status": request.form.get("status") or "open",
        "effectiveness_review": request.form.get("effectiveness_review"),
    }
    aid = request.form.get("action_id")
    uid = str(getattr(current_user, "id", "") or "").strip() or None
    inc_svc.upsert_action(
        incident_id,
        data,
        _admin_actor_label(),
        action_id=int(aid) if aid and aid.isdigit() else None,
        actor_user_id=uid,
    )
    flash("Action saved.", "success")
    return redirect(
        url_for(
            "internal_incident_reporting.admin_detail",
            incident_id=incident_id,
            tab="actions",
        )
    )


@internal_bp.post("/incidents/<int:incident_id>/timeline")
@login_required
@_require_any(P_INVESTIGATE, P_ACCESS)
def admin_timeline_post(incident_id: int):
    _, redir = _incident_mutation_guard(incident_id)
    if redir is not None:
        return redir
    inc_svc.add_timeline_event(
        incident_id,
        {
            "event_time": request.form.get("event_time"),
            "label": request.form.get("label"),
            "body": request.form.get("body"),
            "sort_order": request.form.get("sort_order") or 0,
        },
        _admin_actor_label(),
    )
    flash("Timeline event added.", "success")
    return redirect(
        url_for(
            "internal_incident_reporting.admin_detail",
            incident_id=incident_id,
            tab="timeline",
        )
    )


@internal_bp.post("/incidents/<int:incident_id>/factors")
@login_required
@_require_any(P_INVESTIGATE, P_ACCESS)
def admin_factors_post(incident_id: int):
    _, redir = _incident_mutation_guard(incident_id)
    if redir is not None:
        return redir
    raw = request.form.get("factor_codes") or ""
    codes = [c.strip() for c in raw.split(",") if c.strip()]
    if not codes:
        codes = request.form.getlist("factor_code")
    inc_svc.set_incident_factors(incident_id, codes, _admin_actor_label())
    flash("Contributory factors updated.", "success")
    return redirect(
        url_for(
            "internal_incident_reporting.admin_detail",
            incident_id=incident_id,
            tab="factors",
        )
    )


@internal_bp.post("/incidents/<int:incident_id>/legal-hold")
@login_required
@_require_any(P_LEGAL, P_ACCESS)
def admin_legal_hold_post(incident_id: int):
    _, redir = _incident_mutation_guard(incident_id)
    if redir is not None:
        return redir
    on = request.form.get("on") == "1"
    inc_svc.set_legal_hold(incident_id, on, _admin_actor_label())
    flash("Legal hold updated.", "success")
    return redirect(
        url_for("internal_incident_reporting.admin_detail", incident_id=incident_id)
    )


@internal_bp.post("/incidents/<int:incident_id>/merge")
@login_required
@_require_any(P_MERGE, P_ACCESS)
def admin_merge_post(incident_id: int):
    _, redir = _incident_mutation_guard(incident_id)
    if redir is not None:
        return redir
    other = request.form.get("merge_incident_id")
    if other and other.isdigit():
        _, oredir = _incident_mutation_guard(int(other))
        if oredir is not None:
            flash("The other incident is not accessible with your scope.", "danger")
            return redirect(url_for("internal_incident_reporting.admin_browse"))
        ok, msg = inc_svc.merge_incidents(incident_id, int(other), _admin_actor_label())
        flash(msg if not ok else "Incidents merged.", "danger" if not ok else "success")
    return redirect(url_for("internal_incident_reporting.admin_browse"))


@internal_bp.get("/analytics")
@login_required
@_admin_required
def admin_analytics():
    narrow = _incident_narrow_scope()
    pareto = inc_svc.analytics_pareto(narrow=narrow)
    ttc = inc_svc.analytics_time_to_close(narrow=narrow)
    return render_template(
        "incident_reporting_module/admin/analytics.html",
        pareto=pareto,
        ttc=ttc,
        config=_core_manifest,
    )


@internal_bp.get("/export/incidents.csv")
@login_required
@_require_any(P_EXPORT, P_ACCESS)
def admin_export_csv():
    data = inc_svc.export_incidents_csv(
        {
            "status": request.args.get("status"),
            "category": request.args.get("category"),
            "mode": request.args.get("mode"),
            "q": request.args.get("q"),
            "review": request.args.get("review"),
            "narrow": _incident_narrow_scope(),
        }
    )
    return Response(
        data,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=incidents-{uuid.uuid4().hex[:8]}.csv"
        },
    )


@internal_bp.get("/walkarounds")
@login_required
@_require_any(P_CONFIGURE, P_ACCESS)
def admin_walkarounds():
    redir = _hse_role_guard_redirect()
    if redir is not None:
        return redir
    pending = inc_svc.list_walkaround_records(status="pending", limit=250)
    today = datetime.utcnow().date()
    for p in pending:
        raw = p.get("template_checklist_json")
        if isinstance(raw, (list, tuple)):
            p["_checklist"] = list(raw)
        elif isinstance(raw, str) and raw.strip():
            try:
                p["_checklist"] = json.loads(raw)
            except json.JSONDecodeError:
                p["_checklist"] = []
        else:
            p["_checklist"] = []
        da = p.get("due_at")
        if isinstance(da, datetime):
            p["_due_cmp"] = da.date()
        elif isinstance(da, date):
            p["_due_cmp"] = da
        elif isinstance(da, str) and len(da) >= 10:
            try:
                p["_due_cmp"] = date.fromisoformat(da[:10])
            except ValueError:
                p["_due_cmp"] = None
        else:
            p["_due_cmp"] = None
    completed = inc_svc.list_walkaround_records(status="complete", limit=40)
    wa_ops = inc_svc.walkaround_operational_dashboard()
    finding_severity_ui_json = json.dumps(
        {
            "choices": [list(x) for x in WALKAROUND_SEVERITY_CHOICES],
            "outline": dict(WALKAROUND_SEVERITY_BTN_OUTLINE),
            "short": dict(WALKAROUND_SEVERITY_SHORT),
        }
    )
    return render_template(
        "incident_reporting_module/admin/walkarounds.html",
        pending=pending,
        completed=completed,
        today=today,
        wa_ops=wa_ops,
        config=_core_manifest,
        can_configure=has_permission(P_CONFIGURE),
        severity_choices=WALKAROUND_SEVERITY_CHOICES,
        severity_guidance=WALKAROUND_SEVERITY_GUIDANCE,
        severity_btn_outline=WALKAROUND_SEVERITY_BTN_OUTLINE,
        severity_short=WALKAROUND_SEVERITY_SHORT,
        finding_severity_labels=dict(WALKAROUND_SEVERITY_CHOICES),
        finding_severity_ui_json=finding_severity_ui_json,
    )


@internal_bp.get("/configuration/walkarounds")
@login_required
@_require_any(P_CONFIGURE, P_ACCESS)
def admin_configuration_walkarounds():
    redir = _hse_role_guard_redirect()
    if redir is not None:
        return redir
    templates = inc_svc.list_walkaround_templates()
    for t in templates:
        t["_checklist_form_rows"] = inc_svc.walkaround_checklist_form_display_rows(
            t.get("checklist_json"), extra_blanks=0
        )
    new_checklist_form_rows = inc_svc.walkaround_checklist_form_display_rows(
        None, extra_blanks=0
    )
    return render_template(
        "incident_reporting_module/admin/configuration_walkarounds.html",
        templates=templates,
        new_checklist_form_rows=new_checklist_form_rows,
        config=_core_manifest,
        can_configure=has_permission(P_CONFIGURE),
    )


@internal_bp.post("/walkarounds/template")
@login_required
@_require_any(P_CONFIGURE, P_ACCESS)
def admin_walkaround_template_post():
    redir = _hse_role_guard_redirect()
    if redir is not None:
        return redir
    tid = request.form.get("id")
    prefix = f"tw{int(tid)}_" if tid and tid.isdigit() else "nwl_"
    checklist_json = inc_svc.walkaround_checklist_from_form_prefix(request.form, prefix)
    inc_svc.upsert_walkaround_template(
        int(tid) if tid and tid.isdigit() else None,
        {
            "name": request.form.get("name"),
            "description": request.form.get("description"),
            "site_label": request.form.get("site_label"),
            "checklist_json": checklist_json,
            "interval_days": request.form.get("interval_days") or 7,
            "active": request.form.get("active"),
        },
        _admin_actor_label(),
    )
    flash("HSE walkaround definition saved.", "success")
    return redirect(
        url_for("internal_incident_reporting.admin_configuration_walkarounds")
    )


@internal_bp.post("/walkarounds/template/delete")
@login_required
@_require_any(P_CONFIGURE, P_ACCESS)
def admin_walkaround_template_delete():
    redir = _hse_role_guard_redirect()
    if redir is not None:
        return redir
    tid = request.form.get("id")
    if tid and tid.isdigit():
        inc_svc.delete_walkaround_template(int(tid))
        flash("Walkaround definition removed.", "success")
    return redirect(
        url_for("internal_incident_reporting.admin_configuration_walkarounds")
    )


@internal_bp.post("/walkarounds/complete")
@login_required
@_require_any(P_TRIAGE, P_INVESTIGATE, P_ACCESS)
def admin_walkaround_complete_post():
    redir = _hse_role_guard_redirect()
    if redir is not None:
        return redir
    rid = request.form.get("record_id")
    if not rid or not str(rid).isdigit():
        flash("Missing record.", "warning")
        return redirect(url_for("internal_incident_reporting.admin_walkarounds"))
    answers: Dict[str, object] = {}
    rid_int = int(rid)
    pfx_chk = f"wchk_{rid_int}_"
    pfx_txt = f"wtxt_{rid_int}_"
    for k in request.form:
        if k.startswith(pfx_chk):
            answers[k[len(pfx_chk) :]] = True
        elif k.startswith(pfx_txt):
            answers[k[len(pfx_txt) :]] = (request.form.get(k) or "").strip()
    notes = (request.form.get("notes") or "").strip()
    link = request.form.get("linked_incident_id")
    link_id = int(link) if link and str(link).isdigit() else None
    if link_id:
        lk = inc_svc.get_incident(link_id)
        if not lk or not inc_svc.incident_readable_by_narrow(lk, _incident_narrow_scope()):
            flash("Linked incident was ignored (not found or outside your access scope).", "warning")
            link_id = None
    findings = _walkaround_findings_from_form(int(rid))
    try:
        uploads = _save_walkaround_uploads(int(rid))
    except ValueError as ex:
        flash(str(ex), "danger")
        return redirect(url_for("internal_incident_reporting.admin_walkarounds"))
    uid = str(getattr(current_user, "id", "") or "").strip() or None
    ok, msg = inc_svc.complete_walkaround_record(
        int(rid),
        answers=answers,
        notes=notes or None,
        actor_label=_admin_actor_label(),
        actor_user_id=uid,
        findings=findings,
        attachment_rows=uploads,
        linked_incident_id=link_id,
    )
    if not ok:
        _delete_walkaround_saved_files(uploads)
    flash("Walkaround logged." if ok else msg, "success" if ok else "danger")
    if ok:
        flash(
            "You can open the printable debrief pack from the completed list below.",
            "info",
        )
    return redirect(url_for("internal_incident_reporting.admin_walkarounds"))


@internal_bp.get("/walkarounds/report/<int:record_id>")
@login_required
@_require_any(P_CONFIGURE, P_ACCESS, P_TRIAGE, P_INVESTIGATE)
def admin_walkaround_report(record_id: int):
    redir = _hse_role_guard_redirect()
    if redir is not None:
        return redir
    row = inc_svc.get_walkaround_report_detail(int(record_id))
    if not row:
        flash("Report not found or walkaround is not completed yet.", "warning")
        return redirect(url_for("internal_incident_reporting.admin_walkarounds"))
    atts = row.get("attachments") or []
    by_finding: Dict[int, List[dict]] = {}
    overview: List[dict] = []
    for a in atts:
        fs = a.get("finding_sort")
        if fs is None:
            overview.append(a)
        else:
            by_finding.setdefault(int(fs), []).append(a)
    org_line = None
    try:
        cfg = getattr(current_app, "config", {}) or {}
        core = cfg.get("CORE_MANIFEST") or cfg.get("core_manifest") or {}
        if isinstance(core, dict):
            op = core.get("organization_profile")
            if isinstance(op, dict):
                org_line = (op.get("legal_name") or op.get("trading_name") or "").strip() or None
    except Exception:
        org_line = None
    return render_template(
        "incident_reporting_module/admin/walkaround_report.html",
        r=row,
        findings=row.get("findings_parsed") or [],
        checklist_human=row.get("checklist_human") or [],
        overview_attachments=overview,
        attachments_by_finding=by_finding,
        severity_guidance=WALKAROUND_SEVERITY_GUIDANCE,
        severity_labels=dict(WALKAROUND_SEVERITY_CHOICES),
        severity_choices=WALKAROUND_SEVERITY_CHOICES,
        severity_badge_class=WALKAROUND_SEVERITY_BADGE_CLASS,
        organization_line=org_line,
        document_title="Health & safety walkaround — completion & debrief pack",
        printed_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        config=_core_manifest,
    )


@internal_bp.get("/configuration")
@login_required
@_require_any(P_CONFIGURE, P_ACCESS)
def admin_configuration():
    return render_template(
        "incident_reporting_module/admin/configuration.html",
        rules=inc_svc.list_routing_rules(),
        config=_core_manifest,
        form_role_visibility=inc_svc.get_form_role_visibility(),
        form_visibility_role_choices=INCIDENT_FORM_VISIBILITY_ROLE_CHOICES,
        form_visibility_category_rows=inc_svc.form_visibility_category_rows(),
    )


@internal_bp.post("/configuration/form-role-visibility")
@login_required
@_require_any(P_CONFIGURE)
def admin_configuration_form_role_visibility_post():
    inc_svc.set_form_role_visibility(
        request.form.getlist("ir1_roles"),
        request.form.getlist("hse_roles"),
        request.form.getlist("ir1_categories"),
        request.form.getlist("hse_categories"),
    )
    flash("Form visibility by role saved.", "success")
    return redirect(
        url_for("internal_incident_reporting.admin_configuration")
        + "#form-role-visibility"
    )


@internal_bp.post("/configuration/routing")
@login_required
@_require_any(P_CONFIGURE, P_ACCESS)
def admin_routing_post():
    rid = request.form.get("id")
    inc_svc.upsert_routing_rule(
        {
            "name": request.form.get("name"),
            "category_slug": request.form.get("category_slug") or None,
            "org_severity_codes": request.form.get("org_severity_codes") or None,
            "assignee_label": request.form.get("assignee_label"),
            "sla_hours": int(request.form.get("sla_hours") or 0) or None,
            "priority_order": int(request.form.get("priority_order") or 0),
            "active": request.form.get("active") == "1",
        },
        rule_id=int(rid) if rid and rid.isdigit() else None,
    )
    flash("Routing rule saved.", "success")
    return redirect(url_for("internal_incident_reporting.admin_configuration"))


@internal_bp.post("/configuration/routing/delete")
@login_required
@_require_any(P_CONFIGURE, P_ACCESS)
def admin_routing_delete():
    rid = request.form.get("id")
    if rid and rid.isdigit():
        inc_svc.delete_routing_rule(int(rid))
        flash("Rule removed.", "success")
    else:
        flash("Invalid rule.", "warning")
    return redirect(url_for("internal_incident_reporting.admin_configuration"))


@internal_bp.post("/incidents/<int:incident_id>/subscribe")
@login_required
@_admin_required
def admin_subscribe_post(incident_id: int):
    row = inc_svc.get_incident(incident_id)
    if not row or not inc_svc.incident_readable_by_narrow(row, _incident_narrow_scope()):
        flash("Incident not found.", "danger")
        return redirect(url_for("internal_incident_reporting.admin_browse"))
    uid = str(getattr(current_user, "id", "") or "")
    if not uid:
        flash("Missing user id.", "danger")
        return redirect(url_for("internal_incident_reporting.admin_detail", incident_id=incident_id))
    on = request.form.get("subscribe") == "1"
    inc_svc.set_user_subscribed(
        incident_id,
        uid,
        on,
        events=(request.form.get("events") or "all")[:255],
        actor_label=_admin_actor_label(),
    )
    flash("Subscription updated." if on else "Unsubscribed.", "success")
    return redirect(
        url_for(
            "internal_incident_reporting.admin_detail",
            incident_id=incident_id,
            tab=request.form.get("return_tab") or "summary",
        )
    )


@internal_bp.post("/incidents/<int:incident_id>/upload")
@login_required
@_require_any(P_INVESTIGATE, P_ACCESS)
def admin_upload(incident_id: int):
    row, redir = _incident_mutation_guard(incident_id)
    if redir is not None:
        return redir
    f = request.files.get("file")
    if not f or not f.filename:
        flash("Choose a file.", "warning")
        return redirect(
            url_for(
                "internal_incident_reporting.admin_detail",
                incident_id=incident_id,
                tab="documents",
            )
        )
    safe = secure_filename(f.filename)
    ext = os.path.splitext(safe)[1].lower()
    if ext not in {".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".txt", ".csv"}:
        flash("File type not allowed.", "danger")
        return redirect(
            url_for(
                "internal_incident_reporting.admin_detail",
                incident_id=incident_id,
                tab="documents",
            )
        )
    rel_dir = os.path.join("uploads", "incident_reporting", str(incident_id))
    abs_dir = os.path.join(_app_static_dir(), rel_dir.replace("/", os.sep))
    os.makedirs(abs_dir, exist_ok=True)
    name = f"{uuid.uuid4().hex}{ext}"
    rel_path = f"{rel_dir}/{name}".replace("\\", "/")
    f.save(os.path.join(abs_dir, name))
    inc_svc.add_attachment(incident_id, rel_path, safe, _admin_actor_label())
    try:
        ref = ((row.get("reference_code") or "").strip() or f"#{incident_id}")
        title = ((row.get("title") or "") or "Incident")[:120]
        uid = str(getattr(current_user, "id", "") or "").strip() or None
        inc_svc.notify_subscribers_for_incident(
            incident_id,
            "attachment",
            f"New attachment on {ref} — {title}",
            (
                f"{_admin_actor_label()} uploaded file: {safe}\n"
                f"Open: /plugin/incident_reporting_module/incidents/{incident_id}"
            ),
            exclude_user_id=uid,
        )
    except Exception as ex:
        current_app.logger.warning("subscriber notify (upload): %s", ex)
    flash("File uploaded.", "success")
    return redirect(
        url_for(
            "internal_incident_reporting.admin_detail",
            incident_id=incident_id,
            tab="documents",
        )
    )


# -----------------------------------------------------------------------------
# Portal (authenticated contractor / linked user session)
# -----------------------------------------------------------------------------


@public_bp.get("/")
@_staff_ep
def portal_home():
    return render_template(
        "incident_reporting_module/portal/home.html",
        user=_tb(),
        config=_core_manifest,
    )


@public_bp.get("/my")
@_staff_ep
def portal_my():
    cid = _contractor_id()
    rows = inc_svc.portal_list_my(int(cid)) if cid else []
    return render_template(
        "incident_reporting_module/portal/my.html",
        rows=rows,
        user=_tb(),
        config=_core_manifest,
    )


@public_bp.post("/wizard/start")
@_staff_ep
def portal_wizard_start():
    cid = _contractor_id()
    if not cid:
        flash("Session error.", "danger")
        return redirect(url_for("public_incident_reporting.portal_home"))
    uid = None
    try:
        uid = str(_tb().get("linked_user_id") or "") or None
    except Exception:
        pass
    pub = inc_svc.create_draft_portal(
        contractor_id=int(cid),
        reporter_user_id=uid,
        actor_label=_portal_username() or str(cid),
    )
    return redirect(url_for("public_incident_reporting.portal_wizard", public_uuid=pub))


@public_bp.route("/wizard/<public_uuid>", methods=["GET", "POST"])
@_staff_ep
def portal_wizard(public_uuid: str):
    row = inc_svc.get_incident_by_uuid(public_uuid)
    if not row:
        flash("Draft not found.", "warning")
        return redirect(url_for("public_incident_reporting.portal_my"))
    cid = _contractor_id()
    if int(row.get("reporter_contractor_id") or 0) != int(cid or 0):
        flash("You cannot edit this draft.", "danger")
        return redirect(url_for("public_incident_reporting.portal_my"))
    step = (request.args.get("step") or "1").strip()
    if request.method == "POST":
        core_fields = {
            "title": request.form.get("title"),
            "narrative": request.form.get("narrative"),
            "immediate_actions": request.form.get("immediate_actions"),
            "incident_mode": request.form.get("incident_mode"),
            "category_slug": request.form.get("category_slug"),
            "org_severity_code": request.form.get("org_severity_code"),
            "harm_grade_code": request.form.get("harm_grade_code"),
            "patient_involved": request.form.get("patient_involved"),
            "deidentified_narrative": request.form.get("deidentified_narrative"),
            "site_label": request.form.get("site_label"),
            "shift_reference": request.form.get("shift_reference"),
            "vehicle_reference": request.form.get("vehicle_reference"),
            "safeguarding_required": request.form.get("safeguarding_required"),
            "barrier_notes": request.form.get("barrier_notes"),
            "five_whys": request.form.get("five_whys"),
            "incident_occurred_at": request.form.get("incident_occurred_at"),
            "incident_discovered_at": request.form.get("incident_discovered_at"),
            "exact_location_detail": request.form.get("exact_location_detail"),
            "witnesses_text": request.form.get("witnesses_text"),
            "equipment_involved": request.form.get("equipment_involved"),
            "riddor_notifiable": request.form.get("riddor_notifiable"),
            "reporter_job_title": request.form.get("reporter_job_title"),
            "reporter_department": request.form.get("reporter_department"),
            "reporter_contact_phone": request.form.get("reporter_contact_phone"),
            "people_affected_count": request.form.get("people_affected_count"),
        }
        if inc_svc.tenant_has_medical():
            raw = (request.form.get("medication_json") or "").strip()
            if raw:
                try:
                    core_fields["medication_json"] = json.loads(raw)
                except json.JSONDecodeError:
                    flash("Medication JSON was not valid JSON.", "warning")
            elif request.form.get("merge_merp") == "1":
                core_fields["medication_json"] = inc_svc.merge_medication_payload(
                    row.get("medication_json"), dict(request.form)
                )
        inc_svc.update_incident_core(
            int(row["id"]),
            core_fields,
            _portal_username() or str(cid),
        )
        flash("Saved.", "success")
        return redirect(
            url_for(
                "public_incident_reporting.portal_wizard",
                public_uuid=public_uuid,
                step=request.form.get("next_step") or step,
            )
        )
    cat = (row.get("category_slug") or "").strip()
    auto_sg = cat in _default_sg_categories()
    return render_template(
        "incident_reporting_module/portal/wizard.html",
        incident=row,
        step=step,
        modes=INCIDENT_MODES,
        categories=inc_svc.categories_for_tenant(),
        org_severities=ORG_SEVERITY_DEFAULTS,
        harm_grades=HARM_GRADE_DEFAULTS,
        medical=inc_svc.tenant_has_medical(),
        auto_safeguarding=auto_sg,
        safeguarding_state=inc_svc.safeguarding_badge_state(row),
        merp=inc_svc.merp_display(row.get("medication_json")),
        merp_labels=MERP_FIELD_LABELS,
        user=_tb(),
        config=_core_manifest,
    )


@public_bp.post("/wizard/<public_uuid>/safeguarding")
@_staff_ep
def portal_wizard_safeguarding(public_uuid: str):
    row = inc_svc.get_incident_by_uuid(public_uuid)
    if not row or int(row.get("reporter_contractor_id") or 0) != int(_contractor_id() or 0):
        flash("Not allowed.", "danger")
        return redirect(url_for("public_incident_reporting.portal_my"))
    if not inc_svc.tenant_has_medical():
        flash("Safeguarding is only available for medical industry profile.", "info")
        return redirect(
            url_for("public_incident_reporting.portal_wizard", public_uuid=public_uuid, step="4")
        )
    payload = {
        "incident_public_uuid": public_uuid,
        "summary": (request.form.get("sg_summary") or row.get("title") or "")[:2000],
        "narrative_excerpt": (request.form.get("sg_notes") or "")[:8000],
    }
    op_ev = request.form.get("operational_event_id")
    op_id = int(op_ev) if op_ev and op_ev.isdigit() else None
    ok, msg = inc_svc.portal_create_safeguarding_and_link(
        incident_public_uuid=public_uuid,
        contractor_username=_portal_username(),
        payload=payload,
        operational_event_id=op_id,
    )
    flash("Safeguarding referral linked." if ok else msg, "success" if ok else "danger")
    return redirect(
        url_for("public_incident_reporting.portal_wizard", public_uuid=public_uuid, step="4")
    )


@public_bp.post("/wizard/<public_uuid>/submit")
@_staff_ep
def portal_wizard_submit(public_uuid: str):
    row = inc_svc.get_incident_by_uuid(public_uuid)
    if not row or int(row.get("reporter_contractor_id") or 0) != int(_contractor_id() or 0):
        flash("Not allowed.", "danger")
        return redirect(url_for("public_incident_reporting.portal_my"))
    ok, msg = inc_svc.change_status(
        int(row["id"]), "submitted", _portal_username() or str(_contractor_id())
    )
    flash("Submitted." if ok else msg, "success" if ok else "danger")
    return redirect(url_for("public_incident_reporting.portal_my"))


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_bp
