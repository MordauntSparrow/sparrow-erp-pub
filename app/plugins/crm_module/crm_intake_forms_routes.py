# CRM — configurable intake forms (admin + desk run)
from __future__ import annotations

from flask import flash, redirect, render_template, request, url_for
from flask_login import login_required

from .crm_common import can_edit, crm_access_required, crm_edit_required
from .crm_intake_forms_data import (
    default_fields_json,
    delete_intake_form,
    get_intake_form_by_id,
    insert_intake_form,
    list_intake_forms,
    normalize_fields,
    normalize_stage,
    update_intake_form,
    validate_slug,
)
from .crm_website_lead_bridge import create_lead_from_website_submission


def _parse_fields_post() -> list[dict]:
    names = request.form.getlist("field_name[]")
    labels = request.form.getlist("field_label[]")
    types = request.form.getlist("field_type[]")
    req_flags = request.form.getlist("field_required[]")
    rows: list[dict] = []
    n = max(len(names), len(labels), len(types))
    for i in range(n):
        nm = (names[i] if i < len(names) else "").strip().lower()
        if not nm:
            continue
        lb = (labels[i] if i < len(labels) else "").strip() or nm.replace("_", " ").title()
        typ = (types[i] if i < len(types) else "text").strip().lower() or "text"
        rf = (req_flags[i] if i < len(req_flags) else "0").strip().lower()
        req = rf in ("1", "true", "yes", "on")
        rows.append({"name": nm, "label": lb[:255], "type": typ, "required": bool(req)})
    return normalize_fields(rows)


def _submission_dict_from_request() -> dict:
    raw = request.form.to_dict(flat=False)
    out: dict = {}
    for k, v in raw.items():
        if k == "_csrf_token":
            continue
        if isinstance(v, list):
            out[k] = v[0] if len(v) == 1 else v
        else:
            out[k] = v
    return out


def register_crm_intake_form_routes(crm_bp):
    @crm_bp.route("/intake-forms")
    @login_required
    @crm_access_required
    def intake_forms_list():
        forms = []
        try:
            forms = list_intake_forms()
        except Exception:
            flash(
                "Intake forms table is missing. Run CRM module install/upgrade, then refresh.",
                "warning",
            )
        public_routes_ok = False
        try:
            url_for("crm_public.public_intake_form", slug="x")
            public_routes_ok = True
        except Exception:
            public_routes_ok = False
        return render_template(
            "admin/crm_intake_forms_list.html",
            forms=forms,
            public_routes_ok=public_routes_ok,
            can_edit=can_edit(),
        )

    @crm_bp.route("/intake-forms/new", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def intake_form_new():
        if request.method == "POST":
            slug = (request.form.get("slug") or "").strip().lower()
            title = (request.form.get("title") or "").strip()
            desc = (request.form.get("description") or "").strip() or None
            fields = _parse_fields_post()
            stage = normalize_stage(request.form.get("default_stage"))
            company_field = (request.form.get("company_field") or "company").strip() or "company"
            is_public = request.form.get("is_public") == "1"
            is_active = request.form.get("is_active") == "1"
            try:
                validate_slug(slug)
            except ValueError as e:
                flash(str(e), "danger")
                return render_template(
                    "admin/crm_intake_form_edit.html",
                    form=None,
                    fields=fields,
                    stages=_stages_options(),
                    error_slug=True,
                )
            try:
                fid = insert_intake_form(
                    slug=slug,
                    title=title,
                    description=desc,
                    fields=fields,
                    default_stage=stage,
                    company_field=company_field,
                    is_public=is_public,
                    is_active=is_active,
                )
                flash("Intake form created.", "success")
                return redirect(url_for("crm_module.intake_form_edit", form_id=fid))
            except Exception as e:
                flash(str(e) or "Could not save.", "danger")
        return render_template(
            "admin/crm_intake_form_edit.html",
            form=None,
            fields=default_fields_json(),
            stages=_stages_options(),
        )

    @crm_bp.route("/intake-forms/<int:form_id>/edit", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def intake_form_edit(form_id: int):
        form = get_intake_form_by_id(form_id)
        if not form:
            flash("Form not found.", "danger")
            return redirect(url_for("crm_module.intake_forms_list"))
        if request.method == "POST":
            slug = (request.form.get("slug") or "").strip().lower()
            title = (request.form.get("title") or "").strip()
            desc = (request.form.get("description") or "").strip() or None
            fields = _parse_fields_post()
            stage = normalize_stage(request.form.get("default_stage"))
            company_field = (request.form.get("company_field") or "company").strip() or "company"
            is_public = request.form.get("is_public") == "1"
            is_active = request.form.get("is_active") == "1"
            try:
                validate_slug(slug)
            except ValueError as e:
                flash(str(e), "danger")
                form["fields"] = fields
                return render_template(
                    "admin/crm_intake_form_edit.html",
                    form=form,
                    fields=fields,
                    stages=_stages_options(),
                )
            try:
                update_intake_form(
                    form_id,
                    slug=slug,
                    title=title,
                    description=desc,
                    fields=fields,
                    default_stage=stage,
                    company_field=company_field,
                    is_public=is_public,
                    is_active=is_active,
                )
                flash("Saved.", "success")
                return redirect(url_for("crm_module.intake_form_edit", form_id=form_id))
            except Exception as e:
                flash(str(e) or "Could not save.", "danger")
        return render_template(
            "admin/crm_intake_form_edit.html",
            form=form,
            fields=form.get("fields") or default_fields_json(),
            stages=_stages_options(),
        )

    @crm_bp.route("/intake-forms/<int:form_id>/delete", methods=["POST"])
    @login_required
    @crm_access_required
    @crm_edit_required
    def intake_form_delete(form_id: int):
        if delete_intake_form(form_id):
            flash("Intake form deleted.", "success")
        else:
            flash("Form not found.", "warning")
        return redirect(url_for("crm_module.intake_forms_list"))

    @crm_bp.route("/intake-forms/<int:form_id>/run", methods=["GET", "POST"])
    @login_required
    @crm_access_required
    def intake_form_run(form_id: int):
        form = get_intake_form_by_id(form_id)
        if not form or not form.get("is_active"):
            flash("This form is not available.", "danger")
            return redirect(url_for("crm_module.intake_forms_list"))
        if request.method == "POST":
            sub = _submission_dict_from_request()
            sub["remote_ip"] = request.remote_addr or ""
            sub["page"] = request.path
            sub["referrer"] = request.referrer or ""
            sub["timestamp"] = ""
            sub["intake_form_title"] = form.get("title") or ""
            sub["intake_form_id"] = str(form.get("id") or "")
            res = create_lead_from_website_submission(
                sub,
                form_id=f"crm_intake:{form.get('slug')}",
                stage=str(form.get("default_stage") or "prospecting"),
                company_field=(form.get("company_field") or "company").strip() or None,
                linked_account_id=None,
            )
            if res:
                oid, _ = res
                flash("Lead saved to CRM.", "success")
                if can_edit():
                    return redirect(
                        url_for("crm_module.opportunity_edit", opp_id=int(oid))
                    )
                return redirect(url_for("crm_module.intake_forms_list"))
            flash(
                "Could not create a lead — add at least a name or email field and fill it in.",
                "danger",
            )
        public_url = None
        if form.get("is_public"):
            try:
                public_url = url_for(
                    "crm_public.public_intake_form", slug=form.get("slug")
                )
            except Exception:
                public_url = None
        return render_template(
            "admin/crm_intake_form_run.html",
            form=form,
            desk_mode=True,
            public_url=public_url,
            can_edit=can_edit(),
        )


def _stages_options():
    return [
        ("prospecting", "Prospecting"),
        ("qualification", "Qualification"),
        ("proposal", "Proposal"),
        ("negotiation", "Negotiation"),
    ]
