"""
Admin UI + JSON API for med/kit bag register (INV-MEDS / INV-KIT / CURA-DRUG-INV).
"""

from __future__ import annotations

import csv
from functools import wraps
from io import BytesIO, StringIO

from flask import (
    current_app,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from flask_login import current_user, login_required

from app.objects import has_permission
from app.organization_profile import normalize_organization_industries, tenant_matches_industry

from .med_bag_service import (
    BAG_KINDS,
    BAG_USAGE_CONTEXT_LABELS,
    BAG_USAGE_CONTEXTS,
    RETURN_STATUSES,
    RETURN_STATUS_LABELS,
    TAMPER_TAG_COLOUR_LABELS,
    TAMPER_TAG_COLOURS,
    get_med_bag_service,
)
from .objects import get_inventory_service


PERM_ACCESS = "inventory_control.access"
PERM_EDIT = "inventory_control.edit"
PERM_TRANS = "inventory_control.transactions"

_MED_BAG_ERR_FLASH = {
    "witness_required": "Witness confirmation is required for this action.",
    "second_witness_required": "A second witness is required for this action.",
    "quantity_must_be_positive": "Quantity must be greater than zero.",
    "item_not_on_bag": "That item is not on this bag.",
    "instance_not_found": "That bag instance was not found.",
    "insufficient_qty_on_bag": "Not enough quantity on the bag for this movement.",
    "would_go_negative": "That would reduce on-bag quantity below zero.",
    "invalid_event_type": "Invalid event type.",
    "internal_error": "Something went wrong. Please try again or contact support.",
    "conflict_retry": "Request conflict — retry with the same idempotency key.",
    "tamper_verification_required": "Enter the tamper seal ID and colour shown on the bag — both must match the registered seal when issuing or returning.",
    "tamper_seal_mismatch": "Tamper seal does not match the registered ID and colour. Do not use this bag until investigated — an audit entry was recorded.",
    "tamper_invalid_colour": "Choose a valid tag colour: Green, Orange, or Red.",
    "tamper_initial_required": "This bag’s seal record includes initials — enter the initials shown on the tag.",
}


def _flash_med_bag_error(code: str) -> None:
    flash(_MED_BAG_ERR_FLASH.get(code, code.replace("_", " ").title()), "danger")


def _inv_any() -> bool:
    if not getattr(current_user, "is_authenticated", False):
        return False
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return (
        has_permission(PERM_ACCESS)
        or has_permission(PERM_TRANS)
        or has_permission(PERM_EDIT)
    )


def _inv_edit() -> bool:
    if not getattr(current_user, "is_authenticated", False):
        return False
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return has_permission(PERM_EDIT)


def _inv_transact() -> bool:
    if not getattr(current_user, "is_authenticated", False):
        return False
    role = str(getattr(current_user, "role", "") or "").lower()
    if role in ("admin", "superuser", "support_break_glass"):
        return True
    return has_permission(PERM_TRANS) or has_permission(PERM_EDIT)


def _json_safe(data, status=200):
    return jsonify(data), status


def _api_serialize_med_bag_row(row: dict) -> dict:
    out = dict(row)
    for k in ("created_at", "updated_at", "tamper_seal_set_at"):
        v = out.get(k)
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
    return out


def register_med_bag_routes(bp):
    from app.plugins.inventory_control import routes as inv_routes

    svc = get_med_bag_service()
    inv = get_inventory_service()

    def med_bag_medical_industry_required(f):
        """Med / drug bag register is a medical-industry surface (PRD: organisation_profile)."""

        @wraps(f)
        def w(*a, **kw):
            ids = normalize_organization_industries(
                current_app.config.get("organization_industries")
            )
            if tenant_matches_industry(ids, "medical"):
                return f(*a, **kw)
            ep = getattr(request, "endpoint", None) or ""
            if "api_med_bags" in ep:
                return _json_safe(
                    {
                        "ok": False,
                        "error": "feature_disabled",
                        "message": (
                            "Med bag APIs require Medical in Core → Settings → "
                            "General → Industry & categories."
                        ),
                    },
                    403,
                )
            flash(
                "Med and drug bag features are shown when Medical is selected in "
                "organisation industry settings (Core → General).",
                "warning",
            )
            return redirect(url_for("inventory_control_internal.dashboard"))

        return w

    def med_bag_ui_required(f):
        @wraps(f)
        def w(*a, **kw):
            if not _inv_any():
                flash("Access denied: You do not have permission to access Inventory.", "danger")
                return redirect(url_for("routes.dashboard"))
            return f(*a, **kw)

        return w

    def med_bag_edit_required(f):
        @wraps(f)
        def w(*a, **kw):
            if not _inv_edit():
                flash("You need inventory edit permission for this action.", "danger")
                return redirect(url_for("inventory_control_internal.med_bags_templates_list"))
            return f(*a, **kw)

        return w

    @bp.route("/med-bags/templates")
    @login_required
    @med_bag_medical_industry_required
    @med_bag_ui_required
    def med_bags_templates_list():
        rows = svc.list_templates(active_only=False)
        return render_template("admin/med_bags_templates.html", templates=rows, bag_kinds=BAG_KINDS)

    @bp.route("/med-bags/templates/new", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_template_create():
        name = (request.form.get("name") or "").strip()
        code = (request.form.get("code") or "").strip()
        kind = (request.form.get("bag_kind") or "general").strip()
        desc = (request.form.get("description") or "").strip() or None
        if not name or not code:
            flash("Name and code are required.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_templates_list"))
        try:
            tid = svc.create_template(name=name, code=code, bag_kind=kind, description=desc)
            flash("Template created.", "success")
            return redirect(url_for("inventory_control_internal.med_bags_template_detail", template_id=tid))
        except ValueError as e:
            flash(str(e), "danger")
            return redirect(url_for("inventory_control_internal.med_bags_templates_list"))
        except Exception:
            flash("Could not create template.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_templates_list"))

    @bp.route("/med-bags/templates/<int:template_id>")
    @login_required
    @med_bag_medical_industry_required
    @med_bag_ui_required
    def med_bags_template_detail(template_id: int):
        t = svc.get_template(template_id)
        if not t:
            flash("Template not found.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_templates_list"))
        lines = svc.list_template_lines(template_id)
        items = []
        try:
            items = inv.list_items(limit=800, is_active=True) or []
        except Exception:
            pass
        return render_template(
            "admin/med_bags_template_detail.html",
            template=t,
            lines=lines,
            items=items,
            bag_kinds=BAG_KINDS,
            can_edit=_inv_edit(),
        )

    @bp.route("/med-bags/templates/<int:template_id>/update", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_template_update(template_id: int):
        nm = (request.form.get("name") or "").strip()
        if not nm:
            flash("Name is required.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_template_detail", template_id=template_id))
        svc.update_template(
            template_id,
            name=nm,
            bag_kind=(request.form.get("bag_kind") or "").strip() or None,
            description=(request.form.get("description") or "").strip() or None,
            is_active=request.form.get("is_active") == "1",
        )
        flash("Template updated.", "success")
        return redirect(url_for("inventory_control_internal.med_bags_template_detail", template_id=template_id))

    @bp.route("/med-bags/templates/<int:template_id>/lines/add", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_template_line_add(template_id: int):
        raw = (request.form.get("inventory_item_id") or "").strip()
        if not raw.isdigit():
            flash("Choose a catalog item.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_template_detail", template_id=template_id))
        try:
            qty = float(request.form.get("expected_qty") or 1)
        except ValueError:
            qty = 1.0
        try:
            svc.add_template_line(
                template_id,
                inventory_item_id=int(raw),
                expected_qty=qty,
                sort_order=int(request.form.get("sort_order") or 0),
            )
            flash("Line added.", "success")
        except ValueError as e:
            flash(str(e), "warning")
        return redirect(url_for("inventory_control_internal.med_bags_template_detail", template_id=template_id))

    @bp.route("/med-bags/templates/lines/<int:line_id>/delete", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_template_line_delete(line_id: int):
        tid = request.form.get("template_id")
        svc.delete_template_line(line_id)
        flash("Line removed.", "info")
        if tid and str(tid).isdigit():
            return redirect(
                url_for("inventory_control_internal.med_bags_template_detail", template_id=int(tid))
            )
        return redirect(url_for("inventory_control_internal.med_bags_templates_list"))

    @bp.route("/med-bags/instances")
    @login_required
    @med_bag_medical_industry_required
    @med_bag_ui_required
    def med_bags_instances_list():
        st = (request.args.get("status") or "").strip() or None
        top_only = (request.args.get("top_level") or "").strip() == "1"
        rows = svc.list_instances(status=st, top_level_only=top_only)
        tpls = svc.list_templates(active_only=True) if _inv_edit() else []
        return render_template(
            "admin/med_bags_instances.html",
            instances=rows,
            status_filter=st or "",
            top_level_only=top_only,
            templates=tpls,
            can_edit=_inv_edit(),
        )

    @bp.route("/med-bags/instances/new", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_instance_create():
        raw = (request.form.get("template_id") or "").strip()
        if not raw.isdigit():
            flash("Choose a template.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_instances_list"))
        tag = (request.form.get("public_asset_number") or "").strip() or None
        p_raw = (request.form.get("parent_instance_id") or "").strip()
        parent_id = int(p_raw) if p_raw.isdigit() else None
        try:
            iid = svc.create_instance_from_template(
                int(raw),
                public_asset_number=tag,
                parent_instance_id=parent_id,
            )
            if parent_id:
                flash(
                    f"Nested bag #{iid} created inside bag #{parent_id} (e.g. pod).",
                    "success",
                )
            else:
                flash("Bag instance created.", "success")
            return redirect(url_for("inventory_control_internal.med_bags_instance_detail", instance_id=iid))
        except Exception as e:
            flash(str(e), "danger")
            if parent_id:
                return redirect(
                    url_for(
                        "inventory_control_internal.med_bags_instance_detail",
                        instance_id=parent_id,
                    )
                )
            return redirect(url_for("inventory_control_internal.med_bags_instances_list"))

    @bp.route("/med-bags/instances/<int:instance_id>")
    @login_required
    @med_bag_medical_industry_required
    @med_bag_ui_required
    def med_bags_instance_detail(instance_id: int):
        row = svc.get_instance(instance_id)
        if not row:
            flash("Instance not found.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_instances_list"))
        lines = svc.list_instance_lines(instance_id)
        ledger = svc.list_ledger(instance_id, limit=80)
        lot_q = (request.args.get("lot") or "").strip()
        lot_hits = svc.search_lot(lot_q) if lot_q else []
        child_instances = svc.list_child_instances(instance_id)
        nested_tpls = svc.list_templates(active_only=True) if _inv_edit() else []
        seal_events = svc.list_seal_events(instance_id, limit=60)
        return render_template(
            "admin/med_bags_instance_detail.html",
            instance=row,
            child_instances=child_instances,
            lines=lines,
            ledger=ledger,
            seal_events=seal_events,
            lot_query=lot_q,
            lot_hits=lot_hits,
            return_statuses=RETURN_STATUSES,
            return_status_labels=RETURN_STATUS_LABELS,
            can_edit=_inv_edit(),
            can_transact=_inv_transact(),
            templates_for_nested=nested_tpls,
            tamper_tag_colours=TAMPER_TAG_COLOURS,
            tamper_tag_colour_labels=TAMPER_TAG_COLOUR_LABELS,
            bag_usage_context_labels=BAG_USAGE_CONTEXT_LABELS,
            bag_usage_contexts=BAG_USAGE_CONTEXTS,
        )

    @bp.route("/med-bags/instances/<int:instance_id>/usage-context", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_instance_usage_context(instance_id: int):
        raw = (request.form.get("usage_context") or "").strip().lower()
        if raw not in BAG_USAGE_CONTEXTS:
            raw = "patient_care"
        try:
            svc.set_instance_usage_context(instance_id, raw)
            flash("Bag use context updated.", "success")
        except ValueError as e:
            flash(str(e), "danger")
        return redirect(
            url_for("inventory_control_internal.med_bags_instance_detail", instance_id=instance_id)
        )

    @bp.route("/med-bags/instances/<int:instance_id>/loadlist.pdf")
    @login_required
    @med_bag_medical_industry_required
    @med_bag_ui_required
    def med_bags_instance_loadlist_pdf(instance_id: int):
        """Recursive kit loadlist: this bag, nested modules, lots and expiries (print-ready PDF)."""
        data = svc.build_hierarchical_loadlist(instance_id)
        if not data:
            flash("Instance not found.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_instances_list"))
        from .med_bag_loadlist_pdf import render_med_bag_loadlist_pdf

        pdf_bytes = render_med_bag_loadlist_pdf(data)
        label = data.get("root") or {}
        asset = str(label.get("public_asset_number") or instance_id)
        for ch in ('/', "\\", ":", "*", "?", '"', "<", ">", "|"):
            asset = asset.replace(ch, "-")
        fname = f"med-bag-loadlist-{asset[:120] or instance_id}.pdf"
        return send_file(
            BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=fname,
        )

    def _loadlist_check_line_ids_from_form():
        lids = set()
        for k in request.form:
            if k.startswith("status_"):
                suf = k[7:]
                if suf.isdigit():
                    lids.add(int(suf))
            elif k.startswith("lot_") and not k.startswith("orig_lot_"):
                suf = k[4:]
                if suf.isdigit():
                    lids.add(int(suf))
            elif k.startswith("exp_") and not k.startswith("orig_exp_"):
                suf = k[4:]
                if suf.isdigit():
                    lids.add(int(suf))
        return sorted(lids)

    def _rows_from_loadlist_check_form():
        rows = []
        for lid in _loadlist_check_line_ids_from_form():
            row = {"line_id": lid}
            sk = f"status_{lid}"
            if sk in request.form:
                row["return_status"] = request.form.get(sk, "")
            lot = (request.form.get(f"lot_{lid}") or "").strip()
            exp = (request.form.get(f"exp_{lid}") or "").strip()
            orig_lot = (request.form.get(f"orig_lot_{lid}") or "").strip()
            orig_exp = (request.form.get(f"orig_exp_{lid}") or "").strip()
            row["lot_number"] = lot
            row["expiry_date"] = exp
            row["trace_changed"] = (lot != orig_lot) or (exp != orig_exp)
            rows.append(row)
        return rows

    def _movement_line_ids_from_form():
        lids = set()
        for k in request.form:
            if k.startswith("mov_qty_"):
                suf = k[8:]
                if suf.isdigit():
                    lids.add(int(suf))
        return sorted(lids)

    def _parse_movement_rows_from_form():
        """Parse inline restock/disposal rows; optional default witnesses apply when per-row blank."""
        dw1 = (request.form.get("default_witness_1") or "").strip() or None
        dw2 = (request.form.get("default_witness_2") or "").strip() or None
        rows = []
        parse_warnings: list[str] = []
        for lid in _movement_line_ids_from_form():
            raw_qty = (request.form.get(f"mov_qty_{lid}") or "").strip()
            raw_evt = (request.form.get(f"mov_evt_{lid}") or "").strip()
            if not raw_qty and not raw_evt:
                continue
            if not raw_qty or not raw_evt:
                parse_warnings.append(
                    f"Line {lid}: enter both quantity and movement type, or leave both blank."
                )
                continue
            try:
                qty = float(raw_qty)
            except ValueError:
                parse_warnings.append(f"Line {lid}: quantity must be a number.")
                continue
            inst_raw = (request.form.get(f"mov_inst_{lid}") or "").strip()
            item_raw = (request.form.get(f"mov_item_{lid}") or "").strip()
            if not inst_raw.isdigit() or not item_raw.isdigit():
                parse_warnings.append(f"Line {lid}: missing bag or item — refresh the page.")
                continue
            w1 = (request.form.get(f"mov_w1_{lid}") or "").strip() or None
            w2 = (request.form.get(f"mov_w2_{lid}") or "").strip() or None
            if not w1 and dw1:
                w1 = dw1
            if not w2 and dw2:
                w2 = dw2
            rows.append(
                {
                    "line_id": lid,
                    "instance_id": int(inst_raw),
                    "inventory_item_id": int(item_raw),
                    "quantity": qty,
                    "event_type": raw_evt,
                    "witness_user_id": w1,
                    "witness_user_id_2": w2,
                    "notes": (request.form.get(f"mov_notes_{lid}") or "").strip() or None,
                }
            )
        return rows, parse_warnings

    @bp.route(
        "/med-bags/instances/<int:instance_id>/loadlist-check",
        methods=["GET", "POST"],
    )
    @login_required
    @med_bag_medical_industry_required
    @med_bag_ui_required
    def med_bags_instance_loadlist_check(instance_id: int):
        """Digital kit loadlist: parent + nested bags, checklist, lot/expiry, date hints."""
        loadlist = svc.build_hierarchical_loadlist(instance_id)
        if not loadlist:
            flash("Instance not found.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_instances_list"))
        if request.method == "POST":
            perf = str(
                getattr(current_user, "username", "") or getattr(current_user, "id", "") or ""
            )
            mov_rows: list = []
            mov_parse_warnings: list[str] = []
            mov_n = 0
            if _inv_transact():
                mov_rows, mov_parse_warnings = _parse_movement_rows_from_form()
                for msg in mov_parse_warnings[:8]:
                    flash(msg, "warning")
                if mov_rows:
                    mout = svc.apply_loadlist_inline_movements(
                        instance_id,
                        mov_rows,
                        performed_by=perf,
                    )
                    for msg in (mout.get("errors") or [])[:8]:
                        flash(msg, "danger")
                    mov_n = int(mout.get("movements_recorded") or 0)
                    if mov_n:
                        flash(f"{mov_n} stock movement(s) recorded.", "success")

            rows = _rows_from_loadlist_check_form()
            if not rows and not mov_rows and not mov_parse_warnings:
                flash(
                    "Nothing to save — update the checklist, lot/expiry, or fill a stock movement row.",
                    "warning",
                )
                return redirect(
                    url_for(
                        "inventory_control_internal.med_bags_instance_loadlist_check",
                        instance_id=instance_id,
                    )
                )

            if rows:
                out = svc.apply_loadlist_check_updates(
                    instance_id,
                    rows,
                    can_edit_trace=_inv_edit(),
                )
                if out.get("errors"):
                    for msg in out["errors"][:8]:
                        flash(msg, "danger")
                if out.get("ok"):
                    parts = []
                    if out.get("status_updates"):
                        parts.append(f"{out['status_updates']} checklist update(s)")
                    if out.get("trace_updates"):
                        parts.append(f"{out['trace_updates']} lot/expiry update(s)")
                    if parts:
                        flash("Saved: " + "; ".join(parts) + ".", "success")
                    elif not mov_rows:
                        flash("No checklist changes.", "info")
                elif not out.get("errors"):
                    flash("No checklist changes.", "info")
            return redirect(
                url_for(
                    "inventory_control_internal.med_bags_instance_loadlist_check",
                    instance_id=instance_id,
                )
            )
        return render_template(
            "admin/med_bags_loadlist_check.html",
            loadlist=loadlist,
            return_statuses=RETURN_STATUSES,
            return_status_labels=RETURN_STATUS_LABELS,
            can_edit=_inv_edit(),
            can_transact=_inv_transact(),
        )

    @bp.route("/med-bags/instances/<int:instance_id>/loadlist.csv")
    @login_required
    @med_bag_medical_industry_required
    @med_bag_ui_required
    def med_bags_instance_loadlist_csv(instance_id: int):
        """Flattened CSV for spreadsheet date checks (same tree as PDF)."""
        data = svc.build_hierarchical_loadlist(instance_id)
        if not data:
            flash("Instance not found.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_instances_list"))
        buf = StringIO()
        w = csv.writer(buf)
        w.writerow(
            [
                "bag_instance_id",
                "section_depth",
                "section_title",
                "line_id",
                "sku",
                "item_name",
                "quantity_expected",
                "quantity_on_bag",
                "lot_number",
                "expiry_date",
                "expiry_hint",
                "checklist_status",
            ]
        )
        for sec in data.get("sections") or []:
            sid = sec.get("instance_id")
            depth = sec.get("depth", 0)
            title = sec.get("title") or ""
            for ln in sec.get("lines") or []:
                w.writerow(
                    [
                        sid,
                        depth,
                        title,
                        ln.get("id"),
                        ln.get("sku"),
                        ln.get("item_name"),
                        ln.get("quantity_expected"),
                        ln.get("quantity_on_bag"),
                        ln.get("lot_number") or "",
                        ln.get("expiry_date") or "",
                        ln.get("expiry_hint") or "",
                        ln.get("return_status") or "",
                    ]
                )
        raw = buf.getvalue().encode("utf-8-sig")
        label = data.get("root") or {}
        asset = str(label.get("public_asset_number") or instance_id)
        for ch in ('/', "\\", ":", "*", "?", '"', "<", ">", "|"):
            asset = asset.replace(ch, "-")
        fname = f"med-bag-loadlist-{asset[:120] or instance_id}.csv"
        return send_file(
            BytesIO(raw),
            mimetype="text/csv; charset=utf-8",
            as_attachment=True,
            download_name=fname,
        )

    @bp.route("/med-bags/instances/<int:instance_id>/status", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_instance_status(instance_id: int):
        st = (request.form.get("status") or "").strip()
        at = (request.form.get("assignee_type") or "").strip() or None
        aid = (request.form.get("assignee_id") or "").strip() or None
        perf = str(getattr(current_user, "username", "") or getattr(current_user, "id", ""))
        tv_id = (request.form.get("tamper_verify_id") or "").strip() or None
        tv_col = (request.form.get("tamper_verify_colour") or "").strip() or None
        tv_ini = (request.form.get("tamper_verify_initial") or "").strip() or None
        try:
            out = svc.apply_instance_status_change(
                instance_id,
                st,
                assignee_type=at,
                assignee_id=aid,
                tamper_verify_id=tv_id,
                tamper_verify_colour=tv_col,
                tamper_verify_initial=tv_ini,
                performed_by=perf,
            )
            if out.get("ok"):
                flash("Status updated.", "success")
            else:
                err = str(out.get("error", "error"))
                _flash_med_bag_error(err)
        except ValueError as e:
            flash(str(e), "danger")
        except Exception as e:
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.med_bags_instance_detail", instance_id=instance_id))

    @bp.route("/med-bags/instances/<int:instance_id>/tamper-seal", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_instance_tamper_seal(instance_id: int):
        perf = str(getattr(current_user, "username", "") or getattr(current_user, "id", ""))
        try:
            svc.register_tamper_seal(
                instance_id,
                seal_id=(request.form.get("tamper_seal_id") or "").strip(),
                colour=(request.form.get("tamper_seal_colour") or "").strip(),
                performed_by=perf,
                initial=(request.form.get("tamper_seal_initial") or "").strip() or None,
                notes=(request.form.get("tamper_seal_notes") or "").strip() or None,
            )
            flash("Tamper seal registered on this bag.", "success")
        except ValueError as e:
            flash(str(e), "danger")
        except Exception as e:
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.med_bags_instance_detail", instance_id=instance_id))

    @bp.route("/med-bags/instances/<int:instance_id>/sign-out", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_instance_sign_out(instance_id: int):
        """Drug bag sign-out: issue to assignee with tamper check."""
        perf = str(getattr(current_user, "username", "") or getattr(current_user, "id", ""))
        at = (request.form.get("assignee_type") or "").strip() or None
        aid = (request.form.get("assignee_id") or "").strip() or None
        tv_id = (request.form.get("tamper_verify_id") or "").strip() or None
        tv_col = (request.form.get("tamper_verify_colour") or "").strip() or None
        tv_ini = (request.form.get("tamper_verify_initial") or "").strip() or None
        try:
            out = svc.apply_instance_status_change(
                instance_id,
                "issued",
                assignee_type=at,
                assignee_id=aid,
                tamper_verify_id=tv_id,
                tamper_verify_colour=tv_col,
                tamper_verify_initial=tv_ini,
                performed_by=perf,
            )
            if out.get("ok"):
                note = f"Sign-out — assignee {at or '—'} {aid or ''}".strip()
                svc.log_bag_custody_event(
                    instance_id,
                    "drug_bag_sign_out",
                    performed_by=perf,
                    notes=note[:512],
                )
                flash("Bag signed out (issued).", "success")
            else:
                _flash_med_bag_error(str(out.get("error", "error")))
        except ValueError as e:
            flash(str(e), "danger")
        except Exception as e:
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.med_bags_instance_detail", instance_id=instance_id))

    @bp.route("/med-bags/instances/<int:instance_id>/sign-in", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_instance_sign_in(instance_id: int):
        """Drug bag sign-in: return to store with tamper check; clears assignee."""
        perf = str(getattr(current_user, "username", "") or getattr(current_user, "id", ""))
        tv_id = (request.form.get("tamper_verify_id") or "").strip() or None
        tv_col = (request.form.get("tamper_verify_colour") or "").strip() or None
        tv_ini = (request.form.get("tamper_verify_initial") or "").strip() or None
        try:
            out = svc.apply_instance_status_change(
                instance_id,
                "returned",
                assignee_type=None,
                assignee_id=None,
                tamper_verify_id=tv_id,
                tamper_verify_colour=tv_col,
                tamper_verify_initial=tv_ini,
                performed_by=perf,
            )
            if out.get("ok"):
                svc.log_bag_custody_event(
                    instance_id,
                    "drug_bag_sign_in",
                    performed_by=perf,
                    notes="Sign-in — returned to store",
                )
                flash("Bag signed in (returned to store).", "success")
            else:
                _flash_med_bag_error(str(out.get("error", "error")))
        except ValueError as e:
            flash(str(e), "danger")
        except Exception as e:
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.med_bags_instance_detail", instance_id=instance_id))

    @bp.route("/med-bags/instances/lines/<int:line_id>/trace", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_instance_line_trace(line_id: int):
        iid = request.form.get("instance_id")
        if not iid or not str(iid).isdigit():
            flash("Invalid bag reference.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_instances_list"))
        iid_int = int(iid)
        if not svc.instance_line_on_instance(line_id, iid_int):
            flash("That line does not belong to this bag.", "danger")
            return redirect(
                url_for("inventory_control_internal.med_bags_instance_detail", instance_id=iid_int)
            )
        try:
            svc.update_instance_line_trace(
                line_id,
                lot_number=request.form.get("lot_number"),
                expiry_date=request.form.get("expiry_date"),
            )
            flash("Lot and expiry updated.", "success")
        except ValueError as e:
            flash(str(e), "danger")
        return redirect(
            url_for("inventory_control_internal.med_bags_instance_detail", instance_id=iid_int)
        )

    @bp.route("/med-bags/instances/lines/return-status", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_ui_required
    def med_bags_instance_line_return_post():
        try:
            lid = int(request.form.get("line_id") or 0)
            iid = int(request.form.get("instance_id") or 0)
        except (TypeError, ValueError):
            flash("Invalid request.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_instances_list"))
        if lid <= 0 or iid <= 0:
            flash("Invalid request.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_instances_list"))
        if not svc.instance_line_on_instance(lid, iid):
            flash("That line does not belong to this bag.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_instance_detail", instance_id=iid))
        st = (request.form.get("return_status") or "").strip()
        try:
            svc.set_instance_line_return_status(lid, st)
            flash("Return checklist updated.", "success")
        except ValueError as e:
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.med_bags_instance_detail", instance_id=iid))

    @bp.route("/med-bags/instances/restock", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_ui_required
    def med_bags_restock_post():
        if not _inv_transact():
            flash("You need transaction permission to post movements.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_instances_list"))
        try:
            iid = int(request.form.get("instance_id") or 0)
            item_id = int(request.form.get("inventory_item_id") or 0)
            qty = float(request.form.get("quantity") or 0)
        except (TypeError, ValueError):
            flash("Enter a valid item and quantity.", "danger")
            raw_i = (request.form.get("instance_id") or "").strip()
            if raw_i.isdigit():
                return redirect(
                    url_for(
                        "inventory_control_internal.med_bags_instance_detail",
                        instance_id=int(raw_i),
                    )
                )
            return redirect(url_for("inventory_control_internal.med_bags_instances_list"))
        et = (request.form.get("event_type") or "restock_bag").strip()
        w1 = (request.form.get("witness_user_id") or "").strip() or None
        w2 = (request.form.get("witness_user_id_2") or "").strip() or None
        perf = str(getattr(current_user, "username", "") or getattr(current_user, "id", ""))
        out = svc.record_restock_event(
            instance_id=iid,
            inventory_item_id=item_id,
            quantity=qty,
            event_type=et,
            witness_user_id=w1,
            witness_user_id_2=w2,
            performed_by=perf,
            notes=(request.form.get("notes") or "").strip() or None,
        )
        if out.get("ok"):
            flash("Movement recorded.", "success")
        else:
            _flash_med_bag_error(str(out.get("error", "internal_error")))
        return redirect(url_for("inventory_control_internal.med_bags_instance_detail", instance_id=iid))

    @bp.route("/med-bags/instances/<int:instance_id>/parent", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_instance_parent_set(instance_id: int):
        if request.form.get("detach") == "1":
            new_parent = None
        else:
            raw = (request.form.get("parent_instance_id") or "").strip()
            if not raw.isdigit():
                flash("Enter a valid parent bag id, or use Detach.", "warning")
                return redirect(
                    url_for(
                        "inventory_control_internal.med_bags_instance_detail",
                        instance_id=instance_id,
                    )
                )
            new_parent = int(raw)
        try:
            svc.set_instance_parent(instance_id, new_parent)
            flash(
                "Parent bag updated."
                if new_parent
                else "This bag is no longer nested under a parent.",
                "success",
            )
        except ValueError as e:
            flash(str(e), "danger")
        return redirect(
            url_for("inventory_control_internal.med_bags_instance_detail", instance_id=instance_id)
        )

    @bp.route("/med-bags/witness-rules")
    @login_required
    @med_bag_medical_industry_required
    @med_bag_ui_required
    def med_bags_witness_rules():
        if not _inv_edit():
            flash("Edit permission required.", "danger")
            return redirect(url_for("inventory_control_internal.med_bags_templates_list"))
        rules = svc.list_witness_rules()
        return render_template("admin/med_bags_witness_rules.html", rules=rules)

    @bp.route("/med-bags/witness-rules/save", methods=["POST"])
    @login_required
    @med_bag_medical_industry_required
    @med_bag_edit_required
    def med_bags_witness_rules_save():
        for key in (
            "epcr_consumption",
            "restock_hq",
            "restock_bag",
            "disposal",
        ):
            wc = request.form.get(f"witness_count_{key}")
            en = request.form.get(f"enabled_{key}") == "1"
            try:
                wn = int(wc or 0)
            except ValueError:
                wn = 0
            svc.update_witness_rule(key, witness_count=wn, enabled=en)
        flash("Witness rules saved.", "success")
        return redirect(url_for("inventory_control_internal.med_bags_witness_rules"))

    # --- JSON: CURA / integration (CURA-DRUG-INV-001) ---
    @bp.route("/api/med-bags/consumption", methods=["POST"])
    @med_bag_medical_industry_required
    @inv_routes.api_inventory_transact_required
    def api_med_bags_consumption():
        data = request.get_json(silent=True)
        if data is None:
            return _json_safe({"ok": False, "error": "invalid_json"}, 400)
        if not isinstance(data, dict):
            return _json_safe({"ok": False, "error": "invalid_body"}, 400)
        try:
            instance_id = int(data.get("instance_id"))
            inventory_item_id = int(data.get("inventory_item_id"))
        except (TypeError, ValueError):
            return _json_safe({"ok": False, "error": "invalid_ids"}, 400)
        if instance_id <= 0 or inventory_item_id <= 0:
            return _json_safe({"ok": False, "error": "invalid_ids"}, 400)
        try:
            quantity = float(data.get("quantity"))
        except (TypeError, ValueError):
            return _json_safe({"ok": False, "error": "invalid_quantity"}, 400)

        perf = None
        if getattr(current_user, "is_authenticated", False):
            perf = str(getattr(current_user, "username", "") or getattr(current_user, "id", ""))
        if perf is None and getattr(g, "token_user", None):
            perf = str(g.token_user.get("username") or g.token_user.get("id") or "")
        out = svc.record_epcr_consumption(
            instance_id=instance_id,
            inventory_item_id=inventory_item_id,
            quantity=quantity,
            epcr_external_ref=data.get("epcr_external_ref"),
            epcr_episode_ref=data.get("epcr_episode_ref"),
            idempotency_key=data.get("idempotency_key"),
            witness_user_id=data.get("witness_user_id"),
            witness_user_id_2=data.get("witness_user_id_2"),
            performed_by=data.get("performed_by") or perf,
            lot_number=data.get("lot_number"),
            notes=data.get("notes"),
        )
        if out.get("ok"):
            return _json_safe(out, 200)
        err = str(out.get("error", "error"))
        code = 400
        if err == "insufficient_qty_on_bag":
            code = 409
        elif err == "conflict_retry":
            code = 409
        elif err in ("witness_required", "second_witness_required"):
            code = 422
        elif err == "instance_not_found":
            code = 404
        elif err == "internal_error":
            code = 500
        return _json_safe(out, code)

    @bp.route("/api/med-bags/instances/<int:instance_id>", methods=["GET"])
    @med_bag_medical_industry_required
    @inv_routes.api_inventory_access_required
    def api_med_bags_instance_get(instance_id: int):
        row = svc.get_instance(instance_id)
        if not row:
            return _json_safe({"error": "not_found"}, 404)
        lines = svc.list_instance_lines(instance_id)
        children = svc.list_child_instances(instance_id)
        inst_out = _api_serialize_med_bag_row(row)
        kids_out = [_api_serialize_med_bag_row(dict(c)) for c in children]
        return _json_safe(
            {
                "instance": inst_out,
                "lines": lines,
                "child_instances": kids_out,
            }
        )
