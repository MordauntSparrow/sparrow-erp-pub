# Serial equipment UI (merged from former asset_management plugin)
from __future__ import annotations

import logging
from datetime import date
from functools import wraps

from flask import current_app, flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app.objects import has_permission

from app.plugins.fleet_management.objects import get_fleet_service
from app.plugins.inventory_control.objects import (
    add_calendar_months,
    annotate_equipment_consumable_rows,
    get_inventory_service,
)

from .asset_service import get_asset_service
from .equipment_readiness import build_equipment_asset_readiness

logger = logging.getLogger("inventory_control.equipment_assets")


def _movement_recorded_by_label(row: dict) -> str:
    uid = row.get("performed_by_user_id")
    if uid is None or uid == "":
        return "—"
    return str(uid)


PERM_ACCESS = "asset_management.access"
PERM_EDIT = "asset_management.edit"
PERM_ASSIGN = "asset_management.assign"
PERM_RECORDS = "asset_management.records"


def register_equipment_asset_routes(bp):
    """Attach serial-equipment pages to the inventory_control_internal blueprint."""



    def _uid() -> str | None:
        if not getattr(current_user, "is_authenticated", False):
            return None
        return str(getattr(current_user, "id", "") or "") or None


    def _can_access() -> bool:
        if not getattr(current_user, "is_authenticated", False):
            return False
        role = str(getattr(current_user, "role", "") or "").lower()
        if role in ("admin", "superuser", "support_break_glass"):
            return True
        return has_permission("inventory_control.access") or has_permission(PERM_ACCESS)


    def _can_edit() -> bool:
        role = str(getattr(current_user, "role", "") or "").lower()
        if role in ("admin", "superuser", "support_break_glass"):
            return True
        return has_permission(PERM_EDIT)


    def _can_assign() -> bool:
        """Sign out / return to stock (inventory moves on this asset)."""
        role = str(getattr(current_user, "role", "") or "").lower()
        if role in ("admin", "superuser", "support_break_glass"):
            return True
        return has_permission(PERM_EDIT) or has_permission(PERM_ASSIGN)


    def _can_records() -> bool:
        """Servicing plan, faults/issues, maintenance logs (no assignment)."""
        role = str(getattr(current_user, "role", "") or "").lower()
        if role in ("admin", "superuser", "support_break_glass"):
            return True
        return has_permission(PERM_EDIT) or has_permission(PERM_RECORDS)


    def _can_consumable_edit() -> bool:
        """Lot/expiry lines on serial kit: sign-out staff or maintenance records role."""
        return _can_assign() or _can_records()


    def asset_access_required(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not _can_access():
                flash("You do not have access to Asset Management.", "danger")
                return redirect(url_for("routes.dashboard"))
            return f(*args, **kwargs)

        return wrapper


    def asset_edit_required(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not _can_edit():
                flash("You do not have permission to edit assets.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_servicing_dashboard"))
            return f(*args, **kwargs)

        return wrapper


    def asset_assign_required(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not _can_assign():
                flash("You do not have permission to sign assets in or out.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_servicing_dashboard"))
            return f(*args, **kwargs)

        return wrapper


    def _redirect_after_return_to_stock(asset_id: int):
        """Where to send the user after return-to-stock (form field redirect_to)."""
        target = (request.form.get("redirect_to") or "").strip().lower()
        if target == "assignments":
            return redirect(url_for("inventory_control_internal.equipment_assignments_page"))
        if target in ("registry", "equipment_desk"):
            return redirect(url_for("inventory_control_internal.equipment_desk_page"))
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    def _redirect_equipment_desk(
        *,
        q: str | None = None,
        status: str | None = None,
        assigned: bool = False,
    ):
        """Canonical catalogue of serial equipment: Inventory → Equipment desk."""
        kw: dict = {}
        qs = (q or "").strip()
        st = (status or "").strip()
        if qs:
            kw["q"] = qs
        if st:
            kw["status"] = st
        if assigned:
            kw["assigned"] = 1
        return redirect(url_for("inventory_control_internal.equipment_desk_page", **kw))


    def asset_records_required(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not _can_records():
                flash("You do not have permission to change asset maintenance records.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_servicing_dashboard"))
            return f(*args, **kwargs)

        return wrapper


    def asset_consumables_edit_required(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not _can_consumable_edit():
                flash("You do not have permission to edit consumables on this asset.", "danger")
                aid = (request.view_args or {}).get("asset_id")
                if aid:
                    return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=int(aid)))
                return redirect(url_for("inventory_control_internal.equipment_servicing_dashboard"))
            return f(*args, **kwargs)

        return wrapper


    @bp.route("/equipment/servicing")
    @bp.route("/equipment/servicing/index")
    @login_required
    @asset_access_required
    def equipment_servicing_dashboard():
        svc = get_asset_service()
        metrics = svc.dashboard_metrics()
        due = svc.list_maintenance_due(within_days=30)
        kit = {"contractor_kit_pending_count": 0}
        try:
            kit = get_inventory_service().get_contractor_kit_pending_snapshot()
        except Exception:
            logger.exception("asset dashboard kit snapshot")
        return render_template(
            "admin/asset_dashboard.html",
            metrics=metrics,
            maintenance_due=due,
            can_edit=_can_edit(),
            can_assign=_can_assign(),
            contractor_kit_pending_count=kit.get("contractor_kit_pending_count") or 0,
        )


    @bp.route("/equipment/assignments")
    @login_required
    @asset_access_required
    def equipment_assignments_page():
        """Equipment currently signed out — vehicle, user, or contractor."""
        svc = get_asset_service()
        rows = svc.list_equipment_desk_rows(assigned_only=True, limit=500)
        locations = []
        if _can_assign():
            try:
                locations = get_inventory_service().list_locations() or []
            except Exception:
                logger.exception("assignments list_locations")
        return render_template(
            "admin/asset_assignments.html",
            rows=rows,
            locations=locations,
            can_assign=_can_assign(),
        )


    @bp.route("/equipment/assets-catalog")
    @login_required
    @asset_access_required
    def equipment_assets_catalog_redirect():
        """Redirect: full equipment catalogue lives under Inventory → Equipment desk."""
        q = (request.args.get("q") or "").strip() or None
        st = (request.args.get("status") or "").strip() or None
        assigned = (request.args.get("assigned") or "").strip() == "1"
        return _redirect_equipment_desk(q=q, status=st, assigned=assigned)


    @bp.route("/equipment/assets/new", methods=["GET", "POST"])
    @login_required
    @asset_access_required
    @asset_edit_required
    def equipment_asset_new():
        """Wizard: pick or create equipment model, then register one serial unit."""
        inv = get_inventory_service()
        if request.method == "POST":
            mode = (request.form.get("model_mode") or "existing").strip()
            serial = (request.form.get("serial_number") or "").strip()
            item_id = int(request.form.get("item_id") or 0)
            new_sku = (request.form.get("new_model_sku") or "").strip()
            new_name = (request.form.get("new_model_name") or "").strip()
            new_unit = (request.form.get("new_model_unit") or "").strip() or "ea"

            if mode == "new" and new_sku and new_name:
                try:
                    item_id = inv.create_item(
                        {
                            "sku": new_sku[:120],
                            "name": new_name[:255],
                            "unit": new_unit[:32],
                            "is_equipment": True,
                            "requires_serial": True,
                            "is_active": True,
                        }
                    )
                except Exception as e:
                    logger.exception("asset_new create_item")
                    flash(str(e), "danger")
                    return redirect(url_for("inventory_control_internal.equipment_asset_new"))

            if not item_id or not serial:
                flash("Product line and serial number are required.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_asset_new"))
            item = inv.get_item(item_id)
            if not item or not int(item.get("is_equipment") or 0):
                flash("That line is not set up as equipment.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_asset_new"))

            make = (request.form.get("make") or "").strip() or None
            model = (request.form.get("model") or "").strip() or None
            purchase_date = (request.form.get("purchase_date") or "").strip() or None
            warranty_expiry = (request.form.get("warranty_expiry") or "").strip() or None
            warranty_start_date = (request.form.get("warranty_start_date") or "").strip() or None
            warranty_start_basis = (request.form.get("warranty_start_basis") or "").strip() or None
            warranty_months_raw = (request.form.get("warranty_months") or "").strip()
            try:
                warranty_months = int(warranty_months_raw) if warranty_months_raw else None
            except ValueError:
                warranty_months = None
            svc_raw = (request.form.get("service_interval_days") or "").strip()
            try:
                service_interval_days = int(svc_raw) if svc_raw else None
            except ValueError:
                service_interval_days = None
            next_service_due_date = (request.form.get("next_service_due_date") or "").strip() or None
            condition = (request.form.get("condition") or "").strip() or None
            public_asset_code = (request.form.get("public_asset_code") or "").strip() or None

            try:
                aid = inv.create_equipment_asset(
                    item_id=item_id,
                    serial_number=serial,
                    make=make,
                    model=model,
                    purchase_date=purchase_date,
                    warranty_expiry=warranty_expiry,
                    warranty_start_basis=warranty_start_basis or None,
                    warranty_start_date=warranty_start_date,
                    warranty_months=warranty_months,
                    service_interval_days=service_interval_days,
                    next_service_due_date=next_service_due_date,
                    condition=condition,
                    public_asset_code=public_asset_code,
                )
                flash("Equipment saved.", "success")
                return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=aid))
            except Exception as e:
                logger.exception("asset_new create")
                flash(str(e), "danger")
                return redirect(url_for("inventory_control_internal.equipment_asset_new"))
        try:
            raw = inv.list_items(limit=800, is_active=True, is_equipment=True)
        except Exception:
            logger.exception("asset_new list_items")
            raw = []
        equipment_items = [it for it in raw if int(it.get("is_equipment") or 0)]
        equipment_items.sort(key=lambda x: (str(x.get("name") or "")).lower())
        return render_template("admin/asset_new.html", equipment_items=equipment_items)


    @bp.route("/equipment/assets/<int:asset_id>")
    @login_required
    @asset_access_required
    def equipment_asset_detail(asset_id: int):
        svc = get_asset_service()
        row = svc.get_asset_detail(asset_id)
        if not row:
            flash("Asset not found.", "danger")
            return _redirect_equipment_desk()
        maint = svc.list_maintenance_events(asset_id)
        movement = svc.list_equipment_movement_log(asset_id, limit=200)
        current_holder = svc.get_current_holder(asset_id)
        on_vehicle = svc.get_current_vehicle_assignment(asset_id)
        equipment_issues = svc.list_equipment_issues(
            asset_id, include_closed=True, limit=80
        )
        last_service_date = svc.get_last_maintenance_date(asset_id)
        locations = []
        vehicles = []
        inv = get_inventory_service()
        consumable_lines: list = []
        consumable_items_select: list = []
        try:
            raw_cons = inv.list_equipment_asset_consumables(asset_id)
            consumable_lines = annotate_equipment_consumable_rows(
                raw_cons, today=date.today(), near_days=30
            )
            for c in consumable_lines:
                ed = c.get("expiry_date")
                if ed is None:
                    c["expiry_date_iso"] = ""
                elif hasattr(ed, "strftime"):
                    c["expiry_date_iso"] = ed.strftime("%Y-%m-%d")
                else:
                    c["expiry_date_iso"] = str(ed)[:10]
        except Exception:
            logger.exception("asset_detail consumables")
        if _can_consumable_edit():
            try:
                consumable_items_select = inv.list_items(limit=400, is_active=True, is_equipment=False)
            except Exception:
                logger.exception("asset_detail consumable items")
        portal_handoffs_pending: list = []
        assignee_site_locations: list = []
        if _can_assign() or _can_edit():
            try:
                locations = inv.list_locations()
            except Exception:
                locations = []
            try:
                vehicles = get_fleet_service().list_vehicles(limit=500)
            except Exception:
                logger.exception("asset_detail list_vehicles")
                vehicles = []
            assignee_site_locations = sorted(
                [
                    loc
                    for loc in (locations or [])
                    if str(loc.get("type") or "") in ("holding", "virtual")
                ],
                key=lambda L: (
                    0 if str(L.get("type") or "") == "holding" else 1,
                    str(L.get("code") or L.get("name") or "").lower(),
                ),
            )
        if _can_assign():
            try:
                portal_handoffs_pending = inv.list_pending_handoffs_for_asset(int(asset_id))
                for h in portal_handoffs_pending:
                    if h.get("vehicle_id"):
                        try:
                            fv = get_fleet_service().get_vehicle(int(h["vehicle_id"]))
                            h["vehicle_registration"] = (
                                (fv or {}).get("registration")
                                or (fv or {}).get("internal_code")
                                or ""
                            )
                        except Exception:
                            h["vehicle_registration"] = ""
            except Exception:
                logger.exception("asset_detail portal handoffs")
                portal_handoffs_pending = []
        for t in movement:
            t["recorded_by"] = _movement_recorded_by_label(t)
        equipment_readiness = build_equipment_asset_readiness(
            asset=row, consumable_lines=consumable_lines
        )
        return render_template(
            "admin/asset_detail.html",
            asset=row,
            maintenance_events=maint,
            movement_log=movement,
            current_holder=current_holder,
            equipment_readiness=equipment_readiness,
            current_vehicle=on_vehicle,
            equipment_issues=equipment_issues,
            last_service_date=last_service_date,
            inventory_locations=locations,
            assignee_site_locations=assignee_site_locations,
            fleet_vehicles=vehicles,
            consumable_lines=consumable_lines,
            consumable_items_select=consumable_items_select,
            can_edit=_can_edit(),
            can_assign=_can_assign(),
            can_records=_can_records(),
            can_consumable_edit=_can_consumable_edit(),
            consumables_have_discrepancy=any(
                bool(c.get("has_discrepancy")) for c in consumable_lines
            ),
            portal_handoffs_pending=portal_handoffs_pending,
        )


    @bp.route("/equipment/assets/<int:asset_id>/quick-status", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_edit_required
    def equipment_asset_quick_status(asset_id: int):
        """Update lifecycle status from the equipment desk list (no inventory moves)."""
        new_st = (request.form.get("status") or "").strip()
        allowed = {"in_stock", "assigned", "loaned", "maintenance", "retired", "lost"}
        if new_st not in allowed:
            flash("Invalid status.", "danger")
            return _redirect_equipment_desk()
        inv = get_inventory_service()
        asset = inv.get_equipment_asset(int(asset_id))
        if not asset:
            flash("Asset not found.", "danger")
            return _redirect_equipment_desk()
        cur = str(asset.get("status") or "")
        if cur in ("assigned", "loaned") and new_st == "in_stock":
            flash(
                "To sign kit back in, use Sign out → return to storeroom on Assignments or the asset page.",
                "warning",
            )
            q = (request.form.get("q") or "").strip()
            st = (request.form.get("filter_status") or "").strip()
            fa = (request.form.get("filter_assigned") or "").strip() == "1"
            return _redirect_equipment_desk(q=q or None, status=st or None, assigned=fa)
        try:
            inv.update_equipment_asset(int(asset_id), status=new_st)
            flash("Status updated.", "success")
        except Exception as e:
            logger.exception("asset_quick_status")
            flash(str(e), "danger")
        q = (request.form.get("q") or "").strip()
        st = (request.form.get("filter_status") or "").strip()
        fa = (request.form.get("filter_assigned") or "").strip() == "1"
        return _redirect_equipment_desk(q=q or None, status=st or None, assigned=fa)


    @bp.route("/equipment/assets/<int:asset_id>/assign-vehicle", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_edit_required
    def equipment_asset_assign_vehicle(asset_id: int):
        """Sign out asset to a fleet vehicle (wrapper around inventory out + assignee)."""
        try:
            location_id = int(request.form.get("location_id") or 0)
            vehicle_id = int(request.form.get("vehicle_id") or 0)
            if not location_id or not vehicle_id:
                flash("location_id and vehicle_id required.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
            from app.plugins.fleet_management.objects import get_fleet_service

            fv = get_fleet_service().get_vehicle(vehicle_id)
            if not fv:
                flash("Vehicle not found.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
            label = fv.get("registration") or fv.get("internal_code") or str(vehicle_id)
            inv = get_inventory_service()
            asset = inv.get_equipment_asset(int(asset_id))
            if not asset:
                flash("Asset not found.", "danger")
                return _redirect_equipment_desk()
            if str(asset.get("status") or "") != "in_stock":
                flash("Asset must be in stock before sign-out to a vehicle.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
            item_id = int(asset["item_id"])
            inv.record_transaction(
                item_id=item_id,
                location_id=location_id,
                quantity=1.0,
                transaction_type="out",
                performed_by_user_id=_uid(),
                assignee_type="vehicle",
                assignee_id=str(vehicle_id),
                assignee_label=label,
                equipment_asset_id=int(asset_id),
                reference_type="fleet_vehicle_assign",
                reference_id=str(vehicle_id),
                metadata={"fleet_vehicle_id": vehicle_id},
            )
            inv.set_equipment_asset_status(int(asset_id), "assigned")
            flash("Asset assigned to vehicle.", "success")
        except Exception as e:
            logger.exception("assign vehicle")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/portal-handoff", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_assign_required
    def equipment_asset_portal_handoff_create(asset_id: int):
        """Let a contractor complete return / vehicle placement from the employee portal (after this task is created)."""
        svc = get_asset_service()
        inv = get_inventory_service()
        h = svc.get_current_holder(int(asset_id))
        if not h or str(h.get("holder_type") or "") != "contractor":
            flash("Asset must currently be signed out to a contractor.", "danger")
            return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
        try:
            cid = int(str(h.get("holder_id") or "").strip())
        except (TypeError, ValueError):
            flash("Could not read contractor id on the current assignment.", "danger")
            return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
        kind = (request.form.get("handoff_kind") or "").strip()
        try:
            loc_id = int(request.form.get("inventory_location_id") or 0)
        except (TypeError, ValueError):
            loc_id = 0
        vid_raw = (request.form.get("vehicle_id") or "").strip()
        vehicle_id = int(vid_raw) if vid_raw.isdigit() else None
        if not loc_id:
            flash("Choose the storeroom / issue location used for the inventory entries.", "danger")
            return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
        try:
            inv.create_equipment_portal_handoff(
                equipment_asset_id=int(asset_id),
                contractor_id=cid,
                handoff_kind=kind,
                inventory_location_id=loc_id,
                vehicle_id=vehicle_id,
                initiated_by_user_id=_uid(),
                notes=(request.form.get("notes") or "").strip() or None,
            )
            flash(
                "Portal handoff created. The contractor will see it under Employee portal → My equipment.",
                "success",
            )
        except Exception as e:
            logger.exception("portal handoff create")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route(
        "/assets/<int:asset_id>/portal-handoff/<int:handoff_id>/cancel",
        methods=["POST"],
    )
    @login_required
    @asset_access_required
    @asset_assign_required
    def equipment_asset_portal_handoff_cancel(asset_id: int, handoff_id: int):
        inv = get_inventory_service()
        try:
            ho = inv.get_equipment_portal_handoff(int(handoff_id))
            if not ho or int(ho["equipment_asset_id"]) != int(asset_id):
                flash("Handoff not found for this asset.", "danger")
            elif inv.cancel_equipment_portal_handoff(int(handoff_id)):
                flash("Portal handoff cancelled.", "success")
            else:
                flash("Could not cancel (already completed or cancelled).", "warning")
        except Exception as e:
            logger.exception("portal handoff cancel")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/return-stock", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_assign_required
    def equipment_asset_return_to_stock(asset_id: int):
        try:
            location_id = int(request.form.get("location_id") or 0)
            if not location_id:
                flash("Choose a return location.", "danger")
                return _redirect_after_return_to_stock(asset_id)
            inv = get_inventory_service()
            asset = inv.get_equipment_asset(int(asset_id))
            if not asset:
                flash("Asset not found.", "danger")
                return _redirect_equipment_desk()
            if str(asset.get("status") or "") not in ("assigned", "loaned"):
                flash("Asset is not signed out; nothing to return.", "warning")
                return _redirect_after_return_to_stock(asset_id)
            item_id = int(asset["item_id"])
            meta = {}
            fv = (request.form.get("from_vehicle_id") or "").strip()
            if fv:
                meta["from_vehicle_id"] = str(int(fv))
            inv.record_transaction(
                item_id=item_id,
                location_id=location_id,
                quantity=1.0,
                transaction_type="return",
                performed_by_user_id=_uid(),
                equipment_asset_id=int(asset_id),
                metadata=meta or None,
            )
            inv.set_equipment_asset_status(int(asset_id), "in_stock")
            flash("Asset returned to storeroom.", "success")
        except Exception as e:
            logger.exception("return to stock")
            flash(str(e), "danger")
        return _redirect_after_return_to_stock(asset_id)


    @bp.route("/equipment/assets/<int:asset_id>/assign-holder", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_edit_required
    def equipment_asset_assign_holder(asset_id: int):
        """Sign out to a user or contractor (inventory assignee)."""
        try:
            location_id = int(request.form.get("location_id") or 0)
            assignee_type = (request.form.get("assignee_type") or "").strip()
            assignee_id = (request.form.get("assignee_id") or "").strip()
            assignee_label = (request.form.get("assignee_label") or "").strip()
            if not location_id or assignee_type not in ("user", "contractor") or not assignee_id:
                flash("Location, assignee type, and assignee ID are required.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
            inv = get_inventory_service()
            asset = inv.get_equipment_asset(int(asset_id))
            if not asset:
                flash("Asset not found.", "danger")
                return _redirect_equipment_desk()
            if str(asset.get("status") or "") != "in_stock":
                flash("Asset must be in stock before sign-out to a person.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
            item_id = int(asset["item_id"])
            inv.record_transaction(
                item_id=item_id,
                location_id=location_id,
                quantity=1.0,
                transaction_type="out",
                performed_by_user_id=_uid(),
                assignee_type=assignee_type,
                assignee_id=assignee_id,
                assignee_label=assignee_label or assignee_id,
                equipment_asset_id=int(asset_id),
            )
            inv.set_equipment_asset_status(int(asset_id), "assigned")
            flash("Asset assigned.", "success")
        except Exception as e:
            logger.exception("assign holder")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/assign-location", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_edit_required
    def equipment_asset_assign_to_location(asset_id: int):
        """Sign out to a holding or virtual site location (post-event reconciliation pool)."""
        try:
            location_id = int(request.form.get("location_id") or 0)
            target_location_id = int(request.form.get("target_location_id") or 0)
            if not location_id or not target_location_id:
                flash("Storeroom and target holding/site location are required.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
            inv = get_inventory_service()
            loc_rows = inv.list_locations()
            target = next(
                (
                    L
                    for L in (loc_rows or [])
                    if int(L.get("id") or 0) == target_location_id
                ),
                None,
            )
            if not target or str(target.get("type") or "") not in ("holding", "virtual"):
                flash("Target must be a location with type Holding or Virtual.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
            code = (target.get("code") or "").strip()
            name = (target.get("name") or "").strip()
            if code and name:
                default_label = f"{code} — {name}"
            elif name:
                default_label = name
            elif code:
                default_label = code
            else:
                default_label = str(target_location_id)
            label = (request.form.get("assignee_label") or "").strip() or default_label
            asset = inv.get_equipment_asset(int(asset_id))
            if not asset:
                flash("Asset not found.", "danger")
                return _redirect_equipment_desk()
            if str(asset.get("status") or "") != "in_stock":
                flash("Asset must be in stock before sign-out to a holding/site location.", "danger")
                return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
            item_id = int(asset["item_id"])
            inv.record_transaction(
                item_id=item_id,
                location_id=location_id,
                quantity=1.0,
                transaction_type="out",
                performed_by_user_id=_uid(),
                assignee_type="location",
                assignee_id=str(target_location_id),
                assignee_label=label,
                equipment_asset_id=int(asset_id),
                reference_type="holding_site_assign",
                reference_id=str(target_location_id),
                metadata={
                    "holding_location_id": target_location_id,
                    "holding_location_code": target.get("code"),
                },
            )
            inv.set_equipment_asset_status(int(asset_id), "assigned")
            flash(
                "Signed out to holding/site. Log condition, faults, and service on this asset, then return to storeroom when verified.",
                "success",
            )
        except Exception as e:
            logger.exception("assign to holding location")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/servicing-plan", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_records_required
    def equipment_asset_servicing_plan(asset_id: int):
        inv = get_inventory_service()
        try:
            nsd = (request.form.get("next_service_due_date") or "").strip() or None
            sid = (request.form.get("service_interval_days") or "").strip()
            interval = int(sid) if sid not in ("", None) else None
            inv.update_equipment_asset(
                int(asset_id),
                next_service_due_date=nsd,
                service_interval_days=interval,
            )
            flash("Servicing plan saved.", "success")
        except Exception as e:
            logger.exception("servicing plan")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/identity-warranty", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_records_required
    def equipment_asset_identity_warranty(asset_id: int):
        """Make/model, purchase date, and manufacturer warranty clock (delivery vs install)."""
        inv = get_inventory_service()
        try:
            w_start = (request.form.get("warranty_start_date") or "").strip() or None
            w_months_raw = (request.form.get("warranty_months") or "").strip()
            w_exp = (request.form.get("warranty_expiry") or "").strip() or None
            if request.form.get("apply_warranty_months") == "1" and w_start and w_months_raw.isdigit():
                m = int(w_months_raw)
                if m > 0:
                    w_exp = add_calendar_months(date.fromisoformat(w_start[:10]), m).isoformat()

            wm_param = None
            if "warranty_months" in request.form:
                if w_months_raw == "":
                    wm_param = ""
                elif w_months_raw.isdigit():
                    wm_param = int(w_months_raw)
                else:
                    flash("Warranty months must be empty or a whole number.", "danger")
                    return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))

            inv.update_equipment_asset(
                int(asset_id),
                make=(request.form.get("make") or "").strip() or None,
                model=(request.form.get("model") or "").strip() or None,
                purchase_date=(request.form.get("purchase_date") or "").strip() or None,
                warranty_start_basis=(request.form.get("warranty_start_basis") or "").strip() or None,
                warranty_start_date=w_start,
                warranty_months=wm_param,
                warranty_expiry=w_exp,
            )
            flash("Identity and warranty fields saved.", "success")
        except Exception as e:
            logger.exception("asset_identity_warranty")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/issues", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_edit_required
    def equipment_asset_issue_create(asset_id: int):
        svc = get_asset_service()
        try:
            svc.add_equipment_issue(
                equipment_asset_id=asset_id,
                title=request.form.get("title") or "Issue",
                description=request.form.get("description"),
                severity=(request.form.get("severity") or "medium").strip(),
                status=(request.form.get("status") or "open").strip(),
                scheduled_action_date=(request.form.get("scheduled_action_date") or "").strip()
                or None,
                external_reference=(request.form.get("external_reference") or "").strip() or None,
                reported_by_user_id=_uid(),
            )
            flash("Issue logged.", "success")
        except Exception as e:
            logger.exception("issue create")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/issues/<int:issue_id>", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_records_required
    def equipment_asset_issue_update(asset_id: int, issue_id: int):
        svc = get_asset_service()
        try:
            svc.update_equipment_issue(
                int(issue_id),
                status=(request.form.get("status") or "").strip() or None,
                scheduled_action_date=(request.form.get("scheduled_action_date") or "").strip()
                or None,
                external_reference=(request.form.get("external_reference") or "").strip() or None,
                resolution_notes=(request.form.get("resolution_notes") or "").strip() or None,
            )
            flash("Issue updated.", "success")
        except Exception as e:
            logger.exception("issue update")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/maintenance", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_edit_required
    def equipment_asset_add_maintenance(asset_id: int):
        svc = get_asset_service()
        try:
            cost_raw = request.form.get("cost")
            cost = float(cost_raw) if cost_raw not in (None, "") else None
            svc.add_maintenance_event(
                equipment_asset_id=asset_id,
                service_date=request.form.get("service_date") or "",
                service_type=request.form.get("service_type") or "Service",
                notes=request.form.get("notes"),
                cost=cost,
                performed_by=request.form.get("performed_by"),
                created_by=_uid(),
            )
            flash("Maintenance recorded.", "success")
        except Exception as e:
            logger.exception("asset maintenance")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/consumables/add", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_consumables_edit_required
    def equipment_asset_consumable_add(asset_id: int):
        inv = get_inventory_service()
        if not inv.get_equipment_asset(int(asset_id)):
            flash("Asset not found.", "danger")
            return _redirect_equipment_desk()
        label = (request.form.get("label") or "").strip()
        if not label:
            flash("Description / label is required.", "danger")
            return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
        iix = (request.form.get("inventory_item_id") or "").strip()
        inventory_item_id = int(iix) if iix.isdigit() else None
        try:
            qty_raw = (request.form.get("quantity") or "1").strip()
            qty = float(qty_raw) if qty_raw else 1.0
            inv.add_equipment_asset_consumable(
                equipment_asset_id=int(asset_id),
                label=label,
                batch_number=(request.form.get("batch_number") or "").strip() or None,
                lot_number=(request.form.get("lot_number") or "").strip() or None,
                expiry_date=(request.form.get("expiry_date") or "").strip() or None,
                quantity=qty,
                inventory_item_id=inventory_item_id,
                notes=(request.form.get("notes") or "").strip() or None,
            )
            flash("Consumable line added.", "success")
        except Exception as e:
            logger.exception("consumable add")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/consumables/<int:consumable_id>/update", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_consumables_edit_required
    def equipment_asset_consumable_update(asset_id: int, consumable_id: int):
        inv = get_inventory_service()
        iix = (request.form.get("inventory_item_id") or "").strip()
        if iix and not iix.isdigit():
            flash("Invalid inventory item.", "danger")
            return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
        item_id_val = int(iix) if iix.isdigit() else None
        lab = (request.form.get("label") or "").strip()
        if not lab:
            flash("Description is required.", "danger")
            return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))
        try:
            qty_raw = (request.form.get("quantity") or "").strip()
            qty = float(qty_raw) if qty_raw else 1.0
            inv.update_equipment_asset_consumable(
                int(consumable_id),
                equipment_asset_id=int(asset_id),
                label=lab,
                batch_number=(request.form.get("batch_number") or "").strip(),
                lot_number=(request.form.get("lot_number") or "").strip(),
                expiry_date=(request.form.get("expiry_date") or "").strip() or None,
                quantity=qty,
                notes=(request.form.get("notes") or "").strip(),
                inventory_item_id=item_id_val,
                depleted=request.form.get("depleted") == "1",
            )
            flash("Consumable updated.", "success")
        except Exception as e:
            logger.exception("consumable update")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/consumables/<int:consumable_id>/delete", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_consumables_edit_required
    def equipment_asset_consumable_delete(asset_id: int, consumable_id: int):
        inv = get_inventory_service()
        try:
            if inv.delete_equipment_asset_consumable(int(consumable_id), int(asset_id)):
                flash("Line removed.", "success")
            else:
                flash("Line not found.", "warning")
        except Exception as e:
            logger.exception("consumable delete")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/equipment/assets/<int:asset_id>/consumables/<int:consumable_id>/deplete", methods=["POST"])
    @login_required
    @asset_access_required
    @asset_consumables_edit_required
    def equipment_asset_consumable_deplete(asset_id: int, consumable_id: int):
        inv = get_inventory_service()
        reason = (request.form.get("usage_close_reason") or "used_in_call").strip()[:32]
        allowed = {
            "used_in_call",
            "wastage",
            "damaged",
            "expired_disposal",
            "other",
        }
        if reason not in allowed:
            reason = "used_in_call"
        try:
            inv.update_equipment_asset_consumable(
                int(consumable_id),
                equipment_asset_id=int(asset_id),
                depleted=True,
                usage_close_reason=reason,
            )
            flash("Marked as used / depleted.", "success")
        except Exception as e:
            logger.exception("consumable deplete")
            flash(str(e), "danger")
        return redirect(url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id))


    @bp.route("/api/equipment/assign", methods=["POST"])
    @login_required
    @asset_access_required
    def equipment_api_assign():
        """Sign out equipment to user, vehicle, or location (inventory transaction)."""
        if not _can_assign():
            return jsonify({"error": "Forbidden"}), 403
        data = request.get_json() or {}
        equipment_asset_id = data.get("equipment_asset_id")
        location_id = data.get("location_id")
        assignee_type = (data.get("assignee_type") or "").strip()
        assignee_id = (data.get("assignee_id") or "").strip() or None
        assignee_label = (data.get("assignee_label") or "").strip() or None
        is_loan = bool(data.get("is_loan"))
        if equipment_asset_id is None or location_id is None:
            return jsonify({"error": "equipment_asset_id and location_id required"}), 400
        if assignee_type not in ("user", "vehicle", "location", "contractor"):
            return jsonify({"error": "assignee_type must be user|vehicle|location|contractor"}), 400

        inv = get_inventory_service()
        try:
            asset = inv.get_equipment_asset(int(equipment_asset_id))
            if not asset:
                return jsonify({"error": "Asset not found"}), 404
            item_id = int(asset["item_id"])
            if assignee_type == "vehicle" and assignee_id and not assignee_label:
                try:
                    from app.plugins.fleet_management.objects import get_fleet_service

                    fv = get_fleet_service().get_vehicle(int(assignee_id))
                    if fv:
                        assignee_label = fv.get("registration") or fv.get("internal_code")
                except Exception:
                    pass

            result = inv.record_transaction(
                item_id=item_id,
                location_id=int(location_id),
                quantity=1.0,
                transaction_type="out",
                performed_by_user_id=_uid(),
                assignee_type=assignee_type,
                assignee_id=assignee_id,
                assignee_label=assignee_label,
                is_loan=is_loan,
                due_back_date=data.get("due_back_date"),
                equipment_asset_id=int(equipment_asset_id),
            )
            try:
                inv.set_equipment_asset_status(
                    int(equipment_asset_id), "loaned" if is_loan else "assigned"
                )
            except Exception:
                logger.exception("asset status")
            return jsonify({"ok": True, "result": result})
        except Exception as e:
            return jsonify({"error": str(e)}), 400


    @bp.route("/api/equipment/return", methods=["POST"])
    @login_required
    @asset_access_required
    def equipment_api_return_asset():
        """Return equipment to stock (inventory transaction)."""
        if not _can_assign():
            return jsonify({"error": "Forbidden"}), 403
        data = request.get_json() or {}
        equipment_asset_id = data.get("equipment_asset_id")
        location_id = data.get("location_id")
        if equipment_asset_id is None or location_id is None:
            return jsonify({"error": "equipment_asset_id and location_id required"}), 400
        inv = get_inventory_service()
        try:
            asset = inv.get_equipment_asset(int(equipment_asset_id))
            if not asset:
                return jsonify({"error": "Asset not found"}), 404
            item_id = int(asset["item_id"])
            meta = {}
            fv = data.get("from_vehicle_id")
            if fv is not None and str(fv).strip() != "":
                meta["from_vehicle_id"] = str(int(fv))
            result = inv.record_transaction(
                item_id=item_id,
                location_id=int(location_id),
                quantity=1.0,
                transaction_type="return",
                performed_by_user_id=_uid(),
                equipment_asset_id=int(equipment_asset_id),
                metadata=meta or None,
            )
            try:
                inv.set_equipment_asset_status(int(equipment_asset_id), "in_stock")
            except Exception:
                pass
            return jsonify({"ok": True, "result": result})
        except Exception as e:
            return jsonify({"error": str(e)}), 400


