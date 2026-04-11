"""
GET redirects from /plugin/asset_management/* to merged inventory_control URLs.

POST actions should use the new paths (forms in templates are updated).
"""

from __future__ import annotations

from flask import Blueprint, redirect, request, url_for

legacy_asset_management_bp = Blueprint(
    "asset_management",
    __name__,
    url_prefix="/plugin/asset_management",
)


@legacy_asset_management_bp.route("/")
@legacy_asset_management_bp.route("/index")
def _redirect_servicing_home():
    return redirect(url_for("inventory_control_internal.equipment_servicing_dashboard"))


@legacy_asset_management_bp.route("/assignments")
def _redirect_assignments():
    return redirect(url_for("inventory_control_internal.equipment_assignments_page"))


@legacy_asset_management_bp.route("/assets")
def _redirect_assets_catalog():
    kw: dict = {}
    q = (request.args.get("q") or "").strip()
    st = (request.args.get("status") or "").strip()
    if q:
        kw["q"] = q
    if st:
        kw["status"] = st
    if (request.args.get("assigned") or "").strip() == "1":
        kw["assigned"] = 1
    return redirect(url_for("inventory_control_internal.equipment_desk_page", **kw))


@legacy_asset_management_bp.route("/assets/new")
def _redirect_asset_new():
    return redirect(url_for("inventory_control_internal.equipment_asset_new"))


@legacy_asset_management_bp.route("/assets/<int:asset_id>")
def _redirect_asset_detail(asset_id: int):
    return redirect(
        url_for("inventory_control_internal.equipment_asset_detail", asset_id=asset_id)
    )
