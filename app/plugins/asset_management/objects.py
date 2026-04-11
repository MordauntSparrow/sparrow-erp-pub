"""Backward-compatible re-export; implementation lives in inventory_control.asset_service."""

from app.plugins.inventory_control.asset_service import AssetService, get_asset_service

__all__ = ["AssetService", "get_asset_service"]
