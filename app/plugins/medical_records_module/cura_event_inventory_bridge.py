"""
Optional integration: auto virtual inventory location (kit pool) per Cura operational event.

When the event division is created, provision OP-EVT-{id}. On close/delete, serial kit held there
moves to HOLD-IN and the virtual location is removed.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _config_to_dict(raw: Any) -> Dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            v = json.loads(raw)
            return dict(v) if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def event_kit_pool_location_id_from_config(raw: Any) -> Optional[int]:
    v = _config_to_dict(raw).get("inventory_event_kit_pool_location_id")
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def merge_event_kit_pool_config(raw_config: Any, pool_location_id: int) -> str:
    cfg = _config_to_dict(raw_config)
    cfg["inventory_event_kit_pool_location_id"] = int(pool_location_id)
    return json.dumps(cfg)


def strip_event_kit_pool_config(raw_config: Any) -> str:
    cfg = _config_to_dict(raw_config)
    cfg.pop("inventory_event_kit_pool_location_id", None)
    return json.dumps(cfg)


def provision_and_link_event_kit_pool(
    cur,
    conn,
    operational_event_id: int,
    event_name: str,
    actor: str,
) -> Optional[int]:
    """
    Create OP-EVT-{id} and store its inventory_locations.id on the event config (same txn as caller).
    """
    try:
        from app.plugins.inventory_control.objects import get_inventory_service
    except Exception as ex:
        logger.warning("event kit pool: inventory_control unavailable: %s", ex)
        return None
    try:
        inv = get_inventory_service()
        pool_id = inv.provision_cura_operational_event_kit_pool(
            int(operational_event_id), event_name
        )
        if not pool_id:
            return None
        cur.execute(
            "SELECT config FROM cura_operational_events WHERE id = %s",
            (int(operational_event_id),),
        )
        row = cur.fetchone()
        raw_cfg = row[0] if row else None
        new_cfg = merge_event_kit_pool_config(raw_cfg, int(pool_id))
        cur.execute(
            """
            UPDATE cura_operational_events
            SET config = %s, updated_by = %s
            WHERE id = %s
            """,
            (new_cfg, (actor or "")[:128], int(operational_event_id)),
        )
        return int(pool_id)
    except Exception as ex:
        logger.exception("provision_and_link_event_kit_pool: %s", ex)
        return None


def release_event_kit_pool_if_configured(
    operational_event_id: int,
    raw_config: Any,
    *,
    performed_by: Optional[str] = None,
) -> Dict[str, Any]:
    pool_id = event_kit_pool_location_id_from_config(raw_config)
    if not pool_id:
        return {"ok": True, "skipped": True}
    try:
        from app.plugins.inventory_control.objects import get_inventory_service

        return get_inventory_service().release_cura_operational_event_kit_pool(
            pool_location_id=int(pool_id),
            operational_event_id=int(operational_event_id),
            performed_by_user_id=performed_by,
        )
    except Exception as ex:
        logger.exception("release_event_kit_pool_if_configured")
        return {"ok": False, "error": str(ex)}
