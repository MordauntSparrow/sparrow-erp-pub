"""Asset maintenance and reporting on inventory equipment."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from app.objects import get_db_connection

from app.plugins.inventory_control.objects import get_inventory_service
from app.plugins.inventory_control.equipment_display import (
    equipment_asset_status_label,
    equipment_holder_type_label,
)

logger = logging.getLogger("inventory_control.assets")


def get_asset_service():
    return AssetService()


class AssetService:
    def _conn(self):
        return get_db_connection()

    def list_maintenance_events(self, equipment_asset_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT * FROM asset_maintenance_events
                WHERE equipment_asset_id = %s
                ORDER BY service_date DESC, id DESC
                """,
                (int(equipment_asset_id),),
            )
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def add_maintenance_event(
        self,
        *,
        equipment_asset_id: int,
        service_date: str,
        service_type: str,
        notes: Optional[str] = None,
        cost: Optional[float] = None,
        performed_by: Optional[str] = None,
        created_by: Optional[str] = None,
    ) -> int:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO asset_maintenance_events
                    (equipment_asset_id, service_date, service_type, notes, cost, performed_by, created_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(equipment_asset_id),
                    service_date,
                    service_type.strip(),
                    (notes or "").strip() or None,
                    cost,
                    (performed_by or "").strip() or None,
                    created_by,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_assets_with_items(
        self,
        *,
        status: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            sql = """
            SELECT ea.*, i.name AS item_name, i.sku
            FROM inventory_equipment_assets ea
            INNER JOIN inventory_items i ON i.id = ea.item_id
            WHERE 1=1
            """
            params: List[Any] = []
            if status:
                sql += " AND ea.status = %s"
                params.append(status)
            if search:
                sql += " AND (ea.serial_number LIKE %s OR ea.public_asset_code LIKE %s OR i.name LIKE %s)"
                q = f"%{search.strip()}%"
                params.extend([q, q, q])
            sql += " ORDER BY ea.updated_at DESC LIMIT %s"
            params.append(min(max(limit, 1), 500))
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
            for r in rows:
                if isinstance(r.get("metadata"), str):
                    try:
                        r["metadata"] = json.loads(r["metadata"])
                    except Exception:
                        pass
            return rows
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_asset_detail(self, asset_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT ea.*, i.name AS item_name, i.sku, i.id AS item_id
                FROM inventory_equipment_assets ea
                INNER JOIN inventory_items i ON i.id = ea.item_id
                WHERE ea.id = %s
                """,
                (int(asset_id),),
            )
            row = cur.fetchone()
            if row and isinstance(row.get("metadata"), str):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except Exception:
                    pass
            return row
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_transactions_for_asset(self, asset_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        """Raw inventory rows for an equipment asset (legacy). Prefer list_equipment_movement_log()."""
        inv = get_inventory_service()
        conn = inv._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT * FROM inventory_transactions
                WHERE equipment_asset_id = %s
                ORDER BY performed_at DESC
                LIMIT %s
                """,
                (int(asset_id), min(limit, 500)),
            )
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def _parse_tx_metadata(self, raw: Any) -> Dict[str, Any]:
        if raw is None:
            return {}
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                o = json.loads(raw)
                return o if isinstance(o, dict) else {}
            except Exception:
                return {}
        return {}

    def movement_summary_for_row(self, row: Dict[str, Any]) -> str:
        """Single-line description for an inventory row affecting equipment."""
        tx = str(row.get("transaction_type") or "")
        qty = row.get("quantity")
        loc = row.get("location_name") or row.get("location_code") or ""
        if not loc and row.get("location_id") is not None:
            loc = f"Location #{row.get('location_id')}"
        atype = (row.get("assignee_type") or "").strip()
        alab = row.get("assignee_label") or row.get("assignee_id") or ""
        meta = self._parse_tx_metadata(row.get("metadata"))
        from_v = meta.get("from_vehicle_id")
        if tx == "out" and atype == "vehicle" and alab:
            if int(row.get("is_loan") or 0):
                return f"Loaned to vehicle {alab}"
            return f"Installed on vehicle {alab}"
        if tx == "out" and atype == "user" and alab:
            return f"Signed out to user {alab}"
        if tx == "out" and atype == "contractor" and alab:
            return f"Signed out to contractor {alab}"
        if tx == "out" and atype == "location" and alab:
            return f"Moved to {alab}"
        if tx in ("return", "in") and from_v:
            return f"Returned to stock ({loc}) — from vehicle #{from_v}"
        if tx == "return":
            return f"Returned to stock at {loc or 'location'}"
        if tx == "in":
            return f"Received into stock at {loc or 'location'}"
        if tx == "transfer":
            return f"Transfer ({tx}) at {loc or 'location'} — qty {qty}"
        if tx == "adjustment":
            return f"Stock adjustment — qty {qty}"
        return f"{tx} — qty {qty}"

    def list_equipment_movement_log(
        self, asset_id: int, limit: int = 150
    ) -> List[Dict[str, Any]]:
        """Placement history with location names and human-readable summaries."""
        inv = get_inventory_service()
        conn = inv._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT t.*, loc.name AS location_name, loc.code AS location_code
                FROM inventory_transactions t
                LEFT JOIN inventory_locations loc ON loc.id = t.location_id
                WHERE t.equipment_asset_id = %s
                ORDER BY t.performed_at DESC, t.id DESC
                LIMIT %s
                """,
                (int(asset_id), min(max(limit, 1), 500)),
            )
            rows = cur.fetchall() or []
            for r in rows:
                r["movement_summary"] = self.movement_summary_for_row(r)
            return rows
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_current_holder(self, asset_id: int) -> Optional[Dict[str, Any]]:
        """Latest vehicle / user / contractor (or location) assignee when asset is out."""
        inv = get_inventory_service()
        asset = inv.get_equipment_asset(int(asset_id))
        if not asset or str(asset.get("status") or "") not in ("assigned", "loaned"):
            return None
        conn = inv._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT assignee_type, assignee_id, assignee_label, performed_at,
                       performed_by_user_id
                FROM inventory_transactions
                WHERE equipment_asset_id = %s
                  AND transaction_type = 'out'
                  AND assignee_type IN ('vehicle','user','contractor','location')
                  AND assignee_id IS NOT NULL
                  AND assignee_id <> ''
                ORDER BY performed_at DESC, id DESC
                LIMIT 1
                """,
                (int(asset_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            ht = str(row.get("assignee_type") or "")
            out: Dict[str, Any] = {
                "holder_type": ht,
                "holder_id": row.get("assignee_id"),
                "holder_label": row.get("assignee_label") or row.get("assignee_id"),
                "assigned_at": row.get("performed_at"),
                "assigned_by_user_id": row.get("performed_by_user_id"),
            }
            if ht == "vehicle":
                try:
                    out["vehicle_id"] = int(row["assignee_id"])
                except (TypeError, ValueError):
                    out["vehicle_id"] = None
            return out
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_current_vehicle_assignment(
        self, asset_id: int
    ) -> Optional[Dict[str, Any]]:
        h = self.get_current_holder(asset_id)
        if not h or h.get("holder_type") != "vehicle":
            return None
        vid = h.get("vehicle_id")
        if vid is None:
            return None
        return {
            "vehicle_id": int(vid),
            "label": h.get("holder_label") or str(vid),
            "assigned_at": h.get("assigned_at"),
        }

    def contractor_holds_asset(self, contractor_id: int, asset_id: int) -> bool:
        """True if this serial asset is currently signed out to the contractor (latest out tx)."""
        h = self.get_current_holder(int(asset_id))
        if not h:
            return False
        return (
            str(h.get("holder_type") or "") == "contractor"
            and str(h.get("holder_id") or "").strip() == str(int(contractor_id))
        )

    def list_assets_held_by_contractor(self, contractor_id: int) -> List[Dict[str, Any]]:
        """Equipment signed out to a contractor (mobile / walking unit), for employee portal."""
        cid = str(int(contractor_id))
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT ea.id, ea.serial_number, ea.public_asset_code, ea.status, ea.item_id,
                       i.name AS item_name, i.sku, tl.assignee_label AS holder_label,
                       tl.performed_at AS assigned_at
                FROM inventory_equipment_assets ea
                INNER JOIN inventory_items i ON i.id = ea.item_id
                INNER JOIN inventory_transactions tl ON tl.equipment_asset_id = ea.id
                INNER JOIN (
                    SELECT equipment_asset_id, MAX(id) AS mid
                    FROM inventory_transactions
                    WHERE transaction_type = 'out'
                      AND assignee_type IN ('vehicle','user','contractor','location')
                      AND equipment_asset_id IS NOT NULL
                    GROUP BY equipment_asset_id
                ) x ON x.mid = tl.id AND x.equipment_asset_id = tl.equipment_asset_id
                WHERE ea.status IN ('assigned','loaned')
                  AND tl.assignee_type = 'contractor'
                  AND tl.assignee_id = %s
                ORDER BY i.name, ea.serial_number
                """,
                (cid,),
            )
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_equipment_in_holding_pool(self, *, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Serial equipment signed out to a holding or virtual site location — reconciliation
        limbo before final putaway, faults, and maintenance are cleared.
        """
        lim = min(max(int(limit), 1), 200)
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"""
                SELECT ea.id, ea.serial_number, ea.public_asset_code, ea.status,
                       ea.next_service_due_date,
                       i.name AS item_name, i.sku,
                       loc.id AS pool_location_id, loc.code AS pool_location_code,
                       loc.name AS pool_location_name, loc.type AS pool_location_type,
                       tl.assignee_label, tl.performed_at AS assigned_at,
                       (SELECT COUNT(*) FROM asset_equipment_issues aei
                        WHERE aei.equipment_asset_id = ea.id
                          AND aei.status NOT IN ('resolved', 'closed')) AS open_issues
                FROM inventory_equipment_assets ea
                INNER JOIN inventory_items i ON i.id = ea.item_id
                INNER JOIN inventory_transactions tl ON tl.equipment_asset_id = ea.id
                INNER JOIN (
                    SELECT equipment_asset_id, MAX(id) AS mid
                    FROM inventory_transactions
                    WHERE transaction_type = 'out'
                      AND assignee_type IN ('vehicle','user','contractor','location')
                      AND equipment_asset_id IS NOT NULL
                    GROUP BY equipment_asset_id
                ) x ON x.mid = tl.id AND x.equipment_asset_id = tl.equipment_asset_id
                INNER JOIN inventory_locations loc
                  ON loc.type IN ('holding', 'virtual')
                 AND loc.id = CAST(tl.assignee_id AS UNSIGNED)
                WHERE ea.status IN ('assigned', 'loaned')
                  AND tl.assignee_type = 'location'
                ORDER BY tl.performed_at DESC
                LIMIT {lim}
                """
            )
            rows = cur.fetchall() or []
            for r in rows:
                pa = r.get("assigned_at")
                if hasattr(pa, "isoformat"):
                    r["assigned_at"] = pa.isoformat()
                nsd = r.get("next_service_due_date")
                if hasattr(nsd, "isoformat"):
                    r["next_service_due_date"] = nsd.isoformat()
            return rows
        except Exception:
            logger.exception("list_equipment_in_holding_pool")
            return []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def _holder_type_label(self, ht: str) -> str:
        return equipment_holder_type_label(ht)

    def list_equipment_desk_rows(
        self,
        *,
        search: Optional[str] = None,
        status: Optional[str] = None,
        item_id: Optional[int] = None,
        assigned_only: bool = False,
        limit: int = 400,
    ) -> List[Dict[str, Any]]:
        """Serialised equipment with holder, issues count, servicing hints (equipment desk)."""
        rows = self.list_assets_with_items(
            search=search, status=status, limit=min(max(limit, 1), 600)
        )
        if item_id is not None:
            rows = [r for r in rows if int(r.get("item_id") or 0) == int(item_id)]
        if assigned_only:
            rows = [
                r
                for r in rows
                if str(r.get("status") or "") in ("assigned", "loaned")
            ]
        if not rows:
            return []
        ids = [int(r["id"]) for r in rows]
        inv = get_inventory_service()
        conn = inv._connection()
        cur = conn.cursor(dictionary=True)
        holders: Dict[int, Dict[str, Any]] = {}
        issue_counts: Dict[int, int] = {}
        try:
            if ids:
                ph = ",".join(["%s"] * len(ids))
                cur.execute(
                    f"""
                    SELECT t.equipment_asset_id, t.assignee_type, t.assignee_id, t.assignee_label
                    FROM inventory_transactions t
                    INNER JOIN (
                        SELECT equipment_asset_id, MAX(id) AS mid
                        FROM inventory_transactions
                        WHERE transaction_type = 'out'
                          AND assignee_type IN ('vehicle','user','contractor','location')
                          AND equipment_asset_id IN ({ph})
                        GROUP BY equipment_asset_id
                    ) x ON x.mid = t.id AND x.equipment_asset_id = t.equipment_asset_id
                    """,
                    tuple(ids),
                )
                for hrow in cur.fetchall() or []:
                    eid = int(hrow["equipment_asset_id"])
                    holders[eid] = {
                        "holder_type": hrow.get("assignee_type"),
                        "holder_id": hrow.get("assignee_id"),
                        "holder_label": hrow.get("assignee_label")
                        or hrow.get("assignee_id"),
                    }
            try:
                if ids:
                    cur.execute(
                        f"""
                        SELECT equipment_asset_id, COUNT(*) AS c
                        FROM asset_equipment_issues
                        WHERE equipment_asset_id IN ({ph})
                          AND status NOT IN ('resolved','closed')
                        GROUP BY equipment_asset_id
                        """,
                        tuple(ids),
                    )
                    for ir in cur.fetchall() or []:
                        issue_counts[int(ir["equipment_asset_id"])] = int(ir["c"] or 0)
            except Exception:
                logger.exception("list_equipment_desk_rows issue counts")
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        conn2 = self._conn()
        cur2 = conn2.cursor(dictionary=True)
        last_svc: Dict[int, Any] = {}
        try:
            if ids:
                ph = ",".join(["%s"] * len(ids))
                cur2.execute(
                    f"""
                    SELECT equipment_asset_id, MAX(service_date) AS d
                    FROM asset_maintenance_events
                    WHERE equipment_asset_id IN ({ph})
                    GROUP BY equipment_asset_id
                    """,
                    tuple(ids),
                )
                for s in cur2.fetchall() or []:
                    last_svc[int(s["equipment_asset_id"])] = s.get("d")
        finally:
            try:
                cur2.close()
            except Exception:
                pass
            try:
                conn2.close()
            except Exception:
                pass

        out: List[Dict[str, Any]] = []
        for r in rows:
            eid = int(r["id"])
            h = holders.get(eid)
            r = dict(r)
            r["open_issues"] = issue_counts.get(eid, 0)
            r["last_service_date"] = last_svc.get(eid)
            if h and str(r.get("status") or "") in ("assigned", "loaned"):
                r["holder_type"] = h["holder_type"]
                r["holder_id"] = h["holder_id"]
                r["holder_label"] = h["holder_label"]
                r["holder_kind"] = self._holder_type_label(str(h["holder_type"]))
            else:
                r["holder_type"] = None
                r["holder_id"] = None
                r["holder_label"] = None
                r["holder_kind"] = "—"
            # Defaults if columns missing before migrate
            if r.get("operational_state") is None:
                r["operational_state"] = "operational"
            r["status_label"] = equipment_asset_status_label(r.get("status"))
            out.append(r)
        return out

    def list_equipment_issues(
        self, equipment_asset_id: int, *, include_closed: bool = True, limit: int = 100
    ) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            sql = """
            SELECT * FROM asset_equipment_issues
            WHERE equipment_asset_id = %s
            """
            params: List[Any] = [int(equipment_asset_id)]
            if not include_closed:
                sql += " AND status NOT IN ('resolved','closed')"
            sql += " ORDER BY created_at DESC, id DESC LIMIT %s"
            params.append(min(max(limit, 1), 300))
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
        except Exception:
            logger.exception("list_equipment_issues")
            return []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def add_equipment_issue(
        self,
        *,
        equipment_asset_id: int,
        title: str,
        description: Optional[str] = None,
        severity: str = "medium",
        status: str = "open",
        scheduled_action_date: Optional[str] = None,
        external_reference: Optional[str] = None,
        reported_by_user_id: Optional[str] = None,
    ) -> int:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO asset_equipment_issues
                    (equipment_asset_id, reported_by_user_id, title, description,
                     severity, status, scheduled_action_date, external_reference)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(equipment_asset_id),
                    reported_by_user_id,
                    title.strip(),
                    (description or "").strip() or None,
                    severity if severity in ("info","low","medium","high","critical") else "medium",
                    status
                    if status
                    in (
                        "open",
                        "monitoring",
                        "fix_planned",
                        "off_service",
                        "sent_external",
                        "resolved",
                        "closed",
                    )
                    else "open",
                    scheduled_action_date or None,
                    (external_reference or "").strip() or None,
                ),
            )
            conn.commit()
            new_id = int(cur.lastrowid)
            self.sync_operational_state_from_issues(int(equipment_asset_id))
            return new_id
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def update_equipment_issue(
        self,
        issue_id: int,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        severity: Optional[str] = None,
        status: Optional[str] = None,
        scheduled_action_date: Optional[str] = None,
        external_reference: Optional[str] = None,
        resolution_notes: Optional[str] = None,
    ) -> None:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT equipment_asset_id FROM asset_equipment_issues WHERE id = %s",
                (int(issue_id),),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("Issue not found")
            eid = int(row["equipment_asset_id"])
        finally:
            try:
                cur.close()
            except Exception:
                pass
        cur = conn.cursor()
        try:
            updates: List[str] = []
            params: List[Any] = []
            if title is not None:
                updates.append("title = %s")
                params.append(title.strip())
            if description is not None:
                updates.append("description = %s")
                params.append((description or "").strip() or None)
            if severity is not None:
                updates.append("severity = %s")
                params.append(severity)
            if status is not None:
                updates.append("status = %s")
                params.append(status)
                if status in ("resolved", "closed"):
                    updates.append("resolved_at = NOW()")
                else:
                    updates.append("resolved_at = NULL")
            if scheduled_action_date is not None:
                updates.append("scheduled_action_date = %s")
                params.append(scheduled_action_date or None)
            if external_reference is not None:
                updates.append("external_reference = %s")
                params.append((external_reference or "").strip() or None)
            if resolution_notes is not None:
                updates.append("resolution_notes = %s")
                params.append((resolution_notes or "").strip() or None)
            if not updates:
                return
            params.append(int(issue_id))
            cur.execute(
                f"UPDATE asset_equipment_issues SET {', '.join(updates)} WHERE id = %s",
                tuple(params),
            )
            conn.commit()
            self.sync_operational_state_from_issues(eid)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def sync_operational_state_from_issues(self, equipment_asset_id: int) -> None:
        """Derive operational_state on the asset from open issue statuses."""
        inv = get_inventory_service()
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT status FROM asset_equipment_issues
                WHERE equipment_asset_id = %s AND status NOT IN ('resolved','closed')
                """,
                (int(equipment_asset_id),),
            )
            st = [str(r[0]) for r in cur.fetchall()]
        except Exception:
            return
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
        state = "operational"
        if any(x in ("off_service", "sent_external") for x in st):
            state = "unserviceable"
        elif st:
            state = "restricted"
        try:
            inv.update_equipment_asset(
                int(equipment_asset_id), operational_state=state
            )
        except Exception:
            logger.exception("sync_operational_state_from_issues")

    def get_last_maintenance_date(self, equipment_asset_id: int) -> Optional[Any]:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT MAX(service_date) FROM asset_maintenance_events
                WHERE equipment_asset_id = %s
                """,
                (int(equipment_asset_id),),
            )
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def dashboard_metrics(self) -> Dict[str, Any]:
        """Counts assigned assets and maintenance due (approximate)."""
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT status, COUNT(*) AS c FROM inventory_equipment_assets GROUP BY status
                """
            )
            by_status = {r["status"]: r["c"] for r in (cur.fetchall() or [])}
            open_issues = 0
            try:
                cur.execute(
                    """
                    SELECT COUNT(*) AS c FROM asset_equipment_issues
                    WHERE status NOT IN ('resolved','closed')
                    """
                )
                open_issues = int((cur.fetchone() or {}).get("c") or 0)
            except Exception:
                pass
            return {
                "by_status": by_status,
                "total": sum(by_status.values()),
                "open_equipment_issues": open_issues,
            }
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_maintenance_due(
        self, *, within_days: int = 30
    ) -> List[Dict[str, Any]]:
        """Equipment due for service: uses explicit next_service_due_date when set, else interval from last service."""
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT ea.id, ea.serial_number, ea.public_asset_code, ea.service_interval_days,
                       ea.next_service_due_date, ea.purchase_date,
                       ea.status, i.name AS item_name, i.sku,
                       (SELECT MAX(m.service_date) FROM asset_maintenance_events m
                        WHERE m.equipment_asset_id = ea.id) AS last_service_date
                FROM inventory_equipment_assets ea
                INNER JOIN inventory_items i ON i.id = ea.item_id
                WHERE ea.status NOT IN ('retired','lost')
                  AND (
                    ea.next_service_due_date IS NOT NULL
                    OR (ea.service_interval_days IS NOT NULL AND ea.service_interval_days > 0)
                  )
                """
            )
            rows = cur.fetchall() or []
            today = date.today()
            end = today + timedelta(days=within_days)
            out: List[Dict[str, Any]] = []

            def _to_date(val: Any) -> Optional[date]:
                if val is None:
                    return None
                try:
                    if isinstance(val, datetime):
                        return val.date()
                    if isinstance(val, date):
                        return val
                    return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
                except Exception:
                    return None

            for r in rows:
                due: Optional[date] = None
                nsd_raw = r.get("next_service_due_date")
                if nsd_raw:
                    due = _to_date(nsd_raw)
                if due is None:
                    interval = int(r["service_interval_days"] or 0)
                    if interval <= 0:
                        continue
                    last_s = r.get("last_service_date")
                    if last_s:
                        ld = _to_date(last_s)
                        if ld:
                            due = ld + timedelta(days=interval)
                        else:
                            due = today + timedelta(days=interval)
                    else:
                        pd = r.get("purchase_date")
                        pdd = _to_date(pd) if pd else None
                        if pdd:
                            due = pdd + timedelta(days=interval)
                        else:
                            due = today + timedelta(days=interval)
                if due is not None and due <= end:
                    r["next_due_date"] = due.isoformat()
                    r["overdue"] = due < today
                    out.append(r)
            out.sort(key=lambda x: x.get("next_due_date") or "")
            return out
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
