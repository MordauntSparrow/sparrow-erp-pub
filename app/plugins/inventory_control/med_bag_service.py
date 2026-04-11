"""
Med / response kit bag templates, instances, EPCR-linked ledger (INV-MEDS-001–003,
INV-KIT-002, CURA-DRUG-INV-001).
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from mysql.connector import errors as mysql_errors

from app.objects import get_db_connection

logger = logging.getLogger(__name__)

BAG_KINDS = (
    "general",
    "response",
    "burns",
    "oxygen",
    "entonox",
    "trauma",
    "other",
)
INSTANCE_STATUSES = ("in_store", "issued", "returned", "retired")
RETURN_STATUSES = ("present", "missing", "to_add")

# Default tamper tag colours (pull-off seals) — store lowercase in DB.
TAMPER_TAG_COLOURS = ("green", "orange", "red")
TAMPER_TAG_COLOUR_LABELS = {
    "green": "Green",
    "orange": "Orange",
    "red": "Red",
}


def _d(v: Any) -> Decimal:
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _norm_seal(v: Optional[str]) -> str:
    """Normalise seal id/colour for comparison (trim + case-insensitive)."""
    return (v or "").strip().casefold()


def coerce_tamper_tag_colour(raw: Optional[str]) -> str:
    """Return canonical green / orange / red, or raise ValueError."""
    c = _norm_seal(raw)
    if c in TAMPER_TAG_COLOURS:
        return c
    raise ValueError("Seal colour must be Green, Orange, or Red.")


class MedBagService:
    def _conn(self):
        return get_db_connection()

    # --- templates ---
    def list_templates(self, *, active_only: bool = False) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            sql = "SELECT * FROM inventory_med_bag_templates WHERE 1=1"
            params: Tuple[Any, ...] = ()
            if active_only:
                sql += " AND is_active = 1"
            sql += " ORDER BY name ASC"
            cur.execute(sql, params)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    def get_template(self, template_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM inventory_med_bag_templates WHERE id = %s",
                (int(template_id),),
            )
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    def create_template(
        self,
        *,
        name: str,
        code: str,
        bag_kind: str = "general",
        description: Optional[str] = None,
    ) -> int:
        nk = (bag_kind or "general").strip().lower()
        if nk not in BAG_KINDS:
            nk = "other"
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO inventory_med_bag_templates
                  (name, code, bag_kind, description, is_active)
                VALUES (%s, %s, %s, %s, 1)
                """,
                ((name or "").strip()[:255], (code or "").strip()[:64], nk, description),
            )
            conn.commit()
            return int(cur.lastrowid)
        except mysql_errors.IntegrityError as e:
            conn.rollback()
            if "uniq_med_bag_tpl_code" in str(e) or "Duplicate entry" in str(e):
                raise ValueError("A template with this code already exists.") from e
            raise
        finally:
            cur.close()
            conn.close()

    def update_template(
        self,
        template_id: int,
        *,
        name: Optional[str] = None,
        bag_kind: Optional[str] = None,
        description: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> bool:
        fields = []
        vals: List[Any] = []
        if name is not None:
            fields.append("name = %s")
            vals.append(name.strip()[:255])
        if bag_kind is not None:
            nk = bag_kind.strip().lower()
            if nk not in BAG_KINDS:
                nk = "other"
            fields.append("bag_kind = %s")
            vals.append(nk)
        if description is not None:
            fields.append("description = %s")
            vals.append(description)
        if is_active is not None:
            fields.append("is_active = %s")
            vals.append(1 if is_active else 0)
        if not fields:
            return True
        vals.append(int(template_id))
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                f"UPDATE inventory_med_bag_templates SET {', '.join(fields)} WHERE id = %s",
                tuple(vals),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    def list_template_lines(self, template_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT l.*, i.sku, i.name AS item_name
                FROM inventory_med_bag_template_lines l
                INNER JOIN inventory_items i ON i.id = l.inventory_item_id
                WHERE l.template_id = %s
                ORDER BY l.sort_order ASC, l.id ASC
                """,
                (int(template_id),),
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    def add_template_line(
        self,
        template_id: int,
        *,
        inventory_item_id: int,
        expected_qty: float = 1.0,
        sort_order: int = 0,
        notes: Optional[str] = None,
    ) -> int:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT id FROM inventory_med_bag_template_lines
                WHERE template_id = %s AND inventory_item_id = %s LIMIT 1
                """,
                (int(template_id), int(inventory_item_id)),
            )
            if cur.fetchone():
                raise ValueError("This catalogue item is already on the template BOM.")
            cur2 = conn.cursor()
            try:
                cur2.execute(
                    """
                    INSERT INTO inventory_med_bag_template_lines
                      (template_id, inventory_item_id, expected_qty, sort_order, notes)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        int(template_id),
                        int(inventory_item_id),
                        float(expected_qty),
                        int(sort_order),
                        (notes or "")[:512] or None,
                    ),
                )
                conn.commit()
                return int(cur2.lastrowid)
            except Exception:
                conn.rollback()
                raise
            finally:
                cur2.close()
        finally:
            cur.close()
            conn.close()

    def delete_template_line(self, line_id: int) -> bool:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM inventory_med_bag_template_lines WHERE id = %s",
                (int(line_id),),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    def _would_create_parent_cycle(
        self, child_id: int, proposed_parent_id: int, *, cur
    ) -> bool:
        """True if proposed_parent_id is child_id or an ancestor of child_id (would create a loop)."""
        cur_id = int(proposed_parent_id)
        for _ in range(512):
            if cur_id == int(child_id):
                return True
            cur.execute(
                "SELECT parent_instance_id FROM inventory_med_bag_instances WHERE id = %s",
                (cur_id,),
            )
            row = cur.fetchone()
            if not row:
                return False
            pid = row[0] if not isinstance(row, dict) else row.get("parent_instance_id")
            if pid is None:
                return False
            cur_id = int(pid)
        return True

    def _validate_parent_instance_id(
        self, parent_instance_id: Optional[int], *, cur=None
    ) -> None:
        if parent_instance_id is None:
            return
        pid = int(parent_instance_id)
        if pid <= 0:
            raise ValueError("Invalid parent bag.")
        if cur is not None:
            cur.execute(
                "SELECT id FROM inventory_med_bag_instances WHERE id = %s LIMIT 1",
                (pid,),
            )
            if not cur.fetchone():
                raise ValueError("Parent bag instance was not found.")
            return
        conn = self._conn()
        c = conn.cursor()
        try:
            c.execute(
                "SELECT id FROM inventory_med_bag_instances WHERE id = %s LIMIT 1",
                (pid,),
            )
            if not c.fetchone():
                raise ValueError("Parent bag instance was not found.")
        finally:
            c.close()
            conn.close()

    # --- instances ---
    def create_instance_from_template(
        self,
        template_id: int,
        *,
        public_asset_number: Optional[str] = None,
        parent_instance_id: Optional[int] = None,
    ) -> int:
        tpl = self.get_template(template_id)
        if not tpl:
            raise ValueError("Template not found")
        lines = self.list_template_lines(int(template_id))
        if not lines:
            raise ValueError("Template has no lines — add BOM lines first")
        pid = None
        if parent_instance_id is not None and int(parent_instance_id) > 0:
            pid = int(parent_instance_id)
        conn = self._conn()
        cur = conn.cursor()
        try:
            if pid is not None:
                self._validate_parent_instance_id(pid, cur=cur)

            cur.execute(
                """
                INSERT INTO inventory_med_bag_instances
                  (template_id, public_asset_number, status, notes, parent_instance_id)
                VALUES (%s, %s, 'in_store', NULL, %s)
                """,
                (
                    int(template_id),
                    (public_asset_number or "").strip()[:64] or None,
                    pid,
                ),
            )
            iid = int(cur.lastrowid)
            for row in lines:
                eq = _d(row.get("expected_qty") or 1)
                cur.execute(
                    """
                    INSERT INTO inventory_med_bag_instance_lines
                      (instance_id, inventory_item_id, quantity_expected,
                       quantity_on_bag, batch_id, lot_number, expiry_date, return_status)
                    VALUES (%s, %s, %s, %s, NULL, NULL, NULL, NULL)
                    """,
                    (iid, int(row["inventory_item_id"]), float(eq), float(eq)),
                )
            conn.commit()
            return iid
        except mysql_errors.IntegrityError as e:
            conn.rollback()
            if "uniq_med_bag_pub" in str(e) or "Duplicate entry" in str(e):
                raise ValueError(
                    "That public asset number is already assigned to another bag."
                ) from e
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def list_instances(
        self,
        *,
        status: Optional[str] = None,
        limit: int = 200,
        top_level_only: bool = False,
    ) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            sql = """
            SELECT b.*, t.name AS template_name, t.code AS template_code, t.bag_kind,
                   p.public_asset_number AS parent_public_asset_number,
                   pt.code AS parent_template_code
            FROM inventory_med_bag_instances b
            INNER JOIN inventory_med_bag_templates t ON t.id = b.template_id
            LEFT JOIN inventory_med_bag_instances p ON p.id = b.parent_instance_id
            LEFT JOIN inventory_med_bag_templates pt ON pt.id = p.template_id
            WHERE 1=1
            """
            params: List[Any] = []
            if status:
                sql += " AND b.status = %s"
                params.append(status)
            if top_level_only:
                sql += " AND b.parent_instance_id IS NULL"
            sql += " ORDER BY b.id DESC LIMIT %s"
            params.append(min(max(int(limit), 1), 500))
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    def list_child_instances(self, parent_instance_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT b.*, t.name AS template_name, t.code AS template_code, t.bag_kind
                FROM inventory_med_bag_instances b
                INNER JOIN inventory_med_bag_templates t ON t.id = b.template_id
                WHERE b.parent_instance_id = %s
                ORDER BY b.id ASC
                """,
                (int(parent_instance_id),),
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    def set_instance_parent(
        self, instance_id: int, parent_instance_id: Optional[int]
    ) -> bool:
        """Attach or detach a nested bag/pod (e.g. airway pod) to a parent response bag."""
        iid = int(instance_id)
        new_parent: Optional[int]
        if parent_instance_id is None or int(parent_instance_id) <= 0:
            new_parent = None
        else:
            new_parent = int(parent_instance_id)
        if new_parent == iid:
            raise ValueError("A bag cannot be its own parent.")
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id FROM inventory_med_bag_instances WHERE id = %s LIMIT 1",
                (iid,),
            )
            if not cur.fetchone():
                raise ValueError("Instance not found.")
            if new_parent is not None:
                self._validate_parent_instance_id(new_parent, cur=cur)
                if self._would_create_parent_cycle(iid, new_parent, cur=cur):
                    raise ValueError(
                        "That parent is inside this bag — cannot create a circular containment."
                    )
            cur.execute(
                """
                UPDATE inventory_med_bag_instances
                SET parent_instance_id = %s
                WHERE id = %s
                """,
                (new_parent, iid),
            )
            conn.commit()
            return cur.rowcount > 0
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def get_instance(self, instance_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT b.*, t.name AS template_name, t.code AS template_code, t.bag_kind,
                       p.public_asset_number AS parent_public_asset_number,
                       pt.code AS parent_template_code,
                       pt.name AS parent_template_name
                FROM inventory_med_bag_instances b
                INNER JOIN inventory_med_bag_templates t ON t.id = b.template_id
                LEFT JOIN inventory_med_bag_instances p ON p.id = b.parent_instance_id
                LEFT JOIN inventory_med_bag_templates pt ON pt.id = p.template_id
                WHERE b.id = %s
                """,
                (int(instance_id),),
            )
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    def list_instance_lines(self, instance_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT l.*, i.sku, i.name AS item_name
                FROM inventory_med_bag_instance_lines l
                INNER JOIN inventory_items i ON i.id = l.inventory_item_id
                WHERE l.instance_id = %s
                ORDER BY l.id ASC
                """,
                (int(instance_id),),
            )
            rows = cur.fetchall() or []
            for r in rows:
                ed = r.get("expiry_date")
                if hasattr(ed, "isoformat"):
                    r["expiry_date"] = ed.isoformat()
            return rows
        finally:
            cur.close()
            conn.close()

    def list_ledger(self, instance_id: int, *, limit: int = 100) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT l.*, i.sku AS item_sku, i.name AS item_name
                FROM inventory_med_bag_ledger l
                LEFT JOIN inventory_items i ON i.id = l.inventory_item_id
                WHERE l.instance_id = %s
                ORDER BY l.id DESC
                LIMIT %s
                """,
                (int(instance_id), min(max(int(limit), 1), 500)),
            )
            rows = cur.fetchall() or []
            for r in rows:
                ca = r.get("created_at")
                if hasattr(ca, "isoformat"):
                    r["created_at"] = ca.isoformat()
            return rows
        finally:
            cur.close()
            conn.close()

    def instance_line_on_instance(self, line_id: int, instance_id: int) -> bool:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT 1 FROM inventory_med_bag_instance_lines
                WHERE id = %s AND instance_id = %s LIMIT 1
                """,
                (int(line_id), int(instance_id)),
            )
            return cur.fetchone() is not None
        finally:
            cur.close()
            conn.close()

    def get_witness_rule(self, action_type: str) -> Dict[str, Any]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM inventory_med_witness_rules WHERE action_type = %s",
                (action_type,),
            )
            row = cur.fetchone()
            if row:
                return row
            return {"action_type": action_type, "witness_count": 0, "enabled": 1}
        finally:
            cur.close()
            conn.close()

    def list_witness_rules(self) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM inventory_med_witness_rules ORDER BY action_type ASC")
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    def update_witness_rule(
        self, action_type: str, *, witness_count: int, enabled: bool
    ) -> None:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO inventory_med_witness_rules (action_type, witness_count, enabled)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE witness_count = VALUES(witness_count),
                  enabled = VALUES(enabled)
                """,
                (action_type, max(0, min(int(witness_count), 2)), 1 if enabled else 0),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def search_lot(self, q: str, *, limit: int = 50) -> List[Dict[str, Any]]:
        term = (q or "").strip()
        if len(term) < 2:
            return []
        like = f"%{term}%"
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT l.id AS instance_line_id, l.instance_id, l.lot_number, l.quantity_on_bag,
                       l.inventory_item_id, i.sku, i.name AS item_name,
                       b.public_asset_number, b.status
                FROM inventory_med_bag_instance_lines l
                INNER JOIN inventory_med_bag_instances b ON b.id = l.instance_id
                INNER JOIN inventory_items i ON i.id = l.inventory_item_id
                WHERE l.lot_number LIKE %s
                ORDER BY l.instance_id DESC, l.id ASC
                LIMIT %s
                """,
                (like, min(max(int(limit), 1), 200)),
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    def update_instance_line_trace(
        self,
        line_id: int,
        *,
        lot_number: Optional[str] = None,
        expiry_date: Optional[str] = None,
        batch_id: Optional[int] = None,
    ) -> bool:
        fields = []
        vals: List[Any] = []
        if lot_number is not None:
            fields.append("lot_number = %s")
            vals.append(lot_number.strip()[:128] or None)
        if expiry_date is not None:
            es = (expiry_date or "").strip()[:10] or None
            if es:
                try:
                    datetime.strptime(es, "%Y-%m-%d")
                except ValueError as e:
                    raise ValueError("Expiry must be a valid date (YYYY-MM-DD).") from e
            fields.append("expiry_date = %s")
            vals.append(es)
        if batch_id is not None:
            fields.append("batch_id = %s")
            vals.append(batch_id)
        if not fields:
            return True
        vals.append(int(line_id))
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                f"UPDATE inventory_med_bag_instance_lines SET {', '.join(fields)} WHERE id = %s",
                tuple(vals),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    def _update_instance_status_row(
        self,
        cur,
        instance_id: int,
        status: str,
        assignee_type: Optional[str],
        assignee_id: Optional[str],
        equipment_asset_id: Optional[int],
    ) -> None:
        cur.execute(
            """
            UPDATE inventory_med_bag_instances
            SET status = %s,
                assignee_type = %s,
                assignee_id = %s,
                equipment_asset_id = %s
            WHERE id = %s
            """,
            (
                status,
                (assignee_type or None),
                (assignee_id or None)[:128] if assignee_id else None,
                equipment_asset_id,
                int(instance_id),
            ),
        )

    def _insert_seal_audit_row(
        self,
        cur,
        instance_id: int,
        event_type: str,
        *,
        expected_seal_id: Optional[str] = None,
        expected_colour: Optional[str] = None,
        expected_initial: Optional[str] = None,
        entered_seal_id: Optional[str] = None,
        entered_colour: Optional[str] = None,
        entered_initial: Optional[str] = None,
        performed_by: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> None:
        cur.execute(
            """
            INSERT INTO inventory_med_bag_seal_events
              (instance_id, event_type, expected_seal_id, expected_colour,
               expected_initial, entered_seal_id, entered_colour, entered_initial,
               performed_by, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(instance_id),
                event_type[:32],
                (expected_seal_id or None)[:128] if expected_seal_id else None,
                (expected_colour or None)[:16] if expected_colour else None,
                (expected_initial or None)[:16] if expected_initial else None,
                (entered_seal_id or None)[:128] if entered_seal_id else None,
                (entered_colour or None)[:16] if entered_colour else None,
                (entered_initial or None)[:16] if entered_initial else None,
                (performed_by or None)[:128] if performed_by else None,
                (notes or None)[:512] if notes else None,
            ),
        )

    def log_bag_custody_event(
        self,
        instance_id: int,
        event_type: str,
        *,
        performed_by: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> None:
        """Audit drug-bag sign-out / sign-in (custody), separate from seal verification rows."""
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO inventory_med_bag_seal_events
                  (instance_id, event_type, performed_by, notes)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    int(instance_id),
                    event_type[:32],
                    (performed_by or None)[:128] if performed_by else None,
                    (notes or None)[:512] if notes else None,
                ),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def list_seal_events(self, instance_id: int, *, limit: int = 80) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT * FROM inventory_med_bag_seal_events
                WHERE instance_id = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (int(instance_id), min(max(int(limit), 1), 300)),
            )
            rows = cur.fetchall() or []
            for r in rows:
                ca = r.get("created_at")
                if hasattr(ca, "isoformat"):
                    r["created_at"] = ca.isoformat()
            return rows
        finally:
            cur.close()
            conn.close()

    def register_tamper_seal(
        self,
        instance_id: int,
        *,
        seal_id: str,
        colour: str,
        performed_by: Optional[str],
        initial: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> None:
        """Record or update the registered pull-off tamper seal (ID + G/O/R tag + optional initials)."""
        sid = (seal_id or "").strip()[:128]
        try:
            col = coerce_tamper_tag_colour(colour)
        except ValueError as e:
            raise ValueError(str(e)) from e
        ini = (initial or "").strip()[:16] or None
        if not sid:
            raise ValueError("Tamper seal ID is required.")
        conn = self._conn()
        prev_ac = getattr(conn, "autocommit", True)
        if hasattr(conn, "autocommit"):
            conn.autocommit = False
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT tamper_seal_id FROM inventory_med_bag_instances
                WHERE id = %s FOR UPDATE
                """,
                (int(instance_id),),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                raise ValueError("Instance not found.")
            prev = (row.get("tamper_seal_id") or "").strip()
            evt = "tamper_seal_updated" if prev else "tamper_seal_registered"
            perf = (performed_by or None)[:128] if performed_by else None
            cur.execute(
                """
                UPDATE inventory_med_bag_instances
                SET tamper_seal_id = %s,
                    tamper_seal_colour = %s,
                    tamper_seal_initial = %s,
                    tamper_seal_set_at = CURRENT_TIMESTAMP,
                    tamper_seal_set_by = %s
                WHERE id = %s
                """,
                (sid, col, ini, perf, int(instance_id)),
            )
            self._insert_seal_audit_row(
                cur,
                int(instance_id),
                evt,
                expected_seal_id=sid,
                expected_colour=col,
                expected_initial=ini,
                performed_by=perf,
                notes=notes,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            if hasattr(conn, "autocommit"):
                conn.autocommit = prev_ac
            conn.close()

    def apply_instance_status_change(
        self,
        instance_id: int,
        status: str,
        *,
        assignee_type: Optional[str] = None,
        assignee_id: Optional[str] = None,
        equipment_asset_id: Optional[int] = None,
        tamper_verify_id: Optional[str] = None,
        tamper_verify_colour: Optional[str] = None,
        tamper_verify_initial: Optional[str] = None,
        performed_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update bag status. When a tamper seal is registered (ID + tag colour), issuing or returning
        requires matching verification. If optional initials were registered, they must match too.
        """
        st = (status or "").strip().lower()
        if st not in INSTANCE_STATUSES:
            raise ValueError("Invalid status")
        conn = self._conn()
        prev_ac = getattr(conn, "autocommit", True)
        if hasattr(conn, "autocommit"):
            conn.autocommit = False
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT id, tamper_seal_id, tamper_seal_colour, tamper_seal_initial
                FROM inventory_med_bag_instances
                WHERE id = %s FOR UPDATE
                """,
                (int(instance_id),),
            )
            inst = cur.fetchone()
            if not inst:
                conn.rollback()
                return {"ok": False, "error": "instance_not_found"}

            exp_id = (inst.get("tamper_seal_id") or "").strip()
            exp_col = (inst.get("tamper_seal_colour") or "").strip()
            exp_inl = (inst.get("tamper_seal_initial") or "").strip()
            sealed = bool(exp_id and exp_col)
            verify_statuses = ("issued", "returned")

            if st in verify_statuses and sealed:
                ent_id = (tamper_verify_id or "").strip()
                ent_col_raw = (tamper_verify_colour or "").strip()
                try:
                    ent_col = coerce_tamper_tag_colour(ent_col_raw) if ent_col_raw else ""
                except ValueError:
                    conn.rollback()
                    return {"ok": False, "error": "tamper_invalid_colour"}
                if not ent_id or not ent_col:
                    conn.rollback()
                    return {"ok": False, "error": "tamper_verification_required"}
                ent_inl = (tamper_verify_initial or "").strip()
                if exp_inl and not ent_inl:
                    conn.rollback()
                    return {"ok": False, "error": "tamper_initial_required"}
                match = (
                    _norm_seal(ent_id) == _norm_seal(exp_id)
                    and _norm_seal(ent_col) == _norm_seal(exp_col)
                )
                if exp_inl:
                    match = match and (_norm_seal(ent_inl) == _norm_seal(exp_inl))
                perf = (performed_by or None)[:128] if performed_by else None
                if not match:
                    bad = "tamper_mismatch_issue" if st == "issued" else "tamper_mismatch_return"
                    self._insert_seal_audit_row(
                        cur,
                        int(instance_id),
                        bad,
                        expected_seal_id=exp_id,
                        expected_colour=exp_col,
                        expected_initial=exp_inl or None,
                        entered_seal_id=ent_id,
                        entered_colour=ent_col,
                        entered_initial=ent_inl or None,
                        performed_by=perf,
                        notes="Blocked status change — seal does not match registered values.",
                    )
                    conn.commit()
                    return {"ok": False, "error": "tamper_seal_mismatch"}
                good = "tamper_verified_issue" if st == "issued" else "tamper_verified_return"
                self._insert_seal_audit_row(
                    cur,
                    int(instance_id),
                    good,
                    expected_seal_id=exp_id,
                    expected_colour=exp_col,
                    expected_initial=exp_inl or None,
                    entered_seal_id=ent_id,
                    entered_colour=ent_col,
                    entered_initial=ent_inl or None,
                    performed_by=perf,
                    notes=None,
                )

            self._update_instance_status_row(
                cur,
                int(instance_id),
                st,
                assignee_type,
                assignee_id,
                equipment_asset_id,
            )
            conn.commit()
            return {"ok": True, "rows": cur.rowcount}
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            if hasattr(conn, "autocommit"):
                conn.autocommit = prev_ac
            conn.close()

    def set_instance_line_return_status(self, line_id: int, status: Optional[str]) -> bool:
        st = (status or "").strip().lower()
        if st not in RETURN_STATUSES and st != "" and st != "clear":
            raise ValueError("Invalid return status")
        val = None if st in ("", "clear") else st
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE inventory_med_bag_instance_lines SET return_status = %s WHERE id = %s",
                (val, int(line_id)),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    def set_instance_status(
        self,
        instance_id: int,
        status: str,
        *,
        assignee_type: Optional[str] = None,
        assignee_id: Optional[str] = None,
        equipment_asset_id: Optional[int] = None,
    ) -> bool:
        """Direct status update (no tamper check). Prefer apply_instance_status_change from UI/API."""
        st = (status or "").strip().lower()
        if st not in INSTANCE_STATUSES:
            raise ValueError("Invalid status")
        conn = self._conn()
        cur = conn.cursor()
        try:
            self._update_instance_status_row(
                cur, int(instance_id), st, assignee_type, assignee_id, equipment_asset_id
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    def record_epcr_consumption(
        self,
        *,
        instance_id: int,
        inventory_item_id: int,
        quantity: float,
        epcr_external_ref: Optional[str],
        epcr_episode_ref: Optional[str],
        idempotency_key: Optional[str],
        witness_user_id: Optional[str],
        witness_user_id_2: Optional[str],
        performed_by: Optional[str],
        lot_number: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        """CURA-DRUG-INV-001: idempotent consumption row against a bag instance."""
        rule = self.get_witness_rule("epcr_consumption")
        need = int(rule.get("witness_count") or 0) if int(rule.get("enabled") or 0) else 0
        if need >= 1 and not (witness_user_id or "").strip():
            return {"ok": False, "error": "witness_required", "witness_count": need}
        if need >= 2 and not (witness_user_id_2 or "").strip():
            return {"ok": False, "error": "second_witness_required", "witness_count": need}

        qty = _d(quantity)
        if qty <= 0:
            return {"ok": False, "error": "quantity_must_be_positive"}

        conn = self._conn()
        prev_ac = getattr(conn, "autocommit", True)
        try:
            if hasattr(conn, "autocommit"):
                conn.autocommit = False
            cur = conn.cursor(dictionary=True)
            try:
                idem = (idempotency_key or "").strip()[:128] or None

                cur.execute(
                    "SELECT id FROM inventory_med_bag_instances WHERE id = %s FOR UPDATE",
                    (int(instance_id),),
                )
                if not cur.fetchone():
                    conn.rollback()
                    return {"ok": False, "error": "instance_not_found"}

                if idem:
                    cur.execute(
                        "SELECT id FROM inventory_med_bag_ledger WHERE idempotency_key = %s",
                        (idem,),
                    )
                    ex = cur.fetchone()
                    if ex:
                        conn.commit()
                        return {"ok": True, "duplicate": True, "ledger_id": ex["id"]}

                cur.execute(
                    "SELECT id, quantity_on_bag FROM inventory_med_bag_instance_lines WHERE instance_id = %s AND inventory_item_id = %s FOR UPDATE",
                    (int(instance_id), int(inventory_item_id)),
                )
                line = cur.fetchone()
                if not line:
                    conn.rollback()
                    return {"ok": False, "error": "item_not_on_bag"}

                on_bag = _d(line.get("quantity_on_bag"))
                if on_bag < qty:
                    conn.rollback()
                    return {
                        "ok": False,
                        "error": "insufficient_qty_on_bag",
                        "quantity_on_bag": float(on_bag),
                    }

                new_q = on_bag - qty
                cur.execute(
                    """
                    UPDATE inventory_med_bag_instance_lines
                    SET quantity_on_bag = %s
                    WHERE id = %s
                    """,
                    (float(new_q), int(line["id"])),
                )
                try:
                    cur.execute(
                        """
                        INSERT INTO inventory_med_bag_ledger
                          (instance_id, event_type, inventory_item_id, quantity_delta,
                           batch_id, epcr_external_ref, epcr_episode_ref, idempotency_key,
                           lot_number, witness_user_id, witness_user_id_2, performed_by, notes)
                        VALUES (%s, 'epcr_consumption', %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            int(instance_id),
                            int(inventory_item_id),
                            float(-qty),
                            (epcr_external_ref or None)[:128] if epcr_external_ref else None,
                            (epcr_episode_ref or None)[:128] if epcr_episode_ref else None,
                            idem,
                            (lot_number or None)[:128] if lot_number else None,
                            (witness_user_id or None)[:64] if witness_user_id else None,
                            (witness_user_id_2 or None)[:64] if witness_user_id_2 else None,
                            (performed_by or None)[:128] if performed_by else None,
                            (notes or None)[:2000] if notes else None,
                        ),
                    )
                except mysql_errors.IntegrityError as ie:
                    conn.rollback()
                    if idem and (
                        "uniq_med_bag_idem" in str(ie) or "Duplicate entry" in str(ie)
                    ):
                        cur2 = conn.cursor(dictionary=True)
                        try:
                            cur2.execute(
                                "SELECT id FROM inventory_med_bag_ledger WHERE idempotency_key = %s",
                                (idem,),
                            )
                            row = cur2.fetchone()
                            if row:
                                return {
                                    "ok": True,
                                    "duplicate": True,
                                    "ledger_id": row["id"],
                                }
                        finally:
                            cur2.close()
                    logger.warning("record_epcr_consumption idempotency race: %s", ie)
                    return {"ok": False, "error": "conflict_retry"}

                lid = int(cur.lastrowid)
                conn.commit()
                return {"ok": True, "ledger_id": lid, "quantity_on_bag_after": float(new_q)}
            except Exception as e:
                conn.rollback()
                logger.exception("record_epcr_consumption")
                return {"ok": False, "error": "internal_error"}
            finally:
                cur.close()
        finally:
            if hasattr(conn, "autocommit"):
                conn.autocommit = prev_ac
            conn.close()

    def record_restock_event(
        self,
        *,
        instance_id: int,
        inventory_item_id: int,
        quantity: float,
        event_type: str,
        witness_user_id: Optional[str],
        witness_user_id_2: Optional[str],
        performed_by: Optional[str],
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        if event_type not in ("restock_hq", "restock_bag", "disposal"):
            return {"ok": False, "error": "invalid_event_type"}
        rule = self.get_witness_rule(event_type)
        need = int(rule.get("witness_count") or 0) if int(rule.get("enabled") or 0) else 0
        if need >= 1 and not (witness_user_id or "").strip():
            return {"ok": False, "error": "witness_required"}
        if need >= 2 and not (witness_user_id_2 or "").strip():
            return {"ok": False, "error": "second_witness_required"}

        qty = _d(quantity)
        if qty <= 0:
            return {"ok": False, "error": "quantity_must_be_positive"}
        if event_type == "disposal":
            qty = -abs(qty)
        else:
            qty = abs(qty)

        conn = self._conn()
        prev_ac = getattr(conn, "autocommit", True)
        try:
            if hasattr(conn, "autocommit"):
                conn.autocommit = False
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute(
                    "SELECT id FROM inventory_med_bag_instances WHERE id = %s FOR UPDATE",
                    (int(instance_id),),
                )
                if not cur.fetchone():
                    conn.rollback()
                    return {"ok": False, "error": "instance_not_found"}

                cur.execute(
                    "SELECT id, quantity_on_bag FROM inventory_med_bag_instance_lines WHERE instance_id = %s AND inventory_item_id = %s FOR UPDATE",
                    (int(instance_id), int(inventory_item_id)),
                )
                line = cur.fetchone()
                if not line:
                    conn.rollback()
                    return {"ok": False, "error": "item_not_on_bag"}

                on_bag = _d(line.get("quantity_on_bag"))
                new_q = on_bag + qty
                if new_q < 0:
                    conn.rollback()
                    return {
                        "ok": False,
                        "error": "would_go_negative",
                        "quantity_on_bag": float(on_bag),
                    }

                cur.execute(
                    """
                    UPDATE inventory_med_bag_instance_lines
                    SET quantity_on_bag = %s
                    WHERE id = %s
                    """,
                    (float(new_q), int(line["id"])),
                )
                cur.execute(
                    """
                    INSERT INTO inventory_med_bag_ledger
                      (instance_id, event_type, inventory_item_id, quantity_delta,
                       batch_id, epcr_external_ref, epcr_episode_ref, idempotency_key,
                       lot_number, witness_user_id, witness_user_id_2, performed_by, notes)
                    VALUES (%s, %s, %s, %s, NULL, NULL, NULL, NULL, NULL, %s, %s, %s, %s)
                    """,
                    (
                        int(instance_id),
                        event_type,
                        int(inventory_item_id),
                        float(qty),
                        (witness_user_id or None)[:64] if witness_user_id else None,
                        (witness_user_id_2 or None)[:64] if witness_user_id_2 else None,
                        (performed_by or None)[:128] if performed_by else None,
                        (notes or None)[:2000] if notes else None,
                    ),
                )
                lid = int(cur.lastrowid)
                conn.commit()
                return {"ok": True, "ledger_id": lid, "quantity_on_bag_after": float(new_q)}
            except Exception:
                conn.rollback()
                logger.exception("record_restock_event")
                return {"ok": False, "error": "internal_error"}
            finally:
                cur.close()
        finally:
            if hasattr(conn, "autocommit"):
                conn.autocommit = prev_ac
            conn.close()


_svc: Optional[MedBagService] = None


def get_med_bag_service() -> MedBagService:
    global _svc
    if _svc is None:
        _svc = MedBagService()
    return _svc
