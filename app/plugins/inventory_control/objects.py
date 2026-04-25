import calendar
import json
import logging
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from app.objects import get_db_connection

logger = logging.getLogger(__name__)


def add_calendar_months(from_date: date, months: int) -> date:
    """Add calendar months to a date (day clamped to month end)."""
    if months <= 0:
        return from_date
    m0 = from_date.month - 1 + int(months)
    year = from_date.year + m0 // 12
    month = m0 % 12 + 1
    last = calendar.monthrange(year, month)[1]
    return date(year, month, min(from_date.day, last))


def normalize_warranty_start_basis(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, str) and not str(val).strip()):
        return None
    s = str(val).strip().lower()
    if s in ("delivery", "delivered", "receipt", "goods_in"):
        return "delivery"
    if s in ("install", "installation", "fitted", "fitment"):
        return "install"
    if s in ("custom", "manual", "other"):
        return "custom"
    return s[:24] if s else None


# DB ENUM: cost_method must be one of these
COST_METHOD_VALUES = ("FIFO", "LIFO", "AVG")


def _normalize_cost_method(value: Any) -> str:
    """Return a valid cost_method for the DB ENUM; default 'AVG'."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return "AVG"
    s = str(value).strip().upper()
    if s in COST_METHOD_VALUES:
        return s
    if s in ("AVERAGE", "AVERAGED"):
        return "AVG"
    return "AVG"


def _coerce_int_user_id(value: Any) -> Optional[int]:
    """
    inventory_transactions.performed_by_user_id is INT in current schema.
    The core `users.id` is often a UUID string, so coerce only if it looks numeric;
    otherwise return None (and callers can store the UUID in metadata instead).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    s = str(value).strip()
    if s.isdigit():
        try:
            return int(s)
        except Exception:
            return None
    return None


def _bucket_start(dt: datetime, bucket: str) -> datetime:
    if bucket == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    if bucket == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if bucket == "week":
        # Monday as week start
        d0 = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return d0 - timedelta(days=d0.weekday())
    if bucket == "month":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return dt


def _add_bucket(dt: datetime, bucket: str) -> datetime:
    if bucket == "hour":
        return dt + timedelta(hours=1)
    if bucket == "day":
        return dt + timedelta(days=1)
    if bucket == "week":
        return dt + timedelta(days=7)
    if bucket == "month":
        y = dt.year
        m = dt.month + 1
        if m == 13:
            y += 1
            m = 1
        return dt.replace(year=y, month=m, day=1)
    return dt


def _choose_bucket(range_days: int, tx_count: int) -> str:
    """
    Adaptive bucket selection so graphs are readable:
    - high activity / small window -> hourly
    - medium -> daily
    - long window -> weekly/monthly
    """
    rd = max(int(range_days or 30), 1)
    if rd <= 2:
        return "hour"
    if rd <= 14:
        return "hour" if tx_count >= 120 else "day"
    if rd <= 90:
        return "day"
    if rd <= 365:
        return "week"
    return "month"


def _alter_add_column(conn, table: str, col_def: str) -> None:
    """Idempotent add column; ignores duplicate column errors."""
    parts = col_def.strip().split(None, 1)
    col_name = parts[0]
    rest = parts[1] if len(parts) > 1 else ""
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{col_name}` {rest}")
        conn.commit()
    except Exception as e:
        if "Duplicate column" in str(e):
            return
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _ensure_inventory_equipment_schema(conn) -> None:
    """
    Ensure equipment/assignee columns exist (idempotent).
    Safe to call multiple times; avoids breaking requests on older databases.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS `inventory_equipment_assets` (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                item_id INT NOT NULL,
                serial_number VARCHAR(255) NOT NULL,
                status ENUM('in_stock','loaned','assigned','maintenance','retired','lost') NOT NULL DEFAULT 'in_stock',
                metadata JSON,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uniq_inventory_equipment_serial (serial_number),
                INDEX idx_inventory_equipment_item (item_id),
                INDEX idx_inventory_equipment_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass

    _alter_add_column(conn, "inventory_items", "is_equipment TINYINT(1) NOT NULL DEFAULT 0")
    _alter_add_column(conn, "inventory_items", "requires_serial TINYINT(1) NOT NULL DEFAULT 0")
    _alter_add_column(conn, "inventory_items", "category_id INT NULL")

    _alter_add_column(conn, "inventory_transactions", "assignee_type VARCHAR(16) NULL")
    _alter_add_column(conn, "inventory_transactions", "assignee_id VARCHAR(64) NULL")
    _alter_add_column(conn, "inventory_transactions", "assignee_label VARCHAR(255) NULL")
    _alter_add_column(conn, "inventory_transactions", "is_loan TINYINT(1) NOT NULL DEFAULT 0")
    _alter_add_column(conn, "inventory_transactions", "due_back_date DATE NULL")
    _alter_add_column(conn, "inventory_transactions", "equipment_asset_id BIGINT NULL")
    _alter_add_column(conn, "inventory_transactions", "weight DECIMAL(18, 6) NULL")
    _alter_add_column(conn, "inventory_transactions", "weight_uom VARCHAR(32) NULL")

    _alter_add_column(conn, "inventory_equipment_assets", "next_service_due_date DATE NULL")
    _alter_add_column(
        conn,
        "inventory_equipment_assets",
        "operational_state ENUM('operational','restricted','unserviceable') NOT NULL DEFAULT 'operational'",
    )
    _alter_add_column(conn, "inventory_equipment_assets", "make VARCHAR(120) NULL")
    _alter_add_column(conn, "inventory_equipment_assets", "model VARCHAR(120) NULL")
    _alter_add_column(conn, "inventory_equipment_assets", "purchase_date DATE NULL")
    _alter_add_column(conn, "inventory_equipment_assets", "warranty_expiry DATE NULL")
    _alter_add_column(conn, "inventory_equipment_assets", "service_interval_days INT NULL")
    _alter_add_column(conn, "inventory_equipment_assets", "condition VARCHAR(64) NULL")
    _alter_add_column(conn, "inventory_equipment_assets", "public_asset_code VARCHAR(64) NULL")
    _alter_add_column(conn, "inventory_equipment_assets", "end_of_life_at DATE NULL")
    _alter_add_column(
        conn,
        "inventory_equipment_assets",
        "warranty_start_basis VARCHAR(24) NULL COMMENT 'delivery|install|custom'",
    )
    _alter_add_column(conn, "inventory_equipment_assets", "warranty_start_date DATE NULL")
    _alter_add_column(conn, "inventory_equipment_assets", "warranty_months INT NULL")

    cur_cons = conn.cursor()
    try:
        cur_cons.execute(
            """
            CREATE TABLE IF NOT EXISTS `inventory_equipment_asset_consumables` (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                equipment_asset_id BIGINT NOT NULL,
                inventory_item_id INT NULL,
                label VARCHAR(255) NOT NULL,
                batch_number VARCHAR(128) NULL,
                lot_number VARCHAR(128) NULL,
                expiry_date DATE NULL,
                quantity DECIMAL(18, 4) NOT NULL DEFAULT 1,
                depleted TINYINT(1) NOT NULL DEFAULT 0,
                notes TEXT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_inv_equip_cons_asset (equipment_asset_id),
                INDEX idx_inv_equip_cons_expiry (expiry_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
    finally:
        try:
            cur_cons.close()
        except Exception:
            pass

    _alter_add_column(
        conn,
        "inventory_equipment_asset_consumables",
        "usage_close_reason VARCHAR(32) NULL",
    )
    _alter_add_column(
        conn,
        "inventory_equipment_asset_consumables",
        "discrepancy_flag TINYINT(1) NOT NULL DEFAULT 0",
    )
    _alter_add_column(
        conn,
        "inventory_equipment_asset_consumables",
        "discrepancy_details TEXT NULL",
    )
    _alter_add_column(
        conn,
        "inventory_equipment_asset_consumables",
        "discrepancy_reported_at DATETIME NULL",
    )
    _alter_add_column(
        conn,
        "inventory_equipment_asset_consumables",
        "discrepancy_reported_by_contractor_id INT NULL",
    )

    cur_ho = conn.cursor()
    try:
        cur_ho.execute(
            """
            CREATE TABLE IF NOT EXISTS `inventory_equipment_portal_handoffs` (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                equipment_asset_id BIGINT NOT NULL,
                contractor_id INT NOT NULL,
                handoff_kind ENUM('to_vehicle','to_storeroom') NOT NULL,
                vehicle_id INT NULL,
                inventory_location_id INT NOT NULL,
                status ENUM('pending','completed','cancelled') NOT NULL DEFAULT 'pending',
                initiated_by_user_id VARCHAR(64) NULL,
                notes TEXT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at DATETIME NULL,
                cancelled_at DATETIME NULL,
                INDEX idx_inv_handoff_contractor (contractor_id, status),
                INDEX idx_inv_handoff_asset (equipment_asset_id, status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
    finally:
        try:
            cur_ho.close()
        except Exception:
            pass


def _parse_sql_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = str(value).strip()[:10]
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        return None


def annotate_equipment_consumable_rows(
    rows: List[Dict[str, Any]],
    *,
    today: Optional[date] = None,
    near_days: int = 30,
) -> List[Dict[str, Any]]:
    """Add consumable_status / badge hints for UI (expiry vs depleted)."""
    t = today or date.today()
    nd = max(int(near_days or 30), 1)
    out: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        qty = float(rr.get("quantity") or 0)
        dep = bool(int(rr.get("depleted") or 0))
        ed = _parse_sql_date(rr.get("expiry_date"))
        if int(rr.get("discrepancy_flag") or 0):
            rr["has_discrepancy"] = True
        else:
            rr["has_discrepancy"] = False
        if dep or qty <= 0:
            rr["consumable_status"] = "used_up"
            ucr = (rr.get("usage_close_reason") or "").strip()
            if ucr == "wastage":
                rr["consumable_status_label"] = "Wastage / discarded"
            elif ucr == "damaged":
                rr["consumable_status_label"] = "Damaged / depleted"
            elif ucr == "expired_disposal":
                rr["consumable_status_label"] = "Expired disposal"
            elif ucr == "other":
                rr["consumable_status_label"] = "Closed (other)"
            else:
                rr["consumable_status_label"] = "Used / depleted"
            rr["consumable_badge_class"] = "secondary"
        elif ed is None:
            rr["consumable_status"] = "no_expiry"
            rr["consumable_status_label"] = "No expiry"
            rr["consumable_badge_class"] = "light"
        elif ed < t:
            rr["consumable_status"] = "expired"
            rr["consumable_status_label"] = "Expired"
            rr["consumable_badge_class"] = "danger"
        elif ed <= t + timedelta(days=nd):
            dd = (ed - t).days
            rr["consumable_status"] = "near_expiry"
            rr["consumable_status_label"] = f"Due in {dd}d" if dd >= 0 else "Due"
            rr["consumable_badge_class"] = "warning"
        else:
            rr["consumable_status"] = "ok"
            rr["consumable_status_label"] = "In date"
            rr["consumable_badge_class"] = "success"
        out.append(rr)
    return out


class CostingStrategy:
    def compute_cost(
        self,
        *,
        conn,
        item_id: int,
        quantity: float,
        transaction_type: str,
        explicit_unit_cost: Optional[float],
        location_id: Optional[int] = None,
        batch_id: Optional[int] = None,
    ) -> Tuple[float, float]:
        raise NotImplementedError


class AverageCostingStrategy(CostingStrategy):
    """Simple moving-average costing strategy."""

    def compute_cost(
        self,
        *,
        conn,
        item_id: int,
        quantity: float,
        transaction_type: str,
        explicit_unit_cost: Optional[float],
        location_id: Optional[int] = None,
        batch_id: Optional[int] = None,
    ) -> Tuple[float, float]:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT standard_cost FROM inventory_items WHERE id = %s", (item_id,)
            )
            row = cur.fetchone()
            current_avg = float(row["standard_cost"]) if row and row["standard_cost"] is not None else 0.0
        finally:
            cur.close()
        if transaction_type in ("in", "return", "repack") and explicit_unit_cost is not None:
            unit_cost = float(explicit_unit_cost)
        else:
            unit_cost = current_avg
        total_cost = unit_cost * float(quantity)
        return unit_cost, total_cost


def _get_cost_layers(conn, item_id: int, location_id: int, order_asc: bool) -> List[Tuple[float, float]]:
    """Return list of (quantity, unit_cost) layers for this item/location, ordered by performed_at."""
    cur = conn.cursor(dictionary=True)
    try:
        order = "ASC" if order_asc else "DESC"
        cur.execute(
            """
            SELECT quantity, unit_cost FROM inventory_transactions
            WHERE item_id = %s AND location_id = %s AND quantity > 0
            AND transaction_type IN ('in','return','repack')
            AND unit_cost IS NOT NULL
            ORDER BY performed_at """ + order,
            (item_id, location_id),
        )
        rows = cur.fetchall() or []
        return [(float(r["quantity"]), float(r["unit_cost"])) for r in rows]
    finally:
        cur.close()


class FIFOCostingStrategy(CostingStrategy):
    """First-in-first-out: consume oldest cost layers first."""

    def compute_cost(
        self,
        *,
        conn,
        item_id: int,
        quantity: float,
        transaction_type: str,
        explicit_unit_cost: Optional[float],
        location_id: Optional[int] = None,
        batch_id: Optional[int] = None,
    ) -> Tuple[float, float]:
        if transaction_type in ("in", "return", "repack") and explicit_unit_cost is not None:
            return float(explicit_unit_cost), float(explicit_unit_cost) * float(quantity)
        if transaction_type in ("out", "transfer") and location_id is not None and quantity > 0:
            layers = _get_cost_layers(conn, item_id, location_id, order_asc=True)
            remaining = float(quantity)
            total_cost = 0.0
            for qty, uc in layers:
                if remaining <= 0:
                    break
                take = min(qty, remaining)
                total_cost += take * uc
                remaining -= take
            if remaining > 0:
                cur = conn.cursor(dictionary=True)
                try:
                    cur.execute("SELECT standard_cost FROM inventory_items WHERE id = %s", (item_id,))
                    row = cur.fetchone()
                    fallback = float(row["standard_cost"]) if row and row["standard_cost"] else 0.0
                finally:
                    cur.close()
                total_cost += remaining * fallback
            unit_cost = total_cost / float(quantity) if quantity else 0.0
            return unit_cost, total_cost
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT standard_cost FROM inventory_items WHERE id = %s", (item_id,))
            row = cur.fetchone()
            avg = float(row["standard_cost"]) if row and row["standard_cost"] else 0.0
        finally:
            cur.close()
        return avg, avg * float(quantity)


class LIFOCostingStrategy(CostingStrategy):
    """Last-in-first-out: consume newest cost layers first."""

    def compute_cost(
        self,
        *,
        conn,
        item_id: int,
        quantity: float,
        transaction_type: str,
        explicit_unit_cost: Optional[float],
        location_id: Optional[int] = None,
        batch_id: Optional[int] = None,
    ) -> Tuple[float, float]:
        if transaction_type in ("in", "return", "repack") and explicit_unit_cost is not None:
            return float(explicit_unit_cost), float(explicit_unit_cost) * float(quantity)
        if transaction_type in ("out", "transfer") and location_id is not None and quantity > 0:
            layers = _get_cost_layers(conn, item_id, location_id, order_asc=False)
            remaining = float(quantity)
            total_cost = 0.0
            for qty, uc in layers:
                if remaining <= 0:
                    break
                take = min(qty, remaining)
                total_cost += take * uc
                remaining -= take
            if remaining > 0:
                cur = conn.cursor(dictionary=True)
                try:
                    cur.execute("SELECT standard_cost FROM inventory_items WHERE id = %s", (item_id,))
                    row = cur.fetchone()
                    fallback = float(row["standard_cost"]) if row and row["standard_cost"] else 0.0
                finally:
                    cur.close()
                total_cost += remaining * fallback
            unit_cost = total_cost / float(quantity) if quantity else 0.0
            return unit_cost, total_cost
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT standard_cost FROM inventory_items WHERE id = %s", (item_id,))
            row = cur.fetchone()
            avg = float(row["standard_cost"]) if row and row["standard_cost"] else 0.0
        finally:
            cur.close()
        return avg, avg * float(quantity)


class InventoryService:
    """
    Core domain service for Inventory Control.

    Provides item/location/batch CRUD helpers plus stock movement and costing.
    """

    def __init__(self):
        self._conn = None
        self._avg_costing = AverageCostingStrategy()
        self._fifo_costing = FIFOCostingStrategy()
        self._lifo_costing = LIFOCostingStrategy()
        self._schema_ensured = False

    def _connection(self):
        if self._conn is None or not getattr(self._conn, "is_connected", lambda: True)():
            self._conn = get_db_connection()
        if not self._schema_ensured:
            try:
                _ensure_inventory_equipment_schema(self._conn)
            finally:
                self._schema_ensured = True
        return self._conn

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------
    def health_check(self) -> Dict[str, Any]:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            cur.fetchone()
            return {"status": "ok"}
        finally:
            try:
                cur.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Categories (for trend reporting, Ecommerce/POS)
    # ------------------------------------------------------------------
    def list_categories(self, parent_id: Optional[int] = None):
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            if parent_id is not None:
                cur.execute(
                    "SELECT * FROM inventory_categories WHERE parent_id <=> %s ORDER BY sort_order, name",
                    (parent_id,),
                )
            else:
                cur.execute("SELECT * FROM inventory_categories ORDER BY sort_order, name")
            return cur.fetchall() or []
        except Exception:
            return []
        finally:
            cur.close()

    def get_category(self, category_id: int) -> Optional[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM inventory_categories WHERE id = %s", (category_id,))
            return cur.fetchone()
        except Exception:
            return None
        finally:
            cur.close()

    def create_category(self, data: Dict[str, Any]) -> int:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO inventory_categories (name, code, description, parent_id, sort_order)
                   VALUES (%s,%s,%s,%s,%s)""",
                (
                    data.get("name"),
                    data.get("code"),
                    data.get("description"),
                    data.get("parent_id"),
                    data.get("sort_order", 0),
                ),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()

    def update_category(self, category_id: int, data: Dict[str, Any]) -> None:
        conn = self._connection()
        cur = conn.cursor()
        try:
            updates = []
            params = []
            for k in ("name", "code", "description", "parent_id", "sort_order"):
                if k in data:
                    updates.append(f"{k} = %s")
                    params.append(data[k])
            if not updates:
                return
            params.append(category_id)
            cur.execute(
                "UPDATE inventory_categories SET " + ", ".join(updates) + " WHERE id = %s",
                params,
            )
            conn.commit()
        finally:
            cur.close()

    def delete_category(self, category_id: int) -> None:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute("UPDATE inventory_items SET category_id = NULL WHERE category_id = %s", (category_id,))
            cur.execute("DELETE FROM inventory_categories WHERE id = %s", (category_id,))
            conn.commit()
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # Items
    # ------------------------------------------------------------------
    def create_item(self, data: Dict[str, Any]) -> int:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO inventory_items (sku, name, description, barcode, qr_code_data,
                                             category, unit, default_location_id,
                                             reorder_point, reorder_quantity, is_active,
                                             is_equipment, requires_serial,
                                             cost_method, standard_cost, last_cost,
                                             primary_supplier_id, lead_time_days,
                                             external_sku, metadata, category_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    data.get("sku"),
                    data.get("name"),
                    data.get("description"),
                    data.get("barcode"),
                    data.get("qr_code_data"),
                    data.get("category"),
                    data.get("unit"),
                    data.get("default_location_id"),
                    data.get("reorder_point", 0),
                    data.get("reorder_quantity", 0),
                    1 if data.get("is_active", True) else 0,
                    1 if data.get("is_equipment", False) else 0,
                    1 if data.get("requires_serial", False) else 0,
                    _normalize_cost_method(data.get("cost_method")),
                    data.get("standard_cost"),
                    data.get("last_cost"),
                    data.get("primary_supplier_id"),
                    data.get("lead_time_days"),
                    data.get("external_sku"),
                    data.get("metadata"),
                    data.get("category_id"),
                ),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()

    def update_item(self, item_id: int, data: Dict[str, Any]) -> None:
        conn = self._connection()
        cur = conn.cursor()
        try:
            fields = []
            params = []
            for key in (
                "sku",
                "name",
                "description",
                "barcode",
                "qr_code_data",
                "category",
                "unit",
                "default_location_id",
                "reorder_point",
                "reorder_quantity",
                "is_active",
                "is_equipment",
                "requires_serial",
                "cost_method",
                "standard_cost",
                "last_cost",
                "primary_supplier_id",
                "lead_time_days",
                "external_sku",
                "metadata",
                "category_id",
            ):
                if key in data:
                    fields.append(f"{key} = %s")
                    if key == "is_active":
                        params.append(1 if data[key] else 0)
                    elif key in ("is_equipment", "requires_serial"):
                        params.append(1 if data[key] else 0)
                    elif key == "cost_method":
                        params.append(_normalize_cost_method(data[key]))
                    else:
                        params.append(data[key])
            if not fields:
                return
            params.append(item_id)
            cur.execute(
                f"UPDATE inventory_items SET {', '.join(fields)} WHERE id = %s",
                params,
            )
            conn.commit()
        finally:
            cur.close()

    def archive_item(self, item_id: int) -> None:
        self.update_item(item_id, {"is_active": False})

    # ------------------------------------------------------------------
    # Locations
    # ------------------------------------------------------------------
    def create_location(self, data: Dict[str, Any]) -> int:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO inventory_locations (name, code, type, parent_location_id, address, metadata)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (
                    data.get("name"),
                    data.get("code"),
                    data.get("type", "warehouse"),
                    data.get("parent_location_id"),
                    data.get("address"),
                    data.get("metadata"),
                ),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()

    def get_location_id_by_code(self, code: str) -> Optional[int]:
        if not code or not str(code).strip():
            return None
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id FROM inventory_locations WHERE code = %s LIMIT 1",
                (str(code).strip(),),
            )
            row = cur.fetchone()
            return int(row[0]) if row else None
        finally:
            cur.close()

    def provision_cura_operational_event_kit_pool(
        self, operational_event_id: int, event_name: str
    ) -> Optional[int]:
        """
        Auto-create a virtual inventory location used as assignee for kit at a Cura operational event.
        Code OP-EVT-{id} is stable for the event lifetime.
        """
        eid = int(operational_event_id)
        code = f"OP-EVT-{eid}"
        existing = self.get_location_id_by_code(code)
        if existing is not None:
            return int(existing)
        safe_name = (event_name or f"Event {eid}").strip()[:200]
        meta = {
            "cura_operational_event_id": eid,
            "auto_event_kit_pool": True,
            "purpose": (
                "Temporary kit pool for this Cura operational event. On close or delete, serial kit "
                "moves to post-event holding (HOLD-IN) for reconciliation."
            ),
        }
        return self.create_location(
            {
                "name": f"Event kit pool · {safe_name}",
                "code": code,
                "type": "virtual",
                "parent_location_id": None,
                "address": None,
                "metadata": json.dumps(meta),
            }
        )

    def list_equipment_held_at_assignee_location(
        self, assignee_location_id: int
    ) -> List[Dict[str, Any]]:
        """Serial assets whose latest out transaction assigns them to this location id."""
        lid = str(int(assignee_location_id))
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT ea.id, ea.item_id, ea.serial_number, ea.public_asset_code,
                       i.name AS item_name, i.sku AS sku
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
                  AND tl.assignee_type = 'location'
                  AND tl.assignee_id = %s
                """,
                (lid,),
            )
            return cur.fetchall() or []
        finally:
            cur.close()

    def release_cura_operational_event_kit_pool(
        self,
        *,
        pool_location_id: int,
        operational_event_id: int,
        performed_by_user_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Reassign serial kit from the event pool to HOLD-IN (qty-zero out rows), delete the pool location.
        Idempotent if the pool location no longer exists.
        """
        pool = int(pool_location_id)
        eid = int(operational_event_id)
        out: Dict[str, Any] = {"ok": True, "assets_moved": 0, "skipped": False, "error": None}
        hold_id = self.get_location_id_by_code("HOLD-IN")
        if not hold_id:
            out["ok"] = False
            out["error"] = (
                "Post-event holding location HOLD-IN is missing — run the inventory plugin install/upgrade."
            )
            return out
        if pool == int(hold_id):
            out["skipped"] = True
            return out
        chk = self._connection()
        c0 = chk.cursor()
        try:
            c0.execute("SELECT id FROM inventory_locations WHERE id = %s", (pool,))
            if not c0.fetchone():
                out["skipped"] = True
                return out
        finally:
            c0.close()

        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT code, name FROM inventory_locations WHERE id = %s",
                (int(hold_id),),
            )
            hrow = cur.fetchone() or {}
        finally:
            cur.close()
        hc = (hrow.get("code") or "").strip()
        hn = (hrow.get("name") or "").strip()
        if hc and hn:
            hold_label = f"{hc} — {hn}"
        else:
            hold_label = hn or hc or str(hold_id)

        for r in self.list_equipment_held_at_assignee_location(pool):
            aid = int(r["id"])
            iid = int(r["item_id"])
            try:
                self.record_transaction(
                    item_id=iid,
                    location_id=int(hold_id),
                    quantity=0.0,
                    transaction_type="out",
                    performed_by_user_id=performed_by_user_id,
                    assignee_type="location",
                    assignee_id=str(int(hold_id)),
                    assignee_label=hold_label,
                    equipment_asset_id=aid,
                    reference_type="cura_event_kit_pool_release",
                    reference_id=str(eid),
                    metadata={
                        "from_event_pool_location_id": pool,
                        "cura_operational_event_id": eid,
                        "assignee_bump": True,
                    },
                )
                out["assets_moved"] += 1
            except Exception as ex:
                logger.exception("release_cura_operational_event_kit_pool asset %s", aid)
                out["ok"] = False
                out["error"] = str(ex)[:500]
                return out

        conn = self._connection()
        cur2 = conn.cursor()
        try:
            cur2.execute(
                "DELETE FROM inventory_stock_levels WHERE location_id = %s",
                (pool,),
            )
            cur2.execute("DELETE FROM inventory_locations WHERE id = %s", (pool,))
            conn.commit()
        except Exception as ex:
            conn.rollback()
            logger.exception("release_cura_operational_event_kit_pool delete location %s", pool)
            out["ok"] = False
            out["error"] = str(ex)[:500]
        finally:
            cur2.close()
        return out

    def update_location(self, location_id: int, data: Dict[str, Any]) -> None:
        conn = self._connection()
        cur = conn.cursor()
        try:
            fields = []
            params = []
            for key in ("name", "code", "type", "parent_location_id", "address", "metadata"):
                if key in data:
                    fields.append(f"{key} = %s")
                    params.append(data[key])
            if not fields:
                return
            params.append(location_id)
            cur.execute(
                f"UPDATE inventory_locations SET {', '.join(fields)} WHERE id = %s",
                params,
            )
            conn.commit()
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # Batches
    # ------------------------------------------------------------------
    def create_batch(self, data: Dict[str, Any]) -> int:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO inventory_batches
                    (item_id, batch_number, lot_number, expiry_date,
                     manufacture_date, received_date, supplier_id, metadata,
                     weight, weight_uom, unit_weight, unit_weight_uom)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    data.get("item_id"),
                    data.get("batch_number"),
                    data.get("lot_number"),
                    data.get("expiry_date"),
                    data.get("manufacture_date"),
                    data.get("received_date"),
                    data.get("supplier_id"),
                    data.get("metadata"),
                    data.get("weight"),
                    data.get("weight_uom"),
                    data.get("unit_weight"),
                    data.get("unit_weight_uom"),
                ),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()

    def update_batch(self, batch_id: int, data: Dict[str, Any]) -> None:
        conn = self._connection()
        cur = conn.cursor()
        try:
            fields = []
            params = []
            for key in (
                "batch_number",
                "lot_number",
                "expiry_date",
                "manufacture_date",
                "received_date",
                "supplier_id",
                "metadata",
                "weight",
                "weight_uom",
                "unit_weight",
                "unit_weight_uom",
            ):
                if key in data:
                    fields.append(f"{key} = %s")
                    params.append(data[key])
            if not fields:
                return
            params.append(batch_id)
            cur.execute(
                f"UPDATE inventory_batches SET {', '.join(fields)} WHERE id = %s",
                params,
            )
            conn.commit()
        finally:
            cur.close()

    def get_batches_for_item(self, item_id: int):
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM inventory_batches WHERE item_id = %s ORDER BY COALESCE(expiry_date, '9999-12-31') ASC",
                (item_id,),
            )
            return cur.fetchall() or []
        finally:
            cur.close()

    def get_item(self, item_id: int) -> Optional[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM inventory_items WHERE id = %s", (item_id,))
            return cur.fetchone()
        finally:
            cur.close()

    def list_items(
        self,
        *,
        skip: int = 0,
        limit: int = 50,
        search: Optional[str] = None,
        category: Optional[str] = None,
        category_id: Optional[int] = None,
        is_active: Optional[bool] = None,
        is_equipment: Optional[bool] = None,
    ):
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            sql = """SELECT i.*, c.name AS category_name
                     FROM inventory_items i
                     LEFT JOIN inventory_categories c ON c.id = i.category_id
                     WHERE 1=1"""
            params = []
            if search:
                sql += " AND (i.name LIKE %s OR i.sku LIKE %s OR i.barcode LIKE %s)"
                pct = f"%{search}%"
                params.extend([pct, pct, pct])
            if category:
                sql += " AND i.category = %s"
                params.append(category)
            if category_id is not None:
                sql += " AND i.category_id = %s"
                params.append(category_id)
            if is_active is not None:
                sql += " AND i.is_active = %s"
                params.append(1 if is_active else 0)
            if is_equipment is True:
                sql += " AND i.is_equipment = 1"
            elif is_equipment is False:
                sql += " AND (i.is_equipment = 0 OR i.is_equipment IS NULL)"
            sql += " ORDER BY i.name LIMIT %s OFFSET %s"
            params.extend([limit, skip])
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
        finally:
            cur.close()

    def get_location(self, location_id: int) -> Optional[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM inventory_locations WHERE id = %s", (location_id,))
            return cur.fetchone()
        finally:
            cur.close()

    def list_locations(self, *, parent_id: Optional[int] = None):
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            if parent_id is not None:
                cur.execute(
                    "SELECT * FROM inventory_locations WHERE parent_location_id <=> %s ORDER BY code",
                    (parent_id,),
                )
            else:
                cur.execute("SELECT * FROM inventory_locations ORDER BY code")
            return cur.fetchall() or []
        finally:
            cur.close()

    def list_suppliers(self, *, limit: int = 500, skip: int = 0):
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM inventory_suppliers ORDER BY name LIMIT %s OFFSET %s",
                (limit, skip),
            )
            return cur.fetchall() or []
        finally:
            cur.close()

    def list_batches(
        self,
        *,
        item_id: Optional[int] = None,
        supplier_id: Optional[int] = None,
        limit: int = 100,
        skip: int = 0,
    ):
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            sql = "SELECT * FROM inventory_batches WHERE 1=1"
            params = []
            if item_id is not None:
                sql += " AND item_id = %s"
                params.append(item_id)
            if supplier_id is not None:
                sql += " AND supplier_id = %s"
                params.append(supplier_id)
            sql += " ORDER BY COALESCE(expiry_date, '9999-12-31') ASC LIMIT %s OFFSET %s"
            params.extend([limit, skip])
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
        finally:
            cur.close()

    def list_transactions(
        self,
        *,
        item_id: Optional[int] = None,
        location_id: Optional[int] = None,
        transaction_type: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ):
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            sql = "SELECT * FROM inventory_transactions WHERE 1=1"
            params = []
            if item_id is not None:
                sql += " AND item_id = %s"
                params.append(item_id)
            if location_id is not None:
                sql += " AND location_id = %s"
                params.append(location_id)
            if transaction_type:
                sql += " AND transaction_type = %s"
                params.append(transaction_type)
            if from_date:
                sql += " AND performed_at >= %s"
                params.append(from_date)
            if to_date:
                sql += " AND performed_at <= %s"
                params.append(to_date)
            sql += " ORDER BY performed_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, skip])
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # Equipment assets (serialised units)
    # ------------------------------------------------------------------
    def create_equipment_asset(
        self,
        *,
        item_id: int,
        serial_number: str,
        make: Optional[str] = None,
        model: Optional[str] = None,
        purchase_date: Optional[str] = None,
        warranty_expiry: Optional[str] = None,
        warranty_start_basis: Optional[str] = None,
        warranty_start_date: Optional[str] = None,
        warranty_months: Optional[int] = None,
        service_interval_days: Optional[int] = None,
        next_service_due_date: Optional[str] = None,
        condition: Optional[str] = None,
        public_asset_code: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        conn = self._connection()
        cur = conn.cursor()
        try:
            sn = (serial_number or "").strip()
            if not sn:
                raise ValueError("serial_number required")
            wb = normalize_warranty_start_basis(warranty_start_basis)
            wm = int(warranty_months) if warranty_months is not None else None
            pac = (public_asset_code or "").strip() or None
            nsd = next_service_due_date if next_service_due_date else None
            cur.execute(
                """
                INSERT INTO inventory_equipment_assets
                    (item_id, serial_number, status, make, model, purchase_date, warranty_expiry,
                     warranty_start_basis, warranty_start_date, warranty_months,
                     service_interval_days, next_service_due_date, `condition`, public_asset_code, metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(item_id),
                    sn,
                    "in_stock",
                    (make or "").strip() or None,
                    (model or "").strip() or None,
                    purchase_date if purchase_date else None,
                    warranty_expiry if warranty_expiry else None,
                    wb,
                    warranty_start_date if warranty_start_date else None,
                    wm,
                    int(service_interval_days) if service_interval_days is not None else None,
                    nsd,
                    (condition or "").strip() or None,
                    pac,
                    json.dumps(metadata or {}, default=str),
                ),
            )
            conn.commit()
            new_id = cur.lastrowid
            try:
                if not pac:
                    code = f"AST-{int(new_id):06d}"
                    cur.execute(
                        """
                        UPDATE inventory_equipment_assets
                        SET public_asset_code = %s
                        WHERE id = %s AND (public_asset_code IS NULL OR public_asset_code = '')
                        """,
                        (code, int(new_id)),
                    )
                    conn.commit()
            except Exception:
                pass
            return int(new_id)
        finally:
            cur.close()

    def update_equipment_asset(
        self,
        asset_id: int,
        *,
        make: Optional[str] = None,
        model: Optional[str] = None,
        purchase_date: Optional[str] = None,
        warranty_expiry: Optional[str] = None,
        warranty_start_basis: Any = None,
        warranty_start_date: Optional[str] = None,
        warranty_months: Any = None,
        service_interval_days: Optional[int] = None,
        next_service_due_date: Optional[str] = None,
        operational_state: Optional[str] = None,
        condition: Optional[str] = None,
        status: Optional[str] = None,
        public_asset_code: Any = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update equipment asset fields. Omitted kwargs leave existing values unchanged."""
        updates: List[str] = []
        params: List[Any] = []
        if make is not None:
            updates.append("make = %s")
            params.append((make or "").strip() or None)
        if model is not None:
            updates.append("model = %s")
            params.append((model or "").strip() or None)
        if purchase_date is not None:
            updates.append("purchase_date = %s")
            params.append(purchase_date or None)
        if warranty_expiry is not None:
            updates.append("warranty_expiry = %s")
            params.append(warranty_expiry or None)
        if warranty_start_basis is not None:
            updates.append("warranty_start_basis = %s")
            params.append(normalize_warranty_start_basis(warranty_start_basis))
        if warranty_start_date is not None:
            updates.append("warranty_start_date = %s")
            params.append(warranty_start_date or None)
        if warranty_months is not None:
            updates.append("warranty_months = %s")
            wm = warranty_months
            if wm in ("", None):
                params.append(None)
            else:
                params.append(int(wm))
        if service_interval_days is not None:
            updates.append("service_interval_days = %s")
            params.append(int(service_interval_days) if service_interval_days else None)
        if next_service_due_date is not None:
            updates.append("next_service_due_date = %s")
            params.append(next_service_due_date or None)
        if operational_state is not None:
            updates.append("operational_state = %s")
            params.append(operational_state)
        if condition is not None:
            updates.append("`condition` = %s")
            params.append((condition or "").strip() or None)
        if status is not None:
            updates.append("status = %s")
            params.append(status)
        if metadata is not None:
            updates.append("metadata = %s")
            params.append(json.dumps(metadata if isinstance(metadata, dict) else {}, default=str))
        if public_asset_code is not None:
            updates.append("public_asset_code = %s")
            params.append((public_asset_code or "").strip() or None)
        if not updates:
            return
        params.append(int(asset_id))
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                f"UPDATE inventory_equipment_assets SET {', '.join(updates)} WHERE id = %s",
                tuple(params),
            )
            if cur.rowcount == 0:
                raise ValueError("Equipment asset not found")
            conn.commit()
        finally:
            cur.close()

    def list_equipment_assets(
        self,
        *,
        item_id: Optional[int] = None,
        status: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 200,
        skip: int = 0,
    ) -> List[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            sql = "SELECT * FROM inventory_equipment_assets WHERE 1=1"
            params: List[Any] = []
            if item_id is not None:
                sql += " AND item_id = %s"
                params.append(int(item_id))
            if status:
                sql += " AND status = %s"
                params.append(status)
            if search:
                sql += " AND serial_number LIKE %s"
                params.append(f"%{search}%")
            sql += " ORDER BY updated_at DESC LIMIT %s OFFSET %s"
            params.extend([min(int(limit or 200), 500), int(skip or 0)])
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
            cur.close()

    def get_equipment_asset(self, asset_id: int) -> Optional[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM inventory_equipment_assets WHERE id = %s", (int(asset_id),))
            row = cur.fetchone()
            if row and isinstance(row.get("metadata"), str):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except Exception:
                    pass
            return row
        finally:
            cur.close()

    def get_equipment_asset_by_serial(self, serial_number: str) -> Optional[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            sn = (serial_number or "").strip()
            if not sn:
                return None
            cur.execute("SELECT * FROM inventory_equipment_assets WHERE serial_number = %s", (sn,))
            row = cur.fetchone()
            if row and isinstance(row.get("metadata"), str):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except Exception:
                    pass
            return row
        finally:
            cur.close()

    def set_equipment_asset_status(self, asset_id: int, status: str) -> None:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE inventory_equipment_assets SET status = %s WHERE id = %s",
                (status, int(asset_id)),
            )
            conn.commit()
        finally:
            cur.close()

    def list_equipment_asset_consumables(self, equipment_asset_id: int) -> List[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT c.*, i.sku AS inventory_sku, i.name AS inventory_item_name
                FROM inventory_equipment_asset_consumables c
                LEFT JOIN inventory_items i ON i.id = c.inventory_item_id
                WHERE c.equipment_asset_id = %s
                ORDER BY c.depleted ASC, c.expiry_date IS NULL, c.expiry_date ASC, c.id ASC
                """,
                (int(equipment_asset_id),),
            )
            return cur.fetchall() or []
        finally:
            cur.close()

    def add_equipment_asset_consumable(
        self,
        *,
        equipment_asset_id: int,
        label: str,
        batch_number: Optional[str] = None,
        lot_number: Optional[str] = None,
        expiry_date: Optional[str] = None,
        quantity: float = 1.0,
        inventory_item_id: Optional[int] = None,
        notes: Optional[str] = None,
    ) -> int:
        conn = self._connection()
        cur = conn.cursor()
        try:
            lab = (label or "").strip()
            if not lab:
                raise ValueError("label is required")
            qty = float(quantity)
            if qty <= 0:
                raise ValueError("quantity must be positive")
            iid = int(inventory_item_id) if inventory_item_id else None
            cur.execute(
                """
                INSERT INTO inventory_equipment_asset_consumables
                    (equipment_asset_id, inventory_item_id, label, batch_number, lot_number,
                     expiry_date, quantity, depleted, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,0,%s)
                """,
                (
                    int(equipment_asset_id),
                    iid,
                    lab,
                    (batch_number or "").strip() or None,
                    (lot_number or "").strip() or None,
                    (expiry_date or "").strip() or None,
                    qty,
                    (notes or "").strip() or None,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            cur.close()

    def update_equipment_asset_consumable(
        self,
        consumable_id: int,
        *,
        equipment_asset_id: int,
        label: Optional[str] = None,
        batch_number: Any = ...,
        lot_number: Any = ...,
        expiry_date: Any = ...,
        quantity: Optional[float] = None,
        depleted: Optional[bool] = None,
        inventory_item_id: Any = ...,
        notes: Any = ...,
        usage_close_reason: Any = ...,
        discrepancy_flag: Any = ...,
        discrepancy_details: Any = ...,
        discrepancy_reported_at: Any = ...,
        discrepancy_reported_by_contractor_id: Any = ...,
    ) -> None:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id FROM inventory_equipment_asset_consumables
                WHERE id = %s AND equipment_asset_id = %s
                """,
                (int(consumable_id), int(equipment_asset_id)),
            )
            if not cur.fetchone():
                raise ValueError("consumable not found for this asset")
            fields: List[str] = []
            params: List[Any] = []
            if label is not None:
                fields.append("label = %s")
                params.append((label or "").strip())
            if batch_number is not ...:
                v = None if batch_number is None else str(batch_number).strip() or None
                fields.append("batch_number = %s")
                params.append(v)
            if lot_number is not ...:
                v = None if lot_number is None else str(lot_number).strip() or None
                fields.append("lot_number = %s")
                params.append(v)
            if expiry_date is not ...:
                v = None if expiry_date is None else (str(expiry_date).strip() or None)
                fields.append("expiry_date = %s")
                params.append(v)
            if quantity is not None:
                q = float(quantity)
                if q < 0:
                    raise ValueError("quantity invalid")
                fields.append("quantity = %s")
                params.append(q)
            if depleted is not None:
                fields.append("depleted = %s")
                params.append(1 if depleted else 0)
            if inventory_item_id is not ...:
                if inventory_item_id is None or inventory_item_id == "":
                    fields.append("inventory_item_id = NULL")
                else:
                    fields.append("inventory_item_id = %s")
                    params.append(int(inventory_item_id))
            if notes is not ...:
                v = None if notes is None else str(notes).strip() or None
                fields.append("notes = %s")
                params.append(v)
            if usage_close_reason is not ...:
                v = None if usage_close_reason is None else str(usage_close_reason).strip()[:32] or None
                fields.append("usage_close_reason = %s")
                params.append(v)
            if discrepancy_flag is not ...:
                fields.append("discrepancy_flag = %s")
                params.append(1 if discrepancy_flag else 0)
            if discrepancy_details is not ...:
                v = None if discrepancy_details is None else str(discrepancy_details).strip() or None
                fields.append("discrepancy_details = %s")
                params.append(v)
            if discrepancy_reported_at is not ...:
                if discrepancy_reported_at is None:
                    fields.append("discrepancy_reported_at = NULL")
                else:
                    fields.append("discrepancy_reported_at = %s")
                    params.append(discrepancy_reported_at)
            if discrepancy_reported_by_contractor_id is not ...:
                if discrepancy_reported_by_contractor_id is None:
                    fields.append("discrepancy_reported_by_contractor_id = NULL")
                else:
                    fields.append("discrepancy_reported_by_contractor_id = %s")
                    params.append(int(discrepancy_reported_by_contractor_id))
            if not fields:
                return
            params.extend([int(consumable_id), int(equipment_asset_id)])
            cur.execute(
                f"UPDATE inventory_equipment_asset_consumables SET {', '.join(fields)} "
                "WHERE id = %s AND equipment_asset_id = %s",
                tuple(params),
            )
            conn.commit()
        finally:
            cur.close()

    def delete_equipment_asset_consumable(self, consumable_id: int, equipment_asset_id: int) -> bool:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                DELETE FROM inventory_equipment_asset_consumables
                WHERE id = %s AND equipment_asset_id = %s
                """,
                (int(consumable_id), int(equipment_asset_id)),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()

    def consumable_alert_summary_for_assets(
        self,
        equipment_asset_ids: List[int],
        *,
        near_days: int = 30,
    ) -> Dict[int, Dict[str, Any]]:
        """Per-asset counts for fleet / dashboard badges (expiry vs depleted)."""
        ids = [int(x) for x in equipment_asset_ids if x is not None]
        if not ids:
            return {}
        t = date.today()
        nd = max(int(near_days or 30), 1)
        near_end = t + timedelta(days=nd)
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            placeholders = ",".join(["%s"] * len(ids))
            cur.execute(
                f"""
                SELECT equipment_asset_id, quantity, depleted, expiry_date, discrepancy_flag
                FROM inventory_equipment_asset_consumables
                WHERE equipment_asset_id IN ({placeholders})
                """,
                tuple(ids),
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()

        summary: Dict[int, Dict[str, Any]] = {
            i: {
                "total_lines": 0,
                "expired": 0,
                "near": 0,
                "in_date": 0,
                "no_expiry_active": 0,
                "used_up": 0,
                "discrepancy": 0,
            }
            for i in ids
        }
        for r in rows:
            aid = int(r["equipment_asset_id"])
            if aid not in summary:
                continue
            summary[aid]["total_lines"] += 1
            if int(r.get("discrepancy_flag") or 0):
                summary[aid]["discrepancy"] += 1
            qty = float(r.get("quantity") or 0)
            dep = bool(int(r.get("depleted") or 0))
            if dep or qty <= 0:
                summary[aid]["used_up"] += 1
                continue
            ed = _parse_sql_date(r.get("expiry_date"))
            if ed is None:
                summary[aid]["no_expiry_active"] += 1
            elif ed < t:
                summary[aid]["expired"] += 1
            elif ed <= near_end:
                summary[aid]["near"] += 1
            else:
                summary[aid]["in_date"] += 1
        for aid, s in summary.items():
            active = (
                s["expired"] + s["near"] + s["in_date"] + s["no_expiry_active"]
            )
            if s["expired"] > 0:
                s["worst"] = "expired"
            elif s["near"] > 0:
                s["worst"] = "near"
            elif active > 0:
                s["worst"] = "ok"
            else:
                s["worst"] = "none"
        return summary

    def count_kit_consumable_expiry_alerts(self, near_days: int = 30) -> Dict[str, int]:
        """
        Active dated lines on serial assets: expired, or due within near_days from today.
        Excludes depleted lines and retired/lost assets.
        """
        nd = max(int(near_days or 30), 1)
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT
                  SUM(CASE WHEN c.expiry_date < CURDATE() THEN 1 ELSE 0 END) AS expired_lines,
                  SUM(
                    CASE
                      WHEN c.expiry_date >= CURDATE()
                       AND c.expiry_date <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
                      THEN 1 ELSE 0 END
                  ) AS near_lines
                FROM inventory_equipment_asset_consumables c
                INNER JOIN inventory_equipment_assets a ON a.id = c.equipment_asset_id
                WHERE COALESCE(c.depleted, 0) = 0
                  AND COALESCE(c.quantity, 0) > 0
                  AND c.expiry_date IS NOT NULL
                  AND c.expiry_date <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
                  AND (a.status IS NULL OR a.status NOT IN ('retired', 'lost'))
                """,
                (nd, nd),
            )
            row = cur.fetchone() or {}
            return {
                "expired": int(row.get("expired_lines") or 0),
                "near": int(row.get("near_lines") or 0),
            }
        finally:
            cur.close()

    def list_kit_consumable_expiry_alert_rows(
        self,
        *,
        near_days: int = 30,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Line-level rows for emails and dashboards (expired first, then soonest expiry)."""
        nd = max(int(near_days or 30), 1)
        lim = max(min(int(limit or 200), 500), 1)
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT c.id AS consumable_line_id,
                       c.equipment_asset_id,
                       c.label,
                       c.batch_number,
                       c.lot_number,
                       c.expiry_date,
                       c.quantity,
                       a.public_asset_code,
                       a.serial_number,
                       m.name AS equipment_model_name
                FROM inventory_equipment_asset_consumables c
                INNER JOIN inventory_equipment_assets a ON a.id = c.equipment_asset_id
                LEFT JOIN inventory_items m ON m.id = a.item_id
                WHERE COALESCE(c.depleted, 0) = 0
                  AND COALESCE(c.quantity, 0) > 0
                  AND c.expiry_date IS NOT NULL
                  AND c.expiry_date <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
                  AND (a.status IS NULL OR a.status NOT IN ('retired', 'lost'))
                ORDER BY c.expiry_date ASC, c.equipment_asset_id ASC, c.id ASC
                LIMIT %s
                """,
                (nd, lim),
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()

        today_d = date.today()
        out: List[Dict[str, Any]] = []
        for r in rows:
            rr = dict(r)
            ed = _parse_sql_date(rr.get("expiry_date"))
            if ed is None:
                continue
            rr["expiry_date"] = ed.isoformat()
            rr["days_left"] = (ed - today_d).days
            rr["alert_tier"] = "expired" if ed < today_d else "near"
            out.append(rr)
        return out

    def cancel_pending_handoffs_for_asset(self, equipment_asset_id: int) -> None:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE inventory_equipment_portal_handoffs
                SET status = 'cancelled', cancelled_at = NOW()
                WHERE equipment_asset_id = %s AND status = 'pending'
                """,
                (int(equipment_asset_id),),
            )
            conn.commit()
        finally:
            cur.close()

    def create_equipment_portal_handoff(
        self,
        *,
        equipment_asset_id: int,
        contractor_id: int,
        handoff_kind: str,
        inventory_location_id: int,
        vehicle_id: Optional[int] = None,
        initiated_by_user_id: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> int:
        hk = (handoff_kind or "").strip()
        if hk not in ("to_vehicle", "to_storeroom"):
            raise ValueError("handoff_kind must be to_vehicle or to_storeroom")
        if hk == "to_vehicle" and not vehicle_id:
            raise ValueError("vehicle_id required for to_vehicle")
        if hk == "to_storeroom":
            vehicle_id = None
        self.cancel_pending_handoffs_for_asset(int(equipment_asset_id))
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO inventory_equipment_portal_handoffs
                    (equipment_asset_id, contractor_id, handoff_kind, vehicle_id,
                     inventory_location_id, status, initiated_by_user_id, notes)
                VALUES (%s,%s,%s,%s,%s,'pending',%s,%s)
                """,
                (
                    int(equipment_asset_id),
                    int(contractor_id),
                    hk,
                    int(vehicle_id) if vehicle_id is not None else None,
                    int(inventory_location_id),
                    initiated_by_user_id,
                    (notes or "").strip() or None,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            cur.close()

    def list_pending_handoffs_for_contractor(self, contractor_id: int) -> List[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT h.*, ea.serial_number, ea.public_asset_code, ea.item_id
                FROM inventory_equipment_portal_handoffs h
                INNER JOIN inventory_equipment_assets ea ON ea.id = h.equipment_asset_id
                WHERE h.contractor_id = %s AND h.status = 'pending'
                ORDER BY h.created_at DESC
                """,
                (int(contractor_id),),
            )
            return cur.fetchall() or []
        finally:
            cur.close()

    def list_pending_handoffs_for_asset(self, equipment_asset_id: int) -> List[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT * FROM inventory_equipment_portal_handoffs
                WHERE equipment_asset_id = %s AND status = 'pending'
                ORDER BY created_at DESC
                """,
                (int(equipment_asset_id),),
            )
            return cur.fetchall() or []
        finally:
            cur.close()

    def get_equipment_portal_handoff(self, handoff_id: int) -> Optional[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM inventory_equipment_portal_handoffs WHERE id = %s",
                (int(handoff_id),),
            )
            return cur.fetchone()
        finally:
            cur.close()

    def cancel_equipment_portal_handoff(self, handoff_id: int) -> bool:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE inventory_equipment_portal_handoffs
                SET status = 'cancelled', cancelled_at = NOW()
                WHERE id = %s AND status = 'pending'
                """,
                (int(handoff_id),),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()

    def complete_equipment_portal_handoff(self, handoff_id: int, *, contractor_id: int) -> Dict[str, Any]:
        """
        Contractor confirms an admin-initiated handoff (return to store or place on a vehicle).
        Uses return + optional immediate out so location stock stays balanced when moving to vehicle.
        """
        from app.plugins.inventory_control.asset_service import get_asset_service

        svc = get_asset_service()
        ho = self.get_equipment_portal_handoff(int(handoff_id))
        if not ho or str(ho.get("status") or "") != "pending":
            raise ValueError("Handoff not found or already closed")
        if int(ho["contractor_id"]) != int(contractor_id):
            raise ValueError("This task is not assigned to you")
        aid = int(ho["equipment_asset_id"])
        if not svc.contractor_holds_asset(int(contractor_id), aid):
            raise ValueError("This equipment is no longer signed out to you; ask the office to refresh the task")
        asset = self.get_equipment_asset(aid)
        if not asset:
            raise ValueError("Asset missing")
        item_id = int(asset["item_id"])
        loc_id = int(ho["inventory_location_id"])
        kind = str(ho["handoff_kind"] or "")
        hid = int(handoff_id)
        meta_base = {
            "portal_handoff_id": hid,
            "completed_by_contractor_id": int(contractor_id),
        }
        if kind == "to_storeroom":
            self.record_transaction(
                item_id=item_id,
                location_id=loc_id,
                quantity=1.0,
                transaction_type="return",
                performed_by_user_id=None,
                equipment_asset_id=aid,
                reference_type="contractor_portal_handoff",
                reference_id=str(hid),
                metadata={**meta_base, "handoff_kind": "to_storeroom"},
            )
            self.set_equipment_asset_status(aid, "in_stock")
        elif kind == "to_vehicle":
            vid = int(ho["vehicle_id"] or 0)
            if not vid:
                raise ValueError("Handoff missing vehicle")
            from app.plugins.fleet_management.objects import get_fleet_service

            fv = get_fleet_service().get_vehicle(vid)
            if not fv:
                raise ValueError("Vehicle not found")
            label = fv.get("registration") or fv.get("internal_code") or str(vid)
            self.record_transaction(
                item_id=item_id,
                location_id=loc_id,
                quantity=1.0,
                transaction_type="return",
                performed_by_user_id=None,
                equipment_asset_id=aid,
                reference_type="contractor_portal_handoff_bridge",
                reference_id=str(hid),
                metadata={**meta_base, "bridge": "pre_vehicle_out"},
            )
            self.record_transaction(
                item_id=item_id,
                location_id=loc_id,
                quantity=1.0,
                transaction_type="out",
                performed_by_user_id=None,
                assignee_type="vehicle",
                assignee_id=str(vid),
                assignee_label=label,
                equipment_asset_id=aid,
                reference_type="fleet_vehicle_assign",
                reference_id=str(vid),
                metadata={
                    **meta_base,
                    "fleet_vehicle_id": vid,
                    "via_contractor_portal_handoff": True,
                },
            )
            self.set_equipment_asset_status(aid, "assigned")
        else:
            raise ValueError("Invalid handoff kind")
        cur = self._connection().cursor()
        try:
            cur.execute(
                """
                UPDATE inventory_equipment_portal_handoffs
                SET status = 'completed', completed_at = NOW()
                WHERE id = %s AND status = 'pending'
                """,
                (hid,),
            )
            if cur.rowcount == 0:
                raise ValueError("Handoff could not be completed (already processed?)")
            self._connection().commit()
        finally:
            cur.close()
        return {"asset_id": aid, "kind": kind}

    def list_stock_levels(self, item_id: int):
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM inventory_stock_levels WHERE item_id = %s",
                (item_id,),
            )
            return cur.fetchall() or []
        finally:
            cur.close()

    def get_current_qoh(self, item_id: int) -> float:
        """Current quantity on hand across all locations/batches."""
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COALESCE(SUM(quantity_on_hand), 0) FROM inventory_stock_levels WHERE item_id = %s",
                (item_id,),
            )
            row = cur.fetchone()
            return float(row[0] or 0)
        finally:
            cur.close()

    def get_item_stock_series(
        self,
        *,
        item_id: int,
        range_days: int = 30,
        bucket: str = "auto",
    ) -> Dict[str, Any]:
        """
        Returns a stock history series for an item: [{t, qoh, delta}] with adaptive bucketing.
        Computes starting QOH from current stock_levels minus summed deltas in window.
        """
        rd = max(int(range_days or 30), 1)
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        now = datetime.utcnow()
        start_dt = now - timedelta(days=rd)

        try:
            cur.execute(
                "SELECT COUNT(*) AS c FROM inventory_transactions WHERE item_id = %s AND performed_at >= %s",
                (item_id, start_dt),
            )
            tx_count = int((cur.fetchone() or {}).get("c") or 0)
        except Exception:
            tx_count = 0

        bucket_used = _choose_bucket(rd, tx_count) if bucket == "auto" else str(bucket or "day").lower()
        if bucket_used not in ("hour", "day", "week", "month"):
            bucket_used = "day"

        # Group deltas by bucket in SQL
        if bucket_used == "hour":
            bucket_sql = "DATE_FORMAT(performed_at, '%Y-%m-%d %H:00:00')"
        elif bucket_used == "day":
            bucket_sql = "DATE_FORMAT(performed_at, '%Y-%m-%d 00:00:00')"
        elif bucket_used == "week":
            bucket_sql = "DATE_FORMAT(DATE_SUB(DATE(performed_at), INTERVAL WEEKDAY(performed_at) DAY), '%Y-%m-%d 00:00:00')"
        else:  # month
            bucket_sql = "DATE_FORMAT(DATE_SUB(DATE(performed_at), INTERVAL (DAY(performed_at)-1) DAY), '%Y-%m-%d 00:00:00')"

        cur2 = conn.cursor(dictionary=True)
        try:
            cur2.execute(
                f"""
                SELECT {bucket_sql} AS bucket_start,
                       COALESCE(SUM(quantity), 0) AS delta
                FROM inventory_transactions
                WHERE item_id = %s AND performed_at >= %s
                GROUP BY bucket_start
                ORDER BY bucket_start ASC
                """,
                (item_id, start_dt),
            )
            rows = cur2.fetchall() or []
        finally:
            cur2.close()

        delta_by_bucket: Dict[str, float] = {}
        total_delta = 0.0
        for r in rows:
            k = str(r.get("bucket_start"))
            d = float(r.get("delta") or 0)
            delta_by_bucket[k] = d
            total_delta += d

        current_qoh = self.get_current_qoh(item_id)
        start_qoh = current_qoh - total_delta

        # Build contiguous buckets
        start_b = _bucket_start(start_dt, bucket_used)
        end_b = _bucket_start(now, bucket_used)
        series = []
        qoh = float(start_qoh)

        # Ensure at least one point
        steps = 0
        cur_bucket = start_b
        while cur_bucket <= end_b and steps < 5000:
            key = cur_bucket.strftime("%Y-%m-%d %H:%M:%S")
            delta = float(delta_by_bucket.get(key, 0.0))
            qoh += delta
            series.append({"t": cur_bucket.isoformat() + "Z", "delta": delta, "qoh": qoh})
            cur_bucket = _add_bucket(cur_bucket, bucket_used)
            steps += 1

        # Usage estimate: average daily outflow (negative deltas only) over window
        neg_total = 0.0
        for d in delta_by_bucket.values():
            if d < 0:
                neg_total += abs(d)
        avg_daily_out = (neg_total / float(rd)) if rd > 0 else 0.0

        return {
            "range_days": rd,
            "bucket_used": bucket_used,
            "tx_count": tx_count,
            "current_qoh": current_qoh,
            "avg_daily_out": avg_daily_out,
            "series": series,
        }

    def list_stock_levels_all(
        self,
        *,
        item_id: Optional[int] = None,
        location_id: Optional[int] = None,
        limit: int = 1000,
        skip: int = 0,
    ):
        """List stock levels with optional filters (e.g. for CSV export or internal use by Sales/other modules)."""
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            sql = "SELECT * FROM inventory_stock_levels WHERE 1=1"
            params = []
            if item_id is not None:
                sql += " AND item_id = %s"
                params.append(item_id)
            if location_id is not None:
                sql += " AND location_id = %s"
                params.append(location_id)
            sql += " ORDER BY item_id, location_id LIMIT %s OFFSET %s"
            params.extend([limit, skip])
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
        finally:
            cur.close()

    def list_stock_levels_report(
        self,
        *,
        item_id: Optional[int] = None,
        location_id: Optional[int] = None,
        limit: int = 500,
        skip: int = 0,
    ):
        """Stock levels with item and location names for analytics/reports."""
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            sql = """
                SELECT s.id, s.item_id, s.location_id, s.batch_id,
                       s.quantity_on_hand, s.quantity_reserved, s.quantity_available,
                       i.sku AS item_sku, i.name AS item_name,
                       l.code AS location_code, l.name AS location_name
                FROM inventory_stock_levels s
                LEFT JOIN inventory_items i ON i.id = s.item_id
                LEFT JOIN inventory_locations l ON l.id = s.location_id
                WHERE 1=1
                """
            params = []
            if item_id is not None:
                sql += " AND s.item_id = %s"
                params.append(item_id)
            if location_id is not None:
                sql += " AND s.location_id = %s"
                params.append(location_id)
            sql += " ORDER BY i.name, l.code LIMIT %s OFFSET %s"
            params.extend([limit, skip])
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
            for r in rows:
                if r.get("quantity_on_hand") is not None and hasattr(r["quantity_on_hand"], "__float__"):
                    r["quantity_on_hand"] = float(r["quantity_on_hand"])
                if r.get("quantity_available") is not None and hasattr(r["quantity_available"], "__float__"):
                    r["quantity_available"] = float(r["quantity_available"])
            return rows
        finally:
            cur.close()

    def get_analytics_movers(self, days: int = 30, top_n: int = 20):
        """Fast movers (most tx + quantity) and slow movers (least movement) in the last N days."""
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT t.item_id, i.sku, i.name,
                       COUNT(*) AS tx_count,
                       SUM(CASE WHEN t.transaction_type IN ('in','return','repack') THEN t.quantity ELSE 0 END) AS qty_in,
                       SUM(CASE WHEN t.transaction_type IN ('out','transfer') THEN ABS(t.quantity) ELSE 0 END) AS qty_out
                FROM inventory_transactions t
                JOIN inventory_items i ON i.id = t.item_id
                WHERE t.performed_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                GROUP BY t.item_id, i.sku, i.name
                """,
                (days,),
            )
            rows = cur.fetchall() or []
            for r in rows:
                r["qty_in"] = float(r.get("qty_in") or 0)
                r["qty_out"] = float(r.get("qty_out") or 0)
                r["total_movement"] = r["qty_in"] + r["qty_out"]
            rows.sort(key=lambda x: (x["tx_count"], x["total_movement"]), reverse=True)
            fast = rows[:top_n]
            slow = rows[-top_n:] if len(rows) > top_n else []
            slow.reverse()
            return {"fast_movers": fast, "slow_movers": slow}
        finally:
            cur.close()

    def get_analytics_activity(self, days: int = 7, limit: int = 200):
        """Recent transactions with item and location names for activity report."""
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT t.id, t.item_id, t.location_id, t.batch_id, t.quantity, t.transaction_type,
                       t.unit_cost, t.performed_at, t.reference_type, t.reference_id,
                       i.sku AS item_sku, i.name AS item_name,
                       l.code AS location_code, l.name AS location_name
                FROM inventory_transactions t
                LEFT JOIN inventory_items i ON i.id = t.item_id
                LEFT JOIN inventory_locations l ON l.id = t.location_id
                WHERE t.performed_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY t.performed_at DESC
                LIMIT %s
                """,
                (days, limit),
            )
            rows = cur.fetchall() or []
            for r in rows:
                if r.get("performed_at") and hasattr(r["performed_at"], "isoformat"):
                    r["performed_at"] = r["performed_at"].isoformat()
            return rows
        finally:
            cur.close()

    def get_dashboard_metrics(self) -> Dict[str, Any]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT COUNT(*) AS n FROM inventory_items WHERE is_active = 1")
            total_items = (cur.fetchone() or {}).get("n", 0) or 0
            cur.execute(
                """
                SELECT COUNT(*) AS n FROM (
                    SELECT i.id FROM inventory_items i
                    LEFT JOIN inventory_stock_levels s ON s.item_id = i.id
                    WHERE i.is_active = 1 AND i.reorder_point > 0
                    GROUP BY i.id
                    HAVING COALESCE(SUM(s.quantity_on_hand), 0) < MAX(i.reorder_point)
                ) t
                """
            )
            low_stock = (cur.fetchone() or {}).get("n", 0) or 0
            cur.execute(
                """
                SELECT COALESCE(SUM(s.quantity_on_hand * COALESCE(i.standard_cost, 0)), 0) AS v
                FROM inventory_stock_levels s
                JOIN inventory_items i ON i.id = s.item_id
                """
            )
            total_value = float((cur.fetchone() or {}).get("v", 0) or 0)
            cur.execute(
                """
                SELECT COUNT(DISTINCT b.id) AS n
                FROM inventory_batches b
                INNER JOIN inventory_stock_levels s
                  ON s.batch_id = b.id AND s.quantity_on_hand > 0
                INNER JOIN inventory_locations l ON l.id = s.location_id
                WHERE b.expiry_date IS NOT NULL
                  AND b.expiry_date <= DATE_ADD(CURDATE(), INTERVAL 30 DAY)
                  AND COALESCE(LOWER(l.type), '') <> 'training'
                """
            )
            expiring_soon = (cur.fetchone() or {}).get("n", 0) or 0
            cur.execute(
                "SELECT COUNT(*) AS n FROM inventory_transactions WHERE performed_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)"
            )
            recent_tx = (cur.fetchone() or {}).get("n", 0) or 0

            # Chart: stock value by category
            cur.execute(
                """
                SELECT COALESCE(i.category_id, 0) AS category_id,
                       COALESCE(c.name, 'Uncategorised') AS category_name,
                       COALESCE(SUM(s.quantity_on_hand * COALESCE(i.standard_cost, 0)), 0) AS value
                FROM inventory_stock_levels s
                JOIN inventory_items i ON i.id = s.item_id
                LEFT JOIN inventory_categories c ON c.id = i.category_id
                GROUP BY i.category_id, c.name
                """
            )
            value_by_category = [
                {"category_id": r.get("category_id"), "category_name": r.get("category_name") or "Uncategorised", "value": round(float(r.get("value") or 0), 2)}
                for r in (cur.fetchall() or [])
            ]

            # Chart: movements by type (last 7 days)
            cur.execute(
                """
                SELECT transaction_type AS type, COUNT(*) AS count
                FROM inventory_transactions
                WHERE performed_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY transaction_type
                """
            )
            movements_by_type = [{"type": r.get("type"), "count": r.get("count", 0)} for r in (cur.fetchall() or [])]

            # Chart: activity last 7 days (count per day)
            cur.execute(
                """
                SELECT DATE(performed_at) AS day, COUNT(*) AS count
                FROM inventory_transactions
                WHERE performed_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY DATE(performed_at)
                ORDER BY day
                """
            )
            movements_by_day = [{"day": (r.get("day").isoformat() if hasattr(r.get("day"), "isoformat") else str(r.get("day"))), "count": r.get("count", 0)} for r in (cur.fetchall() or [])]

            return {
                "total_items": total_items,
                "low_stock_count": low_stock,
                "total_value": round(total_value, 2),
                "expiring_batches_count": expiring_soon,
                "recent_transactions_count": recent_tx,
                "value_by_category": value_by_category,
                "movements_by_type": movements_by_type,
                "movements_by_day": movements_by_day,
            }
        finally:
            cur.close()

    def get_contractor_kit_pending_snapshot(self) -> Dict[str, Any]:
        """Pending portal kit requests — shared by inventory command center and asset hub."""
        snap: Dict[str, Any] = {
            "contractor_kit_pending_count": 0,
            "contractor_kit_pending_rows": [],
        }
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            try:
                cur.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM inventory_contractor_kit_requests
                    WHERE status = 'pending'
                    """
                )
                snap["contractor_kit_pending_count"] = int(
                    (cur.fetchone() or {}).get("n") or 0
                )
            except Exception:
                logger.exception("contractor_kit_pending_snapshot: count")
            try:
                cur.execute(
                    """
                    SELECT k.id, k.contractor_id, k.need_from, k.need_until,
                           k.request_text, k.created_at, c.name AS contractor_name
                    FROM inventory_contractor_kit_requests k
                    INNER JOIN tb_contractors c ON c.id = k.contractor_id
                    WHERE k.status = 'pending'
                    ORDER BY k.created_at ASC
                    LIMIT 6
                    """
                )
                kit_rows = cur.fetchall() or []
                for kr in kit_rows:
                    for key in ("need_from", "need_until", "created_at"):
                        v = kr.get(key)
                        if hasattr(v, "isoformat"):
                            kr[key] = v.isoformat()
                    txt = (kr.get("request_text") or "").strip()
                    kr["request_excerpt"] = (txt[:140] + "…") if len(txt) > 140 else txt
                snap["contractor_kit_pending_rows"] = kit_rows
            except Exception:
                logger.exception("contractor_kit_pending_snapshot: rows")
        finally:
            cur.close()
        return snap

    def get_dashboard_command_center(self) -> Dict[str, Any]:
        """
        Rich context for the admin dashboard: where stock sits, lots nearing expiry,
        low-SKU lines, serialized equipment warranty window, and equipment status counts.
        Queries are defensive so a partial schema never breaks the dashboard.
        """
        out: Dict[str, Any] = {
            "location_summaries": [],
            "expiring_lots": [],
            "expired_lot_rows": [],
            "low_stock_items": [],
            "warranty_watch": [],
            "equipment_by_status": {},
            "equipment_assigned": 0,
            "equipment_in_stock": 0,
            "location_count": 0,
            "recent_moves": [],
            "expiring_within_30d_count": 0,
            "contractor_kit_pending_count": 0,
            "contractor_kit_pending_rows": [],
            "holding_pool_equipment": [],
            "kit_consumable_expired_line_count": 0,
            "kit_consumable_near_line_count": 0,
            "kit_consumable_alert_rows": [],
        }
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            try:
                cur.execute("SELECT COUNT(*) AS n FROM inventory_locations")
                out["location_count"] = int((cur.fetchone() or {}).get("n") or 0)
            except Exception:
                logger.exception("command_center: location_count")

            try:
                cur.execute(
                    """
                    SELECT l.id, l.code, l.name, l.type,
                           COUNT(DISTINCT s.item_id) AS sku_count,
                           COALESCE(SUM(s.quantity_on_hand), 0) AS total_qty
                    FROM inventory_locations l
                    INNER JOIN inventory_stock_levels s
                      ON s.location_id = l.id AND s.quantity_on_hand > 0
                    GROUP BY l.id, l.code, l.name, l.type
                    ORDER BY total_qty DESC
                    LIMIT 14
                    """
                )
                out["location_summaries"] = cur.fetchall() or []
            except Exception:
                logger.exception("command_center: location_summaries")

            try:
                cur.execute(
                    """
                    SELECT b.id AS batch_id, b.batch_number, b.lot_number, b.expiry_date,
                           i.id AS item_id, i.name AS item_name, i.sku,
                           l.id AS location_id, l.code AS location_code, l.name AS location_name,
                           SUM(s.quantity_on_hand) AS qty
                    FROM inventory_batches b
                    INNER JOIN inventory_items i ON i.id = b.item_id
                    INNER JOIN inventory_stock_levels s
                      ON s.batch_id = b.id AND s.quantity_on_hand > 0
                    INNER JOIN inventory_locations l ON l.id = s.location_id
                    WHERE b.expiry_date IS NOT NULL
                      AND b.expiry_date >= CURDATE()
                      AND b.expiry_date <= DATE_ADD(CURDATE(), INTERVAL 90 DAY)
                      AND COALESCE(LOWER(l.type), '') <> 'training'
                    GROUP BY b.id, b.batch_number, b.lot_number, b.expiry_date,
                             i.id, i.name, i.sku, l.id, l.code, l.name
                    ORDER BY b.expiry_date ASC, l.name ASC
                    LIMIT 28
                    """
                )
                rows = cur.fetchall() or []
                today_d = date.today()
                within30 = 0
                for r in rows:
                    ed = r.get("expiry_date")
                    if hasattr(ed, "isoformat") and ed is not None:
                        try:
                            if (ed - today_d).days <= 30:
                                within30 += 1
                        except Exception:
                            pass
                        r["expiry_date"] = ed.isoformat()
                out["expiring_lots"] = rows
                out["expiring_within_30d_count"] = within30
            except Exception:
                logger.exception("command_center: expiring_lots")

            try:
                cur.execute(
                    """
                    SELECT b.id AS batch_id, b.batch_number, i.sku, i.name AS item_name,
                           b.expiry_date, l.id AS location_id, l.code AS location_code,
                           l.name AS location_name, SUM(s.quantity_on_hand) AS qty
                    FROM inventory_batches b
                    INNER JOIN inventory_items i ON i.id = b.item_id
                    INNER JOIN inventory_stock_levels s
                      ON s.batch_id = b.id AND s.quantity_on_hand > 0
                    INNER JOIN inventory_locations l ON l.id = s.location_id
                    WHERE b.expiry_date IS NOT NULL AND b.expiry_date < CURDATE()
                      AND COALESCE(LOWER(l.type), '') <> 'training'
                    GROUP BY b.id, b.batch_number, i.sku, i.name, b.expiry_date,
                             l.id, l.code, l.name
                    ORDER BY b.expiry_date DESC
                    LIMIT 24
                    """
                )
                er = cur.fetchall() or []
                for r in er:
                    ed = r.get("expiry_date")
                    if hasattr(ed, "isoformat"):
                        r["expiry_date"] = ed.isoformat()
                out["expired_lot_rows"] = er
            except Exception:
                logger.exception("command_center: expired_lot_rows")

            try:
                cur.execute(
                    """
                    SELECT i.id, i.sku, i.name, i.reorder_point,
                           COALESCE(SUM(s.quantity_on_hand), 0) AS on_hand
                    FROM inventory_items i
                    LEFT JOIN inventory_stock_levels s ON s.item_id = i.id
                    WHERE i.is_active = 1 AND i.reorder_point > 0
                      AND (i.is_equipment = 0 OR i.is_equipment IS NULL)
                    GROUP BY i.id, i.sku, i.name, i.reorder_point
                    HAVING COALESCE(SUM(s.quantity_on_hand), 0) < i.reorder_point
                    ORDER BY (i.reorder_point - COALESCE(SUM(s.quantity_on_hand), 0)) DESC
                    LIMIT 14
                    """
                )
                out["low_stock_items"] = cur.fetchall() or []
            except Exception:
                logger.exception("command_center: low_stock_items")

            try:
                cur.execute(
                    """
                    SELECT a.id AS asset_id, a.serial_number, a.status,
                           a.warranty_expiry, a.warranty_start_basis, a.warranty_start_date, a.warranty_months,
                           i.name AS item_name, i.sku,
                           DATEDIFF(a.warranty_expiry, CURDATE()) AS days_left
                    FROM inventory_equipment_assets a
                    INNER JOIN inventory_items i ON i.id = a.item_id
                    WHERE a.warranty_expiry IS NOT NULL
                      AND a.status NOT IN ('retired', 'lost')
                      AND a.warranty_expiry <= DATE_ADD(CURDATE(), INTERVAL 90 DAY)
                    ORDER BY a.warranty_expiry ASC
                    LIMIT 18
                    """
                )
                wr = cur.fetchall() or []
                for r in wr:
                    we = r.get("warranty_expiry")
                    if hasattr(we, "isoformat"):
                        r["warranty_expiry"] = we.isoformat()
                    ws = r.get("warranty_start_date")
                    if hasattr(ws, "isoformat"):
                        r["warranty_start_date"] = ws.isoformat()
                out["warranty_watch"] = wr
            except Exception:
                logger.exception("command_center: warranty_watch")

            try:
                cur.execute(
                    """
                    SELECT status, COUNT(*) AS c
                    FROM inventory_equipment_assets
                    GROUP BY status
                    """
                )
                for r in cur.fetchall() or []:
                    st = r.get("status") or ""
                    out["equipment_by_status"][st] = int(r.get("c") or 0)
                out["equipment_assigned"] = int(
                    out["equipment_by_status"].get("assigned", 0)
                    + out["equipment_by_status"].get("loaned", 0)
                )
                out["equipment_in_stock"] = int(
                    out["equipment_by_status"].get("in_stock", 0)
                )
            except Exception:
                logger.exception("command_center: equipment_by_status")

            try:
                cur.execute(
                    """
                    SELECT t.id, t.transaction_type, t.quantity, t.performed_at,
                           i.sku, i.name AS item_name,
                           l.code AS location_code
                    FROM inventory_transactions t
                    LEFT JOIN inventory_items i ON i.id = t.item_id
                    LEFT JOIN inventory_locations l ON l.id = t.location_id
                    ORDER BY t.performed_at DESC
                    LIMIT 10
                    """
                )
                mv = cur.fetchall() or []
                for r in mv:
                    pa = r.get("performed_at")
                    if hasattr(pa, "isoformat"):
                        r["performed_at"] = pa.isoformat()
                out["recent_moves"] = mv
            except Exception:
                logger.exception("command_center: recent_moves")

            try:
                nd_k = 30
                cur.execute(
                    """
                    SELECT
                      SUM(CASE WHEN c.expiry_date < CURDATE() THEN 1 ELSE 0 END) AS expired_lines,
                      SUM(
                        CASE
                          WHEN c.expiry_date >= CURDATE()
                           AND c.expiry_date <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
                          THEN 1 ELSE 0 END
                      ) AS near_lines
                    FROM inventory_equipment_asset_consumables c
                    INNER JOIN inventory_equipment_assets a ON a.id = c.equipment_asset_id
                    WHERE COALESCE(c.depleted, 0) = 0
                      AND COALESCE(c.quantity, 0) > 0
                      AND c.expiry_date IS NOT NULL
                      AND c.expiry_date <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
                      AND (a.status IS NULL OR a.status NOT IN ('retired', 'lost'))
                    """,
                    (nd_k, nd_k),
                )
                kcnt = cur.fetchone() or {}
                out["kit_consumable_expired_line_count"] = int(
                    kcnt.get("expired_lines") or 0
                )
                out["kit_consumable_near_line_count"] = int(kcnt.get("near_lines") or 0)
                cur.execute(
                    """
                    SELECT c.id AS consumable_line_id,
                           c.equipment_asset_id,
                           c.label,
                           c.batch_number,
                           c.lot_number,
                           c.expiry_date,
                           c.quantity,
                           a.public_asset_code,
                           a.serial_number,
                           m.name AS equipment_model_name
                    FROM inventory_equipment_asset_consumables c
                    INNER JOIN inventory_equipment_assets a ON a.id = c.equipment_asset_id
                    LEFT JOIN inventory_items m ON m.id = a.item_id
                    WHERE COALESCE(c.depleted, 0) = 0
                      AND COALESCE(c.quantity, 0) > 0
                      AND c.expiry_date IS NOT NULL
                      AND c.expiry_date <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
                      AND (a.status IS NULL OR a.status NOT IN ('retired', 'lost'))
                    ORDER BY c.expiry_date ASC, c.equipment_asset_id ASC, c.id ASC
                    LIMIT 12
                    """,
                    (nd_k,),
                )
                krows = cur.fetchall() or []
                today_d = date.today()
                for kr in krows:
                    ed = kr.get("expiry_date")
                    if hasattr(ed, "isoformat") and ed is not None:
                        try:
                            kr["days_left"] = (ed - today_d).days
                            kr["alert_tier"] = (
                                "expired" if ed < today_d else "near"
                            )
                        except Exception:
                            kr["days_left"] = None
                            kr["alert_tier"] = "near"
                        kr["expiry_date"] = ed.isoformat()
                out["kit_consumable_alert_rows"] = krows
            except Exception:
                logger.exception("command_center: kit_consumable_alerts")

        finally:
            cur.close()
        try:
            kit_snap = self.get_contractor_kit_pending_snapshot()
            out["contractor_kit_pending_count"] = kit_snap["contractor_kit_pending_count"]
            out["contractor_kit_pending_rows"] = kit_snap["contractor_kit_pending_rows"]
        except Exception:
            logger.exception("command_center: merge kit snapshot")
        try:
            from app.plugins.inventory_control.asset_service import get_asset_service

            out["holding_pool_equipment"] = get_asset_service().list_equipment_in_holding_pool(
                limit=35
            )
        except Exception:
            logger.exception("command_center: holding_pool_equipment")
        return out

    def get_movement_summary(self, days: int = 7) -> str:
        """Narrative summary of inventory movements for dashboard AI summary block."""
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT transaction_type, COUNT(*) AS cnt
                FROM inventory_transactions
                WHERE performed_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                GROUP BY transaction_type
                """,
                (days,),
            )
            by_type = {r["transaction_type"]: r["cnt"] for r in (cur.fetchall() or [])}
            cur.execute(
                "SELECT COUNT(*) AS n FROM (SELECT i.id FROM inventory_items i "
                "LEFT JOIN inventory_stock_levels s ON s.item_id = i.id WHERE i.is_active = 1 AND i.reorder_point > 0 "
                "GROUP BY i.id HAVING COALESCE(SUM(s.quantity_on_hand), 0) < MAX(i.reorder_point)) t"
            )
            low = (cur.fetchone() or {}).get("n", 0) or 0
            cur.execute(
                """
                SELECT COUNT(DISTINCT b.id) AS n
                FROM inventory_batches b
                INNER JOIN inventory_stock_levels s
                  ON s.batch_id = b.id AND s.quantity_on_hand > 0
                INNER JOIN inventory_locations l ON l.id = s.location_id
                WHERE b.expiry_date IS NOT NULL
                  AND b.expiry_date <= DATE_ADD(CURDATE(), INTERVAL 30 DAY)
                  AND COALESCE(LOWER(l.type), '') <> 'training'
                """
            )
            expiring = (cur.fetchone() or {}).get("n", 0) or 0
            ins = by_type.get("in", 0) + by_type.get("return", 0) + by_type.get("repack", 0)
            outs = by_type.get("out", 0)
            transfers = by_type.get("transfer", 0)
            adjs = by_type.get("adjustment", 0) + by_type.get("count", 0)
            parts = []
            if ins or outs or transfers or adjs:
                parts.append(f"In the last {days} days: {ins} receipts, {outs} issues, {transfers} transfers, {adjs} adjustments.")
            if low:
                parts.append(f"{low} item(s) are below the reorder point.")
            if expiring:
                parts.append(f"{expiring} batch(es) with expiry within the next 30 days.")
            try:
                kc = self.count_kit_consumable_expiry_alerts(30)
                ke, kn = kc.get("expired", 0), kc.get("near", 0)
                if ke:
                    parts.append(
                        f"{ke} kit consumable line(s) on serial assets are past expiry (open Equipment → asset to replace)."
                    )
                if kn:
                    parts.append(
                        f"{kn} kit consumable line(s) due within 30 days on serial assets."
                    )
            except Exception:
                pass
            if not parts:
                parts.append("No recent movements. Stock levels are stable.")
            return " ".join(parts)
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # Stock / transactions
    # ------------------------------------------------------------------
    def _get_item_cost_method(self, conn, item_id: int) -> str:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT cost_method FROM inventory_items WHERE id = %s", (item_id,)
            )
            row = cur.fetchone()
            if not row:
                return "AVG"
            return row[0] or "AVG"
        finally:
            cur.close()

    def _update_average_cost(
        self, conn, item_id: int, quantity: float, unit_cost: float
    ) -> None:
        """
        Update the moving average cost for an item when new stock arrives.
        """
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT standard_cost FROM inventory_items WHERE id = %s", (item_id,)
            )
            row = cur.fetchone()
            current_avg = float(row["standard_cost"]) if row and row["standard_cost"] is not None else 0.0

            cur.execute(
                "SELECT COALESCE(SUM(quantity_on_hand),0) AS qoh "
                "FROM inventory_stock_levels WHERE item_id = %s",
                (item_id,),
            )
            row2 = cur.fetchone()
            current_qoh = float(row2["qoh"]) if row2 and row2["qoh"] is not None else 0.0

            new_qoh = current_qoh + float(quantity)
            if new_qoh <= 0:
                new_avg = unit_cost
            else:
                new_avg = ((current_qoh * current_avg) + (quantity * unit_cost)) / new_qoh

            cur2 = conn.cursor()
            try:
                cur2.execute(
                    "UPDATE inventory_items SET standard_cost = %s, last_cost = %s WHERE id = %s",
                    (new_avg, unit_cost, item_id),
                )
                conn.commit()
            finally:
                cur2.close()
        finally:
            cur.close()

    def _upsert_stock_level(
        self,
        conn,
        *,
        item_id: int,
        location_id: int,
        batch_id: Optional[int],
        delta_qty: float,
        transaction_id: int,
    ) -> Dict[str, Any]:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT * FROM inventory_stock_levels
                WHERE item_id = %s AND location_id = %s AND (batch_id <=> %s)
                """,
                (item_id, location_id, batch_id),
            )
            row = cur.fetchone()
        finally:
            cur.close()

        if row:
            new_qoh = float(row["quantity_on_hand"]) + float(delta_qty)
            new_reserved = float(row["quantity_reserved"])
            new_available = new_qoh - new_reserved
            cur2 = conn.cursor()
            try:
                cur2.execute(
                    """
                    UPDATE inventory_stock_levels
                    SET quantity_on_hand = %s,
                        quantity_reserved = %s,
                        quantity_available = %s,
                        last_transaction_id = %s
                    WHERE id = %s
                    """,
                    (new_qoh, new_reserved, new_available, transaction_id, row["id"]),
                )
                conn.commit()
            finally:
                cur2.close()
            row["quantity_on_hand"] = new_qoh
            row["quantity_reserved"] = new_reserved
            row["quantity_available"] = new_available
            row["last_transaction_id"] = transaction_id
            return row

        qoh = float(delta_qty)
        reserved = 0.0
        available = qoh - reserved
        cur3 = conn.cursor()
        try:
            cur3.execute(
                """
                INSERT INTO inventory_stock_levels
                    (item_id, location_id, batch_id,
                     quantity_on_hand, quantity_reserved, quantity_available,
                     last_transaction_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (item_id, location_id, batch_id, qoh, reserved, available, transaction_id),
            )
            conn.commit()
            stock_id = cur3.lastrowid
        finally:
            cur3.close()

        return {
            "id": stock_id,
            "item_id": item_id,
            "location_id": location_id,
            "batch_id": batch_id,
            "quantity_on_hand": qoh,
            "quantity_reserved": reserved,
            "quantity_available": available,
            "last_transaction_id": transaction_id,
        }

    def record_transaction(
        self,
        *,
        item_id: int,
        location_id: int,
        quantity: float,
        transaction_type: str,
        batch_id: Optional[int] = None,
        unit_cost: Optional[float] = None,
        reference_type: Optional[str] = None,
        reference_id: Optional[str] = None,
        performed_by_user_id: Optional[Any] = None,
        assignee_type: Optional[str] = None,
        assignee_id: Optional[str] = None,
        assignee_label: Optional[str] = None,
        is_loan: bool = False,
        due_back_date: Optional[str] = None,
        equipment_asset_id: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        client_action_id: Optional[str] = None,
        weight: Optional[float] = None,
        weight_uom: Optional[str] = None,
        uom: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Record a stock movement and update denormalised stock levels.
        """
        conn = self._connection()
        cost_method = self._get_item_cost_method(conn, item_id)
        if cost_method == "FIFO":
            strategy = self._fifo_costing
        elif cost_method == "LIFO":
            strategy = self._lifo_costing
        else:
            strategy = self._avg_costing
        unit_cost_eff, total_cost = strategy.compute_cost(
            conn=conn,
            item_id=item_id,
            quantity=quantity,
            transaction_type=transaction_type,
            explicit_unit_cost=unit_cost,
            location_id=location_id,
            batch_id=batch_id,
        )

        # Direction of stock movement
        if transaction_type in ("in", "return", "count"):
            delta_qty = float(quantity)
        elif transaction_type in ("out", "transfer"):
            delta_qty = -float(quantity)
        elif transaction_type == "adjustment":
            delta_qty = float(quantity)
        elif transaction_type == "repack":
            delta_qty = float(quantity)  # caller uses positive for in, negative for out
        else:
            raise ValueError(f"Unsupported transaction_type: {transaction_type}")

        # Weight: explicit params override metadata
        weight_val = weight
        weight_uom_val = weight_uom
        if weight_val is None and (metadata or {}).get("weight") is not None:
            weight_val = (metadata or {}).get("weight")
            weight_uom_val = (metadata or {}).get("weight_uom") or weight_uom_val
        uom_val = uom

        performed_by_db = _coerce_int_user_id(performed_by_user_id)
        tx_metadata = dict(metadata) if isinstance(metadata, dict) else ({} if metadata is None else metadata)
        if performed_by_db is None and performed_by_user_id is not None and isinstance(tx_metadata, dict):
            tx_metadata.setdefault("performed_by_user", str(performed_by_user_id))
        if isinstance(tx_metadata, dict):
            if assignee_type:
                tx_metadata.setdefault("assignee_type", assignee_type)
            if assignee_id:
                tx_metadata.setdefault("assignee_id", assignee_id)
            if assignee_label:
                tx_metadata.setdefault("assignee_label", assignee_label)
            if is_loan:
                tx_metadata.setdefault("is_loan", True)
            if due_back_date:
                tx_metadata.setdefault("due_back_date", due_back_date)
            if equipment_asset_id:
                tx_metadata.setdefault("equipment_asset_id", int(equipment_asset_id))

        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO inventory_transactions
                    (item_id, location_id, batch_id, transaction_type,
                     quantity, uom, unit_cost, total_cost,
                     reference_type, reference_id,
                     performed_by_user_id,
                     assignee_type, assignee_id, assignee_label,
                     is_loan, due_back_date, equipment_asset_id,
                     metadata, client_action_id,
                     weight, weight_uom)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    item_id,
                    location_id,
                    batch_id,
                    transaction_type,
                    delta_qty,
                    uom_val,
                    unit_cost_eff,
                    total_cost,
                    reference_type,
                    reference_id,
                    performed_by_db,
                    assignee_type,
                    assignee_id,
                    assignee_label,
                    1 if is_loan else 0,
                    due_back_date,
                    equipment_asset_id,
                    json.dumps(tx_metadata) if isinstance(tx_metadata, dict) else tx_metadata,
                    client_action_id,
                    weight_val,
                    weight_uom_val,
                ),
            )
            conn.commit()
            tx_id = cur.lastrowid
        finally:
            cur.close()

        # Update average cost for inbound stock
        if transaction_type in ("in", "return", "repack") and delta_qty > 0 and unit_cost_eff is not None:
            self._update_average_cost(conn, item_id, delta_qty, unit_cost_eff)

        stock_row = self._upsert_stock_level(
            conn,
            item_id=item_id,
            location_id=location_id,
            batch_id=batch_id,
            delta_qty=delta_qty,
            transaction_id=tx_id,
        )

        return {
            "transaction_id": tx_id,
            "unit_cost": unit_cost_eff,
            "total_cost": total_cost,
            "stock": stock_row,
        }

    def get_stock_levels(
        self,
        *,
        item_id: int,
        location_id: Optional[int] = None,
        batch_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            sql = "SELECT * FROM inventory_stock_levels WHERE item_id = %s"
            params = [item_id]
            if location_id is not None:
                sql += " AND location_id = %s"
                params.append(location_id)
            if batch_id is not None:
                sql += " AND (batch_id <=> %s)"
                params.append(batch_id)
            cur.execute(sql, tuple(params))
            row = cur.fetchone()
            return row or {
                "item_id": item_id,
                "location_id": location_id,
                "batch_id": batch_id,
                "quantity_on_hand": 0.0,
                "quantity_reserved": 0.0,
                "quantity_available": 0.0,
            }
        finally:
            cur.close()

    def transfer_stock(
        self,
        *,
        item_id: int,
        from_location_id: int,
        to_location_id: int,
        quantity: float,
        batch_id: Optional[int] = None,
        performed_by_user_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Transfer stock between locations by recording an 'out' then an 'in'.
        """
        out_tx = self.record_transaction(
            item_id=item_id,
            location_id=from_location_id,
            quantity=quantity,
            transaction_type="transfer",
            batch_id=batch_id,
            performed_by_user_id=performed_by_user_id,
            reference_type="transfer",
        )
        in_tx = self.record_transaction(
            item_id=item_id,
            location_id=to_location_id,
            quantity=quantity,
            transaction_type="in",
            batch_id=batch_id,
            performed_by_user_id=performed_by_user_id,
            reference_type="transfer",
        )
        return {"from": out_tx, "to": in_tx}

    def get_training_pool_location_id(self) -> Optional[int]:
        """Reserved location TRAINING-POOL (type training) for OOD stock kept for drills."""
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id FROM inventory_locations WHERE code = %s LIMIT 1",
                ("TRAINING-POOL",),
            )
            r = cur.fetchone()
            return int(r[0]) if r else None
        finally:
            cur.close()

    def transfer_batch_stock_to_training_pool(
        self,
        *,
        batch_id: int,
        from_location_id: int,
        quantity: float,
        performed_by_user_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Move on-hand dated stock into the training pool (patient-care expiry dashboards ignore it there)."""
        pool_id = self.get_training_pool_location_id()
        if not pool_id:
            return {"ok": False, "error": "training_pool_missing"}
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT item_id FROM inventory_batches WHERE id = %s", (int(batch_id),))
            br = cur.fetchone()
            if not br:
                return {"ok": False, "error": "batch_not_found"}
            item_id = int(br["item_id"])
        finally:
            cur.close()
        st = self.get_stock_levels(
            item_id=item_id, location_id=from_location_id, batch_id=int(batch_id)
        )
        qoh = float(st.get("quantity_on_hand") or 0)
        try:
            q = float(quantity)
        except (TypeError, ValueError):
            return {"ok": False, "error": "quantity_invalid"}
        if q <= 0 or q > qoh + 1e-9:
            return {"ok": False, "error": "quantity_invalid", "on_hand": qoh}
        self.transfer_stock(
            item_id=item_id,
            from_location_id=int(from_location_id),
            to_location_id=int(pool_id),
            quantity=q,
            batch_id=int(batch_id),
            performed_by_user_id=performed_by_user_id,
        )
        return {"ok": True}

    def dispose_batch_stock_ood(
        self,
        *,
        batch_id: int,
        from_location_id: int,
        quantity: float,
        performed_by_user_id: Optional[Any] = None,
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Remove on-hand stock for an out-of-date lot (patient use) with audit reference."""
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT item_id FROM inventory_batches WHERE id = %s", (int(batch_id),))
            br = cur.fetchone()
            if not br:
                return {"ok": False, "error": "batch_not_found"}
            item_id = int(br["item_id"])
        finally:
            cur.close()
        st = self.get_stock_levels(
            item_id=item_id, location_id=int(from_location_id), batch_id=int(batch_id)
        )
        qoh = float(st.get("quantity_on_hand") or 0)
        try:
            q = float(quantity)
        except (TypeError, ValueError):
            return {"ok": False, "error": "quantity_invalid"}
        if q <= 0 or q > qoh + 1e-9:
            return {"ok": False, "error": "quantity_invalid", "on_hand": qoh}
        meta = {"reason": "ood_disposal", "notes": (notes or "")[:500]}
        self.record_transaction(
            item_id=item_id,
            location_id=int(from_location_id),
            quantity=q,
            transaction_type="out",
            batch_id=int(batch_id),
            performed_by_user_id=performed_by_user_id,
            reference_type="ood_disposal",
            reference_id=str(int(batch_id)),
            metadata=meta,
        )
        return {"ok": True}

    def repack(
        self,
        *,
        source_batch_id: int,
        location_id: int,
        outputs: List[Dict[str, Any]],
        performed_by_user_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        One source batch -> multiple new batches. Each output has quantity and
        optional weight, weight_uom, batch_number, lot_number.
        """
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM inventory_batches WHERE id = %s", (source_batch_id,))
            source_batch = cur.fetchone()
        finally:
            cur.close()
        if not source_batch:
            raise ValueError("Source batch not found")
        item_id = source_batch["item_id"]
        total_out = sum(float(o.get("quantity", 0)) for o in outputs)
        if total_out <= 0:
            raise ValueError("Outputs must have positive quantities")
        stock = self.get_stock_levels(item_id=item_id, location_id=location_id, batch_id=source_batch_id)
        qoh = float(stock.get("quantity_on_hand", 0))
        if qoh < total_out:
            raise ValueError(f"Insufficient stock: have {qoh}, need {total_out}")
        repack_id = str(uuid.uuid4())
        out_tx = self.record_transaction(
            item_id=item_id,
            location_id=location_id,
            quantity=-total_out,
            transaction_type="repack",
            batch_id=source_batch_id,
            reference_type="repack",
            reference_id=repack_id,
            performed_by_user_id=performed_by_user_id,
            metadata={"repack_id": repack_id},
        )
        new_batches = []
        for o in outputs:
            qty = float(o.get("quantity", 0))
            if qty <= 0:
                continue
            batch_data = {
                "item_id": item_id,
                "batch_number": o.get("batch_number"),
                "lot_number": o.get("lot_number"),
                "weight": o.get("weight"),
                "weight_uom": o.get("weight_uom"),
                "unit_weight": o.get("unit_weight"),
                "unit_weight_uom": o.get("unit_weight_uom"),
            }
            new_batch_id = self.create_batch(batch_data)
            self.record_transaction(
                item_id=item_id,
                location_id=location_id,
                quantity=qty,
                transaction_type="repack",
                batch_id=new_batch_id,
                reference_type="repack",
                reference_id=repack_id,
                performed_by_user_id=performed_by_user_id,
                weight=o.get("weight"),
                weight_uom=o.get("weight_uom"),
                metadata={"repack_id": repack_id},
            )
            new_batches.append({"batch_id": new_batch_id, "quantity": qty})
        return {"repack_id": repack_id, "out_transaction_id": out_tx["transaction_id"], "new_batches": new_batches}

    def get_picking_suggestions_fefo(
        self,
        item_id: int,
        location_id: int,
        quantity: float,
    ) -> List[Dict[str, Any]]:
        """
        FEFO: suggest batches to pick for an outbound movement, ordered by expiry (soonest first).
        Returns list of { batch_id, batch_number, lot_number, expiry_date, quantity_available, quantity_to_take }.
        """
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT s.batch_id, s.quantity_on_hand AS quantity_available,
                       b.batch_number, b.lot_number, b.expiry_date
                FROM inventory_stock_levels s
                JOIN inventory_batches b ON b.id = s.batch_id
                WHERE s.item_id = %s AND s.location_id = %s AND s.batch_id IS NOT NULL
                AND s.quantity_on_hand > 0
                ORDER BY COALESCE(b.expiry_date, '9999-12-31') ASC
                """,
                (item_id, location_id),
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()
        remaining = float(quantity)
        result = []
        for r in rows:
            if remaining <= 0:
                break
            avail = float(r["quantity_available"])
            take = min(avail, remaining)
            if take <= 0:
                continue
            result.append({
                "batch_id": r["batch_id"],
                "batch_number": r.get("batch_number"),
                "lot_number": r.get("lot_number"),
                "expiry_date": r.get("expiry_date").isoformat() if r.get("expiry_date") else None,
                "quantity_available": avail,
                "quantity_to_take": take,
            })
            remaining -= take
        return result

    def rollback_transaction(self, transaction_id: int) -> None:
        """
        Simple rollback: create a compensating transaction with opposite sign.
        """
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM inventory_transactions WHERE id = %s", (transaction_id,)
            )
            tx = cur.fetchone()
        finally:
            cur.close()

        if not tx:
            return
        if tx.get("reversed_transaction_id"):
            return

        if tx["transaction_type"] in ("out", "transfer"):
            inverse_type = "in"
        elif tx["transaction_type"] == "repack" and float(tx["quantity"]) < 0:
            inverse_type = "repack"
        else:
            inverse_type = "out"
        inverse_qty = abs(float(tx["quantity"]))

        inverse = self.record_transaction(
            item_id=tx["item_id"],
            location_id=tx["location_id"],
            quantity=inverse_qty,
            transaction_type=inverse_type,
            batch_id=tx.get("batch_id"),
            unit_cost=tx.get("unit_cost"),
            reference_type="rollback",
            reference_id=str(tx["id"]),
            performed_by_user_id=tx.get("performed_by_user_id"),
            metadata={"rollback_of": tx["id"]},
        )

        cur2 = conn.cursor()
        try:
            cur2.execute(
                "UPDATE inventory_transactions SET reversed_transaction_id = %s WHERE id = %s",
                (inverse["transaction_id"], transaction_id),
            )
            conn.commit()
        finally:
            cur2.close()


    def find_item_by_sku_or_barcode(self, sku_or_barcode: str) -> Optional[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM inventory_items WHERE (sku = %s OR barcode = %s) AND is_active = 1",
                (sku_or_barcode, sku_or_barcode),
            )
            return cur.fetchone()
        finally:
            cur.close()

    def _normalize_invoice_date(self, value: Optional[str]) -> Optional[str]:
        """Return YYYY-MM-DD string or None for MySQL DATE column (reject empty or invalid)."""
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
        if not value:
            return None
        if isinstance(value, str) and len(value) == 10 and value[4] == "-" and value[7] == "-":
            try:
                datetime.strptime(value, "%Y-%m-%d")
                return value
            except ValueError:
                pass
        return None

    def create_invoice_record(
        self,
        *,
        supplier_id: Optional[int] = None,
        external_source: Optional[str] = None,
        external_invoice_id: Optional[str] = None,
        invoice_number: Optional[str] = None,
        invoice_date: Optional[str] = None,
        total_amount: Optional[float] = None,
        currency: Optional[str] = None,
        status: str = "parsed",
        raw_file_path: Optional[str] = None,
        parsed_payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        invoice_date_sql = self._normalize_invoice_date(invoice_date)
        invoice_number_sql = (invoice_number or "").strip() or None
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO inventory_invoices
                    (supplier_id, external_source, external_invoice_id, invoice_number,
                     invoice_date, total_amount, currency, status, raw_file_path, parsed_payload)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    supplier_id,
                    external_source,
                    external_invoice_id,
                    invoice_number_sql,
                    invoice_date_sql,
                    total_amount,
                    currency,
                    status,
                    raw_file_path,
                    json.dumps(parsed_payload) if parsed_payload else None,
                ),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()

    def add_invoice_line(
        self,
        *,
        invoice_id: int,
        item_id: Optional[int] = None,
        sku: Optional[str] = None,
        description: Optional[str] = None,
        quantity: float = 0,
        unit_price: Optional[float] = None,
        line_total: Optional[float] = None,
        external_item_ref: Optional[str] = None,
        match_status: str = "unmapped",
    ) -> int:
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO inventory_invoice_lines
                    (invoice_id, item_id, sku, description, quantity, unit_price, line_total,
                     external_item_ref, match_status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    invoice_id,
                    item_id,
                    sku,
                    description,
                    quantity,
                    unit_price,
                    line_total,
                    external_item_ref,
                    match_status,
                ),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()

    def get_invoice(self, invoice_id: int) -> Optional[Dict[str, Any]]:
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM inventory_invoices WHERE id = %s", (invoice_id,))
            return cur.fetchone()
        finally:
            cur.close()

    def list_invoices(
        self,
        *,
        supplier_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 100,
        skip: int = 0,
    ):
        """List invoices with optional supplier filter (for supplier API)."""
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            sql = "SELECT * FROM inventory_invoices WHERE 1=1"
            params = []
            if supplier_id is not None:
                sql += " AND supplier_id = %s"
                params.append(supplier_id)
            if status:
                sql += " AND status = %s"
                params.append(status)
            sql += " ORDER BY invoice_date DESC, id DESC LIMIT %s OFFSET %s"
            params.extend([limit, skip])
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
        finally:
            cur.close()

    def get_invoice_lines(self, invoice_id: int):
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM inventory_invoice_lines WHERE invoice_id = %s ORDER BY id",
                (invoice_id,),
            )
            return cur.fetchall() or []
        finally:
            cur.close()

    def update_invoice(self, invoice_id: int, **kwargs: Any) -> None:
        """Update invoice header. Pass only fields to update; None clears (e.g. supplier_id)."""
        allowed = ("supplier_id", "external_source", "invoice_number", "invoice_date", "total_amount", "currency")
        updates: List[str] = []
        params: List[Any] = []
        for key in allowed:
            if key not in kwargs:
                continue
            val = kwargs[key]
            if key == "invoice_date":
                val = self._normalize_invoice_date(val) if val else None
            elif key == "invoice_number":
                val = (val or "").strip() or None
            updates.append(f"{key} = %s")
            params.append(val)
        if not updates:
            return
        params.append(invoice_id)
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE inventory_invoices SET " + ", ".join(updates) + " WHERE id = %s",
                tuple(params),
            )
            conn.commit()
        finally:
            cur.close()

    def update_invoice_line(
        self,
        line_id: int,
        *,
        sku: Optional[str] = None,
        description: Optional[str] = None,
        quantity: Optional[float] = None,
        unit_price: Optional[float] = None,
        line_total: Optional[float] = None,
    ) -> None:
        updates = []
        params: List[Any] = []
        if sku is not None:
            updates.append("sku = %s")
            params.append((sku or "").strip() or None)
        if description is not None:
            updates.append("description = %s")
            params.append(description)
        if quantity is not None:
            updates.append("quantity = %s")
            params.append(quantity)
        if unit_price is not None:
            updates.append("unit_price = %s")
            params.append(unit_price)
        if line_total is not None:
            updates.append("line_total = %s")
            params.append(line_total)
        if not updates:
            return
        params.append(line_id)
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE inventory_invoice_lines SET " + ", ".join(updates) + " WHERE id = %s",
                tuple(params),
            )
            conn.commit()
        finally:
            cur.close()

    def update_invoice_line_item(self, line_id: int, item_id: Optional[int]) -> None:
        conn = self._connection()
        cur = conn.cursor()
        try:
            status = "matched" if item_id else "unmapped"
            cur.execute(
                "UPDATE inventory_invoice_lines SET item_id = %s, match_status = %s WHERE id = %s",
                (item_id, status, line_id),
            )
            conn.commit()
        finally:
            cur.close()

    def apply_invoice_to_stock(
        self,
        invoice_id: int,
        location_id: int,
        *,
        performed_by_user_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        lines = self.get_invoice_lines(invoice_id)
        applied = 0
        errors = []
        for line in lines:
            item_id = line.get("item_id")
            if not item_id:
                errors.append({"line_id": line["id"], "reason": "no item mapped"})
                continue
            try:
                self.record_transaction(
                    item_id=item_id,
                    location_id=location_id,
                    quantity=float(line.get("quantity", 0)),
                    transaction_type="in",
                    unit_cost=line.get("unit_price"),
                    reference_type="invoice",
                    reference_id=str(invoice_id),
                    performed_by_user_id=performed_by_user_id,
                )
                applied += 1
            except Exception as e:
                errors.append({"line_id": line["id"], "reason": str(e)})
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE inventory_invoices SET status = 'applied' WHERE id = %s",
                (invoice_id,),
            )
            conn.commit()
        finally:
            cur.close()
        return {"applied": applied, "errors": errors}

    def list_purchase_orders(
        self,
        *,
        supplier_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 100,
        skip: int = 0,
    ):
        """List purchase orders (for supplier API: view PO status)."""
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            sql = "SELECT * FROM inventory_purchase_orders WHERE 1=1"
            params = []
            if supplier_id is not None:
                sql += " AND supplier_id = %s"
                params.append(supplier_id)
            if status:
                sql += " AND status = %s"
                params.append(status)
            sql += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, skip])
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
        except Exception:
            # Table may not exist yet
            return []
        finally:
            cur.close()

    def list_supplier_documents(
        self,
        *,
        supplier_id: int,
        document_type: Optional[str] = None,
        limit: int = 100,
        skip: int = 0,
    ):
        """List documents (compliance, etc.) for a supplier."""
        conn = self._connection()
        cur = conn.cursor(dictionary=True)
        try:
            sql = "SELECT id, supplier_id, name, document_type, file_path, uploaded_at, uploaded_by_token_id FROM inventory_supplier_documents WHERE supplier_id = %s"
            params = [supplier_id]
            if document_type:
                sql += " AND document_type = %s"
                params.append(document_type)
            sql += " ORDER BY uploaded_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, skip])
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
        except Exception:
            return []
        finally:
            cur.close()

    def create_supplier_document(
        self,
        supplier_id: int,
        name: str,
        file_path: str,
        *,
        document_type: str = "compliance",
        uploaded_by_token_id: Optional[int] = None,
    ) -> int:
        """Record a supplier document (e.g. compliance upload). Returns document id."""
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO inventory_supplier_documents
                   (supplier_id, name, document_type, file_path, uploaded_by_token_id)
                   VALUES (%s, %s, %s, %s, %s)""",
                (supplier_id, name, document_type, file_path, uploaded_by_token_id),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()


def get_inventory_service() -> InventoryService:
    return InventoryService()


