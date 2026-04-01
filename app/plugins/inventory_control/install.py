import json
import sys
import os
from pathlib import Path

# Bootstrap import paths (project/app/plugin)
HERE = Path(__file__).resolve()
PLUGIN_DIR = HERE.parent
PLUGINS_DIR = PLUGIN_DIR.parent
APP_ROOT = PLUGINS_DIR.parent
PROJECT_ROOT = APP_ROOT.parent

for p in (str(PROJECT_ROOT), str(APP_ROOT), str(PLUGIN_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from app.objects import get_db_connection  # noqa: E402


def _create_table(conn, name: str, columns_sql: str) -> None:
    """Create a table if it does not already exist."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{name}` (
                {columns_sql}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
        print(f"[inventory_control] Ensured table exists: {name}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _alter_add_column(conn, table: str, col_def: str) -> None:
    """Idempotent add column; col_def is e.g. 'weight DECIMAL(18,6) NULL'."""
    parts = col_def.strip().split(None, 1)
    col_name = parts[0]
    rest = parts[1] if len(parts) > 1 else ""
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{col_name}` {rest}")
        conn.commit()
        print(f"[inventory_control] Added column {table}.{col_name}")
    except Exception as e:
        if "Duplicate column" in str(e):
            pass
        else:
            print(f"[inventory_control] ALTER {table}.{col_name}: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _alter_modify_enum(conn, table: str, column: str, new_type: str) -> None:
    """Idempotent modify column type (e.g. extend ENUM)."""
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` {new_type}")
        conn.commit()
        print(f"[inventory_control] Modified {table}.{column}")
    except Exception as e:
        print(f"[inventory_control] MODIFY {table}.{column}: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _ensure_default_holding_location(conn) -> None:
    """
    Default reconciliation / limbo location for post-event or temporary-site returns.
    Staff verify condition, faults, and final putaway before moving to a main store.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM inventory_locations WHERE code = %s LIMIT 1",
            ("HOLD-IN",),
        )
        if cur.fetchone():
            return
        meta = json.dumps(
            {
                "purpose": "Holding pool — verify condition, log faults/maintenance, then put away to the correct store.",
            }
        )
        cur.execute(
            """
            INSERT INTO inventory_locations (name, code, type, parent_location_id, address, metadata)
            VALUES (%s, %s, %s, NULL, NULL, CAST(%s AS JSON))
            """,
            (
                "Kit return / holding (reconciliation)",
                "HOLD-IN",
                "holding",
                meta,
            ),
        )
        conn.commit()
        print("[inventory_control] Ensured default holding location HOLD-IN")
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[inventory_control] Default holding location: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _upgrade_equipment_assets(conn) -> None:
    """Add equipment asset columns for existing installs (idempotent)."""
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
        "warranty_start_basis VARCHAR(24) NULL",
    )
    _alter_add_column(conn, "inventory_equipment_assets", "warranty_start_date DATE NULL")
    _alter_add_column(conn, "inventory_equipment_assets", "warranty_months INT NULL")


def _ensure_asset_extension_schema(conn) -> None:
    """
    Maintenance events and equipment issues (formerly the asset_management plugin).
    Idempotent; safe after inventory_equipment_assets exists.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS asset_maintenance_events (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                equipment_asset_id BIGINT NOT NULL,
                service_date DATE NOT NULL,
                service_type VARCHAR(128) NOT NULL,
                notes TEXT NULL,
                cost DECIMAL(18,2) NULL,
                performed_by VARCHAR(255) NULL,
                created_by CHAR(36) NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_asset_maint_asset (equipment_asset_id),
                INDEX idx_asset_maint_service_date (service_date),
                CONSTRAINT fk_asset_maint_equipment
                    FOREIGN KEY (equipment_asset_id) REFERENCES inventory_equipment_assets(id)
                    ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
        print("[inventory_control] Ensured asset_maintenance_events")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS asset_equipment_issues (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                equipment_asset_id BIGINT NOT NULL,
                reported_by_user_id CHAR(36) NULL,
                title VARCHAR(255) NOT NULL,
                description TEXT NULL,
                severity ENUM('info','low','medium','high','critical') NOT NULL DEFAULT 'medium',
                status ENUM('open','monitoring','fix_planned','off_service','sent_external','resolved','closed') NOT NULL DEFAULT 'open',
                scheduled_action_date DATE NULL,
                external_reference VARCHAR(255) NULL,
                resolution_notes TEXT NULL,
                resolved_at TIMESTAMP NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_aei_asset (equipment_asset_id),
                INDEX idx_aei_status (status),
                CONSTRAINT fk_aei_equipment FOREIGN KEY (equipment_asset_id)
                    REFERENCES inventory_equipment_assets(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
        print("[inventory_control] Ensured asset_equipment_issues")
    finally:
        try:
            cur.close()
        except Exception:
            pass

    _alter_add_column(conn, "inventory_equipment_assets", "next_service_due_date DATE NULL")
    _alter_add_column(
        conn,
        "inventory_equipment_assets",
        "operational_state ENUM('operational','restricted','unserviceable') NOT NULL DEFAULT 'operational'",
    )


def install():
    """
    Install or update the Inventory Control module database schema.

    Idempotent and safe to call multiple times. Creates core tables and adds
    any missing columns (e.g. equipment fields, assignee/loan columns, weight).
    Use upgrade() for the same behaviour when running migrations only.

    Core tables: inventory_items, inventory_locations, inventory_batches,
    inventory_stock_levels, inventory_transactions, inventory_equipment_assets,
    inventory_suppliers, inventory_invoices, inventory_invoice_lines,
    inventory_supplier_performance, inventory_audit, inventory_categories, etc.
    """
    conn = get_db_connection()
    try:
        # Core item master
        _create_table(
            conn,
            "inventory_items",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            sku VARCHAR(120) NOT NULL,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            barcode VARCHAR(255),
            qr_code_data VARCHAR(512),
            category VARCHAR(255),
            unit VARCHAR(64),
            default_location_id INT NULL,
            reorder_point DECIMAL(18, 4) NOT NULL DEFAULT 0,
            reorder_quantity DECIMAL(18, 4) NOT NULL DEFAULT 0,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            is_equipment TINYINT(1) NOT NULL DEFAULT 0,
            requires_serial TINYINT(1) NOT NULL DEFAULT 0,
            cost_method ENUM('FIFO', 'LIFO', 'AVG') NOT NULL DEFAULT 'AVG',
            standard_cost DECIMAL(18, 4) NULL,
            last_cost DECIMAL(18, 4) NULL,
            primary_supplier_id INT NULL,
            lead_time_days INT NULL,
            external_sku VARCHAR(255),
            metadata JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_inventory_items_sku (sku),
            INDEX idx_inventory_items_name (name)
            """,
        )

        # Ensure new columns exist on older installs
        _alter_add_column(conn, "inventory_items", "is_equipment TINYINT(1) NOT NULL DEFAULT 0")
        _alter_add_column(conn, "inventory_items", "requires_serial TINYINT(1) NOT NULL DEFAULT 0")

        # Locations (warehouses, bins, virtual)
        _create_table(
            conn,
            "inventory_locations",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            code VARCHAR(120) NOT NULL,
            type VARCHAR(64) NOT NULL DEFAULT 'warehouse',
            parent_location_id INT NULL,
            address TEXT,
            metadata JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_inventory_locations_code (code),
            INDEX idx_inventory_locations_name (name)
            """,
        )

        # Batches / lots
        _create_table(
            conn,
            "inventory_batches",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            item_id INT NOT NULL,
            batch_number VARCHAR(255),
            lot_number VARCHAR(255),
            expiry_date DATE NULL,
            manufacture_date DATE NULL,
            received_date DATE NULL,
            supplier_id INT NULL,
            metadata JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_inventory_batches_item (item_id),
            INDEX idx_inventory_batches_expiry (expiry_date),
            INDEX idx_inventory_batches_supplier (supplier_id)
            """,
        )

        # Denormalised stock levels
        _create_table(
            conn,
            "inventory_stock_levels",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            item_id INT NOT NULL,
            location_id INT NOT NULL,
            batch_id INT NULL,
            quantity_on_hand DECIMAL(18, 4) NOT NULL DEFAULT 0,
            quantity_reserved DECIMAL(18, 4) NOT NULL DEFAULT 0,
            quantity_available DECIMAL(18, 4) NOT NULL DEFAULT 0,
            last_transaction_id INT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_inventory_stock_levels (item_id, location_id, batch_id),
            INDEX idx_inventory_stock_item (item_id),
            INDEX idx_inventory_stock_location (location_id)
            """,
        )

        # Immutable transaction history
        _create_table(
            conn,
            "inventory_transactions",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            item_id INT NOT NULL,
            location_id INT NOT NULL,
            batch_id INT NULL,
            transaction_type ENUM('in','out','adjustment','transfer','count','return') NOT NULL,
            quantity DECIMAL(18, 4) NOT NULL,
            uom VARCHAR(64),
            unit_cost DECIMAL(18, 6) NULL,
            total_cost DECIMAL(18, 6) NULL,
            reference_type VARCHAR(64),
            reference_id VARCHAR(255),
            performed_by_user_id INT NULL,
            assignee_type VARCHAR(16) NULL,
            assignee_id VARCHAR(64) NULL,
            assignee_label VARCHAR(255) NULL,
            is_loan TINYINT(1) NOT NULL DEFAULT 0,
            due_back_date DATE NULL,
            equipment_asset_id BIGINT NULL,
            performed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            metadata JSON,
            weight DECIMAL(18, 6) NULL,
            weight_uom VARCHAR(32) NULL,
            reversed_transaction_id BIGINT NULL,
            client_action_id VARCHAR(64) NULL,
            INDEX idx_inventory_tx_item (item_id),
            INDEX idx_inventory_tx_location (location_id),
            INDEX idx_inventory_tx_batch (batch_id),
            INDEX idx_inventory_tx_performed_at (performed_at),
            INDEX idx_inventory_tx_reference (reference_type, reference_id),
            INDEX idx_inventory_tx_client_action (client_action_id)
            """,
        )

        _alter_add_column(conn, "inventory_transactions", "assignee_type VARCHAR(16) NULL")
        _alter_add_column(conn, "inventory_transactions", "assignee_id VARCHAR(64) NULL")
        _alter_add_column(conn, "inventory_transactions", "assignee_label VARCHAR(255) NULL")
        _alter_add_column(conn, "inventory_transactions", "is_loan TINYINT(1) NOT NULL DEFAULT 0")
        _alter_add_column(conn, "inventory_transactions", "due_back_date DATE NULL")
        _alter_add_column(conn, "inventory_transactions", "equipment_asset_id BIGINT NULL")

        # Equipment assets (serialised inventory) - per physical unit
        _create_table(
            conn,
            "inventory_equipment_assets",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            item_id INT NOT NULL,
            serial_number VARCHAR(255) NOT NULL,
            status ENUM('in_stock','loaned','assigned','maintenance','retired','lost') NOT NULL DEFAULT 'in_stock',
            make VARCHAR(120) NULL,
            model VARCHAR(120) NULL,
            purchase_date DATE NULL,
            warranty_expiry DATE NULL,
            service_interval_days INT NULL,
            `condition` VARCHAR(64) NULL,
            metadata JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_inventory_equipment_serial (serial_number),
            INDEX idx_inventory_equipment_item (item_id),
            INDEX idx_inventory_equipment_status (status)
            """,
        )
        _upgrade_equipment_assets(conn)

        # Suppliers
        _create_table(
            conn,
            "inventory_suppliers",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            code VARCHAR(120),
            contact_info JSON,
            default_lead_time_days INT NULL,
            rating DECIMAL(5, 2) NULL,
            metadata JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_inventory_suppliers_code (code),
            INDEX idx_inventory_suppliers_name (name)
            """,
        )

        # Invoices (header)
        _create_table(
            conn,
            "inventory_invoices",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            supplier_id INT NULL,
            external_source VARCHAR(64),
            external_invoice_id VARCHAR(255),
            invoice_number VARCHAR(255),
            invoice_date DATE NULL,
            total_amount DECIMAL(18, 6) NULL,
            currency VARCHAR(16),
            status ENUM('pending','parsed','validated','applied') NOT NULL DEFAULT 'pending',
            raw_file_path VARCHAR(1024),
            parsed_payload JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_inventory_invoices_supplier (supplier_id),
            INDEX idx_inventory_invoices_date (invoice_date),
            INDEX idx_inventory_invoices_status (status)
            """,
        )

        # Invoice lines
        _create_table(
            conn,
            "inventory_invoice_lines",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            invoice_id BIGINT NOT NULL,
            item_id INT NULL,
            sku VARCHAR(255),
            description TEXT,
            quantity DECIMAL(18, 4) NOT NULL,
            unit_price DECIMAL(18, 6) NULL,
            line_total DECIMAL(18, 6) NULL,
            external_item_ref VARCHAR(255),
            parsed_metadata JSON,
            match_status ENUM('matched','ambiguous','unmapped') NOT NULL DEFAULT 'unmapped',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_inventory_invoice_lines_invoice (invoice_id),
            INDEX idx_inventory_invoice_lines_item (item_id),
            INDEX idx_inventory_invoice_lines_match (match_status)
            """,
        )

        # Supplier performance summary
        _create_table(
            conn,
            "inventory_supplier_performance",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            supplier_id INT NOT NULL,
            item_id INT NULL,
            avg_lead_time_days DECIMAL(10, 2) NULL,
            on_time_rate DECIMAL(5, 2) NULL,
            fill_rate DECIMAL(5, 2) NULL,
            avg_cost DECIMAL(18, 6) NULL,
            last_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_inventory_supplier_perf (supplier_id, item_id),
            INDEX idx_inventory_supplier_perf_supplier (supplier_id)
            """,
        )

        # Optional DB-level audit trail
        _create_table(
            conn,
            "inventory_audit",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NULL,
            action VARCHAR(255) NOT NULL,
            item_id INT NULL,
            location_id INT NULL,
            batch_id INT NULL,
            transaction_id BIGINT NULL,
            details JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_inventory_audit_action (action),
            INDEX idx_inventory_audit_item (item_id),
            INDEX idx_inventory_audit_location (location_id),
            INDEX idx_inventory_audit_tx (transaction_id)
            """,
        )

        # Purchase orders (minimal for supplier PO status; Procurement module may extend)
        _create_table(
            conn,
            "inventory_purchase_orders",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            supplier_id INT NOT NULL,
            order_number VARCHAR(120),
            status ENUM('draft','sent','confirmed','partially_received','received','closed') NOT NULL DEFAULT 'draft',
            ordered_at DATE NULL,
            expected_date DATE NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_inventory_po_supplier (supplier_id),
            INDEX idx_inventory_po_status (status)
            """,
        )

        # Supplier compliance / documents (uploaded by supplier or admin)
        _create_table(
            conn,
            "inventory_supplier_documents",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            supplier_id INT NOT NULL,
            name VARCHAR(255),
            document_type VARCHAR(64) NOT NULL DEFAULT 'compliance',
            file_path VARCHAR(1024) NOT NULL,
            uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            uploaded_by_token_id INT NULL,
            metadata JSON,
            INDEX idx_inventory_supplier_docs_supplier (supplier_id),
            INDEX idx_inventory_supplier_docs_type (document_type)
            """,
        )

        # API tokens for external supplier access (customer-facing features in Sales module)
        _create_table(
            conn,
            "inventory_api_tokens",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            token_hash VARCHAR(255) NOT NULL UNIQUE,
            name VARCHAR(255),
            role ENUM('supplier','customer') NOT NULL,
            supplier_id INT NULL,
            customer_id INT NULL,
            scopes JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_used_at TIMESTAMP NULL,
            INDEX idx_inventory_api_tokens_hash (token_hash),
            INDEX idx_inventory_api_tokens_role (role)
            """,
        )

        # Categories for items (trend reporting, Ecommerce/POS ready)
        _create_table(
            conn,
            "inventory_categories",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(120) NOT NULL,
            code VARCHAR(64) NULL,
            description TEXT NULL,
            parent_id INT NULL,
            sort_order INT NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_inventory_categories_code (code),
            INDEX idx_inventory_categories_parent (parent_id)
            """,
        )
        _alter_add_column(conn, "inventory_items", "category_id INT NULL")
        cur = None
        try:
            cur = conn.cursor()
            cur.execute("ALTER TABLE inventory_items ADD INDEX idx_inventory_items_category (category_id)")
            conn.commit()
        except Exception as e:
            if "Duplicate" not in str(e):
                print(f"[inventory_control] Index category: {e}")
        finally:
            try:
                cur.close()
            except Exception:
                pass

        _alter_add_column(
            conn, "inventory_equipment_assets", "public_asset_code VARCHAR(64) NULL"
        )
        _alter_add_column(conn, "inventory_equipment_assets", "end_of_life_at DATE NULL")
        _alter_add_column(
            conn,
            "inventory_equipment_assets",
            "warranty_start_basis VARCHAR(24) NULL",
        )
        _alter_add_column(conn, "inventory_equipment_assets", "warranty_start_date DATE NULL")
        _alter_add_column(conn, "inventory_equipment_assets", "warranty_months INT NULL")
        cur_idx = None
        try:
            cur_idx = conn.cursor()
            cur_idx.execute(
                "CREATE UNIQUE INDEX uniq_inventory_equipment_public_code ON inventory_equipment_assets (public_asset_code)"
            )
            conn.commit()
            print("[inventory_control] Unique index on public_asset_code")
        except Exception as e:
            if "Duplicate" not in str(e):
                print(f"[inventory_control] public_asset_code index: {e}")
        finally:
            if cur_idx:
                try:
                    cur_idx.close()
                except Exception:
                    pass

        cur_idx2 = None
        try:
            cur_idx2 = conn.cursor()
            cur_idx2.execute(
                "CREATE INDEX idx_inv_tx_assignee ON inventory_transactions (assignee_type, assignee_id, performed_at)"
            )
            conn.commit()
            print("[inventory_control] Index idx_inv_tx_assignee")
        except Exception as e:
            if "Duplicate" not in str(e):
                print(f"[inventory_control] idx_inv_tx_assignee: {e}")
        finally:
            if cur_idx2:
                try:
                    cur_idx2.close()
                except Exception:
                    pass

        # Schema migrations: weight and repack support
        _alter_add_column(conn, "inventory_transactions", "weight DECIMAL(18, 6) NULL")
        _alter_add_column(conn, "inventory_transactions", "weight_uom VARCHAR(32) NULL")
        _alter_add_column(conn, "inventory_batches", "weight DECIMAL(18, 6) NULL")
        _alter_add_column(conn, "inventory_batches", "weight_uom VARCHAR(32) NULL")
        _alter_add_column(conn, "inventory_batches", "unit_weight DECIMAL(18, 6) NULL")
        _alter_add_column(conn, "inventory_batches", "unit_weight_uom VARCHAR(32) NULL")
        _alter_modify_enum(conn, "inventory_transactions", "transaction_type",
            "ENUM('in','out','adjustment','transfer','count','return','repack') NOT NULL")

        _create_table(
            conn,
            "inventory_equipment_asset_consumables",
            """
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
            usage_close_reason VARCHAR(32) NULL,
            discrepancy_flag TINYINT(1) NOT NULL DEFAULT 0,
            discrepancy_details TEXT NULL,
            discrepancy_reported_at DATETIME NULL,
            discrepancy_reported_by_contractor_id INT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_inv_equip_cons_asset (equipment_asset_id),
            INDEX idx_inv_equip_cons_expiry (expiry_date)
            """,
        )

        _create_table(
            conn,
            "inventory_equipment_portal_handoffs",
            """
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
            """,
        )

        _create_table(
            conn,
            "inventory_contractor_kit_requests",
            """
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            contractor_id INT NOT NULL,
            need_from DATE NOT NULL,
            need_until DATE NOT NULL,
            request_text TEXT NOT NULL,
            status VARCHAR(24) NOT NULL DEFAULT 'pending',
            office_notes TEXT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resolved_at DATETIME NULL,
            resolved_by VARCHAR(128) NULL,
            KEY idx_ickr_contractor (contractor_id),
            KEY idx_ickr_status (status, created_at),
            CONSTRAINT fk_ickr_contractor FOREIGN KEY (contractor_id)
              REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE
            """,
        )

        _ensure_asset_extension_schema(conn)
        _ensure_default_holding_location(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def upgrade() -> None:
    """
    Run schema migrations for the Inventory Control plugin.
    Idempotent: adds missing tables and columns without dropping data.
    Safe to run after initial install or after deploying new code.
    """
    install()


if __name__ == "__main__":
    # CLI: python install.py install | upgrade | uninstall
    # - install: full idempotent setup (creates tables, adds missing columns).
    # - upgrade: same as install(); use for "Repair DB" or after deploying plugin updates.
    action = (sys.argv[1] if len(sys.argv) > 1 else "install").lower()
    if action == "install":
        install()
    elif action == "upgrade":
        upgrade()
    elif action == "uninstall":
        # Optional: tear-down (e.g. drop tables) if needed later
        pass
    else:
        print("Usage: python install.py install|upgrade|uninstall")
        sys.exit(1)

