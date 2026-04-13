"""Idempotent schema for Fleet Management."""

import sys
from pathlib import Path

HERE = Path(__file__).resolve()
PLUGIN_DIR = HERE.parent
PLUGINS_DIR = PLUGIN_DIR.parent
APP_ROOT = PLUGINS_DIR.parent
PROJECT_ROOT = APP_ROOT.parent

for p in (str(PROJECT_ROOT), str(APP_ROOT), str(PLUGIN_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from app.objects import get_db_connection  # noqa: E402

from app.plugins.fleet_management.vdi_schema_default import (  # noqa: E402
    DEFAULT_VDI_SCHEMA,
)


def _create_table(conn, name: str, columns_sql: str) -> None:
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
        print(f"[fleet_management] Ensured table: {name}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _alter_add_column(conn, table: str, col_def: str) -> None:
    parts = col_def.strip().split(None, 1)
    col_name = parts[0]
    rest = parts[1] if len(parts) > 1 else ""
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{col_name}` {rest}")
        conn.commit()
        print(f"[fleet_management] Added column {table}.{col_name}")
    except Exception as e:
        if "Duplicate column" not in str(e):
            print(f"[fleet_management] ALTER {table}.{col_name}: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _alter_add_index(conn, table: str, index_name: str, cols: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"CREATE INDEX `{index_name}` ON `{table}` ({cols})"
        )
        conn.commit()
        print(f"[fleet_management] Index {index_name} on {table}")
    except Exception as e:
        if "Duplicate" not in str(e) and "exists" not in str(e).lower():
            print(f"[fleet_management] INDEX {index_name}: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _migrate_fleet_vehicle_optional_registration(conn) -> None:
    """Allow unregistered assets (quad bikes, buggies); widen unit label for names/callsigns."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            ALTER TABLE fleet_vehicles
            MODIFY COLUMN registration VARCHAR(32) NULL,
            MODIFY COLUMN internal_code VARCHAR(64) NOT NULL
            """
        )
        conn.commit()
        print(
            "[fleet_management] fleet_vehicles: registration nullable, internal_code VARCHAR(64)"
        )
    except Exception as e:
        if "Duplicate" not in str(e) and "same" not in str(e).lower():
            print(f"[fleet_management] optional registration migrate: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _migrate_vehicle_status_enum(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            ALTER TABLE fleet_vehicles
            MODIFY COLUMN status ENUM(
                'active','off_road','maintenance','decommissioned','pending_road_test'
            ) NOT NULL DEFAULT 'active'
            """
        )
        conn.commit()
        print("[fleet_management] Updated fleet_vehicles.status enum (pending_road_test)")
    except Exception as e:
        if "Duplicate" not in str(e) and "same" not in str(e).lower():
            print(f"[fleet_management] status enum migrate: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _migrate_fleet_extensions(conn) -> None:
    """Idempotent: VIN, photos, VDI, issues board, notifications (existing DBs)."""
    import json

    _alter_add_column(conn, "fleet_vehicles", "vin VARCHAR(64) NULL")
    _alter_add_column(conn, "fleet_vehicles", "id_photo_path VARCHAR(512) NULL")
    _alter_add_column(conn, "fleet_vehicles", "off_road_reason VARCHAR(32) NULL")
    _migrate_vehicle_status_enum(conn)

    _create_table(
        conn,
        "fleet_vdi_schema",
        """
        id TINYINT NOT NULL PRIMARY KEY DEFAULT 1,
        schema_json JSON NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        """,
    )
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM fleet_vdi_schema WHERE id = 1")
        n = cur.fetchone()[0]
        if not n:
            cur.execute(
                "INSERT INTO fleet_vdi_schema (id, schema_json) VALUES (1, %s)",
                (json.dumps(DEFAULT_VDI_SCHEMA),),
            )
            conn.commit()
            print("[fleet_management] Seeded default VDI schema")
    except Exception as e:
        print(f"[fleet_management] fleet_vdi_schema seed: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass

    _create_table(
        conn,
        "fleet_vdi_submissions",
        """
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        vehicle_id BIGINT NOT NULL,
        actor_type ENUM('user','contractor') NOT NULL,
        actor_id VARCHAR(64) NOT NULL,
        mileage_reported INT NULL,
        responses JSON NOT NULL,
        photo_paths JSON NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_fleet_vdi_vehicle (vehicle_id),
        INDEX idx_fleet_vdi_created (created_at),
        CONSTRAINT fk_fleet_vdi_vehicle
            FOREIGN KEY (vehicle_id) REFERENCES fleet_vehicles(id) ON DELETE CASCADE
        """,
    )

    _create_table(
        conn,
        "fleet_issues",
        """
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        vehicle_id BIGINT NOT NULL,
        actor_type ENUM('user','contractor') NOT NULL,
        actor_id VARCHAR(64) NOT NULL,
        title VARCHAR(255) NOT NULL,
        description TEXT NULL,
        photo_paths JSON NULL,
        kanban_stage VARCHAR(32) NOT NULL DEFAULT 'reported',
        scheduled_service_date DATE NULL,
        manager_notes TEXT NULL,
        vehicle_marked_vor TINYINT(1) NOT NULL DEFAULT 0,
        vehicle_marked_workshop TINYINT(1) NOT NULL DEFAULT 0,
        completed_at DATETIME NULL,
        resolution_summary TEXT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_fleet_issues_vehicle (vehicle_id),
        INDEX idx_fleet_issues_stage (kanban_stage),
        CONSTRAINT fk_fleet_issues_vehicle
            FOREIGN KEY (vehicle_id) REFERENCES fleet_vehicles(id) ON DELETE CASCADE
        """,
    )

    _alter_add_column(
        conn,
        "fleet_issues",
        "vehicle_marked_workshop TINYINT(1) NOT NULL DEFAULT 0",
    )

    _create_table(
        conn,
        "fleet_notifications",
        """
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        kind VARCHAR(32) NOT NULL,
        vehicle_id BIGINT NULL,
        message VARCHAR(512) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        dismissed_at DATETIME NULL,
        INDEX idx_fleet_notif_open (dismissed_at),
        INDEX idx_fleet_notif_vehicle (vehicle_id)
        """,
    )

    _create_table(
        conn,
        "fleet_vehicle_installed_parts",
        """
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        vehicle_id BIGINT NOT NULL,
        inventory_item_id INT NULL,
        inventory_transaction_id BIGINT NULL,
        part_number VARCHAR(160) NULL,
        part_description VARCHAR(512) NULL,
        quantity DECIMAL(18,4) NOT NULL DEFAULT 1,
        installed_date DATE NOT NULL,
        odometer_at_install INT NULL,
        warranty_expires_date DATE NULL,
        warranty_terms VARCHAR(512) NULL,
        invoice_reference VARCHAR(160) NULL,
        notes TEXT NULL,
        metadata JSON NULL,
        created_by CHAR(36) NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_fleet_vparts_vehicle (vehicle_id),
        INDEX idx_fleet_vparts_installed (installed_date),
        CONSTRAINT fk_fleet_vparts_vehicle
            FOREIGN KEY (vehicle_id) REFERENCES fleet_vehicles(id) ON DELETE CASCADE
        """,
    )

    _migrate_fleet_types_safety_servicing(conn)


def _migrate_fleet_types_safety_servicing(conn) -> None:
    """Vehicle types, servicing targets on compliance, safety check submissions."""
    import json

    from app.plugins.fleet_management.safety_schema_default import DEFAULT_SAFETY_SCHEMA

    _create_table(
        conn,
        "fleet_vehicle_types",
        """
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(160) NOT NULL,
        service_interval_days INT NULL,
        service_interval_miles INT NULL,
        safety_check_interval_days INT NOT NULL DEFAULT 42,
        safety_schema_json JSON NOT NULL,
        sort_order INT NOT NULL DEFAULT 0,
        active TINYINT(1) NOT NULL DEFAULT 1,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_fleet_vtypes_active (active),
        INDEX idx_fleet_vtypes_sort (sort_order, name)
        """,
    )

    _alter_add_column(conn, "fleet_vehicles", "vehicle_type_id BIGINT NULL")

    _alter_add_column(conn, "fleet_compliance", "last_service_date DATE NULL")
    _alter_add_column(conn, "fleet_compliance", "last_service_mileage INT NULL")
    _alter_add_column(conn, "fleet_compliance", "next_service_due_date DATE NULL")
    _alter_add_column(conn, "fleet_compliance", "next_service_due_mileage INT NULL")
    _alter_add_column(conn, "fleet_compliance", "servicing_notes TEXT NULL")

    _create_table(
        conn,
        "fleet_safety_checks",
        """
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        vehicle_id BIGINT NOT NULL,
        performed_by_user_id CHAR(36) NULL,
        performed_at DATE NOT NULL,
        mileage_at_check INT NULL,
        responses JSON NOT NULL,
        photo_paths JSON NULL,
        summary_notes TEXT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_fleet_safety_vehicle (vehicle_id),
        INDEX idx_fleet_safety_performed (performed_at),
        CONSTRAINT fk_fleet_safety_vehicle
            FOREIGN KEY (vehicle_id) REFERENCES fleet_vehicles(id) ON DELETE CASCADE
        """,
    )
    _alter_add_column(
        conn,
        "fleet_safety_checks",
        "check_form_key VARCHAR(64) NULL COMMENT 'workshop or custom contractor form id'",
    )

    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM fleet_vehicle_types")
        n = cur.fetchone()[0]
        if not n:
            from app.plugins.fleet_management.fleet_industry_seed import (
                build_fleet_vehicle_type_seed_rows,
                load_tenant_industries_for_fleet_seed,
            )

            industries = load_tenant_industries_for_fleet_seed()
            type_rows = build_fleet_vehicle_type_seed_rows(industries)
            sch_json = json.dumps(DEFAULT_SAFETY_SCHEMA)
            placeholders = ", ".join(
                "(%s,%s,%s,%s,%s,%s,1)" for _ in type_rows
            )
            flat_params: list = []
            for name, sid, sim, scd, so in type_rows:
                flat_params.extend([name, sid, sim, scd, sch_json, so])
            cur.execute(
                f"""
                INSERT INTO fleet_vehicle_types
                  (name, service_interval_days, service_interval_miles,
                   safety_check_interval_days, safety_schema_json, sort_order, active)
                VALUES
                  {placeholders}
                """,
                tuple(flat_params),
            )
            conn.commit()
            print(
                "[fleet_management] Seeded fleet_vehicle_types "
                f"(industries={industries!r}, {len(type_rows)} types)"
            )
    except Exception as e:
        print(f"[fleet_management] fleet_vehicle_types seed: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass

    cur2 = conn.cursor()
    try:
        cur2.execute(
            """
            SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_SCHEMA = DATABASE()
              AND TABLE_NAME = 'fleet_vehicles'
              AND CONSTRAINT_NAME = 'fk_fleet_vehicles_type'
            """
        )
        exists = cur2.fetchone()[0]
        if not exists:
            cur2.execute(
                """
                ALTER TABLE fleet_vehicles
                ADD CONSTRAINT fk_fleet_vehicles_type
                FOREIGN KEY (vehicle_type_id) REFERENCES fleet_vehicle_types(id)
                ON DELETE SET NULL
                """
            )
            conn.commit()
            print("[fleet_management] Added FK fleet_vehicles.vehicle_type_id")
    except Exception as e:
        if "Duplicate" not in str(e) and "already exists" not in str(e).lower():
            print(f"[fleet_management] fleet_vehicles type FK: {e}")
    finally:
        try:
            cur2.close()
        except Exception:
            pass


def install():
    conn = get_db_connection()
    try:
        _create_table(
            conn,
            "fleet_vehicles",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            internal_code VARCHAR(32) NOT NULL,
            registration VARCHAR(32) NOT NULL,
            make VARCHAR(120) NULL,
            model VARCHAR(120) NULL,
            year SMALLINT NULL,
            fuel_type VARCHAR(32) NULL,
            status ENUM('active','off_road','maintenance','decommissioned','pending_road_test') NOT NULL DEFAULT 'active',
            vin VARCHAR(64) NULL,
            id_photo_path VARCHAR(512) NULL,
            off_road_reason VARCHAR(32) NULL,
            last_lat DECIMAL(10,7) NULL,
            last_lng DECIMAL(10,7) NULL,
            last_location_at DATETIME NULL,
            telematics_provider VARCHAR(64) NULL,
            notes TEXT NULL,
            metadata JSON NULL,
            vehicle_type_id BIGINT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_fleet_internal_code (internal_code),
            INDEX idx_fleet_registration (registration),
            INDEX idx_fleet_status (status)
            """,
        )

        _create_table(
            conn,
            "fleet_compliance",
            """
            vehicle_id BIGINT NOT NULL PRIMARY KEY,
            mot_expiry DATE NULL,
            tax_expiry DATE NULL,
            insurance_expiry DATE NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            CONSTRAINT fk_fleet_compliance_vehicle
                FOREIGN KEY (vehicle_id) REFERENCES fleet_vehicles(id) ON DELETE CASCADE
            """,
        )

        _create_table(
            conn,
            "fleet_mileage_logs",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            vehicle_id BIGINT NOT NULL,
            driver_user_id CHAR(36) NULL,
            start_mileage INT NOT NULL,
            end_mileage INT NOT NULL,
            purpose VARCHAR(512) NULL,
            logged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_by CHAR(36) NULL,
            metadata JSON NULL,
            INDEX idx_fleet_mileage_vehicle (vehicle_id),
            INDEX idx_fleet_mileage_logged (logged_at),
            CONSTRAINT fk_fleet_mileage_vehicle
                FOREIGN KEY (vehicle_id) REFERENCES fleet_vehicles(id) ON DELETE CASCADE
            """,
        )

        _create_table(
            conn,
            "fleet_maintenance_events",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            vehicle_id BIGINT NOT NULL,
            service_date DATE NOT NULL,
            service_type VARCHAR(128) NOT NULL,
            provider VARCHAR(255) NULL,
            cost DECIMAL(18,2) NULL,
            odometer_at_service INT NULL,
            notes TEXT NULL,
            created_by CHAR(36) NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_fleet_maint_vehicle (vehicle_id),
            INDEX idx_fleet_maint_date (service_date),
            CONSTRAINT fk_fleet_maint_vehicle
                FOREIGN KEY (vehicle_id) REFERENCES fleet_vehicles(id) ON DELETE CASCADE
            """,
        )

        _create_table(
            conn,
            "fleet_driver_assignments",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            vehicle_id BIGINT NOT NULL,
            user_id CHAR(36) NOT NULL,
            assignment_role ENUM('primary','temporary') NOT NULL DEFAULT 'primary',
            effective_from DATE NOT NULL,
            effective_to DATE NULL,
            notes VARCHAR(512) NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_fleet_drv_vehicle (vehicle_id),
            INDEX idx_fleet_drv_user (user_id),
            CONSTRAINT fk_fleet_drv_vehicle
                FOREIGN KEY (vehicle_id) REFERENCES fleet_vehicles(id) ON DELETE CASCADE
            """,
        )

        # Legacy: retained for existing databases; UI and code paths use fleet_issues only.
        _create_table(
            conn,
            "fleet_defects",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            vehicle_id BIGINT NOT NULL,
            reported_by_user_id CHAR(36) NOT NULL,
            title VARCHAR(255) NOT NULL,
            description TEXT NULL,
            status ENUM('open','in_progress','resolved','closed') NOT NULL DEFAULT 'open',
            reported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP NULL,
            resolution_notes TEXT NULL,
            INDEX idx_fleet_def_vehicle (vehicle_id),
            INDEX idx_fleet_def_status (status),
            CONSTRAINT fk_fleet_def_vehicle
                FOREIGN KEY (vehicle_id) REFERENCES fleet_vehicles(id) ON DELETE CASCADE
            """,
        )

        _create_table(
            conn,
            "fleet_audit",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            user_id CHAR(36) NULL,
            action VARCHAR(64) NOT NULL,
            entity_type VARCHAR(64) NOT NULL,
            entity_id VARCHAR(64) NULL,
            details JSON NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_fleet_audit_entity (entity_type, entity_id),
            INDEX idx_fleet_audit_created (created_at)
            """,
        )

        _alter_add_index(conn, "fleet_compliance", "idx_fleet_comp_mot", "mot_expiry")
        _alter_add_index(conn, "fleet_compliance", "idx_fleet_comp_tax", "tax_expiry")
        _alter_add_index(conn, "fleet_compliance", "idx_fleet_comp_ins", "insurance_expiry")

        _migrate_fleet_vehicle_optional_registration(conn)
        _migrate_fleet_extensions(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def upgrade():
    install()


if __name__ == "__main__":
    # Version page "Run upgrades" executes: python install.py upgrade
    cmd = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
    if cmd == "upgrade":
        upgrade()
    else:
        install()
