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
from app.organization_profile import (  # noqa: E402
    load_tenant_industries_for_install,
    tenant_matches_industry,
)
from triage_slug_industries import VENTUS_TRIAGE_SLUG_INDUSTRIES  # noqa: E402

MIGRATIONS_TABLE = "ventus_response_migrations"

# Synthetic ledger keys (not SQL files): OEM triage + standby presets run once per DB so
# repeated init_db / preDeploy does not upsert over admin edits or resurrect deleted rows.
_LEDGER_BASE_TRIAGE_FORMS = "__ventus_base_triage_forms_seeded_v1__"
_LEDGER_STANDBY_PRESETS = "__ventus_standby_presets_seeded_v1__"


def _ledger_has(conn, key: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT 1 FROM `{MIGRATIONS_TABLE}` WHERE filename = %s LIMIT 1", (key,)
        )
        return bool(cur.fetchone())
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _ledger_mark(conn, key: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"INSERT IGNORE INTO `{MIGRATIONS_TABLE}` (filename) VALUES (%s)", (key,)
        )
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _triage_seed_row_visible(slug, tenant_inds):
    req = VENTUS_TRIAGE_SLUG_INDUSTRIES.get(slug)
    if req is None:
        return True
    return tenant_matches_industry(tenant_inds, *req)


def _apply_triage_seed_defaults(rows, _tenant_inds):
    """rows: list of [slug, name, desc, schema, active, is_default]; mutates is_default."""
    for r in rows:
        r[5] = 0
    pick = None
    slugs = {r[0] for r in rows}
    if "event_medical" in slugs:
        pick = "event_medical"
    elif "security_response" in slugs:
        pick = "security_response"
    elif rows:
        pick = rows[0][0]
    if pick:
        for r in rows:
            if r[0] == pick:
                r[5] = 1
                break


def _triage_seeds_for_tenant(tenant_inds):
    """Built-in response triage forms only (no demo/training profiles)."""
    raw = [list(t) for t in BASE_TRIAGE_FORM_SEEDS]
    filtered = [r for r in raw if _triage_seed_row_visible(r[0], tenant_inds)]
    if not filtered:
        em = next((list(t) for t in BASE_TRIAGE_FORM_SEEDS if t[0] == "event_medical"), None)
        filtered = [em] if em else raw[:1]
    _apply_triage_seed_defaults(filtered, tenant_inds)
    return filtered


BASE_TRIAGE_FORM_SEEDS = [
    (
        "event_medical",
        "Event Medical",
        "On-site medical cover: zone, security, and crowd context. The operational event is identified by the dispatch division you select on the form.",
        '{"show_exclusions": false, "questions": [{"key":"event_zone","label":"Event zone / stand","type":"text"},{"key":"security_required","label":"Security required?","type":"select","options":["unknown","yes","no"]},{"key":"crowd_density","label":"Crowd density","type":"select","options":["low","medium","high","unknown"]}]}',
        1,
        0,
    ),
    (
        "security_response",
        "Security Response",
        "Security team dispatch for incidents and welfare escalations.",
        '{"show_exclusions": false, "questions": [{"key":"incident_type","label":"Incident type","type":"select","options":["theft","violence","trespass","welfare","other"],"required":true},{"key":"threat_level","label":"Threat level","type":"select","options":["low","medium","high","critical"],"required":true},{"key":"suspect_description","label":"Suspect description","type":"textarea"},{"key":"police_notified","label":"Police already notified?","type":"select","options":["unknown","yes","no"]}]}',
        1,
        0,
    ),
]


def table_exists(conn, name):
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE %s", (name,))
        return bool(cur.fetchone())
    finally:
        try:
            cur.close()
        except Exception:
            pass


def create_migrations_table(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{MIGRATIONS_TABLE}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL UNIQUE,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
        print(f"Ensured table exists: {MIGRATIONS_TABLE}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def create_table(conn, name, columns_sql):
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
        print(f"Created or ensured table: {name}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def install(seed_demo: bool = False):
    # seed_demo retained for CLI compatibility; triage no longer ships demo-only profiles.
    conn = get_db_connection()
    try:
        create_migrations_table(conn)

        # response_triage: store original triage payloads and resolved coordinates
        create_table(
            conn,
            "response_triage",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            created_by VARCHAR(120),
            vita_record_id VARCHAR(120),
            first_name VARCHAR(120),
            middle_name VARCHAR(120),
            last_name VARCHAR(120),
            patient_dob DATE,
            phone_number VARCHAR(80),
            address VARCHAR(512),
            postcode VARCHAR(64),
            entry_requirements JSON,
            reason_for_call VARCHAR(255),
            onset_datetime DATETIME,
            patient_alone TINYINT(1) DEFAULT 0,
            exclusion_data JSON,
            risk_flags JSON,
            decision VARCHAR(64),
            coordinates JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            """,
        )

        # Ensure backward-compat columns exist if table pre-exists
        try:
            cur = conn.cursor()
            try:
                cur.execute("ALTER TABLE mdt_jobs ADD COLUMN data JSON")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_jobs ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE mdt_jobs ADD COLUMN claimedBy VARCHAR(80)")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE mdt_jobs ADD COLUMN claimedAt DATETIME")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE mdt_jobs ADD COLUMN completedAt DATETIME")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE mdt_jobs ADD COLUMN chief_complaint VARCHAR(255)")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_jobs ADD COLUMN private_notes LONGTEXT")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_jobs ADD COLUMN public_notes LONGTEXT")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_jobs ADD COLUMN final_status VARCHAR(128)")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_jobs ADD COLUMN outcome VARCHAR(255)")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_jobs ADD COLUMN lastStatusTime DATETIME")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_jobs ADD COLUMN created_by VARCHAR(120)")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_jobs ADD COLUMN division VARCHAR(64) NOT NULL DEFAULT 'general'")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_jobs ADD INDEX idx_mdt_jobs_division_status_created (division, status, created_at)")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_jobs ADD COLUMN division_snapshot_name VARCHAR(255) NULL")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_jobs ADD COLUMN division_snapshot_color VARCHAR(32) NULL")
            except Exception:
                pass
            conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass
        # mdt_jobs: CAD jobs table
        create_table(
            conn,
            "mdt_jobs",
            """
            cad INT AUTO_INCREMENT PRIMARY KEY,
            status VARCHAR(32) NOT NULL DEFAULT 'queued',
            data JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_by VARCHAR(120),
            division VARCHAR(64) NOT NULL DEFAULT 'general',
            claimedBy VARCHAR(80),
            claimedAt DATETIME,
            completedAt DATETIME,
            chief_complaint VARCHAR(255),
            outcome VARCHAR(255),
            lastStatusTime DATETIME,
            division_snapshot_name VARCHAR(255) NULL,
            division_snapshot_color VARCHAR(32) NULL,
            INDEX idx_mdt_jobs_status_created_at (status, created_at),
            INDEX idx_mdt_jobs_division_status_created (division, status, created_at)
            """,
        )

        # Ensure additional columns exist for sign-on compatibility
        try:
            cur = conn.cursor()
            # Remove legacy over-restrictive unique indexes if present.
            try:
                cur.execute("ALTER TABLE mdts_signed_on DROP INDEX status_UNIQUE")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE mdts_signed_on DROP INDEX ipAddress_UNIQUE")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE mdts_signed_on ADD COLUMN signOnTime DATETIME")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdts_signed_on ADD COLUMN assignedIncident INT")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdts_signed_on ADD COLUMN ipAddress VARCHAR(120)")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE mdts_signed_on ADD COLUMN crew JSON")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdts_signed_on ADD COLUMN lastLat DOUBLE")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdts_signed_on ADD COLUMN lastLon DOUBLE")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdts_signed_on ADD COLUMN lastSeenAt DATETIME")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdts_signed_on ADD COLUMN updatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdts_signed_on ADD COLUMN mealBreakStartedAt DATETIME NULL DEFAULT NULL")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdts_signed_on ADD COLUMN mealBreakUntil DATETIME NULL DEFAULT NULL")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdts_signed_on ADD COLUMN division VARCHAR(64) NOT NULL DEFAULT 'general'")
            except Exception:
                pass
            try:
                cur.execute(
                    "UPDATE mdts_signed_on SET lastSeenAt = COALESCE(lastSeenAt, signOnTime, NOW())")
            except Exception:
                pass
            try:
                cur.execute(
                    "CREATE INDEX idx_mdts_status ON mdts_signed_on (status)")
            except Exception:
                pass
            try:
                cur.execute(
                    "CREATE INDEX idx_mdts_seen ON mdts_signed_on (lastSeenAt)")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdts_signed_on ADD INDEX idx_mdts_division_status (division, status)")
            except Exception:
                pass
            conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass
        # mdts_signed_on: units signed on
        create_table(
            conn,
            "mdts_signed_on",
            """
            callSign VARCHAR(64) PRIMARY KEY,
            signOnTime DATETIME,
            status VARCHAR(64),
            assignedIncident INT,
            ipAddress VARCHAR(120),
            crew JSON,
            lastLat DOUBLE,
            lastLon DOUBLE,
            lastSeenAt DATETIME,
            updatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            mealBreakStartedAt DATETIME NULL DEFAULT NULL,
            mealBreakUntil DATETIME NULL DEFAULT NULL,
            division VARCHAR(64) NOT NULL DEFAULT 'general',
            INDEX idx_mdts_status (status),
            INDEX idx_mdts_seen (lastSeenAt),
            INDEX idx_mdts_division_status (division, status)
            """,
        )

        # mdt_locations: unit locations
        create_table(
            conn,
            "mdt_locations",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            callSign VARCHAR(64),
            latitude DOUBLE,
            longitude DOUBLE,
            timestamp DATETIME,
            status VARCHAR(64),
            INDEX idx_mdt_locations_callSign (callSign)
            """,
        )

        # mdt_positions: live MDT position pings
        create_table(
            conn,
            "mdt_positions",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            callSign VARCHAR(64) NOT NULL,
            latitude DECIMAL(10,7) NOT NULL,
            longitude DECIMAL(10,7) NOT NULL,
            recorded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_call_time (callSign, recorded_at)
            """,
        )

        # mdt_dispatch_settings: dispatch assignment mode (auto/manual)
        create_table(
            conn,
            "mdt_dispatch_settings",
            """
            id TINYINT PRIMARY KEY,
            mode VARCHAR(16) NOT NULL DEFAULT 'auto',
            motd_text TEXT,
            motd_updated_by VARCHAR(120),
            motd_updated_at TIMESTAMP NULL DEFAULT NULL,
            default_division VARCHAR(64) NOT NULL DEFAULT 'general',
            updated_by VARCHAR(120),
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            """,
        )
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    "ALTER TABLE mdt_dispatch_settings ADD COLUMN motd_text TEXT")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_dispatch_settings ADD COLUMN motd_updated_by VARCHAR(120)")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_dispatch_settings ADD COLUMN motd_updated_at TIMESTAMP NULL DEFAULT NULL")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_dispatch_settings ADD COLUMN default_division VARCHAR(64) NOT NULL DEFAULT 'general'")
            except Exception:
                pass
            cur.execute("""
                INSERT INTO mdt_dispatch_settings (id, mode, default_division, updated_by)
                VALUES (1, 'manual', 'general', 'installer')
                ON DUPLICATE KEY UPDATE id = id
            """)
            conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass

        # mdt_job_units: many-to-one CAD-unit assignments
        create_table(
            conn,
            "mdt_job_units",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            job_cad INT NOT NULL,
            callsign VARCHAR(64) NOT NULL,
            assigned_by VARCHAR(120),
            assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_job_callsign (job_cad, callsign),
            INDEX idx_job_cad (job_cad),
            INDEX idx_callsign (callsign)
            """,
        )

        # mdt_job_comms: call-taker <-> dispatcher incident communications
        create_table(
            conn,
            "mdt_job_comms",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            cad INT NOT NULL,
            message_type VARCHAR(24) NOT NULL DEFAULT 'message',
            sender_role VARCHAR(64),
            sender_user VARCHAR(120),
            message_text LONGTEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_job_comms_cad (cad),
            INDEX idx_job_comms_created_at (created_at)
            """,
        )

        # mdt_response_log: unit status timeline per CAD job
        create_table(
            conn,
            "mdt_response_log",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            callSign VARCHAR(64) NOT NULL,
            cad INT NOT NULL,
            status VARCHAR(32) NOT NULL,
            event_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            crew JSON,
            INDEX idx_response_log_cad_time (cad, event_time),
            INDEX idx_response_log_callsign_time (callSign, event_time),
            INDEX idx_response_log_status_time (status, event_time)
            """,
        )
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    "ALTER TABLE mdt_response_log ADD COLUMN crew JSON")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_response_log ADD COLUMN event_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")
            except Exception:
                pass
            try:
                cur.execute(
                    "CREATE INDEX idx_response_log_cad_time ON mdt_response_log (cad, event_time)")
            except Exception:
                pass
            try:
                cur.execute(
                    "CREATE INDEX idx_response_log_callsign_time ON mdt_response_log (callSign, event_time)")
            except Exception:
                pass
            try:
                cur.execute(
                    "CREATE INDEX idx_response_log_status_time ON mdt_response_log (status, event_time)")
            except Exception:
                pass
            conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass

        # mdt_dispatch_divisions: configured dispatch divisions and visual tags
        create_table(
            conn,
            "mdt_dispatch_divisions",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            slug VARCHAR(64) NOT NULL UNIQUE,
            name VARCHAR(120) NOT NULL,
            color VARCHAR(16) NOT NULL DEFAULT '#64748b',
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            is_default TINYINT(1) NOT NULL DEFAULT 0,
            created_by VARCHAR(120),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_divisions_active (is_active)
            """,
        )
        try:
            cur = conn.cursor()
            seeds = [
                ("general", "General", "#64748b", 1),
                ("emergency", "Emergency", "#ef4444", 0),
                ("urgent_care", "Urgent Care", "#f59e0b", 0),
                ("events", "Events", "#22c55e", 0),
            ]
            for slug, name, color, is_default in seeds:
                cur.execute(
                    """
                    INSERT INTO mdt_dispatch_divisions (slug, name, color, is_active, is_default, created_by)
                    VALUES (%s, %s, %s, 1, %s, 'installer')
                    ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        color = VALUES(color)
                    """,
                    (slug, name, color, is_default),
                )
            conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass

        # mdt_dispatch_assist_requests: explicit cross-division unit request workflow
        create_table(
            conn,
            "mdt_dispatch_assist_requests",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            request_type VARCHAR(32) NOT NULL DEFAULT 'unit_assist',
            from_division VARCHAR(64) NOT NULL,
            to_division VARCHAR(64) NOT NULL,
            callsign VARCHAR(64) NOT NULL,
            cad INT NULL,
            note TEXT,
            requested_by VARCHAR(120),
            status VARCHAR(24) NOT NULL DEFAULT 'pending',
            resolved_by VARCHAR(120),
            resolved_note TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP NULL DEFAULT NULL,
            INDEX idx_assist_status_to_division (status, to_division, created_at),
            INDEX idx_assist_callsign (callsign),
            INDEX idx_assist_cad (cad)
            """,
        )

        # mdt_dispatch_user_settings: per-user dispatch access flags
        create_table(
            conn,
            "mdt_dispatch_user_settings",
            """
            username VARCHAR(120) PRIMARY KEY,
            can_override_all TINYINT(1) NOT NULL DEFAULT 0,
            updated_by VARCHAR(120),
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            """,
        )

        # mdt_dispatch_user_divisions: per-user owned divisions
        create_table(
            conn,
            "mdt_dispatch_user_divisions",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(120) NOT NULL,
            division VARCHAR(64) NOT NULL,
            created_by VARCHAR(120),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_dispatch_user_division (username, division),
            INDEX idx_dispatch_user (username),
            INDEX idx_dispatch_division (division)
            """,
        )

        # mdt_triage_forms: configurable intake forms
        create_table(
            conn,
            "mdt_triage_forms",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            slug VARCHAR(64) NOT NULL UNIQUE,
            name VARCHAR(120) NOT NULL,
            description VARCHAR(255),
            schema_json JSON NOT NULL,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            is_default TINYINT(1) NOT NULL DEFAULT 0,
            created_by VARCHAR(120),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            """,
        )
        try:
            cur = conn.cursor()
            if _ledger_has(conn, _LEDGER_BASE_TRIAGE_FORMS):
                print(
                    "[ventus] Skipping base triage form OEM seed "
                    f"({_LEDGER_BASE_TRIAGE_FORMS} already applied — preserves edits/deletions)."
                )
            else:
                tenant_inds = load_tenant_industries_for_install()
                seeds = _triage_seeds_for_tenant(tenant_inds)
                inserted = 0
                for slug, name, desc, schema_json, is_active, is_default in seeds:
                    cur.execute(
                        "SELECT 1 FROM mdt_triage_forms WHERE slug = %s LIMIT 1", (slug,)
                    )
                    if cur.fetchone():
                        continue
                    cur.execute(
                        """
                        INSERT INTO mdt_triage_forms (slug, name, description, schema_json, is_active, is_default, created_by)
                        VALUES (%s, %s, %s, CAST(%s AS JSON), %s, %s, 'installer')
                        """,
                        (slug, name, desc, schema_json, is_active, is_default),
                    )
                    inserted += 1
                conn.commit()
                _ledger_mark(conn, _LEDGER_BASE_TRIAGE_FORMS)
                print(
                    f"[ventus] Base triage form OEM seed complete ({inserted} new row(s), "
                    f"{len(seeds)} profile(s) considered)."
                )
        finally:
            try:
                cur.close()
            except Exception:
                pass

        # messages: messages to/from units
        create_table(
            conn,
            "messages",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            `from` VARCHAR(120),
            recipient VARCHAR(120),
            text LONGTEXT,
            timestamp DATETIME,
            `read` TINYINT(1) DEFAULT 0,
            INDEX idx_messages_recipient (recipient)
            """,
        )

        # standby_locations: saved standby points per unit
        create_table(
            conn,
            "standby_locations",
            """
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            callSign VARCHAR(64),
            name VARCHAR(255),
            lat DOUBLE,
            lng DOUBLE,
            isNew TINYINT(1) DEFAULT 0,
            updatedBy VARCHAR(120),
            updatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_standby_callsign (callSign),
            INDEX idx_standby_callSign (callSign)
            """,
        )
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    "ALTER TABLE standby_locations ADD COLUMN updatedBy VARCHAR(120)")
            except Exception:
                pass
            try:
                cur.execute(
                    "CREATE UNIQUE INDEX uq_standby_callsign ON standby_locations (callSign)")
            except Exception:
                pass
            conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass

        # mdt_standby_presets: shared standby location presets
        create_table(
            conn,
            "mdt_standby_presets",
            """
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(180) NOT NULL,
            lat DECIMAL(10,7) NOT NULL,
            lng DECIMAL(10,7) NOT NULL,
            what3words VARCHAR(80) NULL,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            created_by VARCHAR(120),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_standby_preset_name (name)
            """,
        )
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    "ALTER TABLE mdt_standby_presets ADD COLUMN what3words VARCHAR(80) NULL")
            except Exception:
                pass
            try:
                cur.execute(
                    "ALTER TABLE mdt_standby_presets ADD COLUMN is_active TINYINT(1) NOT NULL DEFAULT 1")
            except Exception:
                pass
            if _ledger_has(conn, _LEDGER_STANDBY_PRESETS):
                print(
                    "[ventus] Skipping standby preset OEM seed "
                    f"({_LEDGER_STANDBY_PRESETS} already applied)."
                )
            else:
                for name, lat, lng in [
                    ("HQ", 51.5074000, -0.1278000),
                    ("North Standby", 51.5400000, -0.1100000),
                    ("South Standby", 51.4700000, -0.1200000),
                ]:
                    cur.execute(
                        "SELECT 1 FROM mdt_standby_presets WHERE name = %s LIMIT 1", (name,)
                    )
                    if cur.fetchone():
                        continue
                    cur.execute(
                        """
                        INSERT INTO mdt_standby_presets (name, lat, lng, what3words, is_active, created_by)
                        VALUES (%s, %s, %s, NULL, 1, 'installer')
                        """,
                        (name, lat, lng),
                    )
                conn.commit()
                _ledger_mark(conn, _LEDGER_STANDBY_PRESETS)
                print("[ventus] Standby preset OEM seed complete (one-time).")
        finally:
            try:
                cur.close()
            except Exception:
                pass

        # mdt_crew_profiles: per-username crew profile for CAD (all from MySQL: gender, skills, qualifications, profile pic)
        create_table(
            conn,
            "mdt_crew_profiles",
            """
            username VARCHAR(120) NOT NULL PRIMARY KEY,
            contractor_id INT NULL,
            gender VARCHAR(24) NULL COMMENT 'male, female, other - for danger/warning display',
            skills_json JSON NULL COMMENT 'Main skills e.g. Paramedic, ECA',
            qualifications_json JSON NULL COMMENT 'Additional sign-offs e.g. Intubation, Paediatric training',
            profile_picture_path VARCHAR(512) NULL COMMENT 'Relative path for profile image (or use contractor via contractor_id)',
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_crew_profiles_contractor (contractor_id)
            """,
        )

        try:
            cur_bn = conn.cursor()
            try:
                from broadnet_bridge import ensure_broadnet_tables
                ensure_broadnet_tables(cur_bn)
                conn.commit()
                print(
                    "Ventus: optional vendor dispatch integration tables ensured (dormant until DB unlock)."
                )
            finally:
                try:
                    cur_bn.close()
                except Exception:
                    pass
        except Exception as ex:
            print(f"Ventus: optional vendor dispatch tables skipped: {ex}")

        print("Ventus response module: install complete.")
    finally:
        conn.close()


def upgrade(seed_demo: bool = False):
    """Idempotent schema upgrade entrypoint (no demo triage re-seed)."""
    print("Ventus response module: running upgrade...")
    install(seed_demo=seed_demo)
    print("Ventus response module: upgrade complete.")


def uninstall(drop_data: bool = False):
    if not drop_data:
        print('uninstall called without --drop-data; nothing to do')
        return
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            tables = [
                "ventus_broadnet_dispatch_push",
                "ventus_broadnet_settings",
                "mdt_crew_profiles",
                "standby_locations",
                "messages",
                "mdt_locations",
                "mdt_dispatch_settings",
                "mdt_dispatch_divisions",
                "mdt_dispatch_assist_requests",
                "mdt_dispatch_user_divisions",
                "mdt_dispatch_user_settings",
                "mdt_job_units",
                "mdt_job_comms",
                "mdt_response_log",
                "mdt_triage_forms",
                "mdts_signed_on",
                "mdt_jobs",
                "response_triage",
                "mdt_positions",
                "mdt_standby_presets",
                MIGRATIONS_TABLE,
            ]
            for t in tables:
                cur.execute(f"DROP TABLE IF EXISTS `{t}`")
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            conn.commit()
            print('Dropped ventus response module tables')
        finally:
            try:
                cur.close()
            except Exception:
                pass
    finally:
        conn.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Ventus Response Module installer')
    parser.add_argument('command', choices=[
                        'install', 'upgrade', 'uninstall'], help='Action')
    parser.add_argument('--drop-data', action='store_true',
                        help='Drop module tables on uninstall')
    parser.add_argument(
        '--seed-demo',
        action='store_true',
        help='Legacy no-op (demo triage profiles removed from product installs).',
    )
    args = parser.parse_args()

    if args.command == 'install':
        print('[INSTALL] Running install...')
        install(seed_demo=args.seed_demo)
        print('[INSTALL] Complete')
    elif args.command == 'upgrade':
        print('[UPGRADE] Running upgrade...')
        upgrade(seed_demo=False)
        print('[UPGRADE] Complete')
    elif args.command == 'uninstall':
        print('[UNINSTALL] Running uninstall...')
        uninstall(drop_data=args.drop_data)
        print('[UNINSTALL] Complete')
