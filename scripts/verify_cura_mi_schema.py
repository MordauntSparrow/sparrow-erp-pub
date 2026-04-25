#!/usr/bin/env python3
"""
Verify Cura / Minor Injury / operational-event tables exist (medical_records_module).

Run from project root (Flask app context / DB config must load):
  python scripts/verify_cura_mi_schema.py

Exit 0 = all required tables present; 1 = missing required table or DB error.
Optional tables print WARN only.

After a failure, apply upgrades:
  python app/plugins/medical_records_module/install.py upgrade
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Core path for MI event manager, incident-report API, assignments, PCR/SG rollups.
REQUIRED_TABLES = (
    "mr_cura_migrations",
    "cura_operational_events",
    "cura_operational_event_assignments",
    "cura_mi_events",
    "cura_mi_assignments",
    "cura_mi_reports",
    "cura_mi_notices",
    "cura_mi_documents",
    "cura_patient_contact_reports",
    "cura_safeguarding_referrals",
)

OPTIONAL_TABLES = (
    "cura_file_attachments",
    "cura_callsign_mdt_validation_log",
    "cura_tenant_datasets",
    "cura_tenant_settings",
    "cura_safeguarding_audit_events",
    "cura_locations",
    "cura_mpi_risk_flags",
)


def _list_tables(cur) -> set[str]:
    cur.execute(
        """
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
        """
    )
    return {row[0] for row in (cur.fetchall() or []) if row and row[0]}


def _has_column(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
        LIMIT 1
        """,
        (table, column),
    )
    return cur.fetchone() is not None


def main() -> int:
    try:
        from app.objects import get_db_connection
    except Exception as e:
        print("FAIL: could not import app.objects (run from project root, with env configured):", e)
        return 1

    try:
        conn = get_db_connection()
    except Exception as e:
        print("FAIL: could not connect to database:", e)
        return 1

    cur = conn.cursor()
    try:
        try:
            have = _list_tables(cur)
        except Exception as e:
            print("FAIL: could not read INFORMATION_SCHEMA:", e)
            return 1

        missing = [t for t in REQUIRED_TABLES if t not in have]
        if missing:
            print("FAIL: missing required tables:", ", ".join(missing))
            print("  Fix: python app/plugins/medical_records_module/install.py upgrade")
            return 1

        for t in REQUIRED_TABLES:
            print("OK: table", t)

        for t in OPTIONAL_TABLES:
            if t not in have:
                print("WARN: optional table missing (feature may be limited):", t)
            else:
                print("OK: optional", t)

        if not _has_column(cur, "cura_mi_events", "config_json"):
            print("FAIL: cura_mi_events.config_json column missing (unexpected schema)")
            return 1
        print("OK: column cura_mi_events.config_json")

        # Incident report joins operational_event_id on case JSON only; PCR/SG use FK columns.
        for tbl, col in (
            ("cura_patient_contact_reports", "operational_event_id"),
            ("cura_safeguarding_referrals", "operational_event_id"),
        ):
            if not _has_column(cur, tbl, col):
                print(f"FAIL: {tbl}.{col} missing — run medical_records install upgrade")
                return 1
            print(f"OK: column {tbl}.{col}")

        print("")
        print("PASS: Cura / Minor Injury schema looks ready for deployment checks.")
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
