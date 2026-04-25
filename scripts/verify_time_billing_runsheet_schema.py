#!/usr/bin/env python3
"""
Production checklist: verify runsheet_assignments payroll columns (010) and
runsheet free-text client columns (012).

Run from project root:
  python scripts/verify_time_billing_runsheet_schema.py

Exit 0 = OK, 1 = missing columns or DB error.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

REQUIRED_COLUMNS = (
    "payroll_included",
    "withdrawn_at",
    "withdrawn_by_user_id",
    "reactivated_at",
)
MIGRATION_010 = "010_runsheet_assignment_payroll.sql"
MIGRATION_012 = "012_runsheet_free_text_client.sql"
RUNSHEET_COLUMNS = (
    "client_free_text",
    "site_free_text",
)
TEMPLATE_COLUMNS = ("allow_free_text_client_site",)
ROTA_ROLE_COLUMNS = ("ladder_rank",)  # roles — 015
RUNSHEET_ROTA_COLUMNS = (
    "required_role_id",
    "shift_address_line1",
    "shift_pay_model",
    "shift_pay_rate",
)
ASSIGNMENT_ROTA_COLUMNS = (
    "role_eligibility_override",
    "role_eligibility_override_reason",
)
MIGRATION_015 = "015_rota_role_eligibility.sql"
MIGRATION_016 = "016_rota_shift_address_pay.sql"


def main() -> int:
    try:
        from app.objects import get_db_connection
    except Exception as e:
        print("FAIL: could not import app.objects (run from project root):", e)
        return 1

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'runsheet_assignments'
            """
        )
        have = {row[0] for row in (cur.fetchall() or [])}
        missing = [c for c in REQUIRED_COLUMNS if c not in have]
        if missing:
            print("FAIL: runsheet_assignments missing columns:", ", ".join(missing))
            print(
                "  Apply migrations:  python -m app.plugins.time_billing_module.install upgrade"
            )
            return 1

        print("OK: runsheet_assignments has columns:", ", ".join(REQUIRED_COLUMNS))

        cur.execute(
            """
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'runsheets'
            """
        )
        rs_have = {row[0] for row in (cur.fetchall() or [])}
        rs_missing = [c for c in RUNSHEET_COLUMNS if c not in rs_have]
        if rs_missing:
            print("FAIL: runsheets missing columns:", ", ".join(rs_missing))
            print(
                "  Apply migrations:  python -m app.plugins.time_billing_module.install upgrade"
            )
            return 1
        print("OK: runsheets has columns:", ", ".join(RUNSHEET_COLUMNS))

        cur.execute(
            """
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'runsheet_templates'
            """
        )
        rt_have = {row[0] for row in (cur.fetchall() or [])}
        rt_missing = [c for c in TEMPLATE_COLUMNS if c not in rt_have]
        if rt_missing:
            print("FAIL: runsheet_templates missing columns:", ", ".join(rt_missing))
            print(
                "  Apply migrations:  python -m app.plugins.time_billing_module.install upgrade"
            )
            return 1
        print("OK: runsheet_templates has columns:", ", ".join(TEMPLATE_COLUMNS))

        cur.execute(
            """
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'roles'
            """
        )
        role_have = {row[0] for row in (cur.fetchall() or [])}
        role_missing = [c for c in ROTA_ROLE_COLUMNS if c not in role_have]
        if role_missing:
            print("FAIL: roles missing columns:", ", ".join(role_missing))
            print(
                "  Apply migrations:  python -m app.plugins.time_billing_module.install upgrade"
            )
            return 1
        print("OK: roles has columns:", ", ".join(ROTA_ROLE_COLUMNS))

        cur.execute(
            """
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'runsheets'
            """
        )
        rs2_have = {row[0] for row in (cur.fetchall() or [])}
        rs2_missing = [c for c in RUNSHEET_ROTA_COLUMNS if c not in rs2_have]
        if rs2_missing:
            print("FAIL: runsheets missing ROTA columns:", ", ".join(rs2_missing))
            print(
                "  Apply migrations:  python -m app.plugins.time_billing_module.install upgrade"
            )
            return 1
        print("OK: runsheets has ROTA columns:", ", ".join(RUNSHEET_ROTA_COLUMNS))

        cur.execute(
            """
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'runsheet_assignments'
            """
        )
        ra_have = {row[0] for row in (cur.fetchall() or [])}
        ra_missing = [c for c in ASSIGNMENT_ROTA_COLUMNS if c not in ra_have]
        if ra_missing:
            print(
                "FAIL: runsheet_assignments missing ROTA columns:",
                ", ".join(ra_missing),
            )
            print(
                "  Apply migrations:  python -m app.plugins.time_billing_module.install upgrade"
            )
            return 1
        print(
            "OK: runsheet_assignments has ROTA columns:",
            ", ".join(ASSIGNMENT_ROTA_COLUMNS),
        )

        cur.execute(
            "SHOW TABLES LIKE %s",
            ("tb_runsheet_role_audit",),
        )
        if not cur.fetchone():
            print("FAIL: tb_runsheet_role_audit table missing (migration 015).")
            return 1
        print("OK: tb_runsheet_role_audit exists")

        cur.execute(
            "SHOW TABLES LIKE %s",
            ("tb_time_billing_migrations",),
        )
        if cur.fetchone():
            for mf in (MIGRATION_010, MIGRATION_012):
                cur.execute(
                    "SELECT 1 FROM tb_time_billing_migrations WHERE filename = %s LIMIT 1",
                    (mf,),
                )
                if cur.fetchone():
                    print(f"OK: migration ledger records {mf}")
                else:
                    print(
                        f"WARN: {mf} not in tb_time_billing_migrations "
                        "(columns may have been applied manually)."
                    )
            for mf in (MIGRATION_015, MIGRATION_016):
                cur.execute(
                    "SELECT 1 FROM tb_time_billing_migrations WHERE filename = %s LIMIT 1",
                    (mf,),
                )
                if cur.fetchone():
                    print(f"OK: migration ledger records {mf}")
                else:
                    print(
                        f"WARN: {mf} not in tb_time_billing_migrations "
                        "(columns may have been applied manually)."
                    )
        else:
            print("WARN: tb_time_billing_migrations not found (skip ledger check).")

        return 0
    except Exception as e:
        print("FAIL: database error:", e)
        return 1
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
