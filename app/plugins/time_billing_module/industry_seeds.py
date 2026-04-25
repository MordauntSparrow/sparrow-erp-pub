"""
Industry-tagged starter data for Time & Billing (roles, job types, wage rows).

Reads tenant industries from Core manifest (``organization_profile.industries``), same as runtime.
Called after SQL migrations from ``install.install()`` / ``install.upgrade()``.

Neutral baseline lives in ``db/007_seed_minimum.sql`` (Staff + Standard shift + two cards).
This module adds pack rows for cleaning / medical / security / hospitality when those slugs are selected.

**Opt-in (default off):** Packs do not run unless ``SPARROW_ENABLE_TIME_BILLING_INDUSTRY_SEEDS=1``
(or ``SPARROW_FORCE_TIME_BILLING_INDUSTRY_SEEDS=1`` for a one-off re-apply). Fresh deploys therefore
keep only the neutral SQL baseline (e.g. ``Standard shift`` from ``007_seed_minimum.sql``), not
medical/cleaning/etc. job types.

**One-shot per DB when enabled:** After a successful run, a sentinel row is stored in
``tb_time_billing_migrations`` so ``upgrade()`` does not re-upsert packs on every deploy (which would
resurrect deleted job types / roles via ``ON DUPLICATE KEY UPDATE``). Force removes the sentinel first.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import List, Sequence, Tuple

from app.organization_profile import load_tenant_industries_for_install, tenant_matches_industry

# Synthetic ledger filename — not an on-disk migration; marks industry pack seed as done.
_INDUSTRY_SEED_PACKS_LEDGER_KEY = "__time_billing_industry_seed_packs_applied__"


def _force_time_billing_industry_seeds() -> bool:
    v = (os.environ.get("SPARROW_FORCE_TIME_BILLING_INDUSTRY_SEEDS") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _enable_time_billing_industry_seeds() -> bool:
    """Default off: industry packs (medical 'Care visit', etc.) are not applied on init/upgrade."""
    v = (os.environ.get("SPARROW_ENABLE_TIME_BILLING_INDUSTRY_SEEDS") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _time_billing_industry_seeds_should_run() -> bool:
    """Run packs when explicitly enabled, or when forcing a re-apply (support / dev)."""
    return _enable_time_billing_industry_seeds() or _force_time_billing_industry_seeds()


def _industry_seed_packs_already_completed(conn) -> bool:
    if _force_time_billing_industry_seeds():
        return False
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE %s", ("tb_time_billing_migrations",))
        if not cur.fetchone():
            return False
        cur.execute(
            "SELECT 1 FROM tb_time_billing_migrations WHERE filename = %s LIMIT 1",
            (_INDUSTRY_SEED_PACKS_LEDGER_KEY,),
        )
        return bool(cur.fetchone())
    finally:
        cur.close()


def _clear_industry_seed_marker(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM tb_time_billing_migrations WHERE filename = %s",
            (_INDUSTRY_SEED_PACKS_LEDGER_KEY,),
        )
        conn.commit()
    finally:
        cur.close()


def _record_industry_seed_packs_completed(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT IGNORE INTO tb_time_billing_migrations (filename) VALUES (%s)",
            (_INDUSTRY_SEED_PACKS_LEDGER_KEY,),
        )
        conn.commit()
    finally:
        cur.close()


# (name, code)
_RoleRow = Tuple[str, str]
# (name, code, colour_hex)
_JtRow = Tuple[str, str, str]

CLEANING_ROLES: Sequence[_RoleRow] = (
    ("Cleaner", "Cleaner"),
    ("Specialist", "Specialist"),
)
CLEANING_JOB_TYPES: Sequence[_JtRow] = (
    ("School Clean", "School Clean", "#2e7d32"),
    ("Residential Clean", "Residential Clean", "#388e3c"),
    ("Commercial Clean", "Commercial Clean", "#43a047"),
    ("Deep Clean", "Deep Clean", "#1b5e20"),
    ("End of Tenancy Clean", "End of Tenancy Clean", "#0d47a1"),
)

MEDICAL_ROLES: Sequence[_RoleRow] = (
    ("Care staff", "Care staff"),
    ("Clinical role", "Clinical role"),
)
MEDICAL_JOB_TYPES: Sequence[_JtRow] = (
    ("Patient transport", "Patient transport", "#1565c0"),
    ("Clinical shift", "Clinical shift", "#c62828"),
    ("On-call standby", "On-call standby", "#6a1b9a"),
    ("Care visit", "Care visit", "#00695c"),
)

SECURITY_ROLES: Sequence[_RoleRow] = (("Security officer", "Security officer"),)
SECURITY_JOB_TYPES: Sequence[_JtRow] = (
    ("Static guarding", "Static guarding", "#37474f"),
    ("Mobile patrol", "Mobile patrol", "#455a64"),
    ("Keyholding response", "Keyholding response", "#263238"),
)

HOSPITALITY_ROLES: Sequence[_RoleRow] = (("Hospitality staff", "Hospitality staff"),)
HOSPITALITY_JOB_TYPES: Sequence[_JtRow] = (
    ("F&B service", "F&B service", "#ef6c00"),
    ("Reception / front desk", "Reception / front desk", "#f57c00"),
    ("Housekeeping", "Housekeeping", "#fb8c00"),
    ("Events support", "Events support", "#e65100"),
)


def _jt_rate(code: str, pack: str) -> Decimal:
    """Illustrative rates per pack; deep / on-call slightly higher."""
    if pack == "cleaning" and code in ("Deep Clean", "End of Tenancy Clean"):
        return Decimal("15.00")
    if pack == "medical" and code in ("Clinical shift", "On-call standby"):
        return Decimal("22.00")
    if pack == "security" and code in ("Keyholding response", "Mobile patrol"):
        return Decimal("14.50")
    if pack == "hospitality" and code in ("Events support",):
        return Decimal("13.50")
    if pack == "medical":
        return Decimal("18.00")
    if pack == "security":
        return Decimal("12.50")
    if pack == "hospitality":
        return Decimal("11.50")
    return Decimal("14.50")


def apply_time_billing_industry_seed_packs(conn) -> None:
    """
    Upsert roles / job types; insert wage_rate_rows only when missing (INSERT IGNORE).

    Uses wage cards from neutral seed: ``Organisation default`` and ``Alternate card``.
    Completes at most once per database when enabled (see module docstring).
    """
    if not _time_billing_industry_seeds_should_run():
        return

    industries: List[str] = load_tenant_industries_for_install()

    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'roles'")
        if not cur.fetchone():
            return
        cur.execute("SHOW TABLES LIKE 'job_types'")
        if not cur.fetchone():
            return
    finally:
        cur.close()

    if _industry_seed_packs_already_completed(conn):
        return
    if _force_time_billing_industry_seeds():
        _clear_industry_seed_marker(conn)

    packs: List[Tuple[str, Sequence[_RoleRow], Sequence[_JtRow]]] = []
    if tenant_matches_industry(industries, "cleaning"):
        packs.append(("cleaning", CLEANING_ROLES, CLEANING_JOB_TYPES))
    if tenant_matches_industry(industries, "medical"):
        packs.append(("medical", MEDICAL_ROLES, MEDICAL_JOB_TYPES))
    if tenant_matches_industry(industries, "security"):
        packs.append(("security", SECURITY_ROLES, SECURITY_JOB_TYPES))
    if tenant_matches_industry(industries, "hospitality"):
        packs.append(("hospitality", HOSPITALITY_ROLES, HOSPITALITY_JOB_TYPES))

    cur = conn.cursor()
    try:
        for pack_name, roles, job_types in packs:
            for name, code in roles:
                cur.execute(
                    """
                    INSERT INTO roles (name, code, active)
                    VALUES (%s, %s, 1)
                    ON DUPLICATE KEY UPDATE name = VALUES(name), active = 1
                    """,
                    (name, code),
                )
            for name, code, colour in job_types:
                cur.execute(
                    """
                    INSERT INTO job_types (name, code, active, colour_hex)
                    VALUES (%s, %s, 1, %s)
                    ON DUPLICATE KEY UPDATE
                      name = VALUES(name),
                      active = 1,
                      colour_hex = COALESCE(VALUES(colour_hex), colour_hex)
                    """,
                    (name, code, colour),
                )

        def _card_id(preferred: str, *fallbacks: str):
            for nm in (preferred,) + fallbacks:
                cur.execute(
                    "SELECT id FROM wage_rate_cards WHERE name = %s LIMIT 1", (nm,)
                )
                r = cur.fetchone()
                if r:
                    return r[0]
            return None

        default_card_id = _card_id(
            "Organisation default",
            "Default Rate Card",
        )
        premium_card_id = _card_id("Alternate card", "Premium Rate Card")
        if not default_card_id or not premium_card_id:
            conn.commit()
            _record_industry_seed_packs_completed(conn)
            return

        for pack_name, _roles, job_types in packs:
            for _n, code, _c in job_types:
                cur.execute(
                    "SELECT id FROM job_types WHERE code = %s LIMIT 1", (code,)
                )
                jrow = cur.fetchone()
                if not jrow:
                    continue
                jid = jrow[0]
                rate = _jt_rate(code, pack_name)
                prem = rate + Decimal("0.50")
                for cid, r in (
                    (default_card_id, rate),
                    (premium_card_id, prem),
                ):
                    cur.execute(
                        """
                        INSERT IGNORE INTO wage_rate_rows
                          (rate_card_id, job_type_id, rate, effective_from)
                        VALUES (%s, %s, %s, CURDATE())
                        """,
                        (cid, jid, r),
                    )

        conn.commit()
        _record_industry_seed_packs_completed(conn)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
