"""
Approximate wage-only cost hints for the CRM event guide, using Time & Billing wage cards.

Optional: requires wage_rate_cards / wage_rate_rows / job_types tables (time_billing_module).
Set SPARROW_CRM_GUIDE_WAGE_CARD_ID to pick a specific card; otherwise the first active card is used.

Explicit **clinical** / **driver** job type IDs are read from ``crm_guide_wage_job_map`` (configure under
CRM → Quotes → Guide & wage mapping) so costing does not rely only on fuzzy name matching.
"""
from __future__ import annotations

import os
import re
from datetime import date
from decimal import Decimal
from typing import Any

from app.objects import get_db_connection

_CLINICAL_HINTS = re.compile(
    r"medic|paramedic|nurse|hca|clinical|technician|technologist|"
    r"health\s*care|healthcare|emt|emergency\s*care",
    re.I,
)
_DRIVER_HINTS = re.compile(
    r"\bdriver\b|ambulance\s*driver|ecas|response\s*driver|crew\s*driver",
    re.I,
)


def _dec(v: Any) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _hours_from_lead_meta(lead_meta: dict[str, Any]) -> Decimal:
    risk = lead_meta.get("risk") or {}
    inp = risk.get("inputs") or {}
    h = inp.get("duration_hours")
    if h is not None and str(h).strip():
        d = _dec(h)
        if d is not None:
            return max(Decimal("1"), min(Decimal("36"), d))
    dh = lead_meta.get("duration_hours")
    if dh is not None and str(dh).strip():
        d = _dec(dh)
        if d is not None:
            return max(Decimal("1"), min(Decimal("36"), d))
    return Decimal("6")


def _resolve_wage_card_id(conn, on_date: date) -> tuple[int | None, str | None]:
    raw = (os.environ.get("SPARROW_CRM_GUIDE_WAGE_CARD_ID") or "").strip()
    if raw.isdigit():
        cid = int(raw)
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, name FROM wage_rate_cards WHERE id=%s LIMIT 1",
                (cid,),
            )
            row = cur.fetchone()
            if row:
                return int(row["id"]), str(row.get("name") or "")
        finally:
            cur.close()
        return None, None

    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """SELECT id, name FROM wage_rate_cards
            WHERE active=1 ORDER BY id ASC LIMIT 1"""
        )
        row = cur.fetchone()
        if row:
            return int(row["id"]), str(row.get("name") or "")
    finally:
        cur.close()
    return None, None


def _effective_wage_rows(conn, card_id: int, on_date: date) -> list[dict[str, Any]]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT wrr.job_type_id, jt.name AS job_type_name, wrr.rate, wrr.effective_from
            FROM wage_rate_rows wrr
            JOIN job_types jt ON jt.id = wrr.job_type_id
            WHERE wrr.rate_card_id = %s
              AND wrr.effective_from <= %s
              AND (wrr.effective_to IS NULL OR wrr.effective_to >= %s)
            ORDER BY jt.name ASC, wrr.effective_from DESC, wrr.id DESC
            """,
            (card_id, on_date, on_date),
        )
        rows = cur.fetchall() or []
    finally:
        cur.close()
    best: dict[int, dict[str, Any]] = {}
    for r in rows:
        jtid = int(r["job_type_id"])
        if jtid not in best:
            best[jtid] = r
    return list(best.values())


def _job_type_name(conn, job_type_id: int) -> str:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT name FROM job_types WHERE id=%s", (int(job_type_id),))
        row = cur.fetchone()
        return str(row["name"]) if row else f"Job type #{job_type_id}"
    finally:
        cur.close()


def _rate_from_effective_rows(
    rows: list[dict[str, Any]], job_type_id: int
) -> tuple[Decimal | None, str | None]:
    for r in rows:
        if int(r["job_type_id"]) != int(job_type_id):
            continue
        rate = _dec(r.get("rate"))
        if rate is not None and rate > 0:
            return rate, str(r.get("job_type_name") or "")
    return None, None


def load_guide_wage_slot_map(conn) -> dict[str, int]:
    """slot -> job_type_id for CRM-configured guide costing (clinical / driver)."""
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT slot, job_type_id FROM crm_guide_wage_job_map")
            return {str(row[0]): int(row[1]) for row in (cur.fetchall() or [])}
        finally:
            cur.close()
    except Exception:
        return {}


def save_guide_wage_slot_map(
    conn,
    *,
    clinical_job_type_id: int | None,
    driver_job_type_id: int | None,
) -> None:
    cur = conn.cursor()
    try:
        for slot, jtid in (
            ("clinical", clinical_job_type_id),
            ("driver", driver_job_type_id),
        ):
            if jtid is None:
                cur.execute(
                    "DELETE FROM crm_guide_wage_job_map WHERE slot=%s", (slot,)
                )
            else:
                cur.execute(
                    """
                    INSERT INTO crm_guide_wage_job_map (slot, job_type_id)
                    VALUES (%s,%s)
                    ON DUPLICATE KEY UPDATE job_type_id=VALUES(job_type_id)
                    """,
                    (slot, int(jtid)),
                )
        conn.commit()
    finally:
        cur.close()


def list_job_types_on_wage_card(
    conn, card_id: int, on_date: date
) -> list[dict[str, Any]]:
    rows = _effective_wage_rows(conn, card_id, on_date)
    seen: dict[int, dict[str, Any]] = {}
    for r in rows:
        jid = int(r["job_type_id"])
        if jid not in seen:
            seen[jid] = {
                "id": jid,
                "name": str(r.get("job_type_name") or f"#{jid}"),
                "rate": r.get("rate"),
            }
    return sorted(seen.values(), key=lambda x: str(x["name"]).lower())


def list_all_job_types(conn) -> list[dict[str, Any]]:
    try:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT id, name FROM job_types ORDER BY name ASC")
            return cur.fetchall() or []
        finally:
            cur.close()
    except Exception:
        return []


def pick_clinical_and_driver_rates(
    rows: list[dict[str, Any]],
) -> tuple[Decimal | None, Decimal | None, str | None, str | None]:
    """
    Pick representative £/h rates from job type names.
    Uses the highest matching clinical rate (conservative wage estimate) and a driver/crew rate.
    """
    clinical: list[tuple[Decimal, str]] = []
    drivers: list[tuple[Decimal, str]] = []
    for r in rows:
        name = str(r.get("job_type_name") or "")
        rate = _dec(r.get("rate"))
        if rate is None or rate <= 0:
            continue
        lname = name.lower()
        if _DRIVER_HINTS.search(name):
            drivers.append((rate, name))
        elif _CLINICAL_HINTS.search(name) and "driver" not in lname:
            clinical.append((rate, name))
    med_r, med_l = (None, None)
    if clinical:
        med_r, med_l = max(clinical, key=lambda x: x[0])
    drv_r, drv_l = (None, None)
    if drivers:
        drv_r, drv_l = max(drivers, key=lambda x: x[0])
    if drv_r is None and med_r is not None:
        for r in rows:
            name = str(r.get("job_type_name") or "")
            rate = _dec(r.get("rate"))
            if rate is None:
                continue
            lname = name.lower()
            if "driver" in lname or "crew" in lname:
                drv_r, drv_l = rate, name
                break
    return med_r, drv_r, med_l, drv_l


def estimate_guide_staffing_costs(
    lead_meta: dict[str, Any] | None,
    *,
    on_date: date | None = None,
) -> dict[str, Any] | None:
    """
    Returns a dict for templates, or None if lead_meta has no risk block / no staffing hints.
    On missing TB tables or rates, returns partial dict with message (no exception).
    """
    if not lead_meta or not isinstance(lead_meta, dict):
        return None
    risk = lead_meta.get("risk") or {}
    if not risk.get("suggested_medics") and not risk.get("suggested_vehicles"):
        return None

    on = on_date or date.today()
    medics = int(risk.get("suggested_medics") or 0)
    vehicles = int(risk.get("suggested_vehicles") or 0)
    medics = max(0, medics)
    vehicles = max(0, vehicles)
    hours = _hours_from_lead_meta(lead_meta)
    hf = float(hours)

    out: dict[str, Any] = {
        "hours": hf,
        "medics": medics,
        "vehicles": vehicles,
        "lines": [],
        "total_gbp": None,
        "wage_card_name": None,
        "wage_card_id": None,
        "tb_note": None,
        "clinical_job": None,
        "driver_job": None,
        "used_clinical_mapping": False,
        "used_driver_mapping": False,
    }

    conn = get_db_connection()
    try:
        card_id, card_name = _resolve_wage_card_id(conn, on)
        if not card_id:
            out["tb_note"] = (
                "No wage rate card found. Add one under Time & Billing → Wage cards, "
                "or set SPARROW_CRM_GUIDE_WAGE_CARD_ID."
            )
            return out
        out["wage_card_id"] = card_id
        out["wage_card_name"] = card_name or f"Card #{card_id}"
        rows = _effective_wage_rows(conn, card_id, on)
        if not rows:
            out["tb_note"] = (
                "This wage card has no rates effective for today’s date for your job types."
            )
            return out

        slot_map = load_guide_wage_slot_map(conn)
        h_med, h_drv, h_ml, h_dl = pick_clinical_and_driver_rates(rows)

        med_rate: Decimal | None
        med_label: str | None
        if "clinical" in slot_map:
            med_rate, med_label = _rate_from_effective_rows(
                rows, slot_map["clinical"]
            )
            if med_rate is None and medics:
                nm = _job_type_name(conn, slot_map["clinical"])
                med_label = nm
                out["tb_note"] = (
                    f"Clinical is mapped to “{nm}” (#{slot_map['clinical']}) but that job type "
                    f"has no £/h row on wage card “{card_name}” for the costing date. "
                    "Add a rate row in Time & Billing or change the mapping under CRM → Quotes & rules → Guide & wage mapping."
                )
        else:
            med_rate, med_label = h_med, h_ml

        drv_rate: Decimal | None
        drv_label: str | None
        if "driver" in slot_map:
            drv_rate, drv_label = _rate_from_effective_rows(
                rows, slot_map["driver"]
            )
            if drv_rate is None and vehicles:
                nm = _job_type_name(conn, slot_map["driver"])
                drv_label = nm
                extra = (
                    f"Driver/crew is mapped to “{nm}” (#{slot_map['driver']}) with no £/h on this card for the costing date — "
                    "using automatic name match if possible."
                )
                out["tb_note"] = (
                    f"{out['tb_note']} {extra}".strip()
                    if out.get("tb_note")
                    else extra
                )
            if drv_rate is None:
                drv_rate, drv_label = h_drv, h_dl
        else:
            drv_rate, drv_label = h_drv, h_dl

        out["clinical_job"] = med_label
        out["driver_job"] = drv_label
        out["used_clinical_mapping"] = "clinical" in slot_map
        out["used_driver_mapping"] = "driver" in slot_map

        total = Decimal("0")
        if medics and med_rate is not None:
            line_cost = (med_rate * hours) * medics
            total += line_cost
            out["lines"].append(
                {
                    "label": "Clinical / medic cover (wage estimate)",
                    "detail": f"{medics} × {med_label or 'clinical role'} × {hours} h @ £{med_rate}/h",
                    "amount_gbp": float(line_cost.quantize(Decimal("0.01"))),
                }
            )
        elif medics and med_rate is None:
            if "clinical" not in slot_map:
                out["tb_note"] = (
                    "Could not match a clinical job type on the wage card "
                    "(name pattern), or set an explicit mapping under CRM → Quotes & rules → Guide & wage mapping."
                )

        crew_slots = vehicles * 2 if vehicles else 0
        if crew_slots and drv_rate is not None:
            line_cost = (drv_rate * hours) * crew_slots
            total += line_cost
            out["lines"].append(
                {
                    "label": "Response drivers / crew (wage estimate)",
                    "detail": f"{vehicles} vehicles × ~2 crew × {hours} h @ £{drv_rate}/h "
                    f"({drv_label or 'driver role'})",
                    "amount_gbp": float(line_cost.quantize(Decimal("0.01"))),
                }
            )
        elif vehicles and drv_rate is None and med_rate is not None:
            line_cost = (med_rate * Decimal("0.85") * hours) * crew_slots
            total += line_cost
            out["lines"].append(
                {
                    "label": "Response crew (wage estimate, using clinical rate × 0.85)",
                    "detail": f"{vehicles} vehicles × ~2 crew × {hours} h (no driver job type matched)",
                    "amount_gbp": float(line_cost.quantize(Decimal("0.01"))),
                }
            )
        elif vehicles and drv_rate is None and med_rate is None:
            pass

        if out["lines"]:
            out["total_gbp"] = float(total.quantize(Decimal("0.01")))
    except Exception:
        out["tb_note"] = (
            "Time & Billing wage tables are not available or could not be read. "
            "Staffing counts above still reflect the purple guide."
        )
    finally:
        conn.close()

    return out
