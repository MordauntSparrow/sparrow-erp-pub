"""Optional integration with Fleet Management for event plan roster vehicle assignment."""
from __future__ import annotations

from typing import Any

FLEET_VEHICLE_ONFOOT = "onfoot"


def _vehicle_label(row: dict[str, Any]) -> str:
    code = str(row.get("internal_code") or "").strip()
    reg = str(row.get("registration") or "").strip()
    mm = " ".join(
        x
        for x in (
            str(row.get("make") or "").strip(),
            str(row.get("model") or "").strip(),
        )
        if x
    )
    bits = [b for b in (code, reg, mm) if b]
    if bits:
        return " — ".join(bits)[:255]
    return f"Vehicle #{row.get('id')}"


def list_fleet_vehicles_for_event_plan_roster(conn) -> list[dict[str, Any]]:
    """
    Active fleet rows for roster dropdowns (excludes decommissioned).

    Returns ``[{"id": int, "label": str}, ...]``. Empty if fleet tables are missing.
    """
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, internal_code, registration, make, model
            FROM fleet_vehicles
            WHERE status <> 'decommissioned'
            ORDER BY internal_code ASC, id ASC
            LIMIT 500
            """
        )
        raw = cur.fetchall() or []
    except Exception:
        return []
    finally:
        try:
            cur.close()
        except Exception:
            pass
    out: list[dict[str, Any]] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        try:
            vid = int(r["id"])
        except (TypeError, ValueError, KeyError):
            continue
        out.append({"id": vid, "label": _vehicle_label(r)})
    return out


def _labels_for_vehicle_ids(
    conn, ids: set[int]
) -> dict[int, str]:
    if not ids:
        return {}
    cur = conn.cursor(dictionary=True)
    try:
        placeholders = ",".join(["%s"] * len(ids))
        cur.execute(
            f"""
            SELECT id, internal_code, registration, make, model
            FROM fleet_vehicles
            WHERE id IN ({placeholders})
            """,
            tuple(ids),
        )
        rows = cur.fetchall() or []
    except Exception:
        return {}
    finally:
        try:
            cur.close()
        except Exception:
            pass
    return {
        int(r["id"]): _vehicle_label(r)
        for r in rows
        if isinstance(r, dict) and r.get("id") is not None
    }


def apply_fleet_vehicle_display_to_slots(
    slots: list[dict[str, Any] | None], conn
) -> None:
    """Mutates each roster slot dict with ``fleet_vehicle_display`` for HTML preview."""
    ids: set[int] = set()
    for s in slots:
        if not s or not isinstance(s, dict):
            continue
        fv = str(s.get("fleet_vehicle") or "").strip()
        if fv.isdigit():
            try:
                ids.add(int(fv))
            except ValueError:
                pass
    labels = _labels_for_vehicle_ids(conn, ids)
    for s in slots:
        if not s or not isinstance(s, dict):
            continue
        fv = str(s.get("fleet_vehicle") or "").strip()
        if fv == FLEET_VEHICLE_ONFOOT:
            s["fleet_vehicle_display"] = "On foot"
        elif fv.isdigit():
            i = int(fv)
            s["fleet_vehicle_display"] = labels.get(i) or f"Vehicle #{i}"
        else:
            s["fleet_vehicle_display"] = ""


def enrich_roster_rows_for_pdf(
    rows: list[dict[str, Any]], conn
) -> list[dict[str, Any]]:
    """Copy roster rows and add ``fleet_vehicle_display`` for PDF rendering."""
    ids: set[int] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        fv = str(r.get("fleet_vehicle") or "").strip()
        if fv.isdigit():
            try:
                ids.add(int(fv))
            except ValueError:
                pass
    labels = _labels_for_vehicle_ids(conn, ids)
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        nr = dict(r)
        fv = str(nr.get("fleet_vehicle") or "").strip()
        if fv == FLEET_VEHICLE_ONFOOT:
            nr["fleet_vehicle_display"] = "On foot"
        elif fv.isdigit():
            i = int(fv)
            nr["fleet_vehicle_display"] = labels.get(i) or f"Vehicle #{i}"
        else:
            nr["fleet_vehicle_display"] = ""
        out.append(nr)
    return out
