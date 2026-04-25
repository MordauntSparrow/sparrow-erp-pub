"""Fleet data access layer."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

from app.objects import get_db_connection

logger = logging.getLogger("fleet_management")

# Kanban columns for fleet issue tracker (admin)
FLEET_ISSUE_STAGES = (
    "reported",
    "triage",
    "scheduled",
    "in_garage",
    "pending_road_test",
    "done",
)

# Manual mileage log guards (typos, wrong vehicle, extra digit). Fleet editors may override.
FLEET_MILEAGE_MAX_SINGLE_TRIP_MI = 1200
FLEET_MILEAGE_DROP_BELOW_REF_MI = 150
FLEET_MILEAGE_SPIKE_ABOVE_REF_MI = 2500
FLEET_MILEAGE_END_FAR_BELOW_REF_MI = 400


def fleet_validate_manual_mileage_trip(
    start_mi: int, end_mi: int, last_known_mi: Optional[int]
) -> Optional[str]:
    """
    Return a short error message if the trip should be blocked, else None.
    last_known_mi is the highest odometer seen in mileage logs + VDI submissions (or None).
    """
    if start_mi < 0 or end_mi < 0:
        return "Mileage cannot be negative."
    if end_mi < start_mi:
        return "End mileage must not be less than start mileage."
    trip = end_mi - start_mi
    if trip > FLEET_MILEAGE_MAX_SINGLE_TRIP_MI:
        return (
            f"This log covers {trip:,} mi in one entry (limit {FLEET_MILEAGE_MAX_SINGLE_TRIP_MI:,} mi). "
            "If that is wrong, correct the numbers; fleet editors can use Override below if it is genuinely correct."
        )
    if last_known_mi is None:
        return None
    ref = int(last_known_mi)
    if start_mi < ref - FLEET_MILEAGE_DROP_BELOW_REF_MI:
        return (
            f"Start ({start_mi:,} mi) is more than {FLEET_MILEAGE_DROP_BELOW_REF_MI} mi below the last known "
            f"reading ({ref:,} mi). Check for a typo, missing digit, or wrong vehicle — or ask a fleet editor to override."
        )
    if end_mi > ref + FLEET_MILEAGE_SPIKE_ABOVE_REF_MI:
        return (
            f"End ({end_mi:,} mi) is more than {FLEET_MILEAGE_SPIKE_ABOVE_REF_MI} mi above the last known "
            f"reading ({ref:,} mi). Check for an extra digit — or a fleet editor can override if correct."
        )
    if end_mi < ref - FLEET_MILEAGE_END_FAR_BELOW_REF_MI:
        return (
            f"End ({end_mi:,} mi) is far below the last known reading ({ref:,} mi). "
            "If the odometer was replaced, a fleet editor can override."
        )
    return None


def list_time_billing_role_names_for_picklist() -> List[str]:
    """
    Distinct names from `roles` (contractor / time billing).
    Fleet safety portal hints are matched against these role names in the employee portal.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT TRIM(name) AS n FROM roles
            WHERE name IS NOT NULL AND TRIM(name) <> ''
            ORDER BY n ASC
            """
        )
        return [str(r[0]) for r in (cur.fetchall() or []) if r and r[0]]
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def get_fleet_service():
    return FleetService()


class FleetService:
    def _conn(self):
        return get_db_connection()

    def _log_audit(
        self,
        *,
        user_id: Optional[str],
        action: str,
        entity_type: str,
        entity_id: Optional[str] = None,
        details: Optional[dict] = None,
    ) -> None:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO fleet_audit (user_id, action, entity_type, entity_id, details)
                VALUES (%s,%s,%s,%s,%s)
                """,
                (
                    user_id,
                    action,
                    entity_type,
                    entity_id,
                    json.dumps(details or {}, default=str),
                ),
            )
            conn.commit()
        except Exception:
            logger.exception("fleet_audit insert failed")
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def next_internal_code(self) -> str:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT COALESCE(MAX(id), 0) FROM fleet_vehicles")
            n = cur.fetchone()[0] or 0
            return f"FLEET-{int(n) + 1:04d}"
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_vehicles(
        self,
        *,
        status: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 200,
        skip: int = 0,
    ) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            sql = """
            SELECT v.*,
                   c.mot_expiry, c.tax_expiry, c.insurance_expiry,
                   c.last_service_date, c.last_service_mileage,
                   c.next_service_due_date, c.next_service_due_mileage, c.servicing_notes,
                   vt.name AS vehicle_type_name,
                   vt.safety_check_interval_days AS type_safety_interval_days
            FROM fleet_vehicles v
            LEFT JOIN fleet_compliance c ON c.vehicle_id = v.id
            LEFT JOIN fleet_vehicle_types vt ON vt.id = v.vehicle_type_id
            WHERE 1=1
            """
            params: List[Any] = []
            if status:
                sql += " AND v.status = %s"
                params.append(status)
            if search:
                sql += " AND (v.registration LIKE %s OR v.internal_code LIKE %s OR v.make LIKE %s OR v.model LIKE %s)"
                q = f"%{search.strip()}%"
                params.extend([q, q, q, q])
            sql += " ORDER BY v.internal_code ASC, v.registration ASC LIMIT %s OFFSET %s"
            params.extend([min(max(limit, 1), 500), max(skip, 0)])
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
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_vehicle(self, vehicle_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT v.*,
                       c.mot_expiry, c.tax_expiry, c.insurance_expiry,
                       c.last_service_date, c.last_service_mileage,
                       c.next_service_due_date, c.next_service_due_mileage, c.servicing_notes,
                       vt.name AS vehicle_type_name,
                       vt.safety_check_interval_days AS type_safety_interval_days
                FROM fleet_vehicles v
                LEFT JOIN fleet_compliance c ON c.vehicle_id = v.id
                LEFT JOIN fleet_vehicle_types vt ON vt.id = v.vehicle_type_id
                WHERE v.id = %s
                """,
                (int(vehicle_id),),
            )
            row = cur.fetchone()
            if row and isinstance(row.get("metadata"), str):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except Exception:
                    pass
            return row
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_vehicle_by_registration(self, registration: str) -> Optional[Dict[str, Any]]:
        reg = (registration or "").strip().upper().replace(" ", "")
        if not reg:
            return None
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT v.*,
                       c.mot_expiry, c.tax_expiry, c.insurance_expiry,
                       c.last_service_date, c.last_service_mileage,
                       c.next_service_due_date, c.next_service_due_mileage, c.servicing_notes,
                       vt.name AS vehicle_type_name
                FROM fleet_vehicles v
                LEFT JOIN fleet_compliance c ON c.vehicle_id = v.id
                LEFT JOIN fleet_vehicle_types vt ON vt.id = v.vehicle_type_id
                WHERE REPLACE(UPPER(TRIM(v.registration)), ' ', '') = %s
                LIMIT 1
                """,
                (reg,),
            )
            row = cur.fetchone()
            if row and isinstance(row.get("metadata"), str):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except Exception:
                    pass
            return row
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_vehicle_by_internal_code(self, internal_code: str) -> Optional[Dict[str, Any]]:
        code = (internal_code or "").strip()
        if not code:
            return None
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT v.*,
                       c.mot_expiry, c.tax_expiry, c.insurance_expiry,
                       c.last_service_date, c.last_service_mileage,
                       c.next_service_due_date, c.next_service_due_mileage, c.servicing_notes,
                       vt.name AS vehicle_type_name
                FROM fleet_vehicles v
                LEFT JOIN fleet_compliance c ON c.vehicle_id = v.id
                LEFT JOIN fleet_vehicle_types vt ON vt.id = v.vehicle_type_id
                WHERE v.internal_code = %s
                LIMIT 1
                """,
                (code,),
            )
            row = cur.fetchone()
            if row and isinstance(row.get("metadata"), str):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except Exception:
                    pass
            return row
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def create_vehicle(
        self,
        *,
        registration: Optional[str] = None,
        make: Optional[str] = None,
        model: Optional[str] = None,
        year: Optional[int] = None,
        fuel_type: Optional[str] = None,
        status: str = "active",
        notes: Optional[str] = None,
        internal_code: Optional[str] = None,
        mot_expiry: Optional[str] = None,
        tax_expiry: Optional[str] = None,
        insurance_expiry: Optional[str] = None,
        vin: Optional[str] = None,
        vehicle_type_id: Optional[int] = None,
        last_service_date: Optional[str] = None,
        last_service_mileage: Optional[int] = None,
        next_service_due_date: Optional[str] = None,
        next_service_due_mileage: Optional[int] = None,
        servicing_notes: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> int:
        conn = self._conn()
        cur = conn.cursor()
        try:
            code = (internal_code or "").strip() or self.next_internal_code()
            reg_val = (registration or "").strip() or None
            vtid = int(vehicle_type_id) if vehicle_type_id is not None else None
            cur.execute(
                """
                INSERT INTO fleet_vehicles
                    (internal_code, registration, make, model, year, fuel_type, status, notes, vin, vehicle_type_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    code,
                    reg_val,
                    (make or "").strip() or None,
                    (model or "").strip() or None,
                    int(year) if year is not None else None,
                    (fuel_type or "").strip() or None,
                    status,
                    (notes or "").strip() or None,
                    (vin or "").strip() or None,
                    vtid,
                ),
            )
            conn.commit()
            vid = cur.lastrowid
            cur.execute(
                """
                INSERT INTO fleet_compliance (
                    vehicle_id, mot_expiry, tax_expiry, insurance_expiry,
                    last_service_date, last_service_mileage,
                    next_service_due_date, next_service_due_mileage, servicing_notes
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    vid,
                    mot_expiry or None,
                    tax_expiry or None,
                    insurance_expiry or None,
                    last_service_date or None,
                    int(last_service_mileage) if last_service_mileage is not None else None,
                    next_service_due_date or None,
                    int(next_service_due_mileage)
                    if next_service_due_mileage is not None
                    else None,
                    (servicing_notes or "").strip() or None,
                ),
            )
            conn.commit()
            self._log_audit(
                user_id=user_id,
                action="vehicle_create",
                entity_type="fleet_vehicle",
                entity_id=str(vid),
                details={"registration": reg_val, "internal_code": code},
            )
            return int(vid)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def update_vehicle(
        self,
        vehicle_id: int,
        *,
        internal_code: Optional[str] = None,
        registration: Optional[str] = None,
        make: Optional[str] = None,
        model: Optional[str] = None,
        year: Optional[int] = None,
        fuel_type: Optional[str] = None,
        status: Optional[str] = None,
        notes: Optional[str] = None,
        mot_expiry: Optional[str] = None,
        tax_expiry: Optional[str] = None,
        insurance_expiry: Optional[str] = None,
        last_service_date: Optional[str] = None,
        last_service_mileage: Any = None,
        next_service_due_date: Optional[str] = None,
        next_service_due_mileage: Any = None,
        servicing_notes: Optional[str] = None,
        last_lat: Optional[float] = None,
        last_lng: Optional[float] = None,
        telematics_provider: Optional[str] = None,
        vin: Optional[str] = None,
        id_photo_path: Optional[str] = None,
        off_road_reason: Optional[str] = None,
        vehicle_type_id: Any = None,
        user_id: Optional[str] = None,
    ) -> None:
        conn = self._conn()
        cur = conn.cursor()
        try:
            updates = []
            params: List[Any] = []
            if internal_code is not None:
                ic = (internal_code or "").strip()
                if not ic:
                    raise ValueError("Unit name / callsign cannot be empty.")
                updates.append("internal_code = %s")
                params.append(ic)
            if registration is not None:
                updates.append("registration = %s")
                rv = (registration or "").strip()
                params.append(rv or None)
            if make is not None:
                updates.append("make = %s")
                params.append((make or "").strip() or None)
            if model is not None:
                updates.append("model = %s")
                params.append((model or "").strip() or None)
            if year is not None:
                updates.append("year = %s")
                params.append(int(year) if year else None)
            if fuel_type is not None:
                updates.append("fuel_type = %s")
                params.append((fuel_type or "").strip() or None)
            if status is not None:
                updates.append("status = %s")
                params.append(status)
            if notes is not None:
                updates.append("notes = %s")
                params.append((notes or "").strip() or None)
            if last_lat is not None:
                updates.append("last_lat = %s")
                params.append(last_lat)
            if last_lng is not None:
                updates.append("last_lng = %s")
                params.append(last_lng)
            if telematics_provider is not None:
                updates.append("telematics_provider = %s")
                params.append((telematics_provider or "").strip() or None)
            if vin is not None:
                updates.append("vin = %s")
                params.append((vin or "").strip() or None)
            if id_photo_path is not None:
                updates.append("id_photo_path = %s")
                params.append((id_photo_path or "").strip() or None)
            if off_road_reason is not None:
                updates.append("off_road_reason = %s")
                params.append((off_road_reason or "").strip() or None)
            if vehicle_type_id is not None:
                vt_raw = vehicle_type_id
                if vt_raw in ("", None):
                    vtid = None
                else:
                    vtid = int(vt_raw)
                updates.append("vehicle_type_id = %s")
                params.append(vtid)
            if updates:
                params.append(int(vehicle_id))
                cur.execute(
                    f"UPDATE fleet_vehicles SET {', '.join(updates)} WHERE id = %s",
                    tuple(params),
                )
                conn.commit()

            comp_updates = []
            cparams: List[Any] = []
            if mot_expiry is not None:
                comp_updates.append("mot_expiry = %s")
                cparams.append(mot_expiry or None)
            if tax_expiry is not None:
                comp_updates.append("tax_expiry = %s")
                cparams.append(tax_expiry or None)
            if insurance_expiry is not None:
                comp_updates.append("insurance_expiry = %s")
                cparams.append(insurance_expiry or None)
            if last_service_date is not None:
                comp_updates.append("last_service_date = %s")
                cparams.append(last_service_date or None)
            if last_service_mileage is not None:
                comp_updates.append("last_service_mileage = %s")
                if last_service_mileage in ("", None):
                    cparams.append(None)
                else:
                    cparams.append(int(last_service_mileage))
            if next_service_due_date is not None:
                comp_updates.append("next_service_due_date = %s")
                cparams.append(next_service_due_date or None)
            if next_service_due_mileage is not None:
                comp_updates.append("next_service_due_mileage = %s")
                if next_service_due_mileage in ("", None):
                    cparams.append(None)
                else:
                    cparams.append(int(next_service_due_mileage))
            if servicing_notes is not None:
                comp_updates.append("servicing_notes = %s")
                cparams.append((servicing_notes or "").strip() or None)
            if comp_updates:
                cparams.append(int(vehicle_id))
                cur.execute(
                    f"UPDATE fleet_compliance SET {', '.join(comp_updates)} WHERE vehicle_id = %s",
                    tuple(cparams),
                )
                conn.commit()

            self._log_audit(
                user_id=user_id,
                action="vehicle_update",
                entity_type="fleet_vehicle",
                entity_id=str(vehicle_id),
                details={},
            )
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def dashboard_metrics(self) -> Dict[str, Any]:
        """Upcoming / overdue compliance windows."""
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            today = date.today()
            windows = [30, 14, 7]
            out: Dict[str, Any] = {
                "mot_overdue": 0,
                "tax_overdue": 0,
                "insurance_overdue": 0,
                "mot_upcoming": {str(w): 0 for w in windows},
                "tax_upcoming": {str(w): 0 for w in windows},
                "insurance_upcoming": {str(w): 0 for w in windows},
                "servicing_date_overdue": 0,
                "servicing_date_upcoming": {str(w): 0 for w in windows},
                "servicing_mileage_overdue": 0,
                "servicing_mileage_soon": 0,
                "safety_overdue": 0,
                "safety_upcoming": {str(w): 0 for w in windows},
            }
            cur.execute(
                """
                SELECT v.id, v.registration, c.mot_expiry, c.tax_expiry, c.insurance_expiry
                FROM fleet_vehicles v
                JOIN fleet_compliance c ON c.vehicle_id = v.id
                WHERE v.status IN ('active','off_road','maintenance','pending_road_test')
                """
            )

            def _to_date(exp) -> Optional[date]:
                if not exp:
                    return None
                if isinstance(exp, datetime):
                    return exp.date()
                if isinstance(exp, date):
                    return exp
                try:
                    return datetime.strptime(str(exp)[:10], "%Y-%m-%d").date()
                except Exception:
                    return None

            for row in cur.fetchall() or []:
                for field, key_over, key_pre in (
                    ("mot_expiry", "mot_overdue", "mot_upcoming"),
                    ("tax_expiry", "tax_overdue", "tax_upcoming"),
                    ("insurance_expiry", "insurance_overdue", "insurance_upcoming"),
                ):
                    d = _to_date(row.get(field))
                    if not d:
                        continue
                    if d < today:
                        out[key_over] += 1
                    else:
                        days = (d - today).days
                        for w in windows:
                            if days <= w:
                                out[key_pre][str(w)] += 1

            odo_map: Dict[int, int] = {}
            try:
                cur.execute(
                    """
                    SELECT vehicle_id, MAX(m) AS mx FROM (
                      SELECT vehicle_id, GREATEST(start_mileage, end_mileage) AS m
                        FROM fleet_mileage_logs
                      UNION ALL
                      SELECT vehicle_id, mileage_reported AS m FROM fleet_vdi_submissions
                        WHERE mileage_reported IS NOT NULL
                      UNION ALL
                      SELECT vehicle_id, mileage_at_check AS m FROM fleet_safety_checks
                        WHERE mileage_at_check IS NOT NULL
                    ) z GROUP BY vehicle_id
                    """
                )
                for r in cur.fetchall() or []:
                    if r.get("vehicle_id") is not None and r.get("mx") is not None:
                        odo_map[int(r["vehicle_id"])] = int(r["mx"])
            except Exception:
                logger.exception("fleet odometer aggregate for dashboard")

            cur.execute(
                """
                SELECT v.id, c.next_service_due_date, c.next_service_due_mileage
                FROM fleet_vehicles v
                JOIN fleet_compliance c ON c.vehicle_id = v.id
                WHERE v.status IN ('active','off_road','maintenance','pending_road_test')
                """
            )
            for row in cur.fetchall() or []:
                vid = int(row["id"])
                nd = _to_date(row.get("next_service_due_date"))
                nm = row.get("next_service_due_mileage")
                if nd:
                    if nd < today:
                        out["servicing_date_overdue"] += 1
                    else:
                        dleft = (nd - today).days
                        for w in windows:
                            if dleft <= w:
                                out["servicing_date_upcoming"][str(w)] += 1
                odo = odo_map.get(vid)
                if nm is not None and odo is not None:
                    targ = int(nm)
                    if odo >= targ:
                        out["servicing_mileage_overdue"] += 1
                    elif targ - odo <= 750:
                        out["servicing_mileage_soon"] += 1

            last_safety: Dict[int, Any] = {}
            try:
                cur.execute(
                    """
                    SELECT vehicle_id, MAX(performed_at) AS lp
                    FROM fleet_safety_checks
                    GROUP BY vehicle_id
                    """
                )
                for r in cur.fetchall() or []:
                    if r.get("vehicle_id") is not None:
                        last_safety[int(r["vehicle_id"])] = r.get("lp")
            except Exception:
                logger.exception("fleet safety last performed")

            cur.execute(
                """
                SELECT v.id, vt.safety_check_interval_days, v.created_at
                FROM fleet_vehicles v
                LEFT JOIN fleet_vehicle_types vt ON vt.id = v.vehicle_type_id
                WHERE v.status IN ('active','off_road','maintenance','pending_road_test')
                """
            )
            for row in cur.fetchall() or []:
                vid = int(row["id"])
                interval = int(row.get("safety_check_interval_days") or 42)
                anchor = _to_date(last_safety.get(vid)) or _to_date(
                    row.get("created_at")
                )
                if not anchor:
                    continue
                due = anchor + timedelta(days=interval)
                if due < today:
                    out["safety_overdue"] += 1
                else:
                    dleft = (due - today).days
                    for w in windows:
                        if dleft <= w:
                            out["safety_upcoming"][str(w)] += 1

            return out
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def compliance_due_list(self, *, days: int = 30) -> List[Dict[str, Any]]:
        """Rows with any compliance date within `days` or in the past."""
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            today = date.today()
            end = today + timedelta(days=days)
            cur.execute(
                """
                SELECT v.id, v.registration, v.internal_code,
                       c.mot_expiry, c.tax_expiry, c.insurance_expiry
                FROM fleet_vehicles v
                JOIN fleet_compliance c ON c.vehicle_id = v.id
                WHERE v.status IN ('active','off_road','maintenance','pending_road_test')
                  AND (
                    (c.mot_expiry IS NOT NULL AND c.mot_expiry <= %s)
                    OR (c.tax_expiry IS NOT NULL AND c.tax_expiry <= %s)
                    OR (c.insurance_expiry IS NOT NULL AND c.insurance_expiry <= %s)
                  )
                ORDER BY v.internal_code, v.registration
                """,
                (end, end, end),
            )
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def servicing_due_rows(self, *, days: int = 30) -> List[Dict[str, Any]]:
        """Planned service by date (within window) or mileage (due / within 750 mi)."""
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            today = date.today()
            end = today + timedelta(days=days)

            def _d(val) -> Optional[date]:
                if not val:
                    return None
                if isinstance(val, datetime):
                    return val.date()
                if isinstance(val, date):
                    return val
                try:
                    return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
                except Exception:
                    return None

            cur.execute(
                """
                SELECT v.id, v.registration, v.internal_code,
                       c.next_service_due_date, c.next_service_due_mileage,
                       c.last_service_date, c.last_service_mileage
                FROM fleet_vehicles v
                JOIN fleet_compliance c ON c.vehicle_id = v.id
                WHERE v.status IN ('active','off_road','maintenance','pending_road_test')
                ORDER BY v.internal_code, v.registration
                """
            )
            raw = cur.fetchall() or []
            out: List[Dict[str, Any]] = []
            for row in raw:
                vid = int(row["id"])
                odo = self.get_last_known_odometer_mi(vid)
                ndd = _d(row.get("next_service_due_date"))
                nm = row.get("next_service_due_mileage")
                date_hit = bool(ndd and ndd <= end)
                mile_note = ""
                mile_hit = False
                if nm is not None and odo is not None:
                    targ = int(nm)
                    rem = targ - int(odo)
                    if int(odo) >= targ:
                        mile_hit = True
                        mile_note = "at/over mileage target"
                    elif rem <= 750:
                        mile_hit = True
                        mile_note = f"{rem} mi to target"
                elif nm is not None and odo is None:
                    mile_note = "mileage target set — no odometer yet"
                if not date_hit and not mile_hit:
                    continue
                row["last_known_odometer_mi"] = odo
                row["servicing_mileage_note"] = mile_note
                out.append(row)
            return out
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def safety_due_rows(self, *, days: int = 30) -> List[Dict[str, Any]]:
        """Vehicles whose next safety check (by type interval) falls within window or past."""
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            today = date.today()
            end = today + timedelta(days=days)

            def _d(val) -> Optional[date]:
                if not val:
                    return None
                if isinstance(val, datetime):
                    return val.date()
                if isinstance(val, date):
                    return val
                try:
                    return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
                except Exception:
                    return None

            cur.execute(
                """
                SELECT v.id, v.registration, v.internal_code, v.created_at, v.vehicle_type_id,
                       vt.name AS vehicle_type_name,
                       vt.safety_check_interval_days
                FROM fleet_vehicles v
                LEFT JOIN fleet_vehicle_types vt ON vt.id = v.vehicle_type_id
                WHERE v.status IN ('active','off_road','maintenance','pending_road_test')
                ORDER BY v.internal_code, v.registration
                """
            )
            vehicles = cur.fetchall() or []
            cur.execute(
                """
                SELECT vehicle_id, MAX(performed_at) AS lp
                FROM fleet_safety_checks
                GROUP BY vehicle_id
                """
            )
            last_by_vid = {
                int(r["vehicle_id"]): r.get("lp")
                for r in (cur.fetchall() or [])
                if r.get("vehicle_id") is not None
            }
            out: List[Dict[str, Any]] = []
            for row in vehicles:
                vid = int(row["id"])
                interval = int(row.get("safety_check_interval_days") or 42)
                lp = last_by_vid.get(vid)
                anchor = _d(lp) or _d(row.get("created_at"))
                if not anchor:
                    continue
                due_by = anchor + timedelta(days=interval)
                if due_by > end:
                    continue
                row["safety_last_performed"] = lp
                row["safety_due_by"] = due_by
                row["safety_days_remaining"] = (due_by - today).days
                out.append(row)
            return out
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_mileage(
        self, vehicle_id: int, *, limit: int = 100
    ) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT * FROM fleet_mileage_logs
                WHERE vehicle_id = %s
                ORDER BY logged_at DESC
                LIMIT %s
                """,
                (int(vehicle_id), min(limit, 500)),
            )
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_last_known_odometer_mi(self, vehicle_id: int) -> Optional[int]:
        """Highest reading from mileage logs and VDI odometer fields, or None if unknown."""
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT MAX(GREATEST(start_mileage, end_mileage))
                FROM fleet_mileage_logs
                WHERE vehicle_id = %s
                """,
                (int(vehicle_id),),
            )
            row = cur.fetchone()
            m1 = int(row[0]) if row and row[0] is not None else None
            cur.execute(
                """
                SELECT MAX(mileage_reported)
                FROM fleet_vdi_submissions
                WHERE vehicle_id = %s AND mileage_reported IS NOT NULL
                """,
                (int(vehicle_id),),
            )
            row2 = cur.fetchone()
            m2 = int(row2[0]) if row2 and row2[0] is not None else None
            cur.execute(
                """
                SELECT MAX(mileage_at_check)
                FROM fleet_safety_checks
                WHERE vehicle_id = %s AND mileage_at_check IS NOT NULL
                """,
                (int(vehicle_id),),
            )
            row3 = cur.fetchone()
            m3 = int(row3[0]) if row3 and row3[0] is not None else None
            vals = [x for x in (m1, m2, m3) if x is not None]
            return max(vals) if vals else None
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def add_mileage_log(
        self,
        *,
        vehicle_id: int,
        driver_user_id: Optional[str],
        start_mileage: int,
        end_mileage: int,
        purpose: Optional[str],
        created_by: Optional[str],
        sanity_check_override: bool = False,
        sanity_override_note: Optional[str] = None,
    ) -> int:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO fleet_mileage_logs
                    (vehicle_id, driver_user_id, start_mileage, end_mileage, purpose, created_by)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(vehicle_id),
                    driver_user_id,
                    int(start_mileage),
                    int(end_mileage),
                    (purpose or "").strip() or None,
                    created_by,
                ),
            )
            conn.commit()
            mid = cur.lastrowid
            details: Dict[str, Any] = {"vehicle_id": vehicle_id}
            if sanity_check_override:
                details["sanity_check_override"] = True
                note = (sanity_override_note or "").strip()
                if note:
                    details["override_note"] = note[:500]
            self._log_audit(
                user_id=created_by,
                action="mileage_create",
                entity_type="fleet_mileage",
                entity_id=str(mid),
                details=details,
            )
            return int(mid)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def delete_mileage_log(
        self, *, log_id: int, vehicle_id: int, user_id: Optional[str]
    ) -> bool:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                DELETE FROM fleet_mileage_logs
                WHERE id = %s AND vehicle_id = %s
                """,
                (int(log_id), int(vehicle_id)),
            )
            conn.commit()
            if cur.rowcount:
                self._log_audit(
                    user_id=user_id,
                    action="mileage_delete",
                    entity_type="fleet_mileage",
                    entity_id=str(log_id),
                    details={"vehicle_id": vehicle_id},
                )
                return True
            return False
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_maintenance(self, vehicle_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT * FROM fleet_maintenance_events
                WHERE vehicle_id = %s
                ORDER BY service_date DESC, id DESC
                """,
                (int(vehicle_id),),
            )
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def add_maintenance(
        self,
        *,
        vehicle_id: int,
        service_date: str,
        service_type: str,
        provider: Optional[str] = None,
        cost: Optional[float] = None,
        odometer_at_service: Optional[int] = None,
        notes: Optional[str] = None,
        created_by: Optional[str] = None,
    ) -> int:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO fleet_maintenance_events
                    (vehicle_id, service_date, service_type, provider, cost, odometer_at_service, notes, created_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(vehicle_id),
                    service_date,
                    service_type.strip(),
                    (provider or "").strip() or None,
                    cost,
                    int(odometer_at_service) if odometer_at_service is not None else None,
                    (notes or "").strip() or None,
                    created_by,
                ),
            )
            conn.commit()
            eid = cur.lastrowid
            self._log_audit(
                user_id=created_by,
                action="maintenance_create",
                entity_type="fleet_maintenance",
                entity_id=str(eid),
                details={"vehicle_id": vehicle_id},
            )
            return int(eid)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_driver_assignments(self, vehicle_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT * FROM fleet_driver_assignments
                WHERE vehicle_id = %s
                ORDER BY effective_from DESC
                """,
                (int(vehicle_id),),
            )
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def add_driver_assignment(
        self,
        *,
        vehicle_id: int,
        user_id: str,
        assignment_role: str = "primary",
        effective_from: Optional[str] = None,
        effective_to: Optional[str] = None,
        notes: Optional[str] = None,
        created_by: Optional[str] = None,
    ) -> int:
        from datetime import date as _date

        conn = self._conn()
        cur = conn.cursor()
        try:
            ef = effective_from or _date.today().isoformat()
            cur.execute(
                """
                INSERT INTO fleet_driver_assignments
                    (vehicle_id, user_id, assignment_role, effective_from, effective_to, notes)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(vehicle_id),
                    user_id,
                    assignment_role,
                    ef,
                    effective_to,
                    (notes or "").strip() or None,
                ),
            )
            conn.commit()
            aid = cur.lastrowid
            self._log_audit(
                user_id=created_by,
                action="driver_assign",
                entity_type="fleet_driver_assignment",
                entity_id=str(aid),
                details={"vehicle_id": vehicle_id, "user_id": user_id},
            )
            return int(aid)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_equipment_on_vehicle(self, vehicle_id: int) -> List[Dict[str, Any]]:
        """
        Inventory equipment currently signed out to this vehicle (assigned/loaned
        with a matching vehicle assignee on an out transaction).
        """
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            vid = str(int(vehicle_id))
            cur.execute(
                """
                SELECT ea.id, ea.serial_number, ea.public_asset_code, ea.status, ea.item_id,
                       i.name AS item_name, i.sku
                FROM inventory_equipment_assets ea
                INNER JOIN inventory_items i ON i.id = ea.item_id
                WHERE ea.status IN ('assigned','loaned')
                  AND EXISTS (
                    SELECT 1 FROM inventory_transactions t
                    WHERE t.equipment_asset_id = ea.id
                      AND t.assignee_type = 'vehicle'
                      AND t.assignee_id = %s
                      AND t.transaction_type = 'out'
                  )
                ORDER BY ea.serial_number
                """,
                (vid,),
            )
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_vehicle_equipment_activity(
        self, vehicle_id: int, limit: int = 80
    ) -> List[Dict[str, Any]]:
        """
        Movement lines involving this fleet vehicle: assignments (out) and returns
        that recorded from_vehicle_id in transaction metadata.
        """
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            vid = str(int(vehicle_id))
            lim = min(max(int(limit), 1), 200)
            cur.execute(
                """
                SELECT t.id, t.performed_at, t.transaction_type, t.quantity,
                       t.assignee_type, t.assignee_id, t.assignee_label,
                       t.performed_by_user_id,
                       t.is_loan, t.location_id, t.equipment_asset_id, t.metadata,
                       loc.name AS location_name, loc.code AS location_code,
                       ea.serial_number, ea.public_asset_code,
                       i.name AS item_name, i.sku
                FROM inventory_transactions t
                INNER JOIN inventory_equipment_assets ea ON ea.id = t.equipment_asset_id
                INNER JOIN inventory_items i ON i.id = t.item_id
                LEFT JOIN inventory_locations loc ON loc.id = t.location_id
                WHERE t.equipment_asset_id IS NOT NULL
                  AND (
                    (t.assignee_type = 'vehicle' AND t.assignee_id = %s)
                    OR (
                      t.metadata IS NOT NULL
                      AND JSON_UNQUOTE(JSON_EXTRACT(t.metadata, '$.from_vehicle_id')) = %s
                    )
                  )
                ORDER BY t.performed_at DESC, t.id DESC
                LIMIT %s
                """,
                (vid, vid, lim),
            )
            rows = cur.fetchall() or []
            for r in rows:
                r["activity_summary"] = self._equipment_activity_summary(r, vid)
            return rows
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def _equipment_activity_summary(self, row: Dict[str, Any], vehicle_id_str: str) -> str:
        import json

        tx = str(row.get("transaction_type") or "")
        meta = row.get("metadata")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        if not isinstance(meta, dict):
            meta = {}
        sn = row.get("serial_number") or ""
        loc = row.get("location_name") or row.get("location_code") or ""
        if not loc and row.get("location_id") is not None:
            loc = f"#{row.get('location_id')}"
        if tx == "out" and str(row.get("assignee_type") or "") == "vehicle":
            verb = "Loaned to" if int(row.get("is_loan") or 0) else "Installed on"
            if str(row.get("assignee_id") or "") == vehicle_id_str:
                return f"{verb} this vehicle ({sn})"
            return f"{verb} vehicle {row.get('assignee_label') or row.get('assignee_id')}"
        if tx in ("return", "in") and str(meta.get("from_vehicle_id") or "") == vehicle_id_str:
            return f"Returned to storeroom {loc} ({sn})"
        return f"{tx} — {sn}"

    def install_part_from_workshop(
        self,
        vehicle_id: int,
        *,
        installed_date: str,
        inventory_item_id: Optional[int] = None,
        quantity: float = 1.0,
        deduct_stock: bool = False,
        stock_location_id: Optional[int] = None,
        part_number: Optional[str] = None,
        part_description: Optional[str] = None,
        odometer_at_install: Optional[int] = None,
        warranty_expires_date: Optional[str] = None,
        warranty_terms: Optional[str] = None,
        invoice_reference: Optional[str] = None,
        notes: Optional[str] = None,
        created_by: Optional[str] = None,
        performed_by_user_id: Any = None,
    ) -> int:
        """
        Log a fitted part on a vehicle; optionally post inventory 'out' from a storeroom.
        Shared by Fleet vehicle UI and Inventory 'fit part to vehicle' wizard.
        """
        from app.plugins.inventory_control.objects import get_inventory_service

        inv = get_inventory_service()
        v = self.get_vehicle(vehicle_id)
        if not v:
            raise ValueError("Vehicle not found")
        qty = float(quantity) if quantity else 1.0
        item_id = int(inventory_item_id) if inventory_item_id else None

        if item_id:
            it = inv.get_item(item_id)
            if it and not part_description:
                part_description = (it.get("name") or "").strip() or None
            if it and not part_number:
                part_number = (it.get("sku") or "").strip() or None

        inv_tx_id = None
        if deduct_stock and item_id:
            if not stock_location_id:
                raise ValueError("Choose a storeroom to deduct stock from.")
            reg = (v.get("registration") or "").strip()
            tx = inv.record_transaction(
                item_id=item_id,
                location_id=int(stock_location_id),
                quantity=qty,
                transaction_type="out",
                performed_by_user_id=performed_by_user_id,
                assignee_type="vehicle",
                assignee_id=str(int(vehicle_id)),
                assignee_label=reg or str(vehicle_id),
                reference_type="fleet_installed_part",
                reference_id=str(vehicle_id),
                metadata={
                    "part_number": part_number,
                    "installed_date": installed_date,
                },
            )
            inv_tx_id = int(tx.get("transaction_id") or 0) or None

        return self.add_installed_part(
            vehicle_id,
            installed_date=installed_date,
            part_number=part_number,
            part_description=part_description,
            inventory_item_id=item_id,
            inventory_transaction_id=inv_tx_id,
            quantity=qty,
            odometer_at_install=odometer_at_install,
            warranty_expires_date=warranty_expires_date,
            warranty_terms=warranty_terms,
            invoice_reference=invoice_reference,
            notes=notes,
            created_by=created_by,
        )

    def list_installed_parts(self, vehicle_id: int) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT p.*, i.name AS inventory_item_name, i.sku AS inventory_sku
                FROM fleet_vehicle_installed_parts p
                LEFT JOIN inventory_items i ON i.id = p.inventory_item_id
                WHERE p.vehicle_id = %s
                ORDER BY p.installed_date DESC, p.id DESC
                """,
                (int(vehicle_id),),
            )
            rows = cur.fetchall() or []
            for r in rows:
                if isinstance(r.get("metadata"), str):
                    try:
                        r["metadata"] = json.loads(r["metadata"])
                    except Exception:
                        pass
            return rows
        except Exception:
            logger.exception("list_installed_parts")
            return []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def add_installed_part(
        self,
        vehicle_id: int,
        *,
        installed_date: str,
        part_number: Optional[str] = None,
        part_description: Optional[str] = None,
        inventory_item_id: Optional[int] = None,
        inventory_transaction_id: Optional[int] = None,
        quantity: float = 1.0,
        odometer_at_install: Optional[int] = None,
        warranty_expires_date: Optional[str] = None,
        warranty_terms: Optional[str] = None,
        invoice_reference: Optional[str] = None,
        notes: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_by: Optional[str] = None,
    ) -> int:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO fleet_vehicle_installed_parts (
                    vehicle_id, inventory_item_id, inventory_transaction_id,
                    part_number, part_description, quantity, installed_date,
                    odometer_at_install, warranty_expires_date, warranty_terms,
                    invoice_reference, notes, metadata, created_by
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(vehicle_id),
                    int(inventory_item_id) if inventory_item_id else None,
                    int(inventory_transaction_id) if inventory_transaction_id else None,
                    (part_number or "").strip() or None,
                    (part_description or "").strip() or None,
                    float(quantity) if quantity else 1.0,
                    installed_date,
                    int(odometer_at_install) if odometer_at_install is not None else None,
                    warranty_expires_date or None,
                    (warranty_terms or "").strip() or None,
                    (invoice_reference or "").strip() or None,
                    (notes or "").strip() or None,
                    json.dumps(metadata or {}, default=str) if metadata else None,
                    created_by,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def delete_installed_part(self, part_id: int, vehicle_id: int) -> bool:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                DELETE FROM fleet_vehicle_installed_parts
                WHERE id = %s AND vehicle_id = %s
                """,
                (int(part_id), int(vehicle_id)),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # --- Vehicle types & workshop safety checks ---
    def list_vehicle_types(self, *, include_inactive: bool = True) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            sql = "SELECT * FROM fleet_vehicle_types WHERE 1=1"
            if not include_inactive:
                sql += " AND active = 1"
            sql += " ORDER BY sort_order ASC, name ASC"
            cur.execute(sql)
            rows = cur.fetchall() or []
            for r in rows:
                raw = r.get("safety_schema_json")
                if isinstance(raw, str):
                    try:
                        r["safety_schema"] = json.loads(raw)
                    except Exception:
                        r["safety_schema"] = {}
                elif isinstance(raw, dict):
                    r["safety_schema"] = raw
                else:
                    r["safety_schema"] = {}
            return rows
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_vehicle_type(self, type_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM fleet_vehicle_types WHERE id = %s LIMIT 1",
                (int(type_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            raw = row.get("safety_schema_json")
            if isinstance(raw, str):
                try:
                    row["safety_schema"] = json.loads(raw)
                except Exception:
                    row["safety_schema"] = {}
            elif isinstance(raw, dict):
                row["safety_schema"] = raw
            else:
                row["safety_schema"] = {}
            return row
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def create_vehicle_type(
        self,
        *,
        name: str,
        service_interval_days: Optional[int] = None,
        service_interval_miles: Optional[int] = None,
        safety_check_interval_days: int = 42,
        sort_order: int = 0,
        active: bool = True,
        safety_schema: Optional[Dict[str, Any]] = None,
        user_id: Optional[str] = None,
    ) -> int:
        from app.plugins.fleet_management.safety_schema_default import (
            DEFAULT_SAFETY_SCHEMA,
        )

        blob = json.dumps(
            safety_schema if safety_schema is not None else dict(DEFAULT_SAFETY_SCHEMA),
            default=str,
        )
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO fleet_vehicle_types
                  (name, service_interval_days, service_interval_miles,
                   safety_check_interval_days, safety_schema_json, sort_order, active)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    name.strip(),
                    int(service_interval_days) if service_interval_days is not None else None,
                    int(service_interval_miles) if service_interval_miles is not None else None,
                    int(safety_check_interval_days),
                    blob,
                    int(sort_order),
                    1 if active else 0,
                ),
            )
            conn.commit()
            tid = int(cur.lastrowid)
            self._log_audit(
                user_id=user_id,
                action="fleet_vehicle_type_create",
                entity_type="fleet_vehicle_type",
                entity_id=str(tid),
                details={"name": name.strip()},
            )
            return tid
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def update_vehicle_type(
        self,
        type_id: int,
        *,
        name: Optional[str] = None,
        service_interval_days: Any = None,
        service_interval_miles: Any = None,
        safety_check_interval_days: Any = None,
        sort_order: Any = None,
        active: Any = None,
        safety_schema: Any = None,
        user_id: Optional[str] = None,
    ) -> None:
        conn = self._conn()
        cur = conn.cursor()
        try:
            updates = []
            params: List[Any] = []
            if name is not None:
                updates.append("name = %s")
                params.append(name.strip())
            if service_interval_days is not None:
                updates.append("service_interval_days = %s")
                if service_interval_days in ("", None):
                    params.append(None)
                else:
                    params.append(int(service_interval_days))
            if service_interval_miles is not None:
                updates.append("service_interval_miles = %s")
                if service_interval_miles in ("", None):
                    params.append(None)
                else:
                    params.append(int(service_interval_miles))
            if safety_check_interval_days is not None and safety_check_interval_days != "":
                updates.append("safety_check_interval_days = %s")
                params.append(int(safety_check_interval_days))
            if sort_order is not None:
                updates.append("sort_order = %s")
                params.append(int(sort_order))
            if active is not None:
                updates.append("active = %s")
                params.append(1 if active else 0)
            if safety_schema is not None:
                updates.append("safety_schema_json = %s")
                if isinstance(safety_schema, str):
                    params.append(safety_schema)
                else:
                    params.append(json.dumps(safety_schema, default=str))
            if not updates:
                return
            params.append(int(type_id))
            cur.execute(
                f"UPDATE fleet_vehicle_types SET {', '.join(updates)} WHERE id = %s",
                tuple(params),
            )
            conn.commit()
            self._log_audit(
                user_id=user_id,
                action="fleet_vehicle_type_update",
                entity_type="fleet_vehicle_type",
                entity_id=str(type_id),
                details={},
            )
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_raw_safety_schema_for_vehicle(self, vehicle_id: int) -> Dict[str, Any]:
        """Full JSON from vehicle type: title, sections, optional portal_roles (workshop form)."""
        from app.plugins.fleet_management.safety_schema_default import (
            DEFAULT_SAFETY_SCHEMA,
        )

        v = self.get_vehicle(int(vehicle_id))
        if not v or not v.get("vehicle_type_id"):
            return dict(DEFAULT_SAFETY_SCHEMA)
        vt = self.get_vehicle_type(int(v["vehicle_type_id"]))
        if not vt:
            return dict(DEFAULT_SAFETY_SCHEMA)
        sch = vt.get("safety_schema")
        if isinstance(sch, dict) and sch.get("sections"):
            return sch
        return dict(DEFAULT_SAFETY_SCHEMA)

    def get_additional_contractor_safety_forms(self, vehicle_id: int) -> List[Dict[str, Any]]:
        """
        Role-targeted contractor portal inspections (pre-delivery, etc.).
        Stored on the global VDI schema. Falls back to legacy per-type safety_schema.additional_forms
        until the VDI schema is saved with an additional_forms key.
        """
        vdi = self.get_vdi_schema()
        if isinstance(vdi, dict) and "additional_forms" in vdi:
            return list(vdi.get("additional_forms") or [])
        raw = self.get_raw_safety_schema_for_vehicle(vehicle_id)
        return list(raw.get("additional_forms") or [])

    def get_safety_schema_for_vehicle(self, vehicle_id: int) -> Dict[str, Any]:
        """Primary workshop form only (title + sections) for templates and parsing."""
        raw = self.get_raw_safety_schema_for_vehicle(vehicle_id)
        return {
            "version": raw.get("version", 1),
            "title": raw.get("title"),
            "sections": raw.get("sections") or [],
        }

    def get_safety_check_form_for_vehicle(
        self, vehicle_id: int, form_key: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Resolved checklist: workshop (default) or an additional_forms entry by form_id."""
        from app.plugins.fleet_management.safety_schema_build import PRIMARY_FORM_KEY

        raw = self.get_raw_safety_schema_for_vehicle(vehicle_id)
        fk = (form_key or PRIMARY_FORM_KEY).strip().lower()
        if fk in ("", PRIMARY_FORM_KEY, "primary", "default"):
            return {
                "version": raw.get("version", 1),
                "title": raw.get("title"),
                "sections": raw.get("sections") or [],
            }
        for af in self.get_additional_contractor_safety_forms(vehicle_id):
            if str(af.get("form_id") or "").strip().lower() == fk:
                return {
                    "version": af.get("version", 1),
                    "title": af.get("title"),
                    "sections": af.get("sections") or [],
                }
        return None

    def list_safety_form_choices_for_vehicle(self, vehicle_id: int) -> List[Dict[str, str]]:
        """Admin/navigation: primary + each additional form (slug + title)."""
        from app.plugins.fleet_management.safety_schema_build import PRIMARY_FORM_KEY

        raw = self.get_raw_safety_schema_for_vehicle(vehicle_id)
        out = [
            {
                "form_key": PRIMARY_FORM_KEY,
                "title": raw.get("title") or "Workshop safety check",
            }
        ]
        for af in self.get_additional_contractor_safety_forms(vehicle_id):
            slug = str(af.get("form_id") or "").strip()
            if slug:
                out.append({"form_key": slug, "title": af.get("title") or slug})
        return out

    def list_safety_checks(
        self, vehicle_id: Optional[int] = None, *, limit: int = 100
    ) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            if vehicle_id is not None:
                cur.execute(
                    """
                    SELECT * FROM fleet_safety_checks
                    WHERE vehicle_id = %s
                    ORDER BY performed_at DESC, id DESC
                    LIMIT %s
                    """,
                    (int(vehicle_id), min(limit, 500)),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM fleet_safety_checks
                    ORDER BY performed_at DESC, id DESC
                    LIMIT %s
                    """,
                    (min(limit, 500),),
                )
            rows = cur.fetchall() or []
            for r in rows:
                for k in ("responses", "photo_paths"):
                    if isinstance(r.get(k), str):
                        try:
                            r[k] = json.loads(r[k])
                        except Exception:
                            pass
            return rows
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_safety_check(self, check_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM fleet_safety_checks WHERE id = %s LIMIT 1",
                (int(check_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            for k in ("responses", "photo_paths"):
                if isinstance(row.get(k), str):
                    try:
                        row[k] = json.loads(row[k])
                    except Exception:
                        pass
            return row
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def add_safety_check(
        self,
        *,
        vehicle_id: int,
        performed_by_user_id: Optional[str],
        performed_at: str,
        mileage_at_check: Optional[int],
        responses: Dict[str, Any],
        photo_paths: Optional[List[str]] = None,
        summary_notes: Optional[str] = None,
        user_id: Optional[str] = None,
        check_form_key: Optional[str] = None,
    ) -> int:
        from app.plugins.fleet_management.safety_schema_build import PRIMARY_FORM_KEY

        cfk = (check_form_key or "").strip() or PRIMARY_FORM_KEY
        if cfk.lower() in ("primary", "default"):
            cfk = PRIMARY_FORM_KEY
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO fleet_safety_checks
                  (vehicle_id, performed_by_user_id, performed_at, mileage_at_check,
                   responses, photo_paths, summary_notes, check_form_key)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(vehicle_id),
                    performed_by_user_id,
                    performed_at,
                    int(mileage_at_check) if mileage_at_check is not None else None,
                    json.dumps(responses, default=str),
                    json.dumps(photo_paths or [], default=str),
                    (summary_notes or "").strip() or None,
                    cfk[:64] if cfk else None,
                ),
            )
            conn.commit()
            cid = int(cur.lastrowid)
            self._log_audit(
                user_id=user_id or performed_by_user_id,
                action="fleet_safety_check_create",
                entity_type="fleet_safety_check",
                entity_id=str(cid),
                details={"vehicle_id": vehicle_id, "check_form_key": cfk},
            )
            return cid
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def update_safety_check_photos(self, check_id: int, photo_paths: List[str]) -> None:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE fleet_safety_checks SET photo_paths = %s WHERE id = %s
                """,
                (json.dumps(photo_paths), int(check_id)),
            )
            conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # --- Portal form visibility (employee portal + /VDIs) ---
    def get_fleet_portal_form_visibility(self) -> Dict[str, Any]:
        from app.plugins.fleet_management.fleet_portal_visibility import (
            parse_fleet_portal_visibility_json,
        )

        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT vdi_roles_json, safety_roles_json
                FROM fleet_portal_form_visibility WHERE id = 1 LIMIT 1
                """
            )
            row = cur.fetchone()
            if not row:
                return {
                    "vdi_roles": [],
                    "vdi_categories": [],
                    "safety_roles": [],
                    "safety_categories": [],
                }
            vr, vc = parse_fleet_portal_visibility_json(row.get("vdi_roles_json"))
            sr, sc = parse_fleet_portal_visibility_json(row.get("safety_roles_json"))
            return {
                "vdi_roles": vr,
                "vdi_categories": vc,
                "safety_roles": sr,
                "safety_categories": sc,
            }
        except Exception:
            logger.exception("get_fleet_portal_form_visibility")
            return {
                "vdi_roles": [],
                "vdi_categories": [],
                "safety_roles": [],
                "safety_categories": [],
            }
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def set_fleet_portal_form_visibility(
        self,
        *,
        vdi_roles: Optional[Sequence[str]] = None,
        vdi_categories: Optional[Sequence[str]] = None,
        safety_roles: Optional[Sequence[str]] = None,
        safety_categories: Optional[Sequence[str]] = None,
        user_id: Optional[str] = None,
    ) -> None:
        from app.plugins.fleet_management.fleet_portal_visibility import (
            fleet_user_role_category_definitions,
        )

        defs = fleet_user_role_category_definitions()
        allowed_cats = {(d.get("id") or "").strip() for d in defs if (d.get("id") or "").strip()}
        pick = {
            x.strip().lower()
            for x in list_time_billing_role_names_for_picklist()
            if (x or "").strip()
        }

        def norm_roles(seq: Optional[Sequence[str]]) -> List[str]:
            if not seq:
                return []
            return sorted(
                {
                    str(x).strip().lower()
                    for x in seq
                    if str(x).strip().lower() in pick
                }
            )

        def norm_cats(seq: Optional[Sequence[str]]) -> List[str]:
            if not seq:
                return []
            return sorted(
                {
                    str(x).strip()
                    for x in seq
                    if str(x).strip() in allowed_cats
                }
            )

        vdi_obj = {
            "roles": norm_roles(vdi_roles),
            "categories": norm_cats(vdi_categories),
        }
        saf_obj = {
            "roles": norm_roles(safety_roles),
            "categories": norm_cats(safety_categories),
        }
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO fleet_portal_form_visibility (id, vdi_roles_json, safety_roles_json)
                VALUES (1, %s, %s)
                ON DUPLICATE KEY UPDATE
                  vdi_roles_json = VALUES(vdi_roles_json),
                  safety_roles_json = VALUES(safety_roles_json)
                """,
                (json.dumps(vdi_obj), json.dumps(saf_obj)),
            )
            conn.commit()
            self._log_audit(
                user_id=user_id,
                action="fleet_portal_form_visibility_update",
                entity_type="fleet_portal_form_visibility",
                entity_id="1",
                details={"vdi": vdi_obj, "safety": saf_obj},
            )
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # --- VDI (dynamic form) ---
    def get_vdi_schema(self) -> Dict[str, Any]:
        from app.plugins.fleet_management.vdi_schema_default import DEFAULT_VDI_SCHEMA

        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT schema_json FROM fleet_vdi_schema WHERE id = 1 LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return dict(DEFAULT_VDI_SCHEMA)
            raw = row.get("schema_json")
            if isinstance(raw, dict):
                return raw
            if isinstance(raw, str):
                return json.loads(raw)
            return dict(DEFAULT_VDI_SCHEMA)
        except Exception:
            logger.exception("get_vdi_schema")
            return dict(DEFAULT_VDI_SCHEMA)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def save_vdi_schema(self, schema: Any, *, user_id: Optional[str] = None) -> None:
        if isinstance(schema, str):
            schema = json.loads(schema)
        blob = json.dumps(schema, default=str)
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO fleet_vdi_schema (id, schema_json)
                VALUES (1, %s)
                ON DUPLICATE KEY UPDATE schema_json = VALUES(schema_json)
                """,
                (blob,),
            )
            conn.commit()
            self._log_audit(
                user_id=user_id,
                action="vdi_schema_update",
                entity_type="fleet_vdi",
                entity_id="1",
                details={},
            )
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_vdi_submissions(
        self, vehicle_id: Optional[int] = None, *, limit: int = 100
    ) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            if vehicle_id is not None:
                cur.execute(
                    """
                    SELECT * FROM fleet_vdi_submissions
                    WHERE vehicle_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (int(vehicle_id), min(limit, 500)),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM fleet_vdi_submissions
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (min(limit, 500),),
                )
            rows = cur.fetchall() or []
            for r in rows:
                for k in ("responses", "photo_paths"):
                    if isinstance(r.get(k), str):
                        try:
                            r[k] = json.loads(r[k])
                        except Exception:
                            pass
            return rows
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_vdi_submission(self, submission_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM fleet_vdi_submissions WHERE id = %s LIMIT 1",
                (int(submission_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            for k in ("responses", "photo_paths"):
                if isinstance(row.get(k), str):
                    try:
                        row[k] = json.loads(row[k])
                    except Exception:
                        pass
            return row
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def add_vdi_submission(
        self,
        *,
        vehicle_id: int,
        actor_type: str,
        actor_id: str,
        mileage_reported: Optional[int],
        responses: Dict[str, Any],
        photo_paths: Optional[List[str]] = None,
    ) -> int:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO fleet_vdi_submissions
                  (vehicle_id, actor_type, actor_id, mileage_reported, responses, photo_paths)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(vehicle_id),
                    actor_type,
                    str(actor_id),
                    int(mileage_reported) if mileage_reported is not None else None,
                    json.dumps(responses, default=str),
                    json.dumps(photo_paths or [], default=str),
                ),
            )
            conn.commit()
            sid = int(cur.lastrowid)
            self._log_audit(
                user_id=actor_id if actor_type == "user" else None,
                action="vdi_submit",
                entity_type="fleet_vdi_submission",
                entity_id=str(sid),
                details={"vehicle_id": vehicle_id},
            )
            return sid
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def update_vdi_submission_photos(
        self, submission_id: int, photo_paths: List[str]
    ) -> None:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE fleet_vdi_submissions SET photo_paths = %s WHERE id = %s
                """,
                (json.dumps(photo_paths), int(submission_id)),
            )
            conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_issues(
        self,
        *,
        vehicle_id: Optional[int] = None,
        open_only: bool = False,
        limit: int = 300,
    ) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            sql = "SELECT * FROM fleet_issues WHERE 1=1"
            params: List[Any] = []
            if vehicle_id is not None:
                sql += " AND vehicle_id = %s"
                params.append(int(vehicle_id))
            if open_only:
                sql += " AND completed_at IS NULL"
            sql += " ORDER BY updated_at DESC LIMIT %s"
            params.append(min(limit, 500))
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
            for r in rows:
                if isinstance(r.get("photo_paths"), str):
                    try:
                        r["photo_paths"] = json.loads(r["photo_paths"])
                    except Exception:
                        pass
            return rows
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_issue(self, issue_id: int) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM fleet_issues WHERE id = %s LIMIT 1",
                (int(issue_id),),
            )
            row = cur.fetchone()
            if row and isinstance(row.get("photo_paths"), str):
                try:
                    row["photo_paths"] = json.loads(row["photo_paths"])
                except Exception:
                    pass
            return row
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def add_fleet_issue(
        self,
        *,
        vehicle_id: int,
        actor_type: str,
        actor_id: str,
        title: str,
        description: Optional[str] = None,
        photo_paths: Optional[List[str]] = None,
        user_id: Optional[str] = None,
    ) -> int:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO fleet_issues
                  (vehicle_id, actor_type, actor_id, title, description, photo_paths, kanban_stage)
                VALUES (%s,%s,%s,%s,%s,%s,'reported')
                """,
                (
                    int(vehicle_id),
                    actor_type,
                    str(actor_id),
                    title.strip(),
                    (description or "").strip() or None,
                    json.dumps(photo_paths or [], default=str),
                ),
            )
            conn.commit()
            iid = int(cur.lastrowid)
            self._log_audit(
                user_id=user_id or (actor_id if actor_type == "user" else None),
                action="fleet_issue_create",
                entity_type="fleet_issue",
                entity_id=str(iid),
                details={"vehicle_id": vehicle_id},
            )
            return iid
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def update_fleet_issue(
        self,
        issue_id: int,
        *,
        kanban_stage: Optional[str] = None,
        scheduled_service_date: Optional[str] = None,
        manager_notes: Optional[str] = None,
        vehicle_marked_vor: Optional[bool] = None,
        mark_vehicle_pending_road_test: bool = False,
        mark_vehicle_maintenance: bool = False,
        mark_vehicle_active: bool = False,
        resolution_summary: Optional[str] = None,
        complete: bool = False,
        user_id: Optional[str] = None,
    ) -> None:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT vehicle_id FROM fleet_issues WHERE id = %s",
                (int(issue_id),),
            )
            row = cur.fetchone()
            if not row:
                return
            vid = int(row["vehicle_id"])
            updates = []
            params: List[Any] = []
            if kanban_stage is not None:
                updates.append("kanban_stage = %s")
                params.append(kanban_stage)
            if scheduled_service_date is not None:
                updates.append("scheduled_service_date = %s")
                params.append(scheduled_service_date or None)
            if manager_notes is not None:
                updates.append("manager_notes = %s")
                params.append((manager_notes or "").strip() or None)
            if vehicle_marked_vor is not None:
                updates.append("vehicle_marked_vor = %s")
                params.append(1 if vehicle_marked_vor else 0)
            if mark_vehicle_maintenance:
                updates.append("vehicle_marked_workshop = 1")
            if complete:
                updates.append("completed_at = NOW()")
                updates.append("resolution_summary = %s")
                params.append((resolution_summary or "").strip() or None)
            if updates:
                params.append(int(issue_id))
                cur.execute(
                    f"UPDATE fleet_issues SET {', '.join(updates)} WHERE id = %s",
                    tuple(params),
                )
                conn.commit()

            if vehicle_marked_vor is True:
                cur2 = conn.cursor()
                cur2.execute(
                    """
                    UPDATE fleet_vehicles
                    SET status = 'off_road', off_road_reason = 'issue'
                    WHERE id = %s
                    """,
                    (vid,),
                )
                conn.commit()
                cur2.close()
            if mark_vehicle_pending_road_test:
                cur2 = conn.cursor()
                cur2.execute(
                    """
                    UPDATE fleet_vehicles
                    SET status = 'pending_road_test', off_road_reason = 'issue'
                    WHERE id = %s
                    """,
                    (vid,),
                )
                conn.commit()
                cur2.close()
            if mark_vehicle_maintenance:
                cur2 = conn.cursor()
                cur2.execute(
                    """
                    UPDATE fleet_vehicles
                    SET status = 'maintenance', off_road_reason = NULL
                    WHERE id = %s
                    """,
                    (vid,),
                )
                conn.commit()
                cur2.close()
            if mark_vehicle_active:
                cur2 = conn.cursor()
                cur2.execute(
                    """
                    UPDATE fleet_vehicles
                    SET status = 'active', off_road_reason = NULL
                    WHERE id = %s
                      AND (
                        off_road_reason = 'issue'
                        OR status = 'maintenance'
                      )
                    """,
                    (vid,),
                )
                conn.commit()
                cur2.close()

            if complete:
                self._log_audit(
                    user_id=user_id,
                    action="fleet_issue_complete",
                    entity_type="fleet_issue",
                    entity_id=str(issue_id),
                    details={"vehicle_id": vid, "summary": resolution_summary},
                )
            else:
                audit_details: Dict[str, Any] = {}
                if kanban_stage is not None:
                    audit_details["kanban_stage"] = kanban_stage
                if scheduled_service_date is not None:
                    audit_details["scheduled_service_date_updated"] = True
                if manager_notes is not None:
                    audit_details["manager_notes_updated"] = True
                if vehicle_marked_vor is True:
                    audit_details["marked_vor"] = True
                elif vehicle_marked_vor is False:
                    audit_details["cleared_vor_issue_flag"] = True
                if mark_vehicle_maintenance:
                    audit_details["marked_workshop"] = True
                if mark_vehicle_pending_road_test:
                    audit_details["vehicle_pending_road_test"] = True
                if mark_vehicle_active:
                    audit_details["vehicle_returned_in_service"] = True
                self._log_audit(
                    user_id=user_id,
                    action="fleet_issue_update",
                    entity_type="fleet_issue",
                    entity_id=str(issue_id),
                    details=audit_details if audit_details else None,
                )
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_vehicle_history_timeline(
        self, vehicle_id: int, *, limit: int = 80
    ) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        events: List[Dict[str, Any]] = []
        try:
            cur.execute(
                """
                SELECT id, created_at, mileage_reported, responses, actor_type, actor_id
                FROM fleet_vdi_submissions
                WHERE vehicle_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (int(vehicle_id), min(limit, 200)),
            )
            for r in cur.fetchall() or []:
                events.append(
                    {
                        "kind": "vdi",
                        "at": r.get("created_at"),
                        "title": "Daily inspection",
                        "detail": f"Mileage {r.get('mileage_reported') or '—'}",
                        "ref_id": r.get("id"),
                        "payload": r,
                    }
                )

            cur.execute(
                """
                SELECT id, title, kanban_stage, completed_at, created_at, resolution_summary,
                       manager_notes, scheduled_service_date
                FROM fleet_issues
                WHERE vehicle_id = %s
                ORDER BY COALESCE(completed_at, created_at) DESC
                LIMIT %s
                """,
                (int(vehicle_id), min(limit, 200)),
            )
            for r in cur.fetchall() or []:
                label = r.get("title") or "Issue"
                if r.get("completed_at"):
                    label = f"Issue resolved: {label}"
                events.append(
                    {
                        "kind": "issue",
                        "at": r.get("completed_at") or r.get("created_at"),
                        "title": label,
                        "detail": (r.get("resolution_summary") or r.get("manager_notes") or "")[:200],
                        "ref_id": r.get("id"),
                        "payload": r,
                    }
                )

            cur.execute(
                """
                SELECT id, service_date, service_type, notes, cost, created_at
                FROM fleet_maintenance_events
                WHERE vehicle_id = %s
                ORDER BY service_date DESC, id DESC
                LIMIT %s
                """,
                (int(vehicle_id), 50),
            )
            for r in cur.fetchall() or []:
                events.append(
                    {
                        "kind": "maintenance",
                        "at": r.get("service_date"),
                        "title": f"Maintenance: {r.get('service_type')}",
                        "detail": r.get("notes") or "",
                        "ref_id": r.get("id"),
                        "payload": r,
                    }
                )

            cur.execute(
                """
                SELECT id, performed_at, mileage_at_check, summary_notes, created_at
                FROM fleet_safety_checks
                WHERE vehicle_id = %s
                ORDER BY performed_at DESC, id DESC
                LIMIT %s
                """,
                (int(vehicle_id), 50),
            )
            for r in cur.fetchall() or []:
                events.append(
                    {
                        "kind": "safety_check",
                        "at": r.get("performed_at") or r.get("created_at"),
                        "title": "Workshop safety check",
                        "detail": (
                            f"Mileage {r.get('mileage_at_check') or '—'}"
                            + (
                                f" · {(r.get('summary_notes') or '')[:120]}"
                                if r.get("summary_notes")
                                else ""
                            )
                        ),
                        "ref_id": r.get("id"),
                        "payload": r,
                    }
                )

            cur.execute(
                """
                SELECT id, logged_at, start_mileage, end_mileage, purpose
                FROM fleet_mileage_logs
                WHERE vehicle_id = %s
                ORDER BY logged_at DESC
                LIMIT %s
                """,
                (int(vehicle_id), 50),
            )
            for r in cur.fetchall() or []:
                events.append(
                    {
                        "kind": "mileage",
                        "at": r.get("logged_at"),
                        "title": "Mileage log",
                        "detail": f"{r.get('start_mileage')} → {r.get('end_mileage')} {r.get('purpose') or ''}",
                        "ref_id": r.get("id"),
                        "payload": r,
                    }
                )

            events.sort(
                key=lambda x: (
                    x.get("at") is None,
                    x.get("at") or datetime.min,
                ),
                reverse=True,
            )
            return events[:limit]
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def list_open_notifications(self) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT * FROM fleet_notifications
                WHERE dismissed_at IS NULL
                ORDER BY created_at DESC
                LIMIT 100
                """
            )
            return cur.fetchall() or []
        except Exception:
            logger.exception("list_open_notifications")
            return []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def dismiss_fleet_notification(self, notif_id: int) -> None:
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE fleet_notifications
                SET dismissed_at = NOW()
                WHERE id = %s
                """,
                (int(notif_id),),
            )
            conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def _vehicle_compliance_expired(
        self, mot: Any, tax: Any, ins: Any, today: date
    ) -> tuple:
        reasons: List[str] = []

        def _d(x) -> Optional[date]:
            if not x:
                return None
            if isinstance(x, date) and not isinstance(x, datetime):
                return x
            if isinstance(x, datetime):
                return x.date()
            try:
                return datetime.strptime(str(x)[:10], "%Y-%m-%d").date()
            except Exception:
                return None

        for label, raw in (
            ("MOT", mot),
            ("Tax", tax),
            ("Insurance", ins),
        ):
            d = _d(raw)
            if d and d < today:
                reasons.append(label)
        return (len(reasons) > 0, reasons)

    def sync_compliance_vehicle_status(self) -> Dict[str, Any]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        summary: Dict[str, Any] = {"set_off_road": 0, "released": 0, "notified": 0}
        today = date.today()
        try:
            cur.execute(
                """
                SELECT v.id, v.registration, v.internal_code, v.status, v.off_road_reason,
                       c.mot_expiry, c.tax_expiry, c.insurance_expiry
                FROM fleet_vehicles v
                JOIN fleet_compliance c ON c.vehicle_id = v.id
                WHERE v.status != 'decommissioned'
                """
            )
            rows = cur.fetchall() or []
            cur2 = conn.cursor()
            for r in rows:
                vid = int(r["id"])
                expired, reasons = self._vehicle_compliance_expired(
                    r.get("mot_expiry"),
                    r.get("tax_expiry"),
                    r.get("insurance_expiry"),
                    today,
                )
                st = str(r.get("status") or "")
                reason = r.get("off_road_reason")

                if expired:
                    if st in ("active", "pending_road_test", "maintenance") or (
                        st == "off_road" and reason == "compliance"
                    ):
                        cur2.execute(
                            """
                            UPDATE fleet_vehicles
                            SET status = 'off_road', off_road_reason = 'compliance'
                            WHERE id = %s
                              AND NOT (
                                status <=> 'off_road' AND off_road_reason <=> 'compliance'
                              )
                            """,
                            (vid,),
                        )
                        if cur2.rowcount:
                            summary["set_off_road"] += 1
                            _vn = (
                                (r.get("registration") or "").strip()
                                or (r.get("internal_code") or "").strip()
                                or f"#{vid}"
                            )
                            msg = (
                                f"{_vn}: compliance expired ({', '.join(reasons)}); "
                                "vehicle marked VOR (Vehicle off road)."
                            )
                            cur2.execute(
                                """
                                INSERT INTO fleet_notifications (kind, vehicle_id, message)
                                VALUES ('compliance_vor', %s, %s)
                                """,
                                (vid, msg[:500]),
                            )
                            summary["notified"] += 1
                else:
                    if reason == "compliance" and st == "off_road":
                        cur2.execute(
                            """
                            UPDATE fleet_vehicles
                            SET status = 'active', off_road_reason = NULL
                            WHERE id = %s
                            """,
                            (vid,),
                        )
                        if cur2.rowcount:
                            summary["released"] += 1
            conn.commit()
            try:
                cur2.close()
            except Exception:
                pass
            return summary
        except Exception:
            logger.exception("sync_compliance_vehicle_status")
            return summary
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def vehicles_for_crew_picklist(self, *, limit: int = 300) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT v.id, v.internal_code, v.registration, v.make, v.model, v.status,
                       v.id_photo_path, v.off_road_reason,
                       c.mot_expiry, c.tax_expiry, c.insurance_expiry
                FROM fleet_vehicles v
                LEFT JOIN fleet_compliance c ON c.vehicle_id = v.id
                WHERE v.status != 'decommissioned'
                ORDER BY v.internal_code ASC, v.registration ASC
                LIMIT %s
                """,
                (min(limit, 500),),
            )
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
