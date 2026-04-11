"""
Scheduling module services: shifts, availability, time off, swap requests.
Uses tb_contractors, clients, sites, job_types from time_billing_module.
"""
import logging
import json
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from app.objects import get_db_connection

from .cura_planning_staffing import (
    build_role_coverage,
    parse_cura_event_config,
    vehicle_gap_hint,
)
import base64
import hashlib
from cryptography.fernet import Fernet
import requests

logger = logging.getLogger(__name__)


def _safe_float(x: Any) -> Optional[float]:
    """Return float(x) or None for invalid/empty values."""
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _safe_int_id(val: Any) -> Optional[int]:
    """Coerce DB contractor / id values to int; None if invalid."""
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


class ScheduleService:
    @staticmethod
    def get_job_type_requirements(job_type_id: int) -> Dict[str, List[str]]:
        """Return {'skills': [...], 'qualifications': [...]} for job type; empty lists if none."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_job_type_requirements'")
            if not cur.fetchone():
                return {"skills": [], "qualifications": []}
            cur.execute(
                "SELECT required_skills_json, required_qualifications_json FROM schedule_job_type_requirements WHERE job_type_id = %s LIMIT 1",
                (int(job_type_id),),
            )
            r = cur.fetchone() or {}
            skills = []
            quals = []
            try:
                if r.get("required_skills_json"):
                    skills = json.loads(r["required_skills_json"]) if isinstance(r["required_skills_json"], str) else (r["required_skills_json"] or [])
                if r.get("required_qualifications_json"):
                    quals = json.loads(r["required_qualifications_json"]) if isinstance(r["required_qualifications_json"], str) else (r["required_qualifications_json"] or [])
            except Exception:
                skills, quals = [], []
            return {
                "skills": [str(x).strip() for x in (skills or []) if str(x).strip()],
                "qualifications": [str(x).strip() for x in (quals or []) if str(x).strip()],
            }
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def set_job_type_requirements(job_type_id: int, skills: List[str], qualifications: List[str]) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_job_type_requirements'")
            if not cur.fetchone():
                return False
            cur.execute(
                """
                INSERT INTO schedule_job_type_requirements (job_type_id, required_skills_json, required_qualifications_json)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  required_skills_json = VALUES(required_skills_json),
                  required_qualifications_json = VALUES(required_qualifications_json)
                """,
                (
                    int(job_type_id),
                    json.dumps([str(x).strip() for x in (skills or []) if str(x).strip()]) if skills is not None else None,
                    json.dumps([str(x).strip() for x in (qualifications or []) if str(x).strip()]) if qualifications is not None else None,
                ),
            )
            conn.commit()
            return True
        finally:
            cur.close()
            conn.close()

    _CREW_PROFILE_DICT_STRING_KEYS = (
        "label",
        "name",
        "grade",
        "role",
        "title",
        "code",
        "competency",
        "competency_name",
        "type",
    )

    @staticmethod
    def _crew_profile_list_from_json_field(raw: Any) -> List[Any]:
        if raw is None or raw == "":
            return []
        if isinstance(raw, list):
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                return list(parsed) if isinstance(parsed, list) else []
            except Exception:
                return []
        return []

    @staticmethod
    def _tokens_from_crew_profile_item(item: Any) -> Set[str]:
        """Extract comparable lowercase tokens from a profile list entry (str or dict)."""
        out: Set[str] = set()
        if item is None:
            return out
        if isinstance(item, str):
            s = item.strip()
            if s:
                out.add(s.lower())
            return out
        if isinstance(item, dict):
            for k in ScheduleService._CREW_PROFILE_DICT_STRING_KEYS:
                v = item.get(k)
                if v is None:
                    continue
                sv = str(v).strip()
                if sv:
                    out.add(sv.lower())
            g = item.get("grade")
            r = item.get("role")
            if g and r:
                gs = str(g).strip()
                rs = str(r).strip()
                if gs and rs:
                    gl, rl = gs.lower(), rs.lower()
                    out.update(
                        {
                            gl,
                            rl,
                            f"{rl} · {gl}",
                            f"{gl} · {rl}",
                            f"{rl} {gl}",
                            f"{gl} {rl}",
                        }
                    )
            return out
        return out

    @staticmethod
    def _expand_matching_tokens(tokens: Set[str]) -> Set[str]:
        """Split composite display strings so requirements can match a single segment."""
        expanded = set(tokens)
        for t in tokens:
            for sep in (" · ", " | ", " — ", ", ", ",", ";", "/"):
                if sep in t:
                    for part in t.split(sep):
                        p = part.strip().lower()
                        if len(p) >= 2:
                            expanded.add(p)
        return expanded

    @staticmethod
    def _tokens_from_mdt_crew_profile(cur, contractor_id: int) -> Tuple[Set[str], Set[str]]:
        skill_tokens: Set[str] = set()
        qual_tokens: Set[str] = set()
        try:
            cur.execute("SHOW TABLES LIKE 'mdt_crew_profiles'")
            if not cur.fetchone():
                return skill_tokens, qual_tokens
            cur.execute(
                "SELECT skills_json, qualifications_json FROM mdt_crew_profiles WHERE contractor_id = %s LIMIT 1",
                (int(contractor_id),),
            )
            r = cur.fetchone() or {}
            for it in ScheduleService._crew_profile_list_from_json_field(
                r.get("skills_json")
            ):
                skill_tokens |= ScheduleService._tokens_from_crew_profile_item(it)
            for it in ScheduleService._crew_profile_list_from_json_field(
                r.get("qualifications_json")
            ):
                qual_tokens |= ScheduleService._tokens_from_crew_profile_item(it)
        except Exception:
            logger.debug(
                "mdt_crew_profiles read failed for contractor %s",
                contractor_id,
                exc_info=True,
            )
        return (
            ScheduleService._expand_matching_tokens(skill_tokens),
            ScheduleService._expand_matching_tokens(qual_tokens),
        )

    @staticmethod
    def _tokens_from_training_competencies(cur, contractor_id: int) -> Tuple[Set[str], Set[str]]:
        skill_tokens: Set[str] = set()
        qual_tokens: Set[str] = set()
        try:
            from app.plugins.training_module.services import TrainingService

            if not TrainingService.person_competencies_table_exists():
                return skill_tokens, qual_tokens
            cur.execute("SHOW TABLES LIKE 'trn_person_competencies'")
            if not cur.fetchone():
                return skill_tokens, qual_tokens
            jt: Optional[str] = None
            cur.execute("SHOW TABLES LIKE 'hr_staff_details'")
            if cur.fetchone():
                cur.execute(
                    "SELECT job_title FROM hr_staff_details WHERE contractor_id = %s LIMIT 1",
                    (int(contractor_id),),
                )
                jrow = cur.fetchone() or {}
                jt = (jrow.get("job_title") or "").strip() or None
            cur.execute(
                """
                SELECT competency_kind, label, use_hr_job_title, expires_on
                FROM trn_person_competencies
                WHERE contractor_id = %s
                """,
                (int(contractor_id),),
            )
            for row in cur.fetchall() or []:
                rec = dict(row) if not isinstance(row, dict) else row
                if TrainingService._row_expired(rec):
                    continue
                kind = (rec.get("competency_kind") or "").strip().lower()
                label = (rec.get("label") or "").strip()
                use_hr = bool(rec.get("use_hr_job_title"))
                if kind == "skill" and label:
                    skill_tokens.add(label.lower())
                elif kind == "qualification" and label:
                    qual_tokens.add(label.lower())
                elif kind == "clinical_grade":
                    if use_hr and jt:
                        qual_tokens.add(jt.lower())
                    if label:
                        qual_tokens.add(label.lower())
                    if use_hr and jt and label:
                        qual_tokens.add(f"{jt} · {label}".lower())
        except Exception:
            logger.debug(
                "training competency fallback failed for contractor %s",
                contractor_id,
                exc_info=True,
            )
        return (
            ScheduleService._expand_matching_tokens(skill_tokens),
            ScheduleService._expand_matching_tokens(qual_tokens),
        )

    @staticmethod
    def _tokens_from_hr_role_title(cur, contractor_id: int) -> Set[str]:
        """Pay role name and HR job title as qualification tokens (sync may lag)."""
        found: Set[str] = set()
        try:
            cur.execute(
                """
                SELECT r.name AS role_name, h.job_title
                FROM tb_contractors c
                LEFT JOIN roles r ON r.id = c.role_id
                LEFT JOIN hr_staff_details h ON h.contractor_id = c.id
                WHERE c.id = %s
                LIMIT 1
                """,
                (int(contractor_id),),
            )
            row = cur.fetchone()
            if not row:
                return found
            for key in ("role_name", "job_title"):
                v = row.get(key)
                if v is not None and str(v).strip():
                    found.add(str(v).strip().lower())
        except Exception:
            pass
        return ScheduleService._expand_matching_tokens(found)

    @staticmethod
    def _get_contractor_profile_skill_qual_sets(
        cur, contractor_id: int
    ) -> Tuple[Set[str], Set[str]]:
        """
        Skill / qualification tokens for open-shift eligibility.

        Unions ``mdt_crew_profiles`` (HR + Training sync JSON), live Training
        competencies, and HR pay role / job title so eligibility matches real
        records even when sync JSON uses structured dicts or is stale.
        """
        m_sk, m_qu = ScheduleService._tokens_from_mdt_crew_profile(cur, contractor_id)
        t_sk, t_qu = ScheduleService._tokens_from_training_competencies(
            cur, contractor_id
        )
        hr_qu = ScheduleService._tokens_from_hr_role_title(cur, contractor_id)
        skills = set(m_sk) | set(t_sk)
        quals = set(m_qu) | set(t_qu) | hr_qu
        return skills, quals

    @staticmethod
    def list_shift_jobtype_counts_by_day(
        date_from: date,
        date_to: date,
        contractor_id: Optional[int] = None,
        client_id: Optional[int] = None,
    ) -> Dict[str, Dict[int, int]]:
        """
        Return mapping: day_iso -> { job_type_id -> count }.
        Used for month view colored breakdown.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            use_assignments = bool(cur.fetchone())
            where = ["s.work_date >= %s", "s.work_date <= %s"]
            params: List[Any] = [date_from, date_to]
            if contractor_id is not None:
                if use_assignments:
                    where.append("EXISTS (SELECT 1 FROM schedule_shift_assignments a WHERE a.shift_id = s.id AND a.contractor_id = %s)")
                else:
                    where.append("s.contractor_id = %s")
                params.append(contractor_id)
            if client_id is not None:
                where.append("s.client_id = %s")
                params.append(client_id)
            cur.execute(f"""
                SELECT s.work_date, s.job_type_id, COUNT(*) AS cnt
                FROM schedule_shifts s
                WHERE {" AND ".join(where)}
                  AND COALESCE(s.status,'') <> 'cancelled'
                GROUP BY s.work_date, s.job_type_id
            """, params)
            out: Dict[str, Dict[int, int]] = {}
            for r in (cur.fetchall() or []):
                wd = r.get("work_date")
                day_iso = wd.isoformat() if hasattr(wd, "isoformat") else str(wd)
                jt = int(r.get("job_type_id") or 0)
                cnt = int(r.get("cnt") or 0)
                if day_iso not in out:
                    out[day_iso] = {}
                out[day_iso][jt] = cnt
            return out
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _assignments_table_exists(conn) -> bool:
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            return bool(cur.fetchone())
        finally:
            cur.close()

    @staticmethod
    def list_shift_assignments(shift_id: int) -> List[Dict[str, Any]]:
        """Return list of {contractor_id, contractor_name, initials} for a shift."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            if not cur.fetchone():
                return []
            cur.execute("""
                SELECT a.contractor_id, u.name AS contractor_name, u.initials AS contractor_initials
                FROM schedule_shift_assignments a
                JOIN tb_contractors u ON u.id = a.contractor_id
                WHERE a.shift_id = %s
                ORDER BY a.id
            """, (shift_id,))
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_assignments_for_shifts(shift_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        """Return {shift_id: [assignments]} for labor/overlap. Empty if table missing."""
        if not shift_ids:
            return {}
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            if not cur.fetchone():
                return {sid: [] for sid in shift_ids}
            placeholders = ",".join(["%s"] * len(shift_ids))
            cur.execute(f"""
                SELECT a.shift_id, a.contractor_id, u.name AS contractor_name
                FROM schedule_shift_assignments a
                JOIN tb_contractors u ON u.id = a.contractor_id
                WHERE a.shift_id IN ({placeholders})
                ORDER BY a.shift_id, a.id
            """, shift_ids)
            rows = cur.fetchall() or []
            out: Dict[int, List[Dict[str, Any]]] = {sid: [] for sid in shift_ids}
            for r in rows:
                out.setdefault(int(r["shift_id"]), []).append({
                    "contractor_id": r["contractor_id"],
                    "contractor_name": r.get("contractor_name"),
                })
            return out
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_shift_assignment(shift_id: int, contractor_id: int) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            if not cur.fetchone():
                return False
            cur.execute(
                "INSERT IGNORE INTO schedule_shift_assignments (shift_id, contractor_id) VALUES (%s, %s)",
                (shift_id, contractor_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def remove_shift_assignment(shift_id: int, contractor_id: int) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            if not cur.fetchone():
                return False
            cur.execute(
                "DELETE FROM schedule_shift_assignments WHERE shift_id = %s AND contractor_id = %s",
                (shift_id, contractor_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def set_shift_assignments(shift_id: int, contractor_ids: List[int]) -> None:
        """Replace all assignments for a shift with the given list."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            if not cur.fetchone():
                return
            cur.execute("DELETE FROM schedule_shift_assignments WHERE shift_id = %s", (shift_id,))
            for cid in (contractor_ids or []):
                if cid is None:
                    continue
                cur.execute(
                    "INSERT IGNORE INTO schedule_shift_assignments (shift_id, contractor_id) VALUES (%s, %s)",
                    (shift_id, int(cid)),
                )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_shifts(
        contractor_id: Optional[int] = None,
        client_id: Optional[int] = None,
        work_date: Optional[date] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            has_assignments = ScheduleService._assignments_table_exists(conn)
            where = ["1=1"]
            params: List[Any] = []
            if contractor_id is not None and has_assignments:
                where.append("EXISTS (SELECT 1 FROM schedule_shift_assignments a WHERE a.shift_id = s.id AND a.contractor_id = %s)")
                params.append(contractor_id)
            elif contractor_id is not None:
                where.append("s.contractor_id = %s")
                params.append(contractor_id)
            if client_id is not None:
                where.append("s.client_id = %s")
                params.append(client_id)
            if work_date is not None:
                where.append("s.work_date = %s")
                params.append(work_date)
            if date_from is not None:
                where.append("s.work_date >= %s")
                params.append(date_from)
            if date_to is not None:
                where.append("s.work_date <= %s")
                params.append(date_to)
            if status:
                where.append("s.status = %s")
                params.append(status)
            if has_assignments:
                cur.execute(f"""
                    SELECT s.*,
                           c.name AS client_name,
                           st.name AS site_name,
                           st.postcode AS site_postcode,
                           jt.name AS job_type_name,
                           u.name AS contractor_name,
                           u.initials AS contractor_initials,
                           (SELECT COUNT(*) FROM schedule_shift_assignments a WHERE a.shift_id = s.id) AS assignment_count
                    FROM schedule_shifts s
                    JOIN clients c ON c.id = s.client_id
                    LEFT JOIN sites st ON st.id = s.site_id
                    JOIN job_types jt ON jt.id = s.job_type_id
                    LEFT JOIN (
                        SELECT a.shift_id, a.contractor_id
                        FROM schedule_shift_assignments a
                        INNER JOIN (SELECT shift_id, MIN(id) AS mid FROM schedule_shift_assignments GROUP BY shift_id) first ON first.shift_id = a.shift_id AND first.mid = a.id
                    ) first_a ON first_a.shift_id = s.id
                    LEFT JOIN tb_contractors u ON u.id = first_a.contractor_id
                    WHERE {" AND ".join(where)}
                    ORDER BY s.work_date, s.scheduled_start
                """, params)
            else:
                cur.execute(f"""
                    SELECT s.*,
                           c.name AS client_name,
                           st.name AS site_name,
                           st.postcode AS site_postcode,
                           jt.name AS job_type_name,
                           u.name AS contractor_name,
                           u.initials AS contractor_initials
                    FROM schedule_shifts s
                    JOIN clients c ON c.id = s.client_id
                    LEFT JOIN sites st ON st.id = s.site_id
                    JOIN job_types jt ON jt.id = s.job_type_id
                    LEFT JOIN tb_contractors u ON u.id = s.contractor_id
                    WHERE {" AND ".join(where)}
                    ORDER BY s.work_date, s.scheduled_start
                """, params)
            rows = cur.fetchall() or []
            if has_assignments and contractor_id is not None and rows:
                for r in rows:
                    r["contractor_id"] = contractor_id
                    cur.execute("SELECT name, initials FROM tb_contractors WHERE id = %s", (contractor_id,))
                    u = cur.fetchone()
                    if u:
                        r["contractor_name"] = u.get("name")
                        r["contractor_initials"] = u.get("initials")
            return rows
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_open_shifts(
        date_from: date,
        date_to: date,
        client_id: Optional[int] = None,
        job_type_id: Optional[int] = None,
        status: str = "published",
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """List shifts that still need staff (assignment count < required_count)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            has_assignments = bool(cur.fetchone())
            cur.execute("SHOW COLUMNS FROM schedule_shifts LIKE 'required_count'")
            has_required = bool(cur.fetchone())
            where = [
                "s.work_date >= %s",
                "s.work_date <= %s",
                "COALESCE(s.status,'') = %s",
            ]
            params: List[Any] = [date_from, date_to, status]
            if has_assignments and has_required:
                where.append("(SELECT COUNT(*) FROM schedule_shift_assignments a WHERE a.shift_id = s.id) < COALESCE(s.required_count, 1)")
            else:
                where.append("s.contractor_id IS NULL")
            if client_id is not None:
                where.append("s.client_id = %s")
                params.append(client_id)
            if job_type_id is not None:
                where.append("s.job_type_id = %s")
                params.append(job_type_id)
            if limit < 1:
                limit = 1
            if limit > 500:
                limit = 500
            params.append(limit)
            cur.execute(f"""
                SELECT s.*,
                       c.name AS client_name,
                       st.name AS site_name,
                       jt.name AS job_type_name
                FROM schedule_shifts s
                JOIN clients c ON c.id = s.client_id
                LEFT JOIN sites st ON st.id = s.site_id
                JOIN job_types jt ON jt.id = s.job_type_id
                WHERE {" AND ".join(where)}
                ORDER BY s.work_date, s.scheduled_start
                LIMIT %s
            """, params)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_open_shift_claim_mode() -> str:
        """Return 'auto' or 'manager'."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_settings'")
            if not cur.fetchone():
                return "auto"
            cur.execute("SELECT open_shift_claim_mode FROM schedule_settings WHERE id = 1 LIMIT 1")
            r = cur.fetchone() or {}
            mode = str(r.get("open_shift_claim_mode") or "auto").strip().lower()
            return mode if mode in ("auto", "manager") else "auto"
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def set_open_shift_claim_mode(mode: str) -> bool:
        m = (mode or "").strip().lower()
        if m not in ("auto", "manager"):
            return False
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO schedule_settings (id, open_shift_claim_mode) VALUES (1, %s) ON DUPLICATE KEY UPDATE open_shift_claim_mode = VALUES(open_shift_claim_mode)", (m,))
            conn.commit()
            return True
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_overtime_hours_per_week() -> int:
        """Hours per week after which to flag overtime (default 40)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_settings'")
            if not cur.fetchone():
                return 40
            cur.execute("SHOW COLUMNS FROM schedule_settings LIKE 'overtime_hours_per_week'")
            if not cur.fetchone():
                return 40
            cur.execute("SELECT overtime_hours_per_week FROM schedule_settings WHERE id = 1 LIMIT 1")
            r = cur.fetchone() or {}
            v = r.get("overtime_hours_per_week")
            return int(v) if v is not None else 40
        except Exception:
            return 40
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def set_overtime_hours_per_week(hours: int) -> bool:
        hours = max(0, min(168, int(hours)))
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW COLUMNS FROM schedule_settings LIKE 'overtime_hours_per_week'")
            if not cur.fetchone():
                return False
            cur.execute("UPDATE schedule_settings SET overtime_hours_per_week = %s WHERE id = 1", (hours,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_weekly_budget() -> Optional[float]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW COLUMNS FROM schedule_settings LIKE 'weekly_budget'")
            if not cur.fetchone():
                return None
            cur.execute("SELECT weekly_budget FROM schedule_settings WHERE id = 1 LIMIT 1")
            r = cur.fetchone() or {}
            v = r.get("weekly_budget")
            return float(v) if v is not None else None
        except Exception:
            return None
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def set_weekly_budget(amount: Optional[float]) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW COLUMNS FROM schedule_settings LIKE 'weekly_budget'")
            if not cur.fetchone():
                return False
            cur.execute("UPDATE schedule_settings SET weekly_budget = %s WHERE id = 1", (amount,))
            conn.commit()
            return True
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_assignment_label() -> str:
        """Label for client/base/location (e.g. 'Client', 'Base', 'Location'). Default 'Client'."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW COLUMNS FROM schedule_settings LIKE 'assignment_label'")
            if not cur.fetchone():
                return "Client"
            cur.execute("SELECT assignment_label FROM schedule_settings WHERE id = 1 LIMIT 1")
            r = cur.fetchone() or {}
            v = (r.get("assignment_label") or "Client").strip()
            return v[:64] if v else "Client"
        except Exception:
            return "Client"
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def set_assignment_label(label: str) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW COLUMNS FROM schedule_settings LIKE 'assignment_label'")
            if not cur.fetchone():
                return False
            cur.execute(
                "INSERT INTO schedule_settings (id, assignment_label) VALUES (1, %s) ON DUPLICATE KEY UPDATE assignment_label = VALUES(assignment_label)",
                ((label or "Client").strip()[:64],),
            )
            conn.commit()
            return True
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_assignment_instructions(client_id: int) -> Optional[str]:
        """Location/assignment instructions for this client (base) – e.g. access codes, preferences."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_assignment_instructions'")
            if not cur.fetchone():
                return None
            cur.execute(
                "SELECT instructions FROM schedule_assignment_instructions WHERE client_id = %s LIMIT 1",
                (client_id,),
            )
            r = cur.fetchone()
            return (r.get("instructions") or "").strip() or None if r else None
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def set_assignment_instructions(client_id: int, instructions: Optional[str]) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_assignment_instructions'")
            if not cur.fetchone():
                return False
            text = (instructions or "").strip() or None
            cur.execute(
                """
                INSERT INTO schedule_assignment_instructions (client_id, instructions) VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE instructions = VALUES(instructions)
                """,
                (client_id, text),
            )
            conn.commit()
            return True
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_assignment_instructions() -> List[Dict[str, Any]]:
        """All clients with their assignment instructions (for admin list)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_assignment_instructions'")
            if not cur.fetchone():
                return []
            cur.execute("""
                SELECT c.id AS client_id, c.name AS client_name, ai.instructions
                FROM clients c
                LEFT JOIN schedule_assignment_instructions ai ON ai.client_id = c.id
                WHERE c.active = 1
                ORDER BY c.name
            """)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_weekly_labor_totals(week_monday: date) -> Dict[str, Any]:
        """Scheduled hours and labour cost for the week. Returns total_cost, by_contractor, overtime_contractor_ids, total_hours."""
        week_end = week_monday + timedelta(days=6)
        shifts = ScheduleService.list_shifts(date_from=week_monday, date_to=week_end)
        shifts = [s for s in shifts if s.get("status") not in ("cancelled",)]
        shift_ids = [s["id"] for s in shifts]
        assignments_map = ScheduleService.get_assignments_for_shifts(shift_ids) if shift_ids else {}
        total_cost = 0.0
        total_hours = 0.0
        by_contractor: Dict[int, Dict[str, Any]] = {}
        for s in shifts:
            start = s.get("scheduled_start")
            end = s.get("scheduled_end")
            break_mins = int(s.get("break_mins") or 0)
            if start and end and hasattr(start, "hour") and hasattr(end, "hour"):
                mins = (end.hour * 60 + end.minute) - (start.hour * 60 + start.minute) - break_mins
                hours = max(0, mins) / 60.0
            else:
                hours = 0.0
            cost = float(s.get("labour_cost") or 0)
            assignees = assignments_map.get(s["id"]) or []
            if not assignees:
                cid = s.get("contractor_id")
                if cid is not None:
                    assignees = [{"contractor_id": cid}]
            for a in assignees:
                cid = a.get("contractor_id")
                if cid is None:
                    continue
                total_cost += cost
                total_hours += hours
                by_contractor.setdefault(cid, {"hours": 0.0, "cost": 0.0})
                by_contractor[cid]["hours"] += hours
                by_contractor[cid]["cost"] += cost
        threshold = ScheduleService.get_overtime_hours_per_week()
        overtime_contractor_ids = [cid for cid, d in by_contractor.items() if d["hours"] > threshold]
        return {
            "total_cost": total_cost,
            "total_hours": total_hours,
            "by_contractor": by_contractor,
            "overtime_contractor_ids": overtime_contractor_ids,
            "overtime_threshold": threshold,
            "weekly_budget": ScheduleService.get_weekly_budget(),
        }

    @staticmethod
    def _shift_hours(s: Dict[str, Any]) -> float:
        """Scheduled hours for one shift (end - start - break)."""
        start = s.get("scheduled_start")
        end = s.get("scheduled_end")
        break_mins = int(s.get("break_mins") or 0)
        if start and end and hasattr(start, "hour") and hasattr(end, "hour"):
            mins = (end.hour * 60 + end.minute) - (start.hour * 60 + start.minute) - break_mins
            return max(0, mins) / 60.0
        return 0.0

    @staticmethod
    def get_analytics_weekly_summary(num_weeks: int = 12) -> List[Dict[str, Any]]:
        """Last num_weeks (by Monday): total_hours, total_cost, shift_count, overtime_count."""
        today = date.today()
        week_start = today - timedelta(days=today.weekday())
        out: List[Dict[str, Any]] = []
        for i in range(num_weeks):
            monday = week_start - timedelta(days=7 * i)
            tot = ScheduleService.get_weekly_labor_totals(monday)
            out.append({
                "week_monday": monday,
                "total_hours": tot["total_hours"],
                "total_cost": tot["total_cost"],
                "shift_count": 0,
                "overtime_count": len(tot["overtime_contractor_ids"]),
            })
        # shift_count: we need to count shifts for that week
        shifts_per_week = {}
        for i in range(num_weeks):
            monday = week_start - timedelta(days=7 * i)
            week_end = monday + timedelta(days=6)
            sh = ScheduleService.list_shifts(date_from=monday, date_to=week_end)
            shifts_per_week[monday] = len([s for s in sh if s.get("status") != "cancelled"])
        for row in out:
            row["shift_count"] = shifts_per_week.get(row["week_monday"], 0)
        return out

    @staticmethod
    def get_analytics_coverage(
        date_from: date,
        date_to: date,
        client_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Coverage heatmap data: days x job types -> hours. Returns day_labels, job_type_names, grid (list of list)."""
        shifts = ScheduleService.list_shifts(date_from=date_from, date_to=date_to, client_id=client_id)
        shifts = [s for s in shifts if s.get("status") != "cancelled"]
        job_types = ScheduleService.list_job_types()
        jt_id_to_name = {int(j["id"]): j.get("name") or f"Job {j['id']}" for j in job_types}
        jt_order = [int(j["id"]) for j in job_types]
        days_list: List[date] = []
        d = date_from
        while d <= date_to:
            days_list.append(d)
            d += timedelta(days=1)
        grid: List[Dict[int, float]] = [{} for _ in days_list]
        day_to_idx = {d: i for i, d in enumerate(days_list)}
        for s in shifts:
            wd = s.get("work_date")
            jt_id = int(s.get("job_type_id") or 0)
            if wd not in day_to_idx or jt_id not in jt_order:
                continue
            idx = day_to_idx[wd]
            hrs = ScheduleService._shift_hours(s)
            grid[idx][jt_id] = grid[idx].get(jt_id, 0) + hrs
        # Build matrix rows = days, cols = job types
        rows = []
        for i, d in enumerate(days_list):
            row = [round(grid[i].get(jid, 0), 1) for jid in jt_order]
            rows.append({"date": d, "hours_by_job_type": row, "total": round(sum(grid[i].values()), 1)})
        return {
            "day_labels": [d.strftime("%a %d") for d in days_list],
            "job_type_names": [jt_id_to_name.get(jid, "") for jid in jt_order],
            "job_type_ids": jt_order,
            "rows": rows,
            "date_from": date_from,
            "date_to": date_to,
        }

    @staticmethod
    def list_open_shift_claims(status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = ["1=1"]
            params: List[Any] = []
            if status:
                where.append("c.status = %s")
                params.append(status)
            if limit < 1:
                limit = 1
            if limit > 500:
                limit = 500
            params.append(limit)
            cur.execute(f"""
                SELECT c.*, s.work_date, s.scheduled_start, s.scheduled_end, s.client_id, s.site_id, s.job_type_id,
                       cl.name AS client_name, st.name AS site_name, jt.name AS job_type_name,
                       u.name AS claimer_name, u.email AS claimer_email
                FROM schedule_open_shift_claims c
                JOIN schedule_shifts s ON s.id = c.shift_id
                JOIN clients cl ON cl.id = s.client_id
                LEFT JOIN sites st ON st.id = s.site_id
                JOIN job_types jt ON jt.id = s.job_type_id
                JOIN tb_contractors u ON u.id = c.claimer_contractor_id
                WHERE {" AND ".join(where)}
                ORDER BY c.claimed_at DESC
                LIMIT %s
            """, params)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def count_open_shift_claims(status: str = "claimed") -> int:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_open_shift_claims'")
            if not cur.fetchone():
                return 0
            cur.execute("SELECT COUNT(*) FROM schedule_open_shift_claims WHERE status = %s", (status,))
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def check_open_shift_eligibility(contractor_id: int, shift: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Check if a contractor can claim this open shift (overlap, availability, skills/qualifications).
        shift must have work_date, scheduled_start, scheduled_end, job_type_id.
        Returns (True, '') if eligible, (False, 'reason') otherwise.
        """
        wd = shift.get("work_date")
        start = shift.get("scheduled_start")
        end = shift.get("scheduled_end")
        jt_id = shift.get("job_type_id")
        if not wd or not start or not end:
            return False, "Invalid shift time."
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            # Overlap
            cur.execute("""
                SELECT 1
                FROM schedule_shifts
                WHERE contractor_id = %s
                  AND work_date = %s
                  AND COALESCE(status,'') <> 'cancelled'
                  AND NOT (scheduled_end <= %s OR scheduled_start >= %s)
                LIMIT 1
            """, (contractor_id, wd, start, end))
            if cur.fetchone():
                return False, "You already have a shift that overlaps this time."
            # Job type requirements (skills/qualifications)
            req = ScheduleService.get_job_type_requirements(int(jt_id or 0))
            if req.get("skills") or req.get("qualifications"):
                prof_skills, prof_quals = (
                    ScheduleService._get_contractor_profile_skill_qual_sets(
                        cur, contractor_id
                    )
                )
                missing_skills = []
                for r in req.get("skills", []):
                    tok = str(r or "").strip().lower()
                    if tok and tok not in prof_skills:
                        missing_skills.append(str(r).strip())
                missing_quals = []
                for r in req.get("qualifications", []):
                    tok = str(r or "").strip().lower()
                    if tok and tok not in prof_quals:
                        missing_quals.append(str(r).strip())
                if missing_skills or missing_quals:
                    parts = []
                    if missing_skills:
                        parts.append("skills: " + ", ".join(missing_skills))
                    if missing_quals:
                        parts.append("qualifications: " + ", ".join(missing_quals))
                    return False, "Not eligible for this job type (" + "; ".join(parts) + ")."
            # Availability (only if contractor has any availability configured)
            if ScheduleService._contractor_has_any_availability(cur, contractor_id):
                if not ScheduleService._contractor_within_availability(cur, contractor_id, wd, start, end):
                    return False, "This shift is outside your availability window."
            return True, ""
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _contractor_has_any_availability(cur, contractor_id: int) -> bool:
        """True if contractor has any availability or unavailability configured (so we apply availability rules)."""
        cur.execute("SELECT 1 FROM schedule_availability WHERE contractor_id = %s LIMIT 1", (contractor_id,))
        if cur.fetchone():
            return True
        cur.execute("SELECT 1 FROM schedule_unavailability WHERE contractor_id = %s LIMIT 1", (contractor_id,))
        return cur.fetchone() is not None

    @staticmethod
    def _contractor_overlaps_unavailability(cur, contractor_id: int, work_date: date, start_t: time, end_t: time) -> bool:
        """True if the given day/time overlaps any unavailability window."""
        dow = int(work_date.weekday())
        cur.execute("""
            SELECT start_time, end_time, effective_from, effective_to
            FROM schedule_unavailability
            WHERE contractor_id = %s AND day_of_week = %s
        """, (contractor_id, dow))
        rows = cur.fetchall() or []
        for r in rows:
            ef = r.get("effective_from")
            et = r.get("effective_to")
            if ef and work_date < ef:
                continue
            if et and work_date > et:
                continue
            st = r.get("start_time")
            en = r.get("end_time")
            if st is None or en is None:
                continue
            if not (end_t <= st or start_t >= en):
                return True
        return False

    @staticmethod
    def _contractor_within_availability(cur, contractor_id: int, work_date: date, start_t: time, end_t: time) -> bool:
        dow = int(work_date.weekday())  # Monday=0
        # If they have availability windows, shift must be inside at least one (otherwise default-available)
        cur.execute("""
            SELECT start_time, end_time, effective_from, effective_to
            FROM schedule_availability
            WHERE contractor_id = %s AND day_of_week = %s
        """, (contractor_id, dow))
        avail_rows = cur.fetchall() or []
        if avail_rows:
            in_avail = False
            for r in avail_rows:
                ef, et = r.get("effective_from"), r.get("effective_to")
                if ef and work_date < ef or (et and work_date > et):
                    continue
                st, en = r.get("start_time"), r.get("end_time")
                if st is not None and en is not None and st <= start_t and end_t <= en:
                    in_avail = True
                    break
            if not in_avail:
                return False
        # If they have unavailability windows, shift must not overlap any
        if ScheduleService._contractor_overlaps_unavailability(cur, contractor_id, work_date, start_t, end_t):
            return False
        return True

    @staticmethod
    def create_open_shift_claim(shift_id: int, contractor_id: int) -> Tuple[bool, str]:
        """Create a pending claim requiring manager approval (does not assign shift yet)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM schedule_shifts WHERE id = %s LIMIT 1", (shift_id,))
            s = cur.fetchone()
            if not s:
                return False, "Shift not found."
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            has_assignments = bool(cur.fetchone())
            if has_assignments:
                cur.execute("SELECT COUNT(*) AS cnt FROM schedule_shift_assignments WHERE shift_id = %s", (shift_id,))
                cnt = (cur.fetchone() or {}).get("cnt") or 0
                required = int(s.get("required_count") or 1)
                if cnt >= required:
                    return False, "Shift is no longer open."
            else:
                if s.get("contractor_id") is not None:
                    return False, "Shift is no longer open."
            if (s.get("status") or "").lower() != "published":
                return False, "Only published open shifts can be claimed."
            wd = s.get("work_date")
            start = s.get("scheduled_start")
            end = s.get("scheduled_end")
            if not wd or not start or not end:
                return False, "Shift has invalid time/date."
            # Overlap check (via assignments if available)
            if has_assignments:
                cur.execute("""
                    SELECT 1 FROM schedule_shifts s
                    JOIN schedule_shift_assignments a ON a.shift_id = s.id AND a.contractor_id = %s
                    WHERE s.work_date = %s AND COALESCE(s.status,'') <> 'cancelled'
                    AND NOT (s.scheduled_end <= %s OR s.scheduled_start >= %s)
                    LIMIT 1
                """, (contractor_id, wd, start, end))
            else:
                cur.execute("""
                    SELECT 1
                    FROM schedule_shifts
                    WHERE contractor_id = %s
                      AND work_date = %s
                      AND COALESCE(status,'') <> 'cancelled'
                      AND NOT (scheduled_end <= %s OR scheduled_start >= %s)
                    LIMIT 1
                """, (contractor_id, wd, start, end))
            if cur.fetchone():
                return False, "You already have a shift that overlaps this time."
            # Skill/qualification eligibility (only if requirements configured for job type)
            req = ScheduleService.get_job_type_requirements(int(s.get("job_type_id") or 0))
            if req.get("skills") or req.get("qualifications"):
                prof_skills, prof_quals = (
                    ScheduleService._get_contractor_profile_skill_qual_sets(
                        cur, contractor_id
                    )
                )
                missing_skills = []
                for r in req.get("skills", []):
                    tok = str(r or "").strip().lower()
                    if tok and tok not in prof_skills:
                        missing_skills.append(str(r).strip())
                missing_quals = []
                for r in req.get("qualifications", []):
                    tok = str(r or "").strip().lower()
                    if tok and tok not in prof_quals:
                        missing_quals.append(str(r).strip())
                if missing_skills or missing_quals:
                    parts = []
                    if missing_skills:
                        parts.append("skills: " + ", ".join(missing_skills))
                    if missing_quals:
                        parts.append("quals: " + ", ".join(missing_quals))
                    return False, "Not eligible for this job type (" + "; ".join(parts) + ")."
            # Availability check (only if contractor configured availability)
            try:
                if ScheduleService._contractor_has_any_availability(cur, contractor_id):
                    if not ScheduleService._contractor_within_availability(cur, contractor_id, wd, start, end):
                        return False, "This shift is outside your availability window."
            except Exception:
                pass
            # Create claim if not already claimed
            cur.execute("""
                INSERT INTO schedule_open_shift_claims (shift_id, claimer_contractor_id, status)
                VALUES (%s, %s, 'claimed')
            """, (shift_id, contractor_id))
            conn.commit()
            return True, "Claim submitted for manager approval."
        except Exception:
            conn.rollback()
            return False, "Unable to submit claim."
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def resolve_open_shift_claim(claim_id: int, action: str, resolved_by_user_id: Optional[int] = None, admin_notes: Optional[str] = None) -> Tuple[bool, str]:
        """Approve or reject a claimed open shift."""
        act = (action or "").strip().lower()
        if act not in ("approve", "reject"):
            return False, "Invalid action."
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM schedule_open_shift_claims WHERE id = %s LIMIT 1", (claim_id,))
            c = cur.fetchone()
            if not c or c.get("status") != "claimed":
                return False, "Claim not found."
            shift_id = int(c["shift_id"])
            claimer_id = int(c["claimer_contractor_id"])
            if act == "approve":
                cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
                has_assignments = bool(cur.fetchone())
                if has_assignments:
                    cur.execute("SELECT required_count FROM schedule_shifts WHERE id = %s", (shift_id,))
                    r = cur.fetchone()
                    required = int((r or {}).get("required_count") or 1)
                    cur.execute("SELECT COUNT(*) AS cnt FROM schedule_shift_assignments WHERE shift_id = %s", (shift_id,))
                    cnt = (cur.fetchone() or {}).get("cnt") or 0
                    if cnt >= required:
                        conn.rollback()
                        return False, "Shift is no longer open."
                    cur.execute("INSERT IGNORE INTO schedule_shift_assignments (shift_id, contractor_id) VALUES (%s, %s)", (shift_id, claimer_id))
                else:
                    cur.execute("UPDATE schedule_shifts SET contractor_id = %s WHERE id = %s AND contractor_id IS NULL", (claimer_id, shift_id))
                    if cur.rowcount < 1:
                        conn.rollback()
                        return False, "Shift is no longer open."
                cur.execute("""
                    UPDATE schedule_open_shift_claims
                    SET status='approved', resolved_at=NOW(), resolved_by_user_id=%s, admin_notes=%s
                    WHERE id=%s
                """, (resolved_by_user_id, admin_notes, claim_id))
                conn.commit()
                return True, "Approved."
            # reject
            cur.execute("""
                UPDATE schedule_open_shift_claims
                SET status='rejected', resolved_at=NOW(), resolved_by_user_id=%s, admin_notes=%s
                WHERE id=%s AND status='claimed'
            """, (resolved_by_user_id, admin_notes, claim_id))
            conn.commit()
            return (cur.rowcount > 0), ("Rejected." if cur.rowcount > 0 else "Claim not found.")
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def claim_open_shift(shift_id: int, contractor_id: int) -> Tuple[bool, str]:
        """
        Claim an open shift for this contractor.
        - Only published open shifts can be claimed
        - Prevent overlap with existing (non-cancelled) shifts for this contractor
        - If contractor has availability configured, require claim inside an availability window
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM schedule_shifts WHERE id = %s LIMIT 1", (shift_id,))
            s = cur.fetchone()
            if not s:
                return False, "Shift not found."
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            has_assignments = bool(cur.fetchone())
            if has_assignments:
                cur.execute("SELECT COUNT(*) AS cnt FROM schedule_shift_assignments WHERE shift_id = %s", (shift_id,))
                cnt = (cur.fetchone() or {}).get("cnt") or 0
                required = int(s.get("required_count") or 1)
                if cnt >= required:
                    return False, "Shift is no longer open."
            else:
                if s.get("contractor_id") is not None:
                    return False, "Shift is no longer open."
            if (s.get("status") or "").lower() != "published":
                return False, "Only published open shifts can be claimed."
            wd = s.get("work_date")
            start = s.get("scheduled_start")
            end = s.get("scheduled_end")
            if not wd or not start or not end:
                return False, "Shift has invalid time/date."
            # Overlap check (same day; use assignments if available)
            if has_assignments:
                cur.execute("""
                    SELECT 1 FROM schedule_shifts s
                    JOIN schedule_shift_assignments a ON a.shift_id = s.id AND a.contractor_id = %s
                    WHERE s.work_date = %s AND COALESCE(s.status,'') <> 'cancelled'
                    AND NOT (s.scheduled_end <= %s OR s.scheduled_start >= %s)
                    LIMIT 1
                """, (contractor_id, wd, start, end))
            else:
                cur.execute("""
                    SELECT 1
                    FROM schedule_shifts
                    WHERE contractor_id = %s
                      AND work_date = %s
                      AND COALESCE(status,'') <> 'cancelled'
                      AND NOT (scheduled_end <= %s OR scheduled_start >= %s)
                LIMIT 1
                """, (contractor_id, wd, start, end))
            if cur.fetchone():
                return False, "You already have a shift that overlaps this time."
            # Skill/qualification eligibility (only if requirements configured for job type)
            req = ScheduleService.get_job_type_requirements(int(s.get("job_type_id") or 0))
            if req.get("skills") or req.get("qualifications"):
                prof_skills, prof_quals = (
                    ScheduleService._get_contractor_profile_skill_qual_sets(
                        cur, contractor_id
                    )
                )
                missing_skills = []
                for r in req.get("skills", []):
                    tok = str(r or "").strip().lower()
                    if tok and tok not in prof_skills:
                        missing_skills.append(str(r).strip())
                missing_quals = []
                for r in req.get("qualifications", []):
                    tok = str(r or "").strip().lower()
                    if tok and tok not in prof_quals:
                        missing_quals.append(str(r).strip())
                if missing_skills or missing_quals:
                    parts = []
                    if missing_skills:
                        parts.append("skills: " + ", ".join(missing_skills))
                    if missing_quals:
                        parts.append("quals: " + ", ".join(missing_quals))
                    return False, "Not eligible for this job type (" + "; ".join(parts) + ")."
            # Availability check (only if contractor configured availability)
            try:
                if ScheduleService._contractor_has_any_availability(cur, contractor_id):
                    if not ScheduleService._contractor_within_availability(cur, contractor_id, wd, start, end):
                        return False, "This shift is outside your availability window."
            except Exception:
                pass
            # Atomic claim: add assignment only if still open
            if has_assignments:
                cur.execute("SELECT required_count FROM schedule_shifts WHERE id = %s", (shift_id,))
                r = cur.fetchone()
                required = int((r or {}).get("required_count") or 1)
                cur.execute("SELECT COUNT(*) AS cnt FROM schedule_shift_assignments WHERE shift_id = %s", (shift_id,))
                cnt = (cur.fetchone() or {}).get("cnt") or 0
                if cnt >= required:
                    return False, "Shift is no longer open."
                cur.execute("INSERT IGNORE INTO schedule_shift_assignments (shift_id, contractor_id) VALUES (%s, %s)", (shift_id, contractor_id))
                conn.commit()
                return (cur.rowcount > 0, "Shift claimed.") if cur.rowcount > 0 else (False, "Shift is no longer open.")
            cur.execute("""
                UPDATE schedule_shifts
                SET contractor_id = %s, updated_at = NOW()
                WHERE id = %s AND contractor_id IS NULL
            """, (contractor_id, shift_id))
            conn.commit()
            if cur.rowcount > 0:
                return True, "Shift claimed."
            return False, "Shift is no longer open."
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_my_shifts_for_date(contractor_id: int, work_date: date) -> List[Dict[str, Any]]:
        return ScheduleService.list_shifts(
            contractor_id=contractor_id,
            work_date=work_date,
            status=None,
        )

    @staticmethod
    def get_shift(shift_id: int) -> Optional[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT s.*,
                       c.name AS client_name,
                       st.name AS site_name,
                       st.postcode AS site_postcode,
                       jt.name AS job_type_name,
                       u.name AS contractor_name,
                       u.initials AS contractor_initials
                FROM schedule_shifts s
                JOIN clients c ON c.id = s.client_id
                LEFT JOIN sites st ON st.id = s.site_id
                JOIN job_types jt ON jt.id = s.job_type_id
                LEFT JOIN tb_contractors u ON u.id = s.contractor_id
                WHERE s.id = %s
            """, (shift_id,))
            row = cur.fetchone()
            if not row:
                return None
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            if cur.fetchone():
                cur.execute("""
                    SELECT a.contractor_id, u.name AS contractor_name, u.initials AS contractor_initials
                    FROM schedule_shift_assignments a
                    JOIN tb_contractors u ON u.id = a.contractor_id
                    WHERE a.shift_id = %s ORDER BY a.id
                """, (shift_id,))
                assignments = cur.fetchall() or []
                row["assignments"] = assignments
                if assignments:
                    first = assignments[0]
                    row["contractor_id"] = first.get("contractor_id")
                    row["contractor_name"] = first.get("contractor_name")
                    row["contractor_initials"] = first.get("contractor_initials")
            else:
                row["assignments"] = []
            return row
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_shift_tasks(shift_id: int) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_tasks'")
            if not cur.fetchone():
                return []
            cur.execute(
                "SELECT * FROM schedule_shift_tasks WHERE shift_id = %s ORDER BY sort_order, id",
                (shift_id,),
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_shift_task(shift_id: int, title: str, sort_order: int = 0) -> Optional[int]:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_tasks'")
            if not cur.fetchone():
                return None
            cur.execute(
                "INSERT INTO schedule_shift_tasks (shift_id, title, sort_order) VALUES (%s, %s, %s)",
                (shift_id, (title or "").strip()[:255], sort_order),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_shift_task(task_id: int) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM schedule_shift_tasks WHERE id = %s", (task_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def set_shift_task_complete(task_id: int, contractor_id: int, complete: bool = True) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_tasks'")
            if not cur.fetchone():
                return False
            if complete:
                cur.execute(
                    "UPDATE schedule_shift_tasks SET completed_at = NOW(), completed_by_contractor_id = %s WHERE id = %s",
                    (contractor_id, task_id),
                )
            else:
                cur.execute(
                    "UPDATE schedule_shift_tasks SET completed_at = NULL, completed_by_contractor_id = NULL WHERE id = %s",
                    (task_id,),
                )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def log_shift_audit(
        shift_id: int,
        action: str,
        actor_user_id: Optional[int] = None,
        actor_username: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record an audit entry for a shift (create/update/cancelled)."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute("SHOW TABLES LIKE 'schedule_shift_audit'")
                if not cur.fetchone():
                    return
                details_json = json.dumps(details, default=str) if details else None
                cur.execute(
                    """
                    INSERT INTO schedule_shift_audit (shift_id, action, actor_user_id, actor_username, details_json)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (shift_id, action[:50], actor_user_id, (actor_username or "")[:150], details_json),
                )
                conn.commit()
            finally:
                cur.close()
                conn.close()
        except Exception:
            pass

    @staticmethod
    def list_shift_audit(shift_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        """Return audit entries for a shift, newest first."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, shift_id, action, actor_user_id, actor_username, details_json, created_at "
                "FROM schedule_shift_audit WHERE shift_id = %s ORDER BY created_at DESC LIMIT %s",
                (shift_id, max(1, min(limit, 200))),
            )
            rows = cur.fetchall() or []
            for r in rows:
                if r.get("details_json"):
                    try:
                        r["details"] = json.loads(r["details_json"]) if isinstance(r["details_json"], str) else r["details_json"]
                    except Exception:
                        r["details"] = None
            return rows
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_shift_audit_recent(limit: int = 100, shift_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return recent audit entries (all shifts or for one shift)."""
        if shift_id is not None:
            return ScheduleService.list_shift_audit(shift_id, limit=limit)
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, shift_id, action, actor_user_id, actor_username, details_json, created_at "
                "FROM schedule_shift_audit ORDER BY created_at DESC LIMIT %s",
                (max(1, min(limit, 500)),),
            )
            rows = cur.fetchall() or []
            for r in rows:
                if r.get("details_json"):
                    try:
                        r["details"] = json.loads(r["details_json"]) if isinstance(r["details_json"], str) else r["details_json"]
                    except Exception:
                        r["details"] = None
            return rows
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_shift(
        data: Dict[str, Any],
        actor_user_id: Optional[int] = None,
        actor_username: Optional[str] = None,
    ) -> int:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            contractor_ids = data.get("contractor_ids")
            if contractor_ids is None and "contractor_id" in data:
                contractor_ids = [data["contractor_id"]] if data["contractor_id"] is not None else []
            first_contractor_id = contractor_ids[0] if contractor_ids else data.get("contractor_id")
            required_count = int(data.get("required_count") or 1)
            cur.execute("SHOW COLUMNS FROM schedule_shifts LIKE 'required_count'")
            has_required = bool(cur.fetchone())
            if has_required:
                cur.execute("""
                    INSERT INTO schedule_shifts
                    (contractor_id, client_id, site_id, job_type_id, work_date,
                     scheduled_start, scheduled_end, break_mins, notes, status, source,
                     external_id, runsheet_id, runsheet_assignment_id, labour_cost, recurrence_id, required_count)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    first_contractor_id,
                    data["client_id"],
                    data.get("site_id"),
                    data["job_type_id"],
                    data["work_date"],
                    data["scheduled_start"],
                    data["scheduled_end"],
                    int(data.get("break_mins") or 0),
                    data.get("notes"),
                    data.get("status") or "draft",
                    data.get("source") or "manual",
                    data.get("external_id"),
                    data.get("runsheet_id"),
                    data.get("runsheet_assignment_id"),
                    _safe_float(data.get("labour_cost")),
                    data.get("recurrence_id"),
                    required_count,
                ))
            else:
                cur.execute("""
                    INSERT INTO schedule_shifts
                    (contractor_id, client_id, site_id, job_type_id, work_date,
                     scheduled_start, scheduled_end, break_mins, notes, status, source,
                     external_id, runsheet_id, runsheet_assignment_id, labour_cost, recurrence_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    first_contractor_id,
                    data["client_id"],
                    data.get("site_id"),
                    data["job_type_id"],
                    data["work_date"],
                    data["scheduled_start"],
                    data["scheduled_end"],
                    int(data.get("break_mins") or 0),
                    data.get("notes"),
                    data.get("status") or "draft",
                    data.get("source") or "manual",
                    data.get("external_id"),
                    data.get("runsheet_id"),
                    data.get("runsheet_assignment_id"),
                    _safe_float(data.get("labour_cost")),
                    data.get("recurrence_id"),
                ))
            conn.commit()
            sid = cur.lastrowid
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            if cur.fetchone():
                for cid in (contractor_ids or []):
                    if cid is not None:
                        cur.execute(
                            "INSERT IGNORE INTO schedule_shift_assignments (shift_id, contractor_id) VALUES (%s, %s)",
                            (sid, int(cid)),
                        )
                conn.commit()
            if actor_user_id is not None or actor_username:
                ScheduleService.log_shift_audit(sid, "created", actor_user_id, actor_username, details=data)
            return sid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_shift(
        shift_id: int,
        data: Dict[str, Any],
        actor_user_id: Optional[int] = None,
        actor_username: Optional[str] = None,
    ) -> None:
        allowed = {
            "contractor_id", "client_id", "site_id", "job_type_id", "work_date",
            "scheduled_start", "scheduled_end", "actual_start", "actual_end",
            "break_mins", "notes", "status", "labour_cost", "required_count",
        }
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            updates = []
            params: List[Any] = []
            for k in allowed:
                if k in data:
                    updates.append(f"{k} = %s")
                    params.append(data[k])
            if not updates:
                return
            params.append(shift_id)
            cur.execute(
                f"UPDATE schedule_shifts SET {', '.join(updates)} WHERE id = %s",
                params,
            )
            conn.commit()
            if actor_user_id is not None or actor_username:
                action = "cancelled" if data.get("status") == "cancelled" else "updated"
                ScheduleService.log_shift_audit(shift_id, action, actor_user_id, actor_username, details=data)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def find_overlapping_shift_ids(shifts: List[Dict[str, Any]]) -> set:
        """Return set of shift ids that overlap (same contractor, same day, overlapping times). Cancelled excluded."""
        out: set = set()
        shift_ids = [s["id"] for s in shifts]
        assignments_map = ScheduleService.get_assignments_for_shifts(shift_ids) if shift_ids else {}
        by_key: Dict[tuple, List[Dict]] = {}
        for s in shifts:
            if s.get("status") == "cancelled":
                continue
            wd = s.get("work_date")
            if wd is None:
                continue
            assignees = assignments_map.get(s["id"]) or []
            if not assignees:
                cid = s.get("contractor_id")
                if cid is not None:
                    assignees = [{"contractor_id": cid}]
            for a in assignees:
                cid = a.get("contractor_id")
                if cid is None:
                    continue
                key = (cid, wd.isoformat() if hasattr(wd, "isoformat") else str(wd))
                by_key.setdefault(key, []).append(s)
        for group in by_key.values():
            if len(group) < 2:
                continue
            for i, a in enumerate(group):
                start_a = a.get("scheduled_start")
                end_a = a.get("scheduled_end")
                if start_a is None or end_a is None:
                    continue
                for b in group[i + 1 :]:
                    start_b = b.get("scheduled_start")
                    end_b = b.get("scheduled_end")
                    if start_b is None or end_b is None:
                        continue
                    if not (end_a <= start_b or end_b <= start_a):
                        out.add(a.get("id"))
                        out.add(b.get("id"))
        return out

    @staticmethod
    def record_actual_times(shift_id: int, actual_start: Optional[time] = None, actual_end: Optional[time] = None, notes: Optional[str] = None) -> None:
        updates: Dict[str, Any] = {}
        if actual_start is not None:
            updates["actual_start"] = actual_start
        if actual_end is not None:
            updates["actual_end"] = actual_end
        if notes is not None:
            updates["notes"] = notes
        if updates:
            updates["status"] = "completed" if actual_end else "in_progress"
            ScheduleService.update_shift(shift_id, updates)

    @staticmethod
    def get_clock_location(site_id: int) -> Optional[Dict[str, Any]]:
        """Get geofence for a site (lat, lng, radius_meters). None if not set."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_clock_locations'")
            if not cur.fetchone():
                return None
            cur.execute(
                "SELECT * FROM schedule_clock_locations WHERE site_id = %s LIMIT 1",
                (site_id,),
            )
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def set_clock_location(site_id: int, latitude: float, longitude: float, radius_meters: int = 100, name: Optional[str] = None) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_clock_locations'")
            if not cur.fetchone():
                return False
            cur.execute(
                """
                INSERT INTO schedule_clock_locations (site_id, latitude, longitude, radius_meters, name)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE latitude = VALUES(latitude), longitude = VALUES(longitude),
                radius_meters = VALUES(radius_meters), name = VALUES(name)
                """,
                (site_id, latitude, longitude, max(20, min(2000, radius_meters)), (name or "")[:100]),
            )
            conn.commit()
            return True
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_clock_locations() -> List[Dict[str, Any]]:
        """Sites with their clock (geofence) location if set."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_clock_locations'")
            if not cur.fetchone():
                return []
            cur.execute("""
                SELECT cl.*, s.name AS site_name, s.client_id
                FROM schedule_clock_locations cl
                JOIN sites s ON s.id = cl.site_id
                ORDER BY s.name
            """)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _distance_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Approximate distance in meters (Haversine)."""
        import math
        R = 6371000  # Earth radius in meters
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlam = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    @staticmethod
    def validate_clock_location(site_id: Optional[int], lat: Optional[float], lng: Optional[float]) -> Tuple[bool, str]:
        """If site has a clock location, lat/lng required and must be within radius. Returns (ok, error_message)."""
        if not site_id:
            return True, ""
        loc = ScheduleService.get_clock_location(site_id)
        if not loc:
            return True, ""
        if lat is None or lng is None:
            return False, "This site requires clock-in from location. Enable GPS and try again."
        try:
            dist = ScheduleService._distance_meters(
                float(loc["latitude"]), float(loc["longitude"]),
                float(lat), float(lng),
            )
            radius = int(loc.get("radius_meters") or 100)
            if dist > radius:
                return False, f"You must be within {radius}m of the site to clock in."
            return True, ""
        except (TypeError, ValueError):
            return False, "Invalid location."

    @staticmethod
    def clock_in_shift(
        shift_id: int,
        contractor_id: int,
        at_time: Optional[time] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
    ) -> Tuple[bool, str]:
        """Set actual_start to at_time (or now). Validates shift, ownership, date, geofence. Returns (success, message)."""
        shift = ScheduleService.get_shift(shift_id)
        if not shift:
            return False, "Shift not found."
        assignees = shift.get("assignments") or []
        if not assignees and shift.get("contractor_id") == contractor_id:
            pass
        elif not any(a.get("contractor_id") == contractor_id for a in assignees):
            return False, "Not your shift."
        if shift.get("status") == "cancelled":
            return False, "Shift is cancelled."
        # Only allow clock-in against published shifts (imported Sling shifts are published).
        if (shift.get("status") or "").lower() != "published":
            return False, "You can only clock in to published shifts."
        wd = shift.get("work_date")
        if wd and hasattr(wd, "isoformat") and wd.isoformat() != date.today().isoformat():
            return False, "You can only clock in on the shift date."
        if shift.get("actual_start"):
            return False, "Already clocked in."
        ok, err = ScheduleService.validate_clock_location(shift.get("site_id"), lat, lng)
        if not ok:
            return False, err
        from datetime import datetime
        t = at_time or datetime.now().time()
        ScheduleService.record_actual_times(shift_id, actual_start=t)
        return True, "Clocked in."

    @staticmethod
    def clock_out_shift(
        shift_id: int,
        contractor_id: int,
        at_time: Optional[time] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
    ) -> Tuple[bool, str]:
        """Set actual_end to at_time (or now). Validates shift, ownership, already clocked in. Returns (success, message)."""
        shift = ScheduleService.get_shift(shift_id)
        if not shift:
            return False, "Shift not found."
        assignees = shift.get("assignments") or []
        if not assignees and shift.get("contractor_id") == contractor_id:
            pass
        elif not any(a.get("contractor_id") == contractor_id for a in assignees):
            return False, "Not your shift."
        if not shift.get("actual_start"):
            return False, "Clock in first."
        # Clock-out is allowed once clock-in has occurred (in_progress or published).
        if (shift.get("status") or "").lower() not in ("published", "in_progress"):
            return False, "You can only clock out of active shifts."
        if shift.get("actual_end"):
            return False, "Already clocked out."
        ok, err = ScheduleService.validate_clock_location(shift.get("site_id"), lat, lng)
        if not ok:
            return False, err
        from datetime import datetime
        t = at_time or datetime.now().time()
        ScheduleService.record_actual_times(shift_id, actual_end=t)
        return True, "Clocked out."

    @staticmethod
    def list_timesheet_shifts(
        date_from: date,
        date_to: date,
        contractor_id: Optional[int] = None,
        client_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Shifts with actual times for export (non-cancelled, with contractor)."""
        shifts = ScheduleService.list_shifts(
            date_from=date_from,
            date_to=date_to,
            contractor_id=contractor_id,
            client_id=client_id,
        )
        return [s for s in shifts if s.get("status") != "cancelled" and s.get("contractor_id")]

    @staticmethod
    def list_availability(contractor_id: int) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT * FROM schedule_availability
                WHERE contractor_id = %s
                ORDER BY day_of_week, start_time
            """, (contractor_id,))
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_time_off(
        contractor_id: Optional[int] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        status: Optional[str] = None,
        type_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = ["1=1"]
            params: List[Any] = []
            if contractor_id is not None:
                where.append("t.contractor_id = %s")
                params.append(contractor_id)
            if date_from is not None:
                where.append("t.end_date >= %s")
                params.append(date_from)
            if date_to is not None:
                where.append("t.start_date <= %s")
                params.append(date_to)
            if status:
                where.append("t.status = %s")
                params.append(status)
            if type_filter:
                where.append("t.type = %s")
                params.append(type_filter)
            cur.execute(f"""
                SELECT t.*, u.name AS contractor_name, u.email AS contractor_email
                FROM schedule_time_off t
                JOIN tb_contractors u ON u.id = t.contractor_id
                WHERE {" AND ".join(where)}
                ORDER BY t.start_date DESC
            """, params)
            return cur.fetchall() or []
        except Exception as e:
            logger.warning("list_time_off failed (run scheduling install/upgrade if needed): %s", e)
            return []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_time_off(time_off_id: int) -> Optional[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT t.*, u.name AS contractor_name, u.email AS contractor_email
                FROM schedule_time_off t
                JOIN tb_contractors u ON u.id = t.contractor_id
                WHERE t.id = %s
            """, (time_off_id,))
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_time_off(
        contractor_id: int,
        start_date: date,
        end_date: date,
        reason: Optional[str] = None,
        type: str = "annual",
        start_time: Optional[time] = None,
        end_time: Optional[time] = None,
    ) -> int:
        """Create time off request. If start_time/end_time are set, off only during that window on each day in [start_date,end_date]; else whole day(s)."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO schedule_time_off (contractor_id, type, start_date, end_date, start_time, end_time, reason, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'requested')
            """, (contractor_id, type, start_date, end_date, start_time, end_time, reason or None))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_time_off_on_behalf(
        contractor_id: int,
        start_date: date,
        end_date: date,
        type: str = "annual",
        reason: Optional[str] = None,
        status: str = "approved",
        start_time: Optional[time] = None,
        end_time: Optional[time] = None,
    ) -> int:
        """Admin creates time off (e.g. recorded sickness). Default status approved."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO schedule_time_off (contractor_id, type, start_date, end_date, start_time, end_time, reason, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (contractor_id, type, start_date, end_date, start_time, end_time, reason or None, status))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def approve_time_off(time_off_id: int, reviewed_by_user_id: Optional[int] = None, admin_notes: Optional[str] = None) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE schedule_time_off
                SET status = 'approved', reviewed_at = NOW(), reviewed_by_user_id = %s, admin_notes = %s
                WHERE id = %s AND status = 'requested'
            """, (reviewed_by_user_id, admin_notes or None, time_off_id))
            conn.commit()
            return cur.rowcount > 0
        except Exception:
            return False
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def reject_time_off(time_off_id: int, reviewed_by_user_id: Optional[int] = None, admin_notes: Optional[str] = None) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE schedule_time_off
                SET status = 'rejected', reviewed_at = NOW(), reviewed_by_user_id = %s, admin_notes = %s
                WHERE id = %s AND status = 'requested'
            """, (reviewed_by_user_id, admin_notes or None, time_off_id))
            conn.commit()
            return cur.rowcount > 0
        except Exception:
            return False
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def cancel_time_off(time_off_id: int, contractor_id: int) -> bool:
        """Contractor cancels own pending request. Returns True if cancelled."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE schedule_time_off SET status = 'cancelled' WHERE id = %s AND contractor_id = %s AND status = 'requested'",
                (time_off_id, contractor_id),
            )
            conn.commit()
            return cur.rowcount > 0
        except Exception:
            return False
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_clients_and_sites() -> tuple:
        """Return (clients, sites) for dropdowns. Uses time_billing clients/sites."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT id, name FROM clients WHERE active = 1 ORDER BY name")
            clients = cur.fetchall() or []
            cur.execute("SELECT id, client_id, name FROM sites WHERE active = 1 ORDER BY name")
            sites = cur.fetchall() or []
            return clients, sites
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_client(name: str) -> Optional[int]:
        """Create a client (time_billing clients table). Returns new client id or None."""
        if not (name or "").strip():
            return None
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'clients'")
            if not cur.fetchone():
                return None
            cur.execute(
                "INSERT INTO clients (name, active) VALUES (%s, 1)",
                ((name or "").strip()[:255],),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_site(client_id: int, name: str) -> Optional[int]:
        """Create a site (time_billing sites table). Returns new site id or None."""
        if not name or not client_id:
            return None
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'sites'")
            if not cur.fetchone():
                return None
            cur.execute(
                "INSERT INTO sites (client_id, name, active) VALUES (%s, %s, 1)",
                (client_id, (name or "").strip()[:255]),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_job_types() -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT id, name, code FROM job_types WHERE active = 1 ORDER BY name")
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_contractors() -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT id, name, initials, email FROM tb_contractors WHERE status = 'active' ORDER BY name")
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_contractors_for_team_directory() -> List[Dict[str, Any]]:
        """Active contractors for peer team list — no email (privacy)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, name, initials FROM tb_contractors WHERE status = 'active' ORDER BY name"
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_active_contractor_team_ids() -> List[int]:
        """IDs of active contractors allowed in team directory / peer schedule views."""
        return [int(c["id"]) for c in ScheduleService.list_contractors_for_team_directory() if c.get("id") is not None]

    @staticmethod
    def contractor_privacy_absence_dates(contractor_id: int, week_start: date) -> Set[str]:
        """
        Dates (ISO strings) in Mon–Sun week where the contractor has approved time off
        or recurring unavailability. Used on peer schedule views: show OFF only (no type/reason/times).
        """
        week_end = week_start + timedelta(days=6)
        off: Set[str] = set()

        def _as_date(v):
            if v is None:
                return None
            if isinstance(v, datetime):
                return v.date()
            if isinstance(v, date):
                return v
            s = str(v)[:10]
            try:
                return date.fromisoformat(s)
            except ValueError:
                return None

        rows = ScheduleService.list_time_off(
            contractor_id=contractor_id,
            date_from=week_start,
            date_to=week_end,
            status="approved",
        )
        for row in rows:
            sd = _as_date(row.get("start_date"))
            ed = _as_date(row.get("end_date"))
            if not sd or not ed:
                continue
            d = sd
            while d <= ed:
                if week_start <= d <= week_end:
                    off.add(d.isoformat())
                d += timedelta(days=1)

        umap = ScheduleService.get_unavailability_for_week([contractor_id], week_start)
        for u in umap.get(contractor_id) or []:
            iso = u.get("date")
            if iso:
                off.add(str(iso)[:10])

        return off

    @staticmethod
    def get_contractor_portal_summary(contractor_id: int) -> Dict[str, Any]:
        """Summary for employee portal dashboard: shifts_today, shifts_this_week, pending_shift_tasks."""
        today = date.today()
        week_monday = today - timedelta(days=today.weekday())
        week_end = week_monday + timedelta(days=6)
        shifts_today = ScheduleService.list_shifts(
            contractor_id=contractor_id,
            work_date=today,
        )
        shifts_today = [s for s in shifts_today if s.get("status") != "cancelled"]
        shifts_week = ScheduleService.list_shifts(
            date_from=week_monday,
            date_to=week_end,
            contractor_id=contractor_id,
        )
        shifts_week = [s for s in shifts_week if s.get("status") != "cancelled"]
        pending_shift_tasks = 0
        try:
            for s in shifts_week:
                tasks = ScheduleService.list_shift_tasks(s["id"])
                pending_shift_tasks += sum(1 for t in tasks if not t.get("completed_at"))
        except Exception:
            pass
        return {
            "shifts_today": len(shifts_today),
            "shifts_this_week": len(shifts_week),
            "pending_shift_tasks": pending_shift_tasks,
        }

    @staticmethod
    def get_contractor_ids_with_shifts_in_week(week_monday: date) -> List[int]:
        week_end = week_monday + timedelta(days=6)
        shifts = ScheduleService.list_shifts(date_from=week_monday, date_to=week_end)
        shift_ids = [s["id"] for s in shifts if s.get("status") != "cancelled"]
        assignments_map = ScheduleService.get_assignments_for_shifts(shift_ids) if shift_ids else {}
        cids: List[int] = []
        for s in shifts:
            if s.get("status") == "cancelled":
                continue
            assignees = assignments_map.get(s["id"]) or []
            if not assignees and s.get("contractor_id"):
                cids.append(int(s["contractor_id"]))
            for a in assignees:
                if a.get("contractor_id"):
                    cids.append(int(a["contractor_id"]))
        return list(dict.fromkeys(cids))

    # ---------- Availability (contractor self-service + admin) ----------

    @staticmethod
    def get_availability(avail_id: int, contractor_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            if contractor_id is not None:
                cur.execute("SELECT * FROM schedule_availability WHERE id = %s AND contractor_id = %s", (avail_id, contractor_id))
            else:
                cur.execute("SELECT * FROM schedule_availability WHERE id = %s", (avail_id,))
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_availability(
        contractor_id: int,
        day_of_week: int,
        start_time: time,
        end_time: time,
        effective_from: date,
        effective_to: Optional[date] = None,
    ) -> int:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO schedule_availability (contractor_id, day_of_week, start_time, end_time, effective_from, effective_to)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (contractor_id, day_of_week, start_time, end_time, effective_from, effective_to))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_availability(
        avail_id: int,
        contractor_id: int,
        day_of_week: Optional[int] = None,
        start_time: Optional[time] = None,
        end_time: Optional[time] = None,
        effective_from: Optional[date] = None,
        effective_to: Optional[date] = None,
    ) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            updates = []
            params: List[Any] = []
            for k, v in [
                ("day_of_week", day_of_week),
                ("start_time", start_time),
                ("end_time", end_time),
                ("effective_from", effective_from),
                ("effective_to", effective_to),
            ]:
                if v is not None:
                    updates.append(f"{k} = %s")
                    params.append(v)
            if not updates:
                return True
            params.extend([avail_id, contractor_id])
            cur.execute(
                f"UPDATE schedule_availability SET {', '.join(updates)} WHERE id = %s AND contractor_id = %s",
                params,
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_availability(avail_id: int, contractor_id: int) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM schedule_availability WHERE id = %s AND contractor_id = %s", (avail_id, contractor_id))
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    # ---------- Unavailability (when I'm not available) ----------

    @staticmethod
    def list_unavailability(contractor_id: int) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_unavailability'")
            if not cur.fetchone():
                return []
            cur.execute("""
                SELECT * FROM schedule_unavailability
                WHERE contractor_id = %s
                ORDER BY day_of_week, start_time
            """, (contractor_id,))
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_unavailability(una_id: int, contractor_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_unavailability'")
            if not cur.fetchone():
                return None
            if contractor_id is not None:
                cur.execute("SELECT * FROM schedule_unavailability WHERE id = %s AND contractor_id = %s", (una_id, contractor_id))
            else:
                cur.execute("SELECT * FROM schedule_unavailability WHERE id = %s", (una_id,))
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_unavailability(
        contractor_id: int,
        day_of_week: int,
        start_time: time,
        end_time: time,
        effective_from: date,
        effective_to: Optional[date] = None,
    ) -> int:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO schedule_unavailability (contractor_id, day_of_week, start_time, end_time, effective_from, effective_to)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (contractor_id, day_of_week, start_time, end_time, effective_from, effective_to))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_unavailability(
        una_id: int,
        contractor_id: int,
        day_of_week: Optional[int] = None,
        start_time: Optional[time] = None,
        end_time: Optional[time] = None,
        effective_from: Optional[date] = None,
        effective_to: Optional[date] = None,
    ) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_unavailability'")
            if not cur.fetchone():
                return False
            updates = []
            params: List[Any] = []
            for k, v in [
                ("day_of_week", day_of_week),
                ("start_time", start_time),
                ("end_time", end_time),
                ("effective_from", effective_from),
                ("effective_to", effective_to),
            ]:
                if v is not None:
                    updates.append(f"{k} = %s")
                    params.append(v)
            if not updates:
                return True
            params.extend([una_id, contractor_id])
            cur.execute(
                f"UPDATE schedule_unavailability SET {', '.join(updates)} WHERE id = %s AND contractor_id = %s",
                params,
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_unavailability(una_id: int, contractor_id: int) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_unavailability'")
            if not cur.fetchone():
                return False
            cur.execute("DELETE FROM schedule_unavailability WHERE id = %s AND contractor_id = %s", (una_id, contractor_id))
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    # ---------- Availability mode (availability vs unavailability) ----------

    @staticmethod
    def get_availability_mode(contractor_id: int) -> str:
        """Return 'availability' or 'unavailability'. Default 'availability'."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_contractor_prefs'")
            if not cur.fetchone():
                return "availability"
            cur.execute(
                "SELECT pref_value FROM schedule_contractor_prefs WHERE contractor_id = %s AND pref_key = %s",
                (contractor_id, "availability_mode"),
            )
            row = cur.fetchone()
            if row and (row.get("pref_value") or "").strip().lower() == "unavailability":
                return "unavailability"
            return "availability"
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def set_availability_mode(contractor_id: int, mode: str) -> None:
        mode = (mode or "availability").strip().lower()
        if mode not in ("availability", "unavailability"):
            mode = "availability"
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_contractor_prefs'")
            if not cur.fetchone():
                return
            cur.execute("""
                INSERT INTO schedule_contractor_prefs (contractor_id, pref_key, pref_value)
                VALUES (%s, 'availability_mode', %s)
                ON DUPLICATE KEY UPDATE pref_value = VALUES(pref_value)
            """, (contractor_id, mode))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_unavailability_for_week(contractor_ids: List[int], week_start: date) -> Dict[int, List[Dict[str, Any]]]:
        """For each contractor, list unavailability windows that fall in the given week (Mon–Sun). Used by scheduler view."""
        out: Dict[int, List[Dict[str, Any]]] = {cid: [] for cid in contractor_ids if cid}
        if not out:
            return out
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_unavailability'")
            if not cur.fetchone():
                return out
            day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            for i in range(7):
                d = week_start + timedelta(days=i)
                dow = d.weekday()
                placeholders = ",".join(["%s"] * len(contractor_ids))
                cur.execute(f"""
                    SELECT id, contractor_id, day_of_week, start_time, end_time, effective_from, effective_to
                    FROM schedule_unavailability
                    WHERE contractor_id IN ({placeholders}) AND day_of_week = %s
                """, (*contractor_ids, dow))
                for r in cur.fetchall() or []:
                    cid = r.get("contractor_id")
                    if cid not in out:
                        continue
                    ef, et = r.get("effective_from"), r.get("effective_to")
                    if ef and d < ef or (et and d > et):
                        continue
                    st = r.get("start_time")
                    en = r.get("end_time")
                    st_str = st.strftime("%H:%M") if hasattr(st, "strftime") else str(st or "")[:5]
                    en_str = en.strftime("%H:%M") if hasattr(en, "strftime") else str(en or "")[:5]
                    day_name = day_names[dow] if 0 <= dow <= 6 else ""
                    label = f"{day_name} {st_str}–{en_str}"
                    out[cid].append({
                        "date": d.isoformat(),
                        "start_time": st,
                        "end_time": en,
                        "start_time_display": st_str,
                        "end_time_display": en_str,
                        "day_name": day_name,
                        "label": label,
                    })
        finally:
            cur.close()
            conn.close()
        return out

    # ---------- Smart scheduling: conflicts, suggest staff, copy week ----------

    @staticmethod
    def check_shift_conflicts(
        contractor_id: int,
        work_date: date,
        scheduled_start: time,
        scheduled_end: time,
        exclude_shift_id: Optional[int] = None,
    ) -> List[str]:
        """Return list of conflict messages (double-book, time off)."""
        conflicts: List[str] = []
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            use_assignments = bool(cur.fetchone())
            if use_assignments:
                cur.execute("""
                    SELECT s.id, s.work_date, s.scheduled_start, s.scheduled_end, c.name AS client_name
                    FROM schedule_shifts s
                    JOIN schedule_shift_assignments a ON a.shift_id = s.id AND a.contractor_id = %s
                    JOIN clients c ON c.id = s.client_id
                    WHERE s.work_date = %s AND s.status NOT IN ('cancelled')
                    AND s.scheduled_start < %s AND s.scheduled_end > %s
                """, (contractor_id, work_date, scheduled_end, scheduled_start))
            else:
                cur.execute("""
                    SELECT s.id, s.work_date, s.scheduled_start, s.scheduled_end, c.name AS client_name
                    FROM schedule_shifts s
                    JOIN clients c ON c.id = s.client_id
                    WHERE s.contractor_id = %s AND s.work_date = %s AND s.status NOT IN ('cancelled')
                    AND s.scheduled_start < %s AND s.scheduled_end > %s
                """, (contractor_id, work_date, scheduled_end, scheduled_start))
            rows = cur.fetchall() or []
            for r in rows:
                if exclude_shift_id and r.get("id") == exclude_shift_id:
                    continue
                conflicts.append(f"Overlaps existing shift at {r.get('client_name', '—')} ({r.get('scheduled_start')}–{r.get('scheduled_end')})")
            # Time off on this day (whole day or overlapping window)
            cur.execute("""
                SELECT type, start_date, end_date, start_time, end_time FROM schedule_time_off
                WHERE contractor_id = %s AND status IN ('requested', 'approved')
                AND start_date <= %s AND end_date >= %s
            """, (contractor_id, work_date, work_date))
            to_rows = cur.fetchall() or []
            for r in to_rows:
                st, et = r.get("start_time"), r.get("end_time")
                if st is None and et is None:
                    conflicts.append(f"Time off ({r.get('type', '—')}) on this date")
                else:
                    # Partial day: conflict only if shift overlaps the window
                    window_start = st if st is not None else time(0, 0)
                    window_end = et if et is not None else time(23, 59, 59)
                    if not (scheduled_end <= window_start or scheduled_start >= window_end):
                        conflicts.append(f"Time off ({r.get('type', '—')}) {window_start}–{window_end} on this date")
            return conflicts
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def suggest_available_contractors(
        work_date: date,
        start_time: time,
        end_time: time,
        client_id: Optional[int] = None,
        job_type_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return contractors who have no shift and no time off on work_date (suitable for assigning)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            use_assignments = bool(cur.fetchone())
            cur.execute("SELECT id, name, initials, email FROM tb_contractors WHERE status = 'active' ORDER BY name")
            all_c = cur.fetchall() or []
            available = []
            for c in all_c:
                cid = c["id"]
                if use_assignments:
                    cur.execute("""
                        SELECT 1 FROM schedule_shifts s
                        JOIN schedule_shift_assignments a ON a.shift_id = s.id AND a.contractor_id = %s
                        WHERE s.work_date = %s AND s.status NOT IN ('cancelled')
                        AND ((s.scheduled_start < %s AND s.scheduled_end > %s) OR (s.scheduled_start < %s AND s.scheduled_end > %s)
                             OR (s.scheduled_start >= %s AND s.scheduled_end <= %s))
                    """, (cid, work_date, end_time, start_time, end_time, start_time, start_time, end_time))
                else:
                    cur.execute("""
                        SELECT 1 FROM schedule_shifts
                        WHERE contractor_id = %s AND work_date = %s AND status NOT IN ('cancelled')
                        AND ((scheduled_start < %s AND scheduled_end > %s) OR (scheduled_start < %s AND scheduled_end > %s)
                             OR (scheduled_start >= %s AND scheduled_end <= %s))
                    """, (cid, work_date, end_time, start_time, end_time, start_time, start_time, end_time))
                if cur.fetchone():
                    continue
                cur.execute("""
                    SELECT start_time, end_time FROM schedule_time_off
                    WHERE contractor_id = %s AND status IN ('requested', 'approved')
                    AND start_date <= %s AND end_date >= %s
                """, (cid, work_date, work_date))
                to_rows = cur.fetchall() or []
                blocked = False
                for r in to_rows:
                    st, et = r.get("start_time"), r.get("end_time")
                    if st is None and et is None:
                        blocked = True
                        break
                    window_start = st if st is not None else time(0, 0)
                    window_end = et if et is not None else time(23, 59, 59)
                    if not (end_time <= window_start or start_time >= window_end):
                        blocked = True
                        break
                if blocked:
                    continue
                available.append(c)
            return available
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def copy_week_shifts(from_monday: date, to_monday: date) -> int:
        """Copy all shifts from one week (Monday) to another. Creates draft shifts. Returns count."""
        from_end = from_monday + timedelta(days=6)
        shifts = ScheduleService.list_shifts(date_from=from_monday, date_to=from_end)
        if not shifts:
            return 0
        delta_days = (to_monday - from_monday).days
        count = 0
        shift_ids = [s["id"] for s in shifts if s.get("status") != "cancelled"]
        assignments_map = ScheduleService.get_assignments_for_shifts(shift_ids) if shift_ids else {}
        for s in shifts:
            if s.get("status") == "cancelled":
                continue
            new_date = s.get("work_date")
            if hasattr(new_date, "weekday"):
                new_date = new_date + timedelta(days=delta_days)
            else:
                continue
            assignees = assignments_map.get(s["id"]) or []
            contractor_ids = [a["contractor_id"] for a in assignees if a.get("contractor_id")]
            if not contractor_ids and s.get("contractor_id"):
                contractor_ids = [s["contractor_id"]]
            data = {
                "client_id": s["client_id"],
                "site_id": s.get("site_id"),
                "job_type_id": s["job_type_id"],
                "work_date": new_date,
                "scheduled_start": s["scheduled_start"],
                "scheduled_end": s["scheduled_end"],
                "break_mins": s.get("break_mins") or 0,
                "notes": s.get("notes"),
                "status": "draft",
                "source": "manual",
                "required_count": int(s.get("required_count") or 1),
                "contractor_ids": contractor_ids,
            }
            try:
                ScheduleService.create_shift(data)
                count += 1
            except Exception:
                pass
        return count

    # ---------- Templates ----------

    @staticmethod
    def list_templates() -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT t.*, c.name AS client_name, st.name AS site_name, jt.name AS job_type_name
                FROM schedule_templates t
                LEFT JOIN clients c ON c.id = t.client_id
                LEFT JOIN sites st ON st.id = t.site_id
                LEFT JOIN job_types jt ON jt.id = t.job_type_id
                WHERE t.active = 1
                ORDER BY t.name
            """)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_template(template_id: int) -> Optional[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT t.*, c.name AS client_name, st.name AS site_name, jt.name AS job_type_name
                FROM schedule_templates t
                LEFT JOIN clients c ON c.id = t.client_id
                LEFT JOIN sites st ON st.id = t.site_id
                LEFT JOIN job_types jt ON jt.id = t.job_type_id
                WHERE t.id = %s
            """, (template_id,))
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_template(name: str, client_id: Optional[int] = None, site_id: Optional[int] = None, job_type_id: Optional[int] = None) -> int:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO schedule_templates (name, client_id, site_id, job_type_id, active)
                VALUES (%s, %s, %s, %s, 1)
            """, (name, client_id, site_id, job_type_id))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_template(template_id: int, name: Optional[str] = None, client_id: Optional[int] = None, site_id: Optional[int] = None, job_type_id: Optional[int] = None) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            updates = []
            params: List[Any] = []
            for k, v in [("name", name), ("client_id", client_id), ("site_id", site_id), ("job_type_id", job_type_id)]:
                if v is not None:
                    updates.append(f"{k} = %s")
                    params.append(v)
            if not updates:
                return True
            params.append(template_id)
            cur.execute(f"UPDATE schedule_templates SET {', '.join(updates)} WHERE id = %s", params)
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_template_slots(template_id: int) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM schedule_template_slots WHERE template_id = %s ORDER BY day_of_week, start_time", (template_id,))
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_template_slot(template_id: int, day_of_week: int, start_time: time, end_time: time, position_label: Optional[str] = None) -> int:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO schedule_template_slots (template_id, day_of_week, start_time, end_time, position_label)
                VALUES (%s, %s, %s, %s, %s)
            """, (template_id, day_of_week, start_time, end_time, position_label))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_template_slot(slot_id: int) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM schedule_template_slots WHERE id = %s", (slot_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def clone_template(template_id: int) -> Optional[int]:
        """Duplicate a template and all its slots. Returns new template id or None."""
        t = ScheduleService.get_template(template_id)
        if not t:
            return None
        name = "Copy of " + (t.get("name") or "Template")
        new_id = ScheduleService.create_template(
            name,
            client_id=t.get("client_id"),
            site_id=t.get("site_id"),
            job_type_id=t.get("job_type_id"),
        )
        for slot in ScheduleService.list_template_slots(template_id):
            ScheduleService.add_template_slot(
                new_id,
                slot.get("day_of_week", 0),
                slot.get("start_time"),
                slot.get("end_time"),
                slot.get("position_label"),
            )
        return new_id

    @staticmethod
    def create_template_from_week(
        week_monday: date,
        contractor_id: Optional[int] = None,
        name: Optional[str] = None,
    ) -> Optional[int]:
        """Create a new template from shifts in the given week. Optionally filter by contractor. Returns new template id."""
        week_end = week_monday + timedelta(days=6)
        shifts = ScheduleService.list_shifts(
            date_from=week_monday,
            date_to=week_end,
            contractor_id=contractor_id,
        )
        shifts = [s for s in shifts if s.get("status") != "cancelled" and s.get("contractor_id")]
        if not shifts:
            return None
        first = shifts[0]
        template_name = name or ("Week of " + week_monday.isoformat())
        template_id = ScheduleService.create_template(
            template_name,
            client_id=first.get("client_id"),
            site_id=first.get("site_id"),
            job_type_id=first.get("job_type_id"),
        )
        for s in shifts:
            wd = s.get("work_date")
            if not wd or not hasattr(wd, "weekday"):
                continue
            dow = wd.weekday()
            ScheduleService.add_template_slot(
                template_id,
                dow,
                s.get("scheduled_start"),
                s.get("scheduled_end"),
                (s.get("notes") or "")[:100],
            )
        return template_id

    @staticmethod
    def apply_template_to_week(
        template_id: int,
        week_monday: date,
        contractor_id: int,
        slot_assignments: Optional[Dict[int, int]] = None,
    ) -> int:
        """Create draft shifts from template slots. Use slot_assignments (slot_id -> contractor_id) if provided; else one contractor for all. Returns count."""
        t = ScheduleService.get_template(template_id)
        if not t:
            return 0
        slots = ScheduleService.list_template_slots(template_id)
        if not slots:
            return 0
        count = 0
        for slot in slots:
            cid = (slot_assignments or {}).get(slot["id"], contractor_id)
            if not cid:
                continue
            dow = slot.get("day_of_week", 0)
            work_date = week_monday + timedelta(days=dow)
            data = {
                "contractor_id": cid,
                "client_id": t.get("client_id") or 0,
                "site_id": t.get("site_id"),
                "job_type_id": t.get("job_type_id") or 0,
                "work_date": work_date,
                "scheduled_start": slot.get("start_time"),
                "scheduled_end": slot.get("end_time"),
                "break_mins": 0,
                "notes": slot.get("position_label"),
                "status": "draft",
                "source": "manual",
            }
            if data["client_id"] and data["job_type_id"]:
                try:
                    ScheduleService.create_shift(data)
                    count += 1
                except Exception:
                    pass
        return count

    @staticmethod
    def repeat_shift(shift_id: int, num_weeks: int) -> int:
        """Create copies of this shift for the next num_weeks (same weekday). Links them as a series (recurrence_id). Returns count."""
        shift = ScheduleService.get_shift(shift_id)
        if not shift or num_weeks < 1:
            return 0
        recurrence_id = shift.get("recurrence_id") or shift_id
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if not shift.get("recurrence_id"):
                cur.execute("UPDATE schedule_shifts SET recurrence_id = %s WHERE id = %s", (recurrence_id, shift_id))
                conn.commit()
        finally:
            cur.close()
            conn.close()
        count = 0
        for i in range(1, num_weeks + 1):
            wd = shift.get("work_date")
            if not wd or not hasattr(wd, "weekday"):
                continue
            new_date = wd + timedelta(days=7 * i)
            data = {
                "contractor_id": shift["contractor_id"],
                "client_id": shift["client_id"],
                "site_id": shift.get("site_id"),
                "job_type_id": shift["job_type_id"],
                "work_date": new_date,
                "scheduled_start": shift.get("scheduled_start"),
                "scheduled_end": shift.get("scheduled_end"),
                "break_mins": shift.get("break_mins") or 0,
                "notes": shift.get("notes"),
                "status": "draft",
                "source": "manual",
                "recurrence_id": recurrence_id,
            }
            try:
                ScheduleService.create_shift(data)
                count += 1
            except Exception:
                pass
        return count

    @staticmethod
    def list_shifts_in_series(recurrence_id: int) -> List[Dict[str, Any]]:
        """Return shifts with the same recurrence_id, ordered by work_date."""
        if not recurrence_id:
            return []
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """SELECT id, work_date, scheduled_start, scheduled_end, status, contractor_id
                   FROM schedule_shifts WHERE recurrence_id = %s ORDER BY work_date, id""",
                (recurrence_id,),
            )
            return list(cur.fetchall() or [])
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_shift(shift_id: int, scope: str = "this") -> int:
        """Delete shift(s). scope: 'this' (single), 'future' (this + later in series), 'all' (entire series). Returns number deleted."""
        shift = ScheduleService.get_shift(shift_id)
        if not shift:
            return 0
        recurrence_id = shift.get("recurrence_id")
        work_date = shift.get("work_date")
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if scope == "this":
                cur.execute("DELETE FROM schedule_shifts WHERE id = %s", (shift_id,))
                return cur.rowcount
            if scope == "future" and recurrence_id is not None and work_date is not None:
                cur.execute(
                    "DELETE FROM schedule_shifts WHERE recurrence_id = %s AND work_date >= %s",
                    (recurrence_id, work_date),
                )
                return cur.rowcount
            if scope == "all" and recurrence_id is not None:
                cur.execute("DELETE FROM schedule_shifts WHERE recurrence_id = %s", (recurrence_id,))
                return cur.rowcount
            cur.execute("DELETE FROM schedule_shifts WHERE id = %s", (shift_id,))
            return cur.rowcount
        finally:
            conn.commit()
            cur.close()
            conn.close()

    # ---------- Shift swap ----------

    @staticmethod
    def create_swap_request(shift_id: int, requester_contractor_id: int, notes: Optional[str] = None) -> Optional[int]:
        """Offer my shift for swap. Requester must own the shift. Returns swap id or None."""
        shift = ScheduleService.get_shift(shift_id)
        if not shift or shift["contractor_id"] != requester_contractor_id:
            return None
        if shift.get("status") == "cancelled":
            return None
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM shift_swap_requests WHERE shift_id = %s AND status IN ('open','claimed')", (shift_id,))
            if cur.fetchone():
                return None
            cur.execute("""
                INSERT INTO shift_swap_requests (shift_id, requester_contractor_id, status, notes)
                VALUES (%s, %s, 'open', %s)
            """, (shift_id, requester_contractor_id, notes))
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_swap_requests(
        contractor_id: Optional[int] = None,
        status: Optional[str] = None,
        for_claimer: bool = False,
    ) -> List[Dict[str, Any]]:
        """List swap requests. If for_claimer=True, only open ones (that this contractor could claim)."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = ["1=1"]
            params: List[Any] = []
            if contractor_id is not None:
                if for_claimer:
                    where.append("r.status = 'open'")
                    where.append("s.contractor_id != %s")
                    params.append(contractor_id)
                else:
                    where.append("(r.requester_contractor_id = %s OR r.claimer_contractor_id = %s)")
                    params.extend([contractor_id, contractor_id])
            if status:
                where.append("r.status = %s")
                params.append(status)
            cur.execute(f"""
                SELECT r.*, s.work_date, s.scheduled_start, s.scheduled_end,
                       c.name AS client_name, u1.name AS requester_name, u2.name AS claimer_name
                FROM shift_swap_requests r
                JOIN schedule_shifts s ON s.id = r.shift_id
                JOIN clients c ON c.id = s.client_id
                JOIN tb_contractors u1 ON u1.id = r.requester_contractor_id
                LEFT JOIN tb_contractors u2 ON u2.id = r.claimer_contractor_id
                WHERE {" AND ".join(where)}
                ORDER BY r.requested_at DESC
            """, params)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def claim_swap(swap_id: int, claimer_contractor_id: int) -> bool:
        """Claim an open swap. Returns True if updated."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM shift_swap_requests WHERE id = %s AND status = 'open'", (swap_id,))
            r = cur.fetchone()
            if not r or r["requester_contractor_id"] == claimer_contractor_id:
                return False
            cur.execute("""
                UPDATE shift_swap_requests SET status = 'claimed', claimer_contractor_id = %s, claimed_at = NOW()
                WHERE id = %s
            """, (claimer_contractor_id, swap_id))
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def approve_swap(swap_id: int) -> bool:
        """Approve a claimed swap: reassign shift to claimer."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM shift_swap_requests WHERE id = %s AND status = 'claimed'", (swap_id,))
            r = cur.fetchone()
            if not r:
                return False
            ScheduleService.update_shift(r["shift_id"], {"contractor_id": r["claimer_contractor_id"]})
            cur.execute("""
                UPDATE shift_swap_requests SET status = 'approved', resolved_at = NOW()
                WHERE id = %s
            """, (swap_id,))
            conn.commit()
            return True
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def reject_swap(swap_id: int) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("UPDATE shift_swap_requests SET status = 'rejected', resolved_at = NOW() WHERE id = %s AND status IN ('open','claimed')", (swap_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def cancel_swap(swap_id: int, contractor_id: int) -> bool:
        """Requester or claimer cancels."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("UPDATE shift_swap_requests SET status = 'cancelled', resolved_at = NOW() WHERE id = %s AND status IN ('open','claimed') AND (requester_contractor_id = %s OR claimer_contractor_id = %s)", (swap_id, contractor_id, contractor_id))
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def cura_event_planning_overview(
        from_date: date,
        to_date: date,
    ) -> Dict[str, Any]:
        """
        Planning view: Cura operational events vs schedule_shifts linked through run sheets.

        - Expected roster: ``cura_operational_event_assignments`` (Medical Records).
        - Planned cover: ``schedule_shifts`` where ``runsheet_id`` points at a runsheet
          with ``cura_operational_event_id`` (Time Billing run sheet as planning link).

        This is intentionally separate from timesheet actuals / billing.
        """
        if to_date < from_date:
            from_date, to_date = to_date, from_date

        out: Dict[str, Any] = {
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "events": [],
            "note": (
                "Roster expectations come from Cura Event Manager. Where the event came from "
                "the CRM medical event planner, recommended grades/roles are taken from the "
                "linked opportunity (same guide as the planner) and compared to planned shifts. "
                "Planned shifts are schedule rows linked to a run sheet that references this Cura event."
            ),
        }

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SHOW TABLES LIKE 'cura_operational_events'")
            if not cur.fetchone():
                out["warning"] = (
                    "Cura operational tables are not installed (Medical Records / Cura ops)."
                )
                return out

            cur.execute("SHOW TABLES LIKE 'runsheets'")
            has_runsheets = bool(cur.fetchone())
            cur.execute("SHOW TABLES LIKE 'schedule_shift_assignments'")
            has_ssa = bool(cur.fetchone())

            cur.execute(
                "SHOW COLUMNS FROM runsheets LIKE 'cura_operational_event_id'"
            )
            has_cura_col = bool(cur.fetchone()) if has_runsheets else False

            cur.execute("SHOW TABLES LIKE 'cura_operational_event_resources'")
            has_cura_resources = bool(cur.fetchone())
            cur.execute("SHOW TABLES LIKE 'fleet_vehicles'")
            has_fleet_vehicles_tbl = bool(cur.fetchone())
            cur.execute("SHOW TABLES LIKE 'crm_event_plans'")
            has_crm_plans_tbl = bool(cur.fetchone())

            dt_start = datetime.combine(from_date, time.min)
            dt_end = datetime.combine(to_date, time.max)

            cur.execute(
                """
                SELECT id, name, slug, status, starts_at, ends_at, location_summary, config
                FROM cura_operational_events
                WHERE (starts_at IS NULL OR starts_at <= %s)
                  AND (ends_at IS NULL OR ends_at >= %s)
                ORDER BY COALESCE(starts_at, ends_at) ASC, id DESC
                LIMIT 120
                """,
                (dt_end, dt_start),
            )
            ev_rows = cur.fetchall() or []

            for ev in ev_rows:
                eid = _safe_int_id(ev.get("id"))
                if eid is None:
                    continue
                ev_name = (ev.get("name") or "").strip() or f"Event #{eid}"

                sa = ev.get("starts_at")
                ea = ev.get("ends_at")
                if hasattr(sa, "date"):
                    ev_start_d = sa.date()
                elif isinstance(sa, date):
                    ev_start_d = sa
                else:
                    ev_start_d = from_date
                if hasattr(ea, "date"):
                    ev_end_d = ea.date()
                elif isinstance(ea, date):
                    ev_end_d = ea
                else:
                    ev_end_d = to_date
                shift_from = max(from_date, ev_start_d)
                shift_to = min(to_date, ev_end_d)
                if shift_to < shift_from:
                    shift_from, shift_to = from_date, to_date

                cur.execute(
                    """
                    SELECT id, principal_username, expected_callsign, assigned_by, created_at
                    FROM cura_operational_event_assignments
                    WHERE operational_event_id = %s
                    ORDER BY id ASC
                    """,
                    (eid,),
                )
                assign_rows = cur.fetchall() or []

                roster: List[Dict[str, Any]] = []
                uname_set = set()
                for ar in assign_rows:
                    pu = (ar.get("principal_username") or "").strip().lower()
                    if pu:
                        uname_set.add(pu)
                    roster.append(
                        {
                            "username": (ar.get("principal_username") or "").strip(),
                            "expected_callsign": (ar.get("expected_callsign") or "").strip()
                            or None,
                            "assigned_by": (ar.get("assigned_by") or "").strip() or None,
                            "created_at": ar.get("created_at"),
                            "contractor_id": None,
                            "contractor_name": None,
                            "on_schedule": False,
                        }
                    )

                if uname_set:
                    ph = ",".join(["%s"] * len(uname_set))
                    cur.execute(
                        f"""
                        SELECT id, name, username
                        FROM tb_contractors
                        WHERE LOWER(TRIM(username)) IN ({ph})
                        """,
                        tuple(uname_set),
                    )
                    by_u: Dict[str, Dict[str, Any]] = {}
                    for urow in cur.fetchall() or []:
                        key = (urow.get("username") or "").strip().lower()
                        if key:
                            by_u[key] = urow
                    for row in roster:
                        key = (row.get("username") or "").strip().lower()
                        hit = by_u.get(key)
                        if hit:
                            row["contractor_id"] = _safe_int_id(hit.get("id"))
                            row["contractor_name"] = (hit.get("name") or "").strip() or None

                runsheet_ids: List[int] = []
                if has_runsheets and has_cura_col:
                    cur.execute(
                        """
                        SELECT id, work_date, status, job_type_id
                        FROM runsheets
                        WHERE cura_operational_event_id = %s
                        ORDER BY work_date ASC, id ASC
                        """,
                        (eid,),
                    )
                    rs_rows = cur.fetchall() or []
                    runsheet_ids = [
                        rid
                        for r in rs_rows
                        if (rid := _safe_int_id(r.get("id"))) is not None
                    ]
                else:
                    rs_rows = []

                planned_cids: Set[int] = set()
                shift_lines: List[Dict[str, Any]] = []

                if runsheet_ids and shift_to >= shift_from:
                    ph_r = ",".join(["%s"] * len(runsheet_ids))
                    cur.execute(
                        f"""
                        SELECT s.id, s.work_date, s.scheduled_start, s.scheduled_end,
                               s.runsheet_id, s.contractor_id, s.status,
                               jt.name AS job_type_name,
                               u.name AS contractor_name, u.username AS contractor_username
                        FROM schedule_shifts s
                        JOIN job_types jt ON jt.id = s.job_type_id
                        LEFT JOIN tb_contractors u ON u.id = s.contractor_id
                        WHERE s.runsheet_id IN ({ph_r})
                          AND s.work_date BETWEEN %s AND %s
                          AND COALESCE(LOWER(s.status), '') NOT IN ('cancelled', 'no_show')
                        ORDER BY s.work_date ASC, s.scheduled_start ASC, s.id ASC
                        """,
                        tuple(runsheet_ids + [shift_from, shift_to]),
                    )
                    s_rows = cur.fetchall() or []
                    shift_ids = [
                        sid
                        for s in s_rows
                        if (sid := _safe_int_id(s.get("id"))) is not None
                    ]
                    extra_by_shift: Dict[int, List[int]] = {}
                    if has_ssa and shift_ids:
                        ph_s = ",".join(["%s"] * len(shift_ids))
                        cur.execute(
                            f"""
                            SELECT shift_id, contractor_id
                            FROM schedule_shift_assignments
                            WHERE shift_id IN ({ph_s})
                            """,
                            tuple(shift_ids),
                        )
                        for xr in cur.fetchall() or []:
                            sid = _safe_int_id(xr.get("shift_id"))
                            cid_x = _safe_int_id(xr.get("contractor_id"))
                            if sid is not None and cid_x is not None:
                                extra_by_shift.setdefault(sid, []).append(cid_x)

                    for sr in s_rows:
                        sid = _safe_int_id(sr.get("id"))
                        if sid is None:
                            continue
                        cid = _safe_int_id(sr.get("contractor_id"))
                        assignee_ids: List[int] = []
                        assignee_seen: Set[int] = set()
                        if cid is not None:
                            assignee_ids.append(cid)
                            assignee_seen.add(cid)
                            planned_cids.add(cid)
                        for xc in extra_by_shift.get(sid, []):
                            planned_cids.add(xc)
                            if xc not in assignee_seen:
                                assignee_seen.add(xc)
                                assignee_ids.append(xc)
                        shift_lines.append(
                            {
                                "shift_id": sid,
                                "work_date": sr.get("work_date"),
                                "scheduled_start": sr.get("scheduled_start"),
                                "scheduled_end": sr.get("scheduled_end"),
                                "runsheet_id": sr.get("runsheet_id"),
                                "job_type_name": (sr.get("job_type_name") or "").strip(),
                                "contractor_name": (sr.get("contractor_name") or "").strip()
                                or None,
                                "contractor_username": (
                                    sr.get("contractor_username") or ""
                                ).strip()
                                or None,
                                "contractor_id": cid,
                                "assignee_ids": assignee_ids,
                            }
                        )

                for row in roster:
                    rcid = _safe_int_id(row.get("contractor_id"))
                    if rcid is not None and rcid in planned_cids:
                        row["on_schedule"] = True

                roster_n = len(roster)
                matched_cids = {
                    _safe_int_id(r["contractor_id"])
                    for r in roster
                    if r.get("contractor_id") is not None and r.get("on_schedule")
                }
                matched_cids.discard(None)
                roster_matched = len(matched_cids)
                planned_unique = len(planned_cids)
                if roster_n == 0:
                    required_headcount = 0
                    headcount_gap = None
                else:
                    required_headcount = roster_n
                    headcount_gap = max(0, roster_n - roster_matched)

                fleet_resources: List[Dict[str, Any]] = []
                if has_cura_resources:
                    if has_fleet_vehicles_tbl:
                        cur.execute(
                            """
                            SELECT r.id, r.resource_kind, r.resource_id, r.role_label,
                                   r.notes, r.sort_order,
                                   fv.registration, fv.internal_code
                            FROM cura_operational_event_resources r
                            LEFT JOIN fleet_vehicles fv
                              ON fv.id = r.resource_id
                             AND r.resource_kind = 'fleet_vehicle'
                            WHERE r.operational_event_id = %s
                            ORDER BY r.sort_order ASC, r.id ASC
                            """,
                            (eid,),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT id, resource_kind, resource_id, role_label, notes, sort_order
                            FROM cura_operational_event_resources
                            WHERE operational_event_id = %s
                            ORDER BY sort_order ASC, id ASC
                            """,
                            (eid,),
                        )
                    for fr in cur.fetchall() or []:
                        kind = (fr.get("resource_kind") or "").strip()
                        summary = ""
                        if kind == "fleet_vehicle" and fr.get("registration"):
                            summary = (fr.get("registration") or "").strip()
                            ic = (fr.get("internal_code") or "").strip()
                            if ic:
                                summary = f"{summary} ({ic})"
                        elif fr.get("resource_id") is not None:
                            summary = f"{kind} #{fr.get('resource_id')}"
                        else:
                            summary = kind or "Resource"
                        fleet_resources.append(
                            {
                                "kind": kind,
                                "summary": summary,
                                "role_label": (fr.get("role_label") or "").strip()
                                or None,
                                "notes": (fr.get("notes") or "").strip() or None,
                            }
                        )

                parsed_cfg = parse_cura_event_config(ev.get("config"))
                crm_staffing: Optional[Dict[str, Any]] = None
                crm_staffing_source: Optional[str] = None
                packed = parsed_cfg.get("crm_recommended_staffing")
                if isinstance(packed, dict) and isinstance(
                    packed.get("staffing_breakdown"), dict
                ):
                    crm_staffing = packed
                    crm_staffing_source = "cura_event_config"
                if crm_staffing is None:
                    raw_pid = parsed_cfg.get("crm_event_plan_id")
                    if raw_pid is not None:
                        try:
                            plan_id_int = int(raw_pid)
                        except (TypeError, ValueError):
                            plan_id_int = None
                        if plan_id_int is not None and has_crm_plans_tbl:
                            try:
                                from app.plugins.crm_module.crm_event_plan_staffing import (
                                    staffing_snapshot_for_crm_plan_id,
                                )

                                live = staffing_snapshot_for_crm_plan_id(
                                    cur, plan_id_int
                                )
                                if live:
                                    crm_staffing = live
                                    crm_staffing_source = "crm_opportunity_lookup"
                            except ImportError:
                                pass
                            except Exception as crm_ex:
                                logger.debug(
                                    "cura_event_planning CRM staffing lookup: %s",
                                    crm_ex,
                                    exc_info=True,
                                )
                sb_crm = (
                    (crm_staffing or {}).get("staffing_breakdown")
                    if isinstance(crm_staffing, dict)
                    else None
                )
                fleet_vehicle_n = sum(
                    1
                    for f in fleet_resources
                    if (f.get("kind") or "").strip() == "fleet_vehicle"
                )
                role_coverage = (
                    build_role_coverage(sb_crm if isinstance(sb_crm, dict) else None, shift_lines)
                    if sb_crm
                    else []
                )
                vehicle_guide = (
                    vehicle_gap_hint(
                        sb_crm if isinstance(sb_crm, dict) else None,
                        fleet_vehicle_n,
                    )
                    if sb_crm
                    else None
                )

                out["events"].append(
                    {
                        "id": eid,
                        "name": ev_name,
                        "slug": (ev.get("slug") or "").strip() or None,
                        "status": (ev.get("status") or "").strip() or None,
                        "starts_at": sa,
                        "ends_at": ea,
                        "location_summary": (ev.get("location_summary") or "").strip()
                        or None,
                        "shift_date_from": shift_from.isoformat(),
                        "shift_date_to": shift_to.isoformat(),
                        "roster": roster,
                        "roster_count": roster_n,
                        "required_headcount": required_headcount,
                        "roster_matched_on_schedule": roster_matched,
                        "planned_distinct_people": planned_unique,
                        "headcount_gap": headcount_gap,
                        "runsheet_count": len(runsheet_ids),
                        "runsheet_ids": runsheet_ids,
                        "planned_shifts": shift_lines,
                        "fleet_resources": fleet_resources,
                        "has_runsheet_link": bool(has_runsheets and has_cura_col),
                        "crm_event_plan_id": parsed_cfg.get("crm_event_plan_id"),
                        "crm_recommended_staffing": crm_staffing,
                        "crm_staffing_source": crm_staffing_source,
                        "crm_role_coverage": role_coverage,
                        "crm_vehicle_guide": vehicle_guide,
                    }
                )

        except Exception as ex:
            logger.warning("cura_event_planning_overview: %s", ex, exc_info=True)
            out["warning"] = "Could not load Cura planning data (check DB / modules)."
            out["events"] = []
        finally:
            cur.close()
            conn.close()

        return out


class SlingSyncService:
    """
    Import shifts from Sling into `schedule_shifts` for ERP-only clock-in/out.

    Design goals:
    - Sling is used only as the "shift schedule" source (not for timesheets).
    - We map contractors by email (Sling user email -> tb_contractors.email).
    - We map job types + sites best-effort (Sling position/location -> ERP job_types/sites).
    - If a Sling published shift disappears on the next sync, we can cancel it in ERP.
    """

    @staticmethod
    def _derive_fernet() -> Fernet:
        from flask import current_app

        secret = current_app.config.get("SECRET_KEY") or "defaultsecretkey"
        key = hashlib.sha256(str(secret).encode("utf-8")).digest()
        # Fernet expects a urlsafe base64-encoded 32-byte key
        fernet_key = base64.urlsafe_b64encode(key)
        return Fernet(fernet_key)

    @staticmethod
    def _encrypt(plaintext: str) -> str:
        if plaintext is None:
            plaintext = ""
        plaintext = str(plaintext)
        f = SlingSyncService._derive_fernet()
        return f.encrypt(plaintext.encode("utf-8")).decode("utf-8")

    @staticmethod
    def _decrypt(ciphertext: str) -> str:
        if not ciphertext:
            return ""
        try:
            f = SlingSyncService._derive_fernet()
            return f.decrypt(str(ciphertext).encode("utf-8")).decode("utf-8")
        except Exception:
            return ""

    @staticmethod
    def _table_exists(table_name: str) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE %s", (table_name,))
            return bool(cur.fetchone())
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def load_credentials() -> Optional[Dict[str, str]]:
        if not SlingSyncService._table_exists("sling_credentials"):
            return None
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, sling_email_enc, sling_password_enc, sling_base_url FROM sling_credentials WHERE id = 1 LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "email": SlingSyncService._decrypt(row.get("sling_email_enc")),
                "password": SlingSyncService._decrypt(row.get("sling_password_enc")),
                "base_url": row.get("sling_base_url") or "https://api.getsling.com/v1",
            }
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def save_credentials(email: str, password: str, base_url: str = "https://api.getsling.com/v1") -> None:
        email = (email or "").strip()
        password = (password or "").strip()
        if not email or not password:
            raise ValueError("Sling email and password are required.")

        enc_email = SlingSyncService._encrypt(email)
        enc_password = SlingSyncService._encrypt(password)

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO sling_credentials (id, sling_email_enc, sling_password_enc, sling_base_url)
                VALUES (1, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  sling_email_enc = VALUES(sling_email_enc),
                  sling_password_enc = VALUES(sling_password_enc),
                  sling_base_url = VALUES(sling_base_url)
                """,
                (enc_email, enc_password, base_url),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _parse_sling_dt(dt_str: Optional[str]) -> Optional[datetime]:
        if not dt_str or not isinstance(dt_str, str):
            return None
        s = dt_str.strip()
        # Handle RFC3339 "Z"
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None

    @staticmethod
    def _get_sling_settings() -> Dict[str, Optional[int]]:
        """
        Read from schedule_settings if the columns exist.
        Returns:
          default_job_type_id, default_client_id, default_site_id, cancel_missing
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        out = {
            "default_job_type_id": None,
            "default_client_id": None,
            "default_site_id": None,
            "cancel_missing": 1,
        }
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_settings'")
            if not cur.fetchone():
                return out
            # Use SHOW COLUMNS to avoid breaking if older DB is missing new columns.
            for col in ["sling_default_job_type_id", "sling_default_client_id", "sling_default_site_id", "sling_cancel_missing"]:
                cur.execute("SHOW COLUMNS FROM schedule_settings LIKE %s", (col,))
                exists = bool(cur.fetchone())
                if not exists:
                    continue
                cur.execute(f"SELECT {col} FROM schedule_settings WHERE id = 1 LIMIT 1")
                r = cur.fetchone() or {}
                v = r.get(col)
                if col == "sling_default_job_type_id":
                    out["default_job_type_id"] = int(v) if v is not None else None
                elif col == "sling_default_client_id":
                    out["default_client_id"] = int(v) if v is not None else None
                elif col == "sling_default_site_id":
                    out["default_site_id"] = int(v) if v is not None else None
                elif col == "sling_cancel_missing":
                    out["cancel_missing"] = 1 if int(v or 0) == 1 else 0
        finally:
            cur.close()
            conn.close()
        return out

    @staticmethod
    def update_sling_settings(
        default_job_type_id: Optional[int],
        default_client_id: Optional[int],
        default_site_id: Optional[int],
        cancel_missing: Optional[bool] = None,
    ) -> None:
        """Persist Sling defaults into schedule_settings (id=1). Safe on older DBs."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'schedule_settings'")
            if not cur.fetchone():
                return

            # Build dynamic update based on existing columns.
            updates: List[str] = []
            params: List[Any] = []

            def maybe_set(col: str, value: Optional[int]) -> None:
                cur2 = conn.cursor()
                try:
                    cur2.execute("SHOW COLUMNS FROM schedule_settings LIKE %s", (col,))
                    if not cur2.fetchone():
                        return
                finally:
                    cur2.close()
                if value is None:
                    updates.append(f"{col} = NULL")
                    return
                updates.append(f"{col} = %s")
                params.append(int(value))

            maybe_set("sling_default_job_type_id", default_job_type_id)
            maybe_set("sling_default_client_id", default_client_id)
            maybe_set("sling_default_site_id", default_site_id)
            if cancel_missing is not None:
                cur2 = conn.cursor()
                try:
                    cur2.execute("SHOW COLUMNS FROM schedule_settings LIKE %s", ("sling_cancel_missing",))
                    if cur2.fetchone():
                        updates.append("sling_cancel_missing = %s")
                        params.append(1 if bool(cancel_missing) else 0)
                finally:
                    cur2.close()

            if not updates:
                return
            sql = f"UPDATE schedule_settings SET {', '.join(updates)} WHERE id = 1"
            cur.execute(sql, tuple(params))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _find_job_type_id_by_name(position_name: Optional[str]) -> Optional[int]:
        if not position_name:
            return None
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT id
                FROM job_types
                WHERE active = 1 AND LOWER(name) = LOWER(%s)
                ORDER BY id
                LIMIT 1
                """,
                (position_name.strip(),),
            )
            r = cur.fetchone()
            return int(r["id"]) if r and r.get("id") is not None else None
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _find_site_by_name(location_name: Optional[str]) -> Optional[Dict[str, int]]:
        if not location_name:
            return None
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT s.id AS site_id, s.client_id
                FROM sites s
                WHERE s.active = 1 AND LOWER(s.name) = LOWER(%s)
                ORDER BY s.id
                LIMIT 1
                """,
                (location_name.strip(),),
            )
            r = cur.fetchone()
            if not r:
                return None
            return {"site_id": int(r["site_id"]), "client_id": int(r["client_id"])}
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _get_or_create_sling_unmapped_client() -> int:
        """
        Fallback client_id when we cannot map Sling location/client.

        We avoid schema changes by always populating schedule_shifts.client_id (NOT NULL).
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id FROM clients WHERE active = 1 AND LOWER(name) = LOWER(%s) LIMIT 1",
                ("Sling Unmapped",),
            )
            r = cur.fetchone()
            if r and r.get("id") is not None:
                return int(r["id"])
            cur.execute("INSERT INTO clients (name, active) VALUES (%s, 1)", ("Sling Unmapped",))
            conn.commit()
            return int(cur.lastrowid)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _lookup_sling_position_mapping(sling_position_id: Optional[str]) -> Optional[int]:
        if not sling_position_id:
            return None
        if not SlingSyncService._table_exists("sling_id_mappings"):
            return None
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT job_type_id
                FROM sling_id_mappings
                WHERE sling_position_id = %s AND job_type_id IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(sling_position_id),),
            )
            r = cur.fetchone()
            return int(r["job_type_id"]) if r and r.get("job_type_id") is not None else None
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _lookup_sling_location_mapping(sling_location_id: Optional[str]) -> Optional[Dict[str, int]]:
        if not sling_location_id:
            return None
        if not SlingSyncService._table_exists("sling_id_mappings"):
            return None
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT site_id, client_id
                FROM sling_id_mappings
                WHERE sling_location_id = %s AND site_id IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(sling_location_id),),
            )
            r = cur.fetchone()
            if not r:
                return None
            return {
                "site_id": int(r["site_id"]),
                "client_id": int(r["client_id"]) if r.get("client_id") is not None else None,
            }
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _maybe_store_mapping(
        sling_position_id: Optional[str],
        job_type_id: Optional[int],
        sling_location_id: Optional[str],
        site_id: Optional[int],
        client_id: Optional[int],
        sling_position_name: Optional[str] = None,
        sling_location_name: Optional[str] = None,
    ) -> None:
        # Non-fatal if mappings table isn't present.
        if not SlingSyncService._table_exists("sling_id_mappings"):
            return
        if not (sling_position_id or sling_location_id):
            return
        if not (job_type_id or site_id):
            return
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO sling_id_mappings
                  (sling_position_id, job_type_id, sling_location_id, site_id, client_id, sling_position_name, sling_location_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (str(sling_position_id) if sling_position_id else None, job_type_id,
                 str(sling_location_id) if sling_location_id else None, site_id, client_id,
                 (sling_position_name or "")[:255] if sling_position_name else None,
                 (sling_location_name or "")[:255] if sling_location_name else None),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _sling_headers(auth_token: str) -> Dict[str, str]:
        # Sling expects token in `Authorization` header (as per their spec).
        return {"Authorization": auth_token}

    @staticmethod
    def _login_and_token(session: requests.Session, base_url: str, email: str, password: str) -> str:
        url = f"{base_url}/account/login"
        r = session.post(url, json={"email": email, "password": password}, timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"Sling login failed ({r.status_code}).")
        token = r.headers.get("Authorization")
        if not token:
            # Some deployments may return token in JSON; fall back to common shape.
            data = {}
            try:
                data = r.json() or {}
            except Exception:
                pass
            token = data.get("Authorization") or data.get("authorization")
        if not token:
            raise RuntimeError("Sling login succeeded but no Authorization token was returned.")
        return token

    @staticmethod
    def _get_org_id(session: requests.Session, base_url: str, auth_token: str) -> int:
        url = f"{base_url}/account/session"
        r = session.get(url, headers=SlingSyncService._sling_headers(auth_token), timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"Sling session fetch failed ({r.status_code}).")
        data = r.json() or {}
        org_id = data.get("orgId")
        if org_id is None and isinstance(data.get("org"), dict):
            org_id = data["org"].get("id")
        if org_id is None:
            raise RuntimeError("Could not determine Sling orgId from /account/session response.")
        return int(org_id)

    @staticmethod
    def _find_sling_user_id_by_email(
        session: requests.Session, base_url: str, auth_token: str, email: str
    ) -> Optional[int]:
        # Sling's /users GET supports `query` against email/name fields.
        url = f"{base_url}/users"
        r = session.get(url, headers=SlingSyncService._sling_headers(auth_token), params={"query": email}, timeout=30)
        if r.status_code >= 400:
            return None
        data = r.json()
        users = data.get("users") if isinstance(data, dict) else data
        if not isinstance(users, list):
            return None
        target = (email or "").strip().lower()
        for u in users:
            if not isinstance(u, dict):
                continue
            u_email = (u.get("email") or "").strip().lower()
            if u_email and u_email == target:
                uid = u.get("id")
                return int(uid) if uid is not None else None
        return None

    @staticmethod
    def _fetch_published_calendar_events(
        session: requests.Session,
        base_url: str,
        auth_token: str,
        org_id: int,
        sling_user_id: int,
        date_from: date,
        date_to: date,
    ) -> List[Dict[str, Any]]:
        # We request without planning events: showPlanningEvents=false.
        # dates param is an ISO8601 interval; Sling supports date strings as part of the interval.
        url = f"{base_url}/calendar/{org_id}/users/{sling_user_id}"
        dates_param = f"{date_from.isoformat()}/{date_to.isoformat()}"
        r = session.get(
            url,
            headers=SlingSyncService._sling_headers(auth_token),
            params={
                "dates": dates_param,
                "showPlanningEvents": "false",
            },
            timeout=30,
        )
        if r.status_code >= 400:
            return []
        payload = r.json()
        events = payload.get("events") if isinstance(payload, dict) else payload
        if not isinstance(events, list):
            return []
        out = []
        for e in events:
            if isinstance(e, dict) and e.get("id") is not None:
                out.append(e)
        return out

    @staticmethod
    def _fetch_shift_detailed(
        session: requests.Session, base_url: str, auth_token: str, event_id: int
    ) -> Dict[str, Any]:
        url = f"{base_url}/shifts/{event_id}/detailed"
        r = session.get(url, headers=SlingSyncService._sling_headers(auth_token), timeout=30)
        if r.status_code >= 400:
            return {}
        data = r.json() or {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def sync_published_shifts(
        date_from: date,
        date_to: date,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        creds = SlingSyncService.load_credentials()
        if not creds or not creds.get("email") or not creds.get("password"):
            raise RuntimeError("Sling credentials not configured. Please save them in the Sling Sync admin page.")

        settings = SlingSyncService._get_sling_settings()
        cancel_missing = bool(settings.get("cancel_missing", 1))
        default_job_type_id = settings.get("default_job_type_id")
        default_client_id = settings.get("default_client_id")
        default_site_id = settings.get("default_site_id")

        if not default_job_type_id:
            raise RuntimeError("Default job type is required to sync Sling shifts (set it in Sling Sync admin page).")
        # default_client_id is optional: if unset we store shifts under a placeholder client.

        base_url = creds["base_url"]
        email = creds["email"]
        password = creds["password"]

        http = requests.Session()
        auth_token = SlingSyncService._login_and_token(http, base_url, email, password)
        org_id = SlingSyncService._get_org_id(http, base_url, auth_token)

        desired_external_ids: List[str] = []
        processed_shifts = 0
        created = 0
        updated = 0
        cancelled = 0
        unmapped = 0
        errors: List[str] = []

        contractors = ScheduleService.list_contractors()
        # Basic per-sync cache to reduce /users lookups.
        sling_user_id_cache: Dict[str, int] = {}

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            for c in contractors:
                our_contractor_id = int(c["id"])
                our_email = (c.get("email") or "").strip()
                if not our_email:
                    continue
                if our_email.lower() in sling_user_id_cache:
                    sling_user_id = sling_user_id_cache[our_email.lower()]
                else:
                    sling_user_id = SlingSyncService._find_sling_user_id_by_email(
                        http, base_url, auth_token, our_email
                    )
                    if not sling_user_id:
                        continue
                    sling_user_id_cache[our_email.lower()] = sling_user_id

                events = SlingSyncService._fetch_published_calendar_events(
                    http, base_url, auth_token, org_id, sling_user_id, date_from, date_to
                )
                for e in events:
                    event_id = e.get("id")
                    if event_id is None:
                        continue
                    event_id_int = int(event_id)
                    external_id = f"sling:{event_id_int}"

                    # Find/parse scheduled date/time from calendar event.
                    dtstart = SlingSyncService._parse_sling_dt(e.get("dtstart"))
                    dtend = SlingSyncService._parse_sling_dt(e.get("dtend"))
                    if not dtstart or not dtend:
                        # Fallback: attempt detailed for dates too.
                        details = SlingSyncService._fetch_shift_detailed(http, base_url, auth_token, event_id_int)
                        dtstart = SlingSyncService._parse_sling_dt(details.get("dtstart"))
                        dtend = SlingSyncService._parse_sling_dt(details.get("dtend"))

                    if not dtstart or not dtend:
                        errors.append(f"Missing dtstart/dtend for sling event {event_id_int}")
                        continue

                    work_date = dtstart.date()
                    scheduled_start = dtstart.time().replace(microsecond=0)
                    scheduled_end = dtend.time().replace(microsecond=0)

                    details = SlingSyncService._fetch_shift_detailed(http, base_url, auth_token, event_id_int)
                    processed_shifts += 1

                    # Break duration (minutes) if provided.
                    break_mins = 0
                    try:
                        break_mins = int(details.get("breakDuration") or e.get("breakDuration") or 0)
                    except Exception:
                        break_mins = 0

                    position = details.get("position") or {}
                    location = details.get("location") or {}
                    sling_position_id = position.get("id")
                    sling_position_name = position.get("name")
                    sling_location_id = location.get("id")
                    sling_location_name = location.get("name")

                    # Map job type
                    job_type_id = SlingSyncService._lookup_sling_position_mapping(str(sling_position_id) if sling_position_id else None)
                    if not job_type_id and sling_position_name:
                        job_type_id = SlingSyncService._find_job_type_id_by_name(sling_position_name)
                    if not job_type_id:
                        job_type_id = default_job_type_id

                    # Client/site mapping:
                    # On Sling free plan, `location` is not reliably populated for ERP-required client/site mapping.
                    # To keep the transfer painless, we always import shifts under a placeholder client and null site.
                    client_id = SlingSyncService._get_or_create_sling_unmapped_client()
                    site_id = None

                    notes = (details.get("summary") or details.get("notes") or e.get("summary") or "").strip()
                    notes = notes[:10000] if notes else None

                    # If we can't map job type (position), we can't create a valid ERP shift.
                    if not job_type_id:
                        unmapped += 1
                        continue

                    desired_external_ids.append(external_id)

                    # Upsert schedule_shifts by external_id.
                    cur.execute("SELECT id FROM schedule_shifts WHERE external_id = %s LIMIT 1", (external_id,))
                    existing = cur.fetchone()
                    if existing and existing.get("id"):
                        shift_id = int(existing["id"])
                        if not dry_run:
                            cur.execute(
                                """
                                UPDATE schedule_shifts
                                SET contractor_id = %s,
                                    client_id = %s,
                                    site_id = %s,
                                    job_type_id = %s,
                                    work_date = %s,
                                    scheduled_start = %s,
                                    scheduled_end = %s,
                                    break_mins = %s,
                                    notes = %s,
                                    status = 'published',
                                    source = 'scheduler',
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s
                                """,
                                (
                                    our_contractor_id,
                                    int(client_id),
                                    int(site_id) if site_id else None,
                                    int(job_type_id),
                                    work_date,
                                    scheduled_start,
                                    scheduled_end,
                                    break_mins,
                                    notes,
                                    shift_id,
                                ),
                            )
                            conn.commit()
                            ScheduleService.set_shift_assignments(shift_id, [our_contractor_id])
                        updated += 1
                    else:
                        if not dry_run:
                            cur.execute(
                                """
                                INSERT INTO schedule_shifts
                                  (contractor_id, client_id, site_id, job_type_id, work_date,
                                   scheduled_start, scheduled_end, break_mins, notes, status, source,
                                   external_id, required_count)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'published','scheduler',%s,1)
                                """,
                                (
                                    our_contractor_id,
                                    int(client_id),
                                    int(site_id) if site_id else None,
                                    int(job_type_id),
                                    work_date,
                                    scheduled_start,
                                    scheduled_end,
                                    break_mins,
                                    notes,
                                    external_id,
                                ),
                            )
                            conn.commit()
                            shift_id = cur.lastrowid
                            ScheduleService.set_shift_assignments(shift_id, [our_contractor_id])
                        created += 1
        finally:
            cur.close()
            conn.close()

        # Cancel missing shifts within the sync range.
        if cancel_missing and not dry_run:
            desired_set = set(desired_external_ids)
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                # Only cancel future/unclocked shifts.
                if not desired_set:
                    cur.execute(
                        """
                        UPDATE schedule_shifts
                        SET status = 'cancelled'
                        WHERE source = 'scheduler'
                          AND external_id LIKE 'sling:%'
                          AND work_date >= %s AND work_date <= %s
                          AND status IN ('draft','published')
                          AND actual_start IS NULL AND actual_end IS NULL
                        """,
                        (date_from, date_to),
                    )
                else:
                    ext_list = list(desired_set)
                    placeholders = ",".join(["%s"] * len(ext_list))
                    sql = f"""
                        UPDATE schedule_shifts
                        SET status = 'cancelled'
                        WHERE source = 'scheduler'
                          AND external_id LIKE 'sling:%'
                          AND work_date >= %s AND work_date <= %s
                          AND status IN ('draft','published')
                          AND actual_start IS NULL AND actual_end IS NULL
                          AND external_id NOT IN ({placeholders})
                    """
                    params = [date_from, date_to] + ext_list
                    cur.execute(sql, params)
                conn.commit()
                cancelled = cur.rowcount
            finally:
                cur.close()
                conn.close()

        return {
            "processed_shifts": processed_shifts,
            "created": created,
            "updated": updated,
            "cancelled": cancelled,
            "unmapped": unmapped,
            "errors": errors[-10:],  # keep output small
            "desired_external_ids": len(set(desired_external_ids)),
        }
