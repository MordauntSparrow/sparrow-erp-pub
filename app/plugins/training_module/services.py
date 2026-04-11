"""
Training module v2: courses, versions, lessons, quizzes, assignments, certificates, audit.
Legacy TrainingService method names preserved where used by employee_portal.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.objects import get_db_connection

logger = logging.getLogger(__name__)

DELIVERY_TYPES = frozenset({"internal", "internal_signoff", "external_required", "evidence_only"})
LESSON_TYPES = frozenset({"text", "pdf", "video", "link", "checklist", "quiz"})

# Terminal / good states
STATUS_PASSED = "passed"
STATUS_EXEMPT = "exempt"

# Action needed for contractor / portal badge
PENDING_STATUSES = frozenset(
    {
        "assigned",
        "in_progress",
        "failed",
        "expired",
        "awaiting_signoff",
        "awaiting_external_evidence",
        "pending_evidence_review",
    }
)


def _json_safe(obj: Any) -> str:
    try:
        return json.dumps(obj, default=str)[:8000]
    except Exception:
        return "{}"


def _audit(
    event_type: str,
    entity_table: str,
    entity_id: int,
    contractor_id: Optional[int] = None,
    actor_user_id: Optional[int] = None,
    payload: Optional[dict] = None,
) -> None:
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO trn_audit_log
                (event_type, entity_table, entity_id, contractor_id, actor_user_id, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    event_type[:64],
                    entity_table[:64],
                    int(entity_id),
                    int(contractor_id) if contractor_id else None,
                    int(actor_user_id) if actor_user_id else None,
                    _json_safe(payload or {}),
                ),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.warning("training audit log failed: %s", e)


def _slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return (s.strip("-") or "course")[:120]


class TrainingService:
    """Public + admin operations for trn_* schema."""

    # ------------------------------------------------------------------
    # Pending count (employee portal)
    # ------------------------------------------------------------------
    @staticmethod
    def count_pending_for_contractor(contractor_id: int) -> int:
        if not contractor_id:
            return 0
        if not TrainingService._trn_tables_exist():
            return TrainingService._legacy_count_pending(contractor_id)
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            placeholders = ",".join(["%s"] * len(PENDING_STATUSES))
            cur.execute(
                f"""
                SELECT COUNT(*) FROM trn_assignments
                WHERE contractor_id = %s AND status IN ({placeholders})
                """,
                (int(contractor_id),) + tuple(PENDING_STATUSES),
            )
            row = cur.fetchone()
            return int(row[0] if row else 0)
        except Exception as e:
            logger.debug("count_pending_for_contractor: %s", e)
            return 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _trn_tables_exist() -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'trn_assignments'")
            return bool(cur.fetchone())
        except Exception:
            return False
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def training_assignments_table_ready() -> bool:
        """True when modern training course assignments (``trn_assignments``) exist."""
        return TrainingService._trn_tables_exist()

    @staticmethod
    def _legacy_count_pending(contractor_id: int) -> int:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT COUNT(*) FROM training_assignments a
                WHERE a.contractor_id = %s
                  AND NOT EXISTS (SELECT 1 FROM training_completions c WHERE c.assignment_id = a.id)
                """,
                (int(contractor_id),),
            )
            row = cur.fetchone()
            return int(row[0] if row else 0)
        except Exception:
            return 0
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # Legacy-compatible list (portal index)
    # ------------------------------------------------------------------
    @staticmethod
    def list_assignments(
        contractor_id: Optional[int] = None,
        training_item_id: Optional[int] = None,
        include_completed: bool = True,
    ) -> List[Dict[str, Any]]:
        if contractor_id is None:
            return TrainingService.admin_list_assignments(
                contractor_id=None,
                course_id=training_item_id,
                include_completed=include_completed,
            )
        if TrainingService._trn_tables_exist():
            return TrainingService._list_assignments_trn(
                contractor_id, include_completed=include_completed
            )
        return TrainingService._list_assignments_legacy(
            contractor_id, training_item_id, include_completed
        )

    @staticmethod
    def _list_assignments_trn(contractor_id: int, include_completed: bool) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT a.id, a.due_date, a.mandatory, a.status, a.completed_at,
                       a.assigned_at, a.course_id,
                       c.title, c.slug, c.summary AS course_summary, c.delivery_type,
                       c.comp_policy_id
                FROM trn_assignments a
                JOIN trn_courses c ON c.id = a.course_id
                WHERE a.contractor_id = %s
                ORDER BY
                  CASE WHEN a.status = 'passed' THEN 1 ELSE 0 END,
                  a.due_date IS NULL, a.due_date ASC, a.assigned_at DESC
                """,
                (int(contractor_id),),
            )
            rows = cur.fetchall() or []
            out = []
            for r in rows:
                passed = (r.get("status") or "") == STATUS_PASSED
                if not include_completed and passed:
                    continue
                out.append(
                    {
                        "id": r["id"],
                        "training_item_id": r["course_id"],
                        "contractor_id": contractor_id,
                        "due_date": r.get("due_date"),
                        "mandatory": r.get("mandatory"),
                        "assigned_at": r.get("assigned_at"),
                        "title": r.get("title"),
                        "slug": r.get("slug"),
                        "summary": r.get("course_summary"),
                        "item_type": "document",
                        "external_url": None,
                        "completed": passed,
                        "status": r.get("status"),
                        "delivery_type": r.get("delivery_type"),
                        "completed_at": r.get("completed_at"),
                        "comp_policy_id": r.get("comp_policy_id"),
                    }
                )
            return out
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _list_assignments_legacy(
        contractor_id: int,
        training_item_id: Optional[int],
        include_completed: bool,
    ) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = ["a.contractor_id = %s"]
            params: List[Any] = [contractor_id]
            if training_item_id is not None:
                where.append("a.training_item_id = %s")
                params.append(training_item_id)
            cur.execute(
                f"""
                SELECT a.*, t.title, t.slug, t.summary, t.item_type, t.external_url,
                       u.name AS contractor_name, u.email AS contractor_email,
                       (SELECT 1 FROM training_completions c WHERE c.assignment_id = a.id LIMIT 1) AS completed
                FROM training_assignments a
                JOIN training_items t ON t.id = a.training_item_id
                JOIN tb_contractors u ON u.id = a.contractor_id
                WHERE {" AND ".join(where)}
                ORDER BY a.assigned_at DESC
                """,
                params,
            )
            rows = cur.fetchall() or []
            if not include_completed:
                rows = [r for r in rows if not r.get("completed")]
            return rows
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # Assignment detail (portal player)
    # ------------------------------------------------------------------
    @staticmethod
    def get_assignment(
        assignment_id: int, contractor_id: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        if TrainingService._trn_tables_exist():
            return TrainingService._get_assignment_trn(assignment_id, contractor_id)
        return TrainingService._get_assignment_legacy(assignment_id, contractor_id)

    @staticmethod
    def _get_assignment_trn(
        assignment_id: int, contractor_id: Optional[int]
    ) -> Optional[Dict[str, Any]]:
        TrainingService.refresh_assignment_status(assignment_id)
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = "a.id = %s"
            params: List[Any] = [assignment_id]
            if contractor_id is not None:
                where += " AND a.contractor_id = %s"
                params.append(int(contractor_id))
            cur.execute(
                f"""
                SELECT a.*, c.title, c.slug, c.summary, c.delivery_type, c.comp_policy_id,
                       c.require_certificate_verification, c.grace_days
                FROM trn_assignments a
                JOIN trn_courses c ON c.id = a.course_id
                WHERE {where}
                """,
                params,
            )
            row = cur.fetchone()
            if not row:
                return None
            row["completed"] = row.get("status") == STATUS_PASSED
            row["completion_notes"] = row.get("notes")
            lessons = TrainingService._lessons_for_assignment(cur, assignment_id, int(row["course_version_id"]))
            row["lessons"] = lessons
            row["certificate"] = TrainingService._get_certificate_row(cur, assignment_id)
            row["signoff"] = TrainingService._get_signoff_row(cur, assignment_id)
            cur.execute(
                "SELECT * FROM trn_exemptions WHERE course_id = %s AND contractor_id = %s LIMIT 1",
                (int(row["course_id"]), int(row["contractor_id"])),
            )
            row["exemption"] = cur.fetchone()
            return row
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _lessons_for_assignment(cur, assignment_id: int, version_id: int) -> List[Dict[str, Any]]:
        cur.execute(
            """
            SELECT l.*, m.sort_order AS module_sort, m.title AS module_title
            FROM trn_lessons l
            JOIN trn_modules m ON m.id = l.module_id
            WHERE m.course_version_id = %s
            ORDER BY m.sort_order, l.sort_order, l.id
            """,
            (version_id,),
        )
        lessons = cur.fetchall() or []
        for les in lessons:
            lid = int(les["id"])
            cur.execute(
                """
                SELECT completed_at FROM trn_lesson_progress
                WHERE assignment_id = %s AND lesson_id = %s
                """,
                (assignment_id, lid),
            )
            pr = cur.fetchone()
            les["progress_completed_at"] = pr["completed_at"] if pr else None
            if (les.get("lesson_type") or "") == "quiz":
                cur.execute(
                    """
                    SELECT * FROM trn_quiz_attempts
                    WHERE assignment_id = %s AND lesson_id = %s
                    ORDER BY id DESC LIMIT 1
                    """,
                    (assignment_id, lid),
                )
                les["last_quiz_attempt"] = cur.fetchone()
                cur.execute(
                    """
                    SELECT id, question_text, sort_order FROM trn_questions
                    WHERE lesson_id = %s ORDER BY sort_order, id
                    """,
                    (lid,),
                )
                qs = cur.fetchall() or []
                for q in qs:
                    cur.execute(
                        """
                        SELECT id, option_text, sort_order FROM trn_question_options
                        WHERE question_id = %s ORDER BY sort_order, id
                        """,
                        (int(q["id"]),),
                    )
                    q["options"] = cur.fetchall() or []
                les["questions"] = qs
        return lessons

    @staticmethod
    def _get_certificate_row(cur, assignment_id: int) -> Optional[Dict[str, Any]]:
        cur.execute(
            "SELECT * FROM trn_certificates WHERE assignment_id = %s ORDER BY id DESC LIMIT 1",
            (assignment_id,),
        )
        return cur.fetchone()

    @staticmethod
    def _get_signoff_row(cur, assignment_id: int) -> Optional[Dict[str, Any]]:
        cur.execute(
            "SELECT * FROM trn_competency_signoffs WHERE assignment_id = %s LIMIT 1",
            (assignment_id,),
        )
        return cur.fetchone()

    @staticmethod
    def _get_assignment_legacy(
        assignment_id: int, contractor_id: Optional[int]
    ) -> Optional[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = "a.id = %s"
            params: List[Any] = [assignment_id]
            if contractor_id is not None:
                where += " AND a.contractor_id = %s"
                params.append(contractor_id)
            cur.execute(
                f"""
                SELECT a.*, t.title, t.slug, t.summary, t.content, t.item_type, t.external_url,
                       u.name AS contractor_name
                FROM training_assignments a
                JOIN training_items t ON t.id = a.training_item_id
                JOIN tb_contractors u ON u.id = a.contractor_id
                WHERE {where}
                """,
                params,
            )
            row = cur.fetchone()
            if not row:
                return None
            cur.execute(
                "SELECT id, completed_at, notes FROM training_completions WHERE assignment_id = %s",
                (assignment_id,),
            )
            comp = cur.fetchone()
            row["completed"] = comp is not None
            row["completed_at"] = comp["completed_at"] if comp else None
            row["completion_notes"] = comp.get("notes") if comp else None
            row["lessons"] = []
            row["certificate"] = None
            row["signoff"] = None
            row["delivery_type"] = "internal"
            row["comp_policy_id"] = None
            return row
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # Status engine
    # ------------------------------------------------------------------
    @staticmethod
    def refresh_assignment_status(assignment_id: int) -> None:
        if not TrainingService._trn_tables_exist():
            return
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT a.*, c.delivery_type, c.require_certificate_verification
                FROM trn_assignments a
                JOIN trn_courses c ON c.id = a.course_id
                WHERE a.id = %s
                """,
                (assignment_id,),
            )
            a = cur.fetchone()
            if not a:
                return
            if a["status"] == STATUS_EXEMPT:
                return
            if a["status"] == STATUS_PASSED:
                return

            cur.execute(
                """
                SELECT 1 FROM trn_exemptions
                WHERE course_id = %s AND contractor_id = %s
                  AND (exempt_until IS NULL OR exempt_until >= CURDATE())
                LIMIT 1
                """,
                (int(a["course_id"]), int(a["contractor_id"])),
            )
            if cur.fetchone():
                TrainingService._set_status(conn, assignment_id, STATUS_EXEMPT, None)
                return

            due = a.get("due_date")
            grace_end = a.get("grace_ends_at")
            today = date.today()
            dt = (a.get("delivery_type") or "internal").lower()
            if due and a["status"] not in (STATUS_PASSED, STATUS_EXEMPT):
                end = grace_end or due
                if today > end and dt in ("internal", "internal_signoff"):
                    TrainingService._set_status(conn, assignment_id, "expired", None)
                    return

            if dt == "external_required":
                TrainingService._refresh_external(conn, cur, a)
                return
            if dt == "evidence_only":
                TrainingService._refresh_evidence(conn, cur, a)
                return

            # internal / internal_signoff
            if not TrainingService._all_lessons_done(cur, assignment_id, int(a["course_version_id"])):
                st = "in_progress" if TrainingService._any_lesson_started(cur, assignment_id) else "assigned"
                TrainingService._set_status(conn, assignment_id, st, None)
                return

            if dt == "internal_signoff":
                cur.execute(
                    "SELECT 1 FROM trn_competency_signoffs WHERE assignment_id = %s",
                    (assignment_id,),
                )
                if not cur.fetchone():
                    TrainingService._set_status(conn, assignment_id, "awaiting_signoff", None)
                    return

            TrainingService._set_status(conn, assignment_id, STATUS_PASSED, datetime.now())
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _set_status(conn, assignment_id: int, status: str, completed_at: Optional[datetime]) -> None:
        cur = conn.cursor()
        try:
            cur.execute("SELECT status FROM trn_assignments WHERE id = %s", (assignment_id,))
            row = cur.fetchone()
            if not row:
                return
            cur_st = row[0]
            if cur_st in (STATUS_PASSED, STATUS_EXEMPT) and status not in (STATUS_PASSED, STATUS_EXEMPT):
                return
            comp = completed_at if status == STATUS_PASSED else None
            cur.execute(
                "UPDATE trn_assignments SET status = %s, completed_at = %s WHERE id = %s",
                (status, comp, assignment_id),
            )
            conn.commit()
        finally:
            cur.close()

    @staticmethod
    def _all_lessons_done(cur, assignment_id: int, version_id: int) -> bool:
        cur.execute(
            """
            SELECT l.id, l.lesson_type FROM trn_lessons l
            JOIN trn_modules m ON m.id = l.module_id
            WHERE m.course_version_id = %s
            ORDER BY m.sort_order, l.sort_order
            """,
            (version_id,),
        )
        lessons = cur.fetchall() or []
        if not lessons:
            return True
        for les in lessons:
            lid = int(les["id"])
            lt = (les.get("lesson_type") or "").lower()
            if lt == "quiz":
                cur.execute(
                    """
                    SELECT passed FROM trn_quiz_attempts
                    WHERE assignment_id = %s AND lesson_id = %s AND passed = 1
                    LIMIT 1
                    """,
                    (assignment_id, lid),
                )
                if not cur.fetchone():
                    return False
            else:
                cur.execute(
                    """
                    SELECT 1 FROM trn_lesson_progress
                    WHERE assignment_id = %s AND lesson_id = %s
                    """,
                    (assignment_id, lid),
                )
                if not cur.fetchone():
                    return False
        return True

    @staticmethod
    def _any_lesson_started(cur, assignment_id: int) -> bool:
        cur.execute(
            "SELECT 1 FROM trn_lesson_progress WHERE assignment_id = %s LIMIT 1",
            (assignment_id,),
        )
        if cur.fetchone():
            return True
        cur.execute(
            "SELECT 1 FROM trn_quiz_attempts WHERE assignment_id = %s LIMIT 1",
            (assignment_id,),
        )
        return bool(cur.fetchone())

    @staticmethod
    def _refresh_external(conn, cur, a: Dict[str, Any]) -> None:
        aid = int(a["id"])
        cur.execute("SELECT * FROM trn_certificates WHERE assignment_id = %s ORDER BY id DESC LIMIT 1", (aid,))
        cert = cur.fetchone()
        if not cert or not cert.get("file_path"):
            TrainingService._set_status(conn, aid, "awaiting_external_evidence", None)
            return
        exp = cert.get("expires_at")
        if exp and exp < date.today():
            TrainingService._set_status(conn, aid, "expired", None)
            return
        req = int(a.get("require_certificate_verification") or 1)
        if req and not cert.get("verified_at"):
            TrainingService._set_status(conn, aid, "pending_evidence_review", None)
            return
        TrainingService._set_status(conn, aid, STATUS_PASSED, datetime.now())

    @staticmethod
    def _refresh_evidence(conn, cur, a: Dict[str, Any]) -> None:
        aid = int(a["id"])
        cur.execute("SELECT * FROM trn_certificates WHERE assignment_id = %s ORDER BY id DESC LIMIT 1", (aid,))
        cert = cur.fetchone()
        if not cert or not cert.get("file_path"):
            TrainingService._set_status(conn, aid, "awaiting_external_evidence", None)
            return
        req = int(a.get("require_certificate_verification") or 1)
        if req and not cert.get("verified_at"):
            TrainingService._set_status(conn, aid, "pending_evidence_review", None)
            return
        TrainingService._set_status(conn, aid, STATUS_PASSED, datetime.now())

    # ------------------------------------------------------------------
    # Contractor actions
    # ------------------------------------------------------------------
    @staticmethod
    def mark_lesson_complete(assignment_id: int, contractor_id: int, lesson_id: int) -> Tuple[bool, str]:
        if not TrainingService._trn_tables_exist():
            return False, "Training system upgrading — try again later."
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT a.*, c.delivery_type FROM trn_assignments a
                JOIN trn_courses c ON c.id = a.course_id
                WHERE a.id = %s AND a.contractor_id = %s
                """,
                (assignment_id, contractor_id),
            )
            a = cur.fetchone()
            if not a:
                return False, "Not found."
            if (a.get("delivery_type") or "").lower() in ("external_required", "evidence_only"):
                return False, "This course is completed by submitting evidence or a certificate, not lesson checkboxes."
            cur.execute(
                """
                SELECT l.id, l.lesson_type FROM trn_lessons l
                JOIN trn_modules m ON m.id = l.module_id
                WHERE l.id = %s AND m.course_version_id = %s
                """,
                (lesson_id, int(a["course_version_id"])),
            )
            les = cur.fetchone()
            if not les:
                return False, "Invalid lesson."
            if (les.get("lesson_type") or "").lower() == "quiz":
                return False, "Complete the quiz for this lesson."
            cur.execute(
                """
                INSERT INTO trn_lesson_progress (assignment_id, lesson_id, completed_at)
                VALUES (%s, %s, NOW())
                ON DUPLICATE KEY UPDATE completed_at = NOW()
                """,
                (assignment_id, lesson_id),
            )
            conn.commit()
            _audit("lesson_complete", "trn_lessons", lesson_id, contractor_id, None, {"assignment_id": assignment_id})
            TrainingService.refresh_assignment_status(assignment_id)
            return True, "ok"
        except Exception as e:
            conn.rollback()
            return False, str(e)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def submit_quiz(
        assignment_id: int, contractor_id: int, lesson_id: int, selected_option_ids: Sequence[int]
    ) -> Tuple[bool, str]:
        if not TrainingService._trn_tables_exist():
            return False, "Training system upgrading — try again later."
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT a.*, c.delivery_type FROM trn_assignments a
                JOIN trn_courses c ON c.id = a.course_id
                WHERE a.id = %s AND a.contractor_id = %s
                """,
                (assignment_id, contractor_id),
            )
            a = cur.fetchone()
            if not a:
                return False, "Not found."
            if (a.get("delivery_type") or "").lower() in ("external_required", "evidence_only"):
                return False, "Quiz not used for this delivery type."
            cur.execute(
                """
                SELECT l.* FROM trn_lessons l
                JOIN trn_modules m ON m.id = l.module_id
                WHERE l.id = %s AND m.course_version_id = %s AND l.lesson_type = 'quiz'
                """,
                (lesson_id, int(a["course_version_id"])),
            )
            les = cur.fetchone()
            if not les:
                return False, "Not a quiz lesson."
            max_attempts = int(les.get("max_quiz_attempts") or 3)
            cur.execute(
                "SELECT COUNT(*) AS n FROM trn_quiz_attempts WHERE assignment_id = %s AND lesson_id = %s",
                (assignment_id, lesson_id),
            )
            n = int((cur.fetchone() or {}).get("n") or 0)
            if n >= max_attempts:
                return False, "Maximum quiz attempts reached."
            cur.execute(
                "SELECT id FROM trn_questions WHERE lesson_id = %s ORDER BY sort_order, id",
                (lesson_id,),
            )
            qrows = cur.fetchall() or []
            if not qrows:
                return False, "No questions configured."
            correct = 0
            picked = {int(x) for x in selected_option_ids}
            for q in qrows:
                qid = int(q["id"])
                cur.execute(
                    "SELECT id FROM trn_question_options WHERE question_id = %s AND is_correct = 1 ORDER BY id LIMIT 1",
                    (qid,),
                )
                ok = cur.fetchone()
                if ok and int(ok["id"]) in picked:
                    correct += 1
            total = len(qrows)
            pct = int(round(100 * correct / total)) if total else 0
            mark = int(les.get("pass_mark_percent") or 80)
            passed = 1 if pct >= mark else 0
            cur.execute(
                """
                INSERT INTO trn_quiz_attempts
                (assignment_id, lesson_id, score, max_score, passed, attempt_number)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (assignment_id, lesson_id, correct, total, passed, n + 1),
            )
            if passed:
                cur.execute(
                    """
                    INSERT INTO trn_lesson_progress (assignment_id, lesson_id, completed_at)
                    VALUES (%s, %s, NOW())
                    ON DUPLICATE KEY UPDATE completed_at = NOW()
                    """,
                    (assignment_id, lesson_id),
                )
            conn.commit()
            _audit(
                "quiz_attempt",
                "trn_lessons",
                lesson_id,
                contractor_id,
                None,
                {"assignment_id": assignment_id, "passed": bool(passed), "pct": pct},
            )
            TrainingService.refresh_assignment_status(assignment_id)
            if passed:
                return True, f"Passed ({pct}%)."
            return False, f"Not passed ({pct}%). You can retry."
        except Exception as e:
            conn.rollback()
            return False, str(e)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def save_certificate(
        assignment_id: int,
        contractor_id: int,
        provider: Optional[str],
        certificate_number: Optional[str],
        issued_at: Optional[date],
        expires_at: Optional[date],
        file_path: Optional[str],
    ) -> Tuple[bool, str]:
        if not TrainingService._trn_tables_exist():
            return False, "Training system upgrading — try again later."
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id FROM trn_assignments WHERE id = %s AND contractor_id = %s",
                (assignment_id, contractor_id),
            )
            if not cur.fetchone():
                return False, "Not found."
            cur.execute("DELETE FROM trn_certificates WHERE assignment_id = %s", (assignment_id,))
            cur.execute(
                """
                INSERT INTO trn_certificates
                (assignment_id, provider, certificate_number, issued_at, expires_at, file_path)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    assignment_id,
                    (provider or "")[:255] or None,
                    (certificate_number or "")[:128] or None,
                    issued_at,
                    expires_at,
                    file_path,
                ),
            )
            conn.commit()
            _audit("certificate_upload", "trn_assignments", assignment_id, contractor_id, None, {})
            TrainingService.refresh_assignment_status(assignment_id)
            return True, "Evidence saved."
        except Exception as e:
            conn.rollback()
            return False, str(e)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def mark_complete(assignment_id: int, contractor_id: int, notes: Optional[str] = None) -> bool:
        """Legacy: single-click complete. Only for legacy schema or internal one-lesson flows."""
        if TrainingService._trn_tables_exist():
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute(
                    """
                    SELECT a.*, c.delivery_type FROM trn_assignments a
                    JOIN trn_courses c ON c.id = a.course_id
                    WHERE a.id = %s AND a.contractor_id = %s
                    """,
                    (assignment_id, contractor_id),
                )
                row = cur.fetchone()
                if not row:
                    return False
                dt = (row.get("delivery_type") or "").lower()
                if dt in ("external_required", "evidence_only"):
                    return False
                if dt == "internal_signoff":
                    return False
                # Mark all non-quiz lessons complete, then refresh
                cur.execute(
                    """
                    SELECT l.id, l.lesson_type FROM trn_lessons l
                    JOIN trn_modules m ON m.id = l.module_id
                    WHERE m.course_version_id = %s
                    """,
                    (int(row["course_version_id"]),),
                )
                for les in cur.fetchall() or []:
                    if (les.get("lesson_type") or "").lower() == "quiz":
                        continue
                    cur.execute(
                        """
                        INSERT INTO trn_lesson_progress (assignment_id, lesson_id, completed_at)
                        VALUES (%s, %s, NOW())
                        ON DUPLICATE KEY UPDATE completed_at = NOW()
                        """,
                        (assignment_id, int(les["id"])),
                    )
                if notes:
                    cur.execute(
                        "UPDATE trn_assignments SET notes = %s WHERE id = %s",
                        (notes[:500], assignment_id),
                    )
                conn.commit()
                TrainingService.refresh_assignment_status(assignment_id)
                return True
            except Exception:
                conn.rollback()
                return False
            finally:
                cur.close()
                conn.close()

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id FROM training_assignments WHERE id = %s AND contractor_id = %s",
                (assignment_id, contractor_id),
            )
            if not cur.fetchone():
                return False
            cur.execute(
                """
                INSERT INTO training_completions (assignment_id, notes)
                VALUES (%s, %s) ON DUPLICATE KEY UPDATE notes = VALUES(notes)
                """,
                (assignment_id, notes),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # Courses / admin CRUD
    # ------------------------------------------------------------------
    @staticmethod
    def list_courses(active_only: bool = False) -> List[Dict[str, Any]]:
        if not TrainingService._trn_tables_exist():
            return []
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            w = "active = 1" if active_only else "1=1"
            cur.execute(f"SELECT * FROM trn_courses WHERE {w} ORDER BY title")
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_course(course_id: int) -> Optional[Dict[str, Any]]:
        if not TrainingService._trn_tables_exist():
            return None
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM trn_courses WHERE id = %s", (course_id,))
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def ensure_course_version(course_id: int) -> int:
        """Return current published version id, creating v1 if missing."""
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT current_version_id FROM trn_courses WHERE id = %s", (course_id,))
            row = cur.fetchone()
            if row and row.get("current_version_id"):
                return int(row["current_version_id"])
            cur.execute(
                "SELECT id FROM trn_course_versions WHERE course_id = %s ORDER BY version DESC LIMIT 1",
                (course_id,),
            )
            v = cur.fetchone()
            if v:
                vid = int(v["id"])
                cur.execute("UPDATE trn_courses SET current_version_id = %s WHERE id = %s", (vid, course_id))
                conn.commit()
                return vid
            cur.execute(
                "INSERT INTO trn_course_versions (course_id, version, published) VALUES (%s, 1, 1)",
                (course_id,),
            )
            vid = cur.lastrowid
            cur.execute(
                "INSERT INTO trn_modules (course_version_id, sort_order, title) VALUES (%s, 0, 'Module 1')",
                (vid,),
            )
            cur.execute("UPDATE trn_courses SET current_version_id = %s WHERE id = %s", (vid, course_id))
            conn.commit()
            return int(vid)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_course(
        title: str,
        slug: Optional[str],
        summary: Optional[str],
        delivery_type: str,
        grace_days: int = 0,
        comp_policy_id: Optional[int] = None,
        require_certificate_verification: bool = True,
    ) -> int:
        slug = _slugify(slug or title)
        dt = delivery_type if delivery_type in DELIVERY_TYPES else "internal"
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO trn_courses
                (title, slug, summary, delivery_type, grace_days, comp_policy_id,
                 require_certificate_verification, active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
                """,
                (
                    title[:255],
                    slug[:120],
                    summary,
                    dt,
                    int(grace_days),
                    comp_policy_id,
                    1 if require_certificate_verification else 0,
                ),
            )
            cid = cur.lastrowid
            cur.execute(
                "INSERT INTO trn_course_versions (course_id, version, published) VALUES (%s, 1, 1)",
                (cid,),
            )
            vid = cur.lastrowid
            cur.execute(
                "INSERT INTO trn_modules (course_version_id, sort_order, title) VALUES (%s, 0, 'Module 1')",
                (vid,),
            )
            cur.execute("UPDATE trn_courses SET current_version_id = %s WHERE id = %s", (vid, cid))
            conn.commit()
            _audit("course_create", "trn_courses", cid, None, None, {"title": title})
            return int(cid)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_course(
        course_id: int,
        title: Optional[str] = None,
        slug: Optional[str] = None,
        summary: Optional[str] = None,
        delivery_type: Optional[str] = None,
        grace_days: Optional[int] = None,
        comp_policy_id: Optional[Any] = None,
        require_certificate_verification: Optional[bool] = None,
        active: Optional[bool] = None,
    ) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            parts = []
            params: List[Any] = []
            if title is not None:
                parts.append("title = %s")
                params.append(title[:255])
            if slug is not None:
                parts.append("slug = %s")
                params.append(_slugify(slug)[:120])
            if summary is not None:
                parts.append("summary = %s")
                params.append(summary)
            if delivery_type is not None and delivery_type in DELIVERY_TYPES:
                parts.append("delivery_type = %s")
                params.append(delivery_type)
            if grace_days is not None:
                parts.append("grace_days = %s")
                params.append(int(grace_days))
            if comp_policy_id is not None:
                parts.append("comp_policy_id = %s")
                params.append(comp_policy_id if comp_policy_id else None)
            if require_certificate_verification is not None:
                parts.append("require_certificate_verification = %s")
                params.append(1 if require_certificate_verification else 0)
            if active is not None:
                parts.append("active = %s")
                params.append(1 if active else 0)
            if not parts:
                return True
            params.append(course_id)
            cur.execute(f"UPDATE trn_courses SET {', '.join(parts)} WHERE id = %s", params)
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_modules(version_id: int) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM trn_modules WHERE course_version_id = %s ORDER BY sort_order, id",
                (version_id,),
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_lessons(module_id: int) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM trn_lessons WHERE module_id = %s ORDER BY sort_order, id",
                (module_id,),
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_lesson(
        module_id: int,
        title: str,
        lesson_type: str = "text",
        body_text: Optional[str] = None,
        external_url: Optional[str] = None,
        pass_mark_percent: Optional[int] = 80,
    ) -> int:
        lt = lesson_type if lesson_type in LESSON_TYPES else "text"
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM trn_lessons WHERE module_id = %s",
                (module_id,),
            )
            so = int((cur.fetchone() or [0])[0])
            cur.execute(
                """
                INSERT INTO trn_lessons
                (module_id, sort_order, lesson_type, title, body_text, external_url, pass_mark_percent)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    module_id,
                    so,
                    lt,
                    title[:255],
                    body_text,
                    external_url,
                    pass_mark_percent if lt == "quiz" else None,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_question(lesson_id: int, question_text: str) -> int:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM trn_questions WHERE lesson_id = %s",
                (lesson_id,),
            )
            so = int((cur.fetchone() or [0])[0])
            cur.execute(
                "INSERT INTO trn_questions (lesson_id, sort_order, question_text) VALUES (%s, %s, %s)",
                (lesson_id, so, question_text),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_option(question_id: int, option_text: str, is_correct: bool) -> int:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM trn_question_options WHERE question_id = %s",
                (question_id,),
            )
            so = int((cur.fetchone() or [0])[0])
            cur.execute(
                """
                INSERT INTO trn_question_options (question_id, sort_order, option_text, is_correct)
                VALUES (%s, %s, %s, %s)
                """,
                (question_id, so, option_text[:500], 1 if is_correct else 0),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def assign_contractor(
        course_id: int,
        contractor_id: int,
        due_date: Optional[date],
        mandatory: bool,
        assigned_by_user_id: Optional[int],
    ) -> int:
        vid = TrainingService.ensure_course_version(course_id)
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT grace_days FROM trn_courses WHERE id = %s", (course_id,))
            crow = cur.fetchone()
            gd = int(crow["grace_days"] or 0) if crow else 0
            grace_end = None
            if due_date and gd:
                grace_end = due_date + timedelta(days=gd)
            cur.execute(
                """
                INSERT INTO trn_assignments
                (course_id, course_version_id, contractor_id, status, due_date, grace_ends_at,
                 mandatory, assigned_by_user_id)
                VALUES (%s, %s, %s, 'assigned', %s, %s, %s, %s)
                """,
                (
                    course_id,
                    vid,
                    contractor_id,
                    due_date,
                    grace_end,
                    1 if mandatory else 0,
                    assigned_by_user_id,
                ),
            )
            aid = cur.lastrowid
            conn.commit()
            _audit(
                "assignment_create",
                "trn_assignments",
                aid,
                contractor_id,
                assigned_by_user_id,
                {"course_id": course_id},
            )
            return int(aid)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def assign_role(
        course_id: int,
        role_id: int,
        due_date: Optional[date],
        mandatory: bool,
        assigned_by_user_id: Optional[int],
    ) -> int:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            try:
                cur.execute(
                    "SELECT id FROM tb_contractors WHERE status = 'active' AND role_id = %s",
                    (int(role_id),),
                )
                ids = [int(r[0]) for r in (cur.fetchall() or [])]
            except Exception as e:
                logger.warning("assign_role: role_id column or query failed: %s", e)
                return 0
        finally:
            cur.close()
            conn.close()
        n = 0
        for cid in ids:
            TrainingService.assign_contractor(
                course_id, cid, due_date, mandatory, assigned_by_user_id
            )
            n += 1
        return n

    @staticmethod
    def admin_list_assignments(
        contractor_id: Optional[int] = None,
        course_id: Optional[int] = None,
        include_completed: bool = True,
    ) -> List[Dict[str, Any]]:
        if not TrainingService._trn_tables_exist():
            if contractor_id is None:
                return []
            return TrainingService._list_assignments_legacy(
                int(contractor_id), course_id, include_completed
            )
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = ["1=1"]
            params: List[Any] = []
            if contractor_id is not None:
                where.append("a.contractor_id = %s")
                params.append(contractor_id)
            if course_id is not None:
                where.append("a.course_id = %s")
                params.append(course_id)
            cur.execute(
                f"""
                SELECT a.*, c.title AS course_title, c.delivery_type AS course_delivery_type,
                       u.name AS contractor_name, u.email AS contractor_email
                FROM trn_assignments a
                JOIN trn_courses c ON c.id = a.course_id
                JOIN tb_contractors u ON u.id = a.contractor_id
                WHERE {" AND ".join(where)}
                ORDER BY a.assigned_at DESC
                LIMIT 500
                """,
                params,
            )
            rows = cur.fetchall() or []
            if not include_completed:
                rows = [r for r in rows if (r.get("status") or "") != STATUS_PASSED]
            return rows
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def verify_certificate(assignment_id: int, admin_user_id: int) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id FROM trn_certificates WHERE assignment_id = %s ORDER BY id DESC LIMIT 1",
                (assignment_id,),
            )
            crow = cur.fetchone()
            if not crow:
                return False
            cur.execute(
                """
                UPDATE trn_certificates SET verified_by_user_id = %s, verified_at = NOW()
                WHERE id = %s
                """,
                (admin_user_id, int(crow[0])),
            )
            conn.commit()
            _audit("certificate_verify", "trn_assignments", assignment_id, None, admin_user_id, {})
            TrainingService.refresh_assignment_status(assignment_id)
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_signoff(assignment_id: int, supervisor_user_id: int, comments: Optional[str]) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM trn_competency_signoffs WHERE assignment_id = %s", (assignment_id,))
            cur.execute(
                """
                INSERT INTO trn_competency_signoffs (assignment_id, supervisor_user_id, comments)
                VALUES (%s, %s, %s)
                """,
                (assignment_id, supervisor_user_id, comments),
            )
            conn.commit()
            _audit("signoff", "trn_assignments", assignment_id, None, supervisor_user_id, {})
            TrainingService.refresh_assignment_status(assignment_id)
            return True
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def grant_exemption(
        course_id: int,
        contractor_id: int,
        reason: str,
        granted_by_user_id: Optional[int],
        exempt_until: Optional[date],
    ) -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO trn_exemptions (course_id, contractor_id, reason, granted_by_user_id, exempt_until)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE reason = VALUES(reason), granted_by_user_id = VALUES(granted_by_user_id),
                  exempt_until = VALUES(exempt_until)
                """,
                (course_id, contractor_id, reason, granted_by_user_id, exempt_until),
            )
            conn.commit()
            cur.execute(
                "SELECT id FROM trn_assignments WHERE course_id = %s AND contractor_id = %s",
                (course_id, contractor_id),
            )
            for row in cur.fetchall() or []:
                TrainingService.refresh_assignment_status(int(row[0]))
            _audit("exemption", "trn_exemptions", course_id, contractor_id, granted_by_user_id, {})
            return True
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_completions(
        training_item_id: Optional[int] = None,
        contractor_id: Optional[int] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        if not TrainingService._trn_tables_exist():
            return TrainingService._list_completions_legacy(
                training_item_id, contractor_id, date_from, date_to
            )
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = ["a.status = 'passed'", "a.completed_at IS NOT NULL"]
            params: List[Any] = []
            if training_item_id is not None:
                where.append("a.course_id = %s")
                params.append(training_item_id)
            if contractor_id is not None:
                where.append("a.contractor_id = %s")
                params.append(contractor_id)
            if date_from is not None:
                where.append("a.completed_at >= %s")
                params.append(date_from)
            if date_to is not None:
                where.append("a.completed_at <= %s")
                params.append(date_to)
            cur.execute(
                f"""
                SELECT a.id AS assignment_id, a.completed_at, a.notes,
                       a.contractor_id, a.course_id AS training_item_id, a.due_date,
                       c.title AS item_title, u.name AS contractor_name
                FROM trn_assignments a
                JOIN trn_courses c ON c.id = a.course_id
                JOIN tb_contractors u ON u.id = a.contractor_id
                WHERE {" AND ".join(where)}
                ORDER BY a.completed_at DESC
                LIMIT 500
                """,
                params,
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _list_completions_legacy(
        training_item_id: Optional[int],
        contractor_id: Optional[int],
        date_from: Optional[date],
        date_to: Optional[date],
    ) -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            where = ["1=1"]
            params: List[Any] = []
            if training_item_id is not None:
                where.append("a.training_item_id = %s")
                params.append(training_item_id)
            if contractor_id is not None:
                where.append("a.contractor_id = %s")
                params.append(contractor_id)
            if date_from is not None:
                where.append("c.completed_at >= %s")
                params.append(date_from)
            if date_to is not None:
                where.append("c.completed_at <= %s")
                params.append(date_to)
            cur.execute(
                f"""
                SELECT c.id, c.assignment_id, c.completed_at, c.notes,
                       a.contractor_id, a.training_item_id, a.due_date,
                       t.title AS item_title, u.name AS contractor_name
                FROM training_completions c
                JOIN training_assignments a ON a.id = c.assignment_id
                JOIN training_items t ON t.id = a.training_item_id
                JOIN tb_contractors u ON u.id = a.contractor_id
                WHERE {" AND ".join(where)}
                ORDER BY c.completed_at DESC
                """,
                params,
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_contractors() -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, name, email, initials, role_id FROM tb_contractors WHERE status = 'active' ORDER BY name"
            )
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_roles() -> List[Dict[str, Any]]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT id, name FROM roles ORDER BY name")
            return cur.fetchall() or []
        except Exception:
            return []
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # Admin dashboard overview metrics
    # ------------------------------------------------------------------
    @staticmethod
    def admin_overview_metrics(limit_recent: int = 6) -> Dict[str, Any]:
        """
        Small KPI snapshot for the training admin dashboard.
        Intended to be safe to call even during early installs.
        """
        if not TrainingService._trn_tables_exist():
            return TrainingService._admin_overview_metrics_legacy(limit_recent=limit_recent)
        return TrainingService._admin_overview_metrics_trn(limit_recent=limit_recent)

    @staticmethod
    def _admin_overview_metrics_trn(limit_recent: int = 6) -> Dict[str, Any]:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT COUNT(*) AS c FROM trn_courses")
            total_courses = int((cur.fetchone() or {}).get("c") or 0)

            cur.execute("SELECT COUNT(*) AS c FROM trn_assignments")
            total_assignments = int((cur.fetchone() or {}).get("c") or 0)

            placeholders = ",".join(["%s"] * len(PENDING_STATUSES))
            pending_params: Tuple[Any, ...] = tuple(PENDING_STATUSES)

            cur.execute(
                f"SELECT COUNT(*) AS c FROM trn_assignments WHERE status IN ({placeholders})",
                pending_params,
            )
            pending_count = int((cur.fetchone() or {}).get("c") or 0)

            cur.execute(
                "SELECT COUNT(*) AS c FROM trn_assignments WHERE status = %s",
                (STATUS_PASSED,),
            )
            completed_count = int((cur.fetchone() or {}).get("c") or 0)

            cur.execute(
                "SELECT COUNT(*) AS c FROM trn_assignments WHERE status = %s",
                (STATUS_EXEMPT,),
            )
            exempt_count = int((cur.fetchone() or {}).get("c") or 0)

            cur.execute(
                "SELECT COUNT(*) AS c FROM trn_assignments WHERE status = %s",
                ("awaiting_signoff",),
            )
            awaiting_signoff_count = int((cur.fetchone() or {}).get("c") or 0)

            cur.execute(
                """
                SELECT COUNT(*) AS c
                FROM trn_assignments
                WHERE status IN ('awaiting_external_evidence', 'pending_evidence_review')
                """,
            )
            awaiting_external_evidence_count = int((cur.fetchone() or {}).get("c") or 0)

            cur.execute(
                f"""
                SELECT COUNT(*) AS c
                FROM trn_assignments a
                WHERE a.status IN ({placeholders})
                  AND a.due_date IS NOT NULL
                  AND (
                    (a.grace_ends_at IS NOT NULL AND CURDATE() > a.grace_ends_at)
                    OR
                    (a.grace_ends_at IS NULL AND CURDATE() > a.due_date)
                  )
                """,
                pending_params,
            )
            overdue_count = int((cur.fetchone() or {}).get("c") or 0)

            cur.execute(
                f"""
                SELECT
                  a.id,
                  a.status,
                  a.due_date,
                  a.grace_ends_at,
                  a.assigned_at,
                  a.mandatory,
                  c.title AS course_title,
                  u.name AS contractor_name,
                  CASE
                    WHEN a.due_date IS NULL THEN 0
                    WHEN a.status NOT IN ({placeholders}) THEN 0
                    WHEN
                      (
                        (a.grace_ends_at IS NOT NULL AND CURDATE() > a.grace_ends_at)
                        OR
                        (a.grace_ends_at IS NULL AND CURDATE() > a.due_date)
                      )
                    THEN 1 ELSE 0
                  END AS is_overdue
                FROM trn_assignments a
                JOIN trn_courses c ON c.id = a.course_id
                JOIN tb_contractors u ON u.id = a.contractor_id
                ORDER BY a.assigned_at DESC
                LIMIT {int(limit_recent)}
                """,
                pending_params,
            )
            rows = cur.fetchall() or []
            recent_assignments: List[Dict[str, Any]] = []
            for r in rows:
                recent_assignments.append(
                    {
                        "id": int(r.get("id")),
                        "status": r.get("status"),
                        "due_date": r.get("due_date"),
                        "assigned_at": r.get("assigned_at"),
                        "course_title": r.get("course_title"),
                        "contractor_name": r.get("contractor_name"),
                        "is_overdue": bool(r.get("is_overdue")),
                    }
                )

            return {
                "trn_ready": True,
                "total_courses": total_courses,
                "total_assignments": total_assignments,
                "pending_count": pending_count,
                "awaiting_signoff_count": awaiting_signoff_count,
                "awaiting_external_evidence_count": awaiting_external_evidence_count,
                "overdue_count": overdue_count,
                "completed_count": completed_count,
                "exempt_count": exempt_count,
                "recent_assignments": recent_assignments,
            }
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _admin_overview_metrics_legacy(limit_recent: int = 6) -> Dict[str, Any]:
        """
        Best-effort legacy metrics when trn_* tables aren't ready.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            # Legacy "courses" = training_items
            cur.execute("SELECT COUNT(*) AS c FROM training_items")
            total_courses = int((cur.fetchone() or {}).get("c") or 0)

            cur.execute("SELECT COUNT(*) AS c FROM training_assignments")
            total_assignments = int((cur.fetchone() or {}).get("c") or 0)

            cur.execute(
                """
                SELECT COUNT(*) AS c
                FROM training_assignments a
                WHERE NOT EXISTS (SELECT 1 FROM training_completions c WHERE c.assignment_id = a.id)
                """
            )
            pending_count = int((cur.fetchone() or {}).get("c") or 0)

            cur.execute("SELECT COUNT(*) AS c FROM training_completions")
            completed_count = int((cur.fetchone() or {}).get("c") or 0)

            # Legacy doesn't model signoff/external/evidence states.
            awaiting_signoff_count = 0
            awaiting_external_evidence_count = 0
            exempt_count = 0

            cur.execute(
                """
                SELECT COUNT(*) AS c
                FROM training_assignments a
                WHERE a.due_date IS NOT NULL
                  AND a.due_date < CURDATE()
                  AND NOT EXISTS (SELECT 1 FROM training_completions c WHERE c.assignment_id = a.id)
                """
            )
            overdue_count = int((cur.fetchone() or {}).get("c") or 0)

            cur.execute(
                """
                SELECT
                  a.id,
                  (SELECT 1 FROM training_completions c WHERE c.assignment_id = a.id LIMIT 1) AS completed,
                  a.due_date,
                  a.assigned_at,
                  t.title AS course_title,
                  u.name AS contractor_name
                FROM training_assignments a
                JOIN training_items t ON t.id = a.training_item_id
                JOIN tb_contractors u ON u.id = a.contractor_id
                ORDER BY a.assigned_at DESC
                LIMIT %s
                """,
                (int(limit_recent),),
            )
            rows = cur.fetchall() or []
            recent_assignments: List[Dict[str, Any]] = []
            for r in rows:
                is_overdue = bool(r.get("due_date") and r.get("due_date") < date.today() and not r.get("completed"))
                status = "passed" if r.get("completed") else "assigned"
                recent_assignments.append(
                    {
                        "id": int(r.get("id")),
                        "status": status,
                        "due_date": r.get("due_date"),
                        "assigned_at": r.get("assigned_at"),
                        "course_title": r.get("course_title"),
                        "contractor_name": r.get("contractor_name"),
                        "is_overdue": is_overdue,
                    }
                )

            return {
                "trn_ready": False,
                "total_courses": total_courses,
                "total_assignments": total_assignments,
                "pending_count": pending_count,
                "awaiting_signoff_count": awaiting_signoff_count,
                "awaiting_external_evidence_count": awaiting_external_evidence_count,
                "overdue_count": overdue_count,
                "completed_count": completed_count,
                "exempt_count": exempt_count,
                "recent_assignments": recent_assignments,
            }
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # Contractor auto-assign rules (by role)
    # ------------------------------------------------------------------

    @staticmethod
    def list_course_assignment_rules(course_id: int) -> List[Dict[str, Any]]:
        """
        Auto-assign rules for a course (e.g. assign training to contractors by role).
        """
        if not TrainingService._trn_tables_exist():
            return []
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT r.id, r.course_id, r.role_id, rl.name AS role_name,
                       r.due_date_offset_days, r.mandatory, r.active, r.created_at
                FROM trn_course_assignment_rules r
                LEFT JOIN roles rl ON rl.id = r.role_id
                WHERE r.course_id = %s
                ORDER BY r.active DESC, r.role_id IS NULL ASC, rl.name ASC, r.id DESC
                """,
                (int(course_id),),
            )
            return cur.fetchall() or []
        except Exception:
            return []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_course_assignment_rule(
        course_id: int,
        role_id: Optional[int],
        due_date_offset_days: Optional[int],
        mandatory: bool,
        active: bool = True,
        created_by_user_id: Optional[int] = None,
    ) -> bool:
        """
        Upsert rule for a specific (course_id, role_id).
        - role_id can be NULL to mean "all roles" (not used by our v1 admin form, but supported)
        """
        if not TrainingService._trn_tables_exist():
            return False
        if role_id is not None:
            try:
                role_id = int(role_id)
            except Exception:
                role_id = None
        if due_date_offset_days is not None:
            try:
                due_date_offset_days = int(due_date_offset_days)
            except Exception:
                due_date_offset_days = None
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # If multiple rules exist for same role_id, we keep the newest.
            cur.execute(
                """
                INSERT INTO trn_course_assignment_rules
                  (course_id, role_id, due_date_offset_days, mandatory, active)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    int(course_id),
                    role_id,
                    due_date_offset_days,
                    1 if mandatory else 0,
                    1 if active else 0,
                ),
            )
            conn.commit()
            return cur.rowcount >= 0
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_course_assignment_rule(rule_id: int) -> bool:
        if not TrainingService._trn_tables_exist():
            return False
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM trn_course_assignment_rules WHERE id = %s", (int(rule_id),))
            conn.commit()
            return cur.rowcount > 0
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def apply_role_assignment_rules(
        contractor_id: int,
        role_id: Optional[int],
        assigned_by_user_id: Optional[int] = None,
    ) -> int:
        """
        Apply active rules for the given contractor role.
        Creates assignments only if the contractor does not already have a non-passed/non-exempt assignment
        for that course.
        Returns number of assignments created.
        """
        if not TrainingService._trn_tables_exist() or not role_id:
            return 0
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        created = 0
        try:
            cur.execute(
                """
                SELECT course_id, due_date_offset_days, mandatory
                FROM trn_course_assignment_rules
                WHERE active = 1 AND role_id = %s
                """,
                (int(role_id),),
            )
            rules = cur.fetchall() or []
            for rule in rules:
                course_id = int(rule.get("course_id"))
                # Idempotency: if any active/non-compliant assignment exists for this course, don't duplicate.
                cur.execute(
                    """
                    SELECT id FROM trn_assignments
                    WHERE contractor_id = %s AND course_id = %s
                      AND status NOT IN (%s, %s)
                    LIMIT 1
                    """,
                    (int(contractor_id), course_id, STATUS_PASSED, STATUS_EXEMPT),
                )
                if cur.fetchone():
                    continue

                due_date = None
                off = rule.get("due_date_offset_days")
                if off is not None:
                    try:
                        due_date = date.today() + timedelta(days=int(off))
                    except Exception:
                        due_date = None
                TrainingService.assign_contractor(
                    course_id=course_id,
                    contractor_id=int(contractor_id),
                    due_date=due_date,
                    mandatory=bool(rule.get("mandatory")),
                    assigned_by_user_id=assigned_by_user_id,
                )
                created += 1
            return created
        except Exception as e:
            logger.warning("TrainingService.apply_role_assignment_rules failed: %s", e)
            return 0
        finally:
            cur.close()
            conn.close()

    # Legacy item API used by old admin templates — map to courses
    @staticmethod
    def list_items(active_only: bool = True) -> List[Dict[str, Any]]:
        if TrainingService._trn_tables_exist():
            rows = TrainingService.list_courses(active_only=active_only)
            return [
                {
                    "id": r["id"],
                    "title": r["title"],
                    "slug": r["slug"],
                    "summary": r.get("summary"),
                    "content": None,
                    "item_type": "document",
                    "external_url": None,
                    "active": r.get("active"),
                    "delivery_type": r.get("delivery_type"),
                }
                for r in rows
            ]
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            w = "active = 1" if active_only else "1=1"
            cur.execute(f"SELECT * FROM training_items WHERE {w} ORDER BY title")
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_item(item_id: int) -> Optional[Dict[str, Any]]:
        if TrainingService._trn_tables_exist():
            c = TrainingService.get_course(item_id)
            if not c:
                return None
            return {
                "id": c["id"],
                "title": c["title"],
                "slug": c["slug"],
                "summary": c.get("summary"),
                "content": None,
                "item_type": "document",
                "external_url": None,
                "active": c.get("active"),
                "delivery_type": c.get("delivery_type"),
            }
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM training_items WHERE id = %s", (item_id,))
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_item(
        title: str,
        slug: str,
        summary: Optional[str] = None,
        content: Optional[str] = None,
        item_type: str = "document",
        external_url: Optional[str] = None,
    ) -> int:
        if TrainingService._trn_tables_exist():
            return TrainingService.create_course(
                title=title,
                slug=slug,
                summary=summary,
                delivery_type="internal",
            )
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO training_items (title, slug, summary, content, item_type, external_url, active)
                VALUES (%s, %s, %s, %s, %s, %s, 1)
                """,
                (title, slug, summary, content, item_type, external_url),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_item(
        item_id: int,
        title: Optional[str] = None,
        slug: Optional[str] = None,
        summary: Optional[str] = None,
        content: Optional[str] = None,
        item_type: Optional[str] = None,
        external_url: Optional[str] = None,
        active: Optional[bool] = None,
    ) -> bool:
        if TrainingService._trn_tables_exist():
            return TrainingService.update_course(
                item_id,
                title=title,
                slug=slug,
                summary=summary,
                active=active,
            )
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            updates = []
            params: List[Any] = []
            for k, v in [
                ("title", title),
                ("slug", slug),
                ("summary", summary),
                ("content", content),
                ("item_type", item_type),
                ("external_url", external_url),
            ]:
                if v is not None:
                    updates.append(f"{k} = %s")
                    params.append(v)
            if active is not None:
                updates.append("active = %s")
                params.append(1 if active else 0)
            if not updates:
                return True
            params.append(item_id)
            cur.execute(f"UPDATE training_items SET {', '.join(updates)} WHERE id = %s", params)
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def add_assignment(
        training_item_id: int,
        contractor_id: int,
        due_date: Optional[date] = None,
        mandatory: bool = False,
        assigned_by_user_id: Optional[int] = None,
    ) -> int:
        if TrainingService._trn_tables_exist():
            return TrainingService.assign_contractor(
                training_item_id, contractor_id, due_date, mandatory, assigned_by_user_id
            )
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO training_assignments (training_item_id, contractor_id, due_date, mandatory, assigned_by_user_id)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (training_item_id, contractor_id, due_date, 1 if mandatory else 0, assigned_by_user_id),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # Person competencies (HR-linked: skills, qualifications + files, clinical grade)
    # ------------------------------------------------------------------

    COMPETENCY_KINDS = frozenset({"skill", "qualification", "clinical_grade"})

    @staticmethod
    def person_competencies_table_exists() -> bool:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'trn_person_competencies'")
            return bool(cur.fetchone())
        except Exception:
            return False
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def list_person_competencies(contractor_id: int) -> List[Dict[str, Any]]:
        if not contractor_id or not TrainingService.person_competencies_table_exists():
            return []
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT id, contractor_id, competency_kind, label, use_hr_job_title,
                       file_path, issued_on, expires_on, notes, created_by_user_id,
                       created_at, updated_at
                FROM trn_person_competencies
                WHERE contractor_id = %s
                ORDER BY competency_kind ASC, label ASC, id ASC
                """,
                (int(contractor_id),),
            )
            return list(cur.fetchall() or [])
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _row_expired(row: Dict[str, Any]) -> bool:
        exp = row.get("expires_on")
        if exp is None:
            return False
        try:
            if isinstance(exp, datetime):
                exp_d = exp.date()
            elif isinstance(exp, date):
                exp_d = exp
            else:
                exp_d = date.fromisoformat(str(exp)[:10])
        except Exception:
            return False
        return exp_d < date.today()

    @staticmethod
    def add_person_competency(
        contractor_id: int,
        competency_kind: str,
        label: str,
        *,
        use_hr_job_title: bool = False,
        file_path: Optional[str] = None,
        issued_on: Optional[date] = None,
        expires_on: Optional[date] = None,
        notes: Optional[str] = None,
        created_by_user_id: Optional[int] = None,
    ) -> Optional[int]:
        if not contractor_id or not TrainingService.person_competencies_table_exists():
            return None
        kind = (competency_kind or "").strip().lower()
        if kind not in TrainingService.COMPETENCY_KINDS:
            return None
        lab = (label or "").strip()
        if kind != "clinical_grade" and not lab:
            return None
        if kind == "clinical_grade" and not lab and not use_hr_job_title:
            return None
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO trn_person_competencies
                  (contractor_id, competency_kind, label, use_hr_job_title,
                   file_path, issued_on, expires_on, notes, created_by_user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    int(contractor_id),
                    kind,
                    lab if lab else "",
                    1 if use_hr_job_title else 0,
                    file_path,
                    issued_on,
                    expires_on,
                    (notes or "").strip() or None,
                    int(created_by_user_id) if created_by_user_id else None,
                ),
            )
            conn.commit()
            _audit(
                "person_competency_create",
                "trn_person_competencies",
                int(cur.lastrowid),
                contractor_id=int(contractor_id),
                actor_user_id=created_by_user_id,
                payload={"kind": kind, "label": lab},
            )
            rid = int(cur.lastrowid)
            TrainingService._sync_ventus_crew_profile_after_competency_change(
                int(contractor_id))
            return rid
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _sync_ventus_crew_profile_after_competency_change(contractor_id: int) -> None:
        try:
            from app.plugins.hr_module.services import (
                sync_ventus_crew_profile_from_hr_training,
            )

            sync_ventus_crew_profile_from_hr_training(int(contractor_id))
        except Exception:
            pass

    @staticmethod
    def delete_person_competency(competency_id: int, contractor_id: int) -> bool:
        if not competency_id or not contractor_id:
            return False
        if not TrainingService.person_competencies_table_exists():
            return False
        app_static = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "static"))
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT file_path FROM trn_person_competencies WHERE id = %s AND contractor_id = %s",
                (int(competency_id), int(contractor_id)),
            )
            row = cur.fetchone() or {}
            fp = row.get("file_path")
            cur.execute(
                "DELETE FROM trn_person_competencies WHERE id = %s AND contractor_id = %s",
                (int(competency_id), int(contractor_id)),
            )
            conn.commit()
            ok = cur.rowcount > 0
            if ok and fp:
                rel = str(fp).replace("\\", "/").lstrip("/")
                if ".." not in rel.split("/") and "training_competencies" in rel:
                    full = os.path.abspath(os.path.join(app_static, *rel.split("/")))
                    root = os.path.abspath(app_static)
                    if full.startswith(root) and os.path.isfile(full):
                        try:
                            os.remove(full)
                        except OSError:
                            pass
            if ok:
                TrainingService._sync_ventus_crew_profile_after_competency_change(
                    int(contractor_id))
            return ok
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def cad_merge_competencies_into_crew_entry(
        cur,
        crew_entry: Dict[str, Any],
        contractor_id: int,
        job_title: Optional[str],
    ) -> None:
        """Merge Training competencies into crew_entry for CAD API (skills, qualifications, clinical_grade)."""
        if not contractor_id or not TrainingService.person_competencies_table_exists():
            return
        try:
            cur.execute(
                """
                SELECT id, competency_kind, label, use_hr_job_title, file_path, issued_on, expires_on, notes
                FROM trn_person_competencies
                WHERE contractor_id = %s
                ORDER BY id ASC
                """,
                (int(contractor_id),),
            )
            rows = cur.fetchall() or []
        except Exception:
            return

        def _stringify_items(lst: Any) -> List[str]:
            out: List[str] = []
            if not isinstance(lst, list):
                return out
            for x in lst:
                if isinstance(x, dict):
                    s = str(x.get("label") or x.get("name") or x.get("grade") or "").strip()
                else:
                    s = str(x).strip()
                if s:
                    out.append(s)
            return out

        def _uniq_extend(dst: List[str], items: List[str]) -> None:
            seen = {str(x).strip().lower() for x in dst if str(x).strip()}
            for it in items:
                s = str(it).strip()
                if not s:
                    continue
                k = s.lower()
                if k in seen:
                    continue
                seen.add(k)
                dst.append(s)

        skills = _stringify_items(crew_entry.get("skills"))
        quals = _stringify_items(crew_entry.get("qualifications"))
        qual_details: List[Dict[str, Any]] = list(crew_entry.get("qualification_records") or [])
        if not isinstance(qual_details, list):
            qual_details = []

        clinical: Optional[str] = None
        jt = (job_title or "").strip() or None

        for r in rows:
            row = dict(r) if not isinstance(r, dict) else r
            if TrainingService._row_expired(row):
                continue
            kind = (row.get("competency_kind") or "").strip().lower()
            label = (row.get("label") or "").strip()
            use_hr = bool(row.get("use_hr_job_title"))
            if kind == "skill" and label:
                _uniq_extend(skills, [label])
            elif kind == "qualification" and label:
                _uniq_extend(quals, [label])
                qual_details.append({
                    "label": label,
                    "file_path": row.get("file_path"),
                    "issued_on": row.get("issued_on"),
                    "expires_on": row.get("expires_on"),
                    "notes": row.get("notes"),
                })
            elif kind == "clinical_grade":
                if use_hr and jt and label:
                    clinical = f"{jt} · {label}"
                elif use_hr and jt:
                    clinical = jt
                elif label:
                    clinical = label

        crew_entry["skills"] = skills
        crew_entry["qualifications"] = quals
        if qual_details:
            crew_entry["qualification_records"] = qual_details
        if clinical:
            crew_entry["clinical_grade"] = clinical

    @staticmethod
    def dispatch_skill_tokens_for_contractor(
        cur,
        contractor_id: int,
        job_title: Optional[str],
        base_tokens: set,
    ) -> set:
        """Extend base_tokens (lowercase) with Training competencies for dispatch matching."""
        out = set(base_tokens)
        if not contractor_id or not TrainingService.person_competencies_table_exists():
            return out
        try:
            cur.execute(
                """
                SELECT competency_kind, label, use_hr_job_title, expires_on
                FROM trn_person_competencies
                WHERE contractor_id = %s
                """,
                (int(contractor_id),),
            )
            rows = cur.fetchall() or []
        except Exception:
            return out
        jt = (job_title or "").strip() or None
        for r in rows:
            row = dict(r) if not isinstance(r, dict) else r
            if TrainingService._row_expired(row):
                continue
            kind = (row.get("competency_kind") or "").strip().lower()
            label = (row.get("label") or "").strip()
            use_hr = bool(row.get("use_hr_job_title"))
            if kind == "skill" and label:
                out.add(label.lower())
            elif kind == "qualification" and label:
                out.add(label.lower())
            elif kind == "clinical_grade":
                if use_hr and jt:
                    out.add(jt.lower())
                if label:
                    out.add(label.lower())
                if use_hr and jt and label:
                    out.add(f"{jt} · {label}".lower())
        return out
