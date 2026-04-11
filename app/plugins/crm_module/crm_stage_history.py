"""Opportunity stage change audit — separate DB connection so logging never poisons main transactions."""
from __future__ import annotations

from app.objects import get_db_connection


def log_opportunity_stage_change(
    opportunity_id: int,
    from_stage: str | None,
    to_stage: str,
    changed_by: str | None,
) -> None:
    if from_stage is not None and from_stage == to_stage:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO crm_opportunity_stage_history
            (opportunity_id, from_stage, to_stage, changed_by)
            VALUES (%s,%s,%s,%s)
            """,
            (opportunity_id, from_stage, to_stage, changed_by),
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()
