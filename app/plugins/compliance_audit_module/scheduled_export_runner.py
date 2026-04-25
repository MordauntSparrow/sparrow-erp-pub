"""
Scheduled governed exports (PRD P3). Writes ZIP to uploads; logs row with pin_step_up_ok=0, trigger_type=scheduled.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _app_static_uploads() -> Path:
    app_dir = Path(__file__).resolve().parent.parent.parent
    d = app_dir / "static" / "uploads" / "compliance_exports"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _purge_old_exports(max_age_hours: int = 72) -> None:
    root = _app_static_uploads()
    cutoff = datetime.now(timezone.utc).timestamp() - max(1, int(max_age_hours)) * 3600
    for p in root.rglob("*.zip"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink(missing_ok=True)
        except OSError:
            pass


def _execute_one_job(job: dict[str, Any], *, now_utc: datetime) -> None:
    from app.objects import get_db_connection

    from .adapters import ALL_DOMAIN_KEYS, merge_timeline
    from .redaction import apply_redaction_profile
    from .services import (
        GENERATOR_VERSION,
        build_evidence_zip_bytes,
        build_pdf_bytes,
        events_to_csv_bytes,
        events_to_json_bytes,
        insert_export_log,
        manifest_dict,
        sha256_bytes,
    )

    lookback = max(1, min(int(job.get("lookback_days") or 1), 90))
    date_to = now_utc.replace(tzinfo=None)
    date_from = date_to - timedelta(days=lookback)

    domains_raw = job.get("domains_json") or "[]"
    try:
        dom_list = json.loads(domains_raw) if isinstance(domains_raw, str) else domains_raw
        if isinstance(dom_list, list):
            domains = set(dom_list) & set(ALL_DOMAIN_KEYS)
        else:
            domains = None
    except Exception:
        domains = None

    row_cap = max(100, min(int(job.get("row_cap") or 8000), 50000))

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        events = merge_timeline(
            cur,
            domains=domains,
            date_from=date_from,
            date_to=date_to,
            limit=row_cap,
            per_domain_limit=row_cap,
        )
    finally:
        cur.close()
        conn.close()

    if len(events) == 0:
        logger.warning(
            "Scheduled compliance export job %s produced zero rows (check domains / date window / data).",
            job.get("id"),
        )

    profile = (job.get("redaction_profile") or "standard").strip().lower()
    events = apply_redaction_profile(events, profile)

    dom_store = sorted(domains) if domains else sorted(ALL_DOMAIN_KEYS)
    scope = {
        "format": "zip",
        "scheduled_job_id": job.get("id"),
        "label": job.get("label"),
        "lookback_days": lookback,
        "domains": dom_store,
        "redaction_profile": profile,
        "trigger": "scheduled",
    }
    man = manifest_dict(filters=scope, row_count=len(events), generator_version=GENERATOR_VERSION)
    pdf_b = build_pdf_bytes(events, manifest=man, watermark="SCHEDULED EXPORT")
    csv_b = events_to_csv_bytes(events)
    json_b = events_to_json_bytes(events, manifest=man)
    zip_b = build_evidence_zip_bytes(man, pdf_b, csv_b, json_b)

    digest = sha256_bytes(zip_b)
    ts = now_utc.strftime("%Y%m%d_%H%M%S")
    fn = f"job{job['id']}_{ts}_{digest[:10]}.zip"
    dest = _app_static_uploads() / f"job_{job['id']}"
    dest.mkdir(parents=True, exist_ok=True)
    fpath = dest / fn
    fpath.write_bytes(zip_b)
    rel_path = f"uploads/compliance_exports/job_{job['id']}/{fn}".replace("\\", "/")

    insert_export_log(
        user_id=None,
        export_format="zip",
        scope=scope,
        row_count=len(events),
        ip=None,
        pin_ok=False,
        file_hash=digest,
        trigger_type="scheduled",
        stored_path=rel_path,
    )

    today = now_utc.date()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE compliance_scheduled_export_jobs
            SET last_run_on_date = %s, last_run_at = %s, last_status = %s,
                last_error = NULL, last_file_path = %s, last_file_hash = %s
            WHERE id = %s
            """,
            (today, datetime.now(), "ok", rel_path, digest, int(job["id"])),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def run_due_scheduled_exports(*, force: bool = False) -> None:
    from app.objects import get_db_connection

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT 1 FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'compliance_scheduled_export_jobs'
            """
        )
        if not cur.fetchone():
            return

        cur.execute(
            "SELECT * FROM compliance_scheduled_export_jobs WHERE enabled = 1 ORDER BY id ASC"
        )
        jobs = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    hour = now_utc.hour

    for job in jobs:
        try:
            if not force:
                if int(job.get("run_hour_utc", -1)) != hour:
                    continue
                last_d = job.get("last_run_on_date")
                if last_d is not None and isinstance(last_d, date) and last_d == today:
                    continue
                if isinstance(last_d, str) and last_d == str(today):
                    continue
            _execute_one_job(job, now_utc=now_utc)
        except Exception as e:
            logger.exception("Scheduled export job %s failed", job.get("id"))
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE compliance_scheduled_export_jobs
                    SET last_run_at = %s, last_status = %s, last_error = %s
                    WHERE id = %s
                    """,
                    (datetime.now(), "error", str(e)[:2000], int(job["id"])),
                )
                conn.commit()
                cur.close()
                conn.close()
            except Exception:
                pass

    try:
        _purge_old_exports(72)
    except Exception:
        logger.debug("purge compliance exports", exc_info=True)


def run_all_scheduled_exports_now() -> None:
    """Run every enabled job immediately (manual trigger; ignores hour / once-per-day guard)."""
    run_due_scheduled_exports(force=True)
