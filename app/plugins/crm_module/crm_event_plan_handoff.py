"""
Medical / Cura handoff scaffold (PRD section 6).

Contract with ``medical_records_module`` is still TBD. This module builds a JSON-serialisable
payload, writes an audit row, and optionally simulates success when
``SPARROW_CRM_EVENT_HANDOFF_MODE=simulate`` is set (for QA).

Env:
- ``SPARROW_CRM_EVENT_HANDOFF_MODE``: unset / ``off`` → log only, status skipped_disabled.
- ``simulate`` → sets plan handoff fields to a synthetic external ref (no Cura call).
- ``dry_run`` → logs payload summary in handoff log, does not claim sync.
- ``live`` → calls ``medical_records_module.cura_crm_event_plan_handoff.sync_crm_event_plan_to_cura`` (requires
  that plugin and DB tables). Pass the Flask app into ``process_event_plan_handoff`` so PDF paths resolve.

- ``SPARROW_CRM_HANDOFF_REQUIRE_PDF``: when set to ``1`` / ``true`` / ``yes`` / ``on``, ``live`` handoff fails if
  the plan has no stored PDF row (strict production guard).

- ``SPARROW_CRM_HANDOFF_REQUIRE_QUOTE_ACCEPTED``: default ``1`` / ``true`` — when this plan has a ``quote_id``,
  **Send to Cura** (UI or API) is blocked until that quote's status is ``accepted`` (client agreed pricing).
  Set to ``0`` / ``false`` / ``off`` to skip (e.g. QA or opportunity-only plans with stale quote links).

Handoff is **only** triggered by the explicit **Send to Cura** action (no automatic sync on quote save).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from app.objects import get_db_connection

from .crm_clinical_handover import parse_clinical_handover, summary_lines_for_handoff

logger = logging.getLogger(__name__)

HANDOFF_ENV_MODE = "SPARROW_CRM_EVENT_HANDOFF_MODE"
HANDOFF_REQUIRE_PDF_ENV = "SPARROW_CRM_HANDOFF_REQUIRE_PDF"
HANDOFF_REQUIRE_QUOTE_ACCEPTED_ENV = "SPARROW_CRM_HANDOFF_REQUIRE_QUOTE_ACCEPTED"


def handoff_mode() -> str:
    return (os.environ.get(HANDOFF_ENV_MODE) or "off").strip().lower()


def build_handoff_payload(
    plan: dict[str, Any],
    *,
    latest_pdf_path: str | None,
    latest_pdf_hash: str | None,
) -> dict[str, Any]:
    """Shape aligned with PRD 6.2 (extensible; attachment is path + hash until URL contract exists)."""
    loc_parts = [
        plan.get("address_line1"),
        plan.get("postcode"),
        plan.get("what3words"),
    ]
    location_summary = ", ".join(str(x).strip() for x in loc_parts if x and str(x).strip())
    desc_parts = []
    if plan.get("demographics_notes"):
        desc_parts.append(str(plan["demographics_notes"]).strip())
    if plan.get("environment_notes"):
        desc_parts.append(str(plan["environment_notes"]).strip())
    ch_lines = summary_lines_for_handoff(parse_clinical_handover(plan.get("clinical_handover_json")))
    if ch_lines:
        desc_parts.append("\n".join(ch_lines))
    description = "\n\n".join(desc_parts) if desc_parts else None

    def _iso(v: Any) -> Any:
        if v is None:
            return None
        iso = getattr(v, "isoformat", None)
        if callable(iso):
            return iso()
        return v

    return {
        "crm_event_plan_id": plan.get("id"),
        "title": plan.get("title"),
        "description": description,
        "location_summary": location_summary or None,
        "start": _iso(plan.get("start_datetime")),
        "end": _iso(plan.get("end_datetime")),
        "quote_id": plan.get("quote_id"),
        "opportunity_id": plan.get("opportunity_id"),
        "account_id": plan.get("account_id"),
        "attachment": {
            "relative_static_path": latest_pdf_path,
            "sha256": latest_pdf_hash,
        }
        if latest_pdf_path or latest_pdf_hash
        else None,
        "payload_version": 1,
    }


def _latest_pdf_row(conn, plan_id: int) -> dict[str, Any] | None:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """SELECT file_path, pdf_hash FROM crm_event_plan_pdfs
            WHERE plan_id=%s ORDER BY id DESC LIMIT 1""",
            (plan_id,),
        )
        return cur.fetchone()
    finally:
        cur.close()


def _insert_handoff_log(
    conn,
    *,
    plan_id: int,
    trigger: str,
    status: str,
    detail: str | None,
    pdf_hash: str | None,
    external_ref: str | None,
    user_key: str | None,
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO crm_event_plan_handoff_log
            (plan_id, trigger_key, status, detail, pdf_hash, external_ref, created_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (
                plan_id,
                (trigger or "")[:64],
                status[:32],
                detail,
                pdf_hash[:64] if pdf_hash else None,
                external_ref[:128] if external_ref else None,
                (user_key or "")[:64] or None,
            ),
        )
    finally:
        cur.close()


def _update_plan_handoff(
    conn,
    plan_id: int,
    *,
    status: str,
    external_ref: str | None,
    error: str | None,
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE crm_event_plans SET
            handoff_status=%s, handoff_external_ref=%s, handoff_at=UTC_TIMESTAMP(),
            handoff_error=%s
            WHERE id=%s""",
            (
                status[:32],
                external_ref[:128] if external_ref else None,
                error,
                plan_id,
            ),
        )
    finally:
        cur.close()


def _set_cura_operational_event_id(conn, plan_id: int, oid: int | None) -> None:
    if oid is None:
        return
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE crm_event_plans SET cura_operational_event_id=%s WHERE id=%s",
            (int(oid), plan_id),
        )
    finally:
        cur.close()


def _handoff_require_pdf() -> bool:
    v = (os.environ.get(HANDOFF_REQUIRE_PDF_ENV) or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _env_require_quote_accepted() -> bool:
    v = (os.environ.get(HANDOFF_REQUIRE_QUOTE_ACCEPTED_ENV) or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def handoff_blocked_reason(plan: dict[str, Any]) -> str | None:
    """
    If handoff should be refused, return a short user-facing message; else None.

    When a quote is linked, default policy requires status ``accepted`` so Cura is only
    updated after the client agrees pricing (operator still must click **Send to Cura**).
    """
    if not _env_require_quote_accepted():
        return None
    qid = plan.get("quote_id")
    if qid is None or qid == "":
        return None
    try:
        qid_int = int(qid)
    except (TypeError, ValueError):
        return None
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT status FROM crm_quotes WHERE id=%s", (qid_int,))
            row = cur.fetchone()
        finally:
            cur.close()
    finally:
        conn.close()
    if not row:
        return (
            "This plan is linked to a quote that no longer exists. "
            "Unlink or fix the quote, or set the quote to Accepted on a valid revision."
        )
    st = (row.get("status") or "").strip().lower()
    if st != "accepted":
        return (
            "Send to Cura stays off until this plan’s linked quote is set to Accepted "
            "(the client agreed this revision’s pricing). "
            "Moving the opportunity to Won on the Kanban does not change quote status — "
            "open the quote, choose Accepted in Status, save, or use “Mark quote accepted” on the quote or below."
        )
    return None


def process_event_plan_handoff(
    plan_id: int,
    *,
    trigger: str,
    user_key: str | None,
    flask_app=None,
) -> dict[str, Any]:
    """
    Run handoff pipeline. Inserts ``crm_event_plan_handoff_log`` and may update plan handoff_*.
    """
    mode = handoff_mode()
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM crm_event_plans WHERE id=%s", (plan_id,))
        plan = cur.fetchone()
        cur.close()
        if not plan:
            return {"ok": False, "error": "plan_not_found"}

        biz_block = handoff_blocked_reason(plan)
        if biz_block:
            return {"ok": False, "error": biz_block}

        pdf_row = _latest_pdf_row(conn, plan_id)
        latest_path = (pdf_row or {}).get("file_path")
        latest_hash = (pdf_row or {}).get("pdf_hash")
        payload = build_handoff_payload(
            plan,
            latest_pdf_path=latest_path,
            latest_pdf_hash=latest_hash,
        )
        detail_json = json.dumps(payload, default=str, ensure_ascii=False)[:65000]

        if mode in ("", "off", "0", "false", "no"):
            _insert_handoff_log(
                conn,
                plan_id=plan_id,
                trigger=trigger,
                status="skipped_disabled",
                detail="Set SPARROW_CRM_EVENT_HANDOFF_MODE to simulate, dry_run, or live.",
                pdf_hash=latest_hash,
                external_ref=None,
                user_key=user_key,
            )
            conn.commit()
            return {"ok": True, "status": "skipped_disabled", "message": "Handoff disabled (env)."}

        if mode == "dry_run":
            _insert_handoff_log(
                conn,
                plan_id=plan_id,
                trigger=trigger,
                status="dry_run",
                detail=detail_json,
                pdf_hash=latest_hash,
                external_ref=None,
                user_key=user_key,
            )
            _update_plan_handoff(
                conn,
                plan_id,
                status="dry_run",
                external_ref=None,
                error=None,
            )
            conn.commit()
            return {"ok": True, "status": "dry_run", "message": "Logged payload only."}

        if mode == "live":
            if _handoff_require_pdf() and not latest_path:
                msg = (
                    "No event plan PDF on file; generate a PDF before handoff, "
                    "or unset SPARROW_CRM_HANDOFF_REQUIRE_PDF."
                )
                _insert_handoff_log(
                    conn,
                    plan_id=plan_id,
                    trigger=trigger,
                    status="failed",
                    detail=msg,
                    pdf_hash=latest_hash,
                    external_ref=None,
                    user_key=user_key,
                )
                _update_plan_handoff(
                    conn,
                    plan_id,
                    status="failed",
                    external_ref=None,
                    error=msg[:2000],
                )
                conn.commit()
                return {"ok": False, "status": "failed", "error": msg}

            pdf_abs = None
            if latest_path and flask_app is not None:
                from .crm_static_paths import crm_resolve_event_plan_pdf_path

                pdf_abs = crm_resolve_event_plan_pdf_path(flask_app, latest_path)
            try:
                from app.plugins.medical_records_module.cura_crm_event_plan_handoff import (
                    sync_crm_event_plan_to_cura,
                )
            except ImportError as imp_ex:
                _insert_handoff_log(
                    conn,
                    plan_id=plan_id,
                    trigger=trigger,
                    status="failed",
                    detail=f"medical_records_module import: {imp_ex}",
                    pdf_hash=latest_hash,
                    external_ref=None,
                    user_key=user_key,
                )
                _update_plan_handoff(
                    conn,
                    plan_id,
                    status="failed",
                    external_ref=None,
                    error="medical_records_module not installed",
                )
                conn.commit()
                return {
                    "ok": False,
                    "status": "failed",
                    "error": "medical_records_module not available",
                }

            sync_res = sync_crm_event_plan_to_cura(
                crm_plan_id=plan_id,
                plan_row=plan,
                pdf_abs_path=pdf_abs,
                pdf_sha256=latest_hash,
                actor=user_key,
            )
            if not sync_res.get("ok"):
                err = (sync_res.get("error") or "cura_sync_failed")[:2000]
                _insert_handoff_log(
                    conn,
                    plan_id=plan_id,
                    trigger=trigger,
                    status="failed",
                    detail=detail_json,
                    pdf_hash=latest_hash,
                    external_ref=None,
                    user_key=user_key,
                )
                _update_plan_handoff(
                    conn,
                    plan_id,
                    status="failed",
                    external_ref=None,
                    error=err,
                )
                conn.commit()
                return {"ok": False, "status": "failed", "error": err}

            oid = sync_res.get("operational_event_id")
            ext = f"cura_operational_event:{oid}" if oid is not None else None
            log_status = "synced_unchanged" if sync_res.get("unchanged") else "synced"
            plan_status = "synced_unchanged" if sync_res.get("unchanged") else "synced"
            _insert_handoff_log(
                conn,
                plan_id=plan_id,
                trigger=trigger,
                status=log_status,
                detail=detail_json,
                pdf_hash=latest_hash,
                external_ref=ext,
                user_key=user_key,
            )
            _update_plan_handoff(
                conn,
                plan_id,
                status=plan_status,
                external_ref=ext,
                error=None,
            )
            _set_cura_operational_event_id(conn, plan_id, int(oid) if oid is not None else None)
            conn.commit()
            logger.info(
                "CRM event plan %s handoff %s → Cura operational_event_id=%s",
                plan_id,
                log_status,
                oid,
            )
            return {
                "ok": True,
                "status": log_status,
                "external_ref": ext,
                "operational_event_id": oid,
                "message": sync_res.get("message") or "Cura operational event updated.",
            }

        if mode == "simulate":
            ext = f"sim:crm_plan:{plan_id}:{int(datetime.utcnow().timestamp())}"
            _insert_handoff_log(
                conn,
                plan_id=plan_id,
                trigger=trigger,
                status="synced_simulated",
                detail=detail_json,
                pdf_hash=latest_hash,
                external_ref=ext,
                user_key=user_key,
            )
            _update_plan_handoff(
                conn,
                plan_id,
                status="synced_simulated",
                external_ref=ext,
                error=None,
            )
            conn.commit()
            return {
                "ok": True,
                "status": "synced_simulated",
                "external_ref": ext,
                "message": "Simulated accept (no medical_records call yet).",
            }

        _insert_handoff_log(
            conn,
            plan_id=plan_id,
            trigger=trigger,
            status="skipped_unknown_mode",
            detail=f"Unknown {HANDOFF_ENV_MODE}={mode!r}",
            pdf_hash=latest_hash,
            external_ref=None,
            user_key=user_key,
        )
        conn.commit()
        return {"ok": False, "status": "skipped_unknown_mode", "error": "unknown_mode"}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        err = str(e).lower()
        if "doesn't exist" in err or "1146" in err:
            return {
                "ok": False,
                "error": "CRM handoff tables missing — run: python app/plugins/crm_module/install.py upgrade",
            }
        return {"ok": False, "error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass
