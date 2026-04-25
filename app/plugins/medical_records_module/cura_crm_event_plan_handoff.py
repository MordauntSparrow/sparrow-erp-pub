"""
CRM medical event plan → Cura operational period (R4).

Creates or updates ``cura_operational_events`` keyed by stable slug ``crm-ep-{plan_id}``,
copies the latest CRM PDF into ``cura_operational_event_assets``, and runs the same
optional bridges as admin-created events (MI shell, Ventus division, kit pool) in best-effort
try/except blocks.

Called from ``crm_module`` on user **Push to Cura** when handoff mode is ``live`` (default when env is unset).
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import uuid
from typing import Any

from app.objects import get_db_connection

logger = logging.getLogger("medical_records_module.cura_crm_handoff")

ASSET_TITLE = "Medical event plan (CRM)"


def _app_static_dir() -> str:
    app_pkg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return os.path.join(app_pkg_dir, "static")


def _event_asset_upload_dir(event_id: int) -> str:
    base = os.path.join(
        _app_static_dir(), "uploads", "cura_operational_events", str(int(event_id))
    )
    os.makedirs(base, exist_ok=True)
    return base


def _slug_for_crm_plan(crm_plan_id: int) -> str:
    return f"crm-ep-{int(crm_plan_id)}"


def _parse_config(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return {}
        try:
            v = json.loads(s)
            return dict(v) if isinstance(v, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _risk_hazard_lines_from_plan_row(plan_row: dict[str, Any]) -> list[str]:
    raw = plan_row.get("risk_register_json")
    if raw is None:
        return []
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            raw = json.loads(s)
        except json.JSONDecodeError:
            return []
    items: list | None = None
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = raw.get("items") or raw.get("risks") or raw.get("rows")
    if not isinstance(items, list):
        return []
    out: list[str] = []
    for it in items[:12]:
        if not isinstance(it, dict):
            continue
        parts = [
            str(it.get("risk_type") or "").strip(),
            str(it.get("risk_level") or "").strip(),
            str(it.get("risk_comment") or "").strip(),
        ]
        line = " — ".join(p for p in parts if p)[:500]
        if not line:
            for k in ("hazard", "title", "description", "risk", "detail"):
                v = it.get(k)
                if v and str(v).strip():
                    line = str(v).strip()[:500]
                    break
        if line and line not in out:
            out.append(line)
    return out


def _merge_crm_plan_into_mi_config_json(
    existing_raw: Any,
    plan_row: dict[str, Any],
    crm_plan_id: int,
) -> str:
    """Merge CRM plan clinical / context fields into ``cura_mi_events.config_json`` (Cura MI + ops hub)."""
    cfg = _parse_config(existing_raw)
    rp = cfg.get("riskProfile")
    if not isinstance(rp, dict):
        rp = {}

    exp = plan_row.get("expected_attendance")
    if exp is not None and str(exp).strip():
        rp["expectedAttendance"] = str(exp).strip()[:500]

    cap = (plan_row.get("event_map_caption") or "").strip()
    if cap:
        rp["siteMapRef"] = cap[:500]
    elif (plan_row.get("event_map_path") or "").strip():
        rp["siteMapRef"] = str(plan_row.get("event_map_path")).strip()[:500]

    haz = _risk_hazard_lines_from_plan_row(plan_row)
    if haz:
        prev = rp.get("keyHazards")
        if not isinstance(prev, list) or not prev:
            rp["keyHazards"] = haz[:20]

    cfg["riskProfile"] = rp

    ecs = (plan_row.get("event_content_summary") or "").strip()
    if ecs:
        cfg["eventContentSummary"] = ecs[:8000]

    abd = (plan_row.get("attendance_by_day_text") or "").strip()
    if abd:
        cfg["attendanceByDay"] = abd[:8000]

    ot = (plan_row.get("operational_timings") or "").strip()
    if ot:
        cfg["operationalTimings"] = ot[:8000]

    cc = (plan_row.get("command_control_text") or "").strip()
    if cc:
        contacts = cfg.get("contacts")
        if not isinstance(contacts, dict):
            contacts = {}
        if not (contacts.get("controlChannel") or "").strip():
            contacts["controlChannel"] = cc[:255]
        cfg["contacts"] = contacts

    cfg["crm_event_plan_id"] = int(crm_plan_id)

    return json.dumps(cfg, ensure_ascii=False, default=str)


def _sync_crm_plan_into_cura_mi_events(
    cur,
    operational_event_id: int,
    crm_plan_id: int,
    plan_row: dict[str, Any],
    *,
    name: str,
    location_summary: str | None,
    starts_at,
    ends_at,
    actor: str | None,
) -> None:
    """Align linked ``cura_mi_events`` with CRM plan (dates + config_json risk / context)."""
    try:
        cur.execute(
            "SELECT status FROM cura_operational_events WHERE id=%s LIMIT 1",
            (int(operational_event_id),),
        )
        r = cur.fetchone()
        op_st = (r[0] if r else None) or "draft"
    except Exception:
        op_st = "draft"
    try:
        from .cura_event_debrief import push_operational_snapshot_to_mi_events

        push_operational_snapshot_to_mi_events(
            cur,
            int(operational_event_id),
            name=name,
            location_summary=location_summary,
            starts_at=starts_at,
            ends_at=ends_at,
            operational_status=op_st,
            actor=actor,
        )
    except Exception as ex:
        logger.warning("CRM handoff: MI snapshot push: %s", ex)

    ev = str(int(operational_event_id))
    try:
        cur.execute(
            """
            SELECT id, config_json FROM cura_mi_events
            WHERE
              TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operational_event_id')), '')) = %s
              OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operationalEventId')), '')) = %s
            """,
            (ev, ev),
        )
        rows = cur.fetchall() or []
    except Exception as ex:
        logger.warning("CRM handoff: list MI for operational_event: %s", ex)
        return

    act = (actor or "").strip() or None
    for row in rows:
        try:
            mid = int(row[0])
            merged = _merge_crm_plan_into_mi_config_json(row[1], plan_row, crm_plan_id)
            cur.execute(
                "UPDATE cura_mi_events SET config_json=%s, updated_by=%s WHERE id=%s",
                (merged, act, mid),
            )
        except Exception as ex:
            logger.warning("CRM handoff: MI config merge id=%s: %s", row[0], ex)


def _find_linked_operational_event(cur, crm_plan_id: int) -> tuple[int | None, dict[str, Any]]:
    slug = _slug_for_crm_plan(crm_plan_id)
    cur.execute(
        "SELECT id, config FROM cura_operational_events WHERE slug = %s LIMIT 1",
        (slug,),
    )
    row = cur.fetchone()
    if row:
        return int(row[0]), _parse_config(row[1])
    cur.execute(
        """
        SELECT id, config FROM cura_operational_events
        WHERE config IS NOT NULL
          AND JSON_EXTRACT(config, '$.crm_event_plan_id') = %s
        LIMIT 1
        """,
        (int(crm_plan_id),),
    )
    row = cur.fetchone()
    if row:
        return int(row[0]), _parse_config(row[1])
    return None, {}


def _cura_sync_display_name(cur, plan_row: dict[str, Any], crm_plan_id: int) -> str:
    """Prefer CRM ``lead_meta.event_name`` over compound plan / opportunity titles for ops UI."""
    try:
        from app.plugins.crm_module.crm_event_display_name import (
            friendly_event_display_name,
            parse_lead_meta_json,
        )
    except Exception:
        return (
            (plan_row.get("title") or f"CRM event plan {crm_plan_id}").strip()[:255]
            or f"CRM plan {crm_plan_id}"
        )

    lead = None
    qt = None
    on = None
    try:
        oid = plan_row.get("opportunity_id")
        if oid is not None and str(oid).strip().isdigit():
            cur.execute(
                "SELECT name, lead_meta_json FROM crm_opportunities WHERE id=%s LIMIT 1",
                (int(oid),),
            )
            r = cur.fetchone()
            if r:
                on = (str(r[0]).strip() or None) if r[0] is not None else None
                lead = parse_lead_meta_json(r[1])
        qid = plan_row.get("quote_id")
        if qid is not None and str(qid).strip().isdigit():
            cur.execute(
                "SELECT title FROM crm_quotes WHERE id=%s LIMIT 1",
                (int(qid),),
            )
            rq = cur.fetchone()
            if rq and rq[0] is not None:
                s = str(rq[0]).strip()
                qt = s or None
    except Exception:
        pass

    return friendly_event_display_name(
        lead_meta=lead,
        quote_title=qt,
        opportunity_name=on,
        plan_title=(plan_row.get("title") or "").strip() or None,
        fallback=f"CRM event plan {crm_plan_id}",
    )[:255]


def sync_crm_event_plan_to_cura(
    *,
    crm_plan_id: int,
    plan_row: dict[str, Any],
    pdf_abs_path: str | None,
    pdf_sha256: str | None,
    actor: str | None,
) -> dict[str, Any]:
    """
    Create/update Cura operational event and attach PDF when path is readable.

    Returns ``{ok, operational_event_id?, unchanged?, error?, message?}``.
    """
    loc = plan_row.get("location_summary")
    if not loc:
        parts = [
            plan_row.get("address_line1"),
            plan_row.get("postcode"),
            plan_row.get("what3words"),
        ]
        loc = ", ".join(str(x).strip() for x in parts if x and str(x).strip())[:512] or None
    starts_at = plan_row.get("start_datetime")
    ends_at = plan_row.get("end_datetime")
    desc = plan_row.get("description")
    if not desc:
        chunks = []
        if plan_row.get("demographics_notes"):
            chunks.append(str(plan_row["demographics_notes"]).strip())
        if plan_row.get("environment_notes"):
            chunks.append(str(plan_row["environment_notes"]).strip())
        desc = "\n\n".join(chunks) if chunks else None

    conn = get_db_connection()
    cur = conn.cursor()
    act = (actor or "")[:128] or None
    try:
        name = _cura_sync_display_name(cur, plan_row, crm_plan_id)
        eid, existing_cfg = _find_linked_operational_event(cur, crm_plan_id)
        cfg = dict(existing_cfg)
        cfg["crm_event_plan_id"] = int(crm_plan_id)
        cfg["crm_handoff_source"] = "crm_module"
        if desc:
            cfg["crm_description"] = desc[:8000]

        prev_hash = existing_cfg.get("crm_last_pdf_sha256")
        same_pdf = bool(
            pdf_sha256
            and prev_hash == pdf_sha256
            and pdf_abs_path
            and os.path.isfile(pdf_abs_path)
        )
        if pdf_sha256:
            cfg["crm_last_pdf_sha256"] = pdf_sha256
        try:
            from app.plugins.crm_module.crm_event_plan_staffing import (
                staffing_snapshot_for_plan_row,
            )

            snap = staffing_snapshot_for_plan_row(cur, plan_row)
            if snap:
                cfg["crm_recommended_staffing"] = snap
        except Exception as ex:
            logger.warning("CRM recommended staffing snapshot skipped: %s", ex)
        cfg_json = json.dumps(cfg, ensure_ascii=False)

        if eid:
            cur.execute(
                """
                UPDATE cura_operational_events SET
                name=%s, location_summary=%s, starts_at=%s, ends_at=%s,
                config=%s, updated_by=%s
                WHERE id=%s
                """,
                (name, loc, starts_at, ends_at, cfg_json, act, eid),
            )
            try:
                from .cura_event_ventus_bridge import (
                    provision_operational_event_dispatch_division,
                )

                prov = provision_operational_event_dispatch_division(
                    cur, conn, int(eid), act or "", do_commit=False, force_resync=True
                )
                if not prov.get("ok"):
                    logger.warning(
                        "CRM handoff: provision division resync: %s", prov.get("error")
                    )
            except Exception as ex:
                logger.warning("CRM handoff: provision division resync: %s", ex)
        else:
            slug = _slug_for_crm_plan(crm_plan_id)
            try:
                cur.execute(
                    """
                    INSERT INTO cura_operational_events
                      (slug, name, location_summary, starts_at, ends_at, status, config,
                       enforce_assignments, created_by, updated_by)
                    VALUES (%s, %s, %s, %s, %s, 'draft', %s, 0, %s, %s)
                    """,
                    (slug, name, loc, starts_at, ends_at, cfg_json, act, act),
                )
            except Exception as ins_ex:
                if "1062" not in str(ins_ex) and "duplicate" not in str(ins_ex).lower():
                    raise
                slug_alt = f"{slug}-{uuid.uuid4().hex[:8]}"
                cur.execute(
                    """
                    INSERT INTO cura_operational_events
                      (slug, name, location_summary, starts_at, ends_at, status, config,
                       enforce_assignments, created_by, updated_by)
                    VALUES (%s, %s, %s, %s, %s, 'draft', %s, 0, %s, %s)
                    """,
                    (slug_alt, name, loc, starts_at, ends_at, cfg_json, act, act),
                )
            new_id = cur.lastrowid
            if not new_id:
                conn.rollback()
                return {"ok": False, "error": "insert_operational_event_failed"}
            eid = int(new_id)
            try:
                from . import cura_event_debrief as _ced_mi

                _ced_mi.ensure_mi_event_for_operational_period(
                    cur,
                    eid,
                    name=name,
                    location_summary=loc,
                    starts_at=starts_at,
                    ends_at=ends_at,
                    operational_status="draft",
                    actor=act or "",
                )
            except Exception as ex:
                logger.warning("CRM handoff: ensure MI event: %s", ex)
            try:
                from .cura_event_ventus_bridge import provision_operational_event_dispatch_division

                prov = provision_operational_event_dispatch_division(
                    cur, conn, eid, act or "", do_commit=False
                )
                if not prov.get("ok"):
                    logger.warning("CRM handoff: provision division: %s", prov.get("error"))
            except Exception as ex:
                logger.warning("CRM handoff: provision division: %s", ex)
            try:
                from .cura_event_inventory_bridge import provision_and_link_event_kit_pool

                provision_and_link_event_kit_pool(cur, conn, eid, name, act or "")
            except Exception as ex:
                logger.warning("CRM handoff: event kit pool: %s", ex)

        try:
            _sync_crm_plan_into_cura_mi_events(
                cur,
                int(eid),
                int(crm_plan_id),
                plan_row,
                name=name,
                location_summary=loc,
                starts_at=starts_at,
                ends_at=ends_at,
                actor=act,
            )
        except Exception as mex:
            logger.warning("CRM handoff: MI alignment from CRM plan: %s", mex)

        if same_pdf:
            conn.commit()
            return {
                "ok": True,
                "operational_event_id": int(eid),
                "unchanged": True,
                "message": "Operational event and linked Cura MI fields updated; PDF unchanged.",
            }

        if pdf_abs_path and os.path.isfile(pdf_abs_path) and pdf_sha256:
            dest_dir = _event_asset_upload_dir(eid)
            safe = f"crm_plan_{int(crm_plan_id)}_{pdf_sha256[:12]}.pdf"
            full_path = os.path.join(dest_dir, safe)
            shutil.copy2(pdf_abs_path, full_path)
            rel_path = f"uploads/cura_operational_events/{int(eid)}/{safe}".replace("\\", "/")

            cur.execute(
                """
                SELECT id FROM cura_operational_event_assets
                WHERE operational_event_id=%s AND asset_kind='pdf' AND title=%s
                LIMIT 1
                """,
                (eid, ASSET_TITLE),
            )
            prev_asset = cur.fetchone()
            if prev_asset:
                cur.execute(
                    "SELECT storage_path FROM cura_operational_event_assets WHERE id=%s",
                    (prev_asset[0],),
                )
                old = cur.fetchone()
                if old and old[0]:
                    old_rel = str(old[0]).replace("\\", "/")
                    old_abs = os.path.join(_app_static_dir(), *old_rel.split("/"))
                    try:
                        if os.path.isfile(old_abs) and os.path.abspath(old_abs) != os.path.abspath(
                            full_path
                        ):
                            os.remove(old_abs)
                    except OSError:
                        pass
                cur.execute(
                    """
                    UPDATE cura_operational_event_assets SET
                    storage_path=%s, original_filename=%s, mime_type=%s, created_by=%s
                    WHERE id=%s
                    """,
                    (
                        rel_path,
                        f"crm_event_plan_{crm_plan_id}.pdf",
                        "application/pdf",
                        act,
                        prev_asset[0],
                    ),
                )
            else:
                cur.execute(
                    "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM cura_operational_event_assets WHERE operational_event_id=%s",
                    (eid,),
                )
                sort_order = int((cur.fetchone() or [0])[0] or 0)
                cur.execute(
                    """
                    INSERT INTO cura_operational_event_assets
                      (operational_event_id, asset_kind, title, body_text, storage_path,
                       original_filename, mime_type, sort_order, created_by)
                    VALUES (%s, 'pdf', %s, NULL, %s, %s, %s, %s, %s)
                    """,
                    (
                        eid,
                        ASSET_TITLE,
                        rel_path,
                        f"crm_event_plan_{crm_plan_id}.pdf",
                        "application/pdf",
                        sort_order,
                        act,
                    ),
                )

        conn.commit()
        return {
            "ok": True,
            "operational_event_id": eid,
            "unchanged": False,
            "message": "Linked to Cura operational event.",
        }
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("sync_crm_event_plan_to_cura: %s", e)
        return {"ok": False, "error": str(e)}
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
