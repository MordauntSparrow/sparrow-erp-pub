"""Minor injury + event admin under ``/api/cura/minor-injury/...`` (tables ``cura_mi_*``)."""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime

logger = logging.getLogger("medical_records_module.cura_mi")


def register(bp, api_prefix="/api/cura/minor-injury"):
    from flask import request, jsonify
    from app.objects import get_db_connection

    from . import cura_event_ventus_bridge as cevb
    from . import cura_mi_custom_forms as micf
    from . import cura_mi_reference_cards as mirfc
    from .cura_util import safe_json
    from .routes import (
        _audit_epcr_api,
        _cura_auth_principal,
        _epcr_privileged_role,
        _require_epcr_json_api,
    )

    def p(rel: str) -> str:
        r = rel.strip().strip("/")
        ap = (api_prefix or "").strip().strip("/")
        return "/" + ap + "/" + r if ap else "/" + r

    def _uname():
        return _cura_auth_principal()[0] or ""

    def _dt(val):
        if val is None or val == "":
            return None
        if isinstance(val, datetime):
            return val.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(val, str):
            try:
                d = datetime.fromisoformat(val.replace("Z", "+00:00"))
                return d.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        return None

    def _parse_cfg(raw):
        if raw is None or raw == "":
            return {}
        if isinstance(raw, dict):
            return raw
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def _merge_operational_event_into_config(cfg: dict, body: dict) -> dict:
        """
        Apply top-level ``operational_event_id`` / ``operationalEventId`` from JSON body into MI config.
        Used so admins can link an MI event to ``cura_operational_events`` for Cura incident-report / EPCR rollups
        without hand-editing the full config blob. Null / empty clears the link.
        """
        if "operational_event_id" not in body and "operationalEventId" not in body:
            return cfg
        out = dict(cfg) if isinstance(cfg, dict) else {}
        raw_oe = body.get("operational_event_id")
        if raw_oe is None:
            raw_oe = body.get("operationalEventId")
        if raw_oe is None:
            return out
        s = str(raw_oe).strip()
        if s == "" or s.lower() in ("null", "none"):
            out.pop("operational_event_id", None)
            out.pop("operationalEventId", None)
            return out
        try:
            oe = int(s)
            if oe <= 0:
                out.pop("operational_event_id", None)
                out.pop("operationalEventId", None)
            else:
                out["operational_event_id"] = oe
                out.pop("operationalEventId", None)
        except (TypeError, ValueError):
            pass
        return out

    def _row_event(r):
        return {
            "id": r[0],
            "name": r[1],
            "location": r[2],
            "startDate": r[3].isoformat() if r[3] else None,
            "endDate": r[4].isoformat() if r[4] else None,
            "status": r[5],
            "config": _parse_cfg(r[6]),
            "created_by": r[7],
            "updated_by": r[8],
            "created_at": r[9].isoformat() if r[9] else None,
            "updated_at": r[10].isoformat() if r[10] else None,
        }

    def _resolve_slug_to_operational_event_id(conn, slug_raw: str) -> int | None:
        slug = (slug_raw or "").strip().lower()
        if not slug:
            return None
        div_cur = conn.cursor(dictionary=True)
        try:
            divisions = cevb.list_cura_signon_dispatch_divisions(div_cur)
        finally:
            div_cur.close()
        for d in divisions or []:
            if (d.get("slug") or "").strip().lower() == slug:
                oid = d.get("operational_event_id")
                if oid is not None:
                    try:
                        v = int(oid)
                        return v if v > 0 else None
                    except (TypeError, ValueError):
                        return None
        return None

    def _mi_linked_operational_event_id(cfg: dict) -> int | None:
        if not isinstance(cfg, dict):
            return None
        raw = cfg.get("operational_event_id")
        if raw is None:
            raw = cfg.get("operationalEventId")
        if raw is None or str(raw).strip() == "":
            return None
        try:
            v = int(str(raw).strip())
            return v if v > 0 else None
        except (TypeError, ValueError):
            return None

    def _mi_user_can_access_event(cur, conn, event_id, username) -> bool:
        """
        Access if: explicit MI assignment, OR ops-hub roster on the linked operational period,
        OR Cura dispatch division slug (query param) resolves to the same operational_event_id as MI config.
        """
        uname = (username or "").strip()
        if not uname:
            return False
        cur.execute(
            """
            SELECT 1 FROM cura_mi_assignments
            WHERE event_id = %s AND LOWER(principal_username) = LOWER(%s)
            """,
            (event_id, uname),
        )
        if cur.fetchone():
            return True
        cur.execute("SELECT config_json FROM cura_mi_events WHERE id = %s", (event_id,))
        row = cur.fetchone()
        if not row:
            return False
        op_id = _mi_linked_operational_event_id(_parse_cfg(row[0]))
        if op_id is None:
            return False
        cur.execute(
            """
            SELECT 1 FROM cura_operational_event_assignments
            WHERE operational_event_id = %s AND LOWER(principal_username) = LOWER(%s)
            """,
            (op_id, uname),
        )
        if cur.fetchone():
            return True
        slug = (request.args.get("ventus_division_slug") or request.args.get("dispatch_division_slug") or "").strip()
        if slug and conn is not None:
            resolved = _resolve_slug_to_operational_event_id(conn, slug)
            if resolved is not None and resolved == op_id:
                return True
        raw_hint = request.args.get("operational_event_id") or request.args.get("operationalEventId")
        if raw_hint is not None and str(raw_hint).strip() != "" and conn is not None and slug:
            try:
                hint = int(str(raw_hint).strip())
            except (TypeError, ValueError):
                hint = None
            if hint is not None and hint > 0 and hint == op_id:
                if _resolve_slug_to_operational_event_id(conn, slug) == hint:
                    return True
        return False

    @bp.route(p("events/assigned"), methods=["GET", "OPTIONS"])
    def mi_events_assigned():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        token_uname = _uname()
        uid = token_uname
        if _epcr_privileged_role() and (
            (request.args.get("userId") or request.args.get("username") or "").strip()
        ):
            uid = (request.args.get("userId") or request.args.get("username") or "").strip() or token_uname
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if _epcr_privileged_role() and request.args.get("all") == "1":
                cur.execute(
                    """
                    SELECT id, name, location_summary, starts_at, ends_at, status, config_json,
                           created_by, updated_by, created_at, updated_at
                    FROM cura_mi_events ORDER BY starts_at IS NULL, starts_at DESC
                    """
                )
            else:
                slug_raw = (request.args.get("ventus_division_slug") or request.args.get("dispatch_division_slug") or "").strip()
                dispatch_op_id = _resolve_slug_to_operational_event_id(conn, slug_raw) if slug_raw else None
                hint_raw = (
                    request.args.get("operational_event_id") or request.args.get("operationalEventId") or ""
                ).strip()
                hint_op_id = None
                if hint_raw:
                    try:
                        hv = int(hint_raw)
                        if hv > 0:
                            hint_op_id = hv
                    except (TypeError, ValueError):
                        hint_op_id = None
                cols = (
                    "e.id, e.name, e.location_summary, e.starts_at, e.ends_at, e.status, e.config_json, "
                    "e.created_by, e.updated_by, e.created_at, e.updated_at"
                )
                subqueries = [
                    (
                        f"""
                        SELECT {cols}
                        FROM cura_mi_events e
                        INNER JOIN cura_mi_assignments a
                          ON a.event_id = e.id AND LOWER(a.principal_username) = LOWER(%s)
                        """,
                        [uid],
                    ),
                    (
                        f"""
                        SELECT {cols}
                        FROM cura_mi_events e
                        INNER JOIN cura_operational_event_assignments oea ON (
                            TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(e.config_json, '$.operational_event_id')), ''))
                              = CAST(oea.operational_event_id AS CHAR)
                            OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(e.config_json, '$.operationalEventId')), ''))
                              = CAST(oea.operational_event_id AS CHAR)
                        )
                        AND LOWER(oea.principal_username) = LOWER(%s)
                        """,
                        [uid],
                    ),
                ]
                if dispatch_op_id is not None:
                    sop = str(int(dispatch_op_id))
                    subqueries.append(
                        (
                            f"""
                            SELECT {cols}
                            FROM cura_mi_events e
                            WHERE (
                                TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(e.config_json, '$.operational_event_id')), '')) = %s
                                OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(e.config_json, '$.operationalEventId')), '')) = %s
                            )
                            """,
                            [sop, sop],
                        )
                    )
                # Cura sends operational_event_id from session even when Ventus slug resolution on Sparrow
                # is empty (no mdt_dispatch_divisions / plugin). Still require ops-hub roster membership.
                if hint_op_id is not None:
                    sop_h = str(int(hint_op_id))
                    subqueries.append(
                        (
                            f"""
                            SELECT {cols}
                            FROM cura_mi_events e
                            INNER JOIN cura_operational_event_assignments oea
                              ON oea.operational_event_id = %s
                             AND LOWER(oea.principal_username) = LOWER(%s)
                            WHERE (
                                TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(e.config_json, '$.operational_event_id')), '')) = %s
                                OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(e.config_json, '$.operationalEventId')), '')) = %s
                            )
                            """,
                            [hint_op_id, uid, sop_h, sop_h],
                        )
                    )
                union_sql = " UNION ".join(s[0] for s in subqueries)
                params_flat: list = []
                for _, p in subqueries:
                    params_flat.extend(p)
                cur.execute(
                    f"""
                    SELECT * FROM (
                      {union_sql}
                    ) AS mi_visible
                    ORDER BY mi_visible.starts_at IS NULL, mi_visible.starts_at DESC
                    """,
                    tuple(params_flat),
                )
            items = [_row_event(r) for r in cur.fetchall()]
            return jsonify({"items": items}), 200
        except Exception as e:
            if "cura_mi_events" in str(e) or "Unknown table" in str(e):
                return jsonify({"items": [], "message": "Run DB upgrade"}), 503
            logger.exception("mi_events_assigned: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(p("events/<int:event_id>"), methods=["GET", "OPTIONS"])
    def mi_event_one(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, name, location_summary, starts_at, ends_at, status, config_json,
                       created_by, updated_by, created_at, updated_at
                FROM cura_mi_events WHERE id = %s
                """,
                (event_id,),
            )
            r = cur.fetchone()
            if not r:
                return jsonify({"error": "Not found"}), 404
            if not _epcr_privileged_role() and not _mi_user_can_access_event(cur, conn, event_id, _uname()):
                return jsonify({"error": "Unauthorised"}), 403
            return jsonify({"item": _row_event(r)}), 200
        except Exception as e:
            logger.exception("mi_event_one: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(p("events/<int:event_id>/status"), methods=["GET", "OPTIONS"])
    def mi_event_status(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT status, updated_at FROM cura_mi_events WHERE id = %s", (event_id,))
            r = cur.fetchone()
            if not r:
                return jsonify({"error": "Not found"}), 404
            if not _epcr_privileged_role() and not _mi_user_can_access_event(cur, conn, event_id, _uname()):
                return jsonify({"error": "Unauthorised"}), 403
            return jsonify({"status": r[0], "updated_at": r[1].isoformat() if r[1] else None}), 200
        finally:
            cur.close()
            conn.close()

    @bp.route(p("events/<int:event_id>/notices"), methods=["GET", "OPTIONS"])
    def mi_event_notices(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if not _epcr_privileged_role() and not _mi_user_can_access_event(cur, conn, event_id, _uname()):
                return jsonify({"error": "Unauthorised"}), 403
            cur.execute(
                """
                SELECT id, message, severity, expires_at, created_at
                FROM cura_mi_notices
                WHERE event_id = %s
                  AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP())
                ORDER BY created_at DESC
                """,
                (event_id,),
            )
            items = [
                {
                    "id": n[0],
                    "message": n[1],
                    "severity": n[2],
                    "expiresAt": n[3].isoformat() if n[3] else None,
                    "timestamp": n[4].isoformat() if n[4] else None,
                }
                for n in cur.fetchall()
            ]
            return jsonify({"items": items}), 200
        finally:
            cur.close()
            conn.close()

    @bp.route(p("admin/events/<int:event_id>/notices"), methods=["POST", "OPTIONS"])
    def mi_admin_event_notice_create(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        body = request.get_json(silent=True) or {}
        message = (body.get("message") or "").strip()
        if not message:
            return jsonify({"error": "message is required"}), 400
        severity = (body.get("severity") or "info").strip().lower()
        if severity not in ("info", "warning", "error", "success"):
            severity = "info"
        exp = None
        raw_exp = body.get("expires_at") or body.get("expiresAt")
        if raw_exp not in (None, ""):
            try:
                from datetime import datetime

                exp = datetime.fromisoformat(str(raw_exp).replace("Z", "+00:00"))
                exp = exp.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return jsonify({"error": "expires_at must be ISO date-time"}), 400
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM cura_mi_events WHERE id = %s", (event_id,))
            if not cur.fetchone():
                return jsonify({"error": "Not found"}), 404
            cur.execute(
                """
                INSERT INTO cura_mi_notices (event_id, message, severity, expires_at)
                VALUES (%s, %s, %s, %s)
                """,
                (event_id, message, severity, exp),
            )
            nid = cur.lastrowid
            conn.commit()
            _audit_epcr_api(f"MI notice created id={nid} event={event_id}")
            cur.execute(
                """
                SELECT id, message, severity, expires_at, created_at
                FROM cura_mi_notices WHERE id = %s
                """,
                (nid,),
            )
            n = cur.fetchone()
            return (
                jsonify(
                    {
                        "item": {
                            "id": n[0],
                            "message": n[1],
                            "severity": n[2],
                            "expiresAt": n[3].isoformat() if n[3] else None,
                            "timestamp": n[4].isoformat() if n[4] else None,
                        }
                    }
                ),
                201,
            )
        except Exception as e:
            conn.rollback()
            if "cura_mi_notices" in str(e) or "Unknown table" in str(e):
                return jsonify({"error": "Run DB upgrade"}), 503
            logger.exception("mi_admin_event_notice_create: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        p("admin/events/<int:event_id>/notices/<int:notice_id>"),
        methods=["DELETE", "OPTIONS"],
    )
    def mi_admin_event_notice_delete(event_id, notice_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM cura_mi_notices WHERE id = %s AND event_id = %s",
                (notice_id, event_id),
            )
            if cur.rowcount == 0:
                return jsonify({"error": "Not found"}), 404
            conn.commit()
            _audit_epcr_api(f"MI notice deleted id={notice_id} event={event_id}")
            return jsonify({"ok": True}), 200
        except Exception as e:
            conn.rollback()
            logger.exception("mi_admin_event_notice_delete: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(p("events/<int:event_id>/stats"), methods=["GET", "OPTIONS"])
    def mi_event_stats(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if not _epcr_privileged_role() and not _mi_user_can_access_event(cur, conn, event_id, _uname()):
                return jsonify({"error": "Unauthorised"}), 403
            cur.execute(
                """
                SELECT submitted_by, COUNT(*) AS c
                FROM cura_mi_reports
                WHERE event_id = %s AND status = 'submitted'
                GROUP BY submitted_by
                """,
                (event_id,),
            )
            user_submissions = {((r[0] or "") or "unknown"): int(r[1]) for r in cur.fetchall()}
            total = sum(user_submissions.values())
            return (
                jsonify(
                    {
                        "totalSubmissions": total,
                        "userSubmissions": user_submissions,
                        "byUser": user_submissions,
                    }
                ),
                200,
            )
        finally:
            cur.close()
            conn.close()

    @bp.route(p("events/<int:event_id>/reports"), methods=["POST", "OPTIONS"])
    def mi_event_report_post(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _uname()
        body = request.get_json(silent=True) or {}
        idem = (body.get("idempotency_key") or body.get("idempotencyKey") or "").strip() or None
        payload = body.get("payload") or body.get("data")
        if not isinstance(payload, (dict, list)):
            return jsonify({"error": "payload object is required"}), 400
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if isinstance(payload, dict):
                try:
                    v_err = micf.validate_custom_form_responses(cur, event_id, payload)
                except Exception as v_ex:
                    logger.warning("mi custom form validate: %s", v_ex)
                    v_err = None
                if v_err:
                    return jsonify({"error": v_err}), 400
            pj = json.dumps(payload)
            cur.execute("SELECT status FROM cura_mi_events WHERE id = %s", (event_id,))
            ev = cur.fetchone()
            if not ev:
                return jsonify({"error": "Event not found"}), 404
            if (ev[0] or "").lower() in ("closed", "completed") and not _epcr_privileged_role():
                cur.execute(
                    """
                    INSERT INTO cura_mi_reports
                      (event_id, public_id, idempotency_key, status, payload_json, submitted_by, rejection_reason)
                    VALUES (%s, %s, %s, 'rejected', %s, %s, %s)
                    """,
                    (
                        event_id,
                        str(uuid.uuid4()),
                        idem,
                        pj,
                        uname,
                        "Event was closed before sync",
                    ),
                )
                rid = cur.lastrowid
                conn.commit()
                _audit_epcr_api(f"MI report rejected (offline) id={rid} event={event_id}")
                return jsonify({"item": {"id": rid, "status": "rejected"}, "rejected": True}), 202

            if not _epcr_privileged_role() and not _mi_user_can_access_event(cur, conn, event_id, uname):
                return jsonify({"error": "Unauthorised"}), 403

            if idem:
                cur.execute(
                    "SELECT id, status, record_version, payload_json FROM cura_mi_reports WHERE idempotency_key = %s",
                    (idem,),
                )
                ex = cur.fetchone()
                if ex:
                    return (
                        jsonify(
                            {
                                "item": {
                                    "id": ex[0],
                                    "status": ex[1],
                                    "record_version": ex[2],
                                    "payload": safe_json(ex[3]),
                                },
                                "deduplicated": True,
                            }
                        ),
                        200,
                    )

            pid = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO cura_mi_reports
                  (event_id, public_id, idempotency_key, status, payload_json, submitted_by)
                VALUES (%s, %s, %s, 'submitted', %s, %s)
                """,
                (event_id, pid, idem, pj, uname),
            )
            rid = cur.lastrowid
            conn.commit()
            _audit_epcr_api(f"MI report submitted id={rid} event={event_id}")
            return jsonify({"item": {"id": rid, "public_id": pid, "status": "submitted"}}), 201
        except Exception as e:
            conn.rollback()
            logger.exception("mi_event_report_post: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(p("events/<int:event_id>/reports/bulk-sync"), methods=["POST", "OPTIONS"])
    def mi_reports_bulk_sync(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _uname()
        body = request.get_json(silent=True) or {}
        reports = body.get("reports") or body.get("items") or []
        if not isinstance(reports, list):
            return jsonify({"error": "reports array required"}), 400
        conn = get_db_connection()
        cur = conn.cursor()
        results = []
        try:
            cur.execute("SELECT status FROM cura_mi_events WHERE id = %s", (event_id,))
            ev = cur.fetchone()
            if not ev:
                return jsonify({"error": "Event not found"}), 404
            ev_closed = (ev[0] or "").lower() in ("closed", "completed")
            if not _epcr_privileged_role() and not _mi_user_can_access_event(cur, conn, event_id, uname):
                return jsonify({"error": "Unauthorised"}), 403
            for rep in reports:
                if not isinstance(rep, dict):
                    results.append({"ok": False, "error": "invalid item"})
                    continue
                idem = (rep.get("idempotencyKey") or rep.get("idempotency_key") or "").strip() or None
                payload = rep.get("payload") or rep.get("data")
                if not isinstance(payload, (dict, list)):
                    results.append({"ok": False, "error": "payload required"})
                    continue
                if isinstance(payload, dict):
                    v_err = micf.validate_custom_form_responses(cur, event_id, payload)
                    if v_err:
                        results.append({"ok": False, "error": v_err})
                        continue
                pj = json.dumps(payload)
                if ev_closed and not _epcr_privileged_role():
                    cur.execute(
                        """
                        INSERT INTO cura_mi_reports
                          (event_id, public_id, idempotency_key, status, payload_json, submitted_by, rejection_reason)
                        VALUES (%s, %s, %s, 'rejected', %s, %s, %s)
                        """,
                        (
                            event_id,
                            str(uuid.uuid4()),
                            idem,
                            pj,
                            uname,
                            "Event was closed before sync",
                        ),
                    )
                    rid = cur.lastrowid
                    conn.commit()
                    _audit_epcr_api(f"MI bulk rejected id={rid} event={event_id}")
                    results.append({"ok": True, "rejected": True, "id": rid})
                    continue
                if idem:
                    cur.execute(
                        "SELECT id, status, record_version, payload_json FROM cura_mi_reports WHERE idempotency_key = %s",
                        (idem,),
                    )
                    ex = cur.fetchone()
                    if ex:
                        results.append(
                            {
                                "ok": True,
                                "deduplicated": True,
                                "id": ex[0],
                                "status": ex[1],
                                "record_version": ex[2],
                            }
                        )
                        continue
                pid = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO cura_mi_reports
                      (event_id, public_id, idempotency_key, status, payload_json, submitted_by)
                    VALUES (%s, %s, %s, 'submitted', %s, %s)
                    """,
                    (event_id, pid, idem, pj, uname),
                )
                rid = cur.lastrowid
                conn.commit()
                _audit_epcr_api(f"MI bulk submitted id={rid} event={event_id}")
                results.append({"ok": True, "id": rid, "public_id": pid, "status": "submitted"})
            return jsonify({"results": results}), 200
        except Exception as e:
            conn.rollback()
            logger.exception("mi_reports_bulk_sync: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        p("events/<int:event_id>/reports/<int:report_id>/sync-status"),
        methods=["GET", "OPTIONS"],
    )
    def mi_report_sync_status(event_id, report_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if not _epcr_privileged_role() and not _mi_user_can_access_event(cur, conn, event_id, _uname()):
                return jsonify({"error": "Unauthorised"}), 403
            cur.execute(
                """
                SELECT id, event_id, status, record_version, rejection_reason, idempotency_key
                FROM cura_mi_reports WHERE id = %s
                """,
                (report_id,),
            )
            r = cur.fetchone()
            if not r or r[1] != event_id:
                return jsonify({"error": "Not found"}), 404
            return (
                jsonify(
                    {
                        "id": r[0],
                        "event_id": r[1],
                        "status": r[2],
                        "record_version": r[3],
                        "syncError": r[4],
                        "idempotencyKey": r[5],
                    }
                ),
                200,
            )
        finally:
            cur.close()
            conn.close()

    @bp.route(p("events/<int:event_id>/analytics"), methods=["GET", "OPTIONS"])
    def mi_event_analytics(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if not _epcr_privileged_role() and not _mi_user_can_access_event(cur, conn, event_id, _uname()):
                return jsonify({"error": "Unauthorised"}), 403
            cur.execute(
                "SELECT payload_json FROM cura_mi_reports WHERE event_id = %s AND status = %s",
                (event_id, "submitted"),
            )
            injury_counts = {}
            outcome_counts = {}
            body_counts = {}
            for (pj,) in cur.fetchall():
                p = safe_json(pj) if pj else {}
                if not isinstance(p, dict):
                    continue
                it = (p.get("injuryType") or p.get("injury_type") or "unknown") or "unknown"
                oc = (p.get("outcome") or "unknown") or "unknown"
                bl = (p.get("bodyLocation") or p.get("body_location") or "unknown") or "unknown"
                injury_counts[it] = injury_counts.get(it, 0) + 1
                outcome_counts[oc] = outcome_counts.get(oc, 0) + 1
                body_counts[bl] = body_counts.get(bl, 0) + 1
            total = sum(injury_counts.values()) or 1

            def pct_map(d):
                return [{"key": k, "count": v, "percentage": round(100.0 * v / total, 1)} for k, v in sorted(d.items())]

            return (
                jsonify(
                    {
                        "injuryTypes": pct_map(injury_counts),
                        "outcomes": pct_map(outcome_counts),
                        "bodyLocations": pct_map(body_counts),
                        "lastUpdated": None,
                    }
                ),
                200,
            )
        finally:
            cur.close()
            conn.close()

    @bp.route(p("events/<int:event_id>/custom-forms"), methods=["GET", "OPTIONS"])
    def mi_event_custom_forms(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM cura_mi_events WHERE id = %s", (event_id,))
            if not cur.fetchone():
                return jsonify({"error": "Not found"}), 404
            if not _epcr_privileged_role() and not _mi_user_can_access_event(
                cur, conn, event_id, _uname()
            ):
                return jsonify({"error": "Unauthorised"}), 403
            try:
                items = micf.list_assigned_templates_for_event(cur, event_id)
            except Exception as ex:
                if "cura_mi_form_templates" in str(ex) or "Unknown table" in str(ex):
                    return jsonify({"items": [], "message": "Run DB upgrade"}), 503
                raise
            return jsonify({"items": items}), 200
        except Exception as e:
            logger.exception("mi_event_custom_forms: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(p("events/<int:event_id>/reference-cards"), methods=["GET", "OPTIONS"])
    def mi_event_reference_cards(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM cura_mi_events WHERE id = %s", (event_id,))
            if not cur.fetchone():
                return jsonify({"error": "Not found"}), 404
            if not _epcr_privileged_role() and not _mi_user_can_access_event(
                cur, conn, event_id, _uname()
            ):
                return jsonify({"error": "Unauthorised"}), 403
            try:
                # Must match DB assignments exactly: fill_defaults would re-add the default pack after an
                # explicit "clear all" save, so Cura would never show an empty quick-reference set.
                items = mirfc.list_assigned_cards_for_event(cur, event_id, fill_defaults=False)
            except Exception as ex:
                if "cura_mi_reference_card_templates" in str(ex) or "Unknown table" in str(ex):
                    return jsonify({"items": [], "message": "Run DB upgrade"}), 503
                raise
            return jsonify({"items": items}), 200
        except Exception as e:
            logger.exception("mi_event_reference_cards: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(p("admin/form-templates"), methods=["GET", "POST", "OPTIONS"])
    def mi_admin_form_templates():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if request.method == "GET":
                inc = request.args.get("include_inactive") == "1"
                if inc:
                    cur.execute(
                        """
                        SELECT id, slug, name, description, schema_json, is_active,
                               created_by, updated_by, created_at, updated_at
                        FROM cura_mi_form_templates
                        ORDER BY id DESC
                        LIMIT 500
                        """
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, slug, name, description, schema_json, is_active,
                               created_by, updated_by, created_at, updated_at
                        FROM cura_mi_form_templates
                        WHERE is_active = 1
                        ORDER BY name ASC, id ASC
                        LIMIT 500
                        """
                    )
                return jsonify({"items": [micf.row_to_template_item(r) for r in cur.fetchall()]}), 200
            body = request.get_json(silent=True) or {}
            slug = micf.normalize_slug(body.get("slug"))
            if not slug:
                return jsonify({"error": "slug is required (lowercase letters, digits, _-)"}), 400
            name = (body.get("name") or "").strip()
            if not name:
                return jsonify({"error": "name is required"}), 400
            desc = (body.get("description") or "").strip() or None
            raw_schema = body.get("schema") if body.get("schema") is not None else body.get("schema_json")
            schema_dict, s_err = micf.parse_schema_blob(raw_schema)
            if s_err:
                return jsonify({"error": s_err}), 400
            is_act = 1 if body.get("is_active", True) not in (False, 0, "0", "false") else 0
            uname = _uname()
            cur.execute(
                """
                INSERT INTO cura_mi_form_templates
                  (slug, name, description, schema_json, is_active, created_by, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (slug, name[:255], desc, json.dumps(schema_dict), is_act, uname, uname),
            )
            tid = cur.lastrowid
            conn.commit()
            _audit_epcr_api(f"MI form template created id={tid} slug={slug}")
            cur.execute(
                """
                SELECT id, slug, name, description, schema_json, is_active,
                       created_by, updated_by, created_at, updated_at
                FROM cura_mi_form_templates WHERE id = %s
                """,
                (tid,),
            )
            return jsonify({"item": micf.row_to_template_item(cur.fetchone())}), 201
        except Exception as e:
            conn.rollback()
            if "Duplicate" in str(e) or "1062" in str(e):
                return jsonify({"error": "slug already in use"}), 409
            if "cura_mi_form_templates" in str(e) or "Unknown table" in str(e):
                return jsonify({"error": "Run DB upgrade"}), 503
            logger.exception("mi_admin_form_templates: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        p("admin/form-templates/<int:template_id>"),
        methods=["GET", "PATCH", "DELETE", "OPTIONS"],
    )
    def mi_admin_form_template_one(template_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, slug, name, description, schema_json, is_active,
                       created_by, updated_by, created_at, updated_at
                FROM cura_mi_form_templates WHERE id = %s
                """,
                (template_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            if request.method == "GET":
                return jsonify({"item": micf.row_to_template_item(row)}), 200
            if request.method == "DELETE":
                cur.execute("DELETE FROM cura_mi_form_templates WHERE id = %s", (template_id,))
                conn.commit()
                _audit_epcr_api(f"MI form template deleted id={template_id}")
                return jsonify({"ok": True}), 200
            body = request.get_json(silent=True) or {}
            sets = []
            params = []
            if "name" in body:
                sets.append("name=%s")
                params.append((body.get("name") or "").strip()[:255])
            if "description" in body:
                sets.append("description=%s")
                d = body.get("description")
                if d is None:
                    params.append(None)
                else:
                    params.append(str(d).strip() or None)
            if "slug" in body:
                ns = micf.normalize_slug(body.get("slug"))
                if not ns:
                    return jsonify({"error": "invalid slug"}), 400
                sets.append("slug=%s")
                params.append(ns)
            if "schema" in body or "schema_json" in body:
                raw_schema = body.get("schema") if "schema" in body else body.get("schema_json")
                schema_dict, s_err = micf.parse_schema_blob(raw_schema)
                if s_err:
                    return jsonify({"error": s_err}), 400
                sets.append("schema_json=%s")
                params.append(json.dumps(schema_dict))
            if "is_active" in body:
                sets.append("is_active=%s")
                params.append(
                    1 if body.get("is_active") not in (False, 0, "0", "false") else 0
                )
            if not sets:
                return jsonify({"error": "No fields"}), 400
            sets.append("updated_by=%s")
            params.append(_uname())
            params.append(template_id)
            cur.execute(
                f"UPDATE cura_mi_form_templates SET {', '.join(sets)} WHERE id=%s",
                tuple(params),
            )
            conn.commit()
            _audit_epcr_api(f"MI form template updated id={template_id}")
            cur.execute(
                """
                SELECT id, slug, name, description, schema_json, is_active,
                       created_by, updated_by, created_at, updated_at
                FROM cura_mi_form_templates WHERE id = %s
                """,
                (template_id,),
            )
            return jsonify({"item": micf.row_to_template_item(cur.fetchone())}), 200
        except Exception as e:
            conn.rollback()
            if "Duplicate" in str(e) or "1062" in str(e):
                return jsonify({"error": "slug already in use"}), 409
            logger.exception("mi_admin_form_template_one: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        p("admin/events/<int:event_id>/custom-forms"),
        methods=["GET", "PUT", "OPTIONS"],
    )
    def mi_admin_event_custom_forms(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM cura_mi_events WHERE id = %s", (event_id,))
            if not cur.fetchone():
                return jsonify({"error": "Not found"}), 404
            if request.method == "GET":
                try:
                    items = micf.list_assigned_templates_for_event(cur, event_id)
                    cur.execute(
                        """
                        SELECT id, slug, name, description, schema_json, is_active,
                               created_by, updated_by, created_at, updated_at
                        FROM cura_mi_form_templates
                        WHERE is_active = 1
                        ORDER BY name ASC, id ASC
                        LIMIT 500
                        """
                    )
                    pool = [micf.row_to_template_item(r) for r in cur.fetchall()]
                except Exception as ex:
                    if "cura_mi_form_templates" in str(ex) or "Unknown table" in str(ex):
                        return jsonify({"error": "Run DB upgrade"}), 503
                    raise
                return jsonify({"assigned": items, "available_templates": pool}), 200
            body = request.get_json(silent=True) or {}
            raw_ids = body.get("template_ids") or body.get("templateIds") or body.get("form_template_ids")
            if not isinstance(raw_ids, list):
                return jsonify({"error": "template_ids array is required"}), 400
            try:
                err = micf.replace_event_assignments(cur, event_id, raw_ids, _uname())
            except Exception as ex:
                if "cura_mi_event_form_assignments" in str(ex) or "Unknown table" in str(ex):
                    return jsonify({"error": "Run DB upgrade"}), 503
                raise
            if err:
                return jsonify({"error": err}), 400
            conn.commit()
            _audit_epcr_api(f"MI event {event_id} custom forms reassigned")
            items = micf.list_assigned_templates_for_event(cur, event_id)
            return jsonify({"assigned": items}), 200
        except Exception as e:
            conn.rollback()
            if "cura_mi_event_form_assignments" in str(e) or "Unknown table" in str(e):
                return jsonify({"error": "Run DB upgrade"}), 503
            logger.exception("mi_admin_event_custom_forms: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(p("admin/reference-card-templates"), methods=["GET", "POST", "OPTIONS"])
    def mi_admin_reference_card_templates():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if request.method == "GET":
                inc = request.args.get("include_inactive") == "1"
                if inc:
                    cur.execute(
                        """
                        SELECT id, slug, name, description, schema_json, is_active,
                               created_by, updated_by, created_at, updated_at
                        FROM cura_mi_reference_card_templates
                        ORDER BY id DESC
                        LIMIT 500
                        """
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, slug, name, description, schema_json, is_active,
                               created_by, updated_by, created_at, updated_at
                        FROM cura_mi_reference_card_templates
                        WHERE is_active = 1
                        ORDER BY name ASC, id ASC
                        LIMIT 500
                        """
                    )
                return jsonify({"items": [mirfc.row_to_template_item(r) for r in cur.fetchall()]}), 200
            body = request.get_json(silent=True) or {}
            slug = micf.normalize_slug(body.get("slug"))
            if not slug:
                return jsonify({"error": "slug is required (lowercase letters, digits, _-)"}), 400
            name = (body.get("name") or "").strip()
            if not name:
                return jsonify({"error": "name is required"}), 400
            desc = (body.get("description") or "").strip() or None
            raw_schema = body.get("schema") if body.get("schema") is not None else body.get("schema_json")
            schema_dict, s_err = mirfc.parse_card_schema(raw_schema)
            if s_err:
                return jsonify({"error": s_err}), 400
            is_act = 1 if body.get("is_active", True) not in (False, 0, "0", "false") else 0
            uname = _uname()
            cur.execute(
                """
                INSERT INTO cura_mi_reference_card_templates
                  (slug, name, description, schema_json, is_active, created_by, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (slug, name[:255], desc, json.dumps(schema_dict), is_act, uname, uname),
            )
            tid = cur.lastrowid
            conn.commit()
            _audit_epcr_api(f"MI reference card template created id={tid} slug={slug}")
            cur.execute(
                """
                SELECT id, slug, name, description, schema_json, is_active,
                       created_by, updated_by, created_at, updated_at
                FROM cura_mi_reference_card_templates WHERE id = %s
                """,
                (tid,),
            )
            return jsonify({"item": mirfc.row_to_template_item(cur.fetchone())}), 201
        except Exception as e:
            conn.rollback()
            if "Duplicate" in str(e) or "1062" in str(e):
                return jsonify({"error": "slug already in use"}), 409
            if "cura_mi_reference_card_templates" in str(e) or "Unknown table" in str(e):
                return jsonify({"error": "Run DB upgrade"}), 503
            logger.exception("mi_admin_reference_card_templates: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        p("admin/reference-card-templates/<int:template_id>"),
        methods=["GET", "PATCH", "DELETE", "OPTIONS"],
    )
    def mi_admin_reference_card_template_one(template_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, slug, name, description, schema_json, is_active,
                       created_by, updated_by, created_at, updated_at
                FROM cura_mi_reference_card_templates WHERE id = %s
                """,
                (template_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            if request.method == "GET":
                return jsonify({"item": mirfc.row_to_template_item(row)}), 200
            if request.method == "DELETE":
                cur.execute(
                    "DELETE FROM cura_mi_reference_card_templates WHERE id = %s",
                    (template_id,),
                )
                conn.commit()
                _audit_epcr_api(f"MI reference card template deleted id={template_id}")
                return jsonify({"ok": True}), 200
            body = request.get_json(silent=True) or {}
            sets = []
            params = []
            if "name" in body:
                sets.append("name=%s")
                params.append((body.get("name") or "").strip()[:255])
            if "description" in body:
                sets.append("description=%s")
                d = body.get("description")
                if d is None:
                    params.append(None)
                else:
                    params.append(str(d).strip() or None)
            if "slug" in body:
                ns = micf.normalize_slug(body.get("slug"))
                if not ns:
                    return jsonify({"error": "invalid slug"}), 400
                sets.append("slug=%s")
                params.append(ns)
            if "schema" in body or "schema_json" in body:
                raw_schema = body.get("schema") if "schema" in body else body.get("schema_json")
                schema_dict, s_err = mirfc.parse_card_schema(raw_schema)
                if s_err:
                    return jsonify({"error": s_err}), 400
                sets.append("schema_json=%s")
                params.append(json.dumps(schema_dict))
            if "is_active" in body:
                sets.append("is_active=%s")
                params.append(
                    1 if body.get("is_active") not in (False, 0, "0", "false") else 0
                )
            if not sets:
                return jsonify({"error": "No fields"}), 400
            sets.append("updated_by=%s")
            params.append(_uname())
            params.append(template_id)
            cur.execute(
                f"UPDATE cura_mi_reference_card_templates SET {', '.join(sets)} WHERE id=%s",
                tuple(params),
            )
            conn.commit()
            _audit_epcr_api(f"MI reference card template updated id={template_id}")
            cur.execute(
                """
                SELECT id, slug, name, description, schema_json, is_active,
                       created_by, updated_by, created_at, updated_at
                FROM cura_mi_reference_card_templates WHERE id = %s
                """,
                (template_id,),
            )
            return jsonify({"item": mirfc.row_to_template_item(cur.fetchone())}), 200
        except Exception as e:
            conn.rollback()
            if "Duplicate" in str(e) or "1062" in str(e):
                return jsonify({"error": "slug already in use"}), 409
            logger.exception("mi_admin_reference_card_template_one: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        p("admin/events/<int:event_id>/reference-cards"),
        methods=["GET", "PUT", "OPTIONS"],
    )
    def mi_admin_event_reference_cards(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM cura_mi_events WHERE id = %s", (event_id,))
            if not cur.fetchone():
                return jsonify({"error": "Not found"}), 404
            if request.method == "GET":
                try:
                    items = mirfc.list_assigned_cards_for_event(
                        cur, event_id, fill_defaults=False
                    )
                    cur.execute(
                        """
                        SELECT id, slug, name, description, schema_json, is_active,
                               created_by, updated_by, created_at, updated_at
                        FROM cura_mi_reference_card_templates
                        WHERE is_active = 1
                        ORDER BY name ASC, id ASC
                        LIMIT 500
                        """
                    )
                    pool = [mirfc.row_to_template_item(r) for r in cur.fetchall()]
                except Exception as ex:
                    if "cura_mi_reference_card_templates" in str(ex) or "Unknown table" in str(ex):
                        return jsonify({"error": "Run DB upgrade"}), 503
                    raise
                return jsonify({"assigned": items, "available_templates": pool}), 200
            body = request.get_json(silent=True) or {}
            raw_ids = body.get("template_ids") or body.get("templateIds")
            if not isinstance(raw_ids, list):
                return jsonify({"error": "template_ids array is required"}), 400
            try:
                err = mirfc.replace_event_reference_cards(cur, event_id, raw_ids, _uname())
            except Exception as ex:
                if "cura_mi_event_reference_cards" in str(ex) or "Unknown table" in str(ex):
                    return jsonify({"error": "Run DB upgrade"}), 503
                raise
            if err:
                return jsonify({"error": err}), 400
            conn.commit()
            _audit_epcr_api(f"MI event {event_id} reference cards reassigned")
            items = mirfc.list_assigned_cards_for_event(cur, event_id, fill_defaults=False)
            return jsonify({"assigned": items}), 200
        except Exception as e:
            conn.rollback()
            logger.exception("mi_admin_event_reference_cards: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(p("admin/events"), methods=["GET", "POST", "OPTIONS"])
    def mi_admin_events():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if request.method == "GET":
                cur.execute(
                    """
                    SELECT id, name, location_summary, starts_at, ends_at, status, config_json,
                           created_by, updated_by, created_at, updated_at
                    FROM cura_mi_events ORDER BY id DESC LIMIT 500
                    """
                )
                return jsonify({"items": [_row_event(r) for r in cur.fetchall()]}), 200
            body = request.get_json(silent=True) or {}
            name = (body.get("name") or "").strip()
            if not name:
                return jsonify({"error": "name is required"}), 400
            loc = (body.get("location") or body.get("location_summary") or "").strip() or None
            st = (body.get("status") or "upcoming").strip() or "upcoming"
            cfg = body.get("config") or body.get("riskProfile") or {}
            if not isinstance(cfg, dict):
                cfg = {}
            else:
                cfg = dict(cfg)
            cfg = _merge_operational_event_into_config(cfg, body)
            uname = _uname()
            cur.execute(
                """
                INSERT INTO cura_mi_events (name, location_summary, starts_at, ends_at, status, config_json, created_by, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    name,
                    loc,
                    _dt(body.get("startDate") or body.get("starts_at")),
                    _dt(body.get("endDate") or body.get("ends_at")),
                    st,
                    json.dumps(cfg),
                    uname,
                    uname,
                ),
            )
            eid = cur.lastrowid
            conn.commit()
            _audit_epcr_api(f"MI admin created event id={eid}")
            cur.execute(
                """
                SELECT id, name, location_summary, starts_at, ends_at, status, config_json,
                       created_by, updated_by, created_at, updated_at
                FROM cura_mi_events WHERE id = %s
                """,
                (eid,),
            )
            return jsonify({"item": _row_event(cur.fetchone())}), 201
        except Exception as e:
            conn.rollback()
            logger.exception("mi_admin_events: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(p("admin/events/<int:event_id>"), methods=["PATCH", "DELETE", "OPTIONS"])
    def mi_admin_event_one(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if request.method == "DELETE":
                cur.execute("DELETE FROM cura_mi_events WHERE id = %s", (event_id,))
                conn.commit()
                _audit_epcr_api(f"MI admin deleted event id={event_id}")
                return jsonify({"ok": True}), 200
            body = request.get_json(silent=True) or {}
            sets = []
            params = []
            if "name" in body:
                sets.append("name=%s")
                params.append(body["name"])
            if "location" in body or "location_summary" in body:
                sets.append("location_summary=%s")
                params.append(body.get("location") or body.get("location_summary"))
            if "status" in body:
                sets.append("status=%s")
                params.append(body["status"])
            if "config" in body or "operational_event_id" in body or "operationalEventId" in body:
                cur.execute("SELECT config_json FROM cura_mi_events WHERE id = %s", (event_id,))
                row0 = cur.fetchone()
                if not row0:
                    return jsonify({"error": "Not found"}), 404
                existing = _parse_cfg(row0[0])
                if "config" in body:
                    if isinstance(body["config"], dict):
                        existing = {**existing, **body["config"]}
                    else:
                        return jsonify({"error": "config must be an object"}), 400
                existing = _merge_operational_event_into_config(existing, body)
                sets.append("config_json=%s")
                params.append(json.dumps(existing))
            if not sets:
                return jsonify({"error": "No fields"}), 400
            sets.append("updated_by=%s")
            params.append(_uname())
            params.append(event_id)
            cur.execute(
                f"UPDATE cura_mi_events SET {', '.join(sets)} WHERE id=%s",
                tuple(params),
            )
            conn.commit()
            cur.execute(
                """
                SELECT id, name, location_summary, starts_at, ends_at, status, config_json,
                       created_by, updated_by, created_at, updated_at
                FROM cura_mi_events WHERE id = %s
                """,
                (event_id,),
            )
            r = cur.fetchone()
            if not r:
                return jsonify({"error": "Not found"}), 404
            return jsonify({"item": _row_event(r)}), 200
        except Exception as e:
            conn.rollback()
            logger.exception("mi_admin_event_one: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(p("admin/reports/pending"), methods=["GET", "OPTIONS"])
    def mi_admin_reports_pending():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, event_id, public_id, status, payload_json, rejection_reason, submitted_by, created_at
                FROM cura_mi_reports
                WHERE status IN ('rejected', 'draft')
                ORDER BY updated_at DESC
                LIMIT 200
                """
            )
            items = [
                {
                    "id": r[0],
                    "event_id": r[1],
                    "public_id": r[2],
                    "status": r[3],
                    "payload": safe_json(r[4]),
                    "rejection_reason": r[5],
                    "submitted_by": r[6],
                    "created_at": r[7].isoformat() if r[7] else None,
                }
                for r in cur.fetchall()
            ]
            return jsonify({"items": items}), 200
        finally:
            cur.close()
            conn.close()

    @bp.route(p("events/<int:event_id>/export"), methods=["GET", "OPTIONS"])
    def mi_event_export(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, public_id, status, payload_json, submitted_by, created_at
                FROM cura_mi_reports WHERE event_id = %s AND status = %s
                """,
                (event_id, "submitted"),
            )
            rows = cur.fetchall()
            export_obj = {
                "event_id": event_id,
                "report_count": len(rows),
                "reports": [
                    {
                        "id": r[0],
                        "public_id": r[1],
                        "status": r[2],
                        "submitted_by": r[4],
                        "created_at": r[5].isoformat() if r[5] else None,
                    }
                    for r in rows
                ],
            }
            _audit_epcr_api(f"MI export requested event={event_id} count={len(rows)}")
            return jsonify(export_obj), 200
        finally:
            cur.close()
            conn.close()
