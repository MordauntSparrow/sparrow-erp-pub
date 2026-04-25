"""
Exact handover base: ``/plugin/safeguarding_module/api`` (CURA_CLINICAL_SYSTEMS_HANDOVER.md).

The paired SPA uses this prefix only; all safeguarding traffic hits these routes.
"""
from __future__ import annotations

import json
import logging
import os
import uuid

from flask import Blueprint
from werkzeug.utils import secure_filename

logger = logging.getLogger("medical_records_module.safeguarding_module_api")

safeguarding_module_api_bp = Blueprint(
    "safeguarding_module_api",
    __name__,
    url_prefix="/plugin/safeguarding_module/api",
)

_UPLOAD_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "data", "cura_uploads")
)


def _attach_routes(bp: Blueprint) -> None:
    from flask import jsonify, request, send_file
    from app.objects import get_db_connection

    from .cura_util import safe_json
    from .routes import (
        _audit_epcr_api,
        _cura_auth_principal,
        _epcr_privileged_role,
        _require_epcr_json_api,
        _user_may_access_case_data,
    )
    from .safeguarding_auth import (
        CREW_REFERRAL_VISIBILITY_SQL,
        SafeguardingAuditError,
        assignment_guard_json_response,
        crew_referral_visibility_params,
        insert_safeguarding_audit_event,
        principal_may_patch_safeguarding,
        principal_may_read_referral,
    )

    @bp.before_request
    def _safeguarding_plugin_api_gate():
        """Ventus-style: all ``/plugin/safeguarding_module/api/*`` require JWT or clinical session."""
        if request.method == "OPTIONS":
            return None
        return _require_epcr_json_api()

    def _uname():
        return _cura_auth_principal()[0] or ""

    def _row_sg_full(cur, referral_id):
        cur.execute(
            """
            SELECT id, public_id, client_local_id, operational_event_id, status, subject_type, record_version,
                   payload_json, sync_status, sync_error, created_by, updated_by, created_at, updated_at
            FROM cura_safeguarding_referrals WHERE id = %s
            """,
            (referral_id,),
        )
        return cur.fetchone()

    def _to_handover(r):
        if not r:
            return None
        pl = safe_json(r[7])
        return {
            "id": str(r[0]),
            "reference": r[1],
            "status": r[4],
            "createdAt": r[12].isoformat() if r[12] else None,
            "updatedAt": r[13].isoformat() if r[13] else None,
            "subjectType": r[5] or "",
            "data": pl,
            "syncStatus": r[8],
            "syncError": r[9],
            "version": r[6],
            "record_version": r[6],
            "createdBy": r[10],
            "operational_event_id": r[3],
        }

    def _may_read_referral(cur, r, uname):
        return principal_may_read_referral(
            cur,
            operational_event_id=r[3],
            created_by=r[10],
            username=uname,
            privileged=False,
        )

    def _may_status_change(r, uname):
        if _epcr_privileged_role():
            return True
        return (r[10] or "").strip() == (uname or "").strip()

    def _may_delete(r, uname):
        if _epcr_privileged_role():
            return True
        st = (r[4] or "").lower()
        if st != "draft":
            return False
        return (r[10] or "").strip() == (uname or "").strip()

    @bp.route("/referrals", methods=["GET", "POST", "OPTIONS"])
    def sg_referrals():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _uname()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if request.method == "GET":
                st_f = (request.args.get("status") or "").strip() or None
                sub_f = (request.args.get("subjectType") or request.args.get("subject_type") or "").strip() or None
                created_by = (request.args.get("createdBy") or "").strip() or None
                search = (request.args.get("search") or "").strip() or None
                q = (
                    "SELECT id, public_id, status, subject_type, payload_json, created_at, updated_at, created_by, operational_event_id "
                    "FROM cura_safeguarding_referrals WHERE 1=1"
                )
                params = []
                if st_f:
                    q += " AND status = %s"
                    params.append(st_f)
                if sub_f:
                    q += " AND subject_type = %s"
                    params.append(sub_f)
                if created_by:
                    q += " AND created_by = %s"
                    params.append(created_by)
                q += f" AND {CREW_REFERRAL_VISIBILITY_SQL}"
                params.extend(crew_referral_visibility_params(uname))
                if search:
                    q += " AND payload_json LIKE %s"
                    params.append(f"%{search}%")
                q += " ORDER BY updated_at DESC LIMIT 200"
                cur.execute(q, tuple(params))
                rows = cur.fetchall()
                items = []
                for r in rows:
                    oid = r[8]
                    items.append(
                        {
                            "id": str(r[0]),
                            "reference": r[1],
                            "status": r[2],
                            "subjectType": r[3] or "",
                            "createdAt": r[5].isoformat() if r[5] else None,
                            "updatedAt": r[6].isoformat() if r[6] else None,
                            "data": safe_json(r[4]),
                            "createdBy": r[7],
                            "operational_event_id": int(oid) if oid is not None else None,
                        }
                    )
                return jsonify({"referrals": items}), 200

            body = request.get_json(silent=True) or {}
            data = body.get("data")
            if data is None:
                data = body.get("payload")
            if not isinstance(data, (dict, list)):
                return jsonify({"error": "data (object) is required"}), 400
            payload_json = json.dumps(data)
            status = (body.get("status") or "draft").strip() or "draft"
            subject_type = (body.get("subjectType") or body.get("subject_type") or "").strip() or None
            idem = (body.get("idempotency_key") or body.get("idempotencyKey") or "").strip() or None
            operational_event_id = None
            raw_op = body.get("operational_event_id", body.get("operationalEventId"))
            if raw_op is not None and str(raw_op).strip() != "":
                try:
                    operational_event_id = int(raw_op)
                    if operational_event_id <= 0:
                        operational_event_id = None
                except (TypeError, ValueError):
                    return jsonify({"error": "operational_event_id must be a positive integer"}), 400
            ag = assignment_guard_json_response(
                cur, operational_event_id, uname, _epcr_privileged_role(), jsonify
            )
            if ag is not None:
                return ag
            public_id = str(uuid.uuid4())
            try:
                cur.execute(
                    """
                    INSERT INTO cura_safeguarding_referrals
                      (public_id, idempotency_key, operational_event_id, status, subject_type, payload_json, created_by, updated_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (public_id, idem, operational_event_id, status, subject_type, payload_json, uname, uname),
                )
            except Exception as ins_exc:
                if idem and ("1062" in str(ins_exc) or "Duplicate" in str(ins_exc)):
                    conn.rollback()
                    cur.execute(
                        "SELECT id FROM cura_safeguarding_referrals WHERE idempotency_key = %s LIMIT 1",
                        (idem,),
                    )
                    ex = cur.fetchone()
                    if ex:
                        r = _row_sg_full(cur, ex[0])
                        if r and not _may_read_referral(cur, r, uname):
                            return jsonify({"error": "Unauthorised"}), 403
                        return jsonify({**_to_handover(r), "deduplicated": True}), 200
                raise
            rid = cur.lastrowid
            try:
                insert_safeguarding_audit_event(cur, rid, uname, "create", None, required=True)
            except SafeguardingAuditError:
                conn.rollback()
                return (
                    jsonify(
                        {
                            "error": (
                                "Safeguarding audit log is unavailable; the referral was not saved. "
                                "Retry or contact support."
                            )
                        }
                    ),
                    503,
                )
            conn.commit()
            _audit_epcr_api(f"safeguarding_module API created referral id={rid}")
            return jsonify(_to_handover(_row_sg_full(cur, rid))), 201
        except Exception as e:
            conn.rollback()
            logger.exception("sg_referrals: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/referrals/prefill-from-epcr", methods=["POST", "OPTIONS"])
    def sg_prefill():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        body = request.get_json(silent=True) or {}
        raw_cid = body.get("case_id") if "case_id" in body else body.get("caseId")
        try:
            case_id = int(raw_cid)
        except (TypeError, ValueError):
            return jsonify({"error": "case_id is required"}), 400
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT data FROM cases WHERE id = %s", (case_id,))
            r = cur.fetchone()
            if not r:
                return jsonify({"error": "Case not found"}), 404
            case_data = safe_json(r[0])
            if not isinstance(case_data, dict) or not _user_may_access_case_data(case_data):
                return jsonify({"error": "Unauthorised"}), 403
            pt = None
            for sec in case_data.get("sections") or []:
                if isinstance(sec, dict) and (sec.get("name") or "").lower() == "patientinfo":
                    pt = (sec.get("content") or {}).get("ptInfo") or {}
                    break
            if not isinstance(pt, dict):
                pt = {}
            home = pt.get("homeAddress") or {}
            out = {
                "forename": pt.get("forename"),
                "surname": pt.get("surname"),
                "dob": pt.get("dob"),
                "address": home.get("address"),
                "postcode": home.get("postcode"),
                "nhsNumber": pt.get("nhsNumber"),
            }
            _audit_epcr_api(f"safeguarding_module prefill case {case_id}")
            return jsonify({"case_id": case_id, "prefill": out}), 200
        finally:
            cur.close()
            conn.close()

    @bp.route("/referrals/<int:referral_id>", methods=["GET", "PUT", "DELETE", "OPTIONS"])
    def sg_one(referral_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _uname()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            r = _row_sg_full(cur, referral_id)
            if not r:
                return jsonify({"error": "Not found"}), 404
            if not _may_read_referral(cur, r, uname):
                return jsonify({"error": "Unauthorised"}), 403

            if request.method == "GET":
                return jsonify(_to_handover(r)), 200

            if request.method == "DELETE":
                if not _may_delete(r, uname):
                    return jsonify({"error": "Unauthorised"}), 403
                cur.execute("DELETE FROM cura_safeguarding_referrals WHERE id = %s", (referral_id,))
                conn.commit()
                _audit_epcr_api(f"safeguarding_module deleted referral id={referral_id}")
                return jsonify({"ok": True}), 200

            body = request.get_json(silent=True) or {}
            try:
                exp_ver = int(body.get("record_version") or body.get("expected_version") or body.get("version"))
            except (TypeError, ValueError):
                return jsonify({"error": "record_version is required"}), 400
            st = (body.get("status") or r[4]).strip() or r[4]
            subject_type = body.get("subjectType") or body.get("subject_type")
            if subject_type is None:
                subject_type = r[5]
            data = body.get("data")
            sync_status = body.get("syncStatus")
            sync_error = body.get("syncError")
            payload_json = None
            if data is not None:
                if isinstance(data, (dict, list)):
                    payload_json = json.dumps(data)
                else:
                    return jsonify({"error": "data must be an object"}), 400
            if not principal_may_patch_safeguarding(
                privileged=_epcr_privileged_role(),
                row_status=r[4],
                created_by=r[10],
                username=uname,
            ):
                return jsonify({"error": "Unauthorised"}), 403
            prev_status = (r[4] or "").strip()
            sets = [
                "status=%s",
                "subject_type=%s",
                "updated_by=%s",
                "record_version=record_version+1",
            ]
            params = [st, subject_type, uname]
            if payload_json is not None:
                sets.insert(2, "payload_json=%s")
                params.insert(2, payload_json)
            if sync_status is not None:
                sets.append("sync_status=%s")
                params.append(sync_status)
            if sync_error is not None:
                sets.append("sync_error=%s")
                params.append(sync_error)
            new_oid = None
            oid_changed = False
            if "operational_event_id" in body or "operationalEventId" in body:
                raw_op = body.get("operational_event_id", body.get("operationalEventId"))
                if (r[4] or "").lower() == "draft":
                    if raw_op is None or str(raw_op).strip() == "":
                        sets.append("operational_event_id=%s")
                        params.append(None)
                        new_oid = None
                        oid_changed = True
                    else:
                        try:
                            oid = int(raw_op)
                            if oid <= 0:
                                return jsonify({"error": "operational_event_id must be positive"}), 400
                            ag = assignment_guard_json_response(
                                cur, oid, uname, _epcr_privileged_role(), jsonify
                            )
                            if ag is not None:
                                return ag
                            sets.append("operational_event_id=%s")
                            params.append(oid)
                            new_oid = oid
                            oid_changed = True
                        except (TypeError, ValueError):
                            return jsonify({"error": "operational_event_id must be an integer"}), 400
            params.extend([referral_id, exp_ver])
            sql = "UPDATE cura_safeguarding_referrals SET " + ", ".join(sets) + " WHERE id=%s AND record_version=%s"
            cur.execute(sql, tuple(params))
            if cur.rowcount != 1:
                conn.rollback()
                return jsonify({"error": "Version conflict", "record_version": r[6]}), 409
            new_st = (st or "").strip()
            parts_updated = []
            if payload_json is not None:
                parts_updated.append("referral_form")
            if sync_status is not None:
                parts_updated.append("sync_status")
            if sync_error is not None:
                parts_updated.append("sync_error")
            if oid_changed:
                parts_updated.append("operational_event")
            audit_detail = {"from_status": prev_status, "to_status": new_st}
            if parts_updated:
                audit_detail["parts_updated"] = parts_updated
            try:
                insert_safeguarding_audit_event(
                    cur, referral_id, uname, "patch", audit_detail, required=True
                )
            except SafeguardingAuditError:
                conn.rollback()
                return jsonify({"error": "Safeguarding audit unavailable; update aborted."}), 503
            conn.commit()
            return jsonify(_to_handover(_row_sg_full(cur, referral_id))), 200
        except Exception as e:
            conn.rollback()
            logger.exception("sg_one: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    def _post_status(referral_id, new_status):
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _uname()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            r = _row_sg_full(cur, referral_id)
            if not r:
                return jsonify({"error": "Not found"}), 404
            if not _may_read_referral(cur, r, uname):
                return jsonify({"error": "Unauthorised"}), 403
            if not _may_status_change(r, uname):
                return jsonify({"error": "Unauthorised"}), 403
            cur_st = (r[4] or "").lower()
            if cur_st == new_status.lower():
                return jsonify({**_to_handover(r), "deduplicated": True}), 200
            body = request.get_json(silent=True) or {}
            try:
                exp_ver = int(body.get("record_version") or body.get("expected_version") or body.get("version") or r[6])
            except (TypeError, ValueError):
                exp_ver = r[6]
            prev_status = (r[4] or "").strip()
            cur.execute(
                """
                UPDATE cura_safeguarding_referrals
                SET status=%s, updated_by=%s, record_version=record_version+1
                WHERE id=%s AND record_version=%s
                """,
                (new_status, uname, referral_id, exp_ver),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return jsonify({"error": "Version conflict", "record_version": r[6]}), 409
            action = "submit" if new_status.lower() == "submitted" else "close"
            if new_status.lower() not in ("submitted", "closed"):
                action = "status_change"
            try:
                insert_safeguarding_audit_event(
                    cur,
                    referral_id,
                    uname,
                    action,
                    {"from_status": prev_status, "to_status": new_status},
                    required=True,
                )
            except SafeguardingAuditError:
                conn.rollback()
                return jsonify({"error": "Safeguarding audit unavailable; status change aborted."}), 503
            conn.commit()
            _audit_epcr_api(f"safeguarding_module status {new_status} referral id={referral_id}")
            return jsonify(_to_handover(_row_sg_full(cur, referral_id))), 200
        except Exception as e:
            conn.rollback()
            logger.exception("_post_status: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/referrals/<int:referral_id>/submit", methods=["POST", "OPTIONS"])
    def sg_submit(referral_id):
        if request.method == "OPTIONS":
            return "", 200
        return _post_status(referral_id, "submitted")

    @bp.route("/referrals/<int:referral_id>/close", methods=["POST", "OPTIONS"])
    def sg_close(referral_id):
        if request.method == "OPTIONS":
            return "", 200
        return _post_status(referral_id, "closed")

    @bp.route("/referrals/sync", methods=["POST", "OPTIONS"])
    def sg_sync_batch():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        body = request.get_json(silent=True) or {}
        items = body.get("referrals") or body.get("items") or []
        if not isinstance(items, list):
            return jsonify({"error": "referrals array required"}), 400
        return jsonify({"ok": True, "accepted": len(items), "message": "Process each via PUT/POST with idempotency keys"}), 200

    @bp.route("/referrals/<int:referral_id>/sync-submit", methods=["POST", "OPTIONS"])
    def sg_sync_submit(referral_id):
        if request.method == "OPTIONS":
            return "", 200
        return _post_status(referral_id, "submitted")

    @bp.route("/referrals/<int:referral_id>/sync-close", methods=["POST", "OPTIONS"])
    def sg_sync_close(referral_id):
        if request.method == "OPTIONS":
            return "", 200
        return _post_status(referral_id, "closed")

    @bp.route("/referrals/<int:referral_id>/audit", methods=["GET", "OPTIONS"])
    def sg_audit(referral_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _uname()
        try:
            lim = int(request.args.get("limit") or 100)
        except ValueError:
            lim = 100
        lim = max(1, min(lim, 500))
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            r = _row_sg_full(cur, referral_id)
            if not r:
                return jsonify({"error": "Not found"}), 404
            if not _may_read_referral(cur, r, uname):
                return jsonify({"error": "Unauthorised"}), 403
            cur.execute(
                """
                SELECT id, actor_username, action, detail_json, created_at
                FROM cura_safeguarding_audit_events
                WHERE referral_id = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (referral_id, lim),
            )
            items = [
                {
                    "id": a[0],
                    "actor_username": a[1],
                    "action": a[2],
                    "detail": safe_json(a[3]) if a[3] else None,
                    "created_at": a[4].isoformat() if a[4] else None,
                }
                for a in cur.fetchall()
            ]
            return jsonify({"items": items}), 200
        except Exception as e:
            if "cura_safeguarding_audit_events" in str(e):
                return jsonify({"items": []}), 200
            raise
        finally:
            cur.close()
            conn.close()

    @bp.route("/referrals/<int:referral_id>/attachments", methods=["GET", "POST", "OPTIONS"])
    def sg_attachments(referral_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _uname()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            r = _row_sg_full(cur, referral_id)
            if not r:
                return jsonify({"error": "Not found"}), 404
            if not _may_read_referral(cur, r, uname):
                return jsonify({"error": "Unauthorised"}), 403

            if request.method == "GET":
                cur.execute(
                    """
                    SELECT id, storage_key, original_filename, mime_type, byte_size, created_at
                    FROM cura_file_attachments
                    WHERE entity_type = 'safeguarding_referral' AND entity_id = %s
                    ORDER BY id ASC
                    """,
                    (referral_id,),
                )
                base = "/plugin/medical_records_module/api/files"
                items = [
                    {
                        "id": row[0],
                        "name": row[2],
                        "mime_type": row[3],
                        "byte_size": row[4],
                        "created_at": row[5].isoformat() if row[5] else None,
                        "url": f"{base}/{row[0]}",
                    }
                    for row in cur.fetchall()
                ]
                return jsonify({"attachments": items}), 200

            f = request.files.get("file")
            if not f or not f.filename:
                return jsonify({"error": "file is required (multipart)"}), 400
            try:
                max_mb = float(os.environ.get("CURA_UPLOAD_MAX_MB", "25"))
            except ValueError:
                max_mb = 25.0
            max_bytes = int(max_mb * 1024 * 1024)
            f.stream.seek(0, 2)
            sz = f.stream.tell()
            f.stream.seek(0)
            if sz > max_bytes:
                return jsonify({"error": f"File too large (max {max_mb} MB)"}), 413
            os.makedirs(_UPLOAD_ROOT, exist_ok=True)
            safe = secure_filename(f.filename) or "upload"
            store_name = f"{uuid.uuid4().hex}_{safe}"
            abs_path = os.path.join(_UPLOAD_ROOT, store_name)
            f.save(abs_path)
            storage_key = f"cura_uploads/{store_name}"
            cur.execute(
                """
                INSERT INTO cura_file_attachments
                  (entity_type, entity_id, storage_key, original_filename, mime_type, byte_size, created_by)
                VALUES ('safeguarding_referral', %s, %s, %s, %s, %s, %s)
                """,
                (referral_id, storage_key, f.filename, f.mimetype, sz, uname),
            )
            aid = cur.lastrowid
            conn.commit()
            _audit_epcr_api(f"safeguarding_module attachment id={aid} referral={referral_id}")
            return (
                jsonify(
                    {
                        "id": aid,
                        "url": f"/plugin/medical_records_module/api/files/{aid}",
                        "name": f.filename,
                    }
                ),
                201,
            )
        except Exception as e:
            conn.rollback()
            logger.exception("sg_attachments: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/referrals/<int:referral_id>/attachments/<int:attachment_id>", methods=["DELETE", "OPTIONS"])
    def sg_attachment_delete(referral_id, attachment_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _uname()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            r = _row_sg_full(cur, referral_id)
            if not r:
                return jsonify({"error": "Not found"}), 404
            if not _may_read_referral(cur, r, uname):
                return jsonify({"error": "Unauthorised"}), 403
            cur.execute(
                """
                SELECT id FROM cura_file_attachments
                WHERE id = %s AND entity_type = 'safeguarding_referral' AND entity_id = %s
                """,
                (attachment_id, referral_id),
            )
            if not cur.fetchone():
                return jsonify({"error": "Not found"}), 404
            if not _epcr_privileged_role() and (r[10] or "").strip() != uname:
                return jsonify({"error": "Unauthorised"}), 403
            cur.execute("DELETE FROM cura_file_attachments WHERE id = %s", (attachment_id,))
            conn.commit()
            return jsonify({"ok": True}), 200
        finally:
            cur.close()
            conn.close()


_attach_routes(safeguarding_module_api_bp)
