"""
Handover-shaped safeguarding routes under ``/api/safeguarding/...`` (same blueprint prefix as EPCR).

Canonical implementation remains ``/api/cura/safeguarding/referrals``; this facade maps envelopes.
"""
from __future__ import annotations

import json
import logging
import uuid

logger = logging.getLogger("medical_records_module.safeguarding_facade")


def register(bp):
    from flask import request, jsonify
    from app.objects import get_db_connection

    from .cura_util import safe_json
    from .routes import (
        _audit_epcr_api,
        _cura_auth_principal,
        _epcr_privileged_role,
        _require_epcr_json_api,
    )
    from .safeguarding_auth import (
        CREW_REFERRAL_VISIBILITY_SQL,
        SafeguardingAuditError,
        crew_referral_visibility_params,
        insert_safeguarding_audit_event,
        principal_may_patch_safeguarding,
        principal_may_read_referral,
    )

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
        return {
            "id": str(r[0]),
            "reference": r[1],
            "status": r[4],
            "createdAt": r[12].isoformat() if r[12] else None,
            "updatedAt": r[13].isoformat() if r[13] else None,
            "subjectType": r[5] or "",
            "data": safe_json(r[7]),
            "record_version": r[6],
            "operational_event_id": r[3],
        }

    def _may_delete(r, uname):
        if _epcr_privileged_role():
            return True
        st = (r[4] or "").lower()
        if st != "draft":
            return False
        return (r[10] or "").strip() == (uname or "").strip()

    def _may_read_row(cur, r, uname):
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

    @bp.route("/api/safeguarding/referrals", methods=["GET", "POST", "OPTIONS"])
    def facade_sg_list_create():
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
                q = (
                    "SELECT id, public_id, status, subject_type, payload_json, created_at, updated_at, "
                    "created_by, operational_event_id "
                    "FROM cura_safeguarding_referrals WHERE 1=1"
                )
                params = []
                if st_f:
                    q += " AND status = %s"
                    params.append(st_f)
                if sub_f:
                    q += " AND subject_type = %s"
                    params.append(sub_f)
                q += f" AND {CREW_REFERRAL_VISIBILITY_SQL}"
                params.extend(crew_referral_visibility_params(uname))
                q += " ORDER BY updated_at DESC LIMIT 200"
                cur.execute(q, tuple(params))
                rows = cur.fetchall()
                items = []
                for r in rows:
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
                            "operational_event_id": int(r[8]) if r[8] is not None else None,
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
            public_id = str(uuid.uuid4())
            try:
                cur.execute(
                    """
                    INSERT INTO cura_safeguarding_referrals
                      (public_id, idempotency_key, status, subject_type, payload_json, created_by, updated_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (public_id, idem, status, subject_type, payload_json, uname, uname),
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
                        if r and not _may_read_row(cur, r, uname):
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
            _audit_epcr_api(f"Safeguarding facade created referral id={rid}")
            r = _row_sg_full(cur, rid)
            return jsonify(_to_handover(r)), 201
        except Exception as e:
            conn.rollback()
            logger.exception("facade_sg_list_create: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/safeguarding/referrals/<int:referral_id>", methods=["GET", "PUT", "DELETE", "OPTIONS"])
    def facade_sg_one(referral_id):
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
            if not _may_read_row(cur, r, uname):
                return jsonify({"error": "Unauthorised"}), 403

            if request.method == "GET":
                return jsonify(_to_handover(r)), 200

            if request.method == "DELETE":
                if not _may_delete(r, uname):
                    return jsonify({"error": "Unauthorised"}), 403
                # Row delete cascades to cura_safeguarding_audit_events; rely on application AuditLog via _audit_epcr_api.
                cur.execute("DELETE FROM cura_safeguarding_referrals WHERE id = %s", (referral_id,))
                conn.commit()
                _audit_epcr_api(f"Safeguarding facade deleted referral id={referral_id}")
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
            if data is None:
                payload_json = None
            elif isinstance(data, (dict, list)):
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
            sets = ["status=%s", "subject_type=%s", "updated_by=%s", "record_version=record_version+1"]
            params = [st, subject_type, uname]
            if payload_json is not None:
                sets.insert(2, "payload_json=%s")
                params.insert(2, payload_json)
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
            _audit_epcr_api(f"Safeguarding facade PUT referral id={referral_id}")
            r2 = _row_sg_full(cur, referral_id)
            return jsonify(_to_handover(r2)), 200
        except Exception as e:
            conn.rollback()
            logger.exception("facade_sg_one: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    def _post_status_change(referral_id, new_status):
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
            if not _may_read_row(cur, r, uname):
                return jsonify({"error": "Unauthorised"}), 403
            if not _may_status_change(r, uname):
                return jsonify({"error": "Unauthorised"}), 403
            cur_st = (r[4] or "").lower()
            if cur_st == new_status.lower():
                return jsonify({**_to_handover(r), "deduplicated": True}), 200
            body = request.get_json(silent=True) or {}
            try:
                exp_ver = int(body.get("record_version") or body.get("expected_version") or r[6])
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
            _audit_epcr_api(f"Safeguarding facade {new_status} referral id={referral_id}")
            r2 = _row_sg_full(cur, referral_id)
            return jsonify(_to_handover(r2)), 200
        except Exception as e:
            conn.rollback()
            logger.exception("facade_sg_status: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/safeguarding/referrals/<int:referral_id>/submit", methods=["POST", "OPTIONS"])
    def facade_sg_submit(referral_id):
        if request.method == "OPTIONS":
            return "", 200
        return _post_status_change(referral_id, "submitted")

    @bp.route("/api/safeguarding/referrals/<int:referral_id>/close", methods=["POST", "OPTIONS"])
    def facade_sg_close(referral_id):
        if request.method == "OPTIONS":
            return "", 200
        return _post_status_change(referral_id, "closed")
