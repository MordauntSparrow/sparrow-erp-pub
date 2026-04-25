"""
Handover paths on the medical records blueprint (``/plugin/medical_records_module/api/...``).

SPA may use a single base ``.../plugin/medical_records_module/api`` for EPCR, datasets, config, and files.
"""
from __future__ import annotations

import json
import logging
import os
import uuid

logger = logging.getLogger("medical_records_module.handover_aliases")


def register(bp):
    import copy
    from datetime import datetime
    from flask import jsonify, request, send_file
    from werkzeug.utils import secure_filename
    from app.objects import get_db_connection

    from .cura_baseline_datasets import cura_resolved_dataset_payload
    from .cura_util import safe_json
    from .routes import (
        _cura_auth_principal,
        _epcr_privileged_role,
        _require_epcr_json_api,
        _user_may_access_case_data as _may_case,
        _parse_case_json,
        _normalize_epcr_review_drugs_sections,
        _strip_epcr_link_keys_from_dict,
        _epcr_case_save_link_meta,
        _epcr_server_ack,
        _sync_case_patient_match_meta,
    )

    _UPLOAD_ROOT = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "data", "cura_uploads")
    )

    _DS = {
        "drugs": "drugs",
        "clinical-options": "clinical_options",
        "clinical_options": "clinical_options",
        "clinical-indicators": "clinical_indicators",
        "clinical_indicators": "clinical_indicators",
        "iv-fluids": "iv_fluids",
        "iv_fluids": "iv_fluids",
    }

    def _ds_canon(name: str) -> str | None:
        k = (name or "").strip().lower()
        return _DS.get(k, k if k.replace("_", "").isalnum() else None)

    @bp.route("/api/datasets/versions", methods=["GET", "OPTIONS"])
    def alias_dataset_versions():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT name, version FROM cura_tenant_datasets")
            return jsonify({row[0]: row[1] for row in cur.fetchall()}), 200
        except Exception as e:
            if "cura_tenant_datasets" in str(e):
                return jsonify({}), 503
            raise
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/datasets/<string:name>", methods=["GET", "PUT", "OPTIONS"])
    def alias_dataset_name(name):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        canon = _ds_canon(name)
        if not canon:
            return jsonify({"error": "Unknown dataset"}), 400
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if request.method == "GET":
                cur.execute(
                    "SELECT version, payload_json FROM cura_tenant_datasets WHERE name = %s",
                    (canon,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"version": 0, "data": None}), 200
                try:
                    data = json.loads(row[1]) if row[1] else None
                except json.JSONDecodeError:
                    data = None
                data = cura_resolved_dataset_payload(canon, data)
                return jsonify({"version": row[0], "data": data}), 200
            if not _epcr_privileged_role():
                return jsonify({"error": "Unauthorised"}), 403
            body = request.get_json(silent=True) or {}
            payload = body.get("data") if "data" in body else body.get("payload")
            if payload is None:
                return jsonify({"error": "data is required"}), 400
            pj = json.dumps(payload)
            uname = _cura_auth_principal()[0] or ""
            cur.execute("SELECT version FROM cura_tenant_datasets WHERE name = %s", (canon,))
            ex = cur.fetchone()
            if ex:
                nv = int(ex[0] or 0) + 1
                cur.execute(
                    "UPDATE cura_tenant_datasets SET payload_json=%s, version=%s, updated_by=%s WHERE name=%s",
                    (pj, nv, uname, canon),
                )
            else:
                nv = 1
                cur.execute(
                    "INSERT INTO cura_tenant_datasets (name, version, payload_json, updated_by) VALUES (%s,%s,%s,%s)",
                    (canon, nv, pj, uname),
                )
            conn.commit()
            return jsonify({"version": nv, "ok": True}), 200
        except Exception as e:
            conn.rollback()
            logger.exception("alias_dataset: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    _SETTINGS_KEY = "cura_app_settings"

    @bp.route("/api/config/app-settings", methods=["GET", "PUT", "OPTIONS"])
    def alias_app_settings():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if request.method == "GET":
                cur.execute(
                    "SELECT value_json FROM cura_tenant_settings WHERE setting_key = %s",
                    (_SETTINGS_KEY,),
                )
                row = cur.fetchone()
                if not row or not row[0]:
                    return (
                        jsonify(
                            {
                                "billingEnabled": False,
                                "dnarPhotoGateEnabled": False,
                                "serverAddress": "",
                                "localIpAddress": "",
                            }
                        ),
                        200,
                    )
                try:
                    return jsonify(json.loads(row[0])), 200
                except json.JSONDecodeError:
                    return jsonify({}), 200
            if not _epcr_privileged_role():
                return jsonify({"error": "Unauthorised"}), 403
            body = request.get_json(silent=True) or {}
            if not isinstance(body, dict):
                return jsonify({"error": "JSON object required"}), 400
            uname = _cura_auth_principal()[0] or ""
            vj = json.dumps(body)
            cur.execute("SELECT 1 FROM cura_tenant_settings WHERE setting_key = %s", (_SETTINGS_KEY,))
            if cur.fetchone():
                cur.execute(
                    "UPDATE cura_tenant_settings SET value_json=%s, updated_by=%s WHERE setting_key=%s",
                    (vj, uname, _SETTINGS_KEY),
                )
            else:
                cur.execute(
                    "INSERT INTO cura_tenant_settings (setting_key, value_json, updated_by) VALUES (%s,%s,%s)",
                    (_SETTINGS_KEY, vj, uname),
                )
            conn.commit()
            return jsonify({"ok": True}), 200
        finally:
            cur.close()
            conn.close()

    def _resolve_file_path(sk: str):
        if not sk or ".." in sk:
            return None
        sk = sk.replace("\\", "/").lstrip("/")
        if not sk.startswith("cura_uploads/"):
            return None
        rel = sk[13:]
        if not rel or ".." in rel:
            return None
        base = os.path.normpath(_UPLOAD_ROOT)
        full = os.path.normpath(os.path.join(_UPLOAD_ROOT, rel))
        if not full.startswith(base):
            return None
        return full if os.path.isfile(full) else None

    def _may_download_attachment(cur, row):
        et, eid = row[1], row[2]
        if et == "safeguarding_referral":
            cur.execute("SELECT created_by FROM cura_safeguarding_referrals WHERE id = %s", (eid,))
            r = cur.fetchone()
            if not r:
                return False
            if _epcr_privileged_role():
                return True
            return (r[0] or "").strip() == (_cura_auth_principal()[0] or "").strip()
        if et == "patient_contact_report":
            cur.execute("SELECT submitted_by FROM cura_patient_contact_reports WHERE id = %s", (eid,))
            r = cur.fetchone()
            if not r:
                return False
            if _epcr_privileged_role():
                return True
            return (r[0] or "").strip() == (_cura_auth_principal()[0] or "").strip()
        if et == "epcr_case":
            cur.execute("SELECT data FROM cases WHERE id = %s", (eid,))
            r = cur.fetchone()
            if not r:
                return False
            d = safe_json(r[0])
            return isinstance(d, dict) and _may_case(d)
        if et == "mi_event_document":
            if _epcr_privileged_role():
                return True
            cur.execute(
                """
                SELECT e.id FROM cura_mi_documents d
                JOIN cura_mi_events e ON e.id = d.event_id
                JOIN cura_mi_assignments a ON a.event_id = e.id AND a.principal_username = %s
                WHERE d.id = %s
                """,
                (_cura_auth_principal()[0] or "", row[2]),
            )
            return cur.fetchone() is not None
        return _epcr_privileged_role()

    @bp.route("/api/files/<int:file_id>", methods=["GET", "DELETE", "OPTIONS"])
    def alias_files_one(file_id):
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
                SELECT id, entity_type, entity_id, storage_key, original_filename, mime_type
                FROM cura_file_attachments WHERE id = %s
                """,
                (file_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            if not _may_download_attachment(cur, row):
                return jsonify({"error": "Unauthorised"}), 403
            if request.method == "DELETE":
                if not _epcr_privileged_role():
                    return jsonify({"error": "Unauthorised"}), 403
                cur.execute("DELETE FROM cura_file_attachments WHERE id = %s", (file_id,))
                conn.commit()
                return jsonify({"ok": True}), 200
            path = _resolve_file_path(row[3] or "")
            if not path:
                return jsonify({"error": "File not on disk"}), 404
            return send_file(path, mimetype=row[5], download_name=row[4], as_attachment=False)
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/files", methods=["POST", "OPTIONS"])
    def alias_files_post():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _cura_auth_principal()[0] or ""
        et = (request.form.get("entity_type") or "").strip()
        eid_raw = request.form.get("entity_id")
        f = request.files.get("file")
        if not et or eid_raw is None or not f or not f.filename:
            return jsonify({"error": "entity_type, entity_id, file required"}), 400
        try:
            eid = int(eid_raw)
        except ValueError:
            return jsonify({"error": "entity_id must be int"}), 400
        allowed = {"safeguarding_referral", "patient_contact_report", "epcr_case", "mi_event_document"}
        if et not in allowed:
            return jsonify({"error": "Invalid entity_type"}), 400
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if et == "safeguarding_referral":
                cur.execute("SELECT 1 FROM cura_safeguarding_referrals WHERE id = %s", (eid,))
                if not cur.fetchone():
                    return jsonify({"error": "Parent not found"}), 404
            elif et == "patient_contact_report":
                cur.execute("SELECT 1 FROM cura_patient_contact_reports WHERE id = %s", (eid,))
                if not cur.fetchone():
                    return jsonify({"error": "Parent not found"}), 404
            elif et == "epcr_case":
                cur.execute("SELECT data FROM cases WHERE id = %s", (eid,))
                rr = cur.fetchone()
                if not rr or not isinstance(safe_json(rr[0]), dict) or not _may_case(safe_json(rr[0])):
                    return jsonify({"error": "Parent not found or unauthorised"}), 403
            else:
                cur.execute("SELECT 1 FROM cura_mi_events WHERE id = %s", (eid,))
                if not cur.fetchone():
                    return jsonify({"error": "Event not found"}), 404
                if not _epcr_privileged_role():
                    cur.execute(
                        """
                        SELECT 1 FROM cura_mi_assignments
                        WHERE event_id = %s AND principal_username = %s
                        """,
                        (eid, uname),
                    )
                    if not cur.fetchone():
                        return jsonify({"error": "Unauthorised"}), 403
            try:
                max_mb = float(os.environ.get("CURA_UPLOAD_MAX_MB", "25"))
            except ValueError:
                max_mb = 25.0
            max_bytes = int(max_mb * 1024 * 1024)
            f.stream.seek(0, 2)
            sz = f.stream.tell()
            f.stream.seek(0)
            if sz > max_bytes:
                return jsonify({"error": "File too large"}), 413
            os.makedirs(_UPLOAD_ROOT, exist_ok=True)
            safe = secure_filename(f.filename) or "upload"
            store_name = f"{uuid.uuid4().hex}_{safe}"
            abs_path = os.path.join(_UPLOAD_ROOT, store_name)
            f.save(abs_path)
            sk = f"cura_uploads/{store_name}"
            if et == "mi_event_document":
                cur.execute(
                    """
                    INSERT INTO cura_mi_documents (event_id, name, doc_type, storage_key)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (eid, f.filename, request.form.get("doc_type") or "other", sk),
                )
                doc_pk = cur.lastrowid
                cur.execute(
                    """
                    INSERT INTO cura_file_attachments
                      (entity_type, entity_id, storage_key, original_filename, mime_type, byte_size, created_by)
                    VALUES ('mi_event_document', %s, %s, %s, %s, %s, %s)
                    """,
                    (doc_pk, sk, f.filename, f.mimetype, sz, uname),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO cura_file_attachments
                      (entity_type, entity_id, storage_key, original_filename, mime_type, byte_size, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (et, eid, sk, f.filename, f.mimetype, sz, uname),
                )
            aid = cur.lastrowid
            conn.commit()
            return (
                jsonify(
                    {
                        "id": aid,
                        "url": f"/plugin/medical_records_module/api/files/{aid}",
                    }
                ),
                201,
            )
        except Exception as e:
            conn.rollback()
            logger.exception("alias_files_post: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cases/sync", methods=["POST", "OPTIONS"])
    def alias_cases_sync():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        body = request.get_json(silent=True) or {}
        cases_in = body.get("cases") or body.get("items") or []
        if not isinstance(cases_in, list):
            return jsonify({"error": "cases array required"}), 400
        results = []
        for payload in cases_in:
            if not isinstance(payload, dict):
                continue

            case_id = payload.get("id")
            if not case_id:
                results.append({"error": "missing id", "ok": False})
                continue
            conn = get_db_connection()
            cursor = conn.cursor()
            try:
                _normalize_epcr_review_drugs_sections(payload)
                idem_hdr = request.headers.get("Idempotency-Key", "").strip() or None
                idem_body = (payload.get("idempotencyKey") or payload.get("idempotency_key") or "").strip() or None
                idem = idem_hdr or idem_body
                uname = _cura_auth_principal()[0] or ""
                if not _epcr_privileged_role():
                    assigned = payload.get("assignedUsers") or []
                    if not isinstance(assigned, list):
                        assigned = []
                    if uname not in assigned:
                        payload = {**payload, "assignedUsers": [uname]}
                if idem:
                    cursor.execute(
                        "SELECT id FROM cases WHERE idempotency_key = %s LIMIT 1",
                        (idem,),
                    )
                    ex = cursor.fetchone()
                    if ex:
                        results.append({"id": ex[0], "ok": True, "deduplicated": True})
                        continue
                cursor.execute(
                    "SELECT data, dispatch_reference, primary_callsign, dispatch_synced_at, record_version FROM cases WHERE id = %s",
                    (case_id,),
                )
                ex_row = cursor.fetchone()
                if ex_row:
                    existing_data = _parse_case_json(ex_row[0])
                    if not _may_case(existing_data):
                        results.append({"id": case_id, "ok": False, "error": "Unauthorised"})
                        continue
                    ex_meta = (ex_row[1], ex_row[2], ex_row[3], ex_row[4])
                else:
                    ex_meta = None
                case_payload = copy.deepcopy(payload)
                dr, pc, ds, next_rv, err = _epcr_case_save_link_meta(case_payload, ex_meta)
                if err:
                    results.append({"id": case_id, "ok": False, "error": "conflict"})
                    continue
                _strip_epcr_link_keys_from_dict(case_payload)
                payload_str = json.dumps(case_payload)
                status = payload.get("status", "in progress")
                created_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                updated_at = created_at
                ds_sql = ds.strftime("%Y-%m-%d %H:%M:%S") if ds else None
                cursor.execute(
                    """
                    INSERT INTO cases (id, data, status, created_at, updated_at,
                        dispatch_reference, primary_callsign, dispatch_synced_at, record_version, idempotency_key)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        data = VALUES(data), status = VALUES(status), updated_at = VALUES(updated_at),
                        dispatch_reference = VALUES(dispatch_reference), primary_callsign = VALUES(primary_callsign),
                        dispatch_synced_at = VALUES(dispatch_synced_at), record_version = VALUES(record_version),
                        idempotency_key = IFNULL(idempotency_key, VALUES(idempotency_key))
                    """,
                    (case_id, payload_str, status, created_at, updated_at, dr, pc, ds_sql, next_rv, idem),
                )
                _sync_case_patient_match_meta(cursor, case_id, case_payload)
                conn.commit()
                results.append({"id": case_id, "ok": True, "serverAck": _epcr_server_ack(case_id, next_rv, datetime.utcnow())})
            except Exception as e:
                conn.rollback()
                logger.exception("alias_cases_sync row: %s", e)
                results.append({"id": case_id, "ok": False, "error": "server"})
            finally:
                cursor.close()
                conn.close()
        return jsonify({"results": results}), 200
