"""Tenant-scoped datasets and app settings for Cura / EPCR SPA (`/api/cura/datasets/...`, `/api/cura/config/...`)."""
from __future__ import annotations

import json
import logging
import re

from .cura_baseline_datasets import cura_resolved_dataset_payload

logger = logging.getLogger("medical_records_module.cura_datasets")

_DATASET_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$", re.I)
_CANON_NAMES = {
    "incidentresponseoptions": "incident_response_options",
    "incident-response-options": "incident_response_options",
    "incident_response_options": "incident_response_options",
    "drugs": "drugs",
    "clinicaloptions": "clinical_options",
    "clinical-options": "clinical_options",
    "clinical_options": "clinical_options",
    "ivfluids": "iv_fluids",
    "iv-fluids": "iv_fluids",
    "iv_fluids": "iv_fluids",
    "clinicalindicators": "clinical_indicators",
    "clinical-indicators": "clinical_indicators",
    "clinical_indicators": "clinical_indicators",
    "snomedukambulanceconditions": "snomed_uk_ambulance_conditions",
    "snomed-uk-ambulance-conditions": "snomed_uk_ambulance_conditions",
    "snomed_uk_ambulance_conditions": "snomed_uk_ambulance_conditions",
    "snowstorm_conditions": "snomed_uk_ambulance_conditions",
}


def norm_cura_dataset_name(name: str) -> str | None:
    """Normalise a dataset slug for DB/API (shared with session admin UI)."""
    if not name:
        return None
    key = name.strip().lower().replace(" ", "_")
    return _CANON_NAMES.get(key, key if _DATASET_SLUG.match(key) else None)


def register(bp):
    from flask import request, jsonify
    from app.objects import get_db_connection

    from .routes import _audit_epcr_api, _epcr_privileged_role, _require_epcr_json_api

    @bp.route("/api/cura/datasets/versions", methods=["GET", "OPTIONS"])
    def cura_dataset_versions():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT name, version FROM cura_tenant_datasets")
            out = {row[0]: row[1] for row in cur.fetchall()}
            return jsonify({"versions": out}), 200
        except Exception as e:
            if "cura_tenant_datasets" in str(e) or "Unknown table" in str(e):
                return jsonify({"versions": {}, "message": "Run DB upgrade"}), 503
            logger.exception("cura_dataset_versions: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/datasets/<string:name>", methods=["GET", "PUT", "OPTIONS"])
    def cura_dataset_one(name):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        canon = norm_cura_dataset_name(name)
        if not canon:
            return jsonify({"error": "Invalid dataset name"}), 400
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if request.method == "GET":
                cur.execute(
                    "SELECT name, version, payload_json, updated_at FROM cura_tenant_datasets WHERE name = %s",
                    (canon,),
                )
                row = cur.fetchone()
                if not row:
                    if canon == "snomed_uk_ambulance_conditions":
                        pl0 = cura_resolved_dataset_payload(canon, None)
                        return (
                            jsonify(
                                {
                                    "name": canon,
                                    "version": 0,
                                    "payload": pl0,
                                    "updated_at": None,
                                }
                            ),
                            200,
                        )
                    return jsonify({"name": canon, "version": 0, "payload": None}), 200
                try:
                    pl = json.loads(row[2]) if row[2] else None
                except json.JSONDecodeError:
                    pl = None
                pl = cura_resolved_dataset_payload(canon, pl)
                return (
                    jsonify(
                        {
                            "name": row[0],
                            "version": row[1],
                            "payload": pl,
                            "updated_at": row[3].isoformat() if row[3] else None,
                        }
                    ),
                    200,
                )
            if not _epcr_privileged_role():
                return jsonify({"error": "Unauthorised"}), 403
            body = request.get_json(silent=True) or {}
            payload = body.get("payload")
            if payload is None:
                return jsonify({"error": "payload is required"}), 400
            pj = json.dumps(payload)
            cur.execute("SELECT version FROM cura_tenant_datasets WHERE name = %s", (canon,))
            ex = cur.fetchone()
            from .routes import _cura_auth_principal

            uname = _cura_auth_principal()[0] or ""
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
            _audit_epcr_api(f"Cura dataset PUT {canon} v={nv}")
            return jsonify({"name": canon, "version": nv, "ok": True}), 200
        except Exception as e:
            conn.rollback()
            logger.exception("cura_dataset_one: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    _SETTINGS_KEY = "cura_app_settings"

    @bp.route("/api/cura/config/app-settings", methods=["GET", "PUT", "OPTIONS"])
    def cura_app_settings():
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
                    "SELECT value_json, updated_at FROM cura_tenant_settings WHERE setting_key = %s",
                    (_SETTINGS_KEY,),
                )
                row = cur.fetchone()
                if not row or not row[0]:
                    return (
                        jsonify(
                            {
                                "settings": {
                                    "billingEnabled": False,
                                    "dnarPhotoGateEnabled": False,
                                    "serverAddress": "",
                                    "localIpAddress": "",
                                }
                            }
                        ),
                        200,
                    )
                try:
                    data = json.loads(row[0])
                except Exception:
                    data = {}
                return jsonify({"settings": data, "updated_at": row[1].isoformat() if row[1] else None}), 200
            if not _epcr_privileged_role():
                return jsonify({"error": "Unauthorised"}), 403
            body = request.get_json(silent=True) or {}
            settings = body.get("settings")
            if not isinstance(settings, dict):
                return jsonify({"error": "settings object is required"}), 400
            from .routes import _cura_auth_principal

            uname = _cura_auth_principal()[0] or ""
            vj = json.dumps(settings)
            cur.execute(
                "SELECT 1 FROM cura_tenant_settings WHERE setting_key = %s",
                (_SETTINGS_KEY,),
            )
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
            _audit_epcr_api("Cura app-settings updated")
            return jsonify({"ok": True, "settings": settings}), 200
        except Exception as e:
            conn.rollback()
            if "cura_tenant_settings" in str(e) or "Unknown table" in str(e):
                return jsonify({"error": "Run DB upgrade"}), 503
            logger.exception("cura_app_settings: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()
