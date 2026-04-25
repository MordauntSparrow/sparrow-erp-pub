"""
Cura extension JSON API — operational grouping, safeguarding, patient-contact reports.

Routes are registered on ``internal_bp`` under ``/api/cura/...`` (same plugin prefix as EPCR).
"""
from __future__ import annotations

import json
import logging
import re
import uuid
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

logger = logging.getLogger("medical_records_module.cura")

# Bumped when DB contract or capability flags change (client may check via /capabilities).
CURA_SCHEMA_VERSION = 9


def _parse_iso_dt(v):
    if v is None or v == "":
        return None
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def register(bp):
    """Attach Cura routes to the medical records internal blueprint (call after routes module is loaded)."""
    import os

    from flask import request, jsonify, send_file
    from werkzeug.utils import secure_filename
    from app.objects import (
        AuthManager,
        User,
        find_tb_contractors_for_api_login,
        get_contractor_effective_role,
        get_db_connection,
        get_tb_contractor_portal_row,
        linked_user_contractor_pair,
    )

    from app.auth_jwt import DEFAULT_EXPIRY_HOURS
    from . import cura_patient_trace as cpt
    from . import cura_event_ventus_bridge as cevb
    from .cura_util import safe_json
    from . import cura_event_debrief as ced
    from .routes import (
        _EPCR_JSON_API_ROLES,
        _audit_epcr_api,
        _cura_auth_principal,
        _epcr_privileged_role,
        _epcr_user_display_name_from_user_row,
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

    _UPLOAD_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "data", "cura_uploads"))

    def _actor_uname():
        return _cura_auth_principal()[0] or ""

    def _may_patch_operational_event(row, uname):
        if _epcr_privileged_role():
            return True
        return (row[9] or "").strip() == (uname or "").strip()

    def _assignment_guard_error(cur, operational_event_id, username):
        if not operational_event_id or not username:
            return None
        if _epcr_privileged_role():
            return None
        try:
            cur.execute(
                "SELECT enforce_assignments FROM cura_operational_events WHERE id = %s",
                (operational_event_id,),
            )
            r = cur.fetchone()
            if not r:
                return (jsonify({"error": "Operational period not found"}), 404)
            if not r[0]:
                return None
            cur.execute(
                """
                SELECT 1 FROM cura_operational_event_assignments
                WHERE operational_event_id = %s AND principal_username = %s
                """,
                (operational_event_id, username),
            )
            if cur.fetchone():
                return None
            return (jsonify({"error": "You are not assigned to this operational period"}), 403)
        except Exception as ex:
            if "cura_operational_event_assignments" in str(ex) or "Unknown column" in str(ex):
                return (jsonify({"error": "Run database upgrade (migration 004)"}), 503)
            raise

    def _may_patch_safeguarding(row, uname):
        if _epcr_privileged_role():
            return True
        created = (row[9] or "").strip()
        st = (row[4] or "").lower()
        if st in ("submitted", "closed", "archived"):
            return False
        return created == (uname or "").strip()

    def _may_patch_patient_contact(row, uname):
        if _epcr_privileged_role():
            return True
        sub = (row[11] or "").strip()
        st = (row[5] or "").lower()
        if st in ("submitted", "closed", "archived"):
            return False
        return sub == (uname or "").strip()

    def _row_event(r):
        (
            eid,
            slug,
            name,
            location_summary,
            starts_at,
            ends_at,
            status,
            config,
            enforce_assignments,
            created_by,
            updated_by,
            created_at,
            updated_at,
        ) = r
        cfg = config
        if isinstance(cfg, (bytes, str)):
            try:
                cfg = json.loads(cfg)
            except Exception:
                cfg = None
        try:
            enf = bool(int(enforce_assignments)) if enforce_assignments is not None else False
        except (TypeError, ValueError):
            enf = bool(enforce_assignments)
        return {
            "id": eid,
            "slug": slug,
            "name": name,
            "location_summary": location_summary,
            "starts_at": starts_at.isoformat() if starts_at else None,
            "ends_at": ends_at.isoformat() if ends_at else None,
            "status": status,
            "config": cfg,
            "enforce_assignments": enf,
            "created_by": created_by,
            "updated_by": updated_by,
            "created_at": created_at.isoformat() if created_at else None,
            "updated_at": updated_at.isoformat() if updated_at else None,
        }

    @bp.route("/api/cura/capabilities", methods=["GET", "OPTIONS"])
    def cura_capabilities():
        """
        Feature flags for Cura. ``operational_event_incident_report`` defaults True; set False only if you intentionally
        hide incident-report UI (Cura Minor Injury event manager uses it with MI ``config_json.operational_event_id``).
        """
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        return (
            jsonify(
                {
                    "schema_version": CURA_SCHEMA_VERSION,
                    "module": "medical_records_module",
                    "urls": {
                        "epcr_and_cura_json": "/plugin/medical_records_module/api",
                        "safeguarding_handover": "/plugin/safeguarding_module/api",
                        "minor_injury_cura": "/plugin/medical_records_module/api/cura/minor-injury",
                        "minor_injury_handover": "/plugin/minor_injury_module/api",
                        "patient_trace_match": "/plugin/medical_records_module/api/cura/patient-trace/match",
                        "mpi_flags_bundle": "/plugin/medical_records_module/api/cura/mpi/flags-bundle",
                        "operational_event_incident_report": "/plugin/medical_records_module/api/cura/operational-events/{eventId}/incident-report",
                        "cura_me_operational_context": "/plugin/medical_records_module/api/cura/me/operational-context",
                        "cura_users_search": "/plugin/medical_records_module/api/cura/users/search",
                        "cura_validate_mdts_callsign": "/plugin/medical_records_module/api/cura/validate-mdts-callsign",
                        "cura_operational_event_ventus_division_sync": "/plugin/medical_records_module/api/cura/operational-events/{eventId}/ventus-division/sync",
                        "cura_callsign_validation_log": "/plugin/medical_records_module/api/cura/operational-events/{eventId}/callsign-validation-log",
                        "cura_operational_event_cad_correlation": "/plugin/medical_records_module/api/cura/operational-events/{eventId}/cad-correlation",
                        "cura_dispatch_suggested_cad_for_epcr": "/plugin/medical_records_module/api/cura/dispatch/suggested-cad-for-epcr",
                    },
                    "features": {
                        "operational_events": True,
                        "safeguarding_referrals": True,
                        "patient_contact_reports": True,
                        "epcr_cases": True,
                        "standalone_mode": True,
                        "optional_dispatch_link": True,
                        "operational_event_record_counts": True,
                        "file_attachment_metadata": True,
                        "lookup_by_public_id": True,
                        "lookup_operational_event_by_slug": True,
                        "bearer_jwt_auth": True,
                        "operational_event_assignments": True,
                        "analytics_summary": True,
                        "dispatch_job_preview": True,
                        "dispatch_suggested_cad_for_epcr": True,
                        "attachment_multipart_upload": True,
                        "auth_me_refresh_logout": True,
                        "safeguarding_facade_paths": True,
                        "safeguarding_prefill_epcr": True,
                        "safeguarding_audit_log": True,
                        "minor_injury_api": True,
                        "minor_injury_handover_base": True,
                        "patient_trace_match": True,
                        "mpi_flags_bundle": True,
                        "operational_event_incident_report": True,
                        "operational_ventus_integration": True,
                        "operational_event_cad_correlation": True,
                        "tenant_datasets": True,
                        "epcr_case_attachments": True,
                        "attachment_download": True,
                    },
                }
            ),
            200,
        )

    @bp.route("/api/cura/mpi/flags-bundle", methods=["POST", "OPTIONS"])
    def cura_mpi_flags_bundle():
        """Resolve MPI + location and return active operational/clinical risk flags (Cura / crew JWT)."""
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        body = request.get_json(silent=True) or {}
        from . import cura_mpi

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            bundle = cura_mpi.build_flags_bundle_from_triage_fields(cur, body)
            cura_mpi.audit_log_mpi_access(
                cur,
                _actor_uname(),
                "cura_mpi_flags_bundle",
                {"mpi_patient_id": bundle.get("mpiPatientId")},
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.exception("cura_mpi_flags_bundle: %s", e)
            return jsonify({"error": "Unable to load MPI flags"}), 500
        finally:
            cur.close()
            conn.close()
        return jsonify(bundle), 200

    def _tb_role_to_cura_jwt_role(tb_role: str) -> str:
        r = (tb_role or "").strip().lower()
        if r in ("superuser", "admin", "clinical_lead", "support_break_glass"):
            return r
        return "crew"

    @bp.route("/api/cura/auth/token", methods=["POST", "OPTIONS"])
    def cura_auth_token():
        """Issue a short-lived JWT for SPA / MDT (`Authorization: Bearer <token>` on EPCR + Cura JSON)."""
        if request.method == "OPTIONS":
            return "", 200
        from app.auth_jwt import encode_session_token
        from app.compliance_audit import log_security_event

        body = request.get_json(silent=True) or {}
        raw_username = (body.get("username") or "").strip()
        password = body.get("password") or ""
        if not raw_username or not password:
            return jsonify({"error": "username and password are required"}), 400
        login_key = raw_username.lower()
        user_data = User.get_user_by_username_ci(login_key)
        contractor_rows = find_tb_contractors_for_api_login(login_key)

        if len(contractor_rows) > 1:
            return jsonify({"error": "Invalid credentials"}), 401

        if user_data and contractor_rows:
            if not linked_user_contractor_pair(user_data, contractor_rows[0]):
                return jsonify(
                    {
                        "error": (
                            "This login matches both a Sparrow user and a contractor. "
                            "Change one of the accounts so the login name is unique, or link the "
                            "user to the contractor (users.contractor_id / matching email), "
                            "then try again."
                        ),
                    }
                ), 409

        if user_data:
            from app.support_access import support_login_blocked_reason

            if not user_data.get("password_hash"):
                return jsonify({"error": "Invalid credentials"}), 401
            if not AuthManager.verify_password(user_data["password_hash"], password):
                return jsonify({"error": "Invalid credentials"}), 401
            _sup = support_login_blocked_reason(user_data)
            if _sup:
                return jsonify({"error": _sup}), 403
            role = (user_data.get("role") or "").strip().lower()
            if role not in _EPCR_JSON_API_ROLES:
                return jsonify({"error": "Account is not authorised for Cura / EPCR API"}), 403
            try:
                tok = encode_session_token(
                    user_data["id"], user_data["username"], user_data["role"]
                )
            except RuntimeError as e:
                logger.warning("cura_auth_token: %s", e)
                return jsonify({"error": str(e)}), 503
            if not tok:
                return jsonify({"error": "Token signing unavailable"}), 503
            log_security_event(
                "api_login_success",
                user_id=user_data.get("id"),
                role=user_data.get("role"),
            )
            try:
                exp_h = int(os.environ.get("SESSION_TOKEN_EXPIRY_HOURS") or DEFAULT_EXPIRY_HOURS)
            except ValueError:
                exp_h = DEFAULT_EXPIRY_HOURS
            return (
                jsonify(
                    {
                        "access_token": tok,
                        "token_type": "Bearer",
                        "expires_in_hours": exp_h,
                    }
                ),
                200,
            )

        if contractor_rows:
            c = contractor_rows[0]
            cid = int(c["id"])
            ph = c.get("password_hash")
            try:
                pw_ok = bool(ph) and AuthManager.verify_password(ph, password)
            except (ValueError, TypeError, AttributeError):
                pw_ok = False
            if not pw_ok:
                return jsonify({"error": "Invalid credentials"}), 401
            if str(c.get("status") or "").lower() not in ("active", "1", "true", "yes"):
                return jsonify({"error": "Invalid credentials"}), 401
            tb_role = get_contractor_effective_role(cid)
            jwt_role = _tb_role_to_cura_jwt_role(tb_role)
            if jwt_role not in _EPCR_JSON_API_ROLES:
                return jsonify({"error": "Account is not authorised for Cura / EPCR API"}), 403
            canonical_username = (c.get("username") or "").strip().lower() or login_key
            try:
                tok = encode_session_token(f"c:{cid}", canonical_username, jwt_role)
            except RuntimeError as e:
                logger.warning("cura_auth_token: %s", e)
                return jsonify({"error": str(e)}), 503
            if not tok:
                return jsonify({"error": "Token signing unavailable"}), 503
            log_security_event("api_login_success", contractor_id=cid, role=jwt_role)
            try:
                exp_h = int(os.environ.get("SESSION_TOKEN_EXPIRY_HOURS") or DEFAULT_EXPIRY_HOURS)
            except ValueError:
                exp_h = DEFAULT_EXPIRY_HOURS
            return (
                jsonify(
                    {
                        "access_token": tok,
                        "token_type": "Bearer",
                        "expires_in_hours": exp_h,
                    }
                ),
                200,
            )

        return jsonify({"error": "Invalid credentials"}), 401

    @bp.route("/api/cura/auth/me", methods=["GET", "OPTIONS"])
    def cura_auth_me():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname, role, sub = _cura_auth_principal()
        from .cura_role_map import handover_roles_for_sparrow_role

        hints = handover_roles_for_sparrow_role(role)
        cid = None
        if isinstance(sub, str) and sub.startswith("c:"):
            tail = sub[2:].strip()
            if tail.isdigit():
                cid = int(tail)
        contractor_row = get_tb_contractor_portal_row(cid) if cid is not None else None
        raw = (
            None
            if contractor_row
            else (User.get_user_by_username_ci(uname) if uname else None)
        )
        display_name = None
        email_out = ""
        user_id = sub if sub is not None else None
        if contractor_row:
            display_name = (contractor_row.get("name") or "").strip() or uname
            email_out = (contractor_row.get("email") or "").strip()
            user_id = int(contractor_row["id"])
        elif raw:
            display_name = (
                raw.get("name")
                or raw.get("full_name")
                or " ".join(
                    x for x in (raw.get("first_name"), raw.get("last_name")) if x
                ).strip()
                or None
            )
            email_out = (raw.get("email") or "") or ""
            user_id = raw.get("id")
        else:
            return jsonify({"error": "User not found"}), 404
        return (
            jsonify(
                {
                    "id": user_id,
                    "username": uname,
                    "name": display_name or uname,
                    "email": email_out,
                    "role": role,
                    "handover_roles": hints["handover_roles"],
                    "handover_hints": hints,
                }
            ),
            200,
        )

    @bp.route("/api/cura/auth/refresh", methods=["POST", "OPTIONS"])
    def cura_auth_refresh():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname, _role, sub = _cura_auth_principal()
        if not uname:
            return jsonify({"error": "Unauthenticated"}), 401
        try:
            from app.auth_jwt import encode_session_token

            if isinstance(sub, str) and sub.startswith("c:"):
                tail = sub[2:].strip()
                if not tail.isdigit():
                    return jsonify({"error": "Invalid token subject"}), 401
                cid = int(tail)
                tb_role = get_contractor_effective_role(cid)
                jwt_role = _tb_role_to_cura_jwt_role(tb_role)
                if jwt_role not in _EPCR_JSON_API_ROLES:
                    return jsonify(
                        {"error": "Account is not authorised for Cura / EPCR API"}
                    ), 403
                tok = encode_session_token(f"c:{cid}", uname, jwt_role)
            else:
                raw = User.get_user_by_username_ci(uname)
                if not raw:
                    return jsonify({"error": "User not found"}), 404
                role_lower = (raw.get("role") or "").strip().lower()
                if role_lower not in _EPCR_JSON_API_ROLES:
                    return jsonify(
                        {"error": "Account is not authorised for Cura / EPCR API"}
                    ), 403
                tok = encode_session_token(raw["id"], raw["username"], raw["role"])
        except RuntimeError as e:
            logger.warning("cura_auth_refresh: %s", e)
            return jsonify({"error": str(e)}), 503
        if not tok:
            return jsonify({"error": "Token signing unavailable"}), 503
        try:
            exp_h = int(os.environ.get("SESSION_TOKEN_EXPIRY_HOURS") or DEFAULT_EXPIRY_HOURS)
        except ValueError:
            exp_h = DEFAULT_EXPIRY_HOURS
        return (
            jsonify(
                {
                    "access_token": tok,
                    "token_type": "Bearer",
                    "expires_in_hours": exp_h,
                }
            ),
            200,
        )

    @bp.route("/api/cura/auth/logout", methods=["POST", "OPTIONS"])
    def cura_auth_logout():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        return (
            jsonify(
                {
                    "ok": True,
                    "message": "Logout acknowledged; discard Bearer token on the client.",
                }
            ),
            200,
        )

    def _sg_audit(cur, referral_id: int, actor: str, action: str, detail=None, *, required: bool = False):
        insert_safeguarding_audit_event(
            cur, referral_id, actor, action, detail, required=required
        )

    @bp.route(
        "/api/cura/operational-events/by-slug/<string:slug>",
        methods=["GET", "OPTIONS"],
    )
    def cura_operational_event_by_slug(slug):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not slug or not re.match(r"^[a-z0-9][a-z0-9_-]{0,94}$", slug, re.I):
            return jsonify({"error": "Invalid slug"}), 400
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, slug, name, location_summary, starts_at, ends_at, status, config,
                       enforce_assignments, created_by, updated_by, created_at, updated_at
                FROM cura_operational_events WHERE slug = %s
                """,
                (slug,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            return jsonify({"item": _row_event(row)}), 200
        except Exception as e:
            logger.exception("cura_operational_event_by_slug: %s", e)
            return jsonify({"error": "Unavailable; run DB upgrade if needed."}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/operational-events/<int:event_id>/record-counts",
        methods=["GET", "OPTIONS"],
    )
    def cura_operational_event_record_counts(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = (_actor_uname() or "").strip()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM cura_operational_events WHERE id = %s", (event_id,))
            if not cur.fetchone():
                return jsonify({"error": "Not found"}), 404

            cur.execute(
                """
                SELECT COALESCE(status, ''), COUNT(*) FROM cura_patient_contact_reports
                WHERE operational_event_id = %s GROUP BY status
                """,
                (event_id,),
            )
            pcr = {row[0] or "unknown": row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT COALESCE(status, ''), COUNT(*) FROM cura_safeguarding_referrals
                WHERE operational_event_id = %s AND created_by = %s GROUP BY status
                """,
                (event_id, uname),
            )
            sg = {row[0] or "unknown": row[1] for row in cur.fetchall()}

            return (
                jsonify(
                    {
                        "operational_event_id": event_id,
                        "patient_contact_reports_by_status": pcr,
                        "safeguarding_referrals_by_status": sg,
                        "totals": {
                            "patient_contact_reports": sum(pcr.values()),
                            "safeguarding_referrals": sum(sg.values()),
                        },
                    }
                ),
                200,
            )
        except Exception as e:
            logger.exception("cura_operational_event_record_counts: %s", e)
            return jsonify({"error": "Unavailable; run DB upgrade if needed."}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/operational-events/<int:event_id>", methods=["GET", "PATCH", "OPTIONS"])
    def cura_operational_event_one(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _actor_uname()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, slug, name, location_summary, starts_at, ends_at, status, config,
                       enforce_assignments, created_by, updated_by, created_at, updated_at
                FROM cura_operational_events WHERE id = %s
                """,
                (event_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404

            if request.method == "GET":
                return jsonify({"item": _row_event(row)}), 200

            if not _may_patch_operational_event(row, uname):
                return jsonify({"error": "Unauthorised"}), 403

            body = request.get_json(silent=True) or {}
            name = body.get("name")
            slug = body.get("slug")
            location_summary = body.get("location_summary")
            if location_summary is None:
                location_summary = body.get("locationSummary")
            starts_at = body.get("starts_at")
            if starts_at is None:
                starts_at = body.get("startsAt")
            ends_at = body.get("ends_at")
            if ends_at is None:
                ends_at = body.get("endsAt")
            status = body.get("status")
            config = body.get("config")

            slug_val = row[1]
            name_val = row[2]
            loc_val = row[3]
            sa_val = row[4]
            ea_val = row[5]
            st_val = row[6]
            cfg_val = row[7]
            enf_val = int(row[8] or 0) if row[8] is not None else 0
            old_status = str(row[6] or "").strip().lower()
            if "enforce_assignments" in body or "enforceAssignments" in body:
                raw_e = body.get("enforce_assignments", body.get("enforceAssignments"))
                enf_val = 1 if raw_e in (True, 1, "1", "true", "True") else 0

            if name is not None:
                name_val = (name or "").strip()
                if not name_val:
                    return jsonify({"error": "name cannot be empty"}), 400
            if slug is not None:
                s = (slug or "").strip()
                if s and not re.match(r"^[a-z0-9][a-z0-9_-]{0,94}$", s, re.I):
                    return jsonify({"error": "slug must be alphanumeric with optional _-"}), 400
                slug_val = s or slug_val
            if location_summary is not None:
                loc_val = (location_summary or "").strip() or None
            if starts_at is not None:
                sa_val = _parse_iso_dt(starts_at)
            if ends_at is not None:
                ea_val = _parse_iso_dt(ends_at)
            if status is not None:
                st_val = (status or "").strip() or st_val
            new_status = str(st_val or "").strip().lower()
            transitioning_done = new_status in ("closed", "archived") and old_status not in (
                "closed",
                "archived",
            )

            config_json = None
            config_explicit = config is not None
            if config is not None:
                if not isinstance(config, (dict, list)):
                    return jsonify({"error": "config must be a JSON object or array"}), 400
                config_json = json.dumps(config)
                cfg_val = config_json

            if transitioning_done:
                from .cura_event_inventory_bridge import (
                    release_event_kit_pool_if_configured,
                    strip_event_kit_pool_config,
                )

                rel = release_event_kit_pool_if_configured(
                    int(event_id), row[7], performed_by=uname
                )
                if not rel.get("ok"):
                    return jsonify(
                        {"error": rel.get("error") or "Kit pool release failed"}
                    ), 400
                if config_explicit:
                    if isinstance(config, dict):
                        c2 = dict(config)
                        c2.pop("inventory_event_kit_pool_location_id", None)
                        config_json = json.dumps(c2)
                        cfg_val = config_json
                else:
                    config_json = strip_event_kit_pool_config(row[7])
                    cfg_val = config_json
                    config_explicit = True

            if config_explicit:
                cur.execute(
                    """
                    UPDATE cura_operational_events SET
                      slug = %s, name = %s, location_summary = %s, starts_at = %s, ends_at = %s,
                      status = %s, config = %s, enforce_assignments = %s, updated_by = %s
                    WHERE id = %s
                    """,
                    (
                        slug_val,
                        name_val,
                        loc_val,
                        sa_val,
                        ea_val,
                        st_val,
                        config_json,
                        enf_val,
                        uname,
                        event_id,
                    ),
                )
            else:
                cur.execute(
                    """
                    UPDATE cura_operational_events SET
                      slug = %s, name = %s, location_summary = %s, starts_at = %s, ends_at = %s,
                      status = %s, enforce_assignments = %s, updated_by = %s
                    WHERE id = %s
                    """,
                    (slug_val, name_val, loc_val, sa_val, ea_val, st_val, enf_val, uname, event_id),
                )
            try:
                from . import cura_event_debrief as _ced_mi

                _ced_mi.push_operational_snapshot_to_mi_events(
                    cur,
                    int(event_id),
                    name=name_val,
                    location_summary=loc_val,
                    starts_at=sa_val,
                    ends_at=ea_val,
                    operational_status=st_val,
                    actor=uname,
                )
            except Exception as ex:
                logger.warning("cura_operational_event_one PATCH push MI: %s", ex)
            conn.commit()
            _audit_epcr_api(f"Cura operational event updated id={event_id}")
            cur.execute(
                """
                SELECT id, slug, name, location_summary, starts_at, ends_at, status, config,
                       enforce_assignments, created_by, updated_by, created_at, updated_at
                FROM cura_operational_events WHERE id = %s
                """,
                (event_id,),
            )
            row2 = cur.fetchone()
            return jsonify({"item": _row_event(row2)}), 200
        except Exception as e:
            conn.rollback()
            logger.exception("cura_operational_event_one: %s", e)
            if "Duplicate" in str(e) or "1062" in str(e):
                return jsonify({"error": "slug already in use"}), 409
            return jsonify({"error": str(e)}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/operational-events", methods=["GET", "POST", "OPTIONS"])
    def cura_operational_events():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err

        uname = _actor_uname()

        if request.method == "GET":
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT id, slug, name, location_summary, starts_at, ends_at, status, config,
                           enforce_assignments, created_by, updated_by, created_at, updated_at
                    FROM cura_operational_events
                    ORDER BY starts_at IS NULL, starts_at DESC, id DESC
                    LIMIT 200
                    """
                )
                rows = cur.fetchall()
                return jsonify({"items": [_row_event(r) for r in rows]}), 200
            except Exception as e:
                logger.exception("cura_operational_events GET: %s", e)
                return jsonify({"error": "Operational events unavailable; run DB upgrade if needed."}), 500
            finally:
                cur.close()
                conn.close()

        # POST
        body = request.get_json(silent=True) or {}
        name = (body.get("name") or "").strip()
        if not name:
            return jsonify({"error": "name is required"}), 400

        slug = (body.get("slug") or "").strip() or None
        if slug and not re.match(r"^[a-z0-9][a-z0-9_-]{0,94}$", slug, re.I):
            return jsonify({"error": "slug must be alphanumeric with optional _-"}), 400

        location_summary = (body.get("location_summary") or body.get("locationSummary") or "").strip() or None
        status = (body.get("status") or "draft").strip() or "draft"
        starts_at = body.get("starts_at") or body.get("startsAt")
        ends_at = body.get("ends_at") or body.get("endsAt")

        sa, ea = _parse_iso_dt(starts_at), _parse_iso_dt(ends_at)
        config = body.get("config")
        if config is not None and not isinstance(config, (dict, list)):
            return jsonify({"error": "config must be a JSON object or array"}), 400
        config_json = json.dumps(config) if config is not None else None

        if not slug:
            slug = f"evt-{uuid.uuid4().hex[:12]}"

        enf_ins = 0
        if "enforce_assignments" in body or "enforceAssignments" in body:
            raw_e = body.get("enforce_assignments", body.get("enforceAssignments"))
            enf_ins = 1 if raw_e in (True, 1, "1", "true", "True") else 0

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO cura_operational_events
                  (slug, name, location_summary, starts_at, ends_at, status, config, enforce_assignments, created_by, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (slug, name, location_summary, sa, ea, status, config_json, enf_ins, uname, uname),
            )
            eid = cur.lastrowid
            if eid:
                try:
                    from . import cura_event_debrief as _ced_mi

                    _ced_mi.ensure_mi_event_for_operational_period(
                        cur,
                        int(eid),
                        name=name,
                        location_summary=location_summary,
                        starts_at=sa,
                        ends_at=ea,
                        operational_status=status,
                        actor=uname,
                    )
                except Exception as ex:
                    logger.warning("cura_operational_events POST ensure MI: %s", ex)
            if eid:
                try:
                    from .cura_event_ventus_bridge import provision_operational_event_dispatch_division

                    prov = provision_operational_event_dispatch_division(
                        cur, conn, int(eid), uname, do_commit=False
                    )
                    if not prov.get("ok"):
                        logger.warning(
                            "cura_operational_events POST provision division: %s",
                            prov.get("error"),
                        )
                except Exception as ex:
                    logger.warning("cura_operational_events POST provision division: %s", ex)
            if eid and sa is not None and ea is not None:
                try:
                    from .cura_event_inventory_bridge import provision_and_link_event_kit_pool

                    provision_and_link_event_kit_pool(cur, conn, int(eid), name, uname)
                except Exception as ex:
                    logger.warning("cura_operational_events POST event kit pool: %s", ex)
            conn.commit()
            cur.execute(
                """
                SELECT id, slug, name, location_summary, starts_at, ends_at, status, config,
                       enforce_assignments, created_by, updated_by, created_at, updated_at
                FROM cura_operational_events WHERE id = %s
                """,
                (eid,),
            )
            row = cur.fetchone()
            _audit_epcr_api(f"Cura operational event created id={eid}")
            return jsonify({"item": _row_event(row)}), 201
        except Exception as e:
            conn.rollback()
            logger.exception("cura_operational_events POST: %s", e)
            if "Duplicate" in str(e) or "1062" in str(e):
                return jsonify({"error": "slug already in use"}), 409
            return jsonify({"error": str(e)}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/safeguarding/referrals", methods=["GET", "POST", "OPTIONS"])
    def cura_safeguarding_referrals():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err

        if request.method == "GET":
            ev_filter = request.args.get("operational_event_id") or request.args.get("operationalEventId")
            ev_id = None
            if ev_filter is not None and str(ev_filter).strip() != "":
                try:
                    ev_id = int(ev_filter)
                except ValueError:
                    return jsonify({"error": "operational_event_id must be an integer"}), 400
            uname = _actor_uname()
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                if ev_id is not None:
                    cur.execute(
                        """
                        SELECT id, public_id, client_local_id, operational_event_id, status, record_version,
                               sync_status, created_at, updated_at
                        FROM cura_safeguarding_referrals
                        WHERE operational_event_id = %s AND created_by = %s
                        ORDER BY updated_at DESC
                        LIMIT 200
                        """,
                        (ev_id, (uname or "").strip()),
                    )
                else:
                    (cu,) = crew_referral_visibility_params(uname)
                    cur.execute(
                        f"""
                        SELECT id, public_id, client_local_id, operational_event_id, status, record_version,
                               sync_status, created_at, updated_at
                        FROM cura_safeguarding_referrals
                        WHERE {CREW_REFERRAL_VISIBILITY_SQL}
                        ORDER BY updated_at DESC
                        LIMIT 200
                        """,
                        (cu,),
                    )
                rows = cur.fetchall()
                items = [
                    {
                        "id": r[0],
                        "public_id": r[1],
                        "client_local_id": r[2],
                        "operational_event_id": r[3],
                        "status": r[4],
                        "record_version": r[5],
                        "sync_status": r[6],
                        "created_at": r[7].isoformat() if r[7] else None,
                        "updated_at": r[8].isoformat() if r[8] else None,
                    }
                    for r in rows
                ]
                return jsonify({"items": items}), 200
            except Exception as e:
                logger.exception("cura_safeguarding_referrals GET: %s", e)
                return jsonify({"error": "Safeguarding store unavailable; run DB upgrade if needed."}), 500
            finally:
                cur.close()
                conn.close()

        body = request.get_json(silent=True) or {}
        idem = (body.get("idempotency_key") or body.get("idempotencyKey") or "").strip() or None
        public_id = (body.get("public_id") or body.get("publicId") or "").strip() or str(uuid.uuid4())
        client_local_id = (body.get("client_local_id") or body.get("clientLocalId") or "").strip() or None
        payload = body.get("payload") or body.get("payload_json")
        if payload is None:
            return jsonify({"error": "payload is required"}), 400
        if isinstance(payload, (dict, list)):
            payload_json = json.dumps(payload)
        elif isinstance(payload, str):
            payload_json = payload
        else:
            return jsonify({"error": "payload must be JSON-serializable"}), 400

        status = (body.get("status") or "draft").strip() or "draft"
        uname = _actor_uname()
        operational_event_id = body.get("operational_event_id") or body.get("operationalEventId")
        if operational_event_id is not None:
            try:
                operational_event_id = int(operational_event_id)
            except (TypeError, ValueError):
                return jsonify({"error": "operational_event_id must be an integer"}), 400
        else:
            operational_event_id = None

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if operational_event_id is not None:
                ag = _assignment_guard_error(cur, operational_event_id, uname)
                if ag is not None:
                    return ag

            if idem:
                cur.execute(
                    "SELECT id FROM cura_safeguarding_referrals WHERE idempotency_key = %s",
                    (idem,),
                )
                ex = cur.fetchone()
                if ex:
                    cur.execute(
                        "SELECT id, public_id, client_local_id, operational_event_id, status, record_version, payload_json, "
                        "created_at, updated_at, created_by FROM cura_safeguarding_referrals WHERE id = %s",
                        (ex[0],),
                    )
                    r = cur.fetchone()
                    if not principal_may_read_referral(
                        cur,
                        operational_event_id=r[3],
                        created_by=r[9],
                        username=uname,
                        privileged=False,
                    ):
                        return jsonify({"error": "Unauthorised"}), 403
                    return (
                        jsonify(
                            {
                                "item": {
                                    "id": r[0],
                                    "public_id": r[1],
                                    "client_local_id": r[2],
                                    "operational_event_id": r[3],
                                    "status": r[4],
                                    "record_version": r[5],
                                    "payload": safe_json(r[6]),
                                    "created_at": r[7].isoformat() if r[7] else None,
                                    "updated_at": r[8].isoformat() if r[8] else None,
                                },
                                "deduplicated": True,
                            }
                        ),
                        200,
                    )

            cur.execute(
                """
                INSERT INTO cura_safeguarding_referrals
                  (public_id, client_local_id, idempotency_key, status, payload_json, created_by, updated_by, operational_event_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (public_id, client_local_id, idem, status, payload_json, uname, uname, operational_event_id),
            )
            rid = cur.lastrowid
            try:
                _sg_audit(cur, rid, uname, "create", None, required=True)
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
            cur.execute(
                "SELECT id, public_id, client_local_id, operational_event_id, status, record_version, payload_json, "
                "created_at, updated_at FROM cura_safeguarding_referrals WHERE id = %s",
                (rid,),
            )
            r = cur.fetchone()
            _audit_epcr_api(f"Cura safeguarding referral created id={rid}")
            return (
                jsonify(
                    {
                        "item": {
                            "id": r[0],
                            "public_id": r[1],
                            "client_local_id": r[2],
                            "operational_event_id": r[3],
                            "status": r[4],
                            "record_version": r[5],
                            "payload": safe_json(r[6]),
                            "created_at": r[7].isoformat() if r[7] else None,
                            "updated_at": r[8].isoformat() if r[8] else None,
                        }
                    }
                ),
                201,
            )
        except Exception as e:
            conn.rollback()
            logger.exception("cura_safeguarding_referrals POST: %s", e)
            return jsonify({"error": str(e)}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/safeguarding/referrals/by-public-id/<uuid:public_id>",
        methods=["GET", "OPTIONS"],
    )
    def cura_safeguarding_referral_by_public_id(public_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        pid = str(public_id)
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, public_id, client_local_id, operational_event_id, status, record_version, payload_json,
                       sync_status, sync_error, created_by, updated_by, created_at, updated_at
                FROM cura_safeguarding_referrals WHERE public_id = %s
                """,
                (pid,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            uname = _actor_uname()
            if not principal_may_read_referral(
                cur,
                operational_event_id=row[3],
                created_by=row[9],
                username=uname,
                privileged=False,
            ):
                return jsonify({"error": "Unauthorised"}), 403
            return (
                jsonify(
                    {
                        "item": {
                            "id": row[0],
                            "public_id": row[1],
                            "client_local_id": row[2],
                            "operational_event_id": row[3],
                            "status": row[4],
                            "record_version": row[5],
                            "payload": safe_json(row[6]),
                            "sync_status": row[7],
                            "sync_error": row[8],
                            "created_by": row[9],
                            "updated_by": row[10],
                            "created_at": row[11].isoformat() if row[11] else None,
                            "updated_at": row[12].isoformat() if row[12] else None,
                        }
                    }
                ),
                200,
            )
        except Exception as e:
            logger.exception("cura_safeguarding_referral_by_public_id: %s", e)
            return jsonify({"error": "Unavailable; run DB upgrade if needed."}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/safeguarding/referrals/<int:referral_id>",
        methods=["GET", "PATCH", "OPTIONS"],
    )
    def cura_safeguarding_referral_one(referral_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _actor_uname()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, public_id, client_local_id, operational_event_id, status, record_version, payload_json,
                       sync_status, sync_error, created_by, updated_by, created_at, updated_at
                FROM cura_safeguarding_referrals WHERE id = %s
                """,
                (referral_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            if not principal_may_read_referral(
                cur,
                operational_event_id=row[3],
                created_by=row[9],
                username=uname,
                privileged=False,
            ):
                return jsonify({"error": "Unauthorised"}), 403

            if request.method == "GET":
                return (
                    jsonify(
                        {
                            "item": {
                                "id": row[0],
                                "public_id": row[1],
                                "client_local_id": row[2],
                                "operational_event_id": row[3],
                                "status": row[4],
                                "record_version": row[5],
                                "payload": safe_json(row[6]),
                                "sync_status": row[7],
                                "sync_error": row[8],
                                "created_by": row[9],
                                "updated_by": row[10],
                                "created_at": row[11].isoformat() if row[11] else None,
                                "updated_at": row[12].isoformat() if row[12] else None,
                            }
                        }
                    ),
                    200,
                )

            if not _may_patch_safeguarding(row, uname):
                return jsonify({"error": "Unauthorised"}), 403

            body = request.get_json(silent=True) or {}
            exp_ver = body.get("record_version")
            if exp_ver is None:
                exp_ver = body.get("expected_version")
            try:
                exp_ver = int(exp_ver)
            except (TypeError, ValueError):
                return jsonify({"error": "record_version (expected version) is required for PATCH"}), 400

            if "status" in body:
                st = (body.get("status") or "").strip() or row[4]
            else:
                st = row[4]

            payload_json = None
            if "payload" in body or "payload_json" in body:
                payload = body.get("payload") if "payload" in body else body.get("payload_json")
                if isinstance(payload, (dict, list)):
                    payload_json = json.dumps(payload)
                elif isinstance(payload, str):
                    payload_json = payload
                elif payload is None:
                    return jsonify({"error": "payload cannot be null when the field is sent"}), 400
                else:
                    return jsonify({"error": "payload must be JSON-serializable"}), 400

            op_ev_val = row[3]
            if "operational_event_id" in body or "operationalEventId" in body:
                raw_oe = body.get("operational_event_id", body.get("operationalEventId"))
                if raw_oe is None or raw_oe == "" or raw_oe is False:
                    op_ev_val = None
                else:
                    try:
                        op_ev_val = int(raw_oe)
                    except (TypeError, ValueError):
                        return jsonify({"error": "operational_event_id must be an integer or null"}), 400

            sync_status = body.get("sync_status")
            sync_error = body.get("sync_error")

            touched = (
                payload_json is not None
                or "status" in body
                or "operational_event_id" in body
                or "operationalEventId" in body
                or sync_status is not None
                or sync_error is not None
            )
            if not touched:
                return jsonify({"error": "No updatable fields provided"}), 400

            prev_status = (row[4] or "").strip()
            if op_ev_val != row[3]:
                if op_ev_val is not None:
                    ag = assignment_guard_json_response(
                        cur, op_ev_val, uname, _epcr_privileged_role(), jsonify
                    )
                    if ag is not None:
                        return ag

            sets = [
                "status=%s",
                "operational_event_id=%s",
                "sync_status=COALESCE(%s, sync_status)",
                "sync_error=COALESCE(%s, sync_error)",
                "updated_by=%s",
                "record_version=record_version+1",
            ]
            params = [st, op_ev_val, sync_status, sync_error, uname]
            if payload_json is not None:
                sets.insert(2, "payload_json=%s")
                params.insert(2, payload_json)
            params.extend([referral_id, exp_ver])
            sql = "UPDATE cura_safeguarding_referrals SET " + ", ".join(sets) + " WHERE id=%s AND record_version=%s"
            cur.execute(sql, tuple(params))

            if cur.rowcount != 1:
                conn.rollback()
                return jsonify({"error": "Version conflict or not found"}), 409
            new_st = (st or "").strip()
            audit_action = "patch"
            if new_st.lower() != prev_status.lower():
                if new_st.lower() == "submitted":
                    audit_action = "submit"
                elif new_st.lower() == "closed":
                    audit_action = "close"
                else:
                    audit_action = "status_change"
            parts_updated = []
            if payload_json is not None:
                parts_updated.append("referral_form")
            if op_ev_val != row[3]:
                parts_updated.append("operational_event")
            if sync_status is not None:
                parts_updated.append("sync_status")
            if sync_error is not None:
                parts_updated.append("sync_error")
            audit_detail = {"from_status": prev_status, "to_status": new_st}
            if parts_updated:
                audit_detail["parts_updated"] = parts_updated
            try:
                insert_safeguarding_audit_event(
                    cur, referral_id, uname, audit_action, audit_detail, required=True
                )
            except SafeguardingAuditError:
                conn.rollback()
                return jsonify({"error": "Safeguarding audit unavailable; update aborted."}), 503
            conn.commit()
            _audit_epcr_api(f"Cura safeguarding referral updated id={referral_id}")
            cur.execute(
                """
                SELECT id, public_id, client_local_id, operational_event_id, status, record_version, payload_json,
                       sync_status, sync_error, created_by, updated_by, created_at, updated_at
                FROM cura_safeguarding_referrals WHERE id = %s
                """,
                (referral_id,),
            )
            r2 = cur.fetchone()
            return (
                jsonify(
                    {
                        "item": {
                            "id": r2[0],
                            "public_id": r2[1],
                            "client_local_id": r2[2],
                            "operational_event_id": r2[3],
                            "status": r2[4],
                            "record_version": r2[5],
                            "payload": safe_json(r2[6]),
                            "sync_status": r2[7],
                            "sync_error": r2[8],
                            "created_by": r2[9],
                            "updated_by": r2[10],
                            "created_at": r2[11].isoformat() if r2[11] else None,
                            "updated_at": r2[12].isoformat() if r2[12] else None,
                        }
                    }
                ),
                200,
            )
        except Exception as e:
            conn.rollback()
            logger.exception("cura_safeguarding_referral_one: %s", e)
            return jsonify({"error": str(e)}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/safeguarding/referrals/prefill-from-epcr",
        methods=["POST", "OPTIONS"],
    )
    def cura_sg_prefill_from_epcr():
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
            _audit_epcr_api(f"Cura safeguarding prefill from EPCR case {case_id}")
            return jsonify({"case_id": case_id, "prefill": out}), 200
        except Exception as e:
            logger.exception("cura_sg_prefill_from_epcr: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/safeguarding/referrals/<int:referral_id>/audit",
        methods=["GET", "OPTIONS"],
    )
    def cura_sg_audit_list(referral_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _actor_uname()
        try:
            lim = int(request.args.get("limit") or 100)
        except ValueError:
            lim = 100
        lim = max(1, min(lim, 500))
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, public_id, client_local_id, operational_event_id, status, record_version, payload_json,
                       sync_status, sync_error, created_by, updated_by, created_at, updated_at
                FROM cura_safeguarding_referrals WHERE id = %s
                """,
                (referral_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            if not principal_may_read_referral(
                cur,
                operational_event_id=row[3],
                created_by=row[9],
                username=uname,
                privileged=False,
            ):
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
            if "cura_safeguarding_audit_events" in str(e) or "Unknown table" in str(e):
                return jsonify({"error": "Audit log unavailable; run DB upgrade", "items": []}), 503
            logger.exception("cura_sg_audit_list: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/patient-contact-reports", methods=["GET", "POST", "OPTIONS"])
    def cura_patient_contact_reports():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err

        if request.method == "GET":
            ev_filter = request.args.get("operational_event_id") or request.args.get("operationalEventId")
            ev_id = None
            if ev_filter is not None and str(ev_filter).strip() != "":
                try:
                    ev_id = int(ev_filter)
                except ValueError:
                    return jsonify({"error": "operational_event_id must be an integer"}), 400
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                if ev_id is not None:
                    cur.execute(
                        """
                        SELECT id, public_id, operational_event_id, client_local_id, status, record_version,
                               submitted_by, created_at, updated_at
                        FROM cura_patient_contact_reports
                        WHERE operational_event_id = %s
                        ORDER BY updated_at DESC
                        LIMIT 200
                        """,
                        (ev_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, public_id, operational_event_id, client_local_id, status, record_version,
                               submitted_by, created_at, updated_at
                        FROM cura_patient_contact_reports
                        ORDER BY updated_at DESC
                        LIMIT 200
                        """
                    )
                rows = cur.fetchall()
                items = [
                    {
                        "id": r[0],
                        "public_id": r[1],
                        "operational_event_id": r[2],
                        "client_local_id": r[3],
                        "status": r[4],
                        "record_version": r[5],
                        "submitted_by": r[6],
                        "created_at": r[7].isoformat() if r[7] else None,
                        "updated_at": r[8].isoformat() if r[8] else None,
                    }
                    for r in rows
                ]
                return jsonify({"items": items}), 200
            except Exception as e:
                logger.exception("cura_patient_contact_reports GET: %s", e)
                return jsonify({"error": "Patient-contact reports unavailable; run DB upgrade if needed."}), 500
            finally:
                cur.close()
                conn.close()

        body = request.get_json(silent=True) or {}
        idem = (body.get("idempotency_key") or body.get("idempotencyKey") or "").strip() or None
        public_id = (body.get("public_id") or body.get("publicId") or "").strip() or str(uuid.uuid4())
        client_local_id = (body.get("client_local_id") or body.get("clientLocalId") or "").strip() or None
        operational_event_id = body.get("operational_event_id") or body.get("operationalEventId")
        if operational_event_id is not None:
            try:
                operational_event_id = int(operational_event_id)
            except (TypeError, ValueError):
                return jsonify({"error": "operational_event_id must be an integer"}), 400
        else:
            operational_event_id = None

        payload = body.get("payload") or body.get("payload_json")
        if payload is None:
            return jsonify({"error": "payload is required"}), 400
        if isinstance(payload, (dict, list)):
            payload_json = json.dumps(payload)
        elif isinstance(payload, str):
            payload_json = payload
        else:
            return jsonify({"error": "payload must be JSON-serializable"}), 400

        status = (body.get("status") or "draft").strip() or "draft"
        uname = _actor_uname()

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if operational_event_id is not None:
                ag = _assignment_guard_error(cur, operational_event_id, uname)
                if ag is not None:
                    return ag

            if idem:
                cur.execute(
                    "SELECT id FROM cura_patient_contact_reports WHERE idempotency_key = %s",
                    (idem,),
                )
                ex = cur.fetchone()
                if ex:
                    cur.execute(
                        "SELECT id, public_id, operational_event_id, client_local_id, status, record_version, "
                        "payload_json, submitted_by, created_at, updated_at "
                        "FROM cura_patient_contact_reports WHERE id = %s",
                        (ex[0],),
                    )
                    r = cur.fetchone()
                    return (
                        jsonify(
                            {
                                "item": {
                                    "id": r[0],
                                    "public_id": r[1],
                                    "operational_event_id": r[2],
                                    "client_local_id": r[3],
                                    "status": r[4],
                                    "record_version": r[5],
                                    "payload": safe_json(r[6]),
                                    "submitted_by": r[7],
                                    "created_at": r[8].isoformat() if r[8] else None,
                                    "updated_at": r[9].isoformat() if r[9] else None,
                                },
                                "deduplicated": True,
                            }
                        ),
                        200,
                    )

            cur.execute(
                """
                INSERT INTO cura_patient_contact_reports
                  (public_id, operational_event_id, client_local_id, idempotency_key, status, payload_json, submitted_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (public_id, operational_event_id, client_local_id, idem, status, payload_json, uname),
            )
            rid = cur.lastrowid
            conn.commit()
            cur.execute(
                "SELECT id, public_id, operational_event_id, client_local_id, status, record_version, "
                "payload_json, submitted_by, created_at, updated_at FROM cura_patient_contact_reports WHERE id = %s",
                (rid,),
            )
            r = cur.fetchone()
            _audit_epcr_api(f"Cura patient-contact report created id={rid}")
            return (
                jsonify(
                    {
                        "item": {
                            "id": r[0],
                            "public_id": r[1],
                            "operational_event_id": r[2],
                            "client_local_id": r[3],
                            "status": r[4],
                            "record_version": r[5],
                            "payload": safe_json(r[6]),
                            "submitted_by": r[7],
                            "created_at": r[8].isoformat() if r[8] else None,
                            "updated_at": r[9].isoformat() if r[9] else None,
                        }
                    }
                ),
                201,
            )
        except Exception as e:
            conn.rollback()
            logger.exception("cura_patient_contact_reports POST: %s", e)
            return jsonify({"error": str(e)}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/patient-contact-reports/by-public-id/<uuid:public_id>",
        methods=["GET", "OPTIONS"],
    )
    def cura_patient_contact_report_by_public_id(public_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        pid = str(public_id)
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, public_id, operational_event_id, client_local_id, idempotency_key, status, record_version,
                       payload_json, sync_status, sync_error, last_server_ack_at, submitted_by, created_at, updated_at
                FROM cura_patient_contact_reports WHERE public_id = %s
                """,
                (pid,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            return (
                jsonify(
                    {
                        "item": {
                            "id": row[0],
                            "public_id": row[1],
                            "operational_event_id": row[2],
                            "client_local_id": row[3],
                            "idempotency_key": row[4],
                            "status": row[5],
                            "record_version": row[6],
                            "payload": safe_json(row[7]),
                            "sync_status": row[8],
                            "sync_error": row[9],
                            "last_server_ack_at": row[10].isoformat() if row[10] else None,
                            "submitted_by": row[11],
                            "created_at": row[12].isoformat() if row[12] else None,
                            "updated_at": row[13].isoformat() if row[13] else None,
                        }
                    }
                ),
                200,
            )
        except Exception as e:
            logger.exception("cura_patient_contact_report_by_public_id: %s", e)
            return jsonify({"error": "Unavailable; run DB upgrade if needed."}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/patient-contact-reports/<int:report_id>",
        methods=["GET", "PATCH", "OPTIONS"],
    )
    def cura_patient_contact_report_one(report_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _actor_uname()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, public_id, operational_event_id, client_local_id, idempotency_key, status, record_version,
                       payload_json, sync_status, sync_error, last_server_ack_at, submitted_by, created_at, updated_at
                FROM cura_patient_contact_reports WHERE id = %s
                """,
                (report_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404

            if request.method == "GET":
                return (
                    jsonify(
                        {
                            "item": {
                                "id": row[0],
                                "public_id": row[1],
                                "operational_event_id": row[2],
                                "client_local_id": row[3],
                                "idempotency_key": row[4],
                                "status": row[5],
                                "record_version": row[6],
                                "payload": safe_json(row[7]),
                                "sync_status": row[8],
                                "sync_error": row[9],
                                "last_server_ack_at": row[10].isoformat() if row[10] else None,
                                "submitted_by": row[11],
                                "created_at": row[12].isoformat() if row[12] else None,
                                "updated_at": row[13].isoformat() if row[13] else None,
                            }
                        }
                    ),
                    200,
                )

            if not _may_patch_patient_contact(row, uname):
                return jsonify({"error": "Unauthorised"}), 403

            body = request.get_json(silent=True) or {}
            exp_ver = body.get("record_version")
            if exp_ver is None:
                exp_ver = body.get("expected_version")
            try:
                exp_ver = int(exp_ver)
            except (TypeError, ValueError):
                return jsonify({"error": "record_version (expected version) is required for PATCH"}), 400

            if "status" in body:
                st = (body.get("status") or "").strip() or row[5]
            else:
                st = row[5]

            payload_json = None
            if "payload" in body or "payload_json" in body:
                payload = body.get("payload") if "payload" in body else body.get("payload_json")
                if isinstance(payload, (dict, list)):
                    payload_json = json.dumps(payload)
                elif isinstance(payload, str):
                    payload_json = payload
                elif payload is None:
                    return jsonify({"error": "payload cannot be null when the field is sent"}), 400
                else:
                    return jsonify({"error": "payload must be JSON-serializable"}), 400

            op_ev_val = row[2]
            if "operational_event_id" in body or "operationalEventId" in body:
                raw_oe = body.get("operational_event_id", body.get("operationalEventId"))
                if raw_oe is None or raw_oe == "" or raw_oe is False:
                    op_ev_val = None
                else:
                    try:
                        op_ev_val = int(raw_oe)
                    except (TypeError, ValueError):
                        return jsonify({"error": "operational_event_id must be an integer or null"}), 400

            sync_status = body.get("sync_status")
            sync_error = body.get("sync_error")
            ack_in = "last_server_ack_at" in body or "lastServerAckAt" in body
            if ack_in:
                raw_ack = body.get("last_server_ack_at", body.get("lastServerAckAt"))
                last_ack = _parse_iso_dt(raw_ack) if raw_ack not in (None, "", False) else None
            else:
                last_ack = None

            touched = (
                payload_json is not None
                or "status" in body
                or "operational_event_id" in body
                or "operationalEventId" in body
                or sync_status is not None
                or sync_error is not None
                or ack_in
            )
            if not touched:
                return jsonify({"error": "No updatable fields provided"}), 400

            sets = [
                "status=%s",
                "operational_event_id=%s",
                "sync_status=COALESCE(%s, sync_status)",
                "sync_error=COALESCE(%s, sync_error)",
                "record_version=record_version+1",
            ]
            params = [st, op_ev_val, sync_status, sync_error]
            if payload_json is not None:
                sets.insert(2, "payload_json=%s")
                params.insert(2, payload_json)
            if ack_in:
                sets.insert(-1, "last_server_ack_at=%s")
                params.append(last_ack)
            params.extend([report_id, exp_ver])
            sql = "UPDATE cura_patient_contact_reports SET " + ", ".join(sets) + " WHERE id=%s AND record_version=%s"
            cur.execute(sql, tuple(params))

            if cur.rowcount != 1:
                conn.rollback()
                return jsonify({"error": "Version conflict or not found"}), 409
            conn.commit()
            _audit_epcr_api(f"Cura patient-contact report updated id={report_id}")
            cur.execute(
                """
                SELECT id, public_id, operational_event_id, client_local_id, idempotency_key, status, record_version,
                       payload_json, sync_status, sync_error, last_server_ack_at, submitted_by, created_at, updated_at
                FROM cura_patient_contact_reports WHERE id = %s
                """,
                (report_id,),
            )
            r2 = cur.fetchone()
            return (
                jsonify(
                    {
                        "item": {
                            "id": r2[0],
                            "public_id": r2[1],
                            "operational_event_id": r2[2],
                            "client_local_id": r2[3],
                            "idempotency_key": r2[4],
                            "status": r2[5],
                            "record_version": r2[6],
                            "payload": safe_json(r2[7]),
                            "sync_status": r2[8],
                            "sync_error": r2[9],
                            "last_server_ack_at": r2[10].isoformat() if r2[10] else None,
                            "submitted_by": r2[11],
                            "created_at": r2[12].isoformat() if r2[12] else None,
                            "updated_at": r2[13].isoformat() if r2[13] else None,
                        }
                    }
                ),
                200,
            )
        except Exception as e:
            conn.rollback()
            logger.exception("cura_patient_contact_report_one: %s", e)
            return jsonify({"error": str(e)}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/operational-events/<int:event_id>/analytics-summary",
        methods=["GET", "OPTIONS"],
    )
    def cura_operational_event_analytics_summary(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        try:
            min_cell = int(request.args.get("min_cell", "5"))
        except ValueError:
            min_cell = 5
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM cura_operational_events WHERE id = %s", (event_id,))
            if not cur.fetchone():
                return jsonify({"error": "Not found"}), 404

            cur.execute(
                """
                SELECT COALESCE(status, ''), COUNT(*) FROM cura_patient_contact_reports
                WHERE operational_event_id = %s GROUP BY status
                """,
                (event_id,),
            )
            pcr_raw = {row[0] or "unknown": row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT COALESCE(status, ''), COUNT(*) FROM cura_safeguarding_referrals
                WHERE operational_event_id = %s GROUP BY status
                """,
                (event_id,),
            )
            sg_raw = {row[0] or "unknown": row[1] for row in cur.fetchall()}

            pcr_sup = ced.suppress_small_counts(pcr_raw, min_cell)
            sg_sup = ced.suppress_small_counts(sg_raw, min_cell)

            payload = {
                "operational_event_id": event_id,
                "min_cell": min_cell,
                "patient_contact_reports_by_status": pcr_sup,
                "safeguarding_referrals_by_status": sg_sup,
                "totals": {
                    "patient_contact_reports": sum(pcr_raw.values()),
                    "safeguarding_referrals": sum(sg_raw.values()),
                },
                "note": "Per-status counts below min_cell are null (suppressed); totals are not suppressed.",
            }

            deep_flag = (
                request.args.get("include_epcr_deep")
                or request.args.get("includeEpcrDeep")
                or ""
            )
            if str(deep_flag).lower() in ("1", "true", "yes"):
                try:
                    scan_lim = int(request.args.get("scan_limit", "2000") or "2000")
                except ValueError:
                    scan_lim = 2000
                scan_lim = max(100, min(scan_lim, 8000))
                from . import cura_ops_reporting as cor

                payload["epcr_anonymous"] = cor.build_deep_analytics_for_operational_event(
                    cur, event_id, scan_limit=scan_lim, min_cell=min_cell
                )
                payload["minor_injury_anonymous"] = cor.mi_anonymous_stats_for_operational_event(
                    cur, event_id, min_cell=min_cell
                )
                payload["note_deep"] = (
                    "Optional epcr_anonymous / minor_injury_anonymous included because "
                    "include_epcr_deep=1; histograms respect min_cell when > 0."
                )

            return jsonify(payload), 200
        except Exception as e:
            logger.exception("cura_operational_event_analytics_summary: %s", e)
            return jsonify({"error": "Unavailable; run DB upgrade if needed."}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/patient-trace/match", methods=["POST", "OPTIONS"])
    def cura_patient_trace_match():
        """
        Prior EPCR match for Patient Info verification (Cura).

        Body: ``{ "ptInfo": { ... } }`` or flat keys (nhsNumber, dob, forename, surname, postcode, homeAddress, …).
        Optional: ``excludeCaseId``, ``scanLimit`` (default 3500, max 8000), ``resultLimit`` (default 20, max 50).
        """
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        body = request.get_json(silent=True) or {}
        q = cpt.parse_query_ptinfo(body)
        q = cpt.enrich_query_from_body(body, q)
        if not cpt._query_has_minimum_signals(q):
            return (
                jsonify(
                    {
                        "schema_version": CURA_SCHEMA_VERSION,
                        "recommendation": "insufficient_input",
                        "matches": [],
                        "notes": [
                            "Provide NHS number, or DOB with postcode or name, or phone (see documentation).",
                        ],
                        "scannedCases": 0,
                    }
                ),
                200,
            )
        try:
            exclude_id = body.get("excludeCaseId") or body.get("exclude_case_id")
            exclude_id = int(exclude_id) if exclude_id is not None and str(exclude_id).strip() != "" else None
        except (TypeError, ValueError):
            exclude_id = None
        try:
            scan_limit = min(max(int(body.get("scanLimit") or body.get("scan_limit") or 3500), 100), 8000)
        except (TypeError, ValueError):
            scan_limit = 3500
        try:
            result_limit = min(max(int(body.get("resultLimit") or body.get("result_limit") or 20), 1), 50)
        except (TypeError, ValueError):
            result_limit = 20

        conn = get_db_connection()
        cur = conn.cursor()
        matches = []
        scanned = 0
        try:
            from . import cura_mpi

            def _parse_patient_match_meta_raw(raw):
                if raw is None:
                    return None
                if isinstance(raw, dict):
                    d = raw
                else:
                    d = safe_json(raw)
                if not isinstance(d, dict) or not isinstance(d.get("pt"), dict):
                    return None
                return d

            # Two-step id ordering avoids sort_buffer blowups on large `data` JSON.
            cur.execute(
                """
                SELECT id FROM cases
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (scan_limit,),
            )
            ordered_ids = [r[0] for r in cur.fetchall()]
            has_meta_col = cura_mpi._column_exists(cur, "cases", "patient_match_meta")
            by_id_meta = {}
            data_by_id = {}
            if ordered_ids:
                ph = ",".join(["%s"] * len(ordered_ids))
                if has_meta_col:
                    cur.execute(
                        f"""
                        SELECT id, patient_match_meta, status, updated_at, created_at
                        FROM cases
                        WHERE id IN ({ph})
                        """,
                        tuple(ordered_ids),
                    )
                    need_data_ids = []
                    for r in cur.fetchall():
                        cid, meta_raw, status, updated_at, created_at = (
                            r[0],
                            r[1],
                            r[2],
                            r[3],
                            r[4],
                        )
                        parsed = _parse_patient_match_meta_raw(meta_raw)
                        if parsed is not None:
                            by_id_meta[cid] = (parsed, status, updated_at, created_at)
                        else:
                            by_id_meta[cid] = (None, status, updated_at, created_at)
                            need_data_ids.append(cid)
                    if need_data_ids:
                        ph2 = ",".join(["%s"] * len(need_data_ids))
                        cur.execute(
                            f"SELECT id, data FROM cases WHERE id IN ({ph2})",
                            tuple(need_data_ids),
                        )
                        data_by_id = {row[0]: row[1] for row in cur.fetchall()}
                else:
                    cur.execute(
                        f"""
                        SELECT id, data, status, updated_at, created_at
                        FROM cases
                        WHERE id IN ({ph})
                        """,
                        tuple(ordered_ids),
                    )
                    for row in cur.fetchall():
                        data_by_id[row[0]] = row[1]
                        by_id_meta[row[0]] = (None, row[2], row[3], row[4])

            for cid in ordered_ids:
                if cid not in by_id_meta:
                    continue
                scanned += 1
                if exclude_id is not None and int(cid) == int(exclude_id):
                    continue
                meta, status, updated_at, created_at = by_id_meta[cid]
                if meta is not None:
                    access_stub = {"assignedUsers": meta.get("assignedUsers") if isinstance(meta.get("assignedUsers"), list) else []}
                    if not _user_may_access_case_data(access_stub):
                        continue
                    cand = meta["pt"]
                    pres = meta.get("presentingSnippet")
                else:
                    raw_data = data_by_id.get(cid)
                    if raw_data is None:
                        continue
                    data = safe_json(raw_data)
                    if not isinstance(data, dict):
                        continue
                    if not _user_may_access_case_data(data):
                        continue
                    cand = cpt.extract_ptinfo_from_case_payload(data)
                    pres = cpt.extract_presenting_snippet(data)
                score, reasons = cpt.score_match(q, cand)
                if score < 22.0:
                    continue
                hints = cpt.build_verification_hints(cand, updated_at)
                matches.append(
                    {
                        "caseId": cid,
                        "score": round(score, 1),
                        "reasons": reasons,
                        "caseStatus": status,
                        "updatedAt": updated_at.isoformat() if updated_at and hasattr(updated_at, "isoformat") else None,
                        "createdAt": created_at.isoformat() if created_at and hasattr(created_at, "isoformat") else None,
                        "verificationHints": hints,
                        "presentingComplaintSnippet": pres,
                    }
                )
            matches.sort(key=lambda m: (-m["score"], m.get("updatedAt") or ""))
            matches = matches[:result_limit]
            rec, notes = cpt.recommendation_for_matches(matches, q)
            _audit_epcr_api(
                f"patient_trace_match scanned={scanned} results={len(matches)} recommendation={rec}"
            )
            return (
                jsonify(
                    {
                        "schema_version": CURA_SCHEMA_VERSION,
                        "recommendation": rec,
                        "notes": notes,
                        "matches": matches,
                        "scannedCases": scanned,
                        "scanLimit": scan_limit,
                    }
                ),
                200,
            )
        except Exception as e:
            logger.exception("cura_patient_trace_match: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/operational-events/<int:event_id>/incident-report",
        methods=["GET", "OPTIONS"],
    )
    def cura_operational_event_incident_report(event_id):
        """
        Combined operational-period view for Cura dashboards: SG/PCR counts, linked EPCR hints, minor injury rollup.

        Query: ``min_cell`` (suppression, default 5). By default, injury analytics include every MI event whose Cura
        config links to this operational period. Pass ``mi_event_id`` / ``miEventId`` to restrict the rollup to one MI event.
        """
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        try:
            min_cell = int(request.args.get("min_cell", "5"))
        except ValueError:
            min_cell = 5
        mi_event_id = request.args.get("mi_event_id") or request.args.get("miEventId")
        try:
            mi_event_id = int(mi_event_id) if mi_event_id is not None and str(mi_event_id).strip() != "" else None
        except (TypeError, ValueError):
            mi_event_id = None

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            body = ced.build_operational_event_incident_body(
                cur,
                event_id,
                min_cell=min_cell,
                mi_event_id=mi_event_id,
                may_include_case=_user_may_access_case_data,
            )
            if body is None:
                return jsonify({"error": "Not found"}), 404

            _audit_epcr_api(
                f"incident_report operational_event_id={event_id} mi_filter={mi_event_id!r}"
            )
            return jsonify({"schema_version": CURA_SCHEMA_VERSION, **body}), 200
        except Exception as e:
            if "cura_mi_events" in str(e) or "Unknown table" in str(e):
                return jsonify({"error": "Run DB upgrade"}), 503
            logger.exception("cura_operational_event_incident_report: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/operational-events/<int:event_id>/cad-correlation",
        methods=["GET", "OPTIONS"],
    )
    def cura_operational_event_cad_correlation(event_id):
        """
        Ventus CAD / dispatch desk comms correlated to this operational period via ``config.ventus_division_slug``
        and ``mdt_jobs.division``. Clinical / admin / superuser only.
        """
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        try:
            max_cads = int(request.args.get("max_cads", "60"))
        except ValueError:
            max_cads = 60
        try:
            comms_per_cad = int(request.args.get("comms_per_cad", "24"))
        except ValueError:
            comms_per_cad = 24

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            payload = cevb.fetch_cad_dispatch_correlation_for_event(
                cur,
                event_id,
                max_cads=max_cads,
                comms_per_cad=comms_per_cad,
            )
            if payload.get("reason") == "operational_event_not_found":
                return jsonify({"error": "Not found"}), 404
            _audit_epcr_api(f"cad_correlation operational_event_id={event_id}")
            return jsonify({"schema_version": CURA_SCHEMA_VERSION, **payload}), 200
        except Exception as e:
            logger.exception("cura_operational_event_cad_correlation: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    def _may_manage_event_assignments(event_row, uname):
        if _epcr_privileged_role():
            return True
        return (event_row[9] or "").strip() == (uname or "").strip()

    @bp.route(
        "/api/cura/operational-events/<int:event_id>/assignments",
        methods=["GET", "POST", "OPTIONS"],
    )
    def cura_operational_event_assignments(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _actor_uname()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, slug, name, location_summary, starts_at, ends_at, status, config,
                       enforce_assignments, created_by, updated_by, created_at, updated_at
                FROM cura_operational_events WHERE id = %s
                """,
                (event_id,),
            )
            ev = cur.fetchone()
            if not ev:
                return jsonify({"error": "Not found"}), 404

            if request.method == "GET":
                cur.execute(
                    """
                    SELECT id, operational_event_id, principal_username, expected_callsign, assigned_by, created_at
                    FROM cura_operational_event_assignments
                    WHERE operational_event_id = %s
                    ORDER BY principal_username ASC
                    """,
                    (event_id,),
                )
                rows = cur.fetchall()
                items = [
                    {
                        "id": r[0],
                        "operational_event_id": r[1],
                        "principal_username": r[2],
                        "expected_callsign": (r[3] or "").strip() or None,
                        "assigned_by": r[4],
                        "created_at": r[5].isoformat() if r[5] else None,
                    }
                    for r in rows
                ]
                return jsonify({"items": items}), 200

            if not _may_manage_event_assignments(ev, uname):
                return jsonify({"error": "Unauthorised"}), 403
            body = request.get_json(silent=True) or {}
            principal = (body.get("principal_username") or body.get("username") or "").strip()
            if not principal:
                return jsonify({"error": "principal_username (or username) is required"}), 400
            exp_cs = body.get("expected_callsign") or body.get("expectedCallsign")
            exp_cs = (str(exp_cs).strip().upper() if exp_cs else None) or None
            cur.execute(
                """
                INSERT INTO cura_operational_event_assignments
                  (operational_event_id, principal_username, expected_callsign, assigned_by)
                VALUES (%s, %s, %s, %s)
                """,
                (event_id, principal, exp_cs, uname),
            )
            aid = cur.lastrowid
            conn.commit()
            _audit_epcr_api(f"Cura operational event assignment id={aid} event={event_id} user={principal}")
            cur.execute(
                """
                SELECT id, operational_event_id, principal_username, expected_callsign, assigned_by, created_at
                FROM cura_operational_event_assignments WHERE id = %s
                """,
                (aid,),
            )
            r = cur.fetchone()
            return (
                jsonify(
                    {
                        "item": {
                            "id": r[0],
                            "operational_event_id": r[1],
                            "principal_username": r[2],
                            "expected_callsign": (r[3] or "").strip() or None,
                            "assigned_by": r[4],
                            "created_at": r[5].isoformat() if r[5] else None,
                        }
                    }
                ),
                201,
            )
        except Exception as e:
            conn.rollback()
            logger.exception("cura_operational_event_assignments: %s", e)
            if "Duplicate" in str(e) or "1062" in str(e):
                return jsonify({"error": "User already assigned to this period"}), 409
            return jsonify({"error": str(e)}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/operational-events/<int:event_id>/assignments/<int:assignment_id>",
        methods=["DELETE", "OPTIONS"],
    )
    def cura_operational_event_assignment_delete(event_id, assignment_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _actor_uname()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, slug, name, location_summary, starts_at, ends_at, status, config,
                       enforce_assignments, created_by, updated_by, created_at, updated_at
                FROM cura_operational_events WHERE id = %s
                """,
                (event_id,),
            )
            ev = cur.fetchone()
            if not ev:
                return jsonify({"error": "Event not found"}), 404
            if not _may_manage_event_assignments(ev, uname):
                return jsonify({"error": "Unauthorised"}), 403
            cur.execute(
                "DELETE FROM cura_operational_event_assignments WHERE id = %s AND operational_event_id = %s",
                (assignment_id, event_id),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return jsonify({"error": "Not found"}), 404
            conn.commit()
            _audit_epcr_api(f"Cura operational event assignment deleted id={assignment_id} event={event_id}")
            return jsonify({"ok": True}), 200
        except Exception as e:
            conn.rollback()
            logger.exception("cura_operational_event_assignment_delete: %s", e)
            return jsonify({"error": str(e)}), 500
        finally:
            cur.close()
            conn.close()

    def _log_callsign_validation(cur, **kwargs):
        """Best-effort append to cura_callsign_mdt_validation_log (migration 010)."""
        try:
            cur.execute(
                """
                INSERT INTO cura_callsign_mdt_validation_log
                  (operational_event_id, username, callsign, ok, reason_code, detail_json)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    kwargs.get("operational_event_id"),
                    (kwargs.get("username") or "")[:128],
                    (kwargs.get("callsign") or "")[:64],
                    1 if kwargs.get("ok") else 0,
                    (kwargs.get("reason_code") or "")[:64],
                    json.dumps(kwargs.get("detail") or {}),
                ),
            )
        except Exception as ex:
            if "cura_callsign_mdt_validation_log" in str(ex) or "Unknown table" in str(ex):
                logger.warning("callsign validation log skipped (run DB upgrade): %s", ex)
            else:
                logger.warning("callsign validation log failed: %s", ex)

    @bp.route("/api/cura/me/operational-context", methods=["GET", "OPTIONS"])
    def cura_me_operational_context():
        """
        Operational hints for the JWT user.

        ``items`` / ``recommended_*`` come from admin roster assignments (username → event). Cura and MDT still
        have the crew **confirm dispatch division at sign-on**; ``division_slug_to_operational_event_id`` maps
        those Ventus division slugs to Ops IDs for paperwork. When the client has a sign-on division pick that
        resolves here, that choice takes precedence over recommendation for linking new clinical records.
        """
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = (_actor_uname() or "").strip()
        if not uname:
            return jsonify({"error": "No principal on token"}), 400
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT a.operational_event_id, a.expected_callsign,
                       e.id, e.slug, e.name, e.starts_at, e.ends_at, e.status, e.config
                FROM cura_operational_event_assignments a
                INNER JOIN cura_operational_events e ON e.id = a.operational_event_id
                WHERE LOWER(a.principal_username) = LOWER(%s)
                ORDER BY e.id DESC
                """,
                (uname,),
            )
            rows = cur.fetchall()
            items = []
            recommended = None
            recommended_dispatch_division_slug = None
            now = datetime.utcnow()
            for r in rows:
                eid = r[2]
                cfg = cevb.event_config_dict(r[8])
                active = cevb.event_active_for_automation(
                    status=r[7], starts_at=r[5], ends_at=r[6], now=now
                )
                vslug = (cfg.get("ventus_division_slug") or "").strip() or None
                item = {
                    "operational_event_id": eid,
                    "expected_callsign": (r[1] or "").strip() or None,
                    "slug": r[3],
                    "name": r[4],
                    "starts_at": r[5].isoformat() if r[5] and hasattr(r[5], "isoformat") else r[5],
                    "ends_at": r[6].isoformat() if r[6] and hasattr(r[6], "isoformat") else r[6],
                    "status": r[7],
                    "ventus_division_slug": vslug,
                    "active_for_automation": active,
                    "epcr_signon_incident_fields": cevb.normalize_epcr_signon_incident_fields(
                        cfg.get("epcr_signon_incident_fields")
                    ),
                }
                items.append(item)
                if active and recommended is None:
                    recommended = eid
                    recommended_dispatch_division_slug = vslug.lower() if vslug else None

            div_cur = conn.cursor(dictionary=True)
            try:
                dispatch_divisions = cevb.list_cura_signon_dispatch_divisions(div_cur)
            finally:
                div_cur.close()

            fields_by_eid: dict[int, list[dict]] = {}
            need_ids: list[int] = []
            for d in dispatch_divisions or []:
                oid = d.get("operational_event_id")
                if oid is None:
                    continue
                try:
                    need_ids.append(int(oid))
                except (TypeError, ValueError):
                    pass
            if need_ids:
                uniq = sorted(set(need_ids))
                ph = ",".join(["%s"] * len(uniq))
                cur.execute(
                    f"SELECT id, config FROM cura_operational_events WHERE id IN ({ph})",
                    tuple(uniq),
                )
                for er in cur.fetchall() or []:
                    cfg_e = cevb.event_config_dict(er[1])
                    fields_by_eid[int(er[0])] = cevb.normalize_epcr_signon_incident_fields(
                        cfg_e.get("epcr_signon_incident_fields")
                    )

            dispatch_out: list[dict] = []
            for d in dispatch_divisions or []:
                dd = dict(d)
                oid_i = None
                oid_raw = dd.get("operational_event_id")
                if oid_raw is not None:
                    try:
                        oid_i = int(oid_raw)
                    except (TypeError, ValueError):
                        oid_i = None
                dd["epcr_signon_incident_fields"] = (
                    fields_by_eid.get(oid_i, []) if oid_i is not None else []
                )
                dispatch_out.append(dd)

            division_slug_to_operational_event_id: dict[str, int] = {}
            for d in dispatch_out:
                slug_d = (d.get("slug") or "").strip().lower()
                oid = d.get("operational_event_id")
                if slug_d and oid is not None:
                    try:
                        division_slug_to_operational_event_id[slug_d] = int(oid)
                    except (TypeError, ValueError):
                        pass

            return (
                jsonify(
                    {
                        "username": uname,
                        "items": items,
                        "recommended_operational_event_id": recommended,
                        "recommended_dispatch_division_slug": recommended_dispatch_division_slug,
                        "dispatch_divisions": dispatch_out,
                        "division_slug_to_operational_event_id": division_slug_to_operational_event_id,
                    }
                ),
                200,
            )
        except Exception as e:
            logger.exception("cura_me_operational_context: %s", e)
            return jsonify({"error": "Unavailable; run DB upgrade if needed."}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/users/search", methods=["GET", "OPTIONS"])
    def cura_users_search():
        """
        Typeahead for EPCR case collaborators: match Sparrow clinical users by username or name.
        Query param ``q`` (min 2 chars after trim); optional ``limit`` (default 20, max 40).
        """
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err

        raw_q = (request.args.get("q") or "").strip()
        if len(raw_q) < 2:
            return jsonify({"results": []}), 200

        sanitized = re.sub(r"[^a-zA-Z0-9._\- ]+", "", raw_q)[:64].strip()
        if len(sanitized) < 2:
            return jsonify({"results": []}), 200

        lim = request.args.get("limit", "20")
        try:
            limit = int(lim)
        except (TypeError, ValueError):
            limit = 20
        limit = max(1, min(limit, 40))

        pat = f"%{sanitized}%"
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT username, first_name, last_name, role
                FROM users
                WHERE LOWER(role) IN ('crew', 'clinical_lead', 'admin', 'superuser')
                  AND (
                    LOWER(username) LIKE LOWER(%s)
                    OR LOWER(IFNULL(first_name, '')) LIKE LOWER(%s)
                    OR LOWER(IFNULL(last_name, '')) LIKE LOWER(%s)
                    OR LOWER(
                      CONCAT(IFNULL(first_name, ''), '.', IFNULL(last_name, ''))
                    ) LIKE LOWER(%s)
                  )
                ORDER BY username ASC
                LIMIT %s
                """,
                (pat, pat, pat, pat, limit),
            )
            rows = cur.fetchall()
            out = []
            for r in rows:
                ud = {
                    "username": r[0],
                    "first_name": r[1],
                    "last_name": r[2],
                    "role": r[3],
                }
                uname = (ud.get("username") or "").strip()
                if not uname:
                    continue
                disp = _epcr_user_display_name_from_user_row(ud) or uname
                out.append(
                    {
                        "username": uname,
                        "displayName": disp,
                        "role": (ud.get("role") or "").strip() or None,
                    }
                )
            return jsonify({"results": out}), 200
        except Exception as e:
            if "first_name" in str(e) or "Unknown column" in str(e):
                logger.warning("cura_users_search: users table missing name columns: %s", e)
                return jsonify({"error": "User directory schema unavailable"}), 503
            logger.exception("cura_users_search: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/validate-mdts-callsign", methods=["POST", "OPTIONS"])
    def cura_validate_mdts_callsign():
        """
        If callsign is set on Cura, ensure JWT user appears on MDT sign-on crew for that unit.
        Optionally enforce event division slug vs mdts_signed_on.division.
        """
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = (_actor_uname() or "").strip()
        if not uname:
            return jsonify({"error": "No principal on token"}), 400
        body = request.get_json(silent=True) or {}
        callsign = (body.get("callsign") or body.get("callSign") or "").strip().upper()
        if not callsign:
            return (
                jsonify(
                    {
                        "ok": True,
                        "allow_dispatch_timing_sync": True,
                        "reason_code": "no_callsign",
                        "warnings": [],
                    }
                ),
                200,
            )
        raw_ev = body.get("operational_event_id")
        if raw_ev is None:
            raw_ev = body.get("operationalEventId")
        operational_event_id = None
        if raw_ev is not None and str(raw_ev).strip() != "":
            try:
                operational_event_id = int(raw_ev)
            except (TypeError, ValueError):
                return jsonify({"error": "operational_event_id must be an integer"}), 400

        conn = get_db_connection()
        cur = conn.cursor()
        detail: dict = {"callsign": callsign}
        try:
            if not cevb.table_exists(cur, "mdts_signed_on"):
                _log_callsign_validation(
                    cur,
                    operational_event_id=operational_event_id,
                    username=uname,
                    callsign=callsign,
                    ok=False,
                    reason_code="mdts_table_missing",
                    detail=detail,
                )
                conn.commit()
                return (
                    jsonify(
                        {
                            "ok": False,
                            "allow_dispatch_timing_sync": False,
                            "reason_code": "mdts_table_missing",
                            "warnings": ["Ventus MDT sign-on table not available; cannot verify callsign."],
                        }
                    ),
                    200,
                )

            mdts = cevb.fetch_mdts_signed_on_row(cur, callsign)
            if not mdts:
                detail["mdts_found"] = False
                _log_callsign_validation(
                    cur,
                    operational_event_id=operational_event_id,
                    username=uname,
                    callsign=callsign,
                    ok=False,
                    reason_code="unit_not_signed_on",
                    detail=detail,
                )
                conn.commit()
                return (
                    jsonify(
                        {
                            "ok": False,
                            "allow_dispatch_timing_sync": False,
                            "reason_code": "unit_not_signed_on",
                            "warnings": [
                                f"Callsign {callsign} is not signed on in MDT; response timings should not sync until the unit signs on with you on the crew list."
                            ],
                        }
                    ),
                    200,
                )

            detail["mdts_found"] = True
            detail["mdts_division"] = mdts.get("division")
            crew_ok = cevb.username_in_mdts_crew(mdts.get("crew"), uname)
            if not crew_ok:
                detail["crew_usernames"] = cevb.parse_mdts_crew_usernames(mdts.get("crew"))
                _log_callsign_validation(
                    cur,
                    operational_event_id=operational_event_id,
                    username=uname,
                    callsign=callsign,
                    ok=False,
                    reason_code="username_not_in_crew",
                    detail=detail,
                )
                conn.commit()
                return (
                    jsonify(
                        {
                            "ok": False,
                            "allow_dispatch_timing_sync": False,
                            "reason_code": "username_not_in_crew",
                            "warnings": [
                                f"Your user is not listed on the MDT crew for {callsign}. Sign on in MDT with the correct crew list, or clear the callsign on Cura."
                            ],
                        }
                    ),
                    200,
                )

            if operational_event_id is not None:
                cur.execute(
                    """
                    SELECT id, config FROM cura_operational_events WHERE id = %s
                    """,
                    (operational_event_id,),
                )
                evr = cur.fetchone()
                if evr:
                    cfg = cevb.event_config_dict(evr[1])
                    want_slug = (cfg.get("ventus_division_slug") or "").strip()
                    if want_slug and mdts.get("division") is not None:
                        if not cevb.division_matches_event(mdts.get("division"), want_slug):
                            detail["expected_division"] = want_slug
                            _log_callsign_validation(
                                cur,
                                operational_event_id=operational_event_id,
                                username=uname,
                                callsign=callsign,
                                ok=False,
                                reason_code="division_mismatch",
                                detail=detail,
                            )
                            conn.commit()
                            return (
                                jsonify(
                                    {
                                        "ok": False,
                                        "allow_dispatch_timing_sync": False,
                                        "reason_code": "division_mismatch",
                                        "warnings": [
                                            f"MDT division for {callsign} does not match event temporary division ({want_slug})."
                                        ],
                                    }
                                ),
                                200,
                            )

                cur.execute(
                    """
                    SELECT expected_callsign FROM cura_operational_event_assignments
                    WHERE operational_event_id = %s AND LOWER(principal_username) = LOWER(%s)
                    LIMIT 1
                    """,
                    (operational_event_id, uname),
                )
                erow = cur.fetchone()
                if erow and erow[0]:
                    exp = str(erow[0]).strip().upper()
                    if exp and exp != callsign:
                        detail["expected_callsign"] = exp
                        _log_callsign_validation(
                            cur,
                            operational_event_id=operational_event_id,
                            username=uname,
                            callsign=callsign,
                            ok=False,
                            reason_code="expected_callsign_mismatch",
                            detail=detail,
                        )
                        conn.commit()
                        return (
                            jsonify(
                                {
                                    "ok": False,
                                    "allow_dispatch_timing_sync": False,
                                    "reason_code": "expected_callsign_mismatch",
                                    "warnings": [
                                        f"Event assignment expects callsign {exp}; you entered {callsign}."
                                    ],
                                }
                            ),
                            200,
                        )

            _log_callsign_validation(
                cur,
                operational_event_id=operational_event_id,
                username=uname,
                callsign=callsign,
                ok=True,
                reason_code="ok",
                detail=detail,
            )
            conn.commit()
            return (
                jsonify(
                    {
                        "ok": True,
                        "allow_dispatch_timing_sync": True,
                        "reason_code": "ok",
                        "warnings": [],
                    }
                ),
                200,
            )
        except Exception as e:
            conn.rollback()
            logger.exception("cura_validate_mdts_callsign: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/operational-events/<int:event_id>/ventus-division/sync",
        methods=["POST", "OPTIONS"],
    )
    def cura_operational_event_ventus_division_sync(event_id):
        """Upsert mdt_dispatch_divisions and set is_active from event window + status."""
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _actor_uname()
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, slug, name, location_summary, starts_at, ends_at, status, config,
                       enforce_assignments, created_by, updated_by, created_at, updated_at
                FROM cura_operational_events WHERE id = %s
                """,
                (event_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            if not _may_patch_operational_event(row, uname):
                return jsonify({"error": "Unauthorised"}), 403

            cfg = cevb.event_config_dict(row[7])
            slug = (cfg.get("ventus_division_slug") or "").strip()
            if not slug:
                slug = f"cura_evt_{event_id}"
                cfg["ventus_division_slug"] = slug
                cur.execute(
                    "UPDATE cura_operational_events SET config = %s, updated_by = %s WHERE id = %s",
                    (json.dumps(cfg), uname, event_id),
                )
            name = (cfg.get("ventus_division_name") or row[2] or slug)[:120]
            color = (cfg.get("ventus_division_color") or "#6366f1")[:16]
            now = datetime.utcnow()
            active = cevb.event_active_for_automation(
                status=row[6], starts_at=row[4], ends_at=row[5], now=now
            )
            ok = cevb.ensure_ventus_division_for_event(
                cur,
                slug=slug,
                name=name,
                color=color,
                is_active=1 if active else 0,
                updated_by=uname,
                cura_operational_event_id=int(event_id),
                event_window_start=row[4],
                event_window_end=row[5],
            )
            if not ok:
                conn.rollback()
                return jsonify({"error": "mdt_dispatch_divisions not available (Ventus not installed?)"}), 503
            conn.commit()
            _audit_epcr_api(f"Cura ventus division sync event={event_id} slug={slug} active={active}")
            return (
                jsonify(
                    {
                        "ok": True,
                        "ventus_division_slug": slug,
                        "is_active": bool(active),
                        "config": cfg,
                    }
                ),
                200,
            )
        except ValueError as ve:
            conn.rollback()
            return jsonify({"error": str(ve)}), 400
        except Exception as e:
            conn.rollback()
            logger.exception("cura_operational_event_ventus_division_sync: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route(
        "/api/cura/operational-events/<int:event_id>/callsign-validation-log",
        methods=["GET", "OPTIONS"],
    )
    def cura_callsign_validation_log(event_id):
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        if not _epcr_privileged_role():
            return jsonify({"error": "Unauthorised"}), 403
        limit = request.args.get("limit", "50")
        try:
            lim = max(1, min(200, int(limit)))
        except (TypeError, ValueError):
            lim = 50
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM cura_operational_events WHERE id = %s", (event_id,))
            if not cur.fetchone():
                return jsonify({"error": "Not found"}), 404
            cur.execute(
                """
                SELECT id, operational_event_id, username, callsign, ok, reason_code, detail_json, created_at
                FROM cura_callsign_mdt_validation_log
                WHERE operational_event_id = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (event_id, lim),
            )
            rows = cur.fetchall()
            items = []
            for r in rows:
                dj = r[6]
                if isinstance(dj, (bytes, str)):
                    try:
                        dj = json.loads(dj)
                    except Exception:
                        dj = None
                items.append(
                    {
                        "id": r[0],
                        "operational_event_id": r[1],
                        "username": r[2],
                        "callsign": r[3],
                        "ok": bool(r[4]),
                        "reason_code": r[5],
                        "detail": dj,
                        "created_at": r[7].isoformat() if r[7] and hasattr(r[7], "isoformat") else r[7],
                    }
                )
            return jsonify({"items": items}), 200
        except Exception as e:
            if "cura_callsign_mdt_validation_log" in str(e) or "Unknown table" in str(e):
                return jsonify({"items": [], "message": "Run DB upgrade"}), 200
            logger.exception("cura_callsign_validation_log: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/dispatch/suggested-cad-for-epcr", methods=["GET", "OPTIONS"])
    def cura_dispatch_suggested_cad_for_epcr():
        """
        For Cura EPCR dashboard: when the Bearer user is on the MDT crew for ``callsign`` and the
        unit is **on scene** on ``assignedIncident``, return one row with CAD id and
        ``patient_prefill`` (MDT triage → ``PatientInfo.ptInfo``). Otherwise ``items`` is empty;
        use **Create new case** only. Aligns optional ``operational_event_id`` with validate-mdts-callsign.
        """
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = (_actor_uname() or "").strip()
        if not uname:
            return jsonify({"error": "No principal on token"}), 400
        callsign = (request.args.get("callsign") or request.args.get("callSign") or "").strip()
        raw_ev = request.args.get("operational_event_id")
        if raw_ev is None:
            raw_ev = request.args.get("operationalEventId")
        operational_event_id = None
        if raw_ev is not None and str(raw_ev).strip() != "":
            try:
                operational_event_id = int(raw_ev)
            except (TypeError, ValueError):
                return jsonify({"error": "operational_event_id must be an integer"}), 400
        if not callsign:
            return (
                jsonify(
                    {
                        "available": True,
                        "items": [],
                        "skip_reason": "no_callsign",
                    }
                ),
                200,
            )
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if not cevb.table_exists(cur, "mdt_jobs"):
                return jsonify({"available": False, "items": [], "message": "mdt_jobs not present"}), 200
            if not cevb.table_exists(cur, "mdts_signed_on"):
                return jsonify({"available": False, "items": [], "message": "mdts_signed_on not present"}), 200
            pack = cevb.suggested_cad_rows_for_epcr(
                cur,
                username=uname,
                callsign=callsign,
                operational_event_id=operational_event_id,
            )
            return (
                jsonify(
                    {
                        "available": True,
                        "items": pack.get("items") or [],
                        "skip_reason": pack.get("skip_reason"),
                    }
                ),
                200,
            )
        except Exception as e:
            logger.exception("cura_dispatch_suggested_cad_for_epcr: %s", e)
            return jsonify({"error": "An internal error occurred", "available": False, "items": []}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/dispatch/preview", methods=["GET", "OPTIONS"])
    def cura_dispatch_preview():
        """Best-effort read of `mdt_jobs` when Ventus/dispatch shares the same DB (optional prefill)."""
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        ref = (request.args.get("reference") or request.args.get("job_id") or request.args.get("cad") or "").strip()
        if not ref:
            return jsonify({"error": "reference (or job_id or cad) query parameter is required"}), 400
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = 'mdt_jobs'"
            )
            if not cur.fetchone()[0]:
                return jsonify({"available": False, "message": "mdt_jobs not present"}), 200
            row = None
            cad_try = None
            if ref.isdigit():
                try:
                    cad_try = int(ref)
                except ValueError:
                    cad_try = None
            if cad_try is not None:
                try:
                    cur.execute(
                        "SELECT cad, data, status, claimedBy, updated_at FROM mdt_jobs WHERE cad = %s LIMIT 1",
                        (cad_try,),
                    )
                    row = cur.fetchone()
                except Exception:
                    row = None
            if not row:
                try:
                    cur.execute(
                        "SELECT cad, data, status, claimedBy, updated_at FROM mdt_jobs WHERE cad = %s LIMIT 1",
                        (ref,),
                    )
                    row = cur.fetchone()
                except Exception:
                    row = None
            if not row:
                try:
                    cur.execute(
                        "SELECT cad, data, status, claimedBy, updated_at FROM mdt_jobs WHERE id = %s LIMIT 1",
                        (ref,),
                    )
                    row = cur.fetchone()
                except Exception:
                    row = None
            if not row and ref.isdigit():
                try:
                    cur.execute(
                        "SELECT cad, data, status, claimedBy, updated_at FROM mdt_jobs WHERE id = %s LIMIT 1",
                        (int(ref),),
                    )
                    row = cur.fetchone()
                except Exception:
                    row = None
            if not row:
                return jsonify({"available": True, "found": False, "reference": ref}), 200
            raw_data = row[1]
            parsed = None
            if raw_data is not None:
                if isinstance(raw_data, (dict, list)):
                    parsed = raw_data
                elif isinstance(raw_data, (bytes, str)):
                    try:
                        parsed = json.loads(raw_data)
                    except Exception:
                        parsed = None
            cad_val = row[0]
            return (
                jsonify(
                    {
                        "available": True,
                        "found": True,
                        "reference": ref,
                        "job": {
                            "cad": cad_val,
                            "id": cad_val,
                            "data": parsed,
                            "status": row[2],
                            "final_status": row[2],
                            "claimed_by": row[3],
                            "updated_at": row[4].isoformat() if row[4] and hasattr(row[4], "isoformat") else row[4],
                        },
                    }
                ),
                200,
            )
        except Exception as e:
            logger.exception("cura_dispatch_preview: %s", e)
            return jsonify({"error": str(e), "available": False}), 500
        finally:
            cur.close()
            conn.close()

    _CURA_GEO_LOOKUP_UA = "SparrowERP/1.0 (Cura patient-info lookup; internal clinical use)"

    def _cura_http_get_json(url: str, headers: dict | None = None, timeout: float = 18.0):
        req = urllib.request.Request(url, headers=dict(headers or {}))
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _normalise_uk_postcode_for_lookup(pc: str):
        s = re.sub(r"[^A-Za-z0-9]", "", (pc or "").strip().upper())
        if len(s) < 5 or len(s) > 7:
            return None
        return f"{s[:-3]} {s[-3:]}"

    def _nominatim_row(item: dict) -> dict | None:
        if not isinstance(item, dict):
            return None
        addr = item.get("address") or {}
        if not isinstance(addr, dict):
            addr = {}
        line_parts = []
        hn = addr.get("house_number") or addr.get("house_name")
        rd = addr.get("road") or addr.get("pedestrian") or addr.get("footway")
        if hn:
            line_parts.append(str(hn).strip())
        if rd:
            line_parts.append(str(rd).strip())
        line1 = " ".join(line_parts).strip()
        if not line1:
            line1 = (item.get("name") or "").strip() or (item.get("display_name") or "")[:160]
        town = (
            addr.get("city")
            or addr.get("town")
            or addr.get("village")
            or addr.get("hamlet")
            or addr.get("suburb")
            or ""
        )
        if isinstance(town, str):
            town = town.strip()
        pc = (addr.get("postcode") or "").strip() if isinstance(addr.get("postcode"), str) else ""
        disp = (item.get("display_name") or "").strip()
        if not disp and line1:
            disp = ", ".join(x for x in [line1, town, pc] if x)
        if not disp:
            return None
        return {
            "display": disp,
            "line1": line1 or disp[:120],
            "line2": town,
            "postcode": pc,
        }

    @bp.route("/api/cura/lookup/addresses-by-postcode", methods=["GET", "OPTIONS"])
    def cura_lookup_addresses_by_postcode():
        """Validate UK postcode (postcodes.io) and suggest addresses (Nominatim; server-side to avoid browser CORS)."""
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        raw_pc = (request.args.get("postcode") or "").strip()
        norm = _normalise_uk_postcode_for_lookup(raw_pc)
        if not norm:
            return jsonify({"valid": False, "results": [], "message": "Enter a full UK postcode."}), 200
        try:
            pc_enc = urllib.parse.quote(norm)
            pj = _cura_http_get_json(
                f"https://api.postcodes.io/postcodes/{pc_enc}",
                timeout=12.0,
            )
        except urllib.error.HTTPError as e:
            try:
                pj = json.loads(e.read().decode("utf-8"))
            except Exception:
                logger.warning("cura_lookup_addresses postcodes.io HTTP %s", e.code)
                return jsonify({"valid": False, "results": [], "message": "Postcode not found or validation failed."}), 200
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError) as e:
            logger.warning("cura_lookup_addresses postcodes.io: %s", e)
            return jsonify({"valid": False, "results": [], "message": "Postcode validation service unavailable."}), 200
        if pj.get("status") != 200:
            return jsonify(
                {
                    "valid": False,
                    "results": [],
                    "message": (pj.get("error") or "Unknown postcode") if isinstance(pj.get("error"), str) else "Unknown postcode",
                }
            ), 200
        result = pj.get("result") or {}
        canonical = (result.get("postcode") or norm).strip()
        q = urllib.parse.urlencode(
            {
                "format": "json",
                "addressdetails": "1",
                "countrycodes": "gb",
                "postalcode": canonical,
                "limit": "35",
                "email": "clinical-lookup@local",
            }
        )
        url = f"https://nominatim.openstreetmap.org/search?{q}"
        try:
            items = _cura_http_get_json(url, headers={"User-Agent": _CURA_GEO_LOOKUP_UA}, timeout=16.0)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError) as e:
            logger.warning("cura_lookup_addresses nominatim: %s", e)
            return jsonify(
                {
                    "valid": True,
                    "postcode": canonical,
                    "results": [],
                    "message": "Postcode is valid but address suggestions are temporarily unavailable—enter the address manually.",
                }
            ), 200
        if not isinstance(items, list):
            items = []
        seen = set()
        rows = []
        for item in items:
            row = _nominatim_row(item)
            if not row:
                continue
            key = (row.get("display") or "").lower()
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
        return jsonify({"valid": True, "postcode": canonical, "results": rows[:30]}), 200

    @bp.route("/api/cura/lookup/reverse-geocode", methods=["GET", "OPTIONS"])
    def cura_lookup_reverse_geocode():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        try:
            lat = float(request.args.get("lat", "").strip())
            lon = float(request.args.get("lon", "").strip())
        except ValueError:
            return jsonify({"ok": False, "message": "lat and lon must be numbers"}), 400
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            return jsonify({"ok": False, "message": "Coordinates out of range"}), 400
        q = urllib.parse.urlencode(
            {
                "format": "json",
                "addressdetails": "1",
                "lat": str(lat),
                "lon": str(lon),
                "email": "clinical-lookup@local",
            }
        )
        url = f"https://nominatim.openstreetmap.org/reverse?{q}"
        try:
            item = _cura_http_get_json(url, headers={"User-Agent": _CURA_GEO_LOOKUP_UA}, timeout=16.0)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError) as e:
            logger.warning("cura_lookup_reverse_geocode: %s", e)
            return jsonify({"ok": False, "message": "Reverse geocoding unavailable."}), 200
        row = _nominatim_row(item) if isinstance(item, dict) else None
        if not row:
            return jsonify({"ok": False, "message": "No address found for this position."}), 200
        line_bits = [row.get("line1") or ""]
        if row.get("line2"):
            line_bits.append(row["line2"])
        display_line = ", ".join(x for x in line_bits if x)
        return (
            jsonify(
                {
                    "ok": True,
                    "displayLine": display_line or row.get("display"),
                    "line1": row.get("line1"),
                    "line2": row.get("line2"),
                    "postcode": row.get("postcode") or "",
                    "fullDisplay": row.get("display"),
                }
            ),
            200,
        )

    @bp.route("/api/cura/lookup/gp-by-postcode", methods=["GET", "OPTIONS"])
    def cura_lookup_gp_by_postcode():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        norm = _normalise_uk_postcode_for_lookup((request.args.get("postcode") or "").strip())
        if not norm:
            return jsonify({"results": [], "message": "Enter a full UK postcode."}), 200
        pc_compact = re.sub(r"\s+", "", norm)
        q = urllib.parse.urlencode({"PostCode": pc_compact, "Status": "Active", "Limit": "40"})
        url = f"https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations?{q}"
        try:
            data = _cura_http_get_json(url, timeout=18.0)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError) as e:
            logger.warning("cura_lookup_gp_by_postcode: %s", e)
            return jsonify({"results": [], "message": "NHS directory lookup failed—enter GP details manually."}), 200
        orgs = data.get("Organisations") if isinstance(data, dict) else None
        if not isinstance(orgs, list):
            orgs = []
        rows = []
        for o in orgs:
            if not isinstance(o, dict):
                continue
            name = (o.get("Name") or "").strip()
            if not name:
                continue
            desc = (o.get("PrimaryRoleDescription") or "").strip()
            oid = o.get("OrgId")
            ods_code = ""
            if isinstance(oid, dict):
                ods_code = str(oid.get("extension") or oid.get("value") or "").strip()
            addr_parts = []
            for key in ("AddrLine1", "AddrLine2", "AddrLine3", "City", "County"):
                v = o.get(key)
                if isinstance(v, str) and v.strip():
                    addr_parts.append(v.strip())
            postcode_f = (o.get("PostCode") or "").strip()
            rows.append(
                {
                    "name": name,
                    "odsCode": ods_code,
                    "role": desc,
                    "address": ", ".join(addr_parts) if addr_parts else "",
                    "postcode": postcode_f or norm,
                }
            )
        return jsonify({"results": rows, "postcode": norm}), 200

    _ALLOWED_ATTACHMENT_ENTITY = frozenset({"safeguarding_referral", "patient_contact_report", "epcr_case"})

    def _cura_epcr_case_accessible(cur, case_id):
        cur.execute("SELECT data FROM cases WHERE id = %s", (case_id,))
        r = cur.fetchone()
        if not r:
            return False
        data = safe_json(r[0])
        return isinstance(data, dict) and _user_may_access_case_data(data)

    def _cura_attachment_parent_exists(cur, entity_type, entity_id):
        if entity_type == "safeguarding_referral":
            cur.execute("SELECT 1 FROM cura_safeguarding_referrals WHERE id = %s", (entity_id,))
        elif entity_type == "patient_contact_report":
            cur.execute("SELECT 1 FROM cura_patient_contact_reports WHERE id = %s", (entity_id,))
        elif entity_type == "epcr_case":
            return _cura_epcr_case_accessible(cur, entity_id)
        else:
            return False
        return cur.fetchone() is not None

    def _resolve_attachment_disk_path(storage_key: str):
        if not storage_key or not isinstance(storage_key, str):
            return None
        if ".." in storage_key:
            return None
        sk = storage_key.replace("\\", "/").lstrip("/")
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

    @bp.route("/api/cura/attachments", methods=["GET", "POST", "OPTIONS"])
    def cura_attachments():
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _actor_uname()

        if request.method == "GET":
            et = (request.args.get("entity_type") or "").strip()
            eid_raw = request.args.get("entity_id")
            if not et or eid_raw is None or str(eid_raw).strip() == "":
                return jsonify({"error": "entity_type and entity_id are required"}), 400
            if et not in _ALLOWED_ATTACHMENT_ENTITY:
                return jsonify({"error": "Invalid entity_type"}), 400
            try:
                eid = int(eid_raw)
            except (TypeError, ValueError):
                return jsonify({"error": "entity_id must be an integer"}), 400
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                if not _cura_attachment_parent_exists(cur, et, eid):
                    return jsonify({"error": "Parent record not found"}), 404
                cur.execute(
                    """
                    SELECT id, entity_type, entity_id, storage_key, original_filename, mime_type, byte_size,
                           checksum_sha256, created_by, created_at
                    FROM cura_file_attachments
                    WHERE entity_type = %s AND entity_id = %s
                    ORDER BY id ASC
                    """,
                    (et, eid),
                )
                rows = cur.fetchall()
                items = [
                    {
                        "id": r[0],
                        "entity_type": r[1],
                        "entity_id": r[2],
                        "storage_key": r[3],
                        "original_filename": r[4],
                        "mime_type": r[5],
                        "byte_size": r[6],
                        "checksum_sha256": r[7],
                        "created_by": r[8],
                        "created_at": r[9].isoformat() if r[9] else None,
                    }
                    for r in rows
                ]
                return jsonify({"items": items}), 200
            except Exception as e:
                logger.exception("cura_attachments GET: %s", e)
                return jsonify({"error": "Unavailable; run DB upgrade if needed."}), 500
            finally:
                cur.close()
                conn.close()

        body = request.get_json(silent=True) or {}
        et = (body.get("entity_type") or "").strip()
        eid = body.get("entity_id")
        storage_key = (body.get("storage_key") or "").strip()
        if not et or eid is None or not storage_key:
            return jsonify({"error": "entity_type, entity_id, and storage_key are required"}), 400
        if et not in _ALLOWED_ATTACHMENT_ENTITY:
            return jsonify({"error": "Invalid entity_type"}), 400
        try:
            eid = int(eid)
        except (TypeError, ValueError):
            return jsonify({"error": "entity_id must be an integer"}), 400
        orig = (body.get("original_filename") or body.get("originalFilename") or "").strip() or None
        mime = (body.get("mime_type") or body.get("mimeType") or "").strip() or None
        bsz = body.get("byte_size")
        if bsz is not None:
            try:
                bsz = int(bsz)
            except (TypeError, ValueError):
                return jsonify({"error": "byte_size must be an integer"}), 400
        else:
            bsz = None
        cks = (body.get("checksum_sha256") or body.get("checksumSha256") or "").strip() or None
        if cks and len(cks) != 64:
            return jsonify({"error": "checksum_sha256 must be 64 hex chars when provided"}), 400

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if not _cura_attachment_parent_exists(cur, et, eid):
                return jsonify({"error": "Parent record not found"}), 404
            cur.execute(
                """
                INSERT INTO cura_file_attachments
                  (entity_type, entity_id, storage_key, original_filename, mime_type, byte_size, checksum_sha256, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (et, eid, storage_key, orig, mime, bsz, cks, uname),
            )
            aid = cur.lastrowid
            conn.commit()
            _audit_epcr_api(f"Cura attachment metadata created id={aid} entity={et}/{eid}")
            cur.execute(
                """
                SELECT id, entity_type, entity_id, storage_key, original_filename, mime_type, byte_size,
                       checksum_sha256, created_by, created_at
                FROM cura_file_attachments WHERE id = %s
                """,
                (aid,),
            )
            r = cur.fetchone()
            return (
                jsonify(
                    {
                        "item": {
                            "id": r[0],
                            "entity_type": r[1],
                            "entity_id": r[2],
                            "storage_key": r[3],
                            "original_filename": r[4],
                            "mime_type": r[5],
                            "byte_size": r[6],
                            "checksum_sha256": r[7],
                            "created_by": r[8],
                            "created_at": r[9].isoformat() if r[9] else None,
                        }
                    }
                ),
                201,
            )
        except Exception as e:
            conn.rollback()
            logger.exception("cura_attachments POST: %s", e)
            return jsonify({"error": str(e)}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/attachments/<int:attachment_id>/file", methods=["GET", "OPTIONS"])
    def cura_attachment_file(attachment_id):
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
                SELECT entity_type, entity_id, storage_key, original_filename, mime_type
                FROM cura_file_attachments WHERE id = %s
                """,
                (attachment_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            et, eid, sk, orig, mime = row[0], row[1], row[2], row[3], row[4]
            if et not in _ALLOWED_ATTACHMENT_ENTITY:
                return jsonify({"error": "Invalid attachment"}), 400
            if not _cura_attachment_parent_exists(cur, et, eid):
                return jsonify({"error": "Unauthorised"}), 403
            path = _resolve_attachment_disk_path(sk or "")
            if not path:
                return jsonify({"error": "File not available on disk"}), 404
            _audit_epcr_api(f"Cura attachment download id={attachment_id}")
            return send_file(
                path,
                mimetype=mime or None,
                download_name=orig or None,
                as_attachment=False,
            )
        except Exception as e:
            logger.exception("cura_attachment_file: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/attachments/<int:attachment_id>", methods=["DELETE", "OPTIONS"])
    def cura_attachment_delete(attachment_id):
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
            cur.execute("DELETE FROM cura_file_attachments WHERE id = %s", (attachment_id,))
            if cur.rowcount != 1:
                conn.rollback()
                return jsonify({"error": "Not found"}), 404
            conn.commit()
            _audit_epcr_api(f"Cura attachment deleted id={attachment_id}")
            return jsonify({"ok": True}), 200
        except Exception as e:
            conn.rollback()
            logger.exception("cura_attachment_delete: %s", e)
            return jsonify({"error": str(e)}), 500
        finally:
            cur.close()
            conn.close()

    @bp.route("/api/cura/attachments/upload", methods=["POST", "OPTIONS"])
    def cura_attachment_upload():
        """Multipart upload: saves under plugin `data/cura_uploads/` and registers metadata."""
        if request.method == "OPTIONS":
            return "", 200
        auth_err = _require_epcr_json_api()
        if auth_err:
            return auth_err
        uname = _actor_uname()
        try:
            max_mb = float(os.environ.get("CURA_UPLOAD_MAX_MB", "25"))
        except ValueError:
            max_mb = 25.0
        max_bytes = int(max_mb * 1024 * 1024)

        et = (request.form.get("entity_type") or "").strip()
        eid_raw = request.form.get("entity_id")
        if not et or eid_raw is None or str(eid_raw).strip() == "":
            return jsonify({"error": "entity_type and entity_id are required (form fields)"}), 400
        if et not in _ALLOWED_ATTACHMENT_ENTITY:
            return jsonify({"error": "Invalid entity_type"}), 400
        try:
            eid = int(eid_raw)
        except (TypeError, ValueError):
            return jsonify({"error": "entity_id must be an integer"}), 400

        f = request.files.get("file")
        if not f or not f.filename:
            return jsonify({"error": "file is required"}), 400

        f.stream.seek(0, 2)
        sz = f.stream.tell()
        f.stream.seek(0)
        if sz > max_bytes:
            return jsonify({"error": f"File too large (max {max_mb} MB)"}), 413

        safe = secure_filename(f.filename) or "upload"
        store_name = f"{uuid.uuid4().hex}_{safe}"
        try:
            os.makedirs(_UPLOAD_ROOT, exist_ok=True)
        except OSError as e:
            logger.exception("cura_attachment_upload mkdir: %s", e)
            return jsonify({"error": "Upload storage not available"}), 500

        abs_path = os.path.join(_UPLOAD_ROOT, store_name)
        try:
            f.save(abs_path)
        except Exception as e:
            logger.exception("cura_attachment_upload save: %s", e)
            return jsonify({"error": "Failed to save file"}), 500

        storage_key = f"cura_uploads/{store_name}"
        mime = f.mimetype or (request.form.get("mime_type") or None)

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if not _cura_attachment_parent_exists(cur, et, eid):
                try:
                    os.remove(abs_path)
                except OSError:
                    pass
                return jsonify({"error": "Parent record not found"}), 404
            cur.execute(
                """
                INSERT INTO cura_file_attachments
                  (entity_type, entity_id, storage_key, original_filename, mime_type, byte_size, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (et, eid, storage_key, f.filename, mime, sz, uname),
            )
            aid = cur.lastrowid
            conn.commit()
            _audit_epcr_api(f"Cura attachment uploaded id={aid} entity={et}/{eid}")
            cur.execute(
                """
                SELECT id, entity_type, entity_id, storage_key, original_filename, mime_type, byte_size,
                       checksum_sha256, created_by, created_at
                FROM cura_file_attachments WHERE id = %s
                """,
                (aid,),
            )
            r = cur.fetchone()
            return (
                jsonify(
                    {
                        "item": {
                            "id": r[0],
                            "entity_type": r[1],
                            "entity_id": r[2],
                            "storage_key": r[3],
                            "original_filename": r[4],
                            "mime_type": r[5],
                            "byte_size": r[6],
                            "checksum_sha256": r[7],
                            "created_by": r[8],
                            "created_at": r[9].isoformat() if r[9] else None,
                        }
                    }
                ),
                201,
            )
        except Exception as e:
            conn.rollback()
            try:
                os.remove(abs_path)
            except OSError:
                pass
            logger.exception("cura_attachment_upload: %s", e)
            return jsonify({"error": str(e)}), 500
        finally:
            cur.close()
            conn.close()
