import os
import re
import copy
import random
import string
import uuid
import json
import base64
import tempfile
import time
from datetime import datetime, timedelta, timezone
from flask import (
    Blueprint, request, jsonify, render_template, current_app,
    redirect, url_for, flash, session, g, abort, Response, send_file,
    has_request_context, make_response,
)
from flask_login import login_required, current_user, login_user, logout_user
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename
from flask_mail import Message, Mail
from .objects import Patient, AuditLog, Prescription, CareCompanyUser, EmailManager
from .cura_baseline_datasets import (
    cura_resolved_dataset_payload,
    is_cura_dataset_payload_unset,
)
from .cura_settings_dataset_editor import (
    apply_section_to_payload,
    build_editor_sections,
    merge_csv_into_section,
    payload_for_dataset_or_default,
)
from app.objects import PluginManager, AuthManager, User, get_db_connection  # Adjust as needed
from .case_access_render import (
    inject_oohca_role_ecg_case_access_images,
    inject_rtc_case_access_inline_images,
    sanitize_oohca_verification_render_keys,
    sanitize_rtc_section_payload_for_mysql,
)
import logging

logger = logging.getLogger('medical_records_module')
logger.setLevel(logging.INFO)

# In-memory storage for one-time admin PINs.
admin_pin_store = {}  # Example: {"pin": "123456", "expires_at": datetime_object, "generated_by": "ClinicalLeadUser"}

def calculate_age(born, today=None):
    if today is None:
        today = datetime.utcnow().date()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

def generate_pin():
    """Generates a secure 6-digit PIN."""
    return ''.join(random.choices(string.digits, k=6))

# For audit logging, use our raw method.
def log_audit(
    user,
    action,
    patient_id=None,
    *,
    case_id=None,
    principal_role=None,
    route=None,
    ip=None,
    user_agent=None,
    reason=None,
):
    try:
        if has_request_context():
            if principal_role is None:
                principal_role = str(getattr(current_user, "role", "") or "").strip().lower() or None
            if route is None:
                route = str(getattr(request, "endpoint", "") or "").strip() or None
            if ip is None:
                ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip() or None
            if user_agent is None:
                ua = (request.headers.get("User-Agent") or "").strip()
                user_agent = (ua[:255] if ua else None)
        AuditLog.insert_log(
            user,
            action,
            patient_id=patient_id,
            case_id=case_id,
            principal_role=principal_role,
            route=route,
            ip=ip,
            user_agent=user_agent,
            reason=reason,
        )
        logger.info("Audit log created: user=%s, action=%s", user, action)
    except Exception as e:
        # Never allow audit sink failures to break clinical/ops workflows.
        logger.warning("Audit log write skipped: %s", e)


def _epcr_case_session_id(case_id):
    try:
        return str(int(case_id))
    except (TypeError, ValueError):
        return str(case_id).strip()


def _epcr_session_unlock_ttl_seconds():
    try:
        minutes = int(os.environ.get("EPCR_SESSION_UNLOCK_MINUTES", "240"))
    except ValueError:
        minutes = 240
    minutes = max(15, min(minutes, 24 * 60))
    return minutes * 60


def _epcr_unlock_brute_window_seconds():
    try:
        return int(os.environ.get("EPCR_UNLOCK_BRUTE_WINDOW_SECONDS", "900"))
    except ValueError:
        return 900


def _epcr_unlock_brute_max_attempts():
    try:
        n = int(os.environ.get("EPCR_UNLOCK_BRUTE_MAX_ATTEMPTS", "5"))
    except ValueError:
        n = 5
    return max(3, min(n, 20))


def _epcr_strip_epcr_unlock_keys(case_sid: str):
    session.pop(f"epcr_unlocked_{case_sid}", None)
    session.pop(f"epcr_unlock_expires_{case_sid}", None)
    session.pop(f"epcr_access_audit_{case_sid}", None)


def _epcr_session_unlock_valid(case_id) -> bool:
    """
    True if this browser session has a non-expired Caldicott unlock for the case.
    Clears stale session keys when expired.
    """
    cid = _epcr_case_session_id(case_id)
    if not session.get(f"epcr_unlocked_{cid}"):
        return False
    exp = session.get(f"epcr_unlock_expires_{cid}")
    now = time.time()
    if exp is None:
        session[f"epcr_unlock_expires_{cid}"] = now + _epcr_session_unlock_ttl_seconds()
        session.modified = True
        return True
    try:
        exp_f = float(exp)
    except (TypeError, ValueError):
        _epcr_strip_epcr_unlock_keys(cid)
        session.modified = True
        return False
    if now > exp_f:
        try:
            log_audit(
                (getattr(current_user, "username", None) or "unknown"),
                f"EPCR access audit: in-session unlock for case {cid} expired (time limit); grant cleared",
                case_id=cid,
            )
        except Exception:
            pass
        _epcr_strip_epcr_unlock_keys(cid)
        session.modified = True
        return False
    return True


def _epcr_unlock_fail_keys(case_sid: str):
    uid = getattr(current_user, "id", None)
    ukey = str(uid) if uid is not None else "anon"
    base = f"{ukey}_{case_sid}"
    return (
        f"epcr_ul_fails_{base}",
        f"epcr_ul_block_{base}",
    )


def _epcr_unlock_check_brute_block(case_sid: str):
    """Returns (jsonify_err, status) if blocked, else None."""
    _, block_key = _epcr_unlock_fail_keys(case_sid)
    until = session.get(block_key)
    try:
        until_f = float(until) if until is not None else 0.0
    except (TypeError, ValueError):
        until_f = 0.0
    if time.time() < until_f:
        retry = max(0, int(until_f - time.time()) + 1)
        return (
            jsonify(
                error=(
                    f"Too many failed unlock attempts for this case. Try again in about {retry} seconds."
                )
            ),
            429,
        )
    return None


def _epcr_unlock_note_failed_attempt(case_sid: str):
    window = _epcr_unlock_brute_window_seconds()
    max_attempts = _epcr_unlock_brute_max_attempts()
    fail_key, block_key = _epcr_unlock_fail_keys(case_sid)
    raw = session.get(fail_key)
    now = time.time()
    if isinstance(raw, dict):
        count = int(raw.get("count") or 0)
        first_ts = float(raw.get("first_ts") or now)
    else:
        count, first_ts = 0, now
    if now - first_ts > window:
        count, first_ts = 1, now
    else:
        count += 1
    session[fail_key] = {"count": count, "first_ts": first_ts}
    if count >= max_attempts:
        session[block_key] = now + window
        session.pop(fail_key, None)
        try:
            log_audit(
                current_user.username,
                f"EPCR access audit: temporary unlock lockout for case {case_sid} after {max_attempts} failed attempts ({int(window)}s window)",
                case_id=case_sid,
            )
        except Exception:
            pass
    session.modified = True


def _epcr_unlock_clear_brute_keys(case_sid: str):
    fail_key, block_key = _epcr_unlock_fail_keys(case_sid)
    session.pop(fail_key, None)
    session.pop(block_key, None)


def _epcr_pending_request_ttl_days() -> int:
    """Pending EPCR access requests auto-expire after this many days (env EPCR_ACCESS_REQUEST_PENDING_DAYS, default 7)."""
    raw = (os.environ.get("EPCR_ACCESS_REQUEST_PENDING_DAYS") or "7").strip()
    try:
        return max(1, min(int(raw), 90))
    except ValueError:
        return 7


def _epcr_maintain_access_requests(cursor) -> None:
    """
    Mark stale pending rows as expired. Caller should commit the connection when appropriate.
    """
    days = _epcr_pending_request_ttl_days()
    cursor.execute(
        """
        UPDATE epcr_access_requests
        SET status='expired',
            reviewed_at=UTC_TIMESTAMP(),
            review_note='Expired automatically (pending beyond TTL).'
        WHERE status='pending'
          AND created_at < UTC_TIMESTAMP() - INTERVAL %s DAY
        """,
        (days,),
    )


# --- Cura / EPCR JSON API: authentication & authorisation (confidentiality / accountability) ---
_EPCR_JSON_API_ROLES = frozenset(
    {"crew", "admin", "superuser", "clinical_lead", "support_break_glass"}
)


def _cura_auth_principal():
    """
    Username and role for EPCR/Cura JSON: Bearer JWT (g._cura_jwt_principal) or Flask-Login session.
    Returns (username, role_lower, user_id_or_sub). Username is lowercased for stable case JSON / filters.
    """
    jwt_p = getattr(g, "_cura_jwt_principal", None)
    if jwt_p:
        u = str(jwt_p.get("username") or "").strip().lower()
        return (
            u,
            str(jwt_p.get("role") or "").strip().lower(),
            jwt_p.get("sub"),
        )
    if getattr(current_user, "is_authenticated", False):
        u = str(getattr(current_user, "username", "") or "").strip().lower()
        return (
            u,
            str(getattr(current_user, "role", "") or "").strip().lower(),
            getattr(current_user, "id", None),
        )
    return "", "", None


def _require_epcr_json_api():
    """
    Enforce session **or** Bearer JWT + clinical role for /plugin/medical_records_module/api/* EPCR endpoints.
    Returns (response, status) to return from the route, or None if OK.
    """
    g._cura_jwt_principal = None
    auth_header = (request.headers.get("Authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        try:
            from app.auth_jwt import decode_session_token

            token = auth_header[7:].strip()
            payload = decode_session_token(token)
        except RuntimeError as e:
            logger.warning("EPCR API JWT config error: %s", e)
            return jsonify({"error": "Server token configuration error"}), 503
        if not payload:
            return jsonify({"error": "Invalid or expired token"}), 401
        role = (payload.get("role") or "").strip().lower()
        if role not in _EPCR_JSON_API_ROLES:
            logger.warning("EPCR API JWT denied role=%s user=%s", role, payload.get("username"))
            return jsonify({"error": "Unauthorised"}), 403
        g._cura_jwt_principal = payload
        return None

    if not current_user.is_authenticated:
        return jsonify({'error': 'Authentication required'}), 401
    role = (getattr(current_user, 'role', '') or '').strip().lower()
    if role not in _EPCR_JSON_API_ROLES:
        logger.warning("EPCR API denied for user=%s role=%s", getattr(current_user, 'username', '?'), role)
        return jsonify({'error': 'Unauthorised'}), 403
    return None


def _epcr_privileged_role():
    _, role, _ = _cura_auth_principal()
    return role in ("admin", "superuser", "clinical_lead", "support_break_glass")


def _epcr_privileged_browser_session():
    """
    Privileged user on the Sparrow Flask-Login session (browser), not a Bearer JWT.
    Full EPCR JSON over HTTP must match the UI Caldicott path (unlock after code + justification).
    """
    if getattr(g, "_cura_jwt_principal", None):
        return False
    return _epcr_privileged_role()


def _require_epcr_caldicott_unlock_for_privileged_session(case_id):
    """
    For privileged browser sessions, require ``session['epcr_unlocked_<case_id>']`` (see
    ``unlock_epcr_case``). Returns ``(jsonify(...), status)`` to return from the route, or None.
    """
    if not _epcr_privileged_browser_session():
        return None
    try:
        cid = int(case_id)
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid case id", "caldicott_unlock_required": True}), 400
    if _epcr_session_unlock_valid(cid):
        return None
    uname = _cura_auth_principal()[0] or "unknown"
    try:
        log_audit(
            uname,
            f"Denied EPCR API access to case {cid} (Caldicott: unlock from EPCR list with code + justification)",
            case_id=cid,
        )
    except Exception:
        logger.exception("EPCR API Caldicott deny audit failed")
    return (
        jsonify(
            {
                "error": (
                    "This case requires an approved access code and justification. "
                    "Unlock it from the EPCR case list in Sparrow first."
                ),
                "caldicott_unlock_required": True,
            }
        ),
        403,
    )


def _clear_epcr_in_session_unlock_flags():
    """
    Visiting the EPCR case list ends the in-browser 'opened case' grant: the user must go through
    unlock again (one-time code, justification, personal PIN) before viewing any case, including
    the same case ID. Allows uninterrupted viewing/printing while on the case page.
    """
    for k in list(session.keys()):
        if k.startswith("epcr_unlocked_") or k.startswith("epcr_access_audit_") or k.startswith(
            "epcr_unlock_expires_"
        ):
            session.pop(k, None)
    session.modified = True


# Comma-separated Sparrow user roles that may use admin force-close (browser session, not Cura API).
# Override with env EPCR_ADMIN_FORCE_CLOSE_ROLES e.g. "superuser,admin" to tighten.
_DEFAULT_EPCR_ADMIN_FORCE_CLOSE_ROLES = frozenset({"superuser", "admin", "clinical_lead"})

EPCR_ADMIN_FORCE_CLOSE_CATEGORIES: tuple[tuple[str, str], ...] = (
    ("clinician_unavailable", "Clinician unavailable / unable to complete record in Cura"),
    ("duplicate_erroneous", "Duplicate or erroneous case"),
    ("stood_down", "Stood down or did not proceed — administrative closure"),
    ("training_test", "Training or test record"),
    ("dnar_governance", "Governance override (e.g. DNAR photo / completion block in app)"),
    ("other", "Other — explain fully in audit notes"),
)
_EPCR_FORCE_CLOSE_CATEGORY_LABELS: dict[str, str] = dict(EPCR_ADMIN_FORCE_CLOSE_CATEGORIES)


def _epcr_admin_force_close_role_set() -> set[str]:
    raw = (os.environ.get("EPCR_ADMIN_FORCE_CLOSE_ROLES") or "").strip()
    if raw:
        return {x.strip().lower() for x in raw.split(",") if x.strip()}
    return set(_DEFAULT_EPCR_ADMIN_FORCE_CLOSE_ROLES)


def _user_may_epcr_admin_force_close() -> bool:
    role = (getattr(current_user, "role", None) or "").strip().lower()
    return role in _epcr_admin_force_close_role_set()


def _epcr_verify_personal_pin_for_admin_action(data: dict | None) -> tuple[bool, str | None]:
    """Second factor for high-risk EPCR admin actions (same rules as case unlock)."""
    if not data:
        return False, "Invalid request"
    personal_pin = str(data.get("personal_pin") or "").strip()
    if not personal_pin:
        return False, "Personal PIN is required"
    pph = getattr(current_user, "personal_pin_hash", None) or ""
    if not isinstance(pph, str) or not pph.strip():
        return (
            False,
            "No personal PIN is set on your Sparrow account. Set one under your user menu before using this action.",
        )
    if not AuthManager.verify_password(pph.strip(), personal_pin):
        return False, "Incorrect personal PIN"
    return True, None


def _epcr_access_audit_for_template(case_id: int):
    """Session snapshot shown on admin case view / print; None if absent."""
    raw = session.get(f"epcr_access_audit_{case_id}")
    if not isinstance(raw, dict):
        return None
    username = str(raw.get("username") or "").strip()
    justification = str(raw.get("justification") or "").strip()
    unlocked_at = raw.get("unlocked_at")
    display = ""
    dt = _parse_iso_datetime_maybe(unlocked_at)
    if dt:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        display = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    else:
        display = str(unlocked_at or "").strip()
    return {
        "username": username,
        "justification": justification,
        "unlocked_at_iso": str(unlocked_at or ""),
        "unlocked_at_display": display,
    }


def _parse_case_json(data_str):
    try:
        return json.loads(data_str) if data_str else {}
    except Exception:
        return {}


def _epcr_user_display_name_from_user_row(ud):
    """Best-effort display name from a users-table row dict."""
    if not ud or not isinstance(ud, dict):
        return ""
    fn = (ud.get("first_name") or "").strip()
    ln = (ud.get("last_name") or "").strip()
    if fn or ln:
        return " ".join(x for x in (fn, ln) if x)
    for key in ("name", "display_name", "full_name"):
        v = (ud.get(key) or "").strip()
        if v:
            return v
    return (ud.get("username") or "").strip()


def _parse_iso_datetime_maybe(val):
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _epcr_display_timezone():
    """
    IANA zone for clinical UI timestamps. ``cases.created_at`` / ``updated_at`` are stored as naive UTC.
    Override with env ``SPARROW_DISPLAY_TIMEZONE`` (e.g. ``Europe/Dublin``). Falls back to server local
    if the zone name is unknown.
    """
    name = (os.environ.get("SPARROW_DISPLAY_TIMEZONE") or "Europe/London").strip()
    if not name:
        name = "Europe/London"
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(name)
    except Exception:
        return None


def _epcr_format_db_utc_naive_for_display(dt):
    """
    Naive UTC from the DB → wall clock in :func:`_epcr_display_timezone` (handles DST).
    Returns ``YYYY-mm-dd HH:MM:SS`` or ``None``.
    """
    if dt is None:
        return None
    if isinstance(dt, str):
        s = dt.strip()
        if not s:
            return None
        try:
            naive = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return s
    elif isinstance(dt, datetime):
        naive = dt.replace(tzinfo=None) if dt.tzinfo else dt
    else:
        return str(dt)

    aware_utc = naive.replace(tzinfo=timezone.utc)
    tz = _epcr_display_timezone()
    if tz is not None:
        try:
            local = aware_utc.astimezone(tz)
        except Exception:
            local = aware_utc.astimezone()
    else:
        local = aware_utc.astimezone()
    return local.strftime("%Y-%m-%d %H:%M:%S")


def _strip_epcr_link_keys_from_dict(d):
    """Remove first-class case columns from JSON blob so they are not duplicated in `cases.data`."""
    if not isinstance(d, dict):
        return
    for k in (
        "dispatchReference",
        "dispatch_reference",
        "primaryCallsign",
        "primary_callsign",
        "dispatchSyncedAt",
        "dispatch_synced_at",
        "recordVersion",
        "record_version",
        "idempotencyKey",
        "idempotency_key",
        "closeIdempotencyKey",
        "close_idempotency_key",
    ):
        d.pop(k, None)


def _extract_case_idempotency_keys():
    """Idempotency-Key header or JSON body keys (create / close)."""
    h = (request.headers.get("Idempotency-Key") or request.headers.get("idempotency-key") or "").strip()
    return h or None


def _merge_epcr_idempotency_into_case_json(case_data, idempotency_key, close_idempotency_key):
    if idempotency_key:
        case_data["idempotencyKey"] = idempotency_key
    if close_idempotency_key:
        case_data["closeIdempotencyKey"] = close_idempotency_key


def _epcr_server_ack(case_id, record_version, updated_at):
    ts = updated_at.isoformat() if updated_at and hasattr(updated_at, "isoformat") else updated_at
    return {"caseId": case_id, "recordVersion": int(record_version) if record_version is not None else 1, "updatedAt": ts}


# Stable API codes for Cura / scripted clients (do not rename without client coordination).
EPCR_CODE_USE_COLLABORATOR_ENDPOINT = "USE_COLLABORATOR_ENDPOINT"
# Legacy alias (same retry behaviour in Cura):
EPCR_CODE_ASSIGN_OTHERS_REQUIRES_COLLABORATOR = EPCR_CODE_USE_COLLABORATOR_ENDPOINT
EPCR_CODE_RECORD_VERSION_CONFLICT = "EPCR_RECORD_VERSION_CONFLICT"


def _epcr_jsonify_use_collaborator_endpoint():
    """
    Crew sent multiple assignedUsers but no usable operational_event_id for server-side co-roster
    validation (Option A2). Client should create with [self] only then POST .../collaborators
    (Option B retry path).
    """
    return (
        jsonify(
            {
                "error": (
                    "To assign multiple crew on create, include operational_event_id (or operationalEventId) "
                    "so the server can verify everyone is on that operational period roster; or create with "
                    "only your username in assignedUsers, then POST "
                    "/plugin/medical_records_module/api/cases/{caseId}/collaborators "
                    'with body {"usernames":["other.user"]}.'
                ),
                "code": EPCR_CODE_USE_COLLABORATOR_ENDPOINT,
            }
        ),
        409,
    )


def _epcr_jsonify_assign_others_requires_collaborators():
    """Backward-compatible name for :func:`_epcr_jsonify_use_collaborator_endpoint`."""
    return _epcr_jsonify_use_collaborator_endpoint()


def _epcr_normalize_assigned_username_list(raw):
    """Strip, lowercase (canonical Sparrow login identity), de-duplicate case-insensitively."""
    if not isinstance(raw, list):
        return []
    out = []
    seen = set()
    for u in raw:
        if not isinstance(u, str):
            continue
        s = u.strip().lower()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _epcr_normalize_case_identity_fields(payload):
    """Normalise ``assignedUsers`` and ``crewManifest`` / ``crew_manifest`` usernames to lowercase."""
    if not isinstance(payload, dict):
        return
    payload["assignedUsers"] = _epcr_normalize_assigned_username_list(payload.get("assignedUsers"))
    for key in ("crewManifest", "crew_manifest"):
        crew = payload.get(key)
        if not isinstance(crew, list):
            continue
        for row in crew:
            if isinstance(row, dict):
                u = row.get("username")
                if isinstance(u, str):
                    s = u.strip().lower()
                    if s:
                        row["username"] = s


def _epcr_seed_incident_identity_fields(payload, *, case_id=None):
    """
    Ensure Incident Log carries stable case access identifiers.

    ``caseNumber`` must remain the EPCR's own long reference, not the dispatch/CAD number.
    ``pinCode`` is generated once when missing and then persists with the case JSON.
    """
    if not isinstance(payload, dict):
        return
    sections = payload.get("sections")
    if not isinstance(sections, list):
        return

    incident = None
    for section in sections:
        if not isinstance(section, dict):
            continue
        if str(section.get("name") or "").strip().lower() != "incident log":
            continue
        content = section.get("content")
        if not isinstance(content, dict):
            content = {}
            section["content"] = content
        incident = content.get("incident")
        if not isinstance(incident, dict):
            incident = {}
            content["incident"] = incident
        break

    if incident is None:
        return

    case_ref = str(incident.get("caseNumber") or "").strip()
    if not case_ref and case_id not in (None, ""):
        incident["caseNumber"] = str(case_id)

    case_created = str(incident.get("caseCreatedAt") or "").strip()
    if not case_created:
        created_hint = payload.get("createdAt", payload.get("created_at"))
        if isinstance(created_hint, datetime):
            incident["caseCreatedAt"] = created_hint.isoformat()
        elif created_hint is not None:
            created_text = str(created_hint).strip()
            if created_text:
                incident["caseCreatedAt"] = created_text

    pin_code = str(incident.get("pinCode") or "").strip()
    if not pin_code:
        incident["pinCode"] = "".join(random.choices(string.digits, k=4))


def _epcr_crew_co_assignee_others(caller_username, assigned_norm):
    """
    Entries in ``assigned_norm`` that are not the caller (case-insensitive).
    Used on POST so JWT username casing cannot force the multi-assignee / operational-event path
    when the client only listed the same user under different casing.
    """
    if not isinstance(assigned_norm, list):
        return []
    pl = str(caller_username or "").strip().lower()
    if not pl:
        return [u for u in assigned_norm if isinstance(u, str) and u.strip()]
    out = []
    for u in assigned_norm:
        if not isinstance(u, str):
            continue
        if u.strip().lower() != pl:
            out.append(u)
    return out


def _epcr_principal_in_assigned_users(principal_username, assigned_users):
    """
    True if principal matches any entry in case JSON assignedUsers.
    Matching is case-insensitive so JWT/session username aligns with stored crew names
    (GET /cases list filter used exact ``in`` before, which hid all cases on casing drift).
    """
    if not principal_username:
        return False
    pl = str(principal_username).strip().lower()
    if not pl:
        return False
    if not isinstance(assigned_users, list):
        return False
    for u in assigned_users:
        if not isinstance(u, str):
            continue
        if u.strip().lower() == pl:
            return True
    return False


def _epcr_principal_has_case_access(principal_username, case_data):
    """
    True if the authenticated crew user may see this case in GET /cases.
    Uses assignedUsers and crewManifest so collaborator-only / multi-crew rows are not hidden
    when assignedUsers was not yet updated on older payloads.
    """
    if not principal_username or not isinstance(case_data, dict):
        return False
    if _epcr_principal_in_assigned_users(principal_username, case_data.get("assignedUsers")):
        return True
    crew = case_data.get("crewManifest") or case_data.get("crew_manifest")
    if not isinstance(crew, list):
        return False
    pl = str(principal_username).strip().lower()
    if not pl:
        return False
    for row in crew:
        if not isinstance(row, dict):
            continue
        u = row.get("username")
        if isinstance(u, str) and u.strip().lower() == pl:
            return True
    return False


def _epcr_assigned_users_from_json_extract(raw):
    """
    Parse MySQL JSON_EXTRACT(cases.data, '$.assignedUsers') into normalized usernames.
    Not patient-identifiable clinical content — safe to show on the locked EPCR list for audits.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return _epcr_normalize_assigned_username_list(raw)
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8", errors="replace")
        except Exception:
            return []
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return []
        return (
            _epcr_normalize_assigned_username_list(parsed)
            if isinstance(parsed, list)
            else []
        )
    return []


def _epcr_operational_event_id_from_case_payload(payload):
    """Positive int from top-level case JSON, or None."""
    if not isinstance(payload, dict):
        return None
    for key in ("operational_event_id", "operationalEventId"):
        v = payload.get(key)
        if v is None or isinstance(v, bool):
            continue
        s = str(v).strip()
        if not s:
            continue
        try:
            i = int(s)
            if i > 0:
                return i
        except (TypeError, ValueError):
            return None
    return None


def _epcr_validate_crew_co_assignees_operational_event(cursor, operational_event_id, assigned_usernames, caller_username):
    """
    Option A2 (crew only, caller path): every name in ``assigned_usernames`` must exist as a Sparrow user
    and appear in ``cura_operational_event_assignments`` for ``operational_event_id``.
    """
    oid = int(operational_event_id)
    try:
        cursor.execute("SELECT 1 FROM cura_operational_events WHERE id = %s", (oid,))
        if not cursor.fetchone():
            return jsonify({"error": "Operational period not found"}), 404
    except Exception as ex:
        if "cura_operational_events" in str(ex) or "doesn't exist" in str(ex) or "Unknown table" in str(ex):
            return jsonify({"error": "Operational period storage unavailable"}), 503
        raise

    caller = (caller_username or "").strip()
    callers = {u.strip().lower() for u in assigned_usernames}
    if caller and caller.lower() not in callers:
        return jsonify({"error": "assignedUsers must include the authenticated user"}), 400

    invalid = []
    for u in assigned_usernames:
        if not User.get_user_by_username_raw(u):
            invalid.append(u)
    if invalid:
        return jsonify({"error": "Unknown username(s)", "invalidUsernames": invalid}), 400

    not_on = []
    try:
        for u in assigned_usernames:
            cursor.execute(
                """
                SELECT 1 FROM cura_operational_event_assignments
                WHERE operational_event_id = %s
                  AND LOWER(TRIM(principal_username)) = LOWER(%s)
                LIMIT 1
                """,
                (oid, u.strip()),
            )
            if not cursor.fetchone():
                not_on.append(u)
    except Exception as ex:
        if (
            "cura_operational_event_assignments" in str(ex)
            or "doesn't exist" in str(ex)
            or "Unknown table" in str(ex)
        ):
            return jsonify({"error": "Run database upgrade (operational event assignments)"}), 503
        raise

    if not_on:
        return (
            jsonify(
                {
                    "error": (
                        "Each assigned user must be on the operational period roster "
                        "(cura_operational_event_assignments), or add partners after create via the "
                        "collaborators endpoint."
                    ),
                    "notOnOperationalPeriod": not_on,
                }
            ),
            403,
        )
    return None


def _epcr_record_version_conflict_response(case_id, ex_rv, cursor=None):
    """409 body for optimistic-lock failure; includes latest status/close timestamps for Cura."""
    try:
        ex_rv_int = int(ex_rv) if ex_rv is not None else 1
    except (TypeError, ValueError):
        ex_rv_int = 1
    latest = {"recordVersion": ex_rv_int}
    uat = datetime.utcnow()
    st = None
    cl = None
    if cursor is not None and case_id is not None:
        try:
            cursor.execute(
                "SELECT status, closed_at, updated_at FROM cases WHERE id = %s LIMIT 1",
                (int(case_id),),
            )
            row = cursor.fetchone()
            if row:
                st, cl, uat = row[0], row[1], row[2]
                latest["status"] = st
                cl_iso = cl.isoformat() if cl is not None and hasattr(cl, "isoformat") else None
                uat_iso = uat.isoformat() if uat is not None and hasattr(uat, "isoformat") else None
                latest["closedAt"] = cl_iso
                latest["updatedAt"] = uat_iso
                latest["closed_at"] = cl_iso
                latest["updated_at"] = uat_iso
        except Exception:
            logger.exception("EPCR record version conflict: could not load case row for latest fragment")
    if "updatedAt" not in latest:
        u_fallback = uat.isoformat() if hasattr(uat, "isoformat") else None
        latest["updatedAt"] = u_fallback
        latest["updated_at"] = u_fallback
    if "closedAt" not in latest:
        latest["closedAt"] = None
    if "closed_at" not in latest:
        latest["closed_at"] = latest.get("closedAt")
    if "status" not in latest and st is not None:
        latest["status"] = st
    return (
        jsonify(
            {
                "error": "Case was modified elsewhere",
                "code": EPCR_CODE_RECORD_VERSION_CONFLICT,
                "recordVersion": ex_rv_int,
                "serverAck": _epcr_server_ack(case_id, ex_rv_int, uat),
                "latest": latest,
            }
        ),
        409,
    )


def _normalize_epcr_review_drugs_sections(payload):
    """
    Ensure Review / Drugs Administered sections use exact names expected by the SPA when content is sent
    under legacy keys (light normalisation only).
    """
    if not isinstance(payload, dict):
        return
    secs = payload.get("sections")
    if not isinstance(secs, list):
        return
    for sec in secs:
        if not isinstance(sec, dict):
            continue
        n = sec.get("name")
        if not isinstance(n, str):
            continue
        if n.strip().lower() == "review" and n != "Review":
            sec["name"] = "Review"
        if n.strip().lower() in ("drugs administered", "drugsadministered") and n != "Drugs Administered":
            sec["name"] = "Drugs Administered"


def _merge_epcr_link_into_case_json(case_data, dispatch_reference, primary_callsign, dispatch_synced_at, record_version):
    """Expose DB-backed dispatch / version fields on the EPCR JSON (camelCase)."""
    case_data["dispatchReference"] = dispatch_reference
    case_data["primaryCallsign"] = primary_callsign
    if dispatch_synced_at is not None and hasattr(dispatch_synced_at, "isoformat"):
        case_data["dispatchSyncedAt"] = dispatch_synced_at.isoformat()
    else:
        case_data["dispatchSyncedAt"] = None
    try:
        case_data["recordVersion"] = int(record_version) if record_version is not None else 1
    except (TypeError, ValueError):
        case_data["recordVersion"] = 1


def _epcr_case_save_link_meta(
    payload,
    existing_row_meta,
    *,
    version_conflict_case_id=None,
    version_conflict_cursor=None,
):
    """
    Compute columns for cases row from payload + existing DB values.
    existing_row_meta: None (new row) or tuple (dispatch_reference, primary_callsign, dispatch_synced_at, record_version).
    Returns (dr, pc, ds, next_rv, error_response) where error_response is None or (jsonify(...), status_code).
    """
    if not isinstance(payload, dict):
        payload = {}
    if existing_row_meta is None:
        ex_dr = ex_pc = ex_ds = None
        ex_rv = 0
    else:
        ex_dr, ex_pc, ex_ds, ex_rv = existing_row_meta
        if ex_rv is None:
            ex_rv = 1
        if "recordVersion" in payload or "record_version" in payload:
            try:
                ev = int(payload.get("recordVersion", payload.get("record_version")))
            except (TypeError, ValueError):
                return None, None, None, None, (jsonify({"error": "Invalid recordVersion"}), 400)
            if ev != ex_rv:
                return None, None, None, None, _epcr_record_version_conflict_response(
                    version_conflict_case_id, ex_rv, version_conflict_cursor
                )

    if "dispatchReference" in payload or "dispatch_reference" in payload:
        v = payload.get("dispatchReference", payload.get("dispatch_reference"))
        dr = None if v in (None, "") else str(v).strip() or None
    else:
        dr = ex_dr

    if "primaryCallsign" in payload or "primary_callsign" in payload:
        v = payload.get("primaryCallsign", payload.get("primary_callsign"))
        pc = None if v in (None, "") else str(v).strip() or None
    else:
        pc = ex_pc

    if "dispatchSyncedAt" in payload or "dispatch_synced_at" in payload:
        v = payload.get("dispatchSyncedAt", payload.get("dispatch_synced_at"))
        if v in (None, "", False):
            ds = None
        else:
            ds = _parse_iso_datetime_maybe(v)
    else:
        ds = ex_ds

    if existing_row_meta is None:
        next_rv = 1
    else:
        next_rv = ex_rv + 1

    return dr, pc, ds, next_rv, None


def _user_may_access_case_data(case_data):
    """
    True if current user may read/write this case (same rules as GET /api/cases list filter).
    Uses :func:`_epcr_principal_has_case_access` (case-insensitive ``assignedUsers`` and
    ``crewManifest`` usernames). The previous ``uname in assignedUsers`` check was exact-match
    only and ignored ``crewManifest``, so collaborators saw an empty list or got 403 on GET/PUT.
    """
    if _epcr_privileged_role():
        return True
    uname = _cura_auth_principal()[0]
    return _epcr_principal_has_case_access(uname, case_data)


def _sync_case_patient_match_meta(cursor, case_id, case_payload):
    """Persist slim Cura patient-trace fields; no-op if column missing or payload invalid."""
    from . import cura_patient_trace as cpt
    from .cura_mpi import _column_exists

    if not _column_exists(cursor, "cases", "patient_match_meta"):
        return
    if not isinstance(case_payload, dict):
        return
    try:
        meta = json.dumps(cpt.patient_match_meta_from_case(case_payload))
        cursor.execute(
            "UPDATE cases SET patient_match_meta = %s WHERE id = %s",
            (meta, int(case_id)),
        )
    except Exception:
        logger.exception("_sync_case_patient_match_meta case_id=%s", case_id)


def _mpi_case_select_extra(cursor) -> str:
    from . import cura_mpi

    if cura_mpi._column_exists(cursor, "cases", "mpi_patient_id"):
        return ", mpi_patient_id, cura_location_id"
    return ""


def _apply_mpi_columns_to_case_json(case_data, row) -> None:
    """Merge DB MPI columns into PatientInfo.ptInfo when present on cases row."""
    if not row or len(row) < 13:
        return
    from . import cura_mpi

    mpi_patient_id, cura_location_id = row[11], row[12]
    cura_mpi.merge_mpi_into_case_json(case_data, mpi_patient_id, cura_location_id)


def _audit_epcr_api(action_text):
    try:
        log_audit(_cura_auth_principal()[0] or "unknown", action_text, patient_id=None)
    except Exception:
        logger.exception("EPCR API audit log failed")

def send_reset_email(user, token):
    """Sends a password reset email to a care company user."""
    subject = "Password Reset for Care Company Portal"
    sender = current_app.config.get("MAIL_DEFAULT_SENDER")
    recipients = [user.email]
    reset_url = url_for('care_company.reset_password', token=token, _external=True)
    text_body = f"""Dear {user.company_name},

To reset your password, please click the following link:
{reset_url}

If you did not request a password reset, please ignore this email.

Kind regards,
The Support Team
"""
    html_body = f"""<p>Dear {user.company_name},</p>
<p>To reset your password, please click the following link:</p>
<p><a href="{reset_url}">{reset_url}</a></p>
<p>If you did not request a password reset, please ignore this email.</p>
<p>Kind regards,<br>The Support Team</p>"""
    mail = Mail(current_app)
    msg = Message(subject, sender=sender, recipients=recipients)
    msg.body = text_body
    msg.html = html_body
    mail.send(msg)
    logger.info("Password reset email sent to %s", user.email)

# Instantiate PluginManager and load core manifest.
plugin_manager = PluginManager(os.path.abspath('app/plugins'))
core_manifest = plugin_manager.get_core_manifest()


def care_company_feature_enabled():
    """
    Vita/Care company public portal plus plugin admin screens for care company users.
    **Default: disabled.** Set ``CURA_ENABLE_CARE_COMPANY_PORTAL=true`` (or ``1``/``yes``/``on``) to enable.
    ``CURA_DISABLE_CARE_COMPANY_PORTAL=true`` always forces off (overrides enable).
    """
    if (os.environ.get("CURA_DISABLE_CARE_COMPANY_PORTAL") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return False
    return (os.environ.get("CURA_ENABLE_CARE_COMPANY_PORTAL") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def legacy_patient_features_enabled():
    """
    Legacy patient/client record UI from the module’s original urgent-care design.
    **Default: disabled.** Enable explicitly only if you still rely on these screens.

    - Set ``CURA_ENABLE_LEGACY_PATIENTS=true`` (or 1/yes/on) to enable.
    - Set ``CURA_DISABLE_LEGACY_PATIENTS=true`` to force off.
    """
    if (os.environ.get("CURA_DISABLE_LEGACY_PATIENTS") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return False
    return (os.environ.get("CURA_ENABLE_LEGACY_PATIENTS") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


# =============================================================================
# INTERNAL BLUEPRINT (for admin side)
# =============================================================================
internal_template_folder = os.path.join(os.path.dirname(__file__), 'templates')
internal_static_folder = os.path.join(os.path.dirname(__file__), "static")
internal_bp = Blueprint(
    'medical_records_internal',
    __name__,
    url_prefix='/plugin/medical_records_module',
    template_folder=internal_template_folder,
    static_folder=internal_static_folder,
    static_url_path="static",
)

SAFEGUARDING_OVERSIGHT_PIN_SESSION_KEY = "safeguarding_oversight_pin_verified_at"
SAFEGUARDING_OVERSIGHT_INTENDED_URL_KEY = "safeguarding_oversight_intended_url"
SAFEGUARDING_OVERSIGHT_POST_UNLOCK_KEY = "safeguarding_oversight_post_unlock"
SAFEGUARDING_OVERSIGHT_DETAIL_OPENED_KEY = "safeguarding_oversight_referral_detail_was_opened"


@internal_bp.context_processor
def _medical_internal_template_globals():
    return {
        "care_company_feature_enabled": care_company_feature_enabled(),
        "legacy_patient_features_enabled": legacy_patient_features_enabled(),
    }


@internal_bp.before_request
def _epcr_plugin_api_gate():
    """
    Ventus-style central gate: Bearer JWT or Flask-Login session for all JSON API routes
    under ``/plugin/medical_records_module/api/``, except token acquisition and health ping.
    """
    path = (request.path or "").rstrip("/")
    prefix = "/plugin/medical_records_module/api"
    if not (request.path or "").startswith(prefix):
        return None
    if request.method == "OPTIONS":
        return None
    if path == prefix + "/cura/auth/token" and request.method == "POST":
        return None
    if path == prefix + "/ping" and request.method in ("GET", "HEAD"):
        return None
    return _require_epcr_json_api()


_SAFEGUARDING_OVERSIGHT_PIN_ENDPOINTS = frozenset(
    {
        "medical_records_internal.safeguarding_manager_home",
        "medical_records_internal.safeguarding_manager_referral",
        "medical_records_internal.safeguarding_manager_unlock",
    }
)


@internal_bp.before_request
def _safeguarding_oversight_pin_leave_scope():
    """
    Personal PIN for safeguarding oversight applies only while the user stays on safeguarding
    screens. Any other page under this blueprint clears it (static assets excluded).
    """
    ep = getattr(request, "endpoint", None) or ""
    if ep in _SAFEGUARDING_OVERSIGHT_PIN_ENDPOINTS:
        return None
    if ep.endswith(".static"):
        return None
    session.pop(SAFEGUARDING_OVERSIGHT_PIN_SESSION_KEY, None)
    session.pop(SAFEGUARDING_OVERSIGHT_INTENDED_URL_KEY, None)
    return None


@internal_bp.route('/')
def landing():
    """Landing page (router) for Medical Records Module."""
    return render_template("router.html", config=core_manifest)

@internal_bp.route('/crew', methods=['GET'])
@login_required
def crew_view():
    """
    Crew view route.
    When accessed without AJAX parameters, renders the full crew page (crew/crew_home.html)
    that includes the search form, results table, and a persistent modal for PIN entry.
    Allowed roles: "crew", "admin", "superuser", "clinical_lead".
    """
    flash("Crew access is not enabled for the medical records module in production.", "warning")
    return redirect(url_for("medical_records_internal.landing"))

@internal_bp.route('/crew/verify_pin', methods=['POST'])
@login_required
def verify_pin():
    """
    AJAX endpoint to verify the crew member's personal PIN.
    Expects JSON with the key 'personal_pin'.
    Allowed roles: "crew", "admin", "superuser", "clinical_lead".
    """
    allowed_roles = ["crew", "admin", "superuser", "clinical_lead", "support_break_glass"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({"error": "Unauthorised access"}), 403
    data = request.get_json()
    if not data or 'personal_pin' not in data:
        return jsonify({"error": "Missing personal PIN"}), 400
    personal_pin = data.get('personal_pin').strip()
    if not personal_pin:
        return jsonify({"error": "Personal PIN cannot be empty"}), 403
    if not current_user.personal_pin_hash or current_user.personal_pin_hash.strip() == "":
        return jsonify({
            "error": (
                "Personal PIN not set. Open the user menu and choose Personal PIN to set a 6-digit PIN."
            ),
        }), 403
    if not AuthManager.verify_password(current_user.personal_pin_hash, personal_pin):
        logger.warning("User %s provided an invalid personal PIN.", current_user.username)
        return jsonify({"error": "Invalid personal PIN"}), 403
    # Mark the current session as PIN-verified (time-limited) for sensitive crew actions.
    session["crew_pin_verified_at"] = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    log_audit(current_user.username, "Crew verified personal PIN")
    return jsonify({"message": "PIN verified"}), 200

@internal_bp.route('/crew/case-access', methods=['GET', 'POST'])
@login_required
def crew_case_access():
    """
    Crew case access workflow (case-first): verify personal PIN, then open a case using
    case reference + subject DOB + case access PIN.
    """
    flash("Crew access is not enabled for the medical records module in production.", "warning")
    return redirect(url_for("medical_records_internal.landing"))

    # Require PIN verification within a short window.
    verified_at = session.get("crew_pin_verified_at")
    if verified_at:
        try:
            ts = datetime.fromisoformat(str(verified_at))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - ts > timedelta(minutes=15):
                verified_at = None
        except Exception:
            verified_at = None
    if not verified_at:
        flash("Verify your personal PIN to access cases.", "warning")

    error = None
    if request.method == "POST":
        if not verified_at:
            return render_template("crew/crew_home.html", error="Personal PIN verification required.", config=core_manifest)

        def _redact_case_ref(v):
            v = (v or "").strip()
            if len(v) <= 4:
                return "***"
            return ("*" * (len(v) - 4)) + v[-4:]

        case_ref = (request.form.get("case_ref") or "").strip()
        dob_in = (request.form.get("dob") or "").strip()
        access_pin_in = (request.form.get("access_pin") or "").strip()

        current_app.logger.info(
            "Crew case-access attempt: user=%s case_ref=%s",
            getattr(current_user, "username", "unknown"),
            _redact_case_ref(case_ref),
        )

        if not (case_ref and case_ref.isdigit() and len(case_ref) == 10):
            error = "Case Reference Number must be exactly 10 digits."
            return render_template("crew/crew_home.html", error=error, config=core_manifest)

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT data, status, created_at, closed_at, updated_at, "
                "dispatch_reference, primary_callsign, dispatch_synced_at, record_version "
                "FROM cases WHERE id = %s",
                (case_ref,),
            )
            row = cursor.fetchone()
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        if not row:
            error = "The details you entered did not match our records."
            log_audit(
                current_user.username,
                f"Crew case-access denied (not found ref={_redact_case_ref(case_ref)})",
                case_id=int(case_ref) if str(case_ref).isdigit() else None,
            )
            return render_template("crew/crew_home.html", error=error, config=core_manifest)

        (
            data_str,
            status,
            created_at,
            closed_at,
            updated_at,
            dispatch_reference,
            primary_callsign,
            dispatch_synced_at,
            record_version,
        ) = row

        case_data = _parse_case_json(data_str)
        case_data["status"] = status
        case_data["createdAt"] = created_at.isoformat() if created_at else None
        case_data["closedAt"] = closed_at.isoformat() if closed_at else None
        case_data["updatedAt"] = updated_at.isoformat() if updated_at else None
        _merge_epcr_link_into_case_json(case_data, dispatch_reference, primary_callsign, dispatch_synced_at, record_version)

        pt_info = None
        for section in case_data.get("sections", []) or []:
            if str(section.get("name", "") or "").strip().lower() == "patientinfo":
                pt_info = (section.get("content") or {}).get("ptInfo")
                break
        dob_case = str((pt_info or {}).get("dob") or "").strip()
        dob_in_canon = dob_in.split("T", 1)[0] if "T" in dob_in else dob_in
        dob_case_canon = dob_case.split("T", 1)[0] if "T" in dob_case else dob_case
        if not dob_case_canon or not dob_in_canon or dob_in_canon != dob_case_canon:
            error = "The details you entered did not match our records."
            log_audit(
                current_user.username,
                f"Crew case-access denied (dob mismatch ref={_redact_case_ref(case_ref)})",
                case_id=int(case_ref) if str(case_ref).isdigit() else None,
            )
            return render_template("crew/crew_home.html", error=error, config=core_manifest)

        incident_log = {}
        for section in case_data.get("sections", []) or []:
            if str(section.get("name", "") or "").strip().lower() == "incident log":
                incident_log = (section.get("content") or {}).get("incident") or {}
                break
        pin_case = str(incident_log.get("pinCode") or "").strip()
        if not pin_case or access_pin_in != pin_case:
            error = "The details you entered did not match our records."
            log_audit(
                current_user.username,
                f"Crew case-access denied (pin mismatch ref={_redact_case_ref(case_ref)})",
                case_id=int(case_ref) if str(case_ref).isdigit() else None,
            )
            return render_template("crew/crew_home.html", error=error, config=core_manifest)

        # Prepare display fields (never include access PIN in templates).
        case_data["ptInfo"] = pt_info or {}
        case_data["incident"] = {key: (value if value else "Not Provided") for key, value in incident_log.items()}

        log_audit(
            current_user.username,
            f"Crew accessed case ref={_redact_case_ref(case_ref)}",
            case_id=int(case_ref) if str(case_ref).isdigit() else None,
        )
        return render_template("public/case_access_pdf.html", case_data=case_data)

    return render_template("crew/crew_home.html", error=error, config=core_manifest)

@internal_bp.route('/crew/search', methods=['GET'])
@login_required
def crew_search():
    """
    AJAX endpoint for patient search using raw MySQL.
    Expects query parameters: 'date_of_birth' and 'postcode'.
    Allowed roles: "crew", "admin", "superuser", "clinical_lead".
    """
    if not legacy_patient_features_enabled():
        return jsonify({"error": "Legacy patient features are disabled"}), 410
    allowed_roles = ["crew", "admin", "superuser", "clinical_lead", "support_break_glass"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({"error": "Unauthorised access"}), 403
    dob_str = request.args.get('date_of_birth')
    postcode = request.args.get('postcode')
    if not dob_str or not postcode:
        logger.warning("Crew search missing date_of_birth or postcode.")
        return jsonify({"error": "Missing date_of_birth or postcode"}), 400
    try:
        dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "Invalid date_of_birth format; please use YYYY-MM-DD"}), 400
    patients = Patient.search_by_dob_and_postcode(dob, postcode)
    if not patients:
        logger.warning("No patient found for DOB: %s and postcode: %s", dob_str, postcode)
        return jsonify({"error": "No matching patient found"}), 404
    AuditLog.insert_log(current_user.username, "Crew performed patient search", patient_id=patients[0].get("id"))
    current_date = datetime.utcnow().date()
    patients_list = []
    for p in patients:
        age = calculate_age(p.get('date_of_birth'), current_date) if p.get('date_of_birth') else "N/A"
        patients_list.append({
            "id": p.get("id"),
            "first_name": p.get("first_name"),
            "middle_name": p.get("middle_name"),
            "last_name": p.get("last_name"),
            "age": age,
            "gender": p.get("gender"),
            "address": p.get("address"),
            "postcode": p.get("postcode"),
            "date_of_birth": p.get("date_of_birth"),
            "package_type": p.get("package_type"),
            "care_company_id": p.get("care_company_id"),
            "access_requirements": p.get("access_requirements"),
            "risk_flags": p.get("notes"),
            "contact_number": p.get("contact_number")
        })
  
    return jsonify({"message": "Patient search successful.", "patients": patients_list}), 200

@internal_bp.route('/crew/view_record/<id>', methods=['GET'])
@login_required
def view_patient_record(id):
    if not legacy_patient_features_enabled():
        flash("Legacy patient features are disabled.", "warning")
        return redirect(url_for("medical_records_internal.landing"))
    patient = Patient.get_by_id(id)
    if not patient:
        return render_template(
            "crew/crew_home.html",
            error="Patient record not found",
            config=core_manifest
        )

    # For fields that should be dictionaries:
    dict_keys = [
        'gp_details',
        'resuscitation_directive',
        'payment_details',
        'weight'
    ]
    for key in dict_keys:
        value = patient.get(key)
        if isinstance(value, dict):
            continue
        elif isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value.strip())
                patient[key] = parsed if isinstance(parsed, dict) else {}
            except Exception as e:
                print(f"Error parsing {key}: {e}")
                patient[key] = {}
        else:
            patient[key] = {}

    # For fields that should be lists:
    list_keys = [
        'medical_conditions',
        'allergies',
        'medications',
        'previous_visit_records',
        'access_requirements',
        'notes',
        'message_log',
        'next_of_kin_details',
        'lpa_details'
    ]
    for key in list_keys:
        value = patient.get(key)
        if isinstance(value, list):
            continue
        elif isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value.strip())
                patient[key] = parsed if isinstance(parsed, list) else []
            except Exception as e:
                print(f"Error parsing {key}: {e}")
                patient[key] = []
        else:
            patient[key] = []

    age = calculate_age(patient.get("date_of_birth")) if patient.get("date_of_birth") else "N/A"
    log_audit(current_user.username, "Crew viewed patient record", patient_id=id)

    return render_template(
        "crew/view_patient.html",
        patient=patient,
        age=age,
        config=core_manifest
    )

@internal_bp.route('/crew/edit_record/<id>', methods=['GET', 'POST'])
@login_required
def crew_edit_patient_record(id):
    # --- Authorization ---
    allowed_roles = {"admin", "superuser", "clinical_lead", "crew", "support_break_glass"}
    if not getattr(current_user, 'role', '').lower() in allowed_roles:
        flash("Unauthorised access", "danger")
        return redirect(url_for('medical_records_internal.landing'))

    # --- POST: process updates ---
    if request.method == 'POST':
        patient_existing = Patient.get_by_id(id)
        if not patient_existing:
            flash("Patient record not found.", "danger")
            return redirect(url_for('medical_records_internal.crew_search'))

        # -- Simple scalar fields --
        first_name   = request.form.get('first_name', '').strip() or patient_existing.get('first_name')
        middle_name  = request.form.get('middle_name', '').strip() or patient_existing.get('middle_name')
        last_name    = request.form.get('last_name', '').strip() or patient_existing.get('last_name')
        address      = request.form.get('address', '').strip() or patient_existing.get('address')
        postcode     = request.form.get('postcode', '').strip() or patient_existing.get('postcode')
        gender       = request.form.get('gender', '').strip() or patient_existing.get('gender')
        package_type = request.form.get('package_type', '').strip() or patient_existing.get('package_type', '')
        contact_num  = request.form.get('contact_number', '').strip() or patient_existing.get('contact_number', '')

        dob_str = request.form.get('date_of_birth', '').strip()
        if dob_str:
            try:
                dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
            except ValueError:
                dob = patient_existing.get("date_of_birth")
        else:
            dob = patient_existing.get("date_of_birth")

        # -- JSON list fields --
        def load_list(field, default_list=None):
            raw = request.form.get(field, '').strip()
            if raw:
                try:
                    parsed = json.loads(raw)
                    return parsed if isinstance(parsed, list) else (default_list or [])
                except Exception:
                    return default_list or []
            stored = patient_existing.get(field, '[]')
            try:
                parsed = json.loads(stored)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []

        medical_conditions      = load_list('medical_conditions', default_list=[])
        allergies               = load_list('allergies', default_list=[])
        medications             = load_list('medications', default_list=[])
        access_requirements     = load_list('access_requirements', default_list=[])
        previous_visit_records  = load_list('previous_visit_records', default_list=[])
        notes                   = load_list('notes', default_list=[])
        message_log             = load_list('message_log', default_list=[])

        # -- Next‑of‑Kin and LPA arrays from hidden inputs --
        next_of_kin_details = load_list('next_of_kin_details', default_list=[])
        lpa_details         = load_list('lpa_details',          default_list=[])

        # -- Nested JSON / dict fields --
        def load_dict(field):
            raw = patient_existing.get(field, '')
            if isinstance(raw, str) and raw.strip():
                try:
                    parsed = json.loads(raw)
                    return parsed if isinstance(parsed, dict) else {}
                except Exception:
                    return {}
            return raw if isinstance(raw, dict) else {}

        gp_existing     = load_dict('gp_details')
        weight_existing = load_dict('weight')

        gp_details = {
            "name":    request.form.get('gp_name', '').strip()    or gp_existing.get("name", ""),
            "address": request.form.get('gp_address', '').strip() or gp_existing.get("address", ""),
            "contact": request.form.get('gp_contact', '').strip() or gp_existing.get("contact", ""),
            "email":   request.form.get('gp_email', '').strip()   or gp_existing.get("email", "")
        }

        weight = {
            "weight":       request.form.get('weight_value', '').strip() or weight_existing.get("weight", ""),
            "date_weighed": request.form.get('date_weighed', '').strip() or weight_existing.get("date_weighed", ""),
            "source":       request.form.get('weight_source', '').strip() or weight_existing.get("source", "")
        }

        # -- Resuscitation directive --
        resus_existing = load_dict('resuscitation_directive')
        docs = []
        for field, label in [
            ('doc_dnar',     "DNAR"),
            ('doc_respect',  "Respect Form"),
            ('doc_advanced', "Advanced Directive"),
            ('doc_living',   "Living Will"),
            ('doc_lpa',      "LPA"),
            ('doc_care',     "Care Plan"),
        ]:
            if request.form.get(field, '').strip():
                docs.append(label)
        if not docs:
            docs = resus_existing.get("documents", [])

        resuscitation_directive = {
            "for_resuscitation": request.form.get('resus_option', '').strip() or resus_existing.get("for_resuscitation", ""),
            "documents":         docs
        }

        documents = request.form.get('documents', '').strip() or patient_existing.get('documents', "")

        # --- Assemble updates ---
        update_fields = {
            "first_name":             first_name,
            "middle_name":            middle_name,
            "last_name":              last_name,
            "address":                address,
            "date_of_birth":          dob,
            "gender":                 gender,
            "postcode":               postcode,
            "package_type":           package_type,
            "contact_number":         contact_num,
            "gp_details":             json.dumps(gp_details),
            "weight":                 json.dumps(weight),
            "medical_conditions":     json.dumps(medical_conditions),
            "allergies":              json.dumps(allergies),
            "medications":            json.dumps(medications),
            "previous_visit_records": json.dumps(previous_visit_records),
            "access_requirements":    json.dumps(access_requirements),
            "notes":                  json.dumps(notes),
            "message_log":            json.dumps(message_log),
            "next_of_kin_details":    json.dumps(next_of_kin_details),
            "lpa_details":            json.dumps(lpa_details),
            "resuscitation_directive":json.dumps(resuscitation_directive),
            "payment_details":        json.dumps(load_dict('payment_details')),
            "documents":              documents,
        }

        try:
            Patient.update_patient(id, **update_fields)
            log_audit(current_user.username, f"Edited patient record: {id}", patient_id=id)
            flash("Patient record updated successfully.", "success")
        except Exception as e:
            logger.error("Error updating patient record: %s", e)
            flash("Error updating patient record", "danger")
            return redirect(url_for('medical_records_internal.view_patient_record', id=id))

        return redirect(url_for('medical_records_internal.view_patient_record', id=id))

    # --- GET: render the edit form ---
    patient = Patient.get_by_id(id)
    if not patient:
        flash("Patient record not found.", "danger")
        return redirect(url_for('medical_records_internal.admin_patients'))

    log_audit(current_user.username, f"Accessed edit view for patient record: {id}", patient_id=id)

    # --- Parse simple JSON‑dict fields (single objects only) ---
    dict_keys = ['gp_details', 'resuscitation_directive', 'payment_details', 'weight']
    for key in dict_keys:
        raw = patient.get(key)
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw.strip())
                patient[key] = parsed if isinstance(parsed, dict) else {}
            except Exception:
                patient[key] = {}
        else:
            patient[key] = raw if isinstance(raw, dict) else {}

    # --- Parse JSON‑list fields (arrays of dicts) ---
    list_keys = [
        'medical_conditions', 'allergies', 'medications',
        'previous_visit_records', 'access_requirements',
        'next_of_kin_details', 'lpa_details',
    ]
    for key in list_keys:
        raw = patient.get(key)
        if isinstance(raw, list):
            continue
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw.strip())
                if isinstance(parsed, list):
                    patient[key] = parsed
                elif isinstance(parsed, dict):
                    patient[key] = [parsed]
                else:
                    patient[key] = []
            except Exception:
                patient[key] = []
        else:
            patient[key] = []

    age = calculate_age(patient.get("date_of_birth")) if patient.get("date_of_birth") else "N/A"
    return render_template(
        "crew/edit_patient.html",
        patient=patient,
        age=age,
        config=core_manifest
    )



@internal_bp.route('/crew/view_record/<id>/add_message_log_entry', methods=['POST'])
@login_required
def crew_add_message_log_entry(id):
    category = request.form.get('category')
    custom_category = request.form.get('custom_category')
    message_text = request.form.get('message')
    if category == "Other" and custom_category:
        category = custom_category
    if not category or not message_text:
        return jsonify({'error': 'Category and message are required.'}), 400
    new_message = {
        'id': str(uuid.uuid4()),
        'author': current_user.username,
        'timestamp': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        'category': category,
        'text': message_text
    }
    patient = Patient.get_by_id(id)
    if not patient:
        return jsonify({'error': 'Patient record not found.'}), 404
    message_log = patient.get("message_log")
    if not message_log or message_log == "":
        message_log = []
    elif not isinstance(message_log, list):
        try:
            message_log = json.loads(message_log)
            if not isinstance(message_log, list):
                message_log = []
        except Exception:
            message_log = []
    message_log.append(new_message)
    try:
        Patient.update_patient(id, message_log=json.dumps(message_log))
        log_audit(current_user.username, "Crew added message log entry", patient_id=id)
    except Exception as e:
        logger.error("Error updating patient (crew message log): %s", str(e))
        return jsonify({'error': 'Failed to save message log entry.'}), 500
    return jsonify({'success': True}), 200

@internal_bp.route('/crew/view_record/<id>/delete_message_log_entry', methods=['POST'])
@login_required
def crew_delete_message_log_entry(id):
    data = request.get_json()
    message_id = data.get('message_id')
    if not message_id:
        return jsonify({'error': 'Message ID is required.'}), 400
    patient = Patient.get_by_id(id)
    if not patient:
        return jsonify({'error': 'Patient record not found.'}), 404
    message_log = patient.get("message_log")
    if not message_log or message_log == "":
        message_log = []
    elif not isinstance(message_log, list):
        try:
            message_log = json.loads(message_log)
            if not isinstance(message_log, list):
                message_log = []
        except Exception:
            message_log = []
    new_log = [msg for msg in message_log if str(msg.get('id')) != str(message_id)]
    if len(new_log) == len(message_log):
        return jsonify({'error': 'Message not found.'}), 404
    try:
        Patient.update_patient(id, message_log=json.dumps(new_log))
        log_audit(current_user.username, "Crew deleted message log entry", patient_id=id)
    except Exception as e:
        logger.error("Error updating patient (delete message): %s", str(e))
        return jsonify({'error': 'Failed to delete message log entry.'}), 500
    return jsonify({'success': True}), 200

@internal_bp.route('/crew/view_record/<id>/add_risk_flag', methods=['POST'])
@login_required
def crew_add_risk_flag(id):
    flag_type = request.form.get('flag_type')
    custom_flag_type = request.form.get('custom_flag_type')
    description = request.form.get('description')
    if flag_type == "Other" and custom_flag_type:
        flag_type = custom_flag_type
    if not flag_type or not description:
        return jsonify({'error': 'Risk category and description are required.'}), 400
    new_flag = {
        'id': str(uuid.uuid4()),
        'flag_type': flag_type,
        'timestamp': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        'description': description
    }
    patient = Patient.get_by_id(id)
    if not patient:
        return jsonify({'error': 'Patient record not found.'}), 404
    risk_flags = patient.get("notes")
    if not risk_flags or risk_flags == "":
        risk_flags = []
    elif not isinstance(risk_flags, list):
        try:
            risk_flags = json.loads(risk_flags)
            if not isinstance(risk_flags, list):
                risk_flags = []
        except Exception:
            risk_flags = []
    risk_flags.append(new_flag)
    try:
        Patient.update_patient(id, notes=json.dumps(risk_flags))
        log_audit(current_user.username, "Crew added risk flag", patient_id=id)
    except Exception as e:
        logger.error("Error updating patient (crew risk flags): %s", str(e))
        return jsonify({'error': 'Failed to save risk flag.'}), 500
    return jsonify({'success': True}), 200

@internal_bp.route('/crew/view_record/<id>/delete_risk_flag', methods=['POST'])
@login_required
def crew_delete_risk_flag(id):
    data = request.get_json()
    flag_id = data.get('flag_id')
    if not flag_id:
        return jsonify({'error': 'Flag ID is required.'}), 400
    patient = Patient.get_by_id(id)
    if not patient:
        return jsonify({'error': 'Patient record not found.'}), 404
    risk_flags = patient.get("notes")
    if not risk_flags or risk_flags == "":
        risk_flags = []
    elif not isinstance(risk_flags, list):
        try:
            risk_flags = json.loads(risk_flags)
            if not isinstance(risk_flags, list):
                risk_flags = []
        except Exception:
            risk_flags = []
    new_flags = [flag for flag in risk_flags if str(flag.get('id')) != str(flag_id)]
    if len(new_flags) == len(risk_flags):
        return jsonify({'error': 'Risk flag not found.'}), 404
    try:
        Patient.update_patient(id, notes=json.dumps(new_flags))
        log_audit(current_user.username, "Crew deleted risk flag", patient_id=id)
    except Exception as e:
        logger.error("Error updating patient (delete risk flag): %s", str(e))
        return jsonify({'error': 'Failed to delete risk flag.'}), 500
    return jsonify({'success': True}), 200

# -----------------------
# ADMIN ROUTES
# -----------------------
@internal_bp.route('/admin', methods=['GET'])
@login_required
def admin_patients():
    """
    Admin view route that displays a table of all patient records with a live search bar.
    Allowed roles: "admin", "superuser", "clinical_lead".
    """
    if not legacy_patient_features_enabled():
        flash("Legacy patient features are disabled.", "warning")
        return redirect(url_for("medical_records_internal.landing"))
    allowed_roles = ["admin", "superuser", "clinical_lead", "support_break_glass"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        flash("Unauthorised access", "danger")
        return redirect(url_for('medical_records_internal.landing'))
    search = request.args.get('q')
    patients = Patient.get_all(search)
    current_date = datetime.utcnow().date()
    for p in patients:
        if p.get("date_of_birth"):
            p["age"] = calculate_age(p["date_of_birth"], current_date)
        else:
            p["age"] = "N/A"
    log_audit(current_user.username, "Admin accessed patient list")
    return render_template("admin/patients.html", patients=patients, config=core_manifest)

@internal_bp.route('/admin/search', methods=['GET'])
@login_required
def admin_search():
    """
    AJAX endpoint that returns JSON for patient records matching a search query.
    Allowed roles: "admin", "superuser", "clinical_lead".
    """
    if not legacy_patient_features_enabled():
        return jsonify({"error": "Legacy patient features are disabled"}), 410
    allowed_roles = ["admin", "superuser", "clinical_lead", "support_break_glass"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({"error": "Unauthorised access"}), 403
    search = request.args.get('q')
    patients = Patient.get_all(search)
    current_date = datetime.utcnow().date()
    for p in patients:
        if p.get("date_of_birth"):
            p["age"] = calculate_age(p["date_of_birth"], current_date)
        else:
            p["age"] = "N/A"
    return jsonify({"patients": patients})

@internal_bp.route('/admin/search_care_company', methods=['GET'])
@login_required
def search_care_company():
    """
    AJAX endpoint that returns JSON for care company records matching a search query.
    Allowed roles: "admin", "superuser", "clinical_lead".
    """
    allowed_roles = ["admin", "superuser", "clinical_lead", "support_break_glass"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({"error": "Unauthorised access"}), 403

    search = request.args.get('q', '').strip()
    companies = CareCompanyUser.get_all(search)  # calls the static method we just created

    # Transform results to match the expected front-end structure
    # e.g. returning "name" instead of "company_name"
    result_data = []
    for c in companies:
        result_data.append({
            "id": c["id"],
            "name": c["company_name"]  # front-end expects "name"
        })

    return jsonify({"companies": result_data})

@internal_bp.route('/admin/view_record/<id>', methods=['GET'])
@login_required
def admin_view_record(id):
    # --- Authorization ---
    allowed_roles = {"admin", "superuser", "clinical_lead", "support_break_glass"}
    if getattr(current_user, 'role', '').lower() not in allowed_roles:
        return render_template("admin/view_patient.html",
                               error="Unauthorised access",
                               config=core_manifest)

    patient = Patient.get_by_id(id)
    if not patient:
        return render_template("admin/view_patient.html",
                               error="Patient record not found",
                               config=core_manifest)

    unlocked = session.get(f'unlocked_{id}', False)

    # --- parse single‐object JSON → dicts ---
    dict_keys = [
        'gp_details',
        'payment_details',
        'resuscitation_directive',
        'weight',
    ]
    for key in dict_keys:
        raw = patient.get(key)
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw.strip())
                patient[key] = parsed if isinstance(parsed, dict) else {}
            except:
                patient[key] = {}
        else:
            patient[key] = raw if isinstance(raw, dict) else {}

    # --- parse JSON arrays → lists of dicts ---
    list_keys = [
        'medical_conditions',
        'allergies',
        'medications',
        'previous_visit_records',
        'access_requirements',
        'next_of_kin_details',
        'lpa_details',
        'notes',
        'message_log'
    ]
    for key in list_keys:
        raw = patient.get(key)
        if isinstance(raw, list):
            continue
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw.strip())
                if isinstance(parsed, list):
                    patient[key] = parsed
                elif isinstance(parsed, dict):
                    patient[key] = [parsed]
                else:
                    patient[key] = []
            except:
                patient[key] = []
        else:
            patient[key] = []

    # --- mask if still locked ---
    if not unlocked:
        for field in [
            'gp_details',
            'payment_details',
            'resuscitation_directive',
            'weight',
            'medical_conditions',
            'allergies',
            'medications',
            'previous_visit_records',
            'access_requirements',
            'next_of_kin_details',
            'lpa_details'
        ]:
            patient[field] = "*******"

    age = calculate_age(patient.get("date_of_birth")) if patient.get("date_of_birth") else "N/A"
    log_audit(current_user.username, "Admin viewed patient record", patient_id=id)

    return render_template(
        "admin/view_patient.html",
        patient=patient,
        age=age,
        unlocked=unlocked,
        config=core_manifest
    )

@internal_bp.route('/admin/view_record/<id>/add_risk_flag', methods=['POST'])
@login_required
def add_risk_flag(id):
    """
    AJAX endpoint to add a risk flag (note) to the patient's record.
    Expects 'flag_type', optionally 'custom_flag_type', and 'description' in the POST form data.
    """
    flag_type = request.form.get('flag_type')
    custom_flag_type = request.form.get('custom_flag_type')
    description = request.form.get('description')
    if flag_type == "Other" and custom_flag_type:
        flag_type = custom_flag_type
    if not flag_type or not description:
        return jsonify({'error': 'Risk category and description are required.'}), 400
    new_flag = {
        'id': str(uuid.uuid4()),
        'flag_type': flag_type,
        'timestamp': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        'description': description
    }
    patient = Patient.get_by_id(id)
    if not patient:
        return jsonify({'error': 'Patient record not found.'}), 404
    risk_flags = patient.get("notes")
    if not risk_flags or risk_flags == "":
        risk_flags = []
    elif not isinstance(risk_flags, list):
        try:
            risk_flags = json.loads(risk_flags)
            if not isinstance(risk_flags, list):
                risk_flags = []
        except Exception:
            risk_flags = []
    risk_flags.append(new_flag)
    try:
        Patient.update_patient(id, notes=json.dumps(risk_flags))
        log_audit(current_user.username, "Admin added risk flag", patient_id=id)
    except Exception as e:
        logger.error("Error updating patient (admin risk flag): %s", str(e))
        return jsonify({'error': 'Failed to save risk flag.'}), 500
    return jsonify({'success': True}), 200

@internal_bp.route('/admin/view_record/<id>/delete_risk_flag', methods=['POST'])
@login_required
def delete_risk_flag(id):
    """
    AJAX endpoint to delete a risk flag (note) from the patient's record.
    Expects a JSON payload with key 'flag_id'.
    """
    data = request.get_json()
    flag_id = data.get('flag_id')
    if not flag_id:
        return jsonify({'error': 'Flag ID is required.'}), 400
    patient = Patient.get_by_id(id)
    if not patient:
        return jsonify({'error': 'Patient record not found.'}), 404
    risk_flags = patient.get("notes")
    if not risk_flags or risk_flags == "":
        risk_flags = []
    elif not isinstance(risk_flags, list):
        try:
            risk_flags = json.loads(risk_flags)
            if not isinstance(risk_flags, list):
                risk_flags = []
        except Exception:
            risk_flags = []
    new_flags = [flag for flag in risk_flags if str(flag.get('id')) != str(flag_id)]
    if len(new_flags) == len(risk_flags):
        return jsonify({'error': 'Risk flag not found.'}), 404
    try:
        Patient.update_patient(id, notes=json.dumps(new_flags))
        log_audit(current_user.username, "Admin deleted risk flag", patient_id=id)
    except Exception as e:
        logger.error("Error updating patient (delete risk flag): %s", str(e))
        return jsonify({'error': 'Failed to delete risk flag.'}), 500
    return jsonify({'success': True}), 200

@internal_bp.route('/admin/view_record/<id>/add_message_log_entry', methods=['POST'])
@login_required
def add_message_log_entry(id):
    """
    AJAX endpoint to add a message log entry to the patient's record.
    Expects 'category' (optionally 'custom_category') and 'message' in the POST form data.
    """
    category = request.form.get('category')
    custom_category = request.form.get('custom_category')
    message_text = request.form.get('message')
    if category == "Other" and custom_category:
        category = custom_category
    if not category or not message_text:
        return jsonify({'error': 'Category and message are required.'}), 400
    new_message = {
        'id': str(uuid.uuid4()),
        'author': current_user.username,
        'timestamp': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        'category': category,
        'text': message_text
    }
    patient = Patient.get_by_id(id)
    if not patient:
        return jsonify({'error': 'Patient record not found.'}), 404
    message_log = patient.get("message_log")
    if not message_log or message_log == "":
        message_log = []
    elif not isinstance(message_log, list):
        try:
            message_log = json.loads(message_log)
            if not isinstance(message_log, list):
                message_log = []
        except Exception:
            message_log = []
    message_log.append(new_message)
    try:
        Patient.update_patient(id, message_log=json.dumps(message_log))
        log_audit(current_user.username, "Admin added message log entry", patient_id=id)
    except Exception as e:
        logger.error("Error updating patient (admin message log): %s", str(e))
        return jsonify({'error': 'Failed to save message log entry.'}), 500
    return jsonify({'success': True}), 200

@internal_bp.route('/admin/view_record/<id>/delete_message_log_entry', methods=['POST'])
@login_required
def admin_delete_message_log_entry(id):
    """
    AJAX endpoint to delete a message log entry from the patient's record.
    Expects a JSON payload with key 'message_id'.
    """
    data = request.get_json()
    message_id = data.get('message_id')
    if not message_id:
        return jsonify({'error': 'Message ID is required.'}), 400
    patient = Patient.get_by_id(id)
    if not patient:
        return jsonify({'error': 'Patient record not found.'}), 404
    message_log = patient.get("message_log")
    if not message_log or message_log == "":
        message_log = []
    elif not isinstance(message_log, list):
        try:
            message_log = json.loads(message_log)
            if not isinstance(message_log, list):
                message_log = []
        except Exception:
            message_log = []
    new_log = [msg for msg in message_log if str(msg.get('id')) != str(message_id)]
    if len(new_log) == len(message_log):
        return jsonify({'error': 'Message not found.'}), 404
    try:
        Patient.update_patient(id, message_log=json.dumps(new_log))
        log_audit(current_user.username, "Admin deleted message log entry", patient_id=id)
    except Exception as e:
        logger.error("Error updating patient (delete message): %s", str(e))
        return jsonify({'error': 'Failed to delete message log entry.'}), 500
    return jsonify({'success': True}), 200

@internal_bp.route('/admin/unlock_record', methods=['POST'])
@login_required
def unlock_record():
    """
    Unlocks a patient record.
    Expects form fields:
      - 'id'             : patient ID
      - 'pin'            : one‑time PIN
      - 'justification'  : access reason
    Allowed roles: admin, superuser, clinical_lead.
    Returns JSON with error or success + redirect URL.
    """
    allowed_roles = {"admin", "superuser", "clinical_lead", "support_break_glass"}
    patient_id = request.form.get('id') or request.form.get('patient_id')
    pin           = (request.form.get('pin') or "").strip()
    justification = (request.form.get('justification') or "").strip()

    # 1) authorization
    if current_user.role.lower() not in allowed_roles:
        return jsonify(error="Unauthorised access"), 403

    # 2) parameters
    if not patient_id or not pin or not justification:
        return jsonify(error="Missing parameters"), 400

    # 3) verify PIN
    stored     = admin_pin_store.get("pin")
    expires_at = admin_pin_store.get("expires_at")
    if not stored or datetime.utcnow() > expires_at or pin != stored:
        return jsonify(error="Invalid or expired PIN"), 403

    # 4) consume PIN
    admin_pin_store.clear()

    # 5) audit log the unlock with justification
    log_audit(
        current_user.username,
        f"Unlocked record {patient_id} with PIN {pin} — reason: {justification}",
        patient_id=patient_id
    )

    # 6) mark session and respond
    session[f'unlocked_{patient_id}'] = True
    return jsonify({
        "message":      "Record unlocked successfully",
        "redirect_url": url_for('medical_records_internal.admin_view_record', id=patient_id)
    }), 200

@internal_bp.route('/admin/delete_record/<id>', methods=['POST'])
@login_required
def delete_patient_record(id):
    """
    Deletes a patient record.
    Allowed roles: "admin", "superuser", "clinical_lead".
    """
    allowed_roles = ["admin", "superuser", "clinical_lead", "support_break_glass"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        flash("Unauthorised access", "danger")
        return redirect(url_for('medical_records_internal.admin_patients'))
    try:
        Patient.delete_patient(id)
        log_audit(current_user.user, "Admin deleted patient record", patient_id=id)
    except Exception as e:
        logger.error("Error deleting patient record: %s", str(e))
        flash("Error deleting patient record", "danger")
        return redirect(url_for('medical_records_internal.admin_patients'))
    flash("Patient record deleted.", "success")
    return redirect(url_for('medical_records_internal.admin_patients'))

@internal_bp.route('/admin/add_record', methods=['GET', 'POST'])
@login_required
def add_patient_record():
    """
    Allows admin, superuser, or clinical_lead to add a new patient record.
    GET: Renders the add patient form.
    POST: Processes the form and inserts a new patient record.
    """
    allowed_roles = ["admin", "superuser", "clinical_lead", "support_break_glass"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        flash("Unauthorised access", "danger")
        return redirect(url_for('medical_records_internal.admin_patients'))
    
    if request.method == 'POST':
        nhs_number = request.form.get('nhs_number')
        first_name = request.form.get('first_name')
        middle_name = request.form.get('middle_name')
        care_company_id = request.form.get("care_company_id")
        last_name = request.form.get('last_name')
        contact_number = request.form.get("contact_number")
        address = request.form.get('address')
        gp_details = request.form.get('gp_details')
        medical_conditions = request.form.get('medical_conditions')
        allergies = request.form.get('allergies')
        medications = request.form.get('medications')
        previous_visit_records = request.form.get('previous_visit_records')
        package_type = request.form.get('package_type')
        notes = request.form.get('notes')
        message_log = request.form.get('message_log')
        access_requirements = request.form.get('access_requirements')
        payment_details = request.form.get('payment_details')
        next_of_kin_details = request.form.get('next_of_kin_details')
        lpa_details = request.form.get('lpa_details')
        resuscitation_directive = request.form.get('resuscitation_directive')
        documents = request.form.get('documents')
        dob_str = request.form.get('date_of_birth')
        weight = request.form.get('weight')
        gender = request.form.get('gender')
        postcode = request.form.get('postcode')
        try:
            dob = datetime.strptime(dob_str, "%Y-%m-%d").date() if dob_str else None
        except ValueError:
            dob = None
        try:
            Patient.add_patient(
                nhs_number, first_name, middle_name, last_name, address,
                gp_details, medical_conditions, allergies, medications, previous_visit_records,
                package_type, notes, message_log, access_requirements, payment_details,
                next_of_kin_details, lpa_details, resuscitation_directive, documents,
                dob, weight, gender, postcode, care_company_id, contact_number
            )
        except Exception as e:
            logger.error("Error adding new patient record: %s", str(e))
            flash("Error adding new patient record", "danger")
            return redirect(url_for('medical_records_internal.admin_patients'))
        flash("New patient record added successfully.", "success")
        return redirect(url_for('medical_records_internal.admin_patients'))
    return render_template("admin/add_patient.html", config=core_manifest)


@internal_bp.route('/admin/edit_record/<id>', methods=['GET', 'POST'])
@login_required
def edit_patient_record(id):
    # --- Authorization ---
    allowed_roles = {"admin", "superuser", "clinical_lead", "support_break_glass"}
    if not getattr(current_user, 'role', '').lower() in allowed_roles:
        flash("Unauthorised access", "danger")
        return redirect(url_for('medical_records_internal.admin_patients'))

    # --- POST: process updates ---
    if request.method == 'POST':
        patient_existing = Patient.get_by_id(id)
        if not patient_existing:
            flash("Patient record not found.", "danger")
            return redirect(url_for('medical_records_internal.admin_patients'))

        # --- Scalar fields ---
        first_name  = request.form.get('first_name','').strip()  or patient_existing.get('first_name')
        middle_name = request.form.get('middle_name','').strip() or patient_existing.get('middle_name')
        last_name   = request.form.get('last_name','').strip()   or patient_existing.get('last_name')
        address     = request.form.get('address','').strip()     or patient_existing.get('address')
        dob_str     = request.form.get('date_of_birth','').strip()
        try:
            dob = datetime.strptime(dob_str, "%Y-%m-%d").date() if dob_str else patient_existing.get('date_of_birth')
        except ValueError:
            dob = patient_existing.get('date_of_birth')
        gender       = request.form.get('gender','').strip() or patient_existing.get('gender')
        postcode     = request.form.get('postcode','').strip() or patient_existing.get('postcode')
        package_type = request.form.get('package_type','').strip() or patient_existing.get('package_type','')
        contact_num  = request.form.get('contact_number','').strip() or patient_existing.get('contact_number','')

        # --- Helper to load JSON arrays ---
        def load_list(field):
            raw = request.form.get(field,'').strip()
            if raw:
                try:
                    arr = json.loads(raw)
                    return arr if isinstance(arr, list) else []
                except:
                    return []
            stored = patient_existing.get(field,'[]')
            try:
                arr = json.loads(stored)
                return arr if isinstance(arr, list) else []
            except:
                return []

        medical_conditions     = load_list('medical_conditions')
        allergies              = load_list('allergies')
        medications            = load_list('medications')
        access_requirements    = load_list('access_requirements')
        previous_visit_records = load_list('previous_visit_records')
        notes                  = load_list('notes')
        message_log            = load_list('message_log')
        next_of_kin_details    = load_list('next_of_kin_details')
        lpa_details            = load_list('lpa_details')

        # --- Helper to load JSON dicts ---
        def load_dict(field):
            raw = patient_existing.get(field,'')
            if isinstance(raw, str) and raw.strip():
                try:
                    obj = json.loads(raw)
                    return obj if isinstance(obj, dict) else {}
                except:
                    return {}
            return raw if isinstance(raw, dict) else {}

        unlocked = session.get(f'unlocked_{id}', False)

        if unlocked:
            # GP Details
            existing_gp = load_dict('gp_details')
            gp_details = {
                "name":    request.form.get('gp_name','').strip()    or existing_gp.get("name",""),
                "address": request.form.get('gp_address','').strip() or existing_gp.get("address",""),
                "contact": request.form.get('gp_contact','').strip() or existing_gp.get("contact",""),
                "email":   request.form.get('gp_email','').strip()   or existing_gp.get("email","")
            }
            # Weight
            existing_weight = load_dict('weight')
            weight = {
                "weight":       request.form.get('weight_value','').strip() or existing_weight.get("weight",""),
                "date_weighed": request.form.get('date_weighed','').strip() or existing_weight.get("date_weighed",""),
                "source":       request.form.get('weight_source','').strip() or existing_weight.get("source","")
            }
            # Payment
            existing_payment = load_dict('payment_details')
            payment_details = {
                "payment_method": request.form.get('payment_method','').strip() or existing_payment.get("payment_method",""),
                "billing_email":  request.form.get('billing_email','').strip()  or existing_payment.get("billing_email","")
            }
            # Resuscitation
            existing_resus = load_dict('resuscitation_directive')
            docs = []
            for fld,label in [
                ('doc_dnar','DNAR'),
                ('doc_respect','Respect Form'),
                ('doc_advanced','Advanced Directive'),
                ('doc_living','Living Will'),
                ('doc_lpa','LPA'),
                ('doc_care','Care Plan'),
            ]:
                if request.form.get(fld,'').strip():
                    docs.append(label)
            resuscitation_directive = {
                "for_resuscitation": request.form.get('resus_option','').strip() or existing_resus.get("for_resuscitation",""),
                "documents":         docs if docs else existing_resus.get("documents",[])
            }
        else:
            # preserve existing
            gp_details              = load_dict('gp_details')
            weight                  = load_dict('weight')
            payment_details         = load_dict('payment_details')
            resuscitation_directive = load_dict('resuscitation_directive')

        documents = request.form.get('documents','').strip() or patient_existing.get('documents',"")

        # --- Assemble updates ---
        update_fields = {
            "first_name":              first_name,
            "middle_name":             middle_name,
            "last_name":               last_name,
            "address":                 address,
            "date_of_birth":           dob,
            "gender":                  gender,
            "postcode":                postcode,
            "package_type":            package_type,
            "contact_number":          contact_num,
            "gp_details":              json.dumps(gp_details),
            "weight":                  json.dumps(weight),
            "payment_details":         json.dumps(payment_details),
            "resuscitation_directive": json.dumps(resuscitation_directive),
            "medical_conditions":      json.dumps(medical_conditions),
            "allergies":               json.dumps(allergies),
            "medications":             json.dumps(medications),
            "previous_visit_records":  json.dumps(previous_visit_records),
            "access_requirements":     json.dumps(access_requirements),
            "notes":                   json.dumps(notes),
            "message_log":             json.dumps(message_log),
            "next_of_kin_details":     json.dumps(next_of_kin_details),
            "lpa_details":             json.dumps(lpa_details),
            "documents":               documents,
        }

        try:
            Patient.update_patient(id, **update_fields)
            log_audit(current_user.username, f"Edited patient record: {id}", patient_id=id)
            flash("Patient record updated successfully.", "success")
        except Exception as e:
            logger.error("Error updating patient record: %s", e)
            flash("Error updating patient record", "danger")
            return redirect(url_for('medical_records_internal.admin_view_record', id=id))

        return redirect(url_for('medical_records_internal.admin_view_record', id=id))

    # --- GET: render the edit form ---
    patient = Patient.get_by_id(id)
    if not patient:
        flash("Patient record not found.", "danger")
        return redirect(url_for('medical_records_internal.admin_patients'))

    log_audit(current_user.username, f"Accessed edit view for patient record: {id}", patient_id=id)
    unlocked = session.get(f'unlocked_{id}', False)

    # parse dicts
    dict_keys = ['gp_details','payment_details','resuscitation_directive','weight']
    for key in dict_keys:
        raw = patient.get(key)
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw.strip())
                patient[key] = parsed if isinstance(parsed, dict) else {}
            except:
                patient[key] = {}
        else:
            patient[key] = raw if isinstance(raw, dict) else {}

    # parse lists
    list_keys = [
        'medical_conditions','allergies','medications',
        'previous_visit_records','access_requirements',
        'next_of_kin_details','lpa_details'
    ]
    for key in list_keys:
        raw = patient.get(key)
        if isinstance(raw, list):
            continue
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw.strip())
                patient[key] = parsed if isinstance(parsed, list) else []
            except:
                patient[key] = []
        else:
            patient[key] = []

    # mask if locked
    if not unlocked:
        for field in [
            'gp_details','payment_details','resuscitation_directive',
            'weight','next_of_kin_details','lpa_details'
        ]:
            patient[field] = "*******"

    age = calculate_age(patient.get("date_of_birth")) if patient.get("date_of_birth") else "N/A"
    return render_template(
        "admin/edit_patient.html",
        patient=patient,
        unlocked=unlocked,
        age=age,
        config=core_manifest
    )


# =============================================================================
# CLINICAL ROUTES
# =============================================================================
@internal_bp.route('/clinical', methods=['GET', 'POST'])
@login_required
def clinical_view():
    """
    Clinical lead view.
    Allowed roles: "clinical_lead", "superuser".
    - POST (AJAX): Generates a one-time admin PIN and returns JSON.
    - GET: Renders the clinical dashboard.
    """
    allowed_roles = ["clinical_lead", "superuser"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        flash("Unauthorised access", "danger")
        return redirect(url_for('medical_records_internal.landing'))
    if request.method == 'POST':
        payload = request.get_json(silent=True) or {}
        justification = (
            (payload.get("justification") if isinstance(payload, dict) else None)
            or request.form.get("justification")
            or ""
        ).strip()
        if not justification:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({"error": "Justification is required"}), 400
            flash("Justification is required to generate an admin PIN.", "warning")
            return render_template("clinical/clinical_home.html", config=core_manifest)

        new_pin = generate_pin()
        expires_at = datetime.utcnow() + timedelta(minutes=10)
        admin_pin_store["pin"] = new_pin
        admin_pin_store["expires_at"] = expires_at
        admin_pin_store["generated_by"] = f"{current_user.username} (reason: {justification})"
        log_audit(
            current_user.username,
            f"Generated new admin PIN — reason: {justification}",
        )
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"pin": new_pin, "expires_at": expires_at.isoformat()})
        return render_template("clinical/clinical_home.html", modal_pin=new_pin, pin_expires=expires_at.isoformat(), config=core_manifest)
    return render_template("clinical/clinical_home.html", config=core_manifest)

try:
    emailer = EmailManager()
except Exception as e:
    logger.warning("Email manager not configured at startup: %s", e)
    emailer = None

@internal_bp.route('/admin/request_unlock_pin', methods=['POST'])
@login_required
def request_unlock_pin():
    allowed = {'admin', 'superuser', 'support_break_glass'}
    if current_user.role.lower() not in allowed:
        return jsonify(error="Unauthorised access"), 403

    data          = request.get_json() or {}
    patient_id    = data.get('id')
    justification = (data.get('justification') or "").strip()
    if not patient_id or not justification:
        return jsonify(error="Missing parameters"), 400

    # 1) generate & store PIN
    new_pin     = generate_pin()
    expires_at  = datetime.utcnow() + timedelta(minutes=10)
    admin_pin_store.clear()
    admin_pin_store['pin']          = new_pin
    admin_pin_store['expires_at']   = expires_at
    admin_pin_store['generated_by'] = (
        f"System: Requested for Approval By: {current_user.username}"
    )

    # 2) audit log
    log_audit(
        current_user.username,
        f"Requested unlock PIN for record {patient_id} — reason: {justification}",
        patient_id=patient_id
    )

    # 3) fetch recipients via get_db_connection()
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        query = """
            SELECT email, first_name
            FROM users
            WHERE LOWER(role) IN (LOWER(%s), LOWER(%s))
        """
        cursor.execute(query, ("clinical_lead", "superuser"))
        recipients = cursor.fetchall()
        cursor.close()
        conn.close()
    except Exception as e:
        current_app.logger.error("Database error fetching PIN recipients: %s", e)
        return jsonify(error="Internal server error"), 500

    if not recipients:
        current_app.logger.error(
            "No clinical_lead or superuser accounts found when requesting PIN for %s",
            patient_id
        )
        return jsonify(error="No recipients configured"), 500

    # 4) send the email
    subject = f"PIN Request for patient #{patient_id}"
    body = (
        f"{current_user.username} has requested a one‑time PIN to unlock patient record {patient_id}.\n\n"
        f"Justification:\n{justification}\n\n"
        f"PIN (valid until {expires_at.isoformat()} UTC): {new_pin}\n\n"
        "If you approve, please call the requester to share this PIN; otherwise contact them for more info."
    )
    to_addrs = [row[0] for row in recipients]  # row = (email, first_name)

    try:
        active_emailer = emailer or EmailManager()
        active_emailer.send_email(subject=subject, body=body, recipients=to_addrs)
    except Exception as e:
        current_app.logger.error("Failed to send PIN emails: %s", e)
        return jsonify(error="Failed to send emails"), 500

    return jsonify(message="PIN request sent to clinical leads and superusers"), 200


def _clinical_audit_log_category(action: str) -> str:
    """
    Bucket audit log rows for the clinical dashboard (action text only; no schema migration).

    - epcr_case_access: Caldicott browser flow (requests, unlock, viewed case, denials tied to access).
    - epcr_cases_api: REST /api/cases traffic (high volume from Cura clients).
    - cura_safeguarding_oversight: Safeguarding manager in the browser (PIN, list, referral, notes, status).
    - cura_safeguarding_api: Safeguarding JSON/API from Cura apps and server facades (not the oversight UI).
    - cura_mpi: MPI / patient-match flag bundle and related Cura MPI audit lines.
    - cura_mi_api: Minor injury (MI) API audit lines.
    - cura_api: Other Cura / Ventus integration API (events, assignments, attachments, patient-contact, …).
    - cura_ops_ui: Browser Cura ops hub, settings, debrief exports (excluding safeguarding manager).
    - other: legacy patient/crew/admin/care-company lines and anything unmatched.
    """
    a = (action or "").strip()
    al = a.lower()
    if "epcr access audit:" in al or al.startswith("viewed epcr case dashboard"):
        return "epcr_case_access"
    if "denied epcr api access" in al or a.startswith("EPCR API "):
        return "epcr_cases_api"
    if "safeguarding manager" in al or "safeguarding oversight" in al:
        return "cura_safeguarding_oversight"
    if "cura_mpi" in al:
        return "cura_mpi"
    if a.startswith("MI "):
        return "cura_mi_api"
    if (
        a.startswith("Cura safeguarding ")
        or a.startswith("safeguarding_module")
        or a.startswith("Safeguarding facade")
    ):
        return "cura_safeguarding_api"
    if (
        a.startswith("Cura operational event ")
        or a.startswith("Cura patient-contact")
        or a.startswith("cad_correlation")
        or a.startswith("Cura attachment")
        or a.startswith("Cura ventus division sync")
    ):
        return "cura_api"
    if (
        a.startswith("Opened Cura")
        or a.startswith("Cura ops ")
        or a.startswith("Cura settings")
        or "Cura operational debrief" in a
    ):
        return "cura_ops_ui"
    return "other"


@internal_bp.route('/audit_log', methods=['GET'])
@login_required
def view_audit_log():
    """
    Returns the audit log for clinical leads or superusers.
    Each row includes ``category`` for UI filtering (EPCR access vs API vs Cura ops, etc.).
    Allowed roles: "clinical_lead", "superuser".
    """
    if not hasattr(current_user, 'role') or current_user.role.lower() not in ["clinical_lead", "superuser"]:
        return jsonify({"error": "Unauthorised access"}), 403
    fmt = (request.args.get("format") or "").strip().lower()
    try:
        lim_raw = int((request.args.get("limit") or "4000").strip())
    except ValueError:
        lim_raw = 4000
    limit = max(100, min(lim_raw, 8000))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT %s",
        (limit,),
    )
    logs = cursor.fetchall()
    cursor.close()
    conn.close()
    audit_entries = []
    counts = {
        "epcr_case_access": 0,
        "epcr_cases_api": 0,
        "cura_safeguarding_oversight": 0,
        "cura_safeguarding_api": 0,
        "cura_mpi": 0,
        "cura_mi_api": 0,
        "cura_api": 0,
        "cura_ops_ui": 0,
        "other": 0,
    }
    for log in logs:
        action = log.get("action") or ""
        cat = _clinical_audit_log_category(action)
        counts[cat] = counts.get(cat, 0) + 1
        audit_entries.append({
            "category": cat,
            "user": log.get("user"),
            "principal_role": log.get("principal_role"),
            "action": log.get("action"),
            "case_id": log.get("case_id"),
            "patient_id": log.get("patient_id"),
            "route": log.get("route"),
            "ip": log.get("ip"),
            "user_agent": log.get("user_agent"),
            "reason": log.get("reason"),
            "timestamp": log.get("timestamp").isoformat() if isinstance(log.get("timestamp"), datetime) else str(log.get("timestamp")),
        })
    logger.info("Audit log viewed by user %s.", current_user.username)
    payload = {
        "audit_logs": audit_entries,
        "category_counts": counts,
        "limit": limit,
        "categories_help": {
            "epcr_case_access": "EPCR case access (requests, codes, unlock, viewed case, Caldicott denials)",
            "epcr_cases_api": "EPCR cases REST API (Cura sync — reads/writes/collaborators/close)",
            "cura_safeguarding_oversight": "Safeguarding manager in browser (PIN, list, referral detail, notes, status changes)",
            "cura_safeguarding_api": "Safeguarding from Cura / API (referral writes, module API, JSON facade — not oversight UI)",
            "cura_mpi": "Cura MPI / patient-match flags and related bundle audit lines",
            "cura_mi_api": "Minor injury (MI) integration API audit lines",
            "cura_api": "Cura ops API — events, assignments, attachments, patient-contact reports, Ventus division sync, CAD correlation",
            "cura_ops_ui": "Cura event manager & settings in browser (hub, datasets, debrief downloads — not safeguarding manager)",
            "other": "Other (patient records, prescriptions, care company, crew, admin, …)",
        },
    }
    if fmt == "html":
        return render_template("clinical/audit_log.html", config=core_manifest, audit_payload=payload)
    return jsonify(payload), 200


def _safeguarding_oversight_roles_ok() -> bool:
    """Browser UI: clinical leads, admins, and superusers may open the safeguarding manager."""
    r = (getattr(current_user, "role", "") or "").lower()
    return r in ("clinical_lead", "superuser", "admin", "support_break_glass")


def _safeguarding_oversight_pin_fresh() -> bool:
    """
    Time-limited personal PIN session for safeguarding oversight (browser), same window as crew PIN.
    Aligns with Caldicott-style re-authentication before viewing sensitive referral content.
    """
    verified_at = session.get(SAFEGUARDING_OVERSIGHT_PIN_SESSION_KEY)
    if not verified_at:
        return False
    try:
        ts = datetime.fromisoformat(str(verified_at))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) - ts > timedelta(minutes=15):
            session.pop(SAFEGUARDING_OVERSIGHT_PIN_SESSION_KEY, None)
            return False
    except Exception:
        session.pop(SAFEGUARDING_OVERSIGHT_PIN_SESSION_KEY, None)
        return False
    return True


def _sg_oversight_local_url_ok(u: str) -> bool:
    """Reject open redirects: same-site path only."""
    if not u or not u.startswith("/") or u.startswith("//"):
        return False
    sr = request.script_root or ""
    if sr and not (u == sr or u.startswith(sr + "/")):
        return False
    return True


def _safeguarding_oversight_pin_gate_response():
    """If PIN not verified recently, remember target URL and show PIN form."""
    if _safeguarding_oversight_pin_fresh():
        return None
    session[SAFEGUARDING_OVERSIGHT_INTENDED_URL_KEY] = request.full_path
    return render_template(
        "clinical/safeguarding_manager_gate.html",
        config=core_manifest,
    )


def _sg_format_payload_cell_value(v) -> str:
    if v is None:
        return "—"
    if isinstance(v, bool):
        return "Yes" if v else "No"
    if isinstance(v, list):
        if not v:
            return "—"
        parts = []
        for x in v:
            if x is None:
                continue
            if isinstance(x, (dict, list)):
                parts.append(json.dumps(x, ensure_ascii=False))
            else:
                s = str(x).strip()
                if s:
                    parts.append(s)
        return ", ".join(parts) if parts else "—"
    if isinstance(v, dict):
        try:
            return json.dumps(v, ensure_ascii=False, indent=2)
        except Exception:
            return str(v)
    s = str(v).strip()
    return s if s else "—"


_SG_PAYLOAD_SKIP_KEYS = frozenset(
    {
        "activeStep",
        "active_step",
        "form",
        "data",
        "referral",
        "safeguarding",
        "meta",
        "metadata",
    }
)

_SG_PAYLOAD_KNOWN_LABELS = (
    ("subjectName", "Subject name"),
    ("subject_name", "Subject name"),
    ("subjectDob", "Subject date of birth"),
    ("subject_dob", "Subject date of birth"),
    ("subjectAddress", "Subject address"),
    ("subject_address", "Subject address"),
    ("subjectPhone", "Subject phone"),
    ("subject_phone", "Subject phone"),
    ("nhsNumber", "NHS number"),
    ("nhs_number", "NHS number"),
    ("personType", "Person type"),
    ("person_type", "Person type"),
    ("referrerName", "Referrer name"),
    ("referrer_name", "Referrer name"),
    ("referrerRole", "Referrer role"),
    ("referrer_role", "Referrer role"),
    ("referrerOrganisation", "Referrer organisation"),
    ("referrer_organisation", "Referrer organisation"),
    ("referrerPhone", "Referrer phone"),
    ("referrer_phone", "Referrer phone"),
    ("referrerEmail", "Referrer email"),
    ("referrer_email", "Referrer email"),
    ("parentGuardian", "Parent / guardian"),
    ("parent_guardian", "Parent / guardian"),
    ("parentPhone", "Parent / guardian phone"),
    ("parent_phone", "Parent / guardian phone"),
    ("riskLevel", "Risk / urgency (form)"),
    ("risk_level", "Risk / urgency (form)"),
    ("safeguardingUrgency", "Safeguarding urgency"),
    ("safeguarding_urgency", "Safeguarding urgency"),
    ("urgency", "Urgency"),
    ("abuseTypes", "Abuse / concern types"),
    ("abuse_types", "Abuse / concern types"),
    ("otherConcerns", "Other concerns"),
    ("other_concerns", "Other concerns"),
    ("additionalInfo", "Additional information"),
    ("additional_info", "Additional information"),
    ("actionsTaken", "Actions taken"),
    ("actions_taken", "Actions taken"),
    ("immediateRisk", "Immediate risk reported"),
    ("immediate_risk", "Immediate risk reported"),
    ("policeInvolved", "Police involved"),
    ("police_involved", "Police involved"),
    ("policeReference", "Police reference"),
    ("police_reference", "Police reference"),
    ("consent", "Consent given"),
    ("consentDetails", "Consent details"),
    ("consent_details", "Consent details"),
)


def _sg_payload_display_rows(payload: dict) -> list[dict]:
    """Ordered label/value rows for admin UI (no raw JSON dump)."""
    if not isinstance(payload, dict):
        return []
    seen_keys = set()
    labels_shown = set()
    rows: list[dict] = []
    for key, label in _SG_PAYLOAD_KNOWN_LABELS:
        if key not in payload or key in _SG_PAYLOAD_SKIP_KEYS:
            continue
        if label in labels_shown:
            continue
        labels_shown.add(label)
        seen_keys.add(key)
        rows.append({"label": label, "value": _sg_format_payload_cell_value(payload.get(key))})
    rest = sorted(k for k in payload if k not in seen_keys and k not in _SG_PAYLOAD_SKIP_KEYS)
    for key in rest:
        label = key.replace("_", " ")
        if len(label) > 1:
            label = label[0].upper() + label[1:]
        rows.append({"label": label, "value": _sg_format_payload_cell_value(payload.get(key))})
    return rows


def _sg_audit_trail_primary_secondary(action: str | None, detail: dict) -> tuple[str, str]:
    """
    Human-readable audit lines for the oversight table (avoid raw JSON blobs).
    Returns (primary, secondary). Secondary may be empty.
    """
    d = detail if isinstance(detail, dict) else {}
    note = d.get("note")
    fs = (d.get("from_status") or "").strip()
    ts = (d.get("to_status") or "").strip()
    if note is not None and str(note).strip():
        secondary = ""
        if fs and ts and fs != ts:
            secondary = f"Status: {fs} → {ts}"
        return str(note).strip(), secondary

    act = (action or "").strip().lower()
    fields = d.get("fields")
    if not isinstance(fields, list):
        fields = []

    if act == "create":
        return "Referral record created.", ""

    if act == "patch":
        parts = d.get("parts_updated")
        if isinstance(parts, list) and parts:
            if fs == ts and parts == ["referral_form"]:
                return "Referral form saved from Cura or the app (Sparrow status unchanged).", ""
            part_labels = {
                "referral_form": "referral form",
                "operational_event": "operational event link",
                "sync_status": "sync status",
                "sync_error": "sync error",
            }
            bits = [part_labels.get(p, p.replace("_", " ")) for p in parts if p]
            primary = ("Updated: " + ", ".join(bits)) if bits else "Record updated."
            if fs and ts and fs != ts:
                return primary, f"Status: {fs} → {ts}"
            return primary, ""
        if fs == ts and fields == ["payload"]:
            return "Referral form saved from Cura or the app (Sparrow status unchanged).", ""
        if fs == ts and set(fields) == {"payload"}:
            return "Referral form saved from Cura or the app (Sparrow status unchanged).", ""
        bits = []
        if "payload" in fields:
            bits.append("referral form")
        if "operational_event_id" in fields:
            bits.append("operational event link")
        if "sync_status" in fields:
            bits.append("sync status")
        if "sync_error" in fields:
            bits.append("sync error")
        primary = ("Updated: " + ", ".join(bits)) if bits else "Record updated."
        if fs and ts and fs != ts:
            return primary, f"Status: {fs} → {ts}"
        return primary, ""

    if act == "submit" and fs != ts:
        return f"Status: {fs} → {ts}", ""
    if act == "close" and fs != ts:
        return f"Status: {fs} → {ts}", ""
    if act == "status_change" and fs != ts:
        return f"Status: {fs} → {ts}", ""

    if act in ("submit", "close", "status_change"):
        return "Status updated.", ""

    return "", ""


def _sg_manager_payload_dict(payload_json) -> dict:
    if isinstance(payload_json, dict):
        return payload_json
    if payload_json is None:
        return {}
    try:
        out = json.loads(payload_json) if isinstance(payload_json, str) else {}
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}


def _sg_payload_created_by_fallback(payload: dict) -> str:
    """When ``cura_safeguarding_referrals.created_by`` is empty, some clients only send actor in JSON."""
    if not isinstance(payload, dict):
        return ""
    keys = (
        "createdBy",
        "created_by",
        "authorUsername",
        "author_username",
        "submittedBy",
        "submitted_by",
        "lastEditedBy",
        "last_edited_by",
    )
    for k in keys:
        v = payload.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    for nest_key in ("form", "data", "referral", "safeguarding", "meta", "metadata"):
        nest = payload.get(nest_key)
        if isinstance(nest, dict):
            for k in keys:
                v = nest.get(k)
                if v is not None and str(v).strip():
                    return str(v).strip()
    return ""


_SG_CASE_ID_KEYS = (
    "caseId",
    "case_id",
    "epcrCaseId",
    "epcr_case_id",
    "clinicalCaseId",
    "clinical_case_id",
    "patientCaseId",
    "patient_case_id",
)

_SG_URGENCY_KEYS_PRIMARY = (
    "urgency",
    "urgencyLevel",
    "urgency_level",
    "urgency_of_referral",
    "urgencyOfReferral",
    "referralUrgency",
    "referral_urgency",
    "safeguardingUrgency",
    "safeguarding_urgency",
    "responseTime",
    "response_time",
    "timeframe",
    "time_frame",
)

_SG_URGENCY_KEYS_SOFT = ("category", "type", "riskLevel", "risk_level", "priority", "summary")


def _sg_coerce_urgency_text(raw) -> str:
    """Normalise Cura UI values (string, enum object, or short list) to plain text for matching."""
    if raw is None or isinstance(raw, bool):
        return ""
    if isinstance(raw, (int, float)):
        return str(raw).strip()
    if isinstance(raw, dict):
        for k in ("label", "name", "title", "text", "description", "value", "id", "key", "code"):
            v = raw.get(k)
            if v is None or isinstance(v, (dict, list)):
                continue
            if str(v).strip():
                return str(v).strip()
        if len(raw) == 1:
            v = next(iter(raw.values()))
            if v is not None and not isinstance(v, (dict, list)) and str(v).strip():
                return str(v).strip()
        return ""
    if isinstance(raw, (list, tuple)):
        for item in raw:
            t = _sg_coerce_urgency_text(item)
            if t:
                return t
        return ""
    return str(raw).strip()


def _sg_value_looks_like_urgency_only(val) -> bool:
    """True when a field is only an urgency-of-referral tier (not narrative / not adult-child category)."""
    txt = _sg_coerce_urgency_text(val) if isinstance(val, (dict, list)) else (str(val).strip() if val is not None else "")
    s = re.sub(r"\s+", " ", (txt or "").strip().lower())
    if not s or len(s) > 56:
        return False
    if "adult" in s or "child" in s:
        return False
    if s in ("immediate", "urgent", "standard"):
        return True
    if "immediate" in s and len(s) < 40:
        return True
    if "standard" in s and ("72" in s or "hour" in s or len(s) < 28):
        return True
    if "urgent" in s and ("24" in s or "hour" in s or len(s) <= 20):
        return True
    return False


def _sg_first_urgency_hit_in_dict(d: dict) -> str | None:
    if not isinstance(d, dict):
        return None
    for k in _SG_URGENCY_KEYS_PRIMARY:
        if k not in d:
            continue
        t = _sg_coerce_urgency_text(d.get(k))
        if t:
            return t
    for k in _SG_URGENCY_KEYS_SOFT:
        if k not in d:
            continue
        v = d.get(k)
        if _sg_value_looks_like_urgency_only(v):
            t = _sg_coerce_urgency_text(v)
            if t:
                return t
    return None


def _sg_scan_payload_for_urgency_text(payload: dict) -> str:
    """Find urgency-of-referral text from the safeguarding payload (top-level and nested)."""
    if not isinstance(payload, dict):
        return ""
    hit = _sg_first_urgency_hit_in_dict(payload)
    if hit:
        return hit
    for nest_key in ("form", "data", "referral", "safeguarding", "submission", "epcr", "context"):
        nest = payload.get(nest_key)
        if isinstance(nest, dict):
            hit = _sg_first_urgency_hit_in_dict(nest)
            if hit:
                return hit
    return ""


def _sg_case_id_from_dict(d: dict) -> str | None:
    if not isinstance(d, dict):
        return None
    for k in _SG_CASE_ID_KEYS:
        v = d.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def _sg_manager_payload_case_id(payload: dict) -> str | None:
    """Best-effort link back to EPCR — Cura may store case id under several keys."""
    if not isinstance(payload, dict):
        return None
    hit = _sg_case_id_from_dict(payload)
    if hit:
        return hit
    for nest_key in ("meta", "metadata", "source", "link", "epcr", "form", "data", "referral", "safeguarding", "context"):
        nest = payload.get(nest_key)
        if isinstance(nest, dict):
            hit = _sg_case_id_from_dict(nest)
            if hit:
                return hit
            for inner_key in ("case", "epcr", "link", "source"):
                inner = nest.get(inner_key)
                if isinstance(inner, dict):
                    hit = _sg_case_id_from_dict(inner)
                    if hit:
                        return hit
    return None


def _sg_manager_payload_summary(payload: dict) -> str:
    """One-line summary for list views (avoid dumping entire JSON)."""
    if not isinstance(payload, dict):
        return ""

    def clip(s: str) -> str:
        if len(s) > 180:
            return s[:180] + "…"
        return s

    def take_meaningful(val) -> str | None:
        if val is None:
            return None
        s = str(val).strip() if not isinstance(val, str) else val.strip()
        if not s or _sg_value_looks_like_urgency_only(s):
            return None
        return clip(s)

    short_keys = (
        "title",
        "concernSummary",
        "concern_summary",
        "concernType",
        "concern_type",
        "category",
        "type",
        "riskLevel",
        "risk_level",
    )
    long_keys = ("concerns", "narrative", "details", "summary", "notes", "description")

    def scan(d: dict) -> str | None:
        for k in short_keys:
            v = d.get(k)
            if v is None:
                continue
            if isinstance(v, str) and v.strip():
                got = take_meaningful(v)
                if got:
                    return got
            elif isinstance(v, (dict, list)):
                coerced = _sg_coerce_urgency_text(v) if k in ("category", "type", "riskLevel", "risk_level") else None
                if coerced and not _sg_value_looks_like_urgency_only(coerced):
                    got = take_meaningful(coerced)
                    if got:
                        return got
            else:
                got = take_meaningful(str(v).strip())
                if got:
                    return got
        for k in long_keys:
            v = d.get(k)
            if isinstance(v, str) and v.strip():
                got = take_meaningful(v.replace("\n", " "))
                if got:
                    return got
        return None

    got = scan(payload)
    if got:
        return got
    for nest_key in ("form", "data", "referral", "safeguarding"):
        nest = payload.get(nest_key)
        if isinstance(nest, dict):
            got = scan(nest)
            if got:
                return got
    return ""


def _sg_time_since_raised(dt) -> str:
    """Elapsed time since the referral was created (raised), for the management table."""
    if dt is None:
        return "—"
    if not isinstance(dt, datetime):
        return "—"
    now = datetime.now(timezone.utc)
    t = dt
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    delta = now - t
    secs = int(delta.total_seconds())
    if secs < 0:
        return "just now"
    if secs < 60:
        return "just now"
    mins = secs // 60
    if mins < 60:
        return "1 minute ago" if mins == 1 else f"{mins} minutes ago"
    hours = mins // 60
    if hours < 24:
        return "1 hour ago" if hours == 1 else f"{hours} hours ago"
    days = hours // 24
    if days < 30:
        return "1 day ago" if days == 1 else f"{days} days ago"
    months = days // 30
    if months < 12:
        return "1 month ago" if months == 1 else f"{months} months ago"
    years = days // 365
    return "1 year ago" if years == 1 else f"{years} years ago"


def _sg_status_label(status: str | None) -> str:
    known = {
        "draft": "Draft",
        "submitted": "Submitted",
        "closed": "Closed",
        "archived": "Archived",
    }
    s = (status or "").strip().lower()
    if s in known:
        return known[s]
    raw = (status or "").strip()
    if not raw:
        return "—"
    return raw.replace("_", " ").title()


def _sg_summary_display(summary: str | None) -> str:
    """Single-sentence style: first letter uppercase, rest lowercase (e.g. urgent → Urgent)."""
    t = (summary or "").strip()
    if not t:
        return "—"
    return t[0].upper() + t[1:].lower() if len(t) > 1 else t.upper()


def _sg_username_display(username: str | None) -> str:
    """Sparrow usernames: show dots/underscores as spaces with light title casing."""
    u = (username or "").strip()
    if not u:
        return "—"
    cleaned = u.replace(".", " ").replace("_", " ")
    parts = [p for p in cleaned.split() if p]
    if not parts:
        return u
    out = []
    for p in parts:
        if len(p) > 1:
            out.append(p[:1].upper() + p[1:].lower())
        else:
            out.append(p.upper())
    return " ".join(out)


def _sg_dt_display_short(dt) -> str:
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M")
    return str(dt) if dt else "—"


def _sg_subject_type_label(subject_type: str | None) -> str:
    s = (subject_type or "").strip()
    if not s:
        return "—"
    return s.replace("_", " ").title()


def _sg_category_label(payload: dict, subject_type_col: str | None) -> str:
    """
    Adult / child (person category) — Cura uses ``personType`` in the safeguarding payload;
    falls back to DB ``subject_type`` when the app only set the column.
    """
    if isinstance(payload, dict):
        for key in ("personType", "person_type", "subjectCategory"):
            v = payload.get(key)
            if v is not None and str(v).strip():
                return _sg_subject_type_label(str(v))
        for nest_key in ("form", "data", "referral", "safeguarding"):
            nest = payload.get(nest_key)
            if isinstance(nest, dict):
                for key in ("personType", "person_type", "subjectCategory"):
                    v = nest.get(key)
                    if v is not None and str(v).strip():
                        return _sg_subject_type_label(str(v))
    return _sg_subject_type_label(subject_type_col)


def _sg_urgency_meta(payload: dict) -> dict:
    """
    Cura ePCR safeguarding urgency (radio group): Immediate (red), Urgent 24h (amber), Standard 72h (green).
    Reads urgency fields and, when unambiguous, soft keys such as ``category`` / ``summary`` that hold only a tier.
    """
    text = _sg_scan_payload_for_urgency_text(payload) if isinstance(payload, dict) else ""
    s = re.sub(r"\s+", " ", text.strip().lower()) if text else ""
    out = {
        "label": "—",
        "title": "",
        "tier": "unknown",
        "badge_class": "badge bg-light text-dark border",
    }
    if not s:
        return out
    if "immediate" in s:
        return {
            "label": "Immediate",
            "title": "Immediate — Person at immediate risk",
            "tier": "immediate",
            "badge_class": "badge sg-safeguard-urgency-immediate",
        }
    if "urgent" in s:
        return {
            "label": "Urgent",
            "title": "Urgent — Within 24 hours",
            "tier": "urgent",
            "badge_class": "badge sg-safeguard-urgency-urgent",
        }
    if "standard" in s or "72 hour" in s or "72-hour" in s or s.endswith("72h"):
        return {
            "label": "Standard",
            "title": "Standard — Within 72 hours",
            "tier": "standard",
            "badge_class": "badge sg-safeguard-urgency-standard",
        }
    disp = str(text).strip() if text else ""
    if len(disp) > 52:
        disp = disp[:51] + "…"
    return {
        "label": disp,
        "title": disp,
        "tier": "unknown",
        "badge_class": "badge bg-light text-dark border",
    }


def _sg_format_abuse_chip_label(raw: str) -> str:
    t = (raw or "").strip()
    if not t:
        return ""
    if "_" in t or (t.islower() and " " not in t and "/" not in t):
        return t.replace("_", " ").replace("-", " ").title()
    return t


def _sg_collect_abuse_from_dict(d: dict) -> tuple[list[str], str]:
    """Parse Cura ``abuseTypes`` multi-select and optional ``abuseTypeOther``."""
    items: list[str] = []
    other = ""
    if not isinstance(d, dict):
        return items, other
    at = d.get("abuseTypes") or d.get("abuse_types") or d.get("abuseType")
    if isinstance(at, list):
        for x in at:
            if x is None:
                continue
            if isinstance(x, dict):
                lab = x.get("label") or x.get("name") or x.get("title") or x.get("value")
                if lab is not None and str(lab).strip():
                    items.append(_sg_format_abuse_chip_label(str(lab)))
            else:
                s = str(x).strip()
                if s:
                    items.append(_sg_format_abuse_chip_label(s))
    elif isinstance(at, str) and at.strip():
        items.append(_sg_format_abuse_chip_label(at))
    elif isinstance(at, dict):
        lab = at.get("label") or at.get("value")
        if lab is not None and str(lab).strip():
            items.append(_sg_format_abuse_chip_label(str(lab)))
    oth = d.get("abuseTypeOther") or d.get("abuse_type_other") or d.get("abuseOther")
    if oth is not None and str(oth).strip():
        other = str(oth).strip()
    return items, other


def _sg_abuse_concerns_display(payload: dict) -> tuple[str, str]:
    """
    Types of abuse/concern from the Cura safeguarding form (multi-select).
    Returns (short cell text, full string for HTML title tooltip).
    """
    if not isinstance(payload, dict):
        return "—", ""
    all_items: list[str] = []
    other_txt = ""
    li, o = _sg_collect_abuse_from_dict(payload)
    all_items.extend(li)
    if o:
        other_txt = o
    if not all_items and not other_txt:
        for nest_key in ("form", "data", "referral", "safeguarding"):
            nest = payload.get(nest_key)
            if isinstance(nest, dict):
                li2, o2 = _sg_collect_abuse_from_dict(nest)
                all_items.extend(li2)
                if o2:
                    other_txt = o2
                if all_items or other_txt:
                    break
    seen: set[str] = set()
    items: list[str] = []
    for x in all_items:
        k = x.casefold()
        if k not in seen:
            seen.add(k)
            items.append(x)
    if other_txt:
        clip = other_txt if len(other_txt) <= 180 else (other_txt[:177] + "…")
        replaced = False
        merged: list[str] = []
        for i in items:
            il = i.casefold()
            if il == "other" or il.startswith("other (") or "other —" in il:
                merged.append(f"Other ({clip})")
                replaced = True
            else:
                merged.append(i)
        if not replaced:
            merged.append(f"Other ({clip})")
        items = merged
    if not items:
        return "—", ""
    full = ", ".join(items)
    if len(full) <= 160:
        return full, full
    return full[:157] + "…", full


def _sg_audit_action_label(action: str | None) -> str:
    a = (action or "").strip().lower()
    if not a:
        return "—"
    pretty = {
        "create": "Created",
        "patch": "Updated",
        "submit": "Submitted",
        "close": "Closed",
        "status_change": "Status change",
        "manager_note": "Case note",
    }
    if a in pretty:
        return pretty[a]
    s = a.replace("_", " ")
    return s[0].upper() + s[1:].lower() if len(s) > 1 else s.upper()


@internal_bp.route("/clinical/safeguarding-manager/unlock", methods=["POST"])
@login_required
def safeguarding_manager_unlock():
    """Re-authenticate with personal PIN before the safeguarding list or a referral detail (see referral GET)."""
    if not _safeguarding_oversight_roles_ok():
        abort(403)
    actor = getattr(current_user, "username", "") or ""
    pin = (request.form.get("personal_pin") or "").strip()
    if not pin:
        flash("Enter your personal PIN.", "warning")
        return redirect(url_for("medical_records_internal.safeguarding_manager_home"))
    ph = getattr(current_user, "personal_pin_hash", None) or ""
    if not str(ph).strip():
        flash(
            "Your account has no personal PIN set. Use the user menu → Personal PIN to set a 6-digit PIN "
            "before opening safeguarding records.",
            "danger",
        )
        return redirect(url_for("medical_records_internal.landing"))
    if not AuthManager.verify_password(ph, pin):
        logger.warning("User %s failed safeguarding oversight personal PIN check.", actor)
        flash("Invalid personal PIN.", "danger")
        back = session.get(SAFEGUARDING_OVERSIGHT_INTENDED_URL_KEY)
        if back and isinstance(back, str) and _sg_oversight_local_url_ok(back):
            return redirect(back)
        return redirect(url_for("medical_records_internal.safeguarding_manager_home"))
    session[SAFEGUARDING_OVERSIGHT_PIN_SESSION_KEY] = (
        datetime.now(timezone.utc).isoformat()
    )
    session[SAFEGUARDING_OVERSIGHT_POST_UNLOCK_KEY] = True
    log_audit(actor, "Safeguarding oversight: verified personal PIN")
    resume = (request.form.get("resume_url") or "").strip()
    if resume and _sg_oversight_local_url_ok(resume):
        target = resume
        session.pop(SAFEGUARDING_OVERSIGHT_INTENDED_URL_KEY, None)
    else:
        target = session.pop(SAFEGUARDING_OVERSIGHT_INTENDED_URL_KEY, None)
        if not target or not isinstance(target, str) or not _sg_oversight_local_url_ok(target):
            target = url_for("medical_records_internal.safeguarding_manager_home")
    return redirect(target)


@internal_bp.route("/clinical/safeguarding-manager", methods=["GET"])
@login_required
def safeguarding_manager_home():
    """
    Oversight list for safeguarding referrals created from Cura / EPCR (and related APIs).
    """
    if not _safeguarding_oversight_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))

    if session.pop(SAFEGUARDING_OVERSIGHT_POST_UNLOCK_KEY, False):
        pass
    elif session.pop(SAFEGUARDING_OVERSIGHT_DETAIL_OPENED_KEY, False):
        session.pop(SAFEGUARDING_OVERSIGHT_PIN_SESSION_KEY, None)
        session.pop(SAFEGUARDING_OVERSIGHT_INTENDED_URL_KEY, None)

    gate = _safeguarding_oversight_pin_gate_response()
    if gate is not None:
        return gate

    status_f = (request.args.get("status") or "").strip().lower() or None
    if status_f and status_f not in ("draft", "submitted", "closed", "archived"):
        status_f = None
    ev_raw = (request.args.get("operational_event_id") or "").strip()
    ev_id = None
    if ev_raw:
        try:
            ev_id = int(ev_raw)
        except ValueError:
            ev_id = None
    q = (request.args.get("q") or "").strip()

    operational_events = []
    rows_out = []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, name FROM cura_operational_events
            ORDER BY id DESC
            LIMIT 200
            """
        )
        operational_events = [
            {"id": ev["id"], "name": ev["name"] or ""} for ev in cur.fetchall() or []
        ]

        where = ["1=1"]
        params: list = []
        if status_f:
            where.append("r.status = %s")
            params.append(status_f)
        if ev_id is not None:
            where.append("r.operational_event_id = %s")
            params.append(ev_id)
        if q:
            like = f"%{q}%"
            where.append(
                "(r.created_by LIKE %s OR r.public_id LIKE %s OR CAST(r.id AS CHAR) = %s "
                "OR r.client_local_id LIKE %s)"
            )
            params.extend([like, like, q.strip(), like])

        sql = f"""
            SELECT r.id, r.public_id, r.status, r.subject_type, r.operational_event_id,
                   r.created_by, r.created_at, r.updated_at, r.payload_json,
                   e.name AS operational_event_name
            FROM cura_safeguarding_referrals r
            LEFT JOIN cura_operational_events e ON e.id = r.operational_event_id
            WHERE {' AND '.join(where)}
            ORDER BY r.created_at DESC
            LIMIT 400
        """
        cur.execute(sql, tuple(params))
        for row in cur.fetchall() or []:
            created_at = row.get("created_at")
            pj = _sg_manager_payload_dict(row.get("payload_json"))
            ca_display = (
                created_at.strftime("%Y-%m-%d %H:%M")
                if isinstance(created_at, datetime)
                else (str(created_at) if created_at else "—")
            )
            sum_raw = _sg_manager_payload_summary(pj)
            updated_at = row.get("updated_at")
            um = _sg_urgency_meta(pj)
            ab_short, ab_title = _sg_abuse_concerns_display(pj)
            cb_raw = (
                (row.get("created_by") or "").strip() or _sg_payload_created_by_fallback(pj)
            )
            ev_name = (row.get("operational_event_name") or "").strip()
            rows_out.append(
                {
                    "id": row["id"],
                    "public_id": row.get("public_id") or "",
                    "status": row.get("status") or "",
                    "status_label": _sg_status_label(row.get("status")),
                    "subject_type": row.get("subject_type") or "",
                    "category_label": _sg_category_label(
                        pj, row.get("subject_type")
                    ),
                    "operational_event_id": row.get("operational_event_id"),
                    "event_name": ev_name,
                    "created_by": cb_raw,
                    "created_by_display": _sg_username_display(cb_raw),
                    "created_at": created_at,
                    "created_at_display": ca_display,
                    "raised_ago": _sg_time_since_raised(created_at),
                    "updated_at": updated_at,
                    "updated_at_display": _sg_dt_display_short(updated_at),
                    "case_id_hint": _sg_manager_payload_case_id(pj),
                    "summary": sum_raw,
                    "summary_display": _sg_summary_display(sum_raw),
                    "abuse_concerns_display": ab_short,
                    "abuse_concerns_title": ab_title,
                    "urgency_label": um["label"],
                    "urgency_title": um["title"],
                    "urgency_tier": um["tier"],
                    "urgency_badge_class": um["badge_class"],
                }
            )
    except Exception as ex:
        if "cura_safeguarding_referrals" in str(ex) or "Unknown table" in str(ex):
            flash(
                "Safeguarding referrals are not available (database migration may be required).",
                "warning",
            )
        else:
            logger.exception("safeguarding_manager_home: %s", ex)
            flash("Could not load safeguarding referrals.", "danger")
    finally:
        cur.close()
        conn.close()

    log_audit(
        getattr(current_user, "username", "") or "",
        "Opened safeguarding manager list",
    )
    return render_template(
        "clinical/safeguarding_manager.html",
        config=core_manifest,
        referrals=rows_out,
        operational_events=operational_events,
        filters={
            "status": status_f or "",
            "operational_event_id": ev_raw,
            "q": q,
        },
    )


@internal_bp.route(
    "/clinical/safeguarding-manager/referral/<int:referral_id>",
    methods=["GET", "POST"],
)
@login_required
def safeguarding_manager_referral(referral_id):
    if not _safeguarding_oversight_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))

    if request.method == "GET":
        # List PIN does not carry to referral detail: each referral view needs its own PIN, unless this
        # GET immediately follows unlock (post_unlock) or a form POST redirect on the same referral.
        post_unlock = session.pop(SAFEGUARDING_OVERSIGHT_POST_UNLOCK_KEY, None)
        if not post_unlock:
            session.pop(SAFEGUARDING_OVERSIGHT_PIN_SESSION_KEY, None)
            session.pop(SAFEGUARDING_OVERSIGHT_INTENDED_URL_KEY, None)
        gate = _safeguarding_oversight_pin_gate_response()
        if gate is not None:
            return gate
    else:
        if not _safeguarding_oversight_pin_fresh():
            flash("Your safeguarding session expired. Enter your personal PIN again.", "warning")
            return render_template(
                "clinical/safeguarding_manager_gate.html",
                config=core_manifest,
                resume_after_unlock=request.full_path,
            )

    from .safeguarding_auth import SafeguardingAuditError, insert_safeguarding_audit_event

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT r.id, r.public_id, r.client_local_id, r.operational_event_id, r.status, r.subject_type,
                   r.record_version, r.payload_json, r.sync_status, r.sync_error, r.created_by, r.updated_by,
                   r.created_at, r.updated_at, e.name AS operational_event_name
            FROM cura_safeguarding_referrals r
            LEFT JOIN cura_operational_events e ON e.id = r.operational_event_id
            WHERE r.id = %s
            """,
            (referral_id,),
        )
        row = cur.fetchone()
        if not row:
            abort(404)

        if request.method == "POST":
            actor = getattr(current_user, "username", "") or ""
            redir = (
                url_for(
                    "medical_records_internal.safeguarding_manager_referral",
                    referral_id=referral_id,
                )
                + "#section-safeguard-manage"
            )
            if request.form.get("save_status_update") is not None:
                new_st = (request.form.get("new_status") or "").strip().lower()
                comment = (request.form.get("status_comment") or "").strip()
                allowed = ("draft", "submitted", "closed", "archived")
                old_st = (row.get("status") or "").strip().lower()
                if new_st not in allowed:
                    flash("That status is not valid.", "danger")
                elif new_st == "closed" and new_st != old_st and len(comment) < 8:
                    flash(
                        "Closing a referral requires a short outcome summary (for example what was done, "
                        "who holds the case, or why it is closed).",
                        "warning",
                    )
                else:
                    try:
                        exp_ver = int(request.form.get("record_version") or 0)
                    except (TypeError, ValueError):
                        exp_ver = -1
                    if exp_ver != int(row.get("record_version") or 0):
                        flash(
                            "This referral was changed elsewhere. Refresh the page and try again.",
                            "danger",
                        )
                    elif new_st == old_st:
                        flash(
                            "Status is already “"
                            + _sg_status_label(old_st)
                            + "”. Use “Add case note” for updates without changing status.",
                            "info",
                        )
                    else:
                        if new_st == "submitted":
                            audit_action = "submit"
                        elif new_st == "closed":
                            audit_action = "close"
                        else:
                            audit_action = "status_change"
                        audit_detail = {
                            "from_status": old_st or "",
                            "to_status": new_st,
                            "fields": ["status"],
                        }
                        if comment:
                            audit_detail["note"] = comment[:4000]
                        try:
                            cur.execute(
                                """
                                UPDATE cura_safeguarding_referrals
                                SET status=%s, updated_by=%s, record_version=record_version+1
                                WHERE id=%s AND record_version=%s
                                """,
                                (new_st, actor, int(referral_id), exp_ver),
                            )
                            if cur.rowcount != 1:
                                conn.rollback()
                                flash(
                                    "Could not update status (record may have changed). Refresh and try again.",
                                    "danger",
                                )
                            else:
                                insert_safeguarding_audit_event(
                                    cur,
                                    int(referral_id),
                                    actor,
                                    audit_action,
                                    audit_detail,
                                    required=True,
                                )
                                conn.commit()
                                log_audit(
                                    actor,
                                    f"Safeguarding manager set status referral_id={referral_id} "
                                    f"{old_st!r}->{new_st!r}",
                                )
                                flash("Status updated.", "success")
                        except SafeguardingAuditError:
                            conn.rollback()
                            flash(
                                "Status was not saved (audit log unavailable). Try again or contact support.",
                                "danger",
                            )
                        except Exception as ex:
                            conn.rollback()
                            logger.exception("safeguarding_manager_referral status POST: %s", ex)
                            flash("Could not update status.", "danger")
            else:
                note = (request.form.get("manager_note") or "").strip()
                if note:
                    try:
                        insert_safeguarding_audit_event(
                            cur,
                            int(referral_id),
                            actor,
                            "manager_note",
                            {"note": note[:8000]},
                            required=True,
                        )
                        conn.commit()
                        log_audit(
                            actor,
                            f"Safeguarding manager note referral_id={referral_id}",
                        )
                        flash("Case note recorded.", "success")
                    except SafeguardingAuditError:
                        conn.rollback()
                        flash(
                            "Could not save the oversight note (audit log unavailable). Try again or contact support.",
                            "danger",
                        )
                    except Exception as ex:
                        conn.rollback()
                        logger.exception("safeguarding_manager_referral POST: %s", ex)
                        flash("Could not save the oversight note.", "danger")
            # Allow the PRG follow-up GET without a second PIN (user was already verified for this POST).
            session[SAFEGUARDING_OVERSIGHT_POST_UNLOCK_KEY] = True
            return redirect(redir)

        cur.execute(
            """
            SELECT id, actor_username, action, detail_json, created_at
            FROM cura_safeguarding_audit_events
            WHERE referral_id = %s
            ORDER BY id DESC
            """,
            (referral_id,),
        )
        audit_rows = cur.fetchall() or []

        payload = _sg_manager_payload_dict(row.get("payload_json"))
        sum_line = _sg_manager_payload_summary(payload)
        um = _sg_urgency_meta(payload)
        ab_short, ab_full = _sg_abuse_concerns_display(payload)
        cb_raw = (row.get("created_by") or "").strip() or _sg_payload_created_by_fallback(
            payload
        )
        referral = {
            "id": row["id"],
            "public_id": row.get("public_id") or "",
            "client_local_id": row.get("client_local_id") or "",
            "operational_event_id": row.get("operational_event_id"),
            "event_name": (row.get("operational_event_name") or "").strip(),
            "status": row.get("status") or "",
            "status_label": _sg_status_label(row.get("status")),
            "subject_type": row.get("subject_type") or "",
            "subject_type_label": _sg_subject_type_label(row.get("subject_type")),
            "category_label": _sg_category_label(payload, row.get("subject_type")),
            "abuse_concerns_display": ab_short,
            "abuse_concerns_full": ab_full,
            "urgency_label": um["label"],
            "urgency_title": um["title"],
            "urgency_tier": um["tier"],
            "urgency_badge_class": um["badge_class"],
            "record_version": row.get("record_version"),
            "sync_status": row.get("sync_status"),
            "sync_error": row.get("sync_error"),
            "created_by": cb_raw,
            "created_by_display": _sg_username_display(cb_raw),
            "updated_by": row.get("updated_by") or "",
            "updated_by_display": _sg_username_display(row.get("updated_by")),
            "created_at": row.get("created_at"),
            "created_at_display": _sg_dt_display_short(row.get("created_at")),
            "updated_at": row.get("updated_at"),
            "updated_at_display": _sg_dt_display_short(row.get("updated_at")),
            "case_id_hint": _sg_manager_payload_case_id(payload),
            "payload": payload,
            "payload_rows": _sg_payload_display_rows(payload),
            "summary_line": _sg_summary_display(sum_line),
        }
        audit_events = []
        for a in audit_rows:
            dv = (
                _sg_manager_payload_dict(a.get("detail_json"))
                if a.get("detail_json")
                else {}
            )
            pr, sec = _sg_audit_trail_primary_secondary(a.get("action"), dv)
            audit_events.append(
                {
                    "id": a["id"],
                    "actor_username": a.get("actor_username") or "",
                    "actor_display": _sg_username_display(a.get("actor_username")),
                    "action": a.get("action") or "",
                    "action_label": _sg_audit_action_label(a.get("action")),
                    "detail": dv,
                    "detail_primary": pr,
                    "detail_secondary": sec,
                    "created_at": a.get("created_at"),
                    "created_at_display": _sg_dt_display_short(a.get("created_at")),
                }
            )
    except Exception as ex:
        if "cura_safeguarding_referrals" in str(ex) or "Unknown table" in str(ex):
            flash("Safeguarding data is not available (database migration may be required).", "warning")
            return redirect(url_for("medical_records_internal.safeguarding_manager_home"))
        raise
    finally:
        cur.close()
        conn.close()

    log_audit(
        getattr(current_user, "username", "") or "",
        f"Opened safeguarding manager referral detail id={referral_id}",
    )
    session[SAFEGUARDING_OVERSIGHT_DETAIL_OPENED_KEY] = True
    return render_template(
        "clinical/safeguarding_manager_referral.html",
        config=core_manifest,
        referral=referral,
        audit_events=audit_events,
    )


@internal_bp.route('/prescription', methods=['POST'])
@login_required
def add_prescription():
    """
    Adds a prescription record.
    Allowed roles: "clinical_lead", "superuser".
    Expects JSON with keys: 'patient_id', 'prescribed_by', and 'prescription'.
    """
    if not hasattr(current_user, 'role') or current_user.role.lower() not in ["clinical_lead", "superuser"]:
        return jsonify({"error": "Unauthorised access"}), 403
    data = request.get_json()
    required_fields = ['patient_id', 'prescribed_by', 'prescription']
    if not data or any(field not in data for field in required_fields):
        logger.error("Failed to add prescription: Missing required fields.")
        return jsonify({"error": "Missing required prescription fields"}), 400
    try:
        patient_id = int(data['patient_id'])
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid patient_id"}), 400
    try:
        Prescription.insert_prescription(patient_id, data['prescribed_by'], data['prescription'])
    except Exception as e:
        logger.error("Error inserting prescription: %s", str(e))
        return jsonify({"error": "Error inserting prescription"}), 500
    AuditLog.insert_log(current_user.username, "Added prescription", patient_id=patient_id)
    return jsonify({"message": f"Prescription added successfully for patient_id {patient_id}."}), 201

def get_assigned_patient_count(care_company_user_id):
    """Legacy ``patients`` table is not used in this deployment; no DB call."""
    return 0

def _cura_ops_roles_ok():
    r = (getattr(current_user, "role", "") or "").lower()
    return r in ("clinical_lead", "superuser", "admin", "support_break_glass")


def _cura_ops_event_management_ok():
    """
    Operational period documents (Railway-backed uploads under app/static/uploads), bulletins,
    and full anonymous reporting. Admins, standard Cura ops hub roles, or explicit permission.
    """
    if not getattr(current_user, "is_authenticated", False):
        return False
    r = (getattr(current_user, "role", "") or "").lower()
    if r in ("admin", "superuser", "support_break_glass"):
        return True
    if _cura_ops_roles_ok():
        return True
    try:
        from app.objects import has_permission

        return has_permission("medical_records_module.cura_ops_event_management")
    except Exception:
        return False


_CURA_OER_KIND_FLEET = "fleet_vehicle"
_CURA_OER_KIND_EQUIPMENT = "equipment_asset"


def _cura_enrich_operational_event_resources(raw_rows):
    """Attach display strings and admin deep links for fleet + serial equipment rows."""
    fleet = None
    try:
        from app.plugins.fleet_management.objects import get_fleet_service

        fleet = get_fleet_service()
    except Exception:
        pass
    assets = None
    try:
        from app.plugins.inventory_control.asset_service import get_asset_service

        assets = get_asset_service()
    except Exception:
        pass
    out = []
    for r in raw_rows:
        rid_kind = r["resource_kind"]
        pk = int(r["resource_id"])
        row = dict(r)
        row["admin_url"] = None
        if rid_kind == _CURA_OER_KIND_FLEET:
            row["type_label"] = "Vehicle"
            fv = fleet.get_vehicle(pk) if fleet else None
            reg = (fv or {}).get("registration") or ""
            ic = (fv or {}).get("internal_code") or ""
            row["summary"] = (reg or ic or f"#{pk}").strip()
            bits = []
            if ic and str(ic) != str(reg):
                bits.append(str(ic))
            if fv and fv.get("vehicle_type_name"):
                bits.append(str(fv["vehicle_type_name"]))
            row["detail"] = " · ".join(bits) if bits else ""
            if fv:
                try:
                    row["admin_url"] = url_for(
                        "fleet_management.vehicle_detail", vehicle_id=pk
                    )
                except Exception:
                    row["admin_url"] = None
        else:
            row["type_label"] = "Serial equipment"
            ar = assets.get_asset_detail(pk) if assets else None
            name = (ar or {}).get("item_name") or "Equipment"
            row["summary"] = name
            parts = [
                x
                for x in [
                    (ar or {}).get("serial_number") or "",
                    (ar or {}).get("public_asset_code") or "",
                    (ar or {}).get("sku") or "",
                ]
                if x
            ]
            row["detail"] = " · ".join(parts)
            st = (ar or {}).get("status") or ""
            if st:
                row["detail"] = (
                    (row["detail"] + " · ") if row["detail"] else ""
                ) + str(st)
            if ar:
                try:
                    row["admin_url"] = url_for(
                        "inventory_control_internal.equipment_asset_detail", asset_id=pk
                    )
                except Exception:
                    row["admin_url"] = None
        out.append(row)
    return out


def _mr_medical_plugin_static_dir() -> str:
    """…/app/static — same layout as HR uploads; Railway volume symlinks app/static/uploads."""
    app_pkg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return os.path.join(app_pkg_dir, "static")


def _cura_ops_event_asset_upload_dir(event_id: int) -> str:
    base = os.path.join(_mr_medical_plugin_static_dir(), "uploads", "cura_operational_events", str(int(event_id)))
    os.makedirs(base, exist_ok=True)
    return base


def _cura_ops_resolve_asset_file(storage_path: str):
    """Map DB relative path (uploads/cura_operational_events/...) to absolute file under static."""
    rel = (storage_path or "").replace("\\", "/").strip().lstrip("/")
    if not rel or ".." in rel.split("/"):
        return None
    if not rel.startswith("uploads/cura_operational_events/"):
        return None
    static_root = os.path.abspath(_mr_medical_plugin_static_dir())
    candidate = os.path.abspath(os.path.join(static_root, *rel.split("/")))
    try:
        if os.path.commonpath([candidate, static_root]) != static_root:
            return None
    except ValueError:
        return None
    return candidate if os.path.isfile(candidate) else None


def _crm_event_plan_id_from_cura_operational_config(raw_cfg):
    """``crm_event_plan_id`` in ``cura_operational_events.config`` (CRM handoff)."""
    if raw_cfg is None:
        return None
    if isinstance(raw_cfg, dict):
        c = raw_cfg
    elif isinstance(raw_cfg, (bytes, bytearray)):
        try:
            c = json.loads(raw_cfg.decode("utf-8", errors="replace"))
        except Exception:
            return None
    elif isinstance(raw_cfg, str):
        try:
            c = json.loads(raw_cfg)
        except Exception:
            return None
    else:
        return None
    if not isinstance(c, dict):
        return None
    v = c.get("crm_event_plan_id")
    if v is None:
        return None
    try:
        pid = int(v)
        return pid if pid > 0 else None
    except (TypeError, ValueError):
        return None


def _cura_debrief_crm_plan_context(cur, operational_event_id: int) -> dict | None:
    """Optional CRM event plan linked via operational period config (for client medical debrief PDF)."""
    try:
        cur.execute(
            "SELECT config FROM cura_operational_events WHERE id = %s LIMIT 1",
            (int(operational_event_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        plan_id = _crm_event_plan_id_from_cura_operational_config(row[0])
        if plan_id is None:
            return None
        cur.execute(
            """
            SELECT id, title, handoff_status, handoff_at, start_datetime, end_datetime
            FROM crm_event_plans
            WHERE id = %s
            LIMIT 1
            """,
            (int(plan_id),),
        )
        pr = cur.fetchone()
        if not pr:
            return None
        ha = pr[3]
        return {
            "id": int(pr[0]),
            "title": (pr[1] or "").strip() or f"Event plan #{pr[0]}",
            "handoff_status": (pr[2] or "").strip() or None,
            "handoff_at": ha.isoformat() if ha and hasattr(ha, "isoformat") else None,
            "start_datetime": pr[4].isoformat()
            if pr[4] and hasattr(pr[4], "isoformat")
            else None,
            "end_datetime": pr[5].isoformat()
            if pr[5] and hasattr(pr[5], "isoformat")
            else None,
        }
    except Exception:
        return None


@internal_bp.route("/clinical/cura-ops", methods=["GET"])
@login_required
def cura_ops_hub():
    """
    Admin / clinical hub for Cura-linked data: operational periods, MI events, safeguarding counts, EPCR link.
    """
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))

    def _mi_linked_operational_id(raw_cfg):
        """Read operational_event_id / operationalEventId from MI event config_json (Cura incident-report link)."""
        if not raw_cfg:
            return None
        try:
            c = json.loads(raw_cfg) if isinstance(raw_cfg, str) else {}
        except Exception:
            return None
        if not isinstance(c, dict):
            return None
        v = c.get("operational_event_id")
        if v is None:
            v = c.get("operationalEventId")
        if v is None:
            return None
        try:
            oid = int(v)
            return oid if oid > 0 else None
        except (TypeError, ValueError):
            return None

    operational_events = []
    mi_events = []
    sg_draft = sg_submitted = sg_closed = 0
    callsign_validation_failures = []

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        try:
            cur.execute(
                """
                SELECT id, name, slug, status, starts_at, ends_at, config
                FROM cura_operational_events
                ORDER BY id DESC
                LIMIT 100
                """
            )
            for row in cur.fetchall():
                slug_s = str(row[2] or "").strip()
                crm_plan_id = _crm_event_plan_id_from_cura_operational_config(row[6])
                crm_plan_edit_url = None
                if crm_plan_id is not None:
                    try:
                        crm_plan_edit_url = url_for(
                            "medical_records_internal.cura_event_planner.event_plan_edit", plan_id=int(crm_plan_id)
                        )
                    except Exception:
                        crm_plan_edit_url = None
                operational_events.append(
                    {
                        "id": row[0],
                        "name": row[1] or "",
                        "slug": slug_s,
                        "status": row[3] or "",
                        "starts_at": row[4],
                        "ends_at": row[5],
                        "crm_plan_id": crm_plan_id,
                        "crm_cura_badge": crm_plan_id is not None
                        or slug_s.startswith("crm-ep-"),
                        "crm_plan_edit_url": crm_plan_edit_url,
                    }
                )
        except Exception as e:
            logger.warning("cura_ops_hub operational_events: %s", e)

        # Phase C — per-event rollups for ops hub (EPCR cases tag op period in case JSON; PCR/SG use FK columns).
        epcr_by_event = {}
        pcr_by_event = {}
        sg_by_event = {}
        assign_by_event = {}
        try:
            cur.execute(
                """
                SELECT
                  COALESCE(
                    JSON_UNQUOTE(JSON_EXTRACT(data, '$.operational_event_id')),
                    JSON_UNQUOTE(JSON_EXTRACT(data, '$.operationalEventId'))
                  ) AS ev_key,
                  COUNT(*) AS cnt
                FROM cases
                GROUP BY ev_key
                HAVING ev_key IS NOT NULL AND ev_key <> '' AND ev_key <> 'null'
                """
            )
            for ev_key, cnt in cur.fetchall() or []:
                try:
                    eid = int(str(ev_key).strip())
                except (TypeError, ValueError):
                    continue
                epcr_by_event[eid] = int(cnt)
        except Exception as e:
            logger.warning("cura_ops_hub epcr_by_event: %s", e)

        try:
            cur.execute(
                """
                SELECT operational_event_id, COUNT(*)
                FROM cura_patient_contact_reports
                WHERE operational_event_id IS NOT NULL
                GROUP BY operational_event_id
                """
            )
            for eid, cnt in cur.fetchall() or []:
                if eid is not None:
                    pcr_by_event[int(eid)] = int(cnt)
        except Exception as e:
            logger.warning("cura_ops_hub pcr_by_event: %s", e)

        try:
            cur.execute(
                """
                SELECT operational_event_id, COUNT(*)
                FROM cura_safeguarding_referrals
                WHERE operational_event_id IS NOT NULL
                GROUP BY operational_event_id
                """
            )
            for eid, cnt in cur.fetchall() or []:
                if eid is not None:
                    sg_by_event[int(eid)] = int(cnt)
        except Exception as e:
            logger.warning("cura_ops_hub sg_by_event: %s", e)

        try:
            cur.execute(
                """
                SELECT operational_event_id, COUNT(*)
                FROM cura_operational_event_assignments
                GROUP BY operational_event_id
                """
            )
            for eid, cnt in cur.fetchall() or []:
                if eid is not None:
                    assign_by_event[int(eid)] = int(cnt)
        except Exception as e:
            logger.warning("cura_ops_hub assign_by_event: %s", e)

        for r in operational_events:
            eid = r["id"]
            r["kpi_epcr_cases"] = epcr_by_event.get(eid, 0)
            r["kpi_patient_contact"] = pcr_by_event.get(eid, 0)
            r["kpi_safeguarding"] = sg_by_event.get(eid, 0)
            r["kpi_assignments"] = assign_by_event.get(eid, 0)

        try:
            cur.execute(
                """
                SELECT id, name, location_summary, status, starts_at, ends_at, config_json
                FROM cura_mi_events
                ORDER BY id DESC
                LIMIT 100
                """
            )
            op_name_by_id = {r["id"]: (r["name"] or "") for r in operational_events}
            for row in cur.fetchall():
                oid = _mi_linked_operational_id(row[6])
                mi_events.append(
                    {
                        "id": row[0],
                        "name": row[1] or "",
                        "location_summary": row[2] or "",
                        "status": row[3] or "",
                        "starts_at": row[4],
                        "ends_at": row[5],
                        "operational_event_id": oid,
                        "operational_event_name": op_name_by_id.get(oid, "") if oid is not None else "",
                    }
                )
        except Exception as e:
            logger.warning("cura_ops_hub mi_events: %s", e)

        mi_count_by_op: dict[int, int] = {}
        for me in mi_events:
            oid = me.get("operational_event_id")
            if oid is not None:
                mi_count_by_op[int(oid)] = mi_count_by_op.get(int(oid), 0) + 1
        for r in operational_events:
            r["kpi_mi_events"] = mi_count_by_op.get(r["id"], 0)

        try:
            cur.execute(
                "SELECT status, COUNT(*) FROM cura_safeguarding_referrals GROUP BY status"
            )
            for st, c in cur.fetchall():
                s = (st or "").lower()
                if s == "draft":
                    sg_draft = int(c)
                elif s == "submitted":
                    sg_submitted = int(c)
                elif s == "closed":
                    sg_closed = int(c)
        except Exception as e:
            logger.warning("cura_ops_hub safeguarding counts: %s", e)

        try:
            cur.execute(
                """
                SELECT l.id, l.operational_event_id, l.username, l.callsign, l.reason_code, l.created_at, e.name
                FROM cura_callsign_mdt_validation_log l
                LEFT JOIN cura_operational_events e ON e.id = l.operational_event_id
                WHERE l.ok = 0
                ORDER BY l.id DESC
                LIMIT 30
                """
            )
            for row in cur.fetchall() or []:
                callsign_validation_failures.append(
                    {
                        "id": row[0],
                        "operational_event_id": row[1],
                        "username": row[2] or "",
                        "callsign": row[3] or "",
                        "reason_code": row[4] or "",
                        "created_at": row[5],
                        "event_name": row[6] or "",
                    }
                )
        except Exception as e:
            logger.warning("cura_ops_hub callsign_validation_failures: %s", e)

        hub_yoy = {}
        event_dashboards = {}
        try:
            from .cura_ops_reporting import (
                build_deep_analytics_for_operational_event,
                hub_yoy_paperwork_counts,
                merge_pcr_sg_sql_counts,
                mi_anonymous_stats_for_operational_event,
            )

            hub_yoy = hub_yoy_paperwork_counts(cur)
            for r in operational_events:
                eid = int(r["id"])
                try:
                    event_dashboards[eid] = {
                        "epcr": build_deep_analytics_for_operational_event(
                            cur, eid, scan_limit=600
                        ),
                        "mi": mi_anonymous_stats_for_operational_event(cur, eid),
                        "pcr_sg": merge_pcr_sg_sql_counts(cur, eid, 0),
                    }
                except Exception as dex:
                    logger.warning("cura_ops_hub event_dashboard id=%s: %s", eid, dex)
                    event_dashboards[eid] = {"error": str(dex)[:200]}
        except Exception as hub_ex:
            logger.warning("cura_ops_hub dashboard bundle: %s", hub_ex)
    finally:
        cur.close()
        conn.close()

    log_audit(current_user.username, "Opened Cura event manager")
    return render_template(
        "clinical/cura_ops_event_manager.html",
        config=core_manifest,
        operational_events=operational_events,
        mi_events=mi_events,
        sg_draft=sg_draft,
        sg_submitted=sg_submitted,
        sg_closed=sg_closed,
        callsign_validation_failures=callsign_validation_failures,
        event_management_ok=_cura_ops_event_management_ok(),
        hub_yoy=hub_yoy,
        event_dashboards=event_dashboards,
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-periods/create",
    methods=["POST"],
)
@login_required
def cura_ops_operational_period_create():
    """Create a Cura/Ventus event record and register its time-scoped dispatch division (same as Ensure on the detail page)."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    name = (request.form.get("name") or "").strip()
    if not name:
        flash("Event name is required.", "warning")
        return redirect(url_for("medical_records_internal.cura_ops_hub"))
    slug_raw = (request.form.get("slug") or "").strip()
    slug = slug_raw or None
    if slug and len(slug) > 96:
        flash("Short code is too long (max 96 characters).", "warning")
        return redirect(url_for("medical_records_internal.cura_ops_hub"))
    location_summary = (request.form.get("location_summary") or "").strip() or None
    status = (request.form.get("status") or "draft").strip().lower() or "draft"
    if status not in ("draft", "active", "closed", "archived"):
        status = "draft"
    starts_at = (request.form.get("starts_at") or "").strip() or None
    ends_at = (request.form.get("ends_at") or "").strip() or None
    actor = getattr(current_user, "username", "") or ""
    if not starts_at or not ends_at:
        flash(
            "Start and end date/time are required: they set when the dispatch division appears in Cura and Ventus sign-on lists.",
            "warning",
        )
        return redirect(url_for("medical_records_internal.cura_ops_hub"))

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO cura_operational_events
              (name, slug, location_summary, starts_at, ends_at, status, created_by, updated_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (name[:255], slug, location_summary, starts_at or None, ends_at or None, status, actor, actor),
        )
        new_id = cur.lastrowid
        if new_id:
            try:
                from . import cura_event_debrief as _ced_mi

                _ced_mi.ensure_mi_event_for_operational_period(
                    cur,
                    int(new_id),
                    name=name,
                    location_summary=location_summary,
                    starts_at=starts_at or None,
                    ends_at=ends_at or None,
                    operational_status=status,
                    actor=actor,
                )
            except Exception as ex:
                logger.warning("cura_ops create: ensure MI event: %s", ex)
        division_note = ""
        if new_id:
            try:
                from .cura_event_ventus_bridge import provision_operational_event_dispatch_division

                prov = provision_operational_event_dispatch_division(
                    cur, conn, int(new_id), actor, do_commit=False
                )
                dname = (prov.get("name") or name or "").strip()
                if prov.get("ok") and not prov.get("already"):
                    division_note = (
                        f" Dispatch division “{dname[:120]}” is registered for Cura and Ventus and will only "
                        f"appear in their division lists between the start and end you set."
                    )
                elif prov.get("ok") and prov.get("already"):
                    division_note = f" Dispatch division “{dname[:120]}” was already linked; schedule refreshed if applicable."
                elif not prov.get("ok"):
                    logger.warning("cura_ops create: event division not provisioned: %s", prov)
                    division_note = (
                        " Dispatch division could not be registered automatically — open the event and use "
                        "“Re-register division” if MDT/Cura do not show it."
                    )
            except Exception as ex:
                logger.warning("cura_ops create: provision event division: %s", ex)
                division_note = (
                    " Dispatch division registration hit an error — open the event and use “Re-register division” if needed."
                )
            try:
                from .cura_event_inventory_bridge import provision_and_link_event_kit_pool

                provision_and_link_event_kit_pool(
                    cur, conn, int(new_id), name, actor
                )
            except Exception as ex:
                logger.warning("cura_ops create: event kit pool: %s", ex)
        conn.commit()
        flash(
            f"Event “{name[:80]}” saved.{division_note} Open Manage to adjust roster, resources, and reports.",
            "success",
        )
        log_audit(actor, f"Cura ops created operational_event id={new_id} name={name[:80]}")
        return redirect(
            url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=new_id)
        )
    except Exception as e:
        conn.rollback()
        if "Duplicate" in str(e) or "1062" in str(e):
            flash("That short code (slug) is already in use — choose another or leave it blank.", "warning")
        else:
            logger.exception("cura_ops_operational_period_create: %s", e)
            flash("Could not create the event (check database upgrade).", "danger")
        return redirect(url_for("medical_records_internal.cura_ops_hub"))
    finally:
        cur.close()
        conn.close()


@internal_bp.route("/clinical/cura-ops/mi-form-templates", methods=["GET", "POST"])
@login_required
def cura_ops_mi_form_templates():
    """Create and list Cura minor-injury add-on form definitions (Ventus-style JSON schema)."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    from . import cura_mi_custom_forms as micf

    conn = get_db_connection()
    cur = conn.cursor()
    templates = []
    try:
        cur.execute(
            """
            SELECT id, slug, name, description, schema_json, is_active,
                   created_by, updated_by, created_at, updated_at
            FROM cura_mi_form_templates
            ORDER BY id DESC
            LIMIT 500
            """
        )
        templates = [micf.row_to_template_item(r) for r in cur.fetchall() or []]
    except Exception as e:
        if "cura_mi_form_templates" not in str(e) and "Unknown table" not in str(e):
            logger.exception("cura_ops_mi_form_templates list: %s", e)
        templates = []
        flash(
            "Minor injury form templates require database migration 018 (mi custom forms). Run medical_records install upgrade.",
            "warning",
        )
    finally:
        cur.close()
        conn.close()

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        slug = micf.normalize_slug(request.form.get("slug"))
        desc = (request.form.get("description") or "").strip() or None
        raw_schema = (request.form.get("schema_json") or "").strip() or "{}"
        schema_dict, s_err = micf.parse_schema_blob(raw_schema)
        if not name:
            flash("Name is required.", "warning")
        elif not slug:
            flash("Short code (slug) must be lowercase letters, digits, underscores, or hyphens.", "warning")
        elif s_err:
            flash(f"Schema: {s_err}", "warning")
        else:
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    INSERT INTO cura_mi_form_templates
                      (slug, name, description, schema_json, is_active, created_by, updated_by)
                    VALUES (%s, %s, %s, %s, 1, %s, %s)
                    """,
                    (slug, name[:255], desc, json.dumps(schema_dict), actor, actor),
                )
                conn.commit()
                log_audit(actor, f"Cura ops MI form template created slug={slug}")
                flash(f"Form “{name[:80]}” saved. Assign it to MI events from an operational period’s Minor injury row.", "success")
                return redirect(url_for("medical_records_internal.cura_ops_mi_form_templates"))
            except Exception as e:
                conn.rollback()
                if "Duplicate" in str(e) or "1062" in str(e):
                    flash("That slug is already in use.", "warning")
                else:
                    logger.exception("cura_ops_mi_form_templates create: %s", e)
                    flash("Could not save the form (check DB upgrade).", "danger")
            finally:
                cur.close()
                conn.close()

    log_audit(actor, "Opened Cura MI form templates")
    return render_template(
        "clinical/cura_ops_mi_form_templates.html",
        config=core_manifest,
        templates=templates,
    )


@internal_bp.route(
    "/clinical/cura-ops/mi-form-templates/<int:template_id>/edit",
    methods=["GET", "POST"],
)
@login_required
def cura_ops_mi_form_template_edit(template_id):
    """Edit an MI add-on form template (name, slug, schema, active)."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    from . import cura_mi_custom_forms as micf

    conn = get_db_connection()
    cur = conn.cursor()
    tpl = None
    try:
        cur.execute(
            """
            SELECT id, slug, name, description, schema_json, is_active,
                   created_by, updated_by, created_at, updated_at
            FROM cura_mi_form_templates WHERE id = %s
            """,
            (int(template_id),),
        )
        row = cur.fetchone()
        if row:
            tpl = micf.row_to_template_item(row)
    except Exception as e:
        logger.exception("cura_ops_mi_form_template_edit load: %s", e)
        flash("Could not load template.", "danger")
    finally:
        cur.close()
        conn.close()

    if not tpl:
        flash("Form template not found.", "warning")
        return redirect(url_for("medical_records_internal.cura_ops_mi_form_templates"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        slug = micf.normalize_slug(request.form.get("slug"))
        desc = (request.form.get("description") or "").strip() or None
        raw_schema = (request.form.get("schema_json") or "").strip() or "{}"
        schema_dict, s_err = micf.parse_schema_blob(raw_schema)
        is_active = 1 if request.form.get("is_active") else 0
        if not name:
            flash("Name is required.", "warning")
        elif not slug:
            flash("Short code (slug) must be lowercase letters, digits, underscores, or hyphens.", "warning")
        elif s_err:
            flash(f"Schema: {s_err}", "warning")
        else:
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    UPDATE cura_mi_form_templates
                    SET slug=%s, name=%s, description=%s, schema_json=%s, is_active=%s, updated_by=%s
                    WHERE id=%s
                    """,
                    (slug, name[:255], desc, json.dumps(schema_dict), is_active, actor, int(template_id)),
                )
                conn.commit()
                log_audit(actor, f"Cura ops MI form template updated id={template_id} slug={slug}")
                flash("Template saved.", "success")
                return redirect(url_for("medical_records_internal.cura_ops_mi_form_templates"))
            except Exception as e:
                conn.rollback()
                if "Duplicate" in str(e) or "1062" in str(e):
                    flash("That slug is already in use.", "warning")
                else:
                    logger.exception("cura_ops_mi_form_template_edit save: %s", e)
                    flash("Could not save template.", "danger")
            finally:
                cur.close()
                conn.close()

    log_audit(actor, f"Opened MI form template edit id={template_id}")
    return render_template(
        "clinical/cura_ops_mi_form_template_edit.html",
        config=core_manifest,
        tpl=tpl,
    )


@internal_bp.route(
    "/clinical/cura-ops/mi-form-templates/<int:template_id>/delete",
    methods=["POST"],
)
@login_required
def cura_ops_mi_form_template_delete(template_id):
    """Remove an MI form template (event assignments cascade)."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM cura_mi_form_templates WHERE id = %s", (int(template_id),))
        conn.commit()
        if cur.rowcount:
            log_audit(actor, f"Cura ops MI form template deleted id={template_id}")
            flash("Template deleted. It was removed from any MI events that used it.", "success")
        else:
            flash("Template not found.", "warning")
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_mi_form_template_delete: %s", e)
        flash("Could not delete template.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("medical_records_internal.cura_ops_mi_form_templates"))


@internal_bp.route(
    "/clinical/cura-ops/mi-events/<int:mi_event_id>/custom-forms",
    methods=["GET", "POST"],
)
@login_required
def cura_ops_mi_event_custom_forms(mi_event_id):
    """Choose which MI add-on forms appear in Cura for this minor-injury event."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    from . import cura_mi_custom_forms as micf

    if request.method == "GET":
        conn0 = get_db_connection()
        cur0 = conn0.cursor()
        try:
            oid = _cura_ops_operational_id_for_mi_event(cur0, mi_event_id)
            if oid:
                return redirect(
                    url_for(
                        "medical_records_internal.cura_ops_operational_event_detail",
                        event_id=oid,
                    )
                    + f"#cura-mi-{int(mi_event_id)}"
                )
        finally:
            cur0.close()
            conn0.close()

    conn = get_db_connection()
    cur = conn.cursor()
    mi_name = ""
    assigned_ids: list[int] = []
    pool: list = []
    try:
        cur.execute("SELECT name FROM cura_mi_events WHERE id = %s", (int(mi_event_id),))
        row = cur.fetchone()
        if not row:
            flash("Minor injury event not found.", "warning")
            return redirect(url_for("medical_records_internal.cura_ops_hub"))
        mi_name = row[0] or ""
        for item in micf.list_assigned_templates_for_event(cur, mi_event_id):
            assigned_ids.append(int(item["id"]))
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
        pool = [micf.row_to_template_item(r) for r in cur.fetchall() or []]
    except Exception as e:
        if "cura_mi_form_templates" not in str(e) and "Unknown table" not in str(e):
            logger.exception("cura_ops_mi_event_custom_forms load: %s", e)
        flash("Could not load forms (run DB migration 018 if needed).", "danger")
    finally:
        cur.close()
        conn.close()

    if request.method == "POST":
        raw_ids = request.form.getlist("template_id")
        tid_list: list[int] = []
        for x in raw_ids:
            try:
                tid_list.append(int(x))
            except (TypeError, ValueError):
                pass
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            err = micf.replace_event_assignments(cur, mi_event_id, tid_list, actor)
            if err:
                flash(err, "warning")
            else:
                conn.commit()
                log_audit(actor, f"Cura ops MI event {mi_event_id} custom forms updated")
                flash("Add-on forms for this MI event were updated. Cura users will see them on the next form load.", "success")
        except Exception as e:
            conn.rollback()
            logger.exception("cura_ops_mi_event_custom_forms save: %s", e)
            flash("Could not save assignments.", "danger")
        finally:
            cur.close()
            conn.close()
        conn_r = get_db_connection()
        cur_r = conn_r.cursor()
        try:
            oid = _cura_ops_operational_id_for_mi_event(cur_r, mi_event_id)
            if oid:
                return redirect(
                    url_for(
                        "medical_records_internal.cura_ops_operational_event_detail",
                        event_id=oid,
                    )
                    + f"#cura-mi-{int(mi_event_id)}"
                )
        finally:
            cur_r.close()
            conn_r.close()
        return redirect(
            url_for("medical_records_internal.cura_ops_mi_event_custom_forms", mi_event_id=mi_event_id)
        )

    log_audit(actor, f"Opened MI event {mi_event_id} custom forms")
    return render_template(
        "clinical/cura_ops_mi_event_custom_forms.html",
        config=core_manifest,
        mi_event_id=mi_event_id,
        mi_name=mi_name,
        pool=pool,
        assigned_ids=assigned_ids,
    )


@internal_bp.route("/clinical/cura-ops/mi-reference-card-templates", methods=["GET", "POST"])
@login_required
def cura_ops_mi_reference_card_templates():
    """Create and list reusable Cura MI quick-reference card definitions."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    from . import cura_mi_reference_cards as mirfc

    conn = get_db_connection()
    cur = conn.cursor()
    templates = []
    try:
        cur.execute(
            """
            SELECT id, slug, name, description, schema_json, is_active,
                   created_by, updated_by, created_at, updated_at
            FROM cura_mi_reference_card_templates
            ORDER BY id DESC
            LIMIT 500
            """
        )
        templates = [mirfc.row_to_template_item(r) for r in cur.fetchall() or []]
    except Exception as e:
        if "cura_mi_reference_card_templates" not in str(e) and "Unknown table" not in str(e):
            logger.exception("cura_ops_mi_reference_card_templates list: %s", e)
        templates = []
        flash(
            "MI reference cards require database migration 019. Run medical_records install upgrade.",
            "warning",
        )
    finally:
        cur.close()
        conn.close()

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        slug_raw = (request.form.get("slug") or "").strip()
        from . import cura_mi_custom_forms as micf

        slug = micf.normalize_slug(slug_raw)
        desc = (request.form.get("description") or "").strip() or None
        raw_schema = (request.form.get("schema_json") or "").strip() or "{}"
        schema_dict, s_err = mirfc.parse_card_schema(raw_schema)
        if not name:
            flash("Name is required.", "warning")
        elif not slug:
            flash("Slug must be lowercase letters, digits, underscores, or hyphens.", "warning")
        elif s_err:
            flash(f"Schema: {s_err}", "warning")
        else:
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    INSERT INTO cura_mi_reference_card_templates
                      (slug, name, description, schema_json, is_active, created_by, updated_by)
                    VALUES (%s, %s, %s, %s, 1, %s, %s)
                    """,
                    (slug, name[:255], desc, json.dumps(schema_dict), actor, actor),
                )
                conn.commit()
                log_audit(actor, f"Cura ops MI reference card template created slug={slug}")
                flash(
                    f"Card “{name[:80]}” saved. Assign it to MI events from an operational period.",
                    "success",
                )
                return redirect(url_for("medical_records_internal.cura_ops_mi_reference_card_templates"))
            except Exception as e:
                conn.rollback()
                if "Duplicate" in str(e) or "1062" in str(e):
                    flash("That slug is already in use.", "warning")
                else:
                    logger.exception("cura_ops_mi_reference_card_templates create: %s", e)
                    flash("Could not save (check DB upgrade).", "danger")
            finally:
                cur.close()
                conn.close()

    log_audit(actor, "Opened Cura MI reference card templates")
    return render_template(
        "clinical/cura_ops_mi_reference_card_templates.html",
        config=core_manifest,
        templates=templates,
    )


@internal_bp.route(
    "/clinical/cura-ops/mi-reference-card-templates/<int:template_id>/edit",
    methods=["GET", "POST"],
)
@login_required
def cura_ops_mi_reference_card_template_edit(template_id):
    """Edit a quick-reference card template."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    from . import cura_mi_reference_cards as mirfc
    from . import cura_mi_custom_forms as micf

    conn = get_db_connection()
    cur = conn.cursor()
    tpl = None
    try:
        cur.execute(
            """
            SELECT id, slug, name, description, schema_json, is_active,
                   created_by, updated_by, created_at, updated_at
            FROM cura_mi_reference_card_templates WHERE id = %s
            """,
            (int(template_id),),
        )
        row = cur.fetchone()
        if row:
            tpl = mirfc.row_to_template_item(row)
    except Exception as e:
        logger.exception("cura_ops_mi_reference_card_template_edit load: %s", e)
        flash("Could not load template.", "danger")
    finally:
        cur.close()
        conn.close()

    if not tpl:
        flash("Reference card template not found.", "warning")
        return redirect(url_for("medical_records_internal.cura_ops_mi_reference_card_templates"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        slug = micf.normalize_slug(request.form.get("slug"))
        desc = (request.form.get("description") or "").strip() or None
        raw_schema = (request.form.get("schema_json") or "").strip() or "{}"
        schema_dict, s_err = mirfc.parse_card_schema(raw_schema)
        is_active = 1 if request.form.get("is_active") else 0
        if not name:
            flash("Name is required.", "warning")
        elif not slug:
            flash("Slug must be lowercase letters, digits, underscores, or hyphens.", "warning")
        elif s_err:
            flash(f"Schema: {s_err}", "warning")
        else:
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    UPDATE cura_mi_reference_card_templates
                    SET slug=%s, name=%s, description=%s, schema_json=%s, is_active=%s, updated_by=%s
                    WHERE id=%s
                    """,
                    (slug, name[:255], desc, json.dumps(schema_dict), is_active, actor, int(template_id)),
                )
                conn.commit()
                log_audit(actor, f"Cura ops MI reference card updated id={template_id} slug={slug}")
                flash("Card template saved.", "success")
                return redirect(url_for("medical_records_internal.cura_ops_mi_reference_card_templates"))
            except Exception as e:
                conn.rollback()
                if "Duplicate" in str(e) or "1062" in str(e):
                    flash("That slug is already in use.", "warning")
                else:
                    logger.exception("cura_ops_mi_reference_card_template_edit save: %s", e)
                    flash("Could not save template.", "danger")
            finally:
                cur.close()
                conn.close()

    log_audit(actor, f"Opened MI reference card edit id={template_id}")
    return render_template(
        "clinical/cura_ops_mi_reference_card_template_edit.html",
        config=core_manifest,
        tpl=tpl,
    )


@internal_bp.route(
    "/clinical/cura-ops/mi-reference-card-templates/<int:template_id>/delete",
    methods=["POST"],
)
@login_required
def cura_ops_mi_reference_card_template_delete(template_id):
    """Remove a reference card template (event links cascade)."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM cura_mi_reference_card_templates WHERE id = %s", (int(template_id),))
        conn.commit()
        if cur.rowcount:
            log_audit(actor, f"Cura ops MI reference card template deleted id={template_id}")
            flash("Card template deleted.", "success")
        else:
            flash("Template not found.", "warning")
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_mi_reference_card_template_delete: %s", e)
        flash("Could not delete template.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("medical_records_internal.cura_ops_mi_reference_card_templates"))


@internal_bp.route(
    "/clinical/cura-ops/mi-events/<int:mi_event_id>/reference-cards",
    methods=["GET", "POST"],
)
@login_required
def cura_ops_mi_event_reference_cards(mi_event_id):
    """Assign which reusable reference cards appear in Cura for this MI event."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    from . import cura_mi_reference_cards as mirfc

    if request.method == "GET":
        conn0 = get_db_connection()
        cur0 = conn0.cursor()
        try:
            oid = _cura_ops_operational_id_for_mi_event(cur0, mi_event_id)
            if oid:
                return redirect(
                    url_for(
                        "medical_records_internal.cura_ops_operational_event_detail",
                        event_id=oid,
                    )
                    + f"#cura-mi-{int(mi_event_id)}"
                )
        finally:
            cur0.close()
            conn0.close()

    conn = get_db_connection()
    cur = conn.cursor()
    mi_name = ""
    assigned_ids: list[int] = []
    pool: list = []
    try:
        cur.execute("SELECT name FROM cura_mi_events WHERE id = %s", (int(mi_event_id),))
        row = cur.fetchone()
        if not row:
            flash("Minor injury event not found.", "warning")
            return redirect(url_for("medical_records_internal.cura_ops_hub"))
        mi_name = row[0] or ""
        cur.execute(
            """
            SELECT template_id FROM cura_mi_event_reference_cards
            WHERE event_id = %s
            ORDER BY sort_order ASC, id ASC
            """,
            (int(mi_event_id),),
        )
        assigned_ids = [int(r[0]) for r in cur.fetchall() or []]
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
        pool = [mirfc.row_to_template_item(r) for r in cur.fetchall() or []]
    except Exception as e:
        if "cura_mi_reference_card_templates" not in str(e) and "Unknown table" not in str(e):
            logger.exception("cura_ops_mi_event_reference_cards load: %s", e)
        flash("Could not load reference cards (run DB migration 019 if needed).", "danger")
    finally:
        cur.close()
        conn.close()

    if request.method == "POST":
        raw_ids = request.form.getlist("template_id")
        tid_list: list[int] = []
        for x in raw_ids:
            try:
                tid_list.append(int(x))
            except (TypeError, ValueError):
                pass
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            err = mirfc.replace_event_reference_cards(cur, mi_event_id, tid_list, actor)
            if err:
                flash(err, "warning")
            else:
                conn.commit()
                log_audit(actor, f"Cura ops MI event {mi_event_id} reference cards updated")
                flash("Reference cards updated for this MI event.", "success")
        except Exception as e:
            conn.rollback()
            logger.exception("cura_ops_mi_event_reference_cards save: %s", e)
            flash("Could not save assignments.", "danger")
        finally:
            cur.close()
            conn.close()
        conn_r = get_db_connection()
        cur_r = conn_r.cursor()
        try:
            oid = _cura_ops_operational_id_for_mi_event(cur_r, mi_event_id)
            if oid:
                return redirect(
                    url_for(
                        "medical_records_internal.cura_ops_operational_event_detail",
                        event_id=oid,
                    )
                    + f"#cura-mi-{int(mi_event_id)}"
                )
        finally:
            cur_r.close()
            conn_r.close()
        return redirect(
            url_for(
                "medical_records_internal.cura_ops_mi_event_reference_cards",
                mi_event_id=mi_event_id,
            )
        )

    log_audit(actor, f"Opened MI event {mi_event_id} reference cards")
    return render_template(
        "clinical/cura_ops_mi_event_reference_cards.html",
        config=core_manifest,
        mi_event_id=mi_event_id,
        mi_name=mi_name,
        pool=pool,
        assigned_ids=assigned_ids,
    )


@internal_bp.route(
    "/clinical/cura-ops/mi-events/<int:mi_event_id>/notices",
    methods=["GET", "POST"],
)
@login_required
def cura_ops_mi_event_notices(mi_event_id):
    """Post operational notices shown in Cura under Event Notices for this MI event."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""

    if request.method == "GET":
        conn0 = get_db_connection()
        cur0 = conn0.cursor()
        try:
            oid = _cura_ops_operational_id_for_mi_event(cur0, mi_event_id)
            if oid:
                return redirect(
                    url_for(
                        "medical_records_internal.cura_ops_operational_event_detail",
                        event_id=oid,
                    )
                    + f"#cura-mi-{int(mi_event_id)}"
                )
        finally:
            cur0.close()
            conn0.close()

    conn = get_db_connection()
    cur = conn.cursor()
    mi_name = ""
    notices: list[dict] = []
    try:
        cur.execute("SELECT name FROM cura_mi_events WHERE id = %s", (int(mi_event_id),))
        row = cur.fetchone()
        if not row:
            flash("Minor injury event not found.", "warning")
            return redirect(url_for("medical_records_internal.cura_ops_hub"))
        mi_name = row[0] or ""
        cur.execute(
            """
            SELECT id, message, severity, expires_at, created_at
            FROM cura_mi_notices WHERE event_id = %s
            ORDER BY created_at DESC
            """,
            (int(mi_event_id),),
        )
        for n in cur.fetchall() or []:
            notices.append(
                {
                    "id": n[0],
                    "message": n[1],
                    "severity": n[2] or "info",
                    "expires_at": n[3],
                    "created_at": n[4],
                }
            )
    except Exception as e:
        logger.exception("cura_ops_mi_event_notices load: %s", e)
        flash("Could not load notices.", "danger")
    finally:
        cur.close()
        conn.close()

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()
        if action == "delete":
            nid = request.form.get("notice_id")
            try:
                nid_int = int(nid)
            except (TypeError, ValueError):
                nid_int = 0
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    "DELETE FROM cura_mi_notices WHERE id = %s AND event_id = %s",
                    (nid_int, int(mi_event_id)),
                )
                conn.commit()
                if cur.rowcount:
                    log_audit(actor, f"Cura ops MI notice deleted id={nid_int} event={mi_event_id}")
                    flash("Notice removed.", "success")
                else:
                    flash("Notice not found.", "warning")
            except Exception as e:
                conn.rollback()
                logger.exception("cura_ops_mi_event_notices delete: %s", e)
                flash("Could not delete notice.", "danger")
            finally:
                cur.close()
                conn.close()
        else:
            message = (request.form.get("message") or "").strip()
            severity = (request.form.get("severity") or "info").strip().lower()
            if severity not in ("info", "warning", "error", "success"):
                severity = "info"
            exp_raw = (request.form.get("expires_at") or "").strip()
            exp_sql = None
            if exp_raw:
                exp_sql = exp_raw.replace("T", " ")[:19]
            if not message:
                flash("Message is required.", "warning")
            else:
                conn = get_db_connection()
                cur = conn.cursor()
                try:
                    cur.execute(
                        """
                        INSERT INTO cura_mi_notices (event_id, message, severity, expires_at)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (int(mi_event_id), message, severity, exp_sql or None),
                    )
                    conn.commit()
                    log_audit(actor, f"Cura ops MI notice posted event={mi_event_id}")
                    flash("Notice published to Cura for this event.", "success")
                except Exception as e:
                    conn.rollback()
                    logger.exception("cura_ops_mi_event_notices post: %s", e)
                    flash("Could not save notice.", "danger")
                finally:
                    cur.close()
                    conn.close()
        conn_r = get_db_connection()
        cur_r = conn_r.cursor()
        try:
            oid = _cura_ops_operational_id_for_mi_event(cur_r, mi_event_id)
            if oid:
                return redirect(
                    url_for(
                        "medical_records_internal.cura_ops_operational_event_detail",
                        event_id=oid,
                    )
                    + f"#cura-mi-{int(mi_event_id)}"
                )
        finally:
            cur_r.close()
            conn_r.close()
        return redirect(
            url_for("medical_records_internal.cura_ops_mi_event_notices", mi_event_id=mi_event_id)
        )

    log_audit(actor, f"Opened MI event {mi_event_id} notices")
    return render_template(
        "clinical/cura_ops_mi_event_notices.html",
        config=core_manifest,
        mi_event_id=mi_event_id,
        mi_name=mi_name,
        notices=notices,
    )


def _mi_linked_operational_event_id_from_config(raw_cfg):
    """Read operational_event_id / operationalEventId from MI event config_json (Cura link)."""
    if not raw_cfg:
        return None
    try:
        c = json.loads(raw_cfg) if isinstance(raw_cfg, str) else {}
    except Exception:
        return None
    if not isinstance(c, dict):
        return None
    v = c.get("operational_event_id")
    if v is None:
        v = c.get("operationalEventId")
    if v is None:
        return None
    try:
        oid = int(v)
        return oid if oid > 0 else None
    except (TypeError, ValueError):
        return None


def _cura_ops_operational_id_for_mi_event(cur, mi_event_id: int) -> int | None:
    cur.execute("SELECT config_json FROM cura_mi_events WHERE id = %s", (int(mi_event_id),))
    row = cur.fetchone()
    if not row:
        return None
    return _mi_linked_operational_event_id_from_config(row[0])


def _cura_ops_mi_event_belongs_to_operational_event(
    cur, mi_event_id: int, operational_event_id: int
) -> bool:
    ev = str(int(operational_event_id))
    cur.execute(
        """
        SELECT 1 FROM cura_mi_events WHERE id = %s AND (
          TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operational_event_id')), '')) = %s
          OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operationalEventId')), '')) = %s
        )
        LIMIT 1
        """,
        (int(mi_event_id), ev, ev),
    )
    return cur.fetchone() is not None


def _cura_ops_mi_contacts_from_config_json(config_raw) -> dict[str, str]:
    """Extract Cura MI crew-facing contacts from event config_json."""
    empty = {"medicalLeadCallSign": "", "controlChannel": "", "controlExt": ""}
    if not config_raw:
        return dict(empty)
    try:
        raw = config_raw
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return dict(empty)
    if not isinstance(parsed, dict):
        return dict(empty)
    c = parsed.get("contacts")
    if not isinstance(c, dict):
        return dict(empty)
    return {
        "medicalLeadCallSign": str(c.get("medicalLeadCallSign") or "").strip(),
        "controlChannel": str(c.get("controlChannel") or "").strip(),
        "controlExt": str(c.get("controlExt") or "").strip(),
    }


def _cura_ops_mi_merge_contacts_into_config_json(
    config_raw,
    medical_lead_callsign: str,
    control_channel: str,
    control_ext: str,
) -> str:
    cfg: dict = {}
    if config_raw:
        try:
            raw = config_raw
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, dict):
                cfg = dict(parsed)
        except (TypeError, ValueError, json.JSONDecodeError):
            cfg = {}
    cfg["contacts"] = {
        "medicalLeadCallSign": (medical_lead_callsign or "").strip(),
        "controlChannel": (control_channel or "").strip(),
        "controlExt": (control_ext or "").strip(),
    }
    return json.dumps(cfg, ensure_ascii=False, default=str)


def _cura_ops_mi_risk_profile_from_config_json(config_raw) -> dict[str, object]:
    """Extract Cura event risk profile from MI event config_json (matches Cura EventRiskProfile)."""
    empty: dict[str, object] = {
        "expectedAttendance": "",
        "keyHazards": [],
        "nearestAE": "",
        "siteMapRef": "",
    }
    if not config_raw:
        return dict(empty)
    try:
        raw = config_raw
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return dict(empty)
    if not isinstance(parsed, dict):
        return dict(empty)
    rp = parsed.get("riskProfile")
    if not isinstance(rp, dict):
        return dict(empty)
    kh_raw = rp.get("keyHazards")
    hazards: list[str] = []
    if isinstance(kh_raw, list):
        hazards = [str(x).strip() for x in kh_raw if str(x).strip()]
    elif kh_raw is not None:
        s = str(kh_raw).strip()
        if s:
            hazards = [p.strip() for p in s.replace(",", "\n").split("\n") if p.strip()]
    return {
        "expectedAttendance": str(rp.get("expectedAttendance") or "").strip(),
        "keyHazards": hazards,
        "nearestAE": str(rp.get("nearestAE") or "").strip(),
        "siteMapRef": str(rp.get("siteMapRef") or "").strip(),
    }


def _cura_ops_mi_merge_risk_profile_into_config_json(
    config_raw,
    expected_attendance: str,
    key_hazards: list[str],
    nearest_ae: str,
    site_map_ref: str,
) -> str:
    cfg: dict = {}
    if config_raw:
        try:
            raw = config_raw
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, dict):
                cfg = dict(parsed)
        except (TypeError, ValueError, json.JSONDecodeError):
            cfg = {}
    clean_hazards: list[str] = []
    seen: set[str] = set()
    for h in key_hazards or []:
        t = (h or "").strip()
        if not t or t in seen:
            continue
        seen.add(t)
        clean_hazards.append(t)
    cfg["riskProfile"] = {
        "expectedAttendance": (expected_attendance or "").strip(),
        "keyHazards": clean_hazards,
        "nearestAE": (nearest_ae or "").strip(),
        "siteMapRef": (site_map_ref or "").strip(),
    }
    return json.dumps(cfg, ensure_ascii=False, default=str)


def _cura_incident_response_type_choices(cur) -> list[dict[str, str]]:
    """Value/label pairs from tenant ``incident_response_options`` (for sign-on field editor)."""
    raw_pl = None
    try:
        cur.execute(
            "SELECT payload_json FROM cura_tenant_datasets WHERE name = %s LIMIT 1",
            ("incident_response_options",),
        )
        row = cur.fetchone()
        if row and row[0]:
            raw_pl = json.loads(row[0])
    except Exception:
        raw_pl = None
    try:
        payload = cura_resolved_dataset_payload("incident_response_options", raw_pl)
    except Exception:
        payload = {}
    opts = payload.get("responseOptions") if isinstance(payload, dict) else []
    out: list[dict[str, str]] = []
    for o in opts or []:
        if not isinstance(o, dict):
            continue
        v = str(o.get("value") or "").strip()
        if not v:
            continue
        lab = str(o.get("label") or v).strip()[:120]
        out.append({"value": v, "label": lab})
    return out


def _cura_parse_epcr_signon_fields_from_form() -> list[dict]:
    """Build raw field dicts from POST (signon_* list fields)."""
    rts = request.form.getlist("signon_response_type")
    keys = request.form.getlist("signon_key")
    labels = request.form.getlist("signon_label")
    types = request.form.getlist("signon_field_type")
    phs = request.form.getlist("signon_placeholder")
    n = max(len(rts), len(keys), len(labels), len(types), len(phs), 0)
    raw: list[dict] = []
    for i in range(n):
        key = (keys[i] if i < len(keys) else "").strip()
        label = (labels[i] if i < len(labels) else "").strip()
        if not key and not label:
            continue
        row: dict = {
            "key": key,
            "label": label,
            "type": (types[i] if i < len(types) else "text").strip().lower() or "text",
            "placeholder": (phs[i] if i < len(phs) else "").strip(),
        }
        rt = (rts[i] if i < len(rts) else "").strip()
        if rt:
            row["response_type"] = rt
        raw.append(row)
    return raw


def _cura_ops_mi_report_list_row(
    cur, mi_event_id: int, limit: int = 201
) -> tuple[str | None, list[dict]]:
    """Load MI event name and report index rows for Cura ops (MySQL JSON hints avoid full payload scan per row)."""
    cur.execute("SELECT name FROM cura_mi_events WHERE id = %s", (int(mi_event_id),))
    r0 = cur.fetchone()
    if not r0:
        return None, []
    mi_name = r0[0] or ""
    cur.execute(
        """
        SELECT id, public_id, status, submitted_by, created_at, updated_at,
               NULLIF(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(payload_json, '$.patientName')), '')), ''),
               NULLIF(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(payload_json, '$.presentingComplaint')), '')), '')
        FROM cura_mi_reports
        WHERE event_id = %s
        ORDER BY updated_at DESC, id DESC
        LIMIT %s
        """,
        (int(mi_event_id), int(limit)),
    )
    rows: list[dict] = []
    for r in cur.fetchall() or []:
        rows.append(
            {
                "id": r[0],
                "public_id": str(r[1] or ""),
                "status": (r[2] or "") or "unknown",
                "submitted_by": r[3] or "",
                "created_at": r[4],
                "updated_at": r[5],
                "patient_hint": r[6] or "",
                "complaint_hint": (r[7] or "")[:120] if r[7] else "",
            }
        )
    return mi_name, rows


@internal_bp.route("/clinical/cura-ops/mi-events/<int:mi_event_id>/reports", methods=["GET"])
@login_required
def cura_ops_mi_event_reports(mi_event_id):
    """Browse Cura-submitted minor injury reports for one MI event (clinical ops)."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    conn_chk = get_db_connection()
    cur_chk = conn_chk.cursor()
    try:
        oid = _cura_ops_operational_id_for_mi_event(cur_chk, mi_event_id)
    finally:
        cur_chk.close()
        conn_chk.close()
    if oid:
        return redirect(
            url_for(
                "medical_records_internal.cura_ops_operational_event_mi_reports",
                event_id=oid,
                mi_event_id=int(mi_event_id),
            )
        )

    conn = get_db_connection()
    cur = conn.cursor()
    mi_name = ""
    reports: list[dict] = []
    load_err = False
    try:
        mi_name, reports = _cura_ops_mi_report_list_row(cur, mi_event_id, limit=501)
        if mi_name is None:
            flash("Minor injury event not found.", "warning")
            return redirect(url_for("medical_records_internal.cura_ops_hub"))
    except Exception as e:
        if "cura_mi_reports" in str(e) or "Unknown table" in str(e):
            flash("Minor injury reports require database tables from the Cura MI migrations.", "warning")
        else:
            logger.exception("cura_ops_mi_event_reports: %s", e)
            flash("Could not load reports.", "danger")
        load_err = True
    finally:
        cur.close()
        conn.close()

    if load_err:
        return redirect(url_for("medical_records_internal.cura_ops_hub"))

    log_audit(actor, f"Opened MI event {mi_event_id} reports list ({len(reports)} rows)")
    return render_template(
        "clinical/cura_ops_mi_event_reports.html",
        config=core_manifest,
        mi_event_id=mi_event_id,
        mi_name=mi_name,
        reports=reports,
        truncated=len(reports) >= 500,
        operational_event_id=None,
        operational_event_name="",
    )


@internal_bp.route(
    "/clinical/cura-ops/mi-events/<int:mi_event_id>/reports/<int:report_id>",
    methods=["GET"],
)
@login_required
def cura_ops_mi_event_report_detail(mi_event_id, report_id):
    """Read-only view of one minor injury report payload."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    conn_chk = get_db_connection()
    cur_chk = conn_chk.cursor()
    try:
        oid = _cura_ops_operational_id_for_mi_event(cur_chk, mi_event_id)
    finally:
        cur_chk.close()
        conn_chk.close()
    if oid:
        return redirect(
            url_for(
                "medical_records_internal.cura_ops_operational_event_mi_report_detail",
                event_id=oid,
                mi_event_id=int(mi_event_id),
                report_id=int(report_id),
            )
        )

    conn = get_db_connection()
    cur = conn.cursor()
    row = None
    mi_name = ""
    try:
        cur.execute("SELECT name FROM cura_mi_events WHERE id = %s", (int(mi_event_id),))
        r0 = cur.fetchone()
        if not r0:
            flash("Minor injury event not found.", "warning")
            return redirect(url_for("medical_records_internal.cura_ops_hub"))
        mi_name = r0[0] or ""
        cur.execute(
            """
            SELECT id, public_id, status, submitted_by, rejection_reason, created_at, updated_at, payload_json
            FROM cura_mi_reports
            WHERE id = %s AND event_id = %s
            """,
            (int(report_id), int(mi_event_id)),
        )
        row = cur.fetchone()
    except Exception as e:
        logger.exception("cura_ops_mi_event_report_detail load: %s", e)
        flash("Could not load report.", "danger")
    finally:
        cur.close()
        conn.close()

    if not row:
        flash("Report not found for this MI event.", "warning")
        return redirect(
            url_for("medical_records_internal.cura_ops_mi_event_reports", mi_event_id=mi_event_id)
        )

    raw_payload = row[7] or "{}"
    try:
        parsed = json.loads(raw_payload) if isinstance(raw_payload, str) else {}
    except Exception:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}

    rep = {
        "id": row[0],
        "public_id": str(row[1] or ""),
        "status": (row[2] or "") or "unknown",
        "submitted_by": row[3] or "",
        "rejection_reason": row[4] or "",
        "created_at": row[5],
        "updated_at": row[6],
    }

    log_audit(actor, f"Viewed MI report id={report_id} event={mi_event_id}")
    return render_template(
        "clinical/cura_ops_mi_event_report_detail.html",
        config=core_manifest,
        mi_event_id=mi_event_id,
        mi_name=mi_name,
        rep=rep,
        payload=parsed,
        operational_event_id=None,
        operational_event_name="",
    )


_CURA_OPS_DATASET_CATALOG = (
    ("drugs", "Drug reference data", "Used by Cura for drug pickers and dosing hints."),
    ("clinical_options", "Clinical options", "Dropdowns and structured choices shared across forms."),
    ("iv_fluids", "IV fluids", "Fluid protocols and options for the Cura app."),
    ("clinical_indicators", "Clinical indicators", "Indicator modules (e.g. stroke, falls) configuration."),
    (
        "incident_response_options",
        "Incident response options",
        "Labels and choices for incident response pickers in Cura.",
    ),
    (
        "snomed_uk_ambulance_conditions",
        "SNOMED — UK ambulance / emergency conditions",
        "Structured SNOMED CT disorder list for presenting complaint, PMH, and family history pickers in Cura (International edition slices; refresh from Snowstorm on the server).",
    ),
)

_CURA_APP_SETTINGS_KEY = "cura_app_settings"


def _cura_dataset_summary_line(payload, *, using_defaults: bool = False) -> str:
    """Short, non-technical description for settings UI (no raw data)."""
    if using_defaults:
        return (
            "Bundled defaults are shown below. Save a section (or upload JSON) to persist "
            "this dataset on the server for sync and backups."
        )
    if payload is None:
        return "No file uploaded yet. Use a file from your implementation partner or download a backup first."
    if isinstance(payload, dict):
        concepts = payload.get("concepts")
        if isinstance(concepts, list):
            prof = payload.get("profile") or "snomed"
            return f"SNOMED CT list: {len(concepts)} concepts ({prof})."
        if len(payload) == 0:
            return "Uploaded file is empty — upload a complete file to use this dataset."
        return f"Data on file ({len(payload)} groups)."
    if isinstance(payload, list):
        if len(payload) == 0:
            return "Uploaded file is empty — upload a complete file to use this dataset."
        return f"Data on file ({len(payload)} items)."
    return "Data on file."


def _cura_merge_app_settings_from_form(existing: dict | None) -> dict:
    """Apply form fields; keep any extra keys already stored (not shown in UI)."""
    base = dict(existing) if isinstance(existing, dict) else {}
    base["billingEnabled"] = request.form.get("billing_enabled") == "on"
    base["dnarPhotoGateEnabled"] = request.form.get("dnar_photo_gate_enabled") == "on"
    # serverAddress and localIpAddress are supplied by the Cura app — never overwrite from this form.
    return base


def _cura_parse_division_overrides_json(text: str) -> dict | None:
    """
    Parse optional per-dispatch-division overrides from the Cura ops form textarea.
    Returns None when the field is blank (caller removes ``divisionOverrides`` on save).
    Keys must be division slugs (same strings as MDT / Cura sign-on, e.g. ``general``, ``events``).
    Each value is an object with optional ``billingEnabled`` and/or ``dnarPhotoGateEnabled`` booleans.
    """
    t = (text or "").strip()
    if not t:
        return None
    try:
        obj = json.loads(t)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}") from e
    if not isinstance(obj, dict):
        raise ValueError(
            "Per-division overrides must be a JSON object whose keys are division slugs "
            '(e.g. {"general": {"billingEnabled": false}, "private_urgent_care": {"billingEnabled": true}}).'
        )
    norm: dict = {}
    for raw_k, raw_v in obj.items():
        if not isinstance(raw_k, str) or not raw_k.strip():
            continue
        kk = raw_k.strip().lower().replace(" ", "_")
        if not kk:
            continue
        if not isinstance(raw_v, dict):
            raise ValueError(
                f'Division "{raw_k}" must map to an object, e.g. {{"billingEnabled": true}} — not a string or list.'
            )
        entry: dict = {}
        if "billingEnabled" in raw_v:
            entry["billingEnabled"] = bool(raw_v.get("billingEnabled"))
        elif "billing_enabled" in raw_v:
            entry["billingEnabled"] = bool(raw_v.get("billing_enabled"))
        if "dnarPhotoGateEnabled" in raw_v:
            entry["dnarPhotoGateEnabled"] = bool(raw_v.get("dnarPhotoGateEnabled"))
        elif "dnar_photo_gate_enabled" in raw_v:
            entry["dnarPhotoGateEnabled"] = bool(raw_v.get("dnar_photo_gate_enabled"))
        if entry:
            norm[kk] = entry
    return norm


def _cura_apply_division_overrides_to_saved_settings(merged: dict, div_norm: dict | None) -> None:
    """Mutates ``merged`` in place. ``div_norm`` None = remove overrides; empty dict = remove; else set."""
    if div_norm is None or not div_norm:
        merged.pop("divisionOverrides", None)
    else:
        merged["divisionOverrides"] = div_norm


def _cura_persist_dataset_payload(cur, conn, actor: str, canon: str, payload) -> int:
    """Write full dataset JSON and return new version number."""
    pj = json.dumps(payload)
    cur.execute("SELECT version FROM cura_tenant_datasets WHERE name = %s", (canon,))
    ex = cur.fetchone()
    if ex:
        nv = int(ex[0] or 0) + 1
        cur.execute(
            "UPDATE cura_tenant_datasets SET payload_json=%s, version=%s, updated_by=%s WHERE name=%s",
            (pj, nv, actor, canon),
        )
    else:
        nv = 1
        cur.execute(
            "INSERT INTO cura_tenant_datasets (name, version, payload_json, updated_by) VALUES (%s,%s,%s,%s)",
            (canon, nv, pj, actor),
        )
    conn.commit()
    return nv


def _cura_allowed_section_ids(dataset_name: str, payload) -> set[str]:
    return {s["section_id"] for s in build_editor_sections(dataset_name, payload)}


@internal_bp.route("/clinical/cura-ops/datasets", methods=["GET", "POST"])
@login_required
def cura_ops_datasets_legacy():
    """Old URL — send users to Cura settings (datasets are updated via file upload there)."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    if request.method == "POST":
        flash("This screen has moved. Use Cura settings to manage datasets and connection options.", "info")
    return redirect(url_for("medical_records_internal.cura_ops_settings"))


@internal_bp.route("/clinical/cura-ops/settings/datasets/<string:name>/download", methods=["GET"])
@login_required
def cura_ops_settings_dataset_download(name):
    """Download current dataset as a file (not rendered on screen)."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    from .cura_datasets_routes import norm_cura_dataset_name

    canon = norm_cura_dataset_name(name)
    if not canon:
        abort(404)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT payload_json FROM cura_tenant_datasets WHERE name = %s",
            (canon,),
        )
        row = cur.fetchone()
        raw_pl = None
        if row and row[0]:
            try:
                raw_pl = json.loads(row[0])
            except Exception:
                raw_pl = None
        payload = cura_resolved_dataset_payload(canon, raw_pl)
        if payload is None:
            flash("Nothing to download for that dataset yet.", "warning")
            return redirect(url_for("medical_records_internal.cura_ops_settings"))
        try:
            body = json.dumps(payload, indent=2)
        except Exception:
            body = str(payload)
        log_audit(
            getattr(current_user, "username", "") or "",
            f"Cura settings downloaded dataset file name={canon}",
        )
        fn = f"cura_dataset_{canon}.txt"
        return Response(
            body,
            mimetype="text/plain; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{fn}"',
                "Cache-Control": "no-store",
            },
        )
    finally:
        cur.close()
        conn.close()


@internal_bp.route("/clinical/cura-ops/settings", methods=["GET", "POST"])
@login_required
def cura_ops_settings():
    """
    Cura tenant datasets: table editor + CSV import, optional full JSON upload, download backup.
    App settings (billing, local IP hint). Same persistence as Bearer ``/api/cura/datasets/...``
    and ``/api/cura/config/app-settings``.
    """
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))

    from .cura_datasets_routes import norm_cura_dataset_name

    conn = get_db_connection()
    cur = conn.cursor()
    actor = getattr(current_user, "username", "") or ""

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()
        try:
            if action == "upload_dataset":
                name_raw = (request.form.get("dataset_name") or "").strip()
                canon = norm_cura_dataset_name(name_raw)
                if not canon:
                    flash("Unknown dataset.", "warning")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                f = request.files.get("dataset_file")
                if not f or not f.filename:
                    flash("Choose a file to upload.", "warning")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                try:
                    max_mb = float(os.environ.get("CURA_DATASET_UPLOAD_MAX_MB", "32"))
                except ValueError:
                    max_mb = 32.0
                max_bytes = int(max_mb * 1024 * 1024)
                blob = f.read()
                if len(blob) > max_bytes:
                    flash(f"File is too large (maximum {max_mb:.0f} MB).", "danger")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                try:
                    text = blob.decode("utf-8-sig")
                    payload = json.loads(text)
                except UnicodeDecodeError:
                    flash("The file must be plain text UTF-8.", "danger")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                except json.JSONDecodeError:
                    flash("The file is not valid structured data. Use the file supplied by your implementation partner.", "danger")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                if not isinstance(payload, (dict, list)):
                    flash("The file must contain a single structured document (not a plain value).", "warning")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                nv = _cura_persist_dataset_payload(cur, conn, actor, canon, payload)
                flash(
                    f"Saved “{canon.replace('_', ' ')}”. Cura devices will pick this up on their next sync (version {nv}).",
                    "success",
                )
                log_audit(actor, f"Cura settings uploaded dataset {canon} v={nv}")
            elif action == "save_dataset_section":
                name_raw = (request.form.get("dataset_name") or "").strip()
                canon = norm_cura_dataset_name(name_raw)
                if not canon:
                    flash("Unknown dataset.", "warning")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                section_id = (request.form.get("section_id") or "").strip()
                rows_raw = request.form.get("rows_json") or "[]"
                try:
                    rows = json.loads(rows_raw)
                    if not isinstance(rows, list):
                        raise TypeError("not a list")
                    rows = [r if isinstance(r, dict) else {} for r in rows]
                except Exception:
                    flash("Could not read the table data. Refresh the page and try again.", "danger")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                cur.execute(
                    "SELECT payload_json FROM cura_tenant_datasets WHERE name = %s",
                    (canon,),
                )
                row = cur.fetchone()
                raw = row[0] if row else None
                payload = payload_for_dataset_or_default(canon, raw)
                if section_id not in _cura_allowed_section_ids(canon, payload):
                    flash("That table does not belong to this dataset.", "warning")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                new_payload = apply_section_to_payload(canon, payload, section_id, rows)
                nv = _cura_persist_dataset_payload(cur, conn, actor, canon, new_payload)
                flash(
                    f"Saved “{canon.replace('_', ' ')}” (version {nv}). Cura will sync on the next check-in.",
                    "success",
                )
                log_audit(
                    actor,
                    f"Cura settings saved dataset section {canon} section={section_id} v={nv}",
                )
            elif action == "import_dataset_csv":
                name_raw = (request.form.get("dataset_name") or "").strip()
                canon = norm_cura_dataset_name(name_raw)
                if not canon:
                    flash("Unknown dataset.", "warning")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                section_id = (request.form.get("section_id") or "").strip()
                column_map_raw = (request.form.get("column_map_json") or "").strip() or "{}"
                try:
                    cm = json.loads(column_map_raw)
                    if not isinstance(cm, dict):
                        raise TypeError("not an object")
                    column_map = {str(k): str(v) for k, v in cm.items()}
                except Exception:
                    flash(
                        'Column mapping must be JSON, for example {"0":"name","1":"cost"} '
                        "(column index → field name). Use \"_skip\" to ignore a column.",
                        "danger",
                    )
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                csv_text = (request.form.get("csv_paste") or "").strip()
                f = request.files.get("csv_file")
                if f and getattr(f, "filename", None):
                    try:
                        max_mb = float(os.environ.get("CURA_DATASET_UPLOAD_MAX_MB", "32"))
                    except ValueError:
                        max_mb = 32.0
                    max_bytes = int(max_mb * 1024 * 1024)
                    blob = f.read()
                    if len(blob) > max_bytes:
                        flash(f"CSV file is too large (maximum {max_mb:.0f} MB).", "danger")
                        return redirect(url_for("medical_records_internal.cura_ops_settings"))
                    try:
                        csv_text = blob.decode("utf-8-sig")
                    except UnicodeDecodeError:
                        flash("The CSV file must be UTF-8 text.", "danger")
                        return redirect(url_for("medical_records_internal.cura_ops_settings"))
                if not csv_text:
                    flash("Upload a CSV file or paste CSV text.", "warning")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                mode = (request.form.get("import_mode") or "replace").strip().lower()
                skip_header = request.form.get("csv_skip_header") == "on"
                cur.execute(
                    "SELECT payload_json FROM cura_tenant_datasets WHERE name = %s",
                    (canon,),
                )
                row = cur.fetchone()
                raw = row[0] if row else None
                payload = payload_for_dataset_or_default(canon, raw)
                if section_id not in _cura_allowed_section_ids(canon, payload):
                    flash("That table does not belong to this dataset.", "warning")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                new_payload, err = merge_csv_into_section(
                    canon,
                    payload,
                    section_id,
                    csv_text,
                    column_map,
                    mode,
                    skip_header_row=skip_header,
                )
                if err:
                    flash(err, "danger")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                nv = _cura_persist_dataset_payload(cur, conn, actor, canon, new_payload)
                flash(
                    f"Imported CSV into “{canon.replace('_', ' ')}” (version {nv}). Cura will sync on the next check-in.",
                    "success",
                )
                log_audit(
                    actor,
                    f"Cura settings CSV import dataset={canon} section={section_id} mode={mode} v={nv}",
                )
            elif action == "save_app_settings":
                cur.execute(
                    "SELECT value_json FROM cura_tenant_settings WHERE setting_key = %s",
                    (_CURA_APP_SETTINGS_KEY,),
                )
                srow = cur.fetchone()
                existing = {}
                if srow and srow[0]:
                    try:
                        existing = json.loads(srow[0])
                        if not isinstance(existing, dict):
                            existing = {}
                    except Exception:
                        existing = {}
                div_text = request.form.get("division_overrides_json", "")
                try:
                    div_norm = _cura_parse_division_overrides_json(div_text)
                except ValueError as e:
                    flash(str(e), "danger")
                    return redirect(url_for("medical_records_internal.cura_ops_settings"))
                merged = _cura_merge_app_settings_from_form(existing)
                _cura_apply_division_overrides_to_saved_settings(merged, div_norm)
                vj = json.dumps(merged)
                cur.execute(
                    "SELECT 1 FROM cura_tenant_settings WHERE setting_key = %s",
                    (_CURA_APP_SETTINGS_KEY,),
                )
                if cur.fetchone():
                    cur.execute(
                        "UPDATE cura_tenant_settings SET value_json=%s, updated_by=%s WHERE setting_key=%s",
                        (vj, actor, _CURA_APP_SETTINGS_KEY),
                    )
                else:
                    cur.execute(
                        "INSERT INTO cura_tenant_settings (setting_key, value_json, updated_by) VALUES (%s,%s,%s)",
                        (_CURA_APP_SETTINGS_KEY, vj, actor),
                    )
                conn.commit()
                flash("Cura app options saved (billing, DNAR gate, and optional per-division overrides).", "success")
                log_audit(actor, "Cura settings saved app options (form)")
            else:
                flash("Unknown action.", "warning")
        except Exception as e:
            conn.rollback()
            if "cura_tenant" in str(e) or "Unknown table" in str(e):
                flash("Database tables missing — run the medical records module upgrade.", "danger")
            else:
                logger.exception("cura_ops_settings POST: %s", e)
                flash("Could not save. Try again or contact support.", "danger")
        finally:
            cur.close()
            conn.close()
        return redirect(url_for("medical_records_internal.cura_ops_settings"))

    # GET
    db_by_name: dict = {}
    try:
        cur.execute(
            """
            SELECT name, version, payload_json, updated_at, updated_by
            FROM cura_tenant_datasets
            ORDER BY name
            """
        )
        for row in cur.fetchall() or []:
            db_by_name[row[0]] = row
    except Exception as e:
        logger.warning("cura_ops_settings list datasets: %s", e)

    dataset_cards = []
    seen = set()
    for slug, title, help_text in _CURA_OPS_DATASET_CATALOG:
        seen.add(slug)
        row = db_by_name.get(slug)
        raw_pl = None
        if row:
            ver = int(row[1] or 0)
            updated_at = row[3]
            updated_by = row[4] or ""
            try:
                raw_pl = json.loads(row[2]) if row[2] else None
            except Exception:
                raw_pl = None
        else:
            ver = 0
            updated_at = None
            updated_by = ""
        using_defaults = row is None or is_cura_dataset_payload_unset(slug, raw_pl)
        payload = cura_resolved_dataset_payload(slug, raw_pl)
        summary = _cura_dataset_summary_line(payload, using_defaults=using_defaults)
        dataset_cards.append(
            {
                "name": slug,
                "title": title,
                "help": help_text,
                "version": ver,
                "summary": summary,
                "updated_at": updated_at,
                "updated_by": updated_by,
                "exists": row is not None,
                "sections": build_editor_sections(slug, payload),
            }
        )
    for name, row in sorted(db_by_name.items()):
        if name in seen:
            continue
        raw_pl = None
        try:
            raw_pl = json.loads(row[2]) if row[2] else None
        except Exception:
            raw_pl = None
        using_defaults = is_cura_dataset_payload_unset(name, raw_pl)
        payload = cura_resolved_dataset_payload(name, raw_pl)
        summary = _cura_dataset_summary_line(payload, using_defaults=using_defaults)
        dataset_cards.append(
            {
                "name": name,
                "title": name.replace("_", " ").title(),
                "help": "Additional dataset stored for this organisation.",
                "version": int(row[1] or 0),
                "summary": summary,
                "updated_at": row[3],
                "updated_by": row[4] or "",
                "exists": True,
                "sections": build_editor_sections(name, payload),
            }
        )

    app_form = {
        "billing_enabled": False,
        "dnar_photo_gate_enabled": False,
        "division_overrides_json": "{}",
    }
    app_updated_at = None
    try:
        cur.execute(
            "SELECT value_json, updated_at FROM cura_tenant_settings WHERE setting_key = %s",
            (_CURA_APP_SETTINGS_KEY,),
        )
        srow = cur.fetchone()
        if srow and srow[0]:
            try:
                data = json.loads(srow[0])
                if isinstance(data, dict):
                    app_form["billing_enabled"] = bool(data.get("billingEnabled", False))
                    app_form["dnar_photo_gate_enabled"] = bool(data.get("dnarPhotoGateEnabled", False))
                    div_ov = data.get("divisionOverrides") or data.get("division_overrides")
                    if isinstance(div_ov, dict) and div_ov:
                        try:
                            app_form["division_overrides_json"] = json.dumps(div_ov, indent=2)
                        except Exception:
                            app_form["division_overrides_json"] = "{}"
                    else:
                        app_form["division_overrides_json"] = "{}"
            except Exception:
                pass
            app_updated_at = srow[1]
    except Exception as e:
        logger.warning("cura_ops_settings app settings: %s", e)

    cur.close()
    conn.close()

    log_audit(actor, "Opened Cura settings")
    return render_template(
        "clinical/cura_ops_settings.html",
        config=core_manifest,
        dataset_cards=dataset_cards,
        app_form=app_form,
        app_updated_at=app_updated_at,
    )


@internal_bp.route(
    "/clinical/cura-ops/settings/datasets/snomed-uk-ambulance-conditions/refresh",
    methods=["POST"],
)
@login_required
def cura_ops_settings_snomed_uk_ambulance_refresh():
    """Pull SNOMED CT concepts from Snowstorm (server-side) into ``cura_tenant_datasets``."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    from .cura_snomed_conditions import fetch_uk_ambulance_snomed_payload

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        payload = fetch_uk_ambulance_snomed_payload()
        nv = _cura_persist_dataset_payload(
            cur, conn, actor, "snomed_uk_ambulance_conditions", payload
        )
        flash(
            f"SNOMED UK ambulance dataset saved ({len(payload.get('concepts') or [])} concepts, version {nv}). "
            "Cura devices can sync from Datasets.",
            "success",
        )
        log_audit(actor, f"Cura SNOMED UK ambulance refresh v={nv} concepts={len(payload.get('concepts') or [])}")
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_settings_snomed_uk_ambulance_refresh: %s", e)
        flash(f"Could not refresh from Snowstorm: {e}", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("medical_records_internal.cura_ops_settings"))


@internal_bp.route("/clinical/cura-ops/operational-event/<int:event_id>", methods=["GET"])
@login_required
def cura_ops_operational_event_detail(event_id):
    """
    Per–operational period view: KPIs, Ventus config hints, assignments, validation log, CAD summary.
    """
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))

    from . import cura_event_debrief as ced
    from . import cura_event_ventus_bridge as cevb

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, name, slug, location_summary, starts_at, ends_at, status, config,
                   enforce_assignments, created_by, updated_by, created_at, updated_at
            FROM cura_operational_events
            WHERE id = %s
            """,
            (event_id,),
        )
        row = cur.fetchone()
        if not row:
            abort(404)

        cfg = cevb.event_config_dict(row[7])
        _crm_raw = cfg.get("crm_event_plan_id")
        crm_plan_id_for_link = None
        crm_event_plan_edit_url = None
        if _crm_raw is not None:
            try:
                crm_plan_id_for_link = int(_crm_raw)
            except (TypeError, ValueError):
                crm_plan_id_for_link = None
            if crm_plan_id_for_link is not None:
                try:
                    crm_event_plan_edit_url = url_for(
                        "medical_records_internal.cura_event_planner.event_plan_edit",
                        plan_id=crm_plan_id_for_link,
                    )
                except Exception:
                    crm_event_plan_edit_url = None
        try:
            from flask import current_app

            inv_ok = "inventory_control_internal" in getattr(
                current_app, "blueprints", {}
            )
        except Exception:
            inv_ok = False
        event_row = {
            "id": row[0],
            "name": row[1] or "",
            "slug": row[2] or "",
            "location_summary": row[3] or "",
            "starts_at": row[4],
            "ends_at": row[5],
            "status": row[6] or "",
            "enforce_assignments": bool(row[8]),
            "created_by": row[9] or "",
            "updated_by": row[10] or "",
            "created_at": row[11],
            "updated_at": row[12],
            "ventus_division_slug": cfg.get("ventus_division_slug"),
            "ventus_division_name": cfg.get("ventus_division_name"),
            "ventus_division_color": cfg.get("ventus_division_color"),
            "inventory_event_kit_pool_location_id": cfg.get(
                "inventory_event_kit_pool_location_id"
            ),
            "inventory_event_kit_pool_code": f"OP-EVT-{int(event_id)}",
            "inventory_control_available": inv_ok,
        }

        incident_response_type_choices = _cura_incident_response_type_choices(cur)
        signon_norm = cevb.normalize_epcr_signon_incident_fields(cfg.get("epcr_signon_incident_fields"))
        signon_field_rows = list(signon_norm)
        if not signon_field_rows:
            signon_field_rows = [
                {
                    "response_type": "",
                    "key": "",
                    "label": "",
                    "type": "text",
                    "placeholder": "",
                }
            ]

        kpi_epcr = kpi_pcr = kpi_sg = kpi_assign = 0
        try:
            cur.execute(
                """
                SELECT COUNT(*) FROM cases
                WHERE TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data, '$.operational_event_id')), '')) = %s
                   OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data, '$.operationalEventId')), '')) = %s
                """,
                (str(event_id), str(event_id)),
            )
            r0 = cur.fetchone()
            if r0:
                kpi_epcr = int(r0[0])
        except Exception as e:
            logger.warning("cura_ops_operational_event_detail epcr count: %s", e)

        try:
            cur.execute(
                "SELECT COUNT(*) FROM cura_patient_contact_reports WHERE operational_event_id = %s",
                (event_id,),
            )
            r1 = cur.fetchone()
            if r1:
                kpi_pcr = int(r1[0])
        except Exception as e:
            logger.warning("cura_ops_operational_event_detail pcr count: %s", e)

        try:
            cur.execute(
                "SELECT COUNT(*) FROM cura_safeguarding_referrals WHERE operational_event_id = %s",
                (event_id,),
            )
            r2 = cur.fetchone()
            if r2:
                kpi_sg = int(r2[0])
        except Exception as e:
            logger.warning("cura_ops_operational_event_detail sg count: %s", e)

        try:
            cur.execute(
                "SELECT COUNT(*) FROM cura_operational_event_assignments WHERE operational_event_id = %s",
                (event_id,),
            )
            r3 = cur.fetchone()
            if r3:
                kpi_assign = int(r3[0])
        except Exception as e:
            logger.warning("cura_ops_operational_event_detail assign count: %s", e)

        from . import cura_ops_reporting as cop

        mi_stats: dict = {}
        mi_linked_events: list = []
        try:
            mi_stats = cop.mi_anonymous_stats_for_operational_event(
                cur, event_id, min_cell=0
            )
        except Exception as e:
            logger.warning("cura_ops_operational_event_detail mi_stats: %s", e)
            mi_stats = {}
        try:
            evs = str(int(event_id))
            cur.execute(
                """
                SELECT id, name, location_summary, status, starts_at, ends_at, config_json
                FROM cura_mi_events
                WHERE
                  TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operational_event_id')), '')) = %s
                  OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operationalEventId')), '')) = %s
                ORDER BY id
                """,
                (evs, evs),
            )
            for mr in cur.fetchall() or []:
                mid = mr[0]
                cur.execute(
                    """
                    SELECT COUNT(*) FROM cura_mi_reports
                    WHERE event_id = %s AND LOWER(COALESCE(status, '')) = 'submitted'
                    """,
                    (mid,),
                )
                sc_row = cur.fetchone()
                sub_n = int(sc_row[0]) if sc_row else 0
                mi_linked_events.append(
                    {
                        "id": mid,
                        "name": mr[1] or "",
                        "location_summary": mr[2] or "",
                        "status": mr[3] or "",
                        "starts_at": mr[4],
                        "ends_at": mr[5],
                        "submitted_reports": sub_n,
                        "mi_contacts": _cura_ops_mi_contacts_from_config_json(mr[6]),
                        "mi_risk_profile": _cura_ops_mi_risk_profile_from_config_json(mr[6]),
                    }
                )
        except Exception as e:
            logger.warning("cura_ops_operational_event_detail mi_linked_events: %s", e)
            mi_linked_events = []

        from . import cura_mi_custom_forms as _micf
        from . import cura_mi_reference_cards as _mirfc

        forms_pool_mi: list = []
        try:
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
            forms_pool_mi = [_micf.row_to_template_item(r) for r in cur.fetchall() or []]
        except Exception as ex:
            logger.warning("cura_ops_operational_event_detail mi forms pool: %s", ex)

        ref_pool_mi: list = []
        try:
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
            ref_pool_mi = [_mirfc.row_to_template_item(r) for r in cur.fetchall() or []]
        except Exception as ex:
            logger.warning("cura_ops_operational_event_detail mi ref pool: %s", ex)

        for m in mi_linked_events:
            mid = int(m["id"])
            m["mi_forms_pool"] = forms_pool_mi
            m["mi_forms_assigned"] = []
            try:
                for item in _micf.list_assigned_templates_for_event(cur, mid):
                    m["mi_forms_assigned"].append(int(item["id"]))
            except Exception as ex:
                logger.warning("cura_ops_operational_event_detail mi %s forms assigned: %s", mid, ex)
            m["mi_ref_pool"] = ref_pool_mi
            m["mi_ref_assigned"] = []
            try:
                cur.execute(
                    """
                    SELECT template_id FROM cura_mi_event_reference_cards
                    WHERE event_id = %s
                    ORDER BY sort_order ASC, id ASC
                    """,
                    (mid,),
                )
                m["mi_ref_assigned"] = [int(r[0]) for r in cur.fetchall() or []]
            except Exception:
                pass
            m["mi_notices"] = []
            try:
                cur.execute(
                    """
                    SELECT id, message, severity, expires_at, created_at
                    FROM cura_mi_notices WHERE event_id = %s
                    ORDER BY created_at DESC
                    """,
                    (mid,),
                )
                for n in cur.fetchall() or []:
                    m["mi_notices"].append(
                        {
                            "id": n[0],
                            "message": n[1],
                            "severity": n[2] or "info",
                            "expires_at": n[3],
                            "created_at": n[4],
                        }
                    )
            except Exception:
                pass
            try:
                _mn, rpt_rows = _cura_ops_mi_report_list_row(cur, mid, limit=26)
                m["mi_reports_preview"] = rpt_rows[:25]
                m["mi_reports_truncated"] = len(rpt_rows) > 25
            except Exception as ex:
                logger.warning("cura_ops_operational_event_detail mi %s reports preview: %s", mid, ex)
                m["mi_reports_preview"] = []
                m["mi_reports_truncated"] = False

        kpi_mi_events = int(mi_stats.get("mi_events_linked") or len(mi_linked_events))
        kpi_mi_submitted = int(mi_stats.get("mi_submitted_reports") or 0)

        assignments = ced.fetch_event_assignments(cur, event_id)

        validation_rows = []
        try:
            cur.execute(
                """
                SELECT id, username, callsign, ok, reason_code, created_at
                FROM cura_callsign_mdt_validation_log
                WHERE operational_event_id = %s
                ORDER BY id DESC
                LIMIT 50
                """,
                (event_id,),
            )
            for vr in cur.fetchall() or []:
                validation_rows.append(
                    {
                        "id": vr[0],
                        "username": vr[1] or "",
                        "callsign": vr[2] or "",
                        "ok": bool(vr[3]),
                        "reason_code": vr[4] or "",
                        "created_at": vr[5],
                    }
                )
        except Exception as e:
            logger.warning("cura_ops_operational_event_detail validation log: %s", e)

        try:
            cad_summary = cevb.fetch_cad_dispatch_correlation_for_event(
                cur, event_id, max_cads=35, comms_per_cad=0, max_comms_total=0
            )
        except Exception as e:
            logger.warning("cura_ops_operational_event_detail cad summary: %s", e)
            cad_summary = {"available": False, "note": str(e)[:120], "cads": [], "totals": {}}

        event_management_ok = _cura_ops_event_management_ok()
        event_assets = []
        if event_management_ok:
            try:
                cur.execute(
                    """
                    SELECT id, asset_kind, title, body_text, storage_path, original_filename, sort_order, created_by, created_at
                    FROM cura_operational_event_assets
                    WHERE operational_event_id = %s
                    ORDER BY sort_order ASC, id ASC
                    """,
                    (event_id,),
                )
                for ar in cur.fetchall() or []:
                    event_assets.append(
                        {
                            "id": ar[0],
                            "asset_kind": ar[1],
                            "title": ar[2],
                            "body_text": ar[3],
                            "storage_path": ar[4],
                            "original_filename": ar[5],
                            "sort_order": ar[6],
                            "created_by": ar[7],
                            "created_at": ar[8],
                        }
                    )
            except Exception as ex:
                logger.warning("cura_ops_operational_event_detail assets: %s", ex)
                event_assets = []

        event_resources_raw = []
        try:
            cur.execute(
                """
                SELECT id, resource_kind, resource_id, role_label, notes, sort_order,
                       assigned_by, created_at
                FROM cura_operational_event_resources
                WHERE operational_event_id = %s
                ORDER BY sort_order ASC, id ASC
                """,
                (event_id,),
            )
            for t in cur.fetchall() or []:
                event_resources_raw.append(
                    {
                        "id": t[0],
                        "resource_kind": t[1],
                        "resource_id": t[2],
                        "role_label": t[3],
                        "notes": t[4],
                        "sort_order": t[5],
                        "assigned_by": t[6],
                        "created_at": t[7],
                    }
                )
        except Exception as ex:
            logger.warning("cura_ops_operational_event_detail resources: %s", ex)
            event_resources_raw = []
        event_resources = _cura_enrich_operational_event_resources(event_resources_raw)

        fleet_picklist = []
        equipment_picklist = []
        if _cura_ops_roles_ok():
            try:
                from app.plugins.fleet_management.objects import get_fleet_service

                fleet_picklist = (
                    get_fleet_service().list_vehicles(limit=500, skip=0) or []
                )
            except Exception as ex:
                logger.warning("cura_ops event fleet picklist: %s", ex)
            try:
                from app.plugins.inventory_control.asset_service import get_asset_service

                equipment_picklist = (
                    get_asset_service().list_assets_with_items(limit=450) or []
                )
            except Exception as ex:
                logger.warning("cura_ops event equipment picklist: %s", ex)

        log_audit(
            current_user.username,
            f"Opened Cura operational event detail operational_event_id={event_id}",
        )
        return render_template(
            "clinical/cura_ops_operational_event_detail.html",
            config=core_manifest,
            event=event_row,
            incident_response_type_choices=incident_response_type_choices,
            signon_field_rows=signon_field_rows,
            kpi_epcr=kpi_epcr,
            kpi_pcr=kpi_pcr,
            kpi_sg=kpi_sg,
            kpi_assign=kpi_assign,
            kpi_mi_events=kpi_mi_events,
            kpi_mi_submitted=kpi_mi_submitted,
            mi_stats=mi_stats,
            mi_linked_events=mi_linked_events,
            assignments=assignments,
            validation_rows=validation_rows,
            cad_summary=cad_summary,
            debrief_url=url_for(
                "medical_records_internal.cura_ops_operational_event_debrief_pack",
                event_id=event_id,
                format="pdf",
            ),
            debrief_csv_url=url_for(
                "medical_records_internal.cura_ops_operational_event_debrief_pack",
                event_id=event_id,
                format="csv",
            ),
            debrief_json_url=url_for(
                "medical_records_internal.cura_ops_operational_event_debrief_pack",
                event_id=event_id,
                format="json",
            ),
            event_management_ok=event_management_ok,
            event_assets=event_assets,
            report_url=url_for("medical_records_internal.cura_ops_operational_event_report", event_id=event_id),
            event_resources=event_resources,
            fleet_picklist=fleet_picklist,
            equipment_picklist=equipment_picklist,
            cura_ops_resources_editable=_cura_ops_roles_ok(),
            crm_event_plan_edit_url=crm_event_plan_edit_url,
            crm_plan_id_for_link=crm_plan_id_for_link,
        )
    finally:
        cur.close()
        conn.close()


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/minor-injury/<int:mi_event_id>/custom-forms",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_mi_custom_forms(event_id, mi_event_id):
    """Save MI add-on form assignments from the unified operational event page."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    from . import cura_mi_custom_forms as micf

    raw_ids = request.form.getlist("template_id")
    tid_list: list[int] = []
    for x in raw_ids:
        try:
            tid_list.append(int(x))
        except (TypeError, ValueError):
            pass
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if not _cura_ops_mi_event_belongs_to_operational_event(cur, mi_event_id, event_id):
            flash("That minor injury event is not linked to this operational period.", "warning")
            return redirect(
                url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
            )
        err = micf.replace_event_assignments(cur, mi_event_id, tid_list, actor)
        if err:
            flash(err, "warning")
        else:
            conn.commit()
            log_audit(
                actor,
                f"Cura ops operational_event={event_id} MI {mi_event_id} custom forms updated",
            )
            flash(
                "Add-on forms updated. Cura users will see changes on the next form load.",
                "success",
            )
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_operational_event_mi_custom_forms: %s", e)
        flash("Could not save assignments.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
        + f"#cura-mi-{int(mi_event_id)}"
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/minor-injury/<int:mi_event_id>/reference-cards",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_mi_reference_cards(event_id, mi_event_id):
    """Save MI quick-reference card assignments from the unified operational event page."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    from . import cura_mi_reference_cards as mirfc

    raw_ids = request.form.getlist("template_id")
    tid_list: list[int] = []
    for x in raw_ids:
        try:
            tid_list.append(int(x))
        except (TypeError, ValueError):
            pass
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if not _cura_ops_mi_event_belongs_to_operational_event(cur, mi_event_id, event_id):
            flash("That minor injury event is not linked to this operational period.", "warning")
            return redirect(
                url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
            )
        err = mirfc.replace_event_reference_cards(cur, mi_event_id, tid_list, actor)
        if err:
            flash(err, "warning")
        else:
            conn.commit()
            log_audit(
                actor,
                f"Cura ops operational_event={event_id} MI {mi_event_id} reference cards updated",
            )
            flash("Reference cards updated for this event.", "success")
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_operational_event_mi_reference_cards: %s", e)
        flash("Could not save reference cards.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
        + f"#cura-mi-{int(mi_event_id)}"
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/minor-injury/<int:mi_event_id>/crew-contacts",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_mi_crew_contacts(event_id, mi_event_id):
    """Save medical lead / control line fields shown in Cura for this MI event."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    ml = (request.form.get("medical_lead_callsign") or "").strip()
    cc = (request.form.get("control_channel") or "").strip()
    ce = (request.form.get("control_ext") or "").strip()

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if not _cura_ops_mi_event_belongs_to_operational_event(cur, mi_event_id, event_id):
            flash("That minor injury event is not linked to this operational period.", "warning")
            return redirect(
                url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
            )
        cur.execute("SELECT config_json FROM cura_mi_events WHERE id = %s", (int(mi_event_id),))
        row = cur.fetchone()
        if not row:
            flash("Minor injury event not found.", "warning")
            return redirect(
                url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
            )
        new_json = _cura_ops_mi_merge_contacts_into_config_json(row[0], ml, cc, ce)
        cur.execute(
            "UPDATE cura_mi_events SET config_json = %s, updated_by = %s WHERE id = %s",
            (new_json, actor or None, int(mi_event_id)),
        )
        conn.commit()
        log_audit(
            actor,
            f"Cura ops operational_event={event_id} MI {mi_event_id} crew contacts updated",
        )
        flash(
            "Crew contact details saved. They appear in Cura after the next refresh.",
            "success",
        )
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_operational_event_mi_crew_contacts: %s", e)
        flash("Could not save crew contact details.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
        + f"#cura-mi-{int(mi_event_id)}"
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/minor-injury/<int:mi_event_id>/risk-profile",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_mi_risk_profile(event_id, mi_event_id):
    """Save event risk profile fields (expected attendance, hazards, A&E, site map ref) for Cura control card."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    exp = (request.form.get("expected_attendance") or "").strip()
    near = (request.form.get("nearest_ae") or "").strip()
    smap = (request.form.get("site_map_ref") or "").strip()
    hazards = request.form.getlist("key_hazard")
    if not hazards:
        raw_lines = (request.form.get("key_hazards_text") or "").strip()
        if raw_lines:
            hazards = [ln.strip() for ln in raw_lines.replace("\r", "").split("\n") if ln.strip()]

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if not _cura_ops_mi_event_belongs_to_operational_event(cur, mi_event_id, event_id):
            flash("That minor injury event is not linked to this operational period.", "warning")
            return redirect(
                url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
            )
        cur.execute("SELECT config_json FROM cura_mi_events WHERE id = %s", (int(mi_event_id),))
        row = cur.fetchone()
        if not row:
            flash("Minor injury event not found.", "warning")
            return redirect(
                url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
            )
        new_json = _cura_ops_mi_merge_risk_profile_into_config_json(row[0], exp, hazards, near, smap)
        cur.execute(
            "UPDATE cura_mi_events SET config_json = %s, updated_by = %s WHERE id = %s",
            (new_json, actor or None, int(mi_event_id)),
        )
        conn.commit()
        log_audit(
            actor,
            f"Cura ops operational_event={event_id} MI {mi_event_id} risk profile updated",
        )
        flash(
            "Event risk profile saved. It appears on the Cura control card after the next refresh.",
            "success",
        )
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_operational_event_mi_risk_profile: %s", e)
        flash("Could not save event risk profile.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
        + f"#cura-mi-{int(mi_event_id)}"
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/minor-injury/<int:mi_event_id>/notices",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_mi_notices(event_id, mi_event_id):
    """Publish or remove MI notices from the unified operational event page."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    redir = (
        url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
        + f"#cura-mi-{int(mi_event_id)}"
    )

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        mi_linked = _cura_ops_mi_event_belongs_to_operational_event(cur, mi_event_id, event_id)
    finally:
        cur.close()
        conn.close()
    if not mi_linked:
        flash("That minor injury event is not linked to this operational period.", "warning")
        return redirect(
            url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
        )

    action = (request.form.get("action") or "").strip().lower()
    if action == "delete":
        nid = request.form.get("notice_id")
        try:
            nid_int = int(nid)
        except (TypeError, ValueError):
            nid_int = 0
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM cura_mi_notices WHERE id = %s AND event_id = %s",
                (nid_int, int(mi_event_id)),
            )
            conn.commit()
            if cur.rowcount:
                log_audit(actor, f"Cura ops MI notice deleted id={nid_int} event={mi_event_id}")
                flash("Notice removed.", "success")
            else:
                flash("Notice not found.", "warning")
        except Exception as e:
            conn.rollback()
            logger.exception("cura_ops_operational_event_mi_notices delete: %s", e)
            flash("Could not delete notice.", "danger")
        finally:
            cur.close()
            conn.close()
        return redirect(redir)

    message = (request.form.get("message") or "").strip()
    severity = (request.form.get("severity") or "info").strip().lower()
    if severity not in ("info", "warning", "error", "success"):
        severity = "info"
    exp_raw = (request.form.get("expires_at") or "").strip()
    exp_sql = None
    if exp_raw:
        exp_sql = exp_raw.replace("T", " ")[:19]
    if not message:
        flash("Message is required.", "warning")
        return redirect(redir)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO cura_mi_notices (event_id, message, severity, expires_at)
            VALUES (%s, %s, %s, %s)
            """,
            (int(mi_event_id), message, severity, exp_sql or None),
        )
        conn.commit()
        log_audit(actor, f"Cura ops MI notice posted event={mi_event_id} (operational_event={event_id})")
        flash("Notice published to Cura for this event.", "success")
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_operational_event_mi_notices post: %s", e)
        flash("Could not save notice.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(redir)


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/minor-injury/<int:mi_event_id>/reports",
    methods=["GET"],
)
@login_required
def cura_ops_operational_event_mi_reports(event_id, mi_event_id):
    """Full MI report index scoped under an operational event (breadcrumb context)."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    conn = get_db_connection()
    cur = conn.cursor()
    op_name = ""
    mi_name = ""
    reports: list[dict] = []
    load_err = False
    try:
        if not _cura_ops_mi_event_belongs_to_operational_event(cur, mi_event_id, event_id):
            flash("That minor injury event is not linked to this operational period.", "warning")
            return redirect(
                url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
            )
        cur.execute("SELECT name FROM cura_operational_events WHERE id = %s", (int(event_id),))
        orow = cur.fetchone()
        op_name = (orow[0] or "") if orow else ""
        mi_name, reports = _cura_ops_mi_report_list_row(cur, mi_event_id, limit=501)
        if mi_name is None:
            flash("Minor injury event not found.", "warning")
            return redirect(
                url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
            )
    except Exception as e:
        if "cura_mi_reports" in str(e) or "Unknown table" in str(e):
            flash("Minor injury reports require database tables from the Cura MI migrations.", "warning")
        else:
            logger.exception("cura_ops_operational_event_mi_reports: %s", e)
            flash("Could not load reports.", "danger")
        load_err = True
    finally:
        cur.close()
        conn.close()

    if load_err:
        return redirect(
            url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
        )

    log_audit(
        actor,
        f"Opened MI reports operational_event={event_id} mi_event={mi_event_id} ({len(reports)} rows)",
    )
    return render_template(
        "clinical/cura_ops_mi_event_reports.html",
        config=core_manifest,
        mi_event_id=mi_event_id,
        mi_name=mi_name,
        reports=reports,
        truncated=len(reports) >= 500,
        operational_event_id=event_id,
        operational_event_name=op_name,
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/minor-injury/<int:mi_event_id>/reports/<int:report_id>",
    methods=["GET"],
)
@login_required
def cura_ops_operational_event_mi_report_detail(event_id, mi_event_id, report_id):
    """Read-only MI report view with operational-event navigation context."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    conn = get_db_connection()
    cur = conn.cursor()
    row = None
    mi_name = ""
    op_name = ""
    try:
        if not _cura_ops_mi_event_belongs_to_operational_event(cur, mi_event_id, event_id):
            flash("That minor injury event is not linked to this operational period.", "warning")
            return redirect(
                url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
            )
        cur.execute("SELECT name FROM cura_operational_events WHERE id = %s", (int(event_id),))
        orow = cur.fetchone()
        op_name = (orow[0] or "") if orow else ""
        cur.execute("SELECT name FROM cura_mi_events WHERE id = %s", (int(mi_event_id),))
        r0 = cur.fetchone()
        if not r0:
            flash("Minor injury event not found.", "warning")
            return redirect(
                url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
            )
        mi_name = r0[0] or ""
        cur.execute(
            """
            SELECT id, public_id, status, submitted_by, rejection_reason, created_at, updated_at, payload_json
            FROM cura_mi_reports
            WHERE id = %s AND event_id = %s
            """,
            (int(report_id), int(mi_event_id)),
        )
        row = cur.fetchone()
    except Exception as e:
        logger.exception("cura_ops_operational_event_mi_report_detail load: %s", e)
        flash("Could not load report.", "danger")
    finally:
        cur.close()
        conn.close()

    if not row:
        flash("Report not found for this MI event.", "warning")
        return redirect(
            url_for(
                "medical_records_internal.cura_ops_operational_event_mi_reports",
                event_id=event_id,
                mi_event_id=mi_event_id,
            )
        )

    raw_payload = row[7] or "{}"
    try:
        parsed = json.loads(raw_payload) if isinstance(raw_payload, str) else {}
    except Exception:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}

    rep = {
        "id": row[0],
        "public_id": str(row[1] or ""),
        "status": (row[2] or "") or "unknown",
        "submitted_by": row[3] or "",
        "rejection_reason": row[4] or "",
        "created_at": row[5],
        "updated_at": row[6],
    }

    log_audit(actor, f"Viewed MI report id={report_id} mi={mi_event_id} operational_event={event_id}")
    return render_template(
        "clinical/cura_ops_mi_event_report_detail.html",
        config=core_manifest,
        mi_event_id=mi_event_id,
        mi_name=mi_name,
        rep=rep,
        payload=parsed,
        operational_event_id=event_id,
        operational_event_name=op_name,
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/ventus-dispatch-division/ensure",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_ventus_dispatch_division_ensure(event_id):
    """
    Create or refresh the time-scoped Ventus dispatch division for this operational period so MDT
    and Cura share the same division slug during the event window.
    """
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    resync = (request.form.get("resync") or "").strip().lower() in ("1", "true", "on", "yes")
    from .cura_event_ventus_bridge import provision_operational_event_dispatch_division

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        prov = provision_operational_event_dispatch_division(
            cur, conn, int(event_id), actor, do_commit=True, force_resync=resync
        )
        if prov.get("ok"):
            log_audit(
                actor,
                f"Cura ops ensure ventus division operational_event_id={event_id} slug={prov.get('slug')} resync={resync}",
            )
            if resync:
                flash("Dispatch division updated from this event’s name and visibility window.", "success")
            elif prov.get("already") and not prov.get("resynced"):
                flash("Dispatch division is already registered for this event.", "info")
            else:
                flash("Dispatch division registered for MDT and Cura sign-on.", "success")
        else:
            flash(
                f"Could not provision division: {prov.get('error') or 'unknown error'}.",
                "warning",
            )
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_operational_event_ventus_dispatch_division_ensure: %s", e)
        flash("Could not update Ventus division (check logs / DB).", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/epcr-signon-incident-fields",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_epcr_signon_incident_fields(event_id):
    """
    Live per-event Incident Log meta fields (runner/bib, bike tag, marshal name, etc.).
    Stored on ``cura_operational_events.config`` — Cura reads via ``/me/operational-context`` (not datasets).
    """
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    from . import cura_event_ventus_bridge as cevb

    parsed = _cura_parse_epcr_signon_fields_from_form()

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT config FROM cura_operational_events WHERE id = %s", (int(event_id),))
        row = cur.fetchone()
        if not row:
            flash("Operational period not found.", "warning")
            return redirect(url_for("medical_records_internal.cura_ops_hub"))
        cfg = cevb.event_config_dict(row[0])
        fields = cevb.normalize_epcr_signon_incident_fields(parsed)
        cfg["epcr_signon_incident_fields"] = fields
        cur.execute(
            "UPDATE cura_operational_events SET config = %s, updated_by = %s WHERE id = %s",
            (json.dumps(cfg), (actor or None)[:128], int(event_id)),
        )
        conn.commit()
        log_audit(
            actor,
            f"Cura ops operational_event_id={event_id} epcr_signon_incident_fields n={len(fields)}",
        )
        flash(
            "Saved. Crews will see these questions in Cura after the app refreshes (re-open the app or pull to refresh).",
            "success",
        )
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_operational_event_epcr_signon_incident_fields: %s", e)
        flash("Could not save sign-on incident fields.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
        + "#epcr-signon-incident-fields"
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/withdraw-services",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_withdraw_services(event_id):
    """Close event and deactivate dispatch division immediately (cancellation / early stand-down)."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    from .cura_event_ventus_bridge import withdraw_operational_event_services

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        res = withdraw_operational_event_services(cur, conn, int(event_id), actor, do_commit=True)
        if res.get("ok"):
            log_audit(actor, f"Cura ops withdrew services operational_event_id={event_id}")
        else:
            flash(f"Could not withdraw: {res.get('error') or 'unknown'}.", "warning")
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_operational_event_withdraw_services: %s", e)
        flash("Withdraw failed (check logs).", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
    )


def _purge_cura_operational_event_entire(event_id: int, actor: str) -> dict:
    """
    Remove the operational event shell (and division, uploads, validation log, assets).

    Patient-facing records are never deleted: EPCR cases, patient-contact reports, safeguarding referrals,
    and minor-injury events/reports are only unlinked from this event (JSON / FK cleared). Use a dedicated
    clinical deletion workflow if a case or report must be removed.
    """
    import shutil

    eid = int(event_id)
    ev_key = str(eid)
    out = {
        "ok": False,
        "cases_unlinked": 0,
        "pcr_unlinked": 0,
        "sg_unlinked": 0,
        "mi_events_unlinked": 0,
        "validation_deleted": 0,
        "assets_deleted": 0,
    }
    upload_base = os.path.join(
        _mr_medical_plugin_static_dir(), "uploads", "cura_operational_events", ev_key
    )
    try:
        if os.path.isdir(upload_base):
            shutil.rmtree(upload_base, ignore_errors=True)
    except Exception as ex:
        logger.warning("purge event %s rmtree: %s", eid, ex)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        try:
            from .cura_event_inventory_bridge import release_event_kit_pool_if_configured

            cur.execute("SELECT config FROM cura_operational_events WHERE id = %s", (eid,))
            cfg_row = cur.fetchone()
            raw_ev_cfg = cfg_row[0] if cfg_row else None
            rel = release_event_kit_pool_if_configured(
                eid, raw_ev_cfg, performed_by=actor
            )
            if not rel.get("ok"):
                logger.warning(
                    "purge operational_event %s: inventory kit pool release: %s",
                    eid,
                    rel.get("error"),
                )
        except Exception as ex:
            logger.warning("purge operational_event %s: kit pool: %s", eid, ex)

        try:
            cur.execute("DELETE FROM cura_operational_event_assets WHERE operational_event_id = %s", (eid,))
            out["assets_deleted"] = cur.rowcount
        except Exception as ex:
            logger.warning("purge assets: %s", ex)

        try:
            cur.execute(
                """
                UPDATE cases
                SET data = JSON_REMOVE(
                  JSON_REMOVE(data, '$.operational_event_id'),
                  '$.operationalEventId'
                )
                WHERE
                  TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data, '$.operational_event_id')), '')) = %s
                  OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(data, '$.operationalEventId')), '')) = %s
                """,
                (ev_key, ev_key),
            )
            out["cases_unlinked"] = cur.rowcount
        except Exception as ex:
            logger.warning("purge cases unlink: %s", ex)
            raise

        for table, key in (
            ("cura_patient_contact_reports", "pcr_unlinked"),
            ("cura_safeguarding_referrals", "sg_unlinked"),
        ):
            try:
                cur.execute(
                    f"UPDATE {table} SET operational_event_id = NULL WHERE operational_event_id = %s",
                    (eid,),
                )
                out[key] = cur.rowcount
            except Exception as ex:
                logger.warning("purge %s unlink: %s", table, ex)
                raise

        try:
            cur.execute(
                "DELETE FROM cura_callsign_mdt_validation_log WHERE operational_event_id = %s",
                (eid,),
            )
            out["validation_deleted"] = cur.rowcount
        except Exception as ex:
            logger.warning("purge validation log: %s", ex)

        try:
            cur.execute(
                """
                UPDATE cura_mi_events
                SET config_json = JSON_REMOVE(
                  JSON_REMOVE(config_json, '$.operational_event_id'),
                  '$.operationalEventId'
                )
                WHERE
                  TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operational_event_id')), '')) = %s
                  OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config_json, '$.operationalEventId')), '')) = %s
                """,
                (ev_key, ev_key),
            )
            out["mi_events_unlinked"] = cur.rowcount
        except Exception as ex:
            logger.warning("purge mi_events unlink: %s", ex)
            raise

        try:
            from app.plugins.ventus_response_module.routes import (
                _delete_dispatch_division_row,
                _ensure_dispatch_divisions_table,
                _upgrade_mdt_dispatch_divisions_event_scope,
            )

            _ensure_dispatch_divisions_table(cur)
            _upgrade_mdt_dispatch_divisions_event_scope(cur)
            cur.execute(
                "SELECT slug FROM mdt_dispatch_divisions WHERE cura_operational_event_id = %s",
                (eid,),
            )
            for (slug_row,) in cur.fetchall() or []:
                if not slug_row:
                    continue
                try:
                    _delete_dispatch_division_row(cur, str(slug_row), updated_by=actor or "cura_ops_purge")
                except Exception as dex:
                    logger.warning("purge division slug=%s: %s", slug_row, dex)
        except Exception as ex:
            logger.warning("purge mdt_dispatch_divisions: %s", ex)

        cur.execute("DELETE FROM cura_operational_events WHERE id = %s", (eid,))
        conn.commit()
        out["ok"] = True
        log_audit(
            actor,
            f"Cura ops PURGED operational_event id={eid} epcr_unlinked={out['cases_unlinked']} "
            f"pcr_unlinked={out['pcr_unlinked']} sg_unlinked={out['sg_unlinked']} "
            f"mi_unlinked={out['mi_events_unlinked']}",
        )
    except Exception as e:
        conn.rollback()
        logger.exception("_purge_cura_operational_event_entire: %s", e)
        out["error"] = str(e)[:500]
    finally:
        cur.close()
        conn.close()
    return out


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/delete-entire",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_delete_entire(event_id):
    """
    Remove test/training operational event metadata: division, uploads, validation log, assets, roster/resources.
    EPCR cases and clinical reports stay in the database; only the link to this event is cleared.
    Requires exact event name and typing UNLINK in the confirmation field.
    """
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    actor = getattr(current_user, "username", "") or ""
    phrase = (request.form.get("confirm_delete_phrase") or "").strip().casefold()
    understand = phrase == "unlink"
    typed = (request.form.get("confirm_delete_name") or "").strip()
    conn = get_db_connection()
    cur = conn.cursor()
    name = ""
    try:
        cur.execute("SELECT name FROM cura_operational_events WHERE id = %s", (int(event_id),))
        row = cur.fetchone()
        name = (row[0] or "").strip() if row else ""
    finally:
        cur.close()
        conn.close()

    if not name:
        flash("Event not found.", "warning")
        return redirect(url_for("medical_records_internal.cura_ops_hub"))
    if not understand:
        flash("Type UNLINK in the confirmation field to delete this event.", "warning")
        return redirect(
            url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
        )
    if typed.casefold() != name.casefold():
        flash("Event name did not match — nothing was deleted.", "danger")
        return redirect(
            url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
        )

    res = _purge_cura_operational_event_entire(int(event_id), actor)
    if res.get("ok"):
        flash(
            f"Event removed from Sparrow. Unlinked from {res.get('cases_unlinked', 0)} EPCR case(s), "
            f"{res.get('pcr_unlinked', 0)} patient-contact report(s), {res.get('sg_unlinked', 0)} safeguarding "
            f"referral(s), {res.get('mi_events_unlinked', 0)} minor-injury event config(s). "
            f"Patient records were not deleted. Division and event uploads cleared.",
            "success",
        )
        return redirect(url_for("medical_records_internal.cura_ops_hub"))
    flash(f"Delete failed: {res.get('error', 'unknown error')}", "danger")
    return redirect(
        url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/assignments/add",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_assignment_add(event_id):
    """Session-auth roster control: add a Sparrow username to an operational period (Cura Ops ID)."""
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    principal = (request.form.get("principal_username") or "").strip()
    if not principal:
        flash("Username is required.", "warning")
        return redirect(
            url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id)
        )
    exp_raw = (request.form.get("expected_callsign") or "").strip().upper()
    exp_cs = exp_raw or None
    actor = getattr(current_user, "username", "") or ""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO cura_operational_event_assignments
              (operational_event_id, principal_username, expected_callsign, assigned_by)
            VALUES (%s, %s, %s, %s)
            """,
            (event_id, principal, exp_cs, actor),
        )
        conn.commit()
        log_audit(actor, f"Cura ops roster add operational_event_id={event_id} principal={principal}")
    except Exception as e:
        conn.rollback()
        if "Duplicate" in str(e) or "1062" in str(e):
            flash("That user is already assigned to this period.", "warning")
        else:
            logger.exception("cura_ops_operational_event_assignment_add: %s", e)
            flash("Could not add assignment (check DB / username).", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for(
            "medical_records_internal.cura_ops_operational_event_detail",
            event_id=event_id,
            _anchor="section-roster",
        )
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/assignments/<int:assignment_id>/remove",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_assignment_remove(event_id, assignment_id):
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM cura_operational_event_assignments
            WHERE id = %s AND operational_event_id = %s
            """,
            (assignment_id, event_id),
        )
        if cur.rowcount != 1:
            conn.rollback()
            flash("Assignment not found.", "warning")
        else:
            conn.commit()
            log_audit(
                getattr(current_user, "username", "") or "",
                f"Cura ops roster remove operational_event_id={event_id} assignment_id={assignment_id}",
            )
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_operational_event_assignment_remove: %s", e)
        flash("Could not remove assignment.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for(
            "medical_records_internal.cura_ops_operational_event_detail",
            event_id=event_id,
            _anchor="section-roster",
        )
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/resources/vehicle",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_resource_add_vehicle(event_id):
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    vid_raw = (request.form.get("vehicle_id") or "").strip()
    if not vid_raw.isdigit():
        flash("Choose a vehicle.", "warning")
        return redirect(
            url_for(
                "medical_records_internal.cura_ops_operational_event_detail",
                event_id=event_id,
                _anchor="section-resources",
            )
        )
    vehicle_id = int(vid_raw)
    try:
        from app.plugins.fleet_management.objects import get_fleet_service

        if not get_fleet_service().get_vehicle(vehicle_id):
            flash("Vehicle not found.", "warning")
            return redirect(
                url_for(
                    "medical_records_internal.cura_ops_operational_event_detail",
                    event_id=event_id,
                    _anchor="section-resources",
                )
            )
    except Exception:
        flash("Fleet module unavailable.", "danger")
        return redirect(
            url_for(
                "medical_records_internal.cura_ops_operational_event_detail",
                event_id=event_id,
                _anchor="section-resources",
            )
        )
    role_label = (request.form.get("role_label") or "").strip()[:255] or None
    notes = (request.form.get("notes") or "").strip()[:512] or None
    actor = getattr(current_user, "username", "") or ""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM cura_operational_events WHERE id = %s",
            (event_id,),
        )
        if not cur.fetchone():
            abort(404)
        cur.execute(
            """
            SELECT COALESCE(MAX(sort_order), -1) + 1
            FROM cura_operational_event_resources
            WHERE operational_event_id = %s
            """,
            (event_id,),
        )
        nxt = cur.fetchone()
        sort_order = int(nxt[0] if nxt and nxt[0] is not None else 0)
        cur.execute(
            """
            INSERT INTO cura_operational_event_resources
              (operational_event_id, resource_kind, resource_id, role_label, notes,
               sort_order, assigned_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event_id,
                _CURA_OER_KIND_FLEET,
                vehicle_id,
                role_label,
                notes,
                sort_order,
                actor or None,
            ),
        )
        conn.commit()
        log_audit(
            actor,
            f"Cura ops event resource fleet_vehicle operational_event_id={event_id} vehicle_id={vehicle_id}",
        )
    except Exception as e:
        conn.rollback()
        err = str(e).lower()
        if "duplicate" in err or "1062" in err or "unique" in err:
            flash("That vehicle is already assigned to this period.", "warning")
        else:
            logger.exception("cura_ops_operational_event_resource_add_vehicle: %s", e)
            flash("Could not add vehicle link.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for(
            "medical_records_internal.cura_ops_operational_event_detail",
            event_id=event_id,
            _anchor="section-resources",
        )
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/resources/equipment",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_resource_add_equipment(event_id):
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    aid_raw = (request.form.get("equipment_asset_id") or "").strip()
    if not aid_raw.isdigit():
        flash("Choose a serial equipment record.", "warning")
        return redirect(
            url_for(
                "medical_records_internal.cura_ops_operational_event_detail",
                event_id=event_id,
                _anchor="section-resources",
            )
        )
    asset_id = int(aid_raw)
    try:
        from app.plugins.inventory_control.asset_service import get_asset_service

        if not get_asset_service().get_asset_detail(asset_id):
            flash("Equipment record not found.", "warning")
            return redirect(
                url_for(
                    "medical_records_internal.cura_ops_operational_event_detail",
                    event_id=event_id,
                    _anchor="section-resources",
                )
            )
    except Exception:
        flash("Asset / inventory module unavailable.", "danger")
        return redirect(
            url_for(
                "medical_records_internal.cura_ops_operational_event_detail",
                event_id=event_id,
                _anchor="section-resources",
            )
        )
    role_label = (request.form.get("role_label") or "").strip()[:255] or None
    notes = (request.form.get("notes") or "").strip()[:512] or None
    actor = getattr(current_user, "username", "") or ""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM cura_operational_events WHERE id = %s",
            (event_id,),
        )
        if not cur.fetchone():
            abort(404)
        cur.execute(
            """
            SELECT COALESCE(MAX(sort_order), -1) + 1
            FROM cura_operational_event_resources
            WHERE operational_event_id = %s
            """,
            (event_id,),
        )
        nxt = cur.fetchone()
        sort_order = int(nxt[0] if nxt and nxt[0] is not None else 0)
        cur.execute(
            """
            INSERT INTO cura_operational_event_resources
              (operational_event_id, resource_kind, resource_id, role_label, notes,
               sort_order, assigned_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event_id,
                _CURA_OER_KIND_EQUIPMENT,
                asset_id,
                role_label,
                notes,
                sort_order,
                actor or None,
            ),
        )
        conn.commit()
        log_audit(
            actor,
            f"Cura ops event resource equipment_asset operational_event_id={event_id} asset_id={asset_id}",
        )
    except Exception as e:
        conn.rollback()
        err = str(e).lower()
        if "duplicate" in err or "1062" in err or "unique" in err:
            flash("That equipment record is already assigned to this period.", "warning")
        else:
            logger.exception("cura_ops_operational_event_resource_add_equipment: %s", e)
            flash("Could not add equipment link.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for(
            "medical_records_internal.cura_ops_operational_event_detail",
            event_id=event_id,
            _anchor="section-resources",
        )
    )


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/resources/<int:resource_row_id>/remove",
    methods=["POST"],
)
@login_required
def cura_ops_operational_event_resource_remove(event_id, resource_row_id):
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM cura_operational_event_resources
            WHERE id = %s AND operational_event_id = %s
            """,
            (resource_row_id, event_id),
        )
        if cur.rowcount != 1:
            conn.rollback()
            flash("Resource link not found.", "warning")
        else:
            conn.commit()
            log_audit(
                getattr(current_user, "username", "") or "",
                f"Cura ops event resource remove operational_event_id={event_id} row_id={resource_row_id}",
            )
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_operational_event_resource_remove: %s", e)
        flash("Could not remove resource link.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(
        url_for(
            "medical_records_internal.cura_ops_operational_event_detail",
            event_id=event_id,
            _anchor="section-resources",
        )
    )


@internal_bp.route("/clinical/cura-ops/operational-event/<int:event_id>/report", methods=["GET"])
@login_required
def cura_ops_operational_event_report(event_id):
    """Operational period statistics (aggregate only) + JSON export; event assets from DB."""
    if not _cura_ops_event_management_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))

    from . import cura_ops_reporting as cor

    conn = get_db_connection()
    cur = conn.cursor()
    assets_error = None
    try:
        cur.execute(
            "SELECT id, name FROM cura_operational_events WHERE id = %s",
            (event_id,),
        )
        ev = cur.fetchone()
        if not ev:
            abort(404)

        try:
            min_cell = int(request.args.get("min_cell", "0") or "0")
        except (TypeError, ValueError):
            min_cell = 0
        try:
            scan_limit = int(request.args.get("scan_limit", "8000") or "8000")
        except (TypeError, ValueError):
            scan_limit = 8000
        scan_limit = max(100, min(scan_limit, 15000))

        cur.execute(
            "SELECT COUNT(*) FROM cura_operational_event_assignments WHERE operational_event_id = %s",
            (event_id,),
        )
        roster_n = int((cur.fetchone() or [0])[0] or 0)

        deep = cor.build_deep_analytics_for_operational_event(
            cur, event_id, scan_limit=scan_limit, min_cell=min_cell
        )
        pcr_sg = cor.merge_pcr_sg_sql_counts(cur, event_id, min_cell)
        mi_blk = cor.mi_anonymous_stats_for_operational_event(cur, event_id, min_cell=min_cell)

        assets = []
        try:
            cur.execute(
                """
                SELECT id, asset_kind, title, body_text, storage_path, original_filename, mime_type, sort_order, created_by, created_at
                FROM cura_operational_event_assets
                WHERE operational_event_id = %s
                ORDER BY sort_order ASC, id ASC
                """,
                (event_id,),
            )
            for r in cur.fetchall() or []:
                assets.append(
                    {
                        "id": r[0],
                        "asset_kind": r[1],
                        "title": r[2],
                        "body_text": r[3],
                        "storage_path": r[4],
                        "original_filename": r[5],
                        "mime_type": r[6],
                        "sort_order": r[7],
                        "created_by": r[8],
                        "created_at": r[9],
                    }
                )
        except Exception as ex:
            assets_error = str(ex)[:240]
            logger.warning("cura_ops_operational_event_report assets: %s", ex)

        payload = {
            "operational_event_id": event_id,
            "event_name": ev[1],
            "roster_assignments": roster_n,
            "min_cell": min_cell,
            "scan_limit": scan_limit,
            **pcr_sg,
            **deep,
            "minor_injury": mi_blk,
            "assets_meta": assets,
        }
        if request.args.get("format") == "json":
            log_audit(
                current_user.username,
                f"Cura ops analytics JSON operational_event_id={event_id}",
            )
            fn = f"cura_ops_report_{event_id}.json"
            return Response(
                json.dumps(payload, indent=2, default=str),
                mimetype="application/json; charset=utf-8",
                headers={
                    "Content-Disposition": f'attachment; filename="{fn}"',
                    "Cache-Control": "no-store",
                },
            )

        log_audit(current_user.username, f"Cura ops analytics page operational_event_id={event_id}")
        return render_template(
            "clinical/cura_ops_operational_event_report.html",
            config=core_manifest,
            event_id=event_id,
            event_name=ev[1],
            stats=payload,
            assets=assets,
            assets_error=assets_error,
        )
    finally:
        cur.close()
        conn.close()


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/assets/upload-pdf",
    methods=["POST"],
)
@login_required
def cura_ops_event_asset_upload_pdf(event_id):
    if not _cura_ops_event_management_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    title = (request.form.get("title") or "").strip() or "Event document"
    f = request.files.get("file")
    if not f or not f.filename:
        flash("Choose a PDF file.", "warning")
        return redirect(url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id))
    try:
        max_mb = float(os.environ.get("CURA_EVENT_ASSET_MAX_MB", "40"))
    except ValueError:
        max_mb = 40.0
    max_bytes = int(max_mb * 1024 * 1024)
    f.stream.seek(0, 2)
    sz = f.stream.tell()
    f.stream.seek(0)
    if sz > max_bytes:
        flash(f"File too large (max {max_mb} MB).", "danger")
        return redirect(url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id))

    mime = (f.mimetype or "").lower()
    fn_low = (f.filename or "").lower()
    if mime != "application/pdf" and not fn_low.endswith(".pdf"):
        flash("Only PDF uploads are allowed for event plans.", "warning")
        return redirect(url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id))

    orig = secure_filename(f.filename) or "document.pdf"
    safe = f"{uuid.uuid4().hex}_{orig}"
    rel_path = f"uploads/cura_operational_events/{int(event_id)}/{safe}"
    dest_dir = _cura_ops_event_asset_upload_dir(event_id)
    full_path = os.path.join(dest_dir, safe)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM cura_operational_events WHERE id = %s", (event_id,))
        if not cur.fetchone():
            abort(404)
        cur.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM cura_operational_event_assets WHERE operational_event_id = %s",
            (event_id,),
        )
        sort_order = int((cur.fetchone() or [0])[0] or 0)
        f.save(full_path)
        cur.execute(
            """
            INSERT INTO cura_operational_event_assets
              (operational_event_id, asset_kind, title, body_text, storage_path, original_filename, mime_type, sort_order, created_by)
            VALUES (%s, 'pdf', %s, NULL, %s, %s, %s, %s, %s)
            """,
            (
                event_id,
                title[:255],
                rel_path.replace("\\", "/"),
                orig[:255],
                "application/pdf",
                sort_order,
                getattr(current_user, "username", "") or "",
            ),
        )
        conn.commit()
        log_audit(
            current_user.username,
            f"Cura ops uploaded event PDF operational_event_id={event_id} title={title[:80]}",
        )
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_event_asset_upload_pdf: %s", e)
        try:
            if os.path.isfile(full_path):
                os.remove(full_path)
        except OSError:
            pass
        flash("Upload failed (run DB upgrade if table missing).", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id))


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/assets/bulletin-text",
    methods=["POST"],
)
@login_required
def cura_ops_event_asset_bulletin_text(event_id):
    if not _cura_ops_event_management_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    title = (request.form.get("title") or "").strip()
    body = (request.form.get("body_text") or "").strip()
    if not title or not body:
        flash("Bulletin title and body are required.", "warning")
        return redirect(url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id))

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM cura_operational_events WHERE id = %s", (event_id,))
        if not cur.fetchone():
            abort(404)
        cur.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM cura_operational_event_assets WHERE operational_event_id = %s",
            (event_id,),
        )
        sort_order = int((cur.fetchone() or [0])[0] or 0)
        cur.execute(
            """
            INSERT INTO cura_operational_event_assets
              (operational_event_id, asset_kind, title, body_text, storage_path, original_filename, mime_type, sort_order, created_by)
            VALUES (%s, 'bulletin_text', %s, %s, NULL, NULL, NULL, %s, %s)
            """,
            (event_id, title[:255], body, sort_order, getattr(current_user, "username", "") or ""),
        )
        conn.commit()
        flash("Bulletin published for this operational period.", "success")
        log_audit(
            current_user.username,
            f"Cura ops bulletin operational_event_id={event_id} title={title[:80]}",
        )
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_event_asset_bulletin_text: %s", e)
        flash("Could not save bulletin.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id))


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/assets/<int:asset_id>/delete",
    methods=["POST"],
)
@login_required
def cura_ops_event_asset_delete(event_id, asset_id):
    if not _cura_ops_event_management_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT storage_path FROM cura_operational_event_assets
            WHERE id = %s AND operational_event_id = %s
            """,
            (asset_id, event_id),
        )
        row = cur.fetchone()
        if not row:
            flash("Asset not found.", "warning")
        else:
            sp = row[0]
            cur.execute(
                "DELETE FROM cura_operational_event_assets WHERE id = %s AND operational_event_id = %s",
                (asset_id, event_id),
            )
            conn.commit()
            if sp:
                abs_path = _cura_ops_resolve_asset_file(sp)
                if abs_path:
                    try:
                        os.remove(abs_path)
                    except OSError:
                        pass
            log_audit(
                current_user.username,
                f"Cura ops deleted event asset operational_event_id={event_id} asset_id={asset_id}",
            )
    except Exception as e:
        conn.rollback()
        logger.exception("cura_ops_event_asset_delete: %s", e)
        flash("Delete failed.", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("medical_records_internal.cura_ops_operational_event_detail", event_id=event_id))


@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/assets/<int:asset_id>/file",
    methods=["GET"],
)
@login_required
def cura_ops_event_asset_file(event_id, asset_id):
    if not _cura_ops_event_management_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.landing"))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT storage_path, original_filename, mime_type, asset_kind
            FROM cura_operational_event_assets
            WHERE id = %s AND operational_event_id = %s
            """,
            (asset_id, event_id),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            abort(404)
        sp, orig, mime, asset_kind = row[0], row[1], row[2], row[3]
        if (asset_kind or "") != "pdf":
            abort(404)
        path = _cura_ops_resolve_asset_file(sp)
        if not path:
            abort(404)
        log_audit(
            current_user.username,
            f"Cura ops downloaded event PDF operational_event_id={event_id} asset_id={asset_id}",
        )
        return send_file(
            path,
            mimetype=mime or "application/pdf",
            download_name=orig or "event-document.pdf",
            as_attachment=False,
        )
    finally:
        cur.close()
        conn.close()


# Two paths share this endpoint; Werkzeug may bind url_for() to either rule. The handler sets
# default_fmt from the path (.json → JSON). Always pass format=pdf (or csv/json) on links so
# exports are unambiguous regardless of which rule url_for() picks.
# Register …/debrief-pack.json *before* …/debrief-pack (historical convention for url_for).
@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/debrief-pack.json",
    methods=["GET"],
)
@internal_bp.route(
    "/clinical/cura-ops/operational-event/<int:event_id>/debrief-pack",
    methods=["GET"],
)
@login_required
def cura_ops_operational_event_debrief_pack(event_id):
    """
    Final client debrief: PDF (default) or CSV tables; optional JSON (?format=json) for integrations.
    Legacy path …/debrief-pack.json defaults to JSON.
    """
    if not _cura_ops_roles_ok():
        flash("Unauthorised access", "danger")
        return redirect(url_for("medical_records_internal.cura_ops_hub"))

    from . import cura_event_debrief as ced
    from . import cura_event_ventus_bridge as cevb
    from . import cura_ops_debrief_document as codd
    from .cura_routes import CURA_SCHEMA_VERSION

    path = request.path or ""
    default_fmt = "json" if path.rstrip("/").endswith("debrief-pack.json") else "pdf"
    fmt = (request.args.get("format") or default_fmt).strip().lower()
    if fmt not in ("pdf", "csv", "json"):
        fmt = default_fmt

    mi_q = request.args.get("mi_event_id") or request.args.get("miEventId")
    try:
        mi_event_id = int(mi_q) if mi_q is not None and str(mi_q).strip() != "" else None
    except (TypeError, ValueError):
        mi_event_id = None

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        try:
            body = ced.build_operational_event_incident_body(
                cur,
                event_id,
                min_cell=0,
                mi_event_id=mi_event_id,
                may_include_case=_user_may_access_case_data,
                epcr_hits_limit=500,
            )
        except Exception as e:
            if "cura_mi_events" in str(e) or "Unknown table" in str(e):
                flash("Database upgrade required for operational event data.", "warning")
                return redirect(url_for("medical_records_internal.cura_ops_hub"))
            raise
        if body is None:
            abort(404)

        assignments = ced.fetch_event_assignments(cur, event_id)
        validation_log = ced.fetch_event_callsign_validation_log(cur, event_id, limit=250)
        ventus_cad = cevb.fetch_cad_dispatch_correlation_for_event(
            cur, event_id, max_cads=80, comms_per_cad=30, max_comms_total=2000
        )

        mj = body.get("minor_injury") if isinstance(body.get("minor_injury"), dict) else {}
        linked_mi = mj.get("linked_mi_events") if isinstance(mj, dict) else None
        if not isinstance(linked_mi, list):
            linked_mi = []

        crm_plan_ctx = _cura_debrief_crm_plan_context(cur, event_id)
        methodology = (body.get("methodology_notes") or "").strip()
        hub_methodology = (
            "Debrief export parameters: no small-count suppression; up to 500 clinical encounter summary rows; "
            "up to 250 MDT/callsign validation log rows; dispatch (Ventus) desk summary when division integration is configured."
        )
        mi_scope = (
            f" Minor injury analytics: {len(linked_mi)} linked minor-injury event(s) merged"
            + (f" (export filtered to one MI event)." if mi_event_id is not None else ".")
        )
        methodology_notes = " ".join(x for x in (methodology, hub_methodology + mi_scope) if x).strip()

        pack = {
            "schema_version": CURA_SCHEMA_VERSION,
            **body,
            "debrief_export": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "exported_by": getattr(current_user, "username", "") or "",
            },
            "assignments": assignments,
            "callsign_validation_log": validation_log,
            "ventus_cad_correlation": ventus_cad,
            "methodology_notes": methodology_notes,
            "handover_context": {
                "crm_event_plan": crm_plan_ctx,
                "linked_minor_injury_events": len(linked_mi),
                "mi_export_filtered_to_single": mi_event_id is not None,
            },
            "note": "",
        }

        log_audit(
            current_user.username,
            f"Cura operational debrief ({fmt}) operational_event_id={event_id} mi_filter={mi_event_id!r} mi_linked_n={len(linked_mi)}",
        )

        if fmt == "json":
            fn = f"cura_operational_event_{event_id}_debrief.json"
            return Response(
                json.dumps(pack, indent=2, default=str),
                mimetype="application/json; charset=utf-8",
                headers={
                    "Content-Disposition": f'attachment; filename="{fn}"',
                    "Cache-Control": "no-store",
                },
            )
        if fmt == "csv":
            fn = f"cura_operational_event_{event_id}_debrief.csv"
            csv_text = codd.render_debrief_pack_csv(pack)
            return Response(
                csv_text.encode("utf-8-sig"),
                mimetype="text/csv; charset=utf-8",
                headers={
                    "Content-Disposition": f'attachment; filename="{fn}"',
                    "Cache-Control": "no-store",
                },
            )
        # pdf (default)
        try:
            try:
                from app.plugins.crm_module.crm_branding import get_site_branding

                _debrief_branding = get_site_branding()
            except Exception:
                _debrief_branding = {}
            pdf_bytes = codd.render_debrief_pack_pdf(pack, branding=_debrief_branding)
        except Exception as ex:
            logger.exception("cura_ops_operational_event_debrief_pack pdf: %s", ex)
            flash("PDF generation failed; try CSV export.", "danger")
            return redirect(
                url_for(
                    "medical_records_internal.cura_ops_operational_event_detail",
                    event_id=event_id,
                )
            )
        fn = f"cura_operational_event_{event_id}_debrief.pdf"
        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{fn}"',
                "Cache-Control": "no-store",
            },
        )
    finally:
        cur.close()
        conn.close()


@internal_bp.route('/clinical/epcr', methods=['GET'])
@login_required
def view_epcr():
    """
    Renders the Clinical Lead Dashboard page that includes:
      - A table listing EPCR cases along with a search bar.
      - An Actions column with View and Delete buttons.
    
    The case record includes:
      - id, status, created_at, updated_at, closed_at,
      - assignedUsers (from cases.data JSON only — usernames for audit/filtering; no clinical sections)
    """
    # EPCR dashboard is available to admin/clinical/superuser.
    # Caldicott: list shows metadata + assigned Sparrow usernames only (not clinical JSON).
    # Case detail requires an approved one-time code + justification (`unlock_epcr_case`).
    role = (getattr(current_user, 'role', '') or '').lower()
    if role not in ["clinical_lead", "superuser", "admin", "support_break_glass"]:
        return redirect(url_for("medical_records_module"))

    # Leaving the case detail view (back to this list) invalidates in-session unlock for all cases.
    _clear_epcr_in_session_unlock_flags()

    locked_mode = True

    # Fetch cases from the database — metadata only (no clinical JSON blob in the list).
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT id, status, created_at, closed_at, updated_at,
                   dispatch_reference, primary_callsign, dispatch_synced_at, record_version,
                   JSON_EXTRACT(data, '$.assignedUsers') AS assigned_users_json
            FROM cases
            ORDER BY id DESC
            LIMIT 750
            """
        )
        cases = cursor.fetchall() or []
    finally:
        cursor.close()
        conn.close()

    epcr_cases = []
    for case in cases:
        assigned = _epcr_assigned_users_from_json_extract(
            case.get("assigned_users_json")
        )
        epcr_cases.append(
            {
                "id": case.get("id"),
                "assignedUsers": assigned,
                "status": case.get("status"),
                "created_at": _epcr_format_db_utc_naive_for_display(case.get("created_at")),
                "updated_at": _epcr_format_db_utc_naive_for_display(case.get("updated_at")),
                "closed_at": _epcr_format_db_utc_naive_for_display(case.get("closed_at")),
                "data": {},
            }
        )
    log_audit(current_user.username, f"Viewed EPCR case dashboard (locked_mode={locked_mode})")
    logger.info("EPCR dashboard viewed by user %s.", current_user.username)
    tz_label = (os.environ.get("SPARROW_DISPLAY_TIMEZONE") or "Europe/London").strip() or "Europe/London"
    html = render_template(
        'clinical/clinical_epcr.html',
        epcr_cases=epcr_cases,
        current_user=current_user,
        epcr_locked_mode=locked_mode,
        config=core_manifest,
        epcr_display_timezone_label=tz_label,
        epcr_admin_force_close_allowed=_user_may_epcr_admin_force_close(),
        epcr_force_close_categories=EPCR_ADMIN_FORCE_CLOSE_CATEGORIES,
    )
    resp = make_response(html)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"
    resp.headers["Pragma"] = "no-cache"
    return resp

@internal_bp.route('/clinical/epcr/<int:case_id>', methods=['GET'])
@login_required
def view_epcr_case(case_id):
    """
    Detailed view for a single EPCR case.
    Fetches the raw JSON from MySQL, adds status/timestamps,
    and renders the detailed Jinja template which will handle
    ordering and conditional sections.
    """
    role = (getattr(current_user, 'role', '') or '').lower()
    if role not in ("clinical_lead", "superuser", "admin", "support_break_glass"):
        return redirect(url_for('medical_records_internal.view_epcr'))
    cid = _epcr_case_session_id(case_id)
    had_unlock = bool(session.get(f"epcr_unlocked_{cid}"))
    if not _epcr_session_unlock_valid(case_id):
        log_audit(
            current_user.username,
            f"EPCR access audit: denied opening case {case_id} for {current_user.username} "
            f"({'unlock session expired' if had_unlock else 'no valid unlock session'}; code, justification, and personal PIN required via list)",
            case_id=case_id,
        )
        if had_unlock:
            flash(
                "Your access session for this case has expired (time limit). Return to the EPCR list and "
                "unlock again with a new approved code, justification, and personal PIN.",
                "warning",
            )
        else:
            flash(
                "This case record is protected. Use Request code / Enter code on the EPCR list, "
                "then enter your one-time access code and justification before viewing.",
                "warning",
            )
        return redirect(url_for('medical_records_internal.view_epcr'))

    current_app.logger.debug("Entered /clinical/epcr route for case %s", case_id)
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT data, status, created_at, closed_at, updated_at, "
            "dispatch_reference, primary_callsign, dispatch_synced_at, record_version "
            "FROM cases WHERE id = %s",
            (case_id,),
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            current_app.logger.error("No case found for id %s", case_id)
            return redirect(url_for('medical_records_internal.view_epcr'))

        (
            data_str,
            status,
            created_at,
            closed_at,
            updated_at,
            dispatch_reference,
            primary_callsign,
            dispatch_synced_at,
            record_version,
        ) = row
        case_data = json.loads(data_str)
        # Attach metadata for display
        case_data.update({
            'status': status,
            'createdAt': created_at.isoformat() if created_at else None,
            'closedAt':  closed_at.isoformat()  if closed_at  else None,
            'updatedAt': updated_at.isoformat() if updated_at else None
        })
        _merge_epcr_link_into_case_json(
            case_data, dispatch_reference, primary_callsign, dispatch_synced_at, record_version
        )

    except Exception as e:
        current_app.logger.error("Database error loading case %s: %s", case_id, e)
        return redirect(url_for('medical_records_internal.view_epcr'))

    # Render template; all section ordering / existence checks live in Jinja now
    try:
        conn_rtc = get_db_connection()
        cur_rtc = conn_rtc.cursor()
        try:
            inject_rtc_case_access_inline_images(int(case_id), case_data, cur_rtc)
            inject_oohca_role_ecg_case_access_images(int(case_id), case_data, cur_rtc)
        finally:
            cur_rtc.close()
            conn_rtc.close()
    except Exception:
        current_app.logger.exception("inject case-access inline images (RTC/OOHCA) clinical epcr case %s", case_id)

    log_audit(
        current_user.username,
        f"EPCR access audit: {current_user.username} viewed clinical record for case {case_id} (after unlock)",
        case_id=case_id,
    )
    html = render_template(
        'public/case_access_pdf.html',
        case_data=case_data,
        case_id=case_id,
        config=core_manifest,
        show_sparrow_nav=True,
        epcr_access_audit=_epcr_access_audit_for_template(case_id),
    )
    resp = make_response(html)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"
    resp.headers["Pragma"] = "no-cache"
    return resp


@internal_bp.route('/admin/request_epcr_access_code', methods=['POST'])
@login_required
def request_epcr_access_code():
    allowed = {'admin', 'superuser', 'clinical_lead', 'support_break_glass'}
    if (getattr(current_user, "role", "") or "").lower() not in allowed:
        return jsonify(error="Unauthorised access"), 403

    data = request.get_json(silent=True) or {}
    case_id = data.get("case_id") or data.get("id")
    justification = (data.get("justification") or "").strip()
    if not case_id or not justification:
        return jsonify(error="Missing parameters"), 400
    case_id = str(case_id).strip()

    request_id = f"epcr-{uuid.uuid4().hex[:16]}"
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        _epcr_maintain_access_requests(cursor)
        # One active pending request per case per requester: withdraw older duplicates.
        cursor.execute(
            """
            UPDATE epcr_access_requests
            SET status='withdrawn',
                reviewed_at=UTC_TIMESTAMP(),
                review_note='Superseded by a newer request for this case.'
            WHERE case_id=%s
              AND LOWER(requested_by)=LOWER(%s)
              AND status='pending'
            """,
            (case_id, current_user.username),
        )
        cursor.execute(
            """
            INSERT INTO epcr_access_requests
            (request_id, case_id, requested_by, requester_role, justification, status)
            VALUES (%s, %s, %s, %s, %s, 'pending')
            """,
            (
                request_id,
                case_id,
                current_user.username,
                (getattr(current_user, "role", "") or "").lower(),
                justification,
            ),
        )
        conn.commit()
    except Exception as e:
        current_app.logger.error("Failed to persist EPCR access request: %s", e)
        return jsonify(error="Failed to save access request"), 500
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    log_audit(
        current_user.username,
        f"EPCR access audit: {current_user.username} requested access to case {case_id} for the reason of: {justification} (request_id={request_id})",
        patient_id=case_id,
    )

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT TRIM(email) AS email FROM users
            WHERE LOWER(TRIM(role)) IN ('clinical_lead', 'superuser')
              AND email IS NOT NULL AND TRIM(email) <> ''
            """,
        )
        recipients = [
            str(row[0]).strip()
            for row in (cursor.fetchall() or [])
            if row and row[0] and str(row[0]).strip()
        ]
    except Exception as e:
        current_app.logger.error("Database error fetching EPCR code recipients: %s", e)
        return jsonify(error="Internal server error"), 500
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    subject = f"EPCR access request for case #{case_id}"
    body = (
        f"{current_user.username} requested EPCR case access for case {case_id}.\n"
        f"Request ID: {request_id}\n\n"
        f"Justification:\n{justification}\n\n"
        "Review this request in Clinical Lead dashboard and approve/reject it."
    )
    email_sent = False
    warning = None
    if not recipients:
        current_app.logger.warning(
            "EPCR access request %s saved but no approver emails on file (case %s)",
            request_id,
            case_id,
        )
        warning = (
            "Email was not sent: no clinical lead or superuser email addresses are on file. "
            "Your request is still recorded—please call or contact a clinical lead for approval."
        )
    else:
        try:
            active_emailer = emailer if emailer is not None else EmailManager()
            active_emailer.send_email(subject=subject, body=body, recipients=recipients)
            email_sent = True
        except Exception as e:
            current_app.logger.warning(
                "EPCR access request %s saved but approval email not sent: %s",
                request_id,
                e,
                exc_info=True,
            )
            warning = (
                "Email was not sent (system mail may not be configured or delivery failed). "
                "Your request is still recorded—please call or contact a clinical lead for approval."
            )

    payload = {
        "ok": True,
        "request_id": request_id,
        "status": "pending",
        "email_sent": email_sent,
    }
    if warning:
        payload["warning"] = warning
    return jsonify(payload), 200


@internal_bp.route('/clinical/epcr_access_requests', methods=['GET'])
@login_required
def list_epcr_access_requests():
    allowed = {'clinical_lead', 'superuser', 'support_break_glass'}
    if (getattr(current_user, "role", "") or "").lower() not in allowed:
        return jsonify(error="Unauthorised access"), 403

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        mcur = conn.cursor()
        _epcr_maintain_access_requests(mcur)
        mcur.close()
        conn.commit()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT request_id, case_id, requested_by, justification, status,
                   reviewed_by, review_note, created_at, reviewed_at
            FROM epcr_access_requests
            ORDER BY created_at DESC
            LIMIT 250
            """
        )
        rows = cursor.fetchall() or []
    except Exception as e:
        current_app.logger.error("Failed to load EPCR access requests: %s", e)
        return jsonify(error="Failed to load requests"), 500
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    items = []
    for row in rows:
        items.append({
            "request_id": row.get("request_id"),
            "case_id": row.get("case_id"),
            "requested_by": row.get("requested_by"),
            "justification": row.get("justification"),
            "status": row.get("status"),
            "reviewed_by": row.get("reviewed_by"),
            "review_note": row.get("review_note"),
            "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
            "reviewed_at": row.get("reviewed_at").isoformat() if row.get("reviewed_at") else None,
        })
    return jsonify(requests=items), 200


@internal_bp.route('/admin/my_epcr_access_requests', methods=['GET'])
@login_required
def my_epcr_access_requests():
    allowed = {'admin', 'superuser', 'support_break_glass'}
    if (getattr(current_user, "role", "") or "").lower() not in allowed:
        return jsonify(error="Unauthorised access"), 403
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        mcur = conn.cursor()
        _epcr_maintain_access_requests(mcur)
        mcur.close()
        conn.commit()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT r.request_id, r.case_id, r.justification, r.status, r.review_note,
                   r.access_code, r.code_expires_at, r.created_at, r.reviewed_at,
                   c.dispatch_reference AS cad_reference,
                   c.created_at AS case_opened_at,
                   oe.id AS operational_event_id,
                   oe.name AS operational_event_name,
                   oe.location_summary AS operational_event_location
            FROM epcr_access_requests r
            LEFT JOIN cases c ON c.id = r.case_id
            LEFT JOIN cura_operational_events oe ON oe.id = CAST(
                COALESCE(
                    NULLIF(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(c.data, '$.operational_event_id')), '')), ''),
                    NULLIF(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(c.data, '$.operationalEventId')), '')), '')
                ) AS UNSIGNED
            )
            WHERE LOWER(r.requested_by) = LOWER(%s)
            ORDER BY r.created_at DESC
            LIMIT 100
            """,
            (current_user.username,),
        )
        rows = cursor.fetchall() or []
    except Exception as e:
        current_app.logger.error("Failed loading requester EPCR statuses: %s", e)
        return jsonify(error="Failed to load request statuses"), 500
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    items = []
    for row in rows:
        _oid = row.get("operational_event_id")
        try:
            operational_event_id = int(_oid) if _oid is not None else None
        except (TypeError, ValueError):
            operational_event_id = None
        st = (row.get("status") or "").strip().lower()
        items.append({
            "request_id": row.get("request_id"),
            "case_id": row.get("case_id"),
            "justification": row.get("justification"),
            "status": row.get("status"),
            "review_note": row.get("review_note"),
            "access_code": row.get("access_code"),
            "code_expires_at": row.get("code_expires_at").isoformat() if row.get("code_expires_at") else None,
            "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
            "reviewed_at": row.get("reviewed_at").isoformat() if row.get("reviewed_at") else None,
            "cad_reference": (row.get("cad_reference") or "").strip() or None,
            "case_opened_at": row.get("case_opened_at").isoformat() if row.get("case_opened_at") else None,
            "operational_event_id": operational_event_id,
            "operational_event_name": (row.get("operational_event_name") or "").strip() or None,
            "operational_event_location": (row.get("operational_event_location") or "").strip() or None,
            "can_withdraw": st == "pending",
        })
    return jsonify(requests=items), 200


@internal_bp.route('/admin/withdraw_epcr_access_request', methods=['POST'])
@login_required
def withdraw_epcr_access_request():
    """Requester cancels a pending access request (same roles as requesting access)."""
    allowed = {'admin', 'superuser', 'clinical_lead', 'support_break_glass'}
    if (getattr(current_user, "role", "") or "").lower() not in allowed:
        return jsonify(error="Unauthorised access"), 403

    data = request.get_json(silent=True) or {}
    request_id = str(data.get("request_id") or "").strip()
    if not request_id:
        return jsonify(error="Missing request_id"), 400

    conn = None
    cursor = None
    row = None
    try:
        conn = get_db_connection()
        mcur = conn.cursor()
        _epcr_maintain_access_requests(mcur)
        mcur.close()
        conn.commit()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT request_id, case_id, status, requested_by
            FROM epcr_access_requests
            WHERE request_id = %s
            LIMIT 1
            """,
            (request_id,),
        )
        row = cursor.fetchone()
        if not row:
            return jsonify(error="Request not found"), 404
        if (row.get("requested_by") or "").strip().lower() != (current_user.username or "").strip().lower():
            return jsonify(error="You can only withdraw your own requests"), 403
        if (row.get("status") or "").lower() != "pending":
            return jsonify(error="Request is not pending"), 409
        cursor.execute(
            """
            UPDATE epcr_access_requests
            SET status='withdrawn',
                reviewed_at=UTC_TIMESTAMP(),
                review_note='Withdrawn by requester.'
            WHERE request_id=%s AND status='pending'
              AND LOWER(requested_by)=LOWER(%s)
            """,
            (request_id, current_user.username),
        )
        if cursor.rowcount < 1:
            conn.rollback()
            return jsonify(error="Request could not be withdrawn"), 409
        conn.commit()
    except Exception as e:
        current_app.logger.error("Failed to withdraw EPCR request %s: %s", request_id, e)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return jsonify(error="Failed to withdraw request"), 500
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    _cid = row.get("case_id") if row else None
    log_audit(
        current_user.username,
        f"EPCR access audit: {current_user.username} withdrew access request {request_id} for case {_cid}",
        patient_id=_cid,
    )
    return jsonify(ok=True, status="withdrawn"), 200


@internal_bp.route('/clinical/review_epcr_access_request', methods=['POST'])
@login_required
def review_epcr_access_request():
    allowed = {'clinical_lead', 'superuser', 'support_break_glass'}
    if (getattr(current_user, "role", "") or "").lower() not in allowed:
        return jsonify(error="Unauthorised access"), 403

    data = request.get_json(silent=True) or {}
    request_id = str(data.get("request_id") or "").strip()
    decision = str(data.get("decision") or "").strip().lower()
    review_note = (data.get("review_note") or "").strip()
    if not request_id or decision not in {"approve", "reject"}:
        return jsonify(error="Missing parameters"), 400

    conn = None
    cursor = None
    req = None
    requester_email = None
    try:
        conn = get_db_connection()
        mcur = conn.cursor()
        _epcr_maintain_access_requests(mcur)
        mcur.close()
        conn.commit()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT request_id, case_id, requested_by, status
            FROM epcr_access_requests
            WHERE request_id = %s
            LIMIT 1
            """,
            (request_id,),
        )
        req = cursor.fetchone()
        if not req:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            return jsonify(error="Request not found"), 404
        if (req.get("status") or "").lower() != "pending":
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            return jsonify(error="Request is no longer pending (reviewed, withdrawn, or expired)."), 409

        req_by = (req.get("requested_by") or "").strip().lower()
        if req_by and req_by == (current_user.username or "").strip().lower():
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            _cid_self = req.get("case_id")
            log_audit(
                current_user.username,
                f"EPCR access audit: denied self-approval of request {request_id} for case {_cid_self} by {current_user.username}",
                patient_id=_cid_self,
            )
            return jsonify(error="You cannot approve your own EPCR access request"), 403

        cursor.execute(
            "SELECT email FROM users WHERE LOWER(username)=LOWER(%s) LIMIT 1",
            (req.get("requested_by"),),
        )
        row = cursor.fetchone()
        requester_email = row.get("email") if row else None
    except Exception as e:
        current_app.logger.error("Failed to resolve requester email for EPCR request: %s", e)
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return jsonify(error="Failed to review request"), 500

    if decision == "approve":
        case_id = str(req.get("case_id") or "").strip()
        new_pin = generate_pin()
        expires_at = datetime.utcnow() + timedelta(minutes=10)
        try:
            cursor.execute(
                """
                UPDATE epcr_access_requests
                SET status='approved',
                    reviewed_by=%s,
                    reviewed_at=UTC_TIMESTAMP(),
                    review_note=%s,
                    access_code=%s,
                    code_expires_at=%s
                WHERE request_id=%s AND status='pending'
                """,
                (current_user.username, review_note or None, new_pin, expires_at, request_id),
            )
            if cursor.rowcount < 1:
                conn.rollback()
                if cursor:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
                return jsonify(error="Request already reviewed"), 409
            conn.commit()
        except Exception as e:
            conn.rollback()
            current_app.logger.error("Failed to approve EPCR request %s: %s", request_id, e)
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            return jsonify(error="Failed to approve request"), 500

        requested_by = (req.get("requested_by") or "").strip()
        log_audit(
            current_user.username,
            f"EPCR access audit: {current_user.username} approved case access for case {case_id} with pin {new_pin} (request_id={request_id}, requested_by={requested_by})",
            patient_id=case_id,
        )
        if requester_email:
            try:
                active_emailer = emailer or EmailManager()
                active_emailer.send_email(
                    subject=f"EPCR access approved for case #{case_id}",
                    body=(
                        f"Your EPCR access request {request_id} was approved by {current_user.username}.\n\n"
                        f"Case ID: {case_id}\n"
                        f"Access code (valid until {expires_at.isoformat()} UTC): {new_pin}\n\n"
                        f"Reviewer note: {review_note or 'None'}"
                    ),
                    recipients=[requester_email],
                )
            except Exception as e:
                current_app.logger.error("Failed sending EPCR approval email: %s", e)
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return jsonify(ok=True, status="approved"), 200

    case_id = str(req.get("case_id") or "").strip()
    try:
        cursor.execute(
            """
            UPDATE epcr_access_requests
            SET status='rejected',
                reviewed_by=%s,
                reviewed_at=UTC_TIMESTAMP(),
                review_note=%s
            WHERE request_id=%s AND status='pending'
            """,
            (current_user.username, review_note or None, request_id),
        )
        if cursor.rowcount < 1:
            conn.rollback()
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            return jsonify(error="Request already reviewed"), 409
        conn.commit()
    except Exception as e:
        conn.rollback()
        current_app.logger.error("Failed to reject EPCR request %s: %s", request_id, e)
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return jsonify(error="Failed to reject request"), 500

    log_audit(
        current_user.username,
        f"EPCR access audit: {current_user.username} rejected case access for case {case_id} (request_id={request_id}) — note: {review_note or 'No note'}",
        patient_id=case_id,
    )
    if requester_email:
        try:
            active_emailer = emailer or EmailManager()
            active_emailer.send_email(
                subject=f"EPCR access rejected for case #{case_id}",
                body=(
                    f"Your EPCR access request {request_id} was rejected by {current_user.username}.\n\n"
                    f"Reviewer note: {review_note or 'No note provided'}"
                ),
                recipients=[requester_email],
            )
        except Exception as e:
            current_app.logger.error("Failed sending EPCR rejection email: %s", e)
    if cursor:
        try:
            cursor.close()
        except Exception:
            pass
    if conn:
        try:
            conn.close()
        except Exception:
            pass
    return jsonify(ok=True, status="rejected"), 200


@internal_bp.route('/admin/unlock_epcr_case', methods=['POST'])
@login_required
def unlock_epcr_case():
    allowed = {'admin', 'superuser', 'clinical_lead', 'support_break_glass'}
    if (getattr(current_user, "role", "") or "").lower() not in allowed:
        return jsonify(error="Unauthorised access"), 403

    data = request.get_json(silent=True) or {}
    case_id = str(data.get("case_id") or data.get("id") or "").strip()
    pin = str(data.get("pin") or "").strip()
    justification = (data.get("justification") or "").strip()
    if not case_id or not pin or not justification:
        return jsonify(error="Missing parameters"), 400

    case_sid = _epcr_case_session_id(case_id)
    _br = _epcr_unlock_check_brute_block(case_sid)
    if _br is not None:
        return _br

    conn = None
    cursor = None
    req = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT request_id, case_id, access_code, code_expires_at, status, justification
            FROM epcr_access_requests
            WHERE case_id=%s
              AND LOWER(requested_by)=LOWER(%s)
              AND status='approved'
              AND used_at IS NULL
            ORDER BY reviewed_at DESC, created_at DESC
            LIMIT 1
            """,
            (case_id, current_user.username),
        )
        req = cursor.fetchone()
        if not req:
            cursor.close()
            conn.close()
            log_audit(
                current_user.username,
                f"EPCR access audit: denied unlock case {case_id} for {current_user.username} (no approved unused request for this user)",
                case_id=case_id,
            )
            return jsonify(error="No approved request for this case/user"), 403
        exp = req.get("code_expires_at")
        if not exp or datetime.utcnow() > exp or pin != str(req.get("access_code") or ""):
            cursor.close()
            conn.close()
            _epcr_unlock_note_failed_attempt(case_sid)
            log_audit(
                current_user.username,
                f"EPCR access audit: denied unlock case {case_id} for {current_user.username} (invalid or expired access pin)",
                case_id=case_id,
            )
            return jsonify(error="Invalid or expired access code"), 403

        # Second factor: proves the logged-in account holder is at the workstation (not only an
        # unattended session + shoulder-surfed one-time code). Checked before consuming the code.
        personal_pin = str(data.get("personal_pin") or "").strip()
        if not personal_pin:
            cursor.close()
            conn.close()
            return jsonify(error="Personal PIN is required"), 400
        pph = getattr(current_user, "personal_pin_hash", None) or ""
        if not isinstance(pph, str) or not pph.strip():
            cursor.close()
            conn.close()
            log_audit(
                current_user.username,
                f"EPCR access audit: denied unlock case {case_id} for {current_user.username} (no personal PIN configured)",
                case_id=case_id,
            )
            return jsonify(
                error=(
                    "No personal PIN is set on your Sparrow user account. "
                    "Use the user menu → Personal PIN to set a 6-digit PIN before opening EPCR case records."
                )
            ), 403
        if not AuthManager.verify_password(pph.strip(), personal_pin):
            cursor.close()
            conn.close()
            _epcr_unlock_note_failed_attempt(case_sid)
            log_audit(
                current_user.username,
                f"EPCR access audit: denied unlock case {case_id} for {current_user.username} (incorrect personal PIN)",
                case_id=case_id,
            )
            return jsonify(error="Incorrect personal PIN"), 403

        cursor.execute(
            """
            UPDATE epcr_access_requests
            SET used_at=UTC_TIMESTAMP(), used_by=%s
            WHERE request_id=%s AND used_at IS NULL
            """,
            (current_user.username, req.get("request_id")),
        )
        if cursor.rowcount < 1:
            conn.rollback()
            return jsonify(error="Access code already used"), 409
        conn.commit()
    except Exception as e:
        current_app.logger.error("Failed EPCR unlock for case %s: %s", case_id, e)
        return jsonify(error="Unlock verification failed"), 500
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    _epcr_unlock_clear_brute_keys(case_sid)
    _ttl = _epcr_session_unlock_ttl_seconds()
    session[f"epcr_unlocked_{case_sid}"] = True
    session[f"epcr_unlock_expires_{case_sid}"] = time.time() + _ttl
    session[f"epcr_access_audit_{case_sid}"] = {
        "username": current_user.username,
        "justification": justification,
        "unlocked_at": datetime.now(timezone.utc).isoformat(),
    }
    session.modified = True
    requester_reason = (req.get("justification") or "").strip() if req else ""
    extra_req = (
        f" — original access request reason: {requester_reason}"
        if requester_reason
        else ""
    )
    log_audit(
        current_user.username,
        f"EPCR access audit: {current_user.username} used approved one-time access code (value not logged) to access case {case_id} for the justification: {justification}{extra_req} (personal PIN verified; request_id={req.get('request_id') if req else 'n/a'})",
        patient_id=case_id,
    )
    return jsonify(ok=True, redirect_url=url_for('medical_records_internal.view_epcr_case', case_id=case_sid)), 200

@internal_bp.route('/clinical/epcr/delete/<int:case_id>', methods=['POST'])
@login_required
def delete_epcr(case_id):
    """
    Deletes the specified EPCR case.
    Only clinical lead, superuser, or admin may delete.
    Returns a JSON response indicating success/failure.
    """
    if not hasattr(current_user, 'role') or current_user.role.lower() not in ["clinical_lead", "superuser", "admin", "support_break_glass"]:
        return jsonify({"error": "Unauthorised access"}), 403
    if not _epcr_session_unlock_valid(case_id):
        log_audit(
            current_user.username,
            f"EPCR access audit: denied delete case {case_id} for {current_user.username} (valid unlock session required)",
            case_id=case_id,
        )
        return jsonify({"error": "Unlock this case with an access code and justification before deleting."}), 403

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM cases WHERE id = %s", (case_id,))
        conn.commit()
        logger.info("User %s deleted case %s.", current_user.username, case_id)
        log_audit(
            current_user.username,
            f"EPCR access audit: {current_user.username} deleted case {case_id}",
            case_id=case_id,
        )
        response = {"success": True, "case_id": case_id}
    except Exception as e:
        conn.rollback()
        logger.error("Error deleting case %s: %s", case_id, str(e))
        response = {"success": False, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


    return jsonify(response)


@internal_bp.route("/clinical/epcr/<int:case_id>/admin-force-close", methods=["POST"])
@login_required
def admin_force_close_epcr(case_id):
    """
    Close an in-progress EPCR from Sparrow when clinical completion in Cura is impossible or blocked.
    Requires an allowed role (see EPCR_ADMIN_FORCE_CLOSE_ROLES env), personal PIN, category, audit notes,
    and typing FORCE CLOSE. Does not require prior case unlock — this is a separate governance path.
    """
    if not _user_may_epcr_admin_force_close():
        log_audit(
            getattr(current_user, "username", "") or "unknown",
            f"EPCR admin force-close denied for case {case_id} (role not in allowlist)",
            case_id=case_id,
        )
        return jsonify({"success": False, "error": "Your role is not permitted to force-close EPCR cases."}), 403

    data = request.get_json(silent=True) or {}
    ok_pin, pin_err = _epcr_verify_personal_pin_for_admin_action(data)
    if not ok_pin:
        return jsonify({"success": False, "error": pin_err}), 400

    confirm = (data.get("confirm_phrase") or "").strip().upper()
    if confirm != "FORCE CLOSE":
        return jsonify(
            {"success": False, "error": "Type the phrase FORCE CLOSE (uppercase) in the confirmation field."}
        ), 400

    cat = (data.get("category") or "").strip().lower()
    if cat not in _EPCR_FORCE_CLOSE_CATEGORY_LABELS:
        return jsonify({"success": False, "error": "Select a valid closure category."}), 400

    notes = (data.get("audit_notes") or "").strip()
    if len(notes) < 20:
        return jsonify(
            {"success": False, "error": "Audit notes must be at least 20 characters (explain why clinicians could not close)."}
        ), 400

    closed_at_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    closed_at_iso = closed_at_str.replace(" ", "T") + "Z"
    actor = getattr(current_user, "username", "") or ""
    role = (getattr(current_user, "role", "") or "").strip().lower()
    cat_label = _EPCR_FORCE_CLOSE_CATEGORY_LABELS.get(cat, cat)
    stamp = {
        "closedAtUtc": closed_at_str,
        "byUsername": actor,
        "byRole": role,
        "category": cat,
        "categoryLabel": cat_label,
        "auditNotes": notes,
    }

    conn = get_db_connection()
    cursor = conn.cursor()
    http_status = 200
    response_body = None
    try:
        cursor.execute(
            "SELECT data, status, record_version FROM cases WHERE id = %s",
            (case_id,),
        )
        row = cursor.fetchone()
        if not row:
            response_body = {"success": False, "error": "Case not found"}
            http_status = 404
        else:
            data_str, db_status = row[0], (row[1] or "").strip().lower()
            if db_status == "closed":
                response_body = {"success": False, "error": "Case is already closed."}
                http_status = 400
            else:
                case_payload = _parse_case_json(data_str)
                case_payload["epcrAdminForceClose"] = stamp
                case_payload["status"] = "closed"
                case_payload["closedAt"] = closed_at_iso

                payload_str = json.dumps(case_payload, ensure_ascii=False, default=str)
                now_db = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(
                    """
                    UPDATE cases SET data=%s, status=%s, closed_at=%s, updated_at=%s, record_version=record_version+1
                    WHERE id=%s
                    """,
                    (payload_str, "closed", closed_at_str, now_db, case_id),
                )
                _sync_case_patient_match_meta(cursor, case_id, case_payload)
                conn.commit()
                log_audit(
                    actor,
                    (
                        f"EPCR ADMIN FORCE-CLOSE case {case_id}: category={cat} "
                        f"({cat_label}); role={role}; notes_len={len(notes)}"
                    ),
                    case_id=case_id,
                    reason=notes[:500],
                )
                response_body = {
                    "success": True,
                    "case_id": case_id,
                    "closed_at": closed_at_str,
                    "closed_at_display": closed_at_str,
                }
    except Exception as e:
        conn.rollback()
        logger.exception("admin_force_close_epcr case %s: %s", case_id, e)
        response_body = {"success": False, "error": "Could not close case (server error)."}
        http_status = 500
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

    return jsonify(response_body), http_status


@internal_bp.route('/care_company/list', methods=['GET'])
@login_required
def list_care_company_users():
    """
    Lists all care company users along with the number of patients assigned to each.
    Only allowed for admin and superuser roles.
    """
    if current_user.role.lower() not in ['admin', 'superuser', 'clinical_lead', 'support_break_glass']:
        flash("Unauthorised access", "danger")
        return redirect(url_for('routes.dashboard'))
    if not care_company_feature_enabled():
        flash(
            "Care company is disabled. Enable with CURA_ENABLE_CARE_COMPANY_PORTAL=true if you need this feature.",
            "warning",
        )
        return redirect(url_for('medical_records_internal.landing'))
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM care_company_users")
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    for user in users:
        user['assigned_patients'] = get_assigned_patient_count(user['id'])
    return render_template("care_company_users/list.html", users=users, config=core_manifest)

@internal_bp.route('/care_company/add', methods=['GET', 'POST'])
@login_required
def add_care_company_user():
    """
    Adds a new care company user.
    Only allowed for admin and superuser.
    """
    if current_user.role.lower() not in ['admin', 'superuser', 'clinical_lead', 'support_break_glass']:
        flash("Unauthorised access", "danger")
        return redirect(url_for('routes.dashboard'))
    if not care_company_feature_enabled():
        flash(
            "Care company is disabled. Enable with CURA_ENABLE_CARE_COMPANY_PORTAL=true if you need this feature.",
            "warning",
        )
        return redirect(url_for('medical_records_internal.landing'))
    if request.method == 'POST':
        username = request.form.get('username')
        email = request.form.get('email')
        company_name = request.form.get('company_name')
        contact_phone = request.form.get('contact_phone')
        contact_address = request.form.get('contact_address')
        password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')
        company_pin = request.form.get('company_pin')
        
        if password != confirm_password:
            flash("Passwords do not match.", "danger")
            return redirect(url_for('medical_records_internal.add_care_company_user'))
        
        new_password_hash = AuthManager.hash_password(password)
        new_company_pin_hash = None
        if company_pin and company_pin.strip() != "":
            new_company_pin_hash = AuthManager.hash_password(company_pin)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO care_company_users 
            (username, email, password_hash, company_name, contact_phone, contact_address, company_pin_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (username, email, new_password_hash, company_name, contact_phone, contact_address, new_company_pin_hash))
        conn.commit()
        cursor.close()
        conn.close()
        
        flash("Care company user added successfully.", "success")
        return redirect(url_for('medical_records_internal.list_care_company_users'))
    
    return render_template("care_company_users/add.html", config=core_manifest)

@internal_bp.route('/care_company/edit/<user_id>', methods=['GET', 'POST'])
@login_required
def edit_care_company_user(user_id):
    """
    Edits an existing care company user.
    Only allowed for admin and superuser.
    """
    if current_user.role.lower() not in ['admin', 'superuser', 'clinical_lead', 'support_break_glass']:
        flash("Unauthorised access", "danger")
        return redirect(url_for('routes.dashboard'))
    if not care_company_feature_enabled():
        flash(
            "Care company is disabled. Enable with CURA_ENABLE_CARE_COMPANY_PORTAL=true if you need this feature.",
            "warning",
        )
        return redirect(url_for('medical_records_internal.landing'))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM care_company_users WHERE id = %s", (user_id,))
    user = cursor.fetchone()
    if not user:
        cursor.close()
        conn.close()
        flash("User not found", "danger")
        return redirect(url_for('medical_records_internal.list_care_company_users'))
    
    if request.method == 'POST':
        email = request.form.get('email')
        company_name = request.form.get('company_name')
        contact_phone = request.form.get('contact_phone')
        contact_address = request.form.get('contact_address')
        new_password = request.form.get('new_password')
        confirm_new_password = request.form.get('confirm_new_password')
        new_company_pin = request.form.get('new_company_pin')
        
        update_fields = {
            "email": email,
            "company_name": company_name,
            "contact_phone": contact_phone,
            "contact_address": contact_address
        }
        
        if new_password:
            if new_password != confirm_new_password:
                flash("New passwords do not match.", "danger")
                return redirect(url_for('medical_records_internal.edit_care_company_user', user_id=user_id))
            update_fields["password_hash"] = AuthManager.hash_password(new_password)
        
        if new_company_pin and new_company_pin.strip() != "":
            update_fields["company_pin_hash"] = AuthManager.hash_password(new_company_pin)
        
        set_clause = ", ".join([f"{k} = %s" for k in update_fields.keys()])
        params = list(update_fields.values())
        params.append(user_id)
        cursor.execute(f"UPDATE care_company_users SET {set_clause} WHERE id = %s", tuple(params))
        conn.commit()
        cursor.close()
        conn.close()
        flash("User updated successfully.", "success")
        log_audit(current_user.username, "Admin edited care company user", patient_id=user_id)
        return redirect(url_for('medical_records_internal.list_care_company_users'))
    
    cursor.close()
    conn.close()
    return render_template("care_company_users/edit.html", user=user, config=core_manifest)

@internal_bp.route('/care_company/delete/<user_id>', methods=['POST'])
@login_required
def delete_care_company_user(user_id):
    """
    Deletes a care company user.
    Only allowed for admin and superuser.
    """
    if current_user.role.lower() not in ['admin', 'superuser', 'clinical_lead', 'support_break_glass']:
        flash("Unauthorised access", "danger")
        return redirect(url_for('routes.dashboard'))
    if not care_company_feature_enabled():
        flash(
            "Care company is disabled. Enable with CURA_ENABLE_CARE_COMPANY_PORTAL=true if you need this feature.",
            "warning",
        )
        return redirect(url_for('medical_records_internal.landing'))
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM care_company_users WHERE id = %s", (user_id,))
    conn.commit()
    cursor.close()
    conn.close()
    flash("User deleted successfully.", "success")
    log_audit(current_user.username, "Admin deleted care company user", patient_id=user_id)
    return redirect(url_for('medical_records_internal.list_care_company_users'))

# =============================================================================
# PUBLIC BLUEPRINT (Care Company Interface)
# =============================================================================
public_template_folder = os.path.join(os.path.dirname(__file__), 'templates', 'public')

# Root URL /case-access — hospital / public EPCR lookup (e.g. epcr.example.com → /case-access).
# Not under /care_company; no care-company session or portal guards.
case_access_public_bp = Blueprint(
    'epcr_public_case_access',
    __name__,
    url_prefix='',
    template_folder=public_template_folder,
)


def split_camel(value):
    return re.sub(r'([a-z])([A-Z])', r'\1 \2', value).title()


@case_access_public_bp.app_template_filter('splitCamel')
def split_camel_filter(value):
    return split_camel(value)


def _register_case_access_pdf_jinja_helpers(bp):
    """``case_access_pdf.html`` is rendered from internal + public blueprints — register on both."""
    from .case_access_render import (
        epcr_breathing_case_access_view,
        epcr_content_meaningful,
        epcr_fmt_reversible_cause,
        epcr_get_section,
        epcr_head_injury_display,
        epcr_oohca_reversible_pdf_flags,
        epcr_oohca_reversible_path_label,
        epcr_oohca_shock_energy_display,
        epcr_oohca_unified_timeline_rows,
        epcr_post_rosc_meaningful,
    )

    bp.app_template_global("epcr_get_section")(epcr_get_section)
    bp.app_template_global("epcr_breathing_case_access_view")(epcr_breathing_case_access_view)
    bp.app_template_global("epcr_meaningful")(epcr_content_meaningful)
    bp.app_template_global("epcr_oohca_reversible_pdf_flags")(epcr_oohca_reversible_pdf_flags)
    bp.app_template_global("epcr_oohca_reversible_path_label")(epcr_oohca_reversible_path_label)
    bp.app_template_global("epcr_oohca_shock_energy_display")(epcr_oohca_shock_energy_display)
    bp.app_template_global("epcr_oohca_unified_timeline_rows")(epcr_oohca_unified_timeline_rows)
    bp.app_template_global("epcr_post_rosc_meaningful")(epcr_post_rosc_meaningful)
    bp.app_template_filter("epcr_reversible")(epcr_fmt_reversible_cause)
    bp.app_template_filter("epcr_head_injury_display")(epcr_head_injury_display)


_register_case_access_pdf_jinja_helpers(internal_bp)
_register_case_access_pdf_jinja_helpers(case_access_public_bp)


def process_images(obj, temp_dir):
    """
    Recursively replace data:image/*;base64,... strings with temp file paths.

    Intended only for tooling that needs files on disk (e.g. some PDF pipelines).
    Do not use for browser HTML case-access views — file paths are not valid ``<img src>`` URLs.
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str) and value.startswith('data:image'):
                try:
                    header, encoded = value.split(',', 1)
                    ext = header.split('/')[1].split(';')[0]
                    filename = f"{key}.{ext}"
                    filepath = os.path.join(temp_dir, filename)
                    with open(filepath, "wb") as f:
                        f.write(base64.b64decode(encoded))
                    current_app.logger.debug("Processed image for key '%s', saved to: %s", key, filepath)
                    obj[key] = filepath
                except Exception as e:
                    current_app.logger.error("Failed to process image for key '%s': %s", key, e)
            else:
                process_images(value, temp_dir)
    elif isinstance(obj, list):
        for item in obj:
            process_images(item, temp_dir)


@case_access_public_bp.route('/case-access', methods=['GET', 'POST'])
def epcr_public_case_access():
    current_app.logger.debug("Entered /case-access route.")
    error = None
    case_data = None

    if request.method == 'POST':
        current_app.logger.debug("Processing POST request.")

        def _redact_case_ref(v):
            v = (v or "").strip()
            if len(v) <= 4:
                return "***"
            return ("*" * (len(v) - 4)) + v[-4:]

        case_ref = (request.form.get('case_ref') or "").strip()
        dob_in = (request.form.get('dob') or "").strip()
        access_pin_in = (request.form.get('access_pin') or "").strip()

        # Never log DOB/PIN. Only log redacted case reference + minimal request metadata.
        current_app.logger.info(
            "Public case-access attempt: case_ref=%s ip=%s ua=%s",
            _redact_case_ref(case_ref),
            (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip(),
            (request.headers.get("User-Agent") or "")[:200],
        )

        if not (case_ref and case_ref.isdigit() and len(case_ref) == 10):
            error = "Case Reference Number must be exactly 10 digits."
            current_app.logger.warning("Public case-access validation failed: case_ref=%s", _redact_case_ref(case_ref))
            return render_template('case_access.html', error=error)

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            query = (
                "SELECT data, status, created_at, closed_at, updated_at, "
                "dispatch_reference, primary_callsign, dispatch_synced_at, record_version "
                "FROM cases WHERE id = %s"
            )
            cursor.execute(query, (case_ref,))
            row = cursor.fetchone()
            if row:
                (
                    data_str,
                    status,
                    created_at,
                    closed_at,
                    updated_at,
                    dispatch_reference,
                    primary_callsign,
                    dispatch_synced_at,
                    record_version,
                ) = row
                case_data = json.loads(data_str)
                case_data['status'] = status
                case_data['createdAt'] = created_at.isoformat() if created_at else None
                case_data['closedAt'] = closed_at.isoformat() if closed_at else None
                case_data['updatedAt'] = updated_at.isoformat() if updated_at else None
                _merge_epcr_link_into_case_json(
                    case_data, dispatch_reference, primary_callsign, dispatch_synced_at, record_version
                )
                current_app.logger.debug("Public case-access case loaded: case_ref=%s", _redact_case_ref(case_ref))
            else:
                error = "The details you entered did not match our records."
                current_app.logger.warning("Public case-access no case found: case_ref=%s", _redact_case_ref(case_ref))
            cursor.close()
            conn.close()
        except Exception as e:
            error = "An unexpected error occurred. Please try again later."
            current_app.logger.exception(
                "Public case-access database error: case_ref=%s",
                _redact_case_ref(case_ref),
            )
            return render_template('case_access.html', error=error)

        if not case_data:
            current_app.logger.warning("Public case-access empty case data: case_ref=%s", _redact_case_ref(case_ref))
            return render_template('case_access.html', error=error)

        pt_info = None
        for section in case_data.get('sections', []):
            if section.get('name', '').lower() == 'patientinfo':
                pt_info = section.get('content', {}).get('ptInfo')
                break
        dob_case = ""
        if pt_info and pt_info.get("dob"):
            dob_case = str(pt_info.get("dob") or "").strip()

        if not dob_case:
            error = "The details you entered did not match our records."
            current_app.logger.warning(
                "Public case-access DOB missing in case: case_ref=%s",
                _redact_case_ref(case_ref),
            )
            return render_template('case_access.html', error=error)

        # Compare canonical YYYY-MM-DD strings.
        dob_in_canon = dob_in
        if dob_in_canon and "T" in dob_in_canon:
            dob_in_canon = dob_in_canon.split("T", 1)[0]
        dob_case_canon = dob_case
        if dob_case_canon and "T" in dob_case_canon:
            dob_case_canon = dob_case_canon.split("T", 1)[0]

        if not dob_in_canon or dob_in_canon != dob_case_canon:
            error = "The details you entered did not match our records."
            current_app.logger.warning(
                "Public case-access DOB mismatch: case_ref=%s",
                _redact_case_ref(case_ref),
            )
            return render_template('case_access.html', error=error)
        case_data['ptInfo'] = pt_info

        incident_log = None
        for section in case_data.get('sections', []):
            if section.get('name', '').lower() == 'incident log':
                incident_log = section.get('content', {}).get('incident', {})
                break
        if not incident_log:
            incident_log = {}

        # Validate PIN against the raw case value, before substituting display defaults.
        pin_case = str(incident_log.get("pinCode") or "").strip()
        if not pin_case or access_pin_in != pin_case:
            error = "The details you entered did not match our records."
            current_app.logger.warning(
                "Public case-access PIN mismatch: case_ref=%s",
                _redact_case_ref(case_ref),
            )
            return render_template('case_access.html', error=error)

        # Only after successful validation, prepare display-friendly incident fields.
        case_data['incident'] = {key: (value if value else 'Not Provided') for key, value in incident_log.items()}

        # Do not call process_images() here: it replaces data: URIs with temp file paths, which are not
        # valid browser URLs and break <img src> on this HTML view. Admin/clinical UIs keep embedded images.

        # Minimal audit trail (case-keyed audit is added in later refactor).
        try:
            log_audit(
                "public",
                f"Public accessed case (ref={_redact_case_ref(case_ref)})",
                case_id=int(case_ref) if str(case_ref).isdigit() else None,
            )
        except Exception:
            current_app.logger.debug("Public case-access audit write skipped")

        try:
            conn_rtc = get_db_connection()
            cur_rtc = conn_rtc.cursor()
            try:
                inject_rtc_case_access_inline_images(int(case_ref), case_data, cur_rtc)
                inject_oohca_role_ecg_case_access_images(int(case_ref), case_data, cur_rtc)
            finally:
                cur_rtc.close()
                conn_rtc.close()
        except Exception:
            current_app.logger.exception(
                "inject case-access inline images (RTC/OOHCA) public case-access ref=%s",
                _redact_case_ref(case_ref),
            )

        return render_template('case_access_pdf.html', case_data=case_data)
    current_app.logger.debug("GET request; rendering case_access.html.")
    return render_template('case_access.html', error=error)


public_bp = Blueprint(
    'care_company',
    __name__,
    url_prefix='/care_company',
    template_folder=public_template_folder
)


@public_bp.before_request
def _care_company_portal_guard():
    # Legacy bookmark: GET /care_company/case-access → root /case-access (works even when portal is off).
    if (
        request.method == "GET"
        and request.path.rstrip("/") == "/care_company/case-access"
    ):
        return None
    if care_company_feature_enabled():
        return None
    abort(
        503,
        description=(
            "The care company portal is disabled by default. "
            "Set CURA_ENABLE_CARE_COMPANY_PORTAL=true to enable (CURA_DISABLE_CARE_COMPANY_PORTAL forces off)."
        ),
    )


@public_bp.route("/case-access", methods=["GET"])
def legacy_care_company_case_access_redirect():
    """Old path when case access lived under the care-company blueprint."""
    return redirect(url_for("epcr_public_case_access.epcr_public_case_access"), code=301)


@public_bp.route('/login', methods=['GET', 'POST'])
def care_company_login():
    if request.method == 'POST':
        try:
            username = request.form.get('username')
            password = request.form.get('password')
            user = CareCompanyUser.get_user_by_username(username)
            if not user or not user.check_password(password):
                flash("Invalid username or password. Please check your credentials and try again.", "error")
                return render_template("care_company_login.html", config=core_manifest)
            remember = request.form.get('remember') == 'on'
            login_user(user, remember=remember)
            flash("Logged in successfully", "success")
            log_audit(user.id, "Care company user logged in")
            return redirect(url_for('care_company.dashboard'))
        except Exception:
            flash("An unexpected error occurred. Please try again later.", "error")
            return render_template("care_company_login.html", config=core_manifest)
    return render_template("care_company_login.html", config=core_manifest)

@public_bp.route('/logout')
@login_required
def care_company_logout():

    flash("You have been logged out", "success")
    log_audit(current_user.username, "Care company user logged out")
    logout_user()
    session.clear()
    return redirect(url_for('care_company.care_company_login'))

@public_bp.route('/reset_password_request', methods=['GET', 'POST'])
def reset_password_request():
    if request.method == 'POST':
        email = request.form.get('email')
        user = CareCompanyUser.get_user_by_email(email)
        if user:
            token = user.generate_reset_token()
            send_reset_email(user, token)
        
        flash("If that account exists. An email has been sent with instructions to reset your password.", "info")

        return redirect(url_for('care_company.care_company_login'))
    return render_template("reset_password_request.html", config=core_manifest)

@public_bp.route('/reset_password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    user = CareCompanyUser.verify_reset_token(token)
    if not user:
        flash("The reset token is invalid or has expired.", "error")
        return redirect(url_for('care_company.reset_password_request'))
    if request.method == 'POST':
        new_password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')
        if new_password != confirm_password:
            flash("Passwords do not match", "error")
            return render_template("reset_password.html", token=token, config=core_manifest)
        new_hash = AuthManager.hash_password(new_password)
        CareCompanyUser.update_password(user.id, new_hash)
        log_audit(user.id, "Care company user reset password")
        flash("Your password has been reset.", "success")
        return redirect(url_for('care_company.care_company_login'))
    return render_template("reset_password.html", token=token, config=core_manifest)

@public_bp.before_request
def ensure_vita_care_portal_user():
    # If the user isn't authenticated yet, let the login_required decorator handle it.
    if not current_user.is_authenticated:
        return
    # Now that the user is authenticated, ensure they are from the Vita-Care-Portal module.
    if not hasattr(current_user, 'role') or current_user.role != "Vita-Care-Portal":
        return jsonify({"error": "Unauthorised access"}), 403

@public_bp.route('/')
@login_required
def dashboard():
    log_audit(current_user.username, "Care company dashboard accessed")
    search = request.args.get('q')
    patients = Patient.get_all(search, company_id=current_user.id)
    current_date = datetime.utcnow().date()
    for p in patients:
        if p.get("date_of_birth"):
            p["age"] = calculate_age(p["date_of_birth"], current_date)
        else:
            p["age"] = "N/A"
    return render_template("care_company_home.html", clients=patients, config=core_manifest)

@public_bp.route('/add_client_record', methods=['POST'])
@login_required
def add_client_record():
    nhs_number = request.form.get("nhs_number")
    first_name = request.form.get('first_name')
    middle_name = request.form.get('middle_name')
    last_name = request.form.get('last_name')
    address = request.form.get('address')
    contact_number = request.form.get("contact_number")
    gp_details = request.form.get('gp_details') or None
    medical_conditions = request.form.get('medical_conditions') or None
    allergies = request.form.get('allergies') or None
    medications = request.form.get('medications') or None
    previous_visit_records = request.form.get('previous_visit_records') or None
    package_type = "Bronze"
    notes = request.form.get('notes') or None
    message_log = request.form.get('message_log') or None
    access_requirements = request.form.get('access_requirements') or None
    payment_details = request.form.get('payment_details') or None
    next_of_kin_details = request.form.get('next_of_kin_details') or None
    lpa_details = request.form.get('lpa_details') or None
    resuscitation_directive = request.form.get('resuscitation_directive') or None
    documents = request.form.get('documents') or None
    dob_str = request.form.get('date_of_birth')
    weight = request.form.get('weight') or None
    gender = request.form.get('gender') or None
    postcode = request.form.get('postcode')
    try:
        dob = datetime.strptime(dob_str, "%Y-%m-%d").date() if dob_str else None
    except ValueError:
        dob = None
    new_record = {
        "nhs_number": nhs_number,
        "first_name": first_name,
        "middle_name": middle_name,
        "last_name": last_name,
        "address": address,
        "contact_number": contact_number,
        "gp_details": gp_details,
        "medical_conditions": medical_conditions,
        "allergies": allergies,
        "medications": medications,
        "previous_visit_records": previous_visit_records,
        "package_type": package_type,
        "notes": notes,
        "message_log": message_log,
        "access_requirements": access_requirements,
        "payment_details": payment_details,
        "next_of_kin_details": next_of_kin_details,
        "lpa_details": lpa_details,
        "resuscitation_directive": resuscitation_directive,
        "documents": documents,
        "date_of_birth": dob,
        "weight": weight,
        "gender": gender,
        "postcode": postcode,
        "care_company_id": current_user.id  # Assign the current care company user id
    }
    try:
        Patient.add_patient(**new_record)
        log_audit(current_user.username, f"Added client record: {first_name} {last_name}")
    except Exception as e:
        logger.error("Error adding client record: %s", str(e))
        flash("Error adding client record", "danger")
        return redirect(url_for('care_company.dashboard'))
    flash("Client record added successfully.", "success")
    return redirect(url_for('care_company.dashboard'))

from flask import (
    render_template, request, redirect, url_for, flash, session
)
from flask_login import login_required, current_user
from datetime import datetime
import json

@public_bp.route('/edit_client_record/<id>', methods=['GET', 'POST'])
@login_required
def edit_client_record(id):
    # POST: save updates
    if request.method == 'POST':
        client_existing = Patient.get_by_id(id)
        if not client_existing:
            flash("Client record not found.", "danger")
            return redirect(url_for('care_company.dashboard'))

        # simple scalars
        first_name   = request.form.get('first_name', '').strip()   or client_existing.get('first_name')
        middle_name  = request.form.get('middle_name', '').strip()  or client_existing.get('middle_name')
        last_name    = request.form.get('last_name', '').strip()    or client_existing.get('last_name')
        address      = request.form.get('address', '').strip()      or client_existing.get('address')
        postcode     = request.form.get('postcode', '').strip()     or client_existing.get('postcode')
        gender       = request.form.get('gender', '').strip()       or client_existing.get('gender')
        dob_str      = request.form.get('date_of_birth', '').strip()
        try:
            dob = datetime.strptime(dob_str, "%Y-%m-%d").date() if dob_str else client_existing.get('date_of_birth')
        except ValueError:
            dob = client_existing.get('date_of_birth')

        # helper for list fields
        def load_list(field):
            raw = request.form.get(field, '').strip()
            if raw:
                try:
                    arr = json.loads(raw)
                    return arr if isinstance(arr, list) else []
                except:
                    return []
            stored = client_existing.get(field, '[]')
            try:
                arr = json.loads(stored)
                return arr if isinstance(arr, list) else []
            except:
                return []

        medical_conditions     = load_list('medical_conditions')
        allergies              = load_list('allergies')
        medications            = load_list('medications')
        access_requirements    = load_list('access_requirements')
        previous_visit_records = load_list('previous_visit_records')
        notes                  = load_list('notes')
        message_log            = load_list('message_log')
        next_of_kin_details    = load_list('next_of_kin_details')
        lpa_details            = load_list('lpa_details')

        # helper for dict fields
        def load_dict(field):
            raw = client_existing.get(field, '')
            if isinstance(raw, str) and raw.strip():
                try:
                    obj = json.loads(raw)
                    return obj if isinstance(obj, dict) else {}
                except:
                    return {}
            return raw if isinstance(raw, dict) else {}

        unlocked = session.get(f'unlocked_{id}', False)

        if unlocked:
            # user provided these in form
            gp_details = {
                "name":    request.form.get('gp_name','').strip(),
                "address": request.form.get('gp_address','').strip(),
                "contact": request.form.get('gp_contact','').strip(),
                "email":   request.form.get('gp_email','').strip()
            }
            weight = {
                "weight":       request.form.get('weight_value','').strip(),
                "date_weighed": request.form.get('date_weighed','').strip(),
                "source":       request.form.get('weight_source','').strip()
            }
            payment_details = {
                "payment_method": request.form.get('payment_method','').strip(),
                "billing_email":  request.form.get('billing_email','').strip()
            }
            # resuscitation docs
            docs = []
            for fld, label in [
                ('doc_dnar','DNAR'),
                ('doc_respect','Respect Form'),
                ('doc_advanced','Advanced Directive'),
                ('doc_living','Living Will'),
                ('doc_lpa','LPA'),
                ('doc_care','Care Plan')
            ]:
                if request.form.get(fld):
                    docs.append(label)
            resuscitation_directive = {
                "for_resuscitation": request.form.get('resus_option','').strip(),
                "documents":         docs
            }
        else:
            # preserve existing values
            gp_details               = load_dict('gp_details')
            weight                   = load_dict('weight')
            payment_details          = load_dict('payment_details')
            resuscitation_directive  = load_dict('resuscitation_directive')

        # assemble and persist
        update_fields = {
            "first_name":             first_name,
            "middle_name":            middle_name,
            "last_name":              last_name,
            "address":                address,
            "date_of_birth":          dob,
            "gender":                 gender,
            "postcode":               postcode,
            "gp_details":             json.dumps(gp_details),
            "weight":                 json.dumps(weight),
            "payment_details":        json.dumps(payment_details),
            "resuscitation_directive":json.dumps(resuscitation_directive),
            "medical_conditions":     json.dumps(medical_conditions),
            "allergies":              json.dumps(allergies),
            "medications":            json.dumps(medications),
            "previous_visit_records": json.dumps(previous_visit_records),
            "access_requirements":    json.dumps(access_requirements),
            "notes":                  json.dumps(notes),
            "message_log":            json.dumps(message_log),
            "next_of_kin_details":    json.dumps(next_of_kin_details),
            "lpa_details":            json.dumps(lpa_details),
        }

        try:
            Patient.update_patient(id, **update_fields)
            log_audit(current_user.username, f"Edited client record: {id}", patient_id=id)
            flash("Client record updated successfully.", "success")
        except Exception as e:
            logger.error("Error updating client record: %s", e)
            flash("Error updating client record", "danger")
            return redirect(url_for('care_company.view_client_record', id=id))

        return redirect(url_for('care_company.view_client_record', id=id))

    # GET: render edit form
    client = Patient.get_by_id(id)
    if not client or client.get("care_company_id") != current_user.id:
        flash("Client record not found or access denied.", "danger")
        return redirect(url_for('care_company.dashboard'))

    log_audit(current_user.username, f"Accessed edit view for client record: {id}", patient_id=id)
    unlocked = session.get(f'unlocked_{id}', False)

    # parse dict fields
    dict_keys = ['gp_details','payment_details','resuscitation_directive','weight']
    for key in dict_keys:
        raw = client.get(key)
        try:
            parsed = json.loads(raw.strip()) if isinstance(raw, str) else raw
            client[key] = parsed if isinstance(parsed, dict) else {}
        except:
            client[key] = {}

    # parse list fields
    list_keys = [
        'medical_conditions','allergies','medications',
        'previous_visit_records','access_requirements',
        'next_of_kin_details','lpa_details'
    ]
    for key in list_keys:
        raw = client.get(key)
        try:
            parsed = json.loads(raw.strip()) if isinstance(raw, str) else raw
            client[key] = parsed if isinstance(parsed, list) else []
        except:
            client[key] = []

    # mask if locked
    if not unlocked:
        for field in ['gp_details','payment_details','resuscitation_directive','weight','next_of_kin_details','lpa_details']:
            client[field] = "*******"

    age = calculate_age(client.get("date_of_birth")) if client.get("date_of_birth") else "N/A"
    return render_template(
        "edit_client.html",
        client=client,
        unlocked=unlocked,
        age=age,
        config=core_manifest
    )

@public_bp.route('/view_client_record/<id>')
@login_required
def view_client_record(id):
    client = Patient.get_by_id(id)
    if not client:
        return render_template("view_client.html",
                               error="Client record not found",
                               config=core_manifest)
    if client.get("care_company_id") != current_user.id:
        flash("You do not have access to this record", "danger")
        return redirect(url_for("care_company.dashboard"))

    unlocked = session.get(f'unlocked_{id}', False)

    # --- parse single‑object JSON → dicts only ---
    dict_keys = [
        'gp_details',
        'payment_details',
        'resuscitation_directive',
        'weight',
    ]
    for key in dict_keys:
        raw = client.get(key)
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw.strip())
                client[key] = parsed if isinstance(parsed, dict) else {}
            except Exception:
                client[key] = {}
        else:
            client[key] = raw if isinstance(raw, dict) else {}

    # --- parse JSON arrays → lists of dicts ---
    list_keys = [
        'medical_conditions',
        'allergies',
        'medications',
        'previous_visit_records',
        'access_requirements',
        'next_of_kin_details',
        'lpa_details',
        'notes',
        'message_log'
    ]
    for key in list_keys:
        raw = client.get(key)
        if isinstance(raw, list):
            continue
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw.strip())
                if isinstance(parsed, list):
                    client[key] = parsed
                elif isinstance(parsed, dict):
                    client[key] = [parsed]
                else:
                    client[key] = []
            except Exception:
                client[key] = []
        else:
            client[key] = []

    # --- mask sensitive fields if locked ---
    if not unlocked:
        sensitive = [
            'gp_details',
            'payment_details',
            'resuscitation_directive',
            'weight',
            'medical_conditions',
            'allergies',
            'medications',
            'previous_visit_records',
            'access_requirements',
            'next_of_kin_details',
            'lpa_details',
        ]
        for field in sensitive:
            if field in client:
                client[field] = "*******"

    age = calculate_age(client.get("date_of_birth")) if client.get("date_of_birth") else "N/A"
    log_audit(current_user.username, "Care company viewed client record", patient_id=id)

    return render_template(
        "view_client.html",
        client=client,
        age=age,
        unlocked=unlocked,
        config=core_manifest
    )
@public_bp.route('/search')
@login_required
def search():
    search = request.args.get('q')
    patients = Patient.get_all(search)
    current_date = datetime.utcnow().date()
    for p in patients:
        if p.get("date_of_birth"):
            p["age"] = calculate_age(p["date_of_birth"], current_date)
        else:
            p["age"] = "N/A"
    return jsonify({"clients": patients})
@public_bp.route('/add_message_log_entry/<id>', methods=['POST'])
@login_required
def add_message_log_entry(id):
    """
    AJAX endpoint to add a message log entry to the patient's record.
    Expects 'category' (optionally 'custom_category') and 'message' in the POST form data.
    """
    category = request.form.get('category')
    custom_category = request.form.get('custom_category')
    message_text = request.form.get('message')
    if category == "Other" and custom_category:
        category = custom_category
    if not category or not message_text:
        return jsonify({'error': 'Category and message are required.'}), 400
    new_message = {
        'id': str(uuid.uuid4()),
        'author': current_user.username,
        'timestamp': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        'category': category,
        'text': message_text
    }
    patient = Patient.get_by_id(id)
    if not patient:
        return jsonify({'error': 'Patient record not found.'}), 404
    if patient.get("care_company_id") != current_user.id:
        flash("You do not have access to this record", "danger")
        return redirect(url_for("care_company.dashboard"))
    message_log = patient.get("message_log")
    if not message_log or message_log == "":
        message_log = []
    elif not isinstance(message_log, list):
        try:
            message_log = json.loads(message_log)
            if not isinstance(message_log, list):
                message_log = []
        except Exception:
            message_log = []
    message_log.append(new_message)
    try:
        Patient.update_patient(id, message_log=json.dumps(message_log))
        log_audit(current_user.username, "Care company added message log entry", patient_id=id)
    except Exception as e:
        logger.error("Error updating patient (message log): %s", str(e))
        return jsonify({'error': 'Failed to save message log entry.'}), 500
    return jsonify({'success': True}), 200

@public_bp.route('/add_risk_flag/<id>', methods=['POST'])
@login_required
def add_risk_flag(id):
    """
    AJAX endpoint to add a risk flag (note) to the patient's record.
    Expects 'flag_type', optionally 'custom_flag_type', and 'description' in the POST form data.
    """
    flag_type = request.form.get('flag_type')
    custom_flag_type = request.form.get('custom_flag_type')
    description = request.form.get('description')
    if flag_type == "Other" and custom_flag_type:
        flag_type = custom_flag_type
    if not flag_type or not description:
        return jsonify({'error': 'Risk category and description are required.'}), 400
    new_flag = {
        'id': str(uuid.uuid4()),
        'flag_type': flag_type,
        'timestamp': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        'description': description
    }
    patient = Patient.get_by_id(id)
    if not patient:
        return jsonify({'error': 'Patient record not found.'}), 404
    risk_flags = patient.get("notes")
    if not risk_flags or risk_flags == "":
        risk_flags = []
    elif not isinstance(risk_flags, list):
        try:
            risk_flags = json.loads(risk_flags)
            if not isinstance(risk_flags, list):
                risk_flags = []
        except Exception:
            risk_flags = []
    risk_flags.append(new_flag)
    try:
        Patient.update_patient(id, notes=json.dumps(risk_flags))
        log_audit(current_user.username, "Care company added risk flag", patient_id=id)
    except Exception as e:
        logger.error("Error updating patient (risk flag): %s", str(e))
        return jsonify({'error': 'Failed to save risk flag.'}), 500
    return jsonify({'success': True}), 200

@public_bp.route('/unlock_record', methods=['POST'])
@login_required
def unlock_record():
    id = request.form.get("id")
    pin = request.form.get("pin")
    access_reason = request.form.get('access_reason')

    if not AuthManager.verify_password(current_user.company_pin_hash, pin):
        return jsonify({"error": "Invalid PIN"}), 403
    session[f'unlocked_{id}'] = True
    log_audit(current_user.username, f"Care company unlocked record for the following reason: {access_reason}", patient_id=id)
    return jsonify({
        "message": "Record unlocked successfully",
        "redirect_url": url_for('care_company.view_client_record', id=id)
    })

@public_bp.route('/lock_record', methods=['POST'])
@login_required
def lock_record():
    id = request.form.get("id")
    session[f'unlocked_{id}'] = False
    log_audit(current_user.username, "Care company locked record", patient_id=id)
    return redirect(url_for('care_company.dashboard'))


# List endpoint: never ``SELECT … data … ORDER BY created_at`` in one pass — MySQL puts selected
# columns in the sort buffer; large JSON ``data`` rows trigger ER_OUT_OF_SORTMEMORY (1038) on
# Railway/small sort_buffer_size. Sort (id, created_at) only, then join for full rows.
_SQL_EPCR_CASES_LIST_ORDERED_IDS = """
SELECT id, created_at AS _ord_created FROM cases ORDER BY created_at DESC
"""
_SQL_EPCR_CASES_LIST_FULL = (
    "SELECT c.id, c.data, c.status, c.created_at, c.closed_at, c.updated_at, "
    "c.dispatch_reference, c.primary_callsign, c.dispatch_synced_at, c.record_version, "
    "c.idempotency_key, c.close_idempotency_key "
    "FROM ("
    + _SQL_EPCR_CASES_LIST_ORDERED_IDS.strip()
    + ") ord INNER JOIN cases c ON c.id = ord.id ORDER BY ord._ord_created DESC"
)
_SQL_EPCR_CASES_LIST_META = (
    "SELECT c.id, c.status, c.created_at, c.closed_at, c.updated_at, "
    "c.dispatch_reference, c.primary_callsign, c.dispatch_synced_at, c.record_version, "
    "c.idempotency_key, c.close_idempotency_key "
    "FROM ("
    + _SQL_EPCR_CASES_LIST_ORDERED_IDS.strip()
    + ") ord INNER JOIN cases c ON c.id = ord.id ORDER BY ord._ord_created DESC"
)


@internal_bp.route('/api/cases', methods=['GET', 'POST', 'OPTIONS'])
def cases():
    # Handle OPTIONS requests for CORS pre-flight.
    if request.method == 'OPTIONS':
        return '', 200

    auth_err = _require_epcr_json_api()
    if auth_err:
        return auth_err

    # GET: Return cases, optionally filtering by a username passed as a query parameter.
    if request.method == 'GET':
        # Non-privileged users may only list their own assigned cases (ignore spoofed ?username=).
        if _epcr_privileged_role():
            raw_u = request.args.get("username")
            username = (str(raw_u).strip().lower() or None) if raw_u is not None else None
        else:
            un = str(_cura_auth_principal()[0] or "").strip()
            username = un or None

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Caldicott: privileged Sparrow browser must not download full case JSON from the list endpoint.
            if _epcr_privileged_browser_session():
                if request.args.get("username"):
                    logger.info(
                        "EPCR API GET /cases: ignoring username= for privileged browser session "
                        "(metadata-only list; use per-case unlock for clinical content)"
                    )
                cursor.execute(_SQL_EPCR_CASES_LIST_META)
                rows_meta = cursor.fetchall()
                cases_meta = []
                today_meta = datetime.now().date()
                for row in rows_meta:
                    (
                        case_id,
                        status,
                        created_at,
                        closed_at,
                        updated_at,
                        dispatch_reference,
                        primary_callsign,
                        dispatch_synced_at,
                        record_version,
                        idempotency_key,
                        close_idempotency_key,
                    ) = row
                    st_lc = (status or "").strip().lower()
                    if st_lc == "closed" and closed_at:
                        if closed_at.date() != today_meta:
                            continue
                    data = {
                        "id": case_id,
                        "status": status,
                        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S")
                        if hasattr(created_at, "strftime")
                        else created_at,
                        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S")
                        if hasattr(updated_at, "strftime")
                        else updated_at,
                        "assignedUsers": [],
                        "sections": [],
                    }
                    _merge_epcr_link_into_case_json(
                        data,
                        dispatch_reference,
                        primary_callsign,
                        dispatch_synced_at,
                        record_version,
                    )
                    _merge_epcr_idempotency_into_case_json(
                        data, idempotency_key, close_idempotency_key
                    )
                    if closed_at:
                        data["closed_at"] = (
                            closed_at.strftime("%Y-%m-%d %H:%M:%S")
                            if hasattr(closed_at, "strftime")
                            else closed_at
                        )
                    else:
                        data["closed_at"] = None
                    cases_meta.append(data)
                cursor.close()
                conn.close()
                return jsonify(cases_meta), 200

            cursor.execute(_SQL_EPCR_CASES_LIST_FULL)
            rows = cursor.fetchall()

            cases = []
            # Use local time here (change to datetime.utcnow().date() if using UTC)
            today = datetime.now().date()
            for row in rows:
                (
                    case_id,
                    data_str,
                    status,
                    created_at,
                    closed_at,
                    updated_at,
                    dispatch_reference,
                    primary_callsign,
                    dispatch_synced_at,
                    record_version,
                    idempotency_key,
                    close_idempotency_key,
                ) = row
                try:
                    data = json.loads(data_str)
                except Exception:
                    data = {}

                # Merge the metadata from DB into the case data.
                data.update({
                    'id': case_id,
                    'status': status,
                    'created_at': created_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(created_at, "strftime") else created_at,
                    'updated_at': updated_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(updated_at, "strftime") else updated_at,
                })
                _merge_epcr_link_into_case_json(
                    data, dispatch_reference, primary_callsign, dispatch_synced_at, record_version
                )
                _merge_epcr_idempotency_into_case_json(data, idempotency_key, close_idempotency_key)
                if closed_at:
                    data['closed_at'] = closed_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(closed_at, "strftime") else closed_at
                else:
                    data['closed_at'] = None

                # For cases marked as closed, include them only if closed_at is today.
                if (status or "").strip().lower() == "closed" and closed_at:
                    if closed_at.date() != today:
                        continue

                # If a username filter is provided, include cases where that user is on assignedUsers or crewManifest.
                if username:
                    if _epcr_principal_has_case_access(username, data):
                        cases.append(data)
                elif _epcr_privileged_role():
                    # Privileged list-all (no username filter): include every case passing closed-date rule
                    cases.append(data)
                # Non-privileged JWT must always use principal username (never this branch). If it were falsy,
                # listing all cases would be a confidentiality bug; omit rows instead.

            cursor.close()
            conn.close()
            return jsonify(cases), 200

        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            logger.exception("EPCR API cases GET failed: %s", e)
            return jsonify({'error': str(e)}), 500

    # POST: Create a new case.
    if request.method == 'POST':
        payload = request.get_json()
        if not payload:
            return jsonify({'error': 'No JSON payload provided'}), 400

        _normalize_epcr_review_drugs_sections(payload)
        _epcr_normalize_case_identity_fields(payload)
        idem_hdr = _extract_case_idempotency_keys()
        idem_body = (payload.get("idempotencyKey") or payload.get("idempotency_key") or "").strip() or None
        idem = idem_hdr or idem_body

        uname = _cura_auth_principal()[0] or ''
        crew_co_assign_check = None
        if not _epcr_privileged_role():
            assigned_norm = _epcr_normalize_assigned_username_list(payload.get("assignedUsers"))
            others = _epcr_crew_co_assignee_others(uname, assigned_norm)
            if others:
                oid = _epcr_operational_event_id_from_case_payload(payload)
                if oid is None:
                    return _epcr_jsonify_use_collaborator_endpoint()
                crew_co_assign_check = (oid, assigned_norm, uname)
            else:
                if not _epcr_principal_in_assigned_users(uname, assigned_norm):
                    assigned_norm = [uname] if uname else []
                payload["assignedUsers"] = assigned_norm

        # Expect the client to provide an 'id'
        case_id = payload.get('id')
        if not case_id:
            return jsonify({'error': 'Case id not provided in payload'}), 400
        _epcr_seed_incident_identity_fields(payload, case_id=case_id)

        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            if crew_co_assign_check is not None:
                oid, assigned_norm, caller_u = crew_co_assign_check
                v_err = _epcr_validate_crew_co_assignees_operational_event(
                    cursor, oid, assigned_norm, caller_u
                )
                if v_err:
                    cursor.close()
                    conn.close()
                    return v_err
                payload["assignedUsers"] = assigned_norm

            if idem:
                cursor.execute(
                    """
                    SELECT id, data, status, created_at, closed_at, updated_at,
                           dispatch_reference, primary_callsign, dispatch_synced_at, record_version,
                           idempotency_key, close_idempotency_key
                    FROM cases WHERE idempotency_key = %s
                    LIMIT 1
                    """,
                    (idem,),
                )
                idem_row = cursor.fetchone()
                if idem_row:
                    (
                        rid,
                        data_str,
                        st,
                        cat,
                        clt,
                        uat,
                        dr,
                        pc,
                        ds,
                        rv,
                        ik,
                        cck,
                    ) = idem_row
                    caldicott_deny_idem = _require_epcr_caldicott_unlock_for_privileged_session(rid)
                    if caldicott_deny_idem:
                        cursor.close()
                        conn.close()
                        return caldicott_deny_idem
                    existing_data = _parse_case_json(data_str)
                    if not _user_may_access_case_data(existing_data):
                        cursor.close()
                        conn.close()
                        return jsonify({'error': 'Unauthorised'}), 403
                    out = existing_data if isinstance(existing_data, dict) else {}
                    out.update({
                        'id': rid,
                        'status': st,
                        'created_at': cat.strftime("%Y-%m-%d %H:%M:%S") if hasattr(cat, "strftime") else cat,
                        'updated_at': uat.strftime("%Y-%m-%d %H:%M:%S") if hasattr(uat, "strftime") else uat,
                    })
                    _merge_epcr_link_into_case_json(out, dr, pc, ds, rv)
                    _merge_epcr_idempotency_into_case_json(out, ik, cck)
                    if clt:
                        out['closed_at'] = clt.strftime("%Y-%m-%d %H:%M:%S") if hasattr(clt, "strftime") else clt
                    else:
                        out['closed_at'] = None
                    cursor.close()
                    conn.close()
                    _audit_epcr_api(f"EPCR API idempotent replay case {rid} (idempotency)")
                    return (
                        jsonify(
                            {
                                "message": "Case already recorded (idempotent)",
                                "case": out,
                                "deduplicated": True,
                                "serverAck": _epcr_server_ack(rid, rv, uat),
                            }
                        ),
                        200,
                    )

            cursor.execute(
                "SELECT data, dispatch_reference, primary_callsign, dispatch_synced_at, record_version "
                "FROM cases WHERE id = %s",
                (case_id,),
            )
            ex_row = cursor.fetchone()
            if ex_row:
                caldicott_deny_ex = _require_epcr_caldicott_unlock_for_privileged_session(case_id)
                if caldicott_deny_ex:
                    cursor.close()
                    conn.close()
                    return caldicott_deny_ex
                existing_data = _parse_case_json(ex_row[0])
                if not _user_may_access_case_data(existing_data):
                    cursor.close()
                    conn.close()
                    return jsonify({'error': 'Unauthorised'}), 403
                ex_meta = (ex_row[1], ex_row[2], ex_row[3], ex_row[4])
            else:
                ex_meta = None

            case_payload = copy.deepcopy(payload)
            dr, pc, ds, next_rv, err = _epcr_case_save_link_meta(
                case_payload,
                ex_meta,
                version_conflict_case_id=int(case_id),
                version_conflict_cursor=cursor,
            )
            if err:
                cursor.close()
                conn.close()
                return err
            _strip_epcr_link_keys_from_dict(case_payload)
            from . import cura_mpi

            try:
                _pid, _lid = cura_mpi.enrich_case_payload_mpi(cursor, case_payload)
            except Exception:
                logger.exception("enrich_case_payload_mpi (POST case)")
                _pid, _lid = None, None
            try:
                sanitize_rtc_section_payload_for_mysql(case_payload)
                sanitize_oohca_verification_render_keys(case_payload)
            except Exception:
                logger.exception("sanitize case-access render keys (RTC/OOHCA) (POST case)")
            payload_str = json.dumps(case_payload)
            status = payload.get('status', 'in progress')
            created_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            updated_at = created_at
            ds_sql = ds.strftime("%Y-%m-%d %H:%M:%S") if ds else None

            query = """
                INSERT INTO cases (id, data, status, created_at, updated_at,
                    dispatch_reference, primary_callsign, dispatch_synced_at, record_version, idempotency_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                        data = VALUES(data),
                        status = VALUES(status),
                        updated_at = VALUES(updated_at),
                        dispatch_reference = VALUES(dispatch_reference),
                        primary_callsign = VALUES(primary_callsign),
                        dispatch_synced_at = VALUES(dispatch_synced_at),
                        record_version = VALUES(record_version),
                        idempotency_key = IFNULL(idempotency_key, VALUES(idempotency_key))
            """
            cursor.execute(
                query,
                (case_id, payload_str, status, created_at, updated_at, dr, pc, ds_sql, next_rv, idem),
            )
            try:
                cura_mpi.update_cases_mpi_columns(cursor, int(case_id), _pid, _lid)
            except Exception:
                logger.exception("update_cases_mpi_columns (POST case)")
            _sync_case_patient_match_meta(cursor, case_id, case_payload)
            conn.commit()
            cursor.close()
            conn.close()
            _audit_epcr_api(f"EPCR API created/upserted case {case_id}")
            case_payload["recordVersion"] = next_rv
            return (
                jsonify(
                    {
                        "message": "Case created successfully",
                        "case": case_payload,
                        "serverAck": _epcr_server_ack(case_id, next_rv, datetime.utcnow()),
                    }
                ),
                201,
            )
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            logger.exception("EPCR API cases POST failed: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500


@internal_bp.route("/api/cases/<int:case_id>/case-access-html", methods=["GET", "OPTIONS"])
def case_access_html_preview(case_id):
    """
    Render ``case_access_pdf.html`` for JSON API clients (e.g. Cura) without Sparrow browser unlock.
    Privileged Flask-login browser sessions still require Caldicott unlock (same as GET /api/cases/<id>).
    """
    if request.method == "OPTIONS":
        return "", 200

    auth_err = _require_epcr_json_api()
    if auth_err:
        return auth_err

    caldicott_deny = _require_epcr_caldicott_unlock_for_privileged_session(case_id)
    if caldicott_deny:
        return caldicott_deny

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        extra_mpi = _mpi_case_select_extra(cursor)
        query = (
            "SELECT data, status, created_at, closed_at, updated_at, "
            "dispatch_reference, primary_callsign, dispatch_synced_at, record_version, "
            "idempotency_key, close_idempotency_key"
            + extra_mpi
            + " FROM cases WHERE id = %s"
        )
        cursor.execute(query, (case_id,))
        row = cursor.fetchone()
        if not row:
            cursor.close()
            conn.close()
            return jsonify({"error": "Case not found"}), 404

        (
            data_str,
            status,
            created_at,
            closed_at,
            updated_at,
            dispatch_reference,
            primary_callsign,
            dispatch_synced_at,
            record_version,
            idempotency_key,
            close_idempotency_key,
        ) = row[:11]
        case_data = json.loads(data_str)
        if not _user_may_access_case_data(case_data):
            cursor.close()
            conn.close()
            return jsonify({"error": "Unauthorised"}), 403
        _apply_mpi_columns_to_case_json(case_data, row)
        case_data["status"] = status
        case_data["createdAt"] = created_at.isoformat() if created_at else None
        case_data["closedAt"] = closed_at.isoformat() if closed_at else None
        case_data["updatedAt"] = updated_at.isoformat() if updated_at else None
        case_data["closed_at"] = case_data["closedAt"]
        case_data["updated_at"] = case_data["updatedAt"]
        _merge_epcr_link_into_case_json(
            case_data, dispatch_reference, primary_callsign, dispatch_synced_at, record_version
        )
        _merge_epcr_idempotency_into_case_json(case_data, idempotency_key, close_idempotency_key)
        try:
            inject_rtc_case_access_inline_images(case_id, case_data, cursor)
            inject_oohca_role_ecg_case_access_images(case_id, case_data, cursor)
        except Exception:
            logger.exception("inject case-access inline images (RTC/OOHCA) case-access-html case %s", case_id)
        cursor.close()
        conn.close()
        _audit_epcr_api(f"EPCR API case-access-html preview case {case_id}")
        # Cura (and any client) embeds this HTML in iframe srcDoc; root-relative /static/... would resolve to the
        # client origin. Absolute static URLs keep body/chest/spine diagrams and abdomen PNG loading from Sparrow.
        html = render_template(
            "public/case_access_pdf.html",
            case_data=case_data,
            case_id=case_id,
            config=core_manifest,
            show_sparrow_nav=False,
            case_access_static_origin=request.url_root.rstrip("/"),
        )
        resp = make_response(html)
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"
        resp.headers["Pragma"] = "no-cache"
        return resp
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass
        logger.exception("case_access_html_preview case %s: %s", case_id, e)
        return jsonify({"error": "An internal error occurred"}), 500


@internal_bp.route('/api/cases/<int:case_id>', methods=['GET', 'PUT', 'OPTIONS'])
def case_handler(case_id):
    if request.method == 'OPTIONS':
        return '', 200

    auth_err = _require_epcr_json_api()
    if auth_err:
        return auth_err

    caldicott_deny = _require_epcr_caldicott_unlock_for_privileged_session(case_id)
    if caldicott_deny:
        return caldicott_deny

    conn = get_db_connection()
    cursor = conn.cursor()
    if request.method == 'GET':
        try:
            extra_mpi = _mpi_case_select_extra(cursor)
            query = (
                "SELECT data, status, created_at, closed_at, updated_at, "
                "dispatch_reference, primary_callsign, dispatch_synced_at, record_version, "
                "idempotency_key, close_idempotency_key"
                + extra_mpi
                + " FROM cases WHERE id = %s"
            )
            cursor.execute(query, (case_id,))
            row = cursor.fetchone()
            if row:
                (
                    data_str,
                    status,
                    created_at,
                    closed_at,
                    updated_at,
                    dispatch_reference,
                    primary_callsign,
                    dispatch_synced_at,
                    record_version,
                    idempotency_key,
                    close_idempotency_key,
                ) = row[:11]
                case_data = json.loads(data_str)
                if not _user_may_access_case_data(case_data):
                    cursor.close()
                    conn.close()
                    return jsonify({'error': 'Unauthorised'}), 403
                _apply_mpi_columns_to_case_json(case_data, row)
                case_data['status'] = status
                case_data['createdAt'] = created_at.isoformat() if created_at else None
                case_data['closedAt'] = closed_at.isoformat() if closed_at else None
                case_data['updatedAt'] = updated_at.isoformat() if updated_at else None
                case_data['closed_at'] = case_data['closedAt']
                case_data['updated_at'] = case_data['updatedAt']
                _merge_epcr_link_into_case_json(
                    case_data, dispatch_reference, primary_callsign, dispatch_synced_at, record_version
                )
                _merge_epcr_idempotency_into_case_json(case_data, idempotency_key, close_idempotency_key)
                cursor.close()
                conn.close()
                _audit_epcr_api(f"EPCR API read case {case_id}")
                return jsonify(case_data), 200
            else:
                cursor.close()
                conn.close()
                return jsonify({'error': 'Case not found'}), 404
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            return jsonify({'error': str(e)}), 500

    elif request.method == 'PUT':
        payload = request.get_json()
        if not payload:
            cursor.close()
            conn.close()
            return jsonify({'error': 'No JSON payload provided'}), 400

        _normalize_epcr_review_drugs_sections(payload)
        _epcr_normalize_case_identity_fields(payload)
        _epcr_seed_incident_identity_fields(payload, case_id=case_id)
        idem_hdr = _extract_case_idempotency_keys()
        idem_body = (payload.get("idempotencyKey") or payload.get("idempotency_key") or "").strip() or None
        idem = idem_hdr or idem_body
        if idem:
            cursor.execute(
                """
                SELECT id, data, status, created_at, closed_at, updated_at,
                       dispatch_reference, primary_callsign, dispatch_synced_at, record_version,
                       idempotency_key, close_idempotency_key
                FROM cases WHERE idempotency_key = %s LIMIT 1
                """,
                (idem,),
            )
            idem_row = cursor.fetchone()
            if idem_row and int(idem_row[0]) != int(case_id):
                cursor.close()
                conn.close()
                return jsonify({"error": "Idempotency key belongs to a different case"}), 409

        cursor.execute(
            "SELECT data, dispatch_reference, primary_callsign, dispatch_synced_at, record_version "
            "FROM cases WHERE id = %s",
            (case_id,),
        )
        row = cursor.fetchone()
        if row:
            existing = _parse_case_json(row[0])
            if not _user_may_access_case_data(existing):
                cursor.close()
                conn.close()
                return jsonify({'error': 'Unauthorised'}), 403
            ex_meta = (row[1], row[2], row[3], row[4])
        else:
            ex_meta = None
            # Upsert create via PUT: same assignment rules as POST for non-privileged users (Option A2).
            uname = _cura_auth_principal()[0] or ''
            if not _epcr_privileged_role():
                assigned_norm = _epcr_normalize_assigned_username_list(payload.get("assignedUsers"))
                others = _epcr_crew_co_assignee_others(uname, assigned_norm)
                if others:
                    oid = _epcr_operational_event_id_from_case_payload(payload)
                    if oid is None:
                        cursor.close()
                        conn.close()
                        return _epcr_jsonify_use_collaborator_endpoint()
                    v_err = _epcr_validate_crew_co_assignees_operational_event(
                        cursor, oid, assigned_norm, uname
                    )
                    if v_err:
                        cursor.close()
                        conn.close()
                        return v_err
                    payload = {**payload, "assignedUsers": assigned_norm}
                else:
                    if not _epcr_principal_in_assigned_users(uname, assigned_norm):
                        assigned_norm = [uname] if uname else []
                    payload = {**payload, "assignedUsers": assigned_norm}

        case_payload = copy.deepcopy(payload)
        dr, pc, ds, next_rv, err = _epcr_case_save_link_meta(
            case_payload,
            ex_meta,
            version_conflict_case_id=int(case_id),
            version_conflict_cursor=cursor,
        )
        if err:
            cursor.close()
            conn.close()
            return err
        _strip_epcr_link_keys_from_dict(case_payload)
        from . import cura_mpi

        try:
            _pid, _lid = cura_mpi.enrich_case_payload_mpi(cursor, case_payload)
        except Exception:
            logger.exception("enrich_case_payload_mpi (PUT case)")
            _pid, _lid = None, None
        try:
            sanitize_rtc_section_payload_for_mysql(case_payload)
            sanitize_oohca_verification_render_keys(case_payload)
        except Exception:
            logger.exception("sanitize case-access render keys (RTC/OOHCA) (PUT case)")
        payload_str = json.dumps(case_payload)
        ds_sql = ds.strftime("%Y-%m-%d %H:%M:%S") if ds else None
        status = payload.get('status', 'in progress')
        closed_at = payload.get('closedAt', None)
        updated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # If no created_at is provided, use updated_at for new records.
        created_at = updated_at  

        closed_at_str = None
        if closed_at:
            try:
                # Replace trailing 'Z' with '+00:00' so we get a timezone-aware datetime:
                dt_utc = datetime.fromisoformat(closed_at.replace("Z", "+00:00"))
                # Convert the UTC time to local time; this assumes your server's local time zone.
                dt_local = dt_utc.astimezone()
                closed_at_str = dt_local.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                cursor.close()
                conn.close()
                return jsonify({'error': f"Invalid closedAt format: {str(e)}"}), 400

        try:
            if closed_at_str:
                query = """
                    INSERT INTO cases (id, data, status, created_at, updated_at, closed_at,
                        dispatch_reference, primary_callsign, dispatch_synced_at, record_version, idempotency_key)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        data = VALUES(data),
                        status = VALUES(status),
                        updated_at = VALUES(updated_at),
                        closed_at = VALUES(closed_at),
                        dispatch_reference = VALUES(dispatch_reference),
                        primary_callsign = VALUES(primary_callsign),
                        dispatch_synced_at = VALUES(dispatch_synced_at),
                        record_version = VALUES(record_version),
                        idempotency_key = IFNULL(idempotency_key, VALUES(idempotency_key))
                """
                cursor.execute(
                    query,
                    (
                        case_id,
                        payload_str,
                        status,
                        created_at,
                        updated_at,
                        closed_at_str,
                        dr,
                        pc,
                        ds_sql,
                        next_rv,
                        idem,
                    ),
                )
                try:
                    cura_mpi.update_cases_mpi_columns(cursor, int(case_id), _pid, _lid)
                except Exception:
                    logger.exception("update_cases_mpi_columns (PUT case closed)")
                _sync_case_patient_match_meta(cursor, case_id, case_payload)

                # map sections
                sec_map       = {sec['name']: sec['content'] for sec in payload.get('sections', [])}
                incident      = sec_map.get('Incident Log', {}).get('incident', {})
                response_type = (incident.get('responseType') or '').strip().lower()

                # only send finance email if not an "event"
                if response_type != 'event':
                    finance_recipient = os.environ.get('FINANCE_EMAIL')
                    if finance_recipient:
                        # gather data
                        billing   = sec_map.get('Billing', {}).get('billing', {})
                        ptinfo    = sec_map.get('PatientInfo', {}).get('ptInfo', {})
                        member    = ptinfo.get('urgentCareMember', {})
                        handover  = sec_map.get('Clinical Handover', {}).get('handoverData', {})
                        receiving = handover.get('receivingHospital', {})
                        drugs     = sec_map.get('Drugs Administered', {}).get('drugsAdministered', [])

                        # patient name
                        patient_name = " ".join(filter(None, [ptinfo.get('forename'), ptinfo.get('surname')]))

                        # compute drug cost summary
                        total_drug_cost = 0.0
                        for drug in drugs:
                            try:
                                total_drug_cost += float(drug.get('cost', 0))
                            except (TypeError, ValueError):
                                pass
                        overall_drug_cost = total_drug_cost

                        # email subject
                        subject = f"EPCR ({incident.get('responseType')}) - Case: {case_id}"

                        # build email body lines
                        lines = [
                            fmt("Patient Name", patient_name),
                            fmt("Case ID", case_id),
                            fmt("Closed At", closed_at_str),
                            ""
                        ]

                        # Billing
                        if any(billing.get(k) for k in ("payeeName","payeeEmail","payeeAddress","notes")):
                            lines += [
                                "-- Billing Information --",
                                fmt("Payee Name", billing.get("payeeName")),
                                fmt("Payee Email", billing.get("payeeEmail")),
                                fmt("Payee Address", billing.get("payeeAddress")),
                                fmt("Notes", billing.get("notes")),
                                ""
                            ]

                        # Membership
                        if any(member.get(k) for k in ("isMember","membershipNumber","primaryType","membershipLevel")):
                            lines += [
                                "-- Membership --",
                                fmt("Member?", member.get("isMember")),
                                fmt("Membership Number", member.get("membershipNumber")),
                                fmt("Membership Type", member.get("primaryType")),
                                fmt("Membership Level", member.get("membershipLevel")),
                                ""
                            ]

                        # Incident Details
                        lines += [
                            "-- Incident Details --",
                            fmt("Response Type", incident.get("responseType")),
                            fmt("Response Other", incident.get("responseOther")),
                        ]

                        # Conveyance Outcome
                        if handover.get("outcome"):
                            lines += [
                                "",
                                "-- Conveyance Outcome --",
                                fmt("Outcome Code", handover.get("outcome"))
                            ]
                            if handover.get("otherOutcomeDetails"):
                                lines.append(fmt("Details", handover.get("otherOutcomeDetails")))

                        # Receiving Hospital
                        if any(receiving.get(k) for k in ("hospital","ward","otherWard","otherHospitalDetails")):
                            lines += [
                                "",
                                "-- Receiving Hospital --",
                                fmt("Hospital", receiving.get("hospital")),
                                fmt("Ward", receiving.get("ward")),
                                fmt("Other Ward", receiving.get("otherWard")),
                                fmt("Other Hospital Details", receiving.get("otherHospitalDetails")),
                            ]

                        # Drugs Administered
                        if drugs:
                            lines += ["", "-- Drugs Administered --"]
                            for i, drug in enumerate(drugs, start=1):
                                lines += [
                                    fmt(f"Drug {i} Name",            drug.get("drugName")),
                                    fmt(f"Drug {i} Dosage",          drug.get("dosage")),
                                    fmt(f"Drug {i} Batch Number",    drug.get("batchNumber")),
                                    fmt(f"Drug {i} Expiry Date",     drug.get("expiryDate")),
                                    fmt(f"Drug {i} Administered By", drug.get("administeredBy")),
                                    fmt(f"Drug {i} Route",           drug.get("route")),
                                    fmt(f"Drug {i} Time Administered", drug.get("timeAdministered")),
                                    fmt(f"Drug {i} Notes",           drug.get("notes")),
                                    fmt(f"Drug {i} Cost Consent",    drug.get("costConsent")),
                                    fmt(f"Drug {i} Cost £",            drug.get("cost")),      # ← added
                                ]

                            # # Drug cost summary
                            # lines += [
                            #     "",
                            #     "-- Drug Cost Summary --",
                            #     fmt("Total Drug Cost", total_drug_cost),
                            #     fmt("Overall Drug Cost", overall_drug_cost),
                            # ]

                        # Timings
                        lines += [
                            "",
                            "-- Timings --",
                            fmt("Time Of Call", incident.get("timeOfCall")),
                            fmt("Time Mobile", incident.get("timeMobile")),
                            fmt("Time At Scene", incident.get("timeOfScene") or incident.get("timeAtScene")),
                            fmt("Time Leave Scene", incident.get("timeLeaveScene")),
                            fmt("Time At Hospital", incident.get("timeAtHospital")),
                            fmt("Time Handover", incident.get("timeHandover")),
                            fmt("Time Left Hospital", incident.get("timeLeftHospital")),
                            fmt("Time At Treatment Centre", incident.get("timeAtTreatmentCentre")),
                            fmt("Time Of Prealert", incident.get("timeOfPrealert")),
                        ]

                        # Support note
                        lines += [
                            "",
                            "If you have any issues, please contact support."
                        ]

                        body     = "\n".join(lines)
                        to_addrs = [finance_recipient]

                        try:
                            emailer.send_email(subject=subject, body=body, recipients=to_addrs)
                        except Exception as email_err:
                            current_app.logger.error(f"Failed to send finance email: {email_err}")
                    else:
                        current_app.logger.warning("FINANCE_EMAIL not configured; skipping finance notification")

            else:
                query = """
                    INSERT INTO cases (id, data, status, created_at, updated_at,
                        dispatch_reference, primary_callsign, dispatch_synced_at, record_version, idempotency_key)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        data = VALUES(data),
                        status = VALUES(status),
                        updated_at = VALUES(updated_at),
                        dispatch_reference = VALUES(dispatch_reference),
                        primary_callsign = VALUES(primary_callsign),
                        dispatch_synced_at = VALUES(dispatch_synced_at),
                        record_version = VALUES(record_version),
                        idempotency_key = IFNULL(idempotency_key, VALUES(idempotency_key))
                """
                cursor.execute(
                    query,
                    (case_id, payload_str, status, created_at, updated_at, dr, pc, ds_sql, next_rv, idem),
                )
                try:
                    cura_mpi.update_cases_mpi_columns(cursor, int(case_id), _pid, _lid)
                except Exception:
                    logger.exception("update_cases_mpi_columns (PUT case)")
                _sync_case_patient_match_meta(cursor, case_id, case_payload)
            conn.commit()
            cursor.close()
            conn.close()
            _audit_epcr_api(f"EPCR API updated case {case_id}")
            case_payload["recordVersion"] = next_rv
            return (
                jsonify(
                    {
                        "message": "Case updated successfully",
                        "case": case_payload,
                        "serverAck": _epcr_server_ack(case_id, next_rv, datetime.utcnow()),
                    }
                ),
                200,
            )
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            logger.exception("EPCR API case PUT failed: %s", e)
            return jsonify({"error": "An internal error occurred"}), 500


@internal_bp.route("/api/cases/<int:case_id>/collaborators", methods=["POST", "OPTIONS"])
def add_case_collaborators(case_id):
    """Append Sparrow users to assignedUsers / crewManifest for an open case (caller must already have access)."""
    if request.method == "OPTIONS":
        return "", 200

    auth_err = _require_epcr_json_api()
    if auth_err:
        return auth_err

    caldicott_deny_colab = _require_epcr_caldicott_unlock_for_privileged_session(case_id)
    if caldicott_deny_colab:
        return caldicott_deny_colab

    body = request.get_json()
    if not body:
        return jsonify({"error": "No JSON payload provided"}), 400

    raw = body.get("usernames")
    if raw is None and body.get("username"):
        raw = [body.get("username")]
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list) or not raw:
        return jsonify({"error": "Provide usernames as a non-empty array (or username string)"}), 400

    collected = [u for u in raw if isinstance(u, str) and u.strip()]
    new_usernames = _epcr_normalize_assigned_username_list(collected)
    if not new_usernames:
        return jsonify({"error": "No valid usernames provided"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT data, status, closed_at, dispatch_reference, primary_callsign, dispatch_synced_at, record_version "
            "FROM cases WHERE id = %s",
            (case_id,),
        )
        row = cursor.fetchone()
        if not row:
            cursor.close()
            conn.close()
            return jsonify({"error": "Case not found"}), 404

        data_str, status, closed_at, dr, pc, ds, ex_rv = row
        if closed_at is not None or (status and str(status).strip().lower() == "closed"):
            cursor.close()
            conn.close()
            return jsonify({"error": "Cannot add collaborators to a closed case"}), 400

        existing = _parse_case_json(data_str)
        if not isinstance(existing, dict):
            existing = {}
        if not _user_may_access_case_data(existing):
            cursor.close()
            conn.close()
            return jsonify({"error": "Unauthorised"}), 403

        prev_assigned_set = set(_epcr_normalize_assigned_username_list(existing.get("assignedUsers")))

        if ex_rv is None:
            ex_rv = 1

        user_rows = {}
        invalid = []
        for uname in new_usernames:
            ud = User.get_user_by_username_raw(uname)
            if not ud:
                invalid.append(uname)
            else:
                user_rows[uname] = ud
        if invalid:
            cursor.close()
            conn.close()
            return jsonify({"error": "Unknown username(s)", "invalidUsernames": invalid}), 400

        case_payload = copy.deepcopy(existing)
        _epcr_normalize_case_identity_fields(case_payload)

        assigned = _epcr_normalize_assigned_username_list(case_payload.get("assignedUsers"))
        for uname in new_usernames:
            if uname not in assigned:
                assigned.append(uname)
        case_payload["assignedUsers"] = assigned

        manifest = case_payload.get("crewManifest")
        if not isinstance(manifest, list):
            manifest = []
        seen = set()
        for item in manifest:
            if isinstance(item, dict) and item.get("username"):
                seen.add(str(item["username"]).strip().lower())
        for uname in new_usernames:
            if uname in seen:
                continue
            ud = user_rows[uname]
            disp = _epcr_user_display_name_from_user_row(ud)
            manifest.append(
                {
                    "username": uname,
                    "displayName": disp or uname,
                    "source": "manual",
                }
            )
            seen.add(uname)
        case_payload["crewManifest"] = manifest

        client_rv = body.get("recordVersion", body.get("record_version"))
        if client_rv is not None:
            try:
                case_payload["recordVersion"] = int(client_rv)
            except (TypeError, ValueError):
                cursor.close()
                conn.close()
                return jsonify({"error": "Invalid recordVersion"}), 400
        else:
            case_payload["recordVersion"] = ex_rv

        ex_meta = (dr, pc, ds, ex_rv)
        dr2, pc2, ds2, next_rv, err = _epcr_case_save_link_meta(
            case_payload,
            ex_meta,
            version_conflict_case_id=int(case_id),
            version_conflict_cursor=cursor,
        )
        if err:
            cursor.close()
            conn.close()
            return err

        _strip_epcr_link_keys_from_dict(case_payload)
        from . import cura_mpi

        try:
            _pid, _lid = cura_mpi.enrich_case_payload_mpi(cursor, case_payload)
        except Exception:
            logger.exception("enrich_case_payload_mpi (collaborators)")
            _pid, _lid = None, None

        try:
            sanitize_rtc_section_payload_for_mysql(case_payload)
            sanitize_oohca_verification_render_keys(case_payload)
        except Exception:
            logger.exception("sanitize case-access render keys (RTC/OOHCA) (collaborators)")

        payload_str = json.dumps(case_payload)
        ds_sql = ds2.strftime("%Y-%m-%d %H:%M:%S") if ds2 else None
        updated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute(
            """
            UPDATE cases SET data = %s, updated_at = %s,
                dispatch_reference = %s, primary_callsign = %s, dispatch_synced_at = %s, record_version = %s
            WHERE id = %s
            """,
            (payload_str, updated_at, dr2, pc2, ds_sql, next_rv, case_id),
        )
        try:
            cura_mpi.update_cases_mpi_columns(cursor, int(case_id), _pid, _lid)
        except Exception:
            logger.exception("update_cases_mpi_columns (collaborators)")
        _sync_case_patient_match_meta(cursor, case_id, case_payload)

        conn.commit()
        cursor.close()
        conn.close()
        actually_added = [u for u in new_usernames if u not in prev_assigned_set]
        _audit_epcr_api(f"EPCR API added collaborators to case {case_id}")
        if actually_added:
            log_audit(
                _cura_auth_principal()[0] or "unknown",
                f"EPCR API case {case_id} collaborators added: {', '.join(actually_added)}",
                case_id=case_id,
            )
        case_payload["recordVersion"] = next_rv
        return (
            jsonify(
                {
                    "message": "Collaborators updated",
                    "case": case_payload,
                    "serverAck": _epcr_server_ack(case_id, next_rv, datetime.utcnow()),
                }
            ),
            200,
        )
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        logger.exception("EPCR API collaborators POST failed: %s", e)
        return jsonify({"error": "An internal error occurred"}), 500


def fmt(label: str, val) -> str:
    """Format a label/value pair, defaulting to 'N/A' if val is falsy."""
    return f"{label}: {val}" if val else f"{label}: N/A"

@internal_bp.route('/api/cases/<int:case_id>/close', methods=['PUT', 'OPTIONS'])
def close_case(case_id):
    if request.method == 'OPTIONS':
        return '', 200

    auth_err = _require_epcr_json_api()
    if auth_err:
        return auth_err

    caldicott_deny_close = _require_epcr_caldicott_unlock_for_privileged_session(case_id)
    if caldicott_deny_close:
        return caldicott_deny_close

    payload = request.get_json()
    if not payload:
        return jsonify({'error': 'No JSON payload provided'}), 400

    _normalize_epcr_review_drugs_sections(payload)
    _epcr_normalize_case_identity_fields(payload)
    _epcr_seed_incident_identity_fields(payload, case_id=case_id)
    close_idem = _extract_case_idempotency_keys() or (
        (payload.get("closeIdempotencyKey") or payload.get("close_idempotency_key") or "").strip() or None
    )

    # parse closedAt
    closed_at = payload.get('closedAt')
    if closed_at:
        try:
            dt = datetime.fromisoformat(closed_at)
        except Exception as e:
            return jsonify({'error': f"Invalid closedAt format: {str(e)}"}), 400
    else:
        dt = datetime.utcnow()
    closed_at_str = dt.strftime("%Y-%m-%d %H:%M:%S")

    # Update DB
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT data, dispatch_reference, primary_callsign, dispatch_synced_at, record_version,
                   status, close_idempotency_key
            FROM cases WHERE id = %s
            """,
            (case_id,),
        )
        row = cursor.fetchone()
        if not row:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Case not found'}), 404
        existing = _parse_case_json(row[0])
        if not _user_may_access_case_data(existing):
            cursor.close()
            conn.close()
            return jsonify({'error': 'Unauthorised'}), 403
        ex_rv = row[4] if row[4] is not None else 1
        db_status = (row[5] or "").strip().lower()
        db_close_idem = row[6]
        if (
            db_status == "closed"
            and close_idem
            and db_close_idem
            and str(db_close_idem) == str(close_idem)
        ):
            cursor.close()
            conn.close()
            return (
                jsonify(
                    {
                        "message": "Case already closed (idempotent)",
                        "deduplicated": True,
                        "serverAck": _epcr_server_ack(case_id, ex_rv, datetime.utcnow()),
                    }
                ),
                200,
            )
        if "recordVersion" in payload or "record_version" in payload:
            try:
                ev = int(payload.get("recordVersion", payload.get("record_version")))
            except (TypeError, ValueError):
                cursor.close()
                conn.close()
                return jsonify({"error": "Invalid recordVersion"}), 400
            if ev != ex_rv:
                cursor.close()
                conn.close()
                return jsonify({"error": "Case was modified elsewhere", "recordVersion": ex_rv}), 409

        close_payload = copy.deepcopy(payload)
        _strip_epcr_link_keys_from_dict(close_payload)

        cursor.execute(
            "UPDATE cases SET data=%s, status=%s, closed_at=%s, updated_at=%s, record_version=record_version+1, "
            "close_idempotency_key=COALESCE(%s, close_idempotency_key) "
            "WHERE id=%s",
            (
                json.dumps(close_payload),
                "closed",
                closed_at_str,
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                close_idem,
                case_id,
            ),
        )
        _sync_case_patient_match_meta(cursor, case_id, close_payload)
        conn.commit()
        new_rv = ex_rv + 1
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        logger.exception("EPCR API close case failed: %s", e)
        return jsonify({"error": "An internal error occurred"}), 500

    # Prepare email only if responseType != "event"
    sec_map       = {sec['name']: sec['content'] for sec in payload.get('sections', [])}
    incident      = sec_map.get('Incident Log', {}).get('incident', {})
    response_type = (incident.get('responseType') or '').strip().lower()

    if response_type != 'event':
        finance_recipient = os.environ.get('FINANCE_EMAIL')
        if finance_recipient:
            # extract content
            billing    = sec_map.get('Billing', {}).get('billing', {})
            ptinfo     = sec_map.get('PatientInfo', {}).get('ptInfo', {})
            member     = ptinfo.get('urgentCareMember', {})
            handover   = sec_map.get('Clinical Handover', {}).get('handoverData', {})
            receiving  = handover.get('receivingHospital', {})

            # build patient full name
            patient_name = " ".join(filter(None, [ptinfo.get('forename'), ptinfo.get('surname')]))

            subject = f"EPCR ({incident.get('responseType')}) - Case: {case_id}"

            lines = [
                fmt("Patient Name", patient_name),
                fmt("Case ID", case_id),
                fmt("Closed At", closed_at_str),
                ""
            ]

            # Billing block
            if any(billing.get(k) for k in ("payeeName","payeeEmail","payeeAddress","notes")):
                lines += ["-- Billing Information --",
                          fmt("Payee Name", billing.get("payeeName")),
                          fmt("Payee Email", billing.get("payeeEmail")),
                          fmt("Payee Address", billing.get("payeeAddress")),
                          fmt("Notes", billing.get("notes")),
                          ""]

            # Membership block
            if any(member.get(k) for k in ("isMember","membershipNumber","primaryType","membershipLevel")):
                lines += ["-- Membership --",
                          fmt("Member?", member.get("isMember")),
                          fmt("Membership Number", member.get("membershipNumber")),
                          fmt("Membership Type", member.get("primaryType")),
                          fmt("Membership Level", member.get("membershipLevel")),
                          ""]

            # Incident Details
            lines += ["-- Incident Details --",
                      fmt("Response Type", incident.get("responseType")),
                      fmt("Response Other", incident.get("responseOther"))]

            # Conveyance Outcome
            if handover.get("outcome"):
                lines += ["", "-- Conveyance Outcome --",
                          fmt("Outcome Code", handover.get("outcome"))]
                if handover.get("otherOutcomeDetails"):
                    lines.append(fmt("Details", handover.get("otherOutcomeDetails")))

            # Receiving hospital
            if any(receiving.get(k) for k in ("hospital","ward","otherWard","otherHospitalDetails")):
                lines += ["", "-- Receiving Hospital --",
                          fmt("Hospital", receiving.get("hospital")),
                          fmt("Ward", receiving.get("ward")),
                          fmt("Other Ward", receiving.get("otherWard")),
                          fmt("Other Hospital Details", receiving.get("otherHospitalDetails"))]

            # Timings
            lines += ["", "-- Timings --",
                      fmt("Time Of Call", incident.get("timeOfCall")),
                      fmt("Time Mobile", incident.get("timeMobile")),
                      fmt("Time At Scene", incident.get("timeOfScene") or incident.get("timeAtScene")),
                      fmt("Time Leave Scene", incident.get("timeLeaveScene")),
                      fmt("Time At Hospital", incident.get("timeAtHospital")),
                      fmt("Time Handover", incident.get("timeHandover")),
                      fmt("Time Left Hospital", incident.get("timeLeftHospital")),
                      fmt("Time At Treatment Centre", incident.get("timeAtTreatmentCentre")),
                      fmt("Time Of Prealert", incident.get("timeOfPrealert"))]

            body = "\n".join(lines)
            to_addrs = [finance_recipient]

            try:
                emailer.send_email(subject=subject, body=body, recipients=to_addrs)
            except Exception as email_err:
                current_app.logger.error(f"Failed to send finance email: {email_err}")
        else:
            current_app.logger.warning("FINANCE_EMAIL not configured; skipping finance notification")

    cursor.close()
    conn.close()
    _audit_epcr_api(f"EPCR API closed case {case_id}")
    payload["recordVersion"] = new_rv
    return (
        jsonify(
            {
                "message": "Case closed successfully",
                "case": payload,
                "serverAck": _epcr_server_ack(case_id, new_rv, datetime.utcnow()),
            }
        ),
        200,
    )


@internal_bp.route('/api/ping', methods=['GET', 'HEAD', 'OPTIONS'])
def epcr_api_ping():
    """Reachability for Cura / offline banner (no clinical payload). Unauthenticated; see ``_epcr_plugin_api_gate``."""
    if request.method == 'OPTIONS':
        return '', 200
    if request.method == 'HEAD':
        return '', 200
    return jsonify({'ok': True, 'service': 'medical_records_module'}), 200


@internal_bp.route('/api/search', methods=['GET', 'OPTIONS'])
def ecpr_search():
    """
    Patient search (DOB + postcode) for EPCR / Cura — same contract as legacy VITA lookup.
    Requires session authentication and clinical role; audited.
    """
    if request.method == 'OPTIONS':
        return '', 200
    auth_err = _require_epcr_json_api()
    if auth_err:
        return auth_err

    dob_str = request.args.get('date_of_birth')
    postcode = request.args.get('postcode')
    if not dob_str or not postcode:
        logger.warning("EPCR API search missing date_of_birth or postcode.")
        return jsonify({"error": "Missing date_of_birth or postcode"}), 400
    try:
        dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "Invalid date_of_birth format; please use YYYY-MM-DD"}), 400
    patients = Patient.search_by_dob_and_postcode(dob, postcode)
    if not patients:
        logger.warning("EPCR API patient search returned no match")
        return jsonify({"error": "No matching patient found"}), 404

    AuditLog.insert_log(
        _cura_auth_principal()[0] or "unknown",
        "EPCR API patient search (DOB+postcode)",
        patient_id=patients[0].get("id"),
    )
    current_date = datetime.utcnow().date()
    patients_list = []
    for p in patients:
        age = calculate_age(p.get('date_of_birth'), current_date) if p.get('date_of_birth') else "N/A"
        patients_list.append({
            "id": p.get("id"),
            "first_name": p.get("first_name"),
            "middle_name": p.get("middle_name"),
            "last_name": p.get("last_name"),
            "age": age,
            "gender": p.get("gender"),
            "address": p.get("address"),
            "postcode": p.get("postcode"),
            "date_of_birth": p.get("date_of_birth"),
            "package_type": p.get("package_type"),
            "care_company_id": p.get("care_company_id"),
            "access_requirements": p.get("access_requirements"),
            "risk_flags": p.get("notes"),
            "contact_number": p.get("contact_number"),
        })
    return jsonify({"message": "Patient search successful.", "patients": patients_list}), 200

from werkzeug.exceptions import HTTPException

# Catch all HTTP errors (404, 403, 500, etc.)
@public_bp.errorhandler(HTTPException)
def handle_http_exception(err):
    current_app.logger.warning(f"Public route HTTP error {err.code}: {err.description}")
    # Portal disabled / explicit 503 — do not redirect to login (would loop or confuse).
    if err.code == 503:
        return err.get_response()
    try:
        return redirect(url_for('care_company.care_company_login'))
    except Exception:
        return redirect('/care_company/login')

# Catch any other exception
@public_bp.errorhandler(Exception)
def handle_exception(err):
    current_app.logger.error(f"Public route unexpected error: {err}", exc_info=True)
    try:
        return redirect(url_for('care_company.care_company_login'))
    except Exception:
        return redirect('/care_company/login')

# =============================================================================
# Cura extension API (operational events, safeguarding, patient-contact reports)
# =============================================================================
from . import cura_routes
from .cura_datasets_routes import register as register_cura_datasets
from .cura_mi_routes import register as register_cura_mi
from .safeguarding_facade_routes import register as register_safeguarding_facade
from .medical_handover_aliases import register as register_medical_handover_aliases
from .safeguarding_module_api_bp import safeguarding_module_api_bp
from .minor_injury_module_api_bp import minor_injury_module_api_bp


@minor_injury_module_api_bp.before_request
def _minor_injury_plugin_api_gate():
    if request.method == "OPTIONS":
        return None
    return _require_epcr_json_api()


cura_routes.register(internal_bp)
register_safeguarding_facade(internal_bp)
register_cura_datasets(internal_bp)
register_cura_mi(internal_bp)
register_cura_mi(minor_injury_module_api_bp, api_prefix="")
register_medical_handover_aliases(internal_bp)

from app.plugins.crm_module.planner_routes import register_cura_event_planner_on_internal

register_cura_event_planner_on_internal(internal_bp)

# =============================================================================
# Blueprint Registration Functions
# =============================================================================
def get_blueprint():
    return internal_bp


def get_blueprints():
    """Medical records + handover bases (safeguarding + legacy minor_injury URL prefix)."""
    return [internal_bp, safeguarding_module_api_bp, minor_injury_module_api_bp]

def get_public_blueprint():
    """Backward compatibility: care-company portal only. Prefer ``get_public_blueprints()``."""
    return public_bp


def get_public_blueprints():
    """Care-company portal (/care_company/*) and root ``/case-access`` for public EPCR lookup."""
    return [public_bp, case_access_public_bp]
