from app import socketio
import io
import os
import time
import threading
import random
import string
import uuid
import json
import hashlib
import math
import re
from collections import defaultdict
from urllib.parse import urlencode, quote
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from datetime import datetime, timedelta, date, timezone
from flask import (
    Blueprint, request, jsonify, render_template, current_app,
    redirect, url_for, flash, session, g, has_request_context,
    send_file, abort, Response,
)
from flask_login import login_required, current_user, login_user, logout_user
from werkzeug.security import check_password_hash
from flask_mail import Message, Mail
# Adjust as needed
from app.objects import PluginManager, AuthManager, User, get_db_connection, has_permission
from app.organization_profile import (
    normalize_organization_industries,
    tenant_matches_industry,
)
from .objects import ResponseTriage, GOOGLE_MAPS_API_KEY
from .triage_slug_industries import VENTUS_TRIAGE_SLUG_INDUSTRIES
import logging
import requests

# JWT auth for MDT API (Bearer; avoids CSRF for non-browser clients)
_jwt_decode = None
try:
    from app.auth_jwt import decode_session_token as _jwt_decode
except ImportError:
    pass

logger = logging.getLogger('ventus_response_module')
logger.setLevel(logging.INFO)

# Last panic POST time per resolved callsign (light rate limit; in-process only).
_mdt_panic_last_ts = {}
# Running call: separate light rate limit (self-dispatch CAD, not grade-1 panic).
_mdt_running_call_last_ts = {}

# Job statuses where panic should open a new CAD instead of attaching to the patient record.
_MDT_PANIC_TERMINAL_JOB_STATUSES = frozenset({
    'cleared', 'stood_down', 'cancelled', 'completed', 'closed', 'archived', 'ended', 'complete',
})

# Last MDT unit cleared the incident, but CAD stays on the stack until dispatch closure review / Close CAD.
_JOB_STATUS_AWAITING_DISPATCH_REVIEW = 'dispatch_review'

# Jobs that must not be auto-returned to "queued" when the last unit is diverted away (keep review / closure state).
_JOB_STATUSES_SKIP_EMPTY_UNLINK_QUEUE = frozenset({
    'cleared',
    _JOB_STATUS_AWAITING_DISPATCH_REVIEW,
})

# Past / history list and reopen: CADs dispatch closed (cleared) or stood down entirely.
_JOB_STATUSES_PAST_OR_CLOSED_STACK = frozenset({'cleared', 'stood_down'})

# Stored in mdt_job_comms for audit / merged system log, but not listed under CAD "Updates & Messages".
_JOB_COMMS_MESSAGE_TYPES_AUDIT_ONLY = frozenset({'closed', 'reopened'})


def _job_comms_audit_only_types_sql():
    """Return (tuple of types, comma-separated %s placeholders) for NOT IN clauses."""
    parts = tuple(sorted(_JOB_COMMS_MESSAGE_TYPES_AUDIT_ONLY))
    placeholders = ','.join(['%s'] * len(parts)) if parts else ''
    return parts, placeholders

# mdt_response_log.status — job-level markers (timestamps + system log; not MDT ladder values).
_RESPONSE_LOG_STATUS_CLOSURE_REVIEW = 'closure_review'
_RESPONSE_LOG_STATUS_DISPATCH_REOPENED = 'dispatch_reopened'
_RESPONSE_LOG_STATUS_DISPATCH_DIVISION_CHANGED = 'dispatch_division_changed'
_RESPONSE_LOG_STATUS_DISPATCH_SHIFT_TIMES_CHANGED = 'dispatch_shift_times_changed'


def _mdt_next_allocated_job_cad(cur):
    """
    Next CAD number for an explicit INSERT into mdt_jobs — must match mdt_panic and any other
    path that sets cad via MAX(cad)+1. Intake/triage must NOT use response_triage.id as cad:
    that is a separate AUTO_INCREMENT sequence and collides with existing jobs (e.g. panic),
    causing duplicate-key enqueue failures and redirects to the wrong incident.
    """
    cur.execute("SELECT COALESCE(MAX(cad), 0) + 1 AS n FROM mdt_jobs")
    nrow = cur.fetchone()
    if isinstance(nrow, dict):
        raw = nrow.get("n")
    else:
        raw = nrow[0] if nrow else None
    try:
        return int(raw) if raw is not None else 1
    except (TypeError, ValueError):
        return 1


def _ventus_panic_voice_dir():
    d = os.path.join(current_app.instance_path, 'ventus_panic_voice')
    os.makedirs(d, exist_ok=True)
    return d


def _ensure_mdt_job_panic_voice_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mdt_job_panic_voice (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          cad INT NOT NULL,
          callsign VARCHAR(64) NOT NULL,
          rel_path VARCHAR(512) NOT NULL,
          mime_type VARCHAR(128) NOT NULL,
          file_bytes INT NOT NULL,
          sha256_hex CHAR(64) NULL,
          started_at VARCHAR(80) NULL,
          ended_at VARCHAR(80) NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          INDEX ix_mjpv_cad (cad)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )

# Incident updates live in mdt_job_comms (Response Centre / Incident Inbox). They must not be
# duplicated into the crew `messages` thread (Conversations panel / MDT Messages tab).


def _sql_exclude_mirrored_cad_update_messages():
    """SQL predicate: rows fanning out incident updates into `messages` (legacy + blocked going forward)."""
    return (
        "LOWER(COALESCE(text,'')) NOT REGEXP "
        "'^[[:space:]]*cad[[:space:]]*#[0-9]+[[:space:]]+update:'"
    )


def _messages_recent_duplicate_id(cur, sender, recipient, text, window_seconds=5):
    """Return ``messages.id`` if the same row was stored within the last few seconds.

    MDT / SPA clients sometimes POST twice in quick succession (double submit, React Strict
    Mode, or retry). Without this, two identical rows appear and the Messages UI shows
    duplicate *SENT* lines for one user action.
    """
    if not text or not sender or not recipient:
        return None
    try:
        w = max(1, min(30, int(window_seconds)))
    except (TypeError, ValueError):
        w = 5
    cur.execute(
        """
        SELECT id FROM messages
        WHERE LOWER(TRIM(`from`)) = LOWER(TRIM(%s))
          AND LOWER(TRIM(recipient)) = LOWER(TRIM(%s))
          AND text = %s
          AND timestamp >= DATE_SUB(NOW(), INTERVAL %s SECOND)
        ORDER BY id DESC
        LIMIT 1
        """,
        (sender, recipient, text, w),
    )
    row = cur.fetchone()
    if not row:
        return None
    if isinstance(row, dict):
        return row.get("id")
    return row[0] if row else None


def _emit_mdt_job_assigned(cad, callsigns):
    """Notify MDT clients of a new dispatcher-driven assignment.

    Separate from ``jobs_updated`` / ``units_updated`` so crew apps can always
    surface the full-screen job alert (e.g. while the Messages tab is active).
    Payload: ``{type: 'job_assigned', cad, callsign}`` per unit.
    """
    if cad is None:
        return
    try:
        cad_int = int(cad)
    except (TypeError, ValueError):
        return
    for cs in callsigns or []:
        s = str(cs or "").strip().upper()
        if not s:
            continue
        try:
            socketio.emit(
                "mdt_event",
                {"type": "job_assigned", "cad": cad_int, "callsign": s}
            )
        except Exception as ex:
            logger.warning(
                "mdt_event job_assigned emit failed cad=%s callsign=%s: %s",
                cad_int,
                s,
                ex,
            )
        try:
            _mdt_web_push_notify_callsign(
                s,
                "New job",
                f"CAD #{cad_int} assigned — open MDT to respond.",
                tag=f"ventus-new-job-{cad_int}",
                alert=True,
                silent=False,
                require_interaction=True,
                cad=cad_int,
            )
        except Exception:
            pass


# In-memory storage for one-time admin PINs.
# Example: {"pin": "123456", "expires_at": datetime_object, "generated_by": "ClinicalLeadUser"}
admin_pin_store = {}
_schema_bootstrap_state = {
    "mdts_signed_on": False,
    "response_log": False,
    "crew_removal_log": False,
    "session_lifecycle_event": False,
    "crew_profiles": False,
    "mdt_user_mdt_session": False,
}


def _json_compatible(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_compatible(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_compatible(v) for v in value]
    if isinstance(value, tuple):
        return [_json_compatible(v) for v in value]
    return value


def _jsonify_safe(payload, status=200):
    return jsonify(_json_compatible(payload)), status


def _sender_label_from_portal(sender_hint, username):
    hint = _role_key(sender_hint)
    user = str(username or '').strip()
    if hint in ('dispatch', 'dispatcher', 'cad_dispatch', 'controller'):
        return f"Dispatcher ({user})" if user else "Dispatcher"
    if hint in (
        'response_centre', 'response_center', 'response', 'ro', 'response_officer',
        'call_centre', 'call_center', 'call_taker', 'calltaker', 'call_handler', 'callhandler'
    ):
        return f"RO ({user})" if user else "RO"
    raw = str(sender_hint or '').strip()
    return user or raw or 'dispatcher'


def _stack_status_pretty(slug):
    """Human-readable job stack status for logs (aligned with job_system_log response rows)."""
    s = str(slug or '').strip().lower()
    if not s:
        return 'Unknown'
    # Match CAD UI wording (prettyStatus); slug is still dispatch_review in DB/API.
    if s == 'dispatch_review':
        return 'Closure review'
    st = s.replace('_', ' ')
    return ' '.join(x.capitalize() for x in st.split())


def _job_system_log_comms_display_message(message_type, message_text):
    """Build one system-log body line from a job comm row.

    The log card already shows **By** separately, so the message must not repeat
    ``{type} by {actor}: …`` — that duplicated the sender (e.g. reopen rows).
    """
    mt = str(message_type or 'message').strip().lower().replace('_', ' ')
    title = ' '.join(w.capitalize() for w in mt.split()) if mt else 'Event'
    raw = str(message_text or '').strip()
    if raw:
        return f'{title}: {raw}'
    return title


def _job_system_log_created_ts_text(val):
    """Format job ``created_at`` for synthetic CAD-created log lines (no extra TZ math)."""
    if val is None:
        return ''
    try:
        if isinstance(val, datetime):
            return val.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(val, date) and not isinstance(val, datetime):
            return val.isoformat()
    except Exception:
        pass
    return str(val).strip()


def _job_comm_sender_is_viewer(sender_label, viewer_username):
    """True when inbox row was authored by the current user (matches stored label formats)."""
    view = str(viewer_username or '').strip().lower()
    if not view:
        return False
    sl = str(sender_label or '').strip()
    if not sl:
        return False
    sll = sl.lower()
    if sll == view:
        return True
    for prefix in ('dispatcher (', 'ro ('):
        if sll.startswith(prefix) and sl.endswith(')'):
            inner = sl[sl.rfind('(') + 1:sl.rfind(')')].strip().lower()
            if inner == view:
                return True
    return False


def calculate_age(born, today=None):
    if today is None:
        today = datetime.utcnow().date()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


def _ventus_audit_unavailable_response():
    """503 when ``audit_failure=fail_closed`` and ``audit_logs`` insert failed after retry."""
    return jsonify({
        'error': 'audit_unavailable',
        'message': (
            'Operational audit could not be persisted. Changes were not saved. '
            'Retry shortly or contact support if this continues.'
        ),
    }), 503


def _emit_siem_audit_line(*, user, action, cad, ts_iso):
    """
    Single JSON log line for SIEM ingestion when durable ``audit_logs`` write failed.
    Fixed fields: user, cad, action, ts (plus ``event`` discriminator).
    """
    rec = {
        'event': 'ventus_siem_audit',
        'user': user,
        'cad': cad,
        'action': action,
        'ts': ts_iso,
    }
    try:
        logger.error('ventus_siem_audit %s', json.dumps(rec, default=str))
    except Exception as log_err:
        logger.error(
            'ventus_siem_audit_emit_failed user=%s action=%s err=%s',
            user,
            action,
            log_err,
        )


def _ventus_public_server_error(exc):
    """Stable 5xx JSON for MDT/sessionless clients; raw exception text only when Flask debug is on."""
    body = {'error': 'server_error'}
    try:
        if current_app and getattr(current_app, 'debug', False):
            body['detail'] = str(exc)
    except Exception:
        pass
    return jsonify(body), 500


def _ventus_public_server_error_body(exc):
    """Dict body for handlers that return (dict, status) before jsonify."""
    body = {'error': 'server_error'}
    try:
        if current_app and getattr(current_app, 'debug', False):
            body['detail'] = str(exc)
    except Exception:
        pass
    return body


def _mdt_audit_actor():
    """Username for Ventus/MDT audit lines (JWT MDT user or Flask-Login session)."""
    try:
        mu = getattr(g, 'mdt_user', None)
        if isinstance(mu, dict):
            u = str(mu.get('username') or mu.get('id') or '').strip()
            if u:
                return u
    except Exception:
        pass
    try:
        if current_user.is_authenticated:
            u = str(getattr(current_user, 'username', None)
                    or getattr(current_user, 'id', '') or '').strip()
            if u:
                return u
    except Exception:
        pass
    return 'unknown'


def _mdt_signed_on_crew_usernames(crew_raw):
    """Lowercase usernames from mdts_signed_on.crew JSON."""
    out = []
    try:
        if isinstance(crew_raw, (bytes, bytearray)):
            crew_raw = crew_raw.decode('utf-8', errors='ignore')
        if isinstance(crew_raw, str):
            data = json.loads(crew_raw) if crew_raw.strip() else []
        elif isinstance(crew_raw, list):
            data = crew_raw
        else:
            data = []
        for m in data:
            if isinstance(m, dict):
                u = str(m.get('username') or m.get(
                    'user') or '').strip().lower()
                if u:
                    out.append(u)
            else:
                u = str(m).strip().lower()
                if u:
                    out.append(u)
    except Exception:
        pass
    return out


def _unit_crew_has_members(crew_raw):
    """True if mdts_signed_on.crew JSON lists at least one username (staff on the callsign)."""
    return len(_mdt_signed_on_crew_usernames(crew_raw)) > 0


def _mdt_signed_on_crew_display_labels(crew_raw):
    """Display names for CAD panic / audio UI: prefer displayName/name, else username."""
    labels = []
    seen = set()
    try:
        if isinstance(crew_raw, (bytes, bytearray)):
            crew_raw = crew_raw.decode('utf-8', errors='ignore')
        if isinstance(crew_raw, str):
            data = json.loads(crew_raw) if crew_raw.strip() else []
        elif isinstance(crew_raw, list):
            data = crew_raw
        else:
            data = []
        for m in data:
            if isinstance(m, dict):
                label = str(
                    m.get('displayName')
                    or m.get('display_name')
                    or m.get('name')
                    or m.get('fullName')
                    or m.get('full_name')
                    or m.get('username')
                    or m.get('user')
                    or ''
                ).strip()
                if not label:
                    continue
                key = label.lower()
                if key in seen:
                    continue
                seen.add(key)
                labels.append(label)
            else:
                label = str(m).strip()
                if not label:
                    continue
                key = label.lower()
                if key in seen:
                    continue
                seen.add(key)
                labels.append(label)
    except Exception:
        pass
    return labels


def _mdt_jwt_allowed_for_callsign_crew(cur, cs):
    """Bearer JWT user must appear on the signed-on crew list for this callsign."""
    mu = getattr(g, 'mdt_user', None)
    if not isinstance(mu, dict):
        return True
    actor = str(mu.get('username') or mu.get('id') or '').strip().lower()
    if not actor:
        return False
    cur.execute(
        "SELECT crew FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
        (cs,),
    )
    row = cur.fetchone() or {}
    crew_users = _mdt_signed_on_crew_usernames(row.get('crew'))
    return actor in crew_users


def _mdt_panic_voice_access_scope(cur, cs, cad):
    """Whether this callsign may list/stream panic audio for ``cad``.

    ``all`` — unit appears on ``mdt_response_log`` for this CAD (see all units' clips).
    ``own`` — no log row but this unit has a stored panic clip for the CAD.
    ``None`` — no access.
    """
    try:
        cad_i = int(cad)
    except Exception:
        return None
    if cad_i <= 0:
        return None
    cur.execute(
        "SELECT 1 FROM mdt_response_log WHERE callSign = %s AND cad = %s LIMIT 1",
        (cs, cad_i),
    )
    if cur.fetchone():
        return "all"
    cur.execute(
        "SELECT 1 FROM mdt_job_panic_voice WHERE cad = %s AND callsign = %s LIMIT 1",
        (cad_i, cs),
    )
    if cur.fetchone():
        return "own"
    return None


def _audit_fallback_structured(
    *,
    user,
    action,
    cad=None,
    callsign=None,
    details=None,
    route=None,
    err=None,
    ts=None,
):
    """Extended context when ``audit_logs`` insert fails (grep ``ventus_audit_fallback``)."""
    payload = {
        'event': 'ventus_audit_fallback',
        'user': user,
        'action': action,
        'cad': cad,
        'callsign': callsign,
        'ts': ts,
        'route': route,
        'err_type': type(err).__name__ if err is not None else None,
        'err': (str(err)[:500] if err is not None else None),
    }
    if details is not None:
        try:
            payload['details'] = json.loads(json.dumps(details, default=str))
        except Exception:
            payload['details'] = str(details)[:800]
    try:
        logger.error('ventus_audit_fallback %s',
                     json.dumps(payload, default=str))
    except Exception as log_err:
        logger.error(
            'ventus_audit_fallback_emit_failed user=%s action=%s err=%s',
            user,
            action,
            log_err,
        )


# For audit logging: clinical audit_logs table when medical_records is present, else structured fallback only.
def log_audit(user, action, patient_id=None, details=None, *, audit_failure: str = 'siem_fallback'):
    """
    Write Ventus/CAD/MDT audit: ``audit_logs`` via medical_records (retry once), app logger, Sentry breadcrumb.

    ``audit_failure``:
      - ``siem_fallback`` (default): if DB insert fails, emit ``ventus_siem_audit`` + ``ventus_audit_fallback``;
        always returns ``True`` (caller continues).
      - ``fail_closed``: same fallbacks on DB failure; returns ``False`` so caller must rollback and 503.

    Returns:
        ``True`` if ``audit_logs`` row was written, or policy is ``siem_fallback`` after fallback logging.
        ``False`` only when ``audit_failure='fail_closed'`` and DB insert failed.
    """
    user_s = str(user or 'unknown').strip() or 'unknown'
    action_s = str(action or '').strip() or 'unknown_action'
    cad = None
    callsign = None
    if isinstance(details, dict):
        cad = details.get('cad')
        if cad is not None:
            try:
                cad = int(cad)
            except (TypeError, ValueError):
                pass
        callsign = details.get('callsign') or details.get('callSign')
        if callsign is not None:
            callsign = str(callsign).strip().upper() or None
    reason_blob = None
    if details is not None:
        try:
            reason_blob = json.dumps(details, default=str)[:2000]
        except Exception:
            reason_blob = str(details)[:2000]

    full_action = f'ventus:{action_s}'
    if cad is not None:
        full_action += f' cad={cad}'
    if callsign:
        full_action += f' cs={callsign}'
    full_action = full_action[:512]

    principal_role = None
    route = None
    ip = None
    user_agent = None
    if has_request_context():
        try:
            principal_role = str(
                getattr(current_user, 'role', '') or '').strip().lower() or None
        except Exception:
            pass
        try:
            route = str(getattr(request, 'endpoint', '') or '').strip() or None
        except Exception:
            pass
        try:
            ip = (request.headers.get('X-Forwarded-For')
                  or request.remote_addr or '').split(',')[0].strip() or None
            ua = (request.headers.get('User-Agent') or '').strip()
            user_agent = (ua[:255] if ua else None)
        except Exception:
            pass

    case_id = None
    if cad is not None:
        try:
            case_id = str(int(cad))
        except Exception:
            case_id = str(cad)[:64]

    extra = {'user': user_s, 'action': action_s,
             'patient_id': patient_id, 'details': details}
    db_ok = False
    last_err = None
    for attempt in (0, 1):
        try:
            from app.plugins.medical_records_module.objects import AuditLog
            AuditLog.insert_log(
                user_s,
                full_action,
                patient_id=patient_id,
                case_id=case_id,
                principal_role=principal_role,
                route=route,
                ip=ip,
                user_agent=user_agent,
                reason=reason_blob,
            )
            db_ok = True
            break
        except Exception as e:
            last_err = e
            if attempt == 0:
                try:
                    time.sleep(0.05)
                except Exception:
                    pass

    extra['audit_db_ok'] = db_ok
    try:
        audit_logger = getattr(current_app, 'audit_logger',
                               None) if has_request_context() else None
        if audit_logger:
            audit_logger.info(full_action, extra={'extra': extra})
        else:
            logger.info('AUDIT %s', json.dumps(extra, default=str))
    except Exception:
        try:
            logger.info('AUDIT %s', json.dumps(extra, default=str))
        except Exception:
            pass

    try:
        import sentry_sdk
        sentry_sdk.add_breadcrumb(
            category='audit', message=full_action, data=extra, level='info')
    except Exception:
        pass

    policy = (audit_failure or 'siem_fallback').strip().lower()
    if policy not in ('siem_fallback', 'fail_closed'):
        policy = 'siem_fallback'

    ts_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    if not db_ok:
        _emit_siem_audit_line(user=user_s, action=action_s,
                              cad=cad, ts_iso=ts_iso)
        _audit_fallback_structured(
            user=user_s,
            action=action_s,
            cad=cad,
            callsign=callsign,
            details=details,
            route=route,
            err=last_err,
            ts=ts_iso,
        )
        if policy == 'fail_closed':
            return False
    return True


# Sentinel: CAD "all divisions" focus (must not collide with _normalize_division('all') -> empty).
_CAD_DIVISION_ALL = '__all__'


def _normalize_division(value, fallback='general'):
    raw = str(value or '').strip().lower()
    if raw in ('', 'any', 'all', '*'):
        return str(fallback or '')
    safe = ''.join(ch for ch in raw if ch.isalnum() or ch in ('_', '-', '.'))
    return safe or str(fallback or '')


def _division_scope_where_sql(selected_division, include_external, access, has_division, expr):
    """Build ``AND ...`` fragment and args for job/unit division filtering."""
    if not has_division:
        if selected_division == _CAD_DIVISION_ALL:
            return '', []
        if selected_division and not include_external:
            if selected_division != 'general':
                return ' AND 1=0', []
            return '', []
        return '', []
    if selected_division == _CAD_DIVISION_ALL:
        if access.get('restricted') and not access.get('can_override_all'):
            allowed = [d for d in (access.get('divisions') or []) if d]
            if not allowed:
                return f' AND {expr} = %s', ['general']
            ph = ','.join(['%s'] * len(allowed))
            return f' AND {expr} IN ({ph})', list(allowed)
        return '', []
    if selected_division and not include_external:
        return f' AND {expr} = %s', [selected_division]
    return '', []


def _parse_int(value, fallback=None, min_value=None, max_value=None):
    try:
        out = int(value)
    except Exception:
        return fallback
    if min_value is not None and out < min_value:
        out = min_value
    if max_value is not None and out > max_value:
        out = max_value
    return out


def _safe_float(value, fallback=None):
    try:
        return float(value)
    except Exception:
        return fallback


def _coerce_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            try:
                return value.astimezone().replace(tzinfo=None)
            except Exception:
                return value.replace(tzinfo=None)
        return value
    if isinstance(value, date):
        try:
            return datetime(value.year, value.month, value.day)
        except Exception:
            return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
            if dt.tzinfo is not None:
                try:
                    dt = dt.astimezone().replace(tzinfo=None)
                except Exception:
                    dt = dt.replace(tzinfo=None)
            return dt
        except Exception:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(s, fmt)
                except Exception:
                    continue
    return None


def _parse_client_datetime(value):
    return _coerce_datetime(value)


def _extract_job_priority_value(payload):
    if not isinstance(payload, dict):
        return ""
    for key in ("call_priority", "priority", "acuity", "triage_priority"):
        raw = payload.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            return text
    return ""


def _priority_rank(priority_value):
    raw = str(priority_value or "").strip().lower()
    if not raw:
        return 99
    compact = raw.replace(" ", "").replace("-", "").replace("_", "")
    mapping = {
        "p1": 1,
        "1": 1,
        "red": 1,
        "critical": 1,
        "immediate": 1,
        "lifethreatening": 1,
        "grade1": 1,
        "g1": 1,
        "p2": 2,
        "2": 2,
        "amber": 2,
        "urgent": 2,
        "high": 2,
        "grade2": 2,
        "g2": 2,
        "p3": 3,
        "3": 3,
        "yellow": 3,
        "routine": 3,
        "normal": 3,
        "moderate": 3,
        "grade3": 3,
        "g3": 3,
        "p4": 4,
        "4": 4,
        "green": 4,
        "low": 4,
        "nonurgent": 4,
        "grade4": 4,
        "g4": 4,
        "p5": 5,
        "5": 5,
        "deferred": 5,
    }
    return mapping.get(compact, 99)


def _is_high_priority_value(priority_value):
    return _priority_rank(priority_value) <= 2


# Auto reassign: higher-priority queued/claimed CAD may pull units still pre-on-scene from lower-priority jobs.
_PRIORITY_PREEMPT_TARGET_STATUSES = frozenset({'queued', 'claimed'})
_PRIORITY_PREEMPT_SOURCE_STATUSES = frozenset(
    {'assigned', 'mobile', 'claimed', 'received'})


def _mdt_job_data_dict(raw_data):
    try:
        if raw_data is None:
            return {}
        if isinstance(raw_data, (bytes, bytearray)):
            raw_data = raw_data.decode('utf-8', errors='ignore')
        if isinstance(raw_data, str):
            d = json.loads(raw_data) if raw_data.strip() else {}
        elif isinstance(raw_data, dict):
            d = raw_data
        else:
            d = {}
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _strip_resolved_grade1_panic_from_job_payload(payload):
    """Clear live panic flags in stored job JSON after clear/reopen so dispatcher poll does not re-fire alarms."""
    if not isinstance(payload, dict):
        return
    try:
        payload['mdt_grade1_panic_active'] = False
    except Exception:
        pass
    try:
        ps = str(payload.get('priority_source') or '').strip().lower()
        if ps == 'mdt_panic':
            payload['priority_source'] = ''
    except Exception:
        pass


def _ensure_job_incident_inbox_anchor_column(cur):
    """Per-job floor for Response Centre inbox rows after a CAD is reopened (suppress pre-reopen history)."""
    try:
        cur.execute(
            "ALTER TABLE mdt_jobs ADD COLUMN incident_inbox_comms_after_id BIGINT NULL"
        )
    except Exception:
        pass


def run_priority_preemption_for_job(target_cad: int) -> None:
    """Stand down units still pre-on-scene on lower-priority jobs and assign them to ``target_cad``.

    Source jobs must be in ``assigned`` / ``mobile`` / ``claimed`` / ``received`` (not ``on_scene`` or
    transport states). Target must be ``queued`` or ``claimed``. Same division only. Optional
    ``required_skills`` on the target job must be satisfied (Training + HR register).
    """
    try:
        target_cad = int(target_cad)
    except (TypeError, ValueError):
        return

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    reassigned_from = set()
    assigned_units = []
    division_transfer_notifications = []
    committed = False
    try:
        _ensure_job_units_table(cur)
        _ensure_meal_break_columns(cur)
        _ventus_set_g_break_policy(cur)
        _ensure_response_log_table(cur)

        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_job_div_col = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_unit_div = cur.fetchone() is not None

        div_sel = (
            "LOWER(TRIM(COALESCE(division, 'general'))) AS division"
            if has_job_div_col
            else "'general' AS division"
        )
        cur.execute(
            f"SELECT cad, status, data, {div_sel} FROM mdt_jobs WHERE cad = %s LIMIT 1",
            (target_cad,),
        )
        trow = cur.fetchone()
        if not trow:
            return
        targ_status = str(trow.get('status') or '').strip().lower()
        if targ_status not in _PRIORITY_PREEMPT_TARGET_STATUSES:
            return

        new_payload = _mdt_job_data_dict(trow.get('data'))
        new_prio_val = _extract_job_priority_value(new_payload)
        new_rank = _priority_rank(new_prio_val)
        if new_rank >= 99:
            return
        job_is_high_priority = _is_high_priority_value(new_prio_val)
        job_division = _normalize_division(
            trow.get('division'), fallback='general')
        required_skills = _extract_required_skills(new_payload)

        sender_name = _sender_label_from_portal('dispatch', 'priority_preempt')
        cur.execute("SHOW TABLES LIKE 'messages'")
        has_messages = cur.fetchone() is not None

        src_list = sorted(_PRIORITY_PREEMPT_SOURCE_STATUSES)
        ph = ','.join(['%s'] * len(src_list))
        cur.execute(
            f"""
            SELECT cad, status, data, {div_sel}
            FROM mdt_jobs
            WHERE cad <> %s
              AND LOWER(TRIM(COALESCE(status, ''))) IN ({ph})
            """,
            (target_cad,) + tuple(src_list),
        )
        orows = list(cur.fetchall() or [])

        def _row_prio_rank(row):
            return _priority_rank(_extract_job_priority_value(_mdt_job_data_dict(row.get('data'))))

        orows.sort(key=lambda r: (-_row_prio_rank(r), int(r.get('cad') or 0)))

        seen_callsigns = set()

        for orow in orows:
            try:
                old_cad = int(orow.get('cad'))
            except (TypeError, ValueError):
                continue
            old_rank = _row_prio_rank(orow)
            if old_rank <= new_rank:
                continue
            odiv = _normalize_division(
                orow.get('division'), fallback='general')
            if odiv != job_division:
                continue

            for cs in _get_job_unit_callsigns(cur, old_cad):
                cs = str(cs or '').strip()
                if not cs:
                    continue
                cu = cs.upper()
                if cu in seen_callsigns:
                    continue

                cur.execute(
                    "SELECT assignedIncident, crew FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
                    (cs,),
                )
                unit_state = cur.fetchone() or {}
                old_inc = unit_state.get('assignedIncident')
                try:
                    old_inc = int(old_inc) if old_inc is not None else None
                except (TypeError, ValueError):
                    old_inc = None
                if old_inc != old_cad:
                    continue

                cur.execute(
                    """
                    SELECT status, signOnTime, mealBreakStartedAt, mealBreakUntil, mealBreakTakenAt,
                           shiftStartAt, shiftEndAt, shiftDurationMins, breakDueAfterMins
                    FROM mdts_signed_on
                    WHERE callSign = %s
                    LIMIT 1
                    """,
                    (cs,),
                )
                shift_row = cur.fetchone() or {}
                shift_state = _compute_shift_break_state(shift_row)
                if shift_state.get('break_blocked_for_new_jobs') and not job_is_high_priority:
                    continue

                crew_json = unit_state.get('crew') or '[]'
                if not _unit_crew_has_members(crew_json):
                    continue
                if required_skills:
                    unit_skills = _dispatch_skill_set_for_unit(cur, crew_json)
                    if not required_skills.issubset(unit_skills):
                        continue

                unit_div_sql = (
                    "LOWER(TRIM(COALESCE(division, 'general'))) AS division"
                    if has_unit_div
                    else "'general' AS division"
                )
                cur.execute(
                    f"SELECT callSign, crew, {unit_div_sql} FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
                    (cs,),
                )
                unit = cur.fetchone()
                if not unit:
                    continue
                unit_division = _normalize_division(
                    unit.get('division'), fallback='general')
                should_transfer = bool(
                    has_unit_div and unit_division != job_division)
                if should_transfer:
                    instruction_id = _queue_division_transfer_instruction(
                        cur,
                        cs,
                        unit_division,
                        job_division,
                        actor='priority_preempt',
                        reason='priority_preemption',
                        cad=target_cad,
                    )
                    if instruction_id:
                        division_transfer_notifications.append({
                            'callsign': cs,
                            'instruction_id': instruction_id,
                        })

                reassigned_from.add(old_cad)
                cur.execute(
                    "DELETE FROM mdt_job_units WHERE job_cad = %s AND callsign = %s",
                    (old_cad, cs),
                )
                _sync_claimed_by_from_job_units(cur, old_cad)
                cur.execute(
                    "SELECT status FROM mdt_jobs WHERE cad = %s LIMIT 1", (old_cad,))
                old_j = cur.fetchone() or {}
                old_status = str(old_j.get('status') or '').strip().lower()
                remaining_old_units = _get_job_unit_callsigns(cur, old_cad)
                if len(remaining_old_units) == 0 and old_status not in _JOB_STATUSES_SKIP_EMPTY_UNLINK_QUEUE:
                    cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
                    has_updated_at = cur.fetchone() is not None
                    if has_updated_at:
                        cur.execute(
                            "UPDATE mdt_jobs SET status = 'queued', updated_at = NOW() WHERE cad = %s "
                            "AND LOWER(TRIM(COALESCE(status, ''))) <> 'cleared'",
                            (old_cad,),
                        )
                    else:
                        cur.execute(
                            "UPDATE mdt_jobs SET status = 'queued' WHERE cad = %s "
                            "AND LOWER(TRIM(COALESCE(status, ''))) <> 'cleared'",
                            (old_cad,),
                        )
                try:
                    cur.execute(
                        """
                        INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew)
                        VALUES (%s, %s, %s, NOW(), %s)
                        """,
                        (cs, old_cad, 'stood_down', crew_json),
                    )
                except Exception:
                    pass
                if has_messages:
                    cur.execute(
                        """
                        INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                        VALUES (%s, %s, %s, NOW(), 0)
                        """,
                        (
                            sender_name,
                            cs,
                            f"Higher-priority CAD #{target_cad}: stood down from CAD #{old_cad}. Proceed to new assignment.",
                        ),
                    )
                cur.execute(
                    """
                    UPDATE mdts_signed_on
                       SET assignedIncident = %s, status = 'assigned'{division_set}
                     WHERE callSign = %s
                    """.format(division_set=", division = %s" if should_transfer else ""),
                    tuple(
                        [target_cad] +
                        ([job_division] if should_transfer else []) + [cs]
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO mdt_job_units (job_cad, callsign, assigned_by)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        assigned_by = VALUES(assigned_by),
                        assigned_at = CURRENT_TIMESTAMP
                    """,
                    (target_cad, cs, 'priority_preempt'),
                )
                try:
                    cur.execute(
                        """
                        INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew)
                        VALUES (%s, %s, %s, NOW(), %s)
                        """,
                        (cs, target_cad, 'assigned', unit.get('crew') or '[]'),
                    )
                except Exception:
                    pass
                seen_callsigns.add(cu)
                assigned_units.append(cs)

        if not assigned_units:
            return

        cur.execute(
            """
            UPDATE mdt_jobs SET status = 'assigned'
            WHERE cad = %s AND LOWER(TRIM(COALESCE(status, ''))) IN ('queued', 'claimed')
            """,
            (target_cad,),
        )
        _sync_claimed_by_from_job_units(cur, target_cad)

        log_audit(
            'priority_preempt',
            'priority_preempt_assign',
            details={
                'cad': target_cad,
                'units': assigned_units,
                'from_cads': sorted(reassigned_from),
            },
        )
        conn.commit()
        committed = True
    except Exception:
        conn.rollback()
        logger.exception('priority preemption failed cad=%s', target_cad)
    finally:
        cur.close()
        conn.close()

    if not committed:
        return
    try:
        socketio.emit('mdt_event', {'type': 'jobs_updated', 'cad': target_cad})
        for oc in sorted(reassigned_from):
            socketio.emit('mdt_event', {'type': 'jobs_updated', 'cad': oc})
        for cs in assigned_units:
            socketio.emit(
                'mdt_event', {'type': 'units_updated', 'callsign': cs})
        _emit_mdt_job_assigned(target_cad, assigned_units)
        for notif in division_transfer_notifications:
            socketio.emit('mdt_event', {
                'type': 'dispatch_instruction',
                'callsign': notif.get('callsign'),
                'instruction_id': notif.get('instruction_id'),
                'instruction_type': 'division_transfer',
            })
    except Exception:
        logger.exception('priority preemption socket emit cad=%s', target_cad)


def _get_dispatch_break_policy(cur):
    """Meal / break timing defaults from mdt_dispatch_settings (row id=1)."""
    defaults = {
        'break_target_mode': 'fixed_minutes',
        'break_due_fixed_minutes': 240,
        'break_due_shift_percent': 50.0,
        'near_break_warning_minutes': 30,
        'meal_break_default_minutes': 45,
    }
    out = defaults.copy()
    try:
        _ensure_dispatch_settings_table(cur)
        cur.execute(
            """
            SELECT break_target_mode, break_due_fixed_minutes, break_due_shift_percent,
                   near_break_warning_minutes, meal_break_default_minutes
            FROM mdt_dispatch_settings WHERE id = 1 LIMIT 1
            """
        )
        row = cur.fetchone() or {}
        if row.get('break_target_mode') is not None:
            m = str(row.get('break_target_mode') or '').strip().lower()
            if m in ('fixed_minutes', 'shift_percent'):
                out['break_target_mode'] = m
        for k in ('break_due_fixed_minutes', 'near_break_warning_minutes', 'meal_break_default_minutes'):
            v = row.get(k)
            if v is not None:
                try:
                    out[k] = max(1, int(v))
                except Exception:
                    pass
        v = row.get('break_due_shift_percent')
        if v is not None:
            try:
                out['break_due_shift_percent'] = max(1.0, min(99.0, float(v)))
            except Exception:
                pass
    except Exception:
        pass
    return out


def _ventus_set_g_break_policy(cur):
    """Cache break policy on Flask g for _compute_shift_break_state (near-break minutes)."""
    if not has_request_context() or cur is None:
        return
    try:
        g._ventus_break_policy = _get_dispatch_break_policy(cur)
    except Exception:
        try:
            delattr(g, '_ventus_break_policy')
        except Exception:
            pass


def _ventus_near_break_threshold():
    if not has_request_context():
        return 30
    try:
        p = getattr(g, '_ventus_break_policy', None)
        if isinstance(p, dict):
            n = p.get('near_break_warning_minutes')
            if n is not None:
                return max(1, min(240, int(n)))
    except Exception:
        pass
    return 30


def _resolve_default_break_due_after_mins(cur, shift_duration_mins):
    """When client omits break_due_after, derive from admin policy + shift length."""
    pol = _get_dispatch_break_policy(cur)
    mode = str(pol.get('break_target_mode') or 'fixed_minutes').strip().lower()
    if mode == 'shift_percent':
        try:
            sd = int(shift_duration_mins) if shift_duration_mins is not None else None
        except Exception:
            sd = None
        if sd is None or sd < 1:
            mins = int(pol.get('break_due_fixed_minutes') or 240)
        else:
            pct = float(pol.get('break_due_shift_percent') or 50.0)
            pct = max(1.0, min(99.0, pct))
            mins = int(round(sd * (pct / 100.0)))
    else:
        mins = int(pol.get('break_due_fixed_minutes') or 240)
    return max(15, min(24 * 60, int(mins)))


def _compute_shift_break_state(row, now=None):
    now = now or datetime.utcnow()
    source = row or {}
    status = str(source.get("status") or "").strip().lower()

    sign_on_time = _coerce_datetime(source.get(
        "signOnTime") or source.get("sign_on_time"))
    shift_start = _coerce_datetime(source.get(
        "shiftStartAt") or source.get("shift_start_at") or sign_on_time)
    shift_end = _coerce_datetime(source.get(
        "shiftEndAt") or source.get("shift_end_at"))
    shift_duration_mins = _parse_int(
        source.get("shiftDurationMins") or source.get(
            "shift_duration_minutes"),
        fallback=None,
        min_value=0,
        max_value=24 * 60
    )
    break_due_after_mins = _parse_int(
        source.get("breakDueAfterMins") or source.get(
            "break_due_after_minutes"),
        fallback=240,
        min_value=60,
        max_value=12 * 60
    )

    if shift_start and shift_end and shift_duration_mins is None:
        try:
            shift_duration_mins = max(
                0, int((shift_end - shift_start).total_seconds() // 60))
        except Exception:
            shift_duration_mins = None
    if shift_start and shift_end is None and shift_duration_mins is not None:
        shift_end = shift_start + timedelta(minutes=int(shift_duration_mins))
    if shift_start is None and shift_end and shift_duration_mins is not None:
        shift_start = shift_end - timedelta(minutes=int(shift_duration_mins))

    shift_elapsed_mins = None
    if shift_start:
        try:
            shift_elapsed_mins = max(
                0, int((now - shift_start).total_seconds() // 60))
        except Exception:
            shift_elapsed_mins = None

    shift_remaining_mins = None
    if shift_end:
        try:
            shift_remaining_mins = int((shift_end - now).total_seconds() // 60)
        except Exception:
            shift_remaining_mins = None

    meal_break_started = _coerce_datetime(source.get(
        "mealBreakStartedAt") or source.get("meal_break_started_at"))
    meal_break_until = _coerce_datetime(source.get(
        "mealBreakUntil") or source.get("meal_break_until"))
    meal_break_taken_at = _coerce_datetime(source.get(
        "mealBreakTakenAt") or source.get("meal_break_taken_at"))
    meal_break_taken = meal_break_taken_at is not None

    meal_break_remaining_seconds = None
    meal_break_active = False
    if meal_break_until:
        try:
            meal_break_remaining_seconds = max(
                0, int((meal_break_until - now).total_seconds()))
        except Exception:
            meal_break_remaining_seconds = None
    if status == "meal_break" and meal_break_remaining_seconds is not None:
        meal_break_active = meal_break_remaining_seconds > 0

    break_due = False
    break_due_in_minutes = None
    if not meal_break_taken and break_due_after_mins is not None and shift_elapsed_mins is not None:
        break_due_in_minutes = max(
            0, int(break_due_after_mins - shift_elapsed_mins))
        break_due = shift_elapsed_mins >= break_due_after_mins
    _nb_warn = _ventus_near_break_threshold()
    near_break = bool(
        not meal_break_taken and break_due_in_minutes is not None and break_due_in_minutes <= _nb_warn)
    break_blocked_for_new_jobs = bool(
        break_due and not meal_break_active and not meal_break_taken)

    return {
        "shift_start_at": shift_start,
        "shift_end_at": shift_end,
        "shift_duration_minutes": shift_duration_mins,
        "shift_elapsed_minutes": shift_elapsed_mins,
        "shift_remaining_minutes": shift_remaining_mins,
        "break_due_after_minutes": break_due_after_mins,
        "break_due_in_minutes": break_due_in_minutes,
        "break_due": break_due,
        "near_break": near_break,
        "meal_break_started_at": meal_break_started,
        "meal_break_until": meal_break_until,
        "meal_break_taken_at": meal_break_taken_at,
        "meal_break_taken": meal_break_taken,
        "meal_break_active": meal_break_active,
        "meal_break_remaining_seconds": meal_break_remaining_seconds,
        "break_blocked_for_new_jobs": break_blocked_for_new_jobs,
    }


def _request_division_scope():
    raw = str(request.args.get('division') or '').strip()
    low = raw.lower()
    if low in ('__all__', 'all', '*'):
        return _CAD_DIVISION_ALL, True
    division = _normalize_division(raw, fallback='')
    include_external = str(request.args.get('include_external') or '').strip().lower() in (
        '1', 'true', 'yes', 'on'
    )
    return division, include_external


def _extract_job_division(payload, fallback='general'):
    try:
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode('utf-8', errors='ignore')
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        if not isinstance(payload, dict):
            return _normalize_division(fallback, fallback='general')
        for key in ('division', 'dispatch_division', 'operational_division', 'service_division'):
            candidate = _normalize_division(payload.get(key), fallback='')
            if candidate:
                return candidate
    except Exception:
        pass
    return _normalize_division(fallback, fallback='general')


def _normalize_hex_color(value, fallback='#64748b'):
    s = str(value or '').strip()
    if not s:
        return fallback
    if len(s) == 4 and s.startswith('#'):
        try:
            int(s[1:], 16)
            return "#" + "".join(ch * 2 for ch in s[1:]).lower()
        except Exception:
            return fallback
    if len(s) == 7 and s.startswith('#'):
        try:
            int(s[1:], 16)
            return s.lower()
        except Exception:
            return fallback
    return fallback


def _pretty_division_slug_label(slug):
    s = _normalize_division(slug, fallback='general')
    if s == 'general':
        return 'General'
    parts = [p for p in str(s).replace('-', '_').split('_') if p]
    if not parts:
        return s
    return ' '.join(p[:1].upper() + p[1:].lower() if p else '' for p in parts)


def _ensure_job_division_snapshot_columns(cur):
    try:
        cur.execute(
            "ALTER TABLE mdt_jobs ADD COLUMN division_snapshot_name VARCHAR(255) NULL"
        )
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdt_jobs ADD COLUMN division_snapshot_color VARCHAR(32) NULL"
        )
    except Exception:
        pass


def _jobs_have_division_snapshot_columns(cur):
    try:
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division_snapshot_name'")
        return cur.fetchone() is not None
    except Exception:
        return False


def _sync_job_division_snapshot_for_slug(cur, cad, slug):
    """Store display name + colour from the division catalog for audit (e.g. after clear)."""
    _ensure_job_division_snapshot_columns(cur)
    slug = _normalize_division(slug, fallback='general')
    _ensure_dispatch_divisions_table(cur)
    cur.execute(
        "SELECT name, color FROM mdt_dispatch_divisions WHERE LOWER(TRIM(slug)) = %s LIMIT 1",
        (slug,),
    )
    row = cur.fetchone() or {}
    name = (row.get('name') or '').strip() if isinstance(row, dict) else ''
    color = (row.get('color') or '').strip() if isinstance(row, dict) else ''
    if not name:
        name = _pretty_division_slug_label(slug)
    color = _normalize_hex_color(color, fallback='#64748b')
    cur.execute(
        """
        UPDATE mdt_jobs
        SET division_snapshot_name = %s, division_snapshot_color = %s
        WHERE cad = %s
        """,
        (name, color, cad),
    )


def _enrich_cleared_job_rows_division_audit(cur, rows):
    """Set division_label / division_color on cleared-job rows; expects optional sql_division on each row."""
    if not rows:
        return
    has_snap = _jobs_have_division_snapshot_columns(cur)
    need_cat = set()
    for r in rows:
        slug = _normalize_division(
            r.get('sql_division') or r.get('division'), fallback='general')
        snap_n = ((r.get('division_snapshot_name') or '')
                  if has_snap else '').strip()
        if not snap_n:
            need_cat.add(slug)
    catalog = {}
    if need_cat:
        _ensure_dispatch_divisions_table(cur)
        sl = list(need_cat)
        ph = ','.join(['%s'] * len(sl))
        cur.execute(
            f"SELECT LOWER(TRIM(slug)) AS slug, name, color FROM mdt_dispatch_divisions "
            f"WHERE LOWER(TRIM(slug)) IN ({ph})",
            tuple(sl),
        )
        for row in cur.fetchall() or []:
            if not row:
                continue
            k = str(row.get('slug') or '').lower()
            if k:
                catalog[k] = row
    default_c = '#64748b'
    for r in rows:
        slug = _normalize_division(
            r.get('sql_division') or r.get('division'), fallback='general')
        snap_n = ((r.get('division_snapshot_name') or '')
                  if has_snap else '').strip()
        snap_c = ((r.get('division_snapshot_color') or '')
                  if has_snap else '').strip()
        cat = catalog.get(slug.lower())
        cat_n = (cat.get('name') or '').strip() if cat else ''
        cat_c = (cat.get('color') or '').strip() if cat else ''
        if snap_n:
            r['division_label'] = snap_n
        else:
            r['division_label'] = cat_n or _pretty_division_slug_label(slug)
        if snap_c:
            r['division_color'] = _normalize_hex_color(
                snap_c, fallback=default_c)
        elif cat_c:
            r['division_color'] = _normalize_hex_color(
                cat_c, fallback=default_c)
        else:
            r['division_color'] = default_c
        r.pop('division_snapshot_name', None)
        r.pop('division_snapshot_color', None)


def _enrich_job_detail_division_audit(cur, job):
    """division_label / division_color for job detail; cleared jobs prefer frozen snapshot."""
    has_snap = _jobs_have_division_snapshot_columns(cur)
    slug = _normalize_division(job.get('division'), fallback='general')
    snap_n = ((job.get('division_snapshot_name') or '')
              if has_snap else '').strip()
    snap_c = ((job.get('division_snapshot_color') or '')
              if has_snap else '').strip()
    st = str(job.get('status') or '').strip().lower()
    cleared = st == 'cleared'
    _ensure_dispatch_divisions_table(cur)
    cur.execute(
        "SELECT name, color FROM mdt_dispatch_divisions WHERE LOWER(TRIM(slug)) = %s LIMIT 1",
        (slug,),
    )
    crow = cur.fetchone() or {}
    cat_n = (crow.get('name') or '').strip()
    cat_c = (crow.get('color') or '').strip()
    if cleared:
        job['division_label'] = snap_n or cat_n or _pretty_division_slug_label(
            slug)
        if snap_c:
            job['division_color'] = _normalize_hex_color(
                snap_c, fallback='#64748b')
        elif cat_c:
            job['division_color'] = _normalize_hex_color(
                cat_c, fallback='#64748b')
        else:
            job['division_color'] = '#64748b'
    else:
        job['division_label'] = cat_n or _pretty_division_slug_label(slug)
        job['division_color'] = _normalize_hex_color(
            cat_c, fallback='#64748b') if cat_c else '#64748b'
    job.pop('division_snapshot_name', None)
    job.pop('division_snapshot_color', None)


def _ensure_dispatch_settings_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_dispatch_settings (
            id TINYINT PRIMARY KEY,
            mode VARCHAR(16) NOT NULL DEFAULT 'auto',
            motd_text TEXT,
            motd_updated_by VARCHAR(120),
            motd_updated_at TIMESTAMP NULL DEFAULT NULL,
            default_division VARCHAR(64) NOT NULL DEFAULT 'general',
            updated_by VARCHAR(120),
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    # Best-effort compatibility for older schemas.
    try:
        cur.execute(
            "ALTER TABLE mdt_dispatch_settings ADD COLUMN motd_text TEXT")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdt_dispatch_settings ADD COLUMN motd_updated_by VARCHAR(120)")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdt_dispatch_settings ADD COLUMN motd_updated_at TIMESTAMP NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdt_dispatch_settings ADD COLUMN default_division VARCHAR(64) NOT NULL DEFAULT 'general'")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdt_dispatch_settings ADD COLUMN cura_event_job_filter_strict TINYINT(1) NOT NULL DEFAULT 0"
        )
    except Exception:
        pass
    for col_sql in (
        "ALTER TABLE mdt_dispatch_settings ADD COLUMN dispatcher_backup_label_p1 VARCHAR(120) NULL",
        "ALTER TABLE mdt_dispatch_settings ADD COLUMN dispatcher_backup_label_p2 VARCHAR(120) NULL",
        "ALTER TABLE mdt_dispatch_settings ADD COLUMN dispatcher_backup_label_p3 VARCHAR(120) NULL",
    ):
        try:
            cur.execute(col_sql)
        except Exception:
            pass
    for col_sql in (
        "ALTER TABLE mdt_dispatch_settings ADD COLUMN break_target_mode VARCHAR(32) NOT NULL DEFAULT 'fixed_minutes'",
        "ALTER TABLE mdt_dispatch_settings ADD COLUMN break_due_fixed_minutes INT NOT NULL DEFAULT 240",
        "ALTER TABLE mdt_dispatch_settings ADD COLUMN break_due_shift_percent DECIMAL(6,2) NOT NULL DEFAULT 50.00",
        "ALTER TABLE mdt_dispatch_settings ADD COLUMN near_break_warning_minutes INT NOT NULL DEFAULT 30",
        "ALTER TABLE mdt_dispatch_settings ADD COLUMN meal_break_default_minutes INT NOT NULL DEFAULT 45",
    ):
        try:
            cur.execute(col_sql)
        except Exception:
            pass


def _ensure_ventus_dispatch_broadcast_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ventus_dispatch_broadcast (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            sender_username VARCHAR(120) NOT NULL,
            scope_label VARCHAR(512) NOT NULL,
            body TEXT NOT NULL,
            recipient_callsigns TEXT NOT NULL,
            recipient_count INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def _broadcast_scope_label(selected_division, include_external):
    """Human-readable scope stored on broadcast rows (CAD division focus; broadcast never uses external)."""
    if (
        not selected_division
        or str(selected_division).strip() == ''
        or selected_division == _CAD_DIVISION_ALL
    ):
        base = 'All divisions visible to this CAD console'
    else:
        base = f'Division focus: {selected_division}'
    if include_external:
        ext = (
            ' — includes units tagged to other divisions (same as “show other divisions / greyed units” on the map)'
            if selected_division and selected_division != _CAD_DIVISION_ALL
            else ' — includes every division you are allowed to see'
        )
    else:
        ext = (
            ' — only units whose division tag matches the dropdown above'
            if selected_division and selected_division != _CAD_DIVISION_ALL
            else ' — units tagged only to “other” divisions (greyed) are excluded where the map filter applies'
        )
    return base + ext


def _dispatch_broadcast_target_callsigns(cur, selected_division, include_external):
    """Crewed signed-on callsigns for dispatch general broadcast (focused division only).

    ``include_external`` is always forced False at the broadcast route; greyed / other-division
    units are never recipients. Only units with at least one crew member on ``mdts_signed_on.crew``
    within the resolved division scope are included.
    """
    selected_division, include_external, access = _enforce_dispatch_scope(
        cur, selected_division, include_external)
    _ensure_mdts_signed_on_schema(cur)
    cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
    has_division = cur.fetchone() is not None
    cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'lastSeenAt'")
    has_last_seen = cur.fetchone() is not None
    cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'signOnTime'")
    has_sign_on_time = cur.fetchone() is not None
    division_sql = (
        "LOWER(TRIM(COALESCE(m.division, 'general'))) AS division"
        if has_division else "'general' AS division"
    )
    if has_last_seen and has_sign_on_time:
        seen_expr = "COALESCE(m.lastSeenAt, m.signOnTime)"
    elif has_last_seen:
        seen_expr = "m.lastSeenAt"
    elif has_sign_on_time:
        seen_expr = "m.signOnTime"
    else:
        seen_expr = "NOW()"
    sql = """
        SELECT m.callSign, m.crew, {division_sql}
        FROM mdts_signed_on m
        LEFT JOIN standby_locations s ON s.callSign = m.callSign
        WHERE m.status IS NOT NULL
          AND {seen_expr} >= DATE_SUB(NOW(), INTERVAL 120 MINUTE)
    """.format(division_sql=division_sql, seen_expr=seen_expr)
    unit_div_expr = "LOWER(TRIM(COALESCE(m.division, 'general')))"
    div_frag, args = _division_scope_where_sql(
        selected_division, include_external, access, has_division, unit_div_expr
    )
    sql += div_frag
    sql += " ORDER BY m.callSign ASC"
    cur.execute(sql, tuple(args))
    rows = cur.fetchall() or []
    callsigns = []
    for unit in rows:
        if not _unit_crew_has_members(unit.get('crew')):
            continue
        cs = str(unit.get('callSign') or '').strip()
        if cs:
            callsigns.append(cs)
    scope_label = _broadcast_scope_label(selected_division, include_external)
    return callsigns, scope_label


def _read_cura_event_job_filter_strict_from_database(cur) -> bool:
    """Stored preference only (ignores VENTUS_CURA_EVENT_JOB_FILTER_STRICT env)."""
    try:
        _ensure_dispatch_settings_table(cur)
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mdt_dispatch_settings' "
            "AND COLUMN_NAME = 'cura_event_job_filter_strict'"
        )
        if not cur.fetchone():
            return False
        cur.execute(
            "SELECT cura_event_job_filter_strict FROM mdt_dispatch_settings WHERE id = 1 LIMIT 1"
        )
        row = cur.fetchone()
        if isinstance(row, dict):
            return bool(row.get("cura_event_job_filter_strict"))
        return bool(row and row[0])
    except Exception:
        return False


def _cura_event_job_filter_strict_env_active() -> bool:
    ev = (os.environ.get("VENTUS_CURA_EVENT_JOB_FILTER_STRICT")
          or "").strip().lower()
    return ev in ("1", "true", "yes", "on")


def _dispatch_settings_cura_event_job_filter_strict(cur) -> bool:
    """Phase D: tighten job-board scope when viewing a live Cura operational division."""
    if _cura_event_job_filter_strict_env_active():
        return True
    return _read_cura_event_job_filter_strict_from_database(cur)


def _cura_active_operational_event_has_ventus_slug(cur, slug: str) -> bool:
    """True if medical_records `cura_operational_events` has a time-active row with this config.ventus_division_slug."""
    want = _normalize_division(slug, fallback="")
    if not want or want == "general":
        return False
    try:
        cur.execute("SHOW TABLES LIKE 'cura_operational_events'")
        if not cur.fetchone():
            return False
        cur.execute(
            """
            SELECT 1
            FROM cura_operational_events e
            WHERE LOWER(TRIM(COALESCE(e.status, ''))) IN (
                'active', 'live', 'open', 'running',
                'test', 'testing', 'simulation', 'sim', 'rehearsal', 'exercise', 'trial'
            )
              AND LOWER(TRIM(COALESCE(
                    JSON_UNQUOTE(JSON_EXTRACT(e.config, '$.ventus_division_slug')),
                    ''
              ))) = %s
              AND (e.starts_at IS NULL OR e.starts_at <= NOW())
              AND (e.ends_at IS NULL OR e.ends_at >= NOW())
            LIMIT 1
            """,
            (want,),
        )
        return cur.fetchone() is not None
    except Exception as ex:
        logger.warning(
            "_cura_active_operational_event_has_ventus_slug: %s", ex)
        return False


def _apply_cura_event_job_filter_policy(cur, selected_division, include_external, access) -> tuple:
    """
    If strict mode is on, users without can_override_all may not combine ``include_external``
    with a division slug that is the Ventus division of an active Cura operational period.
    """
    if not _dispatch_settings_cura_event_job_filter_strict(cur):
        return selected_division, include_external
    if access.get("can_override_all"):
        return selected_division, include_external
    if not include_external:
        return selected_division, include_external
    sel = _normalize_division(selected_division, fallback="")
    if not sel or sel == "general":
        return selected_division, include_external
    if _cura_active_operational_event_has_ventus_slug(cur, sel):
        return selected_division, False
    return selected_division, include_external


def _ensure_mdts_signed_on_schema(cur):
    """Repair legacy constraints that block real multi-unit dispatch behavior."""
    global _schema_bootstrap_state
    if _schema_bootstrap_state.get("mdts_signed_on"):
        return

    # Ensure columns used by modern routes exist on older databases.
    try:
        cur.execute("ALTER TABLE mdts_signed_on ADD COLUMN signOnTime DATETIME")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE mdts_signed_on ADD COLUMN lastSeenAt DATETIME")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN mealBreakStartedAt DATETIME NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN mealBreakUntil DATETIME NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN mealBreakTakenAt DATETIME NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN shiftStartAt DATETIME NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN shiftEndAt DATETIME NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN shiftDurationMins INT NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN breakDueAfterMins INT NULL DEFAULT 240")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN cad_placeholder TINYINT(1) NOT NULL DEFAULT 0")
    except Exception:
        pass
    try:
        cur.execute(
            "UPDATE mdts_signed_on SET lastSeenAt = COALESCE(lastSeenAt, signOnTime, NOW())")
    except Exception:
        pass

    try:
        cur.execute("SHOW INDEX FROM mdts_signed_on")
        rows = cur.fetchall() or []
    except Exception:
        return

    bad_unique_indexes = set()
    for row in rows:
        if isinstance(row, dict):
            name = str(row.get('Key_name') or '')
        else:
            # SHOW INDEX tuple: Table, Non_unique, Key_name, ...
            name = str(row[2] if len(row) > 2 else '')
        if name in ('status_UNIQUE', 'ipAddress_UNIQUE'):
            bad_unique_indexes.add(name)

    for idx in bad_unique_indexes:
        try:
            cur.execute(f"ALTER TABLE mdts_signed_on DROP INDEX `{idx}`")
        except Exception:
            pass

    try:
        cur.execute("CREATE INDEX idx_mdts_status ON mdts_signed_on (status)")
    except Exception:
        pass
    try:
        cur.execute("CREATE INDEX idx_mdts_seen ON mdts_signed_on (lastSeenAt)")
    except Exception:
        pass
    _schema_bootstrap_state["mdts_signed_on"] = True


def _ensure_mdt_user_mdt_session_table(cur):
    """One active MDT device session per JWT username (optional; see establish_mdt_device_session on signOn)."""
    global _schema_bootstrap_state
    if _schema_bootstrap_state.get("mdt_user_mdt_session"):
        return
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mdt_user_mdt_session (
                username VARCHAR(191) NOT NULL,
                session_id CHAR(36) NOT NULL,
                callsign VARCHAR(64) NOT NULL,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (username),
                KEY idx_mdt_user_session_callsign (callsign)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
    except Exception:
        pass
    _schema_bootstrap_state["mdt_user_mdt_session"] = True


def _mdt_api_session_binding_exempt(path_lower: str, method: str) -> bool:
    if "/api/mdt/signon" in path_lower:
        return True
    if "/api/mdt/signoff" in path_lower:
        return True
    if "/api/mdt/divisions" in path_lower and method == "GET":
        return True
    if "/api/mdt/signed-on-units" in path_lower and method == "GET":
        return True
    if "/api/mdt/push/vapid-public-key" in path_lower:
        return True
    return False


def _mdt_api_jwt_protected_path(path_lower: str) -> bool:
    """Bearer-JWT MDT JSON routes that share the same auth + device-session rules as /api/mdt/*."""
    return "/api/mdt" in path_lower or "/api/messages" in path_lower


def _ensure_dispatch_divisions_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_dispatch_divisions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            slug VARCHAR(64) NOT NULL UNIQUE,
            name VARCHAR(120) NOT NULL,
            color VARCHAR(16) NOT NULL DEFAULT '#64748b',
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            is_default TINYINT(1) NOT NULL DEFAULT 0,
            created_by VARCHAR(120),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_divisions_active (is_active)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    seeds = [
        ('general', 'General', '#64748b', 1),
        ('emergency', 'Emergency', '#ef4444', 0),
        ('urgent_care', 'Urgent Care', '#f59e0b', 0),
        ('events', 'Events', '#22c55e', 0)
    ]
    cur.execute("SELECT COUNT(*) AS n FROM mdt_dispatch_divisions")
    _cnt = cur.fetchone() or {}
    _n = int((_cnt.get('n') if isinstance(_cnt, dict)
             else (_cnt[0] if _cnt else 0)) or 0)
    if _n == 0:
        # First-time bootstrap only: insert default catalog rows.
        for slug, name, color, is_default in seeds:
            cur.execute("""
                INSERT INTO mdt_dispatch_divisions (slug, name, color, is_active, is_default, created_by)
                VALUES (%s, %s, %s, 1, %s, 'system')
                ON DUPLICATE KEY UPDATE
                    name = COALESCE(name, VALUES(name)),
                    color = COALESCE(color, VALUES(color))
            """, (slug, name, color, is_default))
    else:
        # Table already has data: do not re-seed optional divisions (admin deletes would otherwise
        # reappear on the next request). Only restore ``general`` if it is missing.
        cur.execute(
            "SELECT 1 FROM mdt_dispatch_divisions WHERE slug = 'general' LIMIT 1")
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO mdt_dispatch_divisions (slug, name, color, is_active, is_default, created_by)
                VALUES ('general', 'General', '#64748b', 1, 1, 'system')
            """)


def _dispatch_division_has_event_scope_columns(cur) -> bool:
    """Whether ``mdt_dispatch_divisions`` has Cura operational-event linkage columns."""
    try:
        cur.execute(
            """
            SELECT 1 FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mdt_dispatch_divisions'
              AND COLUMN_NAME = 'cura_operational_event_id'
            LIMIT 1
            """
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def _cura_operational_events_table_exists(cur) -> bool:
    try:
        cur.execute(
            """
            SELECT 1 FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cura_operational_events'
            LIMIT 1
            """
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def _upgrade_mdt_dispatch_divisions_event_scope(cur):
    """Add optional linkage from a dispatch division to a Cura operational period + time window."""
    _ensure_dispatch_divisions_table(cur)
    if _dispatch_division_has_event_scope_columns(cur):
        return
    try:
        cur.execute(
            """
            ALTER TABLE mdt_dispatch_divisions
            ADD COLUMN cura_operational_event_id INT NULL DEFAULT NULL,
            ADD COLUMN event_window_start DATETIME NULL DEFAULT NULL,
            ADD COLUMN event_window_end DATETIME NULL DEFAULT NULL
            """
        )
    except Exception as ex:
        logger.warning(
            "upgrade mdt_dispatch_divisions event scope (add columns): %s", ex)
    try:
        cur.execute(
            "CREATE INDEX idx_mdt_divisions_cura_op_event ON mdt_dispatch_divisions (cura_operational_event_id)"
        )
    except Exception:
        pass


def _dispatch_division_has_map_icon_column(cur) -> bool:
    try:
        cur.execute(
            """
            SELECT 1 FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mdt_dispatch_divisions'
              AND COLUMN_NAME = 'map_icon_url'
            LIMIT 1
            """
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def _upgrade_mdt_dispatch_divisions_map_icon(cur):
    _ensure_dispatch_divisions_table(cur)
    if _dispatch_division_has_map_icon_column(cur):
        return
    try:
        cur.execute(
            "ALTER TABLE mdt_dispatch_divisions ADD COLUMN map_icon_url VARCHAR(512) NULL DEFAULT NULL"
        )
    except Exception as ex:
        logger.warning("upgrade mdt_dispatch_divisions map_icon_url: %s", ex)


# Public path prefix for CAD / admin (served by dispatch_division_map_icon_file).
_DIVISION_MAP_ICON_ROUTE_PREFIX = (
    "/plugin/ventus_response_module/dispatch/division-map-icons/"
)


def _ventus_division_map_icons_dir():
    d = os.path.join(current_app.instance_path, "ventus_division_map_icons")
    os.makedirs(d, exist_ok=True)
    return d


def _sanitize_client_division_map_icon_url(val):
    """Accept only icons issued by this app (uuid.png under our route)."""
    if val in (None, "", False):
        return None
    s = str(val).strip()
    if not s:
        return None
    pfx = _DIVISION_MAP_ICON_ROUTE_PREFIX
    if not s.startswith(pfx):
        return None
    tail = s[len(pfx):].strip()
    if "/" in tail or "\\" in tail or ".." in tail:
        return None
    if not re.match(r"^[a-f0-9]{32}\.png$", tail, re.I):
        return None
    return pfx + tail.lower()


def _division_map_icon_abs_path_from_url(url):
    s = _sanitize_client_division_map_icon_url(url)
    if not s:
        return None
    tail = s[len(_DIVISION_MAP_ICON_ROUTE_PREFIX):]
    base = _ventus_division_map_icons_dir()
    path = os.path.normpath(os.path.join(base, tail))
    if not path.startswith(os.path.normpath(base + os.sep)):
        return None
    return path if os.path.isfile(path) else None


def _try_remove_division_map_icon_file(url):
    s = _sanitize_client_division_map_icon_url(url)
    if not s:
        return
    tail = s[len(_DIVISION_MAP_ICON_ROUTE_PREFIX):]
    base = os.path.normpath(_ventus_division_map_icons_dir())
    path = os.path.normpath(os.path.join(base, tail))
    if not str(path).startswith(str(base) + os.sep):
        return
    try:
        if os.path.isfile(path):
            os.remove(path)
    except OSError:
        pass


def _process_uploaded_division_map_icon(file_storage):
    """
    Read PNG/JPEG/WebP; resize to max 48×48; return PNG as (filename, bytes).
    Raises ValueError on invalid input.
    """
    from PIL import Image

    raw = file_storage.read()
    if not raw or len(raw) > 2 * 1024 * 1024:
        raise ValueError("Image empty or too large (max 2 MB before processing)")
    stream = io.BytesIO(raw)
    im = Image.open(stream)
    im.load()
    fmt = (im.format or "").upper()
    if fmt not in ("PNG", "JPEG", "WEBP", "JPG"):
        raise ValueError("Use PNG, JPEG, or WebP")
    im = im.convert("RGBA")
    w, h = im.size
    if w < 1 or h < 1 or max(w, h) > 4096:
        raise ValueError("Image dimensions invalid or too large (max 4096 px per side)")
    im.thumbnail((48, 48), Image.Resampling.LANCZOS)
    out = io.BytesIO()
    im.save(out, format="PNG", optimize=True)
    data = out.getvalue()
    if len(data) > 512 * 1024:
        raise ValueError("Processed PNG exceeds 512 KB")
    return uuid.uuid4().hex + ".png", data


def _naive_utc_dt(val):
    """Coerce DB/driver values to naive UTC-comparable datetimes for division window checks."""
    if val is None:
        return None
    if isinstance(val, datetime):
        if val.tzinfo:
            return val.replace(tzinfo=None)
        return val
    if isinstance(val, date) and not isinstance(val, datetime):
        return datetime.combine(val, datetime.min.time())
    if isinstance(val, (bytes, bytearray)):
        val = val.decode("utf-8", errors="ignore")
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)
            return dt
        except ValueError:
            return None
    return None


_CURA_OP_EVENT_TERMINAL_STATUSES = frozenset(
    {"closed", "archived", "cancelled", "complete", "completed", "ended"}
)


def _cura_operational_event_status_visible_on_dispatch_catalog(st: str) -> bool:
    """Match Cura sync + strict job filter: live periods and drafts in a defined window, not terminal states."""
    s = (st or "").strip().lower()
    if s in _CURA_OP_EVENT_TERMINAL_STATUSES:
        return False
    if s == "draft":
        return True
    return s in (
        "active",
        "live",
        "open",
        "running",
        "test",
        "testing",
        "simulation",
        "sim",
        "rehearsal",
        "exercise",
        "trial",
    )


def _catalog_visible_cura_event_division(row: dict, now_n: datetime, *, filter_event_window: bool) -> bool:
    """
    Event-linked dispatch divisions appear in MDT/CAD picklists when the parent Cura operational event is not
    terminal and the current time is inside any configured window (partial bounds allowed, same idea as
    ``event_active_for_automation``). Draft rows need at least one schedule bound so unscheduled drafts stay hidden.
    """
    oid = row.get("cura_operational_event_id")
    if not oid:
        return True
    if not filter_event_window:
        return True
    st = (row.get("op_status") or "").strip().lower()
    if not _cura_operational_event_status_visible_on_dispatch_catalog(st):
        return False
    ws = _naive_utc_dt(row.get("event_window_start")
                       ) or _naive_utc_dt(row.get("op_starts"))
    we = _naive_utc_dt(row.get("event_window_end")
                       ) or _naive_utc_dt(row.get("op_ends"))
    if st == "draft" and not ws and not we:
        return False
    if ws and now_n < ws:
        return False
    if we and now_n > we:
        return False
    return True


def _ensure_assist_requests_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_dispatch_assist_requests (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            request_type VARCHAR(32) NOT NULL DEFAULT 'unit_assist',
            from_division VARCHAR(64) NOT NULL,
            to_division VARCHAR(64) NOT NULL,
            callsign VARCHAR(64) NOT NULL,
            cad INT NULL,
            note TEXT,
            requested_by VARCHAR(120),
            status VARCHAR(24) NOT NULL DEFAULT 'pending',
            resolved_by VARCHAR(120),
            resolved_note TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP NULL DEFAULT NULL,
            INDEX idx_assist_status_to_division (status, to_division, created_at),
            INDEX idx_assist_callsign (callsign),
            INDEX idx_assist_cad (cad)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def _ensure_dispatch_user_access_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_dispatch_user_settings (
            username VARCHAR(120) PRIMARY KEY,
            can_override_all TINYINT(1) NOT NULL DEFAULT 0,
            updated_by VARCHAR(120),
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_dispatch_user_divisions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(120) NOT NULL,
            division VARCHAR(64) NOT NULL,
            created_by VARCHAR(120),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_dispatch_user_division (username, division),
            INDEX idx_dispatch_user (username),
            INDEX idx_dispatch_division (division)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def _standby_preset_has_division_column(cur) -> bool:
    try:
        cur.execute(
            """
            SELECT 1 FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mdt_standby_presets'
              AND COLUMN_NAME = 'division'
            LIMIT 1
            """
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def _upgrade_mdt_standby_presets_division(cur):
    """Dispatch division slug for CAD map filter / Cura-linked divisions (same slug as Division Control)."""
    try:
        if _standby_preset_has_division_column(cur):
            return
        cur.execute(
            "ALTER TABLE mdt_standby_presets ADD COLUMN division VARCHAR(64) NOT NULL DEFAULT 'general'"
        )
    except Exception as ex:
        logger.warning("upgrade mdt_standby_presets division: %s", ex)
    try:
        cur.execute(
            "CREATE INDEX idx_mdt_standby_preset_division ON mdt_standby_presets (division)"
        )
    except Exception:
        pass


def _dispatch_division_slug_exists(cur, slug) -> bool:
    s = _normalize_division(slug, fallback='')
    if not s:
        return False
    if s == 'general':
        return True
    _ensure_dispatch_divisions_table(cur)
    cur.execute(
        "SELECT 1 FROM mdt_dispatch_divisions WHERE LOWER(TRIM(slug)) = LOWER(TRIM(%s)) LIMIT 1",
        (s,),
    )
    return cur.fetchone() is not None


def _ensure_standby_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS standby_locations (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            callSign VARCHAR(64) NOT NULL,
            name VARCHAR(180) NOT NULL,
            lat DECIMAL(10,7) NOT NULL,
            lng DECIMAL(10,7) NOT NULL,
            source VARCHAR(32) NOT NULL DEFAULT 'manual',
            destinationType VARCHAR(32) NOT NULL DEFAULT 'standby',
            activationMode VARCHAR(32) NOT NULL DEFAULT 'immediate',
            state VARCHAR(32) NOT NULL DEFAULT 'active',
            cad INT NULL,
            what3words VARCHAR(80) NULL,
            address VARCHAR(255) NULL,
            instructionId BIGINT NULL,
            activatedAt DATETIME NULL,
            updatedBy VARCHAR(120),
            updatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_standby_callsign (callSign),
            INDEX idx_standby_updated (updatedAt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    # Backfill for older standby_locations schemas created by legacy installers.
    try:
        cur.execute(
            "ALTER TABLE standby_locations ADD COLUMN updatedBy VARCHAR(120)")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE standby_locations ADD COLUMN source VARCHAR(32) NOT NULL DEFAULT 'manual'")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE standby_locations ADD COLUMN destinationType VARCHAR(32) NOT NULL DEFAULT 'standby'")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE standby_locations ADD COLUMN activationMode VARCHAR(32) NOT NULL DEFAULT 'immediate'")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE standby_locations ADD COLUMN state VARCHAR(32) NOT NULL DEFAULT 'active'")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE standby_locations ADD COLUMN cad INT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE standby_locations ADD COLUMN what3words VARCHAR(80) NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE standby_locations ADD COLUMN address VARCHAR(255) NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE standby_locations ADD COLUMN instructionId BIGINT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE standby_locations ADD COLUMN activatedAt DATETIME NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "CREATE UNIQUE INDEX uq_standby_callsign ON standby_locations (callSign)")
    except Exception:
        pass
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_standby_presets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(180) NOT NULL,
            lat DECIMAL(10,7) NOT NULL,
            lng DECIMAL(10,7) NOT NULL,
            what3words VARCHAR(80) NULL,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            created_by VARCHAR(120),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_standby_preset_name (name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    try:
        cur.execute(
            "ALTER TABLE mdt_standby_presets ADD COLUMN what3words VARCHAR(80) NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdt_standby_presets ADD COLUMN is_active TINYINT(1) NOT NULL DEFAULT 1")
    except Exception:
        pass
    _upgrade_mdt_standby_presets_division(cur)
    # Seed default presets only when the table is empty. Running this on every request used to
    # re-insert HQ / North / South after DELETE (no duplicate name → fresh INSERT), which made
    # admin deletes appear to "snap back". Install/upgrade may still upsert the same names once.
    try:
        cur.execute("SELECT COUNT(*) AS c FROM mdt_standby_presets")
        crow = cur.fetchone()
        if isinstance(crow, dict):
            preset_count = int(crow.get("c") or 0)
        else:
            preset_count = int((crow or (0,))[0] or 0)
    except Exception:
        preset_count = -1
    if preset_count == 0:
        for name, lat, lng in [
            ('HQ', 51.5074000, -0.1278000),
            ('North Standby', 51.5400000, -0.1100000),
            ('South Standby', 51.4700000, -0.1200000),
        ]:
            try:
                cur.execute(
                    """
                    INSERT INTO mdt_standby_presets (name, lat, lng, what3words, is_active, created_by)
                    VALUES (%s, %s, %s, NULL, 1, 'system')
                    """,
                    (name, lat, lng),
                )
            except Exception:
                pass
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_dispatch_instructions (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            callSign VARCHAR(64) NOT NULL,
            instruction_type VARCHAR(64) NOT NULL,
            payload JSON NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            created_by VARCHAR(120) NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            acked_at DATETIME NULL,
            INDEX idx_instr_callsign_status (callSign, status, created_at),
            INDEX idx_instr_type (instruction_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def _supersede_stale_destination_instructions(cur, callsign, keep_instruction_id):
    """Retire older queued standby/transport rows so MDT /instructions poll surfaces the latest only."""
    try:
        kid = int(keep_instruction_id)
    except (TypeError, ValueError):
        return
    if kid <= 0:
        return
    cs = str(callsign or '').strip().upper()
    if not cs:
        return
    cur.execute(
        """
        UPDATE mdt_dispatch_instructions
        SET status = 'superseded',
            acked_at = COALESCE(acked_at, NOW())
        WHERE callSign = %s
          AND id <> %s
          AND instruction_type IN ('standby_location', 'transport_destination')
          AND status IN ('pending', 'sent')
        """,
        (cs, kid),
    )


def _ensure_meal_break_columns(cur):
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN mealBreakStartedAt DATETIME NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN mealBreakUntil DATETIME NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN mealBreakTakenAt DATETIME NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN shiftStartAt DATETIME NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN shiftEndAt DATETIME NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN shiftDurationMins INT NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdts_signed_on ADD COLUMN breakDueAfterMins INT NULL DEFAULT 240")
    except Exception:
        pass


def _get_dispatch_default_division(cur):
    default_division = 'general'
    try:
        _ensure_dispatch_settings_table(cur)
        cur.execute(
            "SELECT default_division FROM mdt_dispatch_settings WHERE id = 1 LIMIT 1")
        row = cur.fetchone()
        if isinstance(row, dict):
            default_division = _normalize_division(
                row.get('default_division'), fallback='general')
        elif row:
            default_division = _normalize_division(row[0], fallback='general')
    except Exception:
        pass
    return default_division or 'general'


def _set_dispatch_default_division(cur, slug, updated_by='system'):
    _ensure_dispatch_settings_table(cur)
    cur.execute("""
        INSERT INTO mdt_dispatch_settings (id, mode, default_division, updated_by)
        VALUES (1, 'auto', %s, %s)
        ON DUPLICATE KEY UPDATE
            default_division = VALUES(default_division),
            updated_by = VALUES(updated_by),
        updated_at = CURRENT_TIMESTAMP
    """, (_normalize_division(slug, fallback='general'), str(updated_by or 'system')))


def _list_dispatch_divisions(cur, include_inactive=False, *, filter_event_window: bool = True):
    """
    Dispatch division catalog for MDT login, Cura context, and dispatcher UIs.

    Rows linked to ``cura_operational_events`` are only listed when ``filter_event_window`` is True,
    the parent event is not terminal (closed/archived/etc.), ``now`` is inside any configured
    schedule window (partial bounds allowed; drafts need at least one bound), and the row is active.
    Admin manage screens pass ``filter_event_window=False`` to see the full catalog.
    """
    _ensure_dispatch_divisions_table(cur)
    _upgrade_mdt_dispatch_divisions_event_scope(cur)
    _upgrade_mdt_dispatch_divisions_map_icon(cur)
    has_scope = _dispatch_division_has_event_scope_columns(cur)
    join_cura = has_scope and _cura_operational_events_table_exists(cur)
    now_n = datetime.utcnow()

    if has_scope:
        sel = """
            SELECT d.slug, d.name, d.color, d.is_active, d.is_default, d.map_icon_url,
                   d.cura_operational_event_id, d.event_window_start, d.event_window_end,
                   e.status AS op_status, e.starts_at AS op_starts, e.ends_at AS op_ends
            FROM mdt_dispatch_divisions d
        """
        if join_cura:
            sel += " LEFT JOIN cura_operational_events e ON e.id = d.cura_operational_event_id"
        else:
            sel = """
                SELECT d.slug, d.name, d.color, d.is_active, d.is_default, d.map_icon_url,
                       d.cura_operational_event_id, d.event_window_start, d.event_window_end,
                       NULL AS op_status, NULL AS op_starts, NULL AS op_ends
                FROM mdt_dispatch_divisions d
            """
        wheres = []
        if not include_inactive:
            wheres.append("d.is_active = 1")
        if wheres:
            sel += " WHERE " + " AND ".join(wheres)
        sel += " ORDER BY d.is_default DESC, d.name ASC, d.slug ASC"
        cur.execute(sel)
    else:
        sql = """
            SELECT slug, name, color, is_active, is_default, map_icon_url
            FROM mdt_dispatch_divisions
        """
        if not include_inactive:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_default DESC, name ASC, slug ASC"
        cur.execute(sql)

    rows = cur.fetchall() or []
    out = []
    seen = set()
    for row in rows:
        row = row or {}
        if has_scope and filter_event_window:
            if not _catalog_visible_cura_event_division(row, now_n, filter_event_window=True):
                continue
        slug = _normalize_division(row.get("slug"), fallback="")
        if not slug or slug in seen:
            continue
        seen.add(slug)
        mu = row.get("map_icon_url")
        if mu is not None:
            mu = str(mu).strip() or None
        else:
            mu = None
        if mu:
            mu = _sanitize_client_division_map_icon_url(mu) or None
        item = {
            "slug": slug,
            "name": str(row.get("name") or slug).strip() or slug,
            "color": _normalize_hex_color(row.get("color"), "#64748b"),
            "is_active": bool(row.get("is_active", 1)),
            "is_default": bool(row.get("is_default", 0)),
            "map_icon_url": mu,
        }
        if has_scope:
            ceid = row.get("cura_operational_event_id")
            if ceid is not None:
                try:
                    ceid_i = int(ceid)
                    item["cura_operational_event_id"] = ceid_i
                    item["operational_event_id"] = ceid_i
                    item["is_event_division"] = True
                except (TypeError, ValueError):
                    item["is_event_division"] = False
            else:
                item["is_event_division"] = False
        out.append(item)
    if "general" not in seen:
        out.insert(
            0,
            {
                "slug": "general",
                "name": "General",
                "color": "#64748b",
                "is_active": True,
                "is_default": not any(d.get("is_default") for d in out),
                "is_event_division": False,
                "map_icon_url": None,
            },
        )
    return out


def _delete_dispatch_division_row(cur, slug_del, updated_by='system'):
    """Remove a division from the catalog and point dependent rows at general. slug_del must be normalized."""
    slug_del = _normalize_division(slug_del, fallback='')
    if not slug_del or slug_del == 'general':
        raise ValueError('Cannot delete the general division')
    _ensure_dispatch_divisions_table(cur)
    _upgrade_mdt_dispatch_divisions_map_icon(cur)
    # Resolve the row by normalized comparison so we delete the real PK even if casing/spacing differs in DB.
    cur.execute(
        """
        SELECT slug FROM mdt_dispatch_divisions
        WHERE LOWER(TRIM(slug)) = LOWER(TRIM(%s))
        LIMIT 1
        """,
        (slug_del,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError('Division not found')
    canon_slug = row.get('slug') if isinstance(row, dict) else row[0]
    canon_slug = str(canon_slug or '').strip()
    slug_norm = _normalize_division(canon_slug, fallback='')
    if not slug_norm or slug_norm == 'general':
        raise ValueError('Cannot delete the general division')
    old_icon = None
    try:
        cur.execute(
            "SELECT map_icon_url FROM mdt_dispatch_divisions WHERE slug = %s LIMIT 1",
            (canon_slug,),
        )
        _ir = cur.fetchone()
        if _ir:
            old_icon = (_ir.get("map_icon_url") if isinstance(_ir, dict) else _ir[0])
            old_icon = str(old_icon).strip() if old_icon else None
    except Exception:
        old_icon = None
    if _get_dispatch_default_division(cur) == slug_norm:
        _set_dispatch_default_division(
            cur, 'general', updated_by=str(updated_by or 'system'))
    _ensure_dispatch_user_access_tables(cur)
    cur.execute(
        "DELETE FROM mdt_dispatch_user_divisions WHERE LOWER(TRIM(division)) = %s",
        (slug_norm,),
    )
    try:
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        if cur.fetchone():
            cur.execute(
                "UPDATE mdt_jobs SET division = 'general' WHERE LOWER(TRIM(COALESCE(division, ''))) = %s",
                (slug_norm,),
            )
    except Exception:
        pass
    try:
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        if cur.fetchone():
            cur.execute(
                "UPDATE mdts_signed_on SET division = 'general' WHERE LOWER(TRIM(COALESCE(division, ''))) = %s",
                (slug_norm,),
            )
    except Exception:
        pass
    try:
        cur.execute("SHOW TABLES LIKE 'mdt_triage_forms'")
        if cur.fetchone():
            cur.execute(
                """
                UPDATE mdt_triage_forms
                SET schema_json = JSON_SET(schema_json, '$.dispatch_division', JSON_QUOTE('general'))
                WHERE LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(schema_json, '$.dispatch_division')), ''))) = %s
                """,
                (slug_norm,)
            )
    except Exception:
        pass
    cur.execute(
        "DELETE FROM mdt_dispatch_divisions WHERE slug = %s", (canon_slug,))
    if old_icon:
        _try_remove_division_map_icon_file(old_icon)


def _rename_dispatch_division_slug(cur, old_slug, new_slug, name, color, is_active, is_default, updated_by='system'):
    """Rename division primary slug and update dependent string references. old_slug/new_slug normalized."""
    old_slug = _normalize_division(old_slug, fallback='')
    new_slug = _normalize_division(new_slug, fallback='')
    if not old_slug or not new_slug or old_slug == new_slug:
        return
    if old_slug == 'general':
        raise ValueError('Cannot rename the general division slug')
    if new_slug == 'general':
        raise ValueError('Slug "general" is reserved')
    _ensure_dispatch_divisions_table(cur)
    cur.execute(
        "SELECT slug FROM mdt_dispatch_divisions WHERE slug = %s LIMIT 1", (old_slug,))
    if not cur.fetchone():
        raise ValueError('Division not found')
    cur.execute(
        "SELECT slug FROM mdt_dispatch_divisions WHERE slug = %s LIMIT 1", (new_slug,))
    if cur.fetchone():
        raise ValueError('A division with that slug already exists')
    _ensure_dispatch_user_access_tables(cur)
    cur.execute(
        "UPDATE mdt_dispatch_user_divisions SET division = %s WHERE LOWER(TRIM(division)) = %s",
        (new_slug, old_slug)
    )
    try:
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        if cur.fetchone():
            cur.execute(
                "UPDATE mdt_jobs SET division = %s WHERE LOWER(TRIM(COALESCE(division, ''))) = %s",
                (new_slug, old_slug)
            )
    except Exception:
        pass
    try:
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        if cur.fetchone():
            cur.execute(
                "UPDATE mdts_signed_on SET division = %s WHERE LOWER(TRIM(COALESCE(division, ''))) = %s",
                (new_slug, old_slug)
            )
    except Exception:
        pass
    try:
        _ensure_assist_requests_table(cur)
        cur.execute(
            "UPDATE mdt_dispatch_assist_requests SET from_division = %s WHERE LOWER(TRIM(from_division)) = %s",
            (new_slug, old_slug)
        )
        cur.execute(
            "UPDATE mdt_dispatch_assist_requests SET to_division = %s WHERE LOWER(TRIM(to_division)) = %s",
            (new_slug, old_slug)
        )
    except Exception:
        pass
    try:
        cur.execute("SHOW TABLES LIKE 'mdt_triage_forms'")
        if cur.fetchone():
            cur.execute(
                """
                UPDATE mdt_triage_forms
                SET schema_json = JSON_SET(schema_json, '$.dispatch_division', JSON_QUOTE(%s))
                WHERE LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(schema_json, '$.dispatch_division')), ''))) = %s
                """,
                (new_slug, old_slug)
            )
    except Exception:
        pass
    if _get_dispatch_default_division(cur) == old_slug:
        _set_dispatch_default_division(
            cur, new_slug, updated_by=str(updated_by or 'system'))
    cur.execute(
        """
        UPDATE mdt_dispatch_divisions
        SET slug = %s, name = %s, color = %s, is_active = %s, is_default = %s, updated_at = CURRENT_TIMESTAMP
        WHERE slug = %s
        """,
        (new_slug, name, color, is_active, is_default, old_slug)
    )
    if not cur.rowcount:
        raise ValueError('Division rename failed')


def _get_dispatch_user_division_access(cur, username=None):
    uname = str(username or getattr(
        current_user, 'username', '') or '').strip()
    uname_key = uname.lower()
    role = str(getattr(current_user, 'role', '') or '').strip().lower()
    privileged_roles = {'admin', 'superuser',
                        'clinical_lead', 'support_break_glass'}
    if role in privileged_roles:
        return {'username': uname, 'restricted': False, 'divisions': [], 'can_override_all': True}
    if not uname:
        return {'username': uname, 'restricted': False, 'divisions': [], 'can_override_all': False}

    try:
        _ensure_dispatch_user_access_tables(cur)
    except Exception:
        return {'username': uname, 'restricted': False, 'divisions': [], 'can_override_all': False}

    can_override = False
    try:
        cur.execute(
            "SELECT can_override_all FROM mdt_dispatch_user_settings WHERE LOWER(username) = %s LIMIT 1", (uname_key,))
        row = cur.fetchone()
        if isinstance(row, dict):
            can_override = bool(row.get('can_override_all'))
        elif row:
            can_override = bool(row[0])
    except Exception:
        can_override = False

    divisions = []
    try:
        cur.execute("""
            SELECT division
            FROM mdt_dispatch_user_divisions
            WHERE LOWER(username) = %s
            ORDER BY division ASC
        """, (uname_key,))
        rows = cur.fetchall() or []
        if rows and isinstance(rows[0], dict):
            divisions = [_normalize_division(
                r.get('division'), fallback='') for r in rows]
        else:
            divisions = [_normalize_division(
                (r[0] if r else ''), fallback='') for r in rows]
        divisions = [d for d in divisions if d]
    except Exception:
        divisions = []

    restricted = len(divisions) > 0
    return {
        'username': uname,
        'restricted': restricted,
        'divisions': divisions,
        'can_override_all': can_override
    }


def _enforce_dispatch_scope(cur, selected_division, include_external):
    access = _get_dispatch_user_division_access(cur)
    raw = str(selected_division or '').strip().lower()
    if raw in ('all', '__all__', '*'):
        selected = _CAD_DIVISION_ALL
        include_ext = True
        selected, include_ext = _apply_cura_event_job_filter_policy(
            cur, selected, include_ext, access)
        return selected, include_ext, access
    selected = _normalize_division(selected_division, fallback='')
    include_ext = bool(include_external)
    if not access.get('restricted'):
        selected, include_ext = _apply_cura_event_job_filter_policy(
            cur, selected, include_ext, access)
        return selected, include_ext, access
    allowed = [d for d in (access.get('divisions') or []) if d]
    if not allowed:
        return 'general', False, access
    if selected not in allowed:
        selected = allowed[0]
    include_ext = bool(include_ext and access.get('can_override_all'))
    selected, include_ext = _apply_cura_event_job_filter_policy(
        cur, selected, include_ext, access)
    return selected, include_ext, access


# Instantiate PluginManager and load core manifest.
plugin_manager = PluginManager(os.path.abspath('app/plugins'))
core_manifest = plugin_manager.get_core_manifest()

def _ventus_tenant_industry_slugs():
    try:
        return normalize_organization_industries(
            current_app.config.get("organization_industries"))
    except Exception:
        return normalize_organization_industries(None)


def ventus_triage_slug_visible_for_tenant(slug):
    """Used by triage loaders; outside Flask request, all slugs visible (install/CLI)."""
    s = str(slug or "").strip().lower()
    req = VENTUS_TRIAGE_SLUG_INDUSTRIES.get(s)
    if req is None:
        return True
    if not has_request_context():
        return True
    ids = _ventus_tenant_industry_slugs()
    return tenant_matches_industry(ids, *req)


def _filter_triage_forms_by_industry(forms):
    if not forms:
        return forms
    return [f for f in forms if f and ventus_triage_slug_visible_for_tenant(
        f.get("slug"))]


_NEUTRAL_FALLBACK_TRIAGE_FORMS = [
    {
        "slug": "general_dispatch",
        "name": "General Dispatch",
        "description": "Site or service task — reference, location, and priority.",
        "is_default": True,
        "show_exclusions": False,
        "questions": [
            {"key": "reference", "label": "Reference / ticket ID",
                "type": "text", "required": True},
            {"key": "site_location", "label": "Site / location",
                "type": "text", "required": True},
            {"key": "priority", "label": "Priority", "type": "select",
                "options": ["low", "normal", "high", "urgent"], "required": True},
            {"key": "brief", "label": "Brief description", "type": "textarea"},
        ],
    },
]


DEFAULT_TRIAGE_FORMS = [
    {
        "slug": "general_dispatch",
        "name": "General Dispatch",
        "description": "Site or service task — reference, location, and priority.",
        "is_default": False,
        "show_exclusions": False,
        "questions": [
            {"key": "reference", "label": "Reference / ticket ID",
                "type": "text", "required": True},
            {"key": "site_location", "label": "Site / location",
                "type": "text", "required": True},
            {"key": "priority", "label": "Priority", "type": "select",
                "options": ["low", "normal", "high", "urgent"], "required": True},
            {"key": "brief", "label": "Brief description", "type": "textarea"},
        ],
    },
    {
        "slug": "urgent_care",
        "name": "Private Urgent Care",
        "description": "Primary urgent care pathway with exclusion screening.",
        "is_default": True,
        "show_exclusions": True,
        "questions": [
            {"key": "is_stable", "label": "Patient clinically stable?",
                "type": "select", "options": ["unknown", "yes", "no"], "required": True},
            {"key": "primary_symptom", "label": "Primary symptom",
                "type": "text", "required": True},
            {"key": "pain_score",
                "label": "Pain score (0-10)", "type": "number", "min": 0, "max": 10},
            {"key": "red_flags", "label": "Observed red flags", "type": "textarea"}
        ]
    },
    {
        "slug": "emergency_999",
        "name": "999 Emergency",
        "description": "Emergency dispatch workflow with critical incident prompts.",
        "is_default": False,
        "show_exclusions": False,
        "questions": [
            {"key": "conscious", "label": "Conscious?", "type": "select",
                "options": ["unknown", "yes", "no"], "required": True},
            {"key": "breathing", "label": "Breathing normally?", "type": "select",
                "options": ["unknown", "yes", "no"], "required": True},
            {"key": "major_bleeding", "label": "Major bleeding?", "type": "select",
                "options": ["unknown", "yes", "no"], "required": True},
            {"key": "immediate_danger",
                "label": "Immediate scene danger", "type": "textarea"}
        ]
    },
    {
        "slug": "event_medical",
        "name": "Event Medical",
        "description": "Event-specific intake with location and welfare context.",
        "is_default": False,
        "show_exclusions": False,
        "questions": [
            {"key": "event_name", "label": "Event name",
                "type": "text", "required": True},
            {"key": "event_zone", "label": "Event zone / stand", "type": "text"},
            {"key": "security_required", "label": "Security required?",
                "type": "select", "options": ["unknown", "yes", "no"]},
            {"key": "crowd_density", "label": "Crowd density",
                "type": "select", "options": ["low", "medium", "high", "unknown"]}
        ]
    }
]


def _default_triage_forms():
    # Deep-copy via JSON to avoid accidental mutation of global defaults.
    return json.loads(json.dumps(DEFAULT_TRIAGE_FORMS))


# Exact legacy display strings from older seeds — only replaced when unchanged
# so customised profile names are preserved.
_LEGACY_TRIAGE_PROFILE_NAME_CAPS = {
    ("general_dispatch", "General dispatch"): "General Dispatch",
    ("facilities_cleaning", "Facilities cleaning"): "Facilities Cleaning",
    ("venue_guest_incident", "Venue / guest incident"): "Venue / Guest Incident",
}


def _normalize_triage_form(raw):
    slug = str(raw.get("slug") or "").strip().lower().replace(" ", "_")
    if not slug:
        return None
    name = str(raw.get("name") or slug.replace("_", " ").title()).strip()
    cap_key = (slug, name)
    if cap_key in _LEGACY_TRIAGE_PROFILE_NAME_CAPS:
        name = _LEGACY_TRIAGE_PROFILE_NAME_CAPS[cap_key]
    description = str(raw.get("description") or "").strip()
    dispatch_division = _normalize_division(
        raw.get("dispatch_division") or raw.get("division"), fallback='general')
    show_exclusions = bool(raw.get("show_exclusions", False))
    priority_config = _normalize_priority_config(raw.get("priority_config"))
    questions = raw.get("questions") if isinstance(
        raw.get("questions"), list) else []
    normalized_questions = []
    for q in questions:
        if not isinstance(q, dict):
            continue
        key = str(q.get("key") or "").strip().lower().replace(" ", "_")
        if not key:
            continue
        q_type = str(q.get("type") or "text").strip().lower()
        if q_type not in ("text", "textarea", "number", "select"):
            q_type = "text"
        options = []
        if q_type == "select":
            options = [str(x)
                       for x in (q.get("options") or []) if str(x).strip()]
            if not options:
                options = ["unknown", "yes", "no"]
        normalized_questions.append({
            "key": key,
            "label": str(q.get("label") or key.replace("_", " ").title()),
            "type": q_type,
            "required": bool(q.get("required", False)),
            "options": options,
            "min": q.get("min"),
            "max": q.get("max")
        })
    return {
        "slug": slug,
        "name": name,
        "description": description,
        "dispatch_division": dispatch_division,
        "is_default": bool(raw.get("is_default", False)),
        "show_exclusions": show_exclusions,
        "questions": normalized_questions,
        "priority_config": priority_config
    }


def _load_triage_forms(cur=None):
    forms = []
    if cur is not None:
        try:
            cur.execute("SHOW TABLES LIKE 'mdt_triage_forms'")
            if cur.fetchone() is not None:
                cur.execute("""
                    SELECT slug, name, description, schema_json, is_default
                    FROM mdt_triage_forms
                    WHERE is_active = 1
                    ORDER BY is_default DESC, name ASC
                """)
                rows = cur.fetchall() or []
                for row in rows:
                    schema = row.get("schema_json")
                    if isinstance(schema, (bytes, bytearray)):
                        schema = schema.decode("utf-8", errors="ignore")
                    if isinstance(schema, str):
                        try:
                            schema = json.loads(schema)
                        except Exception:
                            schema = {}
                    schema = schema if isinstance(schema, dict) else {}
                    schema["slug"] = row.get("slug")
                    schema["name"] = row.get("name")
                    schema["description"] = row.get("description")
                    schema["is_default"] = bool(row.get("is_default", False))
                    normalized = _normalize_triage_form(schema)
                    if normalized:
                        forms.append(normalized)
        except Exception:
            forms = []
    if not forms:
        forms = [_normalize_triage_form(f) for f in _default_triage_forms()]
        forms = [f for f in forms if f]
    forms = _filter_triage_forms_by_industry(forms)
    if not forms:
        forms = [_normalize_triage_form(f) for f in _NEUTRAL_FALLBACK_TRIAGE_FORMS]
        forms = [f for f in forms if f]
    return forms


def _pick_triage_form(forms, slug):
    wanted = str(slug or "").strip().lower()
    if not forms:
        forms = _load_triage_forms(None)
    for form in forms:
        if form and form["slug"] == wanted:
            return form
    return forms[0] if forms else _normalize_triage_form(
        _NEUTRAL_FALLBACK_TRIAGE_FORMS[0])


# Response Intake launch visibility (Feedback V5 — private ambulance / security focus)
INTAKE_SLUGS_ACTIVE = frozenset({
    "urgent_care", "event_medical", "patient_transport", "security_response",
    "general_dispatch", "facilities_cleaning", "venue_guest_incident",
})
INTAKE_SLUGS_HIDDEN = frozenset({
    "training_simulation", "multi_agency_coordination",
})
INTAKE_SLUGS_COMING_SOON = frozenset({
    "emergency_999", "fire_support", "mental_health_support", "private_police",
    "search_and_rescue", "vehicle_recovery", "welfare_check",
})


def _intake_form_category(slug):
    """Return 'active', 'hidden', or 'coming_soon' for intake dashboard gating."""
    s = str(slug or "").strip().lower()
    if s in INTAKE_SLUGS_HIDDEN:
        return "hidden"
    if s in INTAKE_SLUGS_COMING_SOON:
        return "coming_soon"
    return "active"


def _triage_forms_intake_visible(forms):
    """Same profile set as Response Intake tiles and the triage form profile switcher."""
    return [
        f for f in (forms or [])
        if f and _intake_form_category(f.get("slug")) == "active"
    ]


def _default_priority_config():
    return {
        "levels": [
            {"code": "P1", "label": "Grade 1 - Immediate"},
            {"code": "P2", "label": "Grade 2 - Urgent"},
            {"code": "P3", "label": "Grade 3 - Soon"},
            {"code": "P4", "label": "Grade 4 - Routine"},
        ],
        "fallback": "P4",
        "rules": []
    }


def _normalize_priority_level_code(value):
    code = str(value or "").strip().upper().replace(" ", "_")
    code = "".join(ch for ch in code if ch.isalnum() or ch == "_")
    return code[:24] if code else ""


def _normalize_priority_config(raw):
    cfg = raw if isinstance(raw, dict) else {}
    levels_in = cfg.get("levels") if isinstance(
        cfg.get("levels"), list) else []
    levels = []
    for lvl in levels_in:
        if not isinstance(lvl, dict):
            continue
        code = _normalize_priority_level_code(
            lvl.get("code") or lvl.get("value"))
        label = str(lvl.get("label") or code).strip()
        if not code:
            continue
        if any(x["code"] == code for x in levels):
            continue
        levels.append({"code": code, "label": label or code})
    if not levels:
        levels = _default_priority_config()["levels"]

    valid_codes = {x["code"] for x in levels}
    fallback = _normalize_priority_level_code(cfg.get("fallback"))
    if fallback not in valid_codes:
        fallback = levels[-1]["code"]

    rules = []
    raw_rules = cfg.get("rules") if isinstance(cfg.get("rules"), list) else []
    for r in raw_rules:
        if not isinstance(r, dict):
            continue
        field = str(r.get("field") or "").strip()
        op = str(r.get("op") or "equals").strip().lower()
        value = r.get("value")
        target = _normalize_priority_level_code(
            r.get("target") or r.get("then"))
        if not field or target not in valid_codes:
            continue
        if op not in ("equals", "not_equals", "contains", "contains_any", "in", "gte", "gt", "lte", "lt", "is_true", "is_false"):
            op = "equals"
        rules.append({
            "field": field,
            "op": op,
            "value": value,
            "target": target
        })
    return {
        "levels": levels,
        "fallback": fallback,
        "rules": rules
    }


def _priority_levels_for_form(selected_form):
    cfg = _normalize_priority_config(
        (selected_form or {}).get("priority_config"))
    return cfg.get("levels") or _default_priority_config()["levels"]


def _priority_label_for_form(code, selected_form):
    normalized = _normalize_priority_for_form(code, selected_form)
    levels = _priority_levels_for_form(selected_form)
    for lvl in levels:
        if lvl.get("code") == normalized:
            return str(lvl.get("label") or normalized)
    return normalized


def _normalize_priority_for_form(value, selected_form):
    raw = str(value or "").strip()
    if not raw:
        return None
    needle = _normalize_priority_level_code(raw)
    levels = _priority_levels_for_form(selected_form)
    for lvl in levels:
        code = _normalize_priority_level_code(lvl.get("code"))
        label = str(lvl.get("label") or "").strip().lower()
        if needle == code or raw.lower() == label:
            return code
    return None


def _legacy_normalize_priority(value):
    raw = str(value or "").strip().lower().replace(" ", "_")
    mapping = {
        "p1": "P1",
        "grade_1": "P1",
        "grade1": "P1",
        "g1": "P1",
        "critical": "P1",
        "immediate": "P1",
        "p2": "P2",
        "grade_2": "P2",
        "grade2": "P2",
        "g2": "P2",
        "urgent": "P2",
        "high": "P2",
        "p3": "P3",
        "grade_3": "P3",
        "grade3": "P3",
        "g3": "P3",
        "routine": "P3",
        "normal": "P3",
        "low": "P3",
        "p4": "P4",
        "grade_4": "P4",
        "grade4": "P4",
        "g4": "P4",
        "non_urgent": "P4",
        "nonurgent": "P4",
    }
    return mapping.get(raw)


def _normalize_patient_alone(value):
    v = str(value or "").strip().lower()
    if v in ("yes", "y", "true", "1"):
        return 1
    if v in ("no", "n", "false", "0"):
        return 0
    return None


def _parse_intake_coordinate_field(raw):
    """Parse a single latitude or longitude from intake (decimal comma allowed)."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except (TypeError, ValueError):
        return None


_DEFAULT_DISPATCHER_BACKUP_QUICK_LOG_LABELS = {
    "backup_p1": "Grade 1",
    "backup_p2": "Grade 2",
    "backup_p3": "Grade 3",
}
_JOB_COMMS_BACKUP_QUICK_LOG_KEYS = frozenset(
    _DEFAULT_DISPATCHER_BACKUP_QUICK_LOG_LABELS.keys())


def _dispatch_backup_quick_log_effective_and_stored(cur):
    """Effective labels for CAD + stored strings for admin form (empty = use default)."""
    effective = dict(_DEFAULT_DISPATCHER_BACKUP_QUICK_LOG_LABELS)
    stored_display = {k: "" for k in effective}
    try:
        _ensure_dispatch_settings_table(cur)
        cur.execute(
            """
            SELECT dispatcher_backup_label_p1, dispatcher_backup_label_p2, dispatcher_backup_label_p3
            FROM mdt_dispatch_settings
            WHERE id = 1
            LIMIT 1
            """
        )
        row = cur.fetchone() or {}
        mapping = (
            ("backup_p1", "dispatcher_backup_label_p1"),
            ("backup_p2", "dispatcher_backup_label_p2"),
            ("backup_p3", "dispatcher_backup_label_p3"),
        )
        for key, col in mapping:
            v = row.get(col) if isinstance(row, dict) else None
            s = v.strip()[:120] if isinstance(v, str) and v.strip() else ""
            stored_display[key] = s
            if s:
                effective[key] = s
    except Exception:
        pass
    return effective, stored_display


def _get_dispatcher_backup_quick_log_labels(cur):
    """Tier display names for dispatcher quick-log backup buttons (Ventus Admin configurable)."""
    eff, _ = _dispatch_backup_quick_log_effective_and_stored(cur)
    return eff


def _dispatcher_backup_tier1_display_name(cur) -> str:
    """Tier-1 wording for MDT panic / CAD (matches backup_p1 quick-log label)."""
    return _get_dispatcher_backup_quick_log_labels(cur).get(
        "backup_p1"
    ) or _DEFAULT_DISPATCHER_BACKUP_QUICK_LOG_LABELS["backup_p1"]


def _job_comms_backup_quick_log_message(cur, msg_type: str) -> str:
    labels = _get_dispatcher_backup_quick_log_labels(cur)
    tier = labels.get(msg_type) or _DEFAULT_DISPATCHER_BACKUP_QUICK_LOG_LABELS.get(
        msg_type, "Backup"
    )
    return f"Crew requested {tier} backup (dispatcher quick log)"


def _normalize_patient_gender(value):
    v = str(value or "").strip().lower()
    if not v:
        return ""
    mapping = {
        "male": "Male",
        "m": "Male",
        "female": "Female",
        "f": "Female",
        "other": "Other",
        "non-binary": "Other",
        "non_binary": "Other",
        "nonbinary": "Other",
        "unknown": "Unknown",
        "not_known": "Unknown",
        "not_known_yet": "Unknown",
        "declined": "Unknown",
    }
    if v in mapping:
        return mapping[v]
    return v[:1].upper() + v[1:]


def _role_key(value):
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _current_role_key():
    return _role_key(getattr(current_user, "role", ""))


def _user_has_role(*roles):
    if not getattr(current_user, "is_authenticated", False):
        return False
    current = _current_role_key()
    allowed = {_role_key(r) for r in roles if str(r or "").strip()}
    return current in allowed


def _ventus_dispatch_capable():
    """
    CAD board, assign/unassign units, force status — requires permission or legacy dispatcher-type role.
    """
    try:
        if has_permission("ventus_response_module.dispatch"):
            return True
    except Exception:
        pass
    return _user_has_role(
        "dispatcher",
        "admin",
        "superuser",
        "clinical_lead",
        "controller",
    )


def _can_access_call_centre():
    try:
        if has_permission("ventus_response_module.call_taker"):
            return True
    except Exception:
        pass
    return _user_has_role(
        "crew",
        "dispatcher",
        "admin",
        "superuser",
        "clinical_lead",
        "call_taker",
        "calltaker",
        "controller",
        "call_handler",
        "callhandler",
    )


def _compute_system_priority_legacy(reason_for_call, selected_form, decision, exclusion_data, form_answers):
    score = 0
    reason = str(reason_for_call or "").lower()
    slug = str((selected_form or {}).get("slug") or "").lower()
    answers = form_answers if isinstance(form_answers, dict) else {}
    exclusions = exclusion_data if isinstance(exclusion_data, dict) else {}

    critical_terms = [
        "unconscious", "cardiac", "not breathing", "arrest", "seizure",
        "stroke", "major bleed", "severe bleeding", "anaphylaxis"
    ]
    urgent_terms = [
        "chest pain", "collapse", "shortness of breath", "breathing",
        "severe pain", "head injury", "trauma", "violent", "weapon"
    ]
    if any(t in reason for t in critical_terms):
        score += 6
    if any(t in reason for t in urgent_terms):
        score += 3

    if slug in ("emergency_999",):
        score += 3
    if slug in ("security_response", "private_police"):
        score += 2

    if str(decision or "").upper() == "ACCEPT_WITH_EXCLUSION":
        score += 3
    if any(str(v or "").strip().lower() == "yes" for v in exclusions.values()):
        score += 3

    if str(answers.get("conscious") or "").lower() == "no":
        score += 5
    if str(answers.get("breathing") or "").lower() == "no":
        score += 5
    if str(answers.get("major_bleeding") or "").lower() == "yes":
        score += 5
    if str(answers.get("immediate_risk") or "").lower() == "yes":
        score += 3
    if str(answers.get("threat_level") or "").lower() in ("high", "critical"):
        score += 3
    if str(answers.get("agitation_level") or "").lower() in ("high", "critical"):
        score += 2
    if str(answers.get("casualties_reported") or "").lower() == "yes":
        score += 2

    if score >= 9:
        return "P1"
    if score >= 5:
        return "P2"
    if score >= 2:
        return "P3"
    return "P4"


def _evaluate_priority_rule_condition(field, op, value, reason_for_call, decision, exclusion_data, form_answers):
    field_key = str(field or "").strip()
    answers = form_answers if isinstance(form_answers, dict) else {}
    exclusions = exclusion_data if isinstance(exclusion_data, dict) else {}

    if field_key == "reason_for_call":
        actual = str(reason_for_call or "")
    elif field_key == "decision":
        actual = str(decision or "")
    elif field_key == "exclusion_any":
        actual = any(str(v or "").strip().lower() ==
                     "yes" for v in exclusions.values())
    elif field_key.startswith("question:"):
        actual = answers.get(field_key.split(":", 1)[1], "")
    else:
        actual = answers.get(field_key, "")

    op = str(op or "equals").strip().lower()
    if op == "is_true":
        return str(actual).strip().lower() in ("1", "true", "yes", "y")
    if op == "is_false":
        return str(actual).strip().lower() in ("0", "false", "no", "n", "")
    if op == "contains":
        return str(value or "").strip().lower() in str(actual or "").lower()
    if op == "contains_any":
        terms = [t.strip().lower()
                 for t in str(value or "").split(",") if t.strip()]
        actual_l = str(actual or "").lower()
        return any(t in actual_l for t in terms)
    if op == "in":
        allowed = [t.strip().lower()
                   for t in str(value or "").split(",") if t.strip()]
        return str(actual or "").strip().lower() in allowed
    if op in ("gte", "gt", "lte", "lt"):
        try:
            a = float(actual)
            b = float(value)
            if op == "gte":
                return a >= b
            if op == "gt":
                return a > b
            if op == "lte":
                return a <= b
            return a < b
        except Exception:
            return False
    if op == "not_equals":
        return str(actual or "").strip().lower() != str(value or "").strip().lower()
    return str(actual or "").strip().lower() == str(value or "").strip().lower()


def _compute_system_priority(reason_for_call, selected_form, decision, exclusion_data, form_answers):
    cfg = _normalize_priority_config(
        (selected_form or {}).get("priority_config"))
    rules = cfg.get("rules") or []
    valid_codes = {x["code"] for x in (cfg.get("levels") or [])}
    for r in rules:
        try:
            matched = _evaluate_priority_rule_condition(
                field=r.get("field"),
                op=r.get("op"),
                value=r.get("value"),
                reason_for_call=reason_for_call,
                decision=decision,
                exclusion_data=exclusion_data,
                form_answers=form_answers
            )
        except Exception:
            matched = False
        if matched:
            target = _normalize_priority_level_code(r.get("target"))
            if target in valid_codes:
                return target

    legacy_priority = _compute_system_priority_legacy(
        reason_for_call=reason_for_call,
        selected_form=selected_form,
        decision=decision,
        exclusion_data=exclusion_data,
        form_answers=form_answers
    )
    legacy_mapped = _normalize_priority_for_form(
        legacy_priority, selected_form)
    if legacy_mapped:
        return legacy_mapped
    return cfg.get("fallback") or "P4"


# =============================================================================
# INTERNAL BLUEPRINT (for admin side)
# =============================================================================
internal_template_folder = os.path.join(os.path.dirname(__file__), 'templates')
internal = Blueprint(
    'medical_response_internal',
    __name__,
    url_prefix='/plugin/ventus_response_module',
    template_folder=internal_template_folder
)

# Canonical same-origin paths for CAD map proxies (avoid fragile relative URLs from ``/cad``).
_CAD_VENTUS_PLUGIN_HTTP_PATH = '/plugin/ventus_response_module'
_CAD_METOFFICE_MAP_PREVIEW_HTTP_PATH = (
    f'{_CAD_VENTUS_PLUGIN_HTTP_PATH}/map/metoffice-preview.png')


def _mdt_api_jwt_or_session_auth_impl():
    """Require JWT (Bearer) or Flask session for /api/mdt/* and /api/messages/*; enforce device session binding."""
    path = (request.path or "").rstrip("/")
    pl = path.lower()
    if not _mdt_api_jwt_protected_path(pl):
        return None
    if request.method == "OPTIONS":
        return None
    # GET /api/mdt/divisions is used on login screen before sign-on; no auth required
    if request.method == "GET" and pl.endswith("api/mdt/divisions"):
        return None
    # Public VAPID key for Web Push subscription (no secret in response)
    if request.method == "GET" and "/api/mdt/push/vapid-public-key" in pl:
        return None
    g.mdt_user = None
    auth = request.headers.get("Authorization")
    if auth and auth.startswith("Bearer "):
        token = auth[7:].strip()
        if token and _jwt_decode:
            payload = _jwt_decode(token)
            if payload:
                g.mdt_user = {
                    "id": payload.get("sub"),
                    "username": payload.get("username"),
                    "role": payload.get("role") or "",
                }
    if g.mdt_user:
        if not _mdt_api_session_binding_exempt(pl, request.method):
            uname = str(g.mdt_user.get("username") or "").strip().lower()
            if uname:
                conn = None
                cur = None
                try:
                    conn = get_db_connection()
                    cur = conn.cursor(dictionary=True)
                    _ensure_mdt_user_mdt_session_table(cur)
                    cur.execute(
                        """
                        SELECT session_id FROM mdt_user_mdt_session
                        WHERE LOWER(TRIM(username)) = %s LIMIT 1
                        """,
                        (uname,),
                    )
                    row = cur.fetchone() or {}
                    sid = str(row.get("session_id") or "").strip()
                    hdr = (request.headers.get("X-MDT-Session-Id") or "").strip()
                    if sid:
                        if hdr != sid:
                            return jsonify({
                                "error": "mdt_session_invalid",
                                "message": "This MDT session ended — sign on again from the login screen.",
                            }), 403
                    elif hdr:
                        return jsonify({
                            "error": "mdt_session_invalid",
                            "message": "This MDT session ended — sign on again from the login screen.",
                        }), 403
                except Exception:
                    pass
                finally:
                    if cur is not None:
                        try:
                            cur.close()
                        except Exception:
                            pass
                    if conn is not None:
                        try:
                            conn.close()
                        except Exception:
                            pass
        return None
    if current_user.is_authenticated:
        return None
    return jsonify({
        "error": "Unauthorized",
        "message": "MDT API requires session login or Authorization: Bearer <token>. Use /api/login to obtain a JWT."
    }), 401


@internal.before_request
def _mdt_api_jwt_or_session_auth():
    return _mdt_api_jwt_or_session_auth_impl()


# Root-level compatibility blueprint for MDT clients that still call /api/*
# directly instead of /plugin/ventus_response_module/api/*.
api_compat = Blueprint(
    'ventus_response_api_compat',
    __name__,
    url_prefix='',
    template_folder=internal_template_folder
)


@api_compat.before_request
def _mdt_api_jwt_or_session_auth_api_root():
    return _mdt_api_jwt_or_session_auth_impl()


def _cad_user_can_edit_cura_dispatch_policy() -> bool:
    r = (getattr(current_user, "role", "") or "").strip().lower()
    return r in (
        "dispatcher",
        "admin",
        "superuser",
        "clinical_lead",
        "support_break_glass",
    )


@internal.route('/')
def landing():
    """Landing page (router) for Medical response Module."""
    try:
        cura_ops_hub_url = url_for("medical_records_internal.cura_ops_hub")
    except Exception:
        cura_ops_hub_url = "/plugin/medical_records_module/clinical/cura-ops"
    return render_template(
        "response_routing.html",
        config=core_manifest,
        cura_ops_hub_url=cura_ops_hub_url,
    )

# --- CAD/DISPATCHER INTERNAL API ROUTES ---


@internal.route('/jobs', methods=['GET'])
@login_required
def jobs():
    """Return all jobs/incidents (queued, active, etc.) for the job queue and map."""
    selected_division, include_external = _request_division_scope()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        selected_division, include_external, access = _enforce_dispatch_scope(
            cur, selected_division, include_external)
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
        has_updated_at = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'claimedBy'")
        has_claimed_by = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_division = cur.fetchone() is not None
        updated_at_sql = "updated_at" if has_updated_at else "NULL AS updated_at"
        claimed_by_sql = "claimedBy" if has_claimed_by else "NULL AS claimedBy"
        division_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_division else "'general' AS division"
        sql = """
            SELECT cad,
                   TRIM(COALESCE(status, '')) AS status,
                   data,
                   created_at,
                   {updated_at_sql},
                   {claimed_by_sql},
                   {division_sql}
            FROM mdt_jobs
            WHERE LOWER(TRIM(COALESCE(status, ''))) NOT IN ('cleared', 'stood_down')
        """.format(updated_at_sql=updated_at_sql, claimed_by_sql=claimed_by_sql, division_sql=division_sql)
        job_div_expr = "LOWER(TRIM(COALESCE(division, 'general')))"
        div_frag, args = _division_scope_where_sql(
            selected_division, include_external, access, has_division, job_div_expr
        )
        sql += div_frag
        sql += " ORDER BY cad DESC"
        cur.execute(sql, tuple(args))
        jobs = cur.fetchall() or []
        cur.execute("SHOW TABLES LIKE 'mdt_job_units'")
        has_mdt_job_units = cur.fetchone() is not None
        # Parse triage payload and convert coordinates if present
        for job in jobs:
            reason_for_call = None
            lat = None
            lng = None
            address = None
            postcode = None
            what3words = None
            priority = None
            payload = job.get('data')
            try:
                if isinstance(payload, (bytes, bytearray)):
                    payload = payload.decode('utf-8', errors='ignore')
                if isinstance(payload, str):
                    payload = json.loads(payload) if payload else {}
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}
            try:
                reason_for_call = payload.get('reason_for_call')
                address = payload.get('address')
                postcode = payload.get('postcode')
                what3words = payload.get('what3words')
                priority = payload.get('call_priority') or payload.get(
                    'priority') or payload.get('acuity')
                coords = payload.get('coordinates') or {}
                if isinstance(coords, dict):
                    lat = coords.get('lat')
                    lng = coords.get('lng')
            except Exception:
                pass
            try:
                lat = float(lat) if lat is not None else None
                lng = float(lng) if lng is not None else None
            except Exception:
                lat = lng = None
            if lat is None or lng is None:
                rlat, rlng = _resolve_job_lat_lng_for_map(payload)
                if rlat is not None and rlng is not None:
                    lat, lng = rlat, rlng
            job['reason_for_call'] = reason_for_call
            job['address'] = address
            job['postcode'] = postcode
            job['what3words'] = what3words
            job['priority'] = priority
            plab = str(payload.get('call_priority_label')
                       or payload.get('priority_label') or '').strip()
            job['priority_label'] = plab or None
            ps_src = str(payload.get('priority_source') or '').strip()
            job['priority_source'] = ps_src if ps_src else None
            job['mdt_grade1_panic_active'] = bool(
                payload.get('mdt_grade1_panic_active'))
            _panic_at = payload.get('mdt_last_grade1_panic_at')
            job['mdt_last_grade1_panic_at'] = _panic_at if _panic_at else None
            _panic_cs = str(payload.get('mdt_panic_callsign') or '').strip()
            job['mdt_panic_callsign'] = _panic_cs if _panic_cs else None
            job['mdt_panic_on_existing_job'] = bool(
                payload.get('mdt_panic_on_existing_job'))
            job['mdt_panic_crew_signed_on'] = []
            try:
                jcad = int(job['cad'])
                pcs = (_panic_cs or '').strip() or None
                if not pcs:
                    ccb = str(job.get('claimedBy') or '').strip()
                    if ccb:
                        pcs = ccb.split(',')[0].strip() or None
                if not pcs and has_mdt_job_units:
                    cur.execute(
                        """
                        SELECT callsign FROM mdt_job_units
                        WHERE job_cad = %s
                        ORDER BY assigned_at ASC, id ASC
                        LIMIT 1
                        """,
                        (jcad,),
                    )
                    ju = cur.fetchone()
                    if ju and ju.get('callsign'):
                        pcs = str(ju['callsign']).strip()
                if (ps_src == 'mdt_panic' or bool(payload.get('mdt_grade1_panic_active'))) and pcs:
                    cur.execute(
                        "SELECT crew FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
                        (pcs,),
                    )
                    urow = cur.fetchone()
                    if urow:
                        job['mdt_panic_crew_signed_on'] = _mdt_signed_on_crew_display_labels(
                            urow.get('crew')
                        )
            except Exception:
                job['mdt_panic_crew_signed_on'] = []
            job['lat'] = lat
            job['lng'] = lng
            break_units = payload.get('break_override_last_units')
            if not isinstance(break_units, list):
                break_units = []
            break_units = [str(x).strip()
                           for x in break_units if str(x).strip()]
            job['break_override_last_units'] = break_units
            job['break_override_last_at'] = payload.get(
                'break_override_last_at')
            job['break_override_last_by'] = payload.get(
                'break_override_last_by')
            job['break_override_active'] = bool(break_units)
            job_division = _extract_job_division(
                payload, fallback=job.get('division') or 'general')
            job['division'] = job_division
            job['is_external'] = bool(
                selected_division
                and selected_division != _CAD_DIVISION_ALL
                and job_division != selected_division
            )
            job.pop('data', None)
        return jsonify(jobs)
    finally:
        cur.close()
        conn.close()


@internal.route('/jobs/history', methods=['GET'])
@login_required
def jobs_history():
    """Return cleared and stood-down jobs for the history / past panel (reopen supported)."""
    selected_division, include_external = _request_division_scope()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        selected_division, include_external, access = _enforce_dispatch_scope(
            cur, selected_division, include_external)
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'claimedBy'")
        has_claimed_by = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
        has_updated_at = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'data'")
        has_data = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_division = cur.fetchone() is not None
        _ensure_job_division_snapshot_columns(cur)
        has_snap = _jobs_have_division_snapshot_columns(cur)
        snap_name_sql = "division_snapshot_name" if has_snap else "NULL AS division_snapshot_name"
        snap_color_sql = "division_snapshot_color" if has_snap else "NULL AS division_snapshot_color"

        claimed_by_sql = "claimedBy" if has_claimed_by else "NULL AS claimedBy"
        updated_at_sql = "updated_at" if has_updated_at else "NULL AS updated_at"
        data_sql = "data" if has_data else "NULL AS data"
        division_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_division else "'general' AS division"
        order_by_sql = "updated_at DESC" if has_updated_at else "created_at DESC"

        sql = f"""
            SELECT cad,
                   TRIM(COALESCE(status, '')) AS status,
                   {data_sql},
                   created_at,
                   {updated_at_sql},
                   {claimed_by_sql},
                   {division_sql},
                   {snap_name_sql},
                   {snap_color_sql}
            FROM mdt_jobs
            WHERE LOWER(TRIM(COALESCE(status, ''))) IN ('cleared', 'stood_down')
        """
        job_div_expr = "LOWER(TRIM(COALESCE(division, 'general')))"
        div_frag, args = _division_scope_where_sql(
            selected_division, include_external, access, has_division, job_div_expr
        )
        sql += div_frag
        sql += f" ORDER BY {order_by_sql} LIMIT 500"
        cur.execute(sql, tuple(args))
        jobs = cur.fetchall()
        for job in jobs:
            job['sql_division'] = _normalize_division(
                job.get('division'), fallback='general')
        _enrich_cleared_job_rows_division_audit(cur, jobs)
        # Parse triage payload and convert coordinates if present
        for job in jobs:
            reason_for_call = None
            lat = None
            lng = None
            priority = None
            address = None
            postcode = None
            payload = job.get('data')
            try:
                if isinstance(payload, (bytes, bytearray)):
                    payload = payload.decode('utf-8', errors='ignore')
                if isinstance(payload, str):
                    payload = json.loads(payload) if payload else {}
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}
            try:
                reason_for_call = payload.get('reason_for_call')
                address = payload.get('address')
                postcode = payload.get('postcode')
                priority = payload.get('call_priority') or payload.get(
                    'priority') or payload.get('acuity')
                coords = payload.get('coordinates') or {}
                if isinstance(coords, dict):
                    lat = coords.get('lat')
                    lng = coords.get('lng')
            except Exception:
                pass
            try:
                lat = float(lat) if lat is not None else None
                lng = float(lng) if lng is not None else None
            except Exception:
                lat = lng = None
            if lat is None or lng is None:
                rlat, rlng = _resolve_job_lat_lng_for_map(payload)
                if rlat is not None and rlng is not None:
                    lat, lng = rlat, rlng
            sql_div = job.pop('sql_division', None) or 'general'
            job['reason_for_call'] = reason_for_call
            job['priority'] = priority
            plab = str(payload.get('call_priority_label')
                       or payload.get('priority_label') or '').strip()
            job['priority_label'] = plab or None
            job['address'] = address
            job['postcode'] = postcode
            job['lat'] = lat
            job['lng'] = lng
            job_division = _extract_job_division(payload, fallback=sql_div)
            job['division'] = job_division
            job['is_external'] = bool(
                selected_division
                and selected_division != _CAD_DIVISION_ALL
                and job_division != selected_division
            )
            job.pop('data', None)
        return jsonify(jobs)
    except Exception:
        logger.exception("jobs_history failed")
        # Keep UI usable even when legacy schema/data is inconsistent.
        return jsonify([])
    finally:
        cur.close()
        conn.close()


def _aggregate_response_log_segment(events_slice):
    """MAX(event_time) per ladder status within a slice of ``mdt_response_log`` rows (dict rows)."""
    key_by_status = {
        'received': 'received_time',
        'assigned': 'assigned_time',
        'mobile': 'mobile_time',
        'on_scene': 'on_scene_time',
        'leave_scene': 'leave_scene_time',
        'at_hospital': 'at_hospital_time',
        'cleared': 'cleared_time',
        'stood_down': 'stood_down_time',
        _RESPONSE_LOG_STATUS_CLOSURE_REVIEW: 'closure_review_time',
    }
    agg = {}
    for ev in events_slice or []:
        st = str(ev.get('status') or '').strip()
        if st == _RESPONSE_LOG_STATUS_DISPATCH_REOPENED:
            continue
        out_key = key_by_status.get(st)
        if not out_key:
            continue
        et = ev.get('event_time')
        if et is None:
            continue
        prev = agg.get(out_key)
        if prev is None or et > prev:
            agg[out_key] = et
    return agg


def _timing_payload_for_cad_from_events(cad, events):
    """
    Build timings for one CAD from ordered ``mdt_response_log`` rows.

    After ``dispatch_reopened``, status timestamps are taken only from rows at/after that
    reopen so the UI can show a first lifecycle, a reopen marker, then mobile/on-scene/etc.
    for the new deployment. Flat top-level fields mirror the **latest** post-reopen slice
    (or the full history when there is no reopen) for list timers / backward compatibility.
    """
    reopen_times = [
        e['event_time'] for e in (events or [])
        if str(e.get('status') or '').strip() == _RESPONSE_LOG_STATUS_DISPATCH_REOPENED
        and e.get('event_time') is not None
    ]
    if not reopen_times:
        agg = _aggregate_response_log_segment(events)
        row = {'cad': cad, **agg}
        row['dispatch_reopened_time'] = None
        row['timeline'] = None
        return row

    r0 = reopen_times[0]
    pre_events = [e for e in (events or []) if e.get('event_time') is not None and e['event_time'] < r0]
    timeline = [{'kind': 'segment', 'times': _aggregate_response_log_segment(pre_events)}]
    for i, rt in enumerate(reopen_times):
        timeline.append({'kind': 'reopen', 'time': rt})
        nxt = reopen_times[i + 1] if i + 1 < len(reopen_times) else None
        seg_events = [
            e for e in (events or [])
            if e.get('event_time') is not None
            and e['event_time'] >= rt
            and (nxt is None or e['event_time'] < nxt)
        ]
        timeline.append({'kind': 'segment', 'times': _aggregate_response_log_segment(seg_events)})

    last_rt = reopen_times[-1]
    post_events = [e for e in (events or []) if e.get('event_time') is not None and e['event_time'] >= last_rt]
    flat = _aggregate_response_log_segment(post_events)
    row = {'cad': cad, **flat}
    row['dispatch_reopened_time'] = last_rt
    row['timeline'] = timeline
    return row


@internal.route('/jobs/timings', methods=['GET'])
@login_required
def jobs_timings():
    """Return status timestamps per CAD for timing/duration display."""
    cad_args = request.args.getlist('cad')
    cads = []
    for c in cad_args:
        try:
            cads.append(int(c))
        except Exception:
            continue
    # De-dup while preserving order
    cads = list(dict.fromkeys(cads))
    if not cads:
        return jsonify([])

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'mdt_response_log'")
        if cur.fetchone() is None:
            return jsonify([])

        placeholders = ",".join(["%s"] * len(cads))
        cur.execute(
            f"""
            SELECT cad, status, event_time, id
              FROM mdt_response_log
             WHERE cad IN ({placeholders})
             ORDER BY cad ASC, event_time ASC, id ASC
            """,
            tuple(cads),
        )
        raw = cur.fetchall() or []
        by_cad = {}
        for r in raw:
            try:
                c = int(r.get('cad'))
            except Exception:
                continue
            by_cad.setdefault(c, []).append(r)
        out = [_timing_payload_for_cad_from_events(c, by_cad.get(c) or []) for c in cads]
        return jsonify(out)
    except Exception:
        logger.exception("jobs_timings failed")
        return jsonify([])
    finally:
        cur.close()
        conn.close()


@internal.route('/job/<int:cad>', methods=['GET'])
@login_required
def job_detail(cad):
    """Return full job/incident details for the detail panel."""
    selected_division, include_external = _request_division_scope()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        selected_division, include_external, access = _enforce_dispatch_scope(
            cur, selected_division, include_external)
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'private_notes'")
        has_private_notes = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'public_notes'")
        has_public_notes = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'final_status'")
        has_final_status = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'outcome'")
        has_outcome = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
        has_updated_at = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'created_at'")
        has_created_at = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'claimedBy'")
        has_claimed_by = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_division = cur.fetchone() is not None
        _ensure_job_division_snapshot_columns(cur)
        has_snap = _jobs_have_division_snapshot_columns(cur)
        snap_name_sql = "division_snapshot_name" if has_snap else "NULL AS division_snapshot_name"
        snap_color_sql = "division_snapshot_color" if has_snap else "NULL AS division_snapshot_color"

        private_notes_sql = "private_notes" if has_private_notes else "NULL AS private_notes"
        public_notes_sql = "public_notes" if has_public_notes else "NULL AS public_notes"
        final_status_sql = "final_status" if has_final_status else "NULL AS final_status"
        outcome_sql = "outcome" if has_outcome else "NULL AS outcome"
        updated_at_sql = "updated_at" if has_updated_at else "NULL AS updated_at"
        created_at_sql = "created_at" if has_created_at else "NULL AS created_at"
        claimed_by_sql = "claimedBy" if has_claimed_by else "NULL AS claimedBy"
        division_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_division else "'general' AS division"

        cur.execute(f"""
            SELECT cad, status, data, {claimed_by_sql},
                   {private_notes_sql},
                   {public_notes_sql},
                   {final_status_sql},
                   {outcome_sql},
                   {updated_at_sql},
                   {created_at_sql},
                   {division_sql},
                   {snap_name_sql},
                   {snap_color_sql}
            FROM mdt_jobs
            WHERE cad = %s
        """, (cad,))
        job = cur.fetchone()
        if not job:
            return jsonify({'error': 'Not found'}), 404
        # Parse triage_data if JSON
        try:
            job['triage_data'] = json.loads(job['data']) if job['data'] else {}
        except Exception:
            job['triage_data'] = {}
        triage_payload = job.get('triage_data') if isinstance(
            job.get('triage_data'), dict) else {}
        break_units = triage_payload.get('break_override_last_units')
        if not isinstance(break_units, list):
            break_units = []
        break_units = [str(x).strip() for x in break_units if str(x).strip()]
        job['break_override_last_units'] = break_units
        job['break_override_last_at'] = triage_payload.get(
            'break_override_last_at')
        job['break_override_last_by'] = triage_payload.get(
            'break_override_last_by')
        job['break_override_active'] = bool(break_units)
        del job['data']
        job['assigned_units'] = []
        try:
            cur.execute("SHOW TABLES LIKE 'mdt_job_units'")
            if cur.fetchone() is not None:
                job['assigned_units'] = _get_job_unit_callsigns(cur, cad)
        except Exception:
            pass
        job['assigned_unit_states'] = []
        try:
            assigned_units = [str(cs).strip() for cs in (
                job.get('assigned_units') or []) if str(cs).strip()]
            if assigned_units:
                cur.execute(
                    "SHOW COLUMNS FROM mdts_signed_on LIKE 'lastSeenAt'")
                has_last_seen = cur.fetchone() is not None
                cur.execute(
                    "SHOW COLUMNS FROM mdts_signed_on LIKE 'signOnTime'")
                has_sign_on = cur.fetchone() is not None
                if has_last_seen and has_sign_on:
                    last_seen_sql = "COALESCE(lastSeenAt, signOnTime)"
                elif has_last_seen:
                    last_seen_sql = "lastSeenAt"
                elif has_sign_on:
                    last_seen_sql = "signOnTime"
                else:
                    last_seen_sql = "NULL"
                placeholders = ",".join(["%s"] * len(assigned_units))
                cur.execute(
                    f"""
                    SELECT callSign,
                           LOWER(TRIM(COALESCE(status, ''))) AS status,
                           {last_seen_sql} AS last_seen
                    FROM mdts_signed_on
                    WHERE callSign IN ({placeholders})
                    """,
                    tuple(assigned_units),
                )
                online_rows = {str((r or {}).get('callSign') or '').strip(): (
                    r or {}) for r in (cur.fetchall() or [])}
                now_dt = datetime.utcnow()
                for cs in assigned_units:
                    row = online_rows.get(cs) or {}
                    status = str(row.get('status') or '').strip().lower()
                    last_seen = row.get('last_seen')
                    stale = False
                    if isinstance(last_seen, datetime):
                        try:
                            stale = (now_dt - last_seen).total_seconds() > 300
                        except Exception:
                            stale = False
                    signed_on = bool(row)
                    responding = bool(signed_on and not stale)
                    if not signed_on:
                        alert = 'signed_off'
                    elif stale:
                        alert = 'no_signal'
                    else:
                        alert = ''
                    job['assigned_unit_states'].append({
                        'callsign': cs,
                        'status': status,
                        'responding': responding,
                        'alert': alert,
                    })
        except Exception:
            pass
        job['previously_attached_units'] = []
        try:
            job['previously_attached_units'] = _previous_attached_units_for_cad(
                cur, cad, job.get('assigned_units') or []
            )
        except Exception:
            pass
        db_division = _normalize_division(
            job.get('division'), fallback='general')
        job['division'] = _extract_job_division(
            triage_payload, fallback=db_division)
        _enrich_job_detail_division_audit(cur, job)
        if access.get('restricted'):
            allowed = set(access.get('divisions') or [])
            if job['division'] not in allowed and not access.get('can_override_all'):
                return jsonify({'error': 'Not permitted for this division'}), 403
        if (
            selected_division
            and selected_division != _CAD_DIVISION_ALL
            and job['division'] != selected_division
            and not include_external
        ):
            return jsonify({'error': 'Job not in selected division'}), 404
        jlat, jlng, _ = _extract_coords_from_job_data(triage_payload)
        if jlat is None or jlng is None:
            rlat, rlng = _resolve_job_lat_lng_for_map(triage_payload)
            if rlat is not None and rlng is not None:
                jlat, jlng = rlat, rlng
        job['lat'] = jlat
        job['lng'] = jlng
        job['priority'] = _extract_job_priority_value(triage_payload)
        _plab = str(triage_payload.get('call_priority_label')
                    or triage_payload.get('priority_label') or '').strip()
        job['priority_label'] = _plab or None
        return jsonify(job)
    finally:
        cur.close()
        conn.close()


@internal.route('/job/<int:cad>/system-log', methods=['GET'])
@login_required
def job_system_log(cad):
    """Timeline feed combining system and user events for auditing a CAD."""
    selected_division, include_external = _request_division_scope()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        selected_division, include_external, access = _enforce_dispatch_scope(
            cur, selected_division, include_external)
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_division = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'created_at'")
        has_created_at = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'data'")
        has_data = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'created_by'")
        has_job_created_by_col = cur.fetchone() is not None
        division_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_division else "'general' AS division"
        created_sql = "created_at" if has_created_at else "NOW() AS created_at"
        data_sql = "data" if has_data else "NULL AS data"
        job_created_sql = (
            "TRIM(COALESCE(created_by, '')) AS job_row_created_by"
            if has_job_created_by_col
            else "CAST(NULL AS CHAR) AS job_row_created_by"
        )
        cur.execute(f"""
            SELECT cad, TRIM(COALESCE(status, '')) AS status, {created_sql}, {division_sql}, {data_sql}, {job_created_sql}
            FROM mdt_jobs
            WHERE cad = %s
        """, (cad,))
        job = cur.fetchone()
        if not job:
            return jsonify({"error": "Not found"}), 404

        job_division = _normalize_division(
            job.get("division"), fallback="general")
        if access.get('restricted'):
            allowed = set(access.get('divisions') or [])
            if job_division not in allowed and not access.get('can_override_all'):
                return jsonify({'error': 'Not permitted for this division'}), 403
        if (
            selected_division
            and selected_division != _CAD_DIVISION_ALL
            and job_division != selected_division
            and not include_external
        ):
            return jsonify({"error": "Job not in selected division"}), 404

        events = []
        pdata = {}
        payload = job.get("data")
        if payload:
            try:
                if isinstance(payload, (bytes, bytearray)):
                    payload = payload.decode("utf-8", errors="ignore")
                raw_p = json.loads(payload) if isinstance(
                    payload, str) else payload
                if isinstance(raw_p, dict):
                    pdata = raw_p
            except Exception:
                pdata = {}
        created_by_col = str(job.get("job_row_created_by") or "").strip() or None
        created_by_json = None
        if isinstance(pdata, dict):
            for _k in ("created_by", "intake_username", "submitted_by", "intake_user"):
                _v = str(pdata.get(_k) or "").strip()
                if _v:
                    created_by_json = _v
                    break
        creator = (created_by_col or created_by_json or "").strip() or None
        intake_src = str((pdata or {}).get("intake_source") or "").strip().lower()
        pr_src = str((pdata or {}).get("priority_source") or "").strip().lower()
        if intake_src == "response_triage":
            log_source = "intake"
        elif pr_src in ("mdt_panic", "mdt_running_call"):
            log_source = "mdt"
        elif creator:
            log_source = "dispatch"
        else:
            log_source = "system"
        actor = creator or "unknown"
        ts_txt = _job_system_log_created_ts_text(job.get("created_at"))
        if creator:
            msg = f"CAD #{cad} created by {creator}"
            if ts_txt:
                msg += f" at {ts_txt}"
        else:
            msg = f"CAD #{cad} created"
            if ts_txt:
                msg += f" at {ts_txt} (creator not recorded)"
        events.append({
            "source": log_source,
            "type": "incident_created",
            "time": job.get("created_at"),
            "actor": actor,
            "message": msg,
        })

        _ensure_job_units_table(cur)
        cur.execute("""
            SELECT callsign, assigned_by, assigned_at
            FROM mdt_job_units
            WHERE job_cad = %s
            ORDER BY assigned_at ASC
        """, (cad,))
        for row in (cur.fetchall() or []):
            cs = str(row.get("callsign") or "").strip() or "unknown"
            by = str(row.get("assigned_by") or "").strip() or "system"
            events.append({
                "source": "dispatch",
                "type": "unit_assigned",
                "time": row.get("assigned_at"),
                "actor": by,
                "message": f"{cs} assigned to CAD #{cad}"
            })

        _ensure_response_log_table(cur)
        cur.execute("""
            SELECT callSign, status, event_time, crew
            FROM mdt_response_log
            WHERE cad = %s
            ORDER BY event_time ASC
        """, (cad,))
        for row in (cur.fetchall() or []):
            cs = str(row.get("callSign") or "").strip() or "unknown"
            st_raw = str(row.get("status") or "").strip().lower()
            if st_raw == _RESPONSE_LOG_STATUS_CLOSURE_REVIEW:
                events.append({
                    "source": "system",
                    "type": "closure_review",
                    "time": row.get("event_time"),
                    "actor": cs,
                    "message": (
                        f"CAD #{cad} entered closure review (last unit cleared: {cs})"
                    ),
                })
                continue
            if st_raw == _RESPONSE_LOG_STATUS_DISPATCH_REOPENED:
                extra = ""
                try:
                    cr = row.get("crew")
                    if isinstance(cr, (bytes, bytearray)):
                        cr = cr.decode("utf-8", errors="ignore")
                    if isinstance(cr, str) and cr.strip().startswith("{"):
                        d = json.loads(cr)
                        if isinstance(d, dict) and str(d.get("reason") or "").strip():
                            extra = " — " + str(d.get("reason")).strip()
                except Exception:
                    pass
                events.append({
                    "source": "dispatch",
                    "type": "dispatch_reopened",
                    "time": row.get("event_time"),
                    "actor": cs,
                    "message": (
                        f"CAD #{cad} reopened onto the stack by dispatch ({cs}){extra}"
                    ),
                })
                continue
            st = st_raw.replace("_", " ")
            label = " ".join([x.capitalize() for x in st.split()])
            events.append({
                "source": "mdt",
                "type": "status_update",
                "time": row.get("event_time"),
                "actor": cs,
                "message": f"{cs} status {label}"
            })

        _ensure_job_comms_table(cur)
        cur.execute("""
            SELECT message_type, sender_role, sender_user, message_text, created_at
            FROM mdt_job_comms
            WHERE cad = %s
            ORDER BY created_at ASC
        """, (cad,))
        for row in (cur.fetchall() or []):
            msg_type = str(row.get("message_type")
                           or "message").strip().lower()
            actor = str(row.get("sender_user") or row.get(
                "sender_role") or "unknown").strip() or "unknown"
            text = str(row.get("message_text") or "").strip()
            events.append({
                "source": "comms",
                "type": msg_type,
                "time": row.get("created_at"),
                "actor": actor,
                "message": _job_system_log_comms_display_message(msg_type, text),
            })

        events.sort(key=lambda x: x.get("time") or datetime.min)
        return _jsonify_safe({"cad": cad, "events": events})
    except Exception:
        logger.exception("job_system_log failed")
        return jsonify({"error": "Unable to load system log"}), 500
    finally:
        cur.close()
        conn.close()


@internal.route('/units', methods=['GET', 'POST'])
@login_required
def units():
    """GET: active units for the units panel and live map. POST: create a callsign (no crew) for MDT to join."""
    if request.method == 'POST':
        return _create_dispatch_placeholder_unit()
    selected_division, include_external = _request_division_scope()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_mdts_signed_on_schema(cur)
        _ensure_meal_break_columns(cur)
        _ventus_set_g_break_policy(cur)
        _ensure_standby_tables(cur)
        try:
            cur.execute("""
                UPDATE mdts_signed_on
                   SET status = 'on_standby',
                       mealBreakStartedAt = NULL,
                       mealBreakUntil = NULL,
                       mealBreakTakenAt = COALESCE(mealBreakTakenAt, NOW())
                 WHERE LOWER(TRIM(COALESCE(status, ''))) = 'meal_break'
                   AND mealBreakUntil IS NOT NULL
                   AND mealBreakUntil <= NOW()
            """)
            conn.commit()
        except Exception:
            conn.rollback()
        selected_division, include_external, access = _enforce_dispatch_scope(
            cur, selected_division, include_external)
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_division = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'lastSeenAt'")
        has_last_seen = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'signOnTime'")
        has_sign_on_time = cur.fetchone() is not None
        division_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_division else "'general' AS division"
        if has_last_seen and has_sign_on_time:
            seen_expr = "COALESCE(lastSeenAt, signOnTime)"
        elif has_last_seen:
            seen_expr = "lastSeenAt"
        elif has_sign_on_time:
            seen_expr = "signOnTime"
        else:
            seen_expr = "NOW()"
        sql = """
            SELECT m.callSign, m.status,
                   COALESCE(lastLat, NULL) AS latitude, 
                   COALESCE(lastLon, NULL) AS longitude,
                   m.assignedIncident,
                   m.crew,
                   COALESCE(m.cad_placeholder, 0) AS cad_placeholder,
                   s.name AS standby_name,
                   s.lat AS standby_lat,
                   s.lng AS standby_lng,
                   m.mealBreakStartedAt,
                   m.mealBreakUntil,
                   m.mealBreakTakenAt,
                   m.shiftStartAt,
                   m.shiftEndAt,
                   m.shiftDurationMins,
                   m.breakDueAfterMins,
                   m.signOnTime,
                   {division_sql}
            FROM mdts_signed_on m
            LEFT JOIN standby_locations s ON s.callSign = m.callSign
            WHERE m.status IS NOT NULL
              AND {seen_expr} >= DATE_SUB(NOW(), INTERVAL 120 MINUTE)
        """.format(division_sql=division_sql, seen_expr=seen_expr.replace("lastSeenAt", "m.lastSeenAt").replace("signOnTime", "m.signOnTime"))
        unit_div_expr = "LOWER(TRIM(COALESCE(m.division, 'general')))"
        div_frag, args = _division_scope_where_sql(
            selected_division, include_external, access, has_division, unit_div_expr
        )
        sql += div_frag
        sql += " ORDER BY m.callSign ASC"
        cur.execute(sql, tuple(args))
        units = cur.fetchall() or []
        # Convert lat/lon to float if present
        for unit in units:
            try:
                unit['latitude'] = float(
                    unit['latitude']) if unit['latitude'] is not None else None
                unit['longitude'] = float(
                    unit['longitude']) if unit['longitude'] is not None else None
                unit['standby_lat'] = float(
                    unit['standby_lat']) if unit.get('standby_lat') is not None else None
                unit['standby_lng'] = float(
                    unit['standby_lng']) if unit.get('standby_lng') is not None else None
            except Exception:
                unit['latitude'] = unit['longitude'] = None
                unit['standby_lat'] = unit['standby_lng'] = None
            try:
                unit['assignedIncident'] = int(unit['assignedIncident']) if unit.get(
                    'assignedIncident') is not None else None
            except Exception:
                unit['assignedIncident'] = None
            unit_division = _normalize_division(
                unit.get('division'), fallback='general')
            unit['division'] = unit_division
            unit['is_external'] = bool(
                selected_division
                and selected_division != _CAD_DIVISION_ALL
                and unit_division != selected_division
            )
            shift_state = _compute_shift_break_state(unit)
            unit.update(shift_state)
            unit['has_crew'] = _unit_crew_has_members(unit.get('crew'))
        ph_cs = [
            str(u.get('callSign') or '').strip()
            for u in units
            if int(u.get('cad_placeholder') or 0) == 1 and str(u.get('callSign') or '').strip()
        ]
        if ph_cs:
            try:
                ph_marks = ','.join(['%s'] * len(ph_cs))
                cur.execute(
                    f"UPDATE mdts_signed_on SET lastSeenAt = NOW() WHERE callSign IN ({ph_marks})",
                    tuple(ph_cs),
                )
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
        return _jsonify_safe(units, 200)
    finally:
        cur.close()
        conn.close()


def _create_dispatch_placeholder_unit():
    """Insert mdts_signed_on row with empty crew so MDT users can join the callsign."""
    if not _ventus_dispatch_capable():
        return jsonify({'error': 'Unauthorised'}), 403
    data = request.get_json(silent=True) or {}
    raw_cs = str(data.get('callSign') or data.get('callsign') or '').strip().upper()
    if not raw_cs or len(raw_cs) > 64:
        return jsonify({'error': 'callsign required (max 64 characters)'}), 400
    if not re.match(r'^[A-Z0-9][A-Z0-9 _.-]{0,63}$', raw_cs):
        return jsonify({
            'error': 'Invalid callsign (letters, numbers, space, underscore, dot, hyphen only)',
        }), 400
    raw_div = data.get('division')
    division = _normalize_division(raw_div, fallback='general')
    break_due_after = data.get('break_due_after_mins')
    break_due_after_mins = 240

    def _parse_iso_dt(val):
        if val is None or str(val).strip() == '':
            return None
        s = str(val).strip().replace('Z', '+00:00')
        try:
            if 'T' in s and len(s) >= 16:
                return datetime.strptime(s[:16], '%Y-%m-%dT%H:%M')
            if len(s) == 16 and 'T' not in s:
                return datetime.strptime(s, '%Y-%m-%d %H:%M')
            return datetime.fromisoformat(s)
        except Exception:
            return None

    shift_start_at = _parse_iso_dt(data.get('shift_start') or data.get('shiftStart'))
    shift_end_at = _parse_iso_dt(data.get('shift_end') or data.get('shiftEnd'))
    shift_duration_mins = None
    if shift_start_at and shift_end_at:
        try:
            delta = shift_end_at - shift_start_at
            shift_duration_mins = max(1, int(delta.total_seconds() // 60))
        except Exception:
            shift_duration_mins = None

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_mdts_signed_on_schema(cur)
        _ventus_set_g_break_policy(cur)
        if break_due_after is not None and str(break_due_after).strip() != '':
            try:
                break_due_after_mins = max(
                    15, min(24 * 60, int(break_due_after)))
            except (TypeError, ValueError):
                break_due_after_mins = _resolve_default_break_due_after_mins(
                    cur, shift_duration_mins)
        else:
            break_due_after_mins = _resolve_default_break_due_after_mins(
                cur, shift_duration_mins)
        access = _get_dispatch_user_division_access(cur)
        div_final = _normalize_division(division, fallback='general')
        if access.get('restricted'):
            allowed = {d for d in (access.get('divisions') or []) if d}
            if div_final not in allowed:
                return jsonify({
                    'error': 'You may only create callsigns in your assigned division(s).',
                }), 403
        division = div_final
        cur.execute("SELECT callSign FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (raw_cs,))
        if cur.fetchone():
            return jsonify({'error': 'That callsign already exists'}), 409
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_division = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'cad_placeholder'")
        has_ph = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'shiftStartAt'")
        has_shift_start = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'shiftEndAt'")
        has_shift_end = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'shiftDurationMins'")
        has_shift_duration = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'breakDueAfterMins'")
        has_break_due = cur.fetchone() is not None

        ip = str(request.headers.get('X-Forwarded-For') or request.remote_addr or 'cad-dispatch')[:255]
        crew_json = '[]'
        if has_division:
            if has_ph:
                cur.execute(
                    """
                    INSERT INTO mdts_signed_on
                      (callSign, ipAddress, status, crew, division, lastSeenAt, signOnTime, cad_placeholder)
                    VALUES (%s, %s, 'on_standby', %s, %s, NOW(), NOW(), 1)
                    """,
                    (raw_cs, ip, crew_json, division),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO mdts_signed_on
                      (callSign, ipAddress, status, crew, division, lastSeenAt, signOnTime)
                    VALUES (%s, %s, 'on_standby', %s, %s, NOW(), NOW())
                    """,
                    (raw_cs, ip, crew_json, division),
                )
        else:
            if has_ph:
                cur.execute(
                    """
                    INSERT INTO mdts_signed_on
                      (callSign, ipAddress, status, crew, lastSeenAt, signOnTime, cad_placeholder)
                    VALUES (%s, %s, 'on_standby', %s, NOW(), NOW(), 1)
                    """,
                    (raw_cs, ip, crew_json),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO mdts_signed_on
                      (callSign, ipAddress, status, crew, lastSeenAt, signOnTime)
                    VALUES (%s, %s, 'on_standby', %s, NOW(), NOW())
                    """,
                    (raw_cs, ip, crew_json),
                )
        post_set = []
        post_args = []
        if has_shift_start and shift_start_at:
            post_set.append('shiftStartAt = %s')
            post_args.append(shift_start_at)
        if has_shift_end and shift_end_at:
            post_set.append('shiftEndAt = %s')
            post_args.append(shift_end_at)
        if has_shift_duration and shift_duration_mins:
            post_set.append('shiftDurationMins = %s')
            post_args.append(shift_duration_mins)
        if has_break_due:
            post_set.append('breakDueAfterMins = %s')
            post_args.append(break_due_after_mins)
        if post_set:
            post_args.append(raw_cs)
            cur.execute(
                f"UPDATE mdts_signed_on SET {', '.join(post_set)} WHERE callSign = %s",
                tuple(post_args),
            )
        conn.commit()
        try:
            _cad_emit_units_updated_socket(raw_cs)
        except Exception:
            pass
        return jsonify({'ok': True, 'callSign': raw_cs, 'division': division})
    except Exception as ex:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception('create_dispatch_placeholder_unit failed')
        return jsonify({'error': str(ex) or 'Create failed'}), 500
    finally:
        cur.close()
        conn.close()


@internal.route('/crew-grades', methods=['GET'])
@login_required
def crew_grades():
    """Return list of crew grades/roles (e.g. paramedic, AAP, ECA) for MDT and CAD dropdowns."""
    allowed = ["dispatcher", "admin", "superuser", "clinical_lead",
               "controller", "crew", "call_taker", "call_handler"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed:
        return jsonify({'error': 'Unauthorised'}), 403
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        grades = _list_crew_grades(cur)
        return _jsonify_safe({"grades": grades}, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/job/<int:cad>/update-details', methods=['POST'])
@login_required
def update_job_details(cad):
    """Update editable incident details for an existing CAD job."""
    if not _user_has_role("crew", "dispatcher", "admin", "superuser", "clinical_lead", "call_taker", "calltaker", "controller", "call_handler", "callhandler"):
        return jsonify({'error': 'Unauthorised'}), 403

    payload = request.get_json() or {}
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT data FROM mdt_jobs WHERE cad = %s", (cad,))
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Job not found'}), 404

        raw = row.get('data')
        try:
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode('utf-8', errors='ignore')
            data = json.loads(raw) if isinstance(raw, str) and raw else {}
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}

        old_priority_rank = _priority_rank(_extract_job_priority_value(data))

        prev_addr = str(data.get('address') or '').strip()
        prev_pc = str(data.get('postcode') or '').strip()
        prev_w3w = str(data.get('what3words') or '').strip()

        editable_fields = [
            'reason_for_call', 'address', 'postcode', 'what3words',
            'first_name', 'middle_name', 'last_name', 'patient_dob', 'patient_age',
            'phone_number', 'patient_gender', 'caller_name', 'caller_phone',
            'additional_details', 'onset_datetime', 'patient_alone',
            'call_priority', 'priority_source', 'division'
        ]
        changed = {}
        for key in editable_fields:
            if key in payload:
                val = payload.get(key)
                if isinstance(val, str):
                    val = val.strip()
                if key == 'patient_gender':
                    val = _normalize_patient_gender(val)
                if key == 'patient_age' and val is not None and val != '':
                    try:
                        n = int(float(val))
                        val = str(n) if n >= 0 else None
                    except (TypeError, ValueError):
                        val = None
                elif key == 'patient_age' and (val is None or val == ''):
                    val = None
                data[key] = val
                changed[key] = val

        addr = str(data.get('address') or '').strip()
        pc = str(data.get('postcode') or '').strip()
        w3w = str(data.get('what3words') or '').strip()

        stored_lat, stored_lng, _ = _extract_coords_from_job_data(data)
        location_text_changed = (
            addr != prev_addr or pc != prev_pc or w3w != prev_w3w
        )
        location_keys_in_payload = any(
            k in payload for k in ('address', 'postcode', 'what3words')
        )
        have_location = bool(addr or pc or w3w)
        coords_valid = stored_lat is not None and stored_lng is not None
        # Geocode only when location text actually changed, or we are retrying resolve
        # after failure (no valid coords) from a client that submitted location fields.
        # Avoids a what3words/Google/OSM call on every save when the frontend always
        # sends address/postcode/w3w unchanged (e.g. call centre full form).
        need_geocode = have_location and (
            location_text_changed
            or (not coords_valid and location_keys_in_payload)
        )
        if need_geocode:
            resolved = ResponseTriage.get_best_lat_lng(
                address=addr or None,
                postcode=pc or None,
                what3words=w3w or None,
            )
            if isinstance(resolved, dict) and resolved.get('lat') is not None and resolved.get('lng') is not None:
                try:
                    lat = float(resolved.get('lat'))
                    lng = float(resolved.get('lng'))
                    coord_payload = {k: v for k,
                                     v in resolved.items() if k != 'error'}
                    coord_payload['lat'] = lat
                    coord_payload['lng'] = lng
                    data['coordinates'] = coord_payload
                    changed['coordinates'] = coord_payload
                except (TypeError, ValueError):
                    pass
            else:
                err_msg = None
                if isinstance(resolved, dict):
                    err_msg = resolved.get('error')
                err_msg = str(
                    err_msg or 'Could not resolve coordinates').strip()
                # Always persist failure here: we only run this block when location
                # fields changed or coords were missing. Keeping old lat/lng after an
                # edit (e.g. postcode → what3words) left misleading pins when W3W failed.
                data['coordinates'] = {'error': err_msg}
                changed['coordinates'] = data['coordinates']
                try:
                    logger.warning("job %s geocode failed: %s",
                                   cad, err_msg[:200])
                except Exception:
                    pass

        # Optional patch for form-specific answers.
        answers = payload.get('form_answers')
        if isinstance(answers, dict):
            existing = data.get('form_answers')
            if not isinstance(existing, dict):
                existing = {}
            for k, v in answers.items():
                kk = str(k or '').strip()
                if not kk:
                    continue
                existing[kk] = v.strip() if isinstance(v, str) else v
            data['form_answers'] = existing
            changed['form_answers'] = existing

        sender_user_raw = str(
            getattr(current_user, 'username', 'unknown') or 'unknown').strip()
        sender_hint = payload.get('sender_portal') or payload.get(
            'from') or _current_role_key()
        sender_label = _sender_label_from_portal(sender_hint, sender_user_raw)
        incident_update = str(payload.get('incident_update') or '').strip()
        assigned_units_for_push = []
        if incident_update:
            history = data.get('incident_updates')
            if not isinstance(history, list):
                history = []
            history.append({
                'time': datetime.utcnow().isoformat(),
                'by': sender_label,
                'text': incident_update
            })
            data['incident_updates'] = history
            changed['incident_update'] = incident_update
            try:
                assigned_units_for_push = _get_job_unit_callsigns(cur, cad)
            except Exception:
                assigned_units_for_push = []

        new_priority_rank = _priority_rank(_extract_job_priority_value(data))
        if not changed:
            return jsonify({'message': 'No changes submitted', 'cad': cad, 'updated': False})

        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'chief_complaint'")
        has_chief = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
        has_updated_at = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_division = cur.fetchone() is not None
        reason = data.get('reason_for_call')
        division_value = _normalize_division(
            data.get('division'), fallback='general') if 'division' in changed else None
        if division_value:
            data['division'] = division_value
        if has_chief:
            if has_updated_at:
                if has_division and division_value:
                    cur.execute("""
                        UPDATE mdt_jobs
                        SET data = %s, chief_complaint = %s, division = %s, updated_at = NOW()
                        WHERE cad = %s
                    """, (json.dumps(data, default=str), reason, division_value, cad))
                else:
                    cur.execute("""
                        UPDATE mdt_jobs
                        SET data = %s, chief_complaint = %s, updated_at = NOW()
                        WHERE cad = %s
                    """, (json.dumps(data, default=str), reason, cad))
            else:
                if has_division and division_value:
                    cur.execute("""
                        UPDATE mdt_jobs
                        SET data = %s, chief_complaint = %s, division = %s
                        WHERE cad = %s
                    """, (json.dumps(data, default=str), reason, division_value, cad))
                else:
                    cur.execute("""
                        UPDATE mdt_jobs
                        SET data = %s, chief_complaint = %s
                        WHERE cad = %s
                    """, (json.dumps(data, default=str), reason, cad))
        else:
            if has_updated_at:
                if has_division and division_value:
                    cur.execute("""
                        UPDATE mdt_jobs
                        SET data = %s, division = %s, updated_at = NOW()
                        WHERE cad = %s
                    """, (json.dumps(data, default=str), division_value, cad))
                else:
                    cur.execute("""
                        UPDATE mdt_jobs
                        SET data = %s, updated_at = NOW()
                        WHERE cad = %s
                    """, (json.dumps(data, default=str), cad))
            else:
                if has_division and division_value:
                    cur.execute("""
                        UPDATE mdt_jobs
                        SET data = %s, division = %s
                        WHERE cad = %s
                    """, (json.dumps(data, default=str), division_value, cad))
                else:
                    cur.execute("""
                        UPDATE mdt_jobs
                        SET data = %s
                        WHERE cad = %s
                    """, (json.dumps(data, default=str), cad))
        if division_value:
            try:
                cur.execute(
                    "SELECT LOWER(TRIM(COALESCE(status, ''))) AS st FROM mdt_jobs WHERE cad = %s LIMIT 1",
                    (cad,),
                )
                strow = cur.fetchone() or {}
                ost = str((strow.get('st') or '')).lower().strip()
                if ost and ost not in ('cleared', 'stood_down'):
                    _ensure_job_division_snapshot_columns(cur)
                    _sync_job_division_snapshot_for_slug(
                        cur, cad, division_value)
            except Exception:
                logger.exception("division snapshot on job update cad=%s", cad)
        conn.commit()

        if 'call_priority' in changed and new_priority_rank < old_priority_rank:
            try:
                run_priority_preemption_for_job(int(cad))
            except Exception:
                logger.exception(
                    "priority preemption after job details cad=%s", cad)

        # Incident updates: push via socket / job payload only — not into crew `messages`
        # (Response Centre inbox uses mdt_job_comms).

        try:
            socketio.emit('mdt_event', {'type': 'jobs_updated', 'cad': cad})
            if incident_update:
                socketio.emit('mdt_event', {
                    'type': 'job_update',
                    'cad': cad,
                    'text': incident_update,
                    'by': sender_label,
                    'units': assigned_units_for_push
                })
        except Exception:
            pass
        log_audit(
            getattr(current_user, 'username', 'unknown'),
            'job_details_update',
            details={'cad': cad, 'fields': list(changed.keys())},
        )
        return jsonify({'message': 'Job details updated', 'cad': cad, 'updated': True, 'changed': list(changed.keys())})
    finally:
        cur.close()
        conn.close()


@internal.route('/job/<int:cad>/comms', methods=['GET', 'POST'])
@login_required
def job_comms(cad):
    """Job-level communications between call-taker and dispatcher/controller.

    GET omits audit-only rows (``closed``, ``reopened``); those remain in ``mdt_job_comms`` for the
    merged ``job_system_log`` timeline and audit.
    """
    if not _user_has_role("crew", "dispatcher", "admin", "superuser", "clinical_lead", "call_taker", "calltaker", "controller", "call_handler", "callhandler"):
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_job_comms_table(cur)
        cur.execute("SELECT cad FROM mdt_jobs WHERE cad = %s LIMIT 1", (cad,))
        if cur.fetchone() is None:
            return jsonify({'error': 'Job not found'}), 404

        if request.method == 'GET':
            _excl_t, _excl_ph = _job_comms_audit_only_types_sql()
            cur.execute(
                f"""
                SELECT id, cad, message_type, sender_role, sender_user, message_text, created_at
                FROM mdt_job_comms
                WHERE cad = %s
                  AND LOWER(TRIM(COALESCE(message_type, ''))) NOT IN ({_excl_ph})
                ORDER BY created_at ASC, id ASC
                LIMIT 800
                """,
                (cad,) + _excl_t,
            )
            return jsonify(cur.fetchall() or [])

        payload = request.get_json() or {}
        msg_text = str(payload.get('text') or '').strip()
        msg_type = str(payload.get('type') or 'message').strip().lower()
        if msg_type in _JOB_COMMS_BACKUP_QUICK_LOG_KEYS:
            msg_text = msg_text or _job_comms_backup_quick_log_message(
                cur, msg_type)
        elif msg_type not in ('message', 'update'):
            msg_type = 'message'
        if not msg_text:
            return jsonify({'error': 'text is required'}), 400

        sender_role = _current_role_key()
        sender_user_raw = str(
            getattr(current_user, 'username', 'unknown') or 'unknown').strip()
        sender_portal = payload.get(
            'sender_portal') or payload.get('from') or sender_role
        sender_user = _sender_label_from_portal(sender_portal, sender_user_raw)

        cur.execute("""
            INSERT INTO mdt_job_comms (cad, message_type, sender_role, sender_user, message_text)
            VALUES (%s, %s, %s, %s, %s)
        """, (cad, msg_type, sender_role, sender_user, msg_text))
        job_comm_row_id = int(getattr(cur, 'lastrowid', None) or 0)

        assigned_units_for_push = []
        if msg_type == 'update':
            cur.execute("SELECT data FROM mdt_jobs WHERE cad = %s", (cad,))
            row = cur.fetchone()
            if row is None:
                conn.rollback()
                return jsonify({'error': 'Job not found'}), 404

            raw = row.get('data')
            try:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode('utf-8', errors='ignore')
                data = json.loads(raw) if isinstance(raw, str) and raw else {}
                if not isinstance(data, dict):
                    data = {}
            except Exception:
                data = {}

            history = data.get('incident_updates')
            if not isinstance(history, list):
                history = []
            history.append({
                'time': datetime.utcnow().isoformat(),
                'by': sender_user,
                'text': msg_text
            })
            data['incident_updates'] = history
            cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
            has_updated_at = cur.fetchone() is not None
            if has_updated_at:
                cur.execute("UPDATE mdt_jobs SET data = %s, updated_at = NOW() WHERE cad = %s",
                            (json.dumps(data, default=str), cad))
            else:
                cur.execute("UPDATE mdt_jobs SET data = %s WHERE cad = %s",
                            (json.dumps(data, default=str), cad))
            try:
                assigned_units_for_push = _get_job_unit_callsigns(cur, cad)
            except Exception:
                assigned_units_for_push = []

        conn.commit()

        try:
            _jc_payload = {'type': 'job_comm', 'cad': cad,
                          'message_type': msg_type, 'text': msg_text, 'by': sender_user}
            if job_comm_row_id > 0:
                _jc_payload['id'] = job_comm_row_id
            socketio.emit('mdt_event', _jc_payload)
            if msg_type == 'update':
                socketio.emit(
                    'mdt_event', {'type': 'jobs_updated', 'cad': cad})
                socketio.emit('mdt_event', {
                              'type': 'job_update', 'cad': cad, 'text': msg_text, 'by': sender_user, 'units': assigned_units_for_push})
        except Exception:
            pass
        if msg_type == 'update' and assigned_units_for_push:
            try:
                preview = (msg_text or '').strip()[
                    :400] or 'Open MDT for latest CAD details.'
                title = f'CAD #{cad} update'
                for unit_cs in assigned_units_for_push:
                    ucs = str(unit_cs or '').strip().upper()
                    if not ucs or ucs == 'DISPATCHER':
                        continue
                    _mdt_web_push_notify_callsign(
                        ucs,
                        title,
                        preview,
                        tag=f'ventus-cad-update-{cad}',
                        url='/dashboard',
                        alert=False,
                        silent=False,
                        require_interaction=False,
                        cad=cad,
                    )
            except Exception:
                pass

        if msg_type == 'update':
            log_audit(
                getattr(current_user, 'username', 'unknown'),
                'cad_job_comm_update',
                details={'cad': cad, 'sender_user': sender_user,
                         'text_len': len(msg_text)},
            )
        elif msg_type in _JOB_COMMS_BACKUP_QUICK_LOG_KEYS:
            g = msg_type.replace("backup_p", "").strip()
            tier_lbl = _get_dispatcher_backup_quick_log_labels(
                cur).get(msg_type, f"Grade {g}")
            log_audit(
                getattr(current_user, 'username', 'unknown'),
                'crew_backup_quick_log',
                details={
                    'cad': cad,
                    'backup_grade': g,
                    'backup_grade_label': tier_lbl,
                    'sender_user': sender_user,
                },
            )

        return jsonify({'message': 'sent', 'cad': cad, 'type': msg_type}), 200
    finally:
        cur.close()
        conn.close()


@internal.route('/job-comms/recent', methods=['GET'])
@login_required
def recent_job_comms():
    """Recent CAD comms feed for dispatcher notifications. Returns items and unread_count (persisted per user after Clear).

    Omits audit-only ``mdt_job_comms`` rows (``closed``, ``reopened``), same as GET ``/job/<cad>/comms``.
    """
    if not _user_has_role("dispatcher", "admin", "superuser", "clinical_lead", "controller"):
        return jsonify({"error": "Unauthorised access"}), 403

    limit = request.args.get('limit', default=40, type=int) or 40
    if limit < 1:
        limit = 1
    if limit > 200:
        limit = 200

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_job_comms_table(cur)
        _ensure_dispatcher_inbox_cleared_table(cur)
        _ensure_job_incident_inbox_anchor_column(cur)
        username = (getattr(current_user, 'username', None) or '').strip()
        cleared_at = None
        if username:
            cur.execute(
                "SELECT cleared_at FROM dispatcher_inbox_cleared WHERE username = %s LIMIT 1",
                (username,)
            )
            r = cur.fetchone()
            if r and r.get('cleared_at'):
                cleared_at = r.get('cleared_at')

        _excl_t, _excl_ph = _job_comms_audit_only_types_sql()
        if cleared_at is not None:
            # Only show inbox items created after the user last cleared their inbox
            cur.execute(
                f"""
                SELECT c.id, c.cad, c.message_type, c.sender_role, c.sender_user, c.message_text, c.created_at
                FROM mdt_job_comms c
                INNER JOIN mdt_jobs j ON j.cad = c.cad
                WHERE LOWER(TRIM(COALESCE(j.status, ''))) NOT IN ('cleared', 'stood_down')
                  AND c.created_at > %s
                  AND LOWER(TRIM(COALESCE(c.message_type, ''))) NOT IN ({_excl_ph})
                  AND (j.incident_inbox_comms_after_id IS NULL OR c.id > j.incident_inbox_comms_after_id)
                ORDER BY c.id DESC
                LIMIT %s
                """,
                (cleared_at,) + _excl_t + (limit,),
            )
        else:
            cur.execute(
                f"""
                SELECT c.id, c.cad, c.message_type, c.sender_role, c.sender_user, c.message_text, c.created_at
                FROM mdt_job_comms c
                INNER JOIN mdt_jobs j ON j.cad = c.cad
                WHERE LOWER(TRIM(COALESCE(j.status, ''))) NOT IN ('cleared', 'stood_down')
                  AND LOWER(TRIM(COALESCE(c.message_type, ''))) NOT IN ({_excl_ph})
                  AND (j.incident_inbox_comms_after_id IS NULL OR c.id > j.incident_inbox_comms_after_id)
                ORDER BY c.id DESC
                LIMIT %s
                """,
                _excl_t + (limit,),
            )
        rows = cur.fetchall() or []
        rows.reverse()
        if username:
            rows = [r for r in rows if not _job_comm_sender_is_viewer(
                r.get('sender_user'), username)]
        unread_count = len(rows)

        return jsonify({"items": rows, "unread_count": unread_count})
    finally:
        cur.close()
        conn.close()


@internal.route('/job-comms/inbox-cleared', methods=['POST'])
@login_required
def job_comms_inbox_cleared():
    """Mark the dispatcher incident inbox as cleared for the current user (persists across refresh)."""
    if not _user_has_role("dispatcher", "admin", "superuser", "clinical_lead", "controller"):
        return jsonify({"error": "Unauthorised access"}), 403

    username = (getattr(current_user, 'username', None) or '').strip()
    if not username:
        return jsonify({"error": "User not identified"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        _ensure_dispatcher_inbox_cleared_table(cur)
        cur.execute("""
            INSERT INTO dispatcher_inbox_cleared (username, cleared_at)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE cleared_at = NOW()
        """, (username,))
        conn.commit()
        return jsonify({"success": True, "message": "Inbox cleared"})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/mode', methods=['GET', 'POST'])
@login_required
def dispatch_mode():
    """Get or update dispatch mode ('auto' or 'manual')."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_dispatch_settings_table(cur)
        if request.method == 'GET':
            return jsonify({'mode': _get_dispatch_mode(cur)})

        allowed_roles = [
            "dispatcher", "admin", "superuser", "clinical_lead", "controller",
            "call_taker", "calltaker",
        ]
        if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
            return jsonify({'error': 'Unauthorised'}), 403

        payload = request.get_json() or {}
        mode = str(payload.get('mode') or '').strip().lower()
        if mode not in ('auto', 'manual'):
            return jsonify({'error': 'Invalid mode'}), 400

        cur.execute("""
            INSERT INTO mdt_dispatch_settings (id, mode, updated_by)
            VALUES (1, %s, %s)
            ON DUPLICATE KEY UPDATE
                mode = VALUES(mode),
                updated_by = VALUES(updated_by),
                updated_at = CURRENT_TIMESTAMP
        """, (mode, getattr(current_user, 'username', 'unknown')))
        conn.commit()
        return jsonify({'mode': mode, 'message': 'Dispatch mode updated'})
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/motd', methods=['GET', 'POST'])
@login_required
def dispatch_motd():
    """Get or update dispatcher note / message of the day."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_dispatch_settings_table(cur)
        if request.method == 'GET':
            return jsonify(_get_dispatch_motd(cur))

        allowed_roles = ["dispatcher", "admin", "superuser", "clinical_lead"]
        if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
            return jsonify({'error': 'Unauthorised'}), 403

        payload = request.get_json() or {}
        text = str(payload.get('text') or '').strip()
        if len(text) > 4000:
            return jsonify({'error': 'Message too long (max 4000 chars)'}), 400

        cur.execute("""
            INSERT INTO mdt_dispatch_settings (id, mode, motd_text, motd_updated_by, motd_updated_at, updated_by)
            VALUES (1, 'auto', %s, %s, NOW(), %s)
            ON DUPLICATE KEY UPDATE
                motd_text = VALUES(motd_text),
                motd_updated_by = VALUES(motd_updated_by),
                motd_updated_at = NOW(),
                updated_by = VALUES(updated_by),
                updated_at = CURRENT_TIMESTAMP
        """, (
            text or None,
            getattr(current_user, 'username', 'unknown'),
            getattr(current_user, 'username', 'unknown')
        ))
        conn.commit()
        return jsonify({
            'message': 'Dispatch note updated',
            **_get_dispatch_motd(cur)
        })
    finally:
        cur.close()
        conn.close()


_BROADCAST_MESSAGE_PREFIX = 'General Broadcast'


@internal.route('/dispatch/messages/broadcast', methods=['GET', 'POST'])
@login_required
def dispatch_messages_broadcast():
    """CAD: fan-out a message to all crewed units in the current dispatch scope; GET = recent log."""
    allowed_roles = [
        "dispatcher", "admin", "superuser", "clinical_lead", "controller",
    ]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_ventus_dispatch_broadcast_table(cur)
        if request.method == 'GET':
            cur.execute("""
                SELECT id, created_at, sender_username, scope_label, body,
                       recipient_count, recipient_callsigns
                FROM ventus_dispatch_broadcast
                ORDER BY id DESC
                LIMIT 50
            """)
            items = []
            for row in (cur.fetchall() or []):
                items.append({
                    'id': row.get('id'),
                    'created_at': row.get('created_at'),
                    'sender_username': row.get('sender_username'),
                    'scope_label': row.get('scope_label'),
                    'body': row.get('body'),
                    'recipient_count': int(row.get('recipient_count') or 0),
                })
            return _jsonify_safe({'items': items}, 200)

        selected_division, include_external = _request_division_scope()
        data = request.get_json(silent=True) or {}
        if isinstance(data, dict):
            if data.get('division') is not None:
                raw_div = str(data.get('division') or '').strip()
                if raw_div:
                    low_div = raw_div.lower()
                    if low_div in ('__all__', 'all', '*'):
                        selected_division = _CAD_DIVISION_ALL
                        include_external = True
                    else:
                        selected_division = _normalize_division(
                            raw_div, fallback='')
            if data.get('include_external') is not None:
                include_external = str(data.get('include_external')).strip().lower() in (
                    '1', 'true', 'yes', 'on')
        # Broadcasts never include greyed / other-division units (ignore map “show other divisions”).
        include_external = False
        body_plain = str(data.get('text') or data.get('body') or '').strip()
        if not body_plain:
            return jsonify({'error': 'Message text required'}), 400
        if len(body_plain) > 2000:
            return jsonify({'error': 'Message too long (max 2000 chars)'}), 400

        callsigns, scope_label = _dispatch_broadcast_target_callsigns(
            cur, selected_division, include_external)
        if not callsigns:
            return jsonify({
                'error': 'No crewed units in the current CAD scope to message.',
            }), 409

        # Resolve each target the same way as GET /api/messages/<callsign> so rows match MDT polls.
        canon_targets = []
        seen_canon = set()
        for cs in callsigns:
            cs_raw = str(cs or '').strip().upper()
            if not cs_raw:
                continue
            c = _mdt_resolve_callsign(cur, cs_raw)
            if c and c not in seen_canon:
                seen_canon.add(c)
                canon_targets.append(c)
        if not canon_targets:
            return jsonify({
                'error': 'No crewed units in the current CAD scope to message.',
            }), 409

        fanout_text = f"{_BROADCAST_MESSAGE_PREFIX}\n\n{body_plain}"
        username = str(getattr(current_user, 'username', '') or '').strip()
        sender = _sender_label_from_portal('dispatcher', username)

        cur.execute("""
            INSERT INTO ventus_dispatch_broadcast
            (sender_username, scope_label, body, recipient_callsigns, recipient_count)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            username or 'unknown',
            scope_label[:512],
            body_plain,
            json.dumps(canon_targets),
            len(canon_targets),
        ))
        broadcast_row_id = cur.lastrowid

        inserted = 0
        for cs_canon in canon_targets:
            dup_id = _messages_recent_duplicate_id(
                cur, sender, cs_canon, fanout_text)
            if dup_id is not None:
                continue
            cur.execute("""
                INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                VALUES (%s, %s, %s, NOW(), 0)
            """, (sender, cs_canon, fanout_text))
            inserted += 1
            new_id = cur.lastrowid
            _bc_payload = {
                'type': 'message_posted',
                'from': str(sender or 'dispatcher'),
                'to': cs_canon,
                'text': fanout_text,
                'message_id': int(new_id) if new_id else None,
                'broadcast': True,
            }
            try:
                cs_room = re.sub(
                    r'[^A-Za-z0-9_-]', '', str(cs_canon or '').strip()).upper()[:64]
                if cs_room:
                    socketio.emit('mdt_event', _bc_payload,
                                  room=f'mdt_callsign_{cs_room}')
            except Exception:
                pass
            try:
                socketio.emit('mdt_event', _bc_payload)
            except Exception:
                pass
            try:
                _mdt_web_push_notify_callsign(
                    cs_canon,
                    _BROADCAST_MESSAGE_PREFIX,
                    (f"{_BROADCAST_MESSAGE_PREFIX}: {body_plain}")[:500],
                    tag='ventus-broadcast',
                    url='/dashboard',
                    alert=False,
                    silent=False,
                    require_interaction=False,
                )
            except Exception:
                pass

        conn.commit()
        try:
            log_audit(
                username or 'unknown',
                'dispatch_general_broadcast',
                details={
                    'broadcast_id': int(broadcast_row_id) if broadcast_row_id else None,
                    'recipient_count': len(canon_targets),
                    'scope': scope_label,
                },
            )
        except Exception:
            pass

        return _jsonify_safe({
            'message': 'Broadcast sent',
            'sent': inserted,
            'callsigns': canon_targets,
            'scope_label': scope_label,
            'broadcast_id': int(broadcast_row_id) if broadcast_row_id else None,
        }, 200)
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception('dispatch_messages_broadcast failed')
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/backup-quick-log-labels', methods=['GET', 'POST'])
@login_required
def dispatch_backup_quick_log_labels():
    """Names shown on dispatcher CAD quick-log backup buttons (Grade vs Priority vs Category, etc.)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_dispatch_settings_table(cur)
        if request.method == 'GET':
            eff, stored = _dispatch_backup_quick_log_effective_and_stored(cur)
            return jsonify({'labels': eff, 'stored': stored})

        allowed_roles = ["dispatcher", "admin", "superuser", "clinical_lead"]
        if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
            return jsonify({'error': 'Unauthorised'}), 403

        payload = request.get_json() or {}
        labels_in = payload.get('labels') if isinstance(
            payload.get('labels'), dict) else payload

        def _clean(v):
            s = str(v or '').strip()
            return s[:120] if s else None

        v1 = _clean(labels_in.get('backup_p1'))
        v2 = _clean(labels_in.get('backup_p2'))
        v3 = _clean(labels_in.get('backup_p3'))

        cur.execute(
            """
            INSERT INTO mdt_dispatch_settings (
                id, mode,
                dispatcher_backup_label_p1, dispatcher_backup_label_p2, dispatcher_backup_label_p3,
                updated_by
            )
            VALUES (1, 'auto', %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                dispatcher_backup_label_p1 = VALUES(dispatcher_backup_label_p1),
                dispatcher_backup_label_p2 = VALUES(dispatcher_backup_label_p2),
                dispatcher_backup_label_p3 = VALUES(dispatcher_backup_label_p3),
                updated_by = VALUES(updated_by),
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                v1,
                v2,
                v3,
                getattr(current_user, 'username', 'unknown'),
            ),
        )
        conn.commit()
        return jsonify({
            'message': 'Backup quick-log labels updated',
            'labels': _get_dispatcher_backup_quick_log_labels(cur),
        })
    finally:
        cur.close()
        conn.close()


def _cura_event_job_policy_payload(cur):
    env_on = _cura_event_job_filter_strict_env_active()
    db_on = _read_cura_event_job_filter_strict_from_database(cur)
    return {
        "cura_event_job_filter_strict": bool(env_on or db_on),
        "cura_event_job_filter_strict_database": db_on,
        "environment_override_active": env_on,
    }


@internal.route("/dispatch/cura-event-job-policy", methods=["GET", "POST"])
@login_required
def dispatch_cura_event_job_policy():
    """
    UI + API: enable/disable strict job filtering for active Cura operational-event divisions (Phase D).
    """
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_dispatch_settings_table(cur)
        if request.method == "GET":
            return jsonify(_cura_event_job_policy_payload(cur))

        allowed_roles = ["dispatcher", "admin", "superuser", "clinical_lead"]
        if not hasattr(current_user, "role") or current_user.role.lower() not in allowed_roles:
            return jsonify({"error": "Unauthorised"}), 403

        if _cura_event_job_filter_strict_env_active():
            return (
                jsonify(
                    {
                        "error": "Server environment variable VENTUS_CURA_EVENT_JOB_FILTER_STRICT is set; "
                        "remove it on the host to allow this control to apply.",
                        **_cura_event_job_policy_payload(cur),
                    }
                ),
                409,
            )

        payload = request.get_json() or {}
        if "cura_event_job_filter_strict" not in payload:
            return jsonify({"error": "Field cura_event_job_filter_strict (boolean) is required"}), 400
        want = bool(payload.get("cura_event_job_filter_strict"))
        val = 1 if want else 0
        uname = str(getattr(current_user, "username", "") or "unknown")[:120]

        cur.execute("SELECT id FROM mdt_dispatch_settings WHERE id = 1 LIMIT 1")
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO mdt_dispatch_settings (id, mode, default_division, cura_event_job_filter_strict, updated_by)
                VALUES (1, 'auto', 'general', %s, %s)
                """,
                (val, uname),
            )
        else:
            cur.execute(
                """
                UPDATE mdt_dispatch_settings
                SET cura_event_job_filter_strict = %s, updated_by = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
                """,
                (val, uname),
            )
        conn.commit()
        out = _cura_event_job_policy_payload(cur)
        out["message"] = "Cura event job filter policy saved."
        return jsonify(out)
    finally:
        cur.close()
        conn.close()


def _dispatch_break_policy_api_payload(cur):
    """JSON-serialisable break/meal defaults for admin API and CAD division bootstrap."""
    p = _get_dispatch_break_policy(cur)
    try:
        pct = float(p.get('break_due_shift_percent') or 50.0)
    except Exception:
        pct = 50.0
    pct = max(1.0, min(99.0, pct))
    return {
        'break_target_mode': str(p.get('break_target_mode') or 'fixed_minutes'),
        'break_due_fixed_minutes': int(p.get('break_due_fixed_minutes') or 240),
        'break_due_shift_percent': round(pct, 2),
        'near_break_warning_minutes': int(p.get('near_break_warning_minutes') or 30),
        'meal_break_default_minutes': int(p.get('meal_break_default_minutes') or 45),
    }


@internal.route('/dispatch/break-policy', methods=['GET', 'POST'])
@login_required
def dispatch_break_policy():
    """
    Admin: when to treat a meal break as due (fixed minutes from shift start vs % of planned shift),
    how many minutes before that to flag near-break, and default meal-break length when not specified.
    """
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_dispatch_settings_table(cur)
        if request.method == 'GET':
            return jsonify(_dispatch_break_policy_api_payload(cur))

        allowed_roles = ['dispatcher', 'admin', 'superuser', 'clinical_lead']
        if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
            return jsonify({'error': 'Unauthorised'}), 403

        payload = request.get_json() or {}
        base = _get_dispatch_break_policy(cur)

        mode = str(payload.get('break_target_mode') or base.get('break_target_mode') or 'fixed_minutes').strip().lower()
        if mode not in ('fixed_minutes', 'shift_percent'):
            return jsonify({'error': 'break_target_mode must be fixed_minutes or shift_percent'}), 400

        def _parse_int(key, lo, hi, default):
            if key not in payload:
                return int(default)
            try:
                v = int(payload.get(key))
            except Exception:
                raise ValueError(f'{key} must be an integer')
            if v < lo or v > hi:
                raise ValueError(f'{key} must be between {lo} and {hi}')
            return v

        def _parse_pct(key, default):
            if key not in payload:
                return round(float(default), 2)
            try:
                v = float(payload.get(key))
            except Exception:
                raise ValueError(f'{key} must be a number')
            if v < 1.0 or v > 99.0:
                raise ValueError(f'{key} must be between 1 and 99')
            return round(v, 2)

        try:
            fixed = _parse_int(
                'break_due_fixed_minutes', 15, 24 * 60, base.get('break_due_fixed_minutes'))
            near = _parse_int(
                'near_break_warning_minutes', 1, 240, base.get('near_break_warning_minutes'))
            meal = _parse_int(
                'meal_break_default_minutes', 5, 180, base.get('meal_break_default_minutes'))
            pct = _parse_pct('break_due_shift_percent', base.get('break_due_shift_percent') or 50.0)
        except ValueError as ve:
            return jsonify({'error': str(ve)}), 400

        uname = str(getattr(current_user, 'username', '') or 'unknown')[:120]

        cur.execute('SELECT id FROM mdt_dispatch_settings WHERE id = 1 LIMIT 1')
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO mdt_dispatch_settings (
                    id, mode, default_division,
                    break_target_mode, break_due_fixed_minutes, break_due_shift_percent,
                    near_break_warning_minutes, meal_break_default_minutes,
                    updated_by
                )
                VALUES (1, 'auto', 'general', %s, %s, %s, %s, %s, %s)
                """,
                (mode, fixed, pct, near, meal, uname),
            )
        else:
            cur.execute(
                """
                UPDATE mdt_dispatch_settings
                   SET break_target_mode = %s,
                       break_due_fixed_minutes = %s,
                       break_due_shift_percent = %s,
                       near_break_warning_minutes = %s,
                       meal_break_default_minutes = %s,
                       updated_by = %s,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = 1
                """,
                (mode, fixed, pct, near, meal, uname),
            )
        conn.commit()
        out = _dispatch_break_policy_api_payload(cur)
        out['message'] = 'Break policy saved.'
        return jsonify(out)
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/divisions', methods=['GET'])
@login_required
def dispatch_divisions():
    """List configured and observed operational divisions for dispatcher filtering."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        items = _list_dispatch_divisions(cur, include_inactive=False)
        _, _, access = _enforce_dispatch_scope(cur, '', False)
        out = {d['slug'] for d in items}
        try:
            cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
            has_job_div = cur.fetchone() is not None
            if has_job_div:
                cur.execute("""
                    SELECT DISTINCT LOWER(TRIM(COALESCE(division, 'general'))) AS division
                    FROM mdt_jobs
                    WHERE division IS NOT NULL AND TRIM(division) <> ''
                """)
                for row in (cur.fetchall() or []):
                    d = _normalize_division(row.get('division'), fallback='')
                    if d:
                        out.add(d)
        except Exception:
            pass
        try:
            cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
            has_unit_div = cur.fetchone() is not None
            if has_unit_div:
                cur.execute("""
                    SELECT DISTINCT LOWER(TRIM(COALESCE(division, 'general'))) AS division
                    FROM mdts_signed_on
                    WHERE division IS NOT NULL AND TRIM(division) <> ''
                """)
                for row in (cur.fetchall() or []):
                    d = _normalize_division(row.get('division'), fallback='')
                    if d:
                        out.add(d)
        except Exception:
            pass
        # Full catalog (ignore Cura event window) so slugs observed on units/jobs still get
        # colour/name/map_icon_url when the filtered list omits event-scoped divisions.
        catalog_by_slug_lower = {
            str((x or {}).get("slug") or "").strip().lower(): x
            for x in (_list_dispatch_divisions(cur, include_inactive=True, filter_event_window=False) or [])
            if str((x or {}).get("slug") or "").strip()
        }
        item_slugs_lower = {str((x or {}).get("slug") or "").strip().lower() for x in items}
        missing = [slug for slug in sorted(out) if slug not in item_slugs_lower]
        for slug in missing:
            from_cat = catalog_by_slug_lower.get(slug)
            if from_cat:
                _mi = from_cat.get("map_icon_url")
                _mi = str(_mi).strip() if _mi else None
                if _mi:
                    _mi = _sanitize_client_division_map_icon_url(_mi)
                items.append(
                    {
                        "slug": slug,
                        "name": str(from_cat.get("name") or slug).strip() or slug,
                        "color": _normalize_hex_color(from_cat.get("color"), "#64748b"),
                        "is_active": bool(from_cat.get("is_active", 1)),
                        "is_default": bool(from_cat.get("is_default", 0)),
                        "map_icon_url": _mi,
                    }
                )
            else:
                items.append(
                    {
                        "slug": slug,
                        "name": slug.replace("_", " ").title(),
                        "color": "#64748b",
                        "is_active": True,
                        "is_default": False,
                        "map_icon_url": None,
                    }
                )
        default_division = _get_dispatch_default_division(cur)
        if default_division not in {x['slug'] for x in items}:
            default_division = 'general'
        if access.get('restricted'):
            allowed = set(access.get('divisions') or [])
            items = [x for x in items if x.get('slug') in allowed]
            # Keep dispatcher-owned divisions visible even if not currently active/configured.
            missing_allowed = [slug for slug in sorted(allowed) if slug and slug not in {
                x.get('slug') for x in items}]
            for slug in missing_allowed:
                from_catalog = catalog_by_slug_lower.get(str(slug or "").strip().lower())
                if from_catalog:
                    _mi = from_catalog.get('map_icon_url')
                    _mi = str(_mi).strip() if _mi else None
                    if _mi:
                        _mi = _sanitize_client_division_map_icon_url(_mi)
                    items.append({
                        'slug': slug,
                        'name': str(from_catalog.get('name') or slug).strip() or slug,
                        'color': _normalize_hex_color(from_catalog.get('color'), '#64748b'),
                        'is_active': bool(from_catalog.get('is_active', 1)),
                        'is_default': bool(from_catalog.get('is_default', 0)),
                        'map_icon_url': _mi,
                    })
                else:
                    items.append({
                        'slug': slug,
                        'name': slug.replace('_', ' ').title(),
                        'color': '#64748b',
                        'is_active': True,
                        'is_default': False,
                        'map_icon_url': None,
                    })
            if not items:
                items = [{
                    'slug': 'general',
                    'name': 'General',
                    'color': '#64748b',
                    'is_active': True,
                    'is_default': True,
                    'map_icon_url': None,
                }]
                allowed = {'general'}
            if default_division not in allowed:
                default_division = items[0]['slug']
        for item in items:
            item['is_default'] = (item['slug'] == default_division)
        items.sort(key=lambda x: (
            0 if x['slug'] == default_division else 1, x['name'].lower(), x['slug']))
        policy = _cura_event_job_policy_payload(cur)
        backup_labels = _get_dispatcher_backup_quick_log_labels(cur)
        break_policy = _dispatch_break_policy_api_payload(cur)
        return jsonify(
            {
                "divisions": [x["slug"] for x in items],
                "items": items,
                "default": default_division,
                "access": access,
                "can_show_external": bool(
                    access.get("can_override_all",
                               False) or not access.get("restricted")
                ),
                "backup_quick_log_labels": backup_labels,
                "break_policy": break_policy,
                **policy,
            }
        )
    finally:
        cur.close()
        conn.close()


@internal.route('/unit/<callsign>/transfer-division', methods=['POST'])
@login_required
def transfer_unit_division(callsign):
    """Transfer a callsign into another division (cross-division support)."""
    allowed_roles = ["dispatcher", "admin", "superuser",
                     "clinical_lead", "controller"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    callsign = str(callsign or '').strip().upper()
    payload = request.get_json() or {}
    raw_tgt = str(payload.get('division') or '').strip().lower()
    if raw_tgt in ('__all__', 'all', '*'):
        return jsonify({'error': 'Select a specific division to transfer into'}), 400
    target_division = _normalize_division(payload.get('division'), fallback='')
    if not target_division:
        return jsonify({'error': 'division required'}), 400
    src_raw = str(payload.get('source') or '').strip().lower()
    xfer_reason = str(payload.get('reason') or '').strip()[:64] or 'manual_transfer'
    if src_raw == 'dispatch_unit_admin':
        xfer_reason = 'dispatch_admin_correction'

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        operator = str(getattr(current_user, 'username', '') or '').strip()
        _, _, access = _enforce_dispatch_scope(cur, target_division, False)
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_division = cur.fetchone() is not None
        if not has_division:
            return jsonify({'error': 'division column missing; run plugin upgrade'}), 409
        if access.get('restricted'):
            allowed = set(access.get('divisions') or [])
            if target_division not in allowed and not access.get('can_override_all'):
                return jsonify({'error': 'Target division not permitted'}), 403

        cur.execute(
            "SELECT callSign, division FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (callsign,))
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Unit not found'}), 404
        prev = _normalize_division(
            (row or {}).get('division'), fallback='general')
        if prev == target_division:
            return jsonify({
                'message': 'Unit already in requested division',
                'callsign': callsign,
                'from_division': prev,
                'to_division': target_division,
                'instruction_id': None
            }), 200
        cur.execute("""
            UPDATE mdts_signed_on
            SET division = %s
            WHERE callSign = %s
        """, (target_division, callsign))
        instruction_id = _queue_division_transfer_instruction(
            cur,
            callsign,
            prev,
            target_division,
            actor=operator or None,
            reason=xfer_reason,
        )
        _insert_unit_dispatch_adjustment_response_log(
            cur,
            callsign,
            _RESPONSE_LOG_STATUS_DISPATCH_DIVISION_CHANGED,
            {
                'dispatch_note': True,
                'from_division': prev,
                'to_division': target_division,
                'by': operator,
                'reason': xfer_reason,
            },
        )
        conn.commit()

        uname = str(getattr(current_user, 'username', '') or 'unknown').strip() or 'unknown'
        try:
            log_audit(
                uname,
                'dispatch_unit_division_changed',
                details={
                    'callsign': callsign,
                    'from_division': prev,
                    'to_division': target_division,
                    'reason': xfer_reason,
                },
            )
        except Exception:
            pass

        sender_name = _sender_label_from_portal(
            'dispatch', getattr(current_user, 'username', ''))
        notify_text = (
            f"DISPATCH: Your operational division has been updated to {target_division}. "
            f"(Previously: {prev}.)"
        )
        message_inserted = False
        try:
            cur.execute("""
                INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                VALUES (%s, %s, %s, NOW(), 0)
            """, (sender_name, callsign, notify_text))
            conn.commit()
            message_inserted = True
        except Exception as ex:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                logger.warning(
                    'division transfer: inbox message insert failed: %s', ex)
            except Exception:
                pass

        try:
            socketio.emit('mdt_event', {
                'type': 'unit_division_transferred',
                'callsign': callsign,
                'from_division': prev,
                'to_division': target_division
            })
            if instruction_id:
                socketio.emit('mdt_event', {
                    'type': 'dispatch_instruction',
                    'callsign': callsign,
                    'instruction_id': instruction_id,
                    'instruction_type': 'division_transfer'
                })
            if message_inserted:
                socketio.emit('mdt_event', {
                    'type': 'message_posted',
                    'from': str(sender_name or 'dispatcher'),
                    'to': callsign,
                    'text': notify_text,
                })
                _mdt_web_push_notify_callsign(
                    callsign,
                    'Division updated',
                    notify_text,
                    tag='division-change',
                    alert=False,
                    silent=False,
                    require_interaction=False,
                )
            socketio.emit(
                'mdt_event', {'type': 'units_updated', 'callsign': callsign})
        except Exception:
            pass
        return jsonify({
            'message': 'Unit division updated',
            'callsign': callsign,
            'from_division': prev,
            'to_division': target_division,
            'instruction_id': instruction_id
        })
    finally:
        cur.close()
        conn.close()


@internal.route('/unit/<callsign>/detail', methods=['GET'])
@login_required
def unit_detail(callsign):
    allowed_roles = ["dispatcher", "admin", "superuser", "clinical_lead",
                     "controller", "crew", "call_taker", "call_handler"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    callsign = str(callsign or '').strip().upper()
    if not callsign:
        return jsonify({'error': 'callsign required'}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_response_log_table(cur)
        _ensure_meal_break_columns(cur)
        _ventus_set_g_break_policy(cur)
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_division = cur.fetchone() is not None
        div_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_division else "'general' AS division"
        cur.execute(f"""
            SELECT callSign, ipAddress, status, assignedIncident, signOnTime,
                   crew, lastLat, lastLon, lastSeenAt, updatedAt, mealBreakStartedAt, mealBreakUntil,
                   mealBreakTakenAt, shiftStartAt, shiftEndAt, shiftDurationMins, breakDueAfterMins, {div_sql}
            FROM mdts_signed_on
            WHERE callSign = %s
            LIMIT 1
        """, (callsign,))
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Unit not found'}), 404

        raw_crew_list = []
        try:
            raw_crew = row.get('crew')
            if isinstance(raw_crew, str):
                raw_crew_list = json.loads(raw_crew) if raw_crew else []
            elif isinstance(raw_crew, list):
                raw_crew_list = raw_crew
        except Exception:
            raw_crew_list = []
        crew = _normalize_crew_to_objects(raw_crew_list, row.get('signOnTime'))
        crew = _enrich_crew_with_profiles(cur, crew)

        current_job = None
        cad = row.get('assignedIncident')
        if cad:
            cur.execute("""
                SELECT cad, status, data, created_at, updated_at
                FROM mdt_jobs
                WHERE cad = %s
                LIMIT 1
            """, (cad,))
            j = cur.fetchone()
            if j:
                reason = ''
                try:
                    payload = j.get('data')
                    if isinstance(payload, (bytes, bytearray)):
                        payload = payload.decode('utf-8', errors='ignore')
                    parsed = json.loads(payload) if isinstance(
                        payload, str) and payload else {}
                    if isinstance(parsed, dict):
                        reason = str(parsed.get(
                            'reason_for_call') or '').strip()
                except Exception:
                    pass
                current_job = {
                    'cad': j.get('cad'),
                    'status': j.get('status'),
                    'reason_for_call': reason,
                    'created_at': j.get('created_at'),
                    'updated_at': j.get('updated_at')
                }

        cur.execute("""
            SELECT COUNT(DISTINCT cad) AS total
            FROM mdt_response_log
            WHERE callSign = %s
              AND event_time >= COALESCE(%s, DATE_SUB(NOW(), INTERVAL 7 DAY))
        """, (callsign, row.get('signOnTime')))
        total_row = cur.fetchone() or {}
        jobs_since_sign_on = int(total_row.get('total') or 0)

        cur.execute("""
            SELECT l.cad, MAX(l.event_time) AS last_event, j.status
            FROM mdt_response_log l
            LEFT JOIN mdt_jobs j ON j.cad = l.cad
            WHERE l.callSign = %s
              AND l.event_time >= COALESCE(%s, DATE_SUB(NOW(), INTERVAL 7 DAY))
            GROUP BY l.cad, j.status
            ORDER BY last_event DESC
            LIMIT 20
        """, (callsign, row.get('signOnTime')))
        recent_jobs = cur.fetchall() or []

        ping_seconds = None
        try:
            cur.execute(
                "SELECT TIMESTAMPDIFF(SECOND, COALESCE(lastSeenAt, signOnTime), NOW()) AS ping_seconds FROM mdts_signed_on WHERE callSign=%s", (callsign,))
            p = cur.fetchone() or {}
            ping_seconds = int(p.get('ping_seconds')) if p.get(
                'ping_seconds') is not None else None
        except Exception:
            ping_seconds = None

        crew_removals = []
        try:
            _ensure_crew_removal_log_table(cur)
            cur.execute("""
                SELECT removed_username, removal_reason, removed_by, removed_at
                FROM mdt_crew_removal_log
                WHERE callSign = %s
                  AND removed_at >= COALESCE(%s, DATE_SUB(NOW(), INTERVAL 7 DAY))
                ORDER BY removed_at DESC
                LIMIT 40
            """, (callsign, row.get('signOnTime')))
            crew_removals = cur.fetchall() or []
        except Exception:
            crew_removals = []

        standby = None
        try:
            _ensure_standby_tables(cur)
            cur.execute("""
                SELECT name, lat, lng, source, destinationType, activationMode, state, cad,
                       what3words, address, instructionId, activatedAt, updatedAt
                FROM standby_locations
                WHERE callSign = %s
                ORDER BY updatedAt DESC, id DESC
                LIMIT 1
            """, (callsign,))
            s = cur.fetchone() or None
            if s:
                standby = {
                    'name': s.get('name'),
                    'lat': float(s.get('lat')) if s.get('lat') is not None else None,
                    'lng': float(s.get('lng')) if s.get('lng') is not None else None,
                    'source': s.get('source'),
                    'destination_type': s.get('destinationType') or 'standby',
                    'activation_mode': s.get('activationMode') or 'immediate',
                    'state': s.get('state') or 'active',
                    'cad': s.get('cad'),
                    'what3words': s.get('what3words'),
                    'address': s.get('address'),
                    'instruction_id': s.get('instructionId'),
                    'activated_at': s.get('activatedAt'),
                    'updated_at': s.get('updatedAt')
                }
        except Exception:
            standby = None

        shift_state = _compute_shift_break_state(row)
        shift_start_at = shift_state.get('shift_start_at')
        shift_end_at = shift_state.get('shift_end_at')
        shift_duration_minutes = shift_state.get('shift_duration_minutes')
        shift_hours = (float(shift_duration_minutes) /
                       60.0) if shift_duration_minutes is not None else None

        manage_crew = False
        crew_grade_opts = []
        try:
            manage_crew = _cad_user_can_manage_signed_on_crew()
            if manage_crew:
                crew_grade_opts = _list_crew_grades(cur)
        except Exception:
            crew_grade_opts = []

        return _jsonify_safe({
            'callsign': callsign,
            'status': row.get('status'),
            'division': _normalize_division(row.get('division'), fallback='general'),
            'ip_address': row.get('ipAddress'),
            'last_seen_at': row.get('lastSeenAt'),
            'last_ping_seconds': ping_seconds,
            'last_lat': float(row.get('lastLat')) if row.get('lastLat') is not None else None,
            'last_lng': float(row.get('lastLon')) if row.get('lastLon') is not None else None,
            'sign_on_time': row.get('signOnTime'),
            'updated_at': row.get('updatedAt'),
            'crew': crew,
            'meal_break_started_at': row.get('mealBreakStartedAt'),
            'meal_break_until': row.get('mealBreakUntil'),
            'meal_break_taken_at': row.get('mealBreakTakenAt'),
            'meal_break_remaining_seconds': shift_state.get('meal_break_remaining_seconds'),
            'meal_break_active': shift_state.get('meal_break_active'),
            'shift_start': shift_start_at.isoformat() + 'Z' if shift_start_at and hasattr(shift_start_at, 'isoformat') else None,
            'shift_end': shift_end_at.isoformat() + 'Z' if shift_end_at and hasattr(shift_end_at, 'isoformat') else None,
            'shift_hours': shift_hours,
            'shift_duration_minutes': shift_duration_minutes,
            'break_due_after_minutes': shift_state.get('break_due_after_minutes'),
            'shift_start_at': shift_start_at,
            'shift_end_at': shift_end_at,
            'shift_elapsed_minutes': shift_state.get('shift_elapsed_minutes'),
            'shift_remaining_minutes': shift_state.get('shift_remaining_minutes'),
            'break_due_in_minutes': shift_state.get('break_due_in_minutes'),
            'break_due': shift_state.get('break_due'),
            'near_break': shift_state.get('near_break'),
            'break_blocked_for_new_jobs': shift_state.get('break_blocked_for_new_jobs'),
            'crew_removals': crew_removals,
            'standby': standby,
            'current_job': current_job,
            'jobs_since_sign_on': jobs_since_sign_on,
            'recent_jobs': recent_jobs,
            'cad_crew_manage_allowed': manage_crew,
            'crew_grade_options': crew_grade_opts,
        }, 200)
    finally:
        cur.close()
        conn.close()


def _unit_system_log_crew_removal_reason_label(reason_key):
    r = str(reason_key or "").strip().lower()
    labels = {
        "different_unit": "Moved to different unit / callsign",
        "crew_swap": "Swapped with another crew member",
        "reassigned": "Reassigned (other)",
        "early_finish": "Early finish / off duty",
        "illness": "Illness",
        "personal": "Personal",
        "other": "Other",
    }
    return labels.get(r, (r.replace("_", " ").title() if r else "Other"))


@internal.route('/unit/<callsign>/system-log', methods=['GET'])
@login_required
def unit_system_log(callsign):
    """Timeline for one signed-on unit: response log, CAD assignments, crew removals, sign-on."""
    allowed_roles = ["dispatcher", "admin", "superuser", "clinical_lead",
                     "controller", "crew", "call_taker", "call_handler"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    callsign = str(callsign or '').strip().upper()
    if not callsign:
        return jsonify({'error': 'callsign required'}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, callsign)
        cur.execute(
            "SELECT callSign, signOnTime FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
            (cs,),
        )
        row0 = cur.fetchone()
        if not row0:
            return jsonify({'error': 'Unit not found'}), 404

        sign_on_time = row0.get('signOnTime')
        events = []

        if sign_on_time:
            events.append({
                "source": "mdt",
                "type": "sign_on",
                "time": sign_on_time,
                "actor": cs,
                "message": f"{cs} signed on",
            })

        _ensure_job_units_table(cur)
        cur.execute("""
            SELECT job_cad AS cad, assigned_by, assigned_at
            FROM mdt_job_units
            WHERE UPPER(TRIM(callsign)) = UPPER(TRIM(%s))
            ORDER BY assigned_at ASC
        """, (cs,))
        for row in (cur.fetchall() or []):
            cad_v = row.get("cad")
            try:
                cad_int = int(cad_v) if cad_v is not None else 0
            except (TypeError, ValueError):
                cad_int = 0
            by = str(row.get("assigned_by") or "").strip() or "system"
            events.append({
                "source": "dispatch",
                "type": "unit_assigned",
                "time": row.get("assigned_at"),
                "actor": by,
                "message": (
                    f"{cs} assigned to CAD #{cad_int}"
                    if cad_int > 0
                    else f"{cs} assigned to CAD"
                ),
            })

        _ensure_response_log_table(cur)
        cur.execute("""
            SELECT cad, status, event_time, crew
            FROM mdt_response_log
            WHERE UPPER(TRIM(callSign)) = UPPER(TRIM(%s))
              AND event_time >= COALESCE(%s, DATE_SUB(NOW(), INTERVAL 90 DAY))
            ORDER BY event_time ASC
            LIMIT 800
        """, (cs, sign_on_time))
        for row in (cur.fetchall() or []):
            cad_v = row.get("cad")
            try:
                cad_int = int(cad_v) if cad_v is not None else 0
            except (TypeError, ValueError):
                cad_int = 0
            st_raw = str(row.get("status") or "").strip().lower()
            if st_raw == _RESPONSE_LOG_STATUS_CLOSURE_REVIEW:
                events.append({
                    "source": "system",
                    "type": "closure_review",
                    "time": row.get("event_time"),
                    "actor": cs,
                    "message": (
                        f"{cs} · CAD #{cad_int} entered closure review (job-level)"
                        if cad_int > 0
                        else f"{cs} · closure review (job-level)"
                    ),
                })
                continue
            if st_raw == _RESPONSE_LOG_STATUS_DISPATCH_REOPENED:
                extra = ""
                try:
                    cr = row.get("crew")
                    if isinstance(cr, (bytes, bytearray)):
                        cr = cr.decode("utf-8", errors="ignore")
                    if isinstance(cr, str) and cr.strip().startswith("{"):
                        d = json.loads(cr)
                        if isinstance(d, dict) and str(d.get("reason") or "").strip():
                            extra = " — " + str(d.get("reason")).strip()
                except Exception:
                    pass
                events.append({
                    "source": "dispatch",
                    "type": "dispatch_reopened",
                    "time": row.get("event_time"),
                    "actor": cs,
                    "message": (
                        f"{cs} · CAD #{cad_int} reopened onto the stack by dispatch{extra}"
                        if cad_int > 0
                        else f"{cs} · CAD reopened onto the stack by dispatch{extra}"
                    ),
                })
                continue
            if st_raw == _RESPONSE_LOG_STATUS_DISPATCH_DIVISION_CHANGED:
                fdiv, tdiv, by_disp = '', '', ''
                try:
                    cr = row.get("crew")
                    if isinstance(cr, (bytes, bytearray)):
                        cr = cr.decode("utf-8", errors="ignore")
                    if isinstance(cr, str) and cr.strip().startswith("{"):
                        d = json.loads(cr)
                        if isinstance(d, dict):
                            fdiv = str(d.get("from_division") or "").strip()
                            tdiv = str(d.get("to_division") or "").strip()
                            by_disp = str(d.get("by") or "").strip()
                except Exception:
                    pass
                by_part = f" (by {by_disp})" if by_disp else ""
                events.append({
                    "source": "dispatch",
                    "type": "division_changed",
                    "time": row.get("event_time"),
                    "actor": by_disp or cs,
                    "message": (
                        f"{cs} division updated: {fdiv or '?'} → {tdiv or '?'}{by_part}"
                    ),
                })
                continue
            if st_raw == _RESPONSE_LOG_STATUS_DISPATCH_SHIFT_TIMES_CHANGED:
                ns_txt, ne_txt, by_disp = "", "", ""
                try:
                    cr = row.get("crew")
                    if isinstance(cr, (bytes, bytearray)):
                        cr = cr.decode("utf-8", errors="ignore")
                    if isinstance(cr, str) and cr.strip().startswith("{"):
                        d = json.loads(cr)
                        if isinstance(d, dict):
                            by_disp = str(d.get("by") or "").strip()
                            ns_txt = _job_system_log_created_ts_text(
                                d.get("new_shift_start"))
                            ne_txt = _job_system_log_created_ts_text(
                                d.get("new_shift_end"))
                except Exception:
                    pass
                by_part = f" (by {by_disp})" if by_disp else ""
                span_msg = (
                    f"{ns_txt} – {ne_txt}" if ns_txt and ne_txt else "updated"
                )
                events.append({
                    "source": "dispatch",
                    "type": "shift_times_changed",
                    "time": row.get("event_time"),
                    "actor": by_disp or cs,
                    "message": f"{cs} shift times {span_msg}{by_part}",
                })
                continue
            st = st_raw.replace("_", " ")
            label = " ".join([x.capitalize() for x in st.split()]) if st else "Update"
            events.append({
                "source": "mdt",
                "type": "status_update",
                "time": row.get("event_time"),
                "actor": cs,
                "message": (
                    f"{cs} status {label} · CAD #{cad_int}"
                    if cad_int > 0
                    else f"{cs} status {label}"
                ),
            })

        _ensure_crew_removal_log_table(cur)
        cur.execute("""
            SELECT removed_username, removal_reason, removed_by, removed_at
            FROM mdt_crew_removal_log
            WHERE UPPER(TRIM(callSign)) = UPPER(TRIM(%s))
              AND removed_at >= COALESCE(%s, DATE_SUB(NOW(), INTERVAL 90 DAY))
            ORDER BY removed_at ASC
        """, (cs, sign_on_time))
        for row in (cur.fetchall() or []):
            un = str(row.get("removed_username") or "").strip() or "unknown"
            rsn = _unit_system_log_crew_removal_reason_label(row.get("removal_reason"))
            by = str(row.get("removed_by") or "").strip() or "unknown"
            events.append({
                "source": "dispatch",
                "type": "crew_removal",
                "time": row.get("removed_at"),
                "actor": by,
                "message": f"Crew removed: {un} ({rsn})",
            })

        events.sort(key=lambda x: x.get("time") or datetime.min)
        return _jsonify_safe({"callsign": cs, "events": events})
    except Exception:
        logger.exception("unit_system_log failed")
        return jsonify({"error": "Unable to load system log"}), 500
    finally:
        cur.close()
        conn.close()


@internal.route('/unit/<callsign>/crew/search', methods=['GET'])
@login_required
def unit_crew_search(callsign):
    """CAD: typeahead for adding crew — users + active contractors with photo/grade hints."""
    if not _cad_user_can_manage_signed_on_crew():
        return jsonify({'error': 'Unauthorised'}), 403
    q = (request.args.get('q') or '').strip()
    if len(q) < 2:
        return jsonify({'results': []}), 200
    if len(q) > 64 or not re.match(r'^[\w.@+\-\s]+$', q, re.I):
        return jsonify({'results': [], 'error': 'invalid query'}), 400
    pat = f'%{q}%'
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    seen = set()
    ordered = []
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, str(callsign or '').strip().upper())
        cur.execute(
            "SELECT callSign FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (cs,))
        if not cur.fetchone():
            return jsonify({'error': 'Unit not found'}), 404
        cur.execute("SHOW TABLES LIKE 'users'")
        if cur.fetchone():
            cur.execute(
                """
                SELECT username
                  FROM users
                 WHERE (username LIKE %s OR LOWER(email) LIKE LOWER(%s)
                        OR TRIM(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''))) LIKE %s)
                 ORDER BY username ASC
                 LIMIT 15
                """,
                (pat, pat, pat),
            )
            for row in cur.fetchall() or []:
                u = str(row.get('username') or '').strip()
                if not u or u.lower() in seen:
                    continue
                seen.add(u.lower())
                ordered.append(u)
        cur.execute("SHOW TABLES LIKE 'tb_contractors'")
        if cur.fetchone():
            active = (
                "(status IS NULL OR LOWER(TRIM(COALESCE(status,''))) IN "
                "('active','1','true','yes'))"
            )
            cur.execute(
                """
                SELECT username
                  FROM tb_contractors
                 WHERE """
                + active
                + """
                   AND username IS NOT NULL AND TRIM(username) <> ''
                   AND username LIKE %s
                 ORDER BY username ASC
                 LIMIT 10
                """,
                (pat,),
            )
            for row in cur.fetchall() or []:
                u = str(row.get('username') or '').strip()
                if not u or u.lower() in seen:
                    continue
                seen.add(u.lower())
                ordered.append(u)
        out = []
        for u in ordered[:20]:
            prev = _crew_dispatch_search_preview(cur, u)
            if prev:
                out.append(prev)
        return jsonify({'results': out}), 200
    except Exception as e:
        logger.exception('unit_crew_search')
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/unit/<callsign>/crew/add', methods=['POST'])
@login_required
def unit_crew_add(callsign):
    """CAD: append one crew member to mdts_signed_on.crew (same rules as MDT add)."""
    if not _cad_user_can_manage_signed_on_crew():
        return jsonify({'error': 'Unauthorised'}), 403
    payload = request.get_json(silent=True) or {}
    add_username = str(payload.get('username') or '').strip()
    if not add_username:
        return jsonify({'error': 'username required'}), 400
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, str(callsign or '').strip().upper())
        cur.execute(
            "SELECT crew, signOnTime FROM mdts_signed_on WHERE callSign = %s ORDER BY signOnTime DESC LIMIT 1",
            (cs,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Unit not signed on'}), 404
        if not _is_valid_crew_username(cur, add_username):
            return jsonify({
                'error': 'Crew member not found. Must be a core user or active contractor.',
                'username': add_username,
            }), 400
        raw_list = []
        try:
            r = row.get('crew')
            if isinstance(r, str):
                raw_list = json.loads(r) if r else []
            elif isinstance(r, list):
                raw_list = r
        except Exception:
            raw_list = []
        crew_objs = _normalize_crew_to_objects(raw_list, row.get('signOnTime'))
        if any(str(c.get('username') or '').strip().lower() == add_username.lower() for c in crew_objs):
            crew_enriched = _enrich_crew_with_profiles(cur, crew_objs)
            return _jsonify_safe({'success': True, 'crew': crew_enriched}, 200)
        now = datetime.utcnow()
        signed_on_iso = now.isoformat() + 'Z'
        add_grade = str(payload.get('grade') or payload.get('role') or '').strip() or None
        if not add_grade:
            preview_row = _crew_dispatch_search_preview(cur, add_username)
            if preview_row and preview_row.get("suggested_grade"):
                add_grade = str(preview_row["suggested_grade"]).strip() or None
        crew_objs.append({
            'username': add_username,
            'signedOnAt': signed_on_iso,
            'grade': add_grade,
        })
        cur.execute(
            "UPDATE mdts_signed_on SET crew = %s WHERE callSign = %s",
            (json.dumps(crew_objs), cs),
        )
        conn.commit()
        log_audit(
            _mdt_audit_actor(),
            'cad_crew_add',
            details={'callsign': cs, 'username': add_username},
        )
        _cad_emit_units_updated_socket(cs)
        crew_enriched = _enrich_crew_with_profiles(cur, crew_objs)
        return _jsonify_safe({'success': True, 'crew': crew_enriched}, 200)
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/unit/<callsign>/crew/remove', methods=['POST'])
@login_required
def unit_crew_remove(callsign):
    """CAD: remove crew member with reason; logs removed_by as dispatcher username."""
    if not _cad_user_can_manage_signed_on_crew():
        return jsonify({'error': 'Unauthorised'}), 403
    payload = request.get_json(silent=True) or {}
    uname = str(payload.get('username') or '').strip()
    if not uname:
        return jsonify({'error': 'username required'}), 400
    reason = _cad_normalize_crew_removal_reason(payload.get('reason'))
    notes = str(payload.get('notes') or '').strip()[:512]
    actor = str(_mdt_audit_actor() or 'unknown').strip()[:64] or 'unknown'
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, str(callsign or '').strip().upper())
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_div_rm = cur.fetchone() is not None
        div_sel = (
            "LOWER(TRIM(COALESCE(division, 'general'))) AS division"
            if has_div_rm else "'general' AS division")
        cur.execute(
            f"SELECT crew, signOnTime, {div_sel} FROM mdts_signed_on WHERE callSign = %s ORDER BY signOnTime DESC LIMIT 1",
            (cs,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Unit not signed on'}), 404
        raw_list = []
        try:
            r = row.get('crew')
            if isinstance(r, str):
                raw_list = json.loads(r) if r else []
            elif isinstance(r, list):
                raw_list = r
        except Exception:
            raw_list = []
        crew_objs = _dedupe_crew_objects_preserve_order(
            _normalize_crew_to_objects(raw_list, row.get('signOnTime')))
        crew_display_before = [
            str(c.get('username') or '').strip()
            for c in crew_objs if str(c.get('username') or '').strip()]
        div_row = str(row.get('division') or 'general').strip().lower() or 'general'
        before_len = len(crew_objs)
        sole_crew_unit = before_len == 1
        un_lc = uname.strip().lower()
        crew_objs = [
            c for c in crew_objs
            if _mdt_crew_raw_entry_username_lc(c) != un_lc
        ]
        if len(crew_objs) == before_len:
            crew_enriched = _enrich_crew_with_profiles(cur, crew_objs)
            return _jsonify_safe({'success': True, 'crew': crew_enriched}, 200)
        _ensure_crew_removal_log_table(cur)
        has_notes = False
        try:
            cur.execute("SHOW COLUMNS FROM mdt_crew_removal_log LIKE 'notes'")
            has_notes = cur.fetchone() is not None
        except Exception:
            pass
        if has_notes:
            cur.execute("""
                INSERT INTO mdt_crew_removal_log
                    (callSign, removed_username, removal_reason, removed_by, removed_at, notes)
                VALUES (%s, %s, %s, %s, NOW(), %s)
            """, (cs, uname, reason, actor, notes or None))
        else:
            cur.execute("""
                INSERT INTO mdt_crew_removal_log
                    (callSign, removed_username, removal_reason, removed_by, removed_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, (cs, uname, reason, actor))
        _ensure_mdt_user_mdt_session_table(cur)
        _ensure_job_units_table(cur)
        affected_cads = set()
        cs_sess = str(cs or '').strip().upper()
        if not crew_objs:
            # Last crew member removed: full unit sign-off in CAD/SQL, but only revoke *this* user's
            # MDT device session — there are no other crew on the unit to invalidate.
            affected_cads, _ = _mdt_sign_off_unit_sql(cur, cs)
            _record_mdt_session_lifecycle_event(
                cur,
                event_type='sign_off',
                callsign=cs,
                division=div_row,
                actor_username=str(actor or '').strip() or None,
                previous_callsign=None,
                crew_usernames=crew_display_before,
                detail={
                    'affected_cads': sorted(affected_cads),
                    'trigger': 'cad_last_crew_removed',
                    'removed_username': uname,
                    'removal_reason': reason,
                },
            )
            try:
                cur.execute(
                    """
                    DELETE FROM mdt_user_mdt_session
                    WHERE LOWER(TRIM(username)) = %s AND UPPER(TRIM(callsign)) = %s
                    """,
                    (un_lc, cs_sess),
                )
            except Exception:
                pass
            try:
                cur.execute(
                    "DELETE FROM mdt_callsign_redirect WHERE from_callsign = %s OR to_callsign = %s",
                    (cs, cs),
                )
            except Exception:
                pass
        else:
            # Other crew remain: only end the removed person's MDT session; leave other members signed on.
            try:
                cur.execute(
                    """
                    DELETE FROM mdt_user_mdt_session
                    WHERE LOWER(TRIM(username)) = %s AND UPPER(TRIM(callsign)) = %s
                    """,
                    (un_lc, cs_sess),
                )
            except Exception:
                pass
            cur.execute(
                "UPDATE mdts_signed_on SET crew = %s, lastSeenAt = NOW() WHERE callSign = %s",
                (json.dumps(crew_objs), cs),
            )
        conn.commit()
        log_audit(
            _mdt_audit_actor(),
            'cad_crew_remove',
            details={
                'callsign': cs,
                'removed_username': uname,
                'reason': reason,
                'unit_signed_off': not bool(crew_objs),
                'sole_crew_before_remove': sole_crew_unit,
            },
        )
        if not crew_objs:
            try:
                socketio.emit(
                    'mdt_event', {'type': 'unit_signoff', 'callsign': cs})
                socketio.emit(
                    'mdt_event', {'type': 'units_updated', 'callsign': cs})
                for cad in sorted(affected_cads):
                    socketio.emit(
                        'mdt_event', {'type': 'jobs_updated', 'cad': cad})
            except Exception:
                pass
            try:
                from app.plugins.time_billing_module.ventus_integration import on_ventus_sign_off
                on_ventus_sign_off(callsign=cs)
            except Exception:
                pass
        else:
            _cad_emit_units_updated_socket(cs)
        crew_enriched = _enrich_crew_with_profiles(cur, crew_objs)
        return _jsonify_safe({'success': True, 'crew': crew_enriched}, 200)
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/standby-locations', methods=['GET'])
@login_required
def standby_locations_list():
    allowed_roles = ["dispatcher", "admin", "superuser", "clinical_lead",
                     "controller", "crew", "call_taker", "call_handler"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_standby_tables(cur)
        _upgrade_mdt_standby_presets_division(cur)
        conn.commit()
        access = _get_dispatch_user_division_access(cur)
        raw_div = str(request.args.get('division') or '').strip()
        unrestricted = (not access.get('restricted')) or bool(
            access.get('can_override_all'))
        allowed = [str(d).strip().lower() for d in (
            access.get('divisions') or []) if str(d).strip()]

        where = ["is_active = 1"]
        args = []
        rd = raw_div.lower()
        if rd in ('', 'all', '*', str(_CAD_DIVISION_ALL).lower()):
            if not unrestricted:
                if not allowed:
                    where.append("LOWER(TRIM(division)) = %s")
                    args.append('general')
                else:
                    ph = ','.join(['%s'] * len(allowed))
                    where.append(f"LOWER(TRIM(division)) IN ({ph})")
                    args.extend(allowed)
        else:
            slug = _normalize_division(raw_div, fallback='')
            if not slug:
                return _jsonify_safe([], 200)
            if not unrestricted:
                if allowed and slug.lower() not in allowed:
                    return _jsonify_safe([], 200)
            where.append("LOWER(TRIM(division)) = LOWER(TRIM(%s))")
            args.append(slug)

        sql = f"""
            SELECT id, name, lat, lng, what3words, division
            FROM mdt_standby_presets
            WHERE {' AND '.join(where)}
            ORDER BY name ASC
        """
        cur.execute(sql, tuple(args))
        rows = cur.fetchall() or []
        for r in rows:
            if r.get('division') is not None:
                r['division'] = _normalize_division(
                    r.get('division'), fallback='general')
        return _jsonify_safe(rows, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/map/w3w-at', methods=['GET'])
@login_required
def cad_map_w3w_at():
    """Lat/lng → what3words (CAD map HUD / click). Proxies what3words convert-to-3wa."""
    if not _ventus_dispatch_capable():
        return jsonify({'error': 'Unauthorised'}), 403
    try:
        lat = float(request.args.get('lat', request.args.get('latitude', '')))
        lng = float(request.args.get('lng', request.args.get('longitude', '')))
    except (TypeError, ValueError):
        return jsonify({'error': 'lat and lng must be numbers'}), 400
    result = ResponseTriage.get_w3w_words_from_coordinates(lat, lng)
    if result.get('error'):
        return jsonify({'error': result['error']}), 400
    result['latitude'] = round(lat, 6)
    result['longitude'] = round(lng, 6)
    east, north = ResponseTriage.wgs84_to_bng_easting_northing(lat, lng)
    if east is not None and north is not None:
        result['easting'] = east
        result['northing'] = north
    return _jsonify_safe(result, 200)


@internal.route('/map/wgs84-to-bng', methods=['GET'])
@login_required
def cad_map_wgs84_to_bng():
    """Decimal latitude/longitude → easting and northing (metres, EPSG:27700)."""
    if not _ventus_dispatch_capable():
        return jsonify({'error': 'Unauthorised'}), 403
    try:
        lat = float(request.args.get('lat', request.args.get('latitude', '')))
        lng = float(request.args.get('lng', request.args.get('longitude', '')))
    except (TypeError, ValueError):
        return jsonify({'error': 'lat and lng must be numbers'}), 400
    east, north = ResponseTriage.wgs84_to_bng_easting_northing(lat, lng)
    if east is None or north is None:
        return jsonify({'error': 'Easting/northing conversion unavailable'}), 503
    return _jsonify_safe({'easting': east, 'northing': north}, 200)


# Embedded Weather DataHub map-images API key (JWT tier bundled for OEM / offline-capable
# installs). Override with ``CAD_METOFFICE_API_KEY`` when you issue a tenant-specific key.
_CAD_METOFFICE_EMBEDDED_MAP_IMAGES_API_KEY = (
    "eyJ4NXQjUzI1NiI6Ik5XVTVZakUxTkRjeVl6a3hZbUl4TkdSaFpqSmpOV1l6T1dGaE9XWXpNMk0yTWpRek5USm1OVEE0TXpOaU9EaG1NVFJqWVdNellXUm1ZalUyTTJJeVpBPT0iLCJraWQiOiJnYXRld2F5X2NlcnRpZmljYXRlX2FsaWFzIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ==.eyJzdWIiOiJKb3JkYW5AbW9yZGF1bnRncm91cC5jby51a0BjYXJib24uc3VwZXIiLCJhcHBsaWNhdGlvbiI6eyJvd25lciI6IkpvcmRhbkBtb3JkYXVudGdyb3VwLmNvLnVrIiwidGllclF1b3RhVHlwZSI6bnVsbCwidGllciI6IlVubGltaXRlZCIsIm5hbWUiOiJtYXBfaW1hZ2VzLTRhOTU1ZWE3LTgwZmItNDEwOS05NmVkLTk5MTIzMjVjM2JiZiIsImlkIjo0NjIxOSwidXVpZCI6ImI1YTUyZTg3LTI2NDUtNDZkOS1hZDUzLWJkZmQ5OTJjZWI2ZCJ9LCJpc3MiOiJodHRwczpcL1wvYXBpLW1hbmFnZXIuYXBpLW1hbmFnZW1lbnQubWV0b2ZmaWNlLmNsb3VkOjQ0M1wvb2F1dGgyXC90b2tlbiIsInRpZXJJbmZvIjp7IndkaF9tYXBfaW1hZ2VzX2ZyZWUiOnsidGllclF1b3RhVHlwZSI6InJlcXVlc3RDb3VudCIsImdyYXBoUUxNYXhDb21wbGV4aXR5IjowLCJncmFwaFFMTWF4RGVwdGgiOjAsInN0b3BPblF1b3RhUmVhY2giOnRydWUsInNwaWtlQXJyZXN0TGltaXQiOjAsInNwaWtlQXJyZXN0VW5pdCI6InNlYyJ9fSwia2V5dHlwZSI6IlBST0RVQ1RJT04iLCJzdWJzY3JpYmVkQVBJcyI6W3sic3Vic2NyaWJlclRlbmFudERvbWFpbiI6ImNhcmJvbi5zdXBlciIsIm5hbWUiOiJtYXAtaW1hZ2VzIiwiY29udGV4dCI6IlwvbWFwLWltYWdlc1wvMS4wLjAiLCJwdWJsaXNoZXIiOiJXREhfQ0kiLCJ2ZXJzaW9uIjoiMS4wLjAiLCJzdWJzY3JpcHRpb25UaWVyIjoid2RoX21hcF9pbWFnZXNfZnJlZSJ9XSwidG9rZW5fdHlwZSI6ImFwaUtleSIsImlhdCI6MTc3Njk4OTI1MCwianRpIjoiOGZkNTg0M2YtOTIzNy00MDBlLWFhZmYtZDUxZTY4ZDdiZDQ4In0=.XIUydp44XRpgXu0YtVyrBVIPNgCTMnQvmSM5te1A_pW2ofUl5qPDDok2YL6EATK-EGnk2yNNHCp_WotRwgVCgGgwT4uacZqYnpbOJdU9UTFPnLMZjWoG7pzdEJEAV1IcXw8uOtyOrS_mjHJfr8ZqKEuoGwbfxlCatgpu3tVENObPLBuf0QhO4DM-ydhbZQHAUzlPPzmb4VQTqhjXiq16h4Iyy450kaimBTPjTOKQTyzh4gLMcO0Qc8xIG1qxNRdgjroB_te2O3WuHi8FWRnOefKjvJI6YVxEwKsnxH2mqUq5VUQLJOuwh2uKKb17ANRZ3v_RhU4vUEMO1vNsdHZtxA=="
)

# Site-specific (Global Spot) API key — separate subscription from map-images; never exposed to the browser.
# Override with ``CAD_METOFFICE_SITE_SPECIFIC_API_KEY`` (Flask config or environment).
_CAD_METOFFICE_EMBEDDED_SITE_SPECIFIC_API_KEY = (
    "eyJ4NXQjUzI1NiI6Ik5XVTVZakUxTkRjeVl6a3hZbUl4TkdSaFpqSmpOV1l6T1dGaE9XWXpNMk0yTWpRek5USm1OVEE0TXpOaU9EaG1NVFJqWVdNellXUm1ZalUyTTJJeVpBPT0iLCJraWQiOiJnYXRld2F5X2NlcnRpZmljYXRlX2FsaWFzIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ==.eyJzdWIiOiJKb3JkYW5AbW9yZGF1bnRncm91cC5jby51a0BjYXJib24uc3VwZXIiLCJhcHBsaWNhdGlvbiI6eyJvd25lciI6IkpvcmRhbkBtb3JkYXVudGdyb3VwLmNvLnVrIiwidGllclF1b3RhVHlwZSI6bnVsbCwidGllciI6IlVubGltaXRlZCIsIm5hbWUiOiJzaXRlX3NwZWNpZmljLTRhOTU1ZWE3LTgwZmItNDEwOS05NmVkLTk5MTIzMjVjM2JiZiIsImlkIjo0NjI1MiwidXVpZCI6IjY2MjcxZjFlLWNjYmQtNGQ2MS04YjEzLTkwZDg3NjE5NmEwYyJ9LCJpc3MiOiJodHRwczpcL1wvYXBpLW1hbmFnZXIuYXBpLW1hbmFnZW1lbnQubWV0b2ZmaWNlLmNsb3VkOjQ0M1wvb2F1dGgyXC90b2tlbiIsInRpZXJJbmZvIjp7IndkaF9zaXRlX3NwZWNpZmljX2ZyZWUiOnsidGllclF1b3RhVHlwZSI6InJlcXVlc3RDb3VudCIsImdyYXBoUUxNYXhDb21wbGV4aXR5IjowLCJncmFwaFFMTWF4RGVwdGgiOjAsInN0b3BPblF1b3RhUmVhY2giOnRydWUsInNwaWtlQXJyZXN0TGltaXQiOjAsInNwaWtlQXJyZXN0VW5pdCI6InNlYyJ9fSwia2V5dHlwZSI6IlBST0RVQ1RJT04iLCJzdWJzY3JpYmVkQVBJcyI6W3sic3Vic2NyaWJlclRlbmFudERvbWFpbiI6ImNhcmJvbi5zdXBlciIsIm5hbWUiOiJTaXRlU3BlY2lmaWNGb3JlY2FzdCIsImNvbnRleHQiOiJcL3NpdGVzcGVjaWZpY1wvdjAiLCJwdWJsaXNoZXIiOiJKYWd1YXJfQ0kiLCJ2ZXJzaW9uIjoidjAiLCJzdWJzY3JpcHRpb25UaWVyIjoid2RoX3NpdGVfc3BlY2lmaWNfZnJlZSJ9XSwidG9rZW5fdHlwZSI6ImFwaUtleSIsImlhdCI6MTc3NzAzMjM3NCwianRpIjoiNDI4Y2U3YjMtZTNiZi00ZjBjLTgwNmYtY2M0ZTQzZTMwNjFjIn0=.DenwEW-YKY2hM193FgQHUB8k9SHGJEbgf2cwf6DmbZiN1XhIQ-_GHplkwBmriISHTbOanlF1VWej8Y5aViA1HiwFOSRbV-Gtp03gLh9jPX40h2POtifz5hXgMQZjQTh577-1-EENK1CBVeOVqtvwRxJNosckobGZI3BJwy4mj-ckWwTHaNYoQPAS5v5puHo1oCyxrKwWs3FHlkCYpnyqxjWmLePE4c93X8XcViSkQ50d9-PsF8TOO3t1Jh-ng16b8ei9GYRJceWaP5sK9vGCoo4egAEPyKC_lAtmzAVCDb0Z3HDbwbsN-D4IKVbM42IsYgVESyuqjfXvRzwQmjq_Lg=="
)

# Order id for map-images (API ``orderId`` from Weather DataHub / Map Images tool or ``GET …/orders``).
# Override ``CAD_METOFFICE_MAP_IMAGES_ORDER_ID`` when you create a different order.
_CAD_METOFFICE_EMBEDDED_MAP_IMAGES_ORDER_ID = "o001848898515"

# Official gateway (Weather DataHub OpenAPI): ``https://data.hub.api.metoffice.gov.uk/map-images/1.0.0``
# (paths under ``/1.0.0/…`` in docs). Embedded JWT context matches ``/map-images/1.0.0``.
# Try 1.1.0 second if the org is moved to a newer product line. Override via ``CAD_METOFFICE_MAP_IMAGES_BASE_URL``.
_CAD_METOFFICE_MAP_IMAGES_API_1_0_0 = "https://data.hub.api.metoffice.gov.uk/map-images/1.0.0"
_CAD_METOFFICE_MAP_IMAGES_API_1_1_0 = "https://data.hub.api.metoffice.gov.uk/map-images/1.1.0"
_CAD_METOFFICE_MAP_IMAGES_API_BASE_DEFAULT = _CAD_METOFFICE_MAP_IMAGES_API_1_0_0

# In-process cache for ``/map/metoffice-preview.png`` (DataHub has daily request limits).
# ``files`` holds raw ``orderDetails.files`` entries (for labels). ``png_by_file`` caches each fileId PNG.
_cad_metoffice_map_images_preview_cache = {
    "order_ts": 0.0,
    "base": None,
    "order_id": None,
    "files": [],
    "bounds": None,
    "png_by_file": {},
}
_cad_metoffice_map_images_preview_lock = threading.Lock()


def _cad_metoffice_api_key():
    """Weather DataHub / API Manager credential (never sent to browser). Config/env override embedded default."""
    v = ""
    try:
        v = str(current_app.config.get("CAD_METOFFICE_API_KEY") or "").strip()
    except RuntimeError:
        pass
    if not v:
        v = os.environ.get("CAD_METOFFICE_API_KEY", "").strip()
    if v:
        return v
    return _CAD_METOFFICE_EMBEDDED_MAP_IMAGES_API_KEY.strip()


def _cad_metoffice_site_specific_api_key():
    """Site-specific / Global Spot credential (never sent to browser). Separate from map-images."""
    v = ""
    try:
        v = str(current_app.config.get("CAD_METOFFICE_SITE_SPECIFIC_API_KEY") or "").strip()
    except RuntimeError:
        pass
    if not v:
        v = os.environ.get("CAD_METOFFICE_SITE_SPECIFIC_API_KEY", "").strip()
    if v:
        return v
    return _CAD_METOFFICE_EMBEDDED_SITE_SPECIFIC_API_KEY.strip()


_CAD_METOFFICE_SITE_SPECIFIC_BASE_DEFAULT = (
    "https://data.hub.api.metoffice.gov.uk/sitespecific/v0/point"
)
_cad_metoffice_site_forecast_cache = {}
_cad_metoffice_site_forecast_lock = threading.Lock()


def _cad_metoffice_site_specific_base_url():
    u = ""
    try:
        u = str(current_app.config.get("CAD_METOFFICE_SITE_SPECIFIC_BASE_URL") or "").strip()
    except RuntimeError:
        pass
    if not u:
        u = os.environ.get("CAD_METOFFICE_SITE_SPECIFIC_BASE_URL", "").strip()
    return (u or _CAD_METOFFICE_SITE_SPECIFIC_BASE_DEFAULT).rstrip("/")


def _cad_metoffice_site_specific_cache_seconds():
    try:
        return float(current_app.config.get("CAD_METOFFICE_SITE_SPECIFIC_CACHE_SECONDS") or 120)
    except RuntimeError:
        pass
    try:
        return float(os.environ.get("CAD_METOFFICE_SITE_SPECIFIC_CACHE_SECONDS") or 120)
    except (TypeError, ValueError):
        return 120.0


def _cad_metoffice_site_specific_data_source():
    """Optional ``dataSource`` query (Met Office codes, e.g. ``BD1`` for blended). Empty = use built-in fallback."""
    v = ""
    try:
        v = str(current_app.config.get("CAD_METOFFICE_SITE_SPECIFIC_DATA_SOURCE") or "").strip()
    except RuntimeError:
        pass
    if not v:
        v = os.environ.get("CAD_METOFFICE_SITE_SPECIFIC_DATA_SOURCE", "").strip()
    return v


def _cad_metoffice_site_forecast_timeseries_from_payload(data):
    """Extract ``timeSeries`` list from site-specific JSON or GeoJSON Feature properties."""
    if isinstance(data, list):
        for item in data:
            ts = _cad_metoffice_site_forecast_timeseries_from_payload(item)
            if ts:
                return ts
        return []
    if not isinstance(data, dict):
        return []
    ts = data.get("timeSeries")
    if isinstance(ts, list) and ts and isinstance(ts[0], dict):
        return ts
    feats = data.get("features")
    if isinstance(feats, list):
        for feat in feats:
            if isinstance(feat, dict):
                props = feat.get("properties")
                if isinstance(props, dict):
                    inner = _cad_metoffice_site_forecast_timeseries_from_payload(props)
                    if inner:
                        return inner
    return []


def _cad_metoffice_site_forecast_location_name(data):
    if not isinstance(data, dict):
        return None
    loc = data.get("location")
    if isinstance(loc, dict):
        n = loc.get("name")
        if n:
            return str(n)
    feats = data.get("features")
    if isinstance(feats, list) and feats:
        p0 = feats[0].get("properties") if isinstance(feats[0], dict) else None
        if isinstance(p0, dict):
            loc = p0.get("location")
            if isinstance(loc, dict) and loc.get("name"):
                return str(loc["name"])
    return None


def _cad_metoffice_parse_ts(value):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _cad_metoffice_site_forecast_pick_nearest_row(time_series):
    if not time_series:
        return None
    now = datetime.now(timezone.utc)
    best = None
    best_delta = None
    for row in time_series:
        if not isinstance(row, dict):
            continue
        t = _cad_metoffice_parse_ts(row.get("time"))
        if t is None:
            continue
        delta = abs((t - now).total_seconds())
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best = row
    return best or (time_series[0] if isinstance(time_series[0], dict) else None)


def _cad_metoffice_site_forecast_row_get(row, *names):
    if not isinstance(row, dict):
        return None
    for n in names:
        if n in row and row[n] is not None:
            return row[n]
    return None


def _cad_metoffice_lake_water_estimate_enabled():
    """Rough inland-lake surface estimate from screen air temp; disable with ``CAD_METOFFICE_LAKE_WATER_ESTIMATE=0``."""
    v = os.environ.get("CAD_METOFFICE_LAKE_WATER_ESTIMATE", "").strip().lower()
    if not v:
        try:
            v = str(current_app.config.get("CAD_METOFFICE_LAKE_WATER_ESTIMATE") or "").strip().lower()
        except RuntimeError:
            v = ""
    if not v:
        return True
    return v not in ("0", "false", "no", "off")


def _cad_metoffice_lake_water_offset_c_for_doy(doy):
    """Piecewise linear offset (T_water − T_screen) °C, northern-temperate shallow-lake ballpark.

    Not calibrated to any gauge network; typical error can exceed several °C for a given lake.
    Based on common seasonal lag: cold water under warming air in spring, warm water under cool
    air in autumn. See e.g. limnology literature on air–lake coupling (e.g. Bachmann et al. 2020;
    air2water-type models) for why a single curve cannot be accurate everywhere.
    """
    d = int(doy)
    if d < 1:
        d = 1
    if d > 366:
        d = 366
    anchors = (
        (1, -0.5),
        (74, -5.5),
        (120, -7.0),
        (152, -4.5),
        (200, -2.0),
        (244, -1.5),
        (274, 1.5),
        (304, 3.5),
        (334, 1.0),
        (366, -0.5),
    )
    for i in range(len(anchors) - 1):
        d0, o0 = anchors[i]
        d1, o1 = anchors[i + 1]
        if d0 <= d <= d1:
            if d1 <= d0:
                return float(o0)
            t = (d - d0) / float(d1 - d0)
            return float(o0) + t * (float(o1) - float(o0))
    return float(anchors[-1][1])


def _cad_metoffice_lake_surface_water_estimate_c(screen_c, feels_c, forecast_dt_utc):
    """Return rounded °C or None. Indicative only — not measured water temperature."""
    if screen_c is None:
        return None
    try:
        ta = float(screen_c)
    except (TypeError, ValueError):
        return None
    fdt = forecast_dt_utc or datetime.now(timezone.utc)
    try:
        doy = int(fdt.timetuple().tm_yday)
    except Exception:
        doy = 180
    offset = _cad_metoffice_lake_water_offset_c_for_doy(doy)
    tw = ta + offset
    if feels_c is not None:
        try:
            chill = float(feels_c) - ta
            tw += max(-2.0, min(0.5, 0.08 * chill))
        except (TypeError, ValueError):
            pass
    tw = max(0.0, min(32.0, tw))
    return round(tw, 1)


def _cad_metoffice_site_forecast_summarize_json(data):
    """Reduce upstream payload to a small dict for the CAD unit modal."""
    ts = _cad_metoffice_site_forecast_timeseries_from_payload(data)
    row = _cad_metoffice_site_forecast_pick_nearest_row(ts)
    loc_name = _cad_metoffice_site_forecast_location_name(data)
    model_run = None
    if isinstance(data, dict):
        feats = data.get("features")
        if isinstance(feats, list) and feats:
            p0 = feats[0].get("properties") if isinstance(feats[0], dict) else None
            if isinstance(p0, dict):
                mr = p0.get("modelRunDate")
                if mr:
                    model_run = str(mr)
    out = {
        "ok": True,
        "locationName": loc_name,
        "modelRunDate": model_run,
        "forecastTime": None,
        "screenTemperatureC": None,
        "feelsLikeTemperatureC": None,
        "probOfPrecipitation": None,
        "windSpeedMs": None,
        "windSpeedMph": None,
        "windGustMs": None,
        "windGustMph": None,
        "windDirection": None,
        "significantWeatherCode": None,
        "uvIndex": None,
        "seaSurfaceTemperatureC": None,
        "lakeSurfaceWaterEstimateC": None,
        "lakeSurfaceWaterEstimateMethod": None,
        "lakeSurfaceWaterEstimateDisclaimer": None,
    }
    if not row:
        return out
    out["forecastTime"] = row.get("time")
    st = _cad_metoffice_site_forecast_row_get(
        row, "screenTemperature", "screenAirTemperature", "maxScreenAirTemperature")
    if st is not None:
        try:
            out["screenTemperatureC"] = float(st)
        except (TypeError, ValueError):
            pass
    fl = _cad_metoffice_site_forecast_row_get(
        row, "feelsLikeTemperature", "feelsLikeAirTemperature")
    if fl is not None:
        try:
            out["feelsLikeTemperatureC"] = float(fl)
        except (TypeError, ValueError):
            pass
    prob = _cad_metoffice_site_forecast_row_get(
        row, "probOfPrecipitation", "probabilityOfPrecipitation")
    if prob is not None:
        try:
            out["probOfPrecipitation"] = int(round(float(prob)))
        except (TypeError, ValueError):
            pass
    ws = _cad_metoffice_site_forecast_row_get(
        row, "windSpeedAt10m", "windSpeed10m", "windSpeed")
    if ws is not None:
        try:
            wsf = float(ws)
            out["windSpeedMs"] = wsf
            out["windSpeedMph"] = round(wsf * 2.2369362920544, 1)
        except (TypeError, ValueError):
            pass
    wd = _cad_metoffice_site_forecast_row_get(
        row, "windDirectionFrom10m", "windDirection10m", "windDirection")
    if wd is not None:
        out["windDirection"] = str(wd)
    wg = _cad_metoffice_site_forecast_row_get(
        row, "windGustSpeed10m", "max10mWindGust", "windGustAt10m")
    if wg is not None:
        try:
            wgf = float(wg)
            out["windGustMs"] = wgf
            out["windGustMph"] = round(wgf * 2.2369362920544, 1)
        except (TypeError, ValueError):
            pass
    sst = _cad_metoffice_site_forecast_row_get(
        row,
        "seaSurfaceTemperature",
        "seaTemperature",
        "sea_surface_temperature",
        "sst",
    )
    if sst is not None:
        try:
            out["seaSurfaceTemperatureC"] = float(sst)
        except (TypeError, ValueError):
            pass
    sw = _cad_metoffice_site_forecast_row_get(row, "significantWeatherCode", "weatherType")
    if sw is not None:
        try:
            out["significantWeatherCode"] = int(sw)
        except (TypeError, ValueError):
            pass
    uv = _cad_metoffice_site_forecast_row_get(
        row,
        "uvIndex",
        "maxUvIndex",
        "maxUVIndex",
        "UVIndex",
        "ultravioletIndex",
    )
    if uv is not None:
        try:
            out["uvIndex"] = round(float(uv), 1)
        except (TypeError, ValueError):
            pass

    # Inland lake ballpark: only when we do not already have open-water SST from the API.
    if (
        _cad_metoffice_lake_water_estimate_enabled()
        and out.get("seaSurfaceTemperatureC") is None
        and out.get("screenTemperatureC") is not None
    ):
        fdt = _cad_metoffice_parse_ts(out.get("forecastTime"))
        est = _cad_metoffice_lake_surface_water_estimate_c(
            out["screenTemperatureC"],
            out.get("feelsLikeTemperatureC"),
            fdt,
        )
        if est is not None:
            out["lakeSurfaceWaterEstimateC"] = est
            out["lakeSurfaceWaterEstimateMethod"] = (
                "seasonal_offset_from_screen_air_temp_nh_temperate"
            )
            out["lakeSurfaceWaterEstimateDisclaimer"] = (
                "Indicative lake-surface estimate from forecast screen air temperature and "
                "day-of-year (UK/temperate shallow-water ballpark). Not measured water temperature; "
                "error can be several °C depending on depth, wind, inflows, and season. "
                "Do not use as the sole basis for clinical or rescue decisions."
            )
    return out


def _cad_metoffice_site_forecast_fetch_raw(lat, lng, timesteps, data_source):
    base = _cad_metoffice_site_specific_base_url()
    url = f"{base}/{timesteps}"
    key = _cad_metoffice_site_specific_api_key()
    params = {
        "excludeParameterMetadata": "true",
        "includeLocationName": "true",
        "latitude": lat,
        "longitude": lng,
    }
    ds = (data_source or "").strip()
    if ds:
        params["dataSource"] = ds
    headers = {
        "accept": "application/json",
        "apikey": key,
        "User-Agent": "VentusCAD/1.0",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=45, allow_redirects=True)
    return resp


def _cad_metoffice_tile_upstream_template():
    """Upstream tile URL with placeholders {z}/{x}/{y} or {Z}/{X}/{Y} (filled per request)."""
    return (str(current_app.config.get('CAD_METOFFICE_MAP_TILE_URL_TEMPLATE') or '').strip()
            or os.environ.get('CAD_METOFFICE_MAP_TILE_URL_TEMPLATE', '').strip())


def _cad_metoffice_map_images_base_url():
    u = ""
    try:
        u = str(current_app.config.get("CAD_METOFFICE_MAP_IMAGES_BASE_URL") or "").strip()
    except RuntimeError:
        pass
    if not u:
        u = os.environ.get("CAD_METOFFICE_MAP_IMAGES_BASE_URL", "").strip()
    return (u or _CAD_METOFFICE_MAP_IMAGES_API_BASE_DEFAULT).rstrip("/")


def _cad_metoffice_map_images_api_base_candidates():
    """Config/env base first, then 1.0.0 (documented gateway), then 1.1.0 — deduped."""
    primary = _cad_metoffice_map_images_base_url().rstrip("/")
    out = []
    seen = set()
    for b in (primary, _CAD_METOFFICE_MAP_IMAGES_API_1_0_0, _CAD_METOFFICE_MAP_IMAGES_API_1_1_0):
        bb = str(b or "").strip().rstrip("/")
        if bb and bb not in seen:
            seen.add(bb)
            out.append(bb)
    return out


def _cad_metoffice_map_images_get_orders_list(base_url):
    """Return lowercased ``orderId`` values from ``GET {base}/orders`` (200 + JSON), else []."""
    base_url = (base_url or "").strip().rstrip("/")
    if not base_url:
        return []
    url = f"{base_url}/orders"
    try:
        resp = requests.get(
            url,
            headers=_cad_metoffice_map_images_auth_headers("application/json"),
            timeout=45,
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        logger.warning("Met Office map-images GET /orders failed for %s: %s", base_url, exc)
        return []
    if resp.status_code != 200:
        logger.warning(
            "Met Office map-images GET /orders HTTP %s for %s",
            resp.status_code,
            base_url,
        )
        return []
    try:
        data = resp.json()
    except ValueError:
        return []
    out = []
    for o in (data.get("orders") or []):
        if not isinstance(o, dict):
            continue
        oid = str(o.get("orderId") or "").strip().lower()
        if oid:
            out.append(oid)
    return out


def _cad_metoffice_map_images_order_ids_to_try(bases):
    """Configured order id first, then ids from the first base that returns a non-empty ``/orders`` list."""
    ids = []
    seen = set()
    main = _cad_metoffice_map_images_order_id()
    if main and main not in seen:
        seen.add(main)
        ids.append(main)
    for b in bases:
        got = _cad_metoffice_map_images_get_orders_list(b)
        for oid in got:
            if oid not in seen:
                seen.add(oid)
                ids.append(oid)
        if got:
            break
    return ids


def _cad_metoffice_map_images_order_id():
    o = ""
    try:
        o = str(current_app.config.get("CAD_METOFFICE_MAP_IMAGES_ORDER_ID") or "").strip()
    except RuntimeError:
        pass
    if not o:
        o = os.environ.get("CAD_METOFFICE_MAP_IMAGES_ORDER_ID", "").strip()
    return (o or _CAD_METOFFICE_EMBEDDED_MAP_IMAGES_ORDER_ID).strip().lower()


def _cad_metoffice_map_images_auth_headers(accept: str):
    """Headers for Weather DataHub map-images only.

    Official ``map_images_download`` uses ``{"apikey": <token>}`` only (no Bearer).
    Sending both can confuse some gateways; match the reference client.
    """
    key = _cad_metoffice_api_key()
    return {
        "apikey": key,
        "Accept": accept,
        "User-Agent": "VentusCAD/1.0",
    }


def _cad_metoffice_map_images_fetch_json(url: str):
    """GET JSON from DataHub; uses ``requests`` so 302 redirects (per Met Office docs) are followed reliably."""
    try:
        resp = requests.get(
            url,
            headers=_cad_metoffice_map_images_auth_headers("application/json"),
            timeout=45,
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        logger.warning(
            "Met Office map-images JSON request failed for %s: %s",
            (url.split("?", 1)[0])[:160],
            exc,
        )
        raise
    if resp.status_code != 200:
        body = (resp.text or "")[:400]
        logger.warning(
            "Met Office map-images JSON HTTP %s for %s: %s",
            resp.status_code,
            (url.split("?", 1)[0])[:160],
            body.replace("\n", " ").strip(),
        )
        exc = requests.HTTPError(f"{resp.status_code} for url: {url}")
        exc.response = resp
        raise exc
    raw = (resp.text or "") if resp.content else ""
    if not raw:
        return {}
    try:
        return resp.json()
    except ValueError as exc:
        logger.warning(
            "Met Office map-images JSON decode error: %s snippet=%r",
            exc,
            raw[:200],
        )
        raise


def _cad_metoffice_map_images_collect_files(order):
    """Gather file descriptors with fileId from /latest JSON (shape varies by detail level)."""
    files = []
    od = order.get("orderDetails") if isinstance(order, dict) else None
    if isinstance(od, dict):
        files = od.get("files") or []
    if not files and isinstance(order, dict):
        files = order.get("files") or []
    if isinstance(files, list) and files:
        return files
    # MINIMAL or alternate payloads: walk for dicts carrying fileId
    found = []
    seen = set()

    def visit(obj):
        if isinstance(obj, dict):
            fid = obj.get("fileId") or obj.get("file_id")
            if isinstance(fid, str) and fid.strip():
                s = fid.strip()
                if s not in seen:
                    seen.add(s)
                    found.append({"fileId": s})
            for v in obj.values():
                visit(v)
        elif isinstance(obj, list):
            for it in obj:
                visit(it)

    if isinstance(order, dict):
        visit(order)
    return found


def _cad_metoffice_png_bytes_look_valid(png):
    if not isinstance(png, (bytes, bytearray)) or len(png) < 24:
        return False
    return bytes(png[:8]) == b"\x89PNG\r\n\x1a\n"


def _cad_metoffice_map_images_fetch_png_with_redirects(png_url: str):
    """GET ``…/latest/{fileId}/data`` per MetOffice ``weather_datahub_utilities`` ``map_images_download.get_order_file``:

    ``Accept: image/png``, ``apikey``, ``requests.get(..., allow_redirects=True)``. Some gateway docs
    mention ``application/x-grib`` for redirects; try that if PNG Accept returns non-image.
    """
    # Order matches reference script first, then OpenAPI variants.
    accept_chain = (
        "image/png",
        "image/png,image/*,*/*",
        "application/x-grib",
    )
    last_status = None
    last_head = None
    for accept in accept_chain:
        try:
            resp = requests.get(
                png_url,
                headers=_cad_metoffice_map_images_auth_headers(accept),
                timeout=90,
                allow_redirects=True,
            )
        except requests.RequestException as exc:
            logger.warning("Met Office map-images PNG request failed (Accept=%r): %s", accept, exc)
            return None, None
        last_status = resp.status_code
        data = resp.content
        if resp.status_code != 200:
            last_head = (data[:64]) if isinstance(data, (bytes, bytearray)) else None
            continue
        if _cad_metoffice_png_bytes_look_valid(data):
            return data, accept
        last_head = (data[:64]) if isinstance(data, (bytes, bytearray)) else None
        logger.warning(
            "Met Office map-images /data non-PNG (Accept=%r, status=%s, head=%r)",
            accept,
            resp.status_code,
            last_head,
        )
    logger.warning(
        "Met Office map-images PNG failed for %s after Accept fallbacks (last HTTP=%s)",
        (png_url.split("?", 1)[0])[:180],
        last_status,
    )
    return None, None


def _cad_metoffice_map_images_ui_parameter_allowlist():
    """Comma-separated parameter prefixes (before ``_ts`` in ``fileId``). Env/config override."""
    raw = os.environ.get("CAD_METOFFICE_MAP_IMAGES_UI_PARAMETERS", "").strip()
    if not raw:
        try:
            raw = str(current_app.config.get("CAD_METOFFICE_MAP_IMAGES_UI_PARAMETERS") or "").strip()
        except RuntimeError:
            raw = ""
    if raw:
        return [x.strip().lower() for x in raw.split(",") if x.strip()]
    # Dispatch-friendly default: precipitation map only (temperature is numeric site-specific, not PNG).
    return ["total_precipitation_rate"]


# Short labels for common DataHub ``fileId`` prefixes (``{param}_ts{hours}_{cycle|run}``).
_CAD_METOFFICE_MAP_IMAGES_PARAM_UI_LABEL = {
    "total_precipitation_rate": "Rain (precipitation)",
    "temperature_at_surface": "Temperature (surface)",
    "mean_sea_level_pressure": "Pressure (mean sea level)",
    "cloud_amount_total": "Total cloud",
    "land_cover": "Land / sea mask",
    "wind_speed_at_10m": "Wind speed (10 m)",
    "wind_speed_of_wind_at_10m": "Wind speed (10 m)",
}


def _cad_metoffice_map_images_preferred_file_in_param_group(file_ids):
    """Among many timesteps / cycles for one parameter, pick one sensible map for CAD."""
    if not file_ids:
        return None

    def sort_key(fid):
        m = re.match(r"^(.+)_ts(\d+)_(.+)$", fid)
        if not m:
            return (9999, 9, 9, 9, fid)
        ts_h = int(m.group(2))
        tail = m.group(3)
        # Prefer canonical +00 / +12 cycle keys over per-run ``YYYYMMDDHH`` suffixes.
        pub = 0 if re.match(r"^[+]\d{2}$", tail) else 1
        hh = 0 if tail == "+00" else (1 if tail == "+12" else 2)
        return (ts_h, pub, hh, len(tail), fid)

    return min(file_ids, key=sort_key)


def _cad_metoffice_map_images_ui_layer_rows(files):
    """One ``fileId`` per allowed parameter (narrow forecast / model cycle list for the UI)."""
    if not isinstance(files, list) or not files:
        return []
    allow = _cad_metoffice_map_images_ui_parameter_allowlist()
    by_param = defaultdict(list)
    for f in files:
        if not isinstance(f, dict):
            continue
        fid = str(f.get("fileId") or f.get("file_id") or "").strip()
        if not fid or "_ts" not in fid:
            continue
        param = fid.split("_ts", 1)[0].strip().lower()
        if param in set(allow):
            by_param[param].append(fid)
    rows = []
    for param in allow:
        p_low = param.lower()
        ids = by_param.get(p_low)
        if not ids:
            continue
        best = _cad_metoffice_map_images_preferred_file_in_param_group(ids)
        label = _CAD_METOFFICE_MAP_IMAGES_PARAM_UI_LABEL.get(
            p_low,
            p_low.replace("_", " ").title()[:72],
        )
        rows.append({"id": best, "label": label})
    return rows


def _cad_metoffice_map_images_pick_file_id(files, allowed_ids=None):
    """Prefer precipitation-style layers; optional ``allowed_ids`` restricts to a UI subset."""
    if not isinstance(files, list) or not files:
        return None
    allowed = None
    if allowed_ids is not None:
        allowed = {str(x).strip() for x in allowed_ids if str(x).strip()}
    hint = os.environ.get("CAD_METOFFICE_MAP_IMAGES_FILE_HINT", "").strip().lower()
    if not hint:
        try:
            hint = str(current_app.config.get("CAD_METOFFICE_MAP_IMAGES_FILE_HINT") or "").strip().lower()
        except RuntimeError:
            hint = ""
    scored = []
    for f in files:
        if not isinstance(f, dict):
            continue
        fid = str(f.get("fileId") or f.get("file_id") or "").strip()
        if not fid:
            continue
        if allowed is not None and fid not in allowed:
            continue
        fl = fid.lower()
        score = 0
        if hint and hint in fl:
            score += 200
        for kw, w in (
            ("precipitation", 50), ("precip", 45), ("rain", 40),
            ("cloud", 30), ("temperature", 25), ("temp", 22),
            ("pressure", 18), ("msl", 15),
        ):
            if kw in fl:
                score += w
        scored.append((score, len(fid), fid))
    scored.sort(key=lambda t: (-t[0], -t[1]))
    return scored[0][2] if scored else None


def _cad_metoffice_map_images_default_file_id_for_order(files):
    """Default overlay file: UI subset when available, else legacy pick across all files."""
    ui = _cad_metoffice_map_images_ui_layer_rows(files)
    if ui:
        ids = {x["id"] for x in ui if x.get("id")}
        fid = _cad_metoffice_map_images_pick_file_id(files, allowed_ids=ids)
        if fid:
            return fid
    return _cad_metoffice_map_images_pick_file_id(files)


def _cad_metoffice_map_images_file_label(f):
    """Human label for a ``/latest`` ``files[]`` entry (DataHub shape varies)."""
    if not isinstance(f, dict):
        return "Layer"
    fid = str(f.get("fileId") or f.get("file_id") or "").strip() or "unknown"
    for key in (
        "name",
        "title",
        "description",
        "parameterName",
        "parameterId",
        "productName",
        "shortName",
        "fileName",
        "label",
    ):
        v = f.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()[:160]
    return (fid[:72] + "…") if len(fid) > 72 else fid


def _cad_metoffice_map_images_fetch_latest_order_bundle():
    """Return ``(base, order_id, order, files)`` or ``(None, None, None, [])`` when no files."""
    bases = _cad_metoffice_map_images_api_base_candidates()
    order_id_candidates = _cad_metoffice_map_images_order_ids_to_try(bases)
    files = []
    last_fetch_error = None
    base = None
    order = None
    order_id = None
    for try_base in bases:
        for oid in order_id_candidates:
            for latest_url in (
                f"{try_base}/orders/{quote(oid, safe='')}/latest?detail=MINIMAL",
                f"{try_base}/orders/{quote(oid, safe='')}/latest",
            ):
                try:
                    order = _cad_metoffice_map_images_fetch_json(latest_url)
                    last_fetch_error = None
                except Exception as exc:
                    last_fetch_error = exc
                    logger.warning(
                        "Met Office map-images order/latest failed (order=%s base=%s %s): %s",
                        oid[:48],
                        try_base.rsplit("/", 1)[-1],
                        "MINIMAL" if "detail=MINIMAL" in latest_url else "default",
                        exc,
                    )
                    continue
                files = _cad_metoffice_map_images_collect_files(order)
                if files:
                    base = try_base
                    order_id = oid
                    break
            if files:
                break
        if files:
            break
    if not files:
        probe = _cad_metoffice_map_images_get_orders_list(bases[0]) if bases else []
        if not probe:
            logger.warning(
                "Met Office map-images: no files for any order id; GET …/orders returned **no orders** "
                "for this API key. Configure at least one Map Images order in Weather DataHub for this "
                "credential. Last /latest error: %s",
                last_fetch_error,
            )
        else:
            logger.warning(
                "Met Office map-images: no file list after trying order ids %s on all bases; "
                "last /latest error: %s",
                ", ".join(order_id_candidates[:6]),
                last_fetch_error,
            )
        return None, None, None, []
    return base, order_id, order, files


def _cad_metoffice_map_images_merge_order_into_cache_locked(now, base, order_id, order, files):
    """Update shared order snapshot; prune PNG cache when file set changes."""
    global _cad_metoffice_map_images_preview_cache
    c = _cad_metoffice_map_images_preview_cache
    new_ids = set()
    for f in files:
        if isinstance(f, dict):
            fid = str(f.get("fileId") or f.get("file_id") or "").strip()
            if fid:
                new_ids.add(fid)
    prev_ids = set()
    for f in c.get("files") or []:
        if isinstance(f, dict):
            fid = str(f.get("fileId") or f.get("file_id") or "").strip()
            if fid:
                prev_ids.add(fid)
    bounds = _cad_metoffice_map_images_bounds_sw_ne_from_order(order)
    pick0 = _cad_metoffice_map_images_default_file_id_for_order(files)
    if not bounds and pick0 and base and order_id:
        bounds = _cad_metoffice_map_images_fetch_bounds_from_file_details(
            base, order_id, pick0)
    if not bounds:
        bounds = [[33.0, -14.0], [68.0, 37.0]]
    c["order_ts"] = now
    c["base"] = base
    c["order_id"] = order_id
    c["files"] = list(files) if isinstance(files, list) else []
    c["bounds"] = bounds
    if new_ids != prev_ids:
        c["png_by_file"] = {}
    else:
        pbf = c.get("png_by_file") or {}
        if isinstance(pbf, dict):
            c["png_by_file"] = {k: v for k, v in pbf.items() if k in new_ids}
        else:
            c["png_by_file"] = {}


def _cad_metoffice_map_images_order_context_fresh(c, now, ttl):
    if not isinstance(c, dict):
        return False
    if not c.get("files"):
        return False
    return (now - float(c.get("order_ts") or 0.0)) < ttl


def _cad_metoffice_map_images_refresh_order_cache_if_stale_locked(now, ttl):
    """Refresh ``/latest`` when stale. On transient failure, keep prior snapshot and bump ``order_ts``."""
    global _cad_metoffice_map_images_preview_cache
    c = _cad_metoffice_map_images_preview_cache
    if _cad_metoffice_map_images_order_context_fresh(c, now, ttl):
        return bool(c.get("files"))
    base, order_id, order, files = _cad_metoffice_map_images_fetch_latest_order_bundle()
    if files:
        _cad_metoffice_map_images_merge_order_into_cache_locked(
            now, base, order_id, order, files)
        return True
    if c.get("files"):
        c["order_ts"] = now
        return True
    return False


def _cad_metoffice_map_images_get_layer_manifest():
    """For CAD UI: small curated layer list (see ``_cad_metoffice_map_images_ui_layer_rows``) + default id."""
    now = time.time()
    ttl = max(60.0, _cad_metoffice_map_images_preview_cache_seconds())
    with _cad_metoffice_map_images_preview_lock:
        c = _cad_metoffice_map_images_preview_cache
        if not _cad_metoffice_map_images_refresh_order_cache_if_stale_locked(now, ttl):
            return {"layers": [], "defaultFileId": None}
        files = c.get("files") or []
        layers = _cad_metoffice_map_images_ui_layer_rows(files)
        if not layers:
            for f in files:
                if not isinstance(f, dict):
                    continue
                fid = str(f.get("fileId") or f.get("file_id") or "").strip()
                if not fid:
                    continue
                layers.append({"id": fid, "label": _cad_metoffice_map_images_file_label(f)})
            layers.sort(key=lambda x: (str(x.get("label") or "").lower(), x.get("id") or ""))
            layers = layers[:24]
        return {
            "layers": layers,
            "defaultFileId": _cad_metoffice_map_images_default_file_id_for_order(files),
        }


def _cad_metoffice_map_images_preview_cache_seconds():
    try:
        return float(current_app.config.get("CAD_METOFFICE_MAP_IMAGES_CACHE_SECONDS") or 300)
    except RuntimeError:
        return 300.0


def _cad_metoffice_map_images_extent_to_leaflet_bounds(region_or_extent):
    """Map Images ``region`` / ``extent``: x = longitude, y = latitude → ``[[south, west], [north, east]]``."""
    ext = region_or_extent
    if isinstance(region_or_extent, dict) and "extent" in region_or_extent:
        ext = region_or_extent.get("extent")
    if not isinstance(ext, dict):
        return None
    x = ext.get("x") or {}
    y = ext.get("y") or {}
    try:
        x0 = float(x.get("lowerBound"))
        x1 = float(x.get("upperBound"))
        y0 = float(y.get("lowerBound"))
        y1 = float(y.get("upperBound"))
    except (TypeError, ValueError):
        return None
    return [[min(y0, y1), min(x0, x1)], [max(y0, y1), max(x0, x1)]]


def _cad_metoffice_map_images_bounds_sw_ne_from_order(order):
    """Leaflet bounds from ``GET …/latest`` JSON ``orderDetails`` regions (when present)."""
    if not isinstance(order, dict):
        return None
    regions = []
    od = order.get("orderDetails")
    if isinstance(od, dict):
        o = od.get("order")
        if isinstance(o, dict):
            regions.extend(o.get("regions") or [])
        if not regions:
            regions.extend(od.get("regions") or [])
    if not isinstance(regions, list):
        return None
    sw_lat = sw_lng = ne_lat = ne_lng = None
    for reg in regions:
        if not isinstance(reg, dict):
            continue
        b = _cad_metoffice_map_images_extent_to_leaflet_bounds(reg)
        if not b:
            continue
        s0, s1 = b[0][0], b[0][1]
        n0, n1 = b[1][0], b[1][1]
        sw_lat = s0 if sw_lat is None else min(sw_lat, s0)
        sw_lng = s1 if sw_lng is None else min(sw_lng, s1)
        ne_lat = n0 if ne_lat is None else max(ne_lat, n0)
        ne_lng = n1 if ne_lng is None else max(ne_lng, n1)
    if sw_lat is None:
        return None
    return [[sw_lat, sw_lng], [ne_lat, ne_lng]]


def _cad_metoffice_map_images_fetch_bounds_from_file_details(base, order_id, file_id):
    """``GET …/latest/{fileId}`` (JSON) for ``fileDetails.file.region`` extent."""
    path = f"{base}/orders/{order_id}/latest/{file_id}"
    url = requests.utils.quote(path, safe=": /")
    try:
        resp = requests.get(
            url,
            headers=_cad_metoffice_map_images_auth_headers("application/json"),
            timeout=45,
            allow_redirects=True,
        )
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    fd = data.get("fileDetails")
    if not isinstance(fd, dict):
        return None
    f = fd.get("file")
    if not isinstance(f, dict):
        return None
    reg = f.get("region")
    if isinstance(reg, dict):
        return _cad_metoffice_map_images_extent_to_leaflet_bounds(reg)
    return None


def _cad_metoffice_map_images_resolve_preview_png(
        requested_file_id=None,
        strict_unknown_file=False):
    """
    Fetch one latest PNG per ``fileId`` — aligned with MetOffice ``map_images_download`` (Python).

    ``GET {base}/orders/{orderId}/latest`` … then ``GET .../latest/{fileId}/data`` with ``apikey`` +
    ``Accept: image/png``. Caches order metadata and one PNG per ``fileId`` (TTL).

    Returns ``(png_bytes|None, file_id|None, err)`` where ``err`` is ``None`` or ``\"unknown_file\"``
    when ``strict_unknown_file`` and ``requested_file_id`` is not in the current order file list.
    """
    global _cad_metoffice_map_images_preview_cache
    now = time.time()
    ttl = max(60.0, _cad_metoffice_map_images_preview_cache_seconds())
    req = (requested_file_id or "").strip() if isinstance(requested_file_id, str) else ""
    if len(req) > 280:
        req = ""
    base = order_id = None
    file_id = None
    with _cad_metoffice_map_images_preview_lock:
        c = _cad_metoffice_map_images_preview_cache
        if not _cad_metoffice_map_images_refresh_order_cache_if_stale_locked(now, ttl):
            return None, None, None
        files = c.get("files") or []
        base = c.get("base")
        order_id = c.get("order_id")
        allowed = set()
        for f in files:
            if isinstance(f, dict):
                fid = str(f.get("fileId") or f.get("file_id") or "").strip()
                if fid:
                    allowed.add(fid)
        if req and req not in allowed:
            if strict_unknown_file:
                return None, None, "unknown_file"
            req = ""
        file_id = (
            req if req in allowed
            else _cad_metoffice_map_images_default_file_id_for_order(files))
        if not file_id:
            logger.warning(
                "Met Office map-images: could not pick fileId for order %s",
                (order_id or "")[:48],
            )
            return None, None, None
        if not base or not order_id:
            logger.warning("Met Office map-images: no API base after trying version fallbacks")
            return None, None, None
        pbf = c.get("png_by_file") or {}
        ent = pbf.get(file_id) if isinstance(pbf, dict) else None
        if (
            isinstance(ent, dict)
            and ent.get("png")
            and (now - float(ent.get("ts") or 0.0)) < ttl
        ):
            return ent["png"], file_id, None

    png_path = f"{base}/orders/{order_id}/latest/{file_id}/data"
    land = False
    if os.environ.get("CAD_METOFFICE_MAP_IMAGES_INCLUDE_LAND", "").strip().lower() in (
        "1", "true", "yes",
    ):
        land = True
    else:
        try:
            land = bool(current_app.config.get("CAD_METOFFICE_MAP_IMAGES_INCLUDE_LAND"))
        except RuntimeError:
            land = False
    png_url = requests.utils.quote(png_path, safe=": /")
    if land:
        png_url = png_url + "?includeLand=true"
    png, _used_accept = _cad_metoffice_map_images_fetch_png_with_redirects(png_url)
    if not png:
        return None, None, None

    with _cad_metoffice_map_images_preview_lock:
        c = _cad_metoffice_map_images_preview_cache
        pbf = c.get("png_by_file")
        if not isinstance(pbf, dict):
            pbf = {}
            c["png_by_file"] = pbf
        pbf[file_id] = {"ts": now, "png": png}
    return png, file_id, None


def _cad_metoffice_weather_image_overlay_bounds():
    """South-west then north-east [lat,lng] for Leaflet ``imageOverlay``.

    Prefer ``CAD_METOFFICE_MAP_IMAGES_BOUNDS_JSON``, else bounds from the last successful
    order snapshot (same TTL as map-images cache), else a wide EU box suitable for UK Map Images orders.
    """
    raw = os.environ.get("CAD_METOFFICE_MAP_IMAGES_BOUNDS_JSON", "").strip()
    if not raw:
        try:
            raw = str(current_app.config.get("CAD_METOFFICE_MAP_IMAGES_BOUNDS_JSON") or "").strip()
        except RuntimeError:
            raw = ""
    if raw:
        try:
            arr = json.loads(raw)
            if isinstance(arr, list) and len(arr) == 2:
                return arr
        except Exception:
            pass
    now = time.time()
    ttl = max(60.0, _cad_metoffice_map_images_preview_cache_seconds())
    with _cad_metoffice_map_images_preview_lock:
        c = _cad_metoffice_map_images_preview_cache
        b = c.get("bounds")
        if (
            isinstance(b, list)
            and len(b) == 2
            and isinstance(b[0], list)
            and isinstance(b[1], list)
            and (now - float(c.get("order_ts") or 0.0)) < ttl
        ):
            return b
    return [[33.0, -14.0], [68.0, 37.0]]


def _cad_metoffice_weather_image_overlay_url_for_dashboard():
    """Same-origin PNG when map-images is used (no public XYZ tile URL)."""
    if _cad_metoffice_leaflet_tile_url_for_dashboard():
        return None
    if not _cad_metoffice_api_key():
        return None
    # Prefer absolute ``https://…/map/metoffice-preview.png`` so the browser never resolves a
    # path-only ``metoffice-preview.png`` beside ``/cad`` (drops ``/map/``).
    if has_request_context():
        try:
            scheme = (request.headers.get("X-Forwarded-Proto", request.scheme) or "").strip().lower()
            if scheme not in ("http", "https"):
                scheme = None
            kw = {"_external": True}
            if scheme:
                kw["_scheme"] = scheme
            return url_for("medical_response_internal.cad_map_metoffice_preview", **kw)
        except Exception:
            pass
    return _CAD_METOFFICE_MAP_PREVIEW_HTTP_PATH


def _cad_metoffice_leaflet_tile_url_for_dashboard():
    """Public tile URL for Leaflet: direct XYZ, or same-origin proxy when key + upstream template are set."""
    direct = (str(current_app.config.get('CAD_METOFFICE_GLOBAL_SPOT_TILE_URL') or '').strip()
              or os.environ.get('CAD_METOFFICE_GLOBAL_SPOT_TILE_URL', '').strip())
    if direct:
        return direct
    if _cad_metoffice_api_key() and _cad_metoffice_tile_upstream_template():
        # Leaflet needs literal ``{z}/{x}/{y}`` in the URL. ``url_for(..., z='{z}')`` fails on ``<int:z>`` routes.
        return '/plugin/ventus_response_module/map/metoffice-tile/{z}/{x}/{y}.png'
    return None


def _cad_metoffice_site_forecast_proxy_url_for_dashboard():
    """Same-origin base URL for site-specific forecast JSON (client appends ``lat`` / ``lng``)."""
    if not _cad_metoffice_site_specific_api_key():
        return None
    if has_request_context():
        try:
            scheme = (request.headers.get("X-Forwarded-Proto", request.scheme) or "").strip().lower()
            if scheme not in ("http", "https"):
                scheme = None
            kw = {"_external": True}
            if scheme:
                kw["_scheme"] = scheme
            return url_for("medical_response_internal.cad_map_metoffice_site_forecast", **kw)
        except Exception:
            pass
    return f"{_CAD_VENTUS_PLUGIN_HTTP_PATH}/map/metoffice-site-forecast"


@internal.route('/map/metoffice-tile/<int:z>/<int:x>/<int:y>.png', methods=['GET'])
@login_required
def cad_map_metoffice_tile(z, x, y):
    """Proxy raster tiles to Met Office (or compatible) upstream with server-held credentials.

    Set ``CAD_METOFFICE_API_KEY`` (JWT / bearer token) and ``CAD_METOFFICE_MAP_TILE_URL_TEMPLATE``
    (environment or Flask config). Template must include ``{z}``, ``{x}``, ``{y}`` placeholders
    matching the upstream service.

    Alternatively set ``CAD_METOFFICE_GLOBAL_SPOT_TILE_URL`` to a public XYZ URL and skip this proxy.
    """
    if not _ventus_dispatch_capable():
        abort(403)
    key = _cad_metoffice_api_key()
    tmpl = _cad_metoffice_tile_upstream_template()
    if not key or not tmpl:
        abort(503)
    if z < 0 or z > 24 or x < 0 or y < 0:
        abort(400)
    max_tiles = 1 << min(z, 24)
    if x >= max_tiles or y >= max_tiles:
        abort(400)
    try:
        upstream = tmpl.format(z=z, x=x, y=y, Z=z, X=x, Y=y)
    except (KeyError, ValueError, IndexError):
        logger.warning(
            'Met Office tile URL template failed for z=%s x=%s y=%s', z, x, y)
        abort(500)
    # Met Office official clients often send the subscription token as ``apikey``; JWT access also accepts Bearer.
    req = Request(
        upstream,
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Accept": "image/png,image/*,*/*",
            "User-Agent": "VentusCAD/1.0",
        },
        method="GET",
    )
    try:
        with urlopen(req, timeout=45) as resp:
            data = resp.read()
            ct = resp.headers.get('Content-Type', 'image/png')
    except HTTPError as e:
        logger.warning(
            'Met Office tile upstream HTTP %s for %s', e.code, upstream[:160])
        abort(502)
    except (URLError, OSError, TimeoutError) as e:
        logger.warning('Met Office tile upstream error: %s', e)
        abort(502)
    return Response(
        data,
        status=200,
        mimetype=ct.split(';')[0].strip() if ct else 'image/png',
        headers={
            'Cache-Control': 'public, max-age=300',
        },
    )


def _cad_map_metoffice_preview_impl():
    """Shared body for ``/map/metoffice-preview.png`` and legacy ``/metoffice-preview.png``."""
    if not _ventus_dispatch_capable():
        abort(403)
    if not _cad_metoffice_api_key():
        abort(503)
    raw_file = request.args.get("file") or request.args.get("fileId") or request.args.get("file_id")
    req = ""
    if isinstance(raw_file, str):
        req = raw_file.strip()
    strict_unknown = bool(req)
    png, _fid, err = _cad_metoffice_map_images_resolve_preview_png(
        requested_file_id=req or None,
        strict_unknown_file=strict_unknown,
    )
    if err == "unknown_file":
        abort(400)
    if not png:
        # Upstream DataHub unreachable, auth/quota failure, or empty order — see server logs.
        abort(503)
    return Response(
        png,
        status=200,
        mimetype='image/png',
        headers={
            'Cache-Control': 'public, max-age=300',
        },
    )


@internal.route('/map/metoffice-preview.png', methods=['GET'])
@login_required
def cad_map_metoffice_preview():
    """Proxy one latest map-images PNG (Weather DataHub); see MetOffice/weather_datahub_utilities."""
    return _cad_map_metoffice_preview_impl()


@internal.route('/metoffice-preview.png', methods=['GET'])
@login_required
def cad_metoffice_preview_wrong_path_alias():
    """Alias: ``metoffice-preview.png`` beside ``/cad`` resolves here without ``/map/``; same handler."""
    return _cad_map_metoffice_preview_impl()


@internal.route('/map/metoffice-site-forecast', methods=['GET'])
@login_required
def cad_map_metoffice_site_forecast():
    """Proxy Global Spot site-specific JSON (hourly by default); see MetOffice ``site_specific_download/ss_download.py``."""
    if not _ventus_dispatch_capable():
        abort(403)
    if not _cad_metoffice_site_specific_api_key():
        return jsonify({"ok": False, "error": "Met Office site-specific API key not configured"}), 503
    try:
        lat = float(request.args.get("lat", request.args.get("latitude", "")))
        lng = float(request.args.get("lng", request.args.get("longitude", "")))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "lat and lng must be numbers"}), 400
    if lat < -90 or lat > 90 or lng < -180 or lng > 180:
        return jsonify({"ok": False, "error": "lat/lng out of range"}), 400
    timesteps = (request.args.get("timesteps") or "hourly").strip().lower()
    if timesteps not in ("hourly", "three-hourly", "daily"):
        return jsonify({"ok": False, "error": "timesteps must be hourly, three-hourly, or daily"}), 400

    cache_lat = round(lat, 4)
    cache_lng = round(lng, 4)
    cache_key = (cache_lat, cache_lng, timesteps)
    now = time.time()
    ttl = max(30.0, _cad_metoffice_site_specific_cache_seconds())
    with _cad_metoffice_site_forecast_lock:
        ent = _cad_metoffice_site_forecast_cache.get(cache_key)
        if ent and (now - ent.get("ts", 0)) < ttl:
            body = dict(ent.get("body") or {})
            body["cached"] = True
            return _jsonify_safe(body, 200)

    ds_explicit = _cad_metoffice_site_specific_data_source()
    if ds_explicit:
        attempts = [ds_explicit]
    else:
        # Prefer ``BD1`` (blended deterministic / “current best” run) per DataHub gateway; omit if unsupported.
        attempts = ["BD1", ""]

    last_resp = None
    chosen_ds = None
    for ds in attempts:
        try:
            resp = _cad_metoffice_site_forecast_fetch_raw(
                cache_lat, cache_lng, timesteps, ds)
        except requests.RequestException as exc:
            logger.warning("Met Office site-forecast upstream error: %s", exc)
            return jsonify({"ok": False, "error": "Upstream request failed"}), 502
        last_resp = resp
        if resp.status_code == 200:
            chosen_ds = ds or None
            break
        if resp.status_code == 400 and not ds_explicit and ds == "BD1":
            continue
        try:
            detail = (resp.text or "")[:500]
        except Exception:
            detail = ""
        logger.warning(
            "Met Office site-forecast HTTP %s (dataSource=%r): %s",
            resp.status_code, ds or None, detail)
        return jsonify({
            "ok": False,
            "error": f"Met Office returned HTTP {resp.status_code}",
        }), 502
    else:
        return jsonify({"ok": False, "error": "Met Office site-forecast unavailable"}), 502

    try:
        data = last_resp.json()
    except ValueError:
        logger.warning("Met Office site-forecast: invalid JSON")
        return jsonify({"ok": False, "error": "Invalid JSON from Met Office"}), 502

    summary = _cad_metoffice_site_forecast_summarize_json(data)
    summary["dataSource"] = chosen_ds
    summary["timesteps"] = timesteps
    summary["cached"] = False
    with _cad_metoffice_site_forecast_lock:
        _cad_metoffice_site_forecast_cache[cache_key] = {"ts": now, "body": dict(summary)}
    return _jsonify_safe(summary, 200)


@internal.route('/map/w3w-grid-section', methods=['GET'])
@login_required
def cad_map_w3w_grid_section():
    """what3words v3 grid-section lines for CAD overlay (bounding box built server-side)."""
    if not _ventus_dispatch_capable():
        return jsonify({'error': 'Unauthorised'}), 403
    try:
        clat = float(request.args.get('center_lat', request.args.get('lat', '')))
        clng = float(request.args.get('center_lng', request.args.get('lng', '')))
    except (TypeError, ValueError):
        return jsonify({'error': 'center_lat and center_lng must be numbers'}), 400
    result = ResponseTriage.get_w3w_grid_section_lines(clat, clng)
    if result.get('error'):
        return jsonify({'error': result['error']}), 400
    lines = result.get('lines')
    if not isinstance(lines, list):
        lines = []
    return _jsonify_safe({'lines': lines}, 200)


@internal.route('/unit/<callsign>/standby', methods=['POST'])
@internal.route('/unit/<callsign>/destination', methods=['POST'])
@login_required
def unit_set_standby(callsign):
    allowed_roles = ["dispatcher", "admin",
                     "superuser", "clinical_lead", "controller"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    callsign = str(callsign or '').strip().upper()
    payload = request.get_json() or {}
    instruction_type = str(payload.get('instruction_type') or payload.get(
        'type') or 'standby_location').strip().lower()
    if instruction_type not in {'standby_location', 'transport_destination'}:
        instruction_type = 'standby_location'
    destination_type = 'transport' if instruction_type == 'transport_destination' else 'standby'
    activation_mode = str(payload.get('activation_mode')
                          or payload.get('activate_on') or '').strip().lower()
    if activation_mode not in {'immediate', 'on_leave_scene'}:
        activation_mode = 'on_leave_scene' if destination_type == 'transport' else 'immediate'
    destination_state = 'pending' if activation_mode == 'on_leave_scene' else 'active'
    cad = payload.get('cad')
    try:
        cad = int(cad) if cad not in (None, '', 'null') else None
    except Exception:
        cad = None
    source = str(payload.get('source') or 'manual').strip().lower()
    if source not in {'manual', 'map_click', 'preset', 'what3words', 'address', 'crew_override', 'location_search'}:
        source = 'manual'
    name = str(payload.get('name') or payload.get(
        'location_name') or '').strip()
    lat = payload.get('lat', payload.get('latitude'))
    lng = payload.get('lng', payload.get('longitude'))
    w3w_in = str(payload.get('what3words') or payload.get('w3w') or '').strip()
    what3words = (
        ResponseTriage.normalize_w3w_words(w3w_in)
        if w3w_in
        else ""
    ) or None
    address = str(payload.get('address') or payload.get(
        'location_text') or '').strip()
    postcode = str(payload.get('postcode') or '').strip()
    if not name:
        name = 'Transport Destination' if destination_type == 'transport' else 'Standby'
    try:
        lat = float(lat)
        lng = float(lng)
    except Exception:
        lat = None
        lng = None
    if lat is None or lng is None:
        resolved = ResponseTriage.get_best_lat_lng(
            address=address or None,
            postcode=postcode or None,
            what3words=what3words or None
        )
        if isinstance(resolved, dict) and resolved.get("lat") is not None and resolved.get("lng") is not None:
            try:
                lat = float(resolved.get("lat"))
                lng = float(resolved.get("lng"))
                if what3words and source == 'manual':
                    source = 'what3words'
                elif address and source == 'manual':
                    source = 'address'
            except Exception:
                lat = None
                lng = None
    if lat is None or lng is None:
        return jsonify({'error': 'lat/lng required (or resolvable what3words/address/postcode)'}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_standby_tables(cur)
        cur.execute(
            "SELECT callSign FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (callsign,))
        if cur.fetchone() is None:
            return jsonify({'error': 'Unit not found'}), 404
        operator = str(getattr(current_user, 'username', '') or '').strip()
        sender_name = f"Dispatcher ({operator})" if operator else "Dispatcher"
        instruction_payload = {
            'name': name,
            'lat': lat,
            'lng': lng,
            'source': source,
            'destination_type': destination_type,
            'activation_mode': activation_mode,
            'state': destination_state,
            'cad': cad,
            'what3words': what3words or None,
            'address': address or None,
            'postcode': postcode or None
        }
        cur.execute("""
            INSERT INTO mdt_dispatch_instructions (callSign, instruction_type, payload, status, created_by)
            VALUES (%s, %s, %s, 'pending', %s)
        """, (callsign, instruction_type, json.dumps(instruction_payload), operator or None))
        instruction_id = cur.lastrowid
        _supersede_stale_destination_instructions(
            cur, callsign, instruction_id)
        cur.execute("""
            INSERT INTO standby_locations
              (callSign, name, lat, lng, source, destinationType, activationMode, state, cad,
               what3words, address, instructionId, activatedAt, updatedBy)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                lat = VALUES(lat),
                lng = VALUES(lng),
                source = VALUES(source),
                destinationType = VALUES(destinationType),
                activationMode = VALUES(activationMode),
                state = VALUES(state),
                cad = VALUES(cad),
                what3words = VALUES(what3words),
                address = VALUES(address),
                instructionId = VALUES(instructionId),
                activatedAt = VALUES(activatedAt),
                updatedBy = VALUES(updatedBy),
                updatedAt = CURRENT_TIMESTAMP
        """, (
            callsign, name, lat, lng, source, destination_type, activation_mode, destination_state, cad,
            what3words or None, address or None, instruction_id,
            datetime.utcnow() if destination_state == 'active' else None,
            operator or 'unknown'
        ))
        cur.execute("SHOW TABLES LIKE 'messages'")
        has_messages = cur.fetchone() is not None
        if has_messages:
            detail_bits = [name, f"{lat:.6f},{lng:.6f}"]
            if what3words:
                detail_bits.append(f"w3w:{what3words}")
            if address:
                detail_bits.append(address)
            if instruction_type == 'transport_destination':
                msg_text = f"Transport destination ({activation_mode}): {' | '.join(detail_bits)}"
            else:
                msg_text = f"Standby instruction: {' | '.join(detail_bits)}"
            cur.execute("""
                INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                VALUES (%s, %s, %s, NOW(), 0)
            """, (sender_name, callsign, msg_text))
        conn.commit()
        try:
            socketio.emit('mdt_event', {
                'type': 'unit_standby_updated',
                'callsign': callsign,
                'name': name,
                'lat': lat,
                'lng': lng,
                'source': source,
                'destination_type': destination_type,
                'activation_mode': activation_mode,
                'state': destination_state,
                'cad': cad,
                'what3words': what3words or None,
                'address': address or None,
                'instruction_id': instruction_id
            })
            socketio.emit('mdt_event', {
                'type': 'dispatch_instruction',
                'callsign': callsign,
                'instruction_id': instruction_id,
                'instruction_type': instruction_type
            })
            socketio.emit(
                'mdt_event', {'type': 'units_updated', 'callsign': callsign})
        except Exception:
            pass
        return jsonify({
            'message': ('Transport destination instruction sent' if instruction_type == 'transport_destination' else 'Standby location instruction sent'),
            'callsign': callsign,
            'instruction_id': instruction_id,
            'instruction_type': instruction_type,
            'standby': {
                'name': name,
                'lat': lat,
                'lng': lng,
                'source': source,
                'destination_type': destination_type,
                'activation_mode': activation_mode,
                'state': destination_state,
                'cad': cad,
                'what3words': what3words or None,
                'address': address or None
            }
        }), 200
    finally:
        cur.close()
        conn.close()


@internal.route('/unit/<callsign>/meal-break', methods=['POST'])
@login_required
def unit_meal_break(callsign):
    allowed_roles = ["dispatcher", "admin",
                     "superuser", "clinical_lead", "controller"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    callsign = str(callsign or '').strip().upper()
    payload = request.get_json() or {}
    action = str(payload.get('action') or 'start').strip().lower()
    if action not in ('start', 'stop', 'cancel'):
        action = 'start'
    minutes_raw = payload.get('minutes')

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        operator = str(getattr(current_user, 'username', '') or '').strip()
        sender_name = f"Dispatcher ({operator})" if operator else "dispatcher"
        _ensure_meal_break_columns(cur)
        _ventus_set_g_break_policy(cur)
        pol = _get_dispatch_break_policy(cur)
        if minutes_raw is None or str(minutes_raw).strip() == '':
            minutes = int(pol.get('meal_break_default_minutes') or 45)
        else:
            minutes = int(minutes_raw)
        if minutes < 5:
            minutes = 5
        if minutes > 180:
            minutes = 180
        cur.execute("""
            SELECT callSign, status, signOnTime, mealBreakStartedAt, mealBreakUntil, mealBreakTakenAt,
                   shiftStartAt, shiftEndAt, shiftDurationMins, breakDueAfterMins
            FROM mdts_signed_on
            WHERE callSign = %s
            LIMIT 1
        """, (callsign,))
        row0 = cur.fetchone()
        if row0 is None:
            return jsonify({'error': 'Unit not found'}), 404
        shift_before = _compute_shift_break_state(row0)

        if action in ('stop', 'cancel'):
            end_msg = (
                'Meal break cancelled by dispatch. Return to job-ready status.'
                if action == 'cancel'
                else 'Meal break ended. Return to job-ready status.'
            )
            cur.execute("""
                UPDATE mdts_signed_on
                   SET status = 'on_standby',
                       mealBreakStartedAt = NULL,
                       mealBreakUntil = NULL,
                       mealBreakTakenAt = COALESCE(mealBreakTakenAt, NOW())
                 WHERE callSign = %s
            """, (callsign,))
            cur.execute("""
                INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                VALUES (%s, %s, %s, NOW(), 0)
            """, (sender_name, callsign, end_msg))
            msg = 'Meal break cancelled' if action == 'cancel' else 'Meal break ended'
        else:
            if shift_before.get('meal_break_active'):
                return jsonify({'error': 'Unit is already on an active meal break'}), 409
            cur.execute("""
                UPDATE mdts_signed_on
                   SET status = 'meal_break',
                       mealBreakStartedAt = NOW(),
                       mealBreakUntil = DATE_ADD(NOW(), INTERVAL %s MINUTE),
                       mealBreakTakenAt = COALESCE(mealBreakTakenAt, NOW())
                 WHERE callSign = %s
            """, (minutes, callsign))
            cur.execute("""
                INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                VALUES (%s, %s, %s, NOW(), 0)
            """, (sender_name, callsign, f'Meal break started for {minutes} minute(s). Status set to meal_break.'))
            msg = 'Meal break started'
        cur.execute("""
            SELECT callSign, status, signOnTime, mealBreakStartedAt, mealBreakUntil, mealBreakTakenAt,
                   shiftStartAt, shiftEndAt, shiftDurationMins, breakDueAfterMins
            FROM mdts_signed_on
            WHERE callSign = %s
            LIMIT 1
        """, (callsign,))
        state_row = cur.fetchone() or {}
        shift_state = _compute_shift_break_state(state_row)
        conn.commit()
        try:
            socketio.emit('mdt_event', {
                          'type': 'unit_meal_break', 'callsign': callsign, 'action': action, 'minutes': minutes})
            socketio.emit('mdt_event', {
                'type': 'status_update',
                'callsign': callsign,
                'status': ('on_standby' if action in ('stop', 'cancel') else 'meal_break'),
            })
            _meal_msg = (
                ('Meal break cancelled by dispatch. Return to job-ready status.' if action ==
                 'cancel' else 'Meal break ended. Return to job-ready status.')
                if action in ('stop', 'cancel')
                else f'Meal break started for {minutes} minute(s). Status set to meal_break.'
            )
            socketio.emit('mdt_event', {
                          'type': 'message_posted', 'from': sender_name, 'to': callsign, 'text': _meal_msg})
            _mdt_web_push_notify_callsign(
                callsign,
                'Ventus MDT',
                _meal_msg,
                tag='meal-break',
                alert=False,
                silent=False,
                require_interaction=False,
            )
            socketio.emit('mdt_event', {
                'type': 'meal_break_state',
                'callsign': callsign,
                'meal_break_started_at': shift_state.get('meal_break_started_at'),
                'meal_break_until': shift_state.get('meal_break_until'),
                'meal_break_remaining_seconds': shift_state.get('meal_break_remaining_seconds'),
                'meal_break_active': shift_state.get('meal_break_active')
            })
            socketio.emit(
                'mdt_event', {'type': 'units_updated', 'callsign': callsign})
        except Exception:
            pass
        return _jsonify_safe({
            'message': msg,
            'callsign': callsign,
            'action': action,
            'minutes': minutes,
            'meal_break_started_at': shift_state.get('meal_break_started_at'),
            'meal_break_until': shift_state.get('meal_break_until'),
            'meal_break_taken_at': shift_state.get('meal_break_taken_at'),
            'meal_break_remaining_seconds': shift_state.get('meal_break_remaining_seconds'),
            'meal_break_active': shift_state.get('meal_break_active'),
            'shift_start_at': shift_state.get('shift_start_at'),
            'shift_end_at': shift_state.get('shift_end_at'),
            'shift_elapsed_minutes': shift_state.get('shift_elapsed_minutes'),
            'shift_remaining_minutes': shift_state.get('shift_remaining_minutes'),
            'break_due': shift_state.get('break_due'),
            'break_due_in_minutes': shift_state.get('break_due_in_minutes')
        }, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/unit/<callsign>/force-signoff', methods=['POST'])
@login_required
def unit_force_signoff(callsign):
    allowed_roles = ["dispatcher", "admin",
                     "superuser", "clinical_lead", "controller"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403
    callsign = str(callsign or '').strip().upper()
    if not callsign:
        return jsonify({'error': 'callsign required'}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        _ensure_standby_tables(cur)
        dcur = conn.cursor(dictionary=True)
        try:
            dcur.execute(
                """
                SELECT crew,
                       LOWER(TRIM(COALESCE(division, 'general'))) AS division
                FROM mdts_signed_on WHERE callSign = %s LIMIT 1
                """,
                (callsign,),
            )
            urow = dcur.fetchone() or {}
        finally:
            dcur.close()
        crew_force = list(_mdt_signed_on_crew_usernames(urow.get('crew')))
        div_force = str(urow.get('division') or 'general').strip().lower() or 'general'
        cur.execute(
            "DELETE FROM mdts_signed_on WHERE callSign = %s", (callsign,))
        if cur.rowcount == 0:
            return jsonify({'error': 'Unit not found'}), 404
        _record_mdt_session_lifecycle_event(
            cur,
            event_type='force_sign_off',
            callsign=callsign,
            division=div_force,
            actor_username=str(
                getattr(current_user, 'username', '') or '').strip() or None,
            previous_callsign=None,
            crew_usernames=crew_force,
            detail={'source': 'dispatcher_force_signoff'},
        )
        try:
            cur.execute(
                "DELETE FROM standby_locations WHERE callSign = %s", (callsign,))
        except Exception:
            pass
        try:
            _ensure_callsign_redirect_table(cur)
            cur.execute(
                "DELETE FROM mdt_callsign_redirect WHERE from_callsign = %s OR to_callsign = %s",
                (callsign, callsign),
            )
        except Exception:
            pass
        conn.commit()
        try:
            socketio.emit(
                'mdt_event', {'type': 'unit_signoff', 'callsign': callsign})
            socketio.emit(
                'mdt_event', {'type': 'units_updated', 'callsign': callsign})
        except Exception:
            pass
        return jsonify({'message': 'Unit force signed off', 'callsign': callsign}), 200
    finally:
        cur.close()
        conn.close()


def _normalize_dispatch_callsign_token(raw):
    """Uppercase alnum + underscore/hyphen, max 64 chars (matches signed-on storage)."""
    s = re.sub(r'[^A-Za-z0-9_-]', '', str(raw or '').strip()).upper()
    return s[:64]


def _rename_signed_unit_callsign_rows(cur, old_cs, new_cs):
    """Rename operational rows; caller must ensure old_cs exists and new_cs is free on mdts_signed_on."""
    cur.execute(
        "UPDATE mdt_job_units SET callsign = %s WHERE callsign = %s", (new_cs, old_cs))
    cur.execute(
        "UPDATE standby_locations SET callSign = %s WHERE callSign = %s", (new_cs, old_cs))
    cur.execute(
        "UPDATE mdt_dispatch_instructions SET callSign = %s WHERE callSign = %s", (new_cs, old_cs))
    cur.execute(
        "UPDATE mdt_response_log SET callSign = %s WHERE callSign = %s", (new_cs, old_cs))
    cur.execute(
        "UPDATE mdt_dispatch_assist_requests SET callsign = %s WHERE callsign = %s", (new_cs, old_cs))
    cur.execute(
        "UPDATE mdt_crew_removal_log SET callSign = %s WHERE callSign = %s", (new_cs, old_cs))
    try:
        cur.execute(
            "UPDATE mdt_session_lifecycle_event SET callsign = %s WHERE callsign = %s",
            (new_cs, old_cs),
        )
    except Exception:
        pass
    cur.execute(
        "UPDATE mdts_signed_on SET callSign = %s WHERE callSign = %s", (new_cs, old_cs))
    if cur.rowcount != 1:
        raise RuntimeError('signed-on callsign update failed')


def _rename_callsign_optional_tables(cur, old_cs, new_cs):
    for sql, args in (
        ("UPDATE mdt_positions SET callSign = %s WHERE callSign = %s", (new_cs, old_cs)),
        ("UPDATE messages SET recipient = %s WHERE LOWER(TRIM(recipient)) = LOWER(%s)", (new_cs, old_cs)),
    ):
        try:
            cur.execute(sql, args)
        except Exception as ex:
            try:
                logger.warning('callsign rename optional skipped: %s', ex)
            except Exception:
                pass


@internal.route('/unit/<callsign>/change-callsign', methods=['POST'])
@login_required
def unit_change_callsign(callsign):
    allowed_roles = ["dispatcher", "admin",
                     "superuser", "clinical_lead", "controller"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    old_cs = str(callsign or '').strip().upper()
    payload = request.get_json(silent=True) or {}
    new_cs = _normalize_dispatch_callsign_token(
        payload.get('new_callsign') if payload.get(
            'new_callsign') is not None else payload.get('callsign')
    )
    if not old_cs:
        return jsonify({'error': 'callsign required'}), 400
    if not new_cs:
        return jsonify({'error': 'new_callsign required'}), 400
    if new_cs == old_cs:
        return jsonify({'error': 'Callsign unchanged'}), 400
    if not re.match(r'^[A-Z0-9][A-Z0-9_-]{0,63}$', new_cs):
        return jsonify({'error': 'Invalid callsign (use letters, digits, hyphen or underscore; max 64)'}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        _ensure_standby_tables(cur)
        _ensure_job_units_table(cur)
        _ensure_response_log_table(cur)
        _ensure_crew_removal_log_table(cur)
        _ensure_assist_requests_table(cur)

        cur.execute(
            "SELECT callSign FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (old_cs,))
        if cur.fetchone() is None:
            return jsonify({'error': 'Unit not found'}), 404
        cur.execute(
            "SELECT callSign FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (new_cs,))
        if cur.fetchone() is not None:
            return jsonify({'error': 'Callsign already in use'}), 409

        _rename_signed_unit_callsign_rows(cur, old_cs, new_cs)
        _rename_callsign_optional_tables(cur, old_cs, new_cs)
        _ensure_callsign_redirect_table(cur)
        cur.execute(
            "UPDATE mdt_callsign_redirect SET to_callsign = %s WHERE to_callsign = %s",
            (new_cs, old_cs),
        )
        cur.execute(
            """
            INSERT INTO mdt_callsign_redirect (from_callsign, to_callsign)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE to_callsign = VALUES(to_callsign)
            """,
            (old_cs, new_cs),
        )
        conn.commit()

        uname_cc = str(getattr(current_user, 'username', '') or 'unknown').strip() or 'unknown'
        try:
            log_audit(
                uname_cc,
                'dispatch_unit_callsign_changed',
                details={'old_callsign': old_cs, 'new_callsign': new_cs},
            )
        except Exception:
            pass

        sender_name = _sender_label_from_portal(
            'dispatch', getattr(current_user, 'username', ''))
        notify_text = (
            f"DISPATCH: Your callsign has been changed to {new_cs}. "
            f"Use this callsign on your MDT (previous: {old_cs})."
        )
        message_inserted = False
        try:
            cur.execute("""
                INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                VALUES (%s, %s, %s, NOW(), 0)
            """, (sender_name, new_cs, notify_text))
            conn.commit()
            message_inserted = True
        except Exception as ex:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                logger.warning(
                    'callsign change: inbox message insert failed: %s', ex)
            except Exception:
                pass

        try:
            _cc_payload = {
                'type': 'callsign_changed',
                'old_callsign': old_cs,
                'new_callsign': new_cs,
                'callSign': new_cs,
                'callsign': new_cs,
                'previousCallSign': old_cs,
                'applyCallSign': new_cs,
                'callsignRemappedByServer': True,
            }
            socketio.emit('mdt_event', _cc_payload)
            # MDT sockets that joined mdt_callsign_<old_cs> on connect apply session here
            # (avoids relying on a single localStorage key matching the old callsign).
            _cc_apply = dict(_cc_payload)
            _cc_apply['clientSessionApply'] = True
            socketio.emit('mdt_event', _cc_apply,
                          room=f'mdt_callsign_{old_cs}')
            if message_inserted:
                socketio.emit('mdt_event', {
                    'type': 'message_posted',
                    'from': str(sender_name or 'dispatcher'),
                    'to': new_cs,
                    'text': notify_text,
                })
                _mdt_web_push_notify_callsign(
                    new_cs,
                    'Callsign updated',
                    notify_text,
                    tag='callsign-change',
                    alert=False,
                    silent=False,
                    require_interaction=False,
                )
            socketio.emit(
                'mdt_event', {'type': 'units_updated', 'callsign': new_cs})
        except Exception:
            pass
        return jsonify({
            'message': 'Callsign updated',
            'old_callsign': old_cs,
            'new_callsign': new_cs,
        }), 200
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            logger.exception('unit_change_callsign failed: %s', e)
        except Exception:
            pass
        return jsonify({'error': 'Failed to update callsign'}), 500
    finally:
        cur.close()
        conn.close()


@internal.route('/unit/<callsign>/change-shift-times', methods=['POST'])
@login_required
def unit_change_shift_times(callsign):
    """Dispatcher correction: update signed-on unit shift window (mdts_signed_on)."""
    allowed_roles = ["dispatcher", "admin",
                     "superuser", "clinical_lead", "controller"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    cs = str(callsign or '').strip().upper()
    if not cs:
        return jsonify({'error': 'callsign required'}), 400
    payload = request.get_json(silent=True) or {}
    raw_start = payload.get('shift_start')
    raw_end = payload.get('shift_end')
    if raw_start is None and raw_end is None:
        return jsonify({'error': 'shift_start and/or shift_end required'}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_mdts_signed_on_schema(cur)
        cur.execute(
            """
            SELECT callSign, signOnTime, shiftStartAt, shiftEndAt, shiftDurationMins
            FROM mdts_signed_on WHERE callSign = %s LIMIT 1
            """,
            (cs,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Unit not found'}), 404

        old_start = _coerce_datetime(row.get('shiftStartAt'))
        old_end = _coerce_datetime(row.get('shiftEndAt'))
        sign_on = _coerce_datetime(row.get('signOnTime'))

        new_start = _coerce_datetime(
            raw_start) if raw_start is not None and str(raw_start).strip() != '' else old_start
        new_end = _coerce_datetime(
            raw_end) if raw_end is not None and str(raw_end).strip() != '' else old_end

        if new_start is None:
            new_start = old_start or sign_on
        if new_end is None:
            new_end = old_end

        if new_start is None or new_end is None:
            return jsonify({'error': 'Could not resolve shift start and end times'}), 400
        if new_end <= new_start:
            return jsonify({'error': 'Shift end must be after shift start'}), 400
        span_mins = int((new_end - new_start).total_seconds() // 60)
        if span_mins < 15:
            return jsonify({'error': 'Shift must be at least 15 minutes'}), 400
        if span_mins > 48 * 60:
            return jsonify({'error': 'Shift longer than 48 hours is not allowed'}), 400

        def _epoch_minutes(dt):
            if dt is None:
                return None
            try:
                return int(dt.timestamp() // 60)
            except Exception:
                return None

        if (_epoch_minutes(old_start) == _epoch_minutes(new_start)
                and _epoch_minutes(old_end) == _epoch_minutes(new_end)):
            return jsonify({
                'message': 'Shift times unchanged',
                'callsign': cs,
                'shift_start': new_start,
                'shift_end': new_end,
            }), 200

        operator = str(getattr(current_user, 'username', '') or '').strip()
        cur.execute(
            """
            UPDATE mdts_signed_on
            SET shiftStartAt = %s, shiftEndAt = %s, shiftDurationMins = %s
            WHERE callSign = %s
            """,
            (new_start, new_end, span_mins, cs),
        )
        _insert_unit_dispatch_adjustment_response_log(
            cur,
            cs,
            _RESPONSE_LOG_STATUS_DISPATCH_SHIFT_TIMES_CHANGED,
            {
                'dispatch_note': True,
                'by': operator,
                'old_shift_start': old_start,
                'old_shift_end': old_end,
                'new_shift_start': new_start,
                'new_shift_end': new_end,
            },
        )
        conn.commit()

        uname = str(getattr(current_user, 'username', '') or 'unknown').strip() or 'unknown'
        try:
            log_audit(
                uname,
                'dispatch_unit_shift_times_changed',
                details={
                    'callsign': cs,
                    'old_shift_start': old_start,
                    'old_shift_end': old_end,
                    'new_shift_start': new_start,
                    'new_shift_end': new_end,
                },
            )
        except Exception:
            pass

        def _fmt_shift_dt(dt):
            if not dt:
                return '—'
            try:
                return dt.strftime('%Y-%m-%d %H:%M')
            except Exception:
                return str(dt)

        sender_name = _sender_label_from_portal(
            'dispatch', getattr(current_user, 'username', ''))
        notify_text = (
            f"DISPATCH: Your shift times were updated by dispatch. "
            f"Shift now runs {_fmt_shift_dt(new_start)} – {_fmt_shift_dt(new_end)}."
        )
        message_inserted = False
        try:
            cur.execute("""
                INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                VALUES (%s, %s, %s, NOW(), 0)
            """, (sender_name, cs, notify_text))
            conn.commit()
            message_inserted = True
        except Exception as ex:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                logger.warning(
                    'shift times change: inbox message insert failed: %s', ex)
            except Exception:
                pass

        try:
            socketio.emit('mdt_event', {
                'type': 'dispatch_shift_updated',
                'callsign': cs,
                'shift_start': new_start.replace(microsecond=0).isoformat(sep='T'),
                'shift_end': new_end.replace(microsecond=0).isoformat(sep='T'),
            })
            if message_inserted:
                socketio.emit('mdt_event', {
                    'type': 'message_posted',
                    'from': str(sender_name or 'dispatcher'),
                    'to': cs,
                    'text': notify_text,
                })
                _mdt_web_push_notify_callsign(
                    cs,
                    'Shift times updated',
                    notify_text,
                    tag='shift-times-change',
                    alert=False,
                    silent=False,
                    require_interaction=False,
                )
            socketio.emit('mdt_event', {'type': 'units_updated', 'callsign': cs})
        except Exception:
            pass

        return jsonify({
            'message': 'Shift times updated',
            'callsign': cs,
            'shift_start': new_start.replace(microsecond=0).isoformat(sep='T'),
            'shift_end': new_end.replace(microsecond=0).isoformat(sep='T'),
            'shift_duration_minutes': span_mins,
        }), 200
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            logger.exception('unit_change_shift_times failed: %s', e)
        except Exception:
            pass
        return jsonify({'error': 'Failed to update shift times'}), 500
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/divisions/manage', methods=['GET', 'POST', 'DELETE'])
@login_required
def dispatch_divisions_manage():
    """Admin management for division catalog (create/update/archive/default)."""
    edit_roles = ["admin", "superuser", "clinical_lead"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in edit_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if request.method == 'GET':
            items = _list_dispatch_divisions(
                cur, include_inactive=True, filter_event_window=False)
            default_division = _get_dispatch_default_division(cur)
            for item in items:
                item['is_default'] = (item['slug'] == default_division)
            items.sort(key=lambda x: (
                0 if x['is_default'] else 1, 0 if x['is_active'] else 1, x['name'].lower(), x['slug']))
            return jsonify({'items': items, 'default': default_division})

        if request.method == 'DELETE':
            payload = request.get_json(silent=True) or {}
            slug_del = _normalize_division(
                payload.get('slug') or request.args.get('slug'),
                fallback='',
            )
            if not slug_del:
                return jsonify({'error': 'slug required'}), 400
            try:
                _ensure_dispatch_divisions_table(cur)
                _delete_dispatch_division_row(
                    cur, slug_del, updated_by=getattr(
                        current_user, 'username', 'unknown')
                )
                conn.commit()
                return jsonify({'message': 'Division deleted', 'slug': slug_del})
            except ValueError as exc:
                msg = str(exc or 'delete failed')
                try:
                    conn.rollback()
                except Exception:
                    pass
                if 'not found' in msg.lower():
                    return jsonify({'error': msg}), 404
                return jsonify({'error': msg}), 400
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    logger.exception(
                        'dispatch_divisions_manage DELETE failed: %s', exc)
                except Exception:
                    pass
                return jsonify({'error': 'Delete failed (database error)'}), 500

        payload = request.get_json(silent=True) or {}
        _act = str(payload.get('action') or payload.get(
            'operation') or '').strip().lower()
        # delete / remove / action=delete (some clients mishandle a JSON key named "delete").
        if _act in ('delete', 'remove') or bool(payload.get('delete')) or bool(payload.get('remove')):
            slug_del = _normalize_division(payload.get('slug'), fallback='')
            if not slug_del:
                return jsonify({'error': 'slug required'}), 400
            try:
                _ensure_dispatch_divisions_table(cur)
                _delete_dispatch_division_row(
                    cur, slug_del, updated_by=getattr(
                        current_user, 'username', 'unknown')
                )
                conn.commit()
                return jsonify({'message': 'Division deleted', 'slug': slug_del})
            except ValueError as exc:
                msg = str(exc or 'delete failed')
                try:
                    conn.rollback()
                except Exception:
                    pass
                if 'not found' in msg.lower():
                    return jsonify({'error': msg}), 404
                return jsonify({'error': msg}), 400
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    logger.exception(
                        'dispatch_divisions_manage delete failed: %s', exc)
                except Exception:
                    pass
                return jsonify({'error': 'Delete failed (database error)'}), 500

        slug = _normalize_division(payload.get('slug'), fallback='')
        if not slug:
            return jsonify({'error': 'slug required'}), 400
        name = str(payload.get('name') or '').strip(
        ) or slug.replace('_', ' ').title()
        color = _normalize_hex_color(payload.get('color'), '#64748b')
        is_active = bool(payload.get('is_active', True))
        is_default = bool(payload.get('is_default', False))
        if slug == 'general':
            name = name or 'General'
            is_active = True
        if is_default:
            is_active = True

        previous_slug = _normalize_division(
            payload.get('previous_slug'), fallback='')
        ia = 1 if is_active else 0
        idef = 1 if is_default else 0
        actor = getattr(current_user, 'username', 'unknown')

        _ensure_dispatch_divisions_table(cur)
        _upgrade_mdt_dispatch_divisions_map_icon(cur)

        rename_mode = bool(previous_slug and previous_slug != slug)
        icon_lookup_slug = previous_slug if rename_mode else slug
        prev_stored_icon = None
        try:
            cur.execute(
                "SELECT map_icon_url FROM mdt_dispatch_divisions WHERE slug = %s LIMIT 1",
                (icon_lookup_slug,),
            )
            pir = cur.fetchone()
            if pir:
                pv = pir.get('map_icon_url') if isinstance(pir, dict) else pir[0]
                prev_stored_icon = str(pv).strip() if pv else None
        except Exception:
            prev_stored_icon = None

        if 'map_icon_url' in payload:
            map_icon_url = _sanitize_client_division_map_icon_url(
                payload.get('map_icon_url'))
        else:
            map_icon_url = prev_stored_icon

        if previous_slug and previous_slug != slug:
            try:
                if is_default:
                    cur.execute(
                        "UPDATE mdt_dispatch_divisions SET is_default = 0")
                _rename_dispatch_division_slug(
                    cur, previous_slug, slug, name, color, ia, idef, updated_by=actor
                )
                if 'map_icon_url' in payload:
                    cur.execute(
                        """
                        UPDATE mdt_dispatch_divisions
                        SET map_icon_url = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE slug = %s
                        """,
                        (map_icon_url, slug),
                    )
                if is_default:
                    _set_dispatch_default_division(cur, slug, updated_by=actor)
                conn.commit()
                if 'map_icon_url' in payload and prev_stored_icon and prev_stored_icon != map_icon_url:
                    _try_remove_division_map_icon_file(prev_stored_icon)
                return jsonify({
                    'message': 'Division saved',
                    'division': {
                        'slug': slug, 'name': name, 'color': color,
                        'is_active': is_active, 'is_default': is_default,
                        'map_icon_url': map_icon_url,
                    }
                })
            except ValueError as exc:
                msg = str(exc or 'rename failed')
                if 'not found' in msg.lower():
                    return jsonify({'error': msg}), 404
                return jsonify({'error': msg}), 400

        if is_default:
            cur.execute("UPDATE mdt_dispatch_divisions SET is_default = 0")
        cur.execute("""
            INSERT INTO mdt_dispatch_divisions (slug, name, color, is_active, is_default, created_by, map_icon_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                color = VALUES(color),
                is_active = VALUES(is_active),
                is_default = VALUES(is_default),
                map_icon_url = VALUES(map_icon_url),
                updated_at = CURRENT_TIMESTAMP
        """, (
            slug, name, color, ia, idef, actor, map_icon_url
        ))
        if is_default:
            _set_dispatch_default_division(cur, slug, updated_by=actor)
        conn.commit()
        if 'map_icon_url' in payload and prev_stored_icon and prev_stored_icon != map_icon_url:
            _try_remove_division_map_icon_file(prev_stored_icon)
        return jsonify({
            'message': 'Division saved',
            'division': {
                'slug': slug, 'name': name, 'color': color,
                'is_active': is_active, 'is_default': is_default,
                'map_icon_url': map_icon_url,
            }
        })
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/division-map-icons/<safe_name>', methods=['GET'])
@login_required
def division_map_icon_file(safe_name):
    """Serve a processed division map/stack icon (login required)."""
    sn = str(safe_name or '').strip()
    if not re.match(r'^[a-f0-9]{32}\.png$', sn, re.I):
        abort(404)
    base = _ventus_division_map_icons_dir()
    path = os.path.normpath(os.path.join(base, sn.lower()))
    if not path.startswith(os.path.normpath(base + os.sep)):
        abort(404)
    if not os.path.isfile(path):
        abort(404)
    return send_file(path, mimetype='image/png')


@internal.route('/dispatch/divisions/map-icon', methods=['POST'])
@login_required
def dispatch_division_map_icon_upload():
    """Upload PNG/JPEG/WebP; returns a URL to store on the division row (Save division)."""
    edit_roles = ["admin", "superuser", "clinical_lead"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in edit_roles:
        return jsonify({'error': 'Unauthorised'}), 403
    f = request.files.get('file')
    if not f or not getattr(f, 'filename', None):
        return jsonify({'error': 'file required'}), 400
    try:
        fname, data = _process_uploaded_division_map_icon(f)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except Exception as exc:
        try:
            logger.warning('dispatch_division_map_icon_upload: %s', exc)
        except Exception:
            pass
        return jsonify({'error': 'Could not read image (use PNG, JPEG, or WebP)'}), 400
    dest = os.path.join(_ventus_division_map_icons_dir(), fname)
    try:
        with open(dest, 'wb') as wf:
            wf.write(data)
    except OSError as exc:
        return jsonify({'error': 'Could not store file: %s' % exc}), 500
    url = _DIVISION_MAP_ICON_ROUTE_PREFIX + fname
    return jsonify({'map_icon_url': url, 'message': 'Uploaded'})


@internal.route('/dispatch/divisions/default', methods=['POST'])
@login_required
def dispatch_set_default_division():
    """Set default focused dispatch division for all dashboards."""
    edit_roles = ["admin", "superuser", "clinical_lead"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in edit_roles:
        return jsonify({'error': 'Unauthorised'}), 403
    payload = request.get_json() or {}
    slug = _normalize_division(payload.get('slug'), fallback='')
    if not slug:
        return jsonify({'error': 'slug required'}), 400
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_dispatch_divisions_table(cur)
        cur.execute(
            "UPDATE mdt_dispatch_divisions SET is_default = CASE WHEN slug = %s THEN 1 ELSE 0 END", (slug,))
        _set_dispatch_default_division(
            cur, slug, updated_by=getattr(current_user, 'username', 'unknown'))
        conn.commit()
        return jsonify({'message': 'Default division updated', 'default': slug})
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/user-divisions/assignment-rows', methods=['GET'])
@login_required
def dispatch_user_division_assignment_rows():
    """Flat list of division assignments (and override-only users) for admin tables."""
    edit_roles = ["admin", "superuser", "clinical_lead"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in edit_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_dispatch_user_access_tables(cur)
        _ensure_dispatch_divisions_table(cur)
        conn.commit()
        cur.execute("""
            SELECT d.username, d.division, d.created_at, d.created_by,
                   COALESCE(s.can_override_all, 0) AS can_override_all,
                   dv.name AS division_name
            FROM mdt_dispatch_user_divisions d
            LEFT JOIN mdt_dispatch_user_settings s
              ON LOWER(TRIM(s.username)) = LOWER(TRIM(d.username))
            LEFT JOIN mdt_dispatch_divisions dv
              ON LOWER(TRIM(dv.slug)) = LOWER(TRIM(d.division))
            ORDER BY LOWER(d.username) ASC, d.division ASC
        """)
        assignments = []
        for r in cur.fetchall() or []:
            uname = str((r or {}).get('username') or '').strip()
            div = _normalize_division((r or {}).get('division'), fallback='')
            div_name = str((r or {}).get('division_name') or '').strip()
            label = div_name or div or '—'
            assignments.append({
                'username': uname,
                'division': div,
                'division_label': label,
                'assigned_at': _json_compatible(r.get('created_at')),
                'assigned_by': r.get('created_by'),
                'can_override_all': bool((r or {}).get('can_override_all')),
            })

        cur.execute("""
            SELECT s.username, s.updated_at, s.updated_by
            FROM mdt_dispatch_user_settings s
            WHERE COALESCE(s.can_override_all, 0) = 1
              AND NOT EXISTS (
                  SELECT 1 FROM mdt_dispatch_user_divisions d
                  WHERE LOWER(TRIM(d.username)) = LOWER(TRIM(s.username))
              )
            ORDER BY LOWER(s.username) ASC
        """)
        override_only = []
        for r in cur.fetchall() or []:
            uname = str((r or {}).get('username') or '').strip()
            if not uname:
                continue
            override_only.append({
                'username': uname,
                'division': None,
                'division_label': 'Cross-division override only',
                'assigned_at': _json_compatible(r.get('updated_at')),
                'assigned_by': r.get('updated_by'),
                'can_override_all': True,
            })
        return _jsonify_safe({'assignments': assignments, 'override_only': override_only}, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/user-divisions/can-override', methods=['POST'])
@login_required
def dispatch_user_division_set_can_override():
    """Set cross-division override for a user without changing division rows."""
    edit_roles = ["admin", "superuser", "clinical_lead"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in edit_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    payload = request.get_json(silent=True) or {}
    username_key = str(payload.get('username') or '').strip().lower()
    if not username_key:
        return jsonify({'error': 'username required'}), 400
    can_override = bool(payload.get('can_override_all', False))
    operator = str(getattr(current_user, 'username', '')
                   or '').strip() or 'unknown'

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        _ensure_dispatch_user_access_tables(cur)
        cur.execute("""
            INSERT INTO mdt_dispatch_user_settings (username, can_override_all, updated_by)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                can_override_all = VALUES(can_override_all),
                updated_by = VALUES(updated_by),
                updated_at = CURRENT_TIMESTAMP
        """, (username_key, 1 if can_override else 0, operator))
        conn.commit()
        return _jsonify_safe({
            'message': 'Cross-division override updated',
            'username': username_key,
            'can_override_all': can_override,
        }, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/user-divisions/remove-assignment', methods=['POST'])
@login_required
def dispatch_user_division_remove_assignment():
    """Remove one division row for a user, or clear cross-division override (override-only rows)."""
    edit_roles = ["admin", "superuser", "clinical_lead"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in edit_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    payload = request.get_json(silent=True) or {}
    username_key = str(payload.get('username') or '').strip().lower()
    if not username_key:
        return jsonify({'error': 'username required'}), 400
    kind = str(payload.get('kind') or '').strip().lower()
    if kind not in ('division', 'override_only'):
        return jsonify({'error': 'kind must be division or override_only'}), 400

    operator = str(getattr(current_user, 'username', '')
                   or '').strip() or 'unknown'
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        _ensure_dispatch_user_access_tables(cur)
        if kind == 'override_only':
            cur.execute("""
                UPDATE mdt_dispatch_user_settings
                SET can_override_all = 0, updated_by = %s, updated_at = CURRENT_TIMESTAMP
                WHERE LOWER(TRIM(username)) = %s
            """, (operator, username_key))
            conn.commit()
            if not cur.rowcount:
                return jsonify({'error': 'No override setting found for this user'}), 404
            return _jsonify_safe({
                'message': 'Cross-division override removed',
                'username': username_key,
            }, 200)

        division = _normalize_division(payload.get('division'), fallback='')
        if not division:
            return jsonify({'error': 'division required'}), 400
        cur.execute("""
            DELETE FROM mdt_dispatch_user_divisions
            WHERE LOWER(TRIM(username)) = %s AND LOWER(TRIM(division)) = %s
        """, (username_key, division))
        deleted = cur.rowcount or 0
        conn.commit()
        if not deleted:
            return jsonify({'error': 'Assignment not found'}), 404
        return _jsonify_safe({
            'message': 'Division assignment removed',
            'username': username_key,
            'division': division,
        }, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/user-divisions', methods=['GET', 'POST'])
@login_required
def dispatch_user_divisions():
    """Admin management of dispatcher/controller division ownership."""
    edit_roles = ["admin", "superuser", "clinical_lead"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in edit_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_dispatch_user_access_tables(cur)
        if request.method == 'GET':
            username = str(request.args.get('username') or '').strip()
            username_key = username.lower()
            if username:
                cur.execute("""
                    SELECT division
                    FROM mdt_dispatch_user_divisions
                    WHERE LOWER(username) = %s
                    ORDER BY division ASC
                """, (username_key,))
                rows = cur.fetchall() or []
                divisions = [_normalize_division(r.get(
                    'division'), fallback='') for r in rows if _normalize_division(r.get('division'), fallback='')]
                cur.execute(
                    "SELECT can_override_all FROM mdt_dispatch_user_settings WHERE LOWER(username) = %s LIMIT 1", (username_key,))
                s = cur.fetchone() or {}
                can_override_all = bool(s.get('can_override_all'))
                return jsonify({
                    'username': username,
                    'divisions': divisions,
                    'can_override_all': can_override_all
                })

            cur.execute("""
                SELECT d.username, d.division, COALESCE(s.can_override_all, 0) AS can_override_all
                FROM mdt_dispatch_user_divisions d
                LEFT JOIN mdt_dispatch_user_settings s ON s.username = d.username
                ORDER BY d.username ASC, d.division ASC
            """)
            rows = cur.fetchall() or []
            grouped = {}
            for row in rows:
                uname = str((row or {}).get('username') or '').strip()
                if not uname:
                    continue
                if uname not in grouped:
                    grouped[uname] = {'username': uname, 'divisions': [
                    ], 'can_override_all': bool((row or {}).get('can_override_all'))}
                d = _normalize_division(
                    (row or {}).get('division'), fallback='')
                if d and d not in grouped[uname]['divisions']:
                    grouped[uname]['divisions'].append(d)

            cur.execute("""
                SELECT username, can_override_all
                FROM mdt_dispatch_user_settings
                WHERE username NOT IN (SELECT DISTINCT username FROM mdt_dispatch_user_divisions)
                ORDER BY username ASC
            """)
            for row in (cur.fetchall() or []):
                uname = str((row or {}).get('username') or '').strip()
                if uname and uname not in grouped:
                    grouped[uname] = {'username': uname, 'divisions': [
                    ], 'can_override_all': bool((row or {}).get('can_override_all'))}
            return jsonify(sorted(grouped.values(), key=lambda x: x['username'].lower()))

        payload = request.get_json() or {}
        username = str(payload.get('username') or '').strip()
        username_key = username.lower()
        if not username:
            return jsonify({'error': 'username required'}), 400
        divisions = payload.get('divisions') if isinstance(
            payload.get('divisions'), list) else []
        divisions = sorted(list(dict.fromkeys([_normalize_division(
            x, fallback='') for x in divisions if _normalize_division(x, fallback='')])))
        can_override_all = bool(payload.get('can_override_all', False))

        cur.execute("""
            INSERT INTO mdt_dispatch_user_settings (username, can_override_all, updated_by)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                can_override_all = VALUES(can_override_all),
                updated_by = VALUES(updated_by),
                updated_at = CURRENT_TIMESTAMP
        """, (username_key, 1 if can_override_all else 0, getattr(current_user, 'username', 'unknown')))

        cur.execute(
            "DELETE FROM mdt_dispatch_user_divisions WHERE LOWER(username) = %s", (username_key,))
        for d in divisions:
            cur.execute("""
                INSERT INTO mdt_dispatch_user_divisions (username, division, created_by)
                VALUES (%s, %s, %s)
            """, (username_key, d, getattr(current_user, 'username', 'unknown')))
        conn.commit()
        return jsonify({'message': 'User division access updated', 'username': username_key, 'divisions': divisions, 'can_override_all': can_override_all})
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/my-division-access', methods=['GET'])
@login_required
def dispatch_my_division_access():
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        access = _get_dispatch_user_division_access(cur)
        return jsonify(access)
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/assist-requests', methods=['GET', 'POST'])
@login_required
def dispatch_assist_requests():
    """Create/list dispatcher cross-division assistance requests."""
    allowed_roles = ["dispatcher", "admin",
                     "superuser", "clinical_lead", "controller"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_assist_requests_table(cur)
        req_division = _normalize_division(
            request.args.get('division'), fallback='')
        req_include_external = str(request.args.get(
            'include_external') or '').strip().lower() in ('1', 'true', 'yes', 'on')
        req_division, req_include_external, access = _enforce_dispatch_scope(
            cur, req_division, req_include_external)
        if request.method == 'GET':
            status = str(request.args.get('status')
                         or 'pending').strip().lower()
            division = req_division
            limit = request.args.get('limit', default=100, type=int) or 100
            if limit < 1:
                limit = 1
            if limit > 400:
                limit = 400

            where = []
            args = []
            if status and status != 'all':
                where.append("status = %s")
                args.append(status)
            if division and division != _CAD_DIVISION_ALL:
                where.append("(from_division = %s OR to_division = %s)")
                args.extend([division, division])
            elif access.get('restricted'):
                allowed = [d for d in (access.get('divisions') or []) if d]
                if allowed:
                    placeholders = ",".join(["%s"] * len(allowed))
                    where.append(
                        f"(from_division IN ({placeholders}) OR to_division IN ({placeholders}))")
                    args.extend(allowed + allowed)
            where_sql = (" WHERE " + " AND ".join(where)) if where else ""
            cur.execute(f"""
                SELECT id, request_type, from_division, to_division, callsign, cad, note,
                       requested_by, status, resolved_by, resolved_note, created_at, resolved_at
                FROM mdt_dispatch_assist_requests
                {where_sql}
                ORDER BY id DESC
                LIMIT %s
            """, tuple(args + [limit]))
            rows = cur.fetchall() or []
            return jsonify(rows)

        payload = request.get_json() or {}
        callsign = str(payload.get('callsign') or '').strip()
        if not callsign:
            return jsonify({'error': 'callsign required'}), 400
        from_division = _normalize_division(
            payload.get('from_division'), fallback='')
        to_division = _normalize_division(
            payload.get('to_division'), fallback='')
        note = str(payload.get('note') or '').strip()
        cad = payload.get('cad')
        try:
            cad = int(cad) if cad not in (None, '') else None
        except Exception:
            cad = None
        if not to_division:
            return jsonify({'error': 'to_division required'}), 400
        if to_division == _CAD_DIVISION_ALL:
            return jsonify({'error': 'Select a specific division for assist'}), 400
        if access.get('restricted'):
            allowed = set(access.get('divisions') or [])
            if to_division not in allowed and not access.get('can_override_all'):
                return jsonify({'error': 'to_division not permitted'}), 403
        if not from_division:
            # derive from unit record if not supplied
            cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
            has_unit_div = cur.fetchone() is not None
            if has_unit_div:
                cur.execute(
                    "SELECT LOWER(TRIM(COALESCE(division, 'general'))) AS division FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (callsign,))
                row = cur.fetchone()
                from_division = _normalize_division(
                    (row or {}).get('division'), fallback='general')
            else:
                from_division = 'general'

        cur.execute("""
            INSERT INTO mdt_dispatch_assist_requests
                (request_type, from_division, to_division, callsign, cad, note, requested_by, status)
            VALUES ('unit_assist', %s, %s, %s, %s, %s, %s, 'pending')
        """, (
            from_division, to_division, callsign, cad, note or None,
            getattr(current_user, 'username', 'unknown')
        ))
        req_id = cur.lastrowid
        conn.commit()
        try:
            socketio.emit('mdt_event', {
                'type': 'assist_request_created',
                'id': req_id,
                'from_division': from_division,
                'to_division': to_division,
                'callsign': callsign,
                'cad': cad
            })
        except Exception:
            pass
        return jsonify({'message': 'Assist request created', 'id': req_id})
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/assist-requests/<int:req_id>/resolve', methods=['POST'])
@login_required
def dispatch_assist_request_resolve(req_id):
    """Approve/reject assist requests and optionally transfer/assign units."""
    allowed_roles = ["dispatcher", "admin",
                     "superuser", "clinical_lead", "controller"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    payload = request.get_json() or {}
    decision = str(payload.get('decision') or '').strip().lower()
    if decision not in ('approve', 'reject', 'cancel'):
        return jsonify({'error': 'decision must be approve/reject/cancel'}), 400
    transfer_division = bool(payload.get('transfer_division', True))
    assign_cad = payload.get('cad')
    try:
        assign_cad = int(assign_cad) if assign_cad not in (None, '') else None
    except Exception:
        assign_cad = None
    note = str(payload.get('note') or '').strip()

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_assist_requests_table(cur)
        _, _, access = _enforce_dispatch_scope(cur, '', False)
        operator = str(getattr(current_user, 'username',
                       'unknown') or 'unknown').strip()
        division_transfer_instruction_id = None
        cur.execute("""
            SELECT id, status, from_division, to_division, callsign, cad
            FROM mdt_dispatch_assist_requests
            WHERE id = %s
            LIMIT 1
        """, (req_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Request not found'}), 404
        if str(row.get('status') or '').lower() != 'pending':
            return jsonify({'error': 'Request already resolved'}), 409

        callsign = str(row.get('callsign') or '').strip()
        from_division = _normalize_division(
            row.get('from_division'), fallback='general')
        to_division = _normalize_division(
            row.get('to_division'), fallback='general')
        if access.get('restricted'):
            allowed = set(access.get('divisions') or [])
            if to_division not in allowed and not access.get('can_override_all'):
                return jsonify({'error': 'Not permitted to resolve this division request'}), 403
        cad = assign_cad if assign_cad is not None else row.get('cad')
        try:
            cad = int(cad) if cad not in (None, '') else None
        except Exception:
            cad = None

        if decision == 'approve':
            cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
            has_unit_div = cur.fetchone() is not None
            if has_unit_div and transfer_division:
                cur.execute(
                    "SELECT LOWER(TRIM(COALESCE(division, 'general'))) AS division FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
                    (callsign,)
                )
                unit_row = cur.fetchone() or {}
                current_div = _normalize_division(
                    unit_row.get('division'), fallback=from_division)
                cur.execute(
                    "UPDATE mdts_signed_on SET division = %s WHERE callSign = %s", (to_division, callsign))
                division_transfer_instruction_id = _queue_division_transfer_instruction(
                    cur,
                    callsign,
                    current_div,
                    to_division,
                    actor=operator,
                    reason='assist_transfer',
                    cad=cad
                )

            if cad is not None:
                _ensure_job_units_table(cur)
                cur.execute(
                    "SELECT status FROM mdt_jobs WHERE cad = %s LIMIT 1", (cad,))
                job = cur.fetchone()
                if not job:
                    conn.rollback()
                    return jsonify({'error': 'CAD job not found'}), 404
                status = str((job or {}).get('status') or '').strip().lower()
                if status in ('cleared', 'stood_down'):
                    conn.rollback()
                    return jsonify({'error': 'Cannot assign to closed CAD'}), 409
                next_status = 'assigned' if status in (
                    'queued', 'claimed', '', 'received', 'stood_down',
                    _JOB_STATUS_AWAITING_DISPATCH_REVIEW) else status
                cur.execute(
                    "UPDATE mdt_jobs SET status = %s WHERE cad = %s", (next_status, cad))
                cur.execute("""
                    INSERT INTO mdt_job_units (job_cad, callsign, assigned_by)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        assigned_by = VALUES(assigned_by),
                        assigned_at = CURRENT_TIMESTAMP
                """, (cad, callsign, getattr(current_user, 'username', 'unknown')))
                _sync_claimed_by_from_job_units(cur, cad)
                cur.execute("""
                    UPDATE mdts_signed_on
                    SET assignedIncident = %s, status = 'assigned'
                    WHERE callSign = %s
                """, (cad, callsign))

        new_status = 'approved' if decision == 'approve' else (
            'rejected' if decision == 'reject' else 'cancelled')
        cur.execute("""
            UPDATE mdt_dispatch_assist_requests
            SET status = %s,
                resolved_by = %s,
                resolved_note = %s,
                resolved_at = NOW()
            WHERE id = %s
        """, (
            new_status,
            operator,
            note or None,
            req_id
        ))
        if not log_audit(
            operator,
            'assist_request_resolve',
            details={
                'request_id': req_id,
                'decision': new_status,
                'callsign': callsign,
                'cad': cad,
            },
            audit_failure='fail_closed',
        ):
            conn.rollback()
            return _ventus_audit_unavailable_response()
        conn.commit()
        try:
            socketio.emit('mdt_event', {
                'type': 'assist_request_resolved',
                'id': req_id,
                'decision': new_status,
                'callsign': callsign,
                'to_division': to_division,
                'cad': cad
            })
            if cad is not None:
                socketio.emit(
                    'mdt_event', {'type': 'jobs_updated', 'cad': cad})
                socketio.emit(
                    'mdt_event', {'type': 'units_updated', 'callsign': callsign})
                _emit_mdt_job_assigned(cad, [callsign])
            if division_transfer_instruction_id:
                socketio.emit('mdt_event', {
                    'type': 'dispatch_instruction',
                    'callsign': callsign,
                    'instruction_id': division_transfer_instruction_id,
                    'instruction_type': 'division_transfer'
                })
        except Exception:
            pass
        return jsonify({
            'message': 'Assist request resolved',
            'status': new_status,
            'id': req_id,
            'division_transfer_instruction_id': division_transfer_instruction_id
        })
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/major-incidents', methods=['GET', 'POST'])
@login_required
def dispatch_major_incidents():
    """List or create major-incident records (CAD-MI-001)."""
    if not _ventus_dispatch_capable():
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_major_incidents_table(cur)
        if request.method == 'GET':
            status = str(request.args.get('status') or 'open').strip().lower()
            limit = request.args.get('limit', default=80, type=int) or 80
            limit = max(1, min(400, limit))
            cad_only = request.args.get('cad', default=None, type=int)
            where = []
            args = []
            if status and status != 'all':
                where.append('status = %s')
                args.append(status)
            if cad_only is not None and cad_only > 0:
                where.append('cad = %s')
                args.append(cad_only)
            where_sql = (' WHERE ' + ' AND '.join(where)) if where else ''
            cur.execute(f"""
                SELECT id, api_version, template_kind, phase, cad, division,
                       source, callsign, summary_title, status,
                       created_by, created_at, updated_at, closed_at, closed_by,
                       payload_json
                  FROM ventus_major_incidents
                 {where_sql}
                 ORDER BY id DESC
                 LIMIT %s
            """, tuple(args + [limit]))
            rows = cur.fetchall() or []
            for r in rows:
                raw = r.get('payload_json')
                try:
                    r['payload'] = json.loads(raw) if isinstance(
                        raw, str) and str(raw).strip() else {}
                except Exception:
                    r['payload'] = {}
                try:
                    del r['payload_json']
                except Exception:
                    pass
            return jsonify(rows)

        body = request.get_json(silent=True)
        errs, parsed = _major_incident_parse_request_body(
            body or {}, source_mdt=False)
        if errs:
            return jsonify({'error': 'validation failed', 'details': errs}), 400

        uname = str(getattr(current_user, 'username', None) or 'unknown')

        if parsed['cad'] is not None:
            cur.execute(
                "SELECT cad FROM mdt_jobs WHERE cad = %s LIMIT 1",
                (parsed['cad'],),
            )
            if not cur.fetchone():
                return jsonify({
                    'error': 'cad not found',
                    'cad': parsed['cad'],
                }), 404
            cur.execute(
                """
                SELECT id FROM ventus_major_incidents
                 WHERE cad = %s AND LOWER(COALESCE(status, '')) = 'open'
                 LIMIT 1
                """,
                (parsed['cad'],),
            )
            if cur.fetchone():
                return jsonify({
                    'error': 'open major incident exists',
                    'cad': parsed['cad'],
                    'message': (
                        'This CAD already has an open Major Incident declaration. '
                        'Stand it down before declaring again.'
                    ),
                }), 409

        mi_id = _major_incident_insert_row(
            cur,
            parsed=parsed,
            source='cad',
            callsign=None,
            created_by=uname,
        )
        summary = _major_incident_summary_title(
            parsed['template'], parsed['payload'])

        comm_text_saved = None
        mi_job_comm_id = None
        if parsed['cad'] is not None:
            _ensure_job_comms_table(cur)
            comm_text_saved = _major_incident_job_comm_text(
                parsed['template'], summary, parsed['payload'])
            cur.execute("""
                INSERT INTO mdt_job_comms
                    (cad, message_type, sender_role, sender_user, message_text)
                VALUES (%s, 'major_incident', 'dispatcher', %s, %s)
            """, (parsed['cad'], uname, comm_text_saved))
            mi_job_comm_id = int(getattr(cur, 'lastrowid', None) or 0) or None

        log_audit(
            uname,
            'cad_major_incident',
            details={
                'id': mi_id,
                'template': parsed['template'],
                'cad': parsed['cad'],
            },
        )
        conn.commit()
        _major_incident_emit_socket(
            mi_id, parsed, None, 'cad', summary,
            comm_text=comm_text_saved,
            job_comm_sender=uname,
            job_comm_id=mi_job_comm_id,
        )
        return jsonify({'ok': True, 'id': mi_id, 'summary': summary}), 201
    except Exception as e:
        conn.rollback()
        logger.exception('dispatch_major_incidents POST')
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/major-incidents/<int:mi_id>/close', methods=['POST'])
@login_required
def dispatch_major_incident_close(mi_id):
    if not _ventus_dispatch_capable():
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_major_incidents_table(cur)
        cur.execute(
            "SELECT id, status FROM ventus_major_incidents WHERE id = %s",
            (mi_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'not found'}), 404
        if str(row.get('status') or '').lower() == 'closed':
            return jsonify({'ok': True, 'id': mi_id, 'message': 'already closed'}), 200
        uname = str(getattr(current_user, 'username', None) or 'unknown')
        cur.execute("""
            UPDATE ventus_major_incidents
               SET status = 'closed',
                   closed_at = NOW(),
                   closed_by = %s
             WHERE id = %s
        """, (uname, mi_id))
        log_audit(
            uname,
            'cad_major_incident_close',
            details={'id': mi_id},
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.exception('dispatch_major_incident_close id=%s', mi_id)
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()

    try:
        socketio.emit('mdt_event', {
            'type': 'major_incident_closed',
            'id': mi_id,
        })
    except Exception:
        pass
    return jsonify({'ok': True, 'id': mi_id})


@internal.route('/jobs/eligibility', methods=['GET'])
@login_required
def jobs_eligibility():
    """Return ranked unit recommendations for each CAD job."""
    selected_division, include_external = _request_division_scope()
    cad_args = request.args.getlist('cad')
    cads = []
    for c in cad_args:
        try:
            cads.append(int(c))
        except Exception:
            continue
    cads = list(dict.fromkeys(cads))
    if not cads:
        return jsonify([])

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        selected_division, include_external, access = _enforce_dispatch_scope(
            cur, selected_division, include_external)
        _ensure_meal_break_columns(cur)
        _ventus_set_g_break_policy(cur)
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_job_div = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_unit_div = cur.fetchone() is not None
        placeholders = ",".join(["%s"] * len(cads))
        job_div_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_job_div else "'general' AS division"
        cur.execute(f"""
            SELECT cad, status, data, created_at, {job_div_sql}
            FROM mdt_jobs
            WHERE cad IN ({placeholders})
        """, cads)
        jobs = cur.fetchall() or []
        job_map = {int(j['cad']): j for j in jobs if j.get('cad') is not None}

        unit_div_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_unit_div else "'general' AS division"
        cur.execute("""
            SELECT callSign,
                   LOWER(TRIM(COALESCE(status, ''))) AS status,
                   lastLat,
                   lastLon,
                   lastSeenAt,
                   crew,
                   signOnTime,
                   mealBreakStartedAt,
                   mealBreakUntil,
                   mealBreakTakenAt,
                   shiftStartAt,
                   shiftEndAt,
                   shiftDurationMins,
                   breakDueAfterMins,
                   {unit_div_sql}
            FROM mdts_signed_on
            WHERE status IS NOT NULL
            ORDER BY callSign ASC
        """.format(unit_div_sql=unit_div_sql))
        units = cur.fetchall() or []

        allowed_units = None
        if access.get('restricted') and not access.get('can_override_all'):
            allowed_units = {d for d in (access.get('divisions') or []) if d}

        avail_states = {'on_standby', 'on_station',
                        'at_station', 'available', 'cleared', 'stood_down'}
        results = []

        for cad in cads:
            job = job_map.get(cad)
            if not job:
                results.append(
                    {'cad': cad, 'recommended': None, 'candidates': []})
                continue

            job_lat, job_lng, payload = _extract_coords_from_job_data(
                job.get('data'))
            job_division = _normalize_division(
                job.get('division') or _extract_job_division(payload), fallback='general')
            required_skills = _extract_required_skills(payload)
            priority_value = _extract_job_priority_value(payload)
            priority_rank = _priority_rank(priority_value)
            allow_break_override = _is_high_priority_value(priority_value)

            ranked = []
            for unit in units:
                callsign = unit.get('callSign')
                if not callsign:
                    continue
                status = (unit.get('status') or '').strip().lower()
                is_available = status in avail_states
                # Keep recommendations aligned with live signed-on crews only.
                if not is_available:
                    continue
                if not _unit_crew_has_members(unit.get('crew')):
                    continue
                last_seen = unit.get('lastSeenAt') or unit.get('signOnTime')
                if isinstance(last_seen, datetime):
                    try:
                        if (datetime.utcnow() - last_seen).total_seconds() > 20 * 60:
                            continue
                    except Exception:
                        pass
                unit_division = _normalize_division(
                    unit.get('division'), fallback='general')
                is_external = bool(
                    job_division and unit_division != job_division)
                if selected_division == _CAD_DIVISION_ALL and allowed_units is not None:
                    if unit_division not in allowed_units:
                        continue
                elif selected_division and not include_external and unit_division != selected_division:
                    continue
                unit_skills = _dispatch_skill_set_for_unit(
                    cur, unit.get('crew'))
                missing = sorted(list(required_skills - unit_skills))
                skill_match = (len(missing) == 0)
                shift_state = _compute_shift_break_state(unit)
                break_blocked = bool(shift_state.get(
                    'break_blocked_for_new_jobs') and not allow_break_override)

                try:
                    ulat = float(unit['lastLat']) if unit.get(
                        'lastLat') is not None else None
                    ulon = float(unit['lastLon']) if unit.get(
                        'lastLon') is not None else None
                except Exception:
                    ulat = ulon = None

                if ulat is not None and ulon is not None and job_lat is not None and job_lng is not None:
                    distance_km = _haversine_km(ulat, ulon, job_lat, job_lng)
                    distance_missing = 0
                else:
                    distance_km = None
                    distance_missing = 1

                rank = (
                    1 if is_external else 0,
                    1 if break_blocked else 0,
                    0 if is_available else 1,
                    0 if skill_match else 1,
                    distance_missing,
                    distance_km if distance_km is not None else 10**9,
                    str(callsign)
                )
                ranked.append({
                    'rank': rank,
                    'callsign': callsign,
                    'status': status,
                    'division': unit_division,
                    'external': is_external,
                    'available': is_available,
                    'skill_match': skill_match,
                    'missing_skills': missing,
                    'distance_km': round(distance_km, 2) if distance_km is not None else None,
                    'priority': priority_value or None,
                    'priority_rank': priority_rank,
                    'break_due': shift_state.get('break_due'),
                    'break_due_in_minutes': shift_state.get('break_due_in_minutes'),
                    'meal_break_taken': shift_state.get('meal_break_taken'),
                    'break_blocked': break_blocked,
                    'break_override_allowed': bool(allow_break_override)
                })

            ranked.sort(key=lambda x: x['rank'])
            top = ranked[:3]
            recommended = next(
                (x for x in ranked if not x.get('break_blocked')), None)
            for item in top:
                item.pop('rank', None)
            if recommended:
                recommended = {k: v for k,
                               v in recommended.items() if k != 'rank'}
            results.append({
                'cad': cad,
                'division': job_division,
                'priority': priority_value or None,
                'recommended': recommended,
                'candidates': top
            })

        return jsonify(results)
    finally:
        cur.close()
        conn.close()


@internal.route('/dispatch/repair_assignments', methods=['POST'])
@login_required
def dispatch_repair_assignments():
    """Backfill/sync mdt_job_units and claimedBy for legacy incidents."""
    allowed_roles = ["dispatcher", "admin", "superuser", "clinical_lead"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_job_units_table(cur)

        cur.execute("SELECT cad, claimedBy FROM mdt_jobs")
        rows = cur.fetchall() or []

        inserted_links = 0
        touched_jobs = set()

        # Backfill mappings from legacy claimedBy CSV values (one-off repair purpose only).
        for row in rows:
            cad = row.get('cad')
            claimed = str(row.get('claimedBy') or '').strip()
            if not cad or not claimed:
                continue
            callsigns = [c.strip() for c in claimed.split(',') if c.strip()]
            for cs in callsigns:
                cur.execute("""
                    INSERT INTO mdt_job_units (job_cad, callsign, assigned_by)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE callsign = callsign
                """, (cad, cs, 'repair_tool'))
                inserted_links += int(cur.rowcount == 1)
                touched_jobs.add(int(cad))

        # Sync claimedBy from mdt_job_units for all jobs with links.
        cur.execute("SELECT DISTINCT job_cad FROM mdt_job_units")
        linked = [int(r['job_cad']) for r in (
            cur.fetchall() or []) if r.get('job_cad') is not None]
        synced = 0
        for cad in linked:
            _sync_claimed_by_from_job_units(cur, cad)
            synced += 1

        conn.commit()
        return jsonify({
            'message': 'Assignment repair complete',
            'inserted_links': inserted_links,
            'jobs_touched': len(touched_jobs),
            'jobs_synced': synced
        })
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/messages/<callsign>', methods=['GET', 'POST'])
@login_required
def messages(callsign):
    """Get or send messages to a unit/dispatcher.

    Uses the same callsign resolution as ``/api/messages/<callsign>`` (MDT poll path) so rows are
    stored under the canonical ``mdts_signed_on.callSign`` after CAD renames / ``mdt_callsign_redirect``.
    """
    cs_req = str(callsign or '').strip().upper()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    if request.method == 'GET':
        try:
            _ensure_callsign_redirect_table(cur)
            cs = _mdt_resolve_callsign(cur, cs_req)
            excl = _sql_exclude_mirrored_cad_update_messages()
            cur.execute(f"""
                SELECT id, `from`, text, timestamp
                FROM messages
                WHERE (LOWER(TRIM(recipient)) IN (LOWER(TRIM(%s)), LOWER(TRIM(%s)))
                   OR LOWER(TRIM(`from`)) IN (LOWER(TRIM(%s)), LOWER(TRIM(%s))))
                  AND ({excl})
                ORDER BY timestamp ASC
            """, (cs, cs_req, cs, cs_req))
            messages = cur.fetchall()
            return jsonify(messages)
        finally:
            cur.close()
            conn.close()
    else:
        data = request.get_json() or {}
        text = data.get('text', '').strip()
        sender_hint = str(data.get('sender_portal')
                          or data.get('from') or 'dispatcher').strip()
        # Basic validation
        if not text:
            cur.close()
            conn.close()
            return jsonify({'error': 'Message text required'}), 400
        if len(text) > 2000:
            cur.close()
            conn.close()
            return jsonify({'error': 'Message too long'}), 400
        # Authorization: align with job comms / broadcast (session CAD users)
        allowed_roles = [
            "dispatcher", "admin", "superuser", "crew", "clinical_lead",
            "controller", "call_taker", "calltaker", "call_handler", "callhandler",
        ]
        if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
            cur.close()
            conn.close()
            return jsonify({'error': 'Unauthorised'}), 403
        _ensure_callsign_redirect_table(cur)
        cs_canon = _mdt_resolve_callsign(cur, cs_req)
        try:
            _ensure_mdts_signed_on_schema(cur)
            cur.execute(
                "SELECT crew FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
                (cs_canon,),
            )
            crow = cur.fetchone()
            if crow and not _unit_crew_has_members(crow.get('crew')):
                cur.close()
                conn.close()
                return jsonify({
                    'error': 'No crew on this callsign — join from the MDT before messages can be sent.',
                }), 409
        except Exception:
            pass
        username = str(getattr(current_user, 'username', '') or '').strip()
        sender = _sender_label_from_portal(sender_hint, username)
        try:
            dup_id = _messages_recent_duplicate_id(cur, sender, cs_canon, text)
            if dup_id is not None:
                return jsonify({'message': 'Message sent', 'id': dup_id, 'deduped': True})
            cur.execute("""
                INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                VALUES (%s, %s, %s, NOW(), 0)
            """, (sender, cs_canon, text))
            new_id = cur.lastrowid
            conn.commit()
            _mp_payload = {
                'type': 'message_posted',
                'from': str(sender or 'dispatcher'),
                'to': cs_canon,
                'text': text,
                'message_id': int(new_id) if new_id else None,
            }
            if cs_req and cs_req != cs_canon:
                _mp_payload['requestedCallSign'] = cs_req
            try:
                cs_room = re.sub(
                    r'[^A-Za-z0-9_-]', '', str(cs_canon or '').strip()).upper()[:64]
                if cs_room:
                    socketio.emit('mdt_event', _mp_payload, room=f'mdt_callsign_{cs_room}')
            except Exception:
                pass
            try:
                socketio.emit('mdt_event', _mp_payload)
            except Exception:
                pass
            try:
                _mdt_web_push_notify_callsign(
                    cs_canon,
                    str(sender or 'Dispatch'),
                    text[:500] if text else 'Open MDT to read the message.',
                    tag='ventus-control-message',
                    url='/dashboard',
                    alert=False,
                    silent=False,
                    require_interaction=False,
                )
            except Exception:
                pass
            try:
                logger.info('Message posted: from=%s to=%s by=%s len=%s', sender, cs_canon, getattr(
                    current_user, 'username', 'unknown'), len(text))
            except Exception:
                pass
            log_audit(
                getattr(current_user, 'username', 'unknown'),
                'post_message',
                details={'to': cs_canon, 'from': sender, 'len': len(text)},
            )
            return jsonify({'message': 'Message sent', 'id': int(new_id) if new_id else None})
        finally:
            cur.close()
            conn.close()


@internal.route('/kpis', methods=['GET'])
@login_required
def kpis():
    """Return analytics: active jobs, cleared today, avg response time, units available."""
    selected_division, include_external = _request_division_scope()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        selected_division, include_external, access = _enforce_dispatch_scope(
            cur, selected_division, include_external)
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_job_div = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_unit_div = cur.fetchone() is not None

        job_div_expr = "LOWER(TRIM(COALESCE(division, 'general')))"
        unit_div_expr = "LOWER(TRIM(COALESCE(division, 'general')))"
        job_div_where, job_div_args = _division_scope_where_sql(
            selected_division, include_external, access, has_job_div, job_div_expr
        )
        unit_div_where, unit_div_args = _division_scope_where_sql(
            selected_division, include_external, access, has_unit_div, unit_div_expr
        )

        # Active jobs
        cur.execute(
            "SELECT COUNT(*) AS count FROM mdt_jobs WHERE LOWER(TRIM(COALESCE(status, ''))) IN ('queued','claimed','assigned','mobile','on_scene')" + job_div_where,
            tuple(job_div_args)
        )
        active_jobs = cur.fetchone()['count']

        # Units available
        cur.execute(
            "SELECT COUNT(*) AS count FROM mdts_signed_on WHERE LOWER(TRIM(COALESCE(status, ''))) IN ('on_standby','on_station','at_station','available')" + unit_div_where,
            tuple(unit_div_args)
        )
        units_available = cur.fetchone()['count']

        # Cleared today
        cur.execute(
            "SELECT COUNT(*) AS count FROM mdt_jobs WHERE LOWER(TRIM(COALESCE(status, ''))) = 'cleared' AND DATE(updated_at) = CURDATE()" + job_div_where,
            tuple(job_div_args)
        )
        cleared_today = cur.fetchone()['count']

        avg_response_time = "--"

        def _parse_dt(value):
            if not value:
                return None
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                except Exception:
                    try:
                        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        return None
            return None

        def _duration_seconds(start, end):
            s = _parse_dt(start)
            e = _parse_dt(end)
            if not s or not e:
                return None
            sec = int((e - s).total_seconds())
            return sec if sec >= 0 else None

        def _avg_duration(values):
            clean = [v for v in values if isinstance(v, int) and v >= 0]
            if not clean:
                return None
            return int(round(sum(clean) / len(clean)))

        def _fmt_duration(seconds):
            if seconds is None:
                return "--"
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            if hours > 0:
                return f"{hours}h {minutes}m"
            return f"{minutes}m"

        stage_averages = {
            "wait_to_assigned": "--",
            "mobile_to_scene": "--",
            "on_scene": "--",
            "leave_to_hospital": "--",
            "at_hospital": "--"
        }

        # Today's status-stage averages across CADs with response-log activity today.
        cur.execute("SHOW TABLES LIKE 'mdt_response_log'")
        has_response_log = cur.fetchone() is not None
        if has_response_log:
            j_div_timing = "LOWER(TRIM(COALESCE(j.division, 'general')))"
            timing_frag, timing_args = _division_scope_where_sql(
                selected_division, include_external, access, has_job_div, j_div_timing
            )
            cur.execute("""
                SELECT
                    l.cad,
                    MAX(CASE WHEN l.status='received'    THEN l.event_time END) AS received_time,
                    MAX(CASE WHEN l.status='assigned'    THEN l.event_time END) AS assigned_time,
                    MAX(CASE WHEN l.status='mobile'      THEN l.event_time END) AS mobile_time,
                    MAX(CASE WHEN l.status='on_scene'    THEN l.event_time END) AS on_scene_time,
                    MAX(CASE WHEN l.status='leave_scene' THEN l.event_time END) AS leave_scene_time,
                    MAX(CASE WHEN l.status='at_hospital' THEN l.event_time END) AS at_hospital_time,
                    MAX(CASE WHEN l.status='cleared'     THEN l.event_time END) AS cleared_time,
                    MAX(CASE WHEN l.status='stood_down'  THEN l.event_time END) AS stood_down_time,
                    MAX(CASE WHEN l.status='closure_review' THEN l.event_time END) AS closure_review_time,
                    MAX(CASE WHEN l.status='dispatch_reopened' THEN l.event_time END) AS dispatch_reopened_time
                FROM mdt_response_log l
                INNER JOIN mdt_jobs j ON j.cad = l.cad
                INNER JOIN (
                    SELECT DISTINCT cad
                    FROM mdt_response_log
                    WHERE DATE(event_time) = CURDATE()
                ) t ON t.cad = l.cad
                WHERE 1=1
            """ + timing_frag + """
                GROUP BY l.cad
            """, tuple(timing_args))
            timing_rows = cur.fetchall() or []

            wait_to_assigned = []
            mobile_to_scene = []
            on_scene = []
            leave_to_hospital = []
            at_hospital = []
            cycle_complete = []

            for row in timing_rows:
                wait_sec = _duration_seconds(
                    row.get('received_time'),
                    row.get('assigned_time') or row.get('mobile_time')
                )
                if wait_sec is not None:
                    wait_to_assigned.append(wait_sec)

                mobile_scene_sec = _duration_seconds(
                    row.get('mobile_time'),
                    row.get('on_scene_time')
                )
                if mobile_scene_sec is not None:
                    mobile_to_scene.append(mobile_scene_sec)

                on_scene_sec = _duration_seconds(
                    row.get('on_scene_time'),
                    row.get('leave_scene_time')
                )
                if on_scene_sec is not None:
                    on_scene.append(on_scene_sec)

                leave_hosp_sec = _duration_seconds(
                    row.get('leave_scene_time'),
                    row.get('at_hospital_time')
                )
                if leave_hosp_sec is not None:
                    leave_to_hospital.append(leave_hosp_sec)

                at_hosp_sec = _duration_seconds(
                    row.get('at_hospital_time'),
                    row.get('cleared_time') or row.get('stood_down_time')
                )
                if at_hosp_sec is not None:
                    at_hospital.append(at_hosp_sec)

                cycle_sec = _duration_seconds(
                    row.get('received_time'),
                    row.get('cleared_time') or row.get('stood_down_time')
                )
                if cycle_sec is not None:
                    cycle_complete.append(cycle_sec)

            stage_averages = {
                "wait_to_assigned": _fmt_duration(_avg_duration(wait_to_assigned)),
                "mobile_to_scene": _fmt_duration(_avg_duration(mobile_to_scene)),
                "on_scene": _fmt_duration(_avg_duration(on_scene)),
                "leave_to_hospital": _fmt_duration(_avg_duration(leave_to_hospital)),
                "at_hospital": _fmt_duration(_avg_duration(at_hospital))
            }
            avg_response_time = _fmt_duration(_avg_duration(cycle_complete))

        # Fallback when response log has no complete cycles today.
        if avg_response_time == "--":
            cur.execute("""
                SELECT AVG(TIMESTAMPDIFF(SECOND, created_at, updated_at)) AS avg_seconds
                FROM mdt_jobs
                WHERE LOWER(TRIM(COALESCE(status, ''))) = 'cleared'
                  AND DATE(created_at) = CURDATE()
                  AND DATE(updated_at) = CURDATE()
            """ + job_div_where, tuple(job_div_args))
            row = cur.fetchone() or {}
            avg_response_time = _fmt_duration(
                _avg_duration([int(row.get('avg_seconds'))]) if row.get(
                    'avg_seconds') is not None else None
            )

        # Unit shift/break KPIs: near break, on meal break, overdue break, finishing soon
        units_near_break = 0
        units_on_meal_break = 0
        units_overdue_break = 0
        units_finishing_soon = 0
        try:
            _ensure_mdts_signed_on_schema(cur)
            _ensure_meal_break_columns(cur)
            cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'lastSeenAt'")
            has_last_seen = cur.fetchone() is not None
            cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'signOnTime'")
            has_sign_on = cur.fetchone() is not None
            seen_expr = "COALESCE(m.lastSeenAt, m.signOnTime)" if (
                has_last_seen and has_sign_on) else ("m.lastSeenAt" if has_last_seen else "m.signOnTime")
            cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'shiftStartAt'")
            has_shift = cur.fetchone() is not None
            unit_kpi_m_expr = "LOWER(TRIM(COALESCE(m.division, 'general')))"
            unit_kpi_frag, unit_kpi_args = _division_scope_where_sql(
                selected_division, include_external, access, has_unit_div, unit_kpi_m_expr
            )
            unit_kpi_where = " WHERE m.status IS NOT NULL AND " + \
                seen_expr + " >= DATE_SUB(NOW(), INTERVAL 120 MINUTE)"
            unit_kpi_where += unit_kpi_frag
            if has_shift:
                cur.execute(
                    "SELECT m.callSign, m.status, m.signOnTime, m.mealBreakStartedAt, m.mealBreakUntil, m.mealBreakTakenAt, m.shiftStartAt, m.shiftEndAt, m.shiftDurationMins, m.breakDueAfterMins FROM mdts_signed_on m" + unit_kpi_where,
                    tuple(unit_kpi_args)
                )
            else:
                cur.execute(
                    "SELECT m.callSign, m.status, m.signOnTime FROM mdts_signed_on m" + unit_kpi_where,
                    tuple(unit_kpi_args)
                )
            unit_rows = cur.fetchall() or []
            for u in unit_rows:
                state = _compute_shift_break_state(u)
                if state.get("meal_break_active"):
                    units_on_meal_break += 1
                elif state.get("break_due"):
                    units_overdue_break += 1
                elif state.get("near_break"):
                    units_near_break += 1
                rem = state.get("shift_remaining_minutes")
                if rem is not None and rem <= 30 and rem >= 0:
                    units_finishing_soon += 1
        except Exception:
            pass

        return jsonify({
            "active_jobs": active_jobs,
            "units_available": units_available,
            "cleared_today": cleared_today,
            "avg_response_time": avg_response_time,
            "stage_averages": stage_averages,
            "units_near_break": units_near_break,
            "units_on_meal_break": units_on_meal_break,
            "units_overdue_break": units_overdue_break,
            "units_finishing_soon": units_finishing_soon
        })
    finally:
        cur.close()
        conn.close()


@internal.route('/kpis/by-division', methods=['GET'])
@login_required
def kpis_by_division():
    """
    Admin overview: same response-stage metrics as CAD KPIs, broken down by dispatch division
    for today (local DB date). Authorised for admin / superuser / clinical_lead only.
    """
    role = str(getattr(current_user, 'role', '') or '').strip().lower()
    if role not in {'admin', 'superuser', 'clinical_lead'}:
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_job_div = cur.fetchone() is not None

        catalog = _list_dispatch_divisions(cur, include_inactive=True, filter_event_window=False) or []
        catalog_by_slug = {str((x or {}).get('slug') or '').strip().lower(): (x or {}) for x in catalog if str((x or {}).get('slug') or '').strip()}

        def _parse_dt(value):
            if not value:
                return None
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                except Exception:
                    try:
                        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        return None
            return None

        def _duration_seconds(start, end):
            s = _parse_dt(start)
            e = _parse_dt(end)
            if not s or not e:
                return None
            sec = int((e - s).total_seconds())
            return sec if sec >= 0 else None

        def _avg_duration(values):
            clean = [v for v in values if isinstance(v, int) and v >= 0]
            if not clean:
                return None
            return int(round(sum(clean) / len(clean)))

        def _fmt_duration(seconds):
            if seconds is None:
                return "--"
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            if hours > 0:
                return f"{hours}h {minutes}m"
            return f"{minutes}m"

        buckets = defaultdict(lambda: {
            'wait_to_assigned': [],
            'mobile_to_scene': [],
            'on_scene': [],
            'leave_to_hospital': [],
            'at_hospital': [],
            'cycle_complete': [],
        })

        cur.execute("SHOW TABLES LIKE 'mdt_response_log'")
        has_response_log = cur.fetchone() is not None
        if has_response_log:
            if has_job_div:
                div_expr = "LOWER(TRIM(COALESCE(j.division, 'general')))"
            else:
                div_expr = "'general'"
            cur.execute(f"""
                SELECT
                    {div_expr} AS job_division,
                    l.cad,
                    MAX(CASE WHEN l.status='received'    THEN l.event_time END) AS received_time,
                    MAX(CASE WHEN l.status='assigned'    THEN l.event_time END) AS assigned_time,
                    MAX(CASE WHEN l.status='mobile'      THEN l.event_time END) AS mobile_time,
                    MAX(CASE WHEN l.status='on_scene'    THEN l.event_time END) AS on_scene_time,
                    MAX(CASE WHEN l.status='leave_scene' THEN l.event_time END) AS leave_scene_time,
                    MAX(CASE WHEN l.status='at_hospital' THEN l.event_time END) AS at_hospital_time,
                    MAX(CASE WHEN l.status='cleared'     THEN l.event_time END) AS cleared_time,
                    MAX(CASE WHEN l.status='stood_down'  THEN l.event_time END) AS stood_down_time,
                    MAX(CASE WHEN l.status='closure_review' THEN l.event_time END) AS closure_review_time,
                    MAX(CASE WHEN l.status='dispatch_reopened' THEN l.event_time END) AS dispatch_reopened_time
                FROM mdt_response_log l
                INNER JOIN mdt_jobs j ON j.cad = l.cad
                INNER JOIN (
                    SELECT DISTINCT cad
                    FROM mdt_response_log
                    WHERE DATE(event_time) = CURDATE()
                ) t ON t.cad = l.cad
                GROUP BY {div_expr}, l.cad
            """)
            for row in (cur.fetchall() or []):
                divk = str(row.get('job_division') or 'general').strip().lower() or 'general'
                b = buckets[divk]
                wait_sec = _duration_seconds(
                    row.get('received_time'),
                    row.get('assigned_time') or row.get('mobile_time')
                )
                if wait_sec is not None:
                    b['wait_to_assigned'].append(wait_sec)
                mobile_scene_sec = _duration_seconds(row.get('mobile_time'), row.get('on_scene_time'))
                if mobile_scene_sec is not None:
                    b['mobile_to_scene'].append(mobile_scene_sec)
                on_scene_sec = _duration_seconds(row.get('on_scene_time'), row.get('leave_scene_time'))
                if on_scene_sec is not None:
                    b['on_scene'].append(on_scene_sec)
                leave_hosp_sec = _duration_seconds(row.get('leave_scene_time'), row.get('at_hospital_time'))
                if leave_hosp_sec is not None:
                    b['leave_to_hospital'].append(leave_hosp_sec)
                at_hosp_sec = _duration_seconds(
                    row.get('at_hospital_time'),
                    row.get('cleared_time') or row.get('stood_down_time')
                )
                if at_hosp_sec is not None:
                    b['at_hospital'].append(at_hosp_sec)
                cycle_sec = _duration_seconds(
                    row.get('received_time'),
                    row.get('cleared_time') or row.get('stood_down_time')
                )
                if cycle_sec is not None:
                    b['cycle_complete'].append(cycle_sec)

        cleared_map = {}
        if has_job_div:
            cur.execute("""
                SELECT LOWER(TRIM(COALESCE(division, 'general'))) AS d, COUNT(*) AS c
                FROM mdt_jobs
                WHERE LOWER(TRIM(COALESCE(status, ''))) = 'cleared' AND DATE(updated_at) = CURDATE()
                GROUP BY d
            """)
        else:
            cur.execute("""
                SELECT 'general' AS d, COUNT(*) AS c
                FROM mdt_jobs
                WHERE LOWER(TRIM(COALESCE(status, ''))) = 'cleared' AND DATE(updated_at) = CURDATE()
            """)
        for crow in (cur.fetchall() or []):
            dk = str(crow.get('d') or 'general').strip().lower() or 'general'
            cleared_map[dk] = int(crow.get('c') or 0)

        fallback_cycle = {}
        if has_job_div:
            cur.execute("""
                SELECT LOWER(TRIM(COALESCE(division, 'general'))) AS d,
                       AVG(TIMESTAMPDIFF(SECOND, created_at, updated_at)) AS avg_seconds
                FROM mdt_jobs
                WHERE LOWER(TRIM(COALESCE(status, ''))) = 'cleared'
                  AND DATE(created_at) = CURDATE()
                  AND DATE(updated_at) = CURDATE()
                GROUP BY d
            """)
        else:
            cur.execute("""
                SELECT 'general' AS d, AVG(TIMESTAMPDIFF(SECOND, created_at, updated_at)) AS avg_seconds
                FROM mdt_jobs
                WHERE LOWER(TRIM(COALESCE(status, ''))) = 'cleared'
                  AND DATE(created_at) = CURDATE()
                  AND DATE(updated_at) = CURDATE()
            """)
        for frow in (cur.fetchall() or []):
            dk = str(frow.get('d') or 'general').strip().lower() or 'general'
            av = frow.get('avg_seconds')
            if av is not None:
                try:
                    fallback_cycle[dk] = int(round(float(av)))
                except (TypeError, ValueError):
                    pass

        seen_slug_order = set()
        ordered_slugs = []
        for s in catalog_by_slug.keys():
            if s not in seen_slug_order:
                seen_slug_order.add(s)
                ordered_slugs.append(s)
        for dk in sorted(buckets.keys()):
            if dk not in seen_slug_order:
                seen_slug_order.add(dk)
                ordered_slugs.append(dk)
        for dk in sorted(cleared_map.keys()):
            if dk not in seen_slug_order:
                seen_slug_order.add(dk)
                ordered_slugs.append(dk)

        rows_out = []
        for slug in ordered_slugs:
            cat = catalog_by_slug.get(slug, {})
            name = str(cat.get('name') or slug).strip() or slug
            color = _normalize_hex_color(cat.get('color'), '#64748b')
            b = buckets.get(slug) or {
                'wait_to_assigned': [], 'mobile_to_scene': [], 'on_scene': [],
                'leave_to_hospital': [], 'at_hospital': [], 'cycle_complete': [],
            }
            stage_averages = {
                'wait_to_assigned': _fmt_duration(_avg_duration(b['wait_to_assigned'])),
                'mobile_to_scene': _fmt_duration(_avg_duration(b['mobile_to_scene'])),
                'on_scene': _fmt_duration(_avg_duration(b['on_scene'])),
                'leave_to_hospital': _fmt_duration(_avg_duration(b['leave_to_hospital'])),
                'at_hospital': _fmt_duration(_avg_duration(b['at_hospital'])),
            }
            n_cycle = len(b['cycle_complete'])
            avg_full = _fmt_duration(_avg_duration(b['cycle_complete']))
            if avg_full == '--':
                fb = fallback_cycle.get(slug)
                avg_full = _fmt_duration(fb) if fb is not None else '--'
            oid = cat.get('cura_operational_event_id') or cat.get('operational_event_id')
            try:
                oid_out = int(oid) if oid is not None else None
            except (TypeError, ValueError):
                oid_out = None
            rows_out.append({
                'slug': slug,
                'name': name,
                'color': color,
                'cleared_today': int(cleared_map.get(slug, 0)),
                'avg_full_cycle': avg_full,
                'stage_averages': stage_averages,
                'cycles_sampled': n_cycle,
                'is_event_division': bool(cat.get('is_event_division')),
                'operational_event_id': oid_out,
            })

        return jsonify({
            'date': datetime.utcnow().strftime('%Y-%m-%d'),
            'rows': rows_out,
        })
    finally:
        cur.close()
        conn.close()


@internal.route('/history', methods=['GET'])
@login_required
def history():
    """Return cleared and stood-down jobs for the history table (same scope as past panel)."""
    selected_division, include_external = _request_division_scope()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        selected_division, include_external, access = _enforce_dispatch_scope(
            cur, selected_division, include_external)
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'claimedBy'")
        has_claimed_by = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
        has_updated_at = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'data'")
        has_data = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_division = cur.fetchone() is not None
        _ensure_job_division_snapshot_columns(cur)
        has_snap = _jobs_have_division_snapshot_columns(cur)
        snap_name_sql = "division_snapshot_name" if has_snap else "NULL AS division_snapshot_name"
        snap_color_sql = "division_snapshot_color" if has_snap else "NULL AS division_snapshot_color"

        claimed_by_sql = "claimedBy" if has_claimed_by else "NULL AS claimedBy"
        completed_sql = "updated_at AS completedAt" if has_updated_at else "created_at AS completedAt"
        data_sql = "data" if has_data else "NULL AS data"
        division_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_division else "'general' AS division"
        order_by_sql = "updated_at DESC" if has_updated_at else "created_at DESC"

        sql = f"""
            SELECT cad, {completed_sql}, TRIM(COALESCE(status, '')) AS status, {claimed_by_sql},
                   {data_sql}, {division_sql},
                   {snap_name_sql}, {snap_color_sql}
            FROM mdt_jobs
            WHERE LOWER(TRIM(COALESCE(status, ''))) IN ('cleared', 'stood_down')
        """
        job_div_expr = "LOWER(TRIM(COALESCE(division, 'general')))"
        div_frag, args = _division_scope_where_sql(
            selected_division, include_external, access, has_division, job_div_expr
        )
        sql += div_frag
        sql += f" ORDER BY {order_by_sql} LIMIT 100"
        cur.execute(sql, tuple(args))
        jobs = cur.fetchall()
        for job in jobs:
            job['sql_division'] = _normalize_division(
                job.get('division'), fallback='general')
        _enrich_cleared_job_rows_division_audit(cur, jobs)
        for job in jobs:
            payload = job.get('data')
            try:
                if isinstance(payload, (bytes, bytearray)):
                    payload = payload.decode('utf-8', errors='ignore')
                if isinstance(payload, str):
                    payload = json.loads(payload) if payload else {}
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}
            sql_div = job.pop('sql_division', None) or 'general'
            job['chief_complaint'] = payload.get('chief_complaint')
            job['outcome'] = payload.get('outcome')
            job['division'] = _extract_job_division(payload, fallback=sql_div)
            job['is_external'] = bool(
                selected_division
                and selected_division != _CAD_DIVISION_ALL
                and job.get('division') != selected_division
            )
            job.pop('data', None)
        return jsonify(jobs)
    except Exception:
        logger.exception("history failed")
        return jsonify([])
    finally:
        cur.close()
        conn.close()


@internal.route('/job/<int:cad>/assign', methods=['POST'])
@login_required
def assign_job(cad):
    """Assign a job to one or many units (callsign(s) in POST data)."""
    data = request.get_json() or {}
    callsigns = data.get('callsigns')
    if not isinstance(callsigns, list):
        single = data.get('callsign')
        callsigns = [single] if single else []
    callsigns = [str(c).strip() for c in callsigns if str(c).strip()]
    callsigns = list(dict.fromkeys(callsigns))
    transfer_division = str(data.get('transfer_division', '1')).strip(
    ).lower() in ('1', 'true', 'yes', 'on')
    break_override = str(data.get('break_override', '0')
                         ).strip().lower() in ('1', 'true', 'yes', 'on')
    divert_confirmed = str(data.get('divert_confirmed', '0')).strip(
    ).lower() in ('1', 'true', 'yes', 'on')
    raw_div = data.get('division')
    selected_division = _normalize_division(raw_div, fallback='')
    if str(raw_div or '').strip().lower() in ('all', '__all__', '*'):
        selected_division = _CAD_DIVISION_ALL
    if not callsigns:
        return jsonify({'error': 'callsign(s) required'}), 400
    if not _ventus_dispatch_capable():
        return jsonify({'error': 'Unauthorised'}), 403
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        selected_division, _, access = _enforce_dispatch_scope(
            cur, selected_division, False)
        # Ensure assignment mapping table exists for multi-unit incidents.
        _ensure_job_units_table(cur)
        _ensure_meal_break_columns(cur)
        _ventus_set_g_break_policy(cur)
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_job_div = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_unit_div = cur.fetchone() is not None

        job_div_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_job_div else "'general' AS division"
        cur.execute(
            f"SELECT status, data, {job_div_sql} FROM mdt_jobs WHERE cad = %s LIMIT 1", (cad,))
        job = cur.fetchone()
        if not job:
            conn.rollback()
            return jsonify({'error': 'Job not found'}), 404

        if selected_division == _CAD_DIVISION_ALL:
            selected_division = _normalize_division(
                job.get('division'), fallback='general')

        status = str(job.get('status') or '').strip().lower()
        job_division = _normalize_division(
            job.get('division') or selected_division, fallback='general')
        _, _, job_payload = _extract_coords_from_job_data(job.get('data'))
        priority_value = _extract_job_priority_value(job_payload)
        job_is_high_priority = _is_high_priority_value(priority_value)
        if access.get('restricted'):
            allowed = set(access.get('divisions') or [])
            if job_division not in allowed and not access.get('can_override_all'):
                conn.rollback()
                return jsonify({'error': 'Not permitted for this division'}), 403
        if status == 'cleared':
            conn.rollback()
            return jsonify({'error': 'Cannot assign closed job'}), 409

        existing_units = _get_job_unit_callsigns(cur, cad)
        existing_set = {str(x).strip().upper()
                        for x in (existing_units or []) if str(x).strip()}

        def _norm_assigned_cad(v):
            try:
                x = int(v)
                return x if x > 0 else None
            except (TypeError, ValueError):
                return None

        already_assigned_here = []
        pending_diverts = []
        for cs in callsigns:
            cur.execute(
                "SELECT callSign, assignedIncident FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
                (cs,),
            )
            urow = cur.fetchone()
            if not urow:
                continue
            old_n = _norm_assigned_cad(urow.get('assignedIncident'))
            if cs.upper() in existing_set or old_n == cad:
                if cs not in already_assigned_here:
                    already_assigned_here.append(cs)
                continue
            if old_n is not None and old_n != cad:
                pending_diverts.append({'callsign': cs, 'from_cad': old_n})

        if already_assigned_here:
            conn.rollback()
            return jsonify({
                'error': 'One or more units are already assigned to this CAD',
                'already_assigned': already_assigned_here,
            }), 409
        if pending_diverts and not divert_confirmed:
            conn.rollback()
            return jsonify({
                'error': 'Unit(s) are allocated to another CAD; confirm to divert them',
                'needs_divert_confirmation': True,
                'diverts': pending_diverts,
            }), 409

        no_crew_units = []
        for cs in callsigns:
            cur.execute(
                "SELECT crew FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (cs,))
            crow = cur.fetchone()
            if not crow:
                continue
            if not _unit_crew_has_members(crow.get('crew')):
                no_crew_units.append(cs)
        if no_crew_units:
            conn.rollback()
            return jsonify({
                'error': (
                    'Cannot assign: no crew on this callsign. '
                    'Staff must join the vehicle from the MDT first.'
                ),
                'no_crew': no_crew_units,
            }), 409

        # Re-assignment must reopen previously stood-down jobs as active.
        next_status = 'assigned' if status in (
            'queued', 'claimed', '', 'received', 'stood_down',
            _JOB_STATUS_AWAITING_DISPATCH_REVIEW) else status

        cur.execute("""
            UPDATE mdt_jobs SET status = %s WHERE cad = %s
        """, (next_status, cad))

        assigned = []
        missing = []
        blocked_for_break = []
        break_override_used = []
        reassigned_from = set()
        division_transfer_notifications = []
        sender_name = _sender_label_from_portal(
            'dispatch', getattr(current_user, 'username', ''))
        cur.execute("SHOW TABLES LIKE 'messages'")
        has_messages = cur.fetchone() is not None
        operator = str(getattr(current_user, 'username', '') or '').strip()
        for cs in callsigns:
            unit_div_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_unit_div else "'general' AS division"
            cur.execute(
                f"SELECT callSign, crew, {unit_div_sql} FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (cs,))
            unit = cur.fetchone()
            if not unit:
                missing.append(cs)
                continue
            cur.execute(
                "SELECT assignedIncident, crew FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (cs,))
            unit_state = cur.fetchone() or {}
            old_cad = unit_state.get('assignedIncident')
            crew_json = unit_state.get('crew') or '[]'
            cur.execute("""
                SELECT status, signOnTime, mealBreakStartedAt, mealBreakUntil, mealBreakTakenAt,
                       shiftStartAt, shiftEndAt, shiftDurationMins, breakDueAfterMins
                FROM mdts_signed_on
                WHERE callSign = %s
                LIMIT 1
            """, (cs,))
            shift_row = cur.fetchone() or {}
            shift_state = _compute_shift_break_state(shift_row)
            if shift_state.get('break_blocked_for_new_jobs') and not job_is_high_priority:
                if not break_override:
                    blocked_for_break.append({
                        'callsign': cs,
                        'break_due_in_minutes': shift_state.get('break_due_in_minutes')
                    })
                    continue
                break_override_used.append({
                    'callsign': cs,
                    'break_due_in_minutes': shift_state.get('break_due_in_minutes')
                })
            if old_cad is not None:
                try:
                    old_cad = int(old_cad)
                except Exception:
                    old_cad = None
            # Reassign flow: stand down unit from previous CAD before assigning this CAD.
            if old_cad and old_cad != cad:
                reassigned_from.add(old_cad)
                cur.execute(
                    "DELETE FROM mdt_job_units WHERE job_cad = %s AND callsign = %s", (old_cad, cs))
                _sync_claimed_by_from_job_units(cur, old_cad)
                cur.execute(
                    "SELECT status FROM mdt_jobs WHERE cad = %s LIMIT 1", (old_cad,))
                old_row = cur.fetchone() or {}
                old_status = str(old_row.get('status') or '').strip().lower()
                remaining_old_units = _get_job_unit_callsigns(cur, old_cad)
                # Superseded incident: once no units remain, return it to dispatch queue
                # (unallocated) unless it has been explicitly cleared or awaits closure review.
                if len(remaining_old_units) == 0 and old_status not in _JOB_STATUSES_SKIP_EMPTY_UNLINK_QUEUE:
                    cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
                    has_updated_at = cur.fetchone() is not None
                    if has_updated_at:
                        cur.execute(
                            "UPDATE mdt_jobs SET status = 'queued', updated_at = NOW() WHERE cad = %s AND LOWER(TRIM(COALESCE(status, ''))) <> 'cleared'",
                            (old_cad,)
                        )
                    else:
                        cur.execute(
                            "UPDATE mdt_jobs SET status = 'queued' WHERE cad = %s AND LOWER(TRIM(COALESCE(status, ''))) <> 'cleared'",
                            (old_cad,)
                        )
                try:
                    cur.execute("""
                        INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew)
                        VALUES (%s, %s, %s, NOW(), %s)
                    """, (cs, old_cad, 'stood_down', crew_json))
                except Exception:
                    pass
                if has_messages:
                    cur.execute("""
                        INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                        VALUES (%s, %s, %s, NOW(), 0)
                    """, (sender_name, cs, f"Stand down from CAD #{old_cad}. Reassigned to CAD #{cad}."))
            unit_division = _normalize_division(
                unit.get('division'), fallback='general')
            if access.get('restricted') and unit_division not in set(access.get('divisions') or []) and not access.get('can_override_all'):
                missing.append(cs)
                continue
            should_transfer = bool(
                has_unit_div and transfer_division and unit_division != job_division)
            if should_transfer and access.get('restricted') and not access.get('can_override_all'):
                # restricted dispatchers can only transfer units into their own allowed divisions
                if job_division not in set(access.get('divisions') or []):
                    should_transfer = False
            cur.execute("""
                UPDATE mdts_signed_on
                   SET assignedIncident = %s, status = 'assigned'{division_set}
                 WHERE callSign = %s
            """.format(division_set=", division = %s" if should_transfer else ""), tuple(
                [cad] + ([job_division] if should_transfer else []) + [cs]
            ))
            if should_transfer:
                instruction_id = _queue_division_transfer_instruction(
                    cur,
                    cs,
                    unit_division,
                    job_division,
                    actor=operator or None,
                    reason='job_assignment_transfer',
                    cad=cad
                )
                if instruction_id:
                    division_transfer_notifications.append({
                        'callsign': cs,
                        'instruction_id': instruction_id
                    })
            cur.execute("""
                INSERT INTO mdt_job_units (job_cad, callsign, assigned_by)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    assigned_by = VALUES(assigned_by),
                    assigned_at = CURRENT_TIMESTAMP
            """, (cad, cs, getattr(current_user, 'username', 'unknown')))
            try:
                cur.execute("""
                    INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew)
                    VALUES (%s, %s, %s, NOW(), %s)
                """, (cs, cad, 'assigned', unit.get('crew') or '[]'))
            except Exception:
                pass
            assigned.append(cs)

        if not assigned:
            conn.rollback()
            return jsonify({
                'error': 'No valid units selected',
                'missing': missing,
                'blocked_for_break': blocked_for_break,
                'requires_break_override': bool(blocked_for_break and not break_override)
            }), 409

        _sync_claimed_by_from_job_units(cur, cad)

        if break_override_used:
            override_entry = {
                'at': datetime.utcnow().isoformat(),
                'by': str(getattr(current_user, 'username', '') or '').strip() or 'unknown',
                'units': [str(x.get('callsign') or '').strip() for x in break_override_used if str(x.get('callsign') or '').strip()]
            }
            history = job_payload.get('break_override_history')
            if not isinstance(history, list):
                history = []
            history.append(override_entry)
            history = history[-20:]
            job_payload['break_override_history'] = history
            job_payload['break_override_last_at'] = override_entry['at']
            job_payload['break_override_last_by'] = override_entry['by']
            job_payload['break_override_last_units'] = override_entry['units']
            cur.execute(
                "UPDATE mdt_jobs SET data = %s WHERE cad = %s",
                (json.dumps(job_payload, default=str), cad)
            )

        if not log_audit(
            getattr(current_user, 'username', 'unknown'),
            'assign_job',
            details={'cad': cad, 'to': assigned, 'missing': missing},
            audit_failure='fail_closed',
        ):
            conn.rollback()
            return _ventus_audit_unavailable_response()
        job_data_for_bridge = job.get("data")
        conn.commit()
        try:
            from . import broadnet_bridge as _bnb
            _bnb.post_for_assignment_if_enabled(
                job_cad=cad,
                assigned_callsigns=assigned,
                job_data_raw=job_data_for_bridge,
            )
        except Exception:
            logger.exception("external dispatch bridge after assign_job")
        # Notify connected realtime clients to refresh job lists/maps
        try:
            socketio.emit(
                'mdt_event', {'type': 'jobs_updated', 'cad': cad})
            for old_cad in sorted(reassigned_from):
                socketio.emit(
                    'mdt_event', {'type': 'jobs_updated', 'cad': old_cad})
            for cs in assigned:
                socketio.emit(
                    'mdt_event', {'type': 'units_updated', 'callsign': cs})
            _emit_mdt_job_assigned(cad, assigned)
            for notif in division_transfer_notifications:
                socketio.emit('mdt_event', {
                    'type': 'dispatch_instruction',
                    'callsign': notif.get('callsign'),
                    'instruction_id': notif.get('instruction_id'),
                    'instruction_type': 'division_transfer'
                })
        except Exception:
            pass
        try:
            logger.info('Job assigned: cad=%s by=%s to=%s missing=%s', cad, getattr(
                current_user, 'username', 'unknown'), ",".join(assigned), ",".join(missing))
        except Exception:
            pass
        return jsonify({
            'message': 'Job assigned',
            'cad': cad,
            'assigned': assigned,
            'missing': missing,
            'division': job_division,
            'division_transfer_instructions': division_transfer_notifications,
            'job_priority': priority_value or None,
            'break_override': break_override,
            'break_override_used': break_override_used,
            'blocked_for_break': blocked_for_break
        })
    finally:
        cur.close()
        conn.close()


@internal.route('/job/<int:cad>/unassign', methods=['POST'])
@login_required
def unassign_job_unit(cad):
    data = request.get_json() or {}
    callsigns = data.get('callsigns')
    if not isinstance(callsigns, list):
        single = data.get('callsign')
        callsigns = [single] if single else []
    callsigns = [str(c).strip() for c in callsigns if str(c).strip()]
    callsigns = list(dict.fromkeys(callsigns))
    if not callsigns:
        return jsonify({'error': 'callsign(s) required'}), 400

    if not _ventus_dispatch_capable():
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_job_units_table(cur)
        sender_name = _sender_label_from_portal(
            'dispatch', getattr(current_user, 'username', ''))
        cur.execute("SHOW TABLES LIKE 'messages'")
        has_messages = cur.fetchone() is not None
        released = []
        for cs in callsigns:
            cur.execute(
                "SELECT crew FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (cs,))
            row = cur.fetchone() or {}
            crew_json = row.get('crew') or '[]'
            cur.execute("""
                UPDATE mdts_signed_on
                   SET assignedIncident = NULL, status = 'on_standby'
                 WHERE callSign = %s AND assignedIncident = %s
            """, (cs, cad))
            cur.execute(
                "DELETE FROM mdt_job_units WHERE job_cad = %s AND callsign = %s", (cad, cs))
            if cur.rowcount >= 0:
                released.append(cs)
            try:
                cur.execute("""
                    INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew)
                    VALUES (%s, %s, %s, NOW(), %s)
                """, (cs, cad, 'stood_down', crew_json))
            except Exception:
                pass
            if has_messages:
                cur.execute("""
                    INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                    VALUES (%s, %s, %s, NOW(), 0)
                """, (sender_name, cs, f"Stood down from CAD #{cad}. Await further assignment."))

        remaining = _sync_claimed_by_from_job_units(cur, cad)
        cur.execute(
            "SELECT status FROM mdt_jobs WHERE cad = %s LIMIT 1", (cad,))
        job = cur.fetchone() or {}
        st = str(job.get('status') or '').strip().lower()
        if st in ('assigned', 'claimed') and len(remaining) == 0:
            cur.execute(
                "UPDATE mdt_jobs SET status = 'queued' WHERE cad = %s AND status IN ('assigned','claimed')", (cad,))

        try:
            from . import broadnet_bridge as _bnb
            _bnb.delete_dispatch_push_rows(cur, cad, callsigns)
        except Exception:
            pass

        conn.commit()
        try:
            socketio.emit('mdt_event', {'type': 'jobs_updated', 'cad': cad})
            for cs in released:
                socketio.emit(
                    'mdt_event', {'type': 'units_updated', 'callsign': cs})
        except Exception:
            pass
        log_audit(
            getattr(current_user, 'username', 'unknown'),
            'unassign_job_units',
            details={'cad': cad, 'released': released, 'remaining': remaining},
        )
        return jsonify({'message': 'Units stood down', 'cad': cad, 'released': released, 'remaining': remaining}), 200
    finally:
        cur.close()
        conn.close()


@internal.route('/job/<int:cad>/standdown', methods=['POST'])
@login_required
def standdown_job(cad):
    data = request.get_json() or {}
    reason = str(data.get('reason') or data.get(
        'outcome') or 'stood_down').strip()
    allowed_roles = ["dispatcher", "admin", "superuser", "clinical_lead",
                     "controller", "call_taker", "calltaker", "call_handler", "callhandler"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_job_units_table(cur)
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
        has_updated_at = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'final_status'")
        has_final_status = cur.fetchone() is not None
        if has_final_status and has_updated_at:
            cur.execute(
                "UPDATE mdt_jobs SET status='stood_down', final_status=%s, updated_at=NOW() WHERE cad=%s", (reason, cad))
        elif has_final_status:
            cur.execute(
                "UPDATE mdt_jobs SET status='stood_down', final_status=%s WHERE cad=%s", (reason, cad))
        elif has_updated_at:
            cur.execute(
                "UPDATE mdt_jobs SET status='stood_down', updated_at=NOW() WHERE cad=%s", (cad,))
        else:
            cur.execute(
                "UPDATE mdt_jobs SET status='stood_down' WHERE cad=%s", (cad,))
        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({'error': 'Job not found'}), 404

        sender_name = _sender_label_from_portal(
            'dispatch', getattr(current_user, 'username', ''))
        cur.execute("SHOW TABLES LIKE 'messages'")
        has_messages = cur.fetchone() is not None
        callsigns = _collect_job_callsigns(cur, cad)
        released = []
        for cs in callsigns:
            cur.execute(
                "SELECT crew FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (cs,))
            row = cur.fetchone() or {}
            crew_json = row.get('crew') or '[]'
            cur.execute("""
                UPDATE mdts_signed_on
                   SET assignedIncident = NULL, status = 'on_standby'
                 WHERE callSign = %s AND assignedIncident = %s
            """, (cs, cad))
            released.append(cs)
            try:
                cur.execute("""
                    INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew)
                    VALUES (%s, %s, %s, NOW(), %s)
                """, (cs, cad, 'stood_down', crew_json))
            except Exception:
                pass
            if has_messages:
                cur.execute("""
                    INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                    VALUES (%s, %s, %s, NOW(), 0)
                """, (sender_name, cs, f"CAD #{cad} stood down. Reason: {reason}"))

        cur.execute("DELETE FROM mdt_job_units WHERE job_cad = %s", (cad,))
        _sync_claimed_by_from_job_units(cur, cad)
        try:
            from . import broadnet_bridge as _bnb
            _bnb.delete_all_push_for_cad(cur, cad)
        except Exception:
            pass
        if not log_audit(
            getattr(current_user, 'username', 'unknown'),
            'standdown_job',
            details={'cad': cad, 'reason': reason, 'released': released},
            audit_failure='fail_closed',
        ):
            conn.rollback()
            return _ventus_audit_unavailable_response()
        conn.commit()
        try:
            socketio.emit('mdt_event', {'type': 'jobs_updated', 'cad': cad})
            for cs in released:
                socketio.emit(
                    'mdt_event', {'type': 'units_updated', 'callsign': cs})
        except Exception:
            pass
        return jsonify({'message': 'Job stood down', 'cad': cad, 'reason': reason, 'released': released}), 200
    finally:
        cur.close()
        conn.close()


@internal.route('/cad', methods=['GET'])
@login_required
def cad_dashboard():
    if not _ventus_dispatch_capable():
        flash(
            "You do not have dispatcher access. Contact your administrator if you need the Ventus dispatcher role or permission.",
            "danger",
        )
        return redirect(url_for("routes.dashboard"))
    _inds = normalize_organization_industries(
        current_app.config.get("organization_industries"))
    _med = tenant_matches_industry(_inds, "medical")
    _cad_labels = {
        "showCuraPolicyAdminLink": bool(
            _med and _cad_user_can_edit_cura_dispatch_policy()),
        "jobDetailSubjectRow": "Patient" if _med else "Subject / contact",
        "intakeCommRole": "Triage Officer" if _med else "Intake desk",
        "panicOnJobHeadline": (
            "PANIC ON PATIENT CAD" if _med else "PANIC ON ACTIVE JOB"),
        "panicPatientCadAudit": (
            "Panic logged on this patient CAD for audit trail (timestamps + job record). Send backup immediately."
            if _med else
            "Panic logged on this CAD for audit trail (timestamps + job record). Send backup immediately."),
    }
    _mo_tile = _cad_metoffice_leaflet_tile_url_for_dashboard()
    _mo_img = _cad_metoffice_weather_image_overlay_url_for_dashboard()
    _mo_layers = None
    _mo_default_fid = None
    if _mo_img:
        try:
            _cad_metoffice_map_images_resolve_preview_png()
        except Exception:
            logger.debug("Met Office map-images: preview warm-up failed", exc_info=True)
        try:
            _man = _cad_metoffice_map_images_get_layer_manifest()
            _mo_layers = _man.get("layers") or []
            _mo_default_fid = _man.get("defaultFileId")
        except Exception:
            logger.debug("Met Office map-images: layer manifest failed", exc_info=True)
            _mo_layers = []
            _mo_default_fid = None
    _mo_site_fc = _cad_metoffice_site_forecast_proxy_url_for_dashboard()
    return render_template(
        "cad/dashboard.html",
        config=core_manifest,
        cad_show_cura_policy_admin_link=_cad_labels["showCuraPolicyAdminLink"],
        cad_industry_ui_labels=_cad_labels,
        cad_metoffice_tile_url=_mo_tile,
        cad_metoffice_weather_image_url=_mo_img,
        cad_metoffice_weather_image_bounds=(
            _cad_metoffice_weather_image_overlay_bounds() if _mo_img else None),
        cad_metoffice_weather_image_layers=_mo_layers,
        cad_metoffice_weather_image_default_file_id=_mo_default_fid,
        cad_metoffice_site_forecast_url=_mo_site_fc,
    )


@internal.route('/cad/major-incident-declare', methods=['GET'])
@login_required
def cad_major_incident_declare_page():
    """Full-page major incident declaration for a specific CAD (same API as modal / MDT)."""
    if not _ventus_dispatch_capable():
        flash(
            "You do not have dispatcher access. Contact your administrator if you need the Ventus dispatcher role or permission.",
            "danger",
        )
        return redirect(url_for("routes.dashboard"))
    cad = request.args.get('cad', type=int)
    if not cad or cad <= 0:
        flash("A valid CAD number is required to open the declaration form.", "warning")
        return redirect(url_for(".cad_dashboard"))
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT cad FROM mdt_jobs WHERE cad = %s LIMIT 1", (cad,))
        row = cur.fetchone()
        if not row:
            flash("That CAD was not found.", "danger")
            return redirect(url_for(".cad_dashboard"))
    finally:
        cur.close()
        conn.close()
    return_url = url_for("medical_response_internal.cad_dashboard", cad=cad, panel="jobs")
    return render_template(
        "cad/major_incident_declare.html",
        config=core_manifest,
        cad_mi_declare_cad=cad,
        cad_mi_declare_fixed_cad=cad,
        cad_mi_return_url=return_url,
    )


@internal.route('/job/<int:cad>/close', methods=['POST'])
@login_required
def close_job(cad):
    """Close/clear a job/incident with notes and outcome.

    CADs in ``dispatch_review`` (closure review after last unit cleared on MDT) stay on the stack until
    dispatch posts here. Without ``force_status``, the job is set to ``cleared`` (any prior non-cleared
    stack status). ``force_status`` is only for exceptional overrides (e.g. automation); the CAD UI
    uses the normal path.
    """
    data = request.get_json() or {}
    private_notes = data.get('private_notes', '').strip()
    public_notes = data.get('public_notes', '').strip()
    # outcome: completed, cancelled, transferred, etc.
    outcome = data.get('outcome', 'completed').strip()
    # Only set if dispatcher is forcing due to issue
    force_status = data.get('force_status', None)

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # Authorization: only dispatchers/admins/clinical lead may close jobs
        allowed_roles = ["dispatcher", "admin", "superuser", "clinical_lead",
                         "controller", "call_taker", "calltaker", "call_handler", "callhandler"]
        if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
            return jsonify({'error': 'Unauthorised'}), 403

        cur.execute(
            """
            SELECT LOWER(TRIM(COALESCE(status, ''))) AS st
            FROM mdt_jobs WHERE cad = %s LIMIT 1
            """,
            (cad,),
        )
        pre_close = cur.fetchone()
        if not pre_close:
            return jsonify({'error': 'Job not found'}), 404
        prior_stack = str(pre_close.get('st') or '').strip().lower()
        if prior_stack == 'cleared':
            return jsonify({'error': 'Job cannot be closed'}), 409
        prior_stack_disp = _stack_status_pretty(prior_stack)

        # If force_status provided, dispatcher is overriding due to comms issue, etc.
        if force_status:
            cur.execute("""
                UPDATE mdt_jobs
                   SET status = %s,
                       final_status = %s,
                       private_notes = %s,
                       public_notes = %s,
                       updated_at = NOW()
                 WHERE cad = %s AND status != 'cleared'
            """, (force_status, outcome, private_notes, public_notes, cad))
        else:
            # Normal close: set stack status cleared; record outcome in final_status
            cur.execute("""
                UPDATE mdt_jobs
                   SET final_status = %s,
                       private_notes = %s,
                       public_notes = %s,
                       status = 'cleared',
                       updated_at = NOW()
                 WHERE cad = %s AND status != 'cleared'
            """, (outcome, private_notes, public_notes, cad))

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({'error': 'Job cannot be closed'}), 409

        try:
            cur.execute("SELECT data FROM mdt_jobs WHERE cad = %s LIMIT 1", (cad,))
            _close_data_row = cur.fetchone() or {}
            _close_payload = _mdt_job_data_dict(_close_data_row.get('data'))
            _strip_resolved_grade1_panic_from_job_payload(_close_payload)
            cur.execute(
                "UPDATE mdt_jobs SET data = %s WHERE cad = %s",
                (json.dumps(_close_payload, default=str), cad),
            )
        except Exception:
            logger.exception("close_job strip panic payload cad=%s", cad)

        try:
            cur.execute(
                """
                SELECT LOWER(TRIM(COALESCE(division, 'general'))) AS division,
                       LOWER(TRIM(COALESCE(status, ''))) AS st
                FROM mdt_jobs WHERE cad = %s LIMIT 1
                """,
                (cad,),
            )
            snap_row = cur.fetchone() or {}
            if str((snap_row.get('st') or '')).lower().strip() == 'cleared':
                _ensure_job_division_snapshot_columns(cur)
                _sync_job_division_snapshot_for_slug(
                    cur, cad, snap_row.get('division') or 'general')
        except Exception:
            logger.exception("division snapshot on close failed cad=%s", cad)

        # Release any linked units and return them to polling state.
        _ensure_job_units_table(cur)
        sender_name = _sender_label_from_portal(
            'dispatch', getattr(current_user, 'username', ''))
        cur.execute("SHOW TABLES LIKE 'messages'")
        has_messages = cur.fetchone() is not None
        callsigns = _collect_job_callsigns(cur, cad)
        for cs in callsigns:
            cur.execute(
                "SELECT crew FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (cs,))
            row = cur.fetchone() or {}
            crew_json = row.get('crew') or '[]'
            cur.execute("""
                UPDATE mdts_signed_on
                   SET assignedIncident = NULL, status = 'on_standby'
                 WHERE callSign = %s AND assignedIncident = %s
            """, (cs, cad))
            try:
                cur.execute("""
                    INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew)
                    VALUES (%s, %s, %s, NOW(), %s)
                """, (cs, cad, 'cleared', crew_json))
            except Exception:
                pass
            if has_messages:
                cur.execute("""
                    INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                    VALUES (%s, %s, %s, NOW(), 0)
                """, (sender_name, cs, f"CAD #{cad} closed ({outcome}). Stand down and return to polling."))

        cur.execute("DELETE FROM mdt_job_units WHERE job_cad = %s", (cad,))
        try:
            from . import broadnet_bridge as _bnb
            _bnb.delete_all_push_for_cad(cur, cad)
        except Exception:
            pass
        _sync_claimed_by_from_job_units(cur, cad)
        _ensure_job_comms_table(cur)
        closer_user = getattr(current_user, 'username', None) or getattr(
            current_user, 'id', None) or 'dispatcher'
        close_comm_text = (
            f"CAD closed ({outcome}). Last known stack status: {prior_stack_disp}."
        )
        if force_status:
            close_comm_text = (
                f"{close_comm_text} Forced final status on record: {_stack_status_pretty(force_status)}."
            )
        if private_notes:
            close_comm_text = f"{close_comm_text} Closure: {private_notes.strip()}"
        if len(close_comm_text) > 4000:
            close_comm_text = close_comm_text[:3997] + '…'
        cur.execute(
            """
            INSERT INTO mdt_job_comms (cad, message_type, sender_role, sender_user, message_text)
            VALUES (%s, 'closed', 'dispatch', %s, %s)
            """,
            (cad, str(closer_user), close_comm_text[:4000])
        )
        if not log_audit(
            getattr(current_user, 'username', 'unknown'),
            'close_job',
            details={
                'cad': cad,
                'outcome': outcome,
                'forced': bool(force_status),
                'prior_stack_status': prior_stack,
                'prior_stack_status_display': prior_stack_disp,
                'closure_notes': (private_notes or '')[:2000] or None,
            },
            audit_failure='fail_closed',
        ):
            conn.rollback()
            return _ventus_audit_unavailable_response()
        conn.commit()

        try:
            emit_msg = {'type': 'jobs_updated', 'cad': cad}
            if public_notes:
                emit_msg['public_notes'] = public_notes
            if force_status:
                emit_msg['forced_status'] = force_status
            socketio.emit('mdt_event', emit_msg)
        except Exception:
            pass

        try:
            logger.info('Job closed: cad=%s outcome=%s forced=%s by=%s', cad, outcome, bool(force_status), getattr(
                current_user, 'username', 'unknown'))
        except Exception:
            pass

        return jsonify({'message': 'Job closed', 'cad': cad, 'outcome': outcome, 'forced': bool(force_status)})
    finally:
        cur.close()
        conn.close()


_REOPEN_REASON_LABELS = {
    'closed_in_error': 'Closed in error',
    'test_training': 'Test/Training',
    'other': 'Other',
}


@internal.route('/job/<int:cad>/reopen', methods=['POST'])
@login_required
def reopen_job(cad):
    """Return a cleared or stood-down CAD to the active stack as ``queued`` (e.g. closed in error)."""
    allowed_roles = ["dispatcher", "admin", "superuser", "clinical_lead",
                     "controller", "call_taker", "calltaker", "call_handler", "callhandler"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        return jsonify({'error': 'Unauthorised'}), 403

    data = request.get_json(silent=True) or {}
    code = str(data.get('reopen_reason') or '').strip().lower()
    legacy_note = str(data.get('reason') or '').strip()
    other_details = str(data.get('other_details') or '').strip()

    if not code:
        if legacy_note:
            code = 'other'
            other_details = legacy_note[:4000]
        else:
            return jsonify({'error': 'Reopen reason is required'}), 400
    if code not in _REOPEN_REASON_LABELS:
        return jsonify({'error': 'Invalid reopen reason'}), 400
    if code == 'other':
        if not other_details:
            return jsonify({'error': 'Further details are required when reopen reason is Other'}), 400
        if len(other_details) > 4000:
            other_details = other_details[:4000]
    else:
        other_details = ''

    label = _REOPEN_REASON_LABELS[code]
    if code == 'other':
        reason_human = f'{label}: {other_details}'
    else:
        reason_human = label

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT LOWER(TRIM(COALESCE(status, ''))) AS st FROM mdt_jobs WHERE cad = %s LIMIT 1",
            (cad,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Job not found'}), 404
        st = str(row.get('st') or '').strip().lower()
        if st not in _JOB_STATUSES_PAST_OR_CLOSED_STACK:
            return jsonify({
                'error': 'Only cleared or stood-down CADs can be reopened onto the stack',
                'status': st,
            }), 409

        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
        has_updated_at = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'final_status'")
        has_final_status = cur.fetchone() is not None

        actor = str(getattr(current_user, 'username', 'unknown') or 'unknown').strip()
        where_st = "LOWER(TRIM(COALESCE(status, ''))) IN ('cleared', 'stood_down')"
        if has_final_status and has_updated_at:
            cur.execute(
                f"""
                UPDATE mdt_jobs
                   SET status = 'queued',
                       final_status = NULL,
                       updated_at = NOW()
                 WHERE cad = %s AND {where_st}
                """,
                (cad,),
            )
        elif has_final_status:
            cur.execute(
                f"""
                UPDATE mdt_jobs
                   SET status = 'queued',
                       final_status = NULL
                 WHERE cad = %s AND {where_st}
                """,
                (cad,),
            )
        elif has_updated_at:
            cur.execute(
                f"""
                UPDATE mdt_jobs
                   SET status = 'queued', updated_at = NOW()
                 WHERE cad = %s AND {where_st}
                """,
                (cad,),
            )
        else:
            cur.execute(
                f"UPDATE mdt_jobs SET status = 'queued' WHERE cad = %s AND {where_st}",
                (cad,),
            )

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({'error': 'Job could not be reopened'}), 409

        try:
            _ensure_job_incident_inbox_anchor_column(cur)
            _ensure_job_comms_table(cur)
            cur.execute(
                "SELECT COALESCE(MAX(id), 0) AS mx FROM mdt_job_comms WHERE cad = %s",
                (cad,),
            )
            _mx_row = cur.fetchone() or {}
            inbox_after_id = int(_mx_row.get('mx') or 0)
            cur.execute("SELECT data FROM mdt_jobs WHERE cad = %s LIMIT 1", (cad,))
            _reopen_data_row = cur.fetchone() or {}
            _reopen_payload = _mdt_job_data_dict(_reopen_data_row.get('data'))
            _strip_resolved_grade1_panic_from_job_payload(_reopen_payload)
            cur.execute(
                """
                UPDATE mdt_jobs
                   SET data = %s,
                       incident_inbox_comms_after_id = %s
                 WHERE cad = %s
                """,
                (json.dumps(_reopen_payload, default=str), inbox_after_id, cad),
            )
        except Exception:
            logger.exception('reopen_job strip panic / inbox anchor cad=%s', cad)

        try:
            _ensure_response_log_table(cur)
            actor_cs = actor[:64] if len(actor) > 64 else actor
            reopen_meta = {
                'prior_job_status': st,
                'reopen_reason': code,
                'reopen_reason_label': label,
            }
            if code == 'other':
                reopen_meta['other_details'] = other_details
            if reason_human:
                reopen_meta['reason'] = reason_human
            cur.execute(
                """
                INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew)
                VALUES (%s, %s, %s, NOW(), %s)
                """,
                (
                    actor_cs,
                    cad,
                    _RESPONSE_LOG_STATUS_DISPATCH_REOPENED,
                    json.dumps(reopen_meta, default=str),
                ),
            )
        except Exception:
            logger.exception('response_log dispatch_reopened cad=%s', cad)

        try:
            _ensure_job_comms_table(cur)
            msg = (
                f"CAD #{cad} returned to the dispatch stack as queued. "
                f"Classification: {reason_human}"
            )
            cur.execute(
                """
                INSERT INTO mdt_job_comms (cad, message_type, sender_role, sender_user, message_text)
                VALUES (%s, 'reopened', 'dispatch', %s, %s)
                """,
                (cad, actor, msg[:4000]),
            )
        except Exception:
            pass

        try:
            _ensure_job_units_table(cur)
            _sync_claimed_by_from_job_units(cur, cad)
        except Exception:
            logger.exception('reopen_job sync job units cad=%s', cad)

        if not log_audit(
            actor,
            'reopen_job',
            details={
                'cad': cad,
                'prior_status': st,
                'reopen_reason': code,
                'other_details': other_details if code == 'other' else None,
                'reason': reason_human,
            },
            audit_failure='fail_closed',
        ):
            conn.rollback()
            return _ventus_audit_unavailable_response()

        conn.commit()

        try:
            socketio.emit('mdt_event', {'type': 'jobs_updated', 'cad': cad})
        except Exception:
            pass

        return jsonify({'message': 'CAD reopened', 'cad': cad, 'status': 'queued'})
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/job/<int:cad>/force-status', methods=['POST'])
@login_required
def force_job_status(cad):
    """Dispatcher force-set job status due to comms issue or error. Use sparingly."""
    data = request.get_json() or {}
    new_status = data.get('status', '').strip()
    reason = data.get('reason', 'Unknown').strip()

    if not new_status:
        return jsonify({'error': 'status required'}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if not _ventus_dispatch_capable():
            return jsonify({'error': 'Unauthorised'}), 403

        cur.execute("""
            UPDATE mdt_jobs
               SET status = %s,
                   private_notes = CONCAT(IFNULL(private_notes, ''), 
                     '\n[Forced by ', %s, ': ', %s, ' at ', DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%S'), ']')
             WHERE cad = %s
        """, (new_status, getattr(current_user, 'username', 'dispatcher'), reason, cad))

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({'error': 'Job not found'}), 404
        conn.commit()

        try:
            socketio.emit('mdt_event',
                          {'type': 'status_forced', 'cad': cad, 'status': new_status,
                              'by': getattr(current_user, 'username', 'dispatcher')})
        except Exception:
            pass

        try:
            logger.warning('Status forced: cad=%s new_status=%s reason=%s by=%s', cad, new_status, reason, getattr(
                current_user, 'username', 'unknown'))
        except Exception:
            pass
        log_audit(
            getattr(current_user, 'username', 'unknown'),
            'force_job_status',
            details={'cad': cad, 'status': new_status, 'reason': reason},
        )

        return jsonify({'message': 'Status forced', 'cad': cad, 'status': new_status})
    finally:
        cur.close()
        conn.close()


@internal.route('/response', methods=['GET'])
@login_required
def response_dashboard():
    if not _user_has_role("crew", "dispatcher", "admin", "superuser", "clinical_lead", "call_taker", "calltaker", "controller"):
        return jsonify({"error": "Unauthorised access"}), 403
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        forms = _load_triage_forms(cur)
    finally:
        cur.close()
        conn.close()
    triage_forms = _triage_forms_intake_visible(forms)
    triage_forms_coming_soon = [
        f for f in forms if _intake_form_category(f.get("slug")) == "coming_soon"]
    return render_template(
        "response/dashboard.html",
        config=core_manifest,
        triage_forms=triage_forms,
        triage_forms_coming_soon=triage_forms_coming_soon,
    )


@internal.route('/response/forms', methods=['GET', 'POST', 'DELETE'])
@login_required
def response_forms():
    if not _user_has_role("crew", "dispatcher", "admin", "superuser", "clinical_lead", "call_taker", "calltaker", "controller"):
        return jsonify({"error": "Unauthorised access"}), 403
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if request.method == 'GET':
            return jsonify(
                _triage_forms_intake_visible(_load_triage_forms(cur)))

        edit_roles = ["admin", "superuser", "clinical_lead"]
        if current_user.role.lower() not in edit_roles:
            return jsonify({"error": "Unauthorised"}), 403

        if request.method == 'DELETE':
            cur.execute("SHOW TABLES LIKE 'mdt_triage_forms'")
            if cur.fetchone() is None:
                return jsonify({"error": "No stored profiles"}), 404
            payload = request.get_json(silent=True) or {}
            slug = str(
                payload.get("slug") or request.args.get("slug") or ""
            ).strip().lower().replace(" ", "_")
            if not slug:
                return jsonify({"error": "slug is required"}), 400
            cur.execute(
                """
                SELECT slug, name, is_default
                FROM mdt_triage_forms
                WHERE slug = %s AND is_active = 1
                LIMIT 1
                """,
                (slug,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Profile not found"}), 404
            was_default = bool(row.get("is_default"))
            display_name = str(row.get("name") or slug).strip()
            cur.execute(
                "DELETE FROM mdt_triage_forms WHERE slug = %s", (slug,))
            if was_default:
                cur.execute(
                    """
                    SELECT slug FROM mdt_triage_forms
                    WHERE is_active = 1
                    ORDER BY name ASC, slug ASC
                    LIMIT 1
                    """
                )
                nxt = cur.fetchone()
                if nxt and nxt.get("slug"):
                    cur.execute(
                        "UPDATE mdt_triage_forms SET is_default = 1 WHERE slug = %s",
                        (nxt["slug"],),
                    )
            conn.commit()
            log_audit(
                getattr(current_user, "username", "unknown"),
                "triage_form_deleted",
                details={"slug": slug, "name": display_name},
            )
            return jsonify({"message": "Profile deleted", "slug": slug})

        payload = request.get_json(silent=True) or {}
        slug = str(payload.get("slug") or "").strip().lower().replace(" ", "_")
        if not slug:
            return jsonify({"error": "slug is required"}), 400
        raw_form = {
            "slug": slug,
            "name": payload.get("name"),
            "description": payload.get("description"),
            "dispatch_division": payload.get("dispatch_division") or payload.get("division"),
            "show_exclusions": bool(payload.get("show_exclusions", False)),
            "questions": payload.get("questions") or [],
            "priority_config": payload.get("priority_config") or {}
        }
        is_default = bool(payload.get("is_default", False))
        normalized = _normalize_triage_form(raw_form)
        if not normalized:
            return jsonify({"error": "invalid form payload"}), 400
        schema = {
            "dispatch_division": normalized.get("dispatch_division") or "general",
            "show_exclusions": normalized["show_exclusions"],
            "questions": normalized["questions"],
            "priority_config": normalized.get("priority_config") or _default_priority_config()
        }
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mdt_triage_forms (
                id INT AUTO_INCREMENT PRIMARY KEY,
                slug VARCHAR(64) NOT NULL UNIQUE,
                name VARCHAR(120) NOT NULL,
                description VARCHAR(255),
                schema_json JSON NOT NULL,
                is_active TINYINT(1) NOT NULL DEFAULT 1,
                is_default TINYINT(1) NOT NULL DEFAULT 0,
                created_by VARCHAR(120),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        if is_default:
            cur.execute("UPDATE mdt_triage_forms SET is_default = 0")
        cur.execute("""
            INSERT INTO mdt_triage_forms (slug, name, description, schema_json, is_active, is_default, created_by)
            VALUES (%s, %s, %s, CAST(%s AS JSON), 1, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                description = VALUES(description),
                schema_json = VALUES(schema_json),
                is_active = 1,
                is_default = VALUES(is_default),
                updated_at = CURRENT_TIMESTAMP
        """, (
            normalized["slug"],
            normalized["name"],
            normalized["description"],
            json.dumps(schema),
            1 if is_default else 0,
            getattr(current_user, 'username', 'unknown')
        ))
        conn.commit()
        return jsonify({"message": "Form saved", "form": normalized})
    finally:
        cur.close()
        conn.close()


@internal.route('/response/patient-lookup', methods=['GET'])
@login_required
def response_patient_lookup():
    """Smart patient lookup using local triage history (name/address/postcode/phone)."""
    if not _user_has_role("crew", "dispatcher", "admin", "superuser", "clinical_lead", "call_taker", "calltaker", "controller"):
        return jsonify({"error": "Unauthorised access"}), 403

    q = str(request.args.get("q") or "").strip()
    raw_tokens = [x.strip()
                  for x in q.replace(",", " ").split(" ") if x.strip()]
    tokens = raw_tokens[:8]
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if q:
            searchable = (
                "first_name",
                "middle_name",
                "last_name",
                "phone_number",
                "postcode",
                "address",
                "vita_record_id",
            )
            params = []
            token_clauses = []
            for tok in (tokens or [q]):
                like = f"%{tok}%"
                token_clauses.append(
                    "(" +
                    " OR ".join([f"{col} LIKE %s" for col in searchable]) + ")"
                )
                params.extend([like] * len(searchable))
            where_sql = " AND ".join(token_clauses)
            sql = f"""
                SELECT id, vita_record_id, first_name, middle_name, last_name, patient_dob,
                       phone_number, address, postcode, created_at
                FROM response_triage
                WHERE {where_sql}
                ORDER BY created_at DESC
                LIMIT 200
            """
            cur.execute(sql, tuple(params))
        else:
            cur.execute("""
                SELECT id, vita_record_id, first_name, middle_name, last_name, patient_dob,
                       phone_number, address, postcode, created_at
                FROM response_triage
                ORDER BY created_at DESC
                LIMIT 30
            """)
        rows = cur.fetchall() or []

        scored = []
        ql = q.lower()
        for r in rows:
            score = 0
            if q:
                all_terms = tokens or [q]
                for key, weight in (
                    ("postcode", 5),
                    ("phone_number", 4),
                    ("address", 3),
                    ("last_name", 3),
                    ("first_name", 2),
                    ("middle_name", 1),
                    ("vita_record_id", 2),
                ):
                    val = str(r.get(key) or "").lower()
                    if not val:
                        continue
                    if ql == val:
                        score += weight * 2
                    if ql and ql in val:
                        score += weight
                    for term in all_terms:
                        tl = str(term or "").strip().lower()
                        if not tl:
                            continue
                        if tl == val:
                            score += weight * 2
                        elif tl in val:
                            score += weight
            scored.append((score, r))

        scored.sort(key=lambda x: (x[0], x[1].get(
            "created_at") or datetime.min), reverse=True)
        dedup = {}
        for score, r in scored:
            key = r.get("vita_record_id") or f"triage-{r.get('id')}"
            if key not in dedup:
                full_name = " ".join([str(r.get("first_name") or "").strip(), str(r.get(
                    "middle_name") or "").strip(), str(r.get("last_name") or "").strip()]).strip()
                dedup[key] = {
                    "id": r.get("id"),
                    "vita_record_id": r.get("vita_record_id"),
                    "full_name": full_name,
                    "first_name": r.get("first_name"),
                    "middle_name": r.get("middle_name"),
                    "last_name": r.get("last_name"),
                    "patient_dob": str(r.get("patient_dob") or ""),
                    "phone_number": r.get("phone_number"),
                    "address": r.get("address"),
                    "postcode": r.get("postcode"),
                    "score": score,
                    "source": "response_triage"
                }
                try:
                    dob = r.get("patient_dob")
                    if isinstance(dob, datetime):
                        dedup[key]["patient_age"] = calculate_age(dob.date())
                    elif isinstance(dob, date):
                        dedup[key]["patient_age"] = calculate_age(dob)
                    else:
                        dedup[key]["patient_age"] = None
                except Exception:
                    dedup[key]["patient_age"] = None
            if len(dedup) >= 25:
                break
        return jsonify({"patients": list(dedup.values())})
    except Exception as e:
        logger.exception("response_patient_lookup failed")
        return jsonify({"patients": [], "error": str(e)}), 200
    finally:
        cur.close()
        conn.close()


@internal.route("/response/mpi-flags-bundle", methods=["GET", "POST", "OPTIONS"])
@login_required
def response_mpi_flags_bundle():
    """
    MPI + premises-linked risk flags for triage/CAD (session auth; call-taker safe).
    Backed by medical_records_module.cura_mpi (cases-first, patient-linked architecture).
    """
    if request.method == "OPTIONS":
        return "", 200
    if not _user_has_role(
        "crew",
        "dispatcher",
        "admin",
        "superuser",
        "clinical_lead",
        "call_taker",
        "calltaker",
        "controller",
    ):
        return jsonify({"error": "Unauthorised access"}), 403

    if request.method == "GET":
        fields = {k: request.args.get(k) for k in (
            "mpi_patient_id",
            "nhs_number",
            "patient_dob",
            "postcode",
            "address",
            "phone_number",
            "last_name",
            "first_name",
        )}
    else:
        fields = request.get_json(silent=True) or {}

    try:
        from app.plugins.medical_records_module import cura_mpi
    except ImportError:
        return jsonify(
            {
                "mpiPatientId": None,
                "curaLocationId": None,
                "flags": [],
                "risk_flag_lines": [],
            }
        ), 200

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        bundle = cura_mpi.build_flags_bundle_from_triage_fields(cur, fields)
        cura_mpi.audit_log_mpi_access(
            cur,
            str(getattr(current_user, "username", "") or ""),
            "ventus_mpi_flags_bundle",
            {"mpi_patient_id": bundle.get("mpiPatientId")},
        )
        conn.commit()
    except Exception as e:
        logger.exception("response_mpi_flags_bundle failed: %s", e)
        conn.rollback()
        return jsonify({"error": "Unable to load risk flags", "details": str(e)}), 500
    finally:
        cur.close()
        conn.close()

    return jsonify(bundle), 200


def _nominatim_place_name_and_address(row):
    """Build a short title and address line from Nominatim jsonv2 (no coordinates)."""
    display_name = str(row.get("display_name") or "").strip()
    addr = row.get("address")
    if not isinstance(addr, dict):
        addr = {}
    amenity = str(addr.get("amenity") or "").strip().lower()
    health_amenities = {
        "hospital",
        "clinic",
        "doctors",
        "dentist",
        "pharmacy",
        "health_centre",
    }
    title = (
        addr.get("hospital")
        or addr.get("clinic")
        or (addr.get("amenity") if amenity in health_amenities else None)
    )
    title = str(title or "").strip()
    if not title and row.get("name"):
        title = str(row.get("name")).strip()
    if not title and display_name:
        title = display_name.split(",")[0].strip()

    order = (
        "house_number",
        "road",
        "pedestrian",
        "neighbourhood",
        "suburb",
        "city_district",
        "city",
        "town",
        "village",
        "municipality",
        "county",
        "state",
        "postcode",
    )
    seen = set()
    parts = []
    for k in order:
        v = addr.get(k)
        if not v:
            continue
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        parts.append(s)
    address_line = ", ".join(parts)
    if not address_line and display_name:
        bits = [b.strip() for b in display_name.split(",") if b.strip()]
        if len(bits) > 1:
            address_line = ", ".join(bits[1:])
    return (title or "Unknown").strip(), (address_line or "").strip()


@internal.route('/location-search', methods=['GET'])
@login_required
def location_search():
    """Lightweight location lookup (e.g. hospital destinations) for dispatch workflows."""
    if not _user_has_role("crew", "dispatcher", "admin", "superuser", "clinical_lead", "call_taker", "calltaker", "controller"):
        return jsonify({"error": "Unauthorised access"}), 403

    q = str(request.args.get("q") or "").strip()
    search_type = str(request.args.get("type") or "general").strip().lower()
    lat = _safe_float(request.args.get("lat"), None)
    lng = _safe_float(request.args.get("lng"), None)
    if not q:
        return jsonify({"items": []})

    query_variants = []
    base_query = q
    if search_type == "hospital" and "hospital" not in base_query.lower():
        base_query = f"{base_query} hospital"
    query_variants.append(base_query)

    # Fallbacks for long phrases that can over-constrain external geocoder matching.
    parts = [x for x in q.split() if x.strip()]
    if len(parts) >= 2:
        short = " ".join(parts[:3])
        if search_type == "hospital" and "hospital" not in short.lower():
            short = f"{short} hospital"
        if short and short not in query_variants:
            query_variants.append(short)
    if search_type == "hospital" and "hospital" not in q.lower():
        if "hospital" not in query_variants:
            query_variants.append("hospital")

    def _nominatim_lookup(query_text):
        params = {
            "q": query_text,
            "format": "jsonv2",
            "limit": 30,
            "addressdetails": 1,
        }
        if lat is not None and lng is not None:
            # Bias search around current map focus (~25km square).
            delta = 0.22
            left = lng - delta
            right = lng + delta
            top = lat + delta
            bottom = lat - delta
            params["viewbox"] = f"{left},{top},{right},{bottom}"
            params["bounded"] = 0
        url = "https://nominatim.openstreetmap.org/search?" + urlencode(params)
        req = Request(url, headers={
            "User-Agent": "VentusResponse/1.0 (Dispatch Location Search)"
        })
        with urlopen(req, timeout=6) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
            return payload if isinstance(payload, list) else []

    raw = []
    for variant in query_variants[:3]:
        try:
            raw = _nominatim_lookup(variant)
        except Exception:
            logger.exception(
                "location_search request failed for variant '%s'", variant)
            raw = []
        if raw:
            break

    items = []
    for row in (raw if isinstance(raw, list) else []):
        display_name = str(row.get("display_name") or "").strip()
        if not display_name:
            continue
        item_type = str(row.get("type") or "").strip().lower()
        item_class = str(row.get("class") or "").strip().lower()
        item_category = str(row.get("category") or "").strip().lower()
        if search_type == "hospital":
            transit_types = {
                "bus_stop",
                "tram_stop",
                "halt",
                "platform",
                "station",
                "bus_station",
                "subway_entrance",
            }
            if item_type in transit_types:
                continue
            if item_class in {"highway", "railway", "aerialway"}:
                continue
            hay = " ".join(
                [
                    display_name.lower(),
                    item_type,
                    item_class,
                    item_category,
                ]
            )
            health_types = {
                "hospital",
                "clinic",
                "doctors",
                "dentist",
                "pharmacy",
                "health_centre",
            }
            health_ok = item_type in health_types
            if not health_ok and "hospital" not in hay and "clinic" not in hay:
                continue
        title, address_line = _nominatim_place_name_and_address(row)
        rlat = _safe_float(row.get("lat"), None)
        rlng = _safe_float(row.get("lon"), None)
        distance_m = None
        if lat is not None and lng is not None and rlat is not None and rlng is not None:
            try:
                distance_m = int(
                    round(_haversine_km(lat, lng, rlat, rlng) * 1000))
            except Exception:
                distance_m = None
        items.append({
            "name": title,
            "address": address_line,
            "lat": rlat,
            "lng": rlng,
            "distance_m": distance_m,
        })

    if lat is not None and lng is not None:
        items.sort(key=lambda x: (x.get("distance_m")
                   is None, x.get("distance_m") or 10**12))

    return jsonify({"items": items[:15]})


@internal.route('/response/triage', methods=['GET', 'POST'])
@login_required
def triage_form():
    if not _user_has_role("crew", "dispatcher", "admin", "superuser", "clinical_lead", "call_taker", "calltaker", "controller"):
        return jsonify({"error": "Unauthorized access"}), 403
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        forms = _load_triage_forms(cur)
        dispatch_divisions = _list_dispatch_divisions(
            cur, include_inactive=False)
        access = _get_dispatch_user_division_access(
            cur, getattr(current_user, "username", None))
        default_dispatch_division = _get_dispatch_default_division(cur)
    finally:
        cur.close()
        conn.close()

    intake_forms = _triage_forms_intake_visible(forms)

    division_slugs = [str((d or {}).get("slug") or "").strip()
                      for d in (dispatch_divisions or [])]
    division_slugs = [s for s in division_slugs if s]
    restricted_divisions = [str(d or "").strip() for d in (access.get(
        "divisions") or []) if str(d or "").strip()] if isinstance(access, dict) else []
    can_override_all = bool((access or {}).get(
        "can_override_all")) if isinstance(access, dict) else False
    if restricted_divisions and not can_override_all:
        allowed_set = set(restricted_divisions)
        dispatch_divisions = [d for d in (dispatch_divisions or []) if str(
            (d or {}).get("slug") or "").strip() in allowed_set]
    if not dispatch_divisions:
        dispatch_divisions = [
            {"slug": "general", "name": "General", "color": "#64748b", "is_default": True}]
    selected_dispatch_division = _normalize_division(
        request.args.get("division") or request.form.get(
            "division") or default_dispatch_division or "general",
        fallback="general",
    )
    allowed_divisions = [str((d or {}).get("slug") or "").strip(
    ) for d in dispatch_divisions if str((d or {}).get("slug") or "").strip()]
    if selected_dispatch_division not in allowed_divisions:
        selected_dispatch_division = allowed_divisions[0] if allowed_divisions else "general"

    selected_slug = request.form.get(
        'form_slug') or request.args.get('form') or ''
    selected_form = _pick_triage_form(intake_forms, selected_slug)
    if _intake_form_category(selected_form.get("slug")) != "active":
        flash("This intake profile is not available.", "warning")
        return redirect(url_for(".response_dashboard"))
    triage_template_ctx = {
        "config": core_manifest,
        "triage_forms": intake_forms,
        "selected_form": selected_form,
        "dispatch_divisions": dispatch_divisions,
        "selected_dispatch_division": selected_dispatch_division,
        "google_maps_api_key": (GOOGLE_MAPS_API_KEY or "")
    }

    if request.method == 'POST':
        # Vita record ID
        vita_record_str = request.form.get('vita_record_id', '')
        vita_record_id = int(
            vita_record_str) if vita_record_str.isdigit() else None

        # Patient details
        first_name = request.form.get('first_name', '').strip()
        middle_name = request.form.get('middle_name', '').strip()
        last_name = request.form.get('last_name', '').strip()
        dob_str = request.form.get('patient_dob', '')
        phone_number = request.form.get('phone_number', '').strip()
        address = request.form.get('address', '').strip()
        postcode = request.form.get('postcode', '').strip()
        what3words = request.form.get('what3words', '').strip()
        manual_lat = _parse_intake_coordinate_field(
            request.form.get('manual_latitude'))
        manual_lng = _parse_intake_coordinate_field(
            request.form.get('manual_longitude'))
        caller_name = request.form.get('caller_name', '').strip()
        caller_phone = request.form.get('caller_phone', '').strip()
        patient_gender = _normalize_patient_gender(
            request.form.get('patient_gender', ''))
        additional_details = ''

        access_requirements_str = request.form.get('access_requirements', '')
        try:
            access_requirements = json.loads(
                access_requirements_str) if access_requirements_str.strip() else []
        except Exception:
            access_requirements = []
        if not isinstance(access_requirements, list):
            access_requirements = []

        # Triage information
        reason_for_call = request.form.get('reason_for_call', '').strip()
        onset_datetime = request.form.get('onset_datetime', '').strip()
        patient_alone = _normalize_patient_alone(
            request.form.get('patient_alone', ''))
        decision = str(request.form.get('decision', 'ACCEPT')).strip().upper()
        if decision not in ('ACCEPT', 'REJECT', 'ACCEPT_WITH_EXCLUSION', 'PENDING'):
            decision = 'ACCEPT'

        risk_flags_str = request.form.get('risk_flags', '')
        try:
            risk_flags = json.loads(
                risk_flags_str) if risk_flags_str.strip() else []
        except Exception:
            risk_flags = []
        if not isinstance(risk_flags, list):
            risk_flags = []

        # Convert date of birth
        patient_dob = None
        if dob_str:
            try:
                patient_dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
            except ValueError:
                flash("Invalid date format for Date of Birth.", "danger")
                return render_template("response/triage_form.html", **triage_template_ctx)

        onset_datetime_db = None
        if onset_datetime:
            parsed = None
            for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    parsed = datetime.strptime(onset_datetime, fmt)
                    break
                except Exception:
                    continue
            if parsed is None:
                flash("Invalid onset date/time format.", "danger")
                return render_template("response/triage_form.html", **triage_template_ctx)
            onset_datetime_db = parsed.strftime("%Y-%m-%d %H:%M:%S")

        # Gather exclusion responses only for forms that use them
        exclusion_data = {}
        if selected_form.get("show_exclusions"):
            exclusion_data = {key: request.form.get(
                key) for key in request.form if key.startswith("exclusion_")}

        # Dynamic per-form questions
        form_answers = {}
        for q in selected_form.get("questions") or []:
            key = str(q.get("key") or "")
            if not key:
                continue
            form_answers[key] = request.form.get(f"extra_{key}", "").strip()

        if not reason_for_call:
            reason_for_call = str(
                form_answers.get("primary_symptom")
                or form_answers.get("event_name")
                or form_answers.get("immediate_danger")
                or ""
            ).strip()

        priority_override = _normalize_priority_for_form(
            request.form.get('call_priority_override', ''),
            selected_form
        )
        if not priority_override:
            priority_override = _legacy_normalize_priority(
                request.form.get('call_priority_override', ''))
            priority_override = _normalize_priority_for_form(
                priority_override, selected_form)
        computed_priority = _compute_system_priority(
            reason_for_call=reason_for_call,
            selected_form=selected_form,
            decision=decision,
            exclusion_data=exclusion_data,
            form_answers=form_answers
        )
        call_priority = priority_override or computed_priority
        priority_source = 'manual' if priority_override else 'system'
        requested_division = _normalize_division(
            request.form.get('division'), fallback='')
        if not requested_division:
            requested_division = _normalize_division(selected_form.get(
                'dispatch_division') or selected_form.get('division'), fallback='')
        if not requested_division:
            slug = str(selected_form.get('slug') or '').strip().lower()
            if slug == 'emergency_999':
                requested_division = 'emergency'
            elif slug == 'urgent_care':
                requested_division = 'urgent_care'
            elif slug == 'event_medical':
                requested_division = 'events'
            else:
                requested_division = 'general'
        if requested_division not in allowed_divisions:
            requested_division = selected_dispatch_division or 'general'

        validation_errors = []
        if not reason_for_call:
            validation_errors.append("Reason for call is required.")
        have_manual_coords = manual_lat is not None and manual_lng is not None
        if (manual_lat is None) ^ (manual_lng is None):
            validation_errors.append(
                "Enter both latitude and longitude, or leave both blank.")
        if not address and not postcode and not what3words and not have_manual_coords:
            validation_errors.append(
                "Provide at least an address, postcode, what3words, or both latitude and longitude."
            )
        for q in selected_form.get("questions") or []:
            if q.get("required") and not str(form_answers.get(q.get("key"), "")).strip():
                validation_errors.append(
                    f"{q.get('label') or q.get('key')} is required.")
        if validation_errors:
            for msg in validation_errors:
                flash(msg, "danger")
            return render_template("response/triage_form.html", **triage_template_ctx)

        # Coordinates: explicit lat/lng from intake overrides what3words / geocoding.
        best_coordinates = ResponseTriage.get_best_lat_lng(
            address=address,
            postcode=postcode,
            what3words=what3words,
            manual_lat=manual_lat,
            manual_lng=manual_lng,
        )

        if "error" in best_coordinates:
            flash(
                "Warning: Unable to resolve map coordinates (check what3words, postcode, address, or latitude/longitude; for W3W set W3W_API_KEY or WHAT3WORDS_API_KEY on the server). The incident will still be created.",
                "warning",
            )

        # 1) Save the triage record
        try:
            new_id = ResponseTriage.create(
                created_by=current_user.username,
                vita_record_id=vita_record_id,
                first_name=first_name,
                middle_name=middle_name,
                last_name=last_name,
                patient_dob=patient_dob,
                phone_number=phone_number,
                address=address,
                postcode=postcode,
                what3words=what3words,
                entry_requirements=access_requirements,
                reason_for_call=reason_for_call,
                onset_datetime=onset_datetime_db,
                patient_alone=patient_alone,
                exclusion_data=exclusion_data,
                risk_flags=risk_flags,
                decision=decision,
                coordinates=best_coordinates
            )
        except Exception as e:
            logger.exception("triage_form create failed")
            flash(f"Unable to save triage record: {e}", "danger")
            return render_template("response/triage_form.html", **triage_template_ctx)

        # 2) Build payload for MDT / job record (cad assigned in step 4 — not response_triage.id)
        triage_data = {
            "vita_record_id": vita_record_id,
            "first_name": first_name,
            "middle_name": middle_name,
            "last_name": last_name,
            "patient_dob": patient_dob,
            "phone_number": phone_number,
            "address": address,
            "postcode": postcode,
            "what3words": what3words,
            "manual_latitude": manual_lat,
            "manual_longitude": manual_lng,
            "caller_name": caller_name,
            "caller_phone": caller_phone,
            "patient_gender": patient_gender,
            "additional_details": additional_details,
            "entry_requirements": access_requirements,
            "reason_for_call": reason_for_call,
            "onset_datetime": onset_datetime_db,
            "patient_alone": patient_alone,
            "exclusion_data": exclusion_data,
            "risk_flags": risk_flags,
            "decision": decision,
            "call_priority": call_priority,
            "call_priority_label": _priority_label_for_form(call_priority, selected_form),
            "priority_source": priority_source,
            "form_slug": selected_form.get("slug"),
            "form_name": selected_form.get("name"),
            "form_answers": form_answers,
            "division": requested_division,
            "coordinates": best_coordinates,
            "response_triage_id": new_id,
            "intake_source": "response_triage",
        }

        # 3) Legacy triage-time outbound dispatch not used here.

        # 4) **ENQUEUE** into internal MDT queue (CAD = next mdt_jobs slot, same as panic)
        conn = get_db_connection()
        cur = conn.cursor()
        job_cad = None
        try:
            cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'created_by'")
            has_created_by = cur.fetchone() is not None
            cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
            has_division = cur.fetchone() is not None
            job_cad = _mdt_next_allocated_job_cad(cur)
            triage_data["cad"] = job_cad
            triage_data["created_by"] = str(
                getattr(current_user, "username", None) or "").strip() or None
            data_json = json.dumps(triage_data, default=str)
            if has_created_by:
                if has_division:
                    cur.execute("""
                      INSERT INTO mdt_jobs (cad, created_by, status, data, division)
                      VALUES (%s, %s, 'queued', %s, %s)
                    """, (
                        job_cad,
                        current_user.username,
                        data_json,
                        requested_division
                    ))
                else:
                    cur.execute("""
                      INSERT INTO mdt_jobs (cad, created_by, status, data)
                      VALUES (%s, %s, 'queued', %s)
                    """, (
                        job_cad,
                        current_user.username,
                        data_json,
                    ))
            else:
                if has_division:
                    cur.execute("""
                      INSERT INTO mdt_jobs (cad, status, data, division)
                      VALUES (%s, 'queued', %s, %s)
                    """, (
                        job_cad,
                        data_json,
                        requested_division
                    ))
                else:
                    cur.execute("""
                      INSERT INTO mdt_jobs (cad, status, data)
                      VALUES (%s, 'queued', %s)
                    """, (
                        job_cad,
                        data_json,
                    ))
            try:
                if has_division:
                    _ensure_job_division_snapshot_columns(cur)
                    _sync_job_division_snapshot_for_slug(
                        cur, job_cad, requested_division)
            except Exception:
                logger.exception(
                    "division snapshot on triage create cad=%s", job_cad)
            conn.commit()
            try:
                socketio.emit(
                    'mdt_event', {'type': 'jobs_updated', 'cad': job_cad})
            except Exception:
                pass
            try:
                run_priority_preemption_for_job(int(job_cad))
            except Exception:
                logger.exception(
                    "priority preemption after triage create cad=%s", job_cad)
            log_audit(
                getattr(current_user, 'username', 'unknown'),
                'triage_create',
                details={
                    'cad': job_cad,
                    'response_triage_id': new_id,
                },
            )
        except Exception as e:
            conn.rollback()
            job_cad = None
            flash(f"Warning: MDT enqueue failed: {e}", "warning")
        finally:
            cur.close()
            conn.close()

        flash("Triage form submitted successfully!", "success")
        # Intake workflow: always hand off to the dedicated call-taker incident workspace.
        if job_cad is not None:
            if _can_access_call_centre():
                return redirect(url_for('.call_centre_job', cad=job_cad))
            if _user_has_role("dispatcher", "admin", "superuser", "clinical_lead", "controller"):
                return redirect(url_for('.cad_dashboard', panel='jobs', cad=job_cad))
        return redirect(url_for('.triage_list'))

    return render_template("response/triage_form.html", **triage_template_ctx)


@internal.route('/response/list')
@login_required
def triage_list():
    # Get all triage responses from the ResponseTriage class
    triage_list = ResponseTriage.get_all()
    return render_template("response/triage_list.html", triage_list=triage_list, config=core_manifest)


@internal.route('/call-centre', methods=['GET'])
@login_required
def call_centre_dashboard():
    """Dedicated call-taker CAD stack workspace."""
    if not _can_access_call_centre():
        return jsonify({"error": "Unauthorised access"}), 403
    return render_template("response/call_centre_dashboard.html", config=core_manifest)


@internal.route('/call-centre/wallboard', methods=['GET'])
@login_required
def call_centre_wallboard():
    """Large-screen TV wallboard for live CAD stack monitoring."""
    if not _can_access_call_centre():
        return jsonify({"error": "Unauthorised access"}), 403
    return render_template("response/call_centre_wallboard.html", config=core_manifest)


@internal.route('/call-centre/job/<int:cad>', methods=['GET'])
@login_required
def call_centre_job(cad):
    """Full-screen single-incident call-taker workspace."""
    if not _can_access_call_centre():
        return jsonify({"error": "Unauthorised access"}), 403
    return render_template("response/call_centre_job.html", config=core_manifest, cad=cad)


@internal.route('/call-centre/stack', methods=['GET'])
@login_required
def call_centre_stack():
    """Expanded CAD stack data optimized for call-centre and wallboard views."""
    if not _can_access_call_centre():
        return jsonify({"error": "Unauthorised access"}), 403
    selected_division, include_external = _request_division_scope()

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        selected_division, include_external, access = _enforce_dispatch_scope(
            cur, selected_division, include_external)
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_division = cur.fetchone() is not None
        division_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_division else "'general' AS division"
        sql = """
            SELECT cad,
                   TRIM(COALESCE(status, '')) AS status,
                   data,
                   created_at,
                   updated_at,
                   {division_sql}
            FROM mdt_jobs
            WHERE LOWER(TRIM(COALESCE(status, ''))) NOT IN ('cleared', 'stood_down')
        """.format(division_sql=division_sql)
        job_div_expr = "LOWER(TRIM(COALESCE(division, 'general')))"
        div_frag, args = _division_scope_where_sql(
            selected_division, include_external, access, has_division, job_div_expr
        )
        sql += div_frag
        sql += " ORDER BY cad DESC LIMIT 600"
        cur.execute(sql, tuple(args))
        rows = cur.fetchall() or []
        cads = [int(r.get('cad')) for r in rows if r.get('cad') is not None]
        assignments = {}
        if cads:
            try:
                _ensure_job_units_table(cur)
                placeholders = ",".join(["%s"] * len(cads))
                cur.execute(
                    f"SELECT job_cad, callsign FROM mdt_job_units WHERE job_cad IN ({placeholders}) ORDER BY assigned_at ASC",
                    cads
                )
                for row in (cur.fetchall() or []):
                    cad = int(row.get('job_cad'))
                    assignments.setdefault(cad, []).append(row.get('callsign'))
            except Exception:
                assignments = {}

        out = []
        for row in rows:
            payload = row.get('data')
            try:
                if isinstance(payload, (bytes, bytearray)):
                    payload = payload.decode('utf-8', errors='ignore')
                if isinstance(payload, str):
                    payload = json.loads(payload) if payload else {}
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}

            cad = int(row.get('cad'))
            lat = lng = None
            try:
                coords = payload.get('coordinates') or {}
                if isinstance(coords, dict):
                    lat = coords.get('lat')
                    lng = coords.get('lng')
                lat = float(lat) if lat is not None else None
                lng = float(lng) if lng is not None else None
            except Exception:
                lat = lng = None
            if lat is None or lng is None:
                rlat, rlng = _resolve_job_lat_lng_for_map(payload)
                if rlat is not None and rlng is not None:
                    lat, lng = rlat, rlng
            out.append({
                'cad': cad,
                'status': row.get('status'),
                'division': _normalize_division(row.get('division'), fallback='general'),
                'created_at': row.get('created_at'),
                'updated_at': row.get('updated_at'),
                'reason_for_call': payload.get('reason_for_call'),
                'priority': payload.get('call_priority') or payload.get('priority') or payload.get('acuity'),
                'address': payload.get('address'),
                'postcode': payload.get('postcode'),
                'what3words': payload.get('what3words'),
                'caller_name': payload.get('caller_name'),
                'caller_phone': payload.get('caller_phone'),
                'assigned_units': assignments.get(cad, []),
                'lat': lat,
                'lng': lng,
            })
        return jsonify(out)
    finally:
        cur.close()
        conn.close()

# -----------------------
# ADMIN ROUTES
# -----------------------


@internal.route('/admin', methods=['GET'])
@login_required
def admin_dashboard():
    allowed_roles = ["admin", "superuser", "clinical_lead"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed_roles:
        flash("Unauthorised access", "danger")
        return redirect(url_for('.landing'))

    broadnet_admin_unlocked = False
    try:
        _bconn = get_db_connection()
        _bcur = _bconn.cursor(dictionary=True)
        try:
            _bcur.execute("SHOW TABLES LIKE 'ventus_broadnet_settings'")
            if _bcur.fetchone():
                _bcur.execute(
                    "SELECT master_enabled FROM ventus_broadnet_settings WHERE id = 1 LIMIT 1"
                )
                _row = _bcur.fetchone() or {}
                try:
                    broadnet_admin_unlocked = bool(
                        int(_row.get("master_enabled") or 0))
                except (TypeError, ValueError):
                    broadnet_admin_unlocked = False
        finally:
            try:
                _bcur.close()
            except Exception:
                pass
            try:
                _bconn.close()
            except Exception:
                pass
    except Exception:
        broadnet_admin_unlocked = False

    return render_template(
        "admin/dashboard.html",
        config=core_manifest,
        broadnet_admin_unlocked=broadnet_admin_unlocked,
    )


def _admin_system_logs_shift_context(meta):
    """Plain-text line for search + table (shift / sign-on disambiguation)."""
    parts = []
    if meta.get('unit_sign_on'):
        parts.append(f"sign-on {meta['unit_sign_on']}")
    if meta.get('unit_shift_start') or meta.get('unit_shift_end'):
        parts.append(
            f"shift {meta.get('unit_shift_start') or '—'}–{meta.get('unit_shift_end') or '—'}"
        )
    if meta.get('unit_shift_duration_minutes') is not None:
        try:
            hm = int(meta['unit_shift_duration_minutes'])
            parts.append(f"duration {hm}m")
        except (TypeError, ValueError):
            pass
    if meta.get('job_opened_at'):
        parts.append(f"CAD opened {meta['job_opened_at']}")
    return ' · '.join(parts) if parts else ''


def _admin_system_logs_append_event(
    bucket, *, time_val, source, typ, actor, message, meta
):
    if time_val is None:
        return
    div = str(meta.get('division') or 'general').strip().lower() or 'general'
    cad_v = meta.get('cad')
    try:
        cad_int = int(cad_v) if cad_v is not None and str(cad_v).strip() != '' else None
    except (TypeError, ValueError):
        cad_int = None
    cs = str(meta.get('callsign') or '').strip() or None
    shift_ctx = _admin_system_logs_shift_context(meta)
    cad_hash = f'#{cad_int}' if cad_int is not None else ''
    search_bits = [
        div,
        str(cad_int) if cad_int is not None else '',
        cad_hash,
        f'cad{cad_int}' if cad_int is not None else '',
        cs or '',
        str(meta.get('job_status') or ''),
        str(meta.get('job_opened_at') or ''),
        str(meta.get('unit_sign_on') or ''),
        str(meta.get('unit_shift_start') or ''),
        str(meta.get('unit_shift_end') or ''),
        str(meta.get('unit_division_live') or ''),
        str(meta.get('response_log_id') or ''),
        shift_ctx,
        source,
        typ,
        actor,
        message,
    ]
    st = ' | '.join(str(x) for x in search_bits if str(x).strip())
    bucket.append({
        'time': time_val,
        'source': source,
        'type': typ,
        'actor': actor,
        'message': message,
        'meta': meta,
        'search_text': st,
    })


def _admin_system_logs_query_window():
    """Parse ``start``/``end`` (ISO) or legacy ``days`` (1–90). Returns (start_dt, end_dt, days_or_none)."""
    start_raw = (request.args.get('start') or request.args.get('from') or '').strip()
    end_raw = (request.args.get('end') or request.args.get('to') or '').strip()
    s = _coerce_datetime(start_raw) if start_raw else None
    e = _coerce_datetime(end_raw) if end_raw else None
    now = datetime.utcnow()
    if s is not None and e is not None:
        if e < s:
            s, e = e, s
        if (e - s) > timedelta(days=90):
            s = e - timedelta(days=90)
        if e > now:
            e = now
        if s > now:
            s = now - timedelta(minutes=1)
        return s, e, None
    try:
        days = int(request.args.get('days') or '14')
    except (TypeError, ValueError):
        days = 14
    days = max(1, min(days, 90))
    e = now
    return e - timedelta(days=days), e, days


@internal.route('/admin/system-logs', methods=['GET'])
@login_required
def admin_system_logs():
    """
    Admin overview: merged CAD + unit timeline (response log, assignments, comms, crew removals).
    Query: ``start``/``end`` (ISO, inclusive) **or** legacy ``days`` (1–90),
    ``scope`` = all|cad|unit (CAD vs unit stream), ``q`` = substring on indexed search text,
    optional ``callsign``, ``division`` (job/unit division slug, case-insensitive),
    ``by`` or ``actor`` (exact match, case-insensitive, on the **By** column).
    Structured filters combine with ``q`` using AND.
    """
    role = str(getattr(current_user, 'role', '') or '').strip().lower()
    if role not in {'admin', 'superuser', 'clinical_lead'}:
        return jsonify({'error': 'Unauthorised'}), 403

    start, end, days = _admin_system_logs_query_window()
    scope = str(request.args.get('scope') or 'all').strip().lower()
    if scope not in ('all', 'cad', 'unit'):
        scope = 'all'
    q_raw = str(request.args.get('q') or '').strip()
    q_lc = q_raw.lower()
    cs_f = str(request.args.get('callsign') or '').strip()
    div_f = str(request.args.get('division') or '').strip()
    by_f = str(request.args.get('by') or request.args.get('actor') or '').strip()
    cs_lc = cs_f.lower()
    div_lc = div_f.lower()
    by_lc = by_f.lower()

    events = []

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'mdt_response_log'")
        has_rl = cur.fetchone() is not None
        cur.execute("SHOW TABLES LIKE 'mdt_jobs'")
        has_jobs = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_job_div = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'created_at'")
        has_job_created = cur.fetchone() is not None
        job_div_sql = (
            "LOWER(TRIM(COALESCE(j.division, 'general')))"
            if has_job_div else "'general'"
        )
        job_created_sql = "j.created_at" if has_job_created else "NULL"

        if has_rl and has_jobs and scope in ('all', 'unit', 'cad'):
            _ensure_response_log_table(cur)
            _ensure_mdts_signed_on_schema(cur)
            cur.execute(f"""
                SELECT
                    l.id AS response_log_id,
                    l.callSign AS callsign,
                    l.cad AS cad,
                    l.status AS ladder_status,
                    l.event_time AS event_time,
                    {job_div_sql} AS job_division,
                    TRIM(COALESCE(j.status, '')) AS job_row_status,
                    {job_created_sql} AS job_created_at,
                    m.signOnTime AS unit_sign_on,
                    m.shiftStartAt AS unit_shift_start,
                    m.shiftEndAt AS unit_shift_end,
                    m.shiftDurationMins AS unit_shift_duration_mins,
                    LOWER(TRIM(COALESCE(m.division, ''))) AS unit_division_raw
                FROM mdt_response_log l
                INNER JOIN mdt_jobs j ON j.cad = l.cad
                LEFT JOIN mdts_signed_on m
                    ON UPPER(TRIM(m.callSign)) = UPPER(TRIM(l.callSign))
                    AND m.signOnTime IS NOT NULL
                    AND m.signOnTime <= l.event_time
                WHERE l.event_time >= %s AND l.event_time <= %s
                ORDER BY l.event_time DESC
                LIMIT 2000
            """, (start, end))
            for row in (cur.fetchall() or []):
                cs = str(row.get('callsign') or '').strip() or 'unknown'
                cad_int = int(row['cad']) if row.get('cad') is not None else None
                jd = str(row.get('job_division') or 'general').strip().lower() or 'general'
                ud = str(row.get('unit_division_raw') or '').strip().lower()
                div = jd or ud or 'general'
                unit_div = ud if ud else None
                meta = {
                    'division': div,
                    'cad': cad_int,
                    'callsign': cs,
                    'job_status': str(row.get('job_row_status') or '').strip() or None,
                    'job_opened_at': _job_system_log_created_ts_text(
                        row.get('job_created_at')),
                    'unit_sign_on': _job_system_log_created_ts_text(
                        row.get('unit_sign_on')),
                    'unit_shift_start': _job_system_log_created_ts_text(
                        row.get('unit_shift_start')),
                    'unit_shift_end': _job_system_log_created_ts_text(
                        row.get('unit_shift_end')),
                    'unit_shift_duration_minutes': row.get('unit_shift_duration_mins'),
                    'response_log_id': row.get('response_log_id'),
                    'unit_division_live': unit_div,
                }
                st_raw = str(row.get('ladder_status') or '').strip().lower()
                if st_raw == _RESPONSE_LOG_STATUS_CLOSURE_REVIEW:
                    msg = (
                        f"{cs} · CAD #{cad_int} entered closure review (job-level)"
                        if cad_int else f"{cs} · closure review (job-level)"
                    )
                    src = 'system'
                    typ = 'closure_review'
                elif st_raw == _RESPONSE_LOG_STATUS_DISPATCH_REOPENED:
                    extra = ""
                    try:
                        cr = row.get('crew')
                        if isinstance(cr, (bytes, bytearray)):
                            cr = cr.decode('utf-8', errors='ignore')
                        if isinstance(cr, str) and cr.strip().startswith('{'):
                            d = json.loads(cr)
                            if isinstance(d, dict) and str(d.get('reason') or '').strip():
                                extra = ' — ' + str(d.get('reason')).strip()
                    except Exception:
                        pass
                    msg = (
                        f"{cs} · CAD #{cad_int} reopened onto the stack by dispatch{extra}"
                        if cad_int
                        else f"{cs} · CAD reopened by dispatch{extra}"
                    )
                    src = 'dispatch'
                    typ = 'dispatch_reopened'
                elif st_raw == _RESPONSE_LOG_STATUS_DISPATCH_DIVISION_CHANGED:
                    fdiv, tdiv, by_disp = '', '', ''
                    try:
                        cr = row.get('crew')
                        if isinstance(cr, (bytes, bytearray)):
                            cr = cr.decode('utf-8', errors='ignore')
                        if isinstance(cr, str) and cr.strip().startswith('{'):
                            d = json.loads(cr)
                            if isinstance(d, dict):
                                fdiv = str(d.get('from_division') or '').strip()
                                tdiv = str(d.get('to_division') or '').strip()
                                by_disp = str(d.get('by') or '').strip()
                    except Exception:
                        pass
                    by_part = f' (by {by_disp})' if by_disp else ''
                    msg = f'{cs} division updated: {fdiv or "?"} → {tdiv or "?"}{by_part}'
                    src = 'dispatch'
                    typ = 'division_changed'
                elif st_raw == _RESPONSE_LOG_STATUS_DISPATCH_SHIFT_TIMES_CHANGED:
                    ns_txt, ne_txt, by_disp = '', '', ''
                    try:
                        cr = row.get('crew')
                        if isinstance(cr, (bytes, bytearray)):
                            cr = cr.decode('utf-8', errors='ignore')
                        if isinstance(cr, str) and cr.strip().startswith('{'):
                            d = json.loads(cr)
                            if isinstance(d, dict):
                                by_disp = str(d.get('by') or '').strip()
                                ns_txt = _job_system_log_created_ts_text(
                                    d.get('new_shift_start'))
                                ne_txt = _job_system_log_created_ts_text(
                                    d.get('new_shift_end'))
                    except Exception:
                        pass
                    by_part = f' (by {by_disp})' if by_disp else ''
                    span_msg = f'{ns_txt} – {ne_txt}' if ns_txt and ne_txt else 'updated'
                    msg = f'{cs} shift times {span_msg}{by_part}'
                    src = 'dispatch'
                    typ = 'shift_times_changed'
                else:
                    st = st_raw.replace('_', ' ')
                    label = ' '.join([x.capitalize()
                                     for x in st.split()]) if st else 'Update'
                    msg = (
                        f"{cs} status {label} · CAD #{cad_int}"
                        if cad_int else f"{cs} status {label}"
                    )
                    src = 'mdt'
                    typ = 'status_update'
                _admin_system_logs_append_event(
                    events,
                    time_val=row.get('event_time'),
                    source=src,
                    typ=typ,
                    actor=cs,
                    message=msg,
                    meta=meta,
                )

        if has_jobs and scope in ('all', 'cad'):
            _ensure_job_comms_table(cur)
            audit_types, audit_ph = _job_comms_audit_only_types_sql()
            not_in = ""
            params = [start, end]
            if audit_ph:
                not_in = f" AND LOWER(TRIM(COALESCE(c.message_type,''))) NOT IN ({audit_ph}) "
                params.extend([t.lower() for t in audit_types])
            cur.execute(f"""
                SELECT c.message_type, c.sender_role, c.sender_user, c.message_text,
                       c.created_at, c.cad,
                       {job_div_sql} AS job_division,
                       TRIM(COALESCE(j.status, '')) AS job_row_status,
                       {job_created_sql} AS job_created_at
                FROM mdt_job_comms c
                INNER JOIN mdt_jobs j ON j.cad = c.cad
                WHERE c.created_at >= %s AND c.created_at <= %s {not_in}
                ORDER BY c.created_at DESC
                LIMIT 800
            """, tuple(params))
            for row in (cur.fetchall() or []):
                cad_int = int(row['cad']) if row.get('cad') is not None else None
                div = str(row.get('job_division') or 'general').strip().lower() or 'general'
                actor = str(row.get('sender_user') or row.get(
                    'sender_role') or 'unknown').strip() or 'unknown'
                msg_type = str(row.get('message_type') or 'message').strip().lower()
                text = str(row.get('message_text') or '').strip()
                msg = _job_system_log_comms_display_message(msg_type, text)
                meta = {
                    'division': div,
                    'cad': cad_int,
                    'callsign': None,
                    'job_status': str(row.get('job_row_status') or '').strip() or None,
                    'job_opened_at': _job_system_log_created_ts_text(
                        row.get('job_created_at')),
                }
                _admin_system_logs_append_event(
                    events,
                    time_val=row.get('created_at'),
                    source='comms',
                    typ=msg_type,
                    actor=actor,
                    message=msg,
                    meta=meta,
                )

        if has_jobs and scope in ('all', 'cad', 'unit'):
            _ensure_job_units_table(cur)
            cur.execute(f"""
                SELECT u.job_cad, u.callsign, u.assigned_by, u.assigned_at,
                       {job_div_sql} AS job_division,
                       TRIM(COALESCE(j.status, '')) AS job_row_status,
                       {job_created_sql} AS job_created_at
                FROM mdt_job_units u
                INNER JOIN mdt_jobs j ON j.cad = u.job_cad
                WHERE u.assigned_at >= %s AND u.assigned_at <= %s
                ORDER BY u.assigned_at DESC
                LIMIT 600
            """, (start, end))
            for row in (cur.fetchall() or []):
                cs = str(row.get('callsign') or '').strip() or 'unknown'
                cad_int = int(row['job_cad']) if row.get('job_cad') is not None else None
                div = str(row.get('job_division') or 'general').strip().lower() or 'general'
                by = str(row.get('assigned_by') or '').strip() or 'system'
                msg = (
                    f"{cs} assigned to CAD #{cad_int}"
                    if cad_int else f"{cs} assigned to CAD"
                )
                meta = {
                    'division': div,
                    'cad': cad_int,
                    'callsign': cs,
                    'job_status': str(row.get('job_row_status') or '').strip() or None,
                    'job_opened_at': _job_system_log_created_ts_text(
                        row.get('job_created_at')),
                }
                _admin_system_logs_append_event(
                    events,
                    time_val=row.get('assigned_at'),
                    source='dispatch',
                    typ='unit_assigned',
                    actor=by,
                    message=msg,
                    meta=meta,
                )

        if has_jobs and scope in ('all', 'cad') and has_job_created:
            cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'data'")
            has_job_data = cur.fetchone() is not None
            data_sql = "j.data AS job_data" if has_job_data else "NULL AS job_data"
            cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'created_by'")
            has_job_created_by = cur.fetchone() is not None
            created_by_sql = (
                "TRIM(COALESCE(j.created_by, '')) AS job_created_by"
                if has_job_created_by
                else "CAST('' AS CHAR) AS job_created_by"
            )
            cur.execute(f"""
                SELECT j.cad, TRIM(COALESCE(j.status, '')) AS status,
                       {job_div_sql} AS job_division,
                       j.created_at AS created_at,
                       {created_by_sql},
                       {data_sql}
                FROM mdt_jobs j
                WHERE j.created_at >= %s AND j.created_at <= %s
                ORDER BY j.created_at DESC
                LIMIT 400
            """, (start, end))
            for row in (cur.fetchall() or []):
                cad_int = int(row['cad']) if row.get('cad') is not None else None
                div = str(row.get('job_division') or 'general').strip().lower() or 'general'
                creator = str(row.get('job_created_by') or '').strip() or 'system'
                pdata = {}
                try:
                    payload = row.get('job_data')
                    if payload:
                        if isinstance(payload, (bytes, bytearray)):
                            payload = payload.decode('utf-8', errors='ignore')
                        raw_p = json.loads(payload) if isinstance(
                            payload, str) and payload else {}
                        if isinstance(raw_p, dict):
                            pdata = raw_p
                except Exception:
                    pass
                if not creator or creator == 'system':
                    for _k in ('created_by', 'intake_username', 'submitted_by', 'intake_user'):
                        _v = str((pdata or {}).get(_k) or '').strip()
                        if _v:
                            creator = _v
                            break
                ts_txt = _job_system_log_created_ts_text(row.get('created_at'))
                msg = (
                    f"CAD #{cad_int} created by {creator} at {ts_txt}"
                    if creator and ts_txt
                    else (f"CAD #{cad_int} created by {creator}" if creator else f"CAD #{cad_int} created")
                )
                meta = {
                    'division': div,
                    'cad': cad_int,
                    'callsign': None,
                    'job_status': str(row.get('status') or '').strip() or None,
                    'job_opened_at': ts_txt,
                }
                intake_src = str((pdata or {}).get('intake_source') or '').strip().lower()
                pr_src = str((pdata or {}).get('priority_source') or '').strip().lower()
                if intake_src == 'response_triage':
                    src = 'intake'
                elif pr_src in ('mdt_panic', 'mdt_running_call'):
                    src = 'mdt'
                elif creator and creator != 'system':
                    src = 'dispatch'
                else:
                    src = 'system'
                _admin_system_logs_append_event(
                    events,
                    time_val=row.get('created_at'),
                    source=src,
                    typ='incident_created',
                    actor=creator,
                    message=msg,
                    meta=meta,
                )

        if scope in ('all', 'unit'):
            _ensure_crew_removal_log_table(cur)
            _ensure_mdts_signed_on_schema(cur)
            cur.execute("""
                SELECT r.callSign AS callsign, r.removed_username, r.removal_reason,
                       r.removed_by, r.removed_at,
                       LOWER(TRIM(COALESCE(m.division, 'general'))) AS unit_division
                FROM mdt_crew_removal_log r
                LEFT JOIN mdts_signed_on m
                    ON UPPER(TRIM(m.callSign)) = UPPER(TRIM(r.callSign))
                WHERE r.removed_at >= %s AND r.removed_at <= %s
                ORDER BY r.removed_at DESC
                LIMIT 500
            """, (start, end))
            for row in (cur.fetchall() or []):
                cs = str(row.get('callsign') or '').strip()
                un = str(row.get('removed_username') or '').strip() or 'unknown'
                rsn = str(row.get('removal_reason') or '').strip() or 'other'
                by = str(row.get('removed_by') or '').strip() or 'unknown'
                div = str(row.get('unit_division') or 'general').strip().lower() or 'general'
                msg = f"Crew removed: {un} ({rsn})"
                meta = {
                    'division': div,
                    'cad': None,
                    'callsign': cs.strip() or None,
                    'job_status': None,
                    'job_opened_at': None,
                }
                _admin_system_logs_append_event(
                    events,
                    time_val=row.get('removed_at'),
                    source='dispatch',
                    typ='crew_removal',
                    actor=by,
                    message=msg,
                    meta=meta,
                )

            cur.execute("SHOW TABLES LIKE 'mdt_session_lifecycle_event'")
            if cur.fetchone():
                cur.execute("""
                    SELECT created_at, event_type, callsign, previous_callsign, division,
                           actor_username, crew_usernames, detail_json
                    FROM mdt_session_lifecycle_event
                    WHERE created_at >= %s AND created_at <= %s
                    ORDER BY created_at DESC
                    LIMIT 800
                """, (start, end))
                for row in (cur.fetchall() or []):
                    ev_raw = str(row.get('event_type') or '').strip().lower()
                    typ_ev = {
                        'sign_on': 'mdt_session_sign_on',
                        'sign_off': 'mdt_session_sign_off',
                        'force_sign_off': 'mdt_session_force_sign_off',
                        'session_expired_sign_off': (
                            'mdt_session_expired_sign_off'),
                    }.get(ev_raw, 'mdt_session_sign_on')
                    cs = str(row.get('callsign') or '').strip() or 'unknown'
                    prev = str(row.get('previous_callsign') or '').strip()
                    div = str(row.get('division') or 'general').strip().lower() or 'general'
                    actor = str(row.get('actor_username') or '').strip() or '—'
                    crew_list = []
                    try:
                        cj = row.get('crew_usernames')
                        if isinstance(cj, (bytes, bytearray)):
                            cj = cj.decode('utf-8', errors='ignore')
                        if isinstance(cj, str) and cj.strip().startswith('['):
                            crew_list = json.loads(cj)
                        elif isinstance(cj, list):
                            crew_list = cj
                    except Exception:
                        crew_list = []
                    crew_txt = ', '.join(
                        str(x).strip() for x in (crew_list or []) if str(x).strip())
                    if ev_raw == 'sign_on':
                        if prev and prev.upper() != cs.upper():
                            msg = f"Crew signed on to {cs} (moved from {prev})"
                        else:
                            msg = f"Crew signed on to {cs}"
                    elif ev_raw == 'sign_off':
                        msg = f"Signed off {cs}"
                    elif ev_raw == 'force_sign_off':
                        msg = f"Force signed off {cs}"
                    else:
                        msg = f"Signed off {cs} (session expired)"
                    if crew_txt:
                        msg += f" — crew: {crew_txt}"
                    meta = {
                        'division': div,
                        'cad': None,
                        'callsign': cs,
                        'job_status': None,
                        'job_opened_at': None,
                    }
                    _admin_system_logs_append_event(
                        events,
                        time_val=row.get('created_at'),
                        source='mdt',
                        typ=typ_ev,
                        actor=actor,
                        message=msg,
                        meta=meta,
                    )

    except Exception:
        logger.exception('admin_system_logs failed')
        return jsonify({'error': 'Unable to load system logs'}), 500
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    if scope == 'unit':
        _unit_types = (
            'status_update', 'closure_review', 'dispatch_reopened', 'unit_assigned',
            'crew_removal', 'division_changed', 'shift_times_changed',
            'mdt_session_sign_on', 'mdt_session_sign_off',
            'mdt_session_force_sign_off', 'mdt_session_expired_sign_off',
        )
        events = [
            e for e in events
            if e.get('meta', {}).get('callsign')
            or (e.get('type') or '') in _unit_types
        ]

    events.sort(key=lambda x: x.get('time') or datetime.min)
    if q_lc:
        tokens = [t for t in q_lc.split() if t]
        if tokens:
            def _admin_log_matches_tokens(ev):
                st = str(ev.get('search_text') or '').lower()
                return all(tok in st for tok in tokens)

            events = [e for e in events if _admin_log_matches_tokens(e)]

    if cs_lc or div_lc or by_lc:
        def _admin_log_passes_structured_filters(ev):
            if cs_lc:
                meta = ev.get('meta') or {}
                m_cs = str(meta.get('callsign') or '').strip().lower()
                act = str(ev.get('actor') or '').strip().lower()
                msg_l = str(ev.get('message') or '').lower()
                if m_cs:
                    if cs_lc not in m_cs:
                        return False
                elif cs_lc not in msg_l and act != cs_lc:
                    return False
            if div_lc:
                d = str((ev.get('meta') or {}).get('division') or '').strip().lower()
                if d != div_lc:
                    return False
            if by_lc:
                act = str(ev.get('actor') or '').strip().lower()
                if act != by_lc:
                    return False
            return True

        events = [e for e in events if _admin_log_passes_structured_filters(e)]

    events = events[-2500:]

    out_rows = []
    for e in events:
        row = dict(e)
        tv = row.get('time')
        if hasattr(tv, 'isoformat'):
            row['time'] = tv.isoformat(sep=' ', timespec='seconds')
        elif tv is not None:
            row['time'] = str(tv)
        out_rows.append(row)

    return _jsonify_safe({
        'days': days,
        'start': start.isoformat(sep=' ', timespec='seconds'),
        'end': end.isoformat(sep=' ', timespec='seconds'),
        'scope': scope,
        'q': q_raw,
        'callsign': cs_f,
        'division': div_f,
        'by': by_f,
        'count': len(out_rows),
        'events': list(reversed(out_rows)),
    })


@internal.route('/admin/vendor-dispatch-bridge/api', methods=['GET', 'POST'])
@login_required
def admin_vendor_dispatch_bridge_api():
    role = str(getattr(current_user, 'role', '') or '').lower()
    if role not in {'admin', 'superuser'}:
        abort(404)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        from . import broadnet_bridge as bnb
        bnb.ensure_broadnet_tables(cur)
        conn.commit()
        if not bnb.is_master_unlocked(cur):
            abort(404)
        if request.method == 'GET':
            return _jsonify_safe(bnb.admin_get_settings_masked(cur), 200)
        data = request.get_json(silent=True) or {}
        bnb.admin_save_settings(
            cur,
            username=str(getattr(current_user, 'username', '') or ''),
            outbound_enabled=bool(data.get('outbound_enabled')),
            org_endpoint_key=bnb.sanitize_org_endpoint_key(
                str(data.get('org_endpoint_key') or '')),
            api_key=str(data.get('api_key') or ''),
            default_team_uuid=str(data.get('default_team_uuid') or '').strip(),
            default_channel_uuid=str(
                data.get('default_channel_uuid') or '').strip(),
            grade_default=int(data.get('grade_default') or 2),
            callsign_channel_map_json=str(
                data.get('callsign_channel_map_json') or '{}'),
            callsign_terminal_map_json=str(
                data.get('callsign_terminal_map_json') or '{}'),
        )
        conn.commit()
        return _jsonify_safe({'message': 'Settings saved.'}, 200)
    except ValueError as ve:
        conn.rollback()
        return jsonify({'error': str(ve)}), 400
    except Exception:
        logger.exception("admin_vendor_dispatch_bridge_api")
        conn.rollback()
        return jsonify({'error': 'Save failed'}), 500
    finally:
        cur.close()
        conn.close()


def _admin_standby_presets_allowed():
    if not hasattr(current_user, 'role') or current_user.role.lower() not in {"admin", "superuser", "clinical_lead"}:
        return False
    return True


@internal.route('/admin/standby-presets', methods=['GET'])
@login_required
def admin_standby_presets_page():
    if not _admin_standby_presets_allowed():
        flash("Unauthorised access", "danger")
        return redirect(url_for('.landing'))
    return redirect(url_for('.admin_dashboard') + '#standby')


@internal.route('/admin/standby-presets/api', methods=['GET'])
@login_required
def admin_standby_presets_api_list():
    if not _admin_standby_presets_allowed():
        return jsonify({'error': 'Unauthorised'}), 403
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_standby_tables(cur)
        _upgrade_mdt_standby_presets_division(cur)
        conn.commit()
        cur.execute("""
            SELECT p.id, p.name, p.lat, p.lng, p.what3words, p.is_active, p.created_by, p.created_at,
                   p.division,
                   COALESCE(d.name, p.division) AS division_display_name
            FROM mdt_standby_presets p
            LEFT JOIN mdt_dispatch_divisions d
              ON LOWER(TRIM(d.slug)) = LOWER(TRIM(p.division))
            ORDER BY p.is_active DESC, p.name ASC
        """)
        rows = cur.fetchall() or []
        for r in rows:
            if r.get('division') is not None:
                r['division'] = _normalize_division(
                    r.get('division'), fallback='general')
            if r.get('lat') is not None:
                try:
                    r['lat'] = float(r['lat'])
                except (TypeError, ValueError):
                    pass
            if r.get('lng') is not None:
                try:
                    r['lng'] = float(r['lng'])
                except (TypeError, ValueError):
                    pass
        return _jsonify_safe({'items': rows}, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/admin/standby-presets/api/resolve-w3w', methods=['POST'])
@login_required
def admin_standby_presets_api_resolve_w3w():
    """Resolve what3words to coordinates. Call from admin UI on blur / explicit lookup only — not per keystroke."""
    if not _admin_standby_presets_allowed():
        return jsonify({'error': 'Unauthorised'}), 403
    payload = request.get_json(silent=True) or {}
    raw = str(payload.get('words') or payload.get('what3words') or '').strip()
    phrase = ResponseTriage.normalize_w3w_words(raw)
    if not phrase:
        return jsonify({'error': 'Enter a what3words address'}), 400
    if not ResponseTriage.w3w_phrase_has_three_words(phrase):
        return jsonify({
            'error': 'what3words must be exactly three words (e.g. filled.count.soap)'
        }), 400
    result = ResponseTriage.get_lat_lng_from_w3w(phrase)
    if 'lat' not in result:
        return jsonify({'error': result.get('error', 'Lookup failed')}), 400
    return _jsonify_safe({
        'lat': float(result['lat']),
        'lng': float(result['lng']),
        'normalized': phrase,
    }, 200)


@internal.route('/admin/standby-presets/api', methods=['POST'])
@login_required
def admin_standby_presets_api_create():
    if not _admin_standby_presets_allowed():
        return jsonify({'error': 'Unauthorised'}), 403
    payload = request.get_json(silent=True) or {}
    name = str(payload.get('name') or '').strip()
    if not name or len(name) > 180:
        return jsonify({'error': 'Name is required (max 180 characters)'}), 400
    exclusive_w3w_map_err = (
        'You can only use Pick on Map or what3words — not both. '
        'Clear what3words to save map coordinates only, or resolve what3words so coordinates match the three-word address.'
    )
    what3words = str(payload.get('what3words')
                     or payload.get('w3w') or '').strip() or None
    lat = lng = None
    try:
        lat = float(payload.get('lat', payload.get('latitude')))
        lng = float(payload.get('lng', payload.get('longitude')))
    except (TypeError, ValueError):
        lat = lng = None

    if what3words:
        w3r = ResponseTriage.get_lat_lng_from_w3w(what3words)
        if 'lat' not in w3r:
            return jsonify({'error': w3r.get('error', 'Could not resolve what3words')}), 400
        rw_lat = float(w3r['lat'])
        rw_lng = float(w3r['lng'])
        phrase_norm = ResponseTriage.normalize_w3w_words(
            what3words) or what3words
        if lat is not None and lng is not None:
            if abs(lat - rw_lat) > 2e-4 or abs(lng - rw_lng) > 2e-4:
                return jsonify({'error': exclusive_w3w_map_err}), 400
        lat, lng = rw_lat, rw_lng
        what3words = phrase_norm
    else:
        if lat is None or lng is None:
            return jsonify({
                'error': 'Valid lat and lng are required (Pick on Map), or provide what3words to resolve.'
            }), 400

    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return jsonify({'error': 'lat/lng out of range'}), 400
    div_slug = _normalize_division(
        payload.get('division') or payload.get('dispatch_division'),
        fallback='general',
    )
    operator = str(getattr(current_user, 'username', '') or '').strip() or None
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_standby_tables(cur)
        _upgrade_mdt_standby_presets_division(cur)
        conn.commit()
        if not _dispatch_division_slug_exists(cur, div_slug):
            return jsonify({'error': 'Unknown dispatch division slug'}), 400
        cur.execute(
            "SELECT id FROM mdt_standby_presets WHERE name = %s LIMIT 1", (name,))
        if cur.fetchone():
            return jsonify({'error': 'A preset with this name already exists'}), 409
        cur.execute("""
            INSERT INTO mdt_standby_presets (name, lat, lng, what3words, is_active, created_by, division)
            VALUES (%s, %s, %s, %s, 1, %s, %s)
        """, (name, lat, lng, what3words, operator, div_slug))
        new_id = cur.lastrowid
        conn.commit()
        return _jsonify_safe({
            'message': 'Preset created',
            'id': new_id,
            'name': name,
            'lat': lat,
            'lng': lng,
            'what3words': what3words,
            'division': div_slug,
        }, 201)
    finally:
        cur.close()
        conn.close()


@internal.route('/admin/standby-presets/api/<int:preset_id>', methods=['PATCH'])
@login_required
def admin_standby_presets_api_patch(preset_id):
    """Set is_active and/or division (inactive presets stay in the list but are hidden from CAD)."""
    if not _admin_standby_presets_allowed():
        return jsonify({'error': 'Unauthorised'}), 403
    payload = request.get_json(silent=True) or {}
    if 'is_active' not in payload and 'division' not in payload:
        return jsonify({'error': 'is_active and/or division is required'}), 400
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_standby_tables(cur)
        _upgrade_mdt_standby_presets_division(cur)
        conn.commit()
        cur.execute(
            "SELECT id FROM mdt_standby_presets WHERE id = %s LIMIT 1", (preset_id,))
        if not cur.fetchone():
            return jsonify({'error': 'Preset not found'}), 404
        sets = []
        vals = []
        div_slug_out = None
        if 'is_active' in payload:
            raw = payload.get('is_active')
            if isinstance(raw, str):
                is_active = raw.strip().lower() in ('1', 'true', 'yes', 'on')
            else:
                is_active = bool(raw)
            ia = 1 if is_active else 0
            sets.append('is_active = %s')
            vals.append(ia)
        if 'division' in payload:
            div_slug_out = _normalize_division(
                payload.get('division'), fallback='general')
            if not _dispatch_division_slug_exists(cur, div_slug_out):
                return jsonify({'error': 'Unknown dispatch division slug'}), 400
            sets.append('division = %s')
            vals.append(div_slug_out)
        vals.append(preset_id)
        cur.execute(
            f"UPDATE mdt_standby_presets SET {', '.join(sets)} WHERE id = %s",
            tuple(vals),
        )
        conn.commit()
        out = {'message': 'Updated', 'id': preset_id}
        if 'is_active' in payload:
            raw_ia = payload.get('is_active')
            if isinstance(raw_ia, str):
                out['is_active'] = raw_ia.strip().lower() in ('1', 'true', 'yes', 'on')
            else:
                out['is_active'] = bool(raw_ia)
        if div_slug_out is not None:
            out['division'] = div_slug_out
        return _jsonify_safe(out, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/admin/standby-presets/api/<int:preset_id>', methods=['DELETE'])
@login_required
def admin_standby_presets_api_delete(preset_id):
    if not _admin_standby_presets_allowed():
        return jsonify({'error': 'Unauthorised'}), 403
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        _ensure_standby_tables(cur)
        conn.commit()
        cur.execute(
            "DELETE FROM mdt_standby_presets WHERE id = %s", (preset_id,))
        deleted = cur.rowcount or 0
        conn.commit()
        if not deleted:
            return jsonify({'error': 'Preset not found'}), 404
        return _jsonify_safe({'message': 'Preset deleted', 'id': preset_id}, 200)
    finally:
        cur.close()
        conn.close()


# =============================================================================
# CLINICAL ROUTES (retired)
# =============================================================================
@internal.route('/clinical', methods=['GET', 'POST'])
@login_required
def clinical_dashboard():
    return redirect(url_for('.landing'))


# Add CORS headers to all responses (must include custom MDT headers or browsers block preflight).
@internal.after_request
def add_cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = (
        'Content-Type,Authorization,Accept,X-Requested-With,'
        'X-MDT-Session-Id,X-CSRFToken,X-CSRF-Token,Idempotency-Key'
    )
    response.headers['Access-Control-Allow-Methods'] = (
        'GET,HEAD,POST,PUT,PATCH,DELETE,OPTIONS'
    )
    return response

# =============================================================================
# MDT ROUTES
# =============================================================================

# Helper to normalize callsign


def _normalize_callsign(payload=None, args=None):
    """
    Accept either 'callSign' or 'callsign' from JSON body or query params.
    """
    cs = None
    if payload:
        cs = payload.get('callSign') or payload.get('callsign')
    if not cs and args:
        cs = args.get('callSign') or args.get('callsign')
    return str(cs or '').strip().upper()


def _ensure_callsign_redirect_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_callsign_redirect (
            from_callsign VARCHAR(64) NOT NULL PRIMARY KEY,
            to_callsign VARCHAR(64) NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_callsign_redirect_to (to_callsign)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def _mdt_resolve_callsign(cur, raw_cs):
    """Map a callSign the MDT still sends to the current mdts_signed_on row after CAD rename."""
    cs = str(raw_cs or '').strip().upper()
    if not cs:
        return cs
    try:
        _ensure_callsign_redirect_table(cur)
        cur.execute(
            "SELECT callSign FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
            (cs,),
        )
        if cur.fetchone():
            return cs
        cur.execute(
            "SELECT to_callsign FROM mdt_callsign_redirect WHERE from_callsign = %s LIMIT 1",
            (cs,),
        )
        row = cur.fetchone()
        if not row:
            return cs
        if isinstance(row, dict):
            to_c = row.get('to_callsign')
        else:
            to_c = row[0] if row else None
        to_c = str(to_c or '').strip().upper()
        if not to_c:
            return cs
        cur.execute(
            "SELECT callSign FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
            (to_c,),
        )
        if cur.fetchone():
            return to_c
    except Exception:
        pass
    return cs


def _mdt_callsign_client_hints(requested_cs, canonical_cs):
    """
    Extra JSON fields for MDT/mobile clients when dispatch renamed the unit.
    Clients should persist callSign/canonicalCallSign when callsignRemappedByServer is true.
    """
    req = str(requested_cs or '').strip().upper()
    can = str(canonical_cs or '').strip().upper()
    if not can or req == can:
        return {}
    return {
        'requestedCallSign': req,
        'canonicalCallSign': can,
        'callSign': can,
        'callsign': can,
        'callsignRemappedByServer': True,
    }


def _ensure_mdt_web_push_subscriptions_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_web_push_subscriptions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            callsign VARCHAR(64) NOT NULL,
            endpoint VARCHAR(768) NOT NULL,
            p256dh VARCHAR(255) NOT NULL,
            auth_secret VARCHAR(255) NOT NULL,
            user_agent VARCHAR(512) NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_mdt_push_endpoint (endpoint(512)),
            INDEX idx_mdt_push_callsign (callsign)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def _mdt_vapid_config():
    pub = (os.environ.get("VENTUS_MDT_VAPID_PUBLIC_KEY") or "").strip()
    priv = (os.environ.get("VENTUS_MDT_VAPID_PRIVATE_KEY") or "").strip()
    # Allow one-line PEM in systemd/Docker (literal \n in the env value)
    if priv and "\\n" in priv and "BEGIN" in priv:
        priv = priv.replace("\\n", "\n")
    subj = (os.environ.get("VENTUS_MDT_VAPID_SUBJECT")
            or "mailto:mdt@localhost").strip()
    if not subj.startswith("mailto:") and not subj.startswith("https:"):
        subj = "mailto:mdt@localhost"
    return pub, priv, subj


def _mdt_web_push_notify_callsign(
    to_callsign,
    title,
    body,
    tag=None,
    url="/dashboard",
    *,
    alert=False,
    silent=False,
    require_interaction=None,
    cad=None,
):
    """Notify registered PWA devices for a unit (non-blocking best-effort).

    ``alert=True``: new-job style — louder vibration, stays on screen; SW focuses app on tap.
    ``silent=True``: no/minimal system sound (messages should use in-app only; routine pushes).
    """
    cs = str(to_callsign or "").strip().upper()
    if not cs or cs == "DISPATCHER":
        return
    try:
        import threading

        _, vapid_private, vapid_subj = _mdt_vapid_config()
        if not vapid_private:
            return
        try:
            from pywebpush import webpush, WebPushException
        except ImportError:
            return

        payload_obj = {
            "title": (title or "Ventus MDT")[:120],
            "body": (body or "")[:500],
            "url": url or "/dashboard",
            "tag": (tag or "ventus-mdt")[:120],
            "alert": bool(alert),
            "silent": bool(silent),
        }
        if require_interaction is not None:
            payload_obj["requireInteraction"] = bool(require_interaction)
        else:
            payload_obj["requireInteraction"] = bool(alert)
        if cad is not None:
            try:
                payload_obj["cad"] = int(cad)
            except (TypeError, ValueError):
                pass
        payload = json.dumps(payload_obj)

        def _run():
            conn = get_db_connection()
            cur = conn.cursor(dictionary=True)
            try:
                _ensure_mdt_web_push_subscriptions_table(cur)
                cur.execute(
                    "SELECT endpoint, p256dh, auth_secret FROM mdt_web_push_subscriptions WHERE callsign = %s",
                    (cs,),
                )
                rows = cur.fetchall() or []
            finally:
                cur.close()
                conn.close()
            for row in rows:
                ep = str(row.get("endpoint") or "").strip()
                p256 = str(row.get("p256dh") or "").strip()
                auth = str(row.get("auth_secret") or "").strip()
                if not ep or not p256 or not auth:
                    continue
                info = {"endpoint": ep, "keys": {"p256dh": p256, "auth": auth}}
                try:
                    webpush(
                        subscription_info=info,
                        data=payload,
                        vapid_private_key=vapid_private,
                        vapid_claims={"sub": vapid_subj},
                        ttl=86400,
                    )
                except WebPushException as ex:
                    st = getattr(ex, "response", None)
                    code = getattr(st, "status_code",
                                   None) if st is not None else None
                    if code in (404, 410):
                        try:
                            c2 = get_db_connection()
                            k = c2.cursor()
                            k.execute(
                                "DELETE FROM mdt_web_push_subscriptions WHERE endpoint = %s LIMIT 1",
                                (ep[:768],),
                            )
                            c2.commit()
                            k.close()
                            c2.close()
                        except Exception:
                            pass
                except Exception:
                    pass

        threading.Thread(target=_run, daemon=True).start()
    except Exception:
        pass


def _get_dispatch_mode(cur):
    """Return ``'auto'`` or ``'manual'`` from ``mdt_dispatch_settings`` (default manual)."""
    try:
        _ensure_dispatch_settings_table(cur)
        cur.execute(
            "SELECT LOWER(TRIM(mode)) AS m FROM mdt_dispatch_settings WHERE id = 1 LIMIT 1"
        )
        row = cur.fetchone()
        if row:
            m = (row.get("m") if isinstance(row, dict) else row[0]) or ""
            m = str(m).strip().lower()
            if m == "auto":
                return "auto"
    except Exception:
        pass
    return "manual"


def _get_dispatch_motd(cur):
    """Return active dispatch message of the day metadata."""
    out = {
        'text': '',
        'updated_by': None,
        'updated_at': None
    }
    try:
        _ensure_dispatch_settings_table(cur)
        cur.execute("""
            SELECT motd_text, motd_updated_by, motd_updated_at
            FROM mdt_dispatch_settings
            WHERE id = 1
            LIMIT 1
        """)
        row = cur.fetchone()
        if not row:
            return out
        if isinstance(row, dict):
            out['text'] = row.get('motd_text') or ''
            out['updated_by'] = row.get('motd_updated_by')
            out['updated_at'] = row.get('motd_updated_at')
        else:
            out['text'] = row[0] or ''
            out['updated_by'] = row[1] if len(row) > 1 else None
            out['updated_at'] = row[2] if len(row) > 2 else None
    except Exception:
        pass
    return out


def _ensure_job_units_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_job_units (
            id INT AUTO_INCREMENT PRIMARY KEY,
            job_cad INT NOT NULL,
            callsign VARCHAR(64) NOT NULL,
            assigned_by VARCHAR(120),
            assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_job_callsign (job_cad, callsign),
            INDEX idx_job_cad (job_cad),
            INDEX idx_callsign (callsign)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    _ensure_response_log_table(cur)


def _ensure_response_log_table(cur):
    global _schema_bootstrap_state
    if _schema_bootstrap_state.get("response_log"):
        return

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_response_log (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            callSign VARCHAR(64) NOT NULL,
            cad INT NOT NULL,
            status VARCHAR(32) NOT NULL,
            event_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            crew JSON,
            INDEX idx_response_log_cad_time (cad, event_time),
            INDEX idx_response_log_callsign_time (callSign, event_time),
            INDEX idx_response_log_status_time (status, event_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    # Backward compatibility for partial schemas.
    try:
        cur.execute("ALTER TABLE mdt_response_log ADD COLUMN crew JSON")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdt_response_log ADD COLUMN event_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")
    except Exception:
        pass
    try:
        cur.execute(
            "CREATE INDEX idx_response_log_cad_time ON mdt_response_log (cad, event_time)")
    except Exception:
        pass
    try:
        cur.execute(
            "CREATE INDEX idx_response_log_callsign_time ON mdt_response_log (callSign, event_time)")
    except Exception:
        pass
    try:
        cur.execute(
            "CREATE INDEX idx_response_log_status_time ON mdt_response_log (status, event_time)")
    except Exception:
        pass
    _schema_bootstrap_state["response_log"] = True


def _insert_unit_dispatch_adjustment_response_log(cur, callsign, status_key, meta_dict):
    """Synthetic ``mdt_response_log`` row for dispatch-driven unit corrections (unit + admin system logs)."""
    _ensure_response_log_table(cur)
    crew_blob = None
    if meta_dict is not None:
        try:
            crew_blob = json.dumps(meta_dict, default=str)
        except Exception:
            crew_blob = json.dumps({'note': str(meta_dict)[:800]})
    cur.execute(
        """
        INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew)
        VALUES (%s, 0, %s, NOW(), %s)
        """,
        (callsign, status_key, crew_blob),
    )


def _ensure_crew_removal_log_table(cur):
    global _schema_bootstrap_state
    if _schema_bootstrap_state.get("crew_removal_log"):
        return
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_crew_removal_log (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            callSign VARCHAR(64) NOT NULL,
            removed_username VARCHAR(120) NOT NULL,
            removal_reason VARCHAR(64) NOT NULL,
            removed_by VARCHAR(64) NOT NULL,
            removed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_crew_remove_callsign_time (callSign, removed_at),
            INDEX idx_crew_remove_user_time (removed_username, removed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    try:
        cur.execute(
            "ALTER TABLE mdt_crew_removal_log ADD COLUMN removed_by VARCHAR(64) NOT NULL DEFAULT ''")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdt_crew_removal_log ADD COLUMN removed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")
    except Exception:
        pass
    try:
        cur.execute(
            "ALTER TABLE mdt_crew_removal_log ADD COLUMN notes VARCHAR(512) NULL DEFAULT NULL")
    except Exception:
        pass
    try:
        cur.execute(
            "CREATE INDEX idx_crew_remove_callsign_time ON mdt_crew_removal_log (callSign, removed_at)")
    except Exception:
        pass
    try:
        cur.execute(
            "CREATE INDEX idx_crew_remove_user_time ON mdt_crew_removal_log (removed_username, removed_at)")
    except Exception:
        pass
    _schema_bootstrap_state["crew_removal_log"] = True


def _ensure_mdt_session_lifecycle_event_table(cur):
    """Append-only MDT sign-on / sign-off rows for admin system logs and audit-style timelines."""
    global _schema_bootstrap_state
    if _schema_bootstrap_state.get("session_lifecycle_event"):
        return
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_session_lifecycle_event (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_type VARCHAR(24) NOT NULL,
            callsign VARCHAR(64) NOT NULL,
            previous_callsign VARCHAR(64) NULL,
            division VARCHAR(64) NULL,
            actor_username VARCHAR(120) NULL,
            crew_usernames JSON NULL,
            detail_json JSON NULL,
            INDEX idx_sess_life_time (created_at),
            INDEX idx_sess_life_cs_time (callsign, created_at),
            INDEX idx_sess_life_type_time (event_type, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    _schema_bootstrap_state["session_lifecycle_event"] = True


def _record_mdt_session_lifecycle_event(
        cur,
        *,
        event_type,
        callsign,
        division=None,
        actor_username=None,
        previous_callsign=None,
        crew_usernames=None,
        detail=None):
    """Best-effort row for admin System Logs (never raises to callers)."""
    try:
        _ensure_mdt_session_lifecycle_event_table(cur)
        cs = str(callsign or '').strip().upper()
        if not cs:
            return
        et = str(event_type or '').strip().lower()
        if et not in ('sign_on', 'sign_off', 'force_sign_off', 'session_expired_sign_off'):
            return
        div = str(division or 'general').strip().lower() or 'general'
        prev = str(previous_callsign or '').strip().upper() or None
        act = str(actor_username or '').strip() or None
        crew_j = None
        if crew_usernames is not None:
            try:
                crew_j = json.dumps(
                    [str(x).strip() for x in crew_usernames if str(x).strip()][:80],
                    default=str,
                )
            except Exception:
                crew_j = None
        det_j = None
        if detail is not None:
            try:
                det_j = json.dumps(detail, default=str)[:4000]
            except Exception:
                det_j = json.dumps({'note': str(detail)[:800]})
        cur.execute(
            """
            INSERT INTO mdt_session_lifecycle_event
            (event_type, callsign, previous_callsign, division, actor_username, crew_usernames, detail_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (et, cs, prev, div, act, crew_j, det_j),
        )
    except Exception as ex:
        try:
            logger.warning('mdt_session_lifecycle_event insert failed: %s', ex)
        except Exception:
            pass


def _normalize_mdt_crew_removal_reason_from_mdt(removal_reason):
    """
    Same validation as POST /api/mdt/<callsign>/crew when removing a member (CrewManager presets).
    Returns a display string stored in mdt_crew_removal_log.removal_reason (e.g. 'Illness', 'Other').
    """
    allowed_reasons = {'illness', 'early finish', 'reassigned', 'personal', 'other'}
    normalized_reason = str(removal_reason or '').strip()
    if normalized_reason.lower() not in allowed_reasons:
        normalized_reason = 'Other'
    return normalized_reason


def _ensure_crew_grades_table(cur):
    """Crew grades/roles (e.g. paramedic, AAP, ECA) for dispatch allocation."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_crew_grades (
            value VARCHAR(64) NOT NULL PRIMARY KEY,
            label VARCHAR(120) NOT NULL,
            sort_order INT NOT NULL DEFAULT 0,
            is_active TINYINT NOT NULL DEFAULT 1,
            INDEX idx_crew_grades_active_order (is_active, sort_order)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    cur.execute("SELECT 1 FROM mdt_crew_grades LIMIT 1")
    if cur.fetchone() is None:
        defaults = [
            ('paramedic', 'Paramedic', 10),
            ('aap', 'AAP (Associate Ambulance Practitioner)', 20),
            ('eca', 'Emergency Care Assistant', 30),
            ('tec', 'Emergency Medical Technician', 40),
            ('student', 'Student', 50),
            ('other', 'Other', 99),
        ]
        for val, label, order in defaults:
            try:
                cur.execute(
                    "INSERT IGNORE INTO mdt_crew_grades (value, label, sort_order) VALUES (%s, %s, %s)",
                    (val, label, order)
                )
            except Exception:
                pass


def _list_crew_grades(cur):
    _ensure_crew_grades_table(cur)
    cur.execute(
        "SELECT value, label FROM mdt_crew_grades WHERE is_active = 1 ORDER BY sort_order ASC, label ASC"
    )
    rows = cur.fetchall() or []
    return [{"value": r.get("value", ""), "label": r.get("label", r.get("value", ""))} for r in rows]


def _suggest_crew_grade_value_from_hr_text(cur, hr_text):
    """
    Best-effort map of HR / contractor job title text to ``mdt_crew_grades.value``
    (used for crew JSON ``grade`` and dispatch skill tokens).
    """
    raw = str(hr_text or "").strip()
    if not raw:
        return None
    blob = raw.lower()
    rows = _list_crew_grades(cur)
    best_val = None
    best_score = 0
    for r in rows:
        val = str(r.get("value") or "").strip()
        lab = str(r.get("label") or "").strip()
        if not val:
            continue
        vl = val.lower()
        ll = lab.lower() if lab else ""
        score = 0
        if vl and vl in blob:
            score = max(score, len(vl) + 1)
        if ll:
            if ll in blob:
                score = max(score, min(len(ll), len(blob)))
            core = ll.split("(")[0].strip()
            if core and core in blob:
                score = max(score, len(core))
        if score > best_score:
            best_score = score
            best_val = val
    if best_val:
        return best_val
    if "paramedic" in blob:
        for r in rows:
            if str(r.get("value") or "").strip().lower() == "paramedic":
                return str(r.get("value")).strip()
    if "associate ambulance" in blob or " aap" in blob:
        for r in rows:
            if str(r.get("value") or "").strip().lower() == "aap":
                return str(r.get("value")).strip()
    if "care assistant" in blob or " eca" in blob or blob.strip() == "eca":
        for r in rows:
            if str(r.get("value") or "").strip().lower() == "eca":
                return str(r.get("value")).strip()
    if "technician" in blob or " emt" in blob or blob.strip() == "emt":
        for r in rows:
            if str(r.get("value") or "").strip().lower() == "tec":
                return str(r.get("value")).strip()
    if "student" in blob:
        for r in rows:
            if str(r.get("value") or "").strip().lower() == "student":
                return str(r.get("value")).strip()
    return None


def _normalize_crew_to_objects(raw_crew, sign_on_time=None):
    """Normalize crew from DB (list of strings or list of dicts) to [{ username, signedOnAt, grade? }, ...] for API."""
    if not raw_crew:
        return []
    out = []
    sign_on_iso = None
    if sign_on_time is not None and hasattr(sign_on_time, 'isoformat'):
        sign_on_iso = sign_on_time.isoformat() + 'Z' if sign_on_time else None
    for el in raw_crew:
        if isinstance(el, dict):
            uname = str((el.get('username') or el.get('user') or '')).strip()
            if not uname:
                continue
            signed_on = el.get('signedOnAt') or el.get('signed_on_at')
            if signed_on and hasattr(signed_on, 'isoformat'):
                signed_on = signed_on.isoformat() + 'Z'
            grade = str(el.get('grade') or el.get(
                'role') or '').strip() or None
            out.append(
                {'username': uname, 'signedOnAt': signed_on or sign_on_iso, 'grade': grade})
        else:
            uname = str(el).strip()
            if uname:
                out.append(
                    {'username': uname, 'signedOnAt': sign_on_iso, 'grade': None})
    return out


def _mdt_merge_sign_on_crew_payload_with_db(
        cur, callsign_canonical, incoming_objs, sign_on_iso):
    """
    If the target call sign is already signed on, merge the MDT payload crew into the stored roster
    instead of replacing it. Typical MDT login sends only the JWT user; CAD may already list other
    crew (e.g. reassignment onto another member's unit) — without merge they would be removed.
    Returns (merged_crew_objects, merged_usernames_for_validation, merged_usernames_alias).
    """
    cs = str(callsign_canonical or '').strip().upper()
    inc = list(incoming_objs or [])
    if not cs or not inc:
        names = [str(o.get('username') or '').strip()
                 for o in inc if str(o.get('username') or '').strip()]
        return inc, names, names
    try:
        cur.execute(
            """
            SELECT crew, signOnTime FROM mdts_signed_on WHERE callSign = %s LIMIT 1
            """,
            (cs,),
        )
        row = cur.fetchone()
    except Exception:
        row = None
    if not row:
        names = [str(o.get('username') or '').strip()
                 for o in inc if str(o.get('username') or '').strip()]
        return inc, names, names
    existing_raw = _mdt_parse_signed_on_crew_list(row.get('crew'))
    existing_norm = _normalize_crew_to_objects(
        existing_raw, row.get('signOnTime'))
    merged = []
    seen_lc = set()
    for el in existing_norm:
        un = str(el.get('username') or '').strip()
        if not un:
            continue
        lc = un.lower()
        inc_match = next(
            (x for x in inc
             if str(x.get('username') or '').strip().lower() == lc),
            None,
        )
        if inc_match:
            gr = inc_match.get('grade')
            if gr is None or str(gr).strip() == '':
                gr = el.get('grade')
            merged.append({
                'username': un,
                'signedOnAt': str(inc_match.get('signedOnAt') or sign_on_iso),
                'grade': gr,
            })
        else:
            so = el.get('signedOnAt') or sign_on_iso
            if so is not None and hasattr(so, 'isoformat'):
                so = so.isoformat() + 'Z'
            elif so is not None and not isinstance(so, str):
                so = str(so)
            elif so is None:
                so = sign_on_iso
            merged.append({
                'username': un,
                'signedOnAt': so,
                'grade': el.get('grade'),
            })
        seen_lc.add(lc)
    for el in inc:
        un = str(el.get('username') or '').strip()
        if not un:
            continue
        lc = un.lower()
        if lc in seen_lc:
            continue
        merged.append({
            'username': un,
            'signedOnAt': str(el.get('signedOnAt') or sign_on_iso),
            'grade': el.get('grade'),
        })
        seen_lc.add(lc)
    names = [str(o.get('username') or '').strip()
             for o in merged if str(o.get('username') or '').strip()]
    return merged, names, names


def _mdt_signed_on_row_has_username_in_crew(cur, username_lc, callsign_upper):
    """
    True only when mdts_signed_on has a live row for this call sign (after redirect resolve)
    and the stored crew JSON includes username_lc. Uses the same username extraction as sign-off
    (_mdt_signed_on_crew_usernames). Tries alternate call-sign keys before giving up.
    """
    cs = str(callsign_upper or '').strip().upper()
    un = str(username_lc or '').strip().lower()
    if not cs or not un:
        return False
    try:
        resolved = _mdt_resolve_callsign(cur, cs)
    except Exception:
        resolved = cs
    keys = []
    if resolved:
        keys.append(resolved)
    if cs and cs != resolved:
        keys.append(cs)
    seen = set()
    for key_cs in keys:
        if key_cs in seen:
            continue
        seen.add(key_cs)
        try:
            cur.execute(
                "SELECT crew FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
                (key_cs,),
            )
            row = cur.fetchone()
        except Exception:
            row = None
        if not row:
            continue
        crew_users = _mdt_signed_on_crew_usernames(row.get('crew'))
        if un in crew_users:
            return True
        # Row exists under this key but user not listed — try next alias; if none left, not on crew.
    return False


def _mdt_parse_signed_on_crew_list(crew_raw):
    """Parse mdts_signed_on.crew into a Python list (same rules as _mdt_signed_on_crew_usernames)."""
    try:
        if isinstance(crew_raw, (bytes, bytearray)):
            crew_raw = crew_raw.decode('utf-8', errors='ignore')
        if isinstance(crew_raw, str):
            return json.loads(crew_raw) if crew_raw.strip() else []
        if isinstance(crew_raw, list):
            return crew_raw
        if isinstance(crew_raw, dict):
            return [crew_raw]
    except Exception:
        pass
    return []


def _mdt_crew_raw_entry_username_lc(el):
    """Lowercase crew identity from a raw mdts_signed_on.crew element (parity with _mdt_signed_on_crew_usernames)."""
    if isinstance(el, dict):
        u = el.get('username') or el.get('user') or ''
        return str(u).strip().lower()
    return str(el).strip().lower()


def _dedupe_crew_raw_list_preserve_order(raw_list):
    """If the same username appears more than once in stored crew JSON, keep the first entry only.

    Without this, removing one reassigned user can drop every duplicate row and clear the roster,
    which triggers a full unit sign-off and kicks remaining crew off the MDT.
    """
    if not raw_list:
        return []
    seen = set()
    out = []
    for el in raw_list:
        lc = _mdt_crew_raw_entry_username_lc(el)
        if not lc or lc in seen:
            continue
        seen.add(lc)
        out.append(el)
    return out


def _dedupe_crew_objects_preserve_order(crew_objs):
    """After ``_normalize_crew_to_objects``: at most one entry per username (first wins)."""
    if not crew_objs:
        return []
    seen = set()
    out = []
    for c in crew_objs:
        lc = _mdt_crew_raw_entry_username_lc(c)
        if not lc or lc in seen:
            continue
        seen.add(lc)
        out.append(c)
    return out


def _mdt_find_signed_on_callsigns_with_username_elsewhere(
        cur, username_lc, target_callsign_resolved):
    """
    Return canonical callSign values (from mdts_signed_on rows) where username_lc appears in crew
    and the row is not the target sign-on call sign (resolved).
    """
    un = str(username_lc or '').strip().lower()
    targ = str(target_callsign_resolved or '').strip().upper()
    if not un or not targ:
        return []
    out = []
    seen = set()
    try:
        cur.execute(
            """
            SELECT callSign, crew FROM mdts_signed_on
            WHERE callSign IS NOT NULL AND TRIM(callSign) <> ''
            """,
        )
        rows = cur.fetchall() or []
    except Exception:
        rows = []
    for r in rows:
        cs_row = str((r.get('callSign') if isinstance(r, dict) else r[0]) or '').strip().upper()
        if not cs_row or cs_row in seen:
            continue
        try:
            res_cs = str(_mdt_resolve_callsign(cur, cs_row) or cs_row).strip().upper()
        except Exception:
            res_cs = cs_row
        if res_cs == targ:
            continue
        crew_raw = r.get('crew') if isinstance(r, dict) else (r[1] if len(r) > 1 else None)
        try:
            crew_users = set(_mdt_signed_on_crew_usernames(crew_raw))
        except Exception:
            crew_users = set()
        if un in crew_users:
            seen.add(cs_row)
            out.append(cs_row)
    return out


def _mdt_takeover_exec_logged_crew_removal_from_unit(
        cur, unit_cs, jwt_username, removed_uname, removal_reason_row, new_callsign, log_source):
    """
    Crew removal log + audit + _mdt_takeover_remove_user_from_signed_on_unit_crew.
    log_source e.g. 'mdt_remote_session_takeover' or 'mdt_remote_session_takeover_crew_elsewhere'.
    """
    _ensure_crew_removal_log_table(cur)
    try:
        has_notes = False
        try:
            cur.execute(
                "SHOW COLUMNS FROM mdt_crew_removal_log LIKE 'notes'")
            has_notes = cur.fetchone() is not None
        except Exception:
            pass
        if has_notes:
            cur.execute("""
                INSERT INTO mdt_crew_removal_log
                    (callSign, removed_username, removal_reason, removed_by, removed_at, notes)
                VALUES (%s, %s, %s, %s, NOW(), %s)
            """, (
                unit_cs,
                removed_uname,
                removal_reason_row,
                unit_cs,
                None,
            ))
        else:
            cur.execute("""
                INSERT INTO mdt_crew_removal_log
                    (callSign, removed_username, removal_reason, removed_by, removed_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                unit_cs,
                removed_uname,
                removal_reason_row,
                unit_cs,
            ))
    except Exception:
        pass
    try:
        log_audit(
            _mdt_audit_actor(),
            'mdt_crew_remove',
            details={
                'callsign': unit_cs,
                'removed_username': removed_uname,
                'reason': removal_reason_row,
                'source': log_source,
                'new_callsign': new_callsign,
            },
        )
    except Exception:
        pass
    _ensure_job_units_table(cur)
    return _mdt_takeover_remove_user_from_signed_on_unit_crew(
        cur, unit_cs, jwt_username)


def _mdt_takeover_remove_user_from_signed_on_unit_crew(cur, unit_callsign, username_lc):
    """
    When the same user signs onto a different call sign and terminates their other session:
    remove only that user from unit_callsign's crew in mdts_signed_on.
    If they were the last crew member, perform a full unit sign-off (mdts + job_units + CAD sync).
    Returns (outcome, affected_cads, row_call_sign) where row_call_sign is the mdts_signed_on.callSign
    key that was updated (for CAD refresh); None when no row was touched.
    """
    cs = str(unit_callsign or '').strip().upper()
    un = str(username_lc or '').strip().lower()
    if not cs or not un:
        return 'missing', set(), None
    try:
        resolved = _mdt_resolve_callsign(cur, cs)
    except Exception:
        resolved = cs
    keys = []
    if resolved:
        keys.append(resolved)
    if cs and cs != resolved:
        keys.append(cs)
    seen = set()
    row = None
    row_cs = None
    for key_cs in keys:
        if not key_cs or key_cs in seen:
            continue
        seen.add(key_cs)
        try:
            cur.execute(
                """
                SELECT crew, signOnTime, callSign,
                       LOWER(TRIM(COALESCE(division, 'general'))) AS division
                FROM mdts_signed_on WHERE callSign = %s LIMIT 1
                """,
                (key_cs,),
            )
            r = cur.fetchone()
        except Exception:
            r = None
        if r:
            row = r
            row_cs = str(r.get('callSign') or key_cs).strip().upper()
            break
    if not row or not row_cs:
        return 'missing', set(), None
    data = _dedupe_crew_raw_list_preserve_order(
        _mdt_parse_signed_on_crew_list(row.get('crew')))
    before = len(data)
    data_filtered = [el for el in data if _mdt_crew_raw_entry_username_lc(el) != un]
    if len(data_filtered) == before:
        return 'user_not_in_crew', set(), None
    if not data_filtered:
        # Last crew on unit: sign off CAD row, then revoke device sessions only for usernames that
        # were on this roster (scoped by call sign). A blanket DELETE BY callsign would remove
        # other crew members' device rows for this vehicle and kick them off the MDT incorrectly.
        everyone_lc = sorted({
            _mdt_crew_raw_entry_username_lc(el)
            for el in data
            if _mdt_crew_raw_entry_username_lc(el)
        })
        affected_cads, _ = _mdt_sign_off_unit_sql(cur, row_cs)
        div_tf = str((row or {}).get('division') or 'general').strip().lower() or 'general'
        everyone_disp = [
            str((el or {}).get('username') or (el or {}).get('user') or '').strip()
            for el in data
            if str((el or {}).get('username') or (el or {}).get('user') or '').strip()
        ]
        _record_mdt_session_lifecycle_event(
            cur,
            event_type='sign_off',
            callsign=row_cs,
            division=div_tf,
            actor_username=un,
            previous_callsign=None,
            crew_usernames=everyone_disp,
            detail={
                'affected_cads': sorted(affected_cads or []),
                'trigger': 'takeover_sign_on_elsewhere',
            },
        )
        row_cs_upper = str(row_cs).strip().upper()
        for who in everyone_lc:
            try:
                cur.execute(
                    """
                    DELETE FROM mdt_user_mdt_session
                    WHERE LOWER(TRIM(username)) = %s AND UPPER(TRIM(callsign)) = %s
                    """,
                    (who, row_cs_upper),
                )
            except Exception:
                pass
        return 'full_signoff', set(affected_cads or []), row_cs
    crew_objs = _normalize_crew_to_objects(
        data_filtered, sign_on_time=row.get('signOnTime'))
    cur.execute(
        "UPDATE mdts_signed_on SET crew = %s, lastSeenAt = NOW() WHERE callSign = %s",
        (json.dumps(crew_objs), row_cs),
    )
    return 'partial', set(), row_cs


def _safe_profile_picture_path(path):
    """Return path only if safe for static serving; otherwise None (no path traversal, no absolute)."""
    if not path or not isinstance(path, str):
        return None
    import re
    cleaned = path.strip()
    if ".." in cleaned or cleaned.startswith("/") or re.match(r"^[a-zA-Z]:", cleaned):
        return None
    if not re.match(r"^[\w/.\-]+$", cleaned):
        return None
    return cleaned


def _ensure_mdt_crew_profiles_table(cur):
    global _schema_bootstrap_state
    if _schema_bootstrap_state.get("crew_profiles"):
        return
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_crew_profiles (
            username VARCHAR(120) NOT NULL PRIMARY KEY,
            contractor_id INT NULL,
            gender VARCHAR(24) NULL,
            skills_json JSON NULL,
            qualifications_json JSON NULL,
            profile_picture_path VARCHAR(512) NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_crew_profiles_contractor (contractor_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    try:
        cur.execute(
            "SHOW COLUMNS FROM mdt_crew_profiles LIKE 'profile_picture_path'")
        if not cur.fetchone():
            cur.execute(
                "ALTER TABLE mdt_crew_profiles ADD COLUMN profile_picture_path VARCHAR(512) NULL AFTER qualifications_json")
    except Exception:
        pass
    _schema_bootstrap_state["crew_profiles"] = True


def _get_contractor_id_for_username(cur, username):
    """
    Resolve a crew login string to ``tb_contractors.id`` for **active** rows, **username only**
    (case-insensitive)—parity with core ``users`` matching, not email or display name.
    """
    uname = (username or "").strip()
    if not uname:
        return None
    active = (
        "(status IS NULL OR LOWER(TRIM(COALESCE(status,''))) IN "
        "('active','1','true','yes'))"
    )
    try:
        cur.execute("SHOW TABLES LIKE 'tb_contractors'")
        if not cur.fetchone():
            return None
        cur.execute(
            """
            SELECT id FROM tb_contractors
            WHERE """
            + active
            + """
              AND username IS NOT NULL AND TRIM(username) <> ''
              AND LOWER(TRIM(username)) = LOWER(%s)
            LIMIT 1
            """,
            (uname,),
        )
        row = cur.fetchone()
        return int(row["id"]) if row and row.get("id") is not None else None
    except Exception:
        return None


def _is_core_user_username(cur, username):
    """Return True if username matches a core user (users table) by username or email."""
    uname = (username or "").strip()
    if not uname:
        return False
    try:
        cur.execute("SHOW TABLES LIKE 'users'")
        if not cur.fetchone():
            return False
        cur.execute(
            "SELECT 1 FROM users WHERE LOWER(TRIM(COALESCE(username,''))) = LOWER(%s) OR LOWER(TRIM(COALESCE(email,''))) = LOWER(%s) LIMIT 1",
            (uname, uname),
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def _is_valid_crew_username(cur, username):
    """Return True if username is a valid crew member: core user (users), active contractor (tb_contractors), or mdt_crew_profiles."""
    uname = (username or "").strip()
    if not uname:
        return False
    if _is_core_user_username(cur, uname):
        return True
    if _get_contractor_id_for_username(cur, uname) is not None:
        return True
    try:
        _ensure_mdt_crew_profiles_table(cur)
        cur.execute(
            "SELECT 1 FROM mdt_crew_profiles WHERE username = %s LIMIT 1", (uname,))
        if cur.fetchone():
            return True
    except Exception:
        pass
    return False


_CAD_CREW_MANAGER_ROLES = frozenset({
    "dispatcher", "admin", "superuser", "clinical_lead", "controller",
})


def _cad_user_can_manage_signed_on_crew():
    try:
        if not current_user.is_authenticated:
            return False
        role = str(getattr(current_user, "role", "") or "").lower()
        return role in _CAD_CREW_MANAGER_ROLES
    except Exception:
        return False


def _cad_normalize_crew_removal_reason(raw):
    s = str(raw or "").strip().lower().replace(" ", "_").replace("-", "_")
    aliases = {
        "reassign": "reassigned",
        "new_unit": "different_unit",
        "unit_change": "different_unit",
        "differentunit": "different_unit",
        "swap": "crew_swap",
        "swapped": "crew_swap",
        "crewswitch": "crew_swap",
        "sick": "illness",
        "finish_early": "early_finish",
        "earlyfinish": "early_finish",
    }
    s = aliases.get(s, s)
    allowed = {
        "illness", "early_finish", "reassigned", "different_unit",
        "crew_swap", "personal", "other",
    }
    return s if s in allowed else "other"


def _crew_dispatch_search_preview(cur, username):
    """Rich row for CAD crew picker: label, optional photo URL, grade/job title hint."""
    uname = (username or "").strip()
    if not uname:
        return None
    label = uname
    profile_picture_url = None
    grade_label = None
    contractor_id = _get_contractor_id_for_username(cur, uname)
    profile = _get_crew_profile(cur, uname)
    if profile and profile.get("profile_picture_path"):
        sp = _safe_profile_picture_path(profile.get("profile_picture_path"))
        if sp:
            profile_picture_url = "/static/" + sp
    if profile and profile.get("contractor_id") is not None:
        try:
            contractor_id = int(profile["contractor_id"])
        except (TypeError, ValueError):
            pass
    try:
        cur.execute("SHOW TABLES LIKE 'users'")
        if cur.fetchone():
            cur.execute(
                """
                SELECT username, first_name, last_name, email
                  FROM users
                 WHERE LOWER(TRIM(username)) = LOWER(%s)
                 LIMIT 1
                """,
                (uname,),
            )
            ur = cur.fetchone()
            if ur:
                fn = f"{(ur.get('first_name') or '').strip()} {(ur.get('last_name') or '').strip()}".strip()
                label = fn or (str(ur.get("email") or "").strip()) or str(
                    ur.get("username") or "").strip() or label
    except Exception:
        pass
    if contractor_id:
        try:
            cur.execute("SHOW TABLES LIKE 'tb_contractors'")
            if cur.fetchone():
                cur.execute(
                    "SELECT job_title FROM tb_contractors WHERE id = %s LIMIT 1",
                    (contractor_id,),
                )
                crow = cur.fetchone()
                if crow and crow.get("job_title"):
                    jt = str(crow.get("job_title")).strip()
                    if jt:
                        grade_label = grade_label or jt
                cur.execute(
                    "SHOW COLUMNS FROM tb_contractors LIKE 'profile_picture_path'")
                if cur.fetchone():
                    cur.execute(
                        "SELECT profile_picture_path FROM tb_contractors WHERE id = %s LIMIT 1",
                        (contractor_id,),
                    )
                    pr2 = cur.fetchone()
                    if pr2 and pr2.get("profile_picture_path") and not profile_picture_url:
                        ssp = _safe_profile_picture_path(
                            pr2.get("profile_picture_path"))
                        if ssp:
                            profile_picture_url = "/static/" + ssp
        except Exception:
            pass
        try:
            cur.execute(
                """
                SELECT h.job_title FROM hr_staff_details h
                WHERE h.contractor_id = %s LIMIT 1
                """,
                (contractor_id,),
            )
            hr = cur.fetchone()
            if hr and hr.get("job_title") and not grade_label:
                grade_label = str(hr.get("job_title")).strip() or None
        except Exception:
            pass
    suggested_grade = _suggest_crew_grade_value_from_hr_text(cur, grade_label)
    return {
        "username": uname,
        "label": label,
        "profile_picture_url": profile_picture_url,
        "grade_label": grade_label,
        "suggested_grade": suggested_grade,
    }


def _cad_emit_units_updated_socket(callsign):
    cs = str(callsign or "").strip().upper()
    if not cs:
        return
    try:
        socketio.emit("mdt_event", {"type": "units_updated", "callsign": cs})
    except Exception:
        logger.exception("cad crew socket emit callsign=%s", cs)


def _validate_crew_usernames(cur, usernames):
    """Return (invalid_list, error_message). invalid_list is non-empty if any username is not a valid crew member."""
    invalid = []
    for u in (usernames or []):
        uname = (u or "").strip()
        if not uname:
            continue
        if not _is_valid_crew_username(cur, uname):
            invalid.append(uname)
    if invalid:
        return invalid, "Crew member(s) not found (must be a user or active contractor): " + ", ".join(invalid)
    return [], None


def _get_crew_profile(cur, username):
    """Return profile row from mdt_crew_profiles (gender, skills_json, qualifications_json, contractor_id, profile_picture_path)."""
    uname = (username or "").strip()
    if not uname:
        return None
    try:
        _ensure_mdt_crew_profiles_table(cur)
        cur.execute(
            "SELECT contractor_id, gender, skills_json, qualifications_json, profile_picture_path FROM mdt_crew_profiles WHERE username = %s LIMIT 1",
            (uname,),
        )
        return cur.fetchone()
    except Exception:
        return None


def _normalize_crew_trait_display_list(raw):
    """Coerce skills/qualifications list items to display strings (stored JSON may use dict rows)."""
    if not isinstance(raw, list):
        return []
    out = []
    for x in raw:
        if isinstance(x, dict):
            name = str(
                x.get("label")
                or x.get("name")
                or x.get("title")
                or x.get("qualification")
                or x.get("skill")
                or x.get("description")
                or ""
            ).strip()
            code = str(x.get("code") or x.get("short_code") or x.get("type") or "").strip()
            if name and code and code.lower() not in name.lower():
                s = f"{code} — {name}"
            elif name:
                s = name
            elif code:
                s = code
            else:
                s = ""
            if s:
                out.append(s)
        elif x is not None:
            s = str(x).strip()
            if s:
                out.append(s)
    return out


def _enrich_crew_with_profiles(cur, crew):
    """Enrich each crew member from MySQL: mdt_crew_profiles (gender, skills, qualifications, profile_picture_path); fallback profile pic from tb_contractors when contractor_id set."""
    if not crew:
        return crew
    try:
        cur.execute("SHOW TABLES LIKE 'tb_contractors'")
        has_contractors = cur.fetchone() is not None
        has_profile_col = False
        if has_contractors:
            cur.execute(
                "SHOW COLUMNS FROM tb_contractors LIKE 'profile_picture_path'")
            has_profile_col = cur.fetchone() is not None
    except Exception:
        has_contractors = False
        has_profile_col = False
    out = []
    for c in crew:
        uname = (c.get("username") or "").strip()
        if not uname:
            out.append({**c, "gender": None, "profile_picture_url": None,
                       "skills": [], "qualifications": []})
            continue
        profile = _get_crew_profile(cur, uname)
        contractor_id = None
        if profile and profile.get("contractor_id") is not None:
            contractor_id = int(profile["contractor_id"])
        if contractor_id is None:
            contractor_id = _get_contractor_id_for_username(cur, uname)
        gender = None
        skills = []
        qualifications = []
        if profile:
            g = (profile.get("gender") or "").strip() or None
            if g:
                gender = g
            try:
                if profile.get("skills_json"):
                    skills = json.loads(profile["skills_json"]) if isinstance(
                        profile["skills_json"], str) else (profile["skills_json"] or [])
                if profile.get("qualifications_json"):
                    qualifications = json.loads(profile["qualifications_json"]) if isinstance(
                        profile["qualifications_json"], str) else (profile["qualifications_json"] or [])
            except Exception:
                pass
        profile_picture_url = None
        if profile and profile.get("profile_picture_path"):
            safe = _safe_profile_picture_path(profile["profile_picture_path"])
            if safe:
                profile_picture_url = "/static/" + safe
        if not profile_picture_url and has_profile_col and contractor_id is not None:
            try:
                cur.execute(
                    "SELECT profile_picture_path FROM tb_contractors WHERE id = %s LIMIT 1", (contractor_id,))
                row = cur.fetchone()
                if row and row.get("profile_picture_path"):
                    safe = _safe_profile_picture_path(
                        row["profile_picture_path"])
                    if safe:
                        profile_picture_url = "/static/" + safe
            except Exception:
                pass
        entry = {
            **c,
            "gender": gender,
            "profile_picture_url": profile_picture_url,
            "skills": list(skills) if isinstance(skills, list) else [],
            "qualifications": list(qualifications) if isinstance(qualifications, list) else [],
        }
        if contractor_id is not None:
            job_title = None
            if has_contractors:
                try:
                    cur.execute(
                        """
                        SELECT h.job_title FROM hr_staff_details h
                        WHERE h.contractor_id = %s LIMIT 1
                        """,
                        (contractor_id,),
                    )
                    jtr = cur.fetchone()
                    if jtr and jtr.get("job_title"):
                        job_title = str(jtr.get("job_title")).strip() or None
                except Exception:
                    try:
                        cur.execute(
                            "SELECT job_title FROM tb_contractors WHERE id = %s LIMIT 1",
                            (contractor_id,),
                        )
                        jtr = cur.fetchone()
                        if jtr and jtr.get("job_title"):
                            job_title = str(
                                jtr.get("job_title")).strip() or None
                    except Exception:
                        pass
            sk_empty = not (isinstance(
                entry.get("skills"), list) and entry["skills"])
            qu_empty = not (
                isinstance(entry.get("qualifications"),
                           list) and entry["qualifications"]
            )
            if sk_empty and qu_empty:
                try:
                    from app.plugins.training_module.services import TrainingService

                    TrainingService.cad_merge_competencies_into_crew_entry(
                        cur, entry, int(contractor_id), job_title
                    )
                except Exception:
                    pass
        entry["skills"] = _normalize_crew_trait_display_list(entry.get("skills"))
        entry["qualifications"] = _normalize_crew_trait_display_list(
            entry.get("qualifications")
        )
        out.append(entry)
    return out


def _ensure_job_comms_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mdt_job_comms (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cad INT NOT NULL,
            message_type VARCHAR(24) NOT NULL DEFAULT 'message',
            sender_role VARCHAR(64),
            sender_user VARCHAR(120),
            message_text LONGTEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_job_comms_cad (cad),
            INDEX idx_job_comms_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def _ensure_dispatcher_inbox_cleared_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dispatcher_inbox_cleared (
            username VARCHAR(120) NOT NULL PRIMARY KEY,
            cleared_at DATETIME NOT NULL,
            INDEX idx_inbox_cleared_at (cleared_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


_MAJOR_INCIDENT_TEMPLATES = frozenset({'methane', 'custom'})

# Human-readable labels for job comm / summary (avoid snake_case in dispatcher UI).
_MAJOR_INCIDENT_FIELD_LABELS = {
    'major_incident_declared': 'Major Incident declared',
    'exact_location': 'Exact location',
    'type_of_incident': 'Type of incident',
    'hazards': 'Hazards',
    'access_routes': 'Access routes',
    'number_casualties': 'Number of casualties',
    'emergency_services': 'Emergency services',
    'phase': 'Phase',
    'command': 'Command / structure',
    'logistics': 'Logistics',
    'summary': 'Summary',
    'title': 'Title',
    'detail': 'Detail',
    'division': 'Division',
}

_MAJOR_INCIDENT_COMM_FIELD_ORDER = [
    'major_incident_declared', 'exact_location', 'type_of_incident',
    'hazards', 'access_routes', 'number_casualties', 'emergency_services',
    'phase', 'command', 'logistics',
    'detail', 'division',
]


def _major_incident_field_label(key):
    k = str(key or '').strip().lower()
    if k in _MAJOR_INCIDENT_FIELD_LABELS:
        return _MAJOR_INCIDENT_FIELD_LABELS[k]
    return ' '.join(x.capitalize() for x in k.replace('-', '_').split('_') if x)


def _major_incident_payload_nonempty(payload):
    """Require at least one meaningful field so rows are not empty noise (CAD + MDT)."""
    if not isinstance(payload, dict):
        return False
    for v in payload.values():
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            return True
        if isinstance(v, bool):
            return True
        if isinstance(v, (int, float)):
            if isinstance(v, float) and v != v:  # NaN
                continue
            return True
    return False


def _ensure_major_incidents_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ventus_major_incidents (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            api_version SMALLINT NOT NULL DEFAULT 1,
            template_kind VARCHAR(32) NOT NULL,
            phase VARCHAR(64) NULL,
            cad INT NULL,
            division VARCHAR(64) NULL,
            payload_json LONGTEXT NOT NULL,
            source VARCHAR(16) NOT NULL,
            callsign VARCHAR(64) NULL,
            client_request_id VARCHAR(128) NULL,
            summary_title VARCHAR(255) NULL,
            status VARCHAR(24) NOT NULL DEFAULT 'open',
            created_by VARCHAR(120) NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            closed_at TIMESTAMP NULL DEFAULT NULL,
            closed_by VARCHAR(120) NULL,
            INDEX idx_vmi_cad (cad),
            INDEX idx_vmi_status (status),
            INDEX idx_vmi_created (created_at),
            INDEX idx_vmi_template (template_kind),
            INDEX idx_vmi_callsign_req (callsign, client_request_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def _major_incident_summary_title(template_kind, payload):
    tk = str(template_kind or '').strip().lower()
    p = payload if isinstance(payload, dict) else {}
    s = str(p.get('summary') or p.get('title') or '').strip()
    if s:
        return s[:255]
    if tk == 'methane':
        parts = []
        for k in (
            'major_incident_declared', 'exact_location', 'type_of_incident',
            'hazards', 'access_routes', 'number_casualties', 'emergency_services',
        ):
            v = p.get(k)
            if v not in (None, ''):
                lbl = _major_incident_field_label(k)
                parts.append(f"{lbl}: {str(v)[:48]}")
        if parts:
            return ', '.join(parts)[:255]
    return (tk.upper() + ' Major Incident')[:255] if tk else 'Major Incident'


def _major_incident_parse_request_body(body, *, source_mdt):
    """Validate CAD-MI-001 / MDT-MI-001 JSON. Returns (errors, parsed) or (errors, None)."""
    errors = []
    if not isinstance(body, dict):
        return ['body must be a JSON object'], None
    try:
        api_version = int(body.get('api_version', 1))
    except (TypeError, ValueError):
        errors.append('api_version must be an integer')
        api_version = None
    if api_version is not None and api_version != 1:
        errors.append('unsupported api_version (only 1 is supported)')

    template = str(
        body.get('template') or body.get('template_kind') or ''
    ).strip().lower()
    if not template:
        errors.append('template is required')
    elif template not in _MAJOR_INCIDENT_TEMPLATES:
        errors.append(
            'template must be one of: methane, custom'
        )

    payload = body.get('payload')
    if not isinstance(payload, dict):
        errors.append('payload must be a JSON object')
    elif not _major_incident_payload_nonempty(payload):
        errors.append(
            'payload must contain at least one non-empty field '
            '(e.g. summary or a structured line)'
        )

    phase = str(body.get('phase') or '').strip()[:64] or None
    cad = body.get('cad')
    try:
        cad_i = int(cad) if cad not in (None, '') else None
    except (TypeError, ValueError):
        errors.append('cad must be an integer when supplied')
        cad_i = None

    division = str(body.get('division') or '').strip()[:64] or None
    client_request_id = str(
        body.get('client_request_id') or body.get('idempotency_key') or ''
    ).strip()[:128] or None
    if source_mdt and not client_request_id:
        errors.append('client_request_id is required for MDT submissions (UUID recommended)')

    if errors:
        return errors, None
    return [], {
        'api_version': 1,
        'template': template,
        'payload': payload,
        'phase': phase,
        'cad': cad_i,
        'division': division,
        'client_request_id': client_request_id,
    }


def _major_incident_insert_row(
    cur,
    *,
    parsed,
    source,
    callsign,
    created_by,
):
    """Insert ventus_major_incidents row; return new id."""
    summary = _major_incident_summary_title(
        parsed['template'], parsed['payload'])
    cur.execute(
        """
        INSERT INTO ventus_major_incidents (
            api_version, template_kind, phase, cad, division,
            payload_json, source, callsign, client_request_id,
            summary_title, status, created_by
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open',%s)
        """,
        (
            parsed['api_version'],
            parsed['template'],
            parsed['phase'],
            parsed['cad'],
            parsed['division'],
            json.dumps(parsed['payload'], default=str),
            source,
            callsign,
            parsed['client_request_id'],
            summary,
            created_by,
        ),
    )
    return int(cur.lastrowid)


def _major_incident_payload_lines_for_comm(payload, template_kind):
    """Ordered (label, value) pairs for job comm text — human labels, no slug dump."""
    p = payload if isinstance(payload, dict) else {}
    sum_str = str(p.get('summary') or '').strip()
    out = []
    seen = set()

    def skip_detail_dup(k, v):
        if k != 'detail' or not sum_str:
            return False
        return str(v).strip() == sum_str

    for k in _MAJOR_INCIDENT_COMM_FIELD_ORDER:
        if k not in p or k in ('summary', 'title'):
            continue
        v = p.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s or skip_detail_dup(k, v):
            continue
        seen.add(k)
        out.append((_major_incident_field_label(k), s))

    for k in sorted(p.keys()):
        if k in seen or k in ('summary', 'title'):
            continue
        v = p.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s or skip_detail_dup(k, v):
            continue
        seen.add(k)
        out.append((_major_incident_field_label(k), s))
    return out


def _major_incident_job_comm_text(template, summary, payload):
    """Inbox/job-comm body: declaration headline, optional summary line, then labelled fields once."""
    tk_disp = str(template or '').strip().upper() or 'MAJOR'
    p = payload if isinstance(payload, dict) else {}
    lines = [f'Major Incident declaration ({tk_disp})']
    sum_clean = str(p.get('summary') or p.get('title') or '').strip()
    if sum_clean:
        lines.append(f"{_major_incident_field_label('summary')}: {sum_clean}")
    tk = str(template or '').strip().lower()
    for lbl, val in _major_incident_payload_lines_for_comm(p, tk):
        lines.append(f'{lbl}: {val}')
    return '\n'.join(lines)[:4000]


def _major_incident_emit_socket(
    mi_id,
    parsed,
    callsign,
    source,
    summary,
    comm_text=None,
    job_comm_sender=None,
    job_comm_id=None,
):
    """Socket: dispatcher alert + optional job comm (inbox / job detail live refresh)."""
    try:
        socketio.emit('mdt_event', {
            'type': 'major_incident_alert',
            'id': mi_id,
            'template': parsed['template'],
            'phase': parsed.get('phase'),
            'cad': parsed['cad'],
            'division': parsed.get('division'),
            'callsign': callsign,
            'source': source,
            'title': summary,
            'continuous_alarm': False,
        })
        cad = parsed.get('cad')
        if cad:
            line = comm_text or _major_incident_job_comm_text(
                parsed['template'], summary, parsed.get('payload'))
            by = str(job_comm_sender or callsign or 'dispatcher').strip() or 'dispatcher'
            _mi_jc = {
                'type': 'job_comm',
                'cad': cad,
                'message_type': 'major_incident',
                'text': line,
                'by': by,
            }
            _mi_jcid = int(job_comm_id or 0)
            if _mi_jcid > 0:
                _mi_jc['id'] = _mi_jcid
            socketio.emit('mdt_event', _mi_jc)
            socketio.emit('mdt_event', {'type': 'jobs_updated', 'cad': cad})
    except Exception:
        logger.exception('major_incident socket emit id=%s', mi_id)


def _queue_division_transfer_instruction(cur, callsign, from_division, to_division, actor=None, reason=None, cad=None):
    """Queue a division/operating-area transfer instruction for MDT clients."""
    cs = str(callsign or '').strip().upper()
    src = _normalize_division(from_division, fallback='general')
    dst = _normalize_division(to_division, fallback='general')
    if not cs or src == dst:
        return None

    _ensure_standby_tables(cur)

    actor_name = str(actor or '').strip()
    payload = {
        'from_division': src,
        'to_division': dst,
        'operating_area': dst,
        'reason': str(reason or 'division_transfer'),
        'transferred_by': actor_name or None,
        'cad': int(cad) if cad not in (None, '') else None,
    }
    cur.execute("""
        INSERT INTO mdt_dispatch_instructions (callSign, instruction_type, payload, status, created_by)
        VALUES (%s, %s, %s, 'pending', %s)
    """, (cs, 'division_transfer', json.dumps(payload), actor_name or None))
    instruction_id = cur.lastrowid

    try:
        cur.execute("SHOW TABLES LIKE 'messages'")
        has_messages = cur.fetchone() is not None
        if has_messages:
            sender_name = _sender_label_from_portal('dispatch', actor_name)
            text = f"Operating area transfer: {src} -> {dst}."
            if payload.get('cad') is not None:
                text += f" CAD #{payload.get('cad')}."
            cur.execute("""
                INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                VALUES (%s, %s, %s, NOW(), 0)
            """, (sender_name, cs, text))
    except Exception:
        pass

    return instruction_id


def _get_job_unit_callsigns(cur, cad):
    cur.execute(
        "SELECT callsign FROM mdt_job_units WHERE job_cad = %s ORDER BY assigned_at ASC",
        (cad,)
    )
    rows = cur.fetchall() or []
    if rows and isinstance(rows[0], dict):
        return [r['callsign'] for r in rows if r.get('callsign')]
    return [r[0] for r in rows if r and r[0]]


def _sync_claimed_by_from_job_units(cur, cad):
    units = _get_job_unit_callsigns(cur, cad)
    claimed_by = ",".join(units) if units else None
    cur.execute(
        "UPDATE mdt_jobs SET claimedBy = %s WHERE cad = %s",
        (claimed_by, cad)
    )
    return units


def _mdt_detach_unit_from_job(
    cur,
    cad,
    callsign,
    is_live_assignment,
    is_linked_assignment,
    empty_job_next_status='queued',
    response_log_crew_json=None,
):
    """Remove one unit from CAD links and reconcile job row when no units remain.

    ``empty_job_next_status``:
    - ``queued`` — stood_down / supersede / panic detach; dispatcher may reassign.
    - ``dispatch_review`` — last MDT unit cleared; CAD stays on stack for closure review / more resources.
    - ``cleared`` — legacy immediate close (unused from MDT clear path; prefer ``dispatch_review``).

    ``response_log_crew_json``: optional JSON string for ``mdt_response_log.crew`` when logging
    ``closure_review`` after the last unit clears (defaults to ``[]``).
    """
    crew_for_closure_log = response_log_crew_json if response_log_crew_json is not None else '[]'
    if not isinstance(crew_for_closure_log, str):
        try:
            crew_for_closure_log = json.dumps(crew_for_closure_log, default=str)
        except Exception:
            crew_for_closure_log = '[]'
    cur.execute("SHOW TABLES LIKE 'mdt_job_units'")
    has_job_units = cur.fetchone() is not None
    remaining_units = []
    if has_job_units:
        cur.execute(
            "DELETE FROM mdt_job_units WHERE job_cad = %s AND callsign = %s",
            (cad, callsign)
        )
        remaining_units = _sync_claimed_by_from_job_units(cur, cad)
    if is_live_assignment or is_linked_assignment:
        if len(remaining_units) == 0:
            nxt = str(empty_job_next_status or 'queued').strip().lower()
            if nxt not in ('queued', 'cleared', _JOB_STATUS_AWAITING_DISPATCH_REVIEW):
                nxt = 'queued'
            cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
            has_updated_at = cur.fetchone() is not None
            if nxt == 'cleared':
                if has_updated_at:
                    cur.execute(
                        "UPDATE mdt_jobs SET status = 'cleared', updated_at = NOW() "
                        "WHERE cad = %s AND LOWER(TRIM(COALESCE(status, ''))) <> 'cleared'",
                        (cad,),
                    )
                else:
                    cur.execute(
                        "UPDATE mdt_jobs SET status = 'cleared' "
                        "WHERE cad = %s AND LOWER(TRIM(COALESCE(status, ''))) <> 'cleared'",
                        (cad,),
                    )
            elif nxt == _JOB_STATUS_AWAITING_DISPATCH_REVIEW:
                if has_updated_at:
                    cur.execute(
                        "UPDATE mdt_jobs SET status = %s, updated_at = NOW() "
                        "WHERE cad = %s AND LOWER(TRIM(COALESCE(status, ''))) NOT IN ('cleared', 'stood_down')",
                        (_JOB_STATUS_AWAITING_DISPATCH_REVIEW, cad),
                    )
                else:
                    cur.execute(
                        "UPDATE mdt_jobs SET status = %s "
                        "WHERE cad = %s AND LOWER(TRIM(COALESCE(status, ''))) NOT IN ('cleared', 'stood_down')",
                        (_JOB_STATUS_AWAITING_DISPATCH_REVIEW, cad),
                    )
                if cur.rowcount:
                    try:
                        _ensure_response_log_table(cur)
                        cs_log = str(callsign or '').strip()[:64] or 'UNKNOWN'
                        cur.execute(
                            """
                            INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew)
                            VALUES (%s, %s, %s, NOW(), %s)
                            """,
                            (
                                cs_log,
                                cad,
                                _RESPONSE_LOG_STATUS_CLOSURE_REVIEW,
                                crew_for_closure_log,
                            ),
                        )
                    except Exception:
                        logger.exception(
                            'response_log closure_review cad=%s', cad)
            else:
                if has_updated_at:
                    cur.execute(
                        "UPDATE mdt_jobs SET status = 'queued', updated_at = NOW() "
                        "WHERE cad = %s AND LOWER(TRIM(COALESCE(status, ''))) <> 'cleared'",
                        (cad,),
                    )
                else:
                    cur.execute(
                        "UPDATE mdt_jobs SET status = 'queued' "
                        "WHERE cad = %s AND LOWER(TRIM(COALESCE(status, ''))) <> 'cleared'",
                        (cad,),
                    )
        else:
            cur.execute(
                "UPDATE mdt_jobs SET status = 'assigned' WHERE cad = %s",
                (cad,)
            )
    return remaining_units


def _previous_attached_units_for_cad(cur, cad, current_callsigns):
    """Units with a terminal detach logged for this CAD who are no longer linked."""
    out = []
    seen = set()
    try:
        _ensure_response_log_table(cur)
        cur.execute("SHOW TABLES LIKE 'mdt_response_log'")
        if cur.fetchone() is None:
            return out
        current = {str(c).strip().upper()
                   for c in (current_callsigns or []) if str(c).strip()}
        cur.execute(
            """
            SELECT l.callSign, l.status AS detach_status, l.event_time AS detached_at
            FROM mdt_response_log l
            INNER JOIN (
                SELECT callSign, MAX(event_time) AS mx
                FROM mdt_response_log
                WHERE cad = %s
                  AND LOWER(TRIM(status)) IN ('cleared', 'stood_down')
                GROUP BY callSign
            ) t ON t.callSign = l.callSign AND l.event_time = t.mx AND l.cad = %s
            ORDER BY l.callSign ASC
            """,
            (cad, cad),
        )
        rows = cur.fetchall() or []
        for r in rows:
            if not isinstance(r, dict):
                continue
            cs = str(r.get('callSign') or '').strip()
            if not cs:
                continue
            key = cs.upper()
            if key in seen or key in current:
                continue
            seen.add(key)
            detached_at = r.get('detached_at')
            detach_status = str(r.get('detach_status') or '').strip().lower()
            last_op = ''
            if detached_at is not None:
                cur.execute(
                    """
                    SELECT status FROM mdt_response_log
                    WHERE cad = %s AND callSign = %s AND event_time < %s
                      AND LOWER(TRIM(status)) NOT IN ('cleared', 'stood_down')
                    ORDER BY event_time DESC LIMIT 1
                    """,
                    (cad, cs, detached_at),
                )
                prow = cur.fetchone() or {}
                last_op = str(prow.get('status') or '').strip().lower()
            display_status = last_op if last_op else (
                detach_status or 'unknown')
            detached_iso = None
            try:
                if isinstance(detached_at, datetime):
                    detached_iso = detached_at.isoformat()
                elif detached_at:
                    detached_iso = str(detached_at)
            except Exception:
                detached_iso = str(detached_at) if detached_at else None
            out.append({
                'callsign': cs,
                'last_status': display_status,
                'detached_at': detached_iso,
                'detach_reason': detach_status,
            })
    except Exception:
        pass
    return out


def _collect_job_callsigns(cur, cad):
    callsigns = list(_get_job_unit_callsigns(cur, cad))
    cur.execute(
        "SELECT callSign FROM mdts_signed_on WHERE assignedIncident = %s", (cad,))
    rows = cur.fetchall() or []
    if rows and isinstance(rows[0], dict):
        callsigns.extend([str(r.get('callSign') or '').strip()
                         for r in rows if r.get('callSign')])
    else:
        callsigns.extend([str(r[0]).strip() for r in rows if r and r[0]])
    return list(dict.fromkeys([c for c in callsigns if c]))


def _extract_coords_from_job_data(payload):
    """Best-effort coordinates extraction from job JSON payload."""
    try:
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode('utf-8', errors='ignore')
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        if not isinstance(payload, dict):
            return (None, None, {})
        coords = payload.get('coordinates') or {}
        if not isinstance(coords, dict):
            coords = {}
        lat = coords.get('lat')
        lng = coords.get('lng')
        if lat is None and coords.get('latitude') is not None:
            lat = coords.get('latitude')
        if lng is None and coords.get('longitude') is not None:
            lng = coords.get('longitude')
        try:
            lat = float(lat) if lat is not None and lat != '' else None
            lng = float(lng) if lng is not None and lng != '' else None
        except Exception:
            lat = lng = None
        if lat is not None and lng is not None:
            if not (-90 <= lat <= 90 and -180 <= lng <= 180):
                lat = lng = None
        return (lat, lng, payload)
    except Exception:
        return (None, None, {})


def _resolve_job_lat_lng_for_map(payload):
    """Return lat/lng only from persisted job JSON (never call external geocoders here).

    Listing/polling endpoints must not hit what3words or other APIs; coordinates are
    resolved and stored on create/update-details (and standby flows) instead.
    """
    if not isinstance(payload, dict):
        return None, None
    lat, lng, _ = _extract_coords_from_job_data(payload)
    if lat is not None and lng is not None:
        return lat, lng
    return None, None


def _extract_required_skills(job_payload):
    keys = ['required_skills', 'requiredSkills',
            'skill_requirements', 'skills_required']
    for key in keys:
        val = job_payload.get(key)
        if isinstance(val, list):
            return {str(x).strip().lower() for x in val if str(x).strip()}
        if isinstance(val, str) and val.strip():
            return {v.strip().lower() for v in val.split(',') if v.strip()}
    return set()


def _extract_unit_skills(crew_payload):
    """Extract skills/grades from crew for job matching. Includes each member's grade/role as a skill."""
    skills = set()
    try:
        crew = crew_payload
        if isinstance(crew_payload, (bytes, bytearray)):
            crew = crew_payload.decode('utf-8', errors='ignore')
        if isinstance(crew, str):
            crew = json.loads(crew) if crew else []
        if not isinstance(crew, list):
            return skills
        for member in crew:
            if isinstance(member, str):
                continue
            if not isinstance(member, dict):
                continue
            grade = member.get('grade') or member.get('role')
            if grade:
                skills.add(str(grade).strip().lower())
            for key in ('skills', 'quals', 'qualifications', 'capabilities'):
                raw = member.get(key)
                if isinstance(raw, list):
                    skills.update(str(s).strip().lower()
                                  for s in raw if str(s).strip())
                elif isinstance(raw, str):
                    skills.update(s.strip().lower()
                                  for s in raw.split(',') if s.strip())
    except Exception:
        pass
    return skills


def _dispatch_skill_set_for_unit(cur, crew_payload):
    """Crew JSON tokens plus Training ``trn_person_competencies`` and HR job title per member."""
    base = _extract_unit_skills(crew_payload)
    try:
        from app.plugins.training_module.services import TrainingService

        if not TrainingService.person_competencies_table_exists():
            return base
        crew = crew_payload
        if isinstance(crew_payload, (bytes, bytearray)):
            crew = crew_payload.decode('utf-8', errors='ignore')
        if isinstance(crew, str):
            crew = json.loads(crew) if crew else []
        if not isinstance(crew, list):
            return base
        for member in crew:
            if isinstance(member, str):
                uname = str(member).strip()
            elif isinstance(member, dict):
                uname = str(member.get('username')
                            or member.get('user') or '').strip()
            else:
                continue
            if not uname:
                continue
            cid = _get_contractor_id_for_username(cur, uname)
            if not cid:
                continue
            jt = None
            try:
                cur.execute(
                    "SELECT job_title FROM tb_contractors WHERE id = %s LIMIT 1",
                    (cid,),
                )
                jr = cur.fetchone()
                if jr and jr.get('job_title'):
                    jt = str(jr.get('job_title')).strip() or None
            except Exception:
                pass
            base = TrainingService.dispatch_skill_tokens_for_contractor(
                cur, cid, jt, base)
        return base
    except Exception:
        return base


def _haversine_km(lat1, lon1, lat2, lon2):
    """Distance between 2 lat/lon points in kilometers."""
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * \
        math.cos(p2) * math.sin(dl / 2) ** 2
    return r * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

# 0) Divisions & crew grades


@internal.route('/api/mdt/divisions', methods=['GET', 'OPTIONS'])
def mdt_divisions():
    """Return list of available divisions/operating areas for MDT login screen."""
    if request.method == 'OPTIONS':
        return '', 200
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        items = _list_dispatch_divisions(cur, include_inactive=False)
        divisions = [
            {"value": x.get("slug", ""), "label": x.get("name", "")} for x in items]
        break_policy = _dispatch_break_policy_api_payload(cur)
        return _jsonify_safe({"divisions": divisions, "break_policy": break_policy}, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/crew-grades', methods=['GET', 'OPTIONS'])
def mdt_crew_grades():
    """Return list of crew grades/roles for MDT sign-on and add-crew dropdowns."""
    if request.method == 'OPTIONS':
        return '', 200
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        grades = _list_crew_grades(cur)
        return _jsonify_safe({"grades": grades}, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/crew-profiles', methods=['GET', 'OPTIONS'])
@login_required
def mdt_crew_profiles_list():
    """List crew profiles (optionally filter by username). For admin/HR to manage gender, skills, qualifications, contractor link."""
    if request.method == 'OPTIONS':
        return '', 200
    allowed = ["dispatcher", "admin", "superuser", "clinical_lead",
               "controller", "call_taker", "call_handler"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed:
        return jsonify({'error': 'Unauthorised'}), 403
    username = (request.args.get('username') or '').strip() or None
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_mdt_crew_profiles_table(cur)
        if username:
            cur.execute(
                "SELECT username, contractor_id, gender, skills_json, qualifications_json, profile_picture_path, updated_at FROM mdt_crew_profiles WHERE username = %s",
                (username,),
            )
            rows = cur.fetchall() or []
        else:
            cur.execute(
                "SELECT username, contractor_id, gender, skills_json, qualifications_json, profile_picture_path, updated_at FROM mdt_crew_profiles ORDER BY username"
            )
            rows = cur.fetchall() or []
        out = []
        for r in rows:
            skills = []
            qualifications = []
            try:
                if r.get('skills_json'):
                    skills = json.loads(r['skills_json']) if isinstance(
                        r['skills_json'], str) else (r['skills_json'] or [])
                if r.get('qualifications_json'):
                    qualifications = json.loads(r['qualifications_json']) if isinstance(
                        r['qualifications_json'], str) else (r['qualifications_json'] or [])
            except Exception:
                pass
            out.append({
                'username': r.get('username'),
                'contractor_id': r.get('contractor_id'),
                'gender': r.get('gender'),
                'skills': skills,
                'qualifications': qualifications,
                'profile_picture_path': r.get('profile_picture_path'),
                'updated_at': r.get('updated_at').isoformat() if r.get('updated_at') and hasattr(r.get('updated_at'), 'isoformat') else str(r.get('updated_at') or ''),
            })
        return _jsonify_safe({'profiles': out}, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/crew-profiles/<username>', methods=['GET', 'PUT', 'OPTIONS'])
@login_required
def mdt_crew_profile(username):
    """GET: read crew profile. PUT: optional link fields only (contractor_id, gender, profile picture). Skills and qualifications are owned by HR + Training sync — not writable from CAD."""
    if request.method == 'OPTIONS':
        return '', 200
    allowed = ["dispatcher", "admin", "superuser", "clinical_lead",
               "controller", "call_taker", "call_handler"]
    if not hasattr(current_user, 'role') or current_user.role.lower() not in allowed:
        return jsonify({'error': 'Unauthorised'}), 403
    uname = (username or '').strip()
    if not uname:
        return jsonify({'error': 'username required'}), 400
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_mdt_crew_profiles_table(cur)
        if request.method == 'GET':
            cur.execute(
                "SELECT username, contractor_id, gender, skills_json, qualifications_json, profile_picture_path, updated_at FROM mdt_crew_profiles WHERE username = %s",
                (uname,),
            )
            r = cur.fetchone()
            if not r:
                return jsonify({'error': 'Not found'}), 404
            skills = []
            qualifications = []
            try:
                if r.get('skills_json'):
                    skills = json.loads(r['skills_json']) if isinstance(
                        r['skills_json'], str) else (r['skills_json'] or [])
                if r.get('qualifications_json'):
                    qualifications = json.loads(r['qualifications_json']) if isinstance(
                        r['qualifications_json'], str) else (r['qualifications_json'] or [])
            except Exception:
                pass
            return _jsonify_safe({
                'username': r.get('username'),
                'contractor_id': r.get('contractor_id'),
                'gender': r.get('gender'),
                'skills': skills,
                'qualifications': qualifications,
                'profile_picture_path': r.get('profile_picture_path'),
                'updated_at': r.get('updated_at').isoformat() if r.get('updated_at') and hasattr(r.get('updated_at'), 'isoformat') else str(r.get('updated_at') or ''),
            }, 200)
        # PUT: contractor_id / gender / profile_picture_path only. skills_json & qualifications_json come from HR + Training.
        payload = request.get_json() or {}
        contractor_id = payload.get('contractor_id')
        if contractor_id is not None and contractor_id != '':
            try:
                contractor_id = int(contractor_id)
            except (TypeError, ValueError):
                contractor_id = None
        else:
            contractor_id = None
        gender = (str(payload.get('gender') or '').strip() or None)
        profile_picture_path = (
            str(payload.get('profile_picture_path') or '').strip() or None)
        cur.execute("""
            INSERT INTO mdt_crew_profiles (username, contractor_id, gender, skills_json, qualifications_json, profile_picture_path)
            VALUES (%s, %s, %s, NULL, NULL, %s)
            ON DUPLICATE KEY UPDATE
                contractor_id = COALESCE(VALUES(contractor_id), contractor_id),
                gender = COALESCE(VALUES(gender), gender),
                profile_picture_path = COALESCE(VALUES(profile_picture_path), profile_picture_path)
        """, (uname, contractor_id, gender, profile_picture_path))
        conn.commit()
        cid_sync = contractor_id
        if cid_sync is None:
            cur.execute(
                "SELECT contractor_id FROM mdt_crew_profiles WHERE username = %s LIMIT 1",
                (uname,),
            )
            r_sync = cur.fetchone()
            if r_sync and r_sync.get("contractor_id") is not None:
                try:
                    cid_sync = int(r_sync["contractor_id"])
                except (TypeError, ValueError):
                    cid_sync = None
        if cid_sync:
            try:
                from app.plugins.hr_module.services import sync_ventus_crew_profile_from_hr_training
                sync_ventus_crew_profile_from_hr_training(int(cid_sync))
            except Exception:
                pass
        return _jsonify_safe({'success': True, 'username': uname}, 200)
    finally:
        cur.close()
        conn.close()


# 1) Sign-On


@internal.route('/api/mdt/signOn', methods=['POST', 'OPTIONS'])
def mdt_sign_on():
    if request.method == 'OPTIONS':
        return '', 200

    payload = request.get_json() or {}
    callsign_raw = str(payload.get('callSign') or payload.get(
        'callsign') or '').strip().upper()
    crew_raw = payload.get('crew')  # may be str or list
    status = str(payload.get('status') or 'on_standby').strip().lower()
    if status == 'available':
        status = 'on_standby'
    division = _normalize_division(payload.get('division'), fallback='general')
    shift_start_raw = payload.get('shift_start') or payload.get(
        'shiftStartAt') or payload.get('shift_start_at')
    shift_end_raw = payload.get('shift_end') or payload.get(
        'shiftEndAt') or payload.get('shift_end_at')
    shift_hours_raw = payload.get('shift_hours') or payload.get(
        'shiftHours') or payload.get('shift_duration_hours')
    shift_duration_raw = payload.get(
        'shift_duration_minutes') or payload.get('shiftDurationMins')
    break_due_raw = (
        payload.get('break_due_after_minutes')
        or payload.get('breakDueAfterMins')
        or payload.get('meal_break_due_after_minutes')
    )

    # normalize crew into list of { username, grade? }; may be [ "user1" ] or [ { username, grade } ]
    crew_input = []
    if isinstance(crew_raw, str):
        crew_input = [crew_raw]
    elif isinstance(crew_raw, list):
        crew_input = crew_raw
    crew_for_validation = []
    crew_objs_for_storage = []
    now = datetime.utcnow()
    sign_on_iso = now.isoformat() + 'Z'
    for member in crew_input:
        if isinstance(member, dict):
            uname = str(member.get('username')
                        or member.get('user') or '').strip()
            if not uname:
                continue
            grade = str(member.get('grade') or member.get(
                'role') or '').strip() or None
            crew_for_validation.append(uname)
            crew_objs_for_storage.append(
                {'username': uname, 'signedOnAt': sign_on_iso, 'grade': grade})
        else:
            uname = str(member).strip()
            if uname:
                crew_for_validation.append(uname)
                crew_objs_for_storage.append(
                    {'username': uname, 'signedOnAt': sign_on_iso, 'grade': None})
    crew = crew_for_validation

    if not callsign_raw or not crew:
        return jsonify({'error': 'callSign (or callsign) and crew required'}), 400

    now = datetime.utcnow()
    shift_start_at = _parse_client_datetime(shift_start_raw) or now
    shift_end_at = _parse_client_datetime(shift_end_raw)
    shift_duration_mins = _parse_int(
        shift_duration_raw, fallback=None, min_value=0, max_value=24 * 60)
    if shift_duration_mins is None:
        shift_hours = _parse_int(
            shift_hours_raw, fallback=None, min_value=0, max_value=24)
        if shift_hours is not None:
            shift_duration_mins = int(shift_hours) * 60
    if shift_duration_mins is None and shift_end_at is not None:
        try:
            shift_duration_mins = max(
                0, int((shift_end_at - shift_start_at).total_seconds() // 60))
        except Exception:
            shift_duration_mins = None
    if shift_end_at is None and shift_duration_mins is not None:
        shift_end_at = shift_start_at + \
            timedelta(minutes=int(shift_duration_mins))
    break_due_after_mins = 240

    sign_lat_raw = payload.get('latitude')
    sign_lon_raw = payload.get('longitude')
    if sign_lat_raw is None:
        sign_lat_raw = payload.get('lat')
    if sign_lon_raw is None:
        sign_lon_raw = payload.get('lng')
    sign_lat_f = None
    sign_lon_f = None
    if sign_lat_raw is not None and sign_lon_raw is not None:
        try:
            la = float(sign_lat_raw)
            lo = float(sign_lon_raw)
            if -90 <= la <= 90 and -180 <= lo <= 180:
                sign_lat_f, sign_lon_f = la, lo
        except (TypeError, ValueError):
            pass

    mdt_session_id_for_response = None
    establish = False
    takeover_emit_full_signoff_cs_list = []
    takeover_emit_cads = set()
    takeover_emit_units_refresh_cs_list = []
    takeover_emit_previous_callsign = None
    takeover_emit_supersedes_sid = None
    takeover_supersede_emit_callsign_room = True
    jwt_sub_for_socket_room = None
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_mdts_signed_on_schema(cur)
        _ventus_set_g_break_policy(cur)
        if break_due_raw is not None and str(break_due_raw).strip() != '':
            _bd = _parse_int(
                break_due_raw, fallback=None, min_value=60, max_value=12 * 60)
            break_due_after_mins = (
                max(15, min(24 * 60, int(_bd)))
                if _bd is not None
                else _resolve_default_break_due_after_mins(cur, shift_duration_mins)
            )
        else:
            break_due_after_mins = _resolve_default_break_due_after_mins(
                cur, shift_duration_mins)
        _ensure_callsign_redirect_table(cur)
        callsign = _mdt_resolve_callsign(cur, callsign_raw)
        crew_objs_for_storage, crew_for_validation, crew = (
            _mdt_merge_sign_on_crew_payload_with_db(
                cur, callsign, crew_objs_for_storage, sign_on_iso))
        # Validate crew against contractor table (and mdt_crew_profiles): only allow valid crew members to sign on.
        invalid_crew, crew_err = _validate_crew_usernames(cur, crew)
        if invalid_crew:
            return jsonify({'error': crew_err or 'Invalid crew member(s)', 'unknown_crew': invalid_crew}), 400

        jwt_mu = getattr(g, 'mdt_user', None) or {}
        jwt_sub_for_socket_room = jwt_mu.get('id')
        jwt_username = str(jwt_mu.get('username') or '').strip().lower()
        if not jwt_username:
            return jsonify({'error': 'Unauthorized', 'message': 'Bearer JWT is required for MDT sign-on'}), 401
        crew_keys = {str(c).strip().lower() for c in crew_for_validation}
        if jwt_username not in crew_keys:
            return jsonify({
                'error': 'crew_mismatch',
                'message': 'The signed-in account must be listed in the crew list for this MDT.',
            }), 403

        # Default True: one JWT username = one active MDT session. Only explicit JSON false opts out (legacy).
        establish = payload.get('establish_mdt_device_session') is not False
        new_sid = None
        if establish:
            _ensure_mdt_user_mdt_session_table(cur)
            client_sid = (
                str(payload.get('mdt_session_id') or '').strip()
                or (request.headers.get('X-MDT-Session-Id') or '').strip()
            )
            terminate_other = bool(
                payload.get('terminate_other_mdt') or payload.get('force_mdt_takeover')
            )
            reason = str(payload.get('crew_change_reason') or '').strip()
            cur.execute(
                """
                SELECT session_id, callsign FROM mdt_user_mdt_session
                WHERE LOWER(TRIM(username)) = %s LIMIT 1
                """,
                (jwt_username,),
            )
            sess_row = cur.fetchone() or {}
            existing_sid = str(sess_row.get('session_id') or '').strip()
            existing_cs = str(sess_row.get('callsign') or '').strip().upper()
            if existing_sid:
                session_alive = bool(
                    existing_cs and _mdt_signed_on_row_has_username_in_crew(
                        cur, jwt_username, existing_cs))
                if not session_alive:
                    # User is not on that unit's crew (or the signed-on row is gone) — the device
                    # session row is stale. Always drop it so sign-on can proceed without a false 409
                    # ("attached to LAPTOP" while not on any active unit).
                    try:
                        cur.execute(
                            """
                            DELETE FROM mdt_user_mdt_session
                            WHERE LOWER(TRIM(username)) = %s
                            """,
                            (jwt_username,),
                        )
                    except Exception:
                        pass
                    existing_sid = ''
                    existing_cs = ''

            crew_elsewhere = _mdt_find_signed_on_callsigns_with_username_elsewhere(
                cur, jwt_username, callsign)

            reuse_ok = bool(
                existing_sid and client_sid and client_sid == existing_sid
                and existing_cs == callsign)
            conflict_session = bool(existing_sid) and not reuse_ok
            need_reason = conflict_session or bool(crew_elsewhere)

            detach_req = str(
                payload.get('detach_prior_device_session_callsign')
                or payload.get('release_session_attached_to_callsign')
                or '').strip().upper()
            if terminate_other and detach_req:
                if conflict_session and existing_cs:
                    if detach_req != str(existing_cs).strip().upper():
                        return jsonify({
                            'error': 'detach_callsign_mismatch',
                            'message': (
                                'The session you are ending does not match the server\'s record. '
                                'Cancel and try again, or refresh the login screen.'
                            ),
                        }), 400
                if crew_elsewhere:
                    wset = {
                        str(x or '').strip().upper()
                        for x in crew_elsewhere}
                    if detach_req not in wset:
                        return jsonify({
                            'error': 'detach_callsign_mismatch',
                            'message': (
                                'The unit you chose to leave is not the one blocking sign-on. '
                                'Cancel and try again.'
                            ),
                        }), 400

            if not terminate_other:
                if conflict_session:
                    return jsonify({
                        'error': 'mdt_session_active',
                        'message': (
                            'This login is already attached to another MDT session / call sign. '
                            'Use the same reason you would when removing a crew member to move here, '
                            'or sign out the other device first.'
                        ),
                        'other_callsign': existing_cs or None,
                    }), 409
                if crew_elsewhere:
                    return jsonify({
                        'error': 'crew_attached_elsewhere',
                        'message': (
                            'You are still listed on another unit\'s crew in CAD while signing on here. '
                            'Remove yourself from that unit first, or confirm with a crew change reason '
                            '(same options as removing a crew member on the MDT).'
                        ),
                        'other_callsign': crew_elsewhere[0],
                        'other_callsigns': crew_elsewhere,
                    }), 409
            elif need_reason and len(reason) < 3:
                return jsonify({
                    'error': 'crew_change_reason_required',
                    'message': (
                        'Enter a crew change reason (at least 3 characters) '
                        'to detach from the other session or unit crew.'
                    ),
                }), 400

            removal_reason_row = ''
            removed_uname = str(
                jwt_mu.get('username') or jwt_username or '').strip()
            if not removed_uname:
                removed_uname = jwt_username
            if terminate_other and need_reason:
                removal_reason_row = _normalize_mdt_crew_removal_reason_from_mdt(
                    reason)

            new_sid = None
            processed_units = set()

            if conflict_session and terminate_other:
                takeover_emit_supersedes_sid = existing_sid or None
                takeover_emit_previous_callsign = existing_cs or None
                try:
                    log_audit(
                        _mdt_audit_actor(),
                        'mdt_remote_session_takeover',
                        details={
                            'username': jwt_username,
                            'new_callsign': callsign,
                            'previous_callsign': existing_cs,
                            'crew_change_reason': reason[:2000],
                        },
                    )
                except Exception:
                    pass
                if existing_cs and existing_cs != callsign:
                    outcome, ac_takeover, takeover_row_cs = (
                        _mdt_takeover_exec_logged_crew_removal_from_unit(
                            cur,
                            existing_cs,
                            jwt_username,
                            removed_uname,
                            removal_reason_row,
                            callsign,
                            'mdt_remote_session_takeover',
                        ))
                    refresh_cs = takeover_row_cs or existing_cs
                    if outcome == 'full_signoff':
                        takeover_emit_full_signoff_cs_list.append(refresh_cs)
                        takeover_emit_cads |= set(ac_takeover or [])
                    elif outcome == 'partial':
                        takeover_supersede_emit_callsign_room = False
                        takeover_emit_units_refresh_cs_list.append(refresh_cs)
                        try:
                            cur.execute(
                                "DELETE FROM mdt_user_mdt_session WHERE LOWER(TRIM(username)) = %s",
                                (jwt_username,),
                            )
                        except Exception:
                            pass
                    else:
                        try:
                            cur.execute(
                                "DELETE FROM mdt_user_mdt_session WHERE LOWER(TRIM(username)) = %s",
                                (jwt_username,),
                            )
                        except Exception:
                            pass
                    processed_units.add(str(existing_cs).strip().upper())
                else:
                    try:
                        cur.execute(
                            "DELETE FROM mdt_user_mdt_session WHERE username = %s",
                            (jwt_username,),
                        )
                    except Exception:
                        pass

            if terminate_other and crew_elsewhere:
                for ocs in crew_elsewhere:
                    ocu = str(ocs or '').strip().upper()
                    if not ocu or ocu == callsign or ocu in processed_units:
                        continue
                    outcome, ac_takeover, takeover_row_cs = (
                        _mdt_takeover_exec_logged_crew_removal_from_unit(
                            cur,
                            ocu,
                            jwt_username,
                            removed_uname,
                            removal_reason_row,
                            callsign,
                            'mdt_remote_session_takeover_crew_elsewhere',
                        ))
                    refresh_cs = takeover_row_cs or ocu
                    if outcome == 'full_signoff':
                        takeover_emit_full_signoff_cs_list.append(refresh_cs)
                        takeover_emit_cads |= set(ac_takeover or [])
                    elif outcome == 'partial':
                        takeover_supersede_emit_callsign_room = False
                        takeover_emit_units_refresh_cs_list.append(refresh_cs)
                        try:
                            cur.execute(
                                "DELETE FROM mdt_user_mdt_session WHERE LOWER(TRIM(username)) = %s",
                                (jwt_username,),
                            )
                        except Exception:
                            pass
                    else:
                        try:
                            cur.execute(
                                "DELETE FROM mdt_user_mdt_session WHERE LOWER(TRIM(username)) = %s",
                                (jwt_username,),
                            )
                        except Exception:
                            pass
                    processed_units.add(ocu)

            if reuse_ok:
                new_sid = existing_sid
            elif conflict_session and terminate_other:
                new_sid = str(uuid.uuid4())
            elif terminate_other and crew_elsewhere:
                new_sid = str(uuid.uuid4())
            if not new_sid:
                new_sid = str(uuid.uuid4())
            mdt_session_id_for_response = new_sid

        try:
            cur.execute("""
                DELETE FROM mdts_signed_on
                WHERE COALESCE(lastSeenAt, signOnTime) < DATE_SUB(NOW(), INTERVAL 120 MINUTE)
                  AND assignedIncident IS NULL
                  AND COALESCE(cad_placeholder, 0) = 0
            """)
        except Exception:
            pass
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_division = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'shiftStartAt'")
        has_shift_start = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'shiftEndAt'")
        has_shift_end = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'shiftDurationMins'")
        has_shift_duration = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'breakDueAfterMins'")
        has_break_due = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'mealBreakTakenAt'")
        has_meal_break_taken = cur.fetchone() is not None
        cur.execute(
            "SHOW COLUMNS FROM mdts_signed_on LIKE 'mealBreakStartedAt'")
        has_meal_break_started = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'mealBreakUntil'")
        has_meal_break_until = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'lastLat'")
        has_last_lat = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'lastLon'")
        has_last_lon = cur.fetchone() is not None
        if has_division:
            cur.execute(
                """
                INSERT INTO mdts_signed_on
                  (callSign, ipAddress, status, crew, division, lastSeenAt, signOnTime)
                VALUES
                  (%s, %s, %s, %s, %s, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                  status           = VALUES(status),
                  crew             = VALUES(crew),
                  division         = VALUES(division),
                  lastSeenAt       = NOW(),
                  assignedIncident = NULL
                """,
                (
                    callsign,
                    request.headers.get('X-Forwarded-For',
                                        request.remote_addr or ''),
                    status,
                    json.dumps(crew_objs_for_storage),
                    division
                )
            )
        else:
            cur.execute(
                """
                INSERT INTO mdts_signed_on
                  (callSign, ipAddress, status, crew, lastSeenAt, signOnTime)
                VALUES
                  (%s, %s, %s, %s, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                  status           = VALUES(status),
                  crew             = VALUES(crew),
                  lastSeenAt       = NOW(),
                  assignedIncident = NULL
                """,
                (
                    callsign,
                    request.headers.get('X-Forwarded-For',
                                        request.remote_addr or ''),
                    status,
                    json.dumps(crew_objs_for_storage)
                )
            )
        # Do not reset signOnTime on duplicate updates (MDT heartbeats / post-rename signOn);
        # backfill only when still NULL (legacy rows).
        try:
            cur.execute(
                "UPDATE mdts_signed_on SET signOnTime = NOW() WHERE callSign = %s AND signOnTime IS NULL",
                (callsign,),
            )
        except Exception:
            pass
        post_set = []
        post_args = []
        if has_shift_start:
            post_set.append("shiftStartAt = %s")
            post_args.append(shift_start_at)
        if has_shift_end:
            post_set.append("shiftEndAt = %s")
            post_args.append(shift_end_at)
        if has_shift_duration:
            post_set.append("shiftDurationMins = %s")
            post_args.append(shift_duration_mins)
        if has_break_due:
            post_set.append("breakDueAfterMins = %s")
            post_args.append(break_due_after_mins)
        if has_meal_break_taken:
            post_set.append("mealBreakTakenAt = NULL")
        if has_meal_break_started:
            post_set.append("mealBreakStartedAt = NULL")
        if has_meal_break_until:
            post_set.append("mealBreakUntil = NULL")
        if sign_lat_f is not None and sign_lon_f is not None and has_last_lat and has_last_lon:
            post_set.append("lastLat = %s")
            post_args.append(sign_lat_f)
            post_set.append("lastLon = %s")
            post_args.append(sign_lon_f)
        if post_set:
            post_args.append(callsign)
            cur.execute(
                f"UPDATE mdts_signed_on SET {', '.join(post_set)} WHERE callSign = %s",
                tuple(post_args)
            )
        try:
            cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'cad_placeholder'")
            if cur.fetchone() and (crew_objs_for_storage or []):
                cur.execute(
                    "UPDATE mdts_signed_on SET cad_placeholder = 0 WHERE callSign = %s",
                    (callsign,),
                )
        except Exception:
            pass
        if establish and mdt_session_id_for_response:
            _ensure_mdt_user_mdt_session_table(cur)
            cur.execute(
                """
                INSERT INTO mdt_user_mdt_session (username, session_id, callsign)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    session_id = VALUES(session_id),
                    callsign = VALUES(callsign),
                    updated_at = NOW()
                """,
                (jwt_username, mdt_session_id_for_response, callsign),
            )
        _ccc = str(payload.get('crew_change_reason') or '').strip()
        _record_mdt_session_lifecycle_event(
            cur,
            event_type='sign_on',
            callsign=callsign,
            division=division,
            actor_username=jwt_username,
            previous_callsign=(
                str(takeover_emit_previous_callsign).strip().upper()
                if takeover_emit_previous_callsign else None),
            crew_usernames=crew,
            detail={
                'callsign_requested': callsign_raw,
                'terminate_other_mdt': bool(
                    establish and (
                        payload.get('terminate_other_mdt')
                        or payload.get('force_mdt_takeover'))),
                'crew_change_reason': (_ccc[:500] if _ccc else None),
            },
        )
        conn.commit()
        try:
            for toff_cs in takeover_emit_full_signoff_cs_list:
                if toff_cs:
                    socketio.emit(
                        'mdt_event', {'type': 'unit_signoff', 'callsign': toff_cs})
                    socketio.emit(
                        'mdt_event', {'type': 'units_updated', 'callsign': toff_cs})
            for cad in sorted(takeover_emit_cads):
                socketio.emit(
                    'mdt_event', {'type': 'jobs_updated', 'cad': cad})
            for uucs in takeover_emit_units_refresh_cs_list:
                if uucs:
                    socketio.emit(
                        'mdt_event',
                        {'type': 'units_updated', 'callsign': uucs},
                    )
            sup_sid = takeover_emit_supersedes_sid
            _toff_first = (
                takeover_emit_full_signoff_cs_list[0]
                if takeover_emit_full_signoff_cs_list else None)
            if sup_sid:
                _sup_payload = {
                    'type': 'mdt_session_superseded',
                    'supersedes_session_id': sup_sid,
                    'new_callsign': callsign,
                    'previous_callsign': (
                        takeover_emit_previous_callsign or _toff_first),
                }
                if mdt_session_id_for_response:
                    _sup_payload['new_mdt_session_id'] = mdt_session_id_for_response
                if jwt_sub_for_socket_room is not None:
                    socketio.emit(
                        'mdt_event',
                        _sup_payload,
                        room=f"mdt_user_{jwt_sub_for_socket_room}",
                    )
                # Backup delivery: losing MDT may not share the same JWT `sub` room key as sign-on
                # (type mismatch) but always joins mdt_callsign_<theirCallSign> from socket auth.
                _prev_cs_emit = (
                    takeover_emit_previous_callsign or _toff_first or '').strip()
                # Partial crew takeover: other members stay on this call sign — do not broadcast
                # session supersede to mdt_callsign_* (only the JWT user's room needs the event).
                if _prev_cs_emit and takeover_supersede_emit_callsign_room:
                    _cs_room = re.sub(
                        r'[^A-Za-z0-9_-]', '', str(_prev_cs_emit).upper())[:64]
                    if _cs_room:
                        socketio.emit(
                            'mdt_event',
                            _sup_payload,
                            room=f"mdt_callsign_{_cs_room}",
                        )
        except Exception:
            pass
        log_audit(
            _mdt_audit_actor(),
            'mdt_sign_on',
            details={
                'callsign': callsign,
                'division': division,
                'status': status,
                'crew_count': len(crew),
            },
        )
    except Exception as e:
        takeover_emit_full_signoff_cs_list = []
        takeover_emit_cads = set()
        takeover_emit_units_refresh_cs_list = []
        takeover_emit_previous_callsign = None
        takeover_emit_supersedes_sid = None
        takeover_supersede_emit_callsign_room = True
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()

    try:
        from app.plugins.time_billing_module.ventus_integration import on_ventus_sign_on
        on_ventus_sign_on(
            callsign=callsign,
            crew=crew,
            shift_start_at=shift_start_at,
            shift_end_at=shift_end_at,
            sign_on_time=now,
        )
    except Exception:
        pass

    try:
        shift_preview = _compute_shift_break_state({
            'status': status,
            'shiftStartAt': shift_start_at,
            'shiftEndAt': shift_end_at,
            'shiftDurationMins': shift_duration_mins,
            'breakDueAfterMins': break_due_after_mins,
            'mealBreakTakenAt': None,
            'mealBreakStartedAt': None,
            'mealBreakUntil': None
        }, now=datetime.utcnow())
        socketio.emit('mdt_event', {
            'type': 'unit_signon',
            'callsign': callsign,
            'status': status,
            'division': division,
            'shift_start_at': shift_preview.get('shift_start_at'),
            'shift_end_at': shift_preview.get('shift_end_at'),
            'break_due_after_minutes': shift_preview.get('break_due_after_minutes')
        })
        socketio.emit(
            'mdt_event', {'type': 'units_updated', 'callsign': callsign})
    except Exception:
        pass

    shift_hours = (float(shift_duration_mins) /
                   60.0) if shift_duration_mins is not None else None
    sign_on_out = {
        'message': 'Signed on',
        'callsign': callsign,
        'division': division,
        'status': status,
        'shift_start': shift_start_at.isoformat() + 'Z' if shift_start_at and hasattr(shift_start_at, 'isoformat') else None,
        'shift_end': shift_end_at.isoformat() + 'Z' if shift_end_at and hasattr(shift_end_at, 'isoformat') else None,
        'shift_hours': shift_hours,
        'shift_duration_minutes': shift_duration_mins,
        'break_due_after_minutes': break_due_after_mins
    }
    if callsign_raw != callsign:
        sign_on_out['requestedCallSign'] = callsign_raw
        sign_on_out['canonicalCallSign'] = callsign
    if mdt_session_id_for_response:
        sign_on_out['mdt_session_id'] = mdt_session_id_for_response
    return _jsonify_safe(sign_on_out, 200)

# 2) Sign-Off


def _mdt_session_stale_for_auto_signoff(row, now=None):
    """
    True when a signed-on row should be purged on MDT status poll: no active CAD assignment and
    either shift end + grace has passed, or lastSeen/signOn + idle grace has passed.
    Tunable: VENTUS_MDT_STALE_SHIFT_GRACE_HOURS (default 36), VENTUS_MDT_STALE_IDLE_HOURS (default 96).
    """
    now = now or datetime.utcnow()
    incident = row.get('assignedIncident')
    if incident is not None and str(incident).strip() != '':
        return False
    # If the unit has heartbeated recently, never treat it as stale here. Wrong shift_end / TZ
    # data would otherwise purge the row on the first GET /status right after sign-on or takeover,
    # which kicks the MDT back to login with session_expired.
    last_seen = _coerce_datetime(row.get('lastSeenAt'))
    if last_seen is not None:
        try:
            if last_seen + timedelta(minutes=10) > now:
                return False
        except Exception:
            pass
    try:
        shift_grace = float(os.environ.get('VENTUS_MDT_STALE_SHIFT_GRACE_HOURS', '36'))
    except (TypeError, ValueError):
        shift_grace = 36.0
    try:
        idle_grace = float(os.environ.get('VENTUS_MDT_STALE_IDLE_HOURS', '96'))
    except (TypeError, ValueError):
        idle_grace = 96.0
    shift_end = _coerce_datetime(row.get('shiftEndAt'))
    if shift_end is not None:
        try:
            if shift_end + timedelta(hours=shift_grace) < now:
                return True
        except Exception:
            pass
    last_v = _coerce_datetime(row.get('lastSeenAt') or row.get('signOnTime'))
    if last_v is not None:
        try:
            if last_v + timedelta(hours=idle_grace) < now:
                return True
        except Exception:
            pass
    return False


def _mdt_sign_off_unit_sql(cur, callsign):
    """
    Remove a unit from mdts_signed_on / mdt_job_units / standby_locations and resync per-CAD queue state.
    Caller must ensure tables exist (_ensure_job_units_table, etc.) and commit/rollback the connection.
    Returns (affected_cads set, sign_on_time).
    """
    affected_cads = set()
    sign_on_time = None
    cur.execute(
        "SELECT assignedIncident, signOnTime FROM mdts_signed_on WHERE callSign = %s", (callsign,))
    for dr in (cur.fetchall() or []):
        cad = dr.get('assignedIncident')
        try:
            if cad is not None:
                affected_cads.add(int(cad))
        except Exception:
            pass
        if sign_on_time is None and dr.get('signOnTime') is not None:
            sign_on_time = dr.get('signOnTime')

    cur.execute(
        "SELECT job_cad FROM mdt_job_units WHERE callsign = %s", (callsign,))
    for dr in (cur.fetchall() or []):
        cad = dr.get('job_cad')
        try:
            if cad is not None:
                affected_cads.add(int(cad))
        except Exception:
            pass

    cur.execute(
        "DELETE FROM mdt_job_units WHERE callsign = %s", (callsign,))
    cur.execute(
        "DELETE FROM mdts_signed_on WHERE callSign = %s", (callsign,))
    try:
        _ensure_standby_tables(cur)
        cur.execute(
            "DELETE FROM standby_locations WHERE callSign = %s", (callsign,))
    except Exception:
        pass

    for cad in sorted(affected_cads):
        remaining = _sync_claimed_by_from_job_units(cur, cad)
        cur.execute(
            "SELECT LOWER(TRIM(COALESCE(status, ''))) AS status FROM mdt_jobs WHERE cad = %s LIMIT 1", (cad,))
        jr = cur.fetchone() or {}
        st = str(jr.get('status') or '').strip().lower()
        if st not in _JOB_STATUSES_SKIP_EMPTY_UNLINK_QUEUE and len(remaining) == 0:
            cur.execute(
                "UPDATE mdt_jobs SET status = 'queued' WHERE cad = %s AND LOWER(TRIM(COALESCE(status, ''))) <> 'cleared'",
                (cad,),
            )

    return affected_cads, sign_on_time


@internal.route('/api/mdt/signed-on-units', methods=['GET', 'OPTIONS'])
def mdt_signed_on_units_list():
    """
    JWT-only (no device session yet): list active signed-on vehicles for the MDT join screen.
    Same freshness window as KPI active-units (last seen / sign-on within 120 minutes).
    """
    if request.method == 'OPTIONS':
        return '', 200
    jwt_mu = getattr(g, 'mdt_user', None) or {}
    jwt_username = str(jwt_mu.get('username') or '').strip()
    if not jwt_username:
        return jsonify({
            'error': 'Unauthorized',
            'message': 'Bearer JWT required',
        }), 401

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_mdts_signed_on_schema(cur)
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_division = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'shiftStartAt'")
        has_shift_start = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'shiftEndAt'")
        has_shift_end = cur.fetchone() is not None

        if has_division:
            div_sel = "LOWER(TRIM(COALESCE(m.division, 'general'))) AS division"
        else:
            div_sel = "'general' AS division"
        if has_shift_start and has_shift_end:
            shift_sel = "m.shiftStartAt, m.shiftEndAt"
        else:
            shift_sel = "NULL AS shiftStartAt, NULL AS shiftEndAt"

        cur.execute(
            f"""
            SELECT m.callSign, m.crew, m.status, {div_sel}, {shift_sel},
                   m.lastSeenAt, m.signOnTime
            FROM mdts_signed_on m
            WHERE COALESCE(m.lastSeenAt, m.signOnTime) >= DATE_SUB(NOW(), INTERVAL 120 MINUTE)
            ORDER BY m.callSign ASC
            """
        )
        rows = cur.fetchall() or []
        out = []

        def _iso_dt(v):
            if v is None:
                return None
            if hasattr(v, 'isoformat'):
                try:
                    s = v.isoformat()
                    return s + 'Z' if getattr(v, 'tzinfo', None) is None and not s.endswith('Z') else s
                except Exception:
                    return str(v)
            return str(v)

        for r in rows:
            cs = str(r.get('callSign') or '').strip().upper()
            if not cs:
                continue
            raw_crew = r.get('crew')
            try:
                parsed = _mdt_parse_signed_on_crew_list(raw_crew)
                crew_objs = _normalize_crew_to_objects(
                    parsed, r.get('signOnTime'))
                crew_enriched = _enrich_crew_with_profiles(cur, crew_objs)
            except Exception:
                crew_enriched = []
            out.append({
                'callsign': cs,
                'status': str(r.get('status') or '').strip() or None,
                'division': str(r.get('division') or 'general').strip().lower() or 'general',
                'crew': crew_enriched,
                'shift_start': _iso_dt(r.get('shiftStartAt')) if has_shift_start else None,
                'shift_end': _iso_dt(r.get('shiftEndAt')) if has_shift_end else None,
            })
        return _jsonify_safe({'units': out}, 200)
    except Exception as e:
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/signOff', methods=['POST', 'OPTIONS'])
def mdt_sign_off():
    if request.method == 'OPTIONS':
        return '', 200

    payload = request.get_json() or {}
    callsign_raw = str(payload.get('callSign') or payload.get(
        'callsign') or '').strip().upper()
    if not callsign_raw:
        return jsonify({'error': 'callSign required'}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    affected_cads = set()
    sign_on_time = None
    crew_removals_summary = []
    try:
        _ensure_callsign_redirect_table(cur)
        callsign = _mdt_resolve_callsign(cur, callsign_raw)
        _ensure_job_units_table(cur)
        _ensure_crew_removal_log_table(cur)

        crew_users = []
        try:
            cur.execute(
                "SELECT crew FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
                (callsign,),
            )
            crew_before = cur.fetchone() or {}
            crew_users = list(_mdt_signed_on_crew_usernames(crew_before.get('crew')))
        except Exception:
            crew_users = []

        _ensure_mdt_user_mdt_session_table(cur)
        # Only revoke device sessions bound to *this* call sign. A user may already have re-signed
        # onto another unit (same JWT); unscoped DELETE BY username would wipe their new session when
        # the old unit signs off or the losing MDT posts signOff — both devices then hit 403.
        cs_sess = str(callsign or '').strip().upper()
        for u in crew_users:
            try:
                cur.execute(
                    """
                    DELETE FROM mdt_user_mdt_session
                    WHERE LOWER(TRIM(username)) = %s AND UPPER(TRIM(callsign)) = %s
                    """,
                    (u, cs_sess),
                )
            except Exception:
                pass
        try:
            cur.execute(
                "DELETE FROM mdt_user_mdt_session WHERE UPPER(TRIM(callsign)) = %s",
                (cs_sess,),
            )
        except Exception:
            pass
        jwt_mu = getattr(g, 'mdt_user', None) or {}
        jwt_un = str(jwt_mu.get('username') or '').strip().lower()
        if jwt_un:
            try:
                cur.execute(
                    """
                    DELETE FROM mdt_user_mdt_session
                    WHERE LOWER(TRIM(username)) = %s AND UPPER(TRIM(callsign)) = %s
                    """,
                    (jwt_un, cs_sess),
                )
            except Exception:
                pass

        div_log = 'general'
        try:
            cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
            if cur.fetchone():
                cur.execute(
                    """
                    SELECT LOWER(TRIM(COALESCE(division, 'general'))) AS d
                    FROM mdts_signed_on WHERE callSign = %s LIMIT 1
                    """,
                    (callsign,),
                )
                drd = cur.fetchone() or {}
                div_log = str(drd.get('d') or 'general').strip().lower() or 'general'
        except Exception:
            pass

        affected_cads, sign_on_time = _mdt_sign_off_unit_sql(cur, callsign)
        _record_mdt_session_lifecycle_event(
            cur,
            event_type='sign_off',
            callsign=callsign,
            division=div_log,
            actor_username=jwt_un or None,
            previous_callsign=None,
            crew_usernames=crew_users,
            detail={'affected_cads': sorted(affected_cads)},
        )

        try:
            cur.execute("""
                SELECT removed_username, removal_reason, removed_by, removed_at
                FROM mdt_crew_removal_log
                WHERE callSign = %s
                  AND removed_at >= COALESCE(%s, DATE_SUB(NOW(), INTERVAL 7 DAY))
                ORDER BY removed_at ASC
            """, (callsign, sign_on_time))
            crew_removals_summary = cur.fetchall() or []
        except Exception:
            crew_removals_summary = []

        try:
            cur.execute(
                "DELETE FROM mdt_callsign_redirect WHERE from_callsign = %s OR to_callsign = %s",
                (callsign, callsign),
            )
        except Exception:
            pass

        if not log_audit(
            _mdt_audit_actor(),
            'mdt_sign_off',
            details={
                'callsign': callsign,
                'affected_cads': sorted(affected_cads),
            },
            audit_failure='fail_closed',
        ):
            conn.rollback()
            return _ventus_audit_unavailable_response()
        conn.commit()
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()

    try:
        from app.plugins.time_billing_module.ventus_integration import on_ventus_sign_off
        on_ventus_sign_off(callsign=callsign)
    except Exception:
        pass

    try:
        socketio.emit(
            'mdt_event', {'type': 'unit_signoff', 'callsign': callsign})
        socketio.emit(
            'mdt_event', {'type': 'units_updated', 'callsign': callsign})
        for cad in sorted(affected_cads):
            socketio.emit('mdt_event', {'type': 'jobs_updated', 'cad': cad})
    except Exception:
        pass

    return _jsonify_safe({'message': 'Signed off', 'crew_changes': crew_removals_summary})


@internal.route('/api/mdt/callsign', methods=['GET', 'POST', 'OPTIONS'])
def mdt_query_callsign():
    """
    Resolve the callSign the device is using to the current signed-on row (after CAD rename).
    MDT apps should call this on resume / periodically and replace stored callSign when redirected.
    """
    if request.method == 'OPTIONS':
        return '', 200
    if request.method == 'GET':
        raw = request.args.get(
            'callSign') or request.args.get('callsign') or ''
    else:
        body = request.get_json(silent=True) or {}
        raw = body.get('callSign') or body.get('callsign') or ''
    raw_cs = str(raw or '').strip().upper()
    if not raw_cs:
        return jsonify({'error': 'callSign (or callsign) required'}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        canonical = _mdt_resolve_callsign(cur, raw_cs)
        cur.execute(
            "SELECT callSign FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (canonical,))
        signed_on = cur.fetchone() is not None
        out = {
            'message': 'OK',
            'callSign': canonical,
            'callsign': canonical,
            'requestedCallSign': raw_cs,
            'signedOn': signed_on,
            'redirected': raw_cs != canonical,
        }
        out.update(_mdt_callsign_client_hints(raw_cs, canonical))
        if raw_cs != canonical:
            out['message'] = 'Callsign was updated by dispatch; use callSign for URLs and payloads'
        return jsonify(out), 200
    finally:
        cur.close()
        conn.close()

# 3) Next job


@internal.route('/api/mdt/next', methods=['GET', 'OPTIONS'])
def mdt_next():
    if request.method == 'OPTIONS':
        return '', 200

    callsign_raw = _normalize_callsign(args=request.args)
    if not callsign_raw:
        return '', 204

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        callsign = _mdt_resolve_callsign(cur, callsign_raw)
        mode = _get_dispatch_mode(cur)
        _ensure_meal_break_columns(cur)
        _ventus_set_g_break_policy(cur)

        # Return existing assignment if present, but self-heal stale pointers.
        cur.execute(
            "SELECT assignedIncident FROM mdts_signed_on WHERE callSign = %s ORDER BY signOnTime DESC LIMIT 1",
            (callsign,)
        )
        row = cur.fetchone() or {}
        assigned_cad = row.get('assignedIncident')
        valid_current = False
        if assigned_cad:
            try:
                assigned_cad = int(assigned_cad)
            except Exception:
                assigned_cad = None
        if assigned_cad:
            cur.execute(
                "SELECT LOWER(TRIM(COALESCE(status, ''))) AS status FROM mdt_jobs WHERE cad = %s LIMIT 1",
                (assigned_cad,)
            )
            jrow = cur.fetchone() or {}
            jstatus = str(jrow.get('status') or '').strip().lower()
            if jstatus and jstatus not in ('cleared', 'stood_down'):
                cur.execute("SHOW TABLES LIKE 'mdt_job_units'")
                has_links = cur.fetchone() is not None
                if has_links:
                    cur.execute(
                        "SELECT 1 FROM mdt_job_units WHERE job_cad = %s AND callsign = %s LIMIT 1",
                        (assigned_cad, callsign)
                    )
                    valid_current = cur.fetchone() is not None
                else:
                    valid_current = True
        if valid_current and assigned_cad:
            return jsonify({'cad': assigned_cad}), 200

        # Attempt to recover latest active assignment from job-unit links.
        recovered_cad = None
        cur.execute("SHOW TABLES LIKE 'mdt_job_units'")
        if cur.fetchone() is not None:
            cur.execute("""
                SELECT ju.job_cad
                FROM mdt_job_units ju
                JOIN mdt_jobs j ON j.cad = ju.job_cad
                WHERE ju.callsign = %s
                  AND LOWER(TRIM(COALESCE(j.status, ''))) NOT IN ('cleared', 'stood_down')
                ORDER BY ju.assigned_at DESC, ju.id DESC
                LIMIT 1
            """, (callsign,))
            rec = cur.fetchone() or {}
            recovered_cad = rec.get('job_cad')
            if recovered_cad:
                try:
                    recovered_cad = int(recovered_cad)
                except Exception:
                    recovered_cad = None
        if recovered_cad:
            cur.execute(
                "UPDATE mdts_signed_on SET assignedIncident = %s WHERE callSign = %s",
                (recovered_cad, callsign)
            )
            return jsonify({'cad': recovered_cad}), 200

        # No active assignment remains; clear stale pointer if present.
        if assigned_cad:
            cur.execute(
                "UPDATE mdts_signed_on SET assignedIncident = NULL WHERE callSign = %s",
                (callsign,)
            )

        # Manual mode: dispatcher assigns explicitly; do not advertise general queue.
        if mode == 'manual':
            return '', 204

        # Auto mode: pick best queued job by skill match and distance.
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_unit_div = cur.fetchone() is not None
        unit_div_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_unit_div else "'general' AS division"
        cur.execute("""
            SELECT callSign,
                   LOWER(TRIM(COALESCE(status, ''))) AS status,
                   lastLat,
                   lastLon,
                   crew,
                   signOnTime,
                   mealBreakStartedAt,
                   mealBreakUntil,
                   mealBreakTakenAt,
                   shiftStartAt,
                   shiftEndAt,
                   shiftDurationMins,
                   breakDueAfterMins,
                   {unit_div_sql}
            FROM mdts_signed_on
            WHERE callSign = %s
            ORDER BY signOnTime DESC
            LIMIT 1
        """.format(unit_div_sql=unit_div_sql), (callsign,))
        unit = cur.fetchone()
        if not unit:
            return '', 204
        if not _unit_crew_has_members(unit.get('crew')):
            return '', 204

        unit_status = (unit.get('status') or '').strip().lower()
        if unit_status not in ('on_standby', 'on_station', 'at_station', 'available', 'cleared', 'stood_down'):
            return '', 204

        unit_lat = unit.get('lastLat')
        unit_lon = unit.get('lastLon')
        try:
            unit_lat = float(unit_lat) if unit_lat is not None else None
            unit_lon = float(unit_lon) if unit_lon is not None else None
        except Exception:
            unit_lat = unit_lon = None
        unit_skills = _dispatch_skill_set_for_unit(cur, unit.get('crew'))
        unit_division = _normalize_division(
            unit.get('division'), fallback='general')
        shift_state = _compute_shift_break_state(unit)
        break_blocked = bool(shift_state.get('break_blocked_for_new_jobs'))

        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_job_div = cur.fetchone() is not None
        if has_job_div:
            cur.execute(
                "SELECT cad, data, created_at, LOWER(TRIM(COALESCE(division, 'general'))) AS division FROM mdt_jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 500"
            )
        else:
            cur.execute(
                "SELECT cad, data, created_at, 'general' AS division FROM mdt_jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 500"
            )
        jobs = cur.fetchall() or []
        if not jobs:
            return '', 204

        candidates = []
        for job in jobs:
            job_division = _normalize_division(
                job.get('division'), fallback='general')
            if unit_division and job_division != unit_division:
                continue
            lat, lng, payload = _extract_coords_from_job_data(job.get('data'))
            priority_value = _extract_job_priority_value(payload)
            priority_rank = _priority_rank(priority_value)
            allow_break_override = _is_high_priority_value(priority_value)
            if break_blocked and not allow_break_override:
                continue
            required_skills = _extract_required_skills(payload)
            if required_skills and not required_skills.issubset(unit_skills):
                continue
            if unit_lat is not None and unit_lon is not None and lat is not None and lng is not None:
                dist_km = _haversine_km(unit_lat, unit_lon, lat, lng)
                has_dist = 0
            else:
                dist_km = 10**9
                has_dist = 1
            candidates.append(
                (priority_rank, has_dist, dist_km, job.get('created_at') or datetime.utcnow(), int(job['cad'])))

        if not candidates:
            return '', 204

        candidates.sort(key=lambda x: (x[0], x[1], x[2], x[3], x[4]))
        return jsonify({'cad': candidates[0][4]}), 200
    finally:
        cur.close()
        conn.close()

# 4) Claim


@internal.route('/api/mdt/<int:cad>/claim', methods=['POST', 'OPTIONS'])
def mdt_claim(cad):
    if request.method == 'OPTIONS':
        return '', 200

    # Only read callsign from query parameters
    callsign_raw = (request.args.get('callSign')
                    or request.args.get('callsign') or '').strip().upper()
    if not callsign_raw:
        return jsonify({'error': 'callSign is required'}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        _ensure_callsign_redirect_table(cur)
        callsign = _mdt_resolve_callsign(cur, callsign_raw)
        _ensure_job_units_table(cur)

        # Fast path: claim direct from queued for auto-dispatch scenarios.
        cur.execute(
            """
            UPDATE mdt_jobs
               SET status = 'claimed'
             WHERE cad = %s AND status = 'queued'
            """,
            (cad,)
        )
        if cur.rowcount == 1:
            cur.execute("""
                INSERT INTO mdt_job_units (job_cad, callsign, assigned_by)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    assigned_by = VALUES(assigned_by),
                    assigned_at = CURRENT_TIMESTAMP
            """, (cad, callsign, 'mdt_claim'))
            _sync_claimed_by_from_job_units(cur, cad)
        else:
            # Manual-dispatch path: only assigned units may claim assigned incidents.
            cur.execute(
                """
                SELECT 1
                FROM mdt_jobs j
                JOIN mdt_job_units ju ON ju.job_cad = j.cad
                WHERE j.cad = %s
                  AND j.status = 'assigned'
                  AND ju.callsign = %s
                LIMIT 1
                """,
                (cad, callsign)
            )
            allowed = cur.fetchone()
            if not allowed:
                conn.rollback()
                return jsonify({'error': 'Job already claimed or not assigned to this unit'}), 409
            cur.execute(
                "UPDATE mdt_jobs SET status = 'claimed' WHERE cad = %s AND status = 'assigned'",
                (cad,)
            )
            _sync_claimed_by_from_job_units(cur, cad)

        # Assign to callsign only; MDT must explicitly ACK receipt by posting
        # status='received' when the crew device has shown the job.
        cur.execute(
            """
            UPDATE mdts_signed_on
               SET assignedIncident = %s,
                   status           = 'assigned'
             WHERE callSign = %s
            """,
            (cad, callsign)
        )

        # Incident updates are on the job record / inbox — do not inject into crew `messages`.

        conn.commit()
        log_audit(
            _mdt_audit_actor(),
            'mdt_claim',
            details={'cad': cad, 'callsign': callsign},
        )
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()

    return jsonify({'message': 'Job claimed'}), 200


# 5) History
@internal.route('/api/mdt/history', methods=['GET', 'POST', 'OPTIONS'])
def mdt_history():
    if request.method == 'OPTIONS':
        return '', 200

    if request.method == 'GET':
        callsign = _normalize_callsign(args=request.args)
        cads = request.args.getlist('cad', type=int)
    else:
        body = request.get_json() or {}
        callsign = body.get('callSign') or body.get('callsign') or ''
        cads = body.get('cads', [])

    if not callsign:
        return jsonify({'error': 'callSign required'}), 400
    if not cads:
        return jsonify([]), 200

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        callsign = _mdt_resolve_callsign(
            cur, str(callsign or '').strip().upper())
        placeholders = ",".join(["%s"] * len(cads))
        sql = f"""
          SELECT cad, status, event_time
          FROM mdt_response_log
          WHERE callSign = %s
            AND cad IN ({placeholders})
          ORDER BY cad ASC, event_time ASC
        """
        cur.execute(sql, [callsign] + cads)
        raw_rows = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    by_cad = {}
    for row in raw_rows:
        try:
            cad_val = int(row.get('cad'))
        except Exception:
            continue
        by_cad.setdefault(cad_val, []).append(row)

    result = []
    for cad in cads:
        try:
            cad_int = int(cad)
        except Exception:
            continue
        events = by_cad.get(cad_int, [])
        if not events:
            continue

        # New cycle starts at the latest explicit (re)assignment/receive marker.
        start_idx = 0
        for i, ev in enumerate(events):
            st = str(ev.get('status') or '').strip().lower()
            if st in ('assigned', 'received'):
                start_idx = i
        cycle = events[start_idx:] if events else []

        def _latest_time(status_key):
            t = None
            for ev in cycle:
                if str(ev.get('status') or '').strip().lower() == status_key:
                    t = ev.get('event_time')
            return t

        result.append({
            'cad': cad_int,
            'received_time': _latest_time('received'),
            'assigned_time': _latest_time('assigned'),
            'mobile_time': _latest_time('mobile'),
            'on_scene_time': _latest_time('on_scene'),
            'leave_scene_time': _latest_time('leave_scene'),
            'at_hospital_time': _latest_time('at_hospital'),
            'cleared_time': _latest_time('cleared'),
            'stood_down_time': _latest_time('stood_down'),
            'closure_review_time': _latest_time(_RESPONSE_LOG_STATUS_CLOSURE_REVIEW),
            'dispatch_reopened_time': _latest_time(_RESPONSE_LOG_STATUS_DISPATCH_REOPENED),
        })

    return _jsonify_safe(result, 200)

# 6) Details


def _mdt_parse_triage_data(raw):
    """Normalize ``mdt_jobs.data`` JSON into a dict for MDT-only display helpers."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return {}
        try:
            out = json.loads(raw)
            return out if isinstance(out, dict) else {}
        except Exception:
            return {}
    return {}


def _mdt_form_answer(triage, key):
    fa = triage.get("form_answers") if isinstance(
        triage.get("form_answers"), dict) else {}
    v = fa.get(key)
    if v is None:
        return ""
    return str(v).strip()


def _mdt_presenting_complaint(triage):
    r = str(triage.get("reason_for_call") or "").strip()
    if r:
        return r
    for fk in ("primary_symptom", "immediate_danger", "event_name"):
        v = _mdt_form_answer(triage, fk)
        if v:
            return v
    return ""


def _mdt_age_display_str(triage):
    dob = triage.get("patient_dob")
    if dob:
        try:
            if hasattr(dob, "year"):
                d = dob
            else:
                s = str(dob).strip().split("T")[0].split(" ")[0]
                d = datetime.strptime(s[:10], "%Y-%m-%d").date()
            today = date.today()
            age = today.year - d.year - \
                ((today.month, today.day) < (d.month, d.day))
            if age >= 0:
                return str(age)
        except Exception:
            pass
    for k in ("patient_age", "age"):
        v = triage.get(k)
        if v is None or str(v).strip() == "":
            continue
        try:
            return str(int(round(float(v))))
        except Exception:
            return str(v).strip()
    return ""


def _mdt_one_risk_flag_text(item):
    if item is None:
        return ""
    if isinstance(item, dict):
        for k in ("description", "detail", "message", "text", "label", "value", "name"):
            if item.get(k):
                return str(item[k]).strip()
        return ""
    return str(item).strip()


def _mdt_risk_flags_line(triage):
    rf = triage.get("risk_flags")
    if rf is None:
        return ""
    if isinstance(rf, str):
        s = rf.strip()
        if s.startswith("["):
            try:
                rf = json.loads(s)
            except Exception:
                return s
        else:
            return s
    if not isinstance(rf, list):
        return str(rf).strip()
    parts = [_mdt_one_risk_flag_text(x) for x in rf]
    return "; ".join(p for p in parts if p)


def _mdt_entry_requirements_line(triage):
    er = triage.get("entry_requirements")
    if er is None or er == "":
        return ""
    if isinstance(er, str):
        s = er.strip()
        try:
            er = json.loads(s)
        except Exception:
            return s
    if not isinstance(er, list):
        return str(er).strip()
    parts = []
    for item in er:
        if isinstance(item, dict):
            label = str(item.get("label") or item.get(
                "type") or "General").strip()
            val = str(item.get("value") or item.get("text") or "").strip()
            if val:
                parts.append(f"{label}: {val}")
            elif label:
                parts.append(label)
        else:
            t = str(item).strip()
            if t:
                parts.append(t)
    return "; ".join(parts)


def _mdt_titleish(s):
    t = str(s or "").strip().lower().replace("_", " ")
    if not t:
        return ""
    return " ".join(w.capitalize() for w in t.split())


def _mdt_division_display(cur, slug):
    slug = str(slug or "").strip().lower()
    if not slug:
        return ""
    try:
        _ensure_dispatch_divisions_table(cur)
        cur.execute(
            "SELECT name FROM mdt_dispatch_divisions WHERE LOWER(TRIM(slug)) = %s LIMIT 1",
            (slug,),
        )
        row = cur.fetchone() or {}
        n = (row.get("name") or "").strip()
        if n:
            return n
    except Exception:
        pass
    return slug.replace("_", " ").title()


def _build_mdt_incident_display(triage, cad, *, job_division=None, cur):
    """Ordered two-column grid for MDT *Current Response* only (CAD uses its own layout).

    Excludes DOB, priority_source, and decision. Render ``rows`` in order; ``kind`` is
    ``pair`` (two columns) or ``full`` (span both columns).
    """
    cp = str(
        triage.get("call_priority_label") or triage.get(
            "call_priority") or triage.get("priority") or ""
    ).strip()
    pair_rows = [
        {
            "left": {"label": "Call Priority", "value": cp},
            "right": {"label": "Presenting Complaint", "value": _mdt_presenting_complaint(triage)},
        },
        {
            "left": {"label": "Age", "value": _mdt_age_display_str(triage)},
            "right": {"label": "Patient Gender", "value": _mdt_titleish(triage.get("patient_gender"))},
        },
        {
            "left": {"label": "First Name", "value": str(triage.get("first_name") or "").strip()},
            "right": {"label": "Surname", "value": str(triage.get("last_name") or "").strip()},
        },
        {
            "left": {"label": "Risk Flags", "value": _mdt_risk_flags_line(triage)},
            "right": {"label": "Entry Requirements", "value": _mdt_entry_requirements_line(triage)},
        },
        {
            "left": {"label": "Address", "value": str(triage.get("address") or "").strip()},
            "right": {"label": "Postcode", "value": str(triage.get("postcode") or "").strip()},
        },
        {
            "left": {"label": "Event Name", "value": _mdt_form_answer(triage, "event_name")},
            "right": {"label": "Event Zone", "value": _mdt_form_answer(triage, "event_zone")},
        },
        {
            "left": {"label": "Crowd Density", "value": _mdt_titleish(_mdt_form_answer(triage, "crowd_density"))},
            "right": {
                "label": "Security Required",
                "value": _mdt_titleish(_mdt_form_answer(triage, "security_required")),
            },
        },
        {
            "left": {"label": "Caller Name", "value": str(triage.get("caller_name") or "").strip()},
            "right": {"label": "Caller No.", "value": str(triage.get("caller_phone") or "").strip()},
        },
        {
            "left": {
                "label": "Division",
                "value": _mdt_division_display(cur, str(triage.get("division") or job_division or "").strip()),
            },
            "right": {"label": "CAD number", "value": f"CAD {int(cad)}" if cad is not None else ""},
        },
    ]
    rows = [{"kind": "pair", **pair_rows[0]}]
    add = str(triage.get("additional_details") or "").strip()
    if add:
        rows.append(
            {"kind": "full", "label": "Additional details", "value": add})
    for pr in pair_rows[1:]:
        rows.append({"kind": "pair", **pr})
    return {"version": 1, "rows": rows}


@internal.route('/api/mdt/<int:cad>', methods=['GET', 'OPTIONS'])
def mdt_details(cad):
    if request.method == 'OPTIONS':
        return '', 200

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_div_col = cur.fetchone() is not None
        sel = "SELECT status, data" + \
            (", division" if has_div_col else "") + \
            " FROM mdt_jobs WHERE cad = %s"
        cur.execute(sel, (cad,))
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Not found'}), 404

        triage = _mdt_parse_triage_data(row.get("data"))
        job_div = (row.get("division")
                   if has_div_col else None) or triage.get("division")
        incident_display = _build_mdt_incident_display(
            triage, cad, job_division=job_div, cur=cur)

        job_st = str(row.get('status') or '').strip().lower()
        payload = {
            'cad': cad,
            'status': job_st or row.get('status'),
            'triage_data': row['data'],
            'incident_display': incident_display,
        }
        # Per-unit ladder state for MDT (CAD-SCENE-001): job row is one status per CAD; optional
        # callsign must match POST /mdt/<cad>/status rules (assigned incident or mdt_job_units).
        qs = request.args.get('callsign') or request.args.get('callSign') or ''
        cs_raw = str(qs or '').strip().upper()
        if cs_raw:
            try:
                _ensure_callsign_redirect_table(cur)
                cs = _mdt_resolve_callsign(cur, cs_raw)
                cur.execute(
                    """
                    SELECT assignedIncident, status
                      FROM mdts_signed_on
                     WHERE callSign = %s
                     ORDER BY signOnTime DESC
                     LIMIT 1
                    """,
                    (cs,),
                )
                urow = cur.fetchone() or {}
                is_live_assignment = False
                try:
                    is_live_assignment = int(urow.get('assignedIncident') or 0) == int(cad)
                except (TypeError, ValueError):
                    pass
                is_linked_assignment = False
                cur.execute("SHOW TABLES LIKE 'mdt_job_units'")
                if cur.fetchone():
                    cur.execute(
                        """
                        SELECT 1 FROM mdt_job_units
                         WHERE job_cad = %s AND callsign = %s
                         LIMIT 1
                        """,
                        (cad, cs),
                    )
                    is_linked_assignment = cur.fetchone() is not None
                if (is_live_assignment or is_linked_assignment) and urow:
                    payload['my_status'] = str(urow.get('status') or '').strip().lower()
                    payload['my_callsign'] = cs
                    if cs_raw != cs:
                        payload['my_requested_callsign'] = cs_raw
            except Exception:
                logger.exception(
                    'mdt_details my_status cad=%s callsign=%s', cad, cs_raw)

        return _jsonify_safe(payload, 200)
    finally:
        cur.close()
        conn.close()

# 6b) CAD comms/update stream for MDT clients (sessionless)


@internal.route('/api/mdt/<int:cad>/comms', methods=['GET', 'POST', 'OPTIONS'])
def mdt_job_comms_api(cad):
    if request.method == 'OPTIONS':
        return '', 200

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_job_comms_table(cur)
        cur.execute("SELECT cad FROM mdt_jobs WHERE cad = %s LIMIT 1", (cad,))
        if cur.fetchone() is None:
            return jsonify({'error': 'Job not found'}), 404

        if request.method == 'GET':
            _excl_t, _excl_ph = _job_comms_audit_only_types_sql()
            cur.execute(
                f"""
                SELECT id, cad, message_type, sender_role, sender_user, message_text, created_at
                FROM mdt_job_comms
                WHERE cad = %s
                  AND LOWER(TRIM(COALESCE(message_type, ''))) NOT IN ({_excl_ph})
                ORDER BY created_at ASC, id ASC
                LIMIT 800
                """,
                (cad,) + _excl_t,
            )
            return _jsonify_safe(cur.fetchall() or [], 200)

        payload = request.get_json(silent=True) or {}
        msg_text = str(payload.get('text') or '').strip()
        msg_type = str(payload.get('type') or 'message').strip().lower()
        # Dispatcher web CAD uses backup_p1/p2/p3; MDT clients stay message/update only.
        if msg_type not in ('message', 'update'):
            msg_type = 'message'
        if not msg_text:
            return jsonify({'error': 'text is required'}), 400

        sender_user = str(
            payload.get('callSign')
            or payload.get('callsign')
            or payload.get('from')
            or 'mdt'
        ).strip()[:120]
        if not sender_user:
            sender_user = 'mdt'

        cur.execute("""
            INSERT INTO mdt_job_comms (cad, message_type, sender_role, sender_user, message_text)
            VALUES (%s, %s, %s, %s, %s)
        """, (cad, msg_type, 'crew', sender_user, msg_text))
        mdt_job_comm_row_id = int(getattr(cur, 'lastrowid', None) or 0)

        assigned_units_for_push = []
        if msg_type == 'update':
            cur.execute("SELECT data FROM mdt_jobs WHERE cad = %s", (cad,))
            row = cur.fetchone() or {}
            raw = row.get('data')
            try:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode('utf-8', errors='ignore')
                data = json.loads(raw) if isinstance(raw, str) and raw else {}
                if not isinstance(data, dict):
                    data = {}
            except Exception:
                data = {}
            history = data.get('incident_updates')
            if not isinstance(history, list):
                history = []
            history.append({
                'time': datetime.utcnow().isoformat(),
                'by': sender_user,
                'text': msg_text
            })
            data['incident_updates'] = history
            cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
            has_updated_at = cur.fetchone() is not None
            if has_updated_at:
                cur.execute("UPDATE mdt_jobs SET data = %s, updated_at = NOW() WHERE cad = %s",
                            (json.dumps(data, default=str), cad))
            else:
                cur.execute("UPDATE mdt_jobs SET data = %s WHERE cad = %s",
                            (json.dumps(data, default=str), cad))
            try:
                assigned_units_for_push = _get_job_unit_callsigns(cur, cad)
            except Exception:
                assigned_units_for_push = []

        # Mirror into dispatcher/MDT inbox feed.
        try:
            cur.execute("SHOW TABLES LIKE 'messages'")
            if cur.fetchone() is not None:
                cur.execute("""
                    INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                    VALUES (%s, %s, %s, NOW(), 0)
                """, (sender_user, 'dispatcher', f"CAD #{cad} {msg_type.upper()}: {msg_text}"))
        except Exception:
            pass

        conn.commit()
        audit_action = 'mdt_job_comm_update' if msg_type == 'update' else 'mdt_job_comm_message'
        log_audit(
            _mdt_audit_actor(),
            audit_action,
            details={
                'cad': cad,
                'message_type': msg_type,
                'sender_user': sender_user,
                'text_len': len(msg_text),
            },
        )

        try:
            _mdt_jc = {'type': 'job_comm', 'cad': cad,
                       'message_type': msg_type, 'text': msg_text, 'by': sender_user}
            if mdt_job_comm_row_id > 0:
                _mdt_jc['id'] = mdt_job_comm_row_id
            socketio.emit('mdt_event', _mdt_jc)
            if msg_type == 'update':
                socketio.emit(
                    'mdt_event', {'type': 'jobs_updated', 'cad': cad})
                socketio.emit('mdt_event', {
                              'type': 'job_update', 'cad': cad, 'text': msg_text, 'by': sender_user, 'units': assigned_units_for_push})
        except Exception:
            pass

        return jsonify({'message': 'sent', 'cad': cad, 'type': msg_type}), 200
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()

# 7) Status update (includes clear & stand-down)


@internal.route('/api/mdt/<int:cad>/status', methods=['POST', 'OPTIONS'])
def mdt_status(cad):
    if request.method == 'OPTIONS':
        return '', 200

    payload = request.get_json() or {}
    status = str(payload.get('status') or '').strip().lower()
    callsign = payload.get('callSign') or payload.get('callsign') or ''

    valid = {
        'received', 'assigned', 'mobile', 'on_scene',
        'leave_scene', 'at_hospital', 'cleared', 'stood_down'
    }
    if not status:
        return jsonify({'error': 'status required'}), 400
    if status not in valid:
        return jsonify({'error': 'Invalid status', 'status': status}), 400
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    destination_to_activate = None
    try:
        _ensure_response_log_table(cur)
        _ensure_standby_tables(cur)
        if not callsign:
            # Fallback for MDT clients that omit callsign in status payload:
            # prefer explicit job-unit links, then signed-on assigned incident.
            cur.execute("SHOW TABLES LIKE 'mdt_job_units'")
            if cur.fetchone() is not None:
                cur.execute(
                    "SELECT callsign FROM mdt_job_units WHERE job_cad = %s ORDER BY id DESC LIMIT 1",
                    (cad,)
                )
                linked = cur.fetchone() or {}
                callsign = str(linked.get('callsign') or '').strip()
            if not callsign:
                cur.execute(
                    "SELECT callSign FROM mdts_signed_on WHERE assignedIncident = %s ORDER BY signOnTime DESC LIMIT 1",
                    (cad,)
                )
                live = cur.fetchone() or {}
                callsign = str(live.get('callSign') or '').strip()
        if not callsign:
            return jsonify({'error': 'callSign required'}), 400

        callsign_pre_resolve = str(callsign or '').strip().upper()
        _ensure_callsign_redirect_table(cur)
        callsign = _mdt_resolve_callsign(cur, callsign_pre_resolve)

        # Reject stale status events for superseded/reassigned incidents.
        # Only the currently assigned CAD (or explicitly linked unit<->CAD) may
        # advance operational statuses.
        cur.execute(
            "SELECT assignedIncident FROM mdts_signed_on WHERE callSign = %s ORDER BY signOnTime DESC LIMIT 1",
            (callsign,)
        )
        live_row = cur.fetchone() or {}
        live_assigned = live_row.get('assignedIncident')
        is_live_assignment = False
        try:
            is_live_assignment = int(live_assigned) == int(cad)
        except Exception:
            is_live_assignment = False

        is_linked_assignment = False
        cur.execute("SHOW TABLES LIKE 'mdt_job_units'")
        if cur.fetchone() is not None:
            cur.execute(
                "SELECT 1 FROM mdt_job_units WHERE job_cad = %s AND callsign = %s LIMIT 1",
                (cad, callsign)
            )
            is_linked_assignment = cur.fetchone() is not None

        if status not in ('cleared', 'stood_down') and not (is_live_assignment or is_linked_assignment):
            err_body = {
                'error': 'stale status update',
                'hint': 'This callsign is not assigned to this CAD (and not linked on mdt_job_units). '
                        'Use POST /api/mdt/<callsign>/status for unit-only states (standby, mobile to station, etc.).',
                'callsign': callsign,
                'callSign': callsign,
                'cad': cad,
                'status': status
            }
            err_body.update(_mdt_callsign_client_hints(
                callsign_pre_resolve, callsign))
            return jsonify(err_body), 409

        # fetch current crew JSON
        cur.execute(
            "SELECT crew FROM mdts_signed_on WHERE callSign = %s ORDER BY signOnTime DESC LIMIT 1",
            (callsign,)
        )
        row = cur.fetchone()
        crew_json = row['crew'] if row and row.get('crew') else '[]'

        # Ensure CAD exists before applying status transitions.
        cur.execute("SELECT cad FROM mdt_jobs WHERE cad = %s LIMIT 1", (cad,))
        if cur.fetchone() is None:
            return jsonify({'error': 'CAD not found'}), 404

        # Stood down: detach; if last unit, return job to queue for reassignment (MCI-friendly).
        # Cleared: detach; if last unit, keep CAD on stack for closure review (multi-resource) — dispatch closes via Close CAD.
        if status == 'stood_down':
            _mdt_detach_unit_from_job(
                cur, cad, callsign, is_live_assignment, is_linked_assignment)
        elif status == 'cleared':
            _mdt_detach_unit_from_job(
                cur, cad, callsign, is_live_assignment, is_linked_assignment,
                empty_job_next_status=_JOB_STATUS_AWAITING_DISPATCH_REVIEW,
                response_log_crew_json=crew_json,
            )
        else:
            # Normal progression (mobile, on_scene, etc.).
            cur.execute(
                "UPDATE mdt_jobs SET status = %s WHERE cad = %s",
                (status, cad)
            )

        # log status change
        cur.execute(
            "INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew) VALUES (%s, %s, %s, NOW(), %s)",
            (callsign, cad, status, crew_json)
        )

        # update live status, but never let an old CAD clear a newer assignment.
        # Only clear assignedIncident when the CAD being cleared/stood_down is
        # still the unit's current assignment.
        cur.execute(
            """
            UPDATE mdts_signed_on
               SET status = CASE
                              WHEN %s IN ('cleared','stood_down') THEN
                                CASE
                                  WHEN assignedIncident = %s OR assignedIncident IS NULL THEN 'on_standby'
                                  ELSE status
                                END
                              ELSE %s
                            END,
                   assignedIncident = CASE
                                        WHEN %s IN ('cleared','stood_down') AND assignedIncident = %s THEN NULL
                                        ELSE assignedIncident
                                      END
             WHERE callSign = %s
            """,
            (status, cad, status, status, cad, callsign)
        )

        if status == 'leave_scene':
            latest_destination = _load_latest_destination(cur, callsign)
            if latest_destination:
                d_type = str(latest_destination.get(
                    'destination_type') or '').strip().lower()
                d_mode = str(latest_destination.get(
                    'activation_mode') or '').strip().lower()
                d_state = str(latest_destination.get(
                    'state') or '').strip().lower()
                if d_type == 'transport' and d_mode == 'on_leave_scene' and d_state in {'pending', 'queued', 'sent'}:
                    cur.execute("""
                        UPDATE standby_locations
                           SET state = 'active',
                               activatedAt = NOW(),
                               updatedAt = CURRENT_TIMESTAMP
                         WHERE callSign = %s
                           AND LOWER(TRIM(COALESCE(destinationType, 'standby'))) = 'transport'
                    """, (callsign,))
                    latest_destination['state'] = 'active'
                    latest_destination['activated_at'] = datetime.utcnow(
                    ).isoformat()
                    destination_to_activate = latest_destination
        elif status == 'at_hospital':
            cur.execute("""
                UPDATE standby_locations
                   SET state = 'completed',
                       updatedAt = CURRENT_TIMESTAMP
                 WHERE callSign = %s
                   AND LOWER(TRIM(COALESCE(destinationType, 'standby'))) = 'transport'
                   AND LOWER(TRIM(COALESCE(state, ''))) IN ('active', 'pending', 'queued', 'sent')
            """, (callsign,))
        elif status in ('cleared', 'stood_down'):
            cur.execute("""
                UPDATE standby_locations
                   SET state = 'cancelled',
                       updatedAt = CURRENT_TIMESTAMP
                 WHERE callSign = %s
                   AND LOWER(TRIM(COALESCE(destinationType, 'standby'))) = 'transport'
                   AND LOWER(TRIM(COALESCE(state, ''))) IN ('active', 'pending', 'queued', 'sent')
            """, (callsign,))

        if not log_audit(
            _mdt_audit_actor(),
            'mdt_status',
            details={'cad': cad, 'callsign': callsign, 'status': status},
            audit_failure='fail_closed',
        ):
            conn.rollback()
            return _ventus_audit_unavailable_response()
        conn.commit()
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()

    try:
        socketio.emit('mdt_event', {
            'type': 'status_update',
            'cad': cad,
            'status': status,
            'callsign': callsign
        })
        socketio.emit('mdt_event', {'type': 'jobs_updated', 'cad': cad})
        if destination_to_activate:
            socketio.emit('mdt_event', {
                'type': 'destination_activated',
                'callsign': callsign,
                'cad': cad,
                'destination': destination_to_activate
            })
            socketio.emit(
                'mdt_event', {'type': 'units_updated', 'callsign': callsign})
    except Exception:
        pass

    ok_body = {
        'message': 'Status updated and logged',
        'destination_to_activate': destination_to_activate,
        'callSign': callsign,
        'callsign': callsign,
    }
    ok_body.update(_mdt_callsign_client_hints(callsign_pre_resolve, callsign))
    return _jsonify_safe(ok_body, 200)

# 8) Location update (real-time position reporting)


def _update_mdt_location(callsign, latitude, longitude):
    raw_cs = str(callsign or '').strip().upper()
    if not raw_cs:
        return {'error': 'callSign required'}, 400
    try:
        latitude = float(latitude)
        longitude = float(longitude)
    except Exception:
        return {'error': 'latitude and longitude must be numeric'}, 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        callsign = _mdt_resolve_callsign(cur, raw_cs)
        _ensure_response_log_table(cur)
        # Keep historic location table available across mixed schemas.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mdt_positions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                callSign VARCHAR(64) NOT NULL,
                latitude DECIMAL(10,7) NOT NULL,
                longitude DECIMAL(10,7) NOT NULL,
                recorded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_call_time (callSign, recorded_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        # Only update columns that exist in mdts_signed_on.
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'lastLat'")
        has_last_lat = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'lastLon'")
        has_last_lon = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'lastSeenAt'")
        has_last_seen = cur.fetchone() is not None

        set_parts = []
        args = []
        if has_last_lat:
            set_parts.append("lastLat = %s")
            args.append(latitude)
        if has_last_lon:
            set_parts.append("lastLon = %s")
            args.append(longitude)
        if has_last_seen:
            set_parts.append("lastSeenAt = NOW()")
        if set_parts:
            args.append(callsign)
            cur.execute(
                f"UPDATE mdts_signed_on SET {', '.join(set_parts)} WHERE callSign = %s",
                tuple(args)
            )

        # Auto-ack delivery heuristic:
        # If a unit is still in "assigned" and we receive a successful ping
        # after dispatch, treat this as transport-level confirmation that the
        # MDT is online and mark current assignment as "received" once.
        cur.execute("""
            SELECT assignedIncident, status, crew
            FROM mdts_signed_on
            WHERE callSign = %s
            ORDER BY signOnTime DESC
            LIMIT 1
        """, (callsign,))
        live = cur.fetchone() or {}
        live_cad = live.get('assignedIncident')
        live_status = str(live.get('status') or '').strip().lower()
        crew_json = live.get('crew') or '[]'
        if live_cad and live_status == 'assigned':
            try:
                live_cad = int(live_cad)
            except Exception:
                live_cad = None
        if live_cad:
            cur.execute("""
                SELECT
                  MAX(CASE WHEN status = 'assigned' THEN event_time END) AS last_assigned,
                  MAX(CASE WHEN status = 'received' THEN event_time END) AS last_received
                FROM mdt_response_log
                WHERE callSign = %s AND cad = %s
            """, (callsign, live_cad))
            marker = cur.fetchone() or {}
            last_assigned = marker.get('last_assigned')
            last_received = marker.get('last_received')
            should_ack = bool(last_assigned) and (
                not last_received or last_received < last_assigned)
            if should_ack:
                cur.execute(
                    "UPDATE mdts_signed_on SET status = 'received' WHERE callSign = %s",
                    (callsign,)
                )
                cur.execute("""
                    INSERT INTO mdt_response_log (callSign, cad, status, event_time, crew)
                    VALUES (%s, %s, 'received', NOW(), %s)
                """, (callsign, live_cad, crew_json))

        cur.execute(
            "INSERT INTO mdt_positions (callSign, latitude, longitude, recorded_at) VALUES (%s, %s, %s, NOW())",
            (callsign, latitude, longitude)
        )
        conn.commit()
        ok = {'message': 'Location updated', 'callSign': callsign}
        ok.update(_mdt_callsign_client_hints(raw_cs, callsign))
        return ok, 200
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error_body(e), 500
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/location', methods=['POST', 'OPTIONS'])
def mdt_update_location():
    if request.method == 'OPTIONS':
        return '', 200

    payload = request.get_json(silent=True) or {}
    callsign = payload.get('callSign') or payload.get('callsign') or ''
    latitude = payload.get('latitude', payload.get('lat'))
    longitude = payload.get('longitude', payload.get('lng'))
    body, code = _update_mdt_location(callsign, latitude, longitude)
    if code == 200:
        try:
            cs = str((body or {}).get('callSign')
                     or callsign or '').strip().upper()
            if cs:
                socketio.emit(
                    'mdt_event', {'type': 'units_updated', 'callsign': cs})
        except Exception:
            pass
    return jsonify(body), code


# --- MDT compatibility aliases (legacy/mobile clients) ---
@internal.route('/api/mdt/<callsign>/location', methods=['POST', 'OPTIONS'])
def mdt_update_location_legacy(callsign):
    """Legacy alias: location endpoint with callsign in URL path."""
    if request.method == 'OPTIONS':
        return '', 200
    payload = request.get_json(silent=True) or {}
    latitude = payload.get('latitude', payload.get('lat'))
    longitude = payload.get('longitude', payload.get('lng'))
    body, code = _update_mdt_location(callsign, latitude, longitude)
    if code == 200:
        try:
            cs = str((body or {}).get('callSign')
                     or callsign or '').strip().upper()
            if cs:
                socketio.emit(
                    'mdt_event', {'type': 'units_updated', 'callsign': cs})
        except Exception:
            pass
    return jsonify(body), code


@internal.route('/api/mdt/<callsign>/major_incident', methods=['POST', 'OPTIONS'])
def mdt_major_incident_submit(callsign):
    """MDT-initiated major incident (CAD-MI-001 / MDT-MI-001). Versioned JSON; idempotent via client_request_id."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400

    body = request.get_json(silent=True)
    errs, parsed = _major_incident_parse_request_body(body or {}, source_mdt=True)
    if errs:
        return jsonify({'error': 'validation failed', 'details': errs}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    mi_id = None
    summary = ''
    cs_resolved = ''
    comm_text_saved = None
    mi_job_comm_id = None
    try:
        _ensure_major_incidents_table(cur)
        _ensure_callsign_redirect_table(cur)
        cs_resolved = _mdt_resolve_callsign(cur, cs_raw)
        if not _mdt_jwt_allowed_for_callsign_crew(cur, cs_resolved):
            return jsonify({'error': 'JWT user is not on this unit crew'}), 403

        cur.execute(
            "SELECT callSign FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
            (cs_resolved,),
        )
        if not cur.fetchone():
            return jsonify({'error': 'Unit is not signed on'}), 403

        cur.execute(
            """
            SELECT id FROM ventus_major_incidents
             WHERE source = 'mdt'
               AND callsign = %s
               AND client_request_id = %s
             LIMIT 1
            """,
            (cs_resolved, parsed['client_request_id']),
        )
        dup = cur.fetchone()
        if dup:
            return _jsonify_safe({
                'ok': True,
                'id': dup['id'],
                'duplicate': True,
                'message': 'Already recorded (idempotent)',
            }, 200)

        if parsed['cad'] is not None:
            cur.execute(
                "SELECT cad FROM mdt_jobs WHERE cad = %s LIMIT 1",
                (parsed['cad'],),
            )
            if not cur.fetchone():
                return jsonify({
                    'error': 'cad not found',
                    'cad': parsed['cad'],
                }), 404
            cur.execute(
                """
                SELECT id FROM ventus_major_incidents
                 WHERE cad = %s AND LOWER(COALESCE(status, '')) = 'open'
                 LIMIT 1
                """,
                (parsed['cad'],),
            )
            if cur.fetchone():
                return jsonify({
                    'error': 'open major incident exists',
                    'cad': parsed['cad'],
                    'message': (
                        'This CAD already has an open Major Incident declaration. '
                        'Stand it down before declaring again.'
                    ),
                }), 409

        mi_id = _major_incident_insert_row(
            cur,
            parsed=parsed,
            source='mdt',
            callsign=cs_resolved,
            created_by=_mdt_audit_actor(),
        )
        summary = _major_incident_summary_title(
            parsed['template'], parsed['payload'])

        if parsed['cad'] is not None:
            _ensure_job_comms_table(cur)
            comm_text_saved = _major_incident_job_comm_text(
                parsed['template'], summary, parsed['payload'])
            cur.execute("""
                INSERT INTO mdt_job_comms
                    (cad, message_type, sender_role, sender_user, message_text)
                VALUES (%s, 'major_incident', 'mdt', %s, %s)
            """, (parsed['cad'], cs_resolved, comm_text_saved))
            mi_job_comm_id = int(getattr(cur, 'lastrowid', None) or 0) or None

        if not log_audit(
            _mdt_audit_actor(),
            'mdt_major_incident',
            details={
                'id': mi_id,
                'template': parsed['template'],
                'cad': parsed['cad'],
                'callsign': cs_resolved,
            },
            audit_failure='fail_closed',
        ):
            conn.rollback()
            return _ventus_audit_unavailable_response()

        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.exception('mdt_major_incident POST callsign=%s', cs_raw)
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()

    if mi_id:
        _major_incident_emit_socket(
            mi_id, parsed, cs_resolved, 'mdt', summary,
            comm_text=comm_text_saved,
            job_comm_id=mi_job_comm_id,
        )
    return _jsonify_safe({
        'ok': True,
        'id': mi_id,
        'message': 'Major Incident recorded',
        'summary': summary,
    }, 201)


@internal.route('/api/mdt/<callsign>/panic', methods=['POST', 'OPTIONS'])
def mdt_panic(callsign):
    """Crew MDT panic (tier 1 / backup_p1 label). On an active patient CAD, logs on that job; otherwise new CAD."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400

    now_ts = time.time()
    last = _mdt_panic_last_ts.get(cs_raw, 0)
    if now_ts - last < 25:
        return jsonify({'error': 'Panic can only be sent once every 25 seconds'}), 429

    payload = request.get_json(silent=True) or {}
    note = str(payload.get('note') or '').strip()[:500]
    lat_in = payload.get('latitude', payload.get('lat'))
    lng_in = payload.get('longitude', payload.get('lng'))

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        tier1_lbl = _dispatcher_backup_tier1_display_name(cur)
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        if not _mdt_jwt_allowed_for_callsign_crew(cur, cs):
            return jsonify({'error': 'JWT user is not on this unit crew'}), 403

        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_udiv = cur.fetchone() is not None
        div_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_udiv else "'general' AS division"
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'assignedIncident'")
        has_asg = cur.fetchone() is not None
        asg_sel = "assignedIncident" if has_asg else "NULL AS assignedIncident"
        cur.execute(
            f"SELECT crew, {asg_sel}, {div_sql} FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
            (cs,),
        )
        urow = cur.fetchone()
        if not urow:
            return jsonify({'error': 'Unit is not signed on'}), 403
        division = _normalize_division(
            urow.get('division'), fallback='general')
        crew_signed_on_labels = _mdt_signed_on_crew_display_labels(
            urow.get('crew'))

        coords = {}
        try:
            la = float(lat_in) if lat_in is not None else None
            ln = float(lng_in) if lng_in is not None else None
            if la is not None and ln is not None and -90 <= la <= 90 and -180 <= ln <= 180:
                coords = {
                    'lat': la,
                    'lng': ln,
                    'latitude': la,
                    'longitude': ln,
                    'source': 'mdt_panic',
                }
        except (TypeError, ValueError):
            pass
        if not coords:
            cur.execute(
                """
                SELECT latitude, longitude FROM mdt_positions
                WHERE callSign = %s ORDER BY recorded_at DESC LIMIT 1
                """,
                (cs,),
            )
            prow = cur.fetchone()
            if prow and prow.get('latitude') is not None and prow.get('longitude') is not None:
                try:
                    la = float(prow['latitude'])
                    ln = float(prow['longitude'])
                    coords = {
                        'lat': la,
                        'lng': ln,
                        'latitude': la,
                        'longitude': ln,
                        'source': 'mdt_last_position',
                    }
                except (TypeError, ValueError):
                    pass

        ts = datetime.utcnow().isoformat() + 'Z'
        actor = _mdt_audit_actor()

        existing_cad = None
        if has_asg:
            try:
                ai = urow.get('assignedIncident')
                if ai is not None and str(ai).strip() != '':
                    existing_cad = int(ai)
            except (TypeError, ValueError):
                existing_cad = None

        on_job_panic = False
        jrow = None
        job_division = division
        if existing_cad and existing_cad > 0:
            cur.execute(
                """
                SELECT cad, status, data,
                       LOWER(TRIM(COALESCE(status, ''))) AS st,
                       LOWER(TRIM(COALESCE(division, ''))) AS job_div
                FROM mdt_jobs WHERE cad = %s LIMIT 1
                """,
                (existing_cad,),
            )
            jrow = cur.fetchone()
            if jrow:
                jst = str(jrow.get('st') or '').strip().lower()
                if jst and jst not in _MDT_PANIC_TERMINAL_JOB_STATUSES:
                    cur.execute(
                        "SELECT 1 AS ok FROM mdt_job_units WHERE job_cad = %s AND callsign = %s LIMIT 1",
                        (existing_cad, cs),
                    )
                    if cur.fetchone():
                        on_job_panic = True
                        job_division = _normalize_division(
                            jrow.get('job_div'), fallback=division)

        if on_job_panic and jrow is not None:
            raw_data = jrow.get('data')
            try:
                if isinstance(raw_data, dict):
                    jdata = dict(raw_data)
                elif isinstance(raw_data, str) and raw_data.strip():
                    jdata = json.loads(raw_data)
                else:
                    jdata = {}
            except Exception:
                jdata = {}
            evs = jdata.get('mdt_grade1_panic_events')
            if not isinstance(evs, list):
                evs = []
            evs.append({
                'at_utc': ts,
                'callsign': cs,
                'note': note or None,
                'coordinates': coords or None,
                'kind': 'mdt_grade1_on_patient_cad',
            })
            jdata['mdt_grade1_panic_events'] = evs
            jdata['mdt_last_grade1_panic_at'] = ts
            jdata['mdt_grade1_panic_active'] = True
            jdata['mdt_panic_on_existing_job'] = True
            new_data_json = json.dumps(jdata, default=str)
            cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'updated_at'")
            has_updated = cur.fetchone() is not None
            if has_updated:
                cur.execute(
                    "UPDATE mdt_jobs SET data = %s, updated_at = NOW() WHERE cad = %s",
                    (new_data_json, existing_cad),
                )
            else:
                cur.execute(
                    "UPDATE mdt_jobs SET data = %s WHERE cad = %s",
                    (new_data_json, existing_cad),
                )

            _ensure_job_comms_table(cur)
            comm_text = (
                f'{tier1_lbl} PANIC (patient CAD): {cs} activated MDT panic — backup requested; '
                f'audit trail kept on this job (UTC {ts}).'
            )
            if note:
                comm_text += f' Note: {note}'
            cur.execute(
                """
                INSERT INTO mdt_job_comms (cad, message_type, sender_role, sender_user, message_text)
                VALUES (%s, 'panic', 'mdt', %s, %s)
                """,
                (existing_cad, cs, comm_text),
            )
            panic_on_job_comm_id = int(getattr(cur, 'lastrowid', None) or 0)

            conn.commit()
            _mdt_panic_last_ts[cs_raw] = now_ts
            if cs != cs_raw:
                _mdt_panic_last_ts[cs] = now_ts

            div_label = _pretty_division_slug_label(job_division)
            try:
                _poj_jc = {
                    'type': 'job_comm',
                    'cad': existing_cad,
                    'message_type': 'panic',
                    'text': comm_text,
                    'by': cs,
                }
                if panic_on_job_comm_id > 0:
                    _poj_jc['id'] = panic_on_job_comm_id
                socketio.emit('mdt_event', _poj_jc)
                socketio.emit(
                    'mdt_event',
                    {
                        'type': 'panic_alert',
                        'division': job_division,
                        'division_label': div_label,
                        'cad': existing_cad,
                        'callsign': cs,
                        'note': note,
                        'on_existing_job': True,
                        'grade1_backup': True,
                        'continuous_alarm': True,
                        'crew_signed_on': crew_signed_on_labels,
                        'mdt_last_grade1_panic_at': ts,
                        'priority_source': 'mdt_panic',
                    }
                )
                socketio.emit(
                    'mdt_event',
                    {'type': 'jobs_updated', 'cad': existing_cad, 'force': True}
                )
                socketio.emit(
                    'mdt_event', {'type': 'units_updated', 'callsign': cs})
                _emit_mdt_job_assigned(existing_cad, [cs])
            except Exception:
                logger.exception(
                    'socket emit mdt panic on-job cad=%s', existing_cad)
            try:
                run_priority_preemption_for_job(int(existing_cad))
            except Exception:
                logger.exception(
                    'priority preemption after mdt panic cad=%s', existing_cad)

            log_audit(
                actor,
                'mdt_panic_on_patient_cad',
                details={'cad': existing_cad, 'callsign': cs,
                         'division': job_division},
            )
            body = {
                'ok': True,
                'cad': existing_cad,
                'callsign': cs,
                'division': job_division,
                'on_existing_job': True,
                'message': f'{tier1_lbl} panic logged on current patient CAD — dispatch alerted',
            }
            if cs_raw != cs:
                body['requestedCallSign'] = cs_raw
                body['canonicalCallSign'] = cs
            return jsonify(body), 200

        cur.execute(
            """
            SELECT ju.job_cad AS job_cad, LOWER(TRIM(COALESCE(j.status, ''))) AS st
            FROM mdt_job_units ju
            INNER JOIN mdt_jobs j ON j.cad = ju.job_cad
            WHERE ju.callsign = %s
            """,
            (cs,),
        )
        prev_jobs = list(cur.fetchall() or [])
        _ensure_job_units_table(cur)
        for row in prev_jobs:
            try:
                old_cad = int(row['job_cad'])
            except (TypeError, ValueError):
                continue
            st = str(row.get('st') or '')
            is_live = st in ('assigned', 'claimed', 'mobile', 'received')
            _mdt_detach_unit_from_job(cur, old_cad, cs, is_live, True)

        cur.execute(
            "UPDATE mdts_signed_on SET assignedIncident = NULL, status = 'on_standby' WHERE callSign = %s",
            (cs,),
        )

        new_cad = _mdt_next_allocated_job_cad(cur)

        details_bits = [f"MDT panic activated at {ts} UTC.", f"Unit: {cs}."]
        if note:
            details_bits.append(f"Crew note: {note}")
        panic_data = {
            'cad': new_cad,
            'reason_for_call': 'Crew distress — MDT panic alert',
            'presenting_complaint': f'MDT PANIC — {tier1_lbl} backup request',
            'incident_type': 'Crew / unit emergency (MDT panic)',
            'additional_details': ' '.join(details_bits),
            'call_priority': 'Grade 1',
            'call_priority_label': f'{tier1_lbl} — immediate backup',
            'triage_category': 'Grade 1',
            'priority_source': 'mdt_panic',
            'division': division,
            'form_slug': 'mdt_panic',
            'form_name': 'MDT Panic',
            'coordinates': coords,
            'mdt_panic_callsign': cs,
            'mdt_panic_at': ts,
            'mdt_grade1_panic_active': True,
            'mdt_last_grade1_panic_at': ts,
            'mdt_panic_on_existing_job': False,
        }
        panic_data['created_by'] = str(cs or '').strip() or None

        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'created_by'")
        has_created_by = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_division = cur.fetchone() is not None
        data_json = json.dumps(panic_data, default=str)
        if has_created_by:
            if has_division:
                cur.execute(
                    """
                    INSERT INTO mdt_jobs (cad, created_by, status, data, division)
                    VALUES (%s, %s, 'queued', %s, %s)
                    """,
                    (new_cad, actor, data_json, division),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO mdt_jobs (cad, created_by, status, data)
                    VALUES (%s, %s, 'queued', %s)
                    """,
                    (new_cad, actor, data_json),
                )
        else:
            if has_division:
                cur.execute(
                    """
                    INSERT INTO mdt_jobs (cad, status, data, division)
                    VALUES (%s, 'queued', %s, %s)
                    """,
                    (new_cad, data_json, division),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO mdt_jobs (cad, status, data)
                    VALUES (%s, 'queued', %s)
                    """,
                    (new_cad, data_json),
                )
        try:
            if has_division:
                _ensure_job_division_snapshot_columns(cur)
                _sync_job_division_snapshot_for_slug(cur, new_cad, division)
        except Exception:
            logger.exception('division snapshot on mdt panic cad=%s', new_cad)

        cur.execute(
            """
            INSERT INTO mdt_job_units (job_cad, callsign, assigned_by)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                assigned_by = VALUES(assigned_by),
                assigned_at = CURRENT_TIMESTAMP
            """,
            (new_cad, cs, 'mdt_panic'),
        )
        _sync_claimed_by_from_job_units(cur, new_cad)
        cur.execute(
            """
            UPDATE mdt_jobs
               SET status = 'on_scene'
             WHERE cad = %s
               AND LOWER(TRIM(COALESCE(status, ''))) = 'queued'
            """,
            (new_cad,),
        )
        # Panic crew are static at the incident — skip "assigned"/mobile ladder (avoids MDT pollStatus false reassignment).
        cur.execute(
            """
            UPDATE mdts_signed_on
               SET assignedIncident = %s,
                   status = 'on_scene'
             WHERE callSign = %s
            """,
            (new_cad, cs),
        )

        _ensure_job_comms_table(cur)
        comm_text = f'{tier1_lbl} PANIC: {cs} activated MDT panic.'
        if note:
            comm_text += f' Note: {note}'
        cur.execute(
            """
            INSERT INTO mdt_job_comms (cad, message_type, sender_role, sender_user, message_text)
            VALUES (%s, 'panic', 'mdt', %s, %s)
            """,
            (new_cad, cs, comm_text),
        )
        panic_new_cad_comm_id = int(getattr(cur, 'lastrowid', None) or 0)

        conn.commit()
        _mdt_panic_last_ts[cs_raw] = now_ts
        if cs != cs_raw:
            _mdt_panic_last_ts[cs] = now_ts

        div_label = _pretty_division_slug_label(division)
        try:
            _pn_jc = {
                'type': 'job_comm',
                'cad': new_cad,
                'message_type': 'panic',
                'text': comm_text,
                'by': cs,
            }
            if panic_new_cad_comm_id > 0:
                _pn_jc['id'] = panic_new_cad_comm_id
            socketio.emit('mdt_event', _pn_jc)
            socketio.emit(
                'mdt_event',
                {
                    'type': 'panic_alert',
                    'division': division,
                    'division_label': div_label,
                    'cad': new_cad,
                    'callsign': cs,
                    'note': note,
                    'on_existing_job': False,
                    'grade1_backup': True,
                    'continuous_alarm': True,
                    'crew_signed_on': crew_signed_on_labels,
                    'mdt_last_grade1_panic_at': ts,
                    'priority_source': 'mdt_panic',
                }
            )
            socketio.emit(
                'mdt_event',
                {'type': 'jobs_updated', 'cad': new_cad, 'force': True}
            )
            socketio.emit(
                'mdt_event', {'type': 'units_updated', 'callsign': cs})
            # Do not emit job_assigned — panic is not a routine allocation; CAD/MDT use panic_alert + jobs_updated.
        except Exception:
            logger.exception('socket emit mdt panic cad=%s', new_cad)
        try:
            run_priority_preemption_for_job(int(new_cad))
        except Exception:
            logger.exception(
                'priority preemption after mdt panic cad=%s', new_cad)

        log_audit(
            actor,
            'mdt_panic',
            details={'cad': new_cad, 'callsign': cs, 'division': division},
        )
        body = {
            'ok': True,
            'cad': new_cad,
            'callsign': cs,
            'division': division,
            'on_existing_job': False,
            'message': f'Panic sent — dispatch alerted and {tier1_lbl} CAD created',
        }
        if cs_raw != cs:
            body['requestedCallSign'] = cs_raw
            body['canonicalCallSign'] = cs
        return jsonify(body), 200
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/crew-user-search', methods=['GET', 'OPTIONS'])
def mdt_crew_user_search():
    """Typeahead for MDT crew add: core users + active contractors (username + friendly label)."""
    if request.method == 'OPTIONS':
        return '', 200
    q = (request.args.get('q') or '').strip()
    if len(q) < 2:
        return jsonify({'results': []}), 200
    if len(q) > 64 or not re.match(r'^[\w.@+\-\s]+$', q, re.I):
        return jsonify({'results': [], 'error': 'invalid query'}), 400
    pat = f'%{q}%'
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    seen = set()
    out = []
    try:
        cur.execute("SHOW TABLES LIKE 'users'")
        if cur.fetchone():
            cur.execute(
                """
                SELECT username,
                       TRIM(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''))) AS full_name,
                       email
                  FROM users
                 WHERE (username LIKE %s OR LOWER(email) LIKE LOWER(%s)
                        OR TRIM(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''))) LIKE %s)
                 ORDER BY username ASC
                 LIMIT 15
                """,
                (pat, pat, pat),
            )
            for row in cur.fetchall() or []:
                u = str(row.get('username') or '').strip()
                if not u or u.lower() in seen:
                    continue
                seen.add(u.lower())
                fn = (row.get('full_name') or '').strip()
                em = (row.get('email') or '').strip()
                label = fn or em or u
                out.append({'username': u, 'label': label})
        cur.execute("SHOW TABLES LIKE 'tb_contractors'")
        if cur.fetchone():
            active = (
                "(status IS NULL OR LOWER(TRIM(COALESCE(status,''))) IN "
                "('active','1','true','yes'))"
            )
            cur.execute(
                """
                SELECT username
                  FROM tb_contractors
                 WHERE """
                + active
                + """
                   AND username IS NOT NULL AND TRIM(username) <> ''
                   AND username LIKE %s
                 ORDER BY username ASC
                 LIMIT 10
                """,
                (pat,),
            )
            for row in cur.fetchall() or []:
                u = str(row.get('username') or '').strip()
                if not u or u.lower() in seen:
                    continue
                seen.add(u.lower())
                out.append({'username': u, 'label': u})
    except Exception as e:
        logger.exception('mdt_crew_user_search')
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()
    return jsonify({'results': out[:20]}), 200


@internal.route('/api/mdt/<callsign>/running_call', methods=['POST', 'OPTIONS'])
def mdt_running_call(callsign):
    """
    MDT self-declares a running call: new CAD, unit linked and on scene (same mechanics as new panic CAD,
    without grade-1 panic / listen-in).
    """
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400
    now_ts = time.time()
    last = _mdt_running_call_last_ts.get(cs_raw, 0)
    if now_ts - last < 20:
        return jsonify({'error': 'Running call can only be declared once every 20 seconds'}), 429

    payload = request.get_json(silent=True) or {}
    lat_in = payload.get('latitude', payload.get('lat'))
    lng_in = payload.get('longitude', payload.get('lng'))

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        if not _mdt_jwt_allowed_for_callsign_crew(cur, cs):
            return jsonify({'error': 'JWT user is not on this unit crew'}), 403

        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_udiv = cur.fetchone() is not None
        div_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_udiv else "'general' AS division"
        cur.execute(
            f"SELECT crew, {div_sql} FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
            (cs,),
        )
        urow = cur.fetchone()
        if not urow:
            return jsonify({'error': 'Unit is not signed on'}), 403
        division = _normalize_division(urow.get('division'), fallback='general')
        crew_signed_on_labels = _mdt_signed_on_crew_display_labels(urow.get('crew'))

        coords = {}
        try:
            la = float(lat_in) if lat_in is not None else None
            ln = float(lng_in) if lng_in is not None else None
            if la is not None and ln is not None and -90 <= la <= 90 and -180 <= ln <= 180:
                coords = {
                    'lat': la, 'lng': ln, 'latitude': la, 'longitude': ln,
                    'source': 'mdt_running_call',
                }
        except (TypeError, ValueError):
            pass
        if not coords:
            cur.execute(
                """
                SELECT latitude, longitude FROM mdt_positions
                WHERE callSign = %s ORDER BY recorded_at DESC LIMIT 1
                """,
                (cs,),
            )
            prow = cur.fetchone()
            if prow and prow.get('latitude') is not None and prow.get('longitude') is not None:
                try:
                    la = float(prow['latitude'])
                    ln = float(prow['longitude'])
                    coords = {
                        'lat': la, 'lng': ln, 'latitude': la, 'longitude': ln,
                        'source': 'mdt_last_position',
                    }
                except (TypeError, ValueError):
                    pass

        ts = datetime.utcnow().isoformat() + 'Z'
        actor = _mdt_audit_actor()

        cur.execute(
            """
            SELECT ju.job_cad AS job_cad, LOWER(TRIM(COALESCE(j.status, ''))) AS st
            FROM mdt_job_units ju
            INNER JOIN mdt_jobs j ON j.cad = ju.job_cad
            WHERE ju.callsign = %s
            """,
            (cs,),
        )
        prev_jobs = list(cur.fetchall() or [])
        _ensure_job_units_table(cur)
        for row in prev_jobs:
            try:
                old_cad = int(row['job_cad'])
            except (TypeError, ValueError):
                continue
            st = str(row.get('st') or '')
            is_live = st in ('assigned', 'claimed', 'mobile', 'received')
            _mdt_detach_unit_from_job(cur, old_cad, cs, is_live, True)

        cur.execute(
            "UPDATE mdts_signed_on SET assignedIncident = NULL, status = 'on_standby' WHERE callSign = %s",
            (cs,),
        )

        new_cad = _mdt_next_allocated_job_cad(cur)
        details_bits = [f'MDT running call declared at {ts} UTC.', f'Unit: {cs}.']
        rc_data = {
            'cad': new_cad,
            'reason_for_call': 'Self-declared running call',
            'presenting_complaint': f'Running call — {cs}',
            'incident_type': 'Running call (MDT)',
            'additional_details': ' '.join(details_bits),
            'call_priority': 'Grade 3',
            'call_priority_label': 'Running call',
            'triage_category': 'Grade 3',
            'priority_source': 'mdt_running_call',
            'division': division,
            'form_slug': 'mdt_running_call',
            'form_name': 'MDT Running call',
            'coordinates': coords or {},
            'mdt_running_call_callsign': cs,
            'mdt_running_call_at': ts,
        }
        rc_data['created_by'] = str(cs or '').strip() or None

        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'created_by'")
        has_created_by = cur.fetchone() is not None
        cur.execute("SHOW COLUMNS FROM mdt_jobs LIKE 'division'")
        has_division = cur.fetchone() is not None
        data_json = json.dumps(rc_data, default=str)
        if has_created_by:
            if has_division:
                cur.execute(
                    """
                    INSERT INTO mdt_jobs (cad, created_by, status, data, division)
                    VALUES (%s, %s, 'queued', %s, %s)
                    """,
                    (new_cad, actor, data_json, division),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO mdt_jobs (cad, created_by, status, data)
                    VALUES (%s, %s, 'queued', %s)
                    """,
                    (new_cad, actor, data_json),
                )
        else:
            if has_division:
                cur.execute(
                    """
                    INSERT INTO mdt_jobs (cad, status, data, division)
                    VALUES (%s, 'queued', %s, %s)
                    """,
                    (new_cad, data_json, division),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO mdt_jobs (cad, status, data)
                    VALUES (%s, 'queued', %s)
                    """,
                    (new_cad, data_json),
                )
        try:
            if has_division:
                _ensure_job_division_snapshot_columns(cur)
                _sync_job_division_snapshot_for_slug(cur, new_cad, division)
        except Exception:
            logger.exception('division snapshot on mdt running_call cad=%s', new_cad)

        cur.execute(
            """
            INSERT INTO mdt_job_units (job_cad, callsign, assigned_by)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                assigned_by = VALUES(assigned_by),
                assigned_at = CURRENT_TIMESTAMP
            """,
            (new_cad, cs, 'mdt_running_call'),
        )
        _sync_claimed_by_from_job_units(cur, new_cad)
        cur.execute(
            """
            UPDATE mdt_jobs
               SET status = 'on_scene'
             WHERE cad = %s
               AND LOWER(TRIM(COALESCE(status, ''))) = 'queued'
            """,
            (new_cad,),
        )
        cur.execute(
            """
            UPDATE mdts_signed_on
               SET assignedIncident = %s,
                   status = 'on_scene'
             WHERE callSign = %s
            """,
            (new_cad, cs),
        )

        _ensure_job_comms_table(cur)
        comm_text = f'RUNNING CALL: {cs} declared a running call from MDT (new CAD).'
        cur.execute(
            """
            INSERT INTO mdt_job_comms (cad, message_type, sender_role, sender_user, message_text)
            VALUES (%s, 'update', 'mdt', %s, %s)
            """,
            (new_cad, cs, comm_text),
        )
        running_call_comm_id = int(getattr(cur, 'lastrowid', None) or 0)

        conn.commit()
        _mdt_running_call_last_ts[cs_raw] = now_ts
        if cs != cs_raw:
            _mdt_running_call_last_ts[cs] = now_ts

        div_label = _pretty_division_slug_label(division)
        try:
            _rc_jc = {
                'type': 'job_comm',
                'cad': new_cad,
                'message_type': 'update',
                'text': comm_text,
                'by': cs,
            }
            if running_call_comm_id > 0:
                _rc_jc['id'] = running_call_comm_id
            socketio.emit('mdt_event', _rc_jc)
            socketio.emit(
                'mdt_event',
                {
                    'type': 'running_call_declared',
                    'division': division,
                    'division_label': div_label,
                    'cad': new_cad,
                    'callsign': cs,
                    'crew_signed_on': crew_signed_on_labels,
                }
            )
            socketio.emit(
                'mdt_event', {'type': 'jobs_updated', 'cad': new_cad, 'force': True})
            socketio.emit(
                'mdt_event', {'type': 'units_updated', 'callsign': cs})
        except Exception:
            logger.exception('socket emit mdt running_call cad=%s', new_cad)
        try:
            run_priority_preemption_for_job(int(new_cad))
        except Exception:
            logger.exception(
                'priority preemption after mdt running_call cad=%s', new_cad)

        log_audit(
            actor,
            'mdt_running_call',
            details={'cad': new_cad, 'callsign': cs, 'division': division},
        )
        body = {
            'ok': True,
            'cad': new_cad,
            'callsign': cs,
            'division': division,
            'message': 'Running call CAD created — you are on scene',
        }
        if cs_raw != cs:
            body['requestedCallSign'] = cs_raw
            body['canonicalCallSign'] = cs
        return jsonify(body), 200
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


_PANIC_VOICE_MAX_BYTES = 52 * 1024 * 1024


@internal.route('/api/mdt/<callsign>/panic_voice', methods=['POST', 'OPTIONS'])
def mdt_panic_voice_upload(callsign):
    """MDT uploads panic listen-in audio (MediaRecorder) for storage on the CAD job (audit / disclosure)."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        if not _mdt_jwt_allowed_for_callsign_crew(cur, cs):
            return jsonify({'error': 'JWT user is not on this unit crew'}), 403

        cad_raw = request.form.get('cad')
        try:
            target_cad = int(cad_raw)
        except (TypeError, ValueError):
            return jsonify({'error': 'cad required'}), 400
        if target_cad <= 0:
            return jsonify({'error': 'invalid cad'}), 400

        cur.execute(
            "SELECT 1 AS ok FROM mdt_job_units WHERE job_cad = %s AND callsign = %s LIMIT 1",
            (target_cad, cs),
        )
        if not cur.fetchone():
            return jsonify({'error': 'Unit is not assigned to this CAD'}), 403

        f = request.files.get('audio')
        if not f or not getattr(f, 'filename', None):
            return jsonify({'error': 'audio file required'}), 400

        blob = f.read()
        if len(blob) > _PANIC_VOICE_MAX_BYTES:
            return jsonify({'error': 'Recording too large'}), 413
        if len(blob) < 32:
            return jsonify({'error': 'Recording empty or too small'}), 400

        mime = (f.mimetype or 'application/octet-stream').strip()[:128]
        if 'webm' in mime.lower():
            mime = 'audio/webm'
        elif not mime.startswith('audio/'):
            mime = 'audio/webm'

        started_at = str(request.form.get('started_at') or '')[:80]
        ended_at = str(request.form.get('ended_at') or '')[:80]

        digest = hashlib.sha256(blob).hexdigest()
        sub = str(target_cad)
        store_dir = os.path.join(_ventus_panic_voice_dir(), sub)
        os.makedirs(store_dir, exist_ok=True)
        fname = f'{uuid.uuid4().hex}.webm'
        rel_path = f'{sub}/{fname}'
        abs_path = os.path.join(store_dir, fname)
        with open(abs_path, 'wb') as out:
            out.write(blob)

        _ensure_mdt_job_panic_voice_table(cur)
        cur.execute(
            """
            INSERT INTO mdt_job_panic_voice
            (cad, callsign, rel_path, mime_type, file_bytes, sha256_hex, started_at, ended_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (target_cad, cs, rel_path, mime, len(blob),
             digest, started_at or None, ended_at or None),
        )
        rec_id = cur.lastrowid

        _ensure_job_comms_table(cur)
        kb = max(1, int(len(blob) / 1024 + 0.5)) if len(blob) else 0
        comm_text = (
            f'Panic voice segment #{rec_id} saved (~{kb} KB). Unit {cs}. '
            f'Use the archived list on this job to play or download.'
        )
        logger.info(
            'panic_voice_upload cad=%s rec_id=%s callsign=%s bytes=%s sha256=%s',
            target_cad,
            rec_id,
            cs,
            len(blob),
            digest,
        )
        cur.execute(
            """
            INSERT INTO mdt_job_comms (cad, message_type, sender_role, sender_user, message_text)
            VALUES (%s, 'update', 'mdt', %s, %s)
            """,
            (target_cad, cs, comm_text),
        )
        panic_voice_job_comm_id = int(getattr(cur, 'lastrowid', None) or 0)
        conn.commit()

        try:
            _pv_jc = {
                'type': 'job_comm',
                'cad': target_cad,
                'message_type': 'update',
                'text': comm_text,
                'by': cs,
            }
            if panic_voice_job_comm_id > 0:
                _pv_jc['id'] = panic_voice_job_comm_id
            socketio.emit('mdt_event', _pv_jc)
            # Dedicated push so CAD archived-audio bar can fetch immediately (not only on 5s poll).
            socketio.emit(
                'mdt_event',
                {
                    'type': 'panic_voice_segment',
                    'cad': target_cad,
                    'rec_id': rec_id,
                    'callsign': cs,
                    'bytes': len(blob),
                },
            )
        except Exception:
            logger.exception('socket emit panic_voice cad=%s', target_cad)

        return jsonify({
            'ok': True,
            'id': rec_id,
            'cad': target_cad,
            'sha256': digest,
            'bytes': len(blob),
        }), 200
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


def _cad_job_panic_voice_staff_ok():
    return _user_has_role(
        "crew",
        "dispatcher",
        "admin",
        "superuser",
        "clinical_lead",
        "call_taker",
        "calltaker",
        "controller",
        "call_handler",
        "callhandler",
    )


def _cad_panic_voice_accessible(cur, cad: int) -> bool:
    """Allow list/play when the CAD job exists or archived panic audio rows still reference this cad."""
    cur.execute("SELECT cad FROM mdt_jobs WHERE cad = %s LIMIT 1", (cad,))
    if cur.fetchone() is not None:
        return True
    _ensure_mdt_job_panic_voice_table(cur)
    cur.execute(
        "SELECT 1 AS ok FROM mdt_job_panic_voice WHERE cad = %s LIMIT 1", (cad,))
    return cur.fetchone() is not None


def _cad_resolve_panic_voice_file_row(cur, cad, rec_id):
    """Return ``(abs_path, mime, row)`` or ``(None, None, None)`` if missing or invalid."""
    _ensure_mdt_job_panic_voice_table(cur)
    cur.execute(
        "SELECT * FROM mdt_job_panic_voice WHERE id = %s AND cad = %s LIMIT 1",
        (rec_id, cad),
    )
    row = cur.fetchone()
    if not row:
        return None, None, None
    rel = str(row.get("rel_path") or "")
    if ".." in rel or rel.startswith(("/", "\\")):
        return None, None, None
    base = os.path.abspath(_ventus_panic_voice_dir())
    abs_path = os.path.abspath(os.path.join(
        base, *rel.replace("\\", "/").split("/")))
    try:
        common = os.path.commonpath([base, abs_path])
    except ValueError:
        return None, None, None
    if os.path.normcase(common) != os.path.normcase(base):
        return None, None, None
    if not os.path.isfile(abs_path):
        return None, None, None
    mime = str(row.get("mime_type") or "audio/webm")
    return abs_path, mime, row


@internal.route('/job/<int:cad>/panic-voice/<int:rec_id>', methods=['GET'])
@login_required
def cad_job_panic_voice_download(cad, rec_id):
    """Download a stored MDT panic recording (logged-in CAD / staff)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        abs_path, mime, row = _cad_resolve_panic_voice_file_row(
            cur, cad, rec_id)
        if not abs_path or not row:
            return jsonify({'error': 'Not found'}), 404
        dl = f'cad{cad}_panic_voice_{rec_id}.webm'
        return send_file(abs_path, mimetype=mime, as_attachment=True, download_name=dl)
    finally:
        cur.close()
        conn.close()


@internal.route("/job/<int:cad>/panic-voices", methods=["GET"])
@login_required
def cad_job_panic_voice_list(cad):
    """JSON list of panic segments for CAD job panel (dispatch / call centre; active or past job)."""
    if not _cad_job_panic_voice_staff_ok():
        return jsonify({"error": "Unauthorised"}), 403
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if not _cad_panic_voice_accessible(cur, cad):
            return jsonify({"error": "Job not found"}), 404
        _ensure_mdt_job_panic_voice_table(cur)
        cur.execute(
            """
            SELECT id, callsign, file_bytes, sha256_hex, started_at, ended_at, created_at, mime_type
            FROM mdt_job_panic_voice
            WHERE cad = %s
            ORDER BY id ASC
            """,
            (cad,),
        )
        rows = cur.fetchall() or []
        out = []
        for row in rows:
            ca = row.get("created_at")
            out.append(
                {
                    "id": int(row["id"]),
                    "callsign": str(row.get("callsign") or ""),
                    "file_bytes": int(row.get("file_bytes") or 0),
                    "sha256_hex": row.get("sha256_hex"),
                    "started_at": row.get("started_at"),
                    "ended_at": row.get("ended_at"),
                    "created_at": ca.isoformat() if hasattr(ca, "isoformat") else (str(ca) if ca else None),
                    "mime_type": str(row.get("mime_type") or "audio/webm"),
                }
            )
        return jsonify({"recordings": out}), 200
    finally:
        cur.close()
        conn.close()


@internal.route("/job/<int:cad>/panic-voice/<int:rec_id>/play", methods=["GET", "HEAD"])
@login_required
def cad_job_panic_voice_play(cad, rec_id):
    """Stream panic audio in-browser for CAD (session cookie); use with HTML5 audio."""
    if not _cad_job_panic_voice_staff_ok():
        return jsonify({"error": "Unauthorised"}), 403
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if not _cad_panic_voice_accessible(cur, cad):
            return jsonify({"error": "Job not found"}), 404
        abs_path, mime, row = _cad_resolve_panic_voice_file_row(
            cur, cad, rec_id)
        if not abs_path or not row:
            return jsonify({"error": "Not found"}), 404
        dl = f"cad{cad}_panic_voice_{rec_id}.webm"
        resp = send_file(abs_path, mimetype=mime,
                         as_attachment=False, download_name=dl, conditional=True)
        resp.headers["Cache-Control"] = "private, max-age=0"
        return resp
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/<callsign>/job/<int:cad>/panic_voice', methods=['GET', 'OPTIONS'])
def mdt_job_panic_voice_list(callsign, cad):
    """List stored panic voice segments for a CAD (MDT crew with JWT; past / closed jobs)."""
    if request.method == 'OPTIONS':
        return '', 200
    if cad <= 0:
        return jsonify({'error': 'invalid cad'}), 400
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        if not _mdt_jwt_allowed_for_callsign_crew(cur, cs):
            return jsonify({'error': 'JWT user is not on this unit crew'}), 403
        _ensure_mdt_job_panic_voice_table(cur)
        scope = _mdt_panic_voice_access_scope(cur, cs, cad)
        if not scope:
            return jsonify({'error': 'No access to this CAD'}), 403
        if scope == 'all':
            cur.execute(
                """
                SELECT id, callsign, file_bytes, sha256_hex, started_at, ended_at, created_at, mime_type
                FROM mdt_job_panic_voice
                WHERE cad = %s
                ORDER BY id ASC
                """,
                (cad,),
            )
        else:
            cur.execute(
                """
                SELECT id, callsign, file_bytes, sha256_hex, started_at, ended_at, created_at, mime_type
                FROM mdt_job_panic_voice
                WHERE cad = %s AND callsign = %s
                ORDER BY id ASC
                """,
                (cad, cs),
            )
        rows = cur.fetchall() or []
        out = []
        for row in rows:
            ca = row.get('created_at')
            out.append({
                'id': int(row['id']),
                'callsign': str(row.get('callsign') or ''),
                'file_bytes': int(row.get('file_bytes') or 0),
                'sha256_hex': row.get('sha256_hex'),
                'started_at': row.get('started_at'),
                'ended_at': row.get('ended_at'),
                'created_at': ca.isoformat() if hasattr(ca, 'isoformat') else (str(ca) if ca else None),
                'mime_type': str(row.get('mime_type') or 'audio/webm'),
            })
        return jsonify({'recordings': out}), 200
    finally:
        cur.close()
        conn.close()


@internal.route(
    '/api/mdt/<callsign>/job/<int:cad>/panic_voice/<int:rec_id>/stream',
    methods=['GET', 'OPTIONS', 'HEAD'],
)
def mdt_job_panic_voice_stream(callsign, cad, rec_id):
    """Stream a panic voice file for in-app playback (JWT crew); not attachment."""
    if request.method == 'OPTIONS':
        return '', 200
    if cad <= 0 or rec_id <= 0:
        return jsonify({'error': 'invalid request'}), 400
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        if not _mdt_jwt_allowed_for_callsign_crew(cur, cs):
            return jsonify({'error': 'JWT user is not on this unit crew'}), 403
        _ensure_mdt_job_panic_voice_table(cur)
        scope = _mdt_panic_voice_access_scope(cur, cs, cad)
        if not scope:
            return jsonify({'error': 'No access to this CAD'}), 403
        cur.execute(
            "SELECT * FROM mdt_job_panic_voice WHERE id = %s AND cad = %s LIMIT 1",
            (rec_id, cad),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Not found'}), 404
        if scope == 'own' and str(row.get('callsign') or '').strip().upper() != cs:
            return jsonify({'error': 'Forbidden'}), 403
        rel = str(row.get('rel_path') or '')
        if '..' in rel or rel.startswith(('/', '\\')):
            return jsonify({'error': 'Invalid path'}), 400
        base = os.path.abspath(_ventus_panic_voice_dir())
        abs_path = os.path.abspath(os.path.join(
            base, *rel.replace('\\', '/').split('/')))
        try:
            common = os.path.commonpath([base, abs_path])
        except ValueError:
            return jsonify({'error': 'Invalid path'}), 400
        if os.path.normcase(common) != os.path.normcase(base):
            return jsonify({'error': 'Invalid path'}), 400
        if not os.path.isfile(abs_path):
            return jsonify({'error': 'File missing'}), 404
        mime = str(row.get('mime_type') or 'audio/webm')
        dl = f'cad{cad}_panic_voice_{rec_id}.webm'
        resp = send_file(abs_path, mimetype=mime,
                         as_attachment=False, download_name=dl, conditional=True)
        resp.headers['Cache-Control'] = 'private, max-age=0'
        return resp
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/<callsign>/crew', methods=['GET', 'POST', 'OPTIONS'])
def mdt_update_crew_legacy(callsign):
    """Legacy alias: get/update signed-on unit crew list."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        if request.method == 'GET':
            cur.execute(
                "SELECT crew, signOnTime FROM mdts_signed_on WHERE callSign = %s ORDER BY signOnTime DESC LIMIT 1",
                (cs,)
            )
            row = cur.fetchone()
            if not row:
                return jsonify({'error': 'callsign not signed on'}), 404
            crew_raw = row.get('crew')
            raw_list = []
            try:
                if isinstance(crew_raw, (bytes, bytearray)):
                    crew_raw = crew_raw.decode('utf-8', errors='ignore')
                if isinstance(crew_raw, str):
                    raw_list = json.loads(crew_raw) if crew_raw else []
                elif isinstance(crew_raw, list):
                    raw_list = crew_raw
            except Exception:
                raw_list = []
            crew = _normalize_crew_to_objects(raw_list, row.get('signOnTime'))
            return _jsonify_safe({'crew': crew}, 200)

        payload = request.get_json(silent=True) or {}
        cur.execute(
            "SELECT crew, signOnTime FROM mdts_signed_on WHERE callSign = %s ORDER BY signOnTime DESC LIMIT 1",
            (cs,)
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'callsign not signed on'}), 404

        add_username = str(payload.get('username') or '').strip()
        if add_username:
            # Add single crew member: must be a valid contractor or in mdt_crew_profiles
            if not _is_valid_crew_username(cur, add_username):
                return jsonify({'error': 'Crew member not found. Must be a core user (username/email) or active contractor.', 'username': add_username}), 400
            raw_list = []
            try:
                r = row.get('crew')
                if isinstance(r, str):
                    raw_list = json.loads(r) if r else []
                elif isinstance(r, list):
                    raw_list = r
            except Exception:
                raw_list = []
            crew_objs = _normalize_crew_to_objects(
                raw_list, row.get('signOnTime'))
            if any(c.get('username', '').lower() == add_username.lower() for c in crew_objs):
                return _jsonify_safe({'success': True, 'crew': crew_objs}, 200)
            now = datetime.utcnow()
            signed_on_iso = now.isoformat() + 'Z'
            add_grade = str(payload.get('grade') or payload.get(
                'role') or '').strip() or None
            crew_objs.append(
                {'username': add_username, 'signedOnAt': signed_on_iso, 'grade': add_grade})
            cur.execute(
                "UPDATE mdts_signed_on SET crew = %s WHERE callSign = %s",
                (json.dumps(crew_objs), cs)
            )
            conn.commit()
            log_audit(
                _mdt_audit_actor(),
                'mdt_crew_add',
                details={'callsign': cs, 'username': add_username},
            )
            return _jsonify_safe({'success': True, 'crew': crew_objs}, 200)

        # Legacy: replace whole crew from payload.crew
        if 'crew' not in payload:
            return jsonify({'error': 'crew or username required'}), 400
        crew_raw = payload.get('crew')
        crew = []
        if isinstance(crew_raw, str):
            crew = [crew_raw]
        elif isinstance(crew_raw, list):
            crew = crew_raw
        crew = [str(x).strip() for x in crew if str(x).strip()]
        removed = str(payload.get('removed') or '').strip()
        prev_usernames_lc = set(_mdt_signed_on_crew_usernames(row.get('crew')))
        jwt_mu = getattr(g, 'mdt_user', None) or {}
        jwt_un = str(jwt_mu.get('username') or '').strip().lower()
        crew_lc_set = {str(x).strip().lower() for x in crew}
        if not crew and prev_usernames_lc:
            return jsonify({
                'error': 'crew_replace_invalid',
                'message': (
                    'That update would clear the entire crew list. '
                    'To leave the vehicle, use sign-off; to remove one person, use remove crew '
                    'rather than sending an empty list.'
                ),
            }), 400
        # MDT CrewManager used to POST only "additional" usernames; the JWT operator was omitted
        # and could be wiped when removing the last extra member. If this is a removal payload,
        # put the operator back on the roster when they were still on the unit before the update.
        if (
            removed
            and jwt_un
            and jwt_un in prev_usernames_lc
            and jwt_un not in crew_lc_set
            and crew
        ):
            jwt_display = str(jwt_mu.get('username') or jwt_un).strip()
            if jwt_display:
                crew.insert(0, jwt_display)
                crew_lc_set.add(jwt_un)
        invalid_crew, crew_err = _validate_crew_usernames(cur, crew)
        if invalid_crew:
            return jsonify({'error': crew_err or 'Invalid crew member(s)', 'unknown_crew': invalid_crew}), 400

        removal_reason = str(payload.get('removal_reason') or '').strip()
        if removed and removal_reason:
            normalized_reason = _normalize_mdt_crew_removal_reason_from_mdt(
                removal_reason)
            _ensure_crew_removal_log_table(cur)
            cur.execute("""
                INSERT INTO mdt_crew_removal_log
                    (callSign, removed_username, removal_reason, removed_by, removed_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, (cs, removed, normalized_reason, cs))

        cur.execute(
            "UPDATE mdts_signed_on SET crew = %s WHERE callSign = %s",
            (json.dumps(crew), cs)
        )
        conn.commit()
        crew_objs = _normalize_crew_to_objects(crew, row.get('signOnTime'))
        log_audit(
            _mdt_audit_actor(),
            'mdt_crew_replace',
            details={'callsign': cs, 'crew_count': len(
                crew), 'removed': removed or None},
        )
        return _jsonify_safe({'success': True, 'crew': crew_objs}, 200)
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/<callsign>/division', methods=['POST', 'OPTIONS'])
def mdt_set_division(callsign):
    """Update the operating area for a signed-on unit. Broadcasts to dispatcher/CAD."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400
    payload = request.get_json(silent=True) or {}
    division = _normalize_division(payload.get('division'), fallback='')
    if not division:
        return jsonify({'error': 'division required'}), 400
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        cur.execute(
            "SELECT callSign FROM mdts_signed_on WHERE callSign = %s LIMIT 1", (cs,))
        if not cur.fetchone():
            return jsonify({'error': 'Unit not signed on'}), 404
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        if not cur.fetchone():
            return jsonify({'error': 'Division not supported'}), 500
        cur.execute(
            "UPDATE mdts_signed_on SET division = %s WHERE callSign = %s", (division, cs))
        conn.commit()
        log_audit(
            _mdt_audit_actor(),
            'mdt_division_change',
            details={'callsign': cs, 'division': division},
        )
        try:
            socketio.emit(
                'mdt_event', {'type': 'units_updated', 'callsign': cs})
        except Exception:
            pass
        return _jsonify_safe({'success': True, 'division': division}, 200)
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/<callsign>/crew/<username>', methods=['DELETE', 'OPTIONS'])
def mdt_remove_crew_member(callsign, username):
    """Remove a crew member from the active session. Body: { reason, notes }. Logged for audit."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    uname = str(username or '').strip()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400
    if not uname:
        return jsonify({'error': 'username required'}), 400
    payload = request.get_json(silent=True) or {}
    reason = str(payload.get('reason') or 'other').strip().lower()
    allowed = {'illness', 'early_finish', 'reassigned', 'other'}
    if reason not in allowed:
        reason = 'other'
    notes = str(payload.get('notes') or '').strip()[:512]
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        cur.execute(
            "SELECT crew, signOnTime FROM mdts_signed_on WHERE callSign = %s ORDER BY signOnTime DESC LIMIT 1",
            (cs,)
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Unit not signed on'}), 404
        raw_list = []
        try:
            r = row.get('crew')
            if isinstance(r, str):
                raw_list = json.loads(r) if r else []
            elif isinstance(r, list):
                raw_list = r
        except Exception:
            raw_list = []
        crew_objs = _dedupe_crew_objects_preserve_order(
            _normalize_crew_to_objects(raw_list, row.get('signOnTime')))
        before_len = len(crew_objs)
        crew_objs = [c for c in crew_objs if str(
            c.get('username') or '').strip().lower() != uname.lower()]
        if len(crew_objs) == before_len:
            return _jsonify_safe({'success': True, 'crew': crew_objs}, 200)
        _ensure_crew_removal_log_table(cur)
        has_notes = False
        try:
            cur.execute("SHOW COLUMNS FROM mdt_crew_removal_log LIKE 'notes'")
            has_notes = cur.fetchone() is not None
        except Exception:
            pass
        if has_notes:
            cur.execute("""
                INSERT INTO mdt_crew_removal_log
                    (callSign, removed_username, removal_reason, removed_by, removed_at, notes)
                VALUES (%s, %s, %s, %s, NOW(), %s)
            """, (cs, uname, reason, cs, notes or None))
        else:
            cur.execute("""
                INSERT INTO mdt_crew_removal_log
                    (callSign, removed_username, removal_reason, removed_by, removed_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, (cs, uname, reason, cs))
        cur.execute(
            "UPDATE mdts_signed_on SET crew = %s, lastSeenAt = NOW() WHERE callSign = %s",
            (json.dumps(crew_objs), cs)
        )
        conn.commit()
        log_audit(
            _mdt_audit_actor(),
            'mdt_crew_remove',
            details={'callsign': cs,
                     'removed_username': uname, 'reason': reason},
        )
        return _jsonify_safe({'success': True, 'crew': crew_objs}, 200)
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


def _load_latest_destination(cur, callsign):
    _ensure_standby_tables(cur)
    cur.execute("SHOW TABLES LIKE 'standby_locations'")
    if cur.fetchone() is None:
        return None
    cur.execute("""
        SELECT id, name, lat, lng, source, destinationType, activationMode, state, cad,
               what3words, address, instructionId, activatedAt, updatedAt
        FROM standby_locations
        WHERE callSign = %s
        ORDER BY updatedAt DESC, id DESC
        LIMIT 1
    """, (str(callsign or '').strip().upper(),))
    row = cur.fetchone()
    if not row:
        return None
    return {
        'id': row.get('id'),
        'name': row.get('name'),
        'lat': float(row.get('lat')) if row.get('lat') is not None else None,
        'lng': float(row.get('lng')) if row.get('lng') is not None else None,
        'source': row.get('source'),
        'destination_type': row.get('destinationType') or 'standby',
        'activation_mode': row.get('activationMode') or 'immediate',
        'state': row.get('state') or 'active',
        'cad': row.get('cad'),
        'what3words': row.get('what3words'),
        'address': row.get('address'),
        'instruction_id': row.get('instructionId'),
        'activated_at': row.get('activatedAt'),
        'updated_at': row.get('updatedAt')
    }


@internal.route('/api/mdt/<callsign>/standby', methods=['GET', 'OPTIONS'])
def mdt_standby_legacy(callsign):
    """Legacy alias: fetch latest standby assignment for a callsign."""
    if request.method == 'OPTIONS':
        return '', 200
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, str(callsign or '').strip().upper())
        row = _load_latest_destination(cur, cs)
        if row is None:
            return _jsonify_safe({'callsign': cs, 'standby': None}, 200)
        return _jsonify_safe({
            'callsign': cs,
            'standby': row
        }, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/<callsign>/destination', methods=['GET', 'OPTIONS'])
def mdt_destination_legacy(callsign):
    """Fetch latest unit destination (transport/standby) for MDT recovery after restart."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return _jsonify_safe({'error': 'callsign required'}, 400)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        destination = _load_latest_destination(cur, cs)
        return _jsonify_safe({'callsign': cs, 'destination': destination}, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/<callsign>/destination/override', methods=['POST', 'OPTIONS'])
def mdt_destination_override_legacy(callsign):
    """Crew override for destination changes (clinical or operational)."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return _jsonify_safe({'error': 'callsign required'}, 400)

    payload = request.get_json(silent=True) or {}
    name = str(payload.get('name') or payload.get(
        'location_name') or '').strip()
    source = 'crew_override'
    reason = str(payload.get('reason') or payload.get('override_reason')
                 or 'clinical_override').strip() or 'clinical_override'
    notes = str(payload.get('notes') or payload.get(
        'override_notes') or '').strip()
    address = str(payload.get('address') or payload.get(
        'location_text') or '').strip()
    what3words = str(payload.get('what3words')
                     or payload.get('w3w') or '').strip()
    postcode = str(payload.get('postcode') or '').strip()
    raw_lat = payload.get('lat', payload.get('latitude'))
    raw_lng = payload.get('lng', payload.get('longitude'))
    try:
        lat = float(raw_lat)
        lng = float(raw_lng)
    except Exception:
        lat = None
        lng = None
    if lat is None or lng is None:
        resolved = ResponseTriage.get_best_lat_lng(
            address=address or None,
            postcode=postcode or None,
            what3words=what3words or None
        )
        if isinstance(resolved, dict) and resolved.get('lat') is not None and resolved.get('lng') is not None:
            try:
                lat = float(resolved.get('lat'))
                lng = float(resolved.get('lng'))
            except Exception:
                lat = None
                lng = None
    if lat is None or lng is None:
        return _jsonify_safe({'error': 'lat/lng required (or resolvable address/what3words/postcode)'}, 400)
    if not name:
        name = 'Crew Override Destination'

    cad = payload.get('cad')
    try:
        cad = int(cad) if cad not in (None, '', 'null') else None
    except Exception:
        cad = None

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        _ensure_standby_tables(cur)
        _ensure_job_comms_table(cur)
        cur.execute(
            "SELECT assignedIncident FROM mdts_signed_on WHERE callSign = %s ORDER BY signOnTime DESC LIMIT 1", (cs,))
        live = cur.fetchone() or {}
        if not live:
            return _jsonify_safe({'error': 'callsign not signed on'}, 404)
        if cad is None:
            try:
                cad = int(live.get('assignedIncident')) if live.get(
                    'assignedIncident') is not None else None
            except Exception:
                cad = None

        instruction_payload = {
            'name': name,
            'lat': lat,
            'lng': lng,
            'source': source,
            'destination_type': 'transport',
            'activation_mode': 'immediate',
            'state': 'active',
            'cad': cad,
            'reason': reason,
            'notes': notes or None,
            'what3words': what3words or None,
            'address': address or None,
            'postcode': postcode or None
        }
        cur.execute("""
            INSERT INTO mdt_dispatch_instructions (callSign, instruction_type, payload, status, created_by, acked_at)
            VALUES (%s, %s, %s, 'acked', %s, NOW())
        """, (cs, 'destination_override', json.dumps(instruction_payload), cs))
        instruction_id = cur.lastrowid
        _supersede_stale_destination_instructions(cur, cs, instruction_id)

        cur.execute("""
            INSERT INTO standby_locations
              (callSign, name, lat, lng, source, destinationType, activationMode, state, cad,
               what3words, address, instructionId, activatedAt, updatedBy)
            VALUES (%s, %s, %s, %s, %s, 'transport', 'immediate', 'active', %s, %s, %s, %s, NOW(), %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                lat = VALUES(lat),
                lng = VALUES(lng),
                source = VALUES(source),
                destinationType = 'transport',
                activationMode = 'immediate',
                state = 'active',
                cad = VALUES(cad),
                what3words = VALUES(what3words),
                address = VALUES(address),
                instructionId = VALUES(instructionId),
                activatedAt = NOW(),
                updatedBy = VALUES(updatedBy),
                updatedAt = CURRENT_TIMESTAMP
        """, (cs, name, lat, lng, source, cad, what3words or None, address or None, instruction_id, cs))

        override_text = f"Destination override by {cs}: {name} ({lat:.6f},{lng:.6f}) reason={reason}"
        if notes:
            override_text = f"{override_text} | notes={notes}"
        if cad is not None:
            cur.execute("""
                INSERT INTO mdt_job_comms (cad, message_type, sender_role, sender_user, message_text)
                VALUES (%s, 'update', 'crew', %s, %s)
            """, (cad, cs, override_text))

        cur.execute("SHOW TABLES LIKE 'messages'")
        has_messages = cur.fetchone() is not None
        if has_messages:
            cur.execute("""
                INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                VALUES (%s, 'dispatcher', %s, NOW(), 0)
            """, (cs, override_text))
        conn.commit()
        log_audit(
            _mdt_audit_actor(),
            'mdt_destination_override',
            details={
                'callsign': cs,
                'cad': cad,
                'reason': reason,
                'name': name,
            },
        )
    finally:
        cur.close()
        conn.close()

    try:
        socketio.emit('mdt_event', {
            'type': 'destination_override',
            'callsign': cs,
            'cad': cad,
            'instruction_id': instruction_id,
            'reason': reason,
            'name': name,
            'lat': lat,
            'lng': lng
        })
        socketio.emit('mdt_event', {
            'type': 'message_posted',
            'from': cs,
            'to': 'dispatcher',
            'text': override_text
        })
        socketio.emit('mdt_event', {'type': 'units_updated', 'callsign': cs})
        if cad is not None:
            socketio.emit('mdt_event', {
                          'type': 'job_update', 'cad': cad, 'text': override_text, 'by': cs, 'units': [cs]})
            socketio.emit('mdt_event', {'type': 'jobs_updated', 'cad': cad})
    except Exception:
        pass

    return _jsonify_safe({
        'message': 'Destination override accepted',
        'callsign': cs,
        'cad': cad,
        'instruction_id': instruction_id,
        'destination': {
            'name': name,
            'lat': lat,
            'lng': lng,
            'source': source,
            'destination_type': 'transport',
            'activation_mode': 'immediate',
            'state': 'active',
            'reason': reason,
            'notes': notes or None,
            'what3words': what3words or None,
            'address': address or None
        }
    }, 200)


@internal.route('/api/mdt/<callsign>/motd', methods=['GET', 'OPTIONS'])
def mdt_motd_for_unit(callsign):
    """Dispatch message-of-the-day for MDT (same source as CAD /dispatch/motd); JWT + device session."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return _jsonify_safe({'error': 'callsign required'}, 400)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_dispatch_settings_table(cur)
        base = _get_dispatch_motd(cur)
        text = str(base.get('text') or '')
        ub = base.get('updated_by')
        ua = base.get('updated_at')
        ua_key = ''
        if ua is not None:
            ua_key = ua.isoformat() if hasattr(ua, 'isoformat') else str(ua)
        ver_src = f"{ub or ''}\x1f{ua_key}\x1f{text}".encode('utf-8', errors='replace')
        version = hashlib.sha256(ver_src).hexdigest()[:40]
        return _jsonify_safe({
            'text': text,
            'updated_by': ub,
            'updated_at': ua,
            'version': version,
        }, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/<callsign>/instructions', methods=['GET', 'OPTIONS'])
def mdt_instructions_legacy(callsign):
    """Fetch pending dispatch instructions for MDT clients."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return _jsonify_safe({'error': 'callsign required'}, 400)
    limit_raw = request.args.get('limit', 20)
    try:
        limit = max(1, min(100, int(limit_raw)))
    except Exception:
        limit = 20
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        _ensure_standby_tables(cur)
        cur.execute("""
            SELECT id, callSign, instruction_type, payload, status, created_by, created_at, acked_at
            FROM mdt_dispatch_instructions
            WHERE callSign = %s
              AND status IN ('pending', 'sent')
            ORDER BY created_at ASC, id ASC
            LIMIT %s
        """, (cs, limit))
        rows = cur.fetchall() or []
        out = []
        for row in rows:
            payload = row.get('payload')
            try:
                if isinstance(payload, (bytes, bytearray)):
                    payload = payload.decode('utf-8', errors='ignore')
                if isinstance(payload, str):
                    payload = json.loads(payload) if payload else {}
            except Exception:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            out.append({
                'id': row.get('id'),
                'callSign': row.get('callSign'),
                'instruction_type': row.get('instruction_type'),
                'payload': payload,
                'status': row.get('status'),
                'created_by': row.get('created_by'),
                'created_at': row.get('created_at'),
                'acked_at': row.get('acked_at')
            })
        # Mark pending as sent once surfaced to client poll.
        pending_ids = [r.get('id') for r in rows if str(
            r.get('status') or '').lower() == 'pending']
        if pending_ids:
            placeholders = ','.join(['%s'] * len(pending_ids))
            cur.execute(
                f"UPDATE mdt_dispatch_instructions SET status = 'sent' WHERE id IN ({placeholders})",
                tuple(pending_ids)
            )
            conn.commit()
        return _jsonify_safe({'callsign': cs, 'instructions': out}, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/<callsign>/instructions/ack', methods=['POST', 'OPTIONS'])
def mdt_instructions_ack_legacy(callsign):
    """Acknowledge one or more dispatch instructions by id."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return _jsonify_safe({'error': 'callsign required'}, 400)
    payload = request.get_json(silent=True) or {}
    ids = payload.get('ids')
    if ids is None:
        single_id = payload.get('id')
        ids = [single_id] if single_id is not None else []
    if not isinstance(ids, list):
        ids = [ids]
    clean_ids = []
    for i in ids:
        try:
            clean_ids.append(int(i))
        except Exception:
            continue
    clean_ids = list(dict.fromkeys([i for i in clean_ids if i > 0]))
    if not clean_ids:
        return _jsonify_safe({'error': 'instruction id(s) required'}, 400)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        _ensure_standby_tables(cur)
        placeholders = ','.join(['%s'] * len(clean_ids))
        params = [cs] + clean_ids
        cur.execute(
            f"""
            UPDATE mdt_dispatch_instructions
            SET status = 'acked',
                acked_at = NOW()
            WHERE callSign = %s
              AND id IN ({placeholders})
            """,
            tuple(params)
        )
        updated = cur.rowcount or 0
        conn.commit()
        if updated:
            log_audit(
                _mdt_audit_actor(),
                'mdt_instruction_ack',
                details={'callsign': cs, 'acked': int(
                    updated), 'ids': clean_ids},
            )
        return _jsonify_safe({'callsign': cs, 'acked': int(updated), 'ids': clean_ids}, 200)
    finally:
        cur.close()
        conn.close()


# Unit-row statuses accepted on POST /api/mdt/<callsign>/status (mdts_signed_on), including while
# still assigned to an incident (e.g. meal break). Keep aligned with Ventus MDT client
# (vue-connector IDLE_UNIT_STATUS_FOR_POST + running_call on unit row; incident ladder uses
# POST /api/mdt/<cad>/status only). 'available' / 'idle' in body are normalized to on_standby below.
_MDT_UNIT_SELF_SERVICE_STATUSES = frozenset({
    'on_standby', 'on_station', 'at_station',
    'mobile_to_station', 'mobile_to_standby',
    'meal_break', 'running_call', 'out_of_service',
})


def _mdt_unit_status_self_service_post(callsign):
    """POST: update mdts_signed_on.status for MDT idle / unit statuses (GET polling reads this row)."""
    payload = request.get_json() or {}
    new_status = str(payload.get('status') or '').strip().lower()
    if new_status in ('available', 'idle'):
        new_status = 'on_standby'
    if new_status not in _MDT_UNIT_SELF_SERVICE_STATUSES:
        return jsonify({'error': 'invalid unit status', 'status': new_status}), 400

    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400
    body_cs = str(payload.get('callSign') or payload.get(
        'callsign') or '').strip().upper()
    if body_cs and body_cs != cs_raw:
        return jsonify({'error': 'callsign mismatch between URL and body'}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_response_log_table(cur)
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        _ensure_meal_break_columns(cur)
        cur.execute(
            "SELECT callSign, assignedIncident FROM mdts_signed_on WHERE callSign = %s LIMIT 1",
            (cs,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'not signed on', 'callsign': cs}), 404

        minutes = _parse_int(
            payload.get('meal_break_minutes') or payload.get('minutes'),
            fallback=30,
            min_value=5,
            max_value=180,
        )
        if new_status == 'meal_break':
            cur.execute(
                """
                UPDATE mdts_signed_on
                   SET status = 'meal_break',
                       mealBreakStartedAt = NOW(),
                       mealBreakUntil = DATE_ADD(NOW(), INTERVAL %s MINUTE),
                       mealBreakTakenAt = COALESCE(mealBreakTakenAt, NOW())
                 WHERE callSign = %s
                """,
                (minutes, cs),
            )
        else:
            cur.execute(
                """
                UPDATE mdts_signed_on
                   SET status = %s,
                       mealBreakStartedAt = NULL,
                       mealBreakUntil = NULL
                 WHERE callSign = %s
                """,
                (new_status, cs),
            )

        ainc = row.get('assignedIncident')
        try:
            ainc_int = int(ainc) if ainc is not None and str(
                ainc).strip() != '' else None
        except Exception:
            ainc_int = None
        if not log_audit(
            _mdt_audit_actor(),
            'mdt_unit_self_status',
            details={'callsign': cs, 'status': new_status,
                     'assigned_incident': ainc_int},
            audit_failure='fail_closed',
        ):
            conn.rollback()
            return _ventus_audit_unavailable_response()
        conn.commit()
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()

    try:
        socketio.emit(
            'mdt_event',
            {'type': 'status_update', 'callsign': cs, 'status': new_status}
        )
        socketio.emit('mdt_event', {'type': 'units_updated', 'callsign': cs})
    except Exception:
        pass

    ok_body = {'message': 'unit status updated',
               'callsign': cs, 'callSign': cs, 'status': new_status}
    ok_body.update(_mdt_callsign_client_hints(cs_raw, cs))
    return _jsonify_safe(ok_body, 200)


@internal.route('/api/mdt/<callsign>/status', methods=['GET', 'POST', 'OPTIONS'])
def mdt_unit_status_legacy(callsign):
    """Return current live unit status for MDT polling (GET); set unit idle/meal/etc. status (POST)."""
    if request.method == 'OPTIONS':
        return '', 200
    if request.method == 'POST':
        return _mdt_unit_status_self_service_post(callsign)
    cs_raw = str(callsign or '').strip().upper()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs = _mdt_resolve_callsign(cur, cs_raw)
        _ensure_meal_break_columns(cur)
        _ventus_set_g_break_policy(cur)
        cur.execute("SHOW COLUMNS FROM mdts_signed_on LIKE 'division'")
        has_division = cur.fetchone() is not None
        div_sql = "LOWER(TRIM(COALESCE(division, 'general'))) AS division" if has_division else "'general' AS division"
        cur.execute(f"""
            SELECT callSign, status, assignedIncident, signOnTime, lastSeenAt,
                   mealBreakStartedAt, mealBreakUntil, mealBreakTakenAt,
                   shiftStartAt, shiftEndAt, shiftDurationMins, breakDueAfterMins, {div_sql}
            FROM mdts_signed_on
            WHERE callSign = %s
            LIMIT 1
        """, (cs,))
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'not signed on', 'callsign': cs}), 404
        if _mdt_session_stale_for_auto_signoff(row):
            _ensure_job_units_table(cur)
            try:
                crew_stale = list(_mdt_signed_on_crew_usernames(row.get('crew')))
                div_stale = str(row.get('division') or 'general').strip().lower() or 'general'
                affected_cads, _stale_sign_on_time = _mdt_sign_off_unit_sql(cur, cs)
                _record_mdt_session_lifecycle_event(
                    cur,
                    event_type='session_expired_sign_off',
                    callsign=cs,
                    division=div_stale,
                    actor_username=None,
                    previous_callsign=None,
                    crew_usernames=crew_stale,
                    detail={
                        'affected_cads': sorted(affected_cads),
                        'reason': 'session_idle_or_shift_ended',
                    },
                )
                try:
                    cur.execute(
                        "DELETE FROM mdt_callsign_redirect WHERE from_callsign = %s OR to_callsign = %s",
                        (cs, cs),
                    )
                except Exception:
                    pass
                log_audit(
                    _mdt_audit_actor(),
                    'mdt_session_expired',
                    details={
                        'callsign': cs,
                        'affected_cads': sorted(affected_cads),
                    },
                    audit_failure='siem_fallback',
                )
                conn.commit()
            except Exception as ex:
                conn.rollback()
                logger.exception('mdt_session_expired purge failed cs=%s', cs)
                return _ventus_public_server_error(ex)
            try:
                from app.plugins.time_billing_module.ventus_integration import on_ventus_sign_off
                on_ventus_sign_off(callsign=cs)
            except Exception:
                pass
            try:
                socketio.emit(
                    'mdt_event', {'type': 'unit_signoff', 'callsign': cs})
                socketio.emit(
                    'mdt_event', {'type': 'units_updated', 'callsign': cs})
                for cad in sorted(affected_cads):
                    socketio.emit('mdt_event', {'type': 'jobs_updated', 'cad': cad})
            except Exception:
                pass
            return jsonify({'error': 'not signed on', 'reason': 'session_expired', 'callsign': cs}), 404
        shift_state = _compute_shift_break_state(row)
        destination = _load_latest_destination(cur, cs)
        shift_start_at = shift_state.get('shift_start_at')
        shift_end_at = shift_state.get('shift_end_at')
        shift_duration_minutes = shift_state.get('shift_duration_minutes')
        shift_hours = (float(shift_duration_minutes) /
                       60.0) if shift_duration_minutes is not None else None
        status_payload = {
            'callsign': cs,
            'status': str(row.get('status') or '').strip().lower(),
            'assigned_incident': row.get('assignedIncident'),
            'last_seen_at': row.get('lastSeenAt'),
            'meal_break_started_at': row.get('mealBreakStartedAt'),
            'meal_break_until': row.get('mealBreakUntil'),
            'meal_break_taken_at': row.get('mealBreakTakenAt'),
            'meal_break_remaining_seconds': shift_state.get('meal_break_remaining_seconds'),
            'meal_break_active': shift_state.get('meal_break_active'),
            'shift_start': shift_start_at.isoformat() + 'Z' if shift_start_at and hasattr(shift_start_at, 'isoformat') else None,
            'shift_end': shift_end_at.isoformat() + 'Z' if shift_end_at and hasattr(shift_end_at, 'isoformat') else None,
            'shift_hours': shift_hours,
            'shift_duration_minutes': shift_duration_minutes,
            'break_due_after_minutes': shift_state.get('break_due_after_minutes'),
            'shift_start_at': shift_start_at,
            'shift_end_at': shift_end_at,
            'shift_elapsed_minutes': shift_state.get('shift_elapsed_minutes'),
            'shift_remaining_minutes': shift_state.get('shift_remaining_minutes'),
            'break_due_in_minutes': shift_state.get('break_due_in_minutes'),
            'break_due': shift_state.get('break_due'),
            'near_break': shift_state.get('near_break'),
            'break_blocked_for_new_jobs': shift_state.get('break_blocked_for_new_jobs'),
            'division': _normalize_division(row.get('division'), fallback='general'),
            'destination': destination
        }
        if cs_raw != cs:
            status_payload['requestedCallSign'] = cs_raw
            status_payload['canonicalCallSign'] = cs
        return _jsonify_safe(status_payload, 200)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/messages/<callsign>', methods=['GET', 'POST', 'OPTIONS'])
def api_messages_legacy(callsign):
    """MDT/mobile API messages endpoint (sessionless)."""
    if request.method == 'OPTIONS':
        return '', 200
    cs_raw = str(callsign or '').strip()
    if not cs_raw:
        return jsonify({'error': 'callsign required'}), 400
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        _ensure_callsign_redirect_table(cur)
        cs_req = cs_raw.upper()
        cs = _mdt_resolve_callsign(cur, cs_req)
        if request.method == 'GET':
            excl = _sql_exclude_mirrored_cad_update_messages()
            cur.execute(f"""
                SELECT id, `from`, recipient, text, timestamp, COALESCE(`read`, 0) AS `read`
                FROM messages
                WHERE (LOWER(TRIM(recipient)) IN (LOWER(TRIM(%s)), LOWER(TRIM(%s)))
                   OR LOWER(TRIM(`from`)) IN (LOWER(TRIM(%s)), LOWER(TRIM(%s))))
                  AND ({excl})
                ORDER BY timestamp ASC
            """, (cs, cs_req, cs, cs_req))
            rows = cur.fetchall() or []
            return _jsonify_safe(rows, 200)

        data = request.get_json(silent=True) or {}
        text = str(data.get('text') or '').strip()
        if not text:
            return jsonify({'error': 'Message text required'}), 400
        if len(text) > 2000:
            return jsonify({'error': 'Message too long'}), 400

        sender = str(
            data.get('from')
            or data.get('sender')
            or data.get('callSign')
            or data.get('callsign')
            or 'mdt'
        ).strip()[:120]
        if not sender:
            sender = 'mdt'
        dup_id = _messages_recent_duplicate_id(cur, sender, cs, text)
        if dup_id is not None:
            return _jsonify_safe({'message': 'Message sent', 'id': dup_id, 'deduped': True}, 200)
        cur.execute("""
            INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
            VALUES (%s, %s, %s, NOW(), 0)
        """, (sender, cs, text))
        new_id = cur.lastrowid
        conn.commit()
        try:
            socketio.emit('mdt_event', {
                'type': 'message_posted',
                'from': sender,
                'to': cs,
                'text': text,
                'message_id': int(new_id) if new_id else None,
            })
        except Exception:
            pass
        return _jsonify_safe({'message': 'Message sent', 'id': int(new_id) if new_id else None}, 200)
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/messages/<callsign>/unread', methods=['GET', 'OPTIONS'])
def api_messages_unread_legacy(callsign):
    """Legacy alias: unread count for callsign."""
    if request.method == 'OPTIONS':
        return '', 200
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'messages'")
        if cur.fetchone() is None:
            return jsonify({'callsign': callsign, 'unread': 0}), 200
        _ensure_callsign_redirect_table(cur)
        cs_req = str(callsign or '').strip().upper()
        cs = _mdt_resolve_callsign(cur, cs_req)
        excl = _sql_exclude_mirrored_cad_update_messages()
        viewer = None
        if getattr(current_user, 'is_authenticated', False):
            if _ventus_dispatch_capable() or _can_access_call_centre():
                viewer = str(getattr(current_user, 'username', '') or '').strip()
        if viewer:
            cur.execute(f"""
                SELECT `from` AS sender_from FROM messages
                WHERE COALESCE(`read`, 0) = 0
                  AND LOWER(TRIM(recipient)) IN (LOWER(TRIM(%s)), LOWER(TRIM(%s)))
                  AND ({excl})
            """, (cs, cs_req))
            unread = sum(
                1 for row in (cur.fetchall() or [])
                if not _job_comm_sender_is_viewer((row or {}).get('sender_from'), viewer)
            )
        else:
            cur.execute(f"""
                SELECT COUNT(*) AS unread
                FROM messages
                WHERE COALESCE(`read`, 0) = 0
                  AND LOWER(TRIM(recipient)) IN (LOWER(TRIM(%s)), LOWER(TRIM(%s)))
                  AND ({excl})
            """, (cs, cs_req))
            row = cur.fetchone() or {}
            unread = int(row.get('unread') or 0)
        return jsonify({'callsign': cs, 'unread': unread}), 200
    finally:
        cur.close()
        conn.close()


@internal.route('/api/messages/<callsign>/read', methods=['POST', 'OPTIONS'])
def api_messages_mark_read_legacy(callsign):
    """Legacy alias: mark all recipient messages read."""
    if request.method == 'OPTIONS':
        return '', 200
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'messages'")
        if cur.fetchone() is None:
            return jsonify({'callsign': callsign, 'updated': 0}), 200
        _ensure_callsign_redirect_table(cur)
        cs_req = str(callsign or '').strip().upper()
        cs = _mdt_resolve_callsign(cur, cs_req)
        cur.execute(
            """
            UPDATE messages SET `read` = 1
            WHERE COALESCE(`read`, 0) = 0
              AND LOWER(TRIM(recipient)) IN (LOWER(TRIM(%s)), LOWER(TRIM(%s)))
            """,
            (cs, cs_req),
        )
        updated = cur.rowcount or 0
        conn.commit()
        return jsonify({'callsign': cs, 'updated': int(updated)}), 200
    except Exception as e:
        conn.rollback()
        return _ventus_public_server_error(e)
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/push/vapid-public-key', methods=['GET', 'OPTIONS'])
def mdt_push_vapid_public_key():
    if request.method == 'OPTIONS':
        return '', 200
    pub, _, _ = _mdt_vapid_config()
    if not pub:
        return jsonify({'error': 'Web Push not configured', 'configured': False}), 503
    return jsonify({'publicKey': pub, 'configured': True}), 200


@internal.route('/api/mdt/<callsign>/push/subscribe', methods=['POST', 'OPTIONS'])
def mdt_push_subscribe(callsign):
    if request.method == 'OPTIONS':
        return '', 200
    if not getattr(g, 'mdt_user', None):
        return jsonify({'error': 'Unauthorized'}), 401
    cs = str(callsign or '').strip().upper()
    if not cs:
        return jsonify({'error': 'callsign required'}), 400
    data = request.get_json(silent=True) or {}
    sub = data.get('subscription') if isinstance(
        data.get('subscription'), dict) else data
    if not isinstance(sub, dict):
        return jsonify({'error': 'subscription object required'}), 400
    endpoint = str(sub.get('endpoint') or '').strip()
    keys = sub.get('keys') or {}
    p256dh = str(keys.get('p256dh') or '').strip()
    auth = str(keys.get('auth') or '').strip()
    if not endpoint or not p256dh or not auth:
        return jsonify({'error': 'subscription.endpoint and keys.p256dh, keys.auth required'}), 400
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        _ensure_mdt_web_push_subscriptions_table(cur)
        ua = (request.headers.get('User-Agent') or '')[:512]
        cur.execute("""
            INSERT INTO mdt_web_push_subscriptions (callsign, endpoint, p256dh, auth_secret, user_agent)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE callsign=VALUES(callsign), p256dh=VALUES(p256dh),
                auth_secret=VALUES(auth_secret), user_agent=VALUES(user_agent), updated_at=CURRENT_TIMESTAMP
        """, (cs, endpoint[:768], p256dh, auth, ua))
        conn.commit()
        return jsonify({'ok': True, 'callsign': cs}), 200
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()


@internal.route('/api/mdt/<callsign>/push/unsubscribe', methods=['POST', 'OPTIONS'])
def mdt_push_unsubscribe(callsign):
    if request.method == 'OPTIONS':
        return '', 200
    if not getattr(g, 'mdt_user', None):
        return jsonify({'error': 'Unauthorized'}), 401
    cs = str(callsign or '').strip().upper()
    data = request.get_json(silent=True) or {}
    endpoint = str(data.get('endpoint') or '').strip()
    if not endpoint:
        return jsonify({'error': 'endpoint required'}), 400
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        _ensure_mdt_web_push_subscriptions_table(cur)
        cur.execute(
            "DELETE FROM mdt_web_push_subscriptions WHERE callsign = %s AND endpoint = %s LIMIT 1",
            (cs, endpoint[:768]),
        )
        conn.commit()
        return jsonify({'ok': True, 'removed': cur.rowcount}), 200
    finally:
        cur.close()
        conn.close()


@internal.route('/api/ping', methods=['GET', 'OPTIONS'])
def api_ping_legacy():
    """Legacy health endpoint for MDT heartbeat."""
    if request.method == 'OPTIONS':
        return '', 200
    return jsonify({'ok': True, 'service': 'ventus_response_module', 'time': datetime.utcnow().isoformat() + 'Z'}), 200


# --- Root-level compatibility aliases (true /api/* paths) ---
@api_compat.route('/api/mdt/signOn', methods=['POST', 'OPTIONS'])
def mdt_sign_on_root_compat():
    return mdt_sign_on()


@api_compat.route('/api/mdt/signed-on-units', methods=['GET', 'OPTIONS'])
def mdt_signed_on_units_list_root_compat():
    return mdt_signed_on_units_list()


@api_compat.route('/api/mdt/signOff', methods=['POST', 'OPTIONS'])
def mdt_sign_off_root_compat():
    return mdt_sign_off()


@api_compat.route('/api/mdt/callsign', methods=['GET', 'POST', 'OPTIONS'])
def mdt_query_callsign_root_compat():
    return mdt_query_callsign()


@api_compat.route('/api/mdt/next', methods=['GET', 'OPTIONS'])
def mdt_next_root_compat():
    return mdt_next()


@api_compat.route('/api/mdt/history', methods=['GET', 'POST', 'OPTIONS'])
def mdt_history_root_compat():
    return mdt_history()


@api_compat.route('/api/mdt/<int:cad>', methods=['GET', 'OPTIONS'])
def mdt_details_root_compat(cad):
    return mdt_details(cad)


@api_compat.route('/api/mdt/<int:cad>/claim', methods=['POST', 'OPTIONS'])
def mdt_claim_root_compat(cad):
    return mdt_claim(cad)


@api_compat.route('/api/mdt/<int:cad>/status', methods=['POST', 'OPTIONS'])
def mdt_status_root_compat(cad):
    return mdt_status(cad)


@api_compat.route('/api/mdt/<int:cad>/comms', methods=['GET', 'POST', 'OPTIONS'])
def mdt_job_comms_root_compat(cad):
    return mdt_job_comms_api(cad)


@api_compat.route('/api/mdt/<callsign>/location', methods=['POST', 'OPTIONS'])
def mdt_update_location_root_compat(callsign):
    return mdt_update_location_legacy(callsign)


@api_compat.route('/api/mdt/<callsign>/crew', methods=['GET', 'POST', 'OPTIONS'])
def mdt_update_crew_root_compat(callsign):
    return mdt_update_crew_legacy(callsign)


@api_compat.route('/api/mdt/<callsign>/standby', methods=['GET', 'OPTIONS'])
def mdt_standby_root_compat(callsign):
    return mdt_standby_legacy(callsign)


@api_compat.route('/api/mdt/<callsign>/destination', methods=['GET', 'OPTIONS'])
def mdt_destination_root_compat(callsign):
    return mdt_destination_legacy(callsign)


@api_compat.route('/api/mdt/<callsign>/destination/override', methods=['POST', 'OPTIONS'])
def mdt_destination_override_root_compat(callsign):
    return mdt_destination_override_legacy(callsign)


@api_compat.route('/api/mdt/<callsign>/motd', methods=['GET', 'OPTIONS'])
def mdt_motd_root_compat(callsign):
    return mdt_motd_for_unit(callsign)


@api_compat.route('/api/mdt/<callsign>/instructions', methods=['GET', 'OPTIONS'])
def mdt_instructions_root_compat(callsign):
    return mdt_instructions_legacy(callsign)


@api_compat.route('/api/mdt/<callsign>/instructions/ack', methods=['POST', 'OPTIONS'])
def mdt_instructions_ack_root_compat(callsign):
    return mdt_instructions_ack_legacy(callsign)


@api_compat.route('/api/mdt/<callsign>/status', methods=['GET', 'POST', 'OPTIONS'])
def mdt_unit_status_root_compat(callsign):
    return mdt_unit_status_legacy(callsign)


@api_compat.route('/api/messages/<callsign>', methods=['GET', 'POST', 'OPTIONS'])
def api_messages_root_compat(callsign):
    return api_messages_legacy(callsign)


@api_compat.route('/api/messages/<callsign>/unread', methods=['GET', 'OPTIONS'])
def api_messages_unread_root_compat(callsign):
    return api_messages_unread_legacy(callsign)


@api_compat.route('/api/messages/<callsign>/read', methods=['POST', 'OPTIONS'])
def api_messages_mark_read_root_compat(callsign):
    return api_messages_mark_read_legacy(callsign)


@api_compat.route('/api/ping', methods=['GET', 'OPTIONS'])
def api_ping_root_compat():
    return api_ping_legacy()


@api_compat.route('/api/mdt/push/vapid-public-key', methods=['GET', 'OPTIONS'])
def mdt_push_vapid_public_key_root_compat():
    return mdt_push_vapid_public_key()


@api_compat.route('/api/mdt/<callsign>/push/subscribe', methods=['POST', 'OPTIONS'])
def mdt_push_subscribe_root_compat(callsign):
    return mdt_push_subscribe(callsign)


@api_compat.route('/api/mdt/<callsign>/push/unsubscribe', methods=['POST', 'OPTIONS'])
def mdt_push_unsubscribe_root_compat(callsign):
    return mdt_push_unsubscribe(callsign)


@api_compat.route('/api/mdt/<callsign>/major_incident', methods=['POST', 'OPTIONS'])
def mdt_major_incident_submit_root_compat(callsign):
    return mdt_major_incident_submit(callsign)


@api_compat.route('/api/mdt/<callsign>/panic', methods=['POST', 'OPTIONS'])
def mdt_panic_root_compat(callsign):
    return mdt_panic(callsign)


@api_compat.route('/api/mdt/<callsign>/running_call', methods=['POST', 'OPTIONS'])
def mdt_running_call_root_compat(callsign):
    return mdt_running_call(callsign)


@api_compat.route('/api/mdt/crew-user-search', methods=['GET', 'OPTIONS'])
def mdt_crew_user_search_root_compat():
    return mdt_crew_user_search()


@api_compat.route('/api/mdt/<callsign>/panic_voice', methods=['POST', 'OPTIONS'])
def mdt_panic_voice_root_compat(callsign):
    return mdt_panic_voice_upload(callsign)


@api_compat.route('/api/mdt/<callsign>/job/<int:cad>/panic_voice', methods=['GET', 'OPTIONS'])
def mdt_job_panic_voice_list_root_compat(callsign, cad):
    return mdt_job_panic_voice_list(callsign, cad)


@api_compat.route(
    '/api/mdt/<callsign>/job/<int:cad>/panic_voice/<int:rec_id>/stream',
    methods=['GET', 'OPTIONS', 'HEAD'],
)
def mdt_job_panic_voice_stream_root_compat(callsign, cad, rec_id):
    return mdt_job_panic_voice_stream(callsign, cad, rec_id)


# =============================================================================
# PUBLIC BLUEPRINT
# =============================================================================
public_template_folder = os.path.join(
    os.path.dirname(__file__), 'templates', 'public')
public = Blueprint(
    'ventus_response',
    __name__,
    url_prefix='/ventus',
    template_folder=public_template_folder
)


@public.before_request
def ensure_ventus_response_portal_user():
    # If the user isn't authenticated yet, let the login_required decorator handle it.
    if not current_user.is_authenticated:
        return
    # Now that the user is authenticated, ensure they are from the Vita-Care-Portal module.
    if not hasattr(current_user, 'role') or current_user.role != "Ventus-Response-Portal":
        return jsonify({"error": "Unauthorised access"}), 403

# =============================================================================
# Blueprint Registration Functions
# =============================================================================


def get_blueprint():
    # Keep the original blueprint name/endpoints so existing template url_for
    # calls (medical_response_internal.*) continue to resolve.
    return internal


def get_public_blueprint():
    return public


@internal.route('/panel/<panel_type>', methods=['GET'])
@login_required
def panel_popup(panel_type):
    """Serve a lightweight panel view usable as a popout window.

    This page subscribes to the BroadcastChannel "ventus_cad" for live updates.
    """
    # Normalize and validate panel type to avoid invalid popout routing.
    normalized = (panel_type or '').strip().lower()
    aliases = {
        'msgs': 'messages',
        'message': 'messages',
        'past': 'past_jobs',
        'pastjobs': 'past_jobs',
        'history': 'past_jobs'
    }
    normalized = aliases.get(normalized, normalized)
    allowed = {'jobs', 'units', 'messages', 'past_jobs', 'audit'}
    if normalized not in allowed:
        return jsonify({'error': 'Invalid panel type'}), 400
    # Popouts now use the full CAD dashboard in single-panel mode to guarantee
    # parity with the main in-page panel content.
    qs = request.args.to_dict(flat=True)
    panel_for_dashboard = normalized if normalized in {
        'jobs', 'units', 'messages'} else 'jobs'
    query = {
        'panel': panel_for_dashboard,
        'title': str(qs.get('title') or panel_for_dashboard).strip(),
        'popout': '1',
    }
    division = str(qs.get('division') or '').strip()
    include_external = str(qs.get('include_external') or '').strip()
    if division:
        query['division'] = division
    if include_external:
        query['include_external'] = include_external
    return redirect(url_for('.cad_dashboard', **query))
