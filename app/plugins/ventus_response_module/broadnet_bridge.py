"""
Optional one-way vendor dispatch bridge: POST incident on CAD unit assignment.

Gated by DB row ``ventus_broadnet_settings`` (``master_enabled`` must be set via SQL;
admin UI only appears when master is on). No inbound webhooks — outbound only.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.objects import get_db_connection

logger = logging.getLogger(__name__)

_BROADNET_API_ROOT = "https://api-dispatch.broadnet.systems/api_dispatch/v1/organisations"

_CREATE_SETTINGS_SQL = """
CREATE TABLE IF NOT EXISTS ventus_broadnet_settings (
    id TINYINT NOT NULL PRIMARY KEY DEFAULT 1,
    master_enabled TINYINT(1) NOT NULL DEFAULT 0,
    outbound_enabled TINYINT(1) NOT NULL DEFAULT 0,
    org_endpoint_key VARCHAR(160) NOT NULL DEFAULT '',
    api_key VARCHAR(512) NOT NULL DEFAULT '',
    default_team_uuid VARCHAR(80) NOT NULL DEFAULT '',
    default_channel_uuid VARCHAR(80) NOT NULL DEFAULT '',
    grade_default TINYINT NOT NULL DEFAULT 2,
    callsign_channel_map JSON NULL,
    callsign_terminal_map JSON NULL,
    updated_by VARCHAR(120) NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_CREATE_PUSH_SQL = """
CREATE TABLE IF NOT EXISTS ventus_broadnet_dispatch_push (
    job_cad INT NOT NULL,
    callsign VARCHAR(64) NOT NULL,
    pushed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    http_status SMALLINT NULL,
    ok TINYINT(1) NOT NULL DEFAULT 0,
    detail VARCHAR(768) NULL,
    PRIMARY KEY (job_cad, callsign),
    INDEX idx_broadnet_push_time (pushed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def _ensure_terminal_map_column(cur) -> None:
    """Add callsign_terminal_map for DBs created before this column existed."""
    try:
        cur.execute(
            """
            SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'ventus_broadnet_settings'
              AND COLUMN_NAME = 'callsign_terminal_map'
            """
        )
        row = cur.fetchone() or {}
        cnt = row.get("cnt") if isinstance(row, dict) else (row[0] if row else 0)
        if int(cnt or 0) == 0:
            cur.execute(
                """
                ALTER TABLE ventus_broadnet_settings
                ADD COLUMN callsign_terminal_map JSON NULL
                AFTER callsign_channel_map
                """
            )
    except Exception:
        logger.exception("vendor dispatch bridge: could not add terminal map column")


def ensure_broadnet_tables(cur) -> None:
    cur.execute(_CREATE_SETTINGS_SQL)
    cur.execute(_CREATE_PUSH_SQL)
    cur.execute("INSERT IGNORE INTO ventus_broadnet_settings (id) VALUES (1)")
    _ensure_terminal_map_column(cur)


def load_broadnet_settings_row(cur) -> Optional[Dict[str, Any]]:
    ensure_broadnet_tables(cur)
    cur.execute("SELECT * FROM ventus_broadnet_settings WHERE id = 1 LIMIT 1")
    return cur.fetchone()


def is_master_unlocked(cur) -> bool:
    row = load_broadnet_settings_row(cur)
    if not row:
        return False
    try:
        return bool(int(row.get("master_enabled") or 0))
    except (TypeError, ValueError):
        return False


def is_outbound_configured(cur) -> bool:
    row = load_broadnet_settings_row(cur)
    if not row:
        return False
    if not int(row.get("master_enabled") or 0):
        return False
    if not int(row.get("outbound_enabled") or 0):
        return False
    if not str(row.get("org_endpoint_key") or "").strip():
        return False
    if not str(row.get("api_key") or "").strip():
        return False
    if not str(row.get("default_team_uuid") or "").strip():
        return False
    ch = str(row.get("default_channel_uuid") or "").strip()
    if ch:
        return True
    if _parse_callsign_uuid_map(row.get("callsign_terminal_map")):
        return True
    return False


def _channel_for_callsign(settings: Dict[str, Any], callsign: str) -> str:
    cs = str(callsign or "").strip().upper()
    raw = settings.get("callsign_channel_map")
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")
    if isinstance(raw, str) and raw.strip():
        try:
            m = json.loads(raw)
        except json.JSONDecodeError:
            m = {}
    elif isinstance(raw, dict):
        m = raw
    else:
        m = {}
    v = m.get(cs) or m.get(str(callsign or "").strip())
    if v and str(v).strip():
        return str(v).strip()
    return str(settings.get("default_channel_uuid") or "").strip()


def _parse_callsign_uuid_map(raw: Any) -> Dict[str, str]:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")
    if isinstance(raw, str) and raw.strip():
        try:
            m = json.loads(raw)
        except json.JSONDecodeError:
            m = {}
    elif isinstance(raw, dict):
        m = raw
    else:
        m = {}
    out: Dict[str, str] = {}
    if not isinstance(m, dict):
        return out
    for k, v in m.items():
        ks = str(k or "").strip().upper()
        vs = str(v or "").strip()
        if ks and vs:
            out[ks] = vs
    return out


def _terminal_for_callsign(settings: Dict[str, Any], callsign: str) -> str:
    m = _parse_callsign_uuid_map(settings.get("callsign_terminal_map"))
    cs = str(callsign or "").strip().upper()
    return str(m.get(cs) or "").strip()


def _norm_exclusions(excl: Any) -> List[str]:
    out: List[str] = []
    if isinstance(excl, str):
        try:
            excl = json.loads(excl)
        except json.JSONDecodeError:
            return out
    if not isinstance(excl, dict):
        return out
    for key, value in excl.items():
        if not isinstance(value, str):
            continue
        vl = value.strip().lower()
        if key == "exclusion_speech" and vl == "no":
            out.append("Exclusion - Full sentences")
        elif str(key).startswith("exclusion_") and vl == "yes":
            label = str(key).replace("exclusion_", "").replace("_", " ").title()
            out.append(f"Exclusion - {label}")
    return out


def _risk_note_lines(flags: Any) -> List[str]:
    lines: List[str] = []
    if isinstance(flags, str):
        try:
            flags = json.loads(flags)
        except json.JSONDecodeError:
            return lines
    if not isinstance(flags, list):
        return lines
    for flag in flags:
        if isinstance(flag, dict):
            ft = str(flag.get("flag_type") or flag.get("type") or "").strip()
            desc = str(flag.get("description") or flag.get("detail") or "").strip()
            ts = str(flag.get("timestamp") or "").strip()
            chunk = " — ".join(x for x in (ft, desc) if x)
            if chunk:
                lines.append(f"Risk: {chunk}" + (f" ({ts})" if ts else ""))
        elif flag:
            lines.append(f"Risk: {flag}")
    return lines


def _job_payload_dict(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict):
        return data
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8", errors="ignore")
    if isinstance(data, str) and data.strip():
        try:
            j = json.loads(data)
            return j if isinstance(j, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _coords_from_payload(payload: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    c = payload.get("coordinates")
    if isinstance(c, str):
        try:
            c = json.loads(c)
        except json.JSONDecodeError:
            c = None
    if not isinstance(c, dict):
        return None
    try:
        lat = float(c.get("lat"))
        lng = float(c.get("lng"))
    except (TypeError, ValueError):
        return None
    if -90 <= lat <= 90 and -180 <= lng <= 180:
        return lat, lng
    return None


def _build_incident_payload(
    *,
    cad: int,
    callsign: str,
    allocation: Dict[str, str],
    team_uuid: str,
    grade: int,
    job_payload: Dict[str, Any],
) -> Dict[str, Any]:
    fn = str(job_payload.get("first_name") or "").strip()
    ln = str(job_payload.get("last_name") or "").strip()
    rfc = str(job_payload.get("reason_for_call") or job_payload.get("chief_complaint") or "").strip()
    desc = f"CAD #{cad} — {callsign} — {rfc or 'Assigned'}"
    if fn or ln:
        desc = f"CAD #{cad} — {callsign} — {fn} {ln}".strip() + (f" — {rfc}" if rfc else "")

    coords = _coords_from_payload(job_payload)
    if not coords:
        raise ValueError("missing_coordinates")
    lat, lng = coords

    notes: List[Dict[str, str]] = []
    for label, key in (
        ("Patient DOB", "patient_dob"),
        ("Phone", "phone_number"),
        ("Address", "address"),
        ("Postcode", "postcode"),
        ("Caller", "caller_name"),
        ("Caller phone", "caller_phone"),
        ("Onset", "onset_datetime"),
        ("Patient alone", "patient_alone"),
        ("Priority", "call_priority"),
    ):
        v = job_payload.get(key)
        if v is not None and str(v).strip():
            notes.append({"content": f"{label}: {v}"})

    excl = job_payload.get("exclusion_data")
    for line in _norm_exclusions(excl):
        notes.append({"content": line})
    for line in _risk_note_lines(job_payload.get("risk_flags")):
        notes.append({"content": line})

    g = int(grade) if grade else 2
    if g < 1:
        g = 1
    if g > 5:
        g = 5

    return {
        "incident": {
            "description": desc[:2000],
            "grade": g,
            "locations": [
                {"lat": lat, "lng": lng, "label": "CAD location"},
            ],
            "notes": notes[:40],
            "team_uuid": team_uuid,
            "allocation": allocation,
        }
    }


def _record_push(cur, cad: int, callsign: str, http_status: Optional[int], ok: bool, detail: str) -> None:
    ensure_broadnet_tables(cur)
    cur.execute(
        """
        INSERT INTO ventus_broadnet_dispatch_push (job_cad, callsign, http_status, ok, detail)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            pushed_at = CURRENT_TIMESTAMP,
            http_status = VALUES(http_status),
            ok = VALUES(ok),
            detail = VALUES(detail)
        """,
        (cad, str(callsign)[:64], http_status, 1 if ok else 0, detail[:760]),
    )


def _already_pushed(cur, cad: int, callsign: str) -> bool:
    ensure_broadnet_tables(cur)
    cur.execute(
        "SELECT 1 FROM ventus_broadnet_dispatch_push WHERE job_cad = %s AND callsign = %s AND ok = 1 LIMIT 1",
        (cad, str(callsign)[:64]),
    )
    return cur.fetchone() is not None


def delete_dispatch_push_rows(cur, cad: int, callsigns: List[str]) -> None:
    if not callsigns:
        return
    cur.execute("SHOW TABLES LIKE 'ventus_broadnet_dispatch_push'")
    if not cur.fetchone():
        return
    for cs in callsigns:
        c = str(cs or "").strip()
        if not c:
            continue
        cur.execute(
            "DELETE FROM ventus_broadnet_dispatch_push WHERE job_cad = %s AND callsign = %s",
            (cad, c[:64]),
        )


def delete_all_push_for_cad(cur, cad: int) -> None:
    cur.execute("SHOW TABLES LIKE 'ventus_broadnet_dispatch_push'")
    if not cur.fetchone():
        return
    cur.execute("DELETE FROM ventus_broadnet_dispatch_push WHERE job_cad = %s", (cad,))


def post_for_assignment_if_enabled(
    *,
    job_cad: int,
    assigned_callsigns: List[str],
    job_data_raw: Any,
) -> None:
    """Fire-and-forget HTTP POST per newly assigned callsign (separate DB connection)."""
    if not assigned_callsigns:
        return
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SHOW TABLES LIKE 'ventus_broadnet_settings'")
        if not cur.fetchone():
            return
        cur.execute(
            "SELECT master_enabled FROM ventus_broadnet_settings WHERE id = 1 LIMIT 1"
        )
        _gate = cur.fetchone() or {}
        try:
            if not int(_gate.get("master_enabled") or 0):
                return
        except (TypeError, ValueError):
            return
        if not is_outbound_configured(cur):
            return
        settings = load_broadnet_settings_row(cur) or {}
        team_uuid = str(settings.get("default_team_uuid") or "").strip()
        org_key = str(settings.get("org_endpoint_key") or "").strip()
        api_key = str(settings.get("api_key") or "").strip()
        try:
            grade = int(settings.get("grade_default") or 2)
        except (TypeError, ValueError):
            grade = 2

        job_payload = _job_payload_dict(job_data_raw)

        url = f"{_BROADNET_API_ROOT}/{org_key}/incidents"
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        for cs in assigned_callsigns:
            csn = str(cs or "").strip()
            if not csn:
                continue
            if _already_pushed(cur, job_cad, csn):
                continue
            term_uuid = _terminal_for_callsign(settings, csn)
            if term_uuid:
                allocation = {
                    "type": "terminal",
                    "terminal_uuid": term_uuid,
                }
            else:
                ch = _channel_for_callsign(settings, csn)
                if not ch:
                    logger.warning(
                        "dispatch bridge: skip %s on CAD %s — no terminal map and no channel",
                        csn, job_cad,
                    )
                    continue
                allocation = {"type": "channel", "channel_uuid": ch}
            try:
                body = _build_incident_payload(
                    cad=job_cad,
                    callsign=csn,
                    allocation=allocation,
                    team_uuid=team_uuid,
                    grade=grade,
                    job_payload=job_payload,
                )
            except ValueError as e:
                if str(e) == "missing_coordinates":
                    logger.warning(
                        "dispatch bridge: skip %s on CAD %s — no lat/lng on job", csn, job_cad)
                    _record_push(
                        cur, job_cad, csn, None, False, "missing_coordinates")
                    conn.commit()
                continue

            try:
                r = requests.post(
                    url, headers=headers, data=json.dumps(body), timeout=12)
                st = r.status_code
                ok = 200 <= st < 300
                snippet = (r.text or "")[:500]
                if ok:
                    alloc_t = str(allocation.get("type") or "")
                    detail = f"ok:{alloc_t}"
                else:
                    detail = f"http_{st}: {snippet}"
                _record_push(cur, job_cad, csn, st, ok, detail)
                conn.commit()
                if not ok:
                    logger.warning(
                        "dispatch bridge POST CAD=%s %s status=%s body=%s",
                        job_cad, csn, st, snippet[:200],
                    )
            except requests.RequestException as ex:
                logger.warning(
                    "dispatch bridge POST CAD=%s %s failed: %s", job_cad, csn, ex)
                _record_push(cur, job_cad, csn, None, False, str(ex)[:760])
                conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def admin_get_settings_masked(cur) -> Dict[str, Any]:
    row = load_broadnet_settings_row(cur) or {}
    out = dict(row)
    key = str(out.get("api_key") or "")
    if len(key) > 8:
        out["api_key"] = ""
        out["api_key_set"] = True
        out["api_key_hint"] = key[:4] + "…" + key[-2:]
    else:
        out["api_key_set"] = bool(key)
        out["api_key_hint"] = ""
    cmap = out.get("callsign_channel_map")
    if isinstance(cmap, (dict, list)):
        out["callsign_channel_map_json"] = json.dumps(cmap, indent=2)
    elif isinstance(cmap, str):
        out["callsign_channel_map_json"] = cmap
    else:
        out["callsign_channel_map_json"] = "{}"
    tmap = out.get("callsign_terminal_map")
    if isinstance(tmap, dict):
        out["callsign_terminal_map_json"] = json.dumps(tmap, indent=2)
    elif isinstance(tmap, str):
        out["callsign_terminal_map_json"] = tmap
    else:
        out["callsign_terminal_map_json"] = "{}"
    return out


def admin_save_settings(
    cur,
    *,
    username: str,
    outbound_enabled: bool,
    org_endpoint_key: str,
    api_key: str,
    default_team_uuid: str,
    default_channel_uuid: str,
    grade_default: int,
    callsign_channel_map_json: str,
    callsign_terminal_map_json: str,
) -> None:
    ensure_broadnet_tables(cur)
    cur.execute("SELECT api_key FROM ventus_broadnet_settings WHERE id = 1 LIMIT 1")
    row = cur.fetchone() or {}
    existing_key = str(row.get("api_key") or "")
    new_key = (api_key or "").strip()[:512]
    if not new_key:
        new_key = existing_key
    cmap: Any = {}
    raw = (callsign_channel_map_json or "").strip()
    if raw:
        cmap = json.loads(raw)
        if not isinstance(cmap, dict):
            raise ValueError("callsign_channel_map must be a JSON object")
    tmap: Any = {}
    raw_t = (callsign_terminal_map_json or "").strip()
    if raw_t:
        tmap = json.loads(raw_t)
        if not isinstance(tmap, dict):
            raise ValueError("callsign_terminal_map must be a JSON object")
    if outbound_enabled:
        if not org_endpoint_key.strip() or not new_key.strip():
            raise ValueError("org_endpoint_key and api_key are required when outbound is enabled")
        if not default_team_uuid.strip():
            raise ValueError("default_team_uuid is required when outbound is enabled")
        if not str(default_channel_uuid or "").strip() and not _parse_callsign_uuid_map(tmap):
            raise ValueError(
                "Provide default_channel_uuid and/or at least one callsign_terminal_map entry when outbound is enabled"
            )
    cmap_json = json.dumps(cmap or {})
    tmap_json = json.dumps(tmap or {})
    cur.execute(
        """
        UPDATE ventus_broadnet_settings SET
            outbound_enabled = %s,
            org_endpoint_key = %s,
            api_key = %s,
            default_team_uuid = %s,
            default_channel_uuid = %s,
            grade_default = %s,
            callsign_channel_map = CAST(%s AS JSON),
            callsign_terminal_map = CAST(%s AS JSON),
            updated_by = %s
        WHERE id = 1
        """,
        (
            1 if outbound_enabled else 0,
            org_endpoint_key.strip()[:160],
            new_key[:512],
            default_team_uuid.strip()[:80],
            default_channel_uuid.strip()[:80],
            max(1, min(5, int(grade_default))),
            cmap_json,
            tmap_json,
            (username or "")[:120],
        ),
    )


def sanitize_org_endpoint_key(raw: str) -> str:
    s = str(raw or "").strip()
    s = re.sub(r"[^a-zA-Z0-9_-]", "", s)
    return s[:160]
