"""
Helpers for ``case_access_pdf.html``: resolve EPCR section names (Vue + React aliases)
and detect when JSON content is structurally empty so blank cards are not rendered.
"""
from __future__ import annotations

import base64
import logging
import math
import os
import re
from datetime import datetime
from typing import Any, Iterable

logger = logging.getLogger(__name__)

_CURA_UPLOAD_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "data", "cura_uploads"))


def _resolve_cura_attachment_disk_path(storage_key: str | None) -> str | None:
    """Same path rules as ``cura_routes._resolve_attachment_disk_path`` (cura_uploads only)."""
    if not storage_key or not isinstance(storage_key, str):
        return None
    sk = storage_key.replace("\\", "/").lstrip("/")
    if not sk.startswith("cura_uploads/"):
        return None
    rel = sk[13:]
    if not rel or ".." in rel:
        return None
    base = os.path.normpath(_CURA_UPLOAD_ROOT)
    full = os.path.normpath(os.path.join(_CURA_UPLOAD_ROOT, rel))
    if not full.startswith(base):
        return None
    return full if os.path.isfile(full) else None


def _epcr_case_attachment_data_urls(
    case_id: int, cursor: Any, raw_ids: Any, *, log_prefix: str = "EPCR attachment"
) -> list[str]:
    """Resolve ``cura_file_attachments`` ids to ``data:image/...;base64,...`` URLs for inline HTML/PDF."""
    out: list[str] = []
    if not isinstance(raw_ids, list) or not raw_ids or cursor is None:
        return out
    for raw in raw_ids:
        try:
            aid = int(raw)
        except (TypeError, ValueError):
            continue
        try:
            cursor.execute(
                """
                SELECT storage_key, mime_type FROM cura_file_attachments
                WHERE id = %s AND entity_type = %s AND entity_id = %s
                """,
                (aid, "epcr_case", int(case_id)),
            )
            row = cursor.fetchone()
            if not row:
                continue
            sk, mime = row[0], row[1]
            path = _resolve_cura_attachment_disk_path(sk)
            if not path:
                continue
            with open(path, "rb") as fh:
                b64 = base64.standard_b64encode(fh.read()).decode("ascii")
            mt = (mime or "image/jpeg").split(";")[0].strip()
            if not mt.startswith("image/"):
                mt = "image/jpeg"
            out.append(f"data:{mt};base64,{b64}")
        except OSError as e:
            logger.warning("%s read failed case=%s id=%s: %s", log_prefix, case_id, raw, e)
        except Exception as e:
            logger.warning("%s skipped case=%s id=%s: %s", log_prefix, case_id, raw, e)
    return out


def inject_rtc_case_access_inline_images(case_id: int, case_data: dict[str, Any], cursor: Any) -> None:
    """
    Populate ``content['_caseAccessRtcPhotoUrls']`` on the RTC section for HTML/PDF rendering.

    - Resolves ``rtcPhotoAttachmentIds`` via ``cura_file_attachments`` + on-disk files (keeps ``cases.data`` small).
    - Appends legacy ``rtcScenePhotos`` data URLs still stored in the JSON blob (sanitized on PUT).
    """
    sections = case_data.get("sections")
    if not isinstance(sections, list):
        return
    for sec in sections:
        if not isinstance(sec, dict):
            continue
        if str(sec.get("name") or "").strip().lower() != "rtc":
            continue
        c = sec.get("content")
        if not isinstance(c, dict):
            continue
        out: list[str] = _epcr_case_attachment_data_urls(
            case_id, cursor, c.get("rtcPhotoAttachmentIds"), log_prefix="RTC attachment"
        )
        legacy = c.get("rtcScenePhotos")
        if isinstance(legacy, list):
            for p in legacy:
                if isinstance(p, str) and p.startswith("data:image") and len(p) < 12_000_000:
                    out.append(p)
        c["_caseAccessRtcPhotoUrls"] = out
        break


def inject_oohca_role_ecg_case_access_images(case_id: int, case_data: dict[str, Any], cursor: Any) -> None:
    """
    Populate ``verification['_caseAccessRoleEcg30sUrls']`` and ``['_caseAccessRoleEcg5minUrls']`` on the OOHCA
    section for case-access HTML/PDF (attachment ids → inline data URLs).
    """
    sections = case_data.get("sections")
    if not isinstance(sections, list):
        return
    for sec in sections:
        if not isinstance(sec, dict):
            continue
        if str(sec.get("name") or "").strip().lower() != "oohca":
            continue
        c = sec.get("content")
        if not isinstance(c, dict):
            break
        v = c.get("verification")
        if not isinstance(v, dict):
            break
        v["_caseAccessRoleEcg30sUrls"] = _epcr_case_attachment_data_urls(
            case_id, cursor, v.get("asystole30sEcgAttachmentIds"), log_prefix="OOHCA ROLE 30s ECG"
        )
        v["_caseAccessRoleEcg5minUrls"] = _epcr_case_attachment_data_urls(
            case_id, cursor, v.get("asystole5minEcgAttachmentIds"), log_prefix="OOHCA ROLE 5min ECG"
        )
        break


def sanitize_oohca_verification_render_keys(case_data: dict[str, Any]) -> None:
    """Strip render-only keys from OOHCA ``verification`` before persisting ``cases.data``."""
    sections = case_data.get("sections")
    if not isinstance(sections, list):
        return
    for sec in sections:
        if not isinstance(sec, dict):
            continue
        if str(sec.get("name") or "").strip().lower() != "oohca":
            continue
        c = sec.get("content")
        if not isinstance(c, dict):
            break
        v = c.get("verification")
        if not isinstance(v, dict):
            break
        for k in list(v.keys()):
            if isinstance(k, str) and k.startswith("_caseAccess"):
                v.pop(k, None)
        break


def sanitize_rtc_section_payload_for_mysql(case_data: dict[str, Any]) -> None:
    """
    Shrink RTC payloads before persisting ``cases.data`` to reduce MySQL row / sort pressure.

    - Drops render-only keys.
    - Caps legacy inline ``rtcScenePhotos`` data URLs (prefer ``rtcPhotoAttachmentIds`` + upload API).
    - Dedupes attachment id list.
    """
    sections = case_data.get("sections")
    if not isinstance(sections, list):
        return
    for sec in sections:
        if not isinstance(sec, dict):
            continue
        if str(sec.get("name") or "").strip().lower() != "rtc":
            continue
        c = sec.get("content")
        if not isinstance(c, dict):
            continue
        c.pop("_caseAccessRtcPhotoUrls", None)
        photos = c.get("rtcScenePhotos")
        if isinstance(photos, list):
            cleaned: list[str] = []
            for p in photos:
                if not isinstance(p, str):
                    continue
                if p.startswith("data:image") and len(p) > 280_000:
                    continue
                cleaned.append(p)
                if len(cleaned) >= 8:
                    break
            c["rtcScenePhotos"] = cleaned
        raw_ids = c.get("rtcPhotoAttachmentIds")
        if isinstance(raw_ids, list):
            seen: set[int] = set()
            out_ids: list[int] = []
            for x in raw_ids[:24]:
                try:
                    n = int(x)
                except (TypeError, ValueError):
                    continue
                if n in seen:
                    continue
                seen.add(n)
                out_ids.append(n)
            c["rtcPhotoAttachmentIds"] = out_ids

_DRAFT_KEY = re.compile(r"^draft", re.IGNORECASE)


def _skip_key(key: str, exclude_keys: frozenset[str] | set[str]) -> bool:
    k = str(key)
    if k in exclude_keys:
        return True
    return bool(_DRAFT_KEY.match(k))


def epcr_content_meaningful(obj: Any, exclude_keys: Iterable[str] | None = None) -> bool:
    """
    True if ``obj`` contains at least one non-empty value when walking dicts/lists recursively.

    - Ignores keys matching ``draft*`` (case-insensitive) and optional ``exclude_keys``.
    - Empty strings / whitespace-only count as empty.
    - ``False`` and numeric ``0`` are treated as meaningful (explicit clinical answers).
    - ``None`` is empty.
    """
    ex: set[str] = {str(x) for x in (exclude_keys or ())}

    def walk(v: Any) -> bool:
        if v is None:
            return False
        if isinstance(v, bool):
            return True
        if isinstance(v, (int, float)):
            return True
        if isinstance(v, str):
            return bool(v.strip())
        if isinstance(v, list):
            return any(walk(x) for x in v)
        if isinstance(v, dict):
            for k, val in v.items():
                if _skip_key(k, ex):
                    continue
                if walk(val):
                    return True
            return False
        return True

    return walk(obj)


def epcr_fmt_reversible_cause(val: Any) -> str:
    """Map OOHCA reversible cause values from Cura (considered / reversed / false) for PDF/HTML."""
    if val is True or val == "true":
        return "Yes"
    if val == "considered":
        return "Considered"
    if val == "reversed":
        return "Reversed"
    if val in (False, "false", "", None):
        return "No"
    return str(val)


def _oohca_reversible_non_negative(val: Any) -> bool:
    """True if crew documented more than the default 'ruled out / not applicable' answer."""
    if val is True or val == "true":
        return True
    if val in ("considered", "reversed"):
        return True
    return False


def epcr_oohca_reversible_pdf_flags(oohca: Any) -> dict[str, bool]:
    """
    Which reversible-cause blocks to print on case-access PDF.

    Cura stores ``reversibleAssessmentPath``: unset | medical | hott | both.
    When unset, infer from legacy payloads so old charts still print if anything was documented.
    When medical/hott/both is chosen, print that pathway even if every row is No (explicit checklist).
    """
    if not isinstance(oohca, dict):
        return {"show_medical": False, "show_hott": False}
    path = str(oohca.get("reversibleAssessmentPath") or "").strip().lower()
    if path == "medical":
        return {"show_medical": True, "show_hott": False}
    if path == "hott":
        return {"show_medical": False, "show_hott": True}
    if path == "both":
        return {"show_medical": True, "show_hott": True}

    show_med = False
    show_hott = False
    rc = oohca.get("reversibleCauses")
    if isinstance(rc, dict):
        show_med = any(_oohca_reversible_non_negative(v) for v in rc.values())
    ht = oohca.get("hottTrauma")
    if isinstance(ht, dict):
        show_hott = any(_oohca_reversible_non_negative(v) for v in ht.values())
    return {"show_medical": show_med, "show_hott": show_hott}


def _oohca_ts_sort(s: Any) -> float:
    """Best-effort sort key for Cura datetime-local style strings."""
    if s is None or not isinstance(s, str):
        return 0.0
    t = s.strip()
    if not t:
        return 0.0
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    try:
        if "T" in t:
            return datetime.fromisoformat(t).timestamp()
    except Exception:
        pass
    return 0.0


def epcr_post_rosc_meaningful(post_rosc: Any) -> bool:
    """True if at least one post-ROSC checklist item is explicitly positive (avoid printing empty grids)."""
    if not isinstance(post_rosc, dict):
        return False
    for v in post_rosc.values():
        if v is True:
            return True
        if isinstance(v, str) and v.strip().lower() in ("yes", "true", "1"):
            return True
    return False


def _oohca_shock_energy_j_text(energy: Any) -> str | None:
    """
    Return a display string like ``150 J`` only when a positive joule value was recorded.
    Never treat 0, null, or empty as documented energy (avoids falsely implying “0 J” on legal/clinical paperwork).
    """
    if energy is None:
        return None
    if isinstance(energy, bool):
        return None
    if isinstance(energy, (int, float)):
        f = float(energy)
        if f > 0 and math.isfinite(f):
            if f == int(f):
                return f"{int(f)} J"
            return f"{f} J"
        return None
    s = str(energy).strip()
    if not s:
        return None
    try:
        f = float(s)
        if f > 0 and math.isfinite(f):
            if f == int(f):
                return f"{int(f)} J"
            return f"{f} J"
    except (TypeError, ValueError):
        pass
    return None


def epcr_oohca_shock_energy_display(energy: Any) -> str:
    """Case-access PDF / table cell: joules only if explicitly recorded and positive; otherwise em dash."""
    return _oohca_shock_energy_j_text(energy) or "—"


def epcr_oohca_reversible_path_label(oohca: Any) -> str | None:
    """Human label for ``reversibleAssessmentPath`` (Cura) for PDF headers."""
    if not isinstance(oohca, dict):
        return None
    path = str(oohca.get("reversibleAssessmentPath") or "").strip().lower()
    if not path or path == "unset":
        return None
    labels = {
        "medical": "Medical cardiac arrest — 4Hs & 4Ts",
        "hott": "Traumatic arrest — HOTT",
        "both": "Both 4Hs & 4Ts and HOTT documented",
    }
    return labels.get(path, path.replace("_", " ").title())


def epcr_oohca_unified_timeline_rows(oohca: Any) -> list[dict[str, Any]]:
    """
    Single chronological view for case-access: explicit milestones (re-arrest, ROLE, hospital, notes)
    plus shocks, arrest drugs, rhythm log, current ROSC time, and pronouncement time when set.

    Mirrors the Cura OOHCA tab “master timeline” narrative for auditors and receiving hospitals.
    """
    if not isinstance(oohca, dict):
        return []
    rows: list[tuple[float, dict[str, Any]]] = []

    def add_row(at_raw: Any, category: str, detail: Any, source: str) -> None:
        at_s = str(at_raw or "").strip()
        if not at_s:
            return
        d = str(detail or "").strip() or "—"
        sk = _oohca_ts_sort(at_s)
        rows.append(
            (
                sk,
                {
                    "time": at_s.replace("T", " "),
                    "category": category,
                    "detail": d,
                    "source": source,
                },
            )
        )

    kind_labels = {
        "collapse": "Arrest — collapse",
        "rosc": "ROSC",
        "rearrest": "Re-arrest",
        "role": "ROLE / termination",
        "hospital": "Hospital conveyance",
        "note": "Note",
    }
    mt = oohca.get("masterArrestTimeline")
    if isinstance(mt, list):
        for e in mt:
            if not isinstance(e, dict):
                continue
            at = e.get("at")
            kind = str(e.get("kind") or "").strip().lower()
            cat = kind_labels.get(kind, kind or "Milestone")
            summ = e.get("summary")
            add_row(at, cat, summ, "Milestone")

    shocks = oohca.get("shocks")
    if isinstance(shocks, list):
        for i, s in enumerate(shocks, start=1):
            if not isinstance(s, dict):
                continue
            t = s.get("time")
            parts = []
            en = s.get("energy")
            jt = _oohca_shock_energy_j_text(en)
            if jt:
                parts.append(jt)
            rh = s.get("rhythm") or ""
            ro = s.get("rhythmOther") or ""
            if rh == "Other" and ro:
                parts.append(str(ro))
            elif rh:
                parts.append(str(rh))
            add_row(t, f"Shock #{i}", " — ".join(parts) if parts else "Shock delivered", "Defibrillation")

    drugs = oohca.get("arrestDrugs")
    if isinstance(drugs, list):
        for d in drugs:
            if not isinstance(d, dict):
                continue
            add_row(d.get("time"), "Arrest drug", d.get("drugName"), "ALS drug")

    rlog = oohca.get("rhythmLog")
    if isinstance(rlog, list):
        for r in rlog:
            if not isinstance(r, dict):
                continue
            add_row(r.get("time"), "Rhythm check", r.get("rhythm"), "Rhythm log")

    rt = oohca.get("roscTime")
    if rt and str(rt).strip():
        add_row(rt, "ROSC (recorded)", "Time ROSC achieved (current chart)", "Outcome")

    pr = oohca.get("pronounce")
    if isinstance(pr, dict):
        whom = str(pr.get("byWhom") or "").strip()
        sr = str(pr.get("seniorRole") or "").strip()
        tail = ", ".join(x for x in (whom, sr) if x)
        ceased = str(pr.get("ceasedResuscitationAt") or pr.get("time") or "").strip()
        vod = str(pr.get("verificationOfDeathAt") or "").strip()
        if ceased:
            add_row(ceased, "Resuscitation ceased (TOR)", tail or "—", "Pronouncement")
        if vod and vod != ceased:
            add_row(vod, "Verification of death", tail or "—", "Pronouncement")

    rows.sort(key=lambda x: (x[0], x[1].get("category") or ""))
    return [r[1] for r in rows]


def _breathing_dot_from_marker(m: Any) -> dict[str, Any] | None:
    """React Cura stores chest clicks as anteriorMarkers/posteriorMarkers {x,y,sign,id}."""
    if not isinstance(m, dict):
        return None
    try:
        x = float(m.get("x"))
        y = float(m.get("y"))
    except (TypeError, ValueError):
        return None
    sign = str(m.get("sign") or "").strip()
    if not sign:
        return None
    return {"x": x, "y": y, "sign": sign}


def epcr_breathing_case_access_view(content: Any) -> dict[str, Any]:
    """
    Normalize **B Breathing and Chest** for ``case_access_pdf.html``.

    - **Vue / legacy Cura**: ``content.chest`` (areas/drawings) + ``content.breathing.records``.
    - **React Cura** (BreathingTab.tsx): ``content.breathingRecords`` and per-record
      ``anteriorMarkers`` / ``posteriorMarkers``; optional ``draftBreathingRecord`` for in-progress dots.
    """
    empty_chest: dict[str, list[Any]] = {
        "areasAnterior": [],
        "areasPosterior": [],
        "drawingsAnterior": [],
        "drawingsPosterior": [],
    }
    if not isinstance(content, dict):
        return {"records": [], "chest": empty_chest}

    chest_raw = content.get("chest")
    chest = chest_raw if isinstance(chest_raw, dict) else {}

    breathing_raw = content.get("breathing")
    breathing = breathing_raw if isinstance(breathing_raw, dict) else {}

    def _coerce_dot_list(key: str) -> list[dict[str, Any]]:
        raw = chest.get(key)
        if not isinstance(raw, list):
            return []
        out: list[dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict) and str(item.get("sign") or "").strip():
                out.append(item)
        return out

    def _coerce_draw_list(key: str) -> list[dict[str, Any]]:
        raw = chest.get(key)
        if not isinstance(raw, list):
            return []
        out: list[dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict):
                out.append(item)
        return out

    ant_areas = _coerce_dot_list("areasAnterior")
    post_areas = _coerce_dot_list("areasPosterior")
    ant_draws = _coerce_draw_list("drawingsAnterior")
    post_draws = _coerce_draw_list("drawingsPosterior")

    legacy_recs = breathing.get("records") if isinstance(breathing.get("records"), list) else []
    react_recs = content.get("breathingRecords") if isinstance(content.get("breathingRecords"), list) else []

    records: list[Any] = list(react_recs) if len(react_recs) > 0 else list(legacy_recs)

    has_legacy_chest = bool(ant_areas or post_areas or ant_draws or post_draws)

    if not has_legacy_chest:
        for rec in react_recs:
            if not isinstance(rec, dict):
                continue
            for key, dest in (
                ("anteriorMarkers", ant_areas),
                ("posteriorMarkers", post_areas),
            ):
                raw_m = rec.get(key)
                if not isinstance(raw_m, list):
                    continue
                for m in raw_m:
                    d = _breathing_dot_from_marker(m)
                    if d is not None:
                        dest.append(d)
        draft = content.get("draftBreathingRecord")
        if isinstance(draft, dict):
            for key, dest in (
                ("anteriorMarkers", ant_areas),
                ("posteriorMarkers", post_areas),
            ):
                raw_m = draft.get(key)
                if not isinstance(raw_m, list):
                    continue
                for m in raw_m:
                    d = _breathing_dot_from_marker(m)
                    if d is not None:
                        dest.append(d)

    return {
        "records": records,
        "chest": {
            "areasAnterior": ant_areas,
            "areasPosterior": post_areas,
            "drawingsAnterior": ant_draws,
            "drawingsPosterior": post_draws,
        },
    }


def epcr_head_injury_display(val: Any) -> str:
    """Y/N/Unknown tri-state and head-injury scalars for case-access PDF (legacy ``Unk`` → ``Unknown``)."""
    if val is None:
        return "—"
    s = str(val).strip()
    if not s:
        return "—"
    if s.casefold() == "unk":
        return "Unknown"
    return s


def epcr_get_section(case_data: Any, *names: str) -> dict[str, Any]:
    """
    Return the first section dict ``{'name': ..., 'content': ...}`` whose ``name`` matches
    any of ``names`` (case-insensitive, stripped). Unknown / missing → ``{}``.
    """
    if not isinstance(case_data, dict):
        return {}
    sections = case_data.get("sections")
    if not isinstance(sections, list):
        return {}
    want = {str(n).strip().lower() for n in names if str(n).strip()}
    if not want:
        return {}
    for sec in sections:
        if not isinstance(sec, dict):
            continue
        n = str(sec.get("name") or "").strip().lower()
        if n in want:
            return sec
    return {}
