"""
Patient trace / prior EPCR match — scoring against stored ``cases.data`` (PatientInfo section).

Used by ``POST /api/cura/patient-trace/match``. Keeps logic testable and out of ``cura_routes.py``.
"""
from __future__ import annotations

import re
from typing import Any


def _digits_only(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"\D", "", str(s))


def normalize_nhs(val: Any) -> str | None:
    d = _digits_only(str(val).strip() if val is not None else "")
    if len(d) < 10:
        return None
    return d


def normalize_postcode(val: Any) -> str | None:
    if val is None or str(val).strip() == "":
        return None
    s = re.sub(r"\s+", "", str(val).strip().upper())
    return s or None


def normalize_phone(val: Any) -> str | None:
    d = _digits_only(str(val).strip() if val is not None else "")
    if len(d) < 10:
        return None
    return d


def normalize_dob(val: Any) -> str | None:
    if val is None or val == "":
        return None
    if isinstance(val, str):
        s = val.strip()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return s[:10]
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                from datetime import datetime

                return datetime.strptime(s[:10] if len(s) >= 10 else s, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def _norm_name(s: Any) -> str:
    if not s or not isinstance(s, str):
        return ""
    return re.sub(r"\s+", " ", s.strip().lower())


def patient_match_meta_from_case(case_data: dict[str, Any]) -> dict[str, Any]:
    """
    Slim denormalized blob for ``cases.patient_match_meta`` so Cura patient-trace can scan
    without loading full ``data`` (large JSON / embedded images).
    """
    au = case_data.get("assignedUsers")
    if not isinstance(au, list):
        au = []
    return {
        "v": 1,
        "assignedUsers": au,
        "pt": extract_ptinfo_from_case_payload(case_data),
        "presentingSnippet": extract_presenting_snippet(case_data),
    }


def extract_ptinfo_from_case_payload(case_data: dict[str, Any]) -> dict[str, Any]:
    """Pull patient fields from EPCR envelope (sections[] or top-level)."""
    out: dict[str, Any] = {
        "nhs": None,
        "dob": None,
        "forename": "",
        "surname": "",
        "postcode": None,
        "phone": None,
        "gp_surgery": None,
        "address_line": None,
    }
    if not isinstance(case_data, dict):
        return out
    pi = None
    secs = case_data.get("sections")
    if isinstance(secs, list):
        for sec in secs:
            if not isinstance(sec, dict):
                continue
            name = (sec.get("name") or "").strip()
            if name == "PatientInfo":
                pi = sec.get("content")
                break
    if pi is None:
        pi = case_data.get("PatientInfo") or case_data.get("patientInfo")
    if not isinstance(pi, dict):
        return out
    pti = pi.get("ptInfo")
    if not isinstance(pti, dict):
        pti = pi
    out["forename"] = _norm_name(pti.get("forename") or pti.get("firstName") or pti.get("first_name"))
    out["surname"] = _norm_name(pti.get("surname") or pti.get("lastName") or pti.get("last_name"))
    out["nhs"] = normalize_nhs(pti.get("nhsNumber") or pti.get("nhs_number"))
    out["dob"] = normalize_dob(pti.get("dob") or pti.get("dateOfBirth") or pti.get("date_of_birth"))
    ha = pti.get("homeAddress")
    if isinstance(ha, dict):
        out["postcode"] = normalize_postcode(ha.get("postcode") or ha.get("postCode"))
        out["phone"] = normalize_phone(ha.get("telephone") or ha.get("phone") or ha.get("mobile"))
        parts = [ha.get("address"), ha.get("line1"), ha.get("line2")]
        line = ", ".join(str(p).strip() for p in parts if p and str(p).strip())
        out["address_line"] = line[:200] if line else None
    gp = pti.get("gpSurgery") or pti.get("gp_surgery")
    if isinstance(gp, dict):
        out["gp_surgery"] = (gp.get("surgeryName") or gp.get("surgery_name") or gp.get("gpName") or "").strip() or None
    elif isinstance(gp, str) and gp.strip():
        out["gp_surgery"] = gp.strip()[:200]
    if out["postcode"] is None:
        out["postcode"] = normalize_postcode(pti.get("postcode"))
    if out["phone"] is None:
        out["phone"] = normalize_phone(pti.get("telephone") or pti.get("phone"))
    return out


def parse_query_ptinfo(body: dict[str, Any]) -> dict[str, Any]:
    """Accept ``{ \"ptInfo\": {...} }`` or flat keys."""
    if not isinstance(body, dict):
        body = {}
    src = body.get("ptInfo") if isinstance(body.get("ptInfo"), dict) else body
    if not isinstance(src, dict):
        src = {}
    return {
        "nhs": normalize_nhs(src.get("nhsNumber") or src.get("nhs_number")),
        "dob": normalize_dob(src.get("dob") or src.get("dateOfBirth") or src.get("date_of_birth")),
        "forename": _norm_name(src.get("forename") or src.get("firstName") or src.get("first_name")),
        "surname": _norm_name(src.get("surname") or src.get("lastName") or src.get("last_name")),
        "postcode": normalize_postcode(
            src.get("postcode")
            or (src.get("homeAddress") or {}).get("postcode")
            if isinstance(src.get("homeAddress"), dict)
            else None
        ),
        "phone": normalize_phone(
            src.get("telephone")
            or src.get("phone")
            or ((src.get("homeAddress") or {}).get("telephone") if isinstance(src.get("homeAddress"), dict) else None)
        ),
        "gp_surgery_norm": _norm_name(_gp_name_from_query(src) or ""),
        "address_line": None,
    }


def _gp_name_from_query(src: dict) -> str | None:
    gp = src.get("gpSurgery") or src.get("gp_surgery")
    if isinstance(gp, dict):
        v = gp.get("surgeryName") or gp.get("surgery_name") or gp.get("gpName")
        return str(v).strip()[:200] if v else None
    if isinstance(gp, str) and gp.strip():
        return gp.strip()[:200]
    return None


def enrich_query_from_body(body: dict[str, Any], q: dict[str, Any]) -> dict[str, Any]:
    src = body.get("ptInfo") if isinstance(body.get("ptInfo"), dict) else body
    if isinstance(src, dict):
        g = _gp_name_from_query(src)
        if g:
            q["gp_surgery_norm"] = _norm_name(g)
        ha = src.get("homeAddress")
        if isinstance(ha, dict) and not q.get("postcode"):
            q["postcode"] = normalize_postcode(ha.get("postcode"))
        if isinstance(ha, dict) and not q.get("phone"):
            q["phone"] = normalize_phone(ha.get("telephone") or ha.get("phone"))
    return q


def score_match(query: dict[str, Any], cand: dict[str, Any]) -> tuple[float, list[str]]:
    reasons: list[str] = []
    score = 0.0
    if query.get("nhs") and cand.get("nhs") and query["nhs"] == cand["nhs"]:
        score += 100.0
        reasons.append("nhs_number")
    if query.get("dob") and cand.get("dob") and query["dob"] == cand["dob"]:
        if query.get("postcode") and cand.get("postcode") and query["postcode"] == cand["postcode"]:
            score += 70.0
            reasons.append("dob_and_postcode")
        elif query.get("surname") and cand.get("surname") and query["surname"] == cand["surname"]:
            if query.get("forename") and cand.get("forename") and query["forename"] == cand["forename"]:
                score += 55.0
                reasons.append("dob_and_full_name")
            else:
                score += 35.0
                reasons.append("dob_and_surname")
    elif query.get("postcode") and cand.get("postcode") and query["postcode"] == cand["postcode"]:
        if query.get("surname") and cand.get("surname") and query["surname"] == cand["surname"]:
            score += 40.0
            reasons.append("postcode_and_surname")
    if query.get("phone") and cand.get("phone") and query["phone"] == cand["phone"]:
        score += 30.0
        reasons.append("phone")
        if query.get("dob") and cand.get("dob") and query["dob"] == cand["dob"]:
            score += 15.0
            reasons.append("phone_and_dob")
    qgn = (query.get("gp_surgery_norm") or "").strip()
    cgn = _norm_name(cand.get("gp_surgery") or "")
    if qgn and cgn and qgn == cgn:
        if query.get("surname") and cand.get("surname") and query["surname"] == cand["surname"]:
            score += 20.0
            reasons.append("gp_surgery_and_surname")
    return score, reasons


def recommendation_for_matches(matches: list[dict[str, Any]], query: dict[str, Any]) -> tuple[str, list[str]]:
    notes: list[str] = []
    if not matches:
        if not _query_has_minimum_signals(query):
            return "insufficient_input", ["Provide NHS number, or DOB plus (postcode or name), or phone for matching."]
        return "no_prior_cases", []
    top = matches[0]
    top_s = float(top.get("score") or 0)
    second_s = float(matches[1].get("score") or 0) if len(matches) > 1 else 0.0
    if top_s >= 100 and "nhs_number" in (top.get("reasons") or []):
        if second_s < 50:
            return "likely_same_patient", notes
        notes.append("Multiple cases share this NHS number; review dates.")
        return "ambiguous_review", notes
    if top_s >= 70:
        if top_s - second_s >= 20:
            return "likely_same_patient", notes
        notes.append("Close scores between top candidates; confirm with patient (postcode/GP).")
        return "ambiguous_review", notes
    if top_s >= 40:
        notes.append("Partial match only; verbal confirmation recommended.")
        return "ambiguous_review", notes
    notes.append("Weak match; treat as hints only.")
    return "ambiguous_review", notes


def _query_has_minimum_signals(query: dict[str, Any]) -> bool:
    n = 0
    if query.get("nhs"):
        n += 2
    if query.get("dob"):
        n += 1
    if query.get("postcode"):
        n += 1
    if query.get("surname"):
        n += 1
    if query.get("forename"):
        n += 1
    if query.get("phone"):
        n += 1
    if (query.get("gp_surgery_norm") or "").strip():
        n += 1
    return n >= 2


def build_verification_hints(cand: dict[str, Any], case_updated) -> dict[str, Any]:
    """Safe display hints for Cura (verbal check with patient)."""
    return {
        "postcodeOnFile": cand.get("postcode"),
        "gpSurgeryName": cand.get("gp_surgery"),
        "lastEncounterAt": case_updated.isoformat() if case_updated and hasattr(case_updated, "isoformat") else None,
        "patientNameHint": _name_hint(cand.get("forename"), cand.get("surname")),
        "note": "Details are from the previous encounter on file; patient may have moved — confirm before linking.",
    }


def _name_hint(fore: str | None, sur: str | None) -> str | None:
    f = (fore or "").strip()
    s = (sur or "").strip()
    if not s:
        return None
    if f:
        return (f[0].upper() + "." if f else "") + s.title()
    return s.title()


def extract_presenting_snippet(case_data: dict[str, Any], max_len: int = 160) -> str | None:
    if not isinstance(case_data, dict):
        return None
    secs = case_data.get("sections")
    if not isinstance(secs, list):
        return None
    for sec in secs:
        if not isinstance(sec, dict):
            continue
        if (sec.get("name") or "").strip() == "Presenting Complaint / History":
            c = sec.get("content")
            if isinstance(c, dict):
                t = (c.get("complaintDescription") or c.get("complaint_description") or "").strip()
                if t:
                    return t[:max_len]
    return None


def _norm_snomed_ref(obj: Any) -> dict[str, str] | None:
    """Normalise a client SNOMED concept object to ``conceptId`` + ``pt`` (preferred term)."""
    if not isinstance(obj, dict):
        return None
    cid = obj.get("conceptId") if obj.get("conceptId") is not None else obj.get("concept_id")
    if cid is None:
        return None
    s = str(cid).strip()
    if not s:
        return None
    pt = obj.get("pt") if obj.get("pt") is not None else obj.get("preferredTerm")
    pt_s = str(pt).strip()[:240] if pt is not None else ""
    return {"conceptId": s, "pt": pt_s or s}


def extract_presenting_snomed_bundle(case_data: dict[str, Any]) -> dict[str, Any]:
    """
    Primary presentation + suspected conditions (SNOMED) from EPCR section
    ``Presenting Complaint / History``. Used for operational debrief / division trending.
    """
    primary: dict[str, str] | None = None
    suspected: list[dict[str, str]] = []
    secs = case_data.get("sections") if isinstance(case_data, dict) else None
    if not isinstance(secs, list):
        return {"primary": None, "suspected": []}
    for sec in secs:
        if not isinstance(sec, dict):
            continue
        if (sec.get("name") or "").strip() != "Presenting Complaint / History":
            continue
        c = sec.get("content")
        if not isinstance(c, dict):
            break
        raw_p = c.get("presentingComplaintSnomed") if c.get("presentingComplaintSnomed") is not None else c.get(
            "presenting_complaint_snomed"
        )
        primary = _norm_snomed_ref(raw_p)
        raw_sus = (
            c.get("suspectedConditionsSnomed")
            if c.get("suspectedConditionsSnomed") is not None
            else c.get("suspected_conditions_snomed")
        )
        if isinstance(raw_sus, list):
            seen_ids: set[str] = set()
            for x in raw_sus:
                ref = _norm_snomed_ref(x)
                if ref and ref["conceptId"] not in seen_ids:
                    seen_ids.add(ref["conceptId"])
                    suspected.append(ref)
        break
    return {"primary": primary, "suspected": suspected}
