"""Build / parse VDI schema from the admin form builder (no raw JSON for users)."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set

from werkzeug.datastructures import ImmutableMultiDict

ALLOWED_FIELD_TYPES = frozenset({"text", "number", "mileage", "bool", "photo"})

_SLUG_RE = re.compile(r"[^a-z0-9_]+")


def _slug(s: str, *, fallback: str) -> str:
    t = _SLUG_RE.sub("_", (s or "").strip().lower()).strip("_")
    if not t:
        t = fallback
    if not t[0].isalpha() and t[0] != "_":
        t = f"f_{t}"
    return t[:64]


def _parse_num(raw: Optional[str]) -> Optional[float]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return float(s) if "." in s else int(s)
    except ValueError:
        return None


def schema_from_builder_form_prefixed(
    form: ImmutableMultiDict, *, key_prefix: str
) -> Dict[str, Any]:
    """
    Same as schema_from_builder_form but field names are prefixed, e.g. key_prefix='xf_0_'
    gives xf_0_sec_0_id, xf_0_form_title, xf_0_field_0_0_type, etc.
    """
    p = (key_prefix or "").strip()
    if not p.endswith("_"):
        p = p + "_"
    title_key = f"{p}form_title"
    title = (form.get(title_key) or "").strip() or "Custom inspection"
    section_indices: Set[int] = set()
    esc = re.escape(p)
    for k in form.keys():
        m = re.match(rf"^{esc}sec_(\d+)_(id|title)$", k)
        if m:
            section_indices.add(int(m.group(1)))

    sections: List[Dict[str, Any]] = []
    seen_field_ids: Set[str] = set()
    seen_section_ids: Set[str] = set()

    for si in sorted(section_indices):
        sid = (form.get(f"{p}sec_{si}_id") or "").strip()
        stitle = (form.get(f"{p}sec_{si}_title") or "").strip()
        field_indices: Set[int] = set()
        for k in form.keys():
            m = re.match(rf"^{esc}field_{si}_(\d+)_(id|type|label)$", k)
            if m:
                field_indices.add(int(m.group(1)))
        fields: List[Dict[str, Any]] = []
        for fi in sorted(field_indices):
            fid = (form.get(f"{p}field_{si}_{fi}_id") or "").strip()
            ftype = (form.get(f"{p}field_{si}_{fi}_type") or "text").strip().lower()
            flabel = (form.get(f"{p}field_{si}_{fi}_label") or "").strip()
            if not flabel and not fid:
                continue
            if ftype not in ALLOWED_FIELD_TYPES:
                ftype = "text"
            if not fid:
                fid = _slug(flabel, fallback=f"field_{si}_{fi}")
            if fid in seen_field_ids:
                raise ValueError(f"Duplicate field id “{fid}”. Each field needs a unique id.")
            seen_field_ids.add(fid)
            req = form.get(f"{p}field_{si}_{fi}_required") in ("1", "on", "true", "yes")
            entry: Dict[str, Any] = {
                "id": fid,
                "type": ftype,
                "label": flabel or fid.replace("_", " ").title(),
                "required": req,
            }
            if ftype in ("number", "mileage"):
                mn = _parse_num(form.get(f"{p}field_{si}_{fi}_min"))
                mx = _parse_num(form.get(f"{p}field_{si}_{fi}_max"))
                st = _parse_num(form.get(f"{p}field_{si}_{fi}_step"))
                if mn is not None:
                    entry["min"] = mn
                if mx is not None:
                    entry["max"] = mx
                if st is not None:
                    entry["step"] = st
            fields.append(entry)
        if not fields and not sid and not stitle:
            continue
        if not sid:
            sid = _slug(stitle, fallback=f"section_{si}")
        if sid in seen_section_ids:
            raise ValueError(f"Duplicate section id “{sid}”. Each section needs a unique id.")
        seen_section_ids.add(sid)
        sections.append({"id": sid, "title": stitle or sid.replace("_", " ").title(), "fields": fields})

    if not sections:
        raise ValueError("Add at least one section with at least one field.")
    total_fields = sum(len(s["fields"]) for s in sections)
    if total_fields == 0:
        raise ValueError("Add at least one field in any section.")

    return {"version": 1, "title": title, "sections": sections}


def schema_from_builder_form(form: ImmutableMultiDict) -> Dict[str, Any]:
    """
    Parse POSTed builder fields into the schema dict stored as JSON server-side.
    Field names: sec_{i}_id, sec_{i}_title, field_{i}_{j}_id, type, label, required, min, max, step.
    """
    title = (form.get("form_title") or "").strip() or "Vehicle daily inspection"
    section_indices: Set[int] = set()
    for k in form.keys():
        m = re.match(r"^sec_(\d+)_(id|title)$", k)
        if m:
            section_indices.add(int(m.group(1)))

    sections: List[Dict[str, Any]] = []
    seen_field_ids: Set[str] = set()
    seen_section_ids: Set[str] = set()

    for si in sorted(section_indices):
        sid = (form.get(f"sec_{si}_id") or "").strip()
        stitle = (form.get(f"sec_{si}_title") or "").strip()
        field_indices: Set[int] = set()
        for k in form.keys():
            m = re.match(rf"^field_{si}_(\d+)_(id|type|label)$", k)
            if m:
                field_indices.add(int(m.group(1)))
        fields: List[Dict[str, Any]] = []
        for fi in sorted(field_indices):
            fid = (form.get(f"field_{si}_{fi}_id") or "").strip()
            ftype = (form.get(f"field_{si}_{fi}_type") or "text").strip().lower()
            flabel = (form.get(f"field_{si}_{fi}_label") or "").strip()
            if not flabel and not fid:
                continue
            if ftype not in ALLOWED_FIELD_TYPES:
                ftype = "text"
            if not fid:
                fid = _slug(flabel, fallback=f"field_{si}_{fi}")
            if fid in seen_field_ids:
                raise ValueError(f"Duplicate field id “{fid}”. Each field needs a unique id.")
            seen_field_ids.add(fid)
            req = form.get(f"field_{si}_{fi}_required") in ("1", "on", "true", "yes")
            entry: Dict[str, Any] = {
                "id": fid,
                "type": ftype,
                "label": flabel or fid.replace("_", " ").title(),
                "required": req,
            }
            if ftype in ("number", "mileage"):
                mn = _parse_num(form.get(f"field_{si}_{fi}_min"))
                mx = _parse_num(form.get(f"field_{si}_{fi}_max"))
                st = _parse_num(form.get(f"field_{si}_{fi}_step"))
                if mn is not None:
                    entry["min"] = mn
                if mx is not None:
                    entry["max"] = mx
                if st is not None:
                    entry["step"] = st
            fields.append(entry)
        if not fields and not sid and not stitle:
            continue
        if not sid:
            sid = _slug(stitle, fallback=f"section_{si}")
        if sid in seen_section_ids:
            raise ValueError(f"Duplicate section id “{sid}”. Each section needs a unique id.")
        seen_section_ids.add(sid)
        sections.append({"id": sid, "title": stitle or sid.replace("_", " ").title(), "fields": fields})

    if not sections:
        raise ValueError("Add at least one section with at least one field.")
    total_fields = sum(len(s["fields"]) for s in sections)
    if total_fields == 0:
        raise ValueError("Add at least one field in any section.")

    return {"version": 1, "title": title, "sections": sections}
