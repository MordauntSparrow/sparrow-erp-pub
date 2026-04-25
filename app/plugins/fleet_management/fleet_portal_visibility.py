"""
Employee portal + /VDIs form access: Sparrow ``roles`` names + optional ``user_role_categories``
from the fleet plugin manifest (parent groups expand to child role lists).

Empty role + category selection for a slot means “no extra filter” (VDI: all portal staff;
safety: fall back to legacy mechanic/manager-style rules in ``fleet_public``).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

_MANIFEST_PATH = Path(__file__).resolve().parent / "manifest.json"


def fleet_user_role_category_definitions() -> List[dict]:
    """``user_role_categories`` from ``fleet_management/manifest.json``."""
    try:
        with open(_MANIFEST_PATH, "r", encoding="utf-8") as fh:
            m = json.load(fh)
    except Exception:
        return []
    raw = m.get("user_role_categories")
    if not isinstance(raw, list):
        return []
    out: List[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        rid = (item.get("id") or "").strip()
        if not rid:
            continue
        roles_raw = item.get("roles")
        roles: List[str] = []
        if isinstance(roles_raw, list):
            for x in roles_raw:
                s = (str(x) or "").strip().lower()
                if s:
                    roles.append(s)
        try:
            sort = int(item.get("sort", 999))
        except (TypeError, ValueError):
            sort = 999
        parent = (item.get("parent") or "").strip() or None
        out.append(
            {
                "id": rid,
                "label": (item.get("label") or rid).strip(),
                "parent": parent,
                "sort": sort,
                "roles": sorted(set(roles)),
            }
        )
    out.sort(key=lambda d: (int(d.get("sort") or 999), (d.get("id") or "").lower()))
    return out


def fleet_form_visibility_category_rows() -> List[dict]:
    """Rows for admin UI: ``id``, ``label``, ``depth``."""
    defs = fleet_user_role_category_definitions()
    by_id = {d["id"]: d for d in defs}

    def depth(iid: str) -> int:
        d = 0
        cur = (by_id.get(iid) or {}).get("parent")
        while cur:
            d += 1
            cur = (by_id.get(cur) or {}).get("parent")
        return d

    rows: List[dict] = []
    for d in defs:
        iid = d.get("id") or ""
        if not iid:
            continue
        rows.append({"id": iid, "label": d.get("label") or iid, "depth": depth(iid)})
    rows.sort(
        key=lambda r: (
            r["depth"],
            int(by_id.get(r["id"], {}).get("sort") or 999),
            (r["id"] or "").lower(),
        )
    )
    return rows


def parse_fleet_portal_visibility_json(raw: Any) -> Tuple[List[str], List[str]]:
    if raw is None:
        return [], []
    if isinstance(raw, (list, tuple)):
        return [str(x).strip().lower() for x in raw if str(x).strip()], []
    if isinstance(raw, dict):
        rlist = raw.get("roles")
        roles = [
            str(x).strip().lower()
            for x in (rlist or [])
            if isinstance(x, (str, int)) and str(x).strip()
        ]
        cats: List[str] = []
        cr = raw.get("categories")
        if isinstance(cr, list):
            for x in cr:
                s = str(x).strip()
                if s:
                    cats.append(s)
        return roles, sorted(set(cats))
    if isinstance(raw, str):
        try:
            v = json.loads(raw)
            return parse_fleet_portal_visibility_json(v)
        except Exception:
            return [], []
    return [], []


def _descendant_category_ids(root_id: str, defs: Sequence[dict]) -> Set[str]:
    by_id = {(d.get("id") or "").strip(): d for d in defs if (d.get("id") or "").strip()}
    rid = (root_id or "").strip()
    if rid not in by_id:
        return set()
    out: Set[str] = {rid}
    changed = True
    while changed:
        changed = False
        for iid, d in by_id.items():
            p = (d.get("parent") or "").strip()
            if p in out and iid not in out:
                out.add(iid)
                changed = True
    return out


def _expand_category_ids_to_roles(
    cat_ids: Sequence[str], defs: Sequence[dict]
) -> Set[str]:
    roles: Set[str] = set()
    allowed_ids = {(d.get("id") or "").strip() for d in defs if (d.get("id") or "").strip()}
    for cid in cat_ids:
        cid = (cid or "").strip()
        if not cid or cid not in allowed_ids:
            continue
        for sub in _descendant_category_ids(cid, defs):
            item = next((x for x in defs if (x.get("id") or "").strip() == sub), None)
            if not item:
                continue
            for r in item.get("roles") or []:
                s = (str(r) or "").strip().lower()
                if s:
                    roles.add(s)
    return roles


def fleet_portal_allowed_role_names(
    roles: Sequence[str], categories: Sequence[str], defs: Sequence[dict]
) -> Set[str]:
    """Union of explicit roles and roles implied by selected category groups."""
    out: Set[str] = {str(r).strip().lower() for r in roles if str(r).strip()}
    out |= _expand_category_ids_to_roles(categories, defs)
    return out


def fleet_portal_visibility_slot_configured(roles: Sequence[str], categories: Sequence[str]) -> bool:
    return bool([x for x in roles if x]) or bool([x for x in categories if x])


def fleet_role_names_match_portal_rules(
    contractor_or_user_role_names: Set[str],
    roles: Sequence[str],
    categories: Sequence[str],
    defs: Sequence[dict],
) -> bool:
    """
    True if any of the caller's role names (lowercased) is in the allowed set built from
    ``roles`` + expanded ``categories``. Empty roles and categories → not used here (caller
    applies legacy behaviour).
    """
    allowed = fleet_portal_allowed_role_names(roles, categories, defs)
    if not allowed:
        return False
    for n in contractor_or_user_role_names:
        nl = str(n).strip().lower()
        if not nl:
            continue
        if nl in allowed:
            return True
        for a in allowed:
            if a and (a in nl or nl in a):
                return True
    return False
