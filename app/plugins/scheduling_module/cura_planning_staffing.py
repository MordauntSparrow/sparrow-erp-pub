"""
Compare CRM-recommended staffing (event planner / opportunity risk) to planned schedule_shifts.
"""
from __future__ import annotations

import json
import re
from typing import Any, List, Optional, Set


def parse_cura_event_config(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return {}
        try:
            v = json.loads(s)
            return dict(v) if isinstance(v, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _norm_tokens(s: str) -> Set[str]:
    t = (s or "").lower()
    words = set(re.findall(r"[a-z0-9]+", t))
    stop = {
        "posts",
        "cover",
        "the",
        "and",
        "or",
        "a",
        "per",
        "your",
        "for",
    }
    return {w for w in words if len(w) >= 3 and w not in stop}


def job_type_matches_crm_role(job_type_name: str, crm_role_label: str) -> bool:
    """
    Heuristic: CRM guide roles (e.g. 'ALS / paramedic posts') vs TB job_types.name.
    Operators can align naming; overlap on substantive tokens is enough for planning hints.
    """
    j = (job_type_name or "").strip().lower()
    r = (crm_role_label or "").strip().lower()
    if not j or not r:
        return False
    if r in j or j in r:
        return True
    for part in re.split(r"[/,]+", r):
        p = part.strip()
        if len(p) >= 4 and (p in j or j in p):
            return True
    jw = _norm_tokens(j)
    rw = _norm_tokens(r)
    return bool(jw & rw)


def build_role_coverage(
    staffing_breakdown: Optional[dict],
    shift_lines: List[dict[str, Any]],
) -> List[dict[str, Any]]:
    """Per CRM clinical role: required count vs planned shifts (job type) in the window."""
    if not staffing_breakdown or not isinstance(staffing_breakdown, dict):
        return []
    clinical = staffing_breakdown.get("clinical_roles") or []
    if not isinstance(clinical, list):
        return []
    out: List[dict[str, Any]] = []
    for cr in clinical:
        if not isinstance(cr, dict):
            continue
        role = (cr.get("role") or "").strip()
        if not role:
            continue
        try:
            req = max(0, int(cr.get("count") or 0))
        except (TypeError, ValueError):
            req = 0
        matching = [
            s
            for s in shift_lines
            if job_type_matches_crm_role(str(s.get("job_type_name") or ""), role)
        ]
        filled_slots = sum(
            1
            for s in matching
            if s.get("assignee_ids")  # non-empty list
        )
        people: Set[int] = set()
        for s in matching:
            for cid in s.get("assignee_ids") or []:
                if cid is None:
                    continue
                try:
                    people.add(int(cid))
                except (TypeError, ValueError):
                    continue
        distinct_people = len(people)
        out.append(
            {
                "role": role,
                "grade_hint": (cr.get("grade_hint") or "").strip() or None,
                "rationale": (cr.get("rationale") or "").strip() or None,
                "required_count": req,
                "matching_shift_rows": len(matching),
                "shift_rows_with_assignee": filled_slots,
                "distinct_people_on_matching_shifts": distinct_people,
                "gap_shift_rows_vs_guide": max(0, req - len(matching)),
                "unfilled_matching_shift_rows": max(0, len(matching) - filled_slots),
                "people_short_vs_guide": max(0, req - distinct_people)
                if len(matching) >= req
                else None,
            }
        )
    return out


def vehicle_gap_hint(
    staffing_breakdown: Optional[dict],
    fleet_vehicle_resource_count: int,
) -> Optional[dict[str, Any]]:
    if not staffing_breakdown or not isinstance(staffing_breakdown, dict):
        return None
    vp = staffing_breakdown.get("vehicle_package")
    if not isinstance(vp, dict):
        return None
    try:
        need = max(0, int(vp.get("vehicles") or 0))
    except (TypeError, ValueError):
        need = 0
    if need <= 0:
        return None
    return {
        "guide_vehicles": need,
        "cura_fleet_resource_rows": fleet_vehicle_resource_count,
        "gap_vs_cura_fleet": max(0, need - fleet_vehicle_resource_count),
        "crew_headcount_hint": vp.get("crew_headcount_hint"),
        "grade_hint": (vp.get("grade_hint") or "").strip() or None,
    }
