"""Derive staffing / resource summary text from structured deployment roster JSON."""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Sequence

from .crm_plan_pdf_context import staff_roster_list_from_plan


def _role_phrase(roles: list[str]) -> str:
    """e.g. [ECA, ECA] -> Double ECA; [Tech, ECA] -> Tech & ECA."""
    cleaned = [r.strip() for r in roles if r and str(r).strip()]
    if not cleaned:
        return ""
    counts: Counter[str] = Counter()
    display: dict[str, str] = {}
    for r in cleaned:
        key = r.strip().lower()
        counts[key] += 1
        if key not in display:
            display[key] = r.strip()
    parts: list[str] = []
    for key in sorted(counts.keys(), key=lambda k: display[k].lower()):
        n = counts[key]
        label = display[key]
        if n == 1:
            parts.append(label)
        elif n == 2:
            parts.append(f"Double {label}")
        elif n == 3:
            parts.append(f"Triple {label}")
        else:
            parts.append(f"{n}× {label}")
    return " & ".join(parts)


def format_staff_roster_summary_text(rows: Sequence[dict[str, Any]]) -> str:
    """
    Build multi-line summary for ``resources_medics`` from roster rows.

    Groups rows that share the same callsign and post / assignment (vehicle type),
    then emits lines like ``1 - Double ECA Ambulance`` or ``1 - Tech & ECA RRV``.

    Also appends aggregate counts by role and by vehicle type.
    """
    lines: list[str] = []
    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    role_totals: Counter[str] = Counter()
    vehicle_totals: Counter[str] = Counter()
    incomplete: list[str] = []

    for raw in rows:
        if not isinstance(raw, dict):
            continue
        cs = str(raw.get("callsign") or "").strip()
        post = str(raw.get("post_assignment") or "").strip()
        role = str(raw.get("role_grade") or "").strip()
        has_any = any(
            str(raw.get(k) or "").strip()
            for k in (
                "callsign",
                "name",
                "role_grade",
                "post_assignment",
                "fleet_vehicle",
                "shift",
                "phone",
                "notes",
            )
        )
        if not has_any:
            continue
        if role:
            role_totals[role.lower()] += 1
        if post:
            vehicle_totals[post] += 1
        if cs and post and role:
            groups[(cs.upper(), post)].append(role)
        elif has_any and (cs or post or role):
            bits = []
            if cs:
                bits.append(f"callsign {cs}")
            if post:
                bits.append(post)
            if role:
                bits.append(role)
            incomplete.append(" · ".join(bits) if bits else "row")

    if groups:
        lines.append("Resource summary (by callsign & vehicle type)")
        for (cs_u, vehicle) in sorted(groups.keys(), key=lambda k: (k[0].lower(), k[1].lower())):
            phrase = _role_phrase(groups[(cs_u, vehicle)])
            if not phrase:
                continue
            lines.append(f"1 - {phrase} {vehicle} — {cs_u}")
    else:
        lines.append(
            "Resource summary: add callsign, role / grade, and post / assignment "
            "for each roster row to list crewed resources here."
        )

    if incomplete:
        lines.append("")
        lines.append("Incomplete roster rows (need callsign, post type, and role for resource lines):")
        for s in incomplete[:12]:
            lines.append(f"- {s}")
        if len(incomplete) > 12:
            lines.append(f"- … and {len(incomplete) - 12} more")

    # Aggregate totals
    if role_totals or vehicle_totals:
        lines.append("")
        lines.append("Totals")
        if role_totals:
            role_bits = []
            for k in sorted(role_totals.keys()):
                # recover display casing from first seen in rows scan
                label = k
                for raw in rows:
                    if not isinstance(raw, dict):
                        continue
                    r = str(raw.get("role_grade") or "").strip()
                    if r.lower() == k:
                        label = r
                        break
                role_bits.append(f"{label}: {role_totals[k]}")
            lines.append("Roles — " + "; ".join(role_bits))
        if vehicle_totals:
            veh_bits = [f"{v}: {vehicle_totals[v]}" for v in sorted(vehicle_totals.keys(), key=str.lower)]
            lines.append("Vehicle / post types — " + "; ".join(veh_bits))

    return "\n".join(lines).strip()[:65000] or ""


def staff_roster_summary_from_plan(plan: dict[str, Any] | None) -> str:
    """Convenience: normalise plan dict roster field and format summary."""
    rows = staff_roster_list_from_plan(plan or {})
    return format_staff_roster_summary_text(rows)
