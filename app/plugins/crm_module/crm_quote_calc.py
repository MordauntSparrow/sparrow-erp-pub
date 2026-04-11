"""
Quote rule calculator (pure). Rules run in sort_order on a running total starting at line-item subtotal.
Rule types: fixed_add, per_head, per_hour, percent_surcharge, minimum_charge, vat.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from .crm_rule_conditions import rule_conditions_met


def _d(v: Any) -> Decimal:
    if v is None:
        return Decimal("0")
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def apply_quote_rules(
    *,
    lines_subtotal: Decimal,
    crowd_size: int | None,
    duration_hours: Decimal | None,
    rules: list[dict[str, Any]],
) -> tuple[Decimal, list[tuple[str, Decimal]]]:
    """
    Returns (final_total, breakdown) where breakdown lines are (label, delta_or_component).
    The first row is always ("Line items", lines_subtotal) when lines_subtotal != 0,
    or omitted when zero and no lines (still show 0 start for clarity — we always show line items row).
    """
    running = _d(lines_subtotal)
    crowd = Decimal(int(crowd_size or 0))
    hours = _d(duration_hours)
    breakdown: list[tuple[str, Decimal]] = []
    breakdown.append(("Line items", running))

    ordered = sorted(rules, key=lambda r: int(r.get("sort_order") or 0))

    for r in ordered:
        if not rule_conditions_met(
            r.get("conditions_json"),
            crowd_size=crowd_size,
            duration_hours=duration_hours,
        ):
            continue

        rt = str(r.get("rule_type") or "").strip()
        raw_label = (r.get("label") or "").strip()
        label = raw_label or rt.replace("_", " ").title()

        if rt == "fixed_add":
            amt = _d(r.get("amount"))
            running += amt
            breakdown.append((label, amt))
        elif rt == "per_head":
            rate = _d(r.get("rate"))
            delta = rate * crowd
            running += delta
            breakdown.append((label, delta))
        elif rt == "per_hour":
            rate = _d(r.get("rate"))
            delta = rate * hours
            running += delta
            breakdown.append((label, delta))
        elif rt == "percent_surcharge":
            pct = _d(r.get("percent"))
            delta = running * (pct / Decimal("100"))
            running += delta
            breakdown.append((label, delta))
        elif rt == "minimum_charge":
            floor_amt = _d(r.get("amount"))
            before = running
            running = max(running, floor_amt)
            adj = running - before
            if adj != 0:
                breakdown.append((label, adj))
        elif rt == "vat":
            pct = _d(r.get("percent"))
            vat_amt = running * (pct / Decimal("100"))
            running += vat_amt
            breakdown.append((label, vat_amt))
        else:
            continue

    return running, breakdown
