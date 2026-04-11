"""DB helpers and total calculation for CRM quotes (R2)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from .crm_quote_calc import apply_quote_rules


def lines_subtotal(rows: list[dict[str, Any]]) -> Decimal:
    t = Decimal("0")
    for row in rows:
        q = Decimal(str(row.get("quantity") or 0))
        p = Decimal(str(row.get("unit_price") or 0))
        t += q * p
    return t


def rules_dict_rows(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in raw:
        out.append(
            {
                "sort_order": r.get("sort_order", 0),
                "rule_type": r.get("rule_type"),
                "amount": r.get("amount"),
                "rate": r.get("rate"),
                "percent": r.get("percent"),
                "label": r.get("label"),
                "conditions_json": r.get("conditions_json"),
            }
        )
    return out


def compute_quote_total(
    *,
    line_rows: list[dict[str, Any]],
    crowd_size: int | None,
    duration_hours: Decimal | None,
    rule_rows: list[dict[str, Any]],
) -> tuple[Decimal, list[tuple[str, Decimal]]]:
    sub = lines_subtotal(line_rows)
    rules = rules_dict_rows(rule_rows)
    return apply_quote_rules(
        lines_subtotal=sub,
        crowd_size=crowd_size,
        duration_hours=duration_hours,
        rules=rules,
    )
