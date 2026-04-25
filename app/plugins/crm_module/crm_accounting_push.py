"""CRM quotes → core accounting draft invoices (no OAuth in CRM)."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from app.core_integrations import push_draft_invoice
from app.core_integrations import repository as int_repository
from app.objects import get_db_connection

from .crm_quote_services import compute_quote_total, lines_subtotal


def sale_ref_for_quote(quote_id: int) -> str:
    return f"crm_quote:{int(quote_id)}"


def _quote_duration_hours(q: dict[str, Any]) -> Decimal | None:
    v = q.get("duration_hours")
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except InvalidOperation:
        return None


def build_invoice_kwargs_for_quote(
    conn, quote_id: int, *, lines: list[dict[str, Any]], rule_rows: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """Return kwargs for ``push_draft_invoice`` or ``None`` if the quote row is missing."""
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT q.id, q.title, q.account_id, q.crowd_size, q.duration_hours, a.name AS account_name
            FROM crm_quotes q
            LEFT JOIN crm_accounts a ON a.id = q.account_id
            WHERE q.id = %s
            """,
            (quote_id,),
        )
        qrow = cur.fetchone()
    finally:
        cur.close()
    if not qrow:
        return None

    total, _breakdown = compute_quote_total(
        line_rows=lines,
        crowd_size=qrow.get("crowd_size"),
        duration_hours=_quote_duration_hours(qrow),
        rule_rows=rule_rows,
    )
    sub = lines_subtotal(lines)
    contact_name = (qrow.get("account_name") or qrow.get("title") or f"Quote #{quote_id}").strip()
    if not contact_name:
        contact_name = f"Quote #{quote_id}"

    norm_lines: list[dict[str, str]] = []
    for row in lines:
        desc = str(row.get("description") or "").strip()
        if not desc:
            continue
        norm_lines.append(
            {
                "description": desc[:512],
                "quantity": str(row.get("quantity") or "1"),
                "unit_amount": str(row.get("unit_price") or "0"),
            }
        )
    try:
        diff = Decimal(str(total)) - sub
    except (InvalidOperation, TypeError, ValueError):
        diff = Decimal("0")
    if diff != Decimal("0"):
        norm_lines.append(
            {
                "description": "Quote rules / adjustments (computed)",
                "quantity": "1",
                "unit_amount": str(diff),
            }
        )
    if not norm_lines:
        norm_lines.append(
            {
                "description": (qrow.get("title") or f"Quote #{quote_id}")[:512],
                "quantity": "1",
                "unit_amount": str(total),
            }
        )

    return {
        "contact_name": contact_name[:255],
        "currency": "GBP",
        "line_items": norm_lines,
    }


def _load_rule_rows(conn, rule_set_id: int | None) -> list[dict[str, Any]]:
    if not rule_set_id:
        return []
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """SELECT id, sort_order, rule_type, amount, rate, percent, label, conditions_json
            FROM crm_quote_rules WHERE rule_set_id = %s ORDER BY sort_order, id""",
            (int(rule_set_id),),
        )
        return cur.fetchall() or []
    finally:
        cur.close()


def push_quote_draft_invoice(
    quote_id: int,
    *,
    lines: list[dict[str, Any]],
    rule_set_id: int | None,
    user_id: str | None,
    force: bool = False,
) -> dict[str, Any]:
    """
    Idempotent when ``accounting_external_id`` is already set (unless ``force``).

    Persists accounting_push_status / errors on the quote row.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT id, accounting_external_id, accounting_push_status, rule_set_id
                FROM crm_quotes WHERE id = %s
                """,
                (quote_id,),
            )
            qmeta = cur.fetchone()
        finally:
            cur.close()
        if not qmeta:
            return {"ok": False, "reason": "not_found", "message": "Quote not found."}

        ext_existing = (qmeta.get("accounting_external_id") or "").strip()
        if ext_existing and not force:
            return {
                "ok": True,
                "skipped": True,
                "reason": "already_pushed",
                "provider": (qmeta.get("accounting_provider") or "").strip() or None,
                "external_id": ext_existing,
                "message": "A draft was already recorded for this quote.",
            }

        rsid = rule_set_id if rule_set_id is not None else qmeta.get("rule_set_id")
        rule_rows = _load_rule_rows(conn, int(rsid) if rsid else None)
        payload = build_invoice_kwargs_for_quote(
            conn, quote_id, lines=lines, rule_rows=rule_rows
        )
        if not payload:
            return {"ok": False, "reason": "not_found", "message": "Quote not found."}

        sale_ref = sale_ref_for_quote(quote_id)
        result = push_draft_invoice(sale_ref, **payload)
        _persist_quote_accounting(conn, quote_id, result, user_id=user_id)
        return result
    finally:
        conn.close()


def _persist_quote_accounting(
    conn, quote_id: int, result: dict[str, Any], *, user_id: str | None
) -> None:
    cur = conn.cursor()
    try:
        if result.get("ok") and not result.get("skipped"):
            cur.execute(
                """
                UPDATE crm_quotes SET
                  accounting_provider = %s,
                  accounting_external_id = %s,
                  accounting_push_status = 'pushed',
                  accounting_last_error = NULL,
                  accounting_pushed_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (
                    (result.get("provider") or "").strip()[:32] or None,
                    (result.get("external_id") or "").strip()[:256] or None,
                    quote_id,
                ),
            )
        elif not result.get("ok"):
            msg = (result.get("message") or result.get("reason") or "Error")[:4000]
            cur.execute(
                """
                UPDATE crm_quotes SET
                  accounting_push_status = 'failed',
                  accounting_last_error = %s
                WHERE id = %s
                """,
                (msg, quote_id),
            )
        conn.commit()
    finally:
        cur.close()
    _ = user_id


def maybe_auto_push_on_quote_accepted(
    quote_id: int,
    *,
    prev_status: str | None,
    new_status: str,
    lines: list[dict[str, Any]],
    rule_set_id: int | None,
    user_id: str | None,
) -> None:
    if (new_status or "").strip().lower() != "accepted":
        return
    if (prev_status or "").strip().lower() == "accepted":
        return
    settings = int_repository.load_settings()
    if not settings.get("auto_draft_invoice"):
        return
    trig = (settings.get("auto_draft_trigger") or "crm_quote_accepted").strip().lower()
    if trig != "crm_quote_accepted":
        return
    push_quote_draft_invoice(
        quote_id,
        lines=lines,
        rule_set_id=rule_set_id,
        user_id=user_id,
        force=False,
    )
