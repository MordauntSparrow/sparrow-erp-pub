"""Extra context for event plan PDFs (account / opportunity / quote labels, roster)."""
from __future__ import annotations

import json
from typing import Any

from app.objects import get_db_connection


def staff_roster_list_from_plan(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalise ``staff_roster_json`` to a list of row dicts."""
    raw = plan.get("staff_roster_json")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            v = json.loads(s)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        except json.JSONDecodeError:
            return []
    return []


def pad_staff_roster(rows: list[dict[str, Any]], size: int = 16) -> list[dict[str, Any] | None]:
    out: list[dict[str, Any] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        out[i] = r
    return out


def pad_staff_roster_for_edit(rows: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
    """Three empty slots for new plans; otherwise one slot per saved row (capped at 40)."""
    n = len(rows)
    size = max(3, min(40, n))
    return pad_staff_roster(rows, size=size)


def management_support_list_from_plan(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalise ``management_support_json`` (command / gold–silver / coordination roles)."""
    raw = plan.get("management_support_json")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            v = json.loads(s)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        except json.JSONDecodeError:
            return []
    return []


def management_support_row_nonempty(r: dict[str, Any]) -> bool:
    return any(str(r.get(k) or "").strip() for k in ("role", "name", "phone", "notes"))


def pad_management_support_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any] | None]:
    """Three empty slots for new plans; otherwise one slot per saved row (capped at 40)."""
    n = len(rows)
    size = max(3, min(40, n))
    out: list[dict[str, Any] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        out[i] = r
    return out


def load_plan_pdf_labels(plan: dict[str, Any]) -> dict[str, Any]:
    """Resolve account, opportunity, and quote display strings for the cover sheet."""
    account_name = ""
    opportunity_name = ""
    quote_label = ""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        aid = plan.get("account_id")
        if aid is not None and str(aid).strip() != "":
            try:
                cur.execute(
                    "SELECT name FROM crm_accounts WHERE id=%s",
                    (int(aid),),
                )
                ar = cur.fetchone()
                if ar:
                    account_name = (ar.get("name") or "").strip()
            except (TypeError, ValueError):
                pass
        oid = plan.get("opportunity_id")
        if oid is not None and str(oid).strip() != "":
            try:
                cur.execute(
                    "SELECT name FROM crm_opportunities WHERE id=%s",
                    (int(oid),),
                )
                orow = cur.fetchone()
                if orow:
                    opportunity_name = (orow.get("name") or "").strip()
            except (TypeError, ValueError):
                pass
        qid = plan.get("quote_id")
        if qid is not None and str(qid).strip() != "":
            try:
                cur.execute(
                    "SELECT id, title, status, revision FROM crm_quotes WHERE id=%s",
                    (int(qid),),
                )
                qr = cur.fetchone()
                if qr:
                    qt = (qr.get("title") or "").strip() or f"Quote #{qr['id']}"
                    quote_label = (
                        f"{qt} (rev {qr.get('revision') or 1}, "
                        f"{qr.get('status') or '—'})"
                    )
            except (TypeError, ValueError):
                pass
    except Exception:
        pass
    finally:
        cur.close()
        conn.close()
    return {
        "pdf_account_name": account_name,
        "pdf_opportunity_name": opportunity_name,
        "pdf_quote_label": quote_label,
    }
