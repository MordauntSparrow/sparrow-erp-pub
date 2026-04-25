"""
Aggregated audit metrics for the SIEM-style dashboard and JSON snapshot API.
Read-only queries against compliance module tables + a bounded timeline sample.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.objects import get_db_connection

from .services import load_timeline, parse_filter_args


def _utc_since(hours: int) -> datetime:
    h = max(1, min(int(hours), 168))
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=h)


def _table_exists(cur, name: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        """,
        (name,),
    )
    return cur.fetchone() is not None


def collect_login_pin_export_stats(*, hours: int = 24) -> dict[str, Any]:
    since = _utc_since(hours)
    out: dict[str, Any] = {
        "window_hours": hours,
        "since_utc": since.isoformat(sep=" ", timespec="seconds"),
        "login_audit": {"ok": 0, "fail": 0, "error": None},
        "pin_step_up": {"ok": 0, "fail": 0, "error": None},
        "exports": {"count": 0, "scheduled": 0, "manual": 0, "error": None},
    }
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if _table_exists(cur, "compliance_login_audit"):
            cur.execute(
                """
                SELECT success, COUNT(*) AS c
                FROM compliance_login_audit
                WHERE occurred_at >= %s
                GROUP BY success
                """,
                (since,),
            )
            for row in cur.fetchall() or []:
                if row.get("success"):
                    out["login_audit"]["ok"] = int(row["c"])
                else:
                    out["login_audit"]["fail"] = int(row["c"])
        else:
            out["login_audit"]["error"] = "table_missing"
        if _table_exists(cur, "compliance_pin_audit"):
            cur.execute(
                """
                SELECT success, COUNT(*) AS c
                FROM compliance_pin_audit
                WHERE created_at >= %s
                GROUP BY success
                """,
                (since,),
            )
            for row in cur.fetchall() or []:
                if row.get("success"):
                    out["pin_step_up"]["ok"] = int(row["c"])
                else:
                    out["pin_step_up"]["fail"] = int(row["c"])
        else:
            out["pin_step_up"]["error"] = "table_missing"
        if _table_exists(cur, "compliance_export_log"):
            cur.execute(
                "SELECT COUNT(*) AS c FROM compliance_export_log WHERE created_at >= %s",
                (since,),
            )
            r = cur.fetchone()
            out["exports"]["count"] = int(r["c"]) if r else 0
            cur.execute(
                """
                SELECT COALESCE(trigger_type, 'manual') AS t, COUNT(*) AS c
                FROM compliance_export_log
                WHERE created_at >= %s
                GROUP BY COALESCE(trigger_type, 'manual')
                """,
                (since,),
            )
            for row in cur.fetchall() or []:
                t = (row.get("t") or "manual").lower()
                c = int(row["c"])
                if t == "scheduled":
                    out["exports"]["scheduled"] = c
                else:
                    out["exports"]["manual"] += c
        else:
            out["exports"]["error"] = "table_missing"
    except Exception as e:
        err = str(e)[:200]
        out["login_audit"]["error"] = out["login_audit"]["error"] or err
        out["pin_step_up"]["error"] = out["pin_step_up"]["error"] or err
        out["exports"]["error"] = out["exports"]["error"] or err
    finally:
        cur.close()
        conn.close()
    return out


def collect_timeline_domain_counts(*, hours: int = 24, row_cap: int = 4000) -> dict[str, Any]:
    """Sample merged timeline in the window; count rows per domain (SIEM-style breakdown)."""
    from .adapters import ALL_DOMAIN_KEYS

    since = _utc_since(hours)
    args = {
        "date_from": since.strftime("%Y-%m-%d %H:%M:%S"),
        "date_to": "",
        "q": "",
    }
    for dk in ALL_DOMAIN_KEYS:
        args[f"dom_{dk}"] = "1"
    filt = parse_filter_args(args)
    filt["domains"] = set(ALL_DOMAIN_KEYS)
    events = load_timeline(filt, row_cap=min(row_cap, 8000))
    counts: dict[str, int] = {}
    for e in events:
        d = str(e.get("domain") or "unknown")
        counts[d] = counts.get(d, 0) + 1
    return {
        "window_hours": hours,
        "sample_rows": len(events),
        "row_cap": row_cap,
        "counts_by_domain": dict(sorted(counts.items(), key=lambda x: (-x[1], x[0]))),
    }


def build_siem_snapshot(*, hours: int = 24) -> dict[str, Any]:
    stats = collect_login_pin_export_stats(hours=hours)
    domains = collect_timeline_domain_counts(hours=hours)
    return {
        "generated_at_utc": datetime.now(timezone.utc)
        .replace(tzinfo=None)
        .isoformat(sep=" ", timespec="seconds"),
        "generator": "compliance_audit_module/siem_feed",
        "stats": stats,
        "timeline_sample": domains,
    }
