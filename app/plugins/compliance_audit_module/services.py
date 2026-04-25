"""Export builders, export log, PDF summary."""
from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import date, datetime
from typing import Any

import zipfile

from app.objects import get_db_connection

from .adapters import ALL_DOMAIN_KEYS, merge_timeline, manifest_dict
from .domain_labels import AUDIT_DOMAIN_LABELS

GENERATOR_VERSION = "compliance_audit_module/1.2.2"


def serialize_timeline_filters_for_storage(filt: dict[str, Any]) -> dict[str, Any]:
    """JSON-friendly filter dict for export log (sets → sorted lists, datetimes → ISO)."""
    out: dict[str, Any] = {}
    for k, v in filt.items():
        if k == "domains":
            if v is None:
                out[k] = sorted(ALL_DOMAIN_KEYS)
            elif isinstance(v, set):
                out[k] = sorted(v)
            elif isinstance(v, list):
                out[k] = sorted(v)
            else:
                out[k] = v
        elif isinstance(v, datetime):
            out[k] = v.isoformat(sep=" ", timespec="seconds")
        elif isinstance(v, date) and not isinstance(v, datetime):
            out[k] = v.isoformat()
        elif v is None:
            out[k] = None
        else:
            out[k] = v
    return out


def human_manifest_scope_summary(scope: Any, *, max_chars: int = 520) -> str:
    """Plain-text summary for PDF footer (no JSON blob). `scope` is manifest['filters']."""
    if not isinstance(scope, dict):
        return "—"
    bits: list[str] = []
    fmt = scope.get("format")
    if fmt:
        bits.append(f"format={fmt}")
    rp = scope.get("redaction_profile")
    if rp:
        bits.append(f"redaction={rp}")
    if scope.get("trigger"):
        bits.append(f"trigger={scope.get('trigger')}")
    if scope.get("label"):
        bits.append(f"job={scope.get('label')}")
    lf = scope.get("filters")
    if isinstance(lf, dict):
        for key in ("date_from", "date_to", "cad", "case_id", "path_like", "q"):
            v = lf.get(key)
            if v is not None and str(v).strip():
                bits.append(f"{key}={v}")
        for key in ("actor_sub", "entity_type_sub", "action_sub"):
            v = lf.get(key)
            if v is not None and str(v).strip():
                bits.append(f"{key}={v}")
        dom = lf.get("domains")
        if isinstance(dom, list) and dom:
            bits.append(f"domains={len(dom)}")
        elif dom:
            bits.append("domains=custom")
    dom_top = scope.get("domains")
    if isinstance(dom_top, list) and dom_top and "domains=" not in " ".join(bits):
        bits.append(f"domains={len(dom_top)}")
    if scope.get("matter_reference"):
        bits.append(f"matter={scope.get('matter_reference')}")
    if scope.get("matter_id") is not None and str(scope.get("matter_id")).strip():
        bits.append(f"matter_id={scope.get('matter_id')}")
    s = "; ".join(bits) if bits else "default scope"
    return s[:max_chars]


def _parse_dt(s: str | None):
    if not s or not str(s).strip():
        return None
    raw = str(s).strip().replace("T", " ")
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        try:
            return datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(raw[:19], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(str(s).strip().replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def parse_filter_args(args) -> dict[str, Any]:
    from datetime import timedelta

    date_from = _parse_dt(args.get("date_from"))
    date_to = _parse_dt(args.get("date_to"))
    raw_to = (args.get("date_to") or "").strip()
    if date_to and len(raw_to) == 10:
        date_to = date_to + timedelta(days=1) - timedelta(seconds=1)
    cad_raw = (args.get("cad") or "").strip()
    cad = int(cad_raw) if cad_raw.isdigit() else None
    case_raw = (args.get("case_id") or "").strip()
    case_id = int(case_raw) if case_raw.isdigit() else None
    path_like = (args.get("path_like") or "").strip() or None
    q = (args.get("q") or "").strip().lower()
    filter_applied = str(args.get("ca_filter_applied") or "").strip().lower() in (
        "1",
        "on",
        "true",
        "yes",
    )
    domains: set[str] = set()
    for dk in ALL_DOMAIN_KEYS:
        if args.get(f"dom_{dk}") in ("1", "on", "true", True):
            domains.add(dk)
    if not filter_applied and len(domains) == 0:
        domains_out: set[str] | None = None
    else:
        domains_out = domains

    def _sub(v) -> str | None:
        s = str(v or "").strip().lower()
        return s or None

    return {
        "date_from": date_from,
        "date_to": date_to,
        "cad": cad,
        "case_id": case_id,
        "path_like": path_like,
        "q": q,
        "domains": domains_out,
        "actor_sub": _sub(args.get("actor")),
        "entity_type_sub": _sub(args.get("entity_type")),
        "action_sub": _sub(args.get("action")),
    }


def load_timeline(filt: dict[str, Any], *, row_cap: int) -> list[dict[str, Any]]:
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        try:
            events = merge_timeline(
                cur,
                domains=filt.get("domains"),
                date_from=filt.get("date_from"),
                date_to=filt.get("date_to"),
                cad=filt.get("cad"),
                case_id=filt.get("case_id"),
                path_like=filt.get("path_like"),
                limit=min(row_cap, 20000),
                per_domain_limit=min(row_cap, 20000),
            )
        finally:
            cur.close()
    finally:
        conn.close()
    q = filt.get("q") or ""
    if q:
        events = [
            e
            for e in events
            if q in (e.get("summary") or "").lower()
            or q in (e.get("actor") or "").lower()
            or q in (e.get("domain") or "").lower()
            or q in (e.get("entity_id") or "").lower()
            or q in (e.get("detail_ref") or "").lower()
            or q in (e.get("action") or "").lower()
            or q in (e.get("entity_type") or "").lower()
        ]
    actor_sub = filt.get("actor_sub") or ""
    if actor_sub:
        events = [e for e in events if actor_sub in (str(e.get("actor") or "").lower())]
    et = filt.get("entity_type_sub") or ""
    if et:
        events = [e for e in events if et in (str(e.get("entity_type") or "").lower())]
    act = filt.get("action_sub") or ""
    if act:
        events = [e for e in events if act in (str(e.get("action") or "").lower())]
    return events


def filt_to_matter_blob(filt: dict[str, Any]) -> dict[str, Any]:
    """Persist-able filter dict for compliance_evidence_matters.filters_json."""
    return serialize_timeline_filters_for_storage(filt)


def filters_from_matter_blob(data: dict[str, Any]) -> dict[str, Any]:
    """Rebuild timeline filt from JSON stored on a matter row."""
    from datetime import timedelta

    date_from = _parse_dt(str(data.get("date_from") or "").strip() or None)
    raw_to = str(data.get("date_to") or "").strip()
    date_to = _parse_dt(raw_to or None)
    if date_to and len(raw_to) == 10:
        date_to = date_to + timedelta(days=1) - timedelta(seconds=1)
    cad_raw = str(data.get("cad") or "").strip()
    cad = int(cad_raw) if cad_raw.isdigit() else None
    case_raw = str(data.get("case_id") or "").strip()
    case_id = int(case_raw) if case_raw.isdigit() else None
    path_like = str(data.get("path_like") or "").strip() or None
    q = str(data.get("q") or "").strip().lower()
    doms = data.get("domains")
    if isinstance(doms, list):
        domains = set(doms) & set(ALL_DOMAIN_KEYS)
        if not domains:
            domains = set(ALL_DOMAIN_KEYS)
    else:
        domains = set(ALL_DOMAIN_KEYS)

    def _sub(v) -> str | None:
        s = str(v or "").strip().lower()
        return s or None

    return {
        "date_from": date_from,
        "date_to": date_to,
        "cad": cad,
        "case_id": case_id,
        "path_like": path_like,
        "q": q,
        "domains": domains,
        "actor_sub": _sub(data.get("actor_sub")),
        "entity_type_sub": _sub(data.get("entity_type_sub")),
        "action_sub": _sub(data.get("action_sub")),
    }


def events_to_csv_bytes(events: list[dict[str, Any]]) -> bytes:
    buf = io.StringIO()
    fields = [
        "occurred_at",
        "domain",
        "actor",
        "action",
        "entity_type",
        "entity_id",
        "summary",
        "detail_ref",
        "integrity_hint",
    ]
    w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    w.writeheader()
    for e in events:
        row = {k: e.get(k) for k in fields}
        t = row.get("occurred_at")
        if isinstance(t, datetime):
            row["occurred_at"] = t.isoformat(sep=" ", timespec="seconds")
        w.writerow(row)
    return buf.getvalue().encode("utf-8")


def events_to_json_bytes(
    events: list[dict[str, Any]],
    *,
    manifest: dict[str, Any],
) -> bytes:
    serializable = []
    for e in events:
        d = dict(e)
        t = d.get("occurred_at")
        if isinstance(t, datetime):
            d["occurred_at"] = t.isoformat(sep=" ", timespec="seconds")
        serializable.append(d)
    payload = {"manifest": manifest, "events": serializable}
    return json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")


def build_pdf_bytes(
    events: list[dict[str, Any]],
    *,
    manifest: dict[str, Any],
    watermark: str | None = None,
) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    bio = io.BytesIO()
    c = canvas.Canvas(bio, pagesize=A4)
    w, h = A4
    y = h - 50
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, "Compliance audit export — summary")
    y -= 24
    c.setFont("Helvetica", 9)
    for line in (
        f"Generated: {datetime.utcnow().isoformat(timespec='seconds')}Z",
        f"Generator: {manifest.get('generator_version')}",
        f"Row count: {manifest.get('row_count')}",
        f"Scope: {human_manifest_scope_summary(manifest.get('filters'))}",
    ):
        c.drawString(50, y, line[:1200])
        y -= 14
        if y < 80:
            c.showPage()
            y = h - 50
            c.setFont("Helvetica", 9)
    y -= 10
    c.setFont("Helvetica-Bold", 10)
    c.drawString(50, y, "Timeline (first 80 rows)")
    y -= 16
    c.setFont("Helvetica", 8)
    for e in events[:80]:
        t = e.get("occurred_at")
        ts = t.isoformat(sep=" ", timespec="seconds")[:19] if isinstance(t, datetime) else str(t)
        dom = AUDIT_DOMAIN_LABELS.get(str(e.get("domain") or ""), e.get("domain"))
        line = f"{ts} | {dom} | {e.get('entity_type')}:{e.get('entity_id')} | {(e.get('summary') or '')[:160]}"
        c.drawString(50, y, line[:2000])
        y -= 12
        if y < 60:
            c.showPage()
            y = h - 50
            c.setFont("Helvetica", 8)
    if watermark:
        c.saveState()
        c.setFillColor(colors.Color(0.85, 0.85, 0.85, alpha=0.35))
        c.setFont("Helvetica-Bold", 36)
        c.translate(w / 2, h / 2)
        c.rotate(35)
        c.drawCentredString(0, 0, watermark[:40])
        c.restoreState()
    c.save()
    return bio.getvalue()


def build_evidence_zip_bytes(
    manifest: dict[str, Any],
    pdf_bytes: bytes,
    csv_bytes: bytes,
    json_bytes: bytes,
) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "manifest.json",
            json.dumps(manifest, indent=2, ensure_ascii=False, default=str).encode("utf-8"),
        )
        zf.writestr("timeline.csv", csv_bytes)
        zf.writestr("timeline.json", json_bytes)
        zf.writestr("index.pdf", pdf_bytes)
    return buf.getvalue()


def insert_export_log(
    *,
    user_id: str | None,
    export_format: str,
    scope: dict[str, Any],
    row_count: int,
    ip: str | None,
    pin_ok: bool,
    file_hash: str | None,
    trigger_type: str = "manual",
    stored_path: str | None = None,
) -> None:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO compliance_export_log
                (user_id, export_format, scope_json, row_count, ip_address, pin_step_up_ok,
                 file_hash, generator_version, trigger_type, stored_path)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(user_id) if user_id else None,
                    export_format[:16],
                    json.dumps(scope, default=str),
                    int(row_count),
                    ip,
                    1 if pin_ok else 0,
                    (file_hash or "")[:64] or None,
                    GENERATOR_VERSION,
                    (trigger_type or "manual")[:16],
                    (stored_path or "")[:1024] or None,
                ),
            )
            conn.commit()
        finally:
            cur.close()
    finally:
        conn.close()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
