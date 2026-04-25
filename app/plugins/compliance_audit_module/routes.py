"""
Compliance & audit module — admin routes (assurance console, exports, PIN).
"""
from __future__ import annotations

import hmac
import json
import os
from functools import wraps

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_login import current_user, login_required

from app.objects import PluginManager, get_db_connection, has_permission

from . import pin_service
from .adapters import ALL_DOMAIN_KEYS, DOMAIN_FETCHERS, events_for_export
from .domain_labels import AUDIT_DOMAIN_LABELS, audit_domain_options
from .services import (
    GENERATOR_VERSION,
    build_evidence_zip_bytes,
    build_pdf_bytes,
    events_to_csv_bytes,
    events_to_json_bytes,
    filt_to_matter_blob,
    filters_from_matter_blob,
    insert_export_log,
    load_timeline,
    manifest_dict,
    parse_filter_args,
    serialize_timeline_filters_for_storage,
    sha256_bytes,
)
from .ui_scope import export_scope_for_ui

_plugin_manager = PluginManager(os.path.abspath("app/plugins"))
_core_manifest = _plugin_manager.get_core_manifest() or {}

_template = os.path.join(os.path.dirname(__file__), "templates")

internal_bp = Blueprint(
    "compliance_audit_internal",
    __name__,
    url_prefix="/plugin/compliance_audit_module",
    template_folder=_template,
)


def _row_cap() -> int:
    try:
        m = (_plugin_manager.load_plugins() or {}).get("compliance_audit_module") or {}
        st = m.get("settings") or {}
        v = (st.get("export_row_cap") or {}).get("value")
        return max(100, min(int(v or 8000), 50000))
    except Exception:
        return 8000


def _step_ttl_sec() -> int:
    try:
        m = (_plugin_manager.load_plugins() or {}).get("compliance_audit_module") or {}
        st = m.get("settings") or {}
        mins = (st.get("step_up_ttl_minutes") or {}).get("value")
        return max(60, int(mins or 15) * 60)
    except Exception:
        return 900


def _lockout_minutes() -> int:
    try:
        m = (_plugin_manager.load_plugins() or {}).get("compliance_audit_module") or {}
        st = m.get("settings") or {}
        v = (st.get("pin_lockout_minutes") or {}).get("value")
        return max(1, min(int(v or 15), 1440))
    except Exception:
        return 15


def _can_view_console() -> bool:
    return has_permission("compliance_audit_module.access") or has_permission(
        "compliance_audit_module.inspection"
    )


def _can_export() -> bool:
    return has_permission("compliance_audit_module.export")


def _inspection_only() -> bool:
    return has_permission("compliance_audit_module.inspection") and not has_permission(
        "compliance_audit_module.access"
    )


def _can_export_ui() -> bool:
    if not _can_export():
        return False
    if _inspection_only():
        return False
    if (request.args.get("mode") or "").strip().lower() == "inspection":
        return False
    return True


def _siem_api_authorized() -> bool:
    if getattr(current_user, "is_authenticated", False) and _can_view_console():
        return True
    tok = (
        request.headers.get("X-Compliance-Audit-Token") or request.args.get("token") or ""
    ).strip()
    expected = (os.environ.get("COMPLIANCE_AUDIT_SIEM_TOKEN") or "").strip()
    if not expected or not tok:
        return False
    try:
        return hmac.compare_digest(
            tok.encode("utf-8"),
            expected.encode("utf-8"),
        )
    except (ValueError, TypeError):
        return False


def _filt_from_request():
    """Timeline / export filters; optional saved matter via matter_id."""
    mid = (request.values.get("matter_id") or "").strip()
    if mid.isdigit():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        row = None
        try:
            cur.execute(
                """
                SELECT id, reference_code, title, note, filters_json
                FROM compliance_evidence_matters
                WHERE id = %s
                """,
                (int(mid),),
            )
            row = cur.fetchone()
        except Exception:
            row = None
        finally:
            cur.close()
            conn.close()
        if row and row.get("filters_json"):
            try:
                blob = json.loads(row["filters_json"])
                filt = filters_from_matter_blob(blob)
                return filt, row
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
    return parse_filter_args(request.values), None


def require_console(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not getattr(current_user, "is_authenticated", False):
            return redirect(url_for("routes.login", next=request.path))
        if not _can_view_console():
            flash("You do not have access to the compliance audit console.", "danger")
            return redirect(url_for("routes.dashboard"))
        return view(*args, **kwargs)

    return wrapped


def require_export(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not getattr(current_user, "is_authenticated", False):
            return redirect(url_for("routes.login", next=request.path))
        if not _can_export():
            flash("Export requires the compliance audit export permission.", "danger")
            return redirect(url_for("compliance_audit_internal.dashboard"))
        return view(*args, **kwargs)

    return wrapped


@internal_bp.route("/")
@login_required
@require_console
def dashboard():
    inspection = request.args.get("mode") == "inspection" or _inspection_only()
    return render_template(
        "admin/compliance_audit_home.html",
        config=_core_manifest,
        can_export=_can_export_ui(),
        inspection_mode=inspection,
    )


@internal_bp.route("/timeline", methods=["GET", "POST"])
@login_required
@require_console
def timeline():
    filt, matter_row = _filt_from_request()
    if filt.get("domains") is None:
        filt = {**filt, "domains": set(ALL_DOMAIN_KEYS)}
    events = load_timeline(filt, row_cap=_row_cap())
    inspection = request.args.get("mode") == "inspection" or _inspection_only()
    return render_template(
        "admin/timeline.html",
        config=_core_manifest,
        events=events,
        filt=filt,
        matter_row=matter_row,
        domain_options=audit_domain_options(),
        domain_label_map=AUDIT_DOMAIN_LABELS,
        can_export=_can_export_ui(),
        inspection_mode=inspection,
    )


@internal_bp.route("/export", methods=["GET", "POST"])
@require_export
@require_console
def export_wizard():
    if _inspection_only():
        flash("Inspection read-only access cannot run exports.", "warning")
        return redirect(url_for("compliance_audit_internal.dashboard"))
    filt, matter_row = _filt_from_request()
    if filt.get("domains") is None:
        filt = {**filt, "domains": set(ALL_DOMAIN_KEYS)}
    preview = load_timeline(filt, row_cap=min(200, _row_cap()))
    need_pin = not pin_service.user_has_pin(str(current_user.id))
    return render_template(
        "admin/export_wizard.html",
        config=_core_manifest,
        filt=filt,
        matter_row=matter_row,
        preview_count=len(preview),
        need_pin_enrol=need_pin,
        step_up_ok=pin_service.step_up_valid(session),
        domain_options=audit_domain_options(),
        can_export=True,
    )


@internal_bp.route("/pin/enrol", methods=["GET", "POST"])
@login_required
@require_export
@require_console
def pin_enrol():
    from app.objects import get_db_connection

    if _inspection_only():
        flash("Inspection read-only access cannot enrol a compliance PIN.", "warning")
        return redirect(url_for("compliance_audit_internal.dashboard"))
    if request.method == "POST":
        p1 = request.form.get("pin") or ""
        p2 = request.form.get("pin_confirm") or ""
        if p1 != p2:
            flash("PIN and confirmation do not match.", "error")
        else:
            conn = get_db_connection()
            try:
                ok, msg = pin_service.set_pin(conn, user_id=str(current_user.id), new_pin=p1)
                flash(msg, "success" if ok else "error")
                if ok:
                    return redirect(url_for("compliance_audit_internal.export_wizard"))
            finally:
                conn.close()
    return render_template(
        "admin/pin_enrol.html",
        config=_core_manifest,
        can_export=True,
    )


@internal_bp.route("/step-up", methods=["POST"])
@login_required
@require_export
@require_console
def step_up():
    from app.objects import get_db_connection

    if _inspection_only():
        flash("Inspection read-only access cannot step up for export.", "warning")
        return redirect(url_for("compliance_audit_internal.dashboard"))
    pin = request.form.get("pin") or ""
    conn = get_db_connection()
    try:
        ok, msg = pin_service.verify_pin_and_update_lockout(
            conn,
            user_id=str(current_user.id),
            pin=pin,
            ip=request.remote_addr,
            lockout_minutes=_lockout_minutes(),
        )
    finally:
        conn.close()
    if ok:
        pin_service.set_step_up(session, _step_ttl_sec())
        flash("Step-up verified. You can download exports for a limited time.", "success")
    else:
        flash(msg, "error")
    return redirect(request.referrer or url_for("compliance_audit_internal.export_wizard"))


@internal_bp.route("/export/download", methods=["POST"])
@login_required
@require_export
@require_console
def export_download():
    if _inspection_only():
        flash("Inspection read-only access cannot run exports.", "warning")
        return redirect(url_for("compliance_audit_internal.dashboard"))
    if not pin_service.step_up_valid(session):
        flash("Confirm your compliance PIN before export (step-up).", "warning")
        return redirect(url_for("compliance_audit_internal.export_wizard"))
    fmt = (request.form.get("format") or "csv").lower().strip()
    if fmt not in ("csv", "json", "pdf", "zip"):
        fmt = "csv"
    profile = (request.form.get("redaction_profile") or "standard").strip().lower()
    legacy_redact = request.form.get("redact") == "1"
    filt = parse_filter_args(request.form)
    if filt.get("domains") is None:
        filt = {**filt, "domains": set(ALL_DOMAIN_KEYS)}
    events = load_timeline(filt, row_cap=_row_cap())
    events = events_for_export(
        events,
        redaction_profile=profile,
        legacy_redact_checkbox=legacy_redact,
    )
    matter_ref = (request.form.get("matter_reference") or "").strip()[:128]
    matter_id_raw = (request.form.get("matter_id") or "").strip()
    scope = {
        "filters": serialize_timeline_filters_for_storage(filt),
        "redaction_profile": profile,
        "legacy_redact_checkbox": legacy_redact,
        "format": fmt,
    }
    if matter_ref:
        scope["matter_reference"] = matter_ref
    if matter_id_raw.isdigit():
        scope["matter_id"] = int(matter_id_raw)
    man = manifest_dict(filters=scope, row_count=len(events), generator_version=GENERATOR_VERSION)
    if fmt == "json":
        body = events_to_json_bytes(events, manifest=man)
        mime = "application/json"
        ext = "json"
    elif fmt == "pdf":
        body = build_pdf_bytes(
            events,
            manifest=man,
            watermark="CONFIDENTIAL",
        )
        mime = "application/pdf"
        ext = "pdf"
    elif fmt == "zip":
        pdf_b = build_pdf_bytes(events, manifest=man, watermark="CONFIDENTIAL")
        csv_b = events_to_csv_bytes(events)
        json_b = events_to_json_bytes(events, manifest=man)
        body = build_evidence_zip_bytes(man, pdf_b, csv_b, json_b)
        mime = "application/zip"
        ext = "zip"
    else:
        body = events_to_csv_bytes(events)
        mime = "text/csv; charset=utf-8"
        ext = "csv"
    digest = sha256_bytes(body)
    try:
        insert_export_log(
            user_id=str(current_user.id),
            export_format=fmt,
            scope=scope,
            row_count=len(events),
            ip=request.remote_addr,
            pin_ok=True,
            file_hash=digest,
            trigger_type="manual",
            stored_path=None,
        )
    except Exception:
        current_app.logger.exception("compliance_export_log insert failed; download still returned")
    fn = f"compliance_audit_{digest[:12]}.{ext}"
    return Response(
        body,
        mimetype=mime,
        headers={
            "Content-Disposition": f'attachment; filename="{fn}"',
            "Cache-Control": "no-store",
        },
    )


@internal_bp.route("/export-log")
@login_required
@require_console
def export_log():
    from app.objects import get_db_connection

    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        try:
            try:
                cur.execute(
                    """
                    SELECT id, user_id, created_at, export_format, scope_json, row_count,
                           pin_step_up_ok, file_hash, generator_version, ip_address,
                           trigger_type, stored_path
                    FROM compliance_export_log
                    ORDER BY id DESC
                    LIMIT 400
                    """
                )
            except Exception:
                cur.execute(
                    """
                    SELECT id, user_id, created_at, export_format, scope_json, row_count,
                           pin_step_up_ok, file_hash, generator_version, ip_address
                    FROM compliance_export_log
                    ORDER BY id DESC
                    LIMIT 400
                    """
                )
            rows = cur.fetchall() or []
            for row in rows:
                if "trigger_type" not in row:
                    row["trigger_type"] = "manual"
                if "stored_path" not in row:
                    row["stored_path"] = None
                row["scope_display"] = export_scope_for_ui(row.get("scope_json"))
        finally:
            cur.close()
    finally:
        conn.close()
    inspection = request.args.get("mode") == "inspection" or _inspection_only()
    return render_template(
        "admin/export_log.html",
        config=_core_manifest,
        rows=rows,
        inspection_mode=inspection,
        can_export=_can_export_ui(),
    )


@internal_bp.route("/checklist")
@internal_bp.route("/data-security-toolkit")
@login_required
@require_console
def dspt_checklist():
    """NHS DSPT orientation: official assessment is only on dsptoolkit.nhs.uk — see template disclaimer."""
    return render_template(
        "admin/dspt_checklist.html",
        config=_core_manifest,
        inspection_mode=request.args.get("mode") == "inspection",
        can_export=_can_export_ui(),
    )


@internal_bp.route("/governance-packs")
@login_required
@require_console
def governance_evidence_packs():
    """Hub: supplier vs organisation governance / DSPT / DCB orientation (see template)."""
    return render_template(
        "admin/governance_evidence_packs.html",
        config=_core_manifest,
        inspection_mode=request.args.get("mode") == "inspection",
        can_export=_can_export_ui(),
    )


@internal_bp.route("/scheduled", methods=["GET", "POST"])
@login_required
@require_console
def scheduled_admin():
    from app.objects import get_db_connection

    conn = get_db_connection()
    if request.method == "POST" and _can_export() and not _inspection_only():
        action = (request.form.get("action") or "").strip()
        cur = conn.cursor()
        try:
            if action == "add":
                label = (request.form.get("label") or "Scheduled export").strip()[:128]
                try:
                    hour = int(request.form.get("run_hour_utc") or 6)
                except ValueError:
                    hour = 6
                hour = max(0, min(hour, 23))
                try:
                    lb = int(request.form.get("lookback_days") or 1)
                except ValueError:
                    lb = 1
                lb = max(1, min(lb, 90))
                try:
                    rc = int(request.form.get("row_cap") or _row_cap())
                except ValueError:
                    rc = _row_cap()
                rc = max(100, min(rc, 50000))
                doms = [d for d in ALL_DOMAIN_KEYS if request.form.get(f"sdom_{d}") in ("1", "on", "true")]
                if not doms:
                    doms = list(ALL_DOMAIN_KEYS)
                prof = (request.form.get("redaction_profile") or "standard").strip()[:32]
                import json as _json

                cur.execute(
                    """
                    INSERT INTO compliance_scheduled_export_jobs
                    (label, run_hour_utc, lookback_days, row_cap, domains_json, redaction_profile, enabled)
                    VALUES (%s, %s, %s, %s, %s, %s, 1)
                    """,
                    (label, hour, lb, rc, _json.dumps(doms), prof),
                )
                conn.commit()
                flash("Scheduled job added.", "success")
            elif action == "toggle":
                try:
                    jid = int(request.form.get("job_id") or 0)
                except (TypeError, ValueError):
                    jid = 0
                if jid > 0:
                    cur.execute(
                        "UPDATE compliance_scheduled_export_jobs SET enabled = IF(enabled=1,0,1) WHERE id = %s",
                        (jid,),
                    )
                    conn.commit()
                    flash("Job updated.", "success")
                else:
                    flash("Invalid job.", "error")
            elif action == "delete":
                try:
                    jid = int(request.form.get("job_id") or 0)
                except (TypeError, ValueError):
                    jid = 0
                if jid > 0:
                    cur.execute("DELETE FROM compliance_scheduled_export_jobs WHERE id = %s", (jid,))
                    conn.commit()
                    flash("Job deleted.", "success")
                else:
                    flash("Invalid job.", "error")
        except Exception as e:
            conn.rollback()
            flash(str(e)[:200], "error")
        finally:
            cur.close()
    jobs = []
    cur2 = None
    try:
        cur2 = conn.cursor(dictionary=True)
        cur2.execute(
            """
            SELECT 1 FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'compliance_scheduled_export_jobs'
            """
        )
        if cur2.fetchone():
            cur2.execute(
                "SELECT * FROM compliance_scheduled_export_jobs ORDER BY id ASC"
            )
            jobs = cur2.fetchall() or []
    finally:
        if cur2:
            try:
                cur2.close()
            except Exception:
                pass
        conn.close()
    return render_template(
        "admin/scheduled.html",
        config=_core_manifest,
        can_export=_can_export_ui(),
        inspection_mode=request.args.get("mode") == "inspection" or _inspection_only(),
        jobs=jobs,
        domain_options=audit_domain_options(),
    )


@internal_bp.route("/scheduled/run-now", methods=["POST"])
@login_required
@require_export
@require_console
def scheduled_run_now():
    if _inspection_only():
        flash("Not available in inspection-only mode.", "warning")
        return redirect(url_for("compliance_audit_internal.scheduled_admin"))
    try:
        from .scheduled_export_runner import run_all_scheduled_exports_now

        run_all_scheduled_exports_now()
        flash("All enabled scheduled jobs ran; ZIPs are under static/uploads/compliance_exports/.", "success")
    except Exception as e:
        flash(str(e)[:200], "error")
    return redirect(url_for("compliance_audit_internal.scheduled_admin"))


@internal_bp.route("/gap-report")
@login_required
@require_console
def gap_report():
    from app.objects import get_db_connection

    from .gap_analysis import analyze_cases

    filt = parse_filter_args(request.values)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        gaps, n = analyze_cases(
            cur,
            date_from=filt.get("date_from"),
            date_to=filt.get("date_to"),
            limit=min(2000, _row_cap()),
        )
    finally:
        cur.close()
        conn.close()
    if (request.args.get("format") or "").lower() == "csv":
        import csv as csv_module
        import io

        buf = io.StringIO()
        w = csv_module.DictWriter(
            buf,
            fieldnames=["case_id", "gap_code", "severity", "detail", "updated_at"],
            extrasaction="ignore",
        )
        w.writeheader()
        for g in gaps:
            w.writerow(g)
        return Response(
            buf.getvalue().encode("utf-8"),
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": 'attachment; filename="compliance_gap_analysis.csv"',
                "Cache-Control": "no-store",
            },
        )
    return render_template(
        "admin/gap_report.html",
        config=_core_manifest,
        gaps=gaps,
        cases_scanned=n,
        filt=filt,
        can_export=_can_export_ui(),
        inspection_mode=request.args.get("mode") == "inspection" or _inspection_only(),
    )


@internal_bp.route("/trust-board")
@login_required
@require_console
def trust_board_pack():
    return render_template(
        "admin/trust_board.html",
        config=_core_manifest,
        can_export=_can_export_ui(),
        inspection_mode=request.args.get("mode") == "inspection" or _inspection_only(),
    )


@internal_bp.route("/ediscovery", methods=["GET", "POST"])
@login_required
@require_console
def ediscovery():
    inspection = request.args.get("mode") == "inspection" or _inspection_only()
    matters = []
    table_ok = True
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, reference_code, title, note, created_at, created_by
            FROM compliance_evidence_matters
            ORDER BY id DESC
            LIMIT 200
            """
        )
        matters = cur.fetchall() or []
    except Exception:
        table_ok = False
        matters = []
    finally:
        cur.close()
        conn.close()

    if (
        request.method == "POST"
        and not inspection
        and has_permission("compliance_audit_module.access")
        and not _inspection_only()
    ):
        action = (request.form.get("action") or "").strip()
        if action == "create":
            ref = (request.form.get("reference_code") or "").strip()[:64]
            title = (request.form.get("title") or "").strip()[:255]
            note = (request.form.get("note") or "").strip()[:4000] or None
            if not ref or not title:
                flash("Reference code and title are required.", "error")
            else:
                filt = parse_filter_args(request.form)
                payload = filt_to_matter_blob(filt)
                conn2 = get_db_connection()
                cur2 = conn2.cursor()
                try:
                    cur2.execute(
                        """
                        INSERT INTO compliance_evidence_matters
                        (reference_code, title, note, filters_json, created_by)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (ref, title, note, json.dumps(payload), str(current_user.id)),
                    )
                    conn2.commit()
                    flash("Saved evidence matter.", "success")
                except Exception as e:
                    conn2.rollback()
                    err = str(e)
                    if "1062" in err or "Duplicate" in err or "UNIQUE" in err:
                        flash("That reference code is already in use.", "error")
                    else:
                        flash("Could not save matter.", "error")
                finally:
                    cur2.close()
                    conn2.close()
            return redirect(url_for("compliance_audit_internal.ediscovery"))
    return render_template(
        "admin/ediscovery.html",
        config=_core_manifest,
        matters=matters,
        table_ok=table_ok,
        domain_options=audit_domain_options(),
        can_export=_can_export_ui(),
        inspection_mode=inspection,
        matter_admin=has_permission("compliance_audit_module.access")
        and not _inspection_only(),
    )


@internal_bp.route("/ediscovery/matter/delete", methods=["POST"])
@login_required
@require_console
def ediscovery_matter_delete():
    if _inspection_only():
        flash("Inspection mode cannot delete saved matters.", "warning")
        return redirect(url_for("compliance_audit_internal.ediscovery"))
    if not has_permission("compliance_audit_module.access"):
        flash("Saving or deleting matters requires full compliance audit access.", "danger")
        return redirect(url_for("compliance_audit_internal.ediscovery"))
    mid = (request.form.get("matter_id") or "").strip()
    if not mid.isdigit():
        flash("Invalid matter.", "error")
        return redirect(url_for("compliance_audit_internal.ediscovery"))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM compliance_evidence_matters WHERE id = %s", (int(mid),))
        conn.commit()
        flash("Matter removed.", "success")
    except Exception as e:
        conn.rollback()
        flash(str(e)[:120], "error")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("compliance_audit_internal.ediscovery"))


@internal_bp.route("/siem-dashboard")
@login_required
@require_console
def siem_dashboard():
    from .siem_feed import build_siem_snapshot

    try:
        h = int(request.args.get("hours") or 24)
    except ValueError:
        h = 24
    h = max(1, min(h, 168))
    snap = build_siem_snapshot(hours=h)
    inspection = request.args.get("mode") == "inspection" or _inspection_only()
    token_configured = bool((os.environ.get("COMPLIANCE_AUDIT_SIEM_TOKEN") or "").strip())
    return render_template(
        "admin/siem_dashboard.html",
        config=_core_manifest,
        snapshot=snap,
        hours=h,
        token_configured=token_configured,
        can_export=_can_export_ui(),
        inspection_mode=inspection,
    )


@internal_bp.get("/api/siem-snapshot")
def api_siem_snapshot():
    if not _siem_api_authorized():
        return jsonify({"error": "unauthorized"}), 401
    from .siem_feed import build_siem_snapshot

    try:
        h = int(request.args.get("hours") or 24)
    except ValueError:
        h = 24
    h = max(1, min(h, 168))
    return jsonify(build_siem_snapshot(hours=h))


@internal_bp.get("/api/audit-events")
def api_audit_events():
    if not _siem_api_authorized():
        return jsonify({"error": "unauthorized"}), 401
    from datetime import datetime

    try:
        limit = int(request.args.get("limit") or 500)
    except ValueError:
        limit = 500
    limit = max(1, min(limit, 5000))
    args = request.args.to_dict(flat=True)
    for part in (request.args.get("domains") or "").split(","):
        p = part.strip()
        if p in ALL_DOMAIN_KEYS:
            args[f"dom_{p}"] = "1"
    filt = parse_filter_args(args)
    if filt.get("domains") is None:
        filt = {**filt, "domains": set(ALL_DOMAIN_KEYS)}
    from .services import _parse_dt

    since = _parse_dt(request.args.get("since"))
    until = _parse_dt(request.args.get("until"))
    if since:
        filt["date_from"] = since
    if until:
        filt["date_to"] = until
    events = load_timeline(filt, row_cap=limit)
    events = events_for_export(
        events,
        redaction_profile="standard",
        legacy_redact_checkbox=False,
    )
    serial = []
    for e in events:
        d = dict(e)
        t = d.get("occurred_at")
        if isinstance(t, datetime):
            d["occurred_at"] = t.isoformat(sep=" ", timespec="seconds")
        serial.append(d)
    return jsonify(
        {
            "generator": GENERATOR_VERSION,
            "row_count": len(serial),
            "events": serial,
        }
    )


def get_blueprint():
    return internal_bp
