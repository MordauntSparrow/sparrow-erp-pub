"""Create CRM records from the event risk calculator or staff intake (shared pipeline)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.objects import get_db_connection

from .crm_common import uid
from .crm_event_risk import compute_event_risk_assessment, split_contact_name
from .crm_stage_history import log_opportunity_stage_change

_VENUE_LABELS = {
    "indoor": "Indoor",
    "outdoor": "Outdoor",
    "both": "Mixed indoor / outdoor",
}
_CROWD_LABELS = {
    "family": "Family / daytime",
    "mixed": "Mixed general public",
    "young_adult": "Young adult / evening-led",
}


def _json_dumps(obj: dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def ensure_account(conn, name: str) -> int:
    name = (name or "").strip() or "Unknown organisation"
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM crm_accounts WHERE name = %s LIMIT 1",
            (name[:255],),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])
        cur.execute(
            "INSERT INTO crm_accounts (name) VALUES (%s)",
            (name[:255],),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        cur.close()


def build_lead_meta(
    *,
    source: str,
    parsed: dict[str, Any],
    risk: dict[str, Any],
) -> dict[str, Any]:
    vt = (parsed.get("venue_type") or "indoor").strip().lower()
    cp = (parsed.get("crowd_profile") or "mixed").strip().lower()
    pg = risk.get("purple_guide") if isinstance(risk.get("purple_guide"), dict) else {}
    gq: dict[str, Any] = {
        "venue_type": vt,
        "venue_label": _VENUE_LABELS.get(vt, vt.replace("_", " ").title()),
        "crowd_profile": cp,
        "crowd_profile_label": _CROWD_LABELS.get(cp, cp.replace("_", " ").title()),
        "alcohol_served": bool(parsed.get("alcohol")),
        "late_finish": bool(parsed.get("late_finish")),
        "activity_risk": parsed.get("activity_risk"),
        "duration_span": parsed.get("duration_span"),
        "drug_risk": parsed.get("drug_risk"),
        "hospital_referrals": parsed.get("hospital_referrals"),
        "alcohol_level": parsed.get("alcohol_level"),
    }
    if pg:
        gq["purple_tier"] = pg.get("tier")
        gq["purple_score"] = pg.get("purple_score")
    return {
        "source": source,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "organisation_name": parsed.get("organisation_name"),
        "event_name": parsed.get("event_name"),
        "contact": {
            "name": parsed.get("contact_name"),
            "email": parsed.get("email"),
            "phone": parsed.get("phone"),
        },
        "expected_attendees": parsed.get("expected_attendees"),
        "duration_hours": str(parsed.get("duration_hours") or ""),
        "message": parsed.get("message"),
        "risk": risk,
        "guide_questions": gq,
    }


def create_opportunity_with_contact(
    *,
    account_id: int,
    opportunity_name: str,
    stage: str,
    lead_meta: dict[str, Any],
    contact_first: str,
    contact_last: str,
    email: str | None,
    phone: str | None,
) -> tuple[int, int | None]:
    """
    Insert opportunity (with lead_meta_json) and optional contact on account.
    Returns (opportunity_id, contact_id or None).
    """
    conn = get_db_connection()
    cur = conn.cursor()
    contact_id = None
    try:
        notes_lines = [
            f"Event: {lead_meta.get('event_name') or opportunity_name}",
        ]
        r = lead_meta.get("risk") or {}
        if r.get("label"):
            notes_lines.append(f"Risk band: {r.get('label')} (score {r.get('score')})")
        pg = r.get("purple_guide") if isinstance(r.get("purple_guide"), dict) else {}
        if pg.get("tier") is not None:
            notes_lines.append(
                f"Purple Guide–style tier (indicative): {pg.get('tier')} "
                f"(planning score {pg.get('purple_score')})"
            )
        if r.get("suggested_medics"):
            notes_lines.append(
                f"Guide — suggested medic team size (indicative): {r.get('suggested_medics')}"
            )
        if r.get("suggested_vehicles"):
            notes_lines.append(
                f"Guide — suggested response vehicles (indicative): {r.get('suggested_vehicles')}"
            )
        sb = r.get("staffing_breakdown") or {}
        sb_roles = sb.get("clinical_roles") or []
        if sb_roles:
            mix = "; ".join(f"{row['count']}× {row['role']}" for row in sb_roles)
            notes_lines.append(f"Guide — indicative skill mix: {mix}")
        tot = sb.get("totals") or {}
        if tot.get("on_foot_clinical") is not None and tot.get("combined_planning_headcount") is not None:
            vct = int(tot.get("response_vehicles") or 0)
            notes_lines.append(
                f"Guide — headcount summary: {tot['on_foot_clinical']} on-foot clinical posts"
                + (
                    f"; {vct} vehicle(s) (~{tot.get('vehicle_crew_positions_hint', 0)} crew) counted separately"
                    if vct
                    else ""
                )
                + f"; indicative combined before rota overlap: {tot['combined_planning_headcount']}."
            )
        if lead_meta.get("message"):
            notes_lines.append(f"Message: {lead_meta.get('message')}")
        notes = "\n".join(notes_lines)[:65000]

        cur.execute(
            """INSERT INTO crm_opportunities
            (account_id, name, stage, amount, notes, lead_meta_json)
            VALUES (%s,%s,%s,NULL,%s,%s)""",
            (
                account_id,
                opportunity_name[:255],
                stage,
                notes,
                _json_dumps(lead_meta),
            ),
        )
        opp_id = int(cur.lastrowid)

        if email or contact_first or contact_last:
            cur.execute(
                """INSERT INTO crm_contacts
                (account_id, first_name, last_name, email, phone)
                VALUES (%s,%s,%s,%s,%s)""",
                (
                    account_id,
                    (contact_first or "?")[:128],
                    (contact_last or "")[:128],
                    (email or None),
                    (phone or None),
                ),
            )
            contact_id = int(cur.lastrowid)

        conn.commit()
        log_opportunity_stage_change(opp_id, None, stage, uid())
        return opp_id, contact_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def intake_from_parsed_form(
    parsed: dict[str, Any],
    *,
    source: str,
    stage: str = "prospecting",
    account_name_override: str | None = None,
    account_id_override: int | None = None,
) -> tuple[int, int | None, dict[str, Any]]:
    """Validate minimal fields, compute risk, persist. Returns (opp_id, contact_id, lead_meta)."""
    org = parsed.get("organisation_name") or ""
    contact_name = parsed.get("contact_name") or ""
    email = parsed.get("email") or ""
    event_name = parsed.get("event_name") or ""
    if not org or not contact_name or not email or not event_name:
        raise ValueError("missing_required_fields")

    attendees = int(parsed.get("expected_attendees") or 0)
    dh = parsed.get("duration_hours")
    risk = compute_event_risk_assessment(
        expected_attendees=attendees,
        duration_hours=dh,
        venue_outdoor=bool(parsed.get("venue_outdoor")),
        alcohol=bool(parsed.get("alcohol")),
        late_finish=bool(parsed.get("late_finish")),
        crowd_profile=str(parsed.get("crowd_profile") or "mixed"),
        activity_risk=parsed.get("activity_risk"),
        drug_risk=parsed.get("drug_risk"),
        duration_span=parsed.get("duration_span"),
        hospital_referrals=parsed.get("hospital_referrals"),
        alcohol_level=parsed.get("alcohol_level"),
    )
    lead_meta = build_lead_meta(source=source, parsed=parsed, risk=risk)
    acc_name = (account_name_override or org).strip()

    if account_id_override is not None:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id FROM crm_accounts WHERE id=%s LIMIT 1",
                (int(account_id_override),),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("invalid_account")
            account_id = int(row[0])
        finally:
            cur.close()
            conn.close()
    else:
        conn = get_db_connection()
        try:
            account_id = ensure_account(conn, acc_name)
        finally:
            conn.close()

    fn, ln = split_contact_name(contact_name)
    opp_title = f"{event_name[:200]} — {org[:40]}"[:255]
    return create_opportunity_with_contact(
        account_id=account_id,
        opportunity_name=opp_title,
        stage=stage,
        lead_meta=lead_meta,
        contact_first=fn,
        contact_last=ln,
        email=email[:255],
        phone=(parsed.get("phone") or None),
    )
