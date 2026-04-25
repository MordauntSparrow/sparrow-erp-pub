"""Create CRM records from the event risk calculator or staff intake (shared pipeline)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.objects import get_db_connection

from .crm_common import uid
from .crm_event_risk import compute_event_risk_from_parsed, split_contact_name
from .crm_geo_utils import routing_hint_for_intake, routing_note_line
from .crm_stage_history import log_opportunity_stage_change


class LeadIntakeMissingRequiredFields(Exception):
    """Organisation, contact name, email, or event name was empty after parsing."""


class LeadIntakeInvalidAccount(Exception):
    """Staff intake referenced an account id that does not exist."""

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
        "hospital_drive_band": parsed.get("hospital_drive_band"),
        "alcohol_level": parsed.get("alcohol_level"),
        "audience_posture": parsed.get("audience_posture"),
        "disorder_risk": parsed.get("disorder_risk"),
        "casualty_history": parsed.get("casualty_history"),
        "audience_dwell": parsed.get("audience_dwell"),
        "season": parsed.get("season"),
        "definitive_care": parsed.get("definitive_care"),
        "additional_hazards": parsed.get("additional_hazards") or [],
        "on_site_facilities": parsed.get("on_site_facilities") or [],
    }
    if pg:
        gq["purple_tier"] = pg.get("tier")
        gq["purple_score"] = pg.get("purple_score")
    if parsed.get("dispatch_base_id") is not None:
        try:
            gq["dispatch_base_id"] = int(parsed["dispatch_base_id"])
        except (TypeError, ValueError):
            pass
    if parsed.get("saved_venue_id") is not None:
        try:
            gq["saved_venue_id"] = int(parsed["saved_venue_id"])
        except (TypeError, ValueError):
            pass
    venue: dict[str, Any] = {}
    if parsed.get("venue_address"):
        venue["address"] = parsed.get("venue_address")
    if parsed.get("venue_postcode"):
        venue["postcode"] = parsed.get("venue_postcode")
    if parsed.get("venue_what3words"):
        venue["what3words"] = parsed.get("venue_what3words")
    if parsed.get("venue_lat") is not None and parsed.get("venue_lng") is not None:
        venue["lat"] = parsed.get("venue_lat")
        venue["lng"] = parsed.get("venue_lng")
    out: dict[str, Any] = {
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
    if venue:
        out["venue"] = venue
    ik = (parsed.get("intake_kind") or "").strip()
    if ik:
        out["intake_kind"] = ik
    pt_blob = parsed.get("private_transfer")
    if isinstance(pt_blob, dict) and pt_blob:
        out["private_transfer"] = pt_blob
    return out


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
        ik = (lead_meta.get("intake_kind") or "").strip()
        if ik == "private_transfer":
            notes_lines = [
                f"Private transfer: {lead_meta.get('event_name') or opportunity_name}",
            ]
        else:
            notes_lines = [
                f"Event: {lead_meta.get('event_name') or opportunity_name}",
            ]
        if ik == "private_transfer":
            pt = (
                lead_meta.get("private_transfer")
                if isinstance(lead_meta.get("private_transfer"), dict)
                else {}
            )
            if pt:
                pname = " ".join(
                    str(x).strip()
                    for x in (pt.get("patient_first"), pt.get("patient_last"))
                    if x
                ).strip()
                if pname:
                    notes_lines.append(f"Patient: {pname}")
                jd = " ".join(
                    x for x in (pt.get("transfer_date"), pt.get("pickup_time")) if x
                ).strip()
                if jd:
                    notes_lines.append(f"Requested date / time: {jd}")
                if pt.get("journey_type"):
                    notes_lines.append(f"Journey: {pt.get('journey_type')}")
                if pt.get("pickup_address"):
                    pc = pt.get("pickup_postcode") or ""
                    notes_lines.append(
                        f"Collect: {pt.get('pickup_address')}"
                        + (f" ({pc})" if pc else "")
                    )
                if pt.get("destination_address"):
                    dc = pt.get("destination_postcode") or ""
                    notes_lines.append(
                        f"Destination: {pt.get('destination_address')}"
                        + (f" ({dc})" if dc else "")
                    )
                if pt.get("crew_vehicle"):
                    notes_lines.append(f"Vehicle (requested): {pt.get('crew_vehicle')}")
                if pt.get("travel_method"):
                    notes_lines.append(f"Travel method: {pt.get('travel_method')}")
                if pt.get("clinical_notes"):
                    notes_lines.append(f"Clinical / mobility notes: {pt.get('clinical_notes')}")
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
        gq = lead_meta.get("guide_questions") or {}
        if gq.get("hospital_drive_band") and gq.get("hospital_drive_band") != "unknown":
            notes_lines.append(
                f"Drive to A&E (organiser): {gq.get('hospital_drive_band')}"
            )
        sv_ref = gq.get("saved_venue_id")
        if isinstance(sv_ref, str) and sv_ref.strip().isdigit():
            sv_ref = int(sv_ref.strip())
        elif not isinstance(sv_ref, int):
            sv_ref = None
        if sv_ref is not None:
            notes_lines.append(f"Client saved venue template (CRM id): {sv_ref}")
        inp = r.get("inputs") or {}
        opt_bits: list[str] = []
        if inp.get("disorder_risk") not in (None, "none"):
            opt_bits.append(f"disorder={inp.get('disorder_risk')}")
        if inp.get("audience_posture") not in (None, "", "unknown"):
            opt_bits.append(f"posture={inp.get('audience_posture')}")
        if inp.get("casualty_history") not in (None, "", "unknown"):
            opt_bits.append(f"casualty_history={inp.get('casualty_history')}")
        if inp.get("audience_dwell") not in (None, "", "unknown"):
            opt_bits.append(f"dwell={inp.get('audience_dwell')}")
        if inp.get("season") not in (None, "", "unknown"):
            opt_bits.append(f"season={inp.get('season')}")
        if inp.get("definitive_care") not in (None, "", "unknown"):
            opt_bits.append(f"definitive_care={inp.get('definitive_care')}")
        ah = inp.get("additional_hazards") or []
        if isinstance(ah, (list, tuple)) and ah:
            opt_bits.append("hazards=" + ",".join(str(x) for x in ah))
        osf = inp.get("on_site_facilities") or []
        if isinstance(osf, (list, tuple)) and osf:
            opt_bits.append("facilities=" + ",".join(str(x) for x in osf))
        if opt_bits:
            notes_lines.append("Optional SAG-style factors: " + ", ".join(opt_bits))
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
        vn = lead_meta.get("venue") if isinstance(lead_meta.get("venue"), dict) else {}
        if vn:
            loc_bits: list[str] = []
            if vn.get("address"):
                loc_bits.append(str(vn["address"]))
            if vn.get("postcode"):
                loc_bits.append(f"Postcode: {vn['postcode']}")
            if vn.get("what3words"):
                loc_bits.append(f"what3words: {vn['what3words']}")
            if vn.get("lat") is not None and vn.get("lng") is not None:
                loc_bits.append(f"Coordinates: {vn['lat']}, {vn['lng']}")
            if loc_bits:
                notes_lines.append("Venue (for mileage / routing): " + " · ".join(loc_bits))
        db_id = gq.get("dispatch_base_id")
        if isinstance(db_id, str) and db_id.strip().isdigit():
            db_id = int(db_id.strip())
        elif not isinstance(db_id, int):
            db_id = None
        route_hint = routing_hint_for_intake(
            conn,
            dispatch_base_id=db_id,
            account_id=account_id,
            venue=vn if vn else None,
        )
        if route_hint:
            notes_lines.append(routing_note_line(route_hint))
        if lead_meta.get("message"):
            notes_lines.append(f"Message: {lead_meta.get('message')}")
        extras = lead_meta.get("event_intake_extras")
        if isinstance(extras, list):
            for row in extras:
                if not isinstance(row, dict):
                    continue
                val = str(row.get("value") or "").strip()
                if not val:
                    continue
                lab = str(row.get("label") or row.get("key") or "Extra").strip()
                notes_lines.append(f"{lab}: {val}")
        pt_ex = lead_meta.get("private_transfer_intake_extras")
        if isinstance(pt_ex, list):
            for row in pt_ex:
                if not isinstance(row, dict):
                    continue
                val = str(row.get("value") or "").strip()
                if not val:
                    continue
                lab = str(row.get("label") or row.get("key") or "Extra").strip()
                notes_lines.append(f"{lab}: {val}")
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
    lead_meta_additions: dict[str, Any] | None = None,
) -> tuple[int, int | None, dict[str, Any]]:
    """Validate minimal fields, compute risk, persist. Returns (opp_id, contact_id, lead_meta)."""
    org = parsed.get("organisation_name") or ""
    contact_name = parsed.get("contact_name") or ""
    email = parsed.get("email") or ""
    event_name = parsed.get("event_name") or ""
    if not org or not contact_name or not email or not event_name:
        raise LeadIntakeMissingRequiredFields()

    risk = compute_event_risk_from_parsed(parsed)
    lead_meta = build_lead_meta(source=source, parsed=parsed, risk=risk)
    if lead_meta_additions:
        for mk, mv in lead_meta_additions.items():
            if mv is None:
                continue
            if isinstance(mv, (list, dict)) and not mv:
                continue
            lead_meta[mk] = mv
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
                raise LeadIntakeInvalidAccount()
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
    opp_id, contact_id = create_opportunity_with_contact(
        account_id=account_id,
        opportunity_name=opp_title,
        stage=stage,
        lead_meta=lead_meta,
        contact_first=fn,
        contact_last=ln,
        email=email[:255],
        phone=(parsed.get("phone") or None),
    )
    return opp_id, contact_id, lead_meta
