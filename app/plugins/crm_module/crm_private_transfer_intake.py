"""Parse staff private-transfer intake (field names aligned with Platinum bookAmbulance-style forms)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any


def _s(form: Any, key: str, default: str = "") -> str:
    v = form.get(key) if hasattr(form, "get") else None
    if v is None:
        return default
    return str(v).strip()


def _join_addr(p1: str, p2: str, town: str) -> str:
    return ", ".join(x for x in (p1, p2, town) if x and str(x).strip())


def parse_private_transfer_form(form: Any) -> dict[str, Any]:
    """
    Build a dict compatible with ``compute_event_risk_from_parsed`` / ``intake_from_parsed_form``.

    Uses conservative non-event defaults for risk heuristics; real operational detail lives in
    ``private_transfer`` and venue fields (pick-up as ``venue_*`` for routing hints).
    """
    patient_first = _s(form, "patientfirst")
    patient_last = _s(form, "patientlast")
    transfer_date = _s(form, "pickupdate")
    pickup_time = _s(form, "pickuptime")
    journey_type = _s(form, "journeyType")

    collect_pc = _s(form, "collectpostcode")
    dest_pc = _s(form, "destinationpostcode")
    pickup_address = _join_addr(
        _s(form, "collectstreet1"),
        _s(form, "collectstreet2"),
        _s(form, "collecttown"),
    )
    destination_address = _join_addr(
        _s(form, "destinationstreet1"),
        _s(form, "destinationstreet2"),
        _s(form, "destinationtown"),
    )

    org = (
        _s(form, "payeecompany")
        or _s(form, "payeename")
        or _s(form, "applicantsname")
        or "Private transfer"
    )
    contact_name = _s(form, "applicantsname")
    email = _s(form, "applicantsmail")
    phone = _s(form, "applicantsphone")

    cv_raw = _s(form, "crewVehicle")
    crew_vehicle = _s(form, "crewVehicle_other_text") if cv_raw == "Other" else cv_raw
    if cv_raw == "Other" and not crew_vehicle:
        crew_vehicle = "Other"
    tm_raw = _s(form, "travelMethod")
    travel_method = _s(form, "travel_other_text") if tm_raw == "Other" else tm_raw
    if tm_raw == "Other" and not travel_method:
        travel_method = "Other"

    infectious: list[str] = []
    if hasattr(form, "getlist"):
        infectious = [str(x).strip() for x in form.getlist("infectious[]") if str(x).strip()]
    medical_history = _s(form, "medicalHistory")
    additional_needs = _s(form, "additionalNeeds")
    medications = _s(form, "currentMedications")
    mobility = _s(form, "serviceMobility")
    access_collect = _s(form, "accessRequirementsCollection")
    access_dest = _s(form, "accessRequirementsDestination")

    clinical_bits = [
        mobility and f"Mobility: {mobility}",
        medical_history and f"Medical history: {medical_history}",
        medications and f"Medications: {medications}",
        additional_needs and f"Additional needs: {additional_needs}",
        infectious and f"Infectious flags: {', '.join(infectious)}",
        access_collect and f"Collect access: {access_collect}",
        access_dest and f"Destination access: {access_dest}",
    ]
    clinical_notes = "\n".join(b for b in clinical_bits if b).strip()

    event_name = (
        f"Private transfer — {patient_first} {patient_last} — {transfer_date}".replace(
            "  ", " "
        ).strip()
    )

    pt_blob: dict[str, Any] = {
        "patient_first": patient_first or None,
        "patient_last": patient_last or None,
        "pronouns": _s(form, "pronouns") or None,
        "gender": _s(form, "gender") or None,
        "dob": _s(form, "dob") or None,
        "patient_weight_kg": _s(form, "patientweight") or None,
        "transfer_date": transfer_date or None,
        "pickup_time": pickup_time or None,
        "return_date": _s(form, "returnDate") or None,
        "return_time": _s(form, "returnTime") or None,
        "journey_type": journey_type or None,
        "pickup_address": pickup_address or None,
        "pickup_postcode": collect_pc or None,
        "destination_address": destination_address or None,
        "destination_postcode": dest_pc or None,
        "crew_vehicle": crew_vehicle or None,
        "travel_method": travel_method or None,
        "escort": _s(form, "escort") or None,
        "clinical_notes": clinical_notes or None,
        "payee_name": _s(form, "payeename") or None,
        "payee_email": _s(form, "Payeemail") or None,
    }

    return {
        "intake_kind": "private_transfer",
        "organisation_name": org,
        "contact_name": contact_name,
        "email": email,
        "phone": phone or None,
        "event_name": event_name,
        "venue_address": pickup_address or None,
        "venue_postcode": collect_pc or None,
        "venue_type": "indoor",
        "expected_attendees": 80,
        "duration_hours": Decimal("4"),
        "venue_outdoor": False,
        "crowd_profile": "corporate",
        "activity_risk": "low",
        "alcohol": False,
        "late_finish": False,
        "private_transfer": pt_blob,
    }
