"""
Default Cura dataset payloads — aligned with Cura ecpr-fixit-buddy ``datasetService.ts``
(DEFAULT_DRUG_COSTS, DEFAULT_CLINICAL_OPTIONS, etc.) so Sparrow admin and API clients see
the same reference data as a fresh Cura install when nothing is stored on the server yet.

API GET for **missing** rows still returns ``payload: null`` (version 0). Rows that exist but
hold empty JSON are treated as unset: responses and admin UI use these baselines, and
migration ``015_cura_dataset_baseline_backfill`` persists them to the database.
"""
from __future__ import annotations

import copy
import json
from typing import Any

# JSON mirrors TypeScript defaults (numbers stay numeric for billing fields).
_DRUGS: dict[str, Any] = json.loads(
    r"""
{
  "Entonox": {"presentation": "CYLINDER", "cost": 25.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "cylinder"},
  "Oxygen": {"presentation": "CYLINDER", "cost": 20.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "cylinder"},
  "Activated Charcoal": {"presentation": "Oral", "cost": 30.0, "billingType": "aggregate", "presentationAmount": 50, "unit": "mg"},
  "Adrenaline 1:1000": {"presentation": "IV", "cost": 5.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "mg"},
  "Adrenaline 1:10000": {"presentation": "IM", "cost": 20.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "mg"},
  "Amiodarone": {"presentation": "IV", "cost": 8.0, "billingType": "aggregate", "presentationAmount": 300, "unit": "mg"},
  "Aspirin": {"presentation": "ORAL", "cost": 4.5, "billingType": "aggregate", "presentationAmount": 300, "unit": "mg"},
  "Atropine Sulphate": {"presentation": "IV", "cost": 25.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "mg"},
  "Benzylpenicillin": {"presentation": "IV", "cost": 10.0, "billingType": "aggregate", "presentationAmount": 1.2, "unit": "g"},
  "Chlorphenamine": {"presentation": "IM", "cost": 10.0, "billingType": "aggregate", "presentationAmount": 10, "unit": "mg"},
  "Dexamethasone": {"presentation": "ORAL", "cost": 3.0, "billingType": "aggregate", "presentationAmount": 2, "unit": "mg"},
  "Furosemide": {"presentation": "IV", "cost": 30.0, "billingType": "aggregate", "presentationAmount": 50, "unit": "mg"},
  "Glucagen Injection Kit": {"presentation": "IM", "cost": 26.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "mg"},
  "Glucogel dextrose gel": {"presentation": "ORAL", "cost": 8.5, "billingType": "aggregate", "presentationAmount": 1, "unit": "tube"},
  "Glyceryl Trinitrate spray": {"presentation": "ORAL", "cost": 7.5, "billingType": "aggregate", "presentationAmount": 1, "unit": "spray"},
  "Hydrocortisone": {"presentation": "IM", "cost": 5.0, "billingType": "aggregate", "presentationAmount": 100, "unit": "mg"},
  "Ibuprofen": {"presentation": "ORAL", "cost": 3.0, "billingType": "aggregate", "presentationAmount": 200, "unit": "mg"},
  "Ipratropium Bromide": {"presentation": "NEB", "cost": 5.0, "billingType": "aggregate", "presentationAmount": 500, "unit": "mcg"},
  "Magnesium Sulphate": {"presentation": "IV", "cost": 15.0, "billingType": "aggregate", "presentationAmount": 10, "unit": "mg"},
  "Morphine": {"presentation": "IV", "cost": 13.0, "billingType": "aggregate", "presentationAmount": 10, "unit": "mg"},
  "Naloxone injection": {"presentation": "IM", "cost": 18.0, "billingType": "aggregate", "presentationAmount": 400, "unit": "mcg"},
  "Ondansetron": {"presentation": "IM/IV", "cost": 8.5, "billingType": "aggregate", "presentationAmount": 4, "unit": "mg"},
  "Paracetamol": {"presentation": "ORAL", "cost": 3.0, "billingType": "aggregate", "presentationAmount": 1000, "unit": "mg"},
  "Paracetamol (IVP)": {"presentation": "IV", "cost": 15.0, "billingType": "aggregate", "presentationAmount": 1000, "unit": "mg"},
  "Prednisolone": {"presentation": "ORAL", "cost": 3.0, "billingType": "aggregate", "presentationAmount": 5, "unit": "mg"},
  "Salbutamol": {"presentation": "NEB", "cost": 5.0, "billingType": "aggregate", "presentationAmount": 5, "unit": "mg"},
  "Tranexamic acid": {"presentation": "IV", "cost": 10.0, "billingType": "aggregate", "presentationAmount": 500, "unit": "mg"},
  "Sodium Chloride 500 ml": {"presentation": "IV", "cost": 8.0, "billingType": "aggregate", "presentationAmount": 500, "unit": "ml"},
  "Glucose 500ml": {"presentation": "IV", "cost": 8.0, "billingType": "aggregate", "presentationAmount": 500, "unit": "ml"},
  "Calpol six years +": {"presentation": "ORAL", "cost": 3.5, "billingType": "aggregate", "presentationAmount": 1, "unit": "dose"},
  "Calpol sachets 2mth +": {"presentation": "ORAL", "cost": 3.5, "billingType": "aggregate", "presentationAmount": 1, "unit": "dose"},
  "Cetirizine": {"presentation": "ORAL", "cost": 3.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "tablet"},
  "Dioralyte sachets": {"presentation": "ORAL", "cost": 3.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "sachet"},
  "Lidocaine": {"presentation": "IM", "cost": 6.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "vial"},
  "Loratadine": {"presentation": "ORAL", "cost": 3.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "tablet"},
  "Salbutamol inhaler": {"presentation": "INHALER", "cost": 15.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "inhaler"},
  "Liquiband": {"presentation": "N/A", "cost": 15.0, "billingType": "aggregate", "presentationAmount": 1, "unit": "item"},
  "Piriton syrup": {"presentation": "ORAL", "cost": 3.5, "billingType": "aggregate", "presentationAmount": 1, "unit": "bottle"}
}
"""
)

_CLINICAL_INDICATORS: dict[str, Any] = {
    "strokeSymptoms": [
        "Facial drooping",
        "Arm weakness",
        "Speech difficulties",
        "Time to call emergency",
    ],
    "fallRiskFactors": [
        "Age > 65",
        "Previous falls",
        "Cognitive impairment",
        "Mobility issues",
        "Medications",
    ],
    "hypoglycaemiaLevels": [
        {
            "level": "Mild",
            "range": "3.0-3.9 mmol/L",
            "symptoms": "Trembling, sweating, hunger",
        },
        {
            "level": "Moderate",
            "range": "2.0-2.9 mmol/L",
            "symptoms": "Confusion, drowsiness, mood changes",
        },
        {
            "level": "Severe",
            "range": "< 2.0 mmol/L",
            "symptoms": "Seizures, unconsciousness",
        },
    ],
}

_IV_FLUIDS: list[Any] = [
    {"name": "Normal Saline (0.9%)", "cost": 8.0},
    {"name": "Glucose 5%", "cost": 8.0},
    {"name": "Glucose 10%", "cost": 10.0},
    {"name": "Hartmann's Solution", "cost": 9.0},
    {"name": "Sodium Chloride 500ml", "cost": 8.0},
]

_INCIDENT_RESPONSE_OPTIONS: dict[str, Any] = {
    "otherValue": "Other",
    "responseOptions": [
        {"value": "Event", "label": "Event"},
        {"value": "Flashaid", "label": "Flashaid"},
        {"value": "Care line", "label": "Care line"},
        {"value": "Other", "label": "Other"},
    ],
}

_CLINICAL_OPTIONS: dict[str, Any] = json.loads(
    r"""
{
  "ecgOptions": ["Sinus Rhythm", "Sinus Tachycardia", "Sinus Bradycardia", "Atrial Fibrillation", "Atrial Flutter", "SVT", "VT", "VF", "Asystole", "PEA", "1st Degree Block", "2nd Degree Block (Type I)", "2nd Degree Block (Type II)", "3rd Degree Block", "Bundle Branch Block", "Paced Rhythm", "STEMI", "NSTEMI", "Other"],
  "pulseLocations": ["Radial", "Brachial", "Carotid", "Femoral", "Popliteal", "Pedal", "Posterior Tibial"],
  "ivInsertionSites": ["Right ACF", "Left ACF", "Right Hand", "Left Hand", "Right Foot", "Left Foot", "Other"],
  "ioInsertionSites": ["Right Proximal Tibia", "Left Proximal Tibia", "Right Distal Tibia", "Left Distal Tibia", "Right Humerus", "Left Humerus", "Other"],
  "ivSizes": [{"size": "14G", "color": "#FF6600"}, {"size": "16G", "color": "#808080"}, {"size": "18G", "color": "#00FF00"}, {"size": "20G", "color": "#FF69B4"}, {"size": "22G", "color": "#0000FF"}, {"size": "24G", "color": "#FFFF00"}],
  "ioSizes": [{"size": "15mm", "color": "#FF69B4"}, {"size": "25mm", "color": "#0000FF"}, {"size": "45mm", "color": "#FFFF00"}],
  "woundTypes": ["Laceration", "Abrasion", "Puncture", "Avulsion", "Contusion", "Incision"],
  "woundTreatments": ["Cleaned", "Dressed", "Sutured", "Steri-strips", "Pressure Applied", "Haemostatic Agent"],
  "burnTypes": ["Thermal", "Chemical", "Electrical", "Radiation", "Friction"],
  "burnTreatments": ["Cooled", "Dressed", "Cling Film", "Burns Gel", "Irrigation"],
  "fractureTreatments": ["Splinted", "Sling", "Immobilized", "Traction", "Box Splint", "SAM Splint"],
  "dislocationTreatments": ["Splinted", "Sling", "Immobilized", "Reduced"],
  "distalChecks": ["Pulse Present", "Sensation Intact", "Movement OK", "Capillary Refill Normal"],
  "spinalAssessment1": ["Midline Tenderness", "Deformity", "Swelling", "Bruising"],
  "spinalAssessment2": ["Neurological Deficit", "Altered Sensation", "Motor Weakness", "Bladder/Bowel Issues"],
  "spinalExtrication": ["Collar Applied", "Head Blocks", "Scoop Stretcher", "Long Board", "Vacuum Mattress", "Self-Extrication", "Other"]
}
"""
)


_SNOMED_UK_AMBULANCE_EMPTY: dict[str, Any] = {
    "schemaVersion": 1,
    "profile": "uk_nhs_ambulance_emergency",
    "concepts": [],
    "source": {"kind": "none", "note": "Use Cura settings → Refresh from Snowstorm or upload JSON."},
}

CURA_BASELINE_DATASET_SLUGS = frozenset(
    {
        "drugs",
        "clinical_options",
        "clinical_indicators",
        "iv_fluids",
        "incident_response_options",
        "snomed_uk_ambulance_conditions",
    }
)


def is_cura_dataset_payload_unset(dataset_name: str, payload: Any) -> bool:
    """
    True when stored JSON is missing or empty so we should treat it as “no server data”
    and fall back to bundled defaults (admin UI, API responses, CSV/save paths).

    Unknown dataset slugs are never “unset” here — we do not replace arbitrary custom data.
    """
    key = (dataset_name or "").strip().lower().replace("-", "_")
    if key not in CURA_BASELINE_DATASET_SLUGS:
        return False
    if key == "snomed_uk_ambulance_conditions":
        if payload is None:
            return True
        if not isinstance(payload, dict):
            return True
        c = payload.get("concepts")
        return not isinstance(c, list) or len(c) == 0
    if payload is None:
        return True
    if key == "iv_fluids":
        return not isinstance(payload, list) or len(payload) == 0
    if key == "incident_response_options":
        if not isinstance(payload, dict):
            return True
        ro = payload.get("responseOptions")
        return not isinstance(ro, list) or len(ro) == 0
    if not isinstance(payload, dict) or len(payload) == 0:
        return True
    return False


def cura_resolved_dataset_payload(dataset_name: str, stored: Any) -> Any:
    """Return stored payload, or a deep copy of the baseline when storage is unset."""
    if is_cura_dataset_payload_unset(dataset_name, stored):
        return get_cura_baseline_payload(dataset_name)
    return stored


def get_cura_baseline_payload(dataset_name: str) -> Any:
    """Return a deep copy of the bundled default payload for this dataset slug, or {} if unknown."""
    key = (dataset_name or "").strip().lower().replace("-", "_")
    if key == "drugs":
        return copy.deepcopy(_DRUGS)
    if key == "clinical_options":
        return copy.deepcopy(_CLINICAL_OPTIONS)
    if key == "clinical_indicators":
        return copy.deepcopy(_CLINICAL_INDICATORS)
    if key == "iv_fluids":
        return copy.deepcopy(_IV_FLUIDS)
    if key == "incident_response_options":
        return copy.deepcopy(_INCIDENT_RESPONSE_OPTIONS)
    if key == "snomed_uk_ambulance_conditions":
        return copy.deepcopy(_SNOMED_UK_AMBULANCE_EMPTY)
    return {}
