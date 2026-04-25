"""Denormalized patient_match_meta for Cura patient-trace (slim scan)."""

from app.plugins.medical_records_module import cura_patient_trace as cpt


def test_patient_match_meta_from_case_includes_pt_and_assigned_users():
    case = {
        "assignedUsers": ["alice", "bob"],
        "sections": [
            {
                "name": "PatientInfo",
                "content": {
                    "ptInfo": {
                        "forename": "Jane",
                        "surname": "Doe",
                        "nhsNumber": "1234567890",
                        "dob": "1990-01-15",
                        "homeAddress": {"postcode": "SW1A1AA", "telephone": "07700900123"},
                    }
                },
            },
            {
                "name": "Presenting Complaint / History",
                "content": {"complaintDescription": "Shortness of breath today"},
            },
        ],
    }
    meta = cpt.patient_match_meta_from_case(case)
    assert meta["v"] == 1
    assert meta["assignedUsers"] == ["alice", "bob"]
    assert meta["pt"]["nhs"] == "1234567890"
    assert meta["pt"]["dob"] == "1990-01-15"
    assert meta["presentingSnippet"] == "Shortness of breath today"


def test_patient_match_meta_normalizes_non_list_assigned_users():
    case = {"assignedUsers": None, "sections": []}
    meta = cpt.patient_match_meta_from_case(case)
    assert meta["assignedUsers"] == []
