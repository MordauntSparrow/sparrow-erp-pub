"""Cura MPI paths must not require a legacy ``patients`` table (case-first EPCR)."""

from unittest.mock import MagicMock

from app.plugins.medical_records_module import cura_mpi


def test_resolve_mpi_patient_id_never_hits_database():
    cur = MagicMock()
    assert (
        cura_mpi.resolve_mpi_patient_id(
            cur,
            {
                "nhsNumber": "1234567890",
                "dob": "1990-01-01",
                "homeAddress": {"postcode": "SW1A1AA"},
            },
        )
        is None
    )
    cur.execute.assert_not_called()


def test_enrich_case_payload_mpi_sets_mpi_from_payload_only():
    """Explicit mpiPatientId is kept; no DB lookup fills missing MPI."""
    cur = MagicMock()

    def exec_side_effect(query, params=None):
        q = query if isinstance(query, str) else str(query)
        if "information_schema.COLUMNS" in q and params == ("cases", "mpi_patient_id"):
            cur.fetchone.return_value = (1,)
        elif "information_schema.TABLES" in q and params == ("cura_locations",):
            cur.fetchone.return_value = None
        elif "SELECT id FROM cura_locations" in q:
            cur.fetchone.return_value = None
        else:
            cur.fetchone.return_value = None

    cur.execute.side_effect = exec_side_effect

    case_no_mpi = {
        "sections": [
            {
                "name": "PatientInfo",
                "content": {
                    "ptInfo": {
                        "nhsNumber": "1234567890",
                        "forename": "x",
                        "surname": "y",
                    }
                },
            }
        ]
    }
    pid, lid = cura_mpi.enrich_case_payload_mpi(cur, case_no_mpi)
    assert pid is None
    assert "mpiPatientId" not in case_no_mpi["sections"][0]["content"]["ptInfo"]

    case_with_mpi = {
        "sections": [
            {
                "name": "PatientInfo",
                "content": {"ptInfo": {"mpiPatientId": 42, "forename": "a"}},
            }
        ]
    }
    pid2, _ = cura_mpi.enrich_case_payload_mpi(cur, case_with_mpi)
    assert pid2 == 42
    assert case_with_mpi["sections"][0]["content"]["ptInfo"]["mpiPatientId"] == 42

    patient_sql = [c for c in cur.execute.call_args_list if "patients" in str(c).lower()]
    assert not patient_sql, "enrich_case_payload_mpi must not query patients table"
