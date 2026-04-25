"""Clinical audit log categorisation for dashboard filtering."""

from app.plugins.medical_records_module.routes import _clinical_audit_log_category


def test_category_epcr_case_access():
    assert (
        _clinical_audit_log_category("EPCR access audit: alice requested access to case 12")
        == "epcr_case_access"
    )
    assert (
        _clinical_audit_log_category("Viewed EPCR case dashboard (locked_mode=True)")
        == "epcr_case_access"
    )


def test_category_epcr_cases_api():
    assert _clinical_audit_log_category("EPCR API updated case 99") == "epcr_cases_api"
    assert (
        _clinical_audit_log_category("Denied EPCR API access to case 3 (Caldicott: …)")
        == "epcr_cases_api"
    )


def test_category_cura_api():
    assert (
        _clinical_audit_log_category("Cura operational event updated id=5")
        == "cura_api"
    )


def test_category_cura_safeguarding_api():
    assert (
        _clinical_audit_log_category("safeguarding_module API created referral id=1")
        == "cura_safeguarding_api"
    )
    assert (
        _clinical_audit_log_category("Safeguarding facade PUT referral id=2")
        == "cura_safeguarding_api"
    )
    assert (
        _clinical_audit_log_category("Cura safeguarding referral updated id=7")
        == "cura_safeguarding_api"
    )


def test_category_cura_safeguarding_oversight():
    assert (
        _clinical_audit_log_category("Opened safeguarding manager list")
        == "cura_safeguarding_oversight"
    )
    assert (
        _clinical_audit_log_category("Opened safeguarding manager referral detail id=1")
        == "cura_safeguarding_oversight"
    )
    assert (
        _clinical_audit_log_category("Safeguarding oversight: verified personal PIN")
        == "cura_safeguarding_oversight"
    )
    assert (
        _clinical_audit_log_category("Safeguarding manager note referral_id=1")
        == "cura_safeguarding_oversight"
    )


def test_category_cura_mpi():
    assert _clinical_audit_log_category("cura_mpi_flags_bundle") == "cura_mpi"


def test_category_cura_mi_api():
    assert _clinical_audit_log_category("MI report submitted id=3 event=9") == "cura_mi_api"


def test_category_cura_ops_ui():
    assert _clinical_audit_log_category("Opened Cura event manager") == "cura_ops_ui"
    assert _clinical_audit_log_category("Cura ops created operational_event id=1") == "cura_ops_ui"


def test_category_other():
    assert _clinical_audit_log_category("Admin viewed patient record") == "other"
    assert _clinical_audit_log_category("") == "other"
