"""Unit checks for run sheet payroll flags (no DB required)."""


def test_assignment_payroll_active_truthy_when_missing_or_one():
    from app.plugins.time_billing_module.services import RunsheetService

    assert RunsheetService._assignment_payroll_active({}) is True
    assert RunsheetService._assignment_payroll_active({"payroll_included": None}) is True
    assert RunsheetService._assignment_payroll_active({"payroll_included": 1}) is True
    assert RunsheetService._assignment_payroll_active({"payroll_included": True}) is True


def test_assignment_payroll_active_false_when_zero_or_false():
    from app.plugins.time_billing_module.services import RunsheetService

    assert RunsheetService._assignment_payroll_active({"payroll_included": 0}) is False
    assert RunsheetService._assignment_payroll_active({"payroll_included": False}) is False
    assert RunsheetService._assignment_payroll_active({"payroll_included": "0"}) is False
    assert RunsheetService._assignment_payroll_active({"payroll_included": "false"}) is False
