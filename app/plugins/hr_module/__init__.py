# HR module: employee details, document uploads, requests.
# Hooks for other plugins (e.g. Time Billing invoicing).


def get_contractor_employment_type(contractor_id: int):
    """
    Used by Time Billing InvoiceService when HR is installed.
    Returns 'paye' | 'self_employed' | None (None → Time Billing reads tb_contractors itself).
    """
    try:
        from app.plugins.hr_module.services import get_contractor_employment_type_for_contractor

        return get_contractor_employment_type_for_contractor(int(contractor_id))
    except Exception:
        return None


def get_contractor_invoice_address_lines(contractor_id: int):
    """Address lines for invoice PDF when contractor portal billing fields are empty."""
    try:
        from app.plugins.hr_module.services import get_contractor_invoice_address_lines as _lines

        return _lines(int(contractor_id))
    except Exception:
        return []
