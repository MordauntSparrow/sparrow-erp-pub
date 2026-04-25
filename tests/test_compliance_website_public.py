from app.plugins.compliance_module.routes import get_public_blueprints


def test_compliance_public_registers_staff_portal_and_website_policies():
    bps = get_public_blueprints()
    assert len(bps) == 2
    names = {b.name for b in bps}
    assert "public_compliance" in names
    assert "website_compliance_policies" in names
