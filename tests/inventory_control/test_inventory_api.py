"""
API tests for Inventory Control plugin using Flask test client.
"""
import pytest


@pytest.fixture
def app():
    from app import create_app
    return create_app()


@pytest.fixture
def client(app):
    app.config["TESTING"] = True
    return app.test_client()


def test_inventory_plugin_blueprint_registered(app):
    assert "inventory_control_internal" in app.blueprints


def test_inventory_dashboard_requires_auth(client):
    r = client.get("/plugin/inventory_control/")
    # Redirect to login or 302 when not authenticated
    assert r.status_code in (302, 401, 403)


def test_inventory_api_health_requires_auth(client):
    r = client.get("/plugin/inventory_control/api/health")
    assert r.status_code in (302, 401, 403)


def test_inventory_openapi_spec_requires_auth(client):
    r = client.get("/plugin/inventory_control/openapi.json")
    assert r.status_code in (302, 401, 403)


def test_inventory_items_list_requires_auth(client):
    r = client.get("/plugin/inventory_control/api/items")
    assert r.status_code in (302, 401, 403)
