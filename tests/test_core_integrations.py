"""Core accounting integrations: crypto, OAuth state, auth gate."""
from __future__ import annotations

import os

import pytest
from flask import session

from app.core_integrations import crypto, service as int_service


@pytest.fixture
def app():
    from app import create_app

    a = create_app()
    a.config["TESTING"] = True
    a.config["SECRET_KEY"] = "test-secret-key-for-integrations"
    return a


def test_encrypt_decrypt_roundtrip(app):
    with app.app_context():
        payload = {"access_token": "at", "refresh_token": "rt", "expires_in": 3600}
        enc = crypto.encrypt_token_payload(payload)
        assert "access_token" not in enc
        out = crypto.decrypt_token_payload(enc)
        assert out["access_token"] == "at"
        assert out["refresh_token"] == "rt"


def test_core_integrations_requires_login(app):
    c = app.test_client()
    r = c.get("/core/integrations")
    assert r.status_code in (302, 401)


def test_validate_oauth_state(app):
    with app.test_request_context():
        session["core_int_oauth_state_xero"] = "goodstate"
        assert int_service.validate_oauth_state("xero", "goodstate") is True
    with app.test_request_context():
        session["core_int_oauth_state_xero"] = "expected"
        assert int_service.validate_oauth_state("xero", "wrong") is False
        assert session.get("core_int_oauth_state_xero") is None
    with app.test_request_context():
        session["core_int_oauth_state_xero"] = "keep"
        assert int_service.validate_oauth_state("xero", None) is False
        assert session.get("core_int_oauth_state_xero") == "keep"


def test_safe_oauth_error_message_no_token_leak():
    from unittest.mock import MagicMock

    from app.core_integrations.providers import _safe_oauth_error_message

    resp = MagicMock()
    resp.status_code = 400
    resp.json.return_value = {
        "error": "invalid_grant",
        "access_token": "SHOULD_NOT_APPEAR",
    }
    msg = _safe_oauth_error_message("xero", resp)
    assert "SHOULD_NOT_APPEAR" not in msg
    assert "invalid_grant" in msg.lower() or "oauth" in msg.lower()


def test_collect_accounting_integration_consumer_cards():
    from app.core_integrations import consumers as ic

    plugins = {
        "crm_module": {
            "enabled": True,
            "system_name": "crm_module",
            "name": "CRM",
            "declared_permissions": [
                {"id": "crm_module.edit", "label": "CRM — create and edit"},
            ],
            "accounting_integration_consumer": {
                "summary": "Draft invoices from quotes.",
                "provider_ids": ["xero", "unknown_provider"],
                "permission_ids": ["crm_module.edit", "missing.perm"],
                "options": [
                    {"label": "Static", "value": "ok"},
                    {"label": "Default provider", "integration_setting": "default_provider"},
                ],
            },
        },
        "disabled_mod": {
            "enabled": False,
            "system_name": "disabled_mod",
            "name": "Off",
            "accounting_integration_consumer": {"summary": "hidden"},
        },
    }
    catalog = [
        {"id": "crm_module.edit", "label": "CRM — create and edit (catalog)"},
    ]
    cards = ic.collect_accounting_integration_consumer_cards(
        plugins,
        permission_catalog=catalog,
        int_settings={"default_provider": "xero", "auto_draft_invoice": True},
        core_manifest={},
    )
    assert len(cards) == 1
    c0 = cards[0]
    assert c0["plugin"] == "crm_module"
    assert c0["provider_ids"] == ["xero"]
    assert c0["permissions"][0]["label"] == "CRM — create and edit (catalog)"
    assert c0["permissions"][1]["id"] == "missing.perm"
    opt_labels = {o["label"]: o["value"] for o in c0["options"]}
    assert opt_labels["Static"] == "ok"
    assert opt_labels["Default provider"] == "xero"


def test_xero_creds_prefers_env_then_database(app):
    from unittest.mock import patch

    from app.core_integrations import service as int_service

    with app.app_context():
        with patch.dict(
            os.environ,
            {"XERO_CLIENT_ID": "env_id", "XERO_CLIENT_SECRET": "env_sec"},
            clear=False,
        ):
            with patch(
                "app.core_integrations.repository.load_oauth_client_credentials"
            ) as ld:
                ld.return_value = ("db_id", "db_sec")
                assert int_service._xero_creds() == ("env_id", "env_sec")
                ld.assert_not_called()
        with patch.dict(
            os.environ,
            {"XERO_CLIENT_ID": "", "XERO_CLIENT_SECRET": ""},
            clear=False,
        ):
            with patch(
                "app.core_integrations.repository.load_oauth_client_credentials"
            ) as ld:
                ld.return_value = ("db_id", "db_sec")
                assert int_service._xero_creds() == ("db_id", "db_sec")
                ld.assert_called_once_with("xero")


def test_oauth_app_configured(app):
    from unittest.mock import patch

    from app.core_integrations import service as int_service

    with app.app_context():
        with patch(
            "app.core_integrations.service._xero_creds", return_value=("a", "b")
        ):
            assert int_service.oauth_app_configured("xero") is True
        with patch("app.core_integrations.service._xero_creds", return_value=("a", "")):
            assert int_service.oauth_app_configured("xero") is False


def test_get_active_provider_none_without_connection(app):
    from unittest.mock import patch

    with app.app_context():
        with patch("app.core_integrations.repository.load_settings") as ls:
            with patch("app.core_integrations.repository.load_connection") as lc:
                ls.return_value = {"default_provider": "xero", "auto_draft_invoice": False}
                lc.return_value = None
                assert int_service.get_active_provider() == "none"


def test_push_draft_invoice_requires_contact_and_lines(app):
    with app.app_context():
        r = int_service.push_draft_invoice("crm_quote:1", line_items=[{"description": "x", "quantity": "1", "unit_amount": "1"}])
        assert r.get("ok") is False
        assert r.get("reason") == "missing_contact"

        r2 = int_service.push_draft_invoice("crm_quote:1", contact_name="Acme Ltd")
        assert r2.get("ok") is False
        assert r2.get("reason") == "missing_lines"
