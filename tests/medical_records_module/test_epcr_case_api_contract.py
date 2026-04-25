"""
EPCR JSON API contract helpers (assign-on-create vs collaborators; record_version 409).
Uses a minimal Flask app context for jsonify.
"""
from unittest.mock import MagicMock

import pytest
from flask import Flask

from datetime import datetime

from app.plugins.medical_records_module.routes import (
    EPCR_CODE_USE_COLLABORATOR_ENDPOINT,
    EPCR_CODE_RECORD_VERSION_CONFLICT,
    _epcr_case_save_link_meta,
    _epcr_crew_co_assignee_others,
    _epcr_format_db_utc_naive_for_display,
    _epcr_jsonify_use_collaborator_endpoint,
    _epcr_normalize_assigned_username_list,
    _epcr_normalize_case_identity_fields,
    _epcr_operational_event_id_from_case_payload,
    _epcr_pending_request_ttl_days,
    _epcr_assigned_users_from_json_extract,
)


@pytest.fixture
def flask_app():
    return Flask(__name__)


def test_use_collaborator_endpoint_response_is_409_with_stable_code(flask_app):
    with flask_app.app_context():
        resp, code = _epcr_jsonify_use_collaborator_endpoint()
    assert code == 409
    data = resp.get_json()
    assert data["code"] == EPCR_CODE_USE_COLLABORATOR_ENDPOINT
    assert "error" in data


def test_operational_event_id_from_payload():
    assert _epcr_operational_event_id_from_case_payload({}) is None
    assert _epcr_operational_event_id_from_case_payload({"operationalEventId": 12}) == 12
    assert _epcr_operational_event_id_from_case_payload({"operational_event_id": "7"}) == 7
    assert _epcr_operational_event_id_from_case_payload({"operational_event_id": 0}) is None


def test_normalize_assigned_username_list_dedupes_lowercase():
    assert _epcr_normalize_assigned_username_list(["a", " A ", "b"]) == ["a", "b"]
    assert _epcr_normalize_assigned_username_list(["Alice", "alice", "Bob"]) == ["alice", "bob"]


def test_epcr_format_db_utc_naive_for_display_london_bst(monkeypatch):
    """After UK spring forward, UTC 22:32 → Europe/London 23:32 same calendar day."""
    monkeypatch.setenv("SPARROW_DISPLAY_TIMEZONE", "Europe/London")
    naive_utc = datetime(2026, 3, 30, 22, 32, 50)
    assert _epcr_format_db_utc_naive_for_display(naive_utc) == "2026-03-30 23:32:50"


def test_normalize_case_identity_fields_lowercases_manifest():
    p = {
        "assignedUsers": ["Crew.One"],
        "crewManifest": [{"username": "Crew.One", "displayName": "X"}],
    }
    _epcr_normalize_case_identity_fields(p)
    assert p["assignedUsers"] == ["crew.one"]
    assert p["crewManifest"][0]["username"] == "crew.one"


def test_crew_co_assignee_others_is_case_insensitive_for_caller():
    assert _epcr_crew_co_assignee_others("jsmith", ["JSmith"]) == []
    assert _epcr_crew_co_assignee_others("jsmith", ["JSmith", "other"]) == ["other"]
    assert _epcr_crew_co_assignee_others("", ["a", "b"]) == ["a", "b"]


def test_epcr_assigned_users_from_json_extract_parses_list_and_string():
    assert _epcr_assigned_users_from_json_extract(None) == []
    assert _epcr_assigned_users_from_json_extract(["x", " Y ", "x"]) == ["x", "y"]
    assert _epcr_assigned_users_from_json_extract('["crew.a", "crew.b"]') == [
        "crew.a",
        "crew.b",
    ]


def test_epcr_pending_request_ttl_days_env(monkeypatch):
    monkeypatch.delenv("EPCR_ACCESS_REQUEST_PENDING_DAYS", raising=False)
    assert _epcr_pending_request_ttl_days() == 7
    monkeypatch.setenv("EPCR_ACCESS_REQUEST_PENDING_DAYS", "3")
    assert _epcr_pending_request_ttl_days() == 3
    monkeypatch.setenv("EPCR_ACCESS_REQUEST_PENDING_DAYS", "999")
    assert _epcr_pending_request_ttl_days() == 90
    monkeypatch.setenv("EPCR_ACCESS_REQUEST_PENDING_DAYS", "0")
    assert _epcr_pending_request_ttl_days() == 1


def test_record_version_conflict_includes_latest_fragment(flask_app):
    from app.plugins.medical_records_module.routes import _epcr_record_version_conflict_response

    cursor = MagicMock()
    uat = datetime(2026, 3, 27, 12, 0, 0)
    cursor.fetchone.return_value = ("in progress", None, uat)

    with flask_app.app_context():
        resp, code = _epcr_record_version_conflict_response(42, 7, cursor=cursor)

    assert code == 409
    data = resp.get_json()
    assert data["code"] == EPCR_CODE_RECORD_VERSION_CONFLICT
    assert data["recordVersion"] == 7
    assert data["serverAck"]["caseId"] == 42
    assert data["serverAck"]["recordVersion"] == 7
    latest = data["latest"]
    assert latest["recordVersion"] == 7
    assert latest["status"] == "in progress"
    assert latest["closedAt"] is None
    assert latest["closed_at"] is None
    assert latest["updatedAt"] == uat.isoformat()
    assert latest["updated_at"] == uat.isoformat()


def test_epcr_case_save_link_meta_version_mismatch_returns_conflict(flask_app):
    cursor = MagicMock()
    cursor.fetchone.return_value = ("closed", None, datetime(2026, 1, 1, 0, 0, 0))
    payload = {"recordVersion": 1}
    ex_meta = (None, None, None, 3)

    with flask_app.app_context():
        dr, pc, ds, nrv, err = _epcr_case_save_link_meta(
            payload,
            ex_meta,
            version_conflict_case_id=99,
            version_conflict_cursor=cursor,
        )

    assert dr is None and err is not None
    resp, code = err
    assert code == 409
    body = resp.get_json()
    assert body["code"] == EPCR_CODE_RECORD_VERSION_CONFLICT
    assert body["latest"]["recordVersion"] == 3
    assert body["latest"]["status"] == "closed"


def test_epcr_case_save_link_meta_new_row_no_conflict(flask_app):
    with flask_app.app_context():
        dr, pc, ds, nrv, err = _epcr_case_save_link_meta({"recordVersion": 1}, None)
    assert err is None
    assert nrv == 1


def test_user_may_access_case_data_matches_list_filter_case_insensitive(monkeypatch, flask_app):
    """JWT username casing must match GET /cases (principal_has_case_access), not exact ``in``."""
    from app.plugins.medical_records_module import routes as r

    monkeypatch.setattr(r, "_cura_auth_principal", lambda: ("Alice.Crew", "crew", 1))
    monkeypatch.setattr(r, "_epcr_privileged_role", lambda: False)
    with flask_app.app_context():
        assert r._user_may_access_case_data({"assignedUsers": ["alice.crew"]})
        assert not r._user_may_access_case_data({"assignedUsers": ["other.user"]})


def test_user_may_access_case_data_allows_crew_manifest_without_assigned_match(monkeypatch, flask_app):
    """Collaborator may appear only in crewManifest (list API already allowed this)."""
    from app.plugins.medical_records_module import routes as r

    monkeypatch.setattr(r, "_cura_auth_principal", lambda: ("bob", "crew", 1))
    monkeypatch.setattr(r, "_epcr_privileged_role", lambda: False)
    case = {
        "assignedUsers": ["primary"],
        "crewManifest": [{"username": "bob", "displayName": "Bob"}],
    }
    with flask_app.app_context():
        assert r._user_may_access_case_data(case)
