"""
Unit tests for safeguarding referral visibility (no DB required).
"""
from unittest.mock import MagicMock

import pytest

from app.plugins.medical_records_module.safeguarding_auth import (
    CREW_REFERRAL_VISIBILITY_SQL,
    crew_referral_visibility_params,
    principal_may_read_referral,
    principal_may_patch_safeguarding,
)


@pytest.fixture
def cursor():
    return MagicMock()


def test_privileged_read_bypasses_row(cursor):
    assert principal_may_read_referral(
        cursor,
        operational_event_id=None,
        created_by="alice",
        username="bob",
        privileged=True,
    )


def test_crew_creator_may_read(cursor):
    assert principal_may_read_referral(
        cursor,
        operational_event_id=99,
        created_by="bob",
        username="bob",
        privileged=False,
    )


def test_crew_other_creator_denied_even_with_shared_event(cursor):
    assert not principal_may_read_referral(
        cursor,
        operational_event_id=99,
        created_by="alice",
        username="bob",
        privileged=False,
    )


def test_crew_visibility_sql_single_bind():
    assert CREW_REFERRAL_VISIBILITY_SQL.count("%s") == 1
    assert crew_referral_visibility_params("bob") == ("bob",)


def test_patch_submitted_locked_for_crew(cursor):
    assert not principal_may_patch_safeguarding(
        privileged=False,
        row_status="submitted",
        created_by="bob",
        username="bob",
    )


def test_patch_draft_creator_ok(cursor):
    assert principal_may_patch_safeguarding(
        privileged=False,
        row_status="draft",
        created_by="bob",
        username="bob",
    )
