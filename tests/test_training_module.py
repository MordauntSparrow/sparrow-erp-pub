"""Smoke tests for training module constants and helpers (no DB required)."""


def test_pending_statuses_excludes_passed():
    from app.plugins.training_module.services import PENDING_STATUSES, STATUS_PASSED, STATUS_EXEMPT

    assert STATUS_PASSED not in PENDING_STATUSES
    assert STATUS_EXEMPT not in PENDING_STATUSES


def test_slugify_training():
    from app.plugins.training_module.services import _slugify

    assert _slugify("Hello World!!") == "hello-world"


def test_delivery_types_frozen():
    from app.plugins.training_module.services import DELIVERY_TYPES

    assert "internal" in DELIVERY_TYPES
    assert "external_required" in DELIVERY_TYPES
