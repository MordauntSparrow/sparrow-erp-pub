"""Lightweight tests for work visit helpers (no DB)."""

from app.plugins.work_module import services as work_services


def test_enrich_stops_with_visit_ids_empty():
    work_services.enrich_stops_with_visit_ids([])
    work_services.enrich_stops_with_visit_ids([{"id": 1}])
    # visit_id may be None when DB has no work_visits / no row


def test_ensure_visits_in_range_no_dates_returns_zero():
    assert work_services.ensure_visits_for_shifts_in_range() == 0
    assert work_services.ensure_visits_for_shifts_in_range(date_from=None, date_to=None) == 0
