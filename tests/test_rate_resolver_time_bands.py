"""Clock overlap pay: night PRORATA + TIME_BANDS helpers (time_billing RateResolver)."""
from datetime import date, time
from decimal import Decimal

from app.plugins.time_billing_module.services import (
    _rr_pay_night_prorata,
    _rr_pay_time_bands,
)


def test_night_prorata_split_cross_midnight():
    """8h gross, 4h in 22:00–06:00 window, no break: half at R_day 10, half at night mult 1.5 → R_n 15."""
    work = date(2026, 6, 1)  # Monday
    p = {
        "window_start": time(22, 0),
        "window_end": time(6, 0),
        "mode": "MULTIPLIER",
        "multiplier": Decimal("1.5"),
    }
    # 18:00–02:00 next day: 4h outside (18–22), 4h inside (22–02)
    pay, meta = _rr_pay_night_prorata(
        work, time(18, 0), time(2, 0), 0, Decimal("10"), p
    )
    assert pay is not None
    assert meta.get("night_prorata") is True
    # 4*10 + 4*15 = 100
    assert pay == Decimal("100.00")


def test_night_prorata_absolute_inside():
    p = {
        "window_start": time(22, 0),
        "window_end": time(6, 0),
        "mode": "ABSOLUTE",
        "absolute_rate": Decimal("20"),
    }
    pay, _ = _rr_pay_night_prorata(
        date(2026, 6, 1), time(18, 0), time(2, 0), 0, Decimal("10"), p
    )
    # 4*10 + 4*20 = 120
    assert pay == Decimal("120.00")


def test_time_bands_max_segment():
    """Day rate 10; band 12:00–13:00 multiplier 2 → 1h at 20, rest at 10 for 09:00–11:00 = 2h*10 only."""
    bands = [
        {
            "window_start": "12:00",
            "window_end": "13:00",
            "multiplier": 2,
        }
    ]
    pay, meta = _rr_pay_time_bands(
        date(2026, 6, 1), time(9, 0), time(11, 0), 0, Decimal("10"), bands
    )
    assert pay == Decimal("20.00")
    assert meta.get("net_hours") == 2.0


def test_time_bands_uplift_per_hour():
    bands = [
        {
            "window_start": "10:00",
            "window_end": "11:00",
            "multiplier": 1,
            "uplift_per_hour": "2.50",
        }
    ]
    pay, _ = _rr_pay_time_bands(
        date(2026, 6, 1), time(10, 0), time(11, 0), 0, Decimal("10"), bands
    )
    # 1h at 10 + 2.5 uplift = 12.5
    assert pay == Decimal("12.50")


def test_time_bands_weekday_filter_excludes():
    bands = [
        {
            "weekdays": [5, 6],
            "window_start": "10:00",
            "window_end": "11:00",
            "multiplier": 2,
        }
    ]
    pay, _ = _rr_pay_time_bands(
        date(2026, 6, 1), time(10, 0), time(11, 0), 0, Decimal("10"), bands
    )
    assert pay == Decimal("10.00")


def test_time_bands_cross_midnight_uses_segment_calendar_day_for_weekday():
    """
    work_date Monday; shift into Tuesday morning. Tuesday-only band (Python weekday 1)
    01:00–03:00 applies only to the slice on the calendar Tuesday, not the Monday portion.
    """
    bands = [
        {
            "weekdays": [1],
            "window_start": "01:00",
            "window_end": "03:00",
            "multiplier": 2,
        }
    ]
    pay, meta = _rr_pay_time_bands(
        date(2026, 6, 1),
        time(22, 0),
        time(2, 0),
        0,
        Decimal("10"),
        bands,
    )
    # Mon 22:00–Tue 01:00 (3h) at 10; Tue 01:00–02:00 (1h) in band at 20 → 50
    assert pay == Decimal("50.00")
    assert meta.get("net_hours") == 4.0


def test_time_bands_bonus_flat_once_per_shift():
    bands = [
        {
            "window_start": "10:00",
            "window_end": "11:00",
            "multiplier": 1,
            "bonus_flat": "40",
            "label": "standby",
        }
    ]
    pay, meta = _rr_pay_time_bands(
        date(2026, 6, 1), time(10, 0), time(11, 0), 0, Decimal("10"), bands
    )
    assert pay == Decimal("50.00")
    assert meta.get("time_band_hourly_pay") == 10.0
    flats = meta.get("time_band_flat_bonuses")
    assert flats and flats[0]["amount"] == 40.0
    assert flats[0]["label"] == "standby"


def test_time_bands_bonus_flat_alias_flat_bonus_per_shift():
    bands = [
        {
            "window_start": "09:00",
            "window_end": "10:00",
            "multiplier": 1,
            "flat_bonus_per_shift": "15",
        }
    ]
    pay, meta = _rr_pay_time_bands(
        date(2026, 6, 3), time(9, 0), time(10, 0), 0, Decimal("12"), bands
    )
    assert pay == Decimal("27.00")
    assert meta.get("time_band_flat_bonuses")[0]["amount"] == 15.0
