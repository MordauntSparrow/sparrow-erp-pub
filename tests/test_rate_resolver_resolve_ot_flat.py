"""RateResolver: TIME_BANDS £/shift flat must not be spread into shift OT tier math."""
import json
from datetime import date, time
from decimal import Decimal
from unittest.mock import patch

from app.plugins.time_billing_module.services import RateResolver


def test_resolve_time_bands_ot_adds_flat_after_tiered_hourly():
    """10h × £10 + £40 flat → OT threshold 8 @1.5×: 8×10 + 2×15 + 40 = 150 (not 154)."""
    bands_pol = {
        "id": 99,
        "name": "Bands",
        "applies_to": "WAGE",
        "mode": "MULTIPLIER",
        "type": "TIME_BANDS",
        "time_bands_json": json.dumps(
            [
                {
                    "window_start": "08:00",
                    "window_end": "18:00",
                    "multiplier": 1,
                    "bonus_flat": "40",
                }
            ]
        ),
    }
    ot_pol = {
        "applies_to": "WAGE",
        "ot_threshold_hours": 8,
        "ot_tier1_mult": 1.5,
    }

    def fake_pol(policy_type, scope, work_date):
        if policy_type == "TIME_BANDS":
            return [bands_pol]
        if policy_type == "OVERTIME_SHIFT":
            return [ot_pol]
        return []

    with patch.object(RateResolver, "_base_rate", return_value=Decimal("10")), patch.object(
        RateResolver, "_policies", side_effect=fake_pol
    ), patch.object(RateResolver, "_is_bank_holiday", return_value=False):
        _rate, pay, meta = RateResolver.resolve_rate_and_pay(
            1,
            None,
            1,
            None,
            date(2026, 6, 1),
            time(8, 0),
            time(18, 0),
            0,
        )
    assert pay == Decimal("150.00")
    cs = meta.get("clock_split") or {}
    assert cs.get("time_band_hourly_pay") == 100.0
