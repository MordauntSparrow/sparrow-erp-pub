"""Calendar policy: structured time_band_rows → time_bands_json (server-side)."""
import json

from app.plugins.time_billing_module.services import TemplateService


def test_normalize_time_band_rows_for_json():
    s = TemplateService._normalize_time_band_rows_for_json(
        [
            {
                "window_start": "22:00",
                "window_end": "06:00",
                "multiplier": 1.25,
                "weekdays": [0, 1, 2],
                "label": "Weeknights",
            }
        ]
    )
    assert s
    data = json.loads(s)
    assert len(data) == 1
    assert data[0]["window_start"].startswith("22:")
    assert data[0]["window_end"].startswith("06:")
    assert data[0]["multiplier"] == 1.25
    assert data[0]["weekdays"] == [0, 1, 2]
    assert data[0]["label"] == "Weeknights"


def test_policy_payload_prefers_time_band_rows():
    row = TemplateService._policy_payload_to_row(
        {
            "name": "Bands test",
            "type": "TIME_BANDS",
            "scope": "GLOBAL",
            "mode": "MULTIPLIER",
            "effective_from": "2026-01-01",
            "time_band_rows": [
                {"window_start": "12:00", "window_end": "13:00", "absolute_rate": "20"},
            ],
            "time_bands_json": [{"window_start": "99:99", "window_end": "99:99"}],  # ignored
        }
    )
    assert row["type"] == "TIME_BANDS"
    parsed = json.loads(row["time_bands_json"])
    assert len(parsed) == 1
    assert parsed[0]["absolute_rate"] == 20.0
