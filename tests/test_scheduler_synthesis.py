"""Regression tests for scheduler synthesis (Open-Meteo fallback)."""

from datetime import datetime, timedelta

from duck_sun.scheduler import (
    _aggregate_hourly_to_daily,
    _om_payload_is_empty,
    _synthesize_baseline_from_alternates,
)


def _iso_hour(date_str: str, hour: int) -> str:
    return f"{date_str}T{hour:02d}:00:00Z"


def _gen_hourly(start_date: str, num_days: int, base_temp_c: float = 20.0):
    """Generate hourly records (every 3 hours) for num_days starting at start_date."""
    start = datetime.fromisoformat(start_date)
    records = []
    for day_offset in range(num_days):
        date_obj = start + timedelta(days=day_offset)
        date_str = date_obj.strftime('%Y-%m-%d')
        for h in range(0, 24, 3):
            records.append({
                'time': _iso_hour(date_str, h),
                'temp_c': base_temp_c + (h - 12) * 0.5,
            })
    return records


def test_om_payload_is_empty_detects_default_payload():
    assert _om_payload_is_empty(None) is True
    assert _om_payload_is_empty({}) is True
    assert _om_payload_is_empty({"daily_forecast": [], "daily_summary": [], "hourly": []}) is True
    assert _om_payload_is_empty({"daily_forecast": [{"date": "2026-05-26"}]}) is False
    assert _om_payload_is_empty({"hourly": [{"time": "x"}]}) is False


def test_aggregate_hourly_to_daily_produces_fahrenheit_and_day_name():
    hourly = _gen_hourly('2026-05-26', num_days=2, base_temp_c=20.0)
    result = _aggregate_hourly_to_daily(hourly, 'NOAA', time_field='time')
    assert len(result) == 2
    assert result[0]['date'] == '2026-05-26'
    assert result[0]['day_name'] == 'Tuesday'
    assert result[0]['high_f'] is not None
    assert result[0]['low_f'] is not None
    assert result[0]['source'] == 'NOAA (fallback)'


def test_synthesis_extends_5_day_accuweather_with_noaa_to_reach_8_days():
    """Reproduces the May 26 dev-box scenario: Open-Meteo + Google dead, AccuWeather 5d, NOAA 8d."""
    accu_data = [
        {'date': f'2026-05-{26 + i}', 'high_f': 70 + i, 'low_f': 50 + i, 'high_c': 21.0, 'low_c': 10.0, 'precip_prob': 5, 'condition': 'Sunny'}
        for i in range(5)
    ]
    noaa_data = _gen_hourly('2026-05-26', num_days=8, base_temp_c=22.0)

    result = _synthesize_baseline_from_alternates(
        google_data={'hourly': [], 'daily': []},  # DEFAULT empty
        accu_data=accu_data,
        noaa_data=noaa_data,
        met_data=None,
    )

    assert result is not None
    daily = result['daily_forecast']
    assert len(daily) == 8, f"Expected 8-day grid, got {len(daily)}"

    # First 5 dates should be AccuWeather-sourced (higher priority)
    accu_count = sum(1 for d in daily if 'AccuWeather' in d.get('source', ''))
    noaa_count = sum(1 for d in daily if 'NOAA' in d.get('source', ''))
    assert accu_count == 5
    assert noaa_count == 3

    # Dates strictly increasing
    dates = [d['date'] for d in daily]
    assert dates == sorted(dates)

    # All have Fahrenheit + day_name populated
    for d in daily:
        assert d.get('high_f') is not None
        assert d.get('low_f') is not None
        assert d.get('day_name')


def test_synthesis_prefers_google_then_extends_with_noaa_then_metno():
    google_data = {
        'daily': [
            {'date': f'2026-05-{26 + i}', 'high_f': 72 + i, 'low_f': 52 + i, 'high_c': 22.0, 'low_c': 11.0, 'precip_prob': 10, 'condition': 'Partly sunny'}
            for i in range(4)
        ],
        'hourly': [
            {'time': '2026-05-26T08:00:00-07:00', 'temp_c': 18.0, 'cloud_cover': 30, 'precip_prob': 5}
        ],
    }
    noaa_data = _gen_hourly('2026-05-26', num_days=8, base_temp_c=22.0)
    met_data = _gen_hourly('2026-05-26', num_days=11, base_temp_c=21.0)

    result = _synthesize_baseline_from_alternates(
        google_data=google_data,
        accu_data=None,
        noaa_data=noaa_data,
        met_data=met_data,
    )

    assert result is not None
    daily = result['daily_forecast']
    assert len(daily) == 8

    google_count = sum(1 for d in daily if 'Google' in d.get('source', ''))
    noaa_count = sum(1 for d in daily if 'NOAA' in d.get('source', ''))
    assert google_count == 4
    assert noaa_count == 4  # extends from 4 → 8

    # Google's hourly should be preserved (used by uncanniness for cloud timing)
    assert len(result['hourly']) >= 1
    assert result['hourly'][0]['source'].startswith('Google')


def test_synthesis_uses_metno_when_only_metno_available():
    met_data = _gen_hourly('2026-05-26', num_days=11, base_temp_c=21.0)

    result = _synthesize_baseline_from_alternates(
        google_data=None,
        accu_data=None,
        noaa_data=None,
        met_data=met_data,
    )

    assert result is not None
    daily = result['daily_forecast']
    assert len(daily) == 8  # capped at 8 even though met_data has 11
    assert all('Met.no' in d.get('source', '') for d in daily)


def test_synthesis_returns_none_when_no_data():
    assert _synthesize_baseline_from_alternates(None, None, None, None) is None
    assert _synthesize_baseline_from_alternates({'daily': [], 'hourly': []}, [], [], []) is None


def test_synthesis_caps_at_8_days_even_with_abundant_sources():
    google_data = {
        'daily': [{'date': f'2026-05-{20 + i}', 'high_f': 70, 'low_f': 50, 'high_c': 21, 'low_c': 10, 'precip_prob': 0, 'condition': 'Sunny'} for i in range(10)],
        'hourly': [],
    }
    result = _synthesize_baseline_from_alternates(google_data, None, None, None)
    assert result is not None
    assert len(result['daily_forecast']) == 8
