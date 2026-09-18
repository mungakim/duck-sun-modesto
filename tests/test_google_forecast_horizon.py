"""Regression tests for the Google Weather 240-hour horizon + Portland location.

Context: the report used to show only 4 days of Google data. That was not a
billing-tier cap - Google's forecast/hours:lookup accepts hours=1..240 and
pageSize=1..24 - it was simply this provider requesting hours=96.
"""

import inspect
from datetime import datetime, timedelta, timezone

from duck_sun.providers.google_weather import (
    GooglePhoenixProvider,
    GooglePortlandProvider,
    GoogleWeatherProvider,
)


def _synthetic_hours(count: int, start_hour_utc: int = 15):
    """Build `count` consecutive hourly records with a daily temp swing."""
    start = datetime(2026, 7, 31, start_hour_utc, 0, tzinfo=timezone.utc)
    records = []
    for i in range(count):
        t = start + timedelta(hours=i)
        # Simple diurnal wave so max/min per day are well defined
        temp_c = 20.0 + 10.0 * ((t.hour - 4) % 24) / 24.0
        records.append({
            "time": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "temp_c": round(temp_c, 1),
            "feels_like_c": round(temp_c, 1),
            "precip_prob": 10,
            "precip_mm": 0.0,
            "dewpoint_c": 8.0,
            "cloud_cover": 20,
            "wind_speed_kmh": 5.0,
            "condition": "Sunny",
            "is_daytime": 6 <= t.hour <= 20,
        })
    return records


def test_api_limits_match_google_documentation():
    assert GoogleWeatherProvider.MAX_FORECAST_HOURS == 240
    assert GoogleWeatherProvider.MAX_PAGE_SIZE == 24


def test_fetch_forecast_defaults_to_full_240_hour_window():
    """The 4-day grid regression was a hours=96 default. Lock the max in."""
    sig = inspect.signature(GoogleWeatherProvider.fetch_forecast)
    assert sig.parameters["hours"].default == GoogleWeatherProvider.MAX_FORECAST_HOURS

    sig_daily = inspect.signature(GoogleWeatherProvider.fetch_daily)
    assert sig_daily.parameters["hours"].default == GoogleWeatherProvider.MAX_FORECAST_HOURS


def test_240_hours_aggregates_to_at_least_eight_full_days():
    """8 columns is what the Excel grid renders; 240h must comfortably fill it."""
    provider = GoogleWeatherProvider()
    daily = provider._aggregate_to_daily(_synthetic_hours(240))

    assert len(daily) >= 8, f"240h should yield >=8 full days, got {len(daily)}"
    # Partial trailing days (no afternoon data) are dropped, not half-reported
    for day in daily:
        assert day["high_f"] >= day["low_f"]
        assert day["date"]


def test_96_hours_only_yields_four_days_documenting_the_old_behavior():
    provider = GoogleWeatherProvider()
    daily = provider._aggregate_to_daily(_synthetic_hours(96))
    assert len(daily) < 8, "96h cannot fill the 8-day grid - that was the bug"


def test_portland_provider_is_a_distinct_location_with_its_own_cache():
    modesto = GoogleWeatherProvider()
    portland = GooglePortlandProvider()

    assert (portland.lat, portland.lon) != (modesto.lat, modesto.lon)
    assert round(portland.lat) == 46 and round(portland.lon) == -123
    # Separate cache file: a Portland fetch must never clobber Modesto's LKG
    assert portland.cache_file != modesto.cache_file
    assert portland.cache_file.name == "google_portland_lkg.json"
    assert modesto.cache_file.name == "google_weather_lkg.json"
    # Shared Pacific timezone keeps Portland's calendar days aligned to Modesto's
    assert portland.timezone == modesto.timezone


def test_unwrap_cache_handles_both_on_disk_layouts():
    """CacheManager's LKG wrapper and the provider's own format share a path."""
    direct = {"timestamp": "x", "hourly": [{"time": "t"}], "daily": []}
    wrapped = {"provider": "google_weather", "timestamp": "x", "data": direct}

    assert GoogleWeatherProvider._unwrap_cache(direct) is direct
    assert GoogleWeatherProvider._unwrap_cache(wrapped) == direct


def test_phoenix_provider_is_a_distinct_location_with_its_own_cache():
    modesto = GoogleWeatherProvider()
    portland = GooglePortlandProvider()
    phoenix = GooglePhoenixProvider()

    assert (phoenix.lat, phoenix.lon) not in {(modesto.lat, modesto.lon), (portland.lat, portland.lon)}
    assert round(phoenix.lat) == 33 and round(phoenix.lon) == -112
    assert phoenix.location_name == "Phoenix, AZ"

    # Separate cache file: a Phoenix fetch must never clobber Modesto's or Portland's LKG
    assert phoenix.cache_file not in {modesto.cache_file, portland.cache_file}
    assert phoenix.cache_file.name == "google_phoenix_lkg.json"

    # Arizona has no DST - aggregate on Phoenix's own calendar day, not Pacific
    assert phoenix.timezone == "America/Phoenix"
