"""Regression test for the Oct 2026 fabricated WEATHER.COM row.

Weather.com's API returned 401 and its scrape matched nothing for five days.
CacheManager rejected the expired LKG and fell through to DEFAULT_VALUES,
which held placeholder temps (55/40, 56/41, 57/42) and the condition
"Default". The report drew those as a real forecast: in the WEATHER.COM row,
in the weighted average at 4x, and in the descriptor row.

A failed weather.com fetch must now produce a blank row instead.
"""

from pathlib import Path

from duck_sun.cache_manager import CacheManager
from tests.test_google_priority import _report

WEATHER_COM_ROW = 16
DAY_COLUMNS = "EFGHIJKLMNOPQRST"  # 8 days x (Hi, Lo)


def _failed_weather_com_fetch(tmp_path: Path, monkeypatch):
    """Run the scheduler's fallback chain for a fetch that returned None, no cache."""
    monkeypatch.setattr(CacheManager, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(CacheManager, "ANALYTICS_FILE", tmp_path / "lessons_learned.json")
    return CacheManager().get_with_fallback("weather_com", None, "HTTP 401")


def test_failed_weather_com_fetch_yields_no_placeholder_data(tmp_path: Path, monkeypatch):
    result = _failed_weather_com_fetch(tmp_path, monkeypatch)

    assert result.source == "DEFAULT"
    assert not result.data, f"weather_com DEFAULT must be empty, got {result.data!r}"


def test_failed_weather_com_fetch_blanks_the_row_not_fakes_it(tmp_path: Path, monkeypatch):
    result = _failed_weather_com_fetch(tmp_path, monkeypatch)
    ws = _report(tmp_path, weather_com_data=result.data)

    assert ws[f"C{WEATHER_COM_ROW}"].value == "WEATHER.COM"
    for col in DAY_COLUMNS:
        value = ws[f"{col}{WEATHER_COM_ROW}"].value
        assert not isinstance(value, (int, float)), (
            f"{col}{WEATHER_COM_ROW} = {value!r}; a failed fetch must not put numbers "
            "into the WEATHER.COM row (they feed the weighted average)"
        )

    descriptors = [ws[f"{col}10"].value for col in DAY_COLUMNS[::2]]
    assert "Default" not in descriptors, f"placeholder condition leaked: {descriptors}"
    assert descriptors[0] == "Mostly Sunny"  # AccuWeather takes over days 0-4
