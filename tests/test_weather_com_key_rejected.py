"""A rejected weather.com API key blanks the row in red - never cached data.

weather.com changes its public apiKey every few months and the API then
answers 401. Retrying can't fix that and cached data would hide it, so the
provider flags it, the scheduler drops any cache, and the Excel row is drawn
as red dashes with a refresh note on the label.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path

import pytest
from curl_cffi import requests as curl_requests

from duck_sun import scheduler
from duck_sun.cache_manager import CacheManager
from duck_sun.excel_report import KEY_REJECTED_DASH_COLOR
from duck_sun.providers import weather_com
from duck_sun.providers.weather_com import KEY_REJECTED_ERROR, WeatherComProvider
from tests.test_google_priority import _report

WEATHER_COM_ROW = 16
DAY_COLUMNS = "EFGHIJKLMNOPQRST"  # 8 days x (Hi, Lo)
CACHED_DAY = {"date": "2026-10-26", "high_f": 73.0, "low_f": 46.0,
              "condition": "Sunny", "precip_prob": 2}


class _FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code

    def json(self):
        return {}


def _fake_session(status_code):
    class _Session:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def get(self, url, **kwargs):
            return _FakeResponse(status_code)

    return _Session


@pytest.fixture
def provider_env(tmp_path: Path, monkeypatch):
    """Provider with its cache in tmp_path, holding a fresh (< 6h) cache."""
    cache_file = tmp_path / "weathercom_cache.json"
    cache_file.write_text(json.dumps({
        "timestamp": datetime.now().isoformat(),
        "call_date": datetime.now().strftime("%Y-%m-%d"),
        "call_count": 1,
        "data": [CACHED_DAY],
    }))
    monkeypatch.setattr(weather_com, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(weather_com, "CACHE_FILE", cache_file)
    monkeypatch.setattr(weather_com, "get_ca_bundle_for_curl", lambda: True)
    monkeypatch.setenv("TWC_API_KEY", "stale-key")


# ---------------------------------------------------------------- provider --

@pytest.mark.parametrize("status", [401, 403])
def test_rejected_key_returns_nothing_even_with_fresh_cache(provider_env, monkeypatch, status):
    monkeypatch.setattr(curl_requests, "Session", _fake_session(status))
    provider = WeatherComProvider()

    assert provider.fetch_sync() is None
    assert provider.key_rejected is True


def test_missing_key_is_treated_as_rejected(provider_env, monkeypatch):
    monkeypatch.delenv("TWC_API_KEY")
    provider = WeatherComProvider()

    assert provider.fetch_sync() is None
    assert provider.key_rejected is True


def test_other_api_failures_still_use_fresh_cache(provider_env, monkeypatch):
    monkeypatch.setattr(curl_requests, "Session", _fake_session(503))
    provider = WeatherComProvider()

    assert provider.fetch_sync() == [CACHED_DAY]
    assert provider.key_rejected is False


# --------------------------------------------------------------- scheduler --

def _scheduler_result(tmp_path: Path, monkeypatch, key_rejected: bool):
    class _FakeProvider:
        def __init__(self):
            self.key_rejected = False

        def fetch_sync(self):
            self.key_rejected = key_rejected
            return None

    monkeypatch.setattr(CacheManager, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(CacheManager, "ANALYTICS_FILE", tmp_path / "lessons_learned.json")
    monkeypatch.setattr(scheduler, "WeatherComProvider", _FakeProvider)
    cache_mgr = CacheManager()
    cache_mgr.save_lkg("weather_com", [CACHED_DAY])  # fresh LKG, well inside 18h
    return asyncio.run(scheduler.fetch_weather_com(cache_mgr))


def test_scheduler_ignores_lkg_cache_when_key_rejected(tmp_path: Path, monkeypatch):
    result = _scheduler_result(tmp_path, monkeypatch, key_rejected=True)

    assert not result.data
    assert result.error_message == KEY_REJECTED_ERROR


def test_scheduler_still_uses_lkg_cache_for_other_failures(tmp_path: Path, monkeypatch):
    result = _scheduler_result(tmp_path, monkeypatch, key_rejected=False)

    assert result.data == [CACHED_DAY]
    assert result.source == "CACHE"


# ------------------------------------------------------------ excel report --

def _dash_color(cell):
    color = cell.font.color
    return color.rgb[-6:] if color is not None and isinstance(color.rgb, str) else None


def test_key_rejected_row_is_red_dashes_with_refresh_note(tmp_path: Path):
    ws = _report(tmp_path, weather_com_data=[], weather_com_key_rejected=True)

    for col in DAY_COLUMNS:
        cell = ws[f"{col}{WEATHER_COM_ROW}"]
        assert cell.value == "--"
        assert _dash_color(cell) == KEY_REJECTED_DASH_COLOR, f"{col}{WEATHER_COM_ROW} dash is not red"

    label = ws[f"C{WEATHER_COM_ROW}"]
    assert label.comment is not None and "TWC_API_KEY" in label.comment.text


def test_ordinary_missing_data_keeps_plain_dashes(tmp_path: Path):
    ws = _report(tmp_path, weather_com_data=[])

    assert ws[f"E{WEATHER_COM_ROW}"].value == "--"
    assert _dash_color(ws[f"E{WEATHER_COM_ROW}"]) != KEY_REJECTED_DASH_COLOR
    assert ws[f"C{WEATHER_COM_ROW}"].comment is None
