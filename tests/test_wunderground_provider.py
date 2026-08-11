"""
Regression tests for the Weather Underground provider.

Covers the failure modes behind an all-dash WUNDERGRND row:
  - forecast JSON embedded as an escaped JS string instead of raw JSON
  - a cached forecast whose dates have rolled into the past
  - the API fallback used when the page loads but carries no forecast arrays
"""

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from duck_sun.providers import wunderground as wu_module
from duck_sun.providers.wunderground import WUndergroundProvider

PACIFIC = ZoneInfo("America/Los_Angeles")


def _dates(count: int, start_offset: int = 0):
    today = datetime.now(PACIFIC)
    return [(today + timedelta(days=start_offset + i)).strftime("%Y-%m-%d") for i in range(count)]


def _twc_payload() -> dict:
    """The shape TWC v3 returns and wunderground.com embeds in its page."""
    return {
        "dayOfWeek": ["Tuesday", "Wednesday", "Thursday"],
        "temperatureMax": [70, 72, 73],
        "temperatureMin": [50, 53, 54],
        "validTimeLocal": [f"{d}T07:00:00-0700" for d in _dates(3)],
        "calendarDayTemperatureMax": [69, 72, 73],
        "calendarDayTemperatureMin": [50, 53, 54],
        "daypart": [{
            "precipChance": [12, 5, 18, 6, 17, 4],
            "wxPhraseLong": ["Sunny", "Clear", "Partly Cloudy", "Clear", "Sunny", "Clear"],
        }],
    }


def _page_html(payload: dict, escaped: bool, compact: bool = False) -> str:
    """Wrap a payload the way the site does - raw JSON or a JS string literal."""
    blob = json.dumps(payload, separators=(',', ':')) if compact else json.dumps(payload)
    if escaped:
        inner = blob.replace('\\', '\\\\').replace('"', '\\"')
        script = f'window.__data=JSON.parse("{inner}");'
    else:
        script = f'window.__data={blob};'
    return f"<html><body><script>{script}</script></body></html>"


@pytest.fixture(autouse=True)
def _isolate_cache(tmp_path, monkeypatch):
    """Keep every test off the repo's real outputs/wunderground_cache.json."""
    monkeypatch.setattr(wu_module, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(wu_module, "CACHE_FILE", tmp_path / "wunderground_cache.json")
    for var in ("WUNDERGROUND_API_KEY", "TWC_API_KEY", "WUNDERGROUND_GEOCODE"):
        monkeypatch.delenv(var, raising=False)


def test_parses_raw_embedded_json():
    days = WUndergroundProvider()._parse_embedded_json(_page_html(_twc_payload(), escaped=False))

    assert days is not None
    assert [d["high_f"] for d in days] == [70.0, 72.0, 73.0]
    assert [d["low_f"] for d in days] == [50.0, 53.0, 54.0]
    assert [d["date"] for d in days] == _dates(3)


def test_parses_escaped_embedded_json():
    """The regression: WU serving JSON.parse("{\\"dayOfWeek\\":...}") used to
    read as 'could not parse forecast arrays' and blank the whole row."""
    days = WUndergroundProvider()._parse_embedded_json(_page_html(_twc_payload(), escaped=True))

    assert days is not None
    assert [d["high_f"] for d in days] == [70.0, 72.0, 73.0]


@pytest.mark.parametrize("escaped", [False, True])
def test_parses_minified_page_json(escaped):
    """Production pages are minified - no whitespace around the colons."""
    days = WUndergroundProvider()._parse_embedded_json(
        _page_html(_twc_payload(), escaped=escaped, compact=True)
    )

    assert days is not None
    assert [d["high_f"] for d in days] == [70.0, 72.0, 73.0]


def test_precip_uses_daytime_daypart_value():
    days = WUndergroundProvider()._parse_embedded_json(_page_html(_twc_payload(), escaped=False))

    # Daytime entries are 12/18/17; the night entries (5/6/4) are the fallback.
    assert [d["precip_prob"] for d in days] == [12, 18, 17]
    assert days[0]["condition"] == "Sunny"


def test_phrases_with_commas_do_not_shift_alignment():
    """'Cloudy, then clear' used to split into two entries and push every later
    daypart value one slot left."""
    payload = _twc_payload()
    payload["daypart"][0]["wxPhraseLong"] = [
        "Cloudy, then Clear", "Clear", None, "Clear", "Sunny", "Clear",
    ]

    days = WUndergroundProvider()._parse_embedded_json(_page_html(payload, escaped=False))

    assert [d["condition"] for d in days] == ["Cloudy, then Clear", "Clear", "Sunny"]
    # Day 2's daytime phrase is null, so it falls back to that day's night entry
    assert [d["precip_prob"] for d in days] == [12, 18, 17]


def test_page_hourly_arrays_do_not_hijack_dates_or_dayparts():
    """The page carries several forecast contexts and a regex takes the first
    match. An hourly validTimeLocal would date every row to the same day and
    collapse the row to one column; hourly precipChance would shift values
    against the wrong days. Both must be ignored in favour of index dating."""
    payload = _twc_payload()
    today = _dates(1)[0]
    hourly_first = {
        # 24 hourly stamps, all today - what the old regex would have grabbed
        "validTimeLocal": [f"{today}T{h:02d}:00:00-0700" for h in range(24)],
        "precipChance": list(range(24)),
        "wxPhraseLong": ["Sunny"] * 24,
    }
    html = (
        f"<script>window.__hourly={json.dumps(hourly_first)};</script>"
        + _page_html(payload, escaped=False)
    )

    days = WUndergroundProvider()._parse_embedded_json(html)

    assert [d["date"] for d in days] == _dates(3)
    assert len({d["date"] for d in days}) == 3
    # The hourly precipChance (0,1,2,…) is skipped for the real daypart array
    assert [d["precip_prob"] for d in days] == [12, 18, 17]
    assert [d["condition"] for d in days] == ["Sunny", "Partly Cloudy", "Sunny"]


def test_short_scrape_is_extended_via_api(monkeypatch):
    """6 scraped days against an 8-column grid dashes the last two columns on
    every run. The API the page renders from returns the full window."""
    provider = WUndergroundProvider()
    six = [{"date": d, "day_name": "Tue", "high_f": 94.0, "low_f": 61.0, "high_c": 34.4,
            "low_c": 16.1, "condition": "Sunny", "precip_prob": 3} for d in _dates(6)]
    # The API disagrees by a degree on the overlap - the page must win there
    ten = [{**six[0], "date": d, "high_f": 95.0} for d in _dates(10)]
    html = '<script>var c={"apiKey":"6532d6454b8aa370768e63d6ba5a832e"};</script>'

    monkeypatch.setattr(provider, "_fetch_page", lambda: html)
    monkeypatch.setattr(provider, "_parse_embedded_json", lambda _: list(six))
    monkeypatch.setattr(provider, "_fetch_via_api", lambda key, geo: list(ten))

    days = provider.fetch_sync()

    assert [d["date"] for d in days] == _dates(10)
    assert [d["high_f"] for d in days[:6]] == [94.0] * 6   # page values kept
    assert [d["high_f"] for d in days[6:]] == [95.0] * 4   # API fills the gap


def test_failed_top_up_keeps_the_scraped_days(monkeypatch):
    """Extending is strictly additive - a failed API call must never cost us
    the days the scrape already produced."""
    provider = WUndergroundProvider()
    six = [{"date": d, "day_name": "Tue", "high_f": 94.0, "low_f": 61.0, "high_c": 34.4,
            "low_c": 16.1, "condition": "Sunny", "precip_prob": 3} for d in _dates(6)]
    html = '<script>var c={"apiKey":"6532d6454b8aa370768e63d6ba5a832e"};</script>'

    monkeypatch.setattr(provider, "_fetch_page", lambda: html)
    monkeypatch.setattr(provider, "_parse_embedded_json", lambda _: list(six))
    monkeypatch.setattr(provider, "_fetch_via_api", lambda key, geo: None)

    assert len(provider.fetch_sync()) == 6


def test_full_scrape_makes_no_api_call(monkeypatch):
    """A scrape that already covers the grid must not touch the API."""
    provider = WUndergroundProvider()
    full = [{"date": d, "day_name": "Tue", "high_f": 94.0, "low_f": 61.0, "high_c": 34.4,
             "low_c": 16.1, "condition": "Sunny", "precip_prob": 3} for d in _dates(8)]

    monkeypatch.setattr(provider, "_fetch_page", lambda: "<html></html>")
    monkeypatch.setattr(provider, "_parse_embedded_json", lambda _: list(full))

    def explode(*_args, **_kwargs):
        raise AssertionError("API must not be called when the scrape covers the grid")

    monkeypatch.setattr(provider, "_fetch_via_api", explode)

    assert len(provider.fetch_sync()) == 8


def test_client_rendered_page_returns_none():
    """No forecast arrays in the page - the provider must report failure so the
    API fallback runs, rather than inventing days."""
    html = "<html><body><div id='app'></div><script>var apiKey='x';</script></body></html>"

    assert WUndergroundProvider()._parse_embedded_json(html) is None


def test_stale_dated_cache_is_rejected(monkeypatch):
    """A cache written 2h ago but holding last week's dates parses fine and
    matches no grid column - exactly the silent all-dash failure. Reject it."""
    provider = WUndergroundProvider()
    old_dates = _dates(3, start_offset=-7)
    provider._save_cache([
        {"date": d, "day_name": "Mon", "high_f": 70.0, "low_f": 50.0,
         "high_c": 21.1, "low_c": 10.0, "condition": "Sunny", "precip_prob": 0}
        for d in old_dates
    ])

    assert provider._get_fresh_cache() is None


def test_fresh_cache_covering_today_is_kept():
    provider = WUndergroundProvider()
    provider._save_cache([
        {"date": d, "day_name": "Tue", "high_f": 70.0, "low_f": 50.0,
         "high_c": 21.1, "low_c": 10.0, "condition": "Sunny", "precip_prob": 0}
        for d in _dates(3)
    ])

    cached = provider._get_fresh_cache()
    assert cached is not None
    assert [d["date"] for d in cached] == _dates(3)


def test_expired_cache_is_rejected():
    provider = WUndergroundProvider()
    provider._save_cache([
        {"date": d, "day_name": "Tue", "high_f": 70.0, "low_f": 50.0,
         "high_c": 21.1, "low_c": 10.0, "condition": "Sunny", "precip_prob": 0}
        for d in _dates(3)
    ])

    stale = json.loads(wu_module.CACHE_FILE.read_text())
    stale["timestamp"] = (datetime.now() - timedelta(hours=wu_module.CACHE_MAX_AGE_HOURS + 1)).isoformat()
    wu_module.CACHE_FILE.write_text(json.dumps(stale))

    assert provider._get_fresh_cache() is None


def test_api_key_and_geocode_harvested_from_page():
    provider = WUndergroundProvider()
    html = (
        '<script>var config={"apiKey":"6532d6454b8aa370768e63d6ba5a832e"};</script>'
        '<script>fetch("https://api.weather.com/v3/wx/forecast/daily/10day'
        '?geocode=37.66,-121.00&units=e");</script>'
    )

    assert provider._extract_api_key(html) == "6532d6454b8aa370768e63d6ba5a832e"
    assert provider._extract_geocode(html) == "37.66,-121.00"
    assert provider._geocode(provider._extract_geocode(html)) == "37.66,-121.00"


def test_client_rendered_page_falls_back_to_api(monkeypatch):
    """Page loads, carries no arrays, but does carry the apiKey: the provider
    must recover through the same API the site's front end calls."""
    provider = WUndergroundProvider()
    html = (
        '<html><body><div id="app"></div>'
        '<script>var c={"apiKey":"6532d6454b8aa370768e63d6ba5a832e"};'
        'fetch("https://api.weather.com/v3/wx?geocode=37.66,-121.00");</script></body></html>'
    )
    monkeypatch.setattr(provider, "_fetch_page", lambda: html)

    calls = {}

    def fake_api(api_key, geocode):
        calls["api_key"] = api_key
        calls["geocode"] = geocode
        payload = _twc_payload()
        dp = payload["daypart"][0]
        return provider._build_days(
            days_of_week=payload["dayOfWeek"],
            max_temps=payload["temperatureMax"],
            min_temps=payload["temperatureMin"],
            precip_chances=dp["precipChance"],
            valid_times=payload["validTimeLocal"],
            wx_phrases=dp["wxPhraseLong"],
        )

    monkeypatch.setattr(provider, "_fetch_via_api", fake_api)

    days = provider.fetch_sync()

    assert calls["api_key"] == "6532d6454b8aa370768e63d6ba5a832e"
    assert calls["geocode"] == "37.66,-121.00"
    assert [d["high_f"] for d in days] == [70.0, 72.0, 73.0]
    # A successful fetch must be cached for the next run
    assert json.loads(wu_module.CACHE_FILE.read_text())["data"]


def test_unreachable_page_falls_back_to_configured_key(monkeypatch):
    """Page blocked outright (firewall): use the operator's key and the
    configured geocode rather than blanking the row."""
    monkeypatch.setenv("TWC_API_KEY", "0" * 32)
    provider = WUndergroundProvider()
    monkeypatch.setattr(provider, "_fetch_page", lambda: None)

    calls = {}

    def fake_api(api_key, geocode):
        calls["api_key"] = api_key
        calls["geocode"] = geocode
        return [{"date": _dates(1)[0], "day_name": "Tue", "high_f": 94.0, "low_f": 61.0,
                 "high_c": 34.4, "low_c": 16.1, "condition": "Sunny", "precip_prob": 3}]

    monkeypatch.setattr(provider, "_fetch_via_api", fake_api)

    days = provider.fetch_sync()

    assert calls["api_key"] == "0" * 32
    assert calls["geocode"] == WUndergroundProvider.GEOCODE
    assert days[0]["high_f"] == 94.0


def test_total_failure_returns_none_not_stale_data(monkeypatch):
    """Every path down and the cache expired: return None. The row goes blank
    rather than showing an old forecast under today's dates."""
    provider = WUndergroundProvider()
    provider._save_cache([
        {"date": d, "day_name": "Tue", "high_f": 70.0, "low_f": 50.0,
         "high_c": 21.1, "low_c": 10.0, "condition": "Sunny", "precip_prob": 0}
        for d in _dates(3, start_offset=-7)
    ])
    monkeypatch.setattr(provider, "_fetch_page", lambda: None)

    assert provider.fetch_sync() is None


def test_calendar_day_fills_null_high_for_today():
    """temperatureMax[0] goes null once today's high has passed; the calendar
    day value keeps today's column populated instead of dropping it."""
    payload = _twc_payload()
    payload["temperatureMax"][0] = None

    provider = WUndergroundProvider()
    dp = payload["daypart"][0]
    days = provider._build_days(
        days_of_week=payload["dayOfWeek"],
        max_temps=payload["temperatureMax"],
        min_temps=payload["temperatureMin"],
        precip_chances=dp["precipChance"],
        valid_times=payload["validTimeLocal"],
        wx_phrases=dp["wxPhraseLong"],
        calendar_max=payload["calendarDayTemperatureMax"],
        calendar_min=payload["calendarDayTemperatureMin"],
    )

    assert len(days) == 3
    assert days[0]["high_f"] == 69.0
