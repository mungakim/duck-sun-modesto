"""
Regression tests for the Weather Underground provider.

Covers the failure modes behind a blank or short WUNDERGRND row:
  - forecast JSON embedded as an escaped JS string instead of raw JSON
  - forecast JSON in Angular's TransferState form (&q; for every quote)
  - a 200 OK page that simply has no forecast arrays in it (the intermittent
    Sep 2026 failure) - must be retried, not treated as final
  - a cached forecast whose dates have rolled into the past
  - hourly arrays reusing the daily field names and hijacking the rows
  - a short summary strip winning over the full 10-day forecast

The provider is pure curl_cffi page scraping - there is no API key anywhere.
"""

import json
import logging
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
    monkeypatch.setattr(wu_module, "FAILED_PAGE_PATH", tmp_path / "wunderground_last_failed_page.html")


@pytest.fixture
def no_sleep(monkeypatch):
    """Record the retry loop's backoff delays instead of actually waiting."""
    delays = []
    monkeypatch.setattr(wu_module, "_sleep", lambda seconds: delays.append(seconds))
    return delays


def _scripted_fetch(monkeypatch, provider, responses):
    """Make _fetch_page return each (html, status) in turn; returns the (url, target) log."""
    calls = []
    queue = list(responses)

    def fake(url, target):
        calls.append((url, target))
        return queue.pop(0)

    monkeypatch.setattr(provider, "_fetch_page", fake)
    return calls


def _full_page(days=10):
    payload = {
        "dayOfWeek": [f"D{i}" for i in range(days)],
        "temperatureMax": [80 + i for i in range(days)],
        "temperatureMin": [50 + i for i in range(days)],
        "validTimeLocal": [f"{d}T07:00:00-0700" for d in _dates(days)],
    }
    return _page_html(payload, escaped=False)


# What the site sent on 2026-09-18: a real page, HTTP 200, and no forecast
# state anywhere in it. The old provider gave up on this after one attempt.
EMPTY_PAGE = (
    "<html><head><title>Modesto, CA 10-Day Weather Forecast | Weather Underground</title></head>"
    "<body><div id='app'></div>"
    "<script id='app-root-state' type='application/json'>{}</script></body></html>"
)


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


def test_longest_daily_arrays_win_over_a_short_strip():
    """The page carries a short summary strip alongside the full forecast.
    Taking the first match is how the provider reported 6 days against an
    8-column grid, dashing the last two columns on every run."""
    ten = {
        "dayOfWeek": [f"D{i}" for i in range(10)],
        "temperatureMax": [90 + i for i in range(10)],
        "temperatureMin": [60 + i for i in range(10)],
        "validTimeLocal": [f"{d}T07:00:00-0700" for d in _dates(10)],
    }
    short_strip = {
        "dayOfWeek": ["D0", "D1", "D2", "D3", "D4", "D5"],
        "temperatureMax": [90, 91, 92, 93, 94, 95],
        "temperatureMin": [60, 61, 62, 63, 64, 65],
    }
    # Short strip first in the page, exactly like the real layout
    html = (
        f"<script>window.__strip={json.dumps(short_strip)};</script>"
        f"<script>window.__data={json.dumps(ten)};</script>"
    )

    days = WUndergroundProvider()._parse_embedded_json(html)

    assert len(days) == 10
    assert [d["date"] for d in days] == _dates(10)
    assert days[-1]["high_f"] == 99.0


def test_forecast_starting_tomorrow_is_dated_forward():
    """Late in the day the page drops today and leads with tomorrow. Dating
    that from index 0 files tomorrow's high under today and shifts the whole
    row by a day - wrong numbers, no error anywhere."""
    tomorrow = datetime.now(PACIFIC) + timedelta(days=1)
    payload = {
        "dayOfWeek": [(tomorrow + timedelta(days=i)).strftime("%A") for i in range(3)],
        "temperatureMax": [88, 89, 90],
        "temperatureMin": [58, 59, 60],
    }

    days = WUndergroundProvider()._parse_embedded_json(_page_html(payload, escaped=False))

    assert [d["date"] for d in days] == _dates(3, start_offset=1)


def test_parses_angular_transfer_state_escaping():
    """Angular's app-root-state escapes every quote as &q; (and & ' < > as
    &a; &s; &l; &g;). A regex written for raw JSON reads that page as
    client-rendered and blanks the row."""
    blob = json.dumps(_twc_payload(), separators=(',', ':'))
    escaped = (blob.replace('&', '&a;').replace('"', '&q;').replace("'", '&s;')
               .replace('<', '&l;').replace('>', '&g;'))
    html = (
        '<html><body><script id="app-root-state" type="application/json">'
        f'{{&q;api.weather.com/v3/wx/forecast/daily/10day&q;:{{&q;b&q;:{escaped}}}}}'
        '</script></body></html>'
    )

    days = WUndergroundProvider()._parse_embedded_json(html)

    assert days is not None
    assert [d["high_f"] for d in days] == [70.0, 72.0, 73.0]
    assert [d["precip_prob"] for d in days] == [12, 18, 17]
    assert days[0]["condition"] == "Sunny"


def test_client_rendered_page_returns_none():
    """No forecast arrays in the page - the provider must report failure and
    fall through to fresh cache rather than inventing days."""
    html = "<html><body><div id='app'></div><script>var x='y';</script></body></html>"

    assert WUndergroundProvider()._parse_embedded_json(html) is None


# ----------------------------------------------------------------------------
# Retry loop - the intermittent blank row
# ----------------------------------------------------------------------------

def test_empty_page_is_retried_until_the_forecast_shows_up(monkeypatch, no_sleep):
    """Sep 2026: 200 OK, no arrays, one attempt, blank row for the whole run.
    The next render usually carries the forecast, so keep asking."""
    provider = WUndergroundProvider()
    calls = _scripted_fetch(
        monkeypatch, provider, [(EMPTY_PAGE, 200), (EMPTY_PAGE, 200), (_full_page(), 200)]
    )

    days = provider.fetch_sync()

    assert days is not None and len(days) == 10
    assert len(calls) == 3
    # Backoff between attempts, and none after the one that succeeded
    assert no_sleep == list(wu_module.RETRY_DELAYS_SECONDS[:2])


def test_first_attempt_success_does_not_wait_or_refetch(monkeypatch, no_sleep):
    provider = WUndergroundProvider()
    calls = _scripted_fetch(monkeypatch, provider, [(_full_page(), 200)])

    assert len(provider.fetch_sync()) == 10
    assert len(calls) == 1
    assert no_sleep == []


def test_retries_rotate_page_variant_and_fingerprint(monkeypatch, no_sleep):
    """A stale render of one URL, or a fingerprint the site has started
    refusing, must not eat the whole budget."""
    provider = WUndergroundProvider()
    budget = wu_module.MAX_FETCH_ATTEMPTS
    calls = _scripted_fetch(monkeypatch, provider, [(None, 503)] * budget)

    assert provider.fetch_sync() is None

    urls = [u for u, _ in calls]
    targets = [t for _, t in calls]
    assert len(calls) == budget
    assert urls[0] == provider.URL
    assert set(provider.ALTERNATE_URLS) <= set(urls)        # every alternate gets a turn
    assert urls.count(provider.URL) == budget // 2          # primary keeps half the budget
    assert len(set(targets)) == len(wu_module.IMPERSONATE_TARGETS)
    assert len(no_sleep) == budget - 1                       # no wait after the last attempt


def test_short_forecast_is_kept_while_later_attempts_try_for_the_full_window(monkeypatch, no_sleep):
    provider = WUndergroundProvider()
    calls = _scripted_fetch(
        monkeypatch, provider, [(_full_page(6), 200), (EMPTY_PAGE, 200), (_full_page(10), 200)]
    )

    days = provider.fetch_sync()

    assert len(days) == 10
    assert len(calls) == 3


def test_short_forecast_is_returned_when_nothing_better_turns_up(monkeypatch, no_sleep):
    provider = WUndergroundProvider()
    budget = wu_module.MAX_FETCH_ATTEMPTS
    _scripted_fetch(
        monkeypatch, provider, [(_full_page(6), 200)] + [(EMPTY_PAGE, 200)] * (budget - 1)
    )

    days = provider.fetch_sync()

    assert len(days) == 6
    assert [d["date"] for d in days] == _dates(6)


def test_unsupported_fingerprint_is_dropped_without_spending_an_attempt(monkeypatch, no_sleep):
    provider = WUndergroundProvider()
    first, second = wu_module.IMPERSONATE_TARGETS[:2]
    seen = []

    def fake(url, target):
        seen.append(target)
        if target == first:
            raise wu_module._UnsupportedTarget(target)
        return _full_page(), 200

    monkeypatch.setattr(provider, "_fetch_page", fake)

    assert len(provider.fetch_sync()) == 10
    assert seen == [first, second]
    assert no_sleep == []


def test_curl_cffi_impersonate_error_maps_to_unsupported_target(monkeypatch):
    """curl_cffi's real message is 'Impersonating <target> is not supported'."""

    class RefusingSession:
        def __init__(self, impersonate):
            raise RuntimeError(f"Impersonating {impersonate} is not supported")

    monkeypatch.setattr("curl_cffi.requests.Session", RefusingSession)

    with pytest.raises(wu_module._UnsupportedTarget):
        WUndergroundProvider()._fetch_page(WUndergroundProvider.URL, "nosuchbrowser999")


def test_empty_page_is_diagnosed_and_saved_for_post_mortem(monkeypatch, no_sleep, caplog):
    """Every failure used to log the same one line. The log must now say what
    the site sent, and the page must be on disk for the next investigation."""
    provider = WUndergroundProvider()
    _scripted_fetch(monkeypatch, provider, [(EMPTY_PAGE, 200)] * wu_module.MAX_FETCH_ATTEMPTS)

    with caplog.at_level(logging.WARNING):
        assert provider.fetch_sync() is None

    assert "HTTP 200" in caplog.text
    assert "Modesto, CA 10-Day Weather Forecast" in caplog.text
    assert "app-root-state" in caplog.text

    saved = wu_module.FAILED_PAGE_PATH.read_text(encoding="utf-8")
    assert saved.startswith("<!-- duck_sun wunderground: fetched ")
    assert "HTTP 200" in saved
    assert "app-root-state" in saved


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


def test_total_failure_returns_none_not_stale_data(monkeypatch):
    """Every path down and the cache expired: return None. The row goes blank
    rather than showing an old forecast under today's dates."""
    provider = WUndergroundProvider()
    provider._save_cache([
        {"date": d, "day_name": "Tue", "high_f": 70.0, "low_f": 50.0,
         "high_c": 21.1, "low_c": 10.0, "condition": "Sunny", "precip_prob": 0}
        for d in _dates(3, start_offset=-7)
    ])
    monkeypatch.setattr(provider, "_fetch_page", lambda url, target: (None, None))
    monkeypatch.setattr(wu_module, "_sleep", lambda seconds: None)

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
