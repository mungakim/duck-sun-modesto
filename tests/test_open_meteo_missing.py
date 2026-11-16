"""When Open-Meteo has no data at all, its row is hidden - nothing stands in.

Until Nov 2026 the scheduler "synthesized" an Open-Meteo baseline from
Google, AccuWeather and NOAA, and the report drew it as the OPEN-METEO row:
another source's numbers under Open-Meteo's name, counted again at weight 1
in the Wtd. Average. That's the same fabricated-row pattern as the
WEATHER.COM placeholders removed in Oct 2026. The grid is now dated by the
clock and the physics engine builds its own Google/NOAA timeline, so the
synthesis is gone: an empty Open-Meteo payload hides row 13 and the run
carries on with the other five sources.
"""

import asyncio
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from openpyxl import load_workbook

from duck_sun import scheduler
from duck_sun.cache_manager import CacheManager, CacheTier, FetchResult
from duck_sun.scheduler import _om_payload_is_empty

PACIFIC = ZoneInfo("America/Los_Angeles")
OPEN_METEO_ROW, GOOGLE_ROW, WTD_AVG_ROW = 13, 18, 19
DAY_HI_COLS = ["E", "G", "I", "K", "M", "O", "Q", "S"]


def test_om_payload_is_empty_detects_default_payload():
    assert _om_payload_is_empty(None) is True
    assert _om_payload_is_empty({}) is True
    assert _om_payload_is_empty({"daily_forecast": [], "daily_summary": [], "hourly": []}) is True
    assert _om_payload_is_empty({"daily_forecast": [{"date": "2026-05-26"}]}) is False
    assert _om_payload_is_empty({"hourly": [{"time": "x"}]}) is False


def test_synthesis_is_gone():
    assert not hasattr(scheduler, "_synthesize_baseline_from_alternates")


def _result(name, data, source="API"):
    tier = CacheTier.FRESH if source == "API" else CacheTier.DEFAULT
    return FetchResult(provider=name, data=data, tier=tier, timestamp=datetime.now(), source=source)


def _results_with_open_meteo_down():
    today = datetime.now(PACIFIC).date()
    dates = [(today + timedelta(days=i)).isoformat() for i in range(10)]
    google_hourly = []
    for d in dates:
        for h in range(24):
            stamp = datetime.fromisoformat(d).replace(hour=h, tzinfo=PACIFIC)
            google_hourly.append({
                "time": stamp.isoformat(), "temp_c": 15.0, "cloud_cover": 10,
                "precip_prob": 0, "condition": "Sunny", "is_daytime": 7 <= h <= 17,
            })
    google = {
        "daily": [{"date": d, "high_f": 70 + i, "low_f": 45 + i, "condition": "Sunny",
                   "precip_prob": 0} for i, d in enumerate(dates)],
        "hourly": google_hourly,
    }
    accu = [{"date": d, "high_f": 90, "low_f": 30, "condition": "Sunny", "precip_prob": 0}
            for d in dates[:5]]
    defaults = CacheManager.DEFAULT_VALUES
    return {
        "open_meteo": _result("open_meteo", defaults["open_meteo"], "DEFAULT"),
        "hrrr": _result("hrrr", defaults["hrrr"], "DEFAULT"),
        "noaa": _result("noaa", []),
        "accuweather": _result("accuweather", accu),
        "google_weather": _result("google_weather", google),
        "google_portland": _result("google_portland", {"daily": [], "hourly": []}),
        "google_phoenix": _result("google_phoenix", {"daily": [], "hourly": []}),
        "weather_com": _result("weather_com", []),
        "wunderground": _result("wunderground", []),
        "mid_org": _result("mid_org", {}),
        "metar": _result("metar", None),
    }


def _run_main_with_open_meteo_down(tmp_path, monkeypatch):
    results = _results_with_open_meteo_down()

    async def _fetch_all(cache_mgr):
        return results

    async def _no_noaa_periods(self):
        raise RuntimeError("offline in tests")

    monkeypatch.setattr(scheduler, "fetch_all_providers", _fetch_all)
    monkeypatch.setattr(scheduler.NOAAProvider, "fetch_forecast_periods", _no_noaa_periods)
    monkeypatch.setattr(scheduler, "MAX_REPORT_RETRIES", 1)  # no live re-fetch
    monkeypatch.setattr(scheduler, "OUTPUT_DIR", tmp_path / "outputs")
    monkeypatch.setattr(scheduler, "REPORT_DIR", tmp_path / "reports")
    monkeypatch.setattr(CacheManager, "CACHE_DIR", tmp_path / "outputs" / "cache")
    monkeypatch.setattr(CacheManager, "ANALYTICS_FILE", tmp_path / "outputs" / "lessons_learned.json")
    monkeypatch.setenv("DUCK_SUN_SKIP_NETWORK_COPY", "1")

    assert asyncio.run(scheduler.main()) == 0, "the run must still succeed without Open-Meteo"
    xlsx = sorted((tmp_path / "reports").rglob("daily_forecast_*.xlsx"))
    jsons = sorted((tmp_path / "outputs").glob("solar_data_*.json"))
    assert len(xlsx) == 1 and len(jsons) == 1
    return load_workbook(xlsx[0]).active, json.loads(jsons[0].read_text())


def test_open_meteo_row_is_hidden_and_holds_nobody_elses_numbers(tmp_path, monkeypatch):
    ws, _ = _run_main_with_open_meteo_down(tmp_path, monkeypatch)

    assert ws.row_dimensions[OPEN_METEO_ROW].hidden is True
    assert ws[f"C{OPEN_METEO_ROW}"].value == "OPEN-METEO", "row numbers must not shift"
    # Synthesis used to copy Google's 70..77 (or AccuWeather's 90) in here
    assert [ws[f"{c}{OPEN_METEO_ROW}"].value for c in DAY_HI_COLS] == ["--"] * 8


def test_other_rows_and_the_average_are_untouched(tmp_path, monkeypatch):
    ws, _ = _run_main_with_open_meteo_down(tmp_path, monkeypatch)

    assert [ws[f"{c}{GOOGLE_ROW}"].value for c in DAY_HI_COLS] == list(range(70, 78))
    assert not ws.row_dimensions[GOOGLE_ROW].hidden
    assert "E13:E18" in ws[f"E{WTD_AVG_ROW}"].value, "formula range stays 13:18"


def test_json_generated_at_is_a_real_time_not_default(tmp_path, monkeypatch):
    _, data = _run_main_with_open_meteo_down(tmp_path, monkeypatch)
    assert data["generated_at"] != "DEFAULT"
