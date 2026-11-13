"""The report's columns are dated by the clock, never by a provider's payload.

Nov 13 2026, 05:24:01: the Open-Meteo fetch failed three times and
CacheManager served the previous morning's pull (11/12 05:29:18 - 23.9h old,
inside open_meteo's 24h limit). The grid took its dates from Open-Meteo's
daily list, so the first column read "TODAY 11-12", every other source
dashed it ("7/8 grid days" for NOAA, Weather.com, WUnderground and Google),
and 11/20 fell off the right-hand edge. The solar grid, dated by the clock,
said 11-13 on the same sheet.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from openpyxl import load_workbook

from duck_sun.cache_manager import CacheManager
from duck_sun.excel_report import generate_excel_report, grid_dates

PACIFIC = ZoneInfo("America/Los_Angeles")
REPORT_TIME = datetime(2026, 11, 13, 5, 24, 1, tzinfo=PACIFIC)  # Friday

# Day columns: hi/lo pairs starting at E (col(3) with COL_OFFSET=2)
DAY_HI_COLS = ["E", "G", "I", "K", "M", "O", "Q", "S"]
DAY_NAME_ROW, DATE_ROW = 11, 12
OPEN_METEO_ROW, GOOGLE_ROW = 13, 18
SOLAR_FIRST_DAY_ROW = 29


def _days(start: str, n: int):
    first = datetime.fromisoformat(start)
    return [(first + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n)]


def _yesterdays_open_meteo():
    """Open-Meteo's 8-day pull from the 11/12 run - starts on 11/12."""
    return {
        "daily_forecast": [
            {"date": d, "day_name": datetime.fromisoformat(d).strftime("%A"),
             # Below Google, so the OPEN-METEO-is-max "-" marker never fires
             "high_f": 60 + i, "low_f": 40 + i, "precip_prob": 0, "condition": "Clear sky"}
            for i, d in enumerate(_days("2026-11-12", 8))
        ],
        "hourly": [],
    }


def _todays_google():
    return {
        "daily": [
            {"date": d, "high_f": 70 + i, "low_f": 45 + i, "condition": "Sunny"}
            for i, d in enumerate(_days("2026-11-13", 10))
        ],
        "hourly": [],
    }


def _render(tmp_path: Path, om_data) -> "Worksheet":  # noqa: F821
    out = tmp_path / "report.xlsx"
    generate_excel_report(
        om_data=om_data,
        noaa_data=[],
        accu_data=[],
        google_data=_todays_google(),
        output_path=out,
        report_timestamp=REPORT_TIME,
    )
    return load_workbook(out).active


def _serve_stale_open_meteo_cache(tmp_path: Path, monkeypatch):
    """Replay the 05:24 fallback: fetch failed, 23.9h-old LKG accepted."""
    monkeypatch.setattr(CacheManager, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(CacheManager, "ANALYTICS_FILE", tmp_path / "lessons_learned.json")
    mgr = CacheManager()
    mgr._cache_path("open_meteo").write_text(json.dumps({
        "provider": "open_meteo",
        "timestamp": (datetime.now() - timedelta(hours=23, minutes=54)).isoformat(),
        "api_success": True,
        "data": _yesterdays_open_meteo(),
    }))
    result = mgr.get_with_fallback("open_meteo", None, "Request error: ")
    assert result.source == "CACHE", "precondition: the <24h cache is accepted"
    assert result.data["daily_forecast"][0]["date"] == "2026-11-12"
    return result.data


def test_stale_open_meteo_cache_does_not_move_today(tmp_path, monkeypatch):
    ws = _render(tmp_path, _serve_stale_open_meteo_cache(tmp_path, monkeypatch))

    assert ws[f"E{DAY_NAME_ROW}"].value == "TODAY"
    assert ws[f"E{DATE_ROW}"].value == "11-13", "first column must be the report's own day"
    assert [ws[f"{c}{DAY_NAME_ROW}"].value for c in DAY_HI_COLS[1:]] == [
        "SAT", "SUN", "MON", "TUE", "WED", "THU", "FRI"
    ]
    assert ws[f"S{DATE_ROW}"].value == "11-20", "the 8th day must not fall off the grid"


def test_stale_open_meteo_cache_costs_only_its_own_row(tmp_path, monkeypatch):
    ws = _render(tmp_path, _serve_stale_open_meteo_cache(tmp_path, monkeypatch))

    google = [ws[f"{c}{GOOGLE_ROW}"].value for c in DAY_HI_COLS]
    assert google == [70, 71, 72, 73, 74, 75, 76, 77], "Google must fill all 8 columns"

    # Yesterday's pull still has 11/13..11/19 - shown under the right dates -
    # and nothing for 11/20. Its 11/12 day is simply not on the grid.
    om = [ws[f"{c}{OPEN_METEO_ROW}"].value for c in DAY_HI_COLS]
    assert om == [61, 62, 63, 64, 65, 66, 67, "--"]


def test_solar_grid_and_temperature_grid_agree_on_today(tmp_path, monkeypatch):
    ws = _render(tmp_path, _serve_stale_open_meteo_cache(tmp_path, monkeypatch))
    # The solar date cell reads "11-13\nFriday"
    assert ws[f"E{DATE_ROW}"].value == "11-13"
    assert ws[f"D{SOLAR_FIRST_DAY_ROW}"].value.splitlines()[0] == "11-13"


def test_grid_is_dated_even_with_no_open_meteo_data(tmp_path):
    ws = _render(tmp_path, {"daily_forecast": [], "hourly": []})
    assert ws[f"E{DATE_ROW}"].value == "11-13"
    assert ws[f"S{DATE_ROW}"].value == "11-20"
    assert ws[f"E{GOOGLE_ROW}"].value == 70


def test_grid_dates_use_the_pacific_calendar_day():
    # 06:00 UTC on 11/13 is still 22:00 on 11/12 in Modesto
    utc = datetime(2026, 11, 13, 6, 0, tzinfo=timezone.utc)
    assert grid_dates(utc)[0] == "2026-11-12"
    # A naive timestamp is Pacific wall-clock, as the scheduler's machine is
    assert grid_dates(datetime(2026, 11, 13, 5, 24))[0] == "2026-11-13"
    assert grid_dates(REPORT_TIME) == _days("2026-11-13", 8)
    assert len(grid_dates(REPORT_TIME, 7)) == 7
