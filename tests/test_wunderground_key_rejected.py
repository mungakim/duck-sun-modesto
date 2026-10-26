"""A rejected WUnderground API key (after a failed scrape) blanks the row in red.

Same rule as Weather.com: a dead key won't fix itself and cached data would
hide it, so the scheduler drops the cache and the Excel WUNDERGRND row is
drawn as red dashes with a refresh note on the label.
"""

import asyncio
from pathlib import Path

from duck_sun import scheduler
from duck_sun.cache_manager import CacheManager
from duck_sun.excel_report import KEY_REJECTED_DASH_COLOR
from duck_sun.providers.wunderground import KEY_REJECTED_ERROR
from tests.test_google_priority import _report
from tests.test_weather_com_key_rejected import CACHED_DAY, DAY_COLUMNS, _dash_color

WUNDERGROUND_ROW = 17


def _scheduler_result(tmp_path: Path, monkeypatch, key_rejected: bool):
    class _FakeProvider:
        def __init__(self):
            self.key_rejected = False

        def fetch_sync(self):
            self.key_rejected = key_rejected
            return None

    monkeypatch.setattr(CacheManager, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(CacheManager, "ANALYTICS_FILE", tmp_path / "lessons_learned.json")
    monkeypatch.setattr(scheduler, "WUndergroundProvider", _FakeProvider)
    cache_mgr = CacheManager()
    cache_mgr.save_lkg("wunderground", [CACHED_DAY])  # fresh LKG, well inside 18h
    return asyncio.run(scheduler.fetch_wunderground(cache_mgr))


def test_scheduler_ignores_lkg_cache_when_key_rejected(tmp_path: Path, monkeypatch):
    result = _scheduler_result(tmp_path, monkeypatch, key_rejected=True)

    assert not result.data
    assert result.error_message == KEY_REJECTED_ERROR


def test_scheduler_still_uses_lkg_cache_for_other_failures(tmp_path: Path, monkeypatch):
    result = _scheduler_result(tmp_path, monkeypatch, key_rejected=False)

    assert result.data == [CACHED_DAY]
    assert result.source == "CACHE"


def test_key_rejected_row_is_red_dashes_with_refresh_note(tmp_path: Path):
    ws = _report(tmp_path, wunderground_data=[], wunderground_key_rejected=True)

    for col in DAY_COLUMNS:
        cell = ws[f"{col}{WUNDERGROUND_ROW}"]
        assert cell.value == "--"
        assert _dash_color(cell) == KEY_REJECTED_DASH_COLOR, f"{col}{WUNDERGROUND_ROW} dash is not red"

    label = ws[f"C{WUNDERGROUND_ROW}"]
    assert label.value == "WUNDERGRND"
    assert label.comment is not None and "WU_API_KEY" in label.comment.text
    # Only the rejected row is flagged
    assert ws["C16"].comment is None
