"""Invalidating a cache must work when a .bak from an earlier run exists.

The report-level retry invalidates a failed provider's cache before
re-fetching. On Windows Path.rename() refuses to overwrite an existing file
(WinError 183), and outputs/cache/ already holds .bak files from earlier
runs, so every invalidation failed - logged on Nov 9 2026 as
"Failed to invalidate cache for open_meteo: [WinError 183] Cannot create a
file when that file already exists". Linux rename() overwrites silently, so
this test makes rename() behave the Windows way.
"""

from pathlib import Path

from duck_sun.cache_manager import CacheManager


def _windows_rename(self, target):
    if Path(target).exists():
        raise FileExistsError(183, "Cannot create a file when that file already exists")
    return _original_rename(self, target)


_original_rename = Path.rename


def test_invalidate_overwrites_an_existing_backup(tmp_path, monkeypatch):
    monkeypatch.setattr(CacheManager, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(Path, "rename", _windows_rename)
    mgr = CacheManager()

    cache = tmp_path / "open_meteo_lkg.json"
    backup = tmp_path / "open_meteo_lkg.json.bak"
    backup.write_text('{"run": "older"}')
    cache.write_text('{"run": "today"}')

    mgr.invalidate_cache("open_meteo")

    assert not cache.exists(), "cache should be gone so the retry can't serve it"
    assert backup.read_text() == '{"run": "today"}'
