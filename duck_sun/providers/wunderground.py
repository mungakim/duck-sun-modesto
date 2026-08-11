"""
Weather Underground Provider for Duck Sun Modesto

Fetches the 10-day forecast for Modesto, CA (95350) from Weather Underground.

Fetch chain (first path that yields data wins):
  1. wunderground.com page (curl_cffi, browser impersonation), parsing the
     forecast JSON embedded in its script tags. The page leads because it is
     the alignment target
  2. TWC v3 API using the apiKey/geocode harvested from that same page - this
     is the path that survives wunderground.com switching to client-side
     rendering, where the page loads fine but carries no forecast arrays
  3. TWC v3 API with WUNDERGROUND_API_KEY / TWC_API_KEY, if the page itself is
     unreachable. The geocode is configured rather than harvested here, so it
     can drift slightly from the 95350 page
  4. Local cache, but only when it is fresh (< 6h) AND still covers today

Never returns stale data. A forecast whose dates no longer cover today would
render as an all-dash row in the report (the grid is keyed by date), so it is
rejected rather than passed on.

Weight: 4.0 (same as AccuWeather - commercial provider)
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, List, Optional, TypedDict
from zoneinfo import ZoneInfo

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    curl_requests = None

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    BeautifulSoup = None

# Import SSL helper for Windows certificate store support
try:
    from duck_sun.ssl_helper import get_ca_bundle_for_curl
except ImportError:
    # Fallback if ssl_helper not available
    def get_ca_bundle_for_curl():
        return os.getenv("DUCK_SUN_CA_BUNDLE", True)

logger = logging.getLogger(__name__)

# Rate limiting configuration
CACHE_DIR = Path("outputs")
CACHE_FILE = CACHE_DIR / "wunderground_cache.json"
DAILY_CALL_LIMIT = 8       # Soft cap on fetches per day (scraping-style endpoint)
CACHE_MAX_AGE_HOURS = 6    # Cache is only usable while this fresh

# Impersonation fingerprints, newest first. curl_cffi raises on a target its
# build does not know, and wunderground.com can start refusing a fingerprint at
# any time, so every target is tried in turn instead of hardcoding just one.
IMPERSONATE_TARGETS = ("firefox135", "chrome136", "chrome120", "chrome110")

PACIFIC = ZoneInfo("America/Los_Angeles")


def _to_int(value) -> Optional[int]:
    """Coerce a JSON scalar (or its raw text) to int, or None."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(str(value).strip().strip('"'))
    except ValueError:
        return None


def _array_pattern(key: str) -> str:
    """
    Regex for a JSON array field, tolerant of pretty-printed whitespace.

    The leading quote is load-bearing: it is what stops "temperatureMax" from
    also matching inside "calendarDayTemperatureMax", which is offset by a day.
    """
    return r'"%s"\s*:\s*\[([^\]]+)\]' % re.escape(key)


class WUndergroundDay(TypedDict):
    """Daily forecast data from Weather Underground."""
    date: str          # YYYY-MM-DD format
    day_name: str      # Day name (Mon, Tue, etc.)
    high_f: float      # High temperature in Fahrenheit
    low_f: float       # Low temperature in Fahrenheit
    high_c: float      # High temperature in Celsius
    low_c: float       # Low temperature in Celsius
    condition: str     # Weather condition text
    precip_prob: int   # Precipitation probability


class WUndergroundProvider:
    """
    Provider for Weather Underground data.

    Prefers the TWC v3 API that wunderground.com's own front end calls, and
    falls back to parsing the forecast JSON embedded in the page. Both paths
    go through curl_cffi with browser impersonation.

    Weight: 4.0 (commercial provider tier)
    """

    # Modesto, CA ZIP code URL
    URL = "https://www.wunderground.com/forecast/us/ca/modesto/95350"

    # Same v3 endpoint family Weather.com uses. wunderground.com renders from
    # this API, so it is a source-replication path, not an approximation.
    API_URL = "https://api.weather.com/v3/wx/forecast/daily/10day"

    # Fallback geocode for the 95350 page when one cannot be harvested from the
    # page itself. Override with WUNDERGROUND_GEOCODE if the API path ever
    # drifts from what wunderground.com shows.
    GEOCODE = "37.66,-121.00"

    def __init__(self):
        logger.info("[WUndergroundProvider] Initializing provider...")
        if not HAS_CURL_CFFI:
            logger.warning("[WUndergroundProvider] curl_cffi not installed - provider disabled")
        if not HAS_BS4:
            logger.warning("[WUndergroundProvider] beautifulsoup4 not installed - page scraping disabled")

        # Ensure cache directory exists
        CACHE_DIR.mkdir(exist_ok=True)

    def _load_cache(self) -> Optional[dict]:
        """Load cached data if it exists."""
        if not CACHE_FILE.exists():
            return None
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"[WUndergroundProvider] Cache load error: {e}")
            return None

    def _save_cache(self, data: List['WUndergroundDay'], increment_call: bool = True) -> None:
        """Save forecast data to cache with call counter."""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            existing = self._load_cache()
            call_count = 0

            if existing and existing.get('call_date') == today:
                call_count = existing.get('call_count', 0)

            if increment_call:
                call_count += 1

            cache = {
                'timestamp': datetime.now().isoformat(),
                'call_date': today,
                'call_count': call_count,
                'daily_limit': DAILY_CALL_LIMIT,
                'data': data
            }
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, indent=2)
            logger.info(f"[WUndergroundProvider] Cache saved: call #{call_count}/{DAILY_CALL_LIMIT} today")
        except Exception as e:
            logger.error(f"[WUndergroundProvider] Cache save failed: {e}")

    def _should_use_cache(self) -> bool:
        """
        Decide whether to skip the network entirely and serve cache.

        True ONLY when the daily call cap has been reached AND the cache is
        still fresh. A stale cache overrides the cap - freshness always wins,
        per the Feb 2026 stale-data policy.
        """
        cache = self._load_cache()
        if not cache:
            return False

        today = datetime.now().strftime('%Y-%m-%d')
        if cache.get('call_date') != today:
            logger.info("[WUndergroundProvider] New day - rate limit reset")
            return False

        call_count = cache.get('call_count', 0)
        if call_count < DAILY_CALL_LIMIT:
            logger.info(f"[WUndergroundProvider] Daily calls: {call_count}/{DAILY_CALL_LIMIT}")
            return False

        age_hours = self._cache_age_hours(cache)
        if age_hours is None or age_hours > CACHE_MAX_AGE_HOURS:
            logger.warning(
                f"[WUndergroundProvider] Rate limit reached ({call_count}/{DAILY_CALL_LIMIT}) "
                f"but cache is stale - overriding limit to fetch fresh"
            )
            return False

        logger.info(
            f"[WUndergroundProvider] Rate limit reached ({call_count}/{DAILY_CALL_LIMIT}), "
            f"using fresh cache ({age_hours:.1f}h old)"
        )
        return True

    @staticmethod
    def _cache_age_hours(cache: dict) -> Optional[float]:
        """Age of a cache payload in hours, or None if unreadable."""
        try:
            age = datetime.now() - datetime.fromisoformat(cache['timestamp'])
            return age.total_seconds() / 3600
        except Exception:
            return None

    def _get_fresh_cache(self) -> Optional[List['WUndergroundDay']]:
        """
        Return cached data only if it is fresh AND still covers today.

        A cache whose dates have rolled into the past parses fine but matches
        no column in the report grid, which is exactly how a silent all-dash
        WUNDERGRND row happens. Reject it instead.
        """
        cache = self._load_cache()
        if not cache or not cache.get('data'):
            return None

        age_hours = self._cache_age_hours(cache)
        if age_hours is None:
            return None

        if age_hours > CACHE_MAX_AGE_HOURS:
            logger.warning(
                f"[WUndergroundProvider] Cache too old ({age_hours:.1f}h > {CACHE_MAX_AGE_HOURS}h max) - rejecting"
            )
            return None

        current = self._drop_past_days(cache['data'])
        if not current:
            logger.error(
                "[WUndergroundProvider] Cached forecast no longer covers today - rejecting "
                "(serving it would render an all-dash WUNDERGRND row)"
            )
            return None

        logger.info(f"[WUndergroundProvider] Using fresh cache ({age_hours:.1f}h old, {len(current)} days)")
        return current

    def _drop_past_days(self, days: List['WUndergroundDay']) -> List['WUndergroundDay']:
        """Drop entries dated before today (Pacific)."""
        today = self._get_date_for_day(0)
        return [d for d in days if isinstance(d, dict) and d.get('date', '') >= today]

    def _extract_array(self, pattern: str, data: str, is_numeric: bool = True) -> List:
        """
        Extract array values from JavaScript data using regex.

        The captured text is decoded as JSON first, which keeps nulls in place
        and survives commas inside phrases ("Cloudy, then clear"). A naive
        comma split would silently shift every later entry by one position.
        """
        match = re.search(pattern, data)
        if not match:
            return []

        arr_str = match.group(1)

        try:
            raw = json.loads(f'[{arr_str}]')
        except Exception:
            raw = [None if x.strip() in ('null', '') else x.strip().strip('"')
                   for x in arr_str.split(',')]

        if is_numeric:
            return [_to_int(v) for v in raw]
        return ["" if v is None else str(v) for v in raw]

    def _get_date_for_day(self, day_index: int) -> str:
        """Get YYYY-MM-DD date string for day index (0 = today)."""
        today = datetime.now(PACIFIC)
        target = today + timedelta(days=day_index)
        return target.strftime('%Y-%m-%d')

    # ------------------------------------------------------------------
    # Shared builder
    # ------------------------------------------------------------------

    def _build_days(
        self,
        days_of_week: List,
        max_temps: List,
        min_temps: List,
        precip_chances: List,
        valid_times: Optional[List] = None,
        wx_phrases: Optional[List] = None,
        calendar_max: Optional[List] = None,
        calendar_min: Optional[List] = None,
    ) -> List[WUndergroundDay]:
        """
        Build WUndergroundDay rows from TWC daily arrays.

        Shared by the API and the page-scrape paths so both produce identical
        shapes. precip_chances / wx_phrases are daypart arrays: two entries per
        day (daytime, then night).
        """
        results: List[WUndergroundDay] = []
        num_days = min(10, len(days_of_week) or 10, len(max_temps), len(min_temps))

        for i in range(num_days):
            high_f = max_temps[i]
            low_f = min_temps[i]

            # temperatureMax goes null for day 0 once the daytime high has
            # passed. wunderground.com then shows the calendar-day high, so
            # use that rather than dropping today's column.
            if high_f is None and calendar_max and i < len(calendar_max):
                high_f = calendar_max[i]
            if low_f is None and calendar_min and i < len(calendar_min):
                low_f = calendar_min[i]

            if high_f is None or low_f is None:
                logger.warning(f"[WUndergroundProvider] Null temps for day {i}, skipping")
                continue

            # Convert to Celsius
            high_c = (high_f - 32) * 5 / 9
            low_c = (low_f - 32) * 5 / 9

            # Get day name (truncate to 3 chars)
            if i < len(days_of_week) and days_of_week[i]:
                day_abbrev = str(days_of_week[i])[:3]
            else:
                day_abbrev = f"D{i}"

            # Prefer the date the API stamped on the row; fall back to offset
            date_str = None
            if valid_times and i < len(valid_times) and valid_times[i]:
                date_str = str(valid_times[i])[:10]
                if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', date_str):
                    date_str = None
            if date_str is None:
                date_str = self._get_date_for_day(i)

            # Daytime daypart entry, falling back to night. Matching the
            # Weather.com rule: the daytime value is what the site displays.
            day_idx, night_idx = i * 2, i * 2 + 1
            precip = self._daypart_value(precip_chances, day_idx, night_idx)
            condition = self._daypart_value(wx_phrases or [], day_idx, night_idx)

            results.append({
                "date": date_str,
                "day_name": day_abbrev,
                "high_f": float(high_f),
                "low_f": float(low_f),
                "high_c": round(high_c, 2),
                "low_c": round(low_c, 2),
                "condition": condition if condition else "Unknown",
                "precip_prob": precip if precip is not None else 0
            })

            logger.debug(f"[WUndergroundProvider] {date_str}: Hi={high_f}F, Lo={low_f}F, Precip={precip}%")

        return results

    @staticmethod
    def _daypart_value(arr: List, day_idx: int, night_idx: int):
        """Daytime daypart entry, falling back to the night entry."""
        if day_idx < len(arr) and arr[day_idx] is not None and arr[day_idx] != "":
            return arr[day_idx]
        if night_idx < len(arr) and arr[night_idx] is not None and arr[night_idx] != "":
            return arr[night_idx]
        return None

    # ------------------------------------------------------------------
    # Fetch paths
    # ------------------------------------------------------------------

    def _fetch_page(self) -> Optional[str]:
        """
        Fetch the wunderground.com forecast page HTML.

        Tries each impersonation fingerprint in turn: curl_cffi raises on a
        target its build does not support, and the site can start refusing an
        individual fingerprint at any time.
        """
        if not HAS_CURL_CFFI:
            return None

        from curl_cffi.requests import Session

        verify = get_ca_bundle_for_curl()

        for target in IMPERSONATE_TARGETS:
            try:
                logger.info(f"[WUndergroundProvider] Fetching {self.URL} (impersonate={target})")
                with Session(impersonate=target) as session:
                    response = session.get(self.URL, timeout=30, verify=verify)

                if response.status_code == 200:
                    return response.text

                logger.warning(f"[WUndergroundProvider] HTTP {response.status_code} with {target}")
            except Exception as e:
                logger.warning(f"[WUndergroundProvider] Fetch with {target} failed: {e}")

        logger.error("[WUndergroundProvider] All impersonation targets failed")
        return None

    @staticmethod
    def _candidate_blobs(html: str) -> Iterator[str]:
        """
        Yield forms of the page text the forecast JSON may appear in.

        Weather Underground embeds its state as raw JSON in some builds and as
        a JS string literal (`JSON.parse("{\\"dayOfWeek\\":...")`) in others.
        The escaped form defeats a regex written for the raw one, which reads
        downstream as "site changed, provider dead".
        """
        yield html
        if '\\"' in html:
            yield html.replace('\\"', '"')

    def _parse_embedded_json(self, html: str) -> Optional[List[WUndergroundDay]]:
        """Parse the forecast arrays embedded in the page. None if absent."""
        # Narrow to script contents when BeautifulSoup is available, but keep
        # the whole document as a fallback: `script.string` is None whenever a
        # script tag holds more than one child node.
        blobs: List[str] = []
        if HAS_BS4:
            try:
                soup = BeautifulSoup(html, 'html.parser')
                for script in soup.find_all('script'):
                    text = script.string or script.get_text() or ""
                    if 'dayOfWeek' in text and 'temperatureMax' in text:
                        blobs.append(text)
            except Exception as e:
                logger.warning(f"[WUndergroundProvider] HTML parse error: {e}")
        blobs.append(html)

        for blob in blobs:
            for candidate in self._candidate_blobs(blob):
                days_of_week = self._extract_array(_array_pattern('dayOfWeek'), candidate, is_numeric=False)
                # temperatureMax/Min, not the calendarDay variants - those are
                # offset by a day relative to the grid. The leading quote in the
                # pattern is what keeps "calendarDayTemperatureMax" from matching.
                max_temps = self._extract_array(_array_pattern('temperatureMax'), candidate)
                min_temps = self._extract_array(_array_pattern('temperatureMin'), candidate)
                if not max_temps or not min_temps:
                    continue

                precip_chances = self._extract_array(_array_pattern('precipChance'), candidate)
                valid_times = self._extract_array(_array_pattern('validTimeLocal'), candidate, is_numeric=False)
                wx_phrases = self._extract_array(_array_pattern('wxPhraseLong'), candidate, is_numeric=False)

                days = self._build_days(
                    days_of_week=days_of_week,
                    max_temps=max_temps,
                    min_temps=min_temps,
                    precip_chances=precip_chances,
                    valid_times=valid_times,
                    wx_phrases=wx_phrases,
                )
                if days:
                    logger.info(f"[WUndergroundProvider] Parsed {len(days)} days from embedded page JSON")
                    return days

        logger.warning("[WUndergroundProvider] No forecast arrays in page (client-rendered or layout changed)")
        return None

    @staticmethod
    def _extract_api_key(html: str) -> Optional[str]:
        """Harvest the TWC apiKey wunderground.com's own front end uses."""
        for pattern in (r'apiKey["\']?\s*[:=]\s*["\']([0-9a-f]{32})["\']', r'apiKey=([0-9a-f]{32})'):
            match = re.search(pattern, html)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _extract_geocode(html: str) -> Optional[str]:
        """Harvest the geocode the page requests its forecast for."""
        match = re.search(r'geocode=(-?\d+\.\d+)(?:,|%2C)(-?\d+\.\d+)', html)
        if match:
            return f"{match.group(1)},{match.group(2)}"

        lat = re.search(r'\\?"latitude\\?":\s*(-?\d+\.\d+)', html)
        lon = re.search(r'\\?"longitude\\?":\s*(-?\d+\.\d+)', html)
        if lat and lon:
            return f"{lat.group(1)},{lon.group(1)}"
        return None

    def _configured_api_key(self) -> Optional[str]:
        """API key from the environment, if the operator supplied one."""
        return os.getenv("WUNDERGROUND_API_KEY") or os.getenv("TWC_API_KEY")

    def _geocode(self, harvested: Optional[str] = None) -> str:
        """Geocode to request: env override > harvested from page > default."""
        return os.getenv("WUNDERGROUND_GEOCODE") or harvested or self.GEOCODE

    def _fetch_via_api(self, api_key: str, geocode: str) -> Optional[List[WUndergroundDay]]:
        """Fetch the 10-day forecast from the TWC v3 API."""
        if not HAS_CURL_CFFI:
            return None

        params = {
            "geocode": geocode,
            "format": "json",
            "units": "e",  # Imperial (Fahrenheit)
            "language": "en-US",
            "apiKey": api_key,
        }
        url = f"{self.API_URL}?{'&'.join(f'{k}={v}' for k, v in params.items())}"

        logger.info(f"[WUndergroundProvider] Fetching TWC API for geocode {geocode}")

        try:
            from curl_cffi.requests import Session

            headers = {
                "Accept": "application/json",
                "Referer": "https://www.wunderground.com/",
                "Origin": "https://www.wunderground.com",
            }

            response = None
            for target in IMPERSONATE_TARGETS:
                try:
                    with Session(impersonate=target) as session:
                        response = session.get(
                            url, headers=headers, timeout=30, verify=get_ca_bundle_for_curl()
                        )
                    break
                except Exception as e:
                    logger.warning(f"[WUndergroundProvider] API call with {target} failed: {e}")

            if response is None:
                return None

            if response.status_code != 200:
                logger.error(f"[WUndergroundProvider] API HTTP {response.status_code}")
                return None

            data = response.json()
            daypart = data.get('daypart', [{}])
            dp = daypart[0] if daypart else {}

            days = self._build_days(
                days_of_week=data.get('dayOfWeek', []),
                max_temps=data.get('temperatureMax', []),
                min_temps=data.get('temperatureMin', []),
                precip_chances=dp.get('precipChance', []),
                valid_times=data.get('validTimeLocal', []),
                wx_phrases=dp.get('wxPhraseLong', []),
                calendar_max=data.get('calendarDayTemperatureMax', []),
                calendar_min=data.get('calendarDayTemperatureMin', []),
            )

            if days:
                logger.info(f"[WUndergroundProvider] Retrieved {len(days)} days from TWC API")
                return days

            logger.error("[WUndergroundProvider] API response carried no usable temperatures")
            return None

        except Exception as e:
            logger.error(f"[WUndergroundProvider] API fetch failed: {e}", exc_info=True)
            return None

    def fetch_sync(self) -> Optional[List[WUndergroundDay]]:
        """
        Synchronously fetch the 10-day forecast from Weather Underground.

        Walks the fetch chain documented at the top of this module and returns
        the first result that covers today. Returns None (never stale data) if
        every path fails - the caller renders that as an empty row rather than
        showing yesterday's forecast under today's dates.
        """
        if not HAS_CURL_CFFI:
            logger.error("[WUndergroundProvider] Missing dependency: curl_cffi")
            return None

        # Only skip the network when the daily cap is hit AND cache is fresh
        if self._should_use_cache():
            cached = self._get_fresh_cache()
            if cached:
                return cached

        # 1. The page itself. It is the alignment target, and it also carries
        #    the exact apiKey and geocode the site renders from, so it leads.
        html = self._fetch_page()
        if html:
            days = self._parse_embedded_json(html)
            if days:
                self._save_cache(days)
                return days

            # 2. The API, using the credentials the page just handed us
            harvested_key = self._extract_api_key(html)
            if harvested_key:
                logger.info("[WUndergroundProvider] Harvested apiKey from page - retrying via API")
                days = self._fetch_via_api(harvested_key, self._geocode(self._extract_geocode(html)))
                if days:
                    self._save_cache(days)
                    return days
            else:
                logger.error("[WUndergroundProvider] No apiKey found in page")

        # 3. The API with an operator-supplied key. Last network resort: the
        #    geocode here is configured rather than harvested, so it can drift
        #    from what wunderground.com shows for the 95350 page.
        api_key = self._configured_api_key()
        if api_key:
            logger.warning("[WUndergroundProvider] Page unreachable - falling back to configured API key")
            days = self._fetch_via_api(api_key, self._geocode())
            if days:
                self._save_cache(days)
                return days

        # 4. Fresh cache as the last resort
        cached = self._get_fresh_cache()
        if cached:
            return cached

        logger.error(
            "[WUndergroundProvider] ALL fetch paths failed and no fresh cache - "
            "the WUNDERGRND row will be blank in this report"
        )
        return None

    async def fetch_async(self) -> Optional[List[WUndergroundDay]]:
        """
        Async wrapper for fetch_sync (curl_cffi is synchronous).

        Returns:
            List of WUndergroundDay dicts, or None on failure
        """
        # curl_cffi is synchronous, so we just call the sync version
        return self.fetch_sync()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    print("=" * 60)
    print("  WEATHER UNDERGROUND PROVIDER TEST")
    print("=" * 60)

    provider = WUndergroundProvider()
    data = provider.fetch_sync()

    if data:
        print(f"\n[RESULTS] Weather Underground 10-Day Forecast ({len(data)} days):")
        print("-" * 50)
        print(f"{'Date':<12} | {'Day':<5} | {'High':<6} | {'Low':<6}")
        print("-" * 50)
        for day in data:
            print(f"{day['date']:<12} | {day['day_name']:<5} | {day['high_f']:.0f}F   | {day['low_f']:.0f}F")
    else:
        print("[FAILED] Could not fetch Weather Underground data")

    print("\n" + "=" * 60)
