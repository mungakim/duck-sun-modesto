"""
Weather.com Provider for Duck Sun Modesto

Fetches 10-day forecast from Weather.com for Downtown Modesto, CA.
Uses curl_cffi with Chrome impersonation for both API and scraping paths.

Primary: TWC v3 internal API (JSON, requires TWC_API_KEY)
Fallback: Web scraping of weather.com/weather/tenday page

Rate limited to 6 curl_cffi requests/day (scraping-style endpoint).
Cache is only used when rate-limited AND fresh (< 6 hours old).
Stale cache is never served — rate limit is overridden if cache expires.

Weight: 4.0 (same as AccuWeather - commercial provider)
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, TypedDict
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

# Cache configuration
CACHE_DIR = Path("outputs")
CACHE_FILE = CACHE_DIR / "weathercom_cache.json"
CACHE_MAX_AGE_HOURS = 6  # Only use cache if less than 6 hours old
DAILY_CALL_LIMIT = 6  # Cap curl_cffi requests per day (scraping-style endpoint)


class WeatherComDay(TypedDict):
    """Daily forecast data from Weather.com."""
    date: str          # YYYY-MM-DD format
    day_name: str      # Day name (Mon, Tue, etc.)
    high_f: float      # High temperature in Fahrenheit
    low_f: float       # Low temperature in Fahrenheit
    high_c: float      # High temperature in Celsius
    low_c: float       # Low temperature in Celsius
    condition: str     # Weather condition text
    precip_prob: int   # Precipitation probability


class WeatherComProvider:
    """
    Provider for Weather.com data via web scraping.

    Uses curl_cffi with Chrome impersonation to bypass anti-bot protection.
    Returns 10-day forecast data compatible with ensemble weighting.

    Weight: 4.0 (commercial provider tier)
    """

    # Use Weather.com's internal API endpoint (JSON) - more reliable
    # This is the same API that powers their website
    API_URL = "https://api.weather.com/v3/wx/forecast/daily/10day"
    GEOCODE = "37.64,-120.99"  # Modesto, CA

    def __init__(self):
        logger.info("[WeatherComProvider] Initializing provider...")
        if not HAS_CURL_CFFI:
            logger.warning("[WeatherComProvider] curl_cffi not installed - provider disabled")
        if not HAS_BS4:
            logger.warning("[WeatherComProvider] beautifulsoup4 not installed - provider disabled")

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
            logger.warning(f"[WeatherComProvider] Cache load error: {e}")
            return None

    def _save_cache(self, data: List['WeatherComDay']) -> None:
        """Save forecast data to cache with call counter."""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            existing = self._load_cache()
            call_count = 0

            if existing and existing.get('call_date') == today:
                call_count = existing.get('call_count', 0)

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
            logger.info(f"[WeatherComProvider] Cache saved ({len(data)} days, call #{call_count}/{DAILY_CALL_LIMIT} today)")
        except Exception as e:
            logger.error(f"[WeatherComProvider] Cache save failed: {e}")

    def _should_use_cache(self) -> bool:
        """
        Check if we should skip the API call and use cache instead.

        Returns True ONLY if:
        - Rate limit has been reached today, AND
        - Cache is still fresh (< CACHE_MAX_AGE_HOURS old)

        If cache is stale, we override the rate limit and fetch fresh data
        to prevent serving outdated forecasts.
        """
        cache = self._load_cache()
        if not cache:
            return False

        today = datetime.now().strftime('%Y-%m-%d')
        if cache.get('call_date') != today:
            return False  # New day, rate limit reset

        call_count = cache.get('call_count', 0)
        if call_count < DAILY_CALL_LIMIT:
            return False  # Under the limit, go ahead and fetch

        # Rate limit reached - check if cache is still fresh enough to use
        try:
            age = datetime.now() - datetime.fromisoformat(cache['timestamp'])
            age_hours = age.total_seconds() / 3600

            if age_hours > CACHE_MAX_AGE_HOURS:
                logger.warning(
                    f"[WeatherComProvider] Rate limit reached ({call_count}/{DAILY_CALL_LIMIT}) "
                    f"but cache is stale ({age_hours:.1f}h) - overriding limit to fetch fresh"
                )
                return False  # Override: stale cache is worse than an extra API call

            logger.info(
                f"[WeatherComProvider] Rate limit reached ({call_count}/{DAILY_CALL_LIMIT}), "
                f"using fresh cache ({age_hours:.1f}h old)"
            )
            return True
        except Exception:
            return False

    def _get_fresh_cache(self) -> Optional[List['WeatherComDay']]:
        """Return cached data only if it's fresh (< CACHE_MAX_AGE_HOURS old)."""
        cache = self._load_cache()
        if not cache or not cache.get('data'):
            return None

        try:
            age = datetime.now() - datetime.fromisoformat(cache['timestamp'])
            age_hours = age.total_seconds() / 3600

            if age_hours > CACHE_MAX_AGE_HOURS:
                logger.warning(f"[WeatherComProvider] Cache too old ({age_hours:.1f}h > {CACHE_MAX_AGE_HOURS}h max) - rejecting")
                return None

            logger.info(f"[WeatherComProvider] Using fresh cache ({age_hours:.1f}h old, limit {CACHE_MAX_AGE_HOURS}h)")
            return cache['data']
        except Exception:
            return None

    def _parse_temp(self, temp_str: str) -> Optional[int]:
        """Extract integer temperature from string like '60°' or '60'."""
        if not temp_str:
            return None
        # Remove degree symbols, slashes, and whitespace
        clean = re.sub(r'[°/\s]', '', temp_str)
        try:
            return int(clean)
        except ValueError:
            return None

    def _get_date_for_day(self, day_index: int) -> str:
        """Get YYYY-MM-DD date string for day index (0 = today)."""
        tz = ZoneInfo("America/Los_Angeles")
        today = datetime.now(tz)
        target = today + timedelta(days=day_index)
        return target.strftime('%Y-%m-%d')

    def fetch_sync(self) -> Optional[List[WeatherComDay]]:
        """
        Synchronously fetch 10-day forecast from Weather.com.

        Fetching strategy:
        1. If rate limit reached AND cache is fresh (< 6h), use cache
        2. Otherwise, always try API first, then scraping, then fresh cache
        3. Never serve cache older than CACHE_MAX_AGE_HOURS

        Returns:
            List of WeatherComDay dicts, or None on failure
        """
        if not HAS_CURL_CFFI:
            logger.error("[WeatherComProvider] Missing curl_cffi dependency")
            return None

        # Check rate limit - only skip API if cache is fresh enough
        if self._should_use_cache():
            return self._get_fresh_cache()

        # Try API endpoint first (requires TWC_API_KEY)
        api_key = os.getenv("TWC_API_KEY")
        if not api_key:
            logger.warning("[WeatherComProvider] TWC_API_KEY not set - skipping API, trying scraping")
            scraped = self._fetch_via_scraping()
            if scraped:
                return scraped
            return self._get_fresh_cache()
        params = {
            "geocode": self.GEOCODE,
            "format": "json",
            "units": "e",  # Imperial (Fahrenheit)
            "language": "en-US",
            "apiKey": api_key
        }
        url = f"{self.API_URL}?{'&'.join(f'{k}={v}' for k, v in params.items())}"

        logger.info(f"[WeatherComProvider] Fetching from Weather.com API for {self.GEOCODE}")

        try:
            from curl_cffi.requests import Session

            with Session(impersonate="chrome136") as session:
                headers = {
                    "Accept": "application/json",
                    "Referer": "https://weather.com/",
                    "Origin": "https://weather.com",
                }
                response = session.get(url, headers=headers, timeout=30, verify=get_ca_bundle_for_curl())

            if response.status_code != 200:
                logger.error(f"[WeatherComProvider] API HTTP {response.status_code}")
                scraped = self._fetch_via_scraping()
                if scraped:
                    return scraped
                return self._get_fresh_cache()

            data = response.json()

            # Extract daily forecast data
            day_of_week = data.get('dayOfWeek', [])
            temp_max = data.get('temperatureMax', [])
            temp_min = data.get('temperatureMin', [])
            narrative = data.get('narrative', [])

            # Extract daypart data for precipitation and conditions
            # daypart[0] contains alternating day/night arrays (2 entries per day)
            daypart = data.get('daypart', [{}])
            dp = daypart[0] if daypart else {}
            precip_chances = dp.get('precipChance', [])
            wx_phrases = dp.get('wxPhraseLong', [])

            if not temp_max or not temp_min:
                logger.error("[WeatherComProvider] No temperature data in API response")
                scraped = self._fetch_via_scraping()
                if scraped:
                    return scraped
                return self._get_fresh_cache()

            results: List[WeatherComDay] = []
            num_days = min(10, len(temp_max), len(temp_min))

            logger.info(f"[WeatherComProvider] Found {num_days} forecast days from API")

            for i in range(num_days):
                high_f = temp_max[i]
                low_f = temp_min[i]

                if high_f is None or low_f is None:
                    logger.warning(f"[WeatherComProvider] Null temps for day {i}")
                    continue

                # Convert to Celsius
                high_c = (high_f - 32) * 5 / 9
                low_c = (low_f - 32) * 5 / 9

                # Get day name
                day_name = day_of_week[i][:3] if i < len(day_of_week) else f"D{i}"

                # Get date
                date_str = self._get_date_for_day(i)

                # Get condition from daypart wxPhraseLong (daytime preferred)
                day_dp_idx = i * 2
                night_dp_idx = i * 2 + 1
                if day_dp_idx < len(wx_phrases) and wx_phrases[day_dp_idx]:
                    condition = wx_phrases[day_dp_idx]
                elif night_dp_idx < len(wx_phrases) and wx_phrases[night_dp_idx]:
                    condition = wx_phrases[night_dp_idx]
                else:
                    condition = narrative[i][:50] if i < len(narrative) else "Unknown"

                # Get precipitation probability (daytime value to match weather.com website)
                day_precip = precip_chances[day_dp_idx] if day_dp_idx < len(precip_chances) else None
                night_precip = precip_chances[night_dp_idx] if night_dp_idx < len(precip_chances) else None
                if day_precip is not None:
                    precip_prob = day_precip
                elif night_precip is not None:
                    precip_prob = night_precip
                else:
                    precip_prob = 0

                results.append({
                    "date": date_str,
                    "day_name": day_name,
                    "high_f": float(high_f),
                    "low_f": float(low_f),
                    "high_c": round(high_c, 2),
                    "low_c": round(low_c, 2),
                    "condition": condition,
                    "precip_prob": precip_prob
                })

                logger.debug(f"[WeatherComProvider] {date_str}: Hi={high_f}F, Lo={low_f}F")

            logger.info(f"[WeatherComProvider] [OK] Retrieved {len(results)} daily records from API")
            self._save_cache(results)
            return results

        except Exception as e:
            logger.error(f"[WeatherComProvider] API fetch failed: {e}", exc_info=True)
            # Try scraping fallback, then fresh cache as last resort
            scraped = self._fetch_via_scraping()
            if scraped:
                return scraped
            cached = self._get_fresh_cache()
            if cached:
                return cached
            logger.error("[WeatherComProvider] All fetch methods failed and no fresh cache available")
            return None

    def _fetch_via_scraping(self) -> Optional[List[WeatherComDay]]:
        """Fallback to web scraping if API fails."""
        logger.info("[WeatherComProvider] Falling back to web scraping...")

        scrape_url = "https://weather.com/weather/tenday/l/37.6393,-120.9969"

        try:
            from curl_cffi.requests import Session

            # Use a session to handle cookies - first visit homepage to get session cookies
            with Session(impersonate="chrome110") as session:
                verify = get_ca_bundle_for_curl()
                # First request to get cookies
                logger.debug("[WeatherComProvider] Getting session cookies from homepage...")
                home_resp = session.get("https://weather.com/", timeout=15, verify=verify)
                logger.debug(f"[WeatherComProvider] Homepage status: {home_resp.status_code}")
                # Now fetch the forecast page with cookies
                response = session.get(scrape_url, timeout=30, verify=verify)

            if response.status_code != 200:
                logger.error(f"[WeatherComProvider] Scraping HTTP {response.status_code}")
                return None

            if not HAS_BS4:
                logger.error("[WeatherComProvider] BeautifulSoup not available for scraping")
                return None

            soup = BeautifulSoup(response.content, 'html.parser')

            day_names = soup.find_all(attrs={"data-testid": "daypartName"})
            high_temps = soup.find_all(
                class_=lambda x: x and 'highTempValue' in str(x) if x else False
            )
            low_temps = soup.find_all(attrs={"data-testid": "lowTempValue"})
            precip_elems = soup.find_all(attrs={"data-testid": "PercentageValue"})
            condition_elems = soup.find_all(attrs={"data-testid": "wxPhrase"})

            if not high_temps or not low_temps:
                logger.error("[WeatherComProvider] No forecast data found in page")
                return None

            results: List[WeatherComDay] = []
            num_days = min(10, len(high_temps), len(low_temps))

            for i in range(num_days):
                high_f = self._parse_temp(high_temps[i].text)
                low_f = self._parse_temp(low_temps[i].text.replace('/', ''))

                if high_f is None or low_f is None:
                    continue

                high_c = (high_f - 32) * 5 / 9
                low_c = (low_f - 32) * 5 / 9
                day_name = day_names[i].text.strip()[:3] if i < len(day_names) else f"D{i}"
                date_str = self._get_date_for_day(i)

                # Extract precipitation percentage from scraped page
                precip_prob = 0
                if i < len(precip_elems):
                    precip_text = precip_elems[i].text.strip().replace('%', '')
                    try:
                        precip_prob = int(precip_text)
                    except ValueError:
                        pass

                # Extract condition text from scraped page
                condition = "Unknown"
                if i < len(condition_elems):
                    condition = condition_elems[i].text.strip()

                results.append({
                    "date": date_str,
                    "day_name": day_name,
                    "high_f": float(high_f),
                    "low_f": float(low_f),
                    "high_c": round(high_c, 2),
                    "low_c": round(low_c, 2),
                    "condition": condition,
                    "precip_prob": precip_prob
                })

            logger.info(f"[WeatherComProvider] [OK] Retrieved {len(results)} records via scraping")
            if results:
                self._save_cache(results)
            return results if results else None

        except Exception as e:
            logger.error(f"[WeatherComProvider] Scraping failed: {e}")
            return None

    async def fetch_async(self) -> Optional[List[WeatherComDay]]:
        """
        Async wrapper for fetch_sync (curl_cffi is synchronous).

        Returns:
            List of WeatherComDay dicts, or None on failure
        """
        # curl_cffi is synchronous, so we just call the sync version
        # In a real production setup, you'd use run_in_executor
        return self.fetch_sync()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    print("=" * 60)
    print("  WEATHER.COM PROVIDER TEST")
    print("=" * 60)

    provider = WeatherComProvider()
    data = provider.fetch_sync()

    if data:
        print(f"\n[RESULTS] Weather.com 10-Day Forecast ({len(data)} days):")
        print("-" * 70)
        print(f"{'Date':<12} {'Day':<5} | {'High':<6} | {'Low':<6} | {'Precip':<7} | {'Condition'}")
        print("-" * 70)
        for day in data:
            print(f"{day['date']:<12} {day['day_name']:<5} | {day['high_f']:.0f}F    | {day['low_f']:.0f}F    | {day['precip_prob']:>3}%    | {day['condition']}")
    else:
        print("[FAILED] Could not fetch Weather.com data")

    print("\n" + "=" * 60)
