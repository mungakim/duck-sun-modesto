"""
Weather.com Provider for Duck Sun Modesto

Scrapes 10-day forecast from Weather.com for Downtown Modesto, CA.
Uses curl_cffi with Chrome impersonation to bypass anti-bot measures.

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

logger = logging.getLogger(__name__)

# Rate limiting configuration
CACHE_DIR = Path("outputs")
CACHE_FILE = CACHE_DIR / "weathercom_cache.json"
DAILY_CALL_LIMIT = 3  # Hard cap: max 3 web scrapes per day

# SSL verification toggle for corporate proxy environments
SKIP_SSL_VERIFY = os.getenv("DUCK_SUN_SKIP_SSL_VERIFY", "").lower() in ("1", "true", "yes")


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
    Extracts forecast data INCLUDING PRECIPITATION from embedded JSON in page.
    Returns 10-day forecast data compatible with ensemble weighting.

    Weight: 4.0 (commercial provider tier)

    NOTE: This uses FREE web scraping, not the paid TWC API ($500/month).
    """

    # Scrape URL for Modesto, CA 10-day forecast
    SCRAPE_URL = "https://weather.com/weather/tenday/l/37.6393,-120.9969"

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

    def _save_cache(self, data: List['WeatherComDay'], increment_call: bool = True) -> None:
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
            logger.info(f"[WeatherComProvider] Cache saved: call #{call_count}/{DAILY_CALL_LIMIT} today")
        except Exception as e:
            logger.error(f"[WeatherComProvider] Cache save failed: {e}")

    def _is_rate_limited(self) -> bool:
        """Check if daily rate limit has been reached."""
        cache = self._load_cache()
        if not cache:
            return False

        today = datetime.now().strftime('%Y-%m-%d')
        if cache.get('call_date') != today:
            logger.info("[WeatherComProvider] New day - rate limit reset")
            return False

        call_count = cache.get('call_count', 0)
        if call_count >= DAILY_CALL_LIMIT:
            logger.warning(f"[WeatherComProvider] RATE LIMIT REACHED ({call_count}/{DAILY_CALL_LIMIT} calls today)")
            return True

        logger.info(f"[WeatherComProvider] Daily calls: {call_count}/{DAILY_CALL_LIMIT}")
        return False

    def _get_cached_data(self) -> Optional[List['WeatherComDay']]:
        """Return cached data if available."""
        cache = self._load_cache()
        if cache and cache.get('data'):
            age = datetime.now() - datetime.fromisoformat(cache['timestamp'])
            logger.info(f"[WeatherComProvider] Using cached data ({age.total_seconds()/3600:.1f}h old)")
            return cache['data']
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
        Synchronously fetch 10-day forecast from Weather.com via web scraping.

        Uses curl_cffi with Chrome impersonation to bypass anti-bot protection.
        Extracts forecast data from embedded JSON in the page's script tags.

        Rate limited to 3 calls/day to avoid anti-bot detection.

        Returns:
            List of WeatherComDay dicts, or None on failure
        """
        if not HAS_CURL_CFFI:
            logger.error("[WeatherComProvider] Missing curl_cffi dependency")
            return None

        # Check rate limit - return cached data if limit reached
        if self._is_rate_limited():
            cached = self._get_cached_data()
            if cached:
                return cached
            logger.warning("[WeatherComProvider] Rate limited and no cache available")
            return None

        # Use web scraping (FREE) - no API key needed
        return self._fetch_via_scraping()

    def _extract_json_array(self, pattern: str, data: str, is_numeric: bool = True) -> List:
        """Extract array values from JavaScript/JSON data using regex."""
        match = re.search(pattern, data)
        if match:
            arr_str = match.group(1)
            if is_numeric:
                values = []
                for x in arr_str.split(','):
                    x = x.strip()
                    if x == 'null' or not x:
                        values.append(None)
                    else:
                        try:
                            values.append(int(x))
                        except ValueError:
                            try:
                                values.append(int(float(x)))
                            except ValueError:
                                values.append(None)
                return values
            else:
                return [x.strip().strip('"').strip("'") for x in arr_str.split(',')]
        return []

    def _fetch_via_scraping(self) -> Optional[List[WeatherComDay]]:
        """
        Fetch forecast via web scraping with embedded JSON extraction.

        Weather.com embeds forecast JSON in script tags, similar to Weather Underground.
        This extracts temps AND precipitation from that embedded data.
        """
        logger.info("[WeatherComProvider] Fetching via web scraping (curl_cffi)...")

        scrape_url = "https://weather.com/weather/tenday/l/37.6393,-120.9969"

        try:
            from curl_cffi.requests import Session

            # Use a session to handle cookies
            with Session(impersonate="chrome110") as session:
                # First request to get cookies
                logger.debug("[WeatherComProvider] Getting session cookies from homepage...")
                home_resp = session.get("https://weather.com/", timeout=15, verify=not SKIP_SSL_VERIFY)
                logger.debug(f"[WeatherComProvider] Homepage status: {home_resp.status_code}")

                # Now fetch the forecast page with cookies
                response = session.get(scrape_url, timeout=30, verify=not SKIP_SSL_VERIFY)

            if response.status_code != 200:
                logger.error(f"[WeatherComProvider] Scraping HTTP {response.status_code}")
                return None

            if not HAS_BS4:
                logger.error("[WeatherComProvider] BeautifulSoup not available for scraping")
                return None

            soup = BeautifulSoup(response.content, 'html.parser')

            # METHOD 1: Try to extract from embedded JSON in script tags (most reliable)
            scripts = soup.find_all('script')
            forecast_json = None

            for script in scripts:
                if script.string and 'temperatureMax' in script.string and 'precipChance' in script.string:
                    forecast_json = script.string
                    logger.info("[WeatherComProvider] Found embedded forecast JSON with precip data")
                    break

            if forecast_json:
                # Extract arrays from embedded JSON
                days_of_week = self._extract_json_array(
                    r'"dayOfWeek":\s*\[([^\]]+)\]',
                    forecast_json,
                    is_numeric=False
                )
                max_temps = self._extract_json_array(
                    r'"temperatureMax":\s*\[([^\]]+)\]',
                    forecast_json
                )
                min_temps = self._extract_json_array(
                    r'"temperatureMin":\s*\[([^\]]+)\]',
                    forecast_json
                )
                # Extract precipitation - daypart has alternating day/night values
                precip_chances = self._extract_json_array(
                    r'"precipChance":\s*\[([^\]]+)\]',
                    forecast_json
                )

                if days_of_week and max_temps and min_temps:
                    logger.info(f"[WeatherComProvider] Extracted from JSON: {len(days_of_week)} days, precip: {precip_chances[:6]}")

                    # Build daily precip from day/night pairs (take max of each pair)
                    daily_precip = []
                    for i in range(0, len(precip_chances), 2):
                        day_p = precip_chances[i] if i < len(precip_chances) and precip_chances[i] is not None else 0
                        night_p = precip_chances[i + 1] if i + 1 < len(precip_chances) and precip_chances[i + 1] is not None else 0
                        daily_precip.append(max(day_p, night_p))

                    results: List[WeatherComDay] = []
                    num_days = min(10, len(days_of_week), len(max_temps), len(min_temps))

                    for i in range(num_days):
                        high_f = max_temps[i]
                        low_f = min_temps[i]

                        if high_f is None or low_f is None:
                            continue

                        high_c = (high_f - 32) * 5 / 9
                        low_c = (low_f - 32) * 5 / 9
                        day_name = days_of_week[i][:3] if days_of_week[i] else f"D{i}"
                        date_str = self._get_date_for_day(i)
                        precip = daily_precip[i] if i < len(daily_precip) else 0

                        results.append({
                            "date": date_str,
                            "day_name": day_name,
                            "high_f": float(high_f),
                            "low_f": float(low_f),
                            "high_c": round(high_c, 2),
                            "low_c": round(low_c, 2),
                            "condition": "Unknown",
                            "precip_prob": precip
                        })

                        logger.debug(f"[WeatherComProvider] {date_str}: Hi={high_f}F, Lo={low_f}F, Precip={precip}%")

                    logger.info(f"[WeatherComProvider] [OK] Retrieved {len(results)} records via JSON extraction")
                    if results:
                        self._save_cache(results)
                    return results if results else None

            # METHOD 2: Fall back to HTML element parsing (no precip available)
            logger.warning("[WeatherComProvider] No embedded JSON found, falling back to HTML parsing (no precip)")

            day_names = soup.find_all(attrs={"data-testid": "daypartName"})
            high_temps = soup.find_all(
                class_=lambda x: x and 'highTempValue' in str(x) if x else False
            )
            low_temps = soup.find_all(attrs={"data-testid": "lowTempValue"})

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

                results.append({
                    "date": date_str,
                    "day_name": day_name,
                    "high_f": float(high_f),
                    "low_f": float(low_f),
                    "high_c": round(high_c, 2),
                    "low_c": round(low_c, 2),
                    "condition": "Unknown",
                    "precip_prob": 0  # No precip available via HTML parsing
                })

            logger.info(f"[WeatherComProvider] [OK] Retrieved {len(results)} records via HTML parsing (no precip)")
            if results:
                self._save_cache(results)
            return results if results else None

        except Exception as e:
            logger.error(f"[WeatherComProvider] Scraping failed: {e}", exc_info=True)
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
        print("-" * 50)
        print(f"{'Day':<15} | {'High':<6} | {'Low':<6}")
        print("-" * 35)
        for day in data:
            print(f"{day['day_name']:<15} | {day['high_f']:.0f}F    | {day['low_f']:.0f}F")
    else:
        print("[FAILED] Could not fetch Weather.com data")

    print("\n" + "=" * 60)
